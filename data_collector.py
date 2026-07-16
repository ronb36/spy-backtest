"""
SPY Backtest Data Collector — v2.3.0 (Supabase)
=================================================
v2.3.0 — Phase 3.5: extend option day bars for active contracts.
Phase 3 fills missing *tickers* only (full history at add time) and never
revisits them, so option_days freshness was frozen at each ticker's add
date (root cause of the 2026-07-10 staleness found in Session 28).
Phase 3.5 appends new daily bars nightly, watermark-anchored and
idempotent — any outage self-heals on the next run.

Replaces GitHub JSON storage with Supabase Postgres.
No file size limits, no LFS, no build caching issues.
Browser fetches directly from Supabase — no proxy needed.

Tables (created via SQL Editor in Supabase dashboard):
  spy_daily     — date PK, o, h, l, c, v
  vix_daily     — date PK, c
  options       — ticker PK, strike, expiry, otm_pct, entry_spy
  option_days   — (ticker, date) PK, o, h, l, c, vw, v
  metadata      — key PK, value

Environment variables (Railway):
  POLYGON_KEY       — Polygon API key
  SUPABASE_URL      — https://pepdnkwytziegjvgkofq.supabase.co
  SUPABASE_KEY      — service_role secret key
  PUSHOVER_USER_TOKEN / PUSHOVER_API_TOKEN — optional notifications
"""

import os
import json
import time
import requests
from datetime import datetime, timedelta, date, timezone
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

COLLECTOR_VERSION = "2.3.0"

# ── Credentials ────────────────────────────────────────────────────────────
POLYGON_KEY   = os.environ["POLYGON_KEY"]
SUPABASE_URL  = os.environ.get("SUPABASE_URL", "https://pepdnkwytziegjvgkofq.supabase.co")
SUPABASE_KEY  = os.environ["SUPABASE_KEY"]
PUSHOVER_USER  = os.environ.get("PUSHOVER_USER_TOKEN")
PUSHOVER_TOKEN = os.environ.get("PUSHOVER_API_TOKEN")

# ── Config ─────────────────────────────────────────────────────────────────
UNIVERSE_PATH = "data_universe.json"

def load_universe_config():
    try:
        with open(UNIVERSE_PATH) as f:
            return json.load(f)
    except Exception as e:
        log(f"⚠ Could not load {UNIVERSE_PATH}: {e} — using defaults")
        return {}

_cfg = load_universe_config()

OTM_TARGETS   = _cfg.get("otm_targets", [
    0.01, 0.02, 0.03, 0.04, 0.05,
    0.06, 0.07, 0.08, 0.09, 0.10,
    0.11, 0.12, 0.13, 0.14, 0.15,
    0.18, 0.20, 0.22, 0.25, 0.28, 0.30,
])
CALL_DELAY    = _cfg.get("call_delay", 0.25)
USE_MONTHLY   = _cfg.get("monthly", True)
USE_WEEKLY    = _cfg.get("weekly", False)
START_DATE    = _cfg.get("start_date", "2024-07-01")
MAX_LOOKAHEAD = _cfg.get("max_expiry_lookahead_days", 180)
EXP_LOOKBACK  = _cfg.get("expiry_lookback_days", 365)


def log(msg):
    print(f"{datetime.now(ET).strftime('%H:%M:%S ET')} — {msg}", flush=True)


def today_str():
    return date.today().strftime("%Y-%m-%d")


# ── Supabase REST helpers ──────────────────────────────────────────────────
SB_HEADERS = {
    "apikey":        SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type":  "application/json",
    "Prefer":        "resolution=merge-duplicates",
}

def sb_url(table):
    return f"{SUPABASE_URL}/rest/v1/{table}"

def sb_upsert(table, rows, chunk_size=500):
    """Upsert rows into a Supabase table in chunks."""
    if not rows:
        return True
    total = 0
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i+chunk_size]
        r = requests.post(sb_url(table), headers=SB_HEADERS,
                          json=chunk, timeout=30)
        if r.status_code not in (200, 201):
            log(f"  ✗ Supabase upsert {table} failed: {r.status_code} {r.text[:200]}")
            return False
        total += len(chunk)
    return True

def sb_select(table, select="*", filters=None, limit=None):
    """Select rows from Supabase (single page, max 1000)."""
    params = {"select": select}
    if filters:
        params.update(filters)
    if limit:
        params["limit"] = limit
    headers = {**SB_HEADERS, "Prefer": ""}
    r = requests.get(sb_url(table), headers=headers, params=params, timeout=30)
    if r.status_code != 200:
        log(f"  ✗ Supabase select {table} failed: {r.status_code}")
        return []
    return r.json()

def sb_select_all(table, select="*"):
    """Paginated select — fetches all rows regardless of count."""
    all_rows = []
    offset = 0
    page_size = 1000
    while True:
        headers = {**SB_HEADERS, "Prefer": "",
                   "Range": f"{offset}-{offset + page_size - 1}",
                   "Range-Unit": "items"}
        r = requests.get(sb_url(table), headers=headers,
                         params={"select": select}, timeout=30)
        if r.status_code not in (200, 206):
            log(f"  ✗ Supabase paginated select {table} failed: {r.status_code}")
            break
        batch = r.json()
        all_rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return all_rows

def sb_get_dates(table, date_col="date"):
    """Get all existing dates from a table — paginated."""
    rows = sb_select_all(table, select=date_col)
    return {r[date_col] for r in rows}

def sb_get_tickers():
    """Get all existing option tickers — paginated."""
    rows = sb_select_all("options", select="ticker")
    return {r["ticker"] for r in rows}

def sb_get_option_day_keys():
    """Get all existing (ticker, date) pairs from option_days — paginated."""
    rows = sb_select_all("option_days", select="ticker,date")
    return {(r["ticker"], r["date"]) for r in rows}

def sb_set_metadata(key, value):
    sb_upsert("metadata", [{"key": key, "value": str(value)}])


# ── Polygon helpers ────────────────────────────────────────────────────────
def fetch_aggs(ticker, start, end, limit=5000):
    url = (f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/"
           f"{start}/{end}?adjusted=true&sort=asc&limit={limit}&apiKey={POLYGON_KEY}")
    try:
        r = requests.get(url, timeout=15)
        data = r.json()
        return data.get("results", [])
    except Exception as e:
        log(f"  ✗ fetch_aggs {ticker}: {e}")
        return []


def find_nearest_strike(spy_price, otm_pct, grid=5):
    target = spy_price * (1 + otm_pct)
    return round(round(target / grid) * grid, 2)


def build_option_ticker(expiry_iso, strike):
    d = datetime.strptime(expiry_iso, "%Y-%m-%d")
    strike_str = str(int(round(strike * 1000))).zfill(8)
    return f"O:SPY{d.strftime('%y%m%d')}C{strike_str}"


def fetch_option_history(ticker, start, end):
    results = fetch_aggs(ticker, start, end)
    time.sleep(CALL_DELAY)
    days = []
    for r in results:
        dt = datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        days.append({
            "date": dt,
            "o":    round(r.get("o", 0), 4),
            "h":    round(r.get("h", 0), 4),
            "l":    round(r.get("l", 0), 4),
            "c":    round(r.get("c", 0), 4),
            "v":    r.get("v", 0),
            "vw":   round(r.get("vw", 0), 4),
        })
    return days


def date_range_str(years_back=2):
    end   = date.today().strftime("%Y-%m-%d")
    start = (date.today() - timedelta(days=365*years_back)).strftime("%Y-%m-%d")
    start = max(start, START_DATE)
    return start, end


# ── Expiry helpers ─────────────────────────────────────────────────────────
def get_monthly_expiries(start_date, end_date):
    import calendar
    expiries = []
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end   = datetime.strptime(end_date, "%Y-%m-%d").date()
    year, month = start.year, start.month
    while date(year, month, 1) <= end:
        cal     = calendar.monthcalendar(year, month)
        fridays = [week[calendar.FRIDAY] for week in cal if week[calendar.FRIDAY] != 0]
        exp     = date(year, month, fridays[2])
        if start <= exp <= end:
            expiries.append(exp.strftime("%Y-%m-%d"))
        month += 1
        if month > 12:
            month = 1
            year += 1
    return expiries


def get_weekly_expiries(start_date, end_date):
    import calendar
    expiries = []
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end   = datetime.strptime(end_date,   "%Y-%m-%d").date()
    current = start
    while current <= end:
        if current.weekday() == calendar.FRIDAY:
            expiries.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return expiries


# ── Phase 1+2: SPY/VIX daily append ───────────────────────────────────────
def append_spy_vix(start, end):
    """Fetch SPY and VIX and upsert any missing dates."""
    existing_spy = sb_get_dates("spy_daily")
    existing_vix = sb_get_dates("vix_daily")

    log(f"Fetching SPY {start} → {end}...")
    spy_raw = fetch_aggs("SPY", start, end)
    time.sleep(CALL_DELAY)
    new_spy = []
    for r in spy_raw:
        dt = datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        if dt not in existing_spy:
            new_spy.append({"date": dt, "o": r["o"], "h": r["h"],
                            "l": r["l"], "c": r["c"], "v": r.get("v", 0)})
    if new_spy:
        sb_upsert("spy_daily", new_spy)
        log(f"  ✓ Added {len(new_spy)} SPY days")
    else:
        log(f"  ✓ SPY up to date")

    log(f"Fetching VIX {start} → {end}...")
    vix_raw = fetch_aggs("I:VIX", start, end)
    time.sleep(CALL_DELAY)
    new_vix = []
    for r in vix_raw:
        dt = datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        if dt not in existing_vix:
            new_vix.append({"date": dt, "c": round(r["c"], 2)})
    if new_vix:
        sb_upsert("vix_daily", new_vix)
        log(f"  ✓ Added {len(new_vix)} VIX days")
    else:
        log(f"  ✓ VIX up to date")


# ── Phase 1.5: ES futures daily OHLC ──────────────────────────────────────
def fetch_es_aggs(ticker, start, end, limit=5000):
    """Fetch ES futures daily OHLC from Massive API using correct aggs endpoint."""
    url = (f"https://api.massive.com/futures/v1/aggs/{ticker}"
           f"?resolution=1session&window_start.gte={start}&window_start.lte={end}"
           f"&limit={limit}&apiKey={POLYGON_KEY}")
    try:
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            log(f"  ✗ fetch_es_aggs {ticker}: HTTP {r.status_code} — {r.text[:100]}")
            return []
        data = r.json()
        return data.get("results", [])
    except Exception as e:
        log(f"  ✗ fetch_es_aggs {ticker}: {e} — raw: {r.text[:80] if 'r' in dir() else 'no response'}")
        return []

def get_es_front_month_ticker(date_str):
    """Return the ES front-month ticker for a given date (YYYY-MM-DD)."""
    d = date.fromisoformat(date_str)
    # Quarterly codes: H=Mar, M=Jun, U=Sep, Z=Dec
    quarters = [(3,"H"),(6,"M"),(9,"U"),(12,"Z")]
    year_digit = str(d.year)[-1]
    for month, code in quarters:
        exp = date(d.year, month, 15)
        if d <= exp:
            return f"ES{code}{year_digit}"
    return f"ESH{str(d.year + 1)[-1]}"

def append_es_daily(start, end):
    """Fetch ES front-month OHLC and upsert missing dates to es_daily."""
    existing = sb_get_dates("es_daily")
    log(f"Phase 1.5: ES daily — {len(existing)} dates already in DM")

    from datetime import date as ddate, timedelta
    quarters = [
        ("ESH", [1,2,3]),   # Mar contract covers Jan–Mar
        ("ESM", [4,5,6]),   # Jun contract covers Apr–Jun
        ("ESU", [7,8,9]),   # Sep contract covers Jul–Sep
        ("ESZ", [10,11,12]),# Dec contract covers Oct–Dec
    ]
    start_d = date.fromisoformat(start)
    end_d   = date.fromisoformat(end)

    all_rows = []
    for year in range(start_d.year, end_d.year + 1):
        year_digit = str(year)[-1]
        for code, months in quarters:
            ticker = f"{code}{year_digit}"
            q_start = date(year, months[0], 1).isoformat()
            q_end   = date(year, months[-1], 28).isoformat()
            # Clip to our overall range
            q_start = max(q_start, start)
            q_end   = min(q_end, end)
            if q_start > q_end:
                continue

            raw = fetch_es_aggs(ticker, q_start, q_end)
            for bar in raw:
                # Massive aggs uses session_end_date as the trading date
                date_iso = bar.get("session_end_date")
                if not date_iso:
                    # Fallback: convert window_start nanoseconds
                    t = bar.get("window_start")
                    if not t:
                        continue
                    date_iso = datetime.utcfromtimestamp(t / 1e9).strftime("%Y-%m-%d")
                if date_iso not in existing:
                    # Use settlement_price when available (official daily settlement)
                    # Fall back to close for open/incomplete sessions
                    settle = bar.get("settlement_price") or 0
                    close  = bar.get("close") or 0
                    all_rows.append({
                        "date":   date_iso,
                        "ticker": ticker,
                        "o":      bar.get("open"),
                        "h":      bar.get("high"),
                        "l":      bar.get("low"),
                        "c":      settle if settle > 0 else close,
                        "v":      bar.get("volume"),
                    })

    new_rows = {r["date"]: r for r in all_rows}  # dedupe by date
    if new_rows:
        sb_upsert("es_daily", list(new_rows.values()))
        log(f"  ✓ ES daily: {len(new_rows)} new dates added")
    else:
        log(f"  ✓ ES daily: up to date")
    return len(new_rows)


def inspect_and_fill(start, end):
    """
    For every SPY trading day × every expiry, compute expected strikes
    and fetch any missing contracts from Polygon.
    This is the core of the daily-anchored DM build.
    """
    log("Phase 3: Inspecting DM for missing contracts (daily-anchored)...")

    # Load SPY prices and existing tickers
    spy_rows = sb_select("spy_daily", select="date,c",
                         filters={"date": f"gte.{start}"})
    spy_by_date = {r["date"]: r["c"] for r in spy_rows}
    existing_tickers = sb_get_tickers()

    today_iso    = today_str()
    two_yr_ago   = start
    future_end   = (date.today() + timedelta(days=MAX_LOOKAHEAD)).strftime("%Y-%m-%d")

    monthly_expiries = get_monthly_expiries(two_yr_ago, future_end) if USE_MONTHLY else []
    weekly_expiries  = get_weekly_expiries(two_yr_ago, future_end)  if USE_WEEKLY  else []
    all_expiries     = sorted(set(monthly_expiries + weekly_expiries))
    monthly_set      = set(monthly_expiries)

    log(f"  Expiry universe: {len(all_expiries)} ({len(monthly_expiries)} monthly + {len(weekly_expiries)} weekly)")
    log(f"  Existing tickers: {len(existing_tickers)}")

    filled    = 0
    no_data   = 0
    already   = 0

    for exp_idx, exp in enumerate(all_expiries):
        exp_dt     = datetime.strptime(exp, "%Y-%m-%d")
        hist_start = max(two_yr_ago,
                         (exp_dt - timedelta(days=EXP_LOOKBACK)).strftime("%Y-%m-%d"))
        hist_end   = min(exp, today_iso)

        # Compute expected tickers for this expiry
        expected = set()
        for spy_date_str, spy_px in spy_by_date.items():
            if spy_date_str < hist_start or spy_date_str > hist_end:
                continue
            for otm_pct in OTM_TARGETS:
                strike = find_nearest_strike(spy_px, otm_pct)
                ticker = build_option_ticker(exp, strike)
                expected.add(ticker)

        missing_tickers = expected - existing_tickers
        already += len(expected) - len(missing_tickers)

        for ticker in sorted(missing_tickers):
            strike    = int(ticker[-8:]) / 1000
            days_data = fetch_option_history(ticker, hist_start, hist_end)
            if days_data:
                first_date = days_data[0]["date"]
                entry_spy  = spy_by_date.get(first_date, spy_by_date.get(hist_end, 0))

                # Upsert option contract
                sb_upsert("options", [{
                    "ticker":    ticker,
                    "strike":    strike,
                    "expiry":    exp,
                    "otm_pct":   round((strike / entry_spy - 1), 3) if entry_spy else 0,
                    "entry_spy": entry_spy,
                }])

                # Upsert option days
                day_rows = [{"ticker": ticker, **d} for d in days_data]
                sb_upsert("option_days", day_rows)

                existing_tickers.add(ticker)
                filled += 1
                log(f"  + {ticker} ({len(days_data)}d)")
            else:
                no_data += 1

        if (exp_idx + 1) % 10 == 0:
            log(f"  Progress: {exp_idx+1}/{len(all_expiries)} expiries, {filled} filled so far")

    log(f"  ✓ Done — {already} present, {filled} filled, {no_data} not in Polygon")
    return filled


# ── Phase 3.5: Extend option day bars (v2.3.0) ─────────────────────────────
def extend_option_days(end):
    """
    Append new daily bars to existing, still-active contracts.

    Watermark = max(date) in option_days. Eligible tickers are those with
    expiry >= watermark — this includes contracts that expired during a
    coverage gap, so their final bars aren't lost — and excludes the bulk
    of long-expired contracts whose history can never grow.

    Fetch window starts 3 days before the watermark and every fetched bar
    is upserted (the (ticker, date) PK + merge-duplicates makes overlap
    idempotent), so partial bars get corrected and outages of any length
    self-heal on the next run.
    """
    log("Phase 3.5: Extending option day bars for active contracts...")

    wm_rows = sb_select("option_days", select="date",
                        filters={"order": "date.desc"}, limit=1)
    if not wm_rows:
        log("  ⚠ option_days is empty — nothing to extend, skipping")
        return 0
    watermark = wm_rows[0]["date"]

    fetch_start = (datetime.strptime(watermark, "%Y-%m-%d")
                   - timedelta(days=3)).strftime("%Y-%m-%d")

    all_opts = sb_select_all("options", select="ticker,expiry")
    eligible = sorted((o for o in all_opts if o["expiry"] >= watermark),
                      key=lambda o: (o["expiry"], o["ticker"]))
    log(f"  Watermark: {watermark} · fetching from {fetch_start} · "
        f"eligible: {len(eligible)} of {len(all_opts)} contracts")

    new_bars  = 0
    upserted  = 0
    touched   = 0
    max_seen  = watermark

    for idx, o in enumerate(eligible):
        ticker   = o["ticker"]
        hist_end = min(o["expiry"], end)
        if hist_end < fetch_start:
            continue
        days_data = fetch_option_history(ticker, fetch_start, hist_end)
        if days_data:
            day_rows = [{"ticker": ticker, **d} for d in days_data]
            if sb_upsert("option_days", day_rows):
                upserted += len(day_rows)
                fresh = [d for d in days_data if d["date"] > watermark]
                if fresh:
                    new_bars += len(fresh)
                    touched  += 1
                    if fresh[-1]["date"] > max_seen:
                        max_seen = fresh[-1]["date"]
        if (idx + 1) % 50 == 0:
            log(f"  Progress: {idx+1}/{len(eligible)} contracts, "
                f"{new_bars} new bars so far")

    log(f"  ✓ Done — {touched} contracts extended, {new_bars} new bars "
        f"({upserted} rows upserted incl. overlap), "
        f"option_days now through {max_seen}")
    return new_bars


# ── Phase 4: Future expiry strikes ─────────────────────────────────────────
def sync_future_strikes(current_spy):
    """Add strikes for future expiries anchored to current SPY price."""
    log(f"Phase 4: Syncing future expiry strikes (SPY=${current_spy:.2f})...")
    today_iso  = today_str()
    future_end = (date.today() + timedelta(days=MAX_LOOKAHEAD)).strftime("%Y-%m-%d")

    future_m = get_monthly_expiries(today_iso, future_end) if USE_MONTHLY else []
    future_w = get_weekly_expiries(today_iso, future_end)  if USE_WEEKLY  else []
    future   = sorted(set(future_m + future_w))

    existing_tickers = sb_get_tickers()
    new_future = 0

    for exp in future:
        for otm_pct in OTM_TARGETS:
            strike = find_nearest_strike(current_spy, otm_pct)
            ticker = build_option_ticker(exp, strike)
            if ticker in existing_tickers:
                continue
            days_data = fetch_option_history(ticker, today_iso, exp)
            if days_data:
                sb_upsert("options", [{
                    "ticker":    ticker,
                    "strike":    strike,
                    "expiry":    exp,
                    "otm_pct":   otm_pct,
                    "entry_spy": current_spy,
                }])
                day_rows = [{"ticker": ticker, **d} for d in days_data]
                sb_upsert("option_days", day_rows)
                existing_tickers.add(ticker)
                new_future += 1

    log(f"  ✓ {new_future} new future contracts added")


# ── Pushover ───────────────────────────────────────────────────────────────
def send_push(title, body):
    if not PUSHOVER_USER or not PUSHOVER_TOKEN:
        return
    try:
        requests.post("https://api.pushover.net/1/messages.json", data={
            "token": PUSHOVER_TOKEN, "user": PUSHOVER_USER,
            "title": title, "message": body, "sound": "pushover",
        }, timeout=10)
    except Exception as e:
        log(f"  Pushover error: {e}")


# ── Main ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log(f"SPY Backtest Data Collector v{COLLECTOR_VERSION} starting...")
    log(f"Supabase: {SUPABASE_URL}")
    log(f"OTM targets: {len(OTM_TARGETS)} | monthly={USE_MONTHLY} weekly={USE_WEEKLY}")

    start, end = date_range_str(years_back=2)

    # Phase 1+2: SPY and VIX daily data
    append_spy_vix(start, end)

    # Phase 1.5: ES futures daily OHLC
    append_es_daily(start, end)

    # Get current SPY for Phase 3+4
    spy_rows = sb_select("spy_daily", select="date,c",
                         filters={"order": "date.desc", "limit": "1"})
    current_spy = spy_rows[0]["c"] if spy_rows else 550.0

    # Phase 3: Fill missing daily-anchored contracts
    filled = inspect_and_fill(start, end)

    # Phase 3.5: Extend option day bars for active contracts (v2.3.0)
    extend_option_days(end)

    # Phase 4: Future expiry strikes
    sync_future_strikes(current_spy)

    # Update metadata
    opt_count = len(sb_get_tickers())
    spy_count = len(sb_get_dates("spy_daily"))
    sb_set_metadata("last_updated", today_str())
    sb_set_metadata("otm_targets", json.dumps(OTM_TARGETS))
    sb_set_metadata("options_count", opt_count)

    log(f"Done — {spy_count} SPY days, {opt_count} contracts")

    send_push("📊 SPY Data Mart", f"Daily update complete\n{opt_count} contracts | SPY ${current_spy:.2f}")
