"""
SPY Backtest Data Collector
============================
Collects and maintains a historical data mart for the SPY covered call backtest.
Runs as a Railway service — initial full load on first run, daily append thereafter.

Data collected:
  - SPY daily OHLCV (2 years)
  - VIX daily close (2 years)
  - Option contracts: 22 strikes (1%–35% OTM in $5 grid) × all monthly expiries
    Each contract: full life history from first available trade date to expiry
    Strike selection anchored to SPY price at each expiry date

Output: spy_data.json committed to ronb36/spy-backtest via GitHub API

Environment variables (Railway):
  POLYGON_KEY     — Polygon/Massive API key
  GITHUB_TOKEN    — Personal access token with repo scope
  GITHUB_REPO     — ronb36/spy-backtest
  RUN_MODE        — "full" (initial load) or "daily" (append, default)
"""

import os
import json
import time
import base64
import requests
from datetime import datetime, timedelta, date, timezone
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

# ── Credentials ────────────────────────────────────────────────────────────
POLYGON_KEY    = os.environ["POLYGON_KEY"]
GITHUB_TOKEN   = os.environ["GITHUB_TOKEN"]
GITHUB_REPO    = os.environ.get("GITHUB_REPO", "ronb36/spy-backtest")
RUN_MODE       = os.environ.get("RUN_MODE", "daily")
DATA_PATH      = "data/spy_data.json"
PUSHOVER_USER  = os.environ.get("PUSHOVER_USER_TOKEN")
PUSHOVER_TOKEN = os.environ.get("PUSHOVER_API_TOKEN")

# ── OTM target levels to store per expiry ─────────────────────────────────
# 0.5% steps from 1%–20%, snapped to $5 grid — matches backfill exactly
# ~39 strikes per expiry; aligns with yield grid display
OTM_TARGETS = [
    0.010, 0.015, 0.020, 0.025, 0.030,   # 1.0–3.0%
    0.035, 0.040, 0.045, 0.050, 0.055,   # 3.5–5.5%
    0.060, 0.065, 0.070, 0.075, 0.080,   # 6.0–8.0%
    0.085, 0.090, 0.095, 0.100, 0.105,   # 8.5–10.5%
    0.110, 0.115, 0.120, 0.125, 0.130,   # 11.0–13.0%
    0.135, 0.140, 0.145, 0.150, 0.155,   # 13.5–15.5%
    0.160, 0.165, 0.170, 0.175, 0.180,   # 16.0–18.0%
    0.185, 0.190, 0.195, 0.200,          # 18.5–20.0%
]  # 39 strikes × $5 grid per expiry — continuous surface, no gaps

# ── Rate limiting ──────────────────────────────────────────────────────────
CALL_DELAY = 0.25   # 4 calls/sec — safely under Polygon's 5/sec limit


def log(msg):
    print(f"{datetime.now(ET).strftime('%H:%M:%S ET')} — {msg}", flush=True)



def send_push(title, body):
    if not PUSHOVER_USER or not PUSHOVER_TOKEN:
        return
    try:
        requests.post("https://api.pushover.net/1/messages.json", data={
            "token":   PUSHOVER_TOKEN,
            "user":    PUSHOVER_USER,
            "title":   title,
            "message": body,
            "sound":   "pushover",
        }, timeout=10)
    except Exception as e:
        log(f"  Pushover error: {e}")

# ── Date helpers ───────────────────────────────────────────────────────────
def today_str():
    return date.today().strftime("%Y-%m-%d")


def date_range_str(years_back=2):
    end   = date.today()
    start = end - timedelta(days=years_back * 365)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def get_monthly_expiries(start_date, end_date):
    """
    Return all 3rd-Friday monthly expiry dates between start and end.
    """
    import calendar
    expiries = []
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end   = datetime.strptime(end_date,   "%Y-%m-%d").date()

    year, month = start.year, start.month
    while date(year, month, 1) <= end:
        # Find 3rd Friday
        cal       = calendar.monthcalendar(year, month)
        fridays   = [week[calendar.FRIDAY] for week in cal if week[calendar.FRIDAY] != 0]
        third_fri = fridays[2]
        exp       = date(year, month, third_fri)
        if start <= exp <= end:
            expiries.append(exp.strftime("%Y-%m-%d"))
        # Advance month
        month += 1
        if month > 12:
            month = 1
            year += 1

    return expiries


def get_quarterly_expiries(start_date, end_date):
    """Return 3rd-Friday expiries for Mar/Jun/Sep/Dec only."""
    all_monthly   = get_monthly_expiries(start_date, end_date)
    quarter_months = {3, 6, 9, 12}
    return [e for e in all_monthly
            if datetime.strptime(e, "%Y-%m-%d").month in quarter_months]


def build_option_ticker(expiry_iso, strike):
    """Build Polygon option ticker e.g. O:SPY241220C00500000"""
    d          = datetime.strptime(expiry_iso, "%Y-%m-%d")
    strike_str = str(int(round(strike * 1000))).zfill(8)
    return f"O:SPY{d.strftime('%y%m%d')}C{strike_str}"


# ── Polygon fetches ────────────────────────────────────────────────────────
def fetch_aggs(ticker, start, end, timespan="day"):
    """Fetch daily OHLCV bars for a ticker."""
    url = (f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/{timespan}"
           f"/{start}/{end}?adjusted=true&sort=asc&limit=50000&apiKey={POLYGON_KEY}")
    try:
        r = requests.get(url, timeout=15).json()
        return r.get("results", [])
    except Exception as e:
        log(f"  fetch_aggs error ({ticker}): {e}")
        return []


def fetch_spy_price_on_date(target_date):
    """Get SPY closing price on a specific date."""
    results = fetch_aggs("SPY", target_date, target_date)
    if results:
        return float(results[0]["c"])
    # Try day before if no data (holiday)
    d = datetime.strptime(target_date, "%Y-%m-%d").date() - timedelta(days=1)
    results = fetch_aggs("SPY", d.strftime("%Y-%m-%d"), d.strftime("%Y-%m-%d"))
    return float(results[0]["c"]) if results else None


def find_nearest_strike(spy_price, otm_pct):
    """Round to nearest $5 strike at target OTM%."""
    target = spy_price * (1 + otm_pct)
    return round(target / 5) * 5


def fetch_option_chain_strikes(expiry_iso, spy_price):
    """
    For a given expiry, find the actual available strikes closest to our OTM targets.
    Uses Polygon options chain snapshot.
    """
    strikes = {}
    for otm_pct in OTM_TARGETS:
        nearest = find_nearest_strike(spy_price, otm_pct)
        strikes[otm_pct] = nearest
    return strikes


def fetch_option_history(ticker, start, end):
    """
    Fetch daily option OHLCV for a specific contract over a date range.
    Returns list of {date, o, h, l, c, v, vw} dicts.
    """
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


# ── GitHub API ─────────────────────────────────────────────────────────────
def github_get_file(path):
    """Get file content and SHA from GitHub."""
    url     = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}",
               "Accept": "application/vnd.github.v3+json"}
    r = requests.get(url, headers=headers, timeout=15)
    if r.status_code == 200:
        data    = r.json()
        raw     = base64.b64decode(data["content"]).decode("utf-8").strip()
        if not raw:
            return None, data["sha"]  # empty file (e.g. .gitkeep replaced)
        try:
            return json.loads(raw), data["sha"]
        except json.JSONDecodeError:
            return None, data["sha"]  # not valid JSON, treat as new
    return None, None


def github_commit_file(path, content_dict, message, sha=None):
    """Commit JSON file to GitHub repo."""
    url     = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}",
               "Accept": "application/vnd.github.v3+json"}
    content_b64 = base64.b64encode(
        json.dumps(content_dict, indent=2).encode("utf-8")
    ).decode("utf-8")
    payload = {"message": message, "content": content_b64}
    if sha:
        payload["sha"] = sha
    r = requests.put(url, headers=headers, json=payload, timeout=30)
    if r.status_code in (200, 201):
        log(f"  ✓ Committed {path} to GitHub")
        return True
    else:
        log(f"  ✗ GitHub commit failed: {r.status_code} {r.text[:200]}")
        return False


# ── Data structure helpers ─────────────────────────────────────────────────
def empty_datamart():
    return {
        "metadata": {
            "last_updated":  None,
            "spy_ticker":    "SPY",
            "otm_targets":   OTM_TARGETS,
            "created":       today_str(),
        },
        "spy":     [],   # [{date, o, h, l, c, v}]
        "vix":     [],   # [{date, c}]
        "options": {}    # {ticker: {strike, expiry, otm_pct, days:[{date,o,h,l,c,v,vw}]}}
    }


def dates_in_dataset(records, key="date"):
    return {r[key] for r in records}


# ── Full load ──────────────────────────────────────────────────────────────
def full_load():
    log("=" * 60)
    log("FULL LOAD — building 2-year data mart from scratch")
    log("=" * 60)

    dm         = empty_datamart()
    start, end = date_range_str(years_back=2)

    # ── SPY ──────────────────────────────────────────────────────────────
    log(f"Fetching SPY daily OHLCV {start} → {end}...")
    spy_raw = fetch_aggs("SPY", start, end)
    time.sleep(CALL_DELAY)
    for r in spy_raw:
        dt = datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        dm["spy"].append({"date": dt, "o": r["o"], "h": r["h"],
                          "l": r["l"], "c": r["c"], "v": r.get("v", 0)})
    log(f"  ✓ {len(dm['spy'])} SPY trading days")

    # ── VIX ──────────────────────────────────────────────────────────────
    log(f"Fetching VIX daily close {start} → {end}...")
    vix_raw = fetch_aggs("I:VIX", start, end)
    time.sleep(CALL_DELAY)
    for r in vix_raw:
        dt = datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        dm["vix"].append({"date": dt, "c": round(r["c"], 2)})
    log(f"  ✓ {len(dm['vix'])} VIX trading days")

    # ── Options ──────────────────────────────────────────────────────────
    future_end = (date.today() + timedelta(days=180)).strftime("%Y-%m-%d")
    expiries = get_monthly_expiries(start, future_end)
    total_contracts = len(expiries) * len(OTM_TARGETS)
    log(f"Processing {len(expiries)} expiries x {len(OTM_TARGETS)} OTM targets = {total_contracts} contracts")

    spy_by_date = {r["date"]: r["c"] for r in dm["spy"]}
    contracts_fetched = 0
    contract_num = 0

    for exp in expiries:
        exp_dt = datetime.strptime(exp, "%Y-%m-%d")

        # Use SPY price ON the expiry date (or nearest prior trading day)
        # For future expiries, use the most recent available SPY price
        spy_price = None
        today_iso = date.today().strftime("%Y-%m-%d")
        lookup_anchor = min(exp, today_iso)  # don't look past today for future expiries
        lookup_dt = datetime.strptime(lookup_anchor, "%Y-%m-%d")
        for days_back in range(0, 30):
            d = (lookup_dt - timedelta(days=days_back)).strftime("%Y-%m-%d")
            if d in spy_by_date:
                spy_price = spy_by_date[d]
                break
        if not spy_price:
            log(f"  No SPY price found near {exp}, skipping")
            continue

        # Full contract life: from max(start, expiry-365) to min(expiry, today)
        opt_start = max(start, (exp_dt - timedelta(days=365)).strftime("%Y-%m-%d"))
        opt_end   = min(exp, date.today().strftime("%Y-%m-%d"))

        log(f"  {exp} — SPY=${spy_price:.2f} → strikes: " +
            ", ".join(f"${find_nearest_strike(spy_price, p)}" for p in OTM_TARGETS))

        for otm_pct in OTM_TARGETS:
            contract_num += 1
            strike = find_nearest_strike(spy_price, otm_pct)
            ticker = build_option_ticker(exp, strike)
            log(f"  [{contract_num}/{total_contracts}] {ticker} ({otm_pct*100:.0f}% OTM @ ${strike}) {opt_start} to {opt_end}...")
            days = fetch_option_history(ticker, opt_start, opt_end)
            if days:
                first_day_spy = spy_by_date.get(days[0]["date"], spy_price)
                dm["options"][ticker] = {
                    "strike":    strike,
                    "expiry":    exp,
                    "otm_pct":   otm_pct,
                    "entry_spy": first_day_spy,
                    "days":      days,
                }
                contracts_fetched += 1
                log(f"    {len(days)} trading days")
            else:
                log(f"    No data returned")

    log(f"Full load complete: SPY {len(dm['spy'])} days | VIX {len(dm['vix'])} days | Options {contracts_fetched}/{total_contracts} contracts")
    dm["metadata"]["last_updated"] = today_str()
    dm["metadata"]["otm_targets"]  = OTM_TARGETS
    return dm


# ── Daily append ───────────────────────────────────────────────────────────
def daily_append(dm):
    log("=" * 60)
    log("DAILY APPEND — adding yesterday's data")
    log("=" * 60)

    # Yesterday (last trading day)
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    # ── SPY ──────────────────────────────────────────────────────────────
    existing_spy_dates = dates_in_dataset(dm["spy"])
    if yesterday not in existing_spy_dates:
        log(f"Appending SPY for {yesterday}...")
        results = fetch_aggs("SPY", yesterday, yesterday)
        time.sleep(CALL_DELAY)
        for r in results:
            dt = datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            if dt not in existing_spy_dates:
                dm["spy"].append({"date": dt, "o": r["o"], "h": r["h"],
                                  "l": r["l"], "c": r["c"], "v": r.get("v", 0)})
                log(f"  ✓ SPY {dt} close ${r['c']:.2f}")
    else:
        log(f"  SPY {yesterday} already in dataset")

    # ── VIX ──────────────────────────────────────────────────────────────
    existing_vix_dates = dates_in_dataset(dm["vix"])
    if yesterday not in existing_vix_dates:
        log(f"Appending VIX for {yesterday}...")
        results = fetch_aggs("I:VIX", yesterday, yesterday)
        time.sleep(CALL_DELAY)
        for r in results:
            dt = datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            if dt not in existing_vix_dates:
                dm["vix"].append({"date": dt, "c": round(r["c"], 2)})
                log(f"  ✓ VIX {dt} close {r['c']:.2f}")
    else:
        log(f"  VIX {yesterday} already in dataset")

    # ── Options — append yesterday to existing contracts ─────────────────
    log(f"Appending option data for {yesterday}...")
    appended = 0
    for ticker, contract in dm["options"].items():
        existing_dates = dates_in_dataset(contract["days"])
        expiry = contract["expiry"]
        # Skip if expired or already have yesterday
        if yesterday > expiry or yesterday in existing_dates:
            continue
        results = fetch_aggs(ticker, yesterday, yesterday)
        time.sleep(CALL_DELAY)
        for r in results:
            dt = datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            if dt not in existing_dates:
                contract["days"].append({
                    "date": dt, "o": round(r.get("o", 0), 4),
                    "h":    round(r.get("h", 0), 4),
                    "l":    round(r.get("l", 0), 4),
                    "c":    round(r.get("c", 0), 4),
                    "v":    r.get("v", 0),
                    "vw":   round(r.get("vw", 0), 4),
                })
                appended += 1
    log(f"  ✓ {appended} option day records appended")

    # ── Dynamic strike sync — ensure today's 22 OTM targets exist ─────────
    # For every future expiry within 180 days, compute the 22 strikes that
    # matter relative to TODAY's SPY. Add any that aren't already in the mart,
    # fetching their full history. This means the mart self-heals after SPY
    # moves — new strikes get added, old ones stay (harmless extra data).
    log("Syncing dynamic strikes for future expiries...")
    spy_by_date = {r["date"]: r["c"] for r in dm["spy"]}

    # Get today's SPY — use yesterday's close (most recent available)
    current_spy = spy_by_date.get(yesterday)
    if not current_spy:
        # Fall back to most recent date in mart
        most_recent = max(spy_by_date.keys())
        current_spy = spy_by_date[most_recent]
        log(f"  Using SPY close from {most_recent} (${current_spy:.2f}) as current price")
    else:
        log(f"  Current SPY: ${current_spy:.2f} (from {yesterday})")

    if not current_spy:
        log("  ✗ No SPY price available — skipping dynamic strike sync")
    else:
        today_iso    = today_str()
        future_date  = (date.today() + timedelta(days=180)).strftime("%Y-%m-%d")
        all_expiries = get_monthly_expiries(today_iso, future_date)

        new_contracts = 0
        skipped       = 0

        for exp in all_expiries:
            exp_dt       = datetime.strptime(exp, "%Y-%m-%d")
            # History window: from max(2yr ago, expiry-365) to today
            hist_start   = max(
                (date.today() - timedelta(days=730)).strftime("%Y-%m-%d"),
                (exp_dt - timedelta(days=365)).strftime("%Y-%m-%d")
            )
            hist_end     = today_iso  # cap at today; contract may not have traded yet

            for otm_pct in OTM_TARGETS:
                strike = find_nearest_strike(current_spy, otm_pct)
                ticker = build_option_ticker(exp, strike)

                if ticker in dm["options"]:
                    skipped += 1
                    continue  # already have this contract

                # New strike for this expiry — fetch full history and add
                log(f"  + {ticker}  ({otm_pct*100:.0f}% OTM, exp={exp})")
                days = fetch_option_history(ticker, hist_start, hist_end)
                time.sleep(CALL_DELAY)
                if days:
                    dm["options"][ticker] = {
                        "strike":    strike,
                        "expiry":    exp,
                        "otm_pct":   otm_pct,
                        "entry_spy": current_spy,
                        "days":      days,
                    }
                    log(f"    → Added {len(days)} days of history")
                    new_contracts += 1
                else:
                    log(f"    → No data returned (contract may not yet trade)")

        log(f"  ✓ Dynamic sync complete — {new_contracts} new contracts added, {skipped} already present")

    log(f"  ✓ Daily append complete")
    dm["metadata"]["last_updated"] = today_str()
    return dm


# ── Gap fill ──────────────────────────────────────────────────────────────
def gap_fill(dm):
    log("=" * 60)
    log("GAP FILL — fetching missing contracts from GAP_TICKERS")
    log("=" * 60)

    gap_tickers_raw = os.environ.get("GAP_TICKERS", "").strip()
    if not gap_tickers_raw:
        log("GAP_TICKERS env var is empty — nothing to fill")
        return dm

    gap_tickers = [t.strip() for t in gap_tickers_raw.split(",") if t.strip()]
    log(f"  {len(gap_tickers)} gap contract(s) to fill: {', '.join(gap_tickers)}")

    spy_by_date = {r["date"]: r["c"] for r in dm["spy"]}
    filled = 0

    for raw_ticker in gap_tickers:
        log(f"  Processing: {raw_ticker}")

        # Parse expiry and OTM from ticker format: O:SPY241220C00580000 or O:SPYYYMMDDCC [X%OTM]
        # Try to parse as real Polygon ticker first
        import re
        m = re.match(r"O:SPY(\d{6})C(\d{8})", raw_ticker)
        if m:
            date_str = m.group(1)  # YYMMDD
            strike_raw = int(m.group(2)) / 1000
            expiry = f"20{date_str[:2]}-{date_str[2:4]}-{date_str[4:6]}"
            ticker = raw_ticker

            # Fetch full history for this contract
            start = (datetime.strptime(expiry, "%Y-%m-%d") - timedelta(days=365)).strftime("%Y-%m-%d")
            end   = expiry
            log(f"    Fetching {ticker} {start} → {end}...")
            days = fetch_option_history(ticker, start, end)
            if days:
                first_day_spy = spy_by_date.get(days[0]["date"], 0)
                # Determine approximate OTM pct
                otm_pct = round((strike_raw / first_day_spy - 1), 2) if first_day_spy else 0.05
                dm["options"][ticker] = {
                    "strike":    strike_raw,
                    "expiry":    expiry,
                    "otm_pct":   otm_pct,
                    "entry_spy": first_day_spy,
                    "days":      days,
                }
                filled += 1
                log(f"    ✓ {len(days)} trading days added")
            else:
                log(f"    ⚠ No data returned for {ticker}")
        else:
            log(f"    ⚠ Could not parse ticker format: {raw_ticker}")
            log(f"    Expected format: O:SPY241220C00580000")

    log(f"\nGap fill complete: {filled}/{len(gap_tickers)} contracts filled")
    log("NOTE: Please clear GAP_TICKERS env var in Railway to avoid re-fetching on next run")
    dm["metadata"]["last_updated"] = today_str()
    return dm


# ── Main ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log("SPY Backtest Data Collector starting...")
    log(f"Mode: {RUN_MODE.upper()} | Repo: {GITHUB_REPO}")

    if RUN_MODE == "full":
        # Full load from scratch
        dm = full_load()
    elif RUN_MODE == "gap":
        # Gap fill — fetch specific missing contracts from GAP_TICKERS env var
        dm, sha = github_get_file(DATA_PATH)
        if dm is None:
            log("No existing data mart found — cannot fill gaps without base data")
            exit(1)
        dm = gap_fill(dm)
    else:
        # Daily append — load existing data first
        log("Loading existing data mart from GitHub...")
        dm, sha = github_get_file(DATA_PATH)
        if dm is None:
            log("No existing data found — switching to full load")
            dm  = full_load()
            sha = None
        else:
            log(f"  ✓ Loaded — last updated: {dm['metadata'].get('last_updated', 'unknown')}")
            log(f"  SPY: {len(dm['spy'])} days | VIX: {len(dm['vix'])} days | "
                f"Options: {len(dm['options'])} contracts")
            dm = daily_append(dm)

    # Commit to GitHub
    log("Committing data mart to GitHub...")
    existing, sha = github_get_file(DATA_PATH)
    msg = f"Data update {today_str()} — {RUN_MODE} load"
    success = github_commit_file(DATA_PATH, dm, msg, sha=sha)
    log("Done.")

    # Pushover notification
    if success:
        spy_count = len(dm["spy"])
        opt_count = len(dm["options"])
        last_spy  = dm["spy"][-1] if dm["spy"] else {}
        spy_close = last_spy.get("c", "—")
        spy_date  = last_spy.get("date", "—")
        otm_str   = ", ".join(str(int(t*100))+"%" for t in OTM_TARGETS)
        if RUN_MODE == "full":
            body = ("Full load complete\n"
                    f"SPY {spy_count} days | VIX {len(dm['vix'])} days | {opt_count} contracts\n"
                    f"OTM targets: {otm_str}")
        elif RUN_MODE == "gap":
            gap_count = len(os.environ.get("GAP_TICKERS","").split(","))
            body = (f"Gap fill complete\n"
                    f"{opt_count} total contracts in mart\n"
                    f"Clear GAP_TICKERS in Railway variables")
        else:
            body = ("Daily append complete\n"
                    f"SPY ${spy_close} ({spy_date})\n"
                    f"{opt_count} contracts tracked through {spy_date}")
        send_push("📊 SPY Data Mart", body)
