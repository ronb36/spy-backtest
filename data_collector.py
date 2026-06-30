"""
SPY Backtest Data Collector
============================
Collects and maintains a historical data mart for the SPY covered call backtest.
Runs as a Railway cron service — smart daily run appends new data, inspects the
DM for missing contracts, and backfills gaps automatically. Idempotent: running
twice produces the same result. Self-healing: if the data mart is missing or
corrupted, the daily run rebuilds it from scratch automatically.

Data collected:
  - SPY daily OHLCV (2 years rolling)
  - VIX daily close (2 years rolling)
  - Option contracts: 39 strikes (1%–20% OTM, 0.5% steps, $5 grid) x all monthly
    expiries, anchored to every SPY trading day within each contract's window
    Each contract: full life history from first available trade date to expiry

Output: spy_data.json committed to ronb36/spy-backtest via GitHub API (Blobs API
        used for files >1MB to avoid GitHub Contents API size limit)

Environment variables (Railway):
  POLYGON_KEY     — Polygon/Massive API key
  GITHUB_TOKEN    — Personal access token with repo scope
  GITHUB_REPO     — ronb36/spy-backtest
  RUN_MODE        — "daily" (default, omit to use default) or "gap" (fill
                    specific contracts listed in GAP_TICKERS)
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
OTM_TARGETS = [round(i * 0.005, 4) for i in range(2, 41)]
# 0.5% steps from 1.0% to 20.0% → 39 targets, matches in-browser backfill exactly
# [0.01, 0.015, 0.02, ..., 0.195, 0.20]

# ── Rate limiting ──────────────────────────────────────────────────────────
# No artificial delay between calls — current Polygon plan has no rate limit.
# If switching to a rate-limited plan, reintroduce time.sleep() between
# fetch_aggs() calls in Phase 3/4 loops.
CALL_DELAY = 0  # kept for compatibility; unused


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
    """Get file content and SHA from GitHub — handles large files via Blobs API."""
    url     = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}",
               "Accept": "application/vnd.github.v3+json"}
    r = requests.get(url, headers=headers, timeout=15)
    if r.status_code != 200:
        return None, None

    data = r.json()
    sha  = data.get("sha")

    # For large files (>1MB), GitHub returns empty content — use Blobs API instead
    raw_content = data.get("content", "").replace("\n", "").strip()
    if not raw_content:
        log(f"  Large file detected — fetching via Blobs API (sha={sha[:8]}...)")
        blob_url = f"https://api.github.com/repos/{GITHUB_REPO}/git/blobs/{sha}"
        br = requests.get(blob_url, headers={"Authorization": f"token {GITHUB_TOKEN}",
                                              "Accept": "application/vnd.github.v3+json"}, timeout=30)
        if br.status_code != 200:
            log(f"  ✗ Blob fetch failed: {br.status_code}")
            return None, sha
        blob  = br.json()
        b64   = blob["content"].replace("\n", "")
        bytes = bytearray()
        # Decode in chunks to avoid memory issues
        chunk = 65536
        for i in range(0, len(b64), chunk):
            bytes.extend(base64.b64decode(b64[i:i+chunk] + "=="))
        raw_content = bytes.decode("utf-8").strip()

    if not raw_content:
        return None, sha
    try:
        return json.loads(raw_content), sha
    except json.JSONDecodeError:
        return None, sha


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


# ── Daily append + smart inspection ───────────────────────────────────────
def daily_append(dm):
    """
    Smart daily run — self-healing, four phases:
    1. Append SPY/VIX OHLCV (full 2-year history if DM is empty, else just
       today's close)
    2. Append today's day to all existing option contracts
    3. Inspect DM vs full expected contract set (every expiry x every SPY
       trading day in that expiry's window) — backfill any gaps with each
       contract's complete history from its true earliest possible date
    4. Add new future-expiry strikes as current SPY moves

    Cron is scheduled to run at 10pm ET — well after the 4:00/4:15pm ET
    official close, so today's trading day is already final and safe to
    fetch directly (no need to wait for "yesterday"). This means tomorrow's
    market open already has today's close available, rather than lagging
    a full day behind.

    Running twice in a row is safe — second run finds nothing new to do.
    Running against an empty datamart builds the complete DM from scratch.
    """
    log("=" * 60)
    log("DAILY SMART RUN — append + inspect + backfill")
    log("=" * 60)

    today_iso  = today_str()
    target_date = today_iso  # cron runs 10pm ET, after the official 4pm/4:15pm close —
                              # today's trading day is already final, fetch it directly
    two_yr_ago = (date.today() - timedelta(days=730)).strftime("%Y-%m-%d")

    # ── Build SPY lookup ──────────────────────────────────────────────────
    spy_by_date = {r["date"]: r["c"] for r in dm["spy"]}

    # ── Phase 1: SPY/VIX append (full 2yr history if DM is empty) ─────────
    existing_spy_dates = dates_in_dataset(dm["spy"])
    if not dm["spy"]:
        log(f"Phase 1: Empty DM — fetching full 2-year SPY/VIX history {two_yr_ago} → {today_iso}...")
        results = fetch_aggs("SPY", two_yr_ago, today_iso)
        for r in results:
            dt = datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            dm["spy"].append({"date": dt, "o": r["o"], "h": r["h"],
                              "l": r["l"], "c": r["c"], "v": r.get("v", 0)})
        existing_spy_dates = dates_in_dataset(dm["spy"])
        log(f"  ✓ {len(dm['spy'])} SPY trading days loaded")

        vix_results = fetch_aggs("I:VIX", two_yr_ago, today_iso)
        for r in vix_results:
            dt = datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            dm["vix"].append({"date": dt, "c": round(r["c"], 2)})
        log(f"  ✓ {len(dm['vix'])} VIX trading days loaded")
    elif target_date not in existing_spy_dates:
        log(f"Phase 1: Appending SPY/VIX for {target_date}...")
        results = fetch_aggs("SPY", target_date, target_date)
        for r in results:
            dt = datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            if dt not in existing_spy_dates:
                dm["spy"].append({"date": dt, "o": r["o"], "h": r["h"],
                                  "l": r["l"], "c": r["c"], "v": r.get("v", 0)})
                spy_by_date[dt] = r["c"]
                log(f"  ✓ SPY {dt} close ${r['c']:.2f}")
    else:
        log(f"Phase 1: SPY {target_date} already present")

    existing_vix_dates = dates_in_dataset(dm["vix"])
    if target_date not in existing_vix_dates and dm["vix"]:
        results = fetch_aggs("I:VIX", target_date, target_date)
        for r in results:
            dt = datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            if dt not in existing_vix_dates:
                dm["vix"].append({"date": dt, "c": round(r["c"], 2)})
                log(f"  ✓ VIX {dt} close {r['c']:.2f}")

    # Refresh spy_by_date after appending
    spy_by_date = {r["date"]: r["c"] for r in dm["spy"]}

    # Current SPY — used in both Phase 3 and Phase 4
    current_spy = spy_by_date.get(target_date) or spy_by_date.get(max(spy_by_date.keys()))
    log(f"  Current SPY: ${current_spy:.2f}")

    # ── Phase 2: Append today to existing option contracts ────────────────
    log(f"Phase 2: Appending {target_date} to existing contracts...")
    appended = 0
    for ticker, contract in dm["options"].items():
        existing_dates = dates_in_dataset(contract["days"])
        expiry = contract["expiry"]
        if target_date > expiry or target_date in existing_dates:
            continue
        results = fetch_aggs(ticker, target_date, target_date)
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

    # ── Phase 3: Smart inspection — daily-anchored strike coverage ──────────
    # For every monthly expiry in the 2yr window, compute expected strikes
    # using EVERY SPY trading day as an anchor. This ensures the backtester
    # always finds the exact strikes it needs at each roll trigger date.
    # Idempotent: second run finds nothing new.
    log("Phase 3: Inspecting DM for missing contracts (daily-anchored)...")

    future_end   = (date.today() + timedelta(days=180)).strftime("%Y-%m-%d")
    all_expiries = get_monthly_expiries(two_yr_ago, future_end)

    # Build full set of expected tickers across all expiries × all SPY days
    # Use sorted SPY dates within each expiry's window
    expected_by_expiry = {}
    for exp in all_expiries:
        exp_dt     = datetime.strptime(exp, "%Y-%m-%d")
        hist_start = max(two_yr_ago,
                         (exp_dt - timedelta(days=365)).strftime("%Y-%m-%d"))
        hist_end   = min(exp, today_iso)
        expected   = set()
        for spy_date_str, spy_px in spy_by_date.items():
            if spy_date_str < hist_start or spy_date_str > hist_end:
                continue
            for otm_pct in OTM_TARGETS:
                strike = find_nearest_strike(spy_px, otm_pct)
                ticker = build_option_ticker(exp, strike)
                expected.add(ticker)
        expected_by_expiry[exp] = (expected, hist_start, hist_end)

    total_expected = sum(len(v[0]) for v in expected_by_expiry.values())
    log(f"  Expected unique contracts across all expiries: {total_expected}")

    missing      = 0
    already_have = 0
    no_data      = 0

    existing_tickers = set(dm["options"].keys())

    for exp, (expected_tickers, hist_start, hist_end) in expected_by_expiry.items():
        missing_tickers = expected_tickers - existing_tickers
        already_have   += len(expected_tickers) - len(missing_tickers)

        if not missing_tickers:
            continue

        exp_dt = datetime.strptime(exp, "%Y-%m-%d")
        for ticker in sorted(missing_tickers):
            strike    = int(ticker[-8:]) / 1000
            days_data = fetch_option_history(ticker, hist_start, hist_end)
            if days_data:
                # Use SPY price nearest to first day of data as entry_spy
                first_date = days_data[0]["date"]
                entry_spy  = spy_by_date.get(first_date, current_spy)
                dm["options"][ticker] = {
                    "strike":    strike,
                    "expiry":    exp,
                    "otm_pct":   round((strike / entry_spy - 1), 3),
                    "entry_spy": entry_spy,
                    "days":      days_data,
                }
                existing_tickers.add(ticker)
                missing += 1
                log(f"  + {ticker} ({len(days_data)}d)")
            else:
                no_data += 1

    log(f"  ✓ Inspection complete — {already_have} present, {missing} filled, {no_data} not in Polygon")

    # ── Phase 4: Add new future strikes if SPY has moved ─────────────────
    # Ensures current-SPY strikes exist for all upcoming expiries
    log(f"Phase 4: Syncing future expiry strikes (SPY=${current_spy:.2f})...")
    new_future = 0

    future_expiries = get_monthly_expiries(today_iso, future_end)
    for exp in future_expiries:
        exp_dt     = datetime.strptime(exp, "%Y-%m-%d")
        hist_start = max(two_yr_ago,
                         (exp_dt - timedelta(days=365)).strftime("%Y-%m-%d"))
        for otm_pct in OTM_TARGETS:
            strike = find_nearest_strike(current_spy, otm_pct)
            ticker = build_option_ticker(exp, strike)
            if ticker in dm["options"]:
                continue
            days_data = fetch_option_history(ticker, hist_start, today_iso)
            if days_data:
                dm["options"][ticker] = {
                    "strike":    strike,
                    "expiry":    exp,
                    "otm_pct":   otm_pct,
                    "entry_spy": current_spy,
                    "days":      days_data,
                }
                new_future += 1
                log(f"  + {ticker} (future, {len(days_data)}d)")

    log(f"  ✓ {new_future} new future-expiry contracts added")

    log(f"Daily smart run complete — {len(dm['options'])} total contracts")
    dm["metadata"]["last_updated"] = today_iso
    dm["metadata"]["otm_targets"]  = OTM_TARGETS
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

# ── Main ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log("SPY Backtest Data Collector starting...")
    log(f"Mode: {RUN_MODE.upper()} | Repo: {GITHUB_REPO}")

    if RUN_MODE == "gap":
        # Gap fill — fetch specific missing contracts from GAP_TICKERS env var
        dm, sha = github_get_file(DATA_PATH)
        if dm is None:
            log("No existing data mart found — cannot fill gaps without base data")
            exit(1)
        dm = gap_fill(dm)
    else:
        # Daily append — load existing data first.
        # If missing/corrupted, self-heal: daily_append builds the complete
        # DM from scratch (full 2yr SPY/VIX, full daily-anchored contract
        # set, future strikes) when handed an empty datamart.
        log("Loading existing data mart from GitHub...")
        dm, sha = github_get_file(DATA_PATH)
        if dm is None:
            log("No existing data found — building from scratch via daily_append")
            dm = empty_datamart()
        else:
            log(f"  ✓ Loaded — last updated: {dm['metadata'].get('last_updated', 'unknown')}")
            log(f"  SPY: {len(dm['spy'])} days | VIX: {len(dm['vix'])} days | "
                f"Options: {len(dm['options'])} contracts")
        dm = daily_append(dm)

    # Commit to GitHub
    json_size_mb = len(json.dumps(dm)) / 1024 / 1024
    log(f"Committing data mart to GitHub... ({json_size_mb:.1f} MB, {len(dm['options'])} contracts)")
    existing, sha = github_get_file(DATA_PATH)
    msg = f"Data update {today_str()} — {RUN_MODE} load"
    success = github_commit_file(DATA_PATH, dm, msg, sha=sha)
    log("Done.")

    # Pushover notification
    if success:
        opt_count = len(dm["options"])
        last_spy  = dm["spy"][-1] if dm["spy"] else {}
        spy_close = last_spy.get("c", "—")
        spy_date  = last_spy.get("date", "—")
        if RUN_MODE == "gap":
            body = (f"Gap fill complete\n"
                    f"{opt_count} total contracts in mart\n"
                    f"Clear GAP_TICKERS in Railway variables")
        else:
            body = ("Daily append complete\n"
                    f"SPY ${spy_close} ({spy_date})\n"
                    f"{opt_count} contracts · {json_size_mb:.1f} MB through {spy_date}")
        send_push("📊 SPY Data Mart", body)
