"""
SPY Backtest Data Collector
============================
Collects and maintains a historical data mart for the SPY covered call backtest.
Runs as a Railway service — initial full load on first run, daily append thereafter.

Data collected:
  - SPY daily OHLCV (2 years)
  - VIX daily close (2 years)
  - Option contracts: 4 strikes (3%, 5%, 7%, 10% OTM) × all monthly/quarterly expiries
    Each contract: daily OHLCV + IV + delta for its active life

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
POLYGON_KEY  = os.environ["POLYGON_KEY"]
GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]
GITHUB_REPO  = os.environ.get("GITHUB_REPO", "ronb36/spy-backtest")
RUN_MODE     = os.environ.get("RUN_MODE", "daily")
DATA_PATH    = "data/spy_data.json"

# ── OTM target levels to store per expiry ─────────────────────────────────
OTM_TARGETS = [0.03, 0.05, 0.07, 0.10]   # 3%, 5%, 7%, 10%

# ── Rate limiting ──────────────────────────────────────────────────────────
CALL_DELAY = 0.25   # 4 calls/sec — safely under Polygon's 5/sec limit


def log(msg):
    print(f"{datetime.now(ET).strftime('%H:%M:%S ET')} — {msg}", flush=True)


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
    # Get all monthly expiries in the window
    expiries = get_monthly_expiries(start, end)
    log(f"Processing {len(expiries)} monthly expiries × {len(OTM_TARGETS)} OTM targets "
        f"= {len(expiries) * len(OTM_TARGETS)} contracts")

    # Build SPY price lookup for fast access
    spy_by_date = {r["date"]: r["c"] for r in dm["spy"]}

    contracts_fetched = 0
    for exp in expiries:
        # Find SPY price ~150 days before expiry (entry date)
        entry_date = (datetime.strptime(exp, "%Y-%m-%d") - timedelta(days=150)).strftime("%Y-%m-%d")
        # Find nearest available SPY price
        spy_price = None
        for days_back in range(0, 10):
            d = (datetime.strptime(entry_date, "%Y-%m-%d") - timedelta(days=days_back)).strftime("%Y-%m-%d")
            if d in spy_by_date:
                spy_price = spy_by_date[d]
                break

        if not spy_price:
            log(f"  ⚠ No SPY price found near {entry_date}, skipping {exp}")
            continue

        for otm_pct in OTM_TARGETS:
            strike = find_nearest_strike(spy_price, otm_pct)
            ticker = build_option_ticker(exp, strike)

            # Fetch option daily history from entry_date to expiry
            opt_start = entry_date
            opt_end   = exp
            log(f"  Fetching {ticker} ({otm_pct*100:.0f}% OTM @ ${strike}) "
                f"{opt_start} → {opt_end}...")

            days = fetch_option_history(ticker, opt_start, opt_end)

            if days:
                dm["options"][ticker] = {
                    "strike":   strike,
                    "expiry":   exp,
                    "otm_pct":  otm_pct,
                    "entry_spy": spy_price,
                    "days":     days,
                }
                contracts_fetched += 1
                log(f"    ✓ {len(days)} trading days")
            else:
                log(f"    ⚠ No data returned — contract may not have traded")

    log(f"\n✓ Full load complete:")
    log(f"  SPY:     {len(dm['spy'])} days")
    log(f"  VIX:     {len(dm['vix'])} days")
    log(f"  Options: {contracts_fetched} contracts")

    dm["metadata"]["last_updated"] = today_str()
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

    # ── Options — append to existing contracts ────────────────────────────
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

    # ── Check for new expiries to add ─────────────────────────────────────
    # Look ahead 150 days — if a new expiry is entering our window, add it
    future_date = (date.today() + timedelta(days=150)).strftime("%Y-%m-%d")
    all_expiries = get_monthly_expiries(
        (date.today() - timedelta(days=730)).strftime("%Y-%m-%d"),
        future_date
    )
    spy_by_date  = {r["date"]: r["c"] for r in dm["spy"]}

    for exp in all_expiries:
        entry_date = (datetime.strptime(exp, "%Y-%m-%d") - timedelta(days=150)).strftime("%Y-%m-%d")
        # If entry date is today, this is a new contract to start tracking
        if entry_date != today_str():
            continue
        spy_price = spy_by_date.get(yesterday)
        if not spy_price:
            continue
        for otm_pct in OTM_TARGETS:
            strike = find_nearest_strike(spy_price, otm_pct)
            ticker = build_option_ticker(exp, strike)
            if ticker not in dm["options"]:
                log(f"  New contract entering window: {ticker}")
                days = fetch_option_history(ticker, today_str(), exp)
                if days:
                    dm["options"][ticker] = {
                        "strike":    strike,
                        "expiry":    exp,
                        "otm_pct":   otm_pct,
                        "entry_spy": spy_price,
                        "days":      days,
                    }
                    log(f"    ✓ Added {ticker}")

    log(f"  ✓ Daily append complete — {appended} option day records added")
    dm["metadata"]["last_updated"] = today_str()
    return dm


# ── Main ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log("SPY Backtest Data Collector starting...")
    log(f"Mode: {RUN_MODE.upper()} | Repo: {GITHUB_REPO}")

    if RUN_MODE == "full":
        # Full load from scratch
        dm = full_load()
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
    github_commit_file(DATA_PATH, dm, msg, sha=sha)
    log("Done.")
