"""
SPY Backtest Data Collector
============================
Collects and maintains a historical data mart for the SPY covered call backtest.
Runs as a Railway cron service — smart daily run appends new data, inspects the
DM for missing contracts, and backfills gaps automatically. Idempotent: running
twice produces the same result.
"forced rebuild "
Data collected:
  - SPY daily OHLCV (2 years rolling)
  - VIX daily close (2 years rolling)
  - Option contracts: 22 strikes (1%–35% OTM in $5 grid) × all monthly expiries
    Each contract: full life history from first available trade date to expiry
    Strike selection anchored to SPY price at each expiry date (historical)
    Plus current-SPY-anchored strikes for future expiries

Output: spy_data.json committed to ronb36/spy-backtest via GitHub API (Blobs API
        used for files >1MB to avoid GitHub Contents API size limit)

Environment variables (Railway):
  POLYGON_KEY     — Polygon/Massive API key
  GITHUB_TOKEN    — Personal access token with repo scope
  GITHUB_REPO     — ronb36/spy-backtest
  RUN_MODE        — "daily" (default) or "full" (rebuild from scratch)
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

COLLECTOR_VERSION = "1.5.0"  # Git Data API commit — no LFS dependency

# ── Load DM universe config ────────────────────────────────────────────────
UNIVERSE_PATH = "data_universe.json"

def load_universe_config():
    try:
        with open(UNIVERSE_PATH) as f:
            cfg = json.load(f)
        return cfg
    except Exception as e:
        log(f"⚠ Could not load {UNIVERSE_PATH}: {e} — using defaults")
        return {}

_cfg = load_universe_config()

OTM_TARGETS   = _cfg.get("otm_targets", [
    0.01, 0.02, 0.03, 0.04, 0.05,
    0.06, 0.07, 0.08, 0.09, 0.10,
    0.11, 0.12, 0.13, 0.14, 0.15,
    0.18, 0.20, 0.22, 0.25, 0.28, 0.30, 0.35,
])
CALL_DELAY    = _cfg.get("call_delay", 0.25)
COMMIT_EVERY  = _cfg.get("commit_every", 10)
USE_MONTHLY   = _cfg.get("monthly", True)
USE_WEEKLY    = _cfg.get("weekly", False)
START_DATE    = _cfg.get("start_date", "2024-07-01")
MAX_LOOKAHEAD = _cfg.get("max_expiry_lookahead_days", 180)
EXP_LOOKBACK  = _cfg.get("expiry_lookback_days", 365)
DO_RESET      = _cfg.get("reset", False)
PRUNE_OTM_ABOVE = _cfg.get("prune_otm_above", None)
PRUNE_BEFORE  = _cfg.get("prune_expiries_before", None)
RUN_MODE       = os.environ.get("RUN_MODE", "daily")
DATA_PATH      = "data/spy_data.json"
PUSHOVER_USER  = os.environ.get("PUSHOVER_USER_TOKEN")
PUSHOVER_TOKEN = os.environ.get("PUSHOVER_API_TOKEN")




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
    """Return 3rd-Friday expiry dates between start and end (inclusive)."""
    import calendar as cal_mod
    expiries = []
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end   = datetime.strptime(end_date,   "%Y-%m-%d").date()
    year, month = start.year, start.month
    while date(year, month, 1) <= end:
        cal = cal_mod.monthcalendar(year, month)
        fridays = [week[cal_mod.FRIDAY] for week in cal if week[cal_mod.FRIDAY] != 0]
        third_fri = fridays[2]
        exp = date(year, month, third_fri)
        if start <= exp <= end:
            expiries.append(exp.strftime("%Y-%m-%d"))
        month += 1
        if month > 12:
            month = 1
            year += 1
    return expiries


def get_weekly_expiries(start_date, end_date):
    """Return all Fridays excluding 3rd Fridays (already in monthly)."""
    monthly_set = set(get_monthly_expiries(start_date, end_date))
    expiries = []
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end   = datetime.strptime(end_date,   "%Y-%m-%d").date()
    current = start
    while current <= end:
        if current.weekday() == 4:  # Friday
            iso = current.strftime("%Y-%m-%d")
            if iso not in monthly_set:
                expiries.append(iso)
        current += timedelta(days=1)
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
    """
    Commit JSON file to GitHub using the Git Data API (no LFS).
    Works for files up to ~100MB — well above current DM size.
    Flow:
      1. Serialize + base64-encode content
      2. Create blob
      3. Get HEAD ref → base tree SHA
      4. Create new tree with updated blob
      5. Create commit
      6. Update HEAD ref
    """
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
        "Content-Type": "application/json",
    }
    base = f"https://api.github.com/repos/{GITHUB_REPO}"

    content_bytes = json.dumps(content_dict, separators=(",", ":")).encode("utf-8")
    size_mb = len(content_bytes) / 1024 / 1024
    log(f"  Preparing commit ({size_mb:.1f} MB)...")

    try:
        # ── Step 1: Create blob ───────────────────────────────────────────
        b64 = base64.b64encode(content_bytes).decode("utf-8")
        r = requests.post(f"{base}/git/blobs", headers=headers,
                          json={"content": b64, "encoding": "base64"}, timeout=60)
        if r.status_code not in (200, 201):
            raise Exception(f"Blob creation failed: {r.status_code} {r.text[:200]}")
        blob_sha = r.json()["sha"]
        log(f"  ✓ Blob created ({size_mb:.1f} MB, sha={blob_sha[:7]}...)")

        # ── Step 2: Get HEAD ref ──────────────────────────────────────────
        r = requests.get(f"{base}/git/ref/heads/main", headers=headers, timeout=15)
        if r.status_code != 200:
            raise Exception(f"Ref fetch failed: {r.status_code}")
        head_commit_sha = r.json()["object"]["sha"]

        # ── Step 3: Get base tree SHA ─────────────────────────────────────
        r = requests.get(f"{base}/git/commits/{head_commit_sha}", headers=headers, timeout=15)
        if r.status_code != 200:
            raise Exception(f"Commit fetch failed: {r.status_code}")
        base_tree_sha = r.json()["tree"]["sha"]

        # ── Step 4: Create new tree ───────────────────────────────────────
        r = requests.post(f"{base}/git/trees", headers=headers,
                          json={"base_tree": base_tree_sha,
                                "tree": [{"path": path, "mode": "100644",
                                          "type": "blob", "sha": blob_sha}]},
                          timeout=30)
        if r.status_code not in (200, 201):
            raise Exception(f"Tree creation failed: {r.status_code}")
        new_tree_sha = r.json()["sha"]

        # ── Step 5: Create commit ─────────────────────────────────────────
        r = requests.post(f"{base}/git/commits", headers=headers,
                          json={"message": message, "tree": new_tree_sha,
                                "parents": [head_commit_sha]}, timeout=30)
        if r.status_code not in (200, 201):
            raise Exception(f"Commit creation failed: {r.status_code}")
        new_commit_sha = r.json()["sha"]

        # ── Step 6: Update HEAD ref ───────────────────────────────────────
        r = requests.patch(f"{base}/git/refs/heads/main", headers=headers,
                           json={"sha": new_commit_sha}, timeout=15)
        if r.status_code not in (200, 201):
            raise Exception(f"Ref update failed: {r.status_code} {r.text[:200]}")

        log(f"  ✓ Committed {path} ({new_commit_sha[:7]})")
        return True

    except Exception as e:
        log(f"  ✗ GitHub commit failed: {e}")
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


# ── Daily append + smart inspection ───────────────────────────────────────
def daily_append(dm):
    """
    Smart daily run — three phases:
    1. Append yesterday's OHLCV to all existing contracts
    2. Inspect DM vs expected contract set — backfill any gaps
       (uses historical SPY price anchored to each expiry date)
    3. Add new future expiry strikes based on current SPY
    Running twice in a row is safe — second run finds nothing to do.
    """
    log("=" * 60)
    log("DAILY SMART RUN — append + inspect + backfill")
    log("=" * 60)

    today_iso  = today_str()
    yesterday  = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    two_yr_ago = START_DATE  # From data_universe.json

    # ── Build SPY lookup ──────────────────────────────────────────────────
    spy_by_date = {r["date"]: r["c"] for r in dm["spy"]}

    # ── Phase 0: Reset / Prune / Bootstrap ───────────────────────────────────
    if DO_RESET:
        log("Phase 0: RESET flag set — wiping DM to empty...")
        dm["spy"] = []; dm["vix"] = []; dm["options"] = {}
        dm["metadata"]["options_count"] = 0
        log("  ✓ DM wiped")

    if PRUNE_OTM_ABOVE is not None:
        before = len(dm["options"])
        dm["options"] = {k: v for k, v in dm["options"].items()
                        if abs(v.get("otm_pct", 0)) <= PRUNE_OTM_ABOVE}
        log(f"Phase 0: Pruned {before - len(dm['options'])} contracts > {PRUNE_OTM_ABOVE*100:.0f}% OTM")

    if len(dm["spy"]) == 0:
        log("Phase 0: Empty DM — bootstrapping full SPY/VIX history...")
        spy_results = fetch_aggs("SPY", START_DATE, today_iso)
        time.sleep(CALL_DELAY)
        for r in spy_results:
            dt = datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            dm["spy"].append({"date": dt, "o": r["o"], "h": r["h"],
                              "l": r["l"], "c": r["c"], "v": r.get("v", 0)})
        log(f"  ✓ {len(dm['spy'])} SPY days loaded")
        vix_results = fetch_aggs("I:VIX", START_DATE, today_iso)
        time.sleep(CALL_DELAY)
        for r in vix_results:
            dt = datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            dm["vix"].append({"date": dt, "c": round(r["c"], 2)})
        log(f"  ✓ {len(dm['vix'])} VIX days loaded")

    # ── Phase 1: SPY daily append ─────────────────────────────────────────
    existing_spy_dates = dates_in_dataset(dm["spy"])
    if yesterday not in existing_spy_dates:
        log(f"Phase 1: Appending SPY/VIX for {yesterday}...")
        results = fetch_aggs("SPY", yesterday, yesterday)
        time.sleep(CALL_DELAY)
        for r in results:
            dt = datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            if dt not in existing_spy_dates:
                dm["spy"].append({"date": dt, "o": r["o"], "h": r["h"],
                                  "l": r["l"], "c": r["c"], "v": r.get("v", 0)})
                spy_by_date[dt] = r["c"]
                log(f"  ✓ SPY {dt} close ${r['c']:.2f}")
    else:
        log(f"Phase 1: SPY {yesterday} already present")

    existing_vix_dates = dates_in_dataset(dm["vix"])
    if yesterday not in existing_vix_dates:
        results = fetch_aggs("I:VIX", yesterday, yesterday)
        time.sleep(CALL_DELAY)
        for r in results:
            dt = datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            if dt not in existing_vix_dates:
                dm["vix"].append({"date": dt, "c": round(r["c"], 2)})
                log(f"  ✓ VIX {dt} close {r['c']:.2f}")

    # Refresh spy_by_date after appending
    spy_by_date = {r["date"]: r["c"] for r in dm["spy"]}

    # Current SPY — used in both Phase 3 and Phase 4
    current_spy = spy_by_date.get(yesterday) or spy_by_date.get(max(spy_by_date.keys()))
    log(f"  Current SPY: ${current_spy:.2f}")

    # ── Phase 2: Append yesterday to existing option contracts ────────────
    log(f"Phase 2: Appending yesterday to existing contracts...")
    appended = 0
    for ticker, contract in dm["options"].items():
        existing_dates = dates_in_dataset(contract["days"])
        expiry = contract["expiry"]
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

    # ── Phase 3: Smart inspection — daily-anchored strike coverage ──────────
    log("Phase 3: Inspecting DM for missing contracts (daily-anchored)...")

    # DM sanity check
    opt_count = len(dm.get("options", {}))
    spy_count = len(dm.get("spy", []))
    log(f"  DM sanity: {opt_count} contracts, {spy_count} SPY days")

    future_end       = (date.today() + timedelta(days=MAX_LOOKAHEAD)).strftime("%Y-%m-%d")
    monthly_expiries = get_monthly_expiries(two_yr_ago, future_end) if USE_MONTHLY else []
    weekly_expiries  = get_weekly_expiries(two_yr_ago, future_end)  if USE_WEEKLY  else []
    all_expiries     = sorted(set(monthly_expiries + weekly_expiries))
    monthly_set      = set(monthly_expiries)
    log(f"  Expiry universe: {len(all_expiries)} expiries ({len(monthly_expiries)} monthly + {len(weekly_expiries)} weekly)")

    missing      = 0
    already_have = 0
    no_data      = 0
    no_data_tickers = []
    weekly_filled   = 0
    weekly_no_data  = 0
    total_expected  = 0
    existing_tickers = set(dm["options"].keys())

    for exp_idx, exp in enumerate(all_expiries):
        is_weekly  = exp not in monthly_set
        exp_dt     = datetime.strptime(exp, "%Y-%m-%d")
        hist_start = max(two_yr_ago,
                         (exp_dt - timedelta(days=EXP_LOOKBACK)).strftime("%Y-%m-%d"))
        hist_end   = min(exp, today_iso)

        expected = set()
        for spy_date_str, spy_px in spy_by_date.items():
            if spy_date_str < hist_start or spy_date_str > hist_end:
                continue
            for otm_pct in OTM_TARGETS:
                strike = find_nearest_strike(spy_px, otm_pct)
                ticker = build_option_ticker(exp, strike)
                expected.add(ticker)

        total_expected += len(expected)
        missing_tickers = expected - existing_tickers
        already_have   += len(expected) - len(missing_tickers)

        for ticker in sorted(missing_tickers):
            strike    = int(ticker[-8:]) / 1000
            days_data = fetch_option_history(ticker, hist_start, hist_end)
            time.sleep(CALL_DELAY)
            if days_data:
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
                if is_weekly: weekly_filled += 1
                log(f"  + {ticker} ({len(days_data)}d){'[W]' if is_weekly else ''}")
            else:
                no_data += 1
                no_data_tickers.append((ticker, strike, exp, hist_start))
                if is_weekly: weekly_no_data += 1

        # Incremental commit every N expiries — saves progress if container crashes
        if (exp_idx + 1) % COMMIT_EVERY == 0 and missing > 0:
            dm["metadata"]["last_updated"] = today_iso
            dm["metadata"]["options_count"] = len(dm["options"])
            msg = f"Phase 3 incremental — {exp_idx+1}/{len(all_expiries)} expiries, {len(dm['options'])} contracts"
            log(f"  → Incremental commit: {len(dm['options'])} contracts after {exp_idx+1} expiries...")
            github_commit_file(DATA_PATH, dm, msg)

    log(f"  ✓ Inspection complete — {already_have} present, {missing} filled, {no_data} not in Polygon")
    log(f"  Weekly breakdown: {weekly_filled} filled, {weekly_no_data} not in Polygon")

    if no_data_tickers:
        weekly_gaps = [(t, s, e, h) for t, s, e, h in no_data_tickers if e not in monthly_set]
        if weekly_gaps:
            log(f"  Weekly gap samples: {', '.join(t for t,s,e,h in weekly_gaps[:5])}")

    # ── Phase 4: Add new future strikes if SPY has moved ─────────────────
    log(f"Phase 4: Syncing future expiry strikes (SPY=${current_spy:.2f})...")
    new_future = 0

    future_expiries_m = get_monthly_expiries(today_iso, future_end) if USE_MONTHLY else []
    future_expiries_w = get_weekly_expiries(today_iso, future_end)  if USE_WEEKLY  else []
    future_expiries   = sorted(set(future_expiries_m + future_expiries_w))

    for exp in future_expiries:
        exp_dt     = datetime.strptime(exp, "%Y-%m-%d")
        hist_start = max(two_yr_ago,
                         (exp_dt - timedelta(days=EXP_LOOKBACK)).strftime("%Y-%m-%d"))
        for otm_pct in OTM_TARGETS:
            strike = find_nearest_strike(current_spy, otm_pct)
            ticker = build_option_ticker(exp, strike)
            if ticker in dm["options"]:
                continue
            days_data = fetch_option_history(ticker, hist_start, today_iso)
            time.sleep(CALL_DELAY)
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
    log(f"SPY Backtest Data Collector v{COLLECTOR_VERSION} starting...")
    log(f"Mode: DAILY | Repo: {GITHUB_REPO}")
    log(f"Universe: start={START_DATE} | OTM 1-{int(max(OTM_TARGETS)*100)}% ({len(OTM_TARGETS)} targets) | monthly={USE_MONTHLY} weekly={USE_WEEKLY} | reset={DO_RESET}")

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
