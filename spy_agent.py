

import os
import time
import schedule
import requests
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

ET = ZoneInfo('America/New_York')

# ── Version ────────────────────────────────────────────────────────────────
VERSION = "1.7.53"
# Full version history: CHANGELOG.md in the PROJECT FOLDER (S46 ruling; the
# in-file log carries only the current release entry, per the S45 convention).
# 1.7.53 — TRIGGER LIST: FIXED ORDER, AND A ▲ ON THE DELTA LEVEL (S59, Ron).
#          ORDER — the list was proximity-sorted on fraction-of-threshold,
#          so it reshuffled between pushes (Delta/Decay/DTE on 09-02 SOD,
#          Delta/DTE/Decay that afternoon). Now fixed: DECAY, DELTA, DTE.
#          Decay leads because it is the harvest trigger — the one we are
#          trying to hit. The layout stops moving, so it can be read
#          positionally.
#          NOTHING IS LOST: the header still names the closest trigger and
#          its gap ("closest: Delta, 0.192 away"), which is where that
#          information belonged anyway.
#          FIRED/HELD STILL FLOAT TO THE TOP — load-bearing, it is what
#          puts the firing trigger first on a ROLL push. Implemented by
#          dropping t[2] from the sort key and relying on Python's stable
#          sort to preserve the constructed order inside each group.
#          RANKING ARITHMETIC UNTOUCHED. t[2] is still fraction-of-
#          threshold; `nearest` is still min() over the whole list and so
#          is independent of display order. The S58 audit of "closest"
#          stands.
#          ▲ ON THE DELTA LEVEL — "= SPY ≈ ▲$773 (defH-10 frame)", for
#          visual parity with the decay pair "▼$756 / ▲$778".
#          Unconditional: delta rises with spot, so its level is always
#          ABOVE spot. The marker is the direction spot must travel, NOT a
#          trend arrow — `arrow` rides on the delta VALUE one line up, and
#          the two must not be conflated.
#          DTE stays bare: it names a date, not a price.
#          DISPLAY-ONLY: trigger arithmetic, thresholds and proximity
#          ranking are all untouched.

# ── Credentials ────────────────────────────────────────────────────────────
POLYGON_KEY    = os.environ["POLYGON_KEY"]
PUSHOVER_USER  = os.environ["PUSHOVER_USER_TOKEN"]
PUSHOVER_TOKEN = os.environ["PUSHOVER_API_TOKEN"]
FINNHUB_KEY    = os.environ.get("FINNHUB_KEY") or os.environ.get("FINNHUB_API")  # optional news
ANTHROPIC_KEY  = os.environ.get("ANTHROPIC_KEY") or os.environ.get("ANTHROPIC_API_KEY")  # AI market brief

# ── Supabase — for entry SPY price lookup ─────────────────────────────────
SUPABASE_URL  = "https://pepdnkwytziegjvgkofq.supabase.co/rest/v1"
SUPABASE_ANON = "sb_publishable_k2TqNhnYKZYUBicrpsRflw_tlODD6Ma"
SB_HEADERS    = {"apikey": SUPABASE_ANON, "Authorization": f"Bearer {SUPABASE_ANON}"}

# ── Position — set these as Railway environment variables ─────────────────
# Update in Railway dashboard after each roll, no code change needed
POSITION = {
    "shares":        int(os.environ.get("SHARES", "600")),
    "cost_basis":    float(os.environ.get("COST_BASIS", "742.48")),
    "premium_sold":  float(os.environ.get("PREMIUM_SOLD", "23.82")),
    "short_strike":  float(os.environ.get("SHORT_STRIKE", "780")),
    "short_expiry":  os.environ.get("SHORT_EXPIRY", "2026-11-20"),
    "contracts":     int(os.environ.get("SHARES", "600")) // 100,
    "cover_price":   None,   # price paid to buy back previous call
    "sell_price":    None,   # price received selling new call (decay basis)
}

# ── Roll Parameters — loaded from roll_params.json ────────────────────────
# Single source of truth: https://ronb36.github.io/spy-backtest/roll_params.json
# Update roll_params.json after each backtester run — agent picks up on next check

# v1.6.2 — trigger-critical fields; a params load missing any of these raises
# (guarded() skips the check + degraded alert) instead of silently running
# the stale inline defaults in score_position.
REQUIRED_PARAM_FIELDS = ("decayTrigger", "deltaThreshold", "dteFloor",
                         "defHorizon")  # vixGate dropped v1.7.4; entryDate position-conditional v1.7.11

def load_roll_params():
    """Fetch ROLL_PARAMS from roll_params.json. Raises on any failure — no fallback."""
    url = "https://ronb36.github.io/spy-backtest/roll_params.json"
    r = requests.get(url, timeout=10)
    print(f"  roll_params.json fetch: HTTP {r.status_code}")
    if r.status_code != 200:
        raise RuntimeError(f"roll_params.json returned HTTP {r.status_code}")
    data = r.json()
    missing = [f for f in REQUIRED_PARAM_FIELDS if data.get(f) in (None, "")]
    if data.get('shortStrike') and data.get('entryDate') in (None, ""):
        missing.append('entryDate')  # v1.7.11 — required only when a short exists
    if missing:
        raise RuntimeError(f"roll_params.json missing required fields: {missing}")
    exp_yield = data.get('optProjYield') or data.get('optAnnYield', '?')
    print(f"  ROLL_PARAMS loaded (optimized {data.get('lastOptimized','?')} · {exp_yield}% proj. yield)")
    return data

try:
    ROLL_PARAMS = load_roll_params()
except Exception as e:
    print(f"  WARNING: roll_params.json fetch failed at startup: {e}")
    print(f"  ROLL_PARAMS is empty — position sync skipped. Will retry on next check.")
    ROLL_PARAMS = {}

# Sync position from JSON immediately at startup (single source of truth)
if ROLL_PARAMS:
    POSITION['flat'] = bool(ROLL_PARAMS) and (not ROLL_PARAMS.get('shortStrike') or bool(ROLL_PARAMS.get('flat')))  # v1.7.11 — absence-as-flat; empty params never flat
    if ROLL_PARAMS.get('contracts'): POSITION['contracts']   = int(ROLL_PARAMS['contracts']); POSITION['shares'] = POSITION['contracts'] * 100
    if ROLL_PARAMS.get('costBasis'):    POSITION['cost_basis']   = float(ROLL_PARAMS['costBasis'])   # v1.7.24 — ledger-derived basis, json-sourced; env COST_BASIS = boot fallback only
    if ROLL_PARAMS.get('shortStrike'): POSITION['short_strike'] = float(ROLL_PARAMS['shortStrike'])
    if ROLL_PARAMS.get('shortExpiry'): POSITION['short_expiry'] = ROLL_PARAMS['shortExpiry']
    if ROLL_PARAMS.get('coverPrice'):  POSITION['cover_price']  = float(ROLL_PARAMS['coverPrice'])
    if ROLL_PARAMS.get('sellPrice'):   POSITION['sell_price']   = float(ROLL_PARAMS['sellPrice'])
    if POSITION.get('sell_price') and POSITION.get('cover_price'):
        POSITION['premium_sold'] = POSITION['sell_price'] - POSITION['cover_price']
    elif ROLL_PARAMS.get('netPremium'):
        POSITION['premium_sold'] = float(ROLL_PARAMS['netPremium'])

# ── Signal state ───────────────────────────────────────────────────────────
last_signal = {"level": None, "sent_at": None}
SIGNAL_RANK = {"HOLD": 0, "WATCH": 1, "ROLL": 2, "ACT": 3}


# ── Failure visibility (v1.5.18) ───────────────────────────────────────────
# Counts consecutive job exceptions; alerts once at threshold, suppresses
# until recovery. Reset happens in send_notification on a successful normal
# send (skips do not touch the counter — a market-closed check proves nothing
# about the data path). Pushover stays reachable when Polygon is down, as the
# 2026-07-13/14 key-rotation outage demonstrated.
FAIL_ALERT_THRESHOLD = 3
_fail_state = {"count": 0, "alerted": False, "last_error": None}

def record_job_failure(e):
    _fail_state["count"] += 1
    _fail_state["last_error"] = str(e)
    print(f"  Consecutive job failures: {_fail_state['count']}")
    if _fail_state["count"] >= FAIL_ALERT_THRESHOLD and not _fail_state["alerted"]:
        _fail_state["alerted"] = True   # set BEFORE send — a send failure must not re-alert
        try:
            send_notification(
                f"⚠️ AGENT DEGRADED — {_fail_state['count']} consecutive job failures.\n"
                f"Last: {_fail_state['last_error']}\n"
                f"Check Railway logs.",
                _internal=True,
            )
        except Exception as ne:
            print(f"  Degraded alert send failed: {ne}")

def guarded(fn):
    """Wrap a scheduled job so no escaping exception (incl. roll_params load,
    which raises by design) can kill the scheduler loop."""
    def _run():
        try:
            fn()
        except Exception as e:
            print(f"  {fn.__name__} failed before its own handler: {e}")
            import traceback; traceback.print_exc()
            record_job_failure(e)
    _run.__name__ = f"guarded_{fn.__name__}"
    return _run


# ── Market hours ───────────────────────────────────────────────────────────
def is_market_open():
    now = datetime.now(ET)
    if now.weekday() >= 5:          # Saturday=5, Sunday=6
        return False
    # 9:30am–4:00pm ET — handles EST/EDT automatically
    market_open  = now.replace(hour=9,  minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0,  second=0, microsecond=0)
    return market_open <= now < market_close


# ── Polygon fetches ────────────────────────────────────────────────────────
def build_option_ticker(expiry_iso, strike):
    d = datetime.strptime(expiry_iso, "%Y-%m-%d")
    strike_str = str(int(round(strike * 1000))).zfill(8)
    return f"O:SPY{d.strftime('%y%m%d')}C{strike_str}"


def fetch_spy_price():
    """Returns (price, day_change) tuple. change is None if unavailable."""
    url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/SPY?apiKey={POLYGON_KEY}"
    r = requests.get(url, timeout=10).json()
    ticker = r.get("ticker", {})
    price  = ticker.get("lastTrade", {}).get("p") or ticker.get("day", {}).get("c")
    change = ticker.get("todaysChangePerc") and ticker.get("todaysChange")
    # prevDay gives us a reliable change calc
    prev_close = ticker.get("prevDay", {}).get("c")
    if price and prev_close and float(prev_close) > 0:
        change = float(price) - float(prev_close)
    elif price:
        change = None
    if price:
        return float(price), change
    # Fallback to prev close, no change available
    r2 = requests.get(f"https://api.polygon.io/v2/aggs/ticker/SPY/prev?adjusted=true&apiKey={POLYGON_KEY}", timeout=10).json()
    return float(r2["results"][0]["c"]), None


def fetch_option_snapshot(ticker):
    url = f"https://api.polygon.io/v3/snapshot/options/SPY/{ticker}?apiKey={POLYGON_KEY}"
    r = requests.get(url, timeout=10).json()
    if not r.get("results"):
        return None
    res = r["results"]
    bid = res.get("last_quote", {}).get("bid", 0)
    ask = res.get("last_quote", {}).get("ask", 0)
    if bid > 0 and ask > 0:
        mid          = (bid + ask) / 2
        price_source = "mid"
    else:
        mid          = res.get("day", {}).get("close", 0)
        price_source = "last"
    prev_close = res.get("day", {}).get("previous_close") or res.get("prevDay", {}).get("c")
    call_change = (mid - float(prev_close)) if (prev_close and float(prev_close) > 0 and mid > 0) else None
    return {
        "mid":          mid,
        "price_source": price_source,
        "change":       call_change,
        "delta":        res.get("greeks", {}).get("delta"),
        "gamma":        res.get("greeks", {}).get("gamma"),
        "theta":        res.get("greeks", {}).get("theta"),
        "iv":           res.get("implied_volatility"),
        "oi":           res.get("open_interest", 0),
    }


_vix_day = {"chg": None, "prev": None}  # v1.7.41 — captured alongside the snapshot

def fetch_vix():
    url = f"https://api.polygon.io/v3/snapshot/indices?ticker=I%3AVIX&apiKey={POLYGON_KEY}"
    r = requests.get(url, timeout=10).json()
    results = r.get("results", [])
    if results:
        # v1.7.41 — stash the day change for the Markets grid; fetch_vix's
        # return type is unchanged (regime/score consumers untouched).
        # Schema fail-soft: prefer session.change, else derive from
        # previous_close; an absent field just means no change leg renders.
        session = results[0].get("session", {}) or {}
        chg  = session.get("change")
        prev = session.get("previous_close")
        _vix_day["chg"]  = round(float(chg), 1)  if chg  is not None else None
        _vix_day["prev"] = round(float(prev), 2) if prev is not None else None
        val = results[0].get("value") or results[0].get("session", {}).get("close")
        if val:
            return round(float(val), 1)
    return None

def vix_change(current):
    """v1.7.41 — day change in VIX points; None if unknown (leg drops)."""
    if _vix_day["chg"] is not None:
        return _vix_day["chg"]
    if current is not None and _vix_day["prev"] is not None:
        return round(current - _vix_day["prev"], 1)
    return None


_yield_day = {}  # v1.7.44 — per-ticker bp change, captured alongside the snapshot

def _fetch_yield(idx):
    """v1.7.44 — one fetcher for the curve (I:TNX 10yr, I:TYX 30yr). Polygon
    returns 44.93 → 4.493%; the same /10 applies to the session change, which
    is stashed in BASIS POINTS for the Markets legs. Schema fail-soft: an
    absent change field just means no leg renders."""
    try:
        url = f"https://api.polygon.io/v3/snapshot/indices?ticker=I%3A{idx}&apiKey={POLYGON_KEY}"
        r = requests.get(url, timeout=10).json()
        results = r.get("results", [])
        if not results:
            return None
        session = results[0].get("session", {}) or {}
        val = results[0].get("value") or session.get("close")
        if not val:
            return None
        val = float(val) / 10
        chg, prev = session.get("change"), session.get("previous_close")
        if chg is not None:
            _yield_day[idx] = round(float(chg) / 10 * 100)          # → bp
        elif prev is not None:
            _yield_day[idx] = round((val - float(prev) / 10) * 100)  # → bp
        else:
            _yield_day.pop(idx, None)
        return val
    except Exception as e:
        print(f"  {idx} fetch error: {e}")
        return None

def fetch_tnx():
    return _fetch_yield("TNX")   # v1.7.44 — 10yr, unchanged contract for existing callers

def fetch_tyx():
    return _fetch_yield("TYX")   # v1.7.44 — 30yr (Ron's S55 spec: "it's moving the mkt")


def fetch_raw_headlines(max_age_hours=36):
    """
    Fetch raw headlines from Finnhub + Polygon news.
    Returns list of headline strings, or empty list.
    """
    headlines = []

    # Finnhub general news
    if FINNHUB_KEY:
        try:
            url = f"https://finnhub.io/api/v1/news?category=general&token={FINNHUB_KEY}"
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                articles = r.json()
                now_ts = datetime.now(timezone.utc).timestamp()
                cutoff = now_ts - (max_age_hours * 3600)
                recent = sorted(
                    [a for a in articles if a.get("datetime", 0) >= cutoff],
                    key=lambda x: x.get("datetime", 0), reverse=True
                )
                for a in recent[:8]:
                    h = a.get("headline", "").strip()
                    if h:
                        headlines.append(h)
                print(f"  Finnhub: {len(headlines)} headlines in last {max_age_hours}h")
                for h in headlines:  # v1.7.30 — raw pool logged (S53 diagnosis was inferred, not verified)
                    print(f"    · {h[:110]}")
        except Exception as e:
            print(f"  Finnhub fetch failed: {e}")

    # Polygon SPY news (supplemental)
    try:
        url = f"https://api.polygon.io/v2/reference/news?ticker=SPY&limit=5&apiKey={POLYGON_KEY}"
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            results = r.json().get("results", [])
            for a in results:
                h = a.get("title", "").strip()
                if h and h not in headlines:
                    headlines.append(h)
                    print(f"    · {h[:110]}")  # v1.7.30 — raw pool logged
            print(f"  Polygon news: {len(results)} articles")
    except Exception as e:
        print(f"  Polygon news fetch failed: {e}")

    return headlines


def fetch_market_brief(max_age_hours=36, mode="sod", facts=None):
    """
    Fetch and summarize market news using Claude.
    Returns list of curated bullet strings (2-3 for SOD, 1-2 for intraday).
    mode="sod"      — forward-looking, next 12-16h (default, unchanged behavior)
    mode="intraday" — backward-looking, what's driving today's move (v1.5.19)
    facts           — v1.7.30 tape-grounding: verified market data string
                      (SPY move, VIX) injected as authoritative; direction
                      claims must reconcile with it or be omitted. Callers
                      in direction-claiming modes (recap/intraday) pass it;
                      agenda modes (ahead/sod) have no session tape.
    Falls back to raw headlines if Anthropic key absent or API fails.
    """
    if mode == "intraday":
        max_age_hours = min(max_age_hours, 6)
    headlines = fetch_raw_headlines(max_age_hours=max_age_hours)
    if not headlines:
        return None

    # If no Anthropic key, fall back to top raw headlines
    if not ANTHROPIC_KEY:
        print("  No ANTHROPIC_API_KEY — using raw headlines")
        return headlines[:4]

    try:
        now_et = datetime.now(ET)
        context = now_et.strftime("%A %B %-d, %Y %-I:%M%p ET")
        headlines_text = "\n".join(f"- {h}" for h in headlines)

        grounding = ""  # v1.7.30 — tape-grounding block (empty when no facts passed)
        if facts:
            grounding = (
                f"Verified market data (authoritative):\n{facts}\n"
                "Any claim about today's US equity market direction MUST match "
                "these numbers. If a headline implies a different direction, "
                "attribute it to that headline's own timeframe or omit the "
                "direction claim entirely. Never infer market direction from "
                "headline sentiment.\n\n"
            )

        if mode == "intraday":
            prompt = (
                f"Current time: {context}\n\n"
                f"Headlines:\n{headlines_text}\n\n"
                f"{grounding}"
                "You are monitoring US equity markets for a covered call trader "
                "during the trading session. Write exactly 1-2 bullet points "
                "explaining what is driving today's US equity market move right now. "
                "Be specific — name the catalyst and direction. Each bullet max 20 words. "
                "Format: one bullet per line starting with a relevant emoji. "
                "No intro, no outro, bullets only."
            )
        elif mode == "ahead":
            # v1.6.1 — SOD: what WILL move the market (agenda framing)
            prompt = (
                f"Current time: {context}\n\n"
                f"Headlines:\n{headlines_text}\n\n"
                "You are monitoring US equity markets for a covered call trader. "
                "It is pre-market. Write exactly 2-3 bullet points covering TODAY'S "
                "known catalysts: scheduled economic data (with release times ET), "
                "notable earnings, Fed speakers, and events already in motion. "
                "Agenda framing — what is coming today, not what already happened. "
                "Each bullet max 20 words. "
                "Format: one bullet per line starting with a relevant emoji. "
                "No intro, no outro, bullets only."
            )
        elif mode == "recap":
            # v1.6.1 — EOD: what MOVED the market + overnight risks
            prompt = (
                f"Current time: {context}\n\n"
                f"Headlines:\n{headlines_text}\n\n"
                f"{grounding}"
                "You are monitoring US equity markets for a covered call trader. "
                "The market has closed. Write exactly 2-3 bullet points: what moved "
                "US equities TODAY (past tense — name the driver and direction) and "
                "what could move them overnight: futures, after-hours earnings, "
                "foreign sessions, geopolitics. Each bullet max 20 words. "
                "Format: one bullet per line starting with a relevant emoji. "
                "No intro, no outro, bullets only."
            )
        else:
            prompt = (
                f"Current time: {context}\n\n"
                f"Headlines:\n{headlines_text}\n\n"
                "You are monitoring US equity markets for a covered call trader. "
                "Write exactly 2-3 bullet points covering what could move US equity markets "
                "in the next 12-16 hours. Be specific — name the catalyst, likely direction, "
                "and magnitude if known. Each bullet max 20 words. "
                "Focus on: geopolitical risk, Fed/rates, earnings, macro data, overnight futures. "
                "Format: one bullet per line starting with a relevant emoji. "
                "No intro, no outro, bullets only."
            )

        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 400,  # v1.7.37 — 200 cut recap bullets mid-number ("down 0." class)
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=15,
        )

        if resp.status_code != 200:
            print(f"  Anthropic API: HTTP {resp.status_code} — falling back to raw headlines")
            return headlines[:4]

        _rj = resp.json()
        text = _rj["content"][0]["text"].strip()
        bullets = [line.strip() for line in text.split("\n") if line.strip()]
        # v1.7.37 — truncation guard: a max_tokens-cut response ends mid-
        # sentence in its FINAL bullet; the meta-leak guard cannot catch a
        # short clean fragment. Drop the cut bullet; if it was the only one,
        # fall back to raw headlines rather than publish a fragment.
        if _rj.get("stop_reason") == "max_tokens":
            if len(bullets) >= 2:
                print("  Market brief hit max_tokens — dropping truncated final bullet")
                bullets = bullets[:-1]
            else:
                print("  Market brief hit max_tokens with a single bullet — falling back to raw headlines")
                return headlines[:4]
        if not bullets:
            return headlines[:4]
        if not _valid_brief(bullets, mode):
            print(f"  Market brief REJECTED by meta-leak guard ({len(bullets)} lines) — dropping news section")  # v1.7.7
            return None
        print(f"  Market brief: {len(bullets)} bullets from Claude")
        return bullets

    except Exception as e:
        print(f"  Market brief failed: {e} — falling back to raw headlines")
        return headlines[:4]



def _valid_brief(bullets, mode):
    """v1.7.7 — meta/refusal leak guard. Haiku sometimes answers ABOUT the
    task instead of doing it (stale headlines -> "I don't have access to
    real-time market data..."); publishing that verbatim is worse than no
    news section. Reject: first-person meta/refusal phrases, runaway prose
    split into too many lines, or lines far beyond the 20-word budget.
    False rejects are cheap (section dropped); false accepts are the bug."""
    cap = 2 if mode == "intraday" else 3
    if len(bullets) > cap + 1:
        return False
    META = ("i don't", "i do not", "i would need", "i cannot", "i can't",
            "i lack", "i'm unable", "i am unable", "as an ai", "i apologize",
            "to write accurate", "headlines provided", "i need access")
    for b in bullets:
        lb = b.lower()
        if any(m in lb for m in META):
            return False
        if len(b) > 160:
            return False
    return True


# Keep alias for any legacy references
def fetch_market_news(max_age_hours=14):
    return fetch_market_brief(max_age_hours=max_age_hours)


def days_to_expiry(expiry_iso):
    """v1.7.52 — DATE difference, not now-difference (D-A, S59).

    Was: (expiry@00:00Z − datetime.now()).days, which floors away the
    elapsed fraction of today and so returned calendar DTE MINUS ONE at
    every moment after 00:00 UTC — i.e. on every push ever sent. Truth on
    2026-09-02 for a 2027-02-26 expiry is 177; this returned 176.

    Three independent sources agree on the date-difference convention:
    the app (index.html:2918 and siblings, Math.round((exp@T00:00:00 −
    date@T00:00:00)/86400000)), the live scan card (178d on 09-01, exact),
    and this file's own entry-IV path (get_entry_iv, .date() subtraction).
    One function was out of step with the rest of the program.

    dte feeds compute_theo, sim_frame_delta, the stop ticket and the decay
    fire levels, so the error was not cosmetic: it priced every theo a day
    short, biasing theo LOW and measured TV decay HIGH — toward premature
    firing — and would have fired the DTE floor at a true 8 days, not 7.
    """
    expiry = datetime.strptime(expiry_iso, "%Y-%m-%d").date()
    return max(0, (expiry - datetime.now(ET).date()).days)


# ── VIX regime ─────────────────────────────────────────────────────────────
def vix_regime(vix):
    if vix < 15:  return {"label": "Low",      "otm_min": 3, "otm_max": 4}
    if vix < 20:  return {"label": "Normal",   "otm_min": 4, "otm_max": 6}
    if vix < 25:  return {"label": "Elevated", "otm_min": 6, "otm_max": 8}
    return              {"label": "High",      "otm_min": 8, "otm_max": 10}


# ── Ex-Dividend Awareness ──────────────────────────────────────────────────
def get_next_ex_div_date():
    """SPY ex-dividend dates are the 3rd Friday of Mar/Jun/Sep/Dec.
    v1.7.19 — retained as the FALLBACK rule under the declared-dividend
    probe (rule-derived: nothing to go stale by neglect)."""
    from calendar import monthcalendar, FRIDAY
    today = datetime.now(timezone.utc).date()
    ex_div_months = [3, 6, 9, 12]
    candidates = []
    for year in [today.year, today.year + 1]:
        for month in ex_div_months:
            fridays = [week[FRIDAY] for week in monthcalendar(year, month) if week[FRIDAY] != 0]
            if len(fridays) >= 3:
                third_fri = fridays[2]
                from datetime import date as dt_date
                ex_div = dt_date(year, month, third_fri)
                if ex_div >= today:
                    candidates.append(ex_div)
    candidates.sort()
    return candidates[0] if candidates else None


# v1.7.19 — ex_div_risk() distance heuristic and days_to_ex_div() PULLED
# (v6.5.0/v6.10.0 precedent): superseded by extrinsic-vs-dividend economics
# in div_status()/div_push_line() below. The build_sms consumer of
# ex_div_risk was DORMANT (exdiv_risk key never populated since the
# score_position rewrite) — dead reader removed with its dead writer.

DIV_EST_AMOUNT = 1.90  # per-share quarterly estimate when nothing is declared

_div_cache = {"day": None, "val": None}

def get_next_dividend():
    """v1.7.19 — next SPY dividend: {ex(date), days, amount, basis}.
    PRIMARY: Polygon /v3/reference/dividends (reference family — a different
    entitlement from the quotes/last-trade endpoints that proved
    NOT_AUTHORIZED; probe-don't-assume per S37, any failure falls through).
    FALLBACK: third-Friday rule + DIV_EST_AMOUNT, basis 'est'.
    Cached per calendar day (~1 probe/day)."""
    from datetime import date as dt_date
    today = dt_date.today()
    if _div_cache["day"] == today and _div_cache["val"]:
        return _div_cache["val"]
    ex, amount, basis = None, None, "est"
    try:
        url = (f"https://api.polygon.io/v3/reference/dividends?ticker=SPY"
               f"&ex_dividend_date.gte={today.isoformat()}"
               f"&order=asc&sort=ex_dividend_date&limit=1&apiKey={POLYGON_KEY}")
        r = requests.get(url, timeout=10).json()
        res = (r.get("results") or [])
        if res and res[0].get("ex_dividend_date"):
            ex = datetime.strptime(res[0]["ex_dividend_date"], "%Y-%m-%d").date()
            amount = float(res[0].get("cash_amount") or 0) or None
            if amount:
                basis = "decl"
    except Exception as e:
        print(f"  Dividend probe failed ({e}) — using third-Friday rule")
    if ex is None or amount is None:
        ex = ex or get_next_ex_div_date()
        amount = amount or DIV_EST_AMOUNT
        basis = "est"
    if ex is None:
        return None
    val = {"ex": ex, "days": (ex - today).days, "amount": amount, "basis": basis}
    _div_cache["day"], _div_cache["val"] = today, val
    return val


def div_status(extrinsic_ps, div):
    """v1.7.19 — assignment economics: a rational counterparty exercises
    ahead of ex-date when the call's extrinsic < the dividend. Inside the
    5-day window: WARN < 1.25×div, ALERT < div. Outside: clear."""
    if div is None:
        return "clear"
    if div["days"] > 5 or extrinsic_ps is None:
        return "clear"
    if extrinsic_ps < div["amount"]:
        return "alert"
    if extrinsic_ps < 1.25 * div["amount"]:
        return "warn"
    return "clear"


def div_push_line(div, extrinsic_ps=None):
    """v1.7.19 — THE div composer (one formatter, one definition — the
    v1.7.18 lesson; this replaces three divergent surface-local variants).
    ALWAYS-ON: the countdown itself is the proof-of-armed (honest zero).
    Quiet (>5d):  'Div: ex 09/18 (42d) $1.91 decl'
    Window (≤5d): '... · extrinsic $12.40 · clear|⚠ WARN|⚠ ALERT'"""
    if div is None:
        return "  Div: — (no ex-date resolvable)", "clear"
    base = (f"  Div: ex {div['ex'].month}/{div['ex'].day:02d} ({div['days']}d) "
            f"${div['amount']:.2f} {div['basis']}")
    if div["days"] > 5:
        return base, "clear"
    st = div_status(extrinsic_ps, div)
    ext = f"${extrinsic_ps:.2f}" if extrinsic_ps is not None else "—"
    tag = {"clear": "clear", "warn": "⚠ WARN", "alert": "⚠ ALERT — assignment economic"}[st]
    return f"{base} · extrinsic {ext} · {tag}", st


# ── v1.7.19 — Market-frame theoretical pricing (P&L DISPLAY ONLY) ──────────
# TWO FRAMES, DELIBERATE: this is TRUE Black-Scholes (proper Φ, dividend
# yield q) for marking the call in P&L lines when the snapshot has no quote
# and the last print's age is unknowable. Every TRIGGER stays on the
# sim-frame erf convention (sim_frame_delta) and the mid/last mark — feeding
# triggers a model price would fork live policy from the sim silently
# (worst-case-shadow ruling: symmetric or not at all).
RF_RATE   = 0.045  # pinned — matches the sim's TNX anchor
DIV_YIELD = 0.011  # SPY trailing yield, config constant

def _phi(x):
    import math
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

def bs_call_price(S, K, dte, iv, r=RF_RATE, q=DIV_YIELD):
    import math
    T = max(dte, 1) / 365.0
    if S <= 0 or K <= 0 or iv is None or iv <= 0:
        return None
    d1 = (math.log(S / K) + (r - q + 0.5 * iv * iv) * T) / (iv * math.sqrt(T))
    d2 = d1 - iv * math.sqrt(T)
    return S * math.exp(-q * T) * _phi(d1) - K * math.exp(-r * T) * _phi(d2)

def _imply_iv(S, K, dte, price, r=RF_RATE, q=DIV_YIELD):
    lo, hi = 0.01, 1.50
    for _ in range(80):
        mid = (lo + hi) / 2
        p = bs_call_price(S, K, dte, mid, r, q)
        if p is None:
            return None
        if p < price:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2

_entry_iv_cache = {}  # {(entry_date, sell_price): iv}

def get_entry_iv():
    """v1.7.19 — IV implied ONCE from the actual fill (sellPrice at the
    entry-SPY chain: DM close → roll_params.entrySpy) at entry-date DTE.
    Anchoring theo to the fill makes the entry mark exact by construction
    and keeps day-over-day changes vega-neutral (same IV both sides)."""
    rp = ROLL_PARAMS
    entry_date = rp.get("entryDate")
    sell_price = float(rp.get("sellPrice") or 0)
    strike     = float(rp.get("shortStrike") or 0)
    expiry     = rp.get("shortExpiry")
    if not (entry_date and sell_price > 0 and strike > 0 and expiry):
        return None
    key = (entry_date, sell_price)
    if key in _entry_iv_cache:
        return _entry_iv_cache[key]
    entry_spy = get_entry_spy_price(entry_date)
    if not entry_spy:
        _es = rp.get("entrySpy")
        entry_spy = float(_es) if _es and float(_es) > 0 else None
    if not entry_spy:
        return None
    try:
        entry_dte = (datetime.strptime(expiry, "%Y-%m-%d").date()
                     - datetime.strptime(entry_date, "%Y-%m-%d").date()).days
    except Exception:
        return None
    iv = _imply_iv(entry_spy, strike, max(entry_dte, 1), sell_price)
    if iv:
        _entry_iv_cache[key] = iv
        print(f"  Entry IV implied: {iv*100:.2f}% (fill ${sell_price:.2f} @ SPY ${entry_spy:.2f}, {entry_dte}d)")
    return iv

_mkt_iv_cache = {"iv": None, "ts": None, "src": None}  # v1.7.25; v1.7.28 — src tier + aware-UTC ts
TRADE_IV_MAX_AGE_MIN = 60  # v1.7.28 — trade-anchor acceptance window (minutes)

def _iv_cache_put(iv, ts, src):
    """v1.7.28 — single cache writer: sanity band, monotonic timestamp.
    An older observation never overwrites a newer one, regardless of tier."""
    if not iv or not (0.02 < iv < 1.40):
        return
    if _mkt_iv_cache["ts"] is not None and ts <= _mkt_iv_cache["ts"]:
        return
    _mkt_iv_cache.update({"iv": iv, "ts": ts, "src": src})

def refresh_market_iv(spy, call_mid, dte, price_source):
    """v1.7.25 — re-imply IV from a REAL bid/ask mid and cache it.
    Only price_source == 'mid' qualifies (day-close 'last' has unknowable
    age — the original v1.7.19 objection stands for it). Silent no-op on
    anything else; theo then rides the freshest prior capture."""
    if price_source != "mid" or not call_mid or call_mid <= 0:
        return
    K = float(ROLL_PARAMS.get("shortStrike") or POSITION.get("short_strike") or 0)
    if K <= 0 or not spy or spy <= 0:
        return
    iv = _imply_iv(spy, K, max(dte, 1), call_mid)
    _iv_cache_put(iv, datetime.now(timezone.utc), "mkt")  # v1.7.28 — aware ts, single writer (v1.7.25 naive-ts fix)

def fetch_trade_anchored_iv(dte):
    """v1.7.28 Phase A — latest option MINUTE-AGG trade paired with the
    SPY minute bar of the SAME minute → time-consistent IV imply.
    Returns (iv, bar_ts_utc) or None on: no bar today, bar older than
    TRADE_IV_MAX_AGE_MIN, no same-minute SPY bar, degenerate imply, or
    any network/shape failure (silent — theo rides the existing cache)."""
    try:
        expiry = ROLL_PARAMS.get("shortExpiry") or POSITION.get("short_expiry")
        K = float(ROLL_PARAMS.get("shortStrike") or POSITION.get("short_strike") or 0)
        if not expiry or K <= 0:
            return None
        occ = build_option_ticker(expiry, K)
        day = datetime.now(ET).strftime("%Y-%m-%d")
        url = (f"https://api.polygon.io/v2/aggs/ticker/{occ}/range/1/minute/"
               f"{day}/{day}?adjusted=true&sort=desc&limit=1&apiKey={POLYGON_KEY}")
        bars = (requests.get(url, timeout=10).json().get("results") or [])
        if not bars:
            return None
        opt_c, t_ms = float(bars[0].get("c") or 0), int(bars[0].get("t") or 0)
        if opt_c <= 0 or t_ms <= 0:
            return None
        bar_ts = datetime.fromtimestamp(t_ms / 1000.0, tz=timezone.utc)
        _age_min = (datetime.now(timezone.utc) - bar_ts).total_seconds() / 60.0
        if _age_min > TRADE_IV_MAX_AGE_MIN:
            # v1.7.34 — S54 ALIGNMENT RULING (scan parity, supersedes the
            # v1.7.30 cold-start special case): any-age SAME-DAY anchor is
            # accepted; 60 min is a STALENESS FLAG, not a rejection. The old
            # rejection guarded a worse floor (frozen entry IV — the FAIL-HIGH
            # class); with the carry tier below, refusing today's own tape in
            # favor of yesterday's close would discard fresher information.
            # The monotonic cache rule still prevents an old bar from ever
            # overwriting a fresher capture.
            print(f"  IV trade-anchor: {int(_age_min)}m-old same-day bar accepted (staleness flag: >{TRADE_IV_MAX_AGE_MIN}m)")
        url2 = (f"https://api.polygon.io/v2/aggs/ticker/SPY/range/1/minute/"
                f"{t_ms}/{t_ms + 59999}?adjusted=true&sort=asc&limit=1&apiKey={POLYGON_KEY}")
        sbars = (requests.get(url2, timeout=10).json().get("results") or [])
        if not sbars:
            return None
        spy_c = float(sbars[0].get("c") or 0)
        if spy_c <= 0:
            return None
        iv = _imply_iv(spy_c, K, max(dte, 1), opt_c)
        return (iv, bar_ts) if iv else None
    except Exception:
        return None

def refresh_trade_iv(dte, price_source):
    """v1.7.28 — second waterfall tier, attempted only when the snapshot
    had no real mid. _iv_cache_put's monotonic-ts rule guarantees a
    delayed bar can never overwrite a fresher mid-based capture."""
    if price_source == "mid":
        return
    got = fetch_trade_anchored_iv(dte)
    if got:
        _iv_cache_put(got[0], got[1], "trade")


def fetch_carry_iv():
    """v1.7.34 — waterfall tier 3 (S54 alignment with backtester v6.31.0):
    IV implied from the position contract's PRIOR-SESSION bar in Supabase
    option_days — the SAME table the backtester's DM loads — paired with
    the SAME-DATE spy_daily bar (temporal pairing in the DM frame,
    vw-first per the sim's EOD-VWAP convention). compute_theo reprices it
    to live spot. Returns (iv, bar_ts_utc) or None. The bar_ts is pinned
    to that session's close so the monotonic cache rule keeps any
    same-day mkt/trade capture strictly fresher."""
    try:
        expiry = ROLL_PARAMS.get("shortExpiry") or POSITION.get("short_expiry")
        K = float(ROLL_PARAMS.get("shortStrike") or POSITION.get("short_strike") or 0)
        if not expiry or K <= 0:
            return None
        occ = build_option_ticker(expiry, K)
        url = f"{SUPABASE_URL}/option_days?ticker=eq.{occ}&order=date.desc&limit=1"
        rows = requests.get(url, headers=SB_HEADERS, timeout=10).json()
        if not isinstance(rows, list) or not rows:
            return None
        b = rows[0]
        px0 = float(b.get("vw") or b.get("c") or 0)
        bdate = b.get("date")
        if px0 <= 0 or not bdate:
            return None
        r2 = requests.get(f"{SUPABASE_URL}/spy_daily?date=eq.{bdate}&limit=1",
                          headers=SB_HEADERS, timeout=10).json()
        if not isinstance(r2, list) or not r2:
            return None
        spy0 = float(r2[0].get("vw") or r2[0].get("c") or 0)
        if spy0 <= 0:
            return None
        dte0 = (datetime.strptime(expiry, "%Y-%m-%d") - datetime.strptime(bdate, "%Y-%m-%d")).days
        iv = _imply_iv(spy0, K, max(dte0, 1), px0)
        if not iv or not (0.02 < iv < 1.40):
            return None
        bar_ts = datetime.strptime(bdate, "%Y-%m-%d").replace(hour=21, minute=0, tzinfo=timezone.utc)
        return (iv, bar_ts)
    except Exception:
        return None


def refresh_snapshot_iv(snap_iv, price_source):
    """
    v1.7.48 — waterfall tier 2.5: the VENDOR'S OWN PUBLISHED implied
    volatility for this exact contract, straight off the snapshot we
    already fetch (fetch_option_snapshot returns it as "iv"; it was
    displayed and then discarded).

    WHY IT EARNED A TIER, and why it outranks carry. On 2026-08-27 the
    12:30 push fired a FALSE ROLL: the contract had traded zero volume all
    morning, so the waterfall fell to carry (prior-session DM bar) and
    marked the call at $39.30 when the real NBBO mid was $40.52. Decay read
    8.5% against a true 5.2% and the agent said ROLL on a position that was
    three points from its trigger. Measured that afternoon:

        published IV  16.152%   (this tier)
        true mid IV   16.19%    -> off by 0.04 pts  ~= $0.08
        carry IV      15.56%    -> off by 0.63 pts  ~= $1.22

    ~15x more accurate than carry, and available on days with NO PRINTS AT
    ALL — which is exactly the day carry fails, because carry drifts most
    when the underlying moves most. The full 2027-01-29 chain pulled the
    same day showed these published IVs forming a smooth monotone skew
    (-0.065 IV pts per $1 of strike, no kinks across 764-780), evidence
    they are modelled consistently rather than noise off thin prints.

    ONE CAVEAT, MEASURED: the vendor's GREEKS are anchored to a stale
    underlying (their delta+IV back-solved to SPY $767.80 while spot was
    ~$770.6). The IV itself is sound; their SPOT is not. So we take their
    IV and price it against OUR OWN fresh spot in compute_theo — never
    their greeks, never their price.

    PRECEDENCE. _iv_cache_put orders by TIMESTAMP, not tier rank, and this
    tier has no timestamp of its own (the vendor publishes no IV asof), so
    stamping it "now" would let it outrank a genuine mkt/trade capture —
    an inversion. Instead this follows refresh_carry_iv's guard exactly:
    fill ONLY when the cache is still empty, so mkt and trade keep
    priority, and carry (which runs after and carries the same guard) is
    skipped whenever this tier succeeds.
    """
    if price_source == "mid" or _mkt_iv_cache["iv"] is not None:
        return
    if not snap_iv or snap_iv <= 0:
        return
    _iv_cache_put(snap_iv, datetime.now(timezone.utc), "snap")
    if _mkt_iv_cache["src"] == "snap":
        print(f"  IV snapshot tier: vendor published IV {snap_iv*100:.1f}% "
              f"(priced against OUR spot, not their greeks)")


def refresh_carry_iv(dte, price_source):
    """v1.7.34 — third tier: only when the cache is still empty after the
    mkt and trade tiers (a real capture of any vintage outranks carry via
    the monotonic-ts rule anyway; this guard just skips the two REST
    calls when they cannot win)."""
    if price_source == "mid" or _mkt_iv_cache["iv"] is not None:
        return
    got = fetch_carry_iv()
    if got:
        _iv_cache_put(got[0], got[1], "carry")
        print(f"  IV carry tier: prior-session DM bar → IV {got[0]*100:.1f}% (option_days, {got[1].date()})")

def compute_theo(spy, spy_change, dte):
    """v1.7.25 — {'now': $, 'prev': $, 'change': $, 'iv': f, 'iv_src': s}
    or None. IV preference: freshest market-implied (cache) → entry-fill
    implied (fallback: boot / never-quoted). prev = prior close spot at
    dte+1, SAME IV — vega-neutral WITHIN the computation; day-over-day
    theo change now carries vega when the market reprices vol."""
    iv, iv_src = _mkt_iv_cache["iv"], _mkt_iv_cache["src"]  # v1.7.28 — src from cache tier ("mkt"/"trade")
    if iv is None:
        iv, iv_src = get_entry_iv(), "entry"
    if iv is None:
        return None
    K = float(ROLL_PARAMS.get("shortStrike") or POSITION.get("short_strike") or 0)
    if K <= 0:
        return None
    now = bs_call_price(spy, K, dte, iv)
    if now is None:
        return None
    prev = None
    if spy_change is not None:
        prev = bs_call_price(spy - spy_change, K, dte + 1, iv)
    return {"now": now, "prev": prev,
            "change": (now - prev) if prev is not None else None,
            "iv": iv, "iv_src": iv_src}


# ── Composite scoring (mirrors web app logic) ──────────────────────────────
# ── Entry SPY price cache (keyed by entryDate) ─────────────────────────────
_entry_spy_cache = {}  # {entryDate: spyClose}

def get_entry_spy_price(entry_date):
    """Fetch SPY close on entry_date from Supabase spy_daily. Cached per entry_date."""
    if not entry_date:
        return None
    if entry_date in _entry_spy_cache:
        return _entry_spy_cache[entry_date]
    try:
        url = f"{SUPABASE_URL}/spy_daily?date=eq.{entry_date}&select=c"
        r = requests.get(url, headers=SB_HEADERS, timeout=8).json()
        price = float(r[0]["c"]) if r else None
        if price:
            _entry_spy_cache[entry_date] = price
            print(f"  Entry SPY on {entry_date}: ${price:.2f}")
        return price
    except Exception as e:
        print(f"  Entry SPY fetch failed: {e}")
        return None


# ── Sim-frame delta (v1.6.3 — trigger parity) ─────────────────────────────
# The backtester's delta trigger fires on callDelta(spy, K, dte, vix, TNX=4.5)
# where normalCDF(x) = 0.5·(1+erf(x)) — the S30-discovered erf convention
# (missing /√2 vs a true normal CDF; sim delta ≈ Φ(√2·d1)). The published
# deltaThreshold is a CALIBRATED CONSTANT in that coordinate system, so the
# live trigger must be computed in the same frame. Polygon market delta stays
# in messages as reference; the S28 gap (sim 0.538 vs mkt 0.509) is mostly
# this convention. True-delta migration (Option B) is a queued design session.
def sim_frame_delta(S, K, dte, vix, tnx=4.5):
    """Exact replica of backtester callDelta (erf convention, TNX pinned 4.5)."""
    import math
    T = max(dte, 1) / 365.0
    sig = max((vix or 15), 5) / 100.0
    r = (tnx or 4.5) / 100.0
    if T <= 0 or sig <= 0 or S <= 0 or K <= 0:
        return 0.0
    d1 = (math.log(S / K) + (r + 0.5 * sig * sig) * T) / (sig * math.sqrt(T))
    return 0.5 * (1.0 + math.erf(d1))


def sim_frame_gamma(S, K, dte, vix, tnx=4.5):
    """v1.6.6 — BS gamma in the sim's frame: φ(d1)/(S·σ·√T), same pinned
    inputs as sim_frame_delta (TNX 4.5, VIX as σ). Used for the overnight
    Δ-drift projection so the drift stays in the frame the deltaThreshold
    is calibrated in (drifting a sim delta with market gamma is the same
    frame-mix defect class the delta fix closed in v1.6.3)."""
    import math
    T = max(dte, 1) / 365.0
    sig = max((vix or 15), 5) / 100.0
    r = (tnx or 4.5) / 100.0
    if T <= 0 or sig <= 0 or S <= 0 or K <= 0:
        return 0.0
    d1 = (math.log(S / K) + (r + 0.5 * sig * sig) * T) / (sig * math.sqrt(T))
    pdf = math.exp(-0.5 * d1 * d1) / math.sqrt(2.0 * math.pi)
    return pdf / (S * sig * math.sqrt(T))


def defense_dte(dte):
    """v1.7.0 — effective horizon for THRESHOLD-SPACE greeks (backtester
    v6.4.x parity): min(defHorizon, dte) when defHorizon > 0, else full dte
    (identity — defHorizon=0 is byte-identical to v1.6.6). Applies ONLY to
    sim_frame_delta/sim_frame_gamma at trigger/drift call sites; the DTE
    trigger, DTE display, grace period, and yield math stay on real dte —
    the defense horizon changes the frame the stop is asked in, not the
    calendar. Full-DTE delta buys ~0.35–0.40 of d1 from the drift term
    (r+σ²/2)√T/σ at long T (S35 phantom-stop evidence)."""
    dh = ROLL_PARAMS.get("defHorizon", 0) or 0
    return min(dh, dte) if dh > 0 else dte


# ── Delta-pair persistence (v1.7.3) ────────────────────────────────────────
# Durable evidence base for the Option B dual-delta design and the
# delta-source calibration decision. vix passed = the σ the sim delta was
# actually computed with (vix_eff at SOD). One writer: this agent.
def score_position(spy, call_mid, vix, dte, delta=None, delta_mkt=None):
    """
    Score the current position using backtester-matching trigger rules.
    Signals: HOLD / WATCH / ROLL   (ACT retired with the VIX gate, v1.7.9)
      HOLD  — no triggers fired
      WATCH — delta within 0.05 of threshold (approaching)
      ROLL  — any trigger fired
    Triggers (matching backtester v5.8.0):
      1. TV Decay ≥ decayTrigger: (entryTimeValue - currentTimeValue) / entryTimeValue
      2. Delta ≥ deltaThreshold
      3. DTE ≤ dteFloor
    v1.6.0 parity rules:
      - Grace period: triggers evaluate only once daysHeld ≥ 3 (sim
        daysHeldSoFar >= 3); DTE ≤ 0 (expiry) bypasses the grace.
    """
    pos = POSITION
    rp  = ROLL_PARAMS

    strike      = pos["short_strike"]
    sell_price  = pos.get("sell_price") or pos.get("premium_sold") or 0
    entry_date  = rp.get("entryDate") or pos.get("short_expiry", "")[:10]
    dist        = strike - spy
    current_otm = (dist / spy) * 100

    # ── TV Decay trigger ───────────────────────────────────────────────────
    # entryTimeValue = max(0, sellPrice - max(0, entrySPY - strike))
    # currentTimeValue = max(0, callMid - max(0, spy - strike))
    entry_spy = get_entry_spy_price(entry_date)
    entry_spy_basis = "dm" if entry_spy else None
    if not entry_spy:
        # v1.6.4 — provisional: the ticket's scan-time spot (v6.1.11 field).
        # Correct intrinsic-stripped formula on entry day; the DM close
        # supersedes it automatically after the nightly collection.
        _es = rp.get("entrySpy")
        if _es and float(_es) > 0:
            entry_spy = float(_es)
            entry_spy_basis = "params"
    # v1.7.45 — initialized so the fallback branch below (and the export in
    # the return dict) can never NameError. None is the "no decomposition"
    # sentinel the TV display row tests against.
    entry_time_value = current_time_value = None
    if entry_spy and sell_price > 0:
        entry_intrinsic    = max(0.0, entry_spy - strike)
        entry_time_value   = max(0.0, sell_price - entry_intrinsic)
        current_intrinsic  = max(0.0, spy - strike)
        current_time_value = max(0.0, call_mid - current_intrinsic)
        if entry_time_value > 0:
            tv_decay = (entry_time_value - current_time_value) / entry_time_value
        else:
            tv_decay = 0.0
    else:
        # Fallback: simple price decay if entry SPY unavailable
        tv_decay = ((sell_price - call_mid) / sell_price) if sell_price > 0 else 0.0

    tv_decay_pct = tv_decay * 100

    # ── Grace period (v1.6.3 — CALENDAR days, restoring v1.6.0) ──────────
    # v1.6.2 switched to trading days on the claim that sim daysHeldSoFar
    # counts trading days. S30 source verification (index.html ~line 1813)
    # falsified that claim: the sim computes a CALENDAR-day difference
    # (Math.round((todayD − entryDate)/86400000)). Parity = calendar days.
    # If the sim ever moves to trading days, this moves with it.
    try:
        days_held = (datetime.now(ET).date()
                     - datetime.strptime(entry_date, "%Y-%m-%d").date()).days
    except Exception:
        days_held = None  # entry date unknown — fail open, evaluate triggers
    # v1.7.12 — grace from json (single source of truth; sim/backtester paired
    # at v6.8.3). Tolerant default 3 preserves behavior if the field is absent
    # or null — ordering-safe in both deploy directions.
    _g = rp.get("graceDays", 3)
    try:
        grace_days = int(_g) if _g is not None else 3
    except (TypeError, ValueError):
        grace_days = 3
    in_grace = days_held is not None and days_held < grace_days

    # ── Trigger evaluation ─────────────────────────────────────────────────
    # v1.7.6: raw = pre-grace-mask condition; fired = raw AND grace mask.
    # Identical to the v1.6.0 expressions by distribution — no signal change.
    decay_raw = tv_decay >= rp.get("decayTrigger", 0.20)
    delta_raw = delta is not None and delta >= rp.get("deltaThreshold", 0.670)
    dte_raw   = dte <= rp.get("dteFloor", 7)
    decay_fired = (not in_grace) and decay_raw
    delta_fired = (not in_grace) and delta_raw
    dte_fired   = dte_raw and ((not in_grace) or dte <= 0)
    any_trigger = decay_fired or delta_fired or dte_fired

    # ── Signal ────────────────────────────────────────────────────────────
    # v1.7.9 — VIX gate excised (vixGate retired v1.7.4; gate was provably
    # always open with the field absent). ROLL iff any trigger.
    if any_trigger:
        signal = "ROLL"
    elif delta is not None and delta >= (rp.get("deltaThreshold", 0.670) - 0.05):
        signal = "WATCH"
    else:
        signal = "HOLD"

    # ── Trigger descriptions (delta first, matching sim reason order) ─────
    roll_triggers = []
    if delta_fired:
        roll_triggers.append(f"Delta {delta:.3f} ≥ {rp.get('deltaThreshold',0.670)}")
    if decay_fired:
        roll_triggers.append(f"TV decay {tv_decay_pct:.1f}% ≥ {rp.get('decayTrigger',0.20)*100:.0f}%")  # v1.7.18
    if dte_fired:
        roll_triggers.append(f"DTE {dte}d ≤ {rp.get('dteFloor',7)}d")

    ann_yield = (call_mid / spy) * (365 / dte) * 100 if dte > 0 else 0

    return {
        "signal":        signal,
        "dist":          dist,
        "otm_pct":       current_otm,
        "tv_decay":      tv_decay_pct,
        # v1.7.45 — the two legs the decay trigger actually divided. Exported
        # so the Position P&L TV row READS the fired values instead of
        # recomputing the intrinsic strip and the entry_spy waterfall in the
        # renderer (one formatter, one definition — the v1.7.16/.18 shadowing
        # shape, which has bitten three times). None on the fallback branch,
        # where no time-value decomposition exists and the row must not render.
        "entry_tv":      entry_time_value,
        "current_tv":    current_time_value,
        "entry_spy":     entry_spy,
        "entry_spy_basis": entry_spy_basis,  # v1.6.4 — dm | params | None
        "roll_triggers": roll_triggers,
        "decay_fired":   decay_fired,
        "delta_fired":   delta_fired,
        "dte_fired":     dte_fired,
        "decay_raw":     decay_raw,    # v1.7.6 — pre-grace-mask conditions
        "delta_raw":     delta_raw,
        "dte_raw":       dte_raw,
        "in_grace":      in_grace,
        "days_held":     days_held,
        "grace_days":    grace_days,   # v1.7.12 — displays read this, never a literal
        "any_trigger":   any_trigger,
        "ann_yield":     ann_yield,
        "premium_decay": tv_decay_pct,  # backward compat alias
        "composite":     0,             # removed — kept for backward compat
        "regime":        vix_regime(vix),
    }


# ── SMS generation ─────────────────────────────────────────────────────────
# v1.5.19: Δ trend memory — prior check's delta, in-memory only (no arrow on
# first check after restart, by design)
_prev_delta = None


def _fmt_expiry(expiry_iso):
    """v1.6.5 — short expiry for the position line: '12/18', or '1/15/27'
    when the expiry year differs from the current year. Empty on parse
    failure (line renders as before)."""
    try:
        d = datetime.strptime(expiry_iso[:10], "%Y-%m-%d").date()
        if d.year != datetime.now(ET).year:
            return f"{d.month}/{d.day}/{d.year % 100}"
        return f"{d.month}/{d.day}"
    except Exception:
        return ""



def build_sms(s, spy, spy_change, call_mid, call_change, price_source, vix, dte, delta,
              brief=None, delta_mkt=None, theo=None, div=None, extrinsic_ps=None,
              trig_src=None,  # v1.7.35 — mark provenance for the Triggers heading (None keeps legacy callers valid)
              tnx=None):      # v1.7.41 — 10yr joins the intraday 2x2 Markets grid (defaulted: legacy callers valid)
    global _prev_delta
    sig       = s["signal"]
    # v1.7.41 — regime var pulled (zero consumers after the 2x2; not dormant)
    now       = datetime.now(ET).strftime("%-I:%M%p ET").replace("AM","am").replace("PM","pm")
    rp        = ROLL_PARAMS
    decay_pct = s["tv_decay"]
    delta_val = delta or 0
    strike    = POSITION["short_strike"]
    shares    = POSITION.get("shares", POSITION.get("contracts", 0) * 100)

    # v1.7.36 — source_tag pulled (zero consumers after the single-char tag; not left dormant)

    # v1.7.19 — always-on dividend line + promotion level
    div_line, div_level = div_push_line(div, extrinsic_ps)

    # ── Line 1: signal (⚠DIV promoted here on warn/alert — assignment-eve
    # must look different from every other morning)
    _emojis = {"HOLD": "🟢", "WATCH": "🟡", "ROLL": "🔴", "ACT": "⚫"}
    # v1.7.49 — PROVISIONAL FIRE (S56, Ron's ruling after the 08-27 false
    # ROLL). A fire whose mark came from the CARRY tier is not trigger-grade
    # enough to display as a red verdict: carry is the prior session's
    # surface, and it drifts most on exactly the days the underlying moves
    # most. On 08-27 it marked the call $1.22 under the real mid and turned
    # a true 5.2% decay into a fired 8.5%. The push looked identical to a
    # fire on a real quote; only Ron pulling the broker book caught it.
    #
    # WHY THE LINE IS AT CARRY AND NOT AT "no real mid". price_source is
    # PERMANENTLY "last" on this plan (the vendor returns no last_quote for
    # any option), so EVERY fire is a theo-* fire. Downgrading all of them
    # would make every ROLL a WATCH and destroy the signal entirely. mkt and
    # trade are real captures; snap (v1.7.48) measured within $0.11 of the
    # true mid. Carry is the demonstrated failure, and since v1.7.48 it is
    # also the rarest — reached only when the vendor publishes no IV at all.
    #
    # DISPLAY-ONLY. score_position is untouched and s["signal"] still reads
    # ROLL; this changes what the push ASKS FOR, not what the trigger
    # decided. The verdict is not suppressed — it is made conditional on the
    # one check that would have caught 08-27.
    _provisional = (sig == "ROLL" and trig_src == "theo-carry")
    _sig_disp = "WATCH" if _provisional else sig
    _div_tag = f" ⚠DIV {div['days']}d" if (div and div_level in ("warn", "alert")) else ""
    line_signal = f"{_emojis.get(_sig_disp, '🟢')} {_sig_disp}{_div_tag} {now}"
    if _provisional:
        line_signal += ("\n  ⚠ PROVISIONAL — fired on carry-IV (prior session)."
                        "\n  CONFIRM against the broker bid/ask before rolling.")

    # ── Position / Daily P&L / Position P&L — v1.7.22 three-section reshape
    # (Ron's S51 spec). Position = entries only (Long @ cost basis, Short @
    # fill), moneyness + VIX + Div stay here. Daily P&L = two rows (SPY,
    # Call): last · market-frame chg · position-frame P&L (S48 one-frame-
    # per-group ruling), no net. Position P&L = Call fill → mark · PRICE chg
    # (v1.7.45: price units, not $ P&L — the row shows two prices and the
    # dollar legs live in Daily P&L above), plus the v1.7.45 TV sub-row
    # (absorbs the v1.7.21 Sell line; share leg + net dropped by ruling).
    # v1.7.19 rules carried: theo-aware mark; no partial numbers (chg · P&L
    # render only when the day-change exists).
    # v1.7.36 (Ron's spec — the v1.7.35 tag wrapped on iPhone): the call row
    # carries a SINGLE-CHAR mark tag and nothing else — t = theo (any tier),
    # l = raw last, m = real NBBO mid. The divergence witness and anchor age
    # are dropped from this row (Ron: "if I keep seeing t I can verify last
    # with Fidelity"); full provenance survives on the Triggers heading tag
    # and in the console log.
    _use_theo = theo is not None and price_source != "mid"
    # v1.7.37 — tag computed ONCE, consumed by both the Daily P&L mark and
    # the Position P&L row (one definition; S54-approved extension).
    _tag = "t" if _use_theo else ("m" if price_source == "mid" else "l")
    mark_str = f"${(theo['now'] if _use_theo else call_mid):.2f}{_tag}"
    _call_chg = (theo["change"] if (_use_theo and theo.get("change") is not None) else call_change)
    _fill = float(rp.get("sellPrice") or 0)
    _mark = theo["now"] if _use_theo else call_mid

    pos_lines = [
        f"  Long {shares} SPY @ ${POSITION['cost_basis']:.2f}",
        f"  Short {POSITION.get('contracts', shares // 100)} {_exp_mmddyy(POSITION.get('short_expiry',''))} "
        f"${strike:g}C" + (f" @ ${_fill:.2f}" if _fill > 0 else ""),
    ]
    _sf = _shares_flag()   # v1.7.24 — sharesHeld tripwire on the heartbeat surface
    if _sf:
        pos_lines.append(_sf)

    daily_lines = []
    _spy_row = f"  SPY ${spy:.2f}"
    if spy_change is not None and shares > 0:
        _spy_row += (f" {_fmt_signed(spy_change, dollars=True, decimals=2)}"
                     f" · {_fmt_signed(spy_change * shares, dollars=True)}")
    daily_lines.append(_spy_row)
    if _mark:
        _call_row = f"  Call {mark_str}"
        if _call_chg is not None and shares > 0:
            _call_row += (f" {_fmt_signed(_call_chg, dollars=True, decimals=2)}"
                          f" · {_fmt_signed(-_call_chg * shares, dollars=True)}")
        daily_lines.append(_call_row)

    ppnl_lines = []
    if _fill > 0 and _mark and shares > 0:
        # v1.7.37 — single-char mark tag (v1.7.36 convention), replacing the
        # spelled " theo"; the row now names its mark source in all three
        # cases (t/l/m), matching the Daily P&L call row.
        # v1.7.45 — PRICE change, not P&L dollars (Ron's spec). The row shows
        # two PRICES, so the change belongs in price units; the dollar P&L
        # already lives in the Daily P&L block. Sign follows PRICE DIRECTION,
        # matching the Daily call row's own convention (_call_chg is rendered
        # raw there and only the P&L leg is inverted for the short). A rising
        # call therefore reads +$0.72 here and −$xxx there — same quantity,
        # different units, each signed in its own frame.
        ppnl_lines.append(f"  Call ${_fill:.2f} → ${_mark:.2f}{_tag}"
                          f" · {_fmt_signed(_mark - _fill, dollars=True, decimals=2)}")
        # v1.7.45 — TV leg. Once the position goes ITM the call price and its
        # time value move in OPPOSITE directions (08-25: price +$0.72 while
        # TV fell $1.25 = 3.4% decay), and a bare price change sitting three
        # lines above "TV Decay 3.4%" invites exactly the misread that costs a
        # fire test. Values come from score_position — the same two legs the
        # decay trigger divided — so this row is self-evidencing rather than a
        # second opinion. Renders only when the decomposition exists.
        _etv, _ctv = s.get("entry_tv"), s.get("current_tv")
        if _etv is not None and _ctv is not None and _etv > 0:
            ppnl_lines.append(f"    TV ${_etv:.2f} → ${_ctv:.2f}"
                              f" · {_fmt_signed(_ctv - _etv, dollars=True, decimals=2)}"
                              f" ({(_etv - _ctv) / _etv * 100:.1f}%)")
    # v1.7.23 — moneyness moves here as row 2 (Ron's spec); always rendered,
    # so the Position P&L section never vanishes. v1.7.13 convention kept:
    # sign-correct label, pct = SPY vs strike (ITM positive) = −otm_pct.
    ppnl_lines.append(f"  {'ITM' if s['dist'] < 0 else 'OTM'}: ${abs(s['dist']):.2f} from Strike "
                      f"({_fmt_signed(-s['otm_pct'], decimals=1)}%)")

    # ── Δ trend arrow (vs prior check, in-memory) ───────────────────────────
    arrow = ""
    if delta is not None and _prev_delta is not None:
        if   delta > _prev_delta: arrow = " ▲"
        elif delta < _prev_delta: arrow = " ▼"
        else:                     arrow = " —"
    if delta is not None:
        _prev_delta = delta

    # ── Consolidated Triggers block — v1.7.18: DELEGATES to the shared
    # build_trigger_block() (one formatter, one definition). This function
    # carried a full local copy of the composer — the v1.7.16 shadowing
    # shape, third sighting: any precision/label fix to the shared path
    # would have silently missed the intraday SMS surface. Duplicate
    # pulled, not left dormant (v6.5.0/v6.10.0 precedent); the Δ trend
    # arrow — the only real delta between the copies — moved into the
    # shared composer as a defaulted param.
    header, trig_lines = build_trigger_block(
        delta_val, decay_pct, dte, vix,
        s["delta_fired"], s["decay_fired"], s["dte_fired"],
        has_entry_spy=s.get("entry_spy") is not None,
        approx=False, entry_spy_basis=s.get("entry_spy_basis"),
        delta_mkt=delta_mkt,
        in_grace=s.get("in_grace", False), days_held=s.get("days_held"),
        delta_raw=s.get("delta_raw"), decay_raw=s.get("decay_raw"),
        dte_raw=s.get("dte_raw"), grace_days=s.get("grace_days", 3),
        arrow=arrow,
        breakout=True)   # v1.7.50 — the "= what it takes" sub-lines reach the
    # intraday surface (Ron, S58). EOD has carried them since v1.7.26 and SOD
    # since v1.7.45; intraday was the last surface printing trigger VALUES
    # without the LEVELS that fire them — and it is the surface read while the
    # levels are still actionable. The heading's mark tag (v1.7.35) already
    # names which mark the decay trigger evaluated on, so the level and its
    # provenance now render together here.
    # v1.7.35 — the frame of the verdict, in the verdict (scan px-tag parity):
    # the Triggers heading names the mark the decay trigger evaluated on.
    # Intraday surface only — EOD stays close-vs-close BY DESIGN and its
    # heading stays untagged.
    if trig_src:
        header = f"{header} · mark {trig_src}" + (
            f" {int((datetime.now(timezone.utc) - _mkt_iv_cache['ts']).total_seconds() // 60)}m"
            if (trig_src in ("theo-mkt", "theo-trade") and _mkt_iv_cache["ts"]) else "")

    # Gate one-liner inside trigger block
    # v1.7.9 — VIX gate line removed (gate excised)

    # ── Assemble ────────────────────────────────────────────────────────────
    lines = [
        line_signal,
        "",
        "Position:",
        *pos_lines,
        "",
        "Daily P&L:",
        *daily_lines,
        "",
        "Position P&L:",   # v1.7.23 — OTM row always present, section always renders
        *ppnl_lines,
        div_line,  # v1.7.39 — div under the ITM/OTM row (position economics:
                   # pending cash event on the share leg + early-assignment
                   # clock on the short call, next to the moneyness that
                   # gates it). v1.7.19 always-on (proof-of-armed) kept.
        "",
        "Markets:",        # v1.7.39 — Ron's S55 spec: the push is his only
                           # market view away from the desk
        *markets_2x2(vix, tnx, fetch_tyx()),   # v1.7.41 — shared 2x2 (VIX+chg · 10yr / Oil · Gold)
    ]
    lines += [
        "",
        header,
        *trig_lines,
    ]
    # ── Intraday market commentary (v1.5.19) ────────────────────────────────
    if brief:
        lines.append("")
        for b in brief[:2]:
            lines.append(b if _starts_with_emoji(b) else f"📊 {b}")

    return "\n".join(lines)


def _starts_with_emoji(text):
    """Rough check: first char outside basic ASCII → assume emoji-prefixed bullet."""
    return bool(text) and ord(text[0]) > 0x2000


# ── Push notification via Pushover ─────────────────────────────────────────
PUSHOVER_CHAR_LIMIT = 1024  # transport cap; Pushover truncates silently past this

def _fit_push_budget(body, limit=PUSHOVER_CHAR_LIMIT):
    """v1.7.38 — transport budget guard (S55; the '…SPY down 0.' class was
    Pushover's 1024-char cap, not Haiku — three sightings all cut at exactly
    message end, the 08-19 6:30pm push at char 1024 on the nose). Shed WHOLE
    trailing lines until the body fits: news sits last on every surface, so
    bullets shed first and no number can ever again be cut mid-value in
    transit. One guard at the single choke point covers every composer,
    present and future (the v1.7.16 one-formatter principle applied to the
    exit door)."""
    if len(body) <= limit:
        return body
    lines = body.split("\n")
    while lines and len("\n".join(lines)) > limit:
        dropped = lines.pop()
        print(f"  Push budget: dropped trailing line ({len(dropped)} chars): {dropped[:60]!r}")
    return "\n".join(lines)

def alert_priority(s, delta):
    """v1.7.43 — escalation ladder for the delta stop (Ron's S55 spec:
    'fire rather than hope I'm watching'). Returns (priority, sound).
    Delta FIRED → Pushover emergency (2): sirens and REPEATS every 60s for
    up to 1h until acknowledged on the phone. Pre-alarm at Δsim ≥ 0.70 →
    high (1): bypasses quiet hours, no repeat. Everything else → normal.
    Decay/DTE stay normal — they are harvest exits measured at closes, not
    intraday defenses. delta is passed EXPLICITLY (sim frame): the state
    dict carries fired/raw booleans, not the value."""
    try:
        if s.get("delta_fired") and not s.get("in_grace"):
            return 2, "siren"
        if delta is not None and delta >= PRE_ALARM_DELTA:
            return 1, "pushover"
    except Exception:
        pass
    return 0, "pushover"

PRE_ALARM_DELTA = 0.70  # v1.7.43 — pre-alarm line (sim frame), below the 0.74 trigger

def send_notification(body, _internal=False, priority=0):
    body = _fit_push_budget(body)
    url = "https://api.pushover.net/1/messages.json"
    data = {
        "token":   PUSHOVER_TOKEN,
        "user":    PUSHOVER_USER,
        "message": body,
        "title":   "SPY Roll Agent",
        "sound":   "pushover"
    }
    # v1.7.43 — emergency escalation: priority 2 REQUIRES retry/expire
    # (Pushover spec: retry ≥ 30s, expire ≤ 10800s) and repeats until the
    # notification is acknowledged; priority 1 bypasses quiet hours.
    if priority:
        data["priority"] = priority
        if priority == 2:
            data["retry"], data["expire"], data["sound"] = 60, 3600, "siren"
    r = requests.post(url, data=data, timeout=10)
    print(f"  Notification sent: {r.status_code} — {body}")
    # v1.5.18 — a successful NORMAL send proves the full data+notify path;
    # reset the failure counter. _internal sends (degraded alerts) don't count.
    if r.status_code == 200 and not _internal:
        if _fail_state["alerted"]:
            print(f"  Recovered after {_fail_state['count']} consecutive job failures.")
        _fail_state["count"] = 0
        _fail_state["alerted"] = False
    return r.status_code == 200


# ── Should notify ──────────────────────────────────────────────────────────
def should_notify(new_signal):
    # Always send — every 30-min check is a heartbeat confirming agent is alive
    return True


# ── Main check ─────────────────────────────────────────────────────────────

def send_flat_update(slot="intraday"):
    """v1.7.10 — FLAT mode surface: long SPY, no short. One compact push;
    no option fetch, no triggers, no delta_pairs. slot labels the flavor."""
    now_time = datetime.now(ET).strftime("%-I:%M%p ET").replace("AM","am").replace("PM","pm")
    shares = POSITION.get('shares', 600)
    # v1.7.13 — "Covered $0.00" conditional: render only when coverPrice
    # parses to a positive float (string "0.00" was truthy → fictional cover)
    _cp = ROLL_PARAMS.get('coverPrice')
    try:
        _cp = float(_cp) if _cp is not None else 0.0
    except (TypeError, ValueError):
        _cp = 0.0
    lines = ["\U0001F7E6 FLAT " + now_time, "",
             "Position:",
             f"  Long {shares} SPY \u2014 no short call",
             (f"  Covered ${_cp:.2f} \u00b7 awaiting re-entry"
              if _cp > 0 else "  Awaiting re-entry"),
             "  Next: scan for entry candidates (fresh sell = always net credit)"]
    try:
        spy, spy_change = fetch_spy_price()
        if spy: lines.insert(3, f"  SPY ${spy:.2f} {'+' if (spy_change or 0) >= 0 else ''}{(spy_change or 0):.2f}")
    except Exception as e:
        print(f"  FLAT: SPY fetch failed ({e}) \u2014 sending without quote")
    try:
        brief = fetch_market_brief(mode="intraday" if slot == "intraday" else slot)
        if brief: lines += [""] + brief[:3]
    except Exception as be:
        print(f"  FLAT: brief failed ({be})")
    msg = "\n".join(lines)
    sent = send_notification(msg)
    print(f"  Notification sent: {sent} \u2014 FLAT {now_time}")


def check_position():
    if not is_market_open():
        print(f"{datetime.now(ET).strftime('%H:%M ET')} — Market closed, skipping.")
        return

    # Refresh params on every check — picks up new OPT results without redeploy
    global ROLL_PARAMS, POSITION
    ROLL_PARAMS = load_roll_params()
    # Sync position fields from JSON if present (overrides env vars)
    POSITION['flat'] = bool(ROLL_PARAMS) and (not ROLL_PARAMS.get('shortStrike') or bool(ROLL_PARAMS.get('flat')))  # v1.7.11 — absence-as-flat; empty params never flat
    if ROLL_PARAMS.get('contracts'): POSITION['contracts']   = int(ROLL_PARAMS['contracts']); POSITION['shares'] = POSITION['contracts'] * 100
    if ROLL_PARAMS.get('costBasis'):    POSITION['cost_basis']   = float(ROLL_PARAMS['costBasis'])   # v1.7.24 — ledger-derived basis, json-sourced; env COST_BASIS = boot fallback only
    if ROLL_PARAMS.get('shortStrike'):  POSITION['short_strike'] = float(ROLL_PARAMS['shortStrike'])
    if ROLL_PARAMS.get('shortExpiry'):  POSITION['short_expiry'] = ROLL_PARAMS['shortExpiry']
    if ROLL_PARAMS.get('coverPrice'):   POSITION['cover_price']  = float(ROLL_PARAMS['coverPrice'])
    if ROLL_PARAMS.get('sellPrice'):    POSITION['sell_price']   = float(ROLL_PARAMS['sellPrice'])
    # Derive netPremium from coverPrice/sellPrice when both present; else fall back to stored value
    if POSITION.get('sell_price') and POSITION.get('cover_price'):
        POSITION['premium_sold'] = POSITION['sell_price'] - POSITION['cover_price']
    elif ROLL_PARAMS.get('netPremium'):
        POSITION['premium_sold'] = float(ROLL_PARAMS['netPremium'])

    if POSITION.get('flat'):  # v1.7.10 — no short: compact flat push, nothing else
        send_flat_update("intraday")
        return

    print(f"{datetime.now(ET).strftime('%H:%M ET')} — Checking position...")

    try:
        spy, spy_change = fetch_spy_price()
        ticker   = build_option_ticker(POSITION["short_expiry"], POSITION["short_strike"])
        snapshot = fetch_option_snapshot(ticker)
        vix      = fetch_vix()
        dte      = days_to_expiry(POSITION["short_expiry"])

        if not snapshot or snapshot["mid"] <= 0:
            print("  Could not fetch option snapshot — skipping.")
            return

        call_mid  = snapshot["mid"]
        delta_mkt = snapshot.get("delta")
        # v1.6.3 — trigger evaluates in the SIM'S delta frame (see
        # sim_frame_delta); Polygon delta kept as displayed reference.
        delta     = sim_frame_delta(spy, POSITION["short_strike"], defense_dte(dte), vix)
        iv        = snapshot.get("iv")

        if not vix:
            print("  Could not fetch VIX — using fallback 18.0")
            vix = 18.0

        delta_disp = f"{delta:.3f}" if delta else "—"
        iv_disp    = f"{iv*100:.1f}%" if iv else "—"
        print(f"  SPY: ${spy:.2f} | Call: ${call_mid:.2f} | VIX: {vix:.1f} | DTE: {dte} | Delta: {delta_disp} | IV: {iv_disp}")

        call_change   = snapshot.get("change")
        price_source  = snapshot.get("price_source", "last")

        # v1.7.34 PHASE B (Ron's S54 ruling) — the INTRADAY TRIGGER MARK is
        # the waterfall theo when the snapshot has no real mid. 'last' has
        # unknowable age: on a printless LEAP day it is the PRIOR close and
        # manufactures false verdicts in both directions (08-13 false FIRE,
        # 08-18 false HOLD: last $38.24 · decay 3.7% while theo $35.32 and
        # the true book $34.88 both sat through the 8% gate at ~12%).
        # Waterfall: mkt mid → same-day trade anchor → carry-IV (prior-
        # session DM bar). The entry-IV floor is NOT trigger-grade: the
        # trigger ABSTAINS LOUDLY there — never a silent HOLD or fire.
        # EOD stays close-vs-close BY DESIGN (sim-matching); SOD projections
        # untouched.
        refresh_market_iv(spy, call_mid, dte, price_source)   # v1.7.25 — real mid re-implies + caches IV
        refresh_trade_iv(dte, price_source)                   # v1.7.28 — minute-trade pair tier (no-mid runs)
        refresh_snapshot_iv(iv, price_source)                 # v1.7.48 — vendor published IV (beats carry ~15x)
        refresh_carry_iv(dte, price_source)                   # v1.7.34 — prior-session DM bar tier
        theo = compute_theo(spy, spy_change, dte)

        if price_source == "mid":
            trig_mark, trig_src = call_mid, "mid"
        elif theo and theo.get("iv_src") in ("mkt", "trade", "snap", "carry"):
            trig_mark, trig_src = theo["now"], "theo-" + theo["iv_src"]
        else:
            trig_mark, trig_src = None, "degraded"

        if trig_mark is None:
            msg = (f"SPY Roll Agent: ⚠ MARK DEGRADED · {datetime.now(ET).strftime('%H:%M')} ET\n\n"
                   f"  ${POSITION['short_strike']:.0f}C trigger mark unavailable — no NBBO mid, "
                   f"no same-day print, no DM carry bar (entry-IV floor is not trigger-grade, v1.7.34).\n"
                   f"  Triggers ABSTAINED — check the broker book manually.\n"
                   f"  last ${call_mid:.2f} · SPY ${spy:.2f}")
            send_notification(msg)
            print("  TRIGGER ABSTAIN — mark degraded (entry-IV floor); notified, no verdict issued.")
            return

        s      = score_position(spy, trig_mark, vix, dte, delta=delta)
        signal = s["signal"]

        print(f"  Signal: {signal} | mark {trig_src} ${trig_mark:.2f} | {s['regime']['label']} vol | {s['otm_pct']:.1f}% OTM | TV decay {s['tv_decay']:.1f}%")
        div  = get_next_dividend()
        _mark_ps = (theo["now"] if (theo and price_source != "mid") else call_mid)
        extrinsic_ps = (_mark_ps - max(0.0, spy - POSITION["short_strike"])) if _mark_ps else None
        if theo:
            _age = ""
            if theo.get("iv_src") in ("mkt", "trade", "snap", "carry") and _mkt_iv_cache["ts"]:  # v1.7.28 — aware-UTC age; v1.7.34 +carry; v1.7.48 +snap
                _age = f" {int((datetime.now(timezone.utc) - _mkt_iv_cache['ts']).total_seconds() // 60)}m"
            print(f"  Theo: ${theo['now']:.2f} (IV {theo['iv']*100:.1f}% {theo.get('iv_src','?')}{_age}) vs {price_source} ${call_mid:.2f}")

        # v1.5.19: intraday market commentary — never blocks the push
        brief = None
        try:
            _facts = None  # v1.7.30 — tape-grounding (same quote the push displays)
            if spy and spy_change is not None:
                _facts = (f"SPY ${spy:.2f}, {'+' if spy_change >= 0 else ''}{spy_change:.2f} "
                          f"({'+' if spy_change >= 0 else ''}{spy_change / (spy - spy_change) * 100:.2f}%) so far today"
                          + (f" · VIX {vix:.1f}" if vix else ""))
            brief = fetch_market_brief(mode="intraday", facts=_facts)
        except Exception as be:
            print(f"  Intraday brief failed: {be} — sending without commentary")

        sms  = build_sms(s, spy, spy_change, call_mid, call_change, price_source, vix, dte, delta,
                         brief=brief, delta_mkt=delta_mkt, theo=theo, div=div,
                         extrinsic_ps=extrinsic_ps, trig_src=trig_src,
                         tnx=fetch_tnx())  # v1.7.41 — 10yr for the 2x2 grid
        _prio, _ = alert_priority(s, delta)   # v1.7.43 — delta escalation ladder
        if _prio:
            print(f"  Alert priority {_prio} ({'delta FIRED' if _prio == 2 else 'pre-alarm ≥' + str(PRE_ALARM_DELTA)})")
        sent = send_notification(sms, priority=_prio)
        if sent:
            last_signal["level"]   = signal
            last_signal["sent_at"] = time.time()

    except Exception as e:
        print(f"  Error: {e}")
        import traceback; traceback.print_exc()
        record_job_failure(e)





# ── ES Futures ─────────────────────────────────────────────────────────────
def get_es_front_month_ticker():
    """
    Derive the ES front-month ticker automatically.
    ES rolls quarterly: Mar(H), Jun(M), Sep(U), Dec(Z).
    Roll typically happens ~1 week before expiry (3rd Friday of month).
    """
    now = datetime.now(ET)
    year_digit = str(now.year)[-1]  # e.g. 2026 → "6"

    # Quarterly expiry months and their codes
    quarters = [
        (3,  "H"),   # March
        (6,  "M"),   # June
        (9,  "U"),   # September
        (12, "Z"),   # December
    ]

    for month, code in quarters:
        # 3rd Friday of expiry month
        import calendar
        first_day = datetime(now.year, month, 1, tzinfo=ET)
        first_weekday = first_day.weekday()  # 0=Mon
        days_to_fri = (4 - first_weekday) % 7
        third_fri = 21 + days_to_fri if days_to_fri != 0 else 15 + 7
        expiry = datetime(now.year, month, third_fri, tzinfo=ET)

        # Use this contract if expiry is still >= 7 days away (pre-roll)
        if expiry >= now + __import__("datetime").timedelta(days=7):
            return f"ES{code}{year_digit}"

    # Wrap to March of next year
    next_year_digit = str(now.year + 1)[-1]
    return f"ESH{next_year_digit}"


def _cme_trade_date():
    """
    v1.7.47 — the CME trade date currently in force. A globex session opens
    at 18:00 ET and carries the NEXT calendar day's trade date, so before
    18:00 the live session is today's and after 18:00 it is tomorrow's.
    Used to select an ES baseline that is STRICTLY BEFORE the running
    session — i.e. the last SETTLED one.
    """
    now = datetime.now(ET)
    d = now.date() + timedelta(days=1) if now.hour >= 18 else now.date()
    return d.strftime("%Y-%m-%d")


def fetch_es_prev_settle():
    """
    v1.7.47 — ES baseline: the most recent es_daily settlement STRICTLY
    BEFORE the running CME trade date.

    Replaces Polygon /prev as the primary source. /prev was returning a bar
    for the session IN PROGRESS (the 6pm-open globex session carries the
    next trade date), so the agent was differencing the live ES price
    against an early partial snapshot of the SAME session rather than
    against the prior settle. On 2026-08-27 that read prev=7730.25 (the
    partial 08-27 bar, a 10-point range twelve hours old) instead of
    7690.00 (the 08-26 settle) — a 40.25-point error that FLIPPED THE SIGN
    of the overnight move: −3.75 reported where +36.50 was true, and a
    ~$2,400 swing in the projected share leg.

    This is the SAME defect class fetch_daily_change() documents for SPY at
    v1.5.20 ("Polygon /prev rolls forward after the close"). The ES path
    never got that fix.

    es_daily.c is the CME 4:00pm ET settlement — verified 2026-08-27
    against an outside quote (Fidelity's baseline for ESU6 was 7690.00,
    matching the 08-26 row to the penny). 4:00pm is also SPY's cash close,
    so both ends of the implied-move subtraction share a frame BY
    CONSTRUCTION, which was the point of Ron's date-matching ruling.

    Returns (settle, date) or (None, None) — fail-soft, caller falls back.
    """
    try:
        cutoff = _cme_trade_date()
        url = (f"{SUPABASE_URL}/es_daily?select=date,c&date=lt.{cutoff}"
               f"&order=date.desc&limit=3")
        rows = requests.get(url, headers=SB_HEADERS, timeout=8).json()
        for row in (rows or []):
            val = float(row.get("c") or 0)
            if val > 0:
                return val, row.get("date")
    except Exception as e:
        print(f"  ES prev settle (es_daily) failed: {e}")
    return None, None


def fetch_es_futures():
    """
    Fetch ES front-month futures price and prior settlement via Massive API.
    Uses same API key as Polygon (shared auth).
    Returns dict with ticker, price, prev, change, change_pct or None.
    """
    ticker = get_es_front_month_ticker()
    try:
        url = f"https://api.massive.com/futures/v1/snapshot?product_code=ES&ticker={ticker}&apiKey={POLYGON_KEY}"
        r = requests.get(url, timeout=10).json()
        results = r.get("results", [])
        if not results:
            print(f"  ES futures: no results for {ticker} from Massive")
            return None

        # Find exact ticker match (response may include spread contracts)
        res = next((x for x in results if x.get("details", {}).get("ticker") == ticker), None)
        if not res:
            print(f"  ES futures: {ticker} not found in Massive results")
            return None

        session    = res.get("session", {})
        last_minute = res.get("last_minute", {})
        last_trade  = res.get("last_trade", {})

        # Prefer last_minute close (most current delayed price), fall back to last_trade, then session close
        price = last_minute.get("close") or last_trade.get("price") or session.get("close")
        if not price:
            print(f"  ES futures: no price for {ticker}")
            return None
        price = float(price)

        # v1.7.47 — PRIMARY: date-matched settle from es_daily (the last
        # SETTLED CME session, strictly before the running trade date).
        prev, prev_date = fetch_es_prev_settle()
        if prev:
            print(f"  ES {ticker}: prev from es_daily settle ({prev_date}): {prev}")

        # v1.7.47 — FALLBACK, now DATE-GUARDED. Polygon /prev returns the bar
        # for the session IN PROGRESS once globex opens at 18:00 ET, which is
        # exactly the bug this release fixes; accepting it unchecked is what
        # produced the 08-27 sign flip. Only taken when its bar predates the
        # running CME trade date.
        # (The prior comment here blamed WEEKENDS for Massive's
        # previous_settlement being 0.0. That was a misdiagnosis: the field
        # reads 0.0 on weekdays too — confirmed Thursday 2026-08-27 — so it
        # is dead always and is not a candidate source.)
        if prev is None:
            try:
                poly_url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev?adjusted=true&apiKey={POLYGON_KEY}"
                pr = requests.get(poly_url, timeout=8).json()
                results_p = pr.get("results", [])
                if results_p:
                    _cand = float(results_p[0].get("c", 0)) or None
                    _bar_t = results_p[0].get("t")
                    _bar_d = (datetime.fromtimestamp(_bar_t / 1000, ET).strftime("%Y-%m-%d")
                              if _bar_t else None)
                    if _cand and _bar_d and _bar_d < _cme_trade_date():
                        prev = _cand
                        print(f"  ES {ticker}: prev from Polygon ({_bar_d}): {prev}")
                    else:
                        print(f"  ES {ticker}: Polygon /prev REJECTED — bar date "
                              f"{_bar_d} is not before CME trade date {_cme_trade_date()}")
            except Exception as pe:
                print(f"  ES prev Polygon fetch failed: {pe}")

        # Last resort: Massive daily aggs — settlement_price when available.
        # v1.7.47 — DATE-GUARDED for the same reason as the Polygon path
        # above: this loop took results[0] unconditionally, and if Massive
        # returns the running session first it reproduces the exact defect
        # this release fixes. session_end_date must predate the running CME
        # trade date.
        if prev is None:
            try:
                aggs_url = (
                    f"https://api.massive.com/futures/v1/aggs/{ticker}"
                    f"?resolution=1session&limit=3&apiKey={POLYGON_KEY}"
                )
                ar = requests.get(aggs_url, timeout=8).json()
                _cut = _cme_trade_date()
                for bar in (ar.get("results") or []):
                    settle = float(bar.get("settlement_price") or 0)
                    close  = float(bar.get("close") or 0)
                    val = settle if settle > 0 else close
                    _sed = (bar.get("session_end_date") or "")[:10]
                    if val > 0 and _sed and _sed < _cut:
                        prev = val
                        print(f"  ES {ticker}: prev from Massive aggs ({_sed}): {prev}")
                        break
            except Exception as ae:
                print(f"  ES prev Massive fallback failed: {ae}")

        if prev is None:
            print(f"  ES {ticker}: NO SETTLED BASELINE — change suppressed "
                  f"(all sources failed the date guard)")

        change     = round(price - prev, 2) if prev else None
        change_pct = round(change / prev * 100, 2) if prev else None

        print(f"  ES {ticker}: price={price} prev={prev} change={change}")
        return {
            "ticker":     ticker,
            "price":      price,
            "prev":       prev,
            "change":     change,
            "change_pct": change_pct,
        }
    except Exception as e:
        print(f"  ES futures fetch error: {e}")
        return None


# ── Commodity futures: Oil (CL) + Gold (GC) — v1.7.39 ──────────────────────
# Ron's S55 spec: the push is his only market view away from the desk, so
# the Markets section carries WTI and Gold on every surface. Same Massive
# snapshot endpoint and price waterfall as ES; front month is DISCOVERED by
# volume among candidate delivery months (no fragile roll-calendar math)
# and cached per process-day. Fail-soft throughout: any miss just drops
# the line — never blocks a push.
_CL_CODES = "FGHJKMNQUVXZ"                # monthly, Jan..Dec
_GC_MONTHS = [2, 4, 6, 8, 10, 12]         # active cycle: G J M Q V Z
_commod_cache = {"date": None, "tickers": {}}

def _commodity_candidates(product):
    """Next 3 plausible front-month tickers for CL or GC."""
    now = datetime.now(ET)
    out = []
    if product == "CL":
        # delivery months: next month onward (spot month expires ~3wks in)
        for k in range(1, 4):
            m = (now.month - 1 + k) % 12 + 1
            y = now.year + (now.month - 1 + k) // 12
            out.append(f"CL{_CL_CODES[m - 1]}{str(y)[-1]}")
    else:  # GC
        cands = []
        m, y = now.month, now.year
        while len(cands) < 3:
            m += 1
            if m > 12:
                m, y = 1, y + 1
            if m in _GC_MONTHS:
                cands.append(f"GC{_CL_CODES[m - 1]}{str(y)[-1]}")
        out = cands
    return out

def _snapshot_future(product, ticker):
    """One Massive snapshot; returns (price, prev, volume) or None. Same
    waterfall as fetch_es_futures: last_minute → last_trade → session close;
    prev from session previous_settlement, else Polygon prev agg."""
    try:
        url = (f"https://api.massive.com/futures/v1/snapshot"
               f"?product_code={product}&ticker={ticker}&apiKey={POLYGON_KEY}")
        r = requests.get(url, timeout=8).json()
        res = next((x for x in (r.get("results") or [])
                    if x.get("details", {}).get("ticker") == ticker), None)
        if not res:
            return None
        session = res.get("session", {})
        price = (res.get("last_minute", {}).get("close")
                 or res.get("last_trade", {}).get("price")
                 or session.get("close"))
        if not price:
            return None
        prev = float(session.get("previous_settlement") or 0) or None
        if prev is None:
            try:
                pr = requests.get(
                    f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev"
                    f"?adjusted=true&apiKey={POLYGON_KEY}", timeout=8).json()
                rp = pr.get("results", [])
                if rp:
                    prev = float(rp[0].get("c", 0)) or None
            except Exception:
                pass
        # v1.7.40 — last-resort tier, mirroring fetch_es_futures: Massive
        # session aggs settlement (the 08-20 SOD rendered Oil/Gold bare —
        # previous_settlement was 0 and the Polygon prev agg missed; ES
        # needed this same third tier for the same reason).
        if prev is None:
            try:
                ar = requests.get(
                    f"https://api.massive.com/futures/v1/aggs/{ticker}"
                    f"?resolution=1session&limit=3&apiKey={POLYGON_KEY}",
                    timeout=8).json()
                for bar in (ar.get("results") or []):
                    settle = float(bar.get("settlement_price") or 0)
                    close  = float(bar.get("close") or 0)
                    val = settle if settle > 0 else close
                    if val > 0 and abs(val - float(price)) > 1e-9:
                        prev = val
                        break
            except Exception:
                pass
        vol = float(session.get("volume") or 0)
        return float(price), prev, vol
    except Exception as e:
        print(f"  {product} snapshot {ticker} failed: {e}")
        return None

def fetch_commodity_futures():
    """Front-month CL and GC. Returns {'CL': {...}, 'GC': {...}} (either may
    be absent). Front month = highest-volume candidate, cached per day."""
    today = datetime.now(ET).date().isoformat()
    if _commod_cache["date"] != today:
        _commod_cache["tickers"] = {}
        for product in ("CL", "GC"):
            best, best_vol = None, -1.0
            for t in _commodity_candidates(product):
                snap = _snapshot_future(product, t)
                if snap and snap[2] > best_vol:
                    best, best_vol = t, snap[2]
            if best:
                _commod_cache["tickers"][product] = best
                print(f"  {product} front month: {best} (vol {best_vol:,.0f})")
        _commod_cache["date"] = today
    out = {}
    for product, ticker in _commod_cache["tickers"].items():
        snap = _snapshot_future(product, ticker)
        if snap:
            price, prev, _ = snap
            pct = round((price - prev) / prev * 100, 2) if prev else None
            out[product] = {"ticker": ticker, "price": price, "prev": prev,
                            "change_pct": pct}
    return out

def commodity_cells():
    """v1.7.40 — ('Oil $87.14 +1.2%' | None, 'Gold $4,527 −0.3%' | None).
    Cells feed _markets_grid so Gold aligns under the 10yr column (Ron's
    S55 grid spec). Change legs render whenever prev resolves."""
    try:
        c = fetch_commodity_futures()
    except Exception as e:
        print(f"  Commodities fetch error: {e}")
        return None, None
    oil = gold = None
    # v1.7.42 — change legs in DOLLARS (Ron's spec off the 08-20 11:30 push),
    # matching the push's P&L convention; VIX stays in points. Derived from
    # price − prev so any resolved prev tier feeds it.
    # v1.7.43 — legs are $-less (the last already carries the $ — Ron's spec)
    # and a leg that ROUNDS to zero is suppressed (the 08-20 'Gold −$0').
    if "CL" in c:
        oil = f"Oil ${c['CL']['price']:.2f}"
        if c["CL"].get("prev"):
            _chg = c['CL']['price'] - c['CL']['prev']
            if round(_chg, 2) != 0:
                oil += f" {_fmt_signed(_chg, decimals=2)}"
    if "GC" in c:
        gold = f"Gold ${c['GC']['price']:,.0f}"
        if c["GC"].get("prev"):
            _chg = c['GC']['price'] - c['GC']['prev']
            if round(_chg) != 0:
                gold += f" {_fmt_signed(_chg, decimals=0)}"
    return oil, gold

def _yield_cell(label, val, idx):
    """v1.7.44 — '10yr 4.70% +3bp'; leg omitted when the change is unknown."""
    if val is None:
        return f"{label} \u2014"
    bp = _yield_day.get(idx)
    return f"{label} {val:.2f}%" + (f" {_fmt_signed(bp, decimals=0)}bp" if bp is not None else "")

def markets_2x2(vix, tnx, tyx=None):
    """v1.7.41/44 — the Markets section body, one composer for every surface.
    Ron's S55 layout: VIX alone (it is the vol regime) / the CURVE paired so
    10s-vs-30s steepening reads at a glance / commodities paired. IV and the
    regime label stay dropped. Cells fail soft throughout: a missing yield
    renders an em dash, missing commodities drop legs or the whole row."""
    _vc = vix_change(vix)
    vix_cell = f"VIX {vix:.1f}" + (f" {_fmt_signed(_vc, decimals=1)}" if _vc is not None else "")
    _oil, _gold = commodity_cells()
    return _markets_grid([
        (vix_cell, None),
        (_yield_cell("10yr", tnx, "TNX"), _yield_cell("30yr", tyx, "TYX") if tyx is not None else None),
        (_oil, _gold),
    ])

def _markets_grid(rows):
    """v1.7.40 — two-column Markets grid (Ron's S55 spec: '10yr and Gold
    line up'). rows = [(left, right|None), ...]; left cells pad to a common
    width so the ' · right' separators align. Best-effort under Pushover's
    proportional font — exact under monospace. Rows with no right cell
    render plain and don't join the padding pool. None-left with a right
    cell demotes the right cell to left (lone Gold reads fine unaligned)."""
    fixed = []
    for left, right in rows:
        if left is None and right is None:
            continue
        if left is None:
            left, right = right, None
        fixed.append((left, right))
    pads = [len(l) for l, r in fixed if r]
    w = max(pads) if pads else 0
    out = []
    for left, right in fixed:
        out.append(f"  {left.ljust(w)} · {right}" if right else f"  {left}")
    return out

# ── SOD fetches ────────────────────────────────────────────────────────────
def fetch_spy_premarket():
    """
    Fetch pre-market SPY price. Uses snapshot extended hours price if available,
    falls back to prior close (reliable on Mondays / no pre-market activity).
    Returns (price, is_live) tuple.
    """
    try:
        r = requests.get(
            f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/SPY?apiKey={POLYGON_KEY}",
            timeout=10
        ).json()
        ticker = r.get("ticker", {})
        # Pre-market price sits in prevDay or lastTrade during extended hours
        premarket = ticker.get("lastTrade", {}).get("p")
        if premarket and float(premarket) > 0:
            return float(premarket), True
        # Day open if market already cracked open
        day_open = ticker.get("day", {}).get("o")
        if day_open and float(day_open) > 0:
            return float(day_open), True
    except Exception:
        pass
    # Fallback — prior close
    try:
        r2 = requests.get(
            f"https://api.polygon.io/v2/aggs/ticker/SPY/prev?adjusted=true&apiKey={POLYGON_KEY}",
            timeout=10
        ).json()
        close = float(r2["results"][0]["c"])
        return close, False
    except Exception:
        return None, False


# ── SOD recap builder ──────────────────────────────────────────────────────
def build_sod_brief(spy_pre, is_live, spy_prev_close, vix, tnx, delta, call_mid,
                    dte, s_prior, gamma=None, theta=None, delta_mkt=None):
    """
    v1.6.1 SOD brief — house style. Overnight block via shared
    build_overnight_block() (ES → implied open → proj P&L → Δ drift →
    projected open signal); trigger block via shared build_trigger_block()
    with projected-at-open values (approx ~). EOD = what moved the market;
    SOD = what will move the market.
    """
    now_dt   = datetime.now(ET)
    now_str  = now_dt.strftime("%-m/%-d/%y")
    now_time = now_dt.strftime("%-I:%M%p ET").replace("AM","am").replace("PM","pm")
    vix_eff  = vix or 18.0

    # Overnight move applies to the prior close (pre-market reference)
    spy_ref = spy_prev_close or spy_pre
    overnight_lines, proj = build_overnight_block(spy_ref, call_mid, vix_eff,
                                                  dte, delta, gamma, theta, s_prior)

    # Trigger block: projected-at-open values when ES projection available,
    # else last-known values from s_prior (no ~ marking)
    if proj:
        sig = proj["signal"]
        hdr_label = sig
        # v1.7.14 — projection-only breach demotes the header to WATCH:
        # every input to proj is a ~ value, and 🔴 is reserved for triggers
        # that fired on close-basis data (s_prior). If the close already
        # breached, the header stays ROLL — the open merely confirms it.
        if sig == "ROLL" and s_prior.get("signal") != "ROLL":
            sig       = "WATCH"
            hdr_label = "WATCH \u00B7 Proj ROLL"
        header, trig_lines = build_trigger_block(
            proj["delta"], proj["decay_pct"], proj["dte"], vix_eff,
            proj["delta_fired"], proj["decay_fired"], proj["dte_fired"],
            bool(s_prior.get("entry_spy")), approx=True,
            entry_spy_basis=s_prior.get("entry_spy_basis"),
            delta_mkt=delta_mkt,  # v1.6.6 — pair display at SOD
            in_grace=s_prior.get("in_grace", False),      # v1.7.6 — grace line only;
            days_held=s_prior.get("days_held"),           # proj flags are raw by design
            grace_days=s_prior.get("grace_days", 3),      # v1.7.12
            breakout=True)   # v1.7.45 — the "= what it takes" sub-lines. SOD
            # carried the trigger VALUES but not the LEVELS, and the levels are
            # the actionable half: "SPY ≈ $773" survives whatever the open does
            # to a projection, where "~0.534" does not. EOD has had these since
            # v1.7.26; SOD is the surface Ron reads before the bell.
    else:
        sig = s_prior["signal"]
        hdr_label = sig
        header, trig_lines = build_trigger_block(
            delta or 0, s_prior["tv_decay"], dte, vix_eff,
            s_prior["delta_fired"], s_prior["decay_fired"], s_prior["dte_fired"],
            bool(s_prior.get("entry_spy")), approx=False,
            entry_spy_basis=s_prior.get("entry_spy_basis"),
            delta_mkt=delta_mkt,  # v1.6.6 — pair display at SOD
            in_grace=s_prior.get("in_grace", False),      # v1.7.6
            days_held=s_prior.get("days_held"),
            grace_days=s_prior.get("grace_days", 3),      # v1.7.12
            delta_raw=s_prior.get("delta_raw"), decay_raw=s_prior.get("decay_raw"),
            dte_raw=s_prior.get("dte_raw"),
            breakout=True)   # v1.7.50 — SOD's NO-PROJECTION path (Ron, S58).
            # v1.7.45 armed breakout on the ES-projection branch only, so on
            # mornings the projection was unavailable SOD silently dropped the
            # sub-lines — a surface that changed shape with data availability
            # rather than with position state. Both SOD branches now render the
            # same block; this one takes no ~ because approx=False here (the
            # values are prior-close actuals, not projections).

    sig_emoji = {"HOLD": "\U0001F7E2", "WATCH": "\U0001F7E1", "ROLL": "\U0001F534", "ACT": "\u26AB"}.get(sig, "\U0001F7E2")

    # v1.7.31 — SOD layout rebuilt on the intraday spine (Ron's S53 spec):
    # ES line ABOVE Position (it drives every projection), then Proj Daily
    # P&L and Proj Position P&L in the intraday format with ES-implied
    # values. All numbers come from proj — the SAME computation the shared
    # overnight block runs for EOD (one definition, two displays). "Proj"
    # prefixes + ~ marks keep the projected frame unmistakable. Old layout
    # retained verbatim as the ES-unavailable fallback.
    exp_us = _exp_mmddyy(POSITION["short_expiry"])
    _sp = POSITION.get("sell_price")
    tnx_str = f"{tnx:.2f}%" if tnx else "\u2014"
    if proj and proj.get("p_call") is not None:
        strike   = POSITION["short_strike"]
        shares   = POSITION["shares"]
        i_spy    = proj["implied_spy"]
        i_move   = proj["implied_move"]
        p_call   = proj["p_call"]
        _cc      = p_call - call_mid if call_mid is not None else 0.0
        _dist    = strike - i_spy                       # compute_state convention: <0 = ITM
        _otm_pct = (_dist / i_spy) * 100
        pnl_bits = []
        if proj.get("shares_leg") is not None:
            pnl_bits.append(f"  SPY ~${i_spy:.2f} {_fmt_signed(i_move, dollars=True, decimals=2)} · {_fmt_signed(proj['shares_leg'], dollars=True)}")
        if proj.get("call_leg") is not None:
            pnl_bits.append(f"  Call ~${p_call:.2f} theo "
                            f"{'+' if _cc >= 0 else '−'}${abs(_cc):.2f} · {_fmt_signed(proj['call_leg'], dollars=True)}")
        if proj.get("shares_leg") is not None and proj.get("call_leg") is not None:
            pnl_bits.append(f"  Net {_fmt_signed(proj['shares_leg'] + proj['call_leg'], dollars=True)}")
        lines = [
            f"{sig_emoji} {hdr_label} \u00B7 SOD {now_str} \u00B7 {now_time}",
            "",
            "Position:",                                            # v1.7.33 — Ron's flow: position first,
            f"  Long {POSITION['shares']} SPY @ ${POSITION['cost_basis']:.2f}",   # then the future for that position,
            f"  Short {POSITION['contracts']} {exp_us} ${POSITION['short_strike']:g}C"  # then what it implies
                + (f" @ ${_sp:.2f}" if _sp else ""),
            "",
            "Overnight:",
            f"  ES {proj['es_price']:,.2f} {_fmt_signed(proj['es_price'] - proj['es_prev'])} ({_fmt_signed(proj['es_pct']*100)}%)",  # v1.7.32 — implied tail dropped (Ron: duplicated by Proj SPY line below)
            "",
            "Proj Daily P&L (ES-implied):",
            *pnl_bits,
            "",
            "Proj Position P&L:",
        ]
        if _sp and _sp > 0:
            # v1.7.46 — PRICE change, not P&L dollars. v1.7.45 made this
            # change on the intraday surface and MISSED this one: the Position
            # P&L row has TWO composers, and the roster was built from the
            # approved items rather than from the row itself. Same convention
            # as intraday — signed by price direction; the dollar legs stay in
            # the Proj Daily P&L block above.
            lines.append(f"  Call ${_sp:.2f} \u2192 ~${p_call:.2f} theo"
                         f" · {_fmt_signed(p_call - _sp, dollars=True, decimals=2)}")
            # v1.7.46 — projected TV leg, mirroring the intraday row. Values
            # come from proj (build_overnight_block already divided them for
            # the projected decay); ~ marks them projected and the % foots to
            # the SOD trigger line's own decay figure.
            _etv, _ptv = proj.get("entry_tv"), proj.get("proj_tv")
            if _etv is not None and _ptv is not None and _etv > 0:
                lines.append(f"    TV ${_etv:.2f} \u2192 ~${_ptv:.2f}"
                             f" · {_fmt_signed(_ptv - _etv, dollars=True, decimals=2)}"
                             f" (~{(_etv - _ptv) / _etv * 100:.1f}%)")
        lines.append(f"  {'ITM' if _dist < 0 else 'OTM'}: ~${abs(_dist):.2f} from Strike "
                     f"({_fmt_signed(-_otm_pct, decimals=1)}%)")
        lines.append(div_push_line(get_next_dividend())[0])  # v1.7.39 — div under OTM (position economics)
        lines += [
            "",
            "Markets:",   # v1.7.39 — Ron's S55 spec
            *markets_2x2(vix_eff, tnx, fetch_tyx()),   # v1.7.41 — shared 2x2; regime/fallback tag dropped with the label
        ]
        lines += [
            "",
            header,
            *trig_lines,
        ]
    else:
        lines = [
            f"{sig_emoji} {hdr_label} \u00B7 SOD {now_str} \u00B7 {now_time}",
            "",
            "Position:",
            f"  Long {POSITION['shares']} SPY @ ${POSITION['cost_basis']:.2f}",
            f"  Short {POSITION['contracts']} {exp_us} ${POSITION['short_strike']:g}C"
                + (f" @ ${_sp:.2f}" if _sp else ""),
            div_push_line(get_next_dividend())[0],   # v1.7.39 — div is position
                # economics; this branch has no Position P&L section, so it
                # rides the Position block (nearest position-owned home)
            "",
            "Overnight:",
            *overnight_lines,
            "",
            "Markets:",   # v1.7.39 — Ron's S55 spec
            *markets_2x2(vix_eff, tnx, fetch_tyx()),   # v1.7.41 — shared 2x2
        ]
        lines += [
            "",
            header,
            *trig_lines,
        ]

    news = fetch_market_brief(max_age_hours=14, mode="ahead")
    if news:
        lines.append("")
        lines.append("Today:")
        for h in news:
            lines.append(f"  {h}")
    elif FINNHUB_KEY or ANTHROPIC_KEY:
        lines.append("")
        lines.append("Today: no recent headlines")
    return "\n".join(lines)


# ── SOD check ──────────────────────────────────────────────────────────────
def sod_brief():
    """
    SOD brief gating:
    - Saturday → always skip
    - Sunday before 18:00 ET → skip
    - Sunday 18:00 ET onwards → OK (pre-Monday overnight)
    - Mon–Fri 09:30–16:20 ET → skip (market open, intraday handles it)
    - Fri 16:20 ET onwards → skip (weekend begins)
    - Mon–Thu 16:20 ET onwards + overnight → OK
    - Mon–Fri before 09:30 ET → OK (pre-market)
    """
    now = datetime.now(ET)
    dow = now.weekday()  # 0=Mon, 4=Fri, 5=Sat, 6=Sun
    hour_min = now.hour * 60 + now.minute

    # Saturday → always skip
    if dow == 5:
        print(f"{now.strftime('%H:%M ET')} — SOD brief skipped (Saturday).")
        return

    # Sunday → only run from 18:00 ET onwards
    if dow == 6 and hour_min < 18 * 60:
        print(f"{now.strftime('%H:%M ET')} — SOD brief skipped (Sunday before 6pm ET).")
        return

    # Friday → only run before market close (before 16:20 ET)
    if dow == 4 and hour_min >= 16 * 60 + 20:
        print(f"{now.strftime('%H:%M ET')} — SOD brief skipped (Friday post-close).")
        return

    # v1.5.18 — Mon–Thu evenings (16:20–24:00 ET) → skip; evening recap covers
    # post-close, SOD resumes at midnight ET. Restores documented v1.5.10
    # behavior — the 00:00/02:00 UTC slots added in v1.5.13 are Sunday-only by
    # intent but fired every weekday evening, doubling the 8pm/10pm ET messages.
    if 0 <= dow <= 3 and hour_min >= 16 * 60 + 20:
        print(f"{now.strftime('%H:%M ET')} — SOD brief skipped (weekday evening — recap covers; SOD resumes midnight ET).")
        return

    # Mon–Fri: skip during market hours (09:30–16:20 ET) — intraday handles those
    if 0 <= dow <= 4 and 9 * 60 + 30 <= hour_min < 16 * 60 + 20:
        print(f"{now.strftime('%H:%M ET')} — SOD brief skipped (market open — intraday active).")
        return

    print(f"{now.strftime('%H:%M ET')} — Running SOD brief...")
    # Refresh params on every SOD brief
    global ROLL_PARAMS, POSITION
    ROLL_PARAMS = load_roll_params()
    POSITION['flat'] = bool(ROLL_PARAMS) and (not ROLL_PARAMS.get('shortStrike') or bool(ROLL_PARAMS.get('flat')))  # v1.7.11 — absence-as-flat; empty params never flat
    if ROLL_PARAMS.get('contracts'): POSITION['contracts']   = int(ROLL_PARAMS['contracts']); POSITION['shares'] = POSITION['contracts'] * 100
    if ROLL_PARAMS.get('costBasis'):    POSITION['cost_basis']   = float(ROLL_PARAMS['costBasis'])   # v1.7.24 — ledger-derived basis, json-sourced; env COST_BASIS = boot fallback only
    if ROLL_PARAMS.get('shortStrike'):  POSITION['short_strike'] = float(ROLL_PARAMS['shortStrike'])
    if ROLL_PARAMS.get('shortExpiry'):  POSITION['short_expiry'] = ROLL_PARAMS['shortExpiry']
    if ROLL_PARAMS.get('coverPrice'):   POSITION['cover_price']  = float(ROLL_PARAMS['coverPrice'])
    if ROLL_PARAMS.get('sellPrice'):    POSITION['sell_price']   = float(ROLL_PARAMS['sellPrice'])
    if POSITION.get('sell_price') and POSITION.get('cover_price'):
        POSITION['premium_sold'] = POSITION['sell_price'] - POSITION['cover_price']
    elif ROLL_PARAMS.get('netPremium'):
        POSITION['premium_sold'] = float(ROLL_PARAMS['netPremium'])

    if POSITION.get('flat'):  # v1.7.10 — no short: compact flat push, nothing else
        send_flat_update("ahead")
        return
    try:
        spy_pre, is_live = fetch_spy_premarket()
        if spy_pre is None:
            print("  Could not fetch SPY price for SOD brief.")
            return

        spy_prev  = fetch_spy_prev_close()
        vix       = fetch_vix()
        tnx       = fetch_tnx()
        ticker    = build_option_ticker(POSITION["short_expiry"], POSITION["short_strike"])
        snapshot  = fetch_option_snapshot(ticker)
        dte       = days_to_expiry(POSITION["short_expiry"])

        call_mid  = snapshot["mid"] if snapshot and snapshot.get("mid", 0) > 0 else None
        delta_mkt = snapshot.get("delta") if snapshot else None
        # v1.6.6 — SOD trigger/projection chain in the SIM'S frame (v1.6.3
        # fixed intraday+EOD; SOD kept market greeks against the sim-frame
        # threshold — internally inconsistent). Polygon delta kept as
        # displayed reference.
        vix_eff   = vix or 18.0
        delta     = sim_frame_delta(spy_pre, POSITION["short_strike"], defense_dte(dte), vix_eff)

        # Use prior close call price if no live snapshot yet
        if not call_mid:
            call_mid = fetch_option_prev_close(ticker) or 0

        s_prior   = score_position(spy_pre, call_mid, vix_eff, dte, delta=delta)

        # v1.6.6 — sim-frame gamma for the Δ-drift projection (same frame as
        # the threshold); MARKET theta retained — it feeds the projected call
        # PRICE (price-space), not a threshold comparison.
        gamma = sim_frame_gamma(spy_pre, POSITION["short_strike"], defense_dte(dte), vix_eff)
        theta = snapshot.get("theta") if snapshot else None
        msg = build_sod_brief(spy_pre, is_live, spy_prev, vix, tnx, delta, call_mid,
                              dte, s_prior, gamma=gamma, theta=theta,
                              delta_mkt=delta_mkt)
        send_notification(msg)
        print("  SOD brief sent.")

    except Exception as e:
        print(f"  SOD brief error: {e}")
        import traceback; traceback.print_exc()
        record_job_failure(e)

# ── EOD fetches — prior day close for P&L calc ────────────────────────────
def fetch_spy_prev_close():
    """Fetch SPY prior day close for day P&L calculation."""
    try:
        r = requests.get(
            f"https://api.polygon.io/v2/aggs/ticker/SPY/prev?adjusted=true&apiKey={POLYGON_KEY}",
            timeout=10
        ).json()
        return float(r["results"][0]["c"])
    except Exception:
        return None


def fetch_daily_change(ticker):
    """
    v1.5.20 — Close-vs-close from the last two daily bars.
    Polygon /prev rolls forward after the close (evening "prev" IS today),
    which zeroed the EOD day-change. Range endpoint + explicit last-2 bars
    is immune to that.
    Returns (today_close, prior_close) or (None, None).
    """
    try:
        to_d   = datetime.now(ET).strftime("%Y-%m-%d")
        from_d = (datetime.now(ET) - timedelta(days=10)).strftime("%Y-%m-%d")
        r = requests.get(
            f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{from_d}/{to_d}"
            f"?adjusted=true&sort=asc&limit=10&apiKey={POLYGON_KEY}",
            timeout=10
        ).json()
        bars = r.get("results") or []
        if len(bars) >= 2:
            return float(bars[-1]["c"]), float(bars[-2]["c"])
        if len(bars) == 1:
            return float(bars[-1]["c"]), None
        return None, None
    except Exception as e:
        print(f"  fetch_daily_change({ticker}) failed: {e}")
        return None, None


def fetch_option_prev_close(ticker):
    """Fetch short call prior day close for day P&L calculation."""
    try:
        r = requests.get(
            f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev?adjusted=true&apiKey={POLYGON_KEY}",
            timeout=10
        ).json()
        results = r.get("results")
        if results:
            return float(results[0]["c"])
        return None
    except Exception:
        return None


# ── EOD recap builder ──────────────────────────────────────────────────────
# ── Shared message blocks (v1.6.1) ─────────────────────────────────────────
def _fmt_signed(val, dollars=False, decimals=None):
    # v1.7.16 — decimals honored in BOTH branches via sentinel defaults:
    # dollars → 0 (P&L callers unchanged), plain → 2. The old dollars branch
    # hardcoded ,.0f and silently ignored decimals — the +$3 / +0.26 defects.
    if val is None: return "—"
    sign = "+" if val >= 0 else "−"
    if dollars:
        d = 0 if decimals is None else decimals
        return f"{sign}${abs(val):,.{d}f}"
    d = 2 if decimals is None else decimals
    return f"{sign}{abs(val):.{d}f}"


def _exp_mmddyy(iso):
    # v1.7.16 — single expiry renderer for SOD + intraday position lines
    return f"{iso[5:7]}/{iso[8:10]}/{iso[2:4]}" if iso and len(iso) == 10 else (iso or "")


def _shares_flag():
    # v1.7.24 — integrity tripwire: ledger sharesHeld (json, FIFO-walk
    # derived at statement import) vs contracts×100. Mismatch = partial
    # assignment or stale json — flag loudly rather than display a wrong
    # share count silently. Empty string when absent or matching.
    try:
        _sh = ROLL_PARAMS.get('sharesHeld')
        if _sh and int(_sh) != POSITION.get('shares', 0):
            return f"  ⚠ ledger shares {int(_sh)} ≠ contracts×100 {POSITION.get('shares', 0)}"
    except (TypeError, ValueError):
        pass
    return ""


def build_overnight_block(spy_ref, call_mid, vix, dte, delta, gamma, theta, s):
    """
    Shared ES → implied open → projections block. Used by EOD recap and
    SOD brief so the two can never diverge (v1.6.1).
    spy_ref: SPY price the overnight move applies to (EOD: today's close;
    SOD: prior close). Returns (lines, proj) — proj is None when ES is
    unavailable, else a dict of projected values and fired flags.
    """
    pos       = POSITION
    rp        = ROLL_PARAMS
    strike    = pos["short_strike"]
    shares    = pos["shares"]
    delta_val = delta or 0
    decay_pct = s["tv_decay"]
    fmt_signed = _fmt_signed

    lines = []
    proj  = None
    es    = None
    try:
        es = fetch_es_futures()
    except Exception as ee:
        print(f"  ES fetch failed: {ee}")

    if es and es.get("price") and es.get("prev") and spy_ref:
        es_pct       = (es["price"] - es["prev"]) / es["prev"]
        implied_spy  = spy_ref * (1 + es_pct)
        implied_move = implied_spy - spy_ref

        lines.append(f"  ES {es['price']:,.2f} {fmt_signed(es['price'] - es['prev'])} ({fmt_signed(es_pct*100)}%)")
        lines.append(f"  Implied SPY open ~${implied_spy:.2f} ({fmt_signed(implied_move, dollars=True, decimals=2)})")

        # v1.7.20 — Implied Call parenthetical is MARKET frame (actual price
        # change vs prior mark), matching the Implied SPY line directly above
        # (Ron's S48 ruling, reversing v1.7.14's short-frame parenthetical:
        # adjacent lines in the market-inputs group carry ONE frame; the
        # short-position minus lives in the P&L breakout below, where it
        # belongs). Projection math unchanged (v1.7.13).
        p_call = call_mid + delta_val * implied_move + (theta or 0)  # theta ≤ 0: one night's decay
        if call_mid is not None:
            _cc = p_call - call_mid      # market frame: call up = positive
            lines.append(f"  Implied Call open ~${p_call:.2f} "
                         f"({'+' if _cc >= 0 else '−'}${abs(_cc):.2f})")

        if delta is not None and shares > 0 and call_mid is not None:
            # v1.7.20 — ONE DEFINITION: P&L legs derived FROM p_call, the same
            # projection the Implied Call line displays; the old parallel
            # formula (1−Δ)·move·shares RETIRED (it silently dropped theta, so
            # Net could not foot with the call line's own projection — the
            # v1.7.16 two-formatter class, fifth sighting). Net is now
            # theta-inclusive by construction; every number in this block
            # foots against every other (S48 ruling).
            shares_leg = implied_move * shares
            call_leg   = -(p_call - call_mid) * shares   # short frame
            proj_pnl   = shares_leg + call_leg
            lines.append(f"  Proj P&L: Shares {fmt_signed(shares_leg, dollars=True)}"
                         f" · Call {fmt_signed(call_leg, dollars=True)}"
                         f" → Net {fmt_signed(proj_pnl, dollars=True)}")
        elif delta is not None and shares > 0:
            # call_mid unavailable: directional share-leg only, labeled honestly
            proj_pnl = implied_move * shares
            lines.append(f"  Proj P&L: Shares {fmt_signed(proj_pnl, dollars=True)} (call mark unavailable)")

        # Δ drift via gamma (linear; ignores overnight IV change — projection only)
        proj_delta = None
        if delta is not None and gamma:
            proj_delta = min(1.0, max(0.0, delta_val + gamma * implied_move))
            lines.append(f"  Delta drift: {delta_val:.3f} → ~{proj_delta:.3f}")

        # Projected open signal: run projected values through live thresholds
        p_delta = proj_delta if proj_delta is not None else delta_val
        p_dte   = dte   # v1.7.52 — was max(0, dte - 1) (D-B, S59). dte is
                        # computed at runtime, THIS morning; SOD runs pre-open
                        # and projects to TODAY's open, so the day was being
                        # subtracted twice. Stacked on D-A, SOD read 175 when
                        # truth was 177 — and because the two surfaces used
                        # different formulas, DTE appeared to RISE from 175 at
                        # 8:20am to 176 at 2:00pm on 2026-09-02, which is what
                        # surfaced both defects.
        p_decay_pct = decay_pct
        entry_spy   = s.get("entry_spy")
        sell_price  = pos.get("sell_price") or 0
        entry_tv = proj_tv = None                      # v1.7.13 — for display
        if entry_spy and sell_price > 0:
            entry_tv = max(0.0, sell_price - max(0.0, entry_spy - strike))
            proj_tv  = max(0.0, p_call - max(0.0, implied_spy - strike))
            if entry_tv > 0:
                p_decay_pct = (entry_tv - proj_tv) / entry_tv * 100

        # v1.7.13 — TV frame line (display only; makes the projected-decay
        # frame auditable in-message). v1.7.14: call mark line moved up into
        # the market-inputs group as "Implied Call open".
        if entry_tv is not None and entry_tv > 0 and proj_tv is not None:
            _intr = max(0.0, implied_spy - strike)
            _intr_note = f" (intrinsic ~${_intr:.2f})" if _intr > 0 else ""
            lines.append(f"  TV: ${entry_tv:.2f} entry → proj ~${proj_tv:.2f} (decay ~{p_decay_pct:.1f}%){_intr_note}")  # v1.7.18 — derived %, same units+precision as the trigger line (Ron: TV was dollars here, a % there)

        # v1.7.21 — Sell line: raw short economics (fill → projected mark),
        # the PRICE frame beside the trigger's entry-TV frame (Ron's S51
        # spec). Positive = short ahead (cover < collected). Uses p_call —
        # the same projection the Implied Call and P&L lines foot to.
        if sell_price > 0 and call_mid is not None and shares > 0:
            _sell_pnl = sell_price - p_call
            lines.append(f"  Sell: ${sell_price:.2f} → ~${p_call:.2f} · "
                         f"P&L {fmt_signed(_sell_pnl, dollars=True, decimals=2)}/sh "
                         f"({fmt_signed(_sell_pnl * shares, dollars=True)})")

        p_delta_f = p_delta >= rp["deltaThreshold"]
        p_decay_f = p_decay_pct >= rp["decayTrigger"] * 100
        p_dte_f   = p_dte <= rp["dteFloor"]
        if p_delta_f or p_decay_f or p_dte_f:
            p_sig   = "ROLL"   # v1.7.9 — gate excised; proj dict keeps the
            p_emoji = "🟡"     # raw verdict, display is Proj-marked (v1.7.14)
            fired = []
            if p_delta_f: fired.append(f"Delta ~{p_delta:.3f} ≥ {rp['deltaThreshold']:.3f}")
            if p_decay_f: fired.append(f"decay ~{p_decay_pct:.1f}% ≥ {rp['decayTrigger']*100:.0f}%")  # v1.7.18
            if p_dte_f:   fired.append(f"{p_dte}d ≤ {rp['dteFloor']}d")
            grace_note = ""
            if s.get("in_grace") and s.get("days_held") is not None:  # v1.7.8 — Ron's ruling
                grace_note = f" · ⏸ grace day {s['days_held']}/{s.get('grace_days', 3)} — would be held"
            lines.append(f"  Proj open: {p_emoji} Proj {p_sig} ({' · '.join(fired)}){grace_note}")
        else:
            p_sig = "HOLD"
            lines.append(f"  Proj open: 🟢 HOLD (decay ~{p_decay_pct:.1f}% · {p_dte}d)")  # v1.7.18 — one decimal

        proj = {
            "signal": p_sig, "implied_spy": implied_spy,
            "delta": p_delta, "dte": p_dte, "decay_pct": p_decay_pct,
            "delta_fired": p_delta_f, "decay_fired": p_decay_f,
            "dte_fired": p_dte_f,
            # v1.7.38 — open_line export PULLED (zero consumers after the
            # evening Proj-open verdict was removed; not left dormant).
            # v1.7.31 — projection internals exported for the SOD renderer's
            # intraday-style layout. ONE definition: these are the very values
            # the lines above printed — the SOD builder re-displays, never
            # recomputes (v1.7.16 two-formatter class stays dead).
            "es_price": es["price"], "es_prev": es["prev"], "es_pct": es_pct,
            "implied_move": implied_move,
            "p_call": p_call if call_mid is not None else None,
            "shares_leg": (implied_move * shares) if (delta is not None and shares > 0) else None,
            "call_leg": (-(p_call - call_mid) * shares) if (delta is not None and shares > 0 and call_mid is not None) else None,
            # v1.7.46 — the two TV legs the projected decay divided, exported
            # under the same rule as the block above: the SOD builder
            # re-displays, never recomputes. Both None on the no-entry_spy
            # branch, where the SOD TV row must not render.
            "entry_tv": entry_tv, "proj_tv": proj_tv,
        }
    else:
        lines.append("  ES data unavailable")
    return lines, proj


def _stop_ticket(rp, dte, vix, delta_val, delta_thres, delta_fired, delta_held):
    """v1.7.43 — pre-computed stop ticket (Ron's S55 spec): when Δsim is at
    the pre-alarm line (≥ 0.70) or fired, render the buy-to-close order so
    the alarm arrives with the ticket already written — respond, don't
    compute. Cover priced by compute_theo (the certified IV waterfall) at
    the FIRE SPOT (S* where Δsim = threshold, bisected in the defH frame),
    the same S* the Sunday overshoot study prices; if already past the
    line, the ticket is the conservative floor. Fail-soft: any missing
    input → no line."""
    try:
        if delta_held or delta_val is None or delta_val < PRE_ALARM_DELTA:
            return None
        K = float(rp.get("shortStrike") or 0)
        n = int(rp.get("contracts") or 0)
        if K <= 0 or n <= 0:
            return None
        d_eff = defense_dte(dte)
        lo, hi = K * 0.9, K * 1.4
        for _ in range(40):
            m = (lo + hi) / 2
            if sim_frame_delta(m, K, d_eff, vix) < delta_thres:
                lo = m
            else:
                hi = m
        fire_spot = (lo + hi) / 2
        t = compute_theo(fire_spot, 0.0, dte)
        if not t:
            return None
        tag = "FIRED — " if delta_fired else ""
        return (f"      = {tag}Stop ticket: BTC {n} @ ~${t['now']:.2f} lmt"
                f" (SPY ≥ ${fire_spot:.0f})")
    except Exception:
        return None


def _decay_fire_spots(K, dte, fire_tv):
    """v1.7.51 — the TWO SPY levels at which time value reaches fire_tv,
    holding today's dte and the IV compute_theo is already using.

    TV(S) = theo(S) − max(0, S − K) is single-peaked near the strike, so any
    target below the peak has two roots: one BELOW (the option simply loses
    value) and one ABOVE (extrinsic collapses as it goes ITM). The decay
    trigger fires at either. Reporting only the lower root would read as
    "this fires on a sell-off", which is false on exactly the days that
    matter — the sim's co-fire rows (delta and decay together) are the
    rally case, and cycle 9 rolled down on a decay fire during a rally.

    Frame: SPOT, today. Deliberately NOT the defH frame the delta sub-line
    uses — the decay test runs on the current mark, the delta test runs
    projected. Both lines are stamped so the pair cannot be read as one
    coordinate system (S59, Ron).

    Returns (down, up); either may be None if that root sits outside the
    bracket or IV is unavailable. Takes no mark: the IV provenance already
    lives on the block heading (v1.7.35 trig_src).
    """
    def tv(S):
        t = compute_theo(S, 0.0, dte)
        return None if not t else t["now"] - max(0.0, S - K)

    peak = tv(K)                      # TV maximum sits at/near the strike
    if peak is None or peak <= fire_tv:
        return (None, None)           # already at or past target at the peak

    down = up = None
    lo, hi = K * 0.70, K              # TV rises with S here
    f = tv(lo)
    if f is not None and f < fire_tv:
        for _ in range(60):
            m = (lo + hi) / 2
            v = tv(m)
            if v is None:
                lo = hi = 0
                break
            if v < fire_tv:
                lo = m
            else:
                hi = m
        down = (lo + hi) / 2 or None

    lo, hi = K, K * 1.40              # TV falls with S here
    f = tv(hi)
    if f is not None and f < fire_tv:
        for _ in range(60):
            m = (lo + hi) / 2
            v = tv(m)
            if v is None:
                lo = hi = 0
                break
            if v > fire_tv:
                lo = m
            else:
                hi = m
        up = (lo + hi) / 2 or None

    return (down, up)


def build_trigger_block(delta_val, decay_pct, dte, vix, delta_fired, decay_fired,
                        dte_fired, has_entry_spy, approx=False, entry_spy_basis=None,
                        delta_mkt=None, in_grace=False, days_held=None,
                        delta_raw=None, decay_raw=None, dte_raw=None,
                        grace_days=3, arrow="", breakout=False):  # v1.7.12 — json-sourced; default keeps old callers valid. v1.7.18 — arrow: Δ trend marker (SMS caller), default empty keeps EOD/SOD callers valid. v1.7.26 — breakout: EOD-only "=" sub-lines, default False keeps SMS/SOD byte-identical
    """
    Shared proximity-sorted ☑/☐ trigger block (v1.6.1). Used by EOD recap
    and SOD brief. approx=True marks Δ/decay values as projections (~) —
    the SOD brief shows projected-at-open values, not last-close values.
    v1.7.6: in_grace/days_held + raw pre-mask conditions render met-but-held
    lines (⏸) and an always-visible grace line during the window. Raw args
    default to the fired args when omitted (legacy-caller safe: no
    suppression can be inferred without raws).
    Returns (header, lines).  # v1.7.9 — gate line removed
    """
    rp          = ROLL_PARAMS
    delta_thres = rp["deltaThreshold"]
    decay_need  = rp["decayTrigger"] * 100
    dte_floor   = rp["dteFloor"]
    decay_label = ("TV Decay" if entry_spy_basis == "dm"
                   else "TV Decay~" if entry_spy_basis == "params"
                   else ("TV Decay" if has_entry_spy else "Decay"))  # v1.6.4 — basis-aware; legacy binary as fallback
    a           = "~" if approx else ""

    # v1.7.6 — grace-held = condition met raw but masked by the grace window
    delta_raw = delta_fired if delta_raw is None else delta_raw
    decay_raw = decay_fired if decay_raw is None else decay_raw
    dte_raw   = dte_fired   if dte_raw   is None else dte_raw
    delta_held = in_grace and delta_raw and not delta_fired
    decay_held = in_grace and decay_raw and not decay_fired
    dte_held   = in_grace and dte_raw   and not dte_fired

    # v1.7.26 — breakout sub-lines (EOD opt-in): distance in native units.
    # Computed only for unfired/unheld triggers; any missing input → no line.
    sub_delta = sub_decay = sub_dte = None
    if breakout:
        try:
            sell_p = float(rp.get("sellPrice") or 0)
            if sell_p > 0 and not (decay_fired or decay_held):
                # v1.7.52 — the "= Call $x.xx (~$y/sh from here)" line is
                # RETIRED (S59, Ron); the decay trigger now names SPY levels
                # only, matching its two neighbours. fire_mark survives as the
                # TARGET of the solve, not as rendered output. `bleed` and this
                # block's use of the ~ marker are pulled entirely rather than
                # left dormant (retired-never-dormant).
                #
                # TARGET: sellPrice × (1 − decayTrigger). Equals the trigger's
                # true entry-TV target only while the ENTRY was OTM (entry
                # intrinsic 0) — the standing case. Carried forward from
                # v1.7.50 unchanged; correcting it is trigger arithmetic, not
                # display, and is queued rather than done here.
                #
                # v1.7.51 — SPY-equivalent, BOTH SIDES. TV(S) is single-peaked
                # near the strike, so a target below the peak has a root on
                # each side: below, the option loses value; above, extrinsic
                # collapses as it goes ITM. The trigger fires at either, so a
                # lone downside level would read as "fires on a sell-off" —
                # false on exactly the days that matter. Recomputed every push
                # and allowed to drift silently as theta pulls both levels
                # toward the strike; the number is always current.
                fire_mark = sell_p * (1 - decay_need / 100.0)
                _dn, _up = _decay_fire_spots(
                    float(rp.get("shortStrike") or 0), dte, fire_mark)
                if _dn or _up:
                    _lv = " / ".join(x for x in (
                        f"\u25bc${_dn:.0f}" if _dn else None,
                        f"\u25b2${_up:.0f}" if _up else None) if x)
                    sub_decay = f"      = SPY \u2248 {_lv} (spot frame)"
        except Exception:
            pass
        try:
            K = float(rp.get("shortStrike") or 0)
            if K > 0 and not (delta_fired or delta_held) and delta_val < delta_thres:
                d_eff = defense_dte(dte)
                lo, hi = K * 0.9, K * 1.4   # fire spot is above here for a short call
                for _ in range(40):
                    m = (lo + hi) / 2
                    if sim_frame_delta(m, K, d_eff, vix) < delta_thres:
                        lo = m
                    else:
                        hi = m
                fire_spot = (lo + hi) / 2
                sub_delta = f"      = SPY ≈ \u25b2${fire_spot:.0f} (defH-{d_eff} frame)"  # v1.7.53 — ▲ for visual parity with the decay pair (S59, Ron). Unconditional: delta rises with spot, so this level is always ABOVE it. The marker is the direction spot must travel, not a trend arrow — `arrow` (v1.6.x) rides on the delta VALUE in the trigger line above.
        except Exception:
            pass

        try:
            expiry = rp.get("shortExpiry")
            if expiry and not (dte_fired or dte_held):
                fire_d = (datetime.strptime(expiry, "%Y-%m-%d")
                          - timedelta(days=int(dte_floor))).strftime("%m/%d/%y")
                sub_dte = f"      = {fire_d} if nothing fires first"
        except Exception:
            pass

    # v1.7.43 — stop ticket: ALL surfaces (not breakout-gated); renders only
    # at pre-alarm/fired, so the quiet-state pushes are byte-identical.
    _ticket_line = _stop_ticket(rp, dte, vix, delta_val, delta_thres,
                                delta_fired, delta_held)
    if _ticket_line:
        sub_delta = (sub_delta + "\n" + _ticket_line) if sub_delta else _ticket_line

    trig_items = [
        (decay_fired, decay_held,
         max(0.0, (decay_need - decay_pct) / decay_need) if decay_need > 0 else 1.0,
         (f"{decay_label} {a}{decay_pct:.1f}% ≥ {decay_need:.0f}%" if (decay_fired or decay_held)
          else f"{decay_label} {a}{decay_pct:.1f}% (need ≥{decay_need:.0f}%)")  # v1.7.18 — one decimal: same units+precision as the Overnight TV frame (Ron caught −1% proj vs −2% trigger, both ≈−1.5)
         + (" (grace)" if decay_held else ""),
         sub_decay),
        (delta_fired, delta_held,
         max(0.0, (delta_thres - delta_val) / delta_thres) if delta_thres > 0 else 1.0,
         (f"Delta {a}{delta_val:.3f}{arrow} ≥ {delta_thres:.3f}" if (delta_fired or delta_held)
          else f"Delta {a}{delta_val:.3f}{arrow} (need {delta_thres:.3f})")
         # v1.7.45 — the mkt leg is SUPPRESSED under approx. proj["delta"] is
         # ES-projected; delta_mkt is the PRIOR CLOSE's market delta and is
         # not projectable (no projected market delta is computed anywhere).
         # Rendering one projected and one actual value side by side invited a
         # comparison that cannot validly be made: on 08-25 SOD they showed
         # ~0.534 vs 0.570 and the 0.036 gap was mostly frame, not
         # information. EOD/intraday are unaffected — both legs are actuals
         # there and the pair display (v1.6.6) stands.
         + (f" · mkt {delta_mkt:.3f}" if (delta_mkt is not None and not approx) else "")
         + (" (grace)" if delta_held else ""),
         sub_delta),
        (dte_fired, dte_held,
         max(0.0, (dte - dte_floor) / dte) if dte > 0 else 0.0,
         (f"DTE {dte}d ≤ {dte_floor}d" if (dte_fired or dte_held)
          else f"DTE {dte}d (need ≤{dte_floor}d)")
         + (" (grace)" if dte_held else ""),
         sub_dte),
    ]
    # v1.7.53 — FIXED DISPLAY ORDER: decay, delta, DTE (S59, Ron). The
    # proximity sort is retired from the LIST; the header still names the
    # closest trigger and its gap, so no information is lost and the layout
    # stops moving between pushes. Decay leads because it is the harvest
    # trigger — the one we are trying to hit.
    # FIRED/HELD STILL FLOAT TO THE TOP. That is load-bearing: it is what
    # puts the firing trigger first on a ROLL push. Python's sort is stable,
    # so the constructed order above survives inside each group.
    # RANKING ARITHMETIC UNTOUCHED. t[2] is still fraction-of-threshold and
    # `nearest` below is still min() over the whole list, so it does not
    # depend on display order (the S58 audit of "closest" stands).
    trig_items.sort(key=lambda t: not (t[0] or t[1]))

    if delta_fired or decay_fired or dte_fired:
        header = "Triggers (FIRED):"
    elif delta_held or decay_held or dte_held:
        header = "Triggers (GRACE-HELD):"   # v1.7.6 — met raw, masked by grace
    else:
        nearest = min(trig_items, key=lambda t: t[2])
        n_line  = nearest[3]
        if n_line.startswith("Delta"):   # v1.7.29 — consumer site updated with the label (roster miss #2, caught by smoke)
            gap_str = f"Delta, {max(0.0, delta_thres - delta_val):.3f} away"
        elif "Decay" in n_line:
            gap_str = f"decay, {max(0.0, decay_need - decay_pct):.1f}pts away"  # v1.7.18 — one decimal, matches trigger line
        else:
            gap_str = f"DTE, {max(0, dte - dte_floor)}d away"
        header = f"Triggers (closest: {gap_str}):"

    lines = []
    for fired, held, _, line, sub in trig_items:
        lines.append(f"  {'☑' if fired else '⏸' if held else '☐'} {line}")
        if sub:                                                  # v1.7.26
            lines.append(sub)

    if in_grace and days_held is not None:                       # v1.7.6
        lines.append(f"  ⏸ Grace: day {days_held}/{grace_days} — triggers suspended")  # v1.7.12

    # v1.7.9 — VIX gate line removed (gate excised)
    return header, lines


def build_eod_recap(spy, call_mid, vix, dte, delta, iv, s, tnx, gamma=None, theta=None, delta_mkt=None,
                    include_overnight=True):
    # v1.6.5 — include_overnight=False for the 4:20pm slot: ES at 4:20 is the
    # closed session (prev-close basis = today's move), so the implied-open
    # projection double-counted the day. Evening slots (6:20pm+) pass True.
    pos       = POSITION
    shares    = pos["shares"]
    contracts = pos["contracts"]
    strike    = pos["short_strike"]
    rp        = ROLL_PARAMS
    signal    = s["signal"]
    # v1.7.41 — regime var pulled (zero consumers after the 2x2; not dormant)
    decay_pct = s["tv_decay"]
    delta_val = delta or 0

    now_str   = datetime.now(ET).strftime("%-m/%-d/%y")
    now_time  = datetime.now(ET).strftime("%-I:%M%p ET").replace("AM","am").replace("PM","pm")
    sig_emoji = {"HOLD": "🟢", "WATCH": "🟡", "ROLL": "🔴", "ACT": "⚫"}.get(signal, "🟢")

    # v1.7.16 — this was a full LOCAL DUPLICATE of _fmt_signed with the same
    # broken dollars branch, shadowing the module fix: the reason the missing-$
    # class kept recurring. One formatter, one definition (alias like the
    # overnight block's, v1.7.13 precedent).
    fmt_signed = _fmt_signed

    # ── Position / Daily P&L / Position P&L — v1.7.22 three-section reshape
    # (Ron's S51 spec, same shape as intraday). Close-vs-close from daily
    # bars retained; VIX/IV and 10yr stay in Position. Call leg keeps the
    # short-frame sign in P&L only (S48 one-frame-per-group ruling).
    ticker = build_option_ticker(pos["short_expiry"], strike)
    spy_close,  spy_prior  = fetch_daily_change("SPY")
    call_close, call_prior = fetch_daily_change(ticker)

    # Prefer the daily bar close for the recap; fall back to live values
    spy_disp  = spy_close  if spy_close  else spy
    call_disp = call_close if call_close else call_mid

    spy_chg  = (spy_disp - spy_prior)   if spy_prior  else None
    call_chg = (call_disp - call_prior) if call_prior else None

    spy_pct_str = f" ({fmt_signed(spy_chg / spy_prior * 100, decimals=2)}%)" if (spy_chg is not None and spy_prior) else ""

    # v1.7.41 — iv_str pulled (zero consumers after the 2x2 dropped IV; not dormant)
    _sp = pos.get("sell_price")
    pos_block = [
        f"  Long {shares} SPY @ ${pos['cost_basis']:.2f}",
        f"  Short {pos.get('contracts', pos.get('shares', 0) // 100)} {_exp_mmddyy(pos.get('short_expiry',''))} "
        f"${strike:g}C" + (f" @ ${_sp:.2f}" if _sp else ""),
    ]

    daily_lines = []
    _spy_row = f"  SPY ${spy_disp:.2f}"
    if spy_chg is not None:
        _spy_row += (f" {fmt_signed(spy_chg, dollars=True, decimals=2)}{spy_pct_str}"
                     f" · {fmt_signed(spy_chg * shares, dollars=True)}")
    daily_lines.append(_spy_row)
    if call_disp:
        _call_row = f"  Call ${call_disp:.2f}"
        if call_chg is not None:
            _call_row += (f" {fmt_signed(call_chg, dollars=True, decimals=2)}"
                          f" · {fmt_signed(-call_chg * 100 * contracts, dollars=True)}")
        daily_lines.append(_call_row)

    ppnl_lines = []
    if _sp and call_disp:
        ppnl_lines.append(f"  Call ${_sp:.2f} → ${call_disp:.2f}"
                          f" · {fmt_signed((_sp - call_disp) * shares, dollars=True)}")
    # v1.7.23 — close-basis moneyness (the distance overnight projections
    # key off); always rendered. Same convention as intraday (v1.7.13).
    _dist_c = strike - spy_disp
    _otm_c  = (_dist_c / spy_disp) * 100
    ppnl_lines.append(f"  {'ITM' if _dist_c < 0 else 'OTM'}: ${abs(_dist_c):.2f} from Strike "
                      f"({fmt_signed(-_otm_c, decimals=1)}%)")

    # ── Overnight block: shared helper (v1.6.1; conditional v1.6.5) ────────
    overnight_lines = None
    _proj = None  # v1.7.37 — defined on every path; the spine branch keys on it
    if include_overnight:
        overnight_lines, _proj = build_overnight_block(spy_disp, call_mid, vix, dte,
                                                       delta, gamma, theta, s)

    # ── Consolidated Triggers block: shared helper (v1.6.1) ────────────────
    header, trig_lines = build_trigger_block(
        delta_val, decay_pct, dte, vix,
        s["delta_fired"], s["decay_fired"], s["dte_fired"],
        bool(s.get("entry_spy")), approx=False,
        entry_spy_basis=s.get("entry_spy_basis"),   # v1.6.5 — tilde fix (7/21 cert defect)
        delta_mkt=delta_mkt,                        # v1.6.5 — pair-collection continuity at EOD
        in_grace=s.get("in_grace", False),          # v1.7.6
        days_held=s.get("days_held"),
        grace_days=s.get("grace_days", 3),          # v1.7.12
        breakout=True,                              # v1.7.26 — EOD-only sub-lines
        delta_raw=s.get("delta_raw"), decay_raw=s.get("decay_raw"),
        dte_raw=s.get("dte_raw"))

    # v1.7.19 — shared div composer, always-on (quiet form on this surface)
    # v1.7.39 — relocated: renders inside Position P&L under the ITM/OTM row
    # (Ron's S55 spec — the div is position economics: a pending cash event
    # on the share leg and the early-assignment clock on the short call, so
    # it lives next to the moneyness line that gates that risk).
    exdiv_str = div_push_line(get_next_dividend())[0]

    lines = [
        f"{sig_emoji} {signal} · EOD {now_str} · {now_time}",
        "",
        "Position:",
        *pos_block,
        "",
        "Daily P&L:",
        *daily_lines,
    ]
    lines += [
        "",
        "Position P&L:",   # v1.7.23 — OTM row always present, section always renders
        *ppnl_lines,
        exdiv_str,         # v1.7.39 — div under the ITM/OTM row (position economics)
    ]
    if overnight_lines is not None and _proj is not None:
        # v1.7.38 — evening recap is a DAY RECAP (Ron's S55 tense doctrine:
        # EOD = what happened · SOD = what's ahead · intraday = what's moving
        # now). The v1.7.37 Proj Daily/Position P&L blocks and Proj-open
        # verdict are PULLED — an evening ES-implied open has the whole
        # overnight session to go stale, and the SOD re-derives everything
        # from fresh 6:20am inputs before any of it is actionable. The ES
        # line stays as the early-tone TRAILER (SOD leads with ES; EOD
        # trails with it — Ron's spec), re-displayed from _proj.
        # v1.7.39 — Markets header over the context group (Ron's S55 spec);
        # Oil/Gold line joins it; Div moved out (see Position P&L above).
        lines += [
            "",
            "Overnight:",
            f"  ES {_proj['es_price']:,.2f} {fmt_signed(_proj['es_price'] - _proj['es_prev'])} ({fmt_signed(_proj['es_pct']*100)}%)",
            "",
            "Markets:",
            *markets_2x2(vix, tnx, fetch_tyx()),   # v1.7.41 — shared 2x2; IV + regime dropped (Ron's spec)
        ]
        lines += [
            "",
            header, *trig_lines,
        ]
    else:
        # ES-unavailable evening or the 4:20 slot — v1.7.22 layout, with the
        # v1.7.39 Markets header + Oil/Gold applied here too (all-surface spec).
        if overnight_lines is not None:
            lines += ["", "Overnight:", *overnight_lines]
        lines += [
            "",
            "Markets:",
            *markets_2x2(vix, tnx, fetch_tyx()),   # v1.7.41 — shared 2x2; IV + regime dropped (Ron's spec)
        ]
        lines += [
            "",
            header, *trig_lines,
        ]
    _facts = None  # v1.7.30 — tape-grounding from builder's own display vars (same numbers the push shows)
    if spy_chg is not None and spy_prior:
        _facts = (f"SPY closed ${spy_disp:.2f}, {fmt_signed(spy_chg, dollars=True, decimals=2)} "
                  f"({fmt_signed(spy_chg / spy_prior * 100, decimals=2)}%) on the day · VIX {vix:.1f}")
    news = fetch_market_brief(max_age_hours=10, mode="recap", facts=_facts)
    if news:
        lines.append("")
        lines.append("News:")
        for h in news:
            lines.append(f"  {h}")
    elif FINNHUB_KEY or ANTHROPIC_KEY:
        lines.append("")
        lines.append("News: no recent headlines")
    return "\n".join(lines)


# ── EOD check ──────────────────────────────────────────────────────────────
def eod_recap(include_overnight=False):
    # v1.6.5 — default False: the scheduled 4:20pm slot has no live ES.
    # evening_recap() passes True (ES live from ~6:10pm, first slot 6:20).
    now = datetime.now(ET)
    if now.weekday() >= 5:
        print(f"{now.strftime('%H:%M ET')} — EOD recap skipped (weekend).")
        return

    print(f"{now.strftime('%H:%M ET')} — Running EOD recap...")
    # Refresh params on every EOD recap
    global ROLL_PARAMS, POSITION
    ROLL_PARAMS = load_roll_params()
    POSITION['flat'] = bool(ROLL_PARAMS) and (not ROLL_PARAMS.get('shortStrike') or bool(ROLL_PARAMS.get('flat')))  # v1.7.11 — absence-as-flat; empty params never flat
    if ROLL_PARAMS.get('contracts'): POSITION['contracts']   = int(ROLL_PARAMS['contracts']); POSITION['shares'] = POSITION['contracts'] * 100
    if ROLL_PARAMS.get('costBasis'):    POSITION['cost_basis']   = float(ROLL_PARAMS['costBasis'])   # v1.7.24 — ledger-derived basis, json-sourced; env COST_BASIS = boot fallback only
    if ROLL_PARAMS.get('shortStrike'):  POSITION['short_strike'] = float(ROLL_PARAMS['shortStrike'])
    if ROLL_PARAMS.get('shortExpiry'):  POSITION['short_expiry'] = ROLL_PARAMS['shortExpiry']
    if ROLL_PARAMS.get('coverPrice'):   POSITION['cover_price']  = float(ROLL_PARAMS['coverPrice'])
    if ROLL_PARAMS.get('sellPrice'):    POSITION['sell_price']   = float(ROLL_PARAMS['sellPrice'])
    if POSITION.get('sell_price') and POSITION.get('cover_price'):
        POSITION['premium_sold'] = POSITION['sell_price'] - POSITION['cover_price']
    elif ROLL_PARAMS.get('netPremium'):
        POSITION['premium_sold'] = float(ROLL_PARAMS['netPremium'])

    if POSITION.get('flat'):  # v1.7.10 — no short: compact flat push, nothing else
        send_flat_update("recap")
        return
    try:
        spy, spy_change = fetch_spy_price()
        ticker   = build_option_ticker(POSITION["short_expiry"], POSITION["short_strike"])
        snapshot = fetch_option_snapshot(ticker)
        vix      = fetch_vix() or 18.0
        tnx      = fetch_tnx()
        dte      = days_to_expiry(POSITION["short_expiry"])

        if not snapshot or snapshot["mid"] <= 0:
            print("  Could not fetch option snapshot for EOD recap.")
            return

        call_mid  = snapshot["mid"]
        delta_mkt = snapshot.get("delta")
        # v1.6.3 — trigger evaluates in the SIM'S delta frame (see
        # sim_frame_delta); Polygon delta kept as displayed reference.
        delta     = sim_frame_delta(spy, POSITION["short_strike"], defense_dte(dte), vix)
        iv        = snapshot.get("iv")
        # v1.6.6 — sim-frame gamma: since v1.6.3 the overnight Δ-drift mixed
        # a sim delta with MARKET gamma. MARKET theta retained (feeds the
        # projected call PRICE — price-space, not a threshold quantity).
        gamma    = sim_frame_gamma(spy, POSITION["short_strike"], defense_dte(dte), vix)
        theta    = snapshot.get("theta")

        s    = score_position(spy, call_mid, vix, dte, delta=delta)
        msg  = build_eod_recap(spy, call_mid, vix, dte, delta, iv, s, tnx,
                               gamma=gamma, theta=theta, delta_mkt=delta_mkt,
                               include_overnight=include_overnight)
        send_notification(msg)
        print(f"  EOD recap sent.")

    except Exception as e:
        print(f"  EOD recap error: {e}")
        import traceback; traceback.print_exc()
        record_job_failure(e)

# ── Evening recap (6pm, 8pm, 10pm ET) ─────────────────────────────────────
def evening_recap():
    """Re-run EOD recap at 6pm, 8pm, 10pm ET. Skips weekends."""
    now = datetime.now(ET)
    if now.weekday() >= 5:
        print(f"{now.strftime('%H:%M ET')} — Evening recap skipped (weekend).")
        return
    print(f"{now.strftime('%H:%M ET')} — Running evening recap...")
    eod_recap(include_overnight=True)  # v1.6.5 — ES live in evening slots

# ── Scheduler ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"SPY Roll Agent v{VERSION} starting...")
    if POSITION.get('flat'):
        print(f"Position: FLAT \u2014 long {POSITION['shares']} SPY (no short call)")  # v1.7.11
    else:
        print(f"Position: {POSITION['shares']} shares | Strike: ${POSITION['short_strike']} | Expiry: {POSITION['short_expiry']}")
    # v1.5.18 — banner must tolerate empty ROLL_PARAMS: the startup fetch is
    # already try/excepted to {}, but direct ['key'] access here turned any
    # boot-time fetch failure into a KeyError crash loop.
    if ROLL_PARAMS:
        print(f"Roll Params (optimized {ROLL_PARAMS.get('lastOptimized','?')} · {ROLL_PARAMS.get('optProjYield') or ROLL_PARAMS.get('optAnnYield','?')}% proj. yield):")
        print(f"  Decay trigger:   {ROLL_PARAMS.get('decayTrigger',0)*100:.0f}%")
        print(f"  DTE floor:       {ROLL_PARAMS.get('dteFloor','?')}d")
        print(f"  Delta threshold: {ROLL_PARAMS.get('deltaThreshold',0):.3f}")
        _dh = ROLL_PARAMS.get('defHorizon', 0) or 0
        print(f"  Defense horizon: {str(_dh)+'d' if _dh > 0 else 'off (full DTE)'}")
        # v1.7.13 — Grace line (json-sourced, same coercion as the eval site)
        _bg = ROLL_PARAMS.get('graceDays', 3)
        try:
            _bg = int(_bg) if _bg is not None else 3
        except (TypeError, ValueError):
            _bg = 3
        print(f"  Grace:           {_bg}d")
    else:
        print("Roll Params: UNAVAILABLE at startup — retrying at each scheduled job.")
        try:
            send_notification(
                "⚠️ AGENT started DEGRADED — roll_params.json unavailable at boot.\n"
                "Monitoring will resume automatically when the fetch succeeds.\n"
                "Check GitHub Pages / Railway logs.",
                _internal=True,
            )
        except Exception as ne:
            print(f"  Degraded-start alert send failed: {ne}")
    # Schedule on exact half-hours ET, expressed in UTC (Railway runs UTC)
    # EST: ET+5 | EDT: ET+4 — we use EDT (summer) offsets; is_market_open() gates correctly either way
    # ── Intraday checks: 9:30am–3:30pm ET expressed in UTC (EDT offset) ──
    # v1.7.7 — first slot 10:00 ET (14:00 UTC): the quote feed is ~15-min
    # delayed, so a 9:30 slot scores pre-open ghosts; 10:00 reads ~9:45 RTH.
    intraday_utc = [
        "14:00", "14:30", "15:00", "15:30",
        "16:00", "16:30", "17:00", "17:30", "18:00",
        "18:30", "19:00", "19:30",
    ]
    for t in intraday_utc:
        schedule.every().day.at(t).do(guarded(check_position))

    # ── Evening recap: 6:30pm, 8pm, 10pm ET (EDT: 22:30, 00:00, 02:00 UTC)
    # Runs eod_recap(include_overnight=True) — evening slots carry the
    # Overnight/ES block; the 4:20pm EOD no longer does (v1.6.5).
    # v1.7.5: first slot 6:20→6:30pm ET — the 6:00pm collector cron
    # completes ~6:20; 6:30 clears the DM update (TV Decay reads spy_daily)
    # with ~10min margin. ES feed settled well before (v1.6.5 rationale).
    # (Sunday collision with sod_brief is moot: evening_recap skips weekends.)
    evening_utc = ["22:30", "00:00", "02:00"]
    for t in evening_utc:
        schedule.every().day.at(t).do(guarded(evening_recap))

    # ── Sunday futures open: 6:20pm ET = 22:20 UTC (v1.6.5, was 6:15) ──
    # First SOD of the week — ES reopens 6:00pm, 10-min delayed feed, +10min
    # margin for a settled first print (same rationale as evening slots)
    schedule.every().sunday.at("22:20").do(guarded(sod_brief))

    # ── SOD brief: 6:00am / 8:00am / 9:15am ET (v1.6.1 — trimmed from 8) ──
    # EDT: 10:00 / 12:00 / 13:15 UTC. Sunday evening keeps its single 22:15
    # UTC slot above (the old 00:00/02:00 UTC Sunday-coverage slots are gone —
    # Sunday now gets one futures-open brief, then Monday resumes at 6am ET).
    # sod_brief() gates itself: skips Sat, Sun<18:00 ET, market hours, Fri post-close
    sod_times_utc = [
        "10:00", "12:00", "13:15",
    ]
    for t in sod_times_utc:
        schedule.every().day.at(t).do(guarded(sod_brief))

    # ── EOD recap: 4:20pm ET = 20:20 UTC (EDT) ────────────────────────
    schedule.every().day.at("20:20").do(guarded(eod_recap))

    print(f"Scheduled: {len(intraday_utc)} intraday | {len(evening_utc)} evening (1st 6:30pm) | {len(sod_times_utc)} SOD + Sun 6:20pm | EOD 4:20pm ET (no overnight)")
    guarded(check_position)()  # run immediately on start if market is open
    while True:
        schedule.run_pending()
        time.sleep(30)
