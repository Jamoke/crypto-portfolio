"""
Polymarket Service — Plan 4.5
Scans Polymarket prediction markets for opportunities where Claude's
probability estimate diverges significantly from the market price.

Flow:
  1. Fetch active markets from Polymarket CLOB API (filtered by category)
  2. For each candidate market, ask Claude to estimate the true probability
  3. If |claude_prob - market_prob| > min_edge_pct AND confidence >= min_confidence:
     → Publish opportunity to Redis
  4. In simulation mode: log + publish only
  5. In live mode: place CLOB limit order via Polymarket API (Phase 4+)

Configuration: strategy_config.yaml (prediction_markets block)
Must be enabled: prediction_markets.enabled = true in strategy_config.yaml

Env vars required for live trading:
  POLYMARKET_API_KEY      — from docs.polymarket.com
  POLYMARKET_PRIVATE_KEY  — Polygon wallet private key for order signing
"""

import os
import json
import time
import logging
import threading
import yaml
import redis
import requests
import anthropic
import sqlite3
from pathlib import Path
from datetime import datetime, timezone, timedelta
from prometheus_client import Counter, Gauge, Histogram, start_http_server

import whale_tracker
import exit_monitor
from sizing import kelly_size

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Environment ───────────────────────────────────────────────────────────────
REDIS_URL              = os.environ.get("REDIS_URL", "redis://redis:6379")
ANTHROPIC_API_KEY      = os.environ["ANTHROPIC_API_KEY"]
SIMULATION_MODE        = os.environ.get("SIMULATION_MODE", "true").lower() == "true"
POLYMARKET_API_KEY     = os.environ.get("POLYMARKET_API_KEY", "")
POLYMARKET_PRIVATE_KEY = os.environ.get("POLYMARKET_PRIVATE_KEY", "")

# ── Config ────────────────────────────────────────────────────────────────────
STRATEGY_CONFIG = Path("/app/config/strategy_config.yaml")
DB_PATH         = Path("/app/data/polymarket.db")

POLYMARKET_CLOB_BASE = "https://clob.polymarket.com"

r = redis.from_url(REDIS_URL, decode_responses=True)
claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# ── Prometheus metrics ────────────────────────────────────────────────────────
markets_scanned   = Counter("polymarket_markets_scanned_total", "Markets scanned per cycle")
opportunities     = Counter("polymarket_opportunities_total", "Opportunities identified")
positions_open    = Gauge("polymarket_positions_open", "Currently open prediction market positions")

# Phase A — whale tracker metrics
whales_tracked            = Gauge("polymarket_whales_tracked", "Whale addresses passing filter threshold")
whale_refresh_duration    = Histogram("polymarket_whale_refresh_duration_seconds",
                                      "Duration of whale refresh runs")
whale_refresh_last_success = Gauge("polymarket_whale_refresh_last_success_timestamp",
                                   "Unix timestamp of the last successful whale refresh")
whale_refresh_failures    = Counter("polymarket_whale_refresh_failures_total",
                                    "Whale refresh attempts that raised an exception")
whale_events_seen         = Gauge("polymarket_whale_events_seen",
                                  "OrderFilled events consumed during the latest whale refresh")

# Phase B — scanner filter and sizing metrics
rejects          = Counter("polymarket_rejects_total",
                           "Markets rejected by scanner filters, by reason",
                           ["reason"])
kelly_fraction   = Histogram("polymarket_kelly_fraction",
                             "Distribution of computed Kelly fractions on opportunities",
                             buckets=[0.0, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.25])
whale_concurrences = Counter("polymarket_whale_concurrences_total",
                             "Opportunities where at least one tracked whale holds the same side")
exits            = Counter("polymarket_exits_total",
                           "Simulated position exits, by reason",
                           ["reason"])


def load_pm_config() -> dict:
    try:
        with open(STRATEGY_CONFIG) as f:
            cfg = yaml.safe_load(f)
        return cfg.get("strategies", {}).get("prediction_markets", {})
    except Exception as e:
        logger.error(f"Could not load strategy config: {e}")
        return {}


# ── Database ──────────────────────────────────────────────────────────────────

def _ensure_column(conn: sqlite3.Connection, table: str, column: str, decl: str) -> None:
    """SQLite ADD COLUMN is not idempotent — check pragma first."""
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")


def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id       TEXT    NOT NULL,
            question        TEXT    NOT NULL,
            side            TEXT    NOT NULL,
            amount_usd      REAL    NOT NULL,
            entry_price     REAL    NOT NULL,
            claude_prob     REAL    NOT NULL,
            market_prob     REAL    NOT NULL,
            opened_at       TEXT    NOT NULL,
            resolution_date TEXT,
            closed_at       TEXT,
            outcome         TEXT,
            pnl_usd         REAL,
            simulation      INTEGER DEFAULT 1
        )
    """)
    # Phase B migration — idempotent per _ensure_column
    _ensure_column(conn, "positions", "expected_gap",      "REAL")
    _ensure_column(conn, "positions", "kelly_fraction",    "REAL")
    _ensure_column(conn, "positions", "whale_concurrence", "INTEGER DEFAULT 0")
    _ensure_column(conn, "positions", "exit_price",        "REAL")
    _ensure_column(conn, "positions", "exit_reason",       "TEXT")
    _ensure_column(conn, "positions", "claude_model",      "TEXT")
    _ensure_column(conn, "positions", "depth_usd",         "REAL")
    _ensure_column(conn, "positions", "hours_to_resolution", "REAL")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS scans (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            scanned_at  TEXT NOT NULL,
            markets_checked INTEGER,
            opportunities_found INTEGER
        )
    """)
    conn.commit()
    conn.close()


# ── Market discovery ──────────────────────────────────────────────────────────

CATEGORY_KEYWORDS = {
    "crypto": ["bitcoin", "ethereum", "crypto", "btc", "eth", "solana", "defi", "nft", "blockchain"],
    "technology": ["ai", "artificial intelligence", "apple", "google", "microsoft", "tesla", "tech"],
    "macro_economy": ["fed", "interest rate", "gdp", "recession", "inflation", "unemployment", "s&p"],
}


def fetch_active_markets(categories: list[str]) -> list[dict]:
    """Fetch active markets from Polymarket CLOB API, filtered by category keywords."""
    try:
        resp = requests.get(
            f"{POLYMARKET_CLOB_BASE}/markets",
            params={"active": "true", "closed": "false", "limit": 200},
            timeout=15,
        )
        resp.raise_for_status()
        all_markets = resp.json().get("data", [])
    except Exception as e:
        logger.warning(f"Could not fetch Polymarket markets: {e}")
        return []

    # Filter by category keywords
    keywords = []
    for cat in categories:
        keywords.extend(CATEGORY_KEYWORDS.get(cat, []))

    filtered = []
    for market in all_markets:
        question = str(market.get("question", "")).lower()
        if any(kw in question for kw in keywords):
            filtered.append(market)

    logger.info(f"Fetched {len(all_markets)} markets, {len(filtered)} match categories: {categories}")
    markets_scanned.inc(len(filtered))
    return filtered


def fetch_orderbook_depth(token_id: str) -> float:
    """
    Fetch the Polymarket CLOB orderbook for a token and return the
    minimum USD-denominated depth across bid and ask sides (top 10 levels).

    Returns 0.0 on any error — caller treats that as "depth unknown,"
    which gets rejected by the min_depth filter. Read-only.
    """
    try:
        resp = requests.get(
            f"{POLYMARKET_CLOB_BASE}/book",
            params={"token_id": token_id},
            timeout=10,
        )
        resp.raise_for_status()
        book = resp.json()
    except Exception as e:
        logger.debug(f"orderbook fetch failed for {token_id[:10]}: {e}")
        return 0.0

    def _side_depth(levels) -> float:
        total = 0.0
        for lvl in (levels or [])[:10]:
            try:
                total += float(lvl.get("price", 0) or 0) * float(lvl.get("size", 0) or 0)
            except (TypeError, ValueError):
                continue
        return total

    bid_depth = _side_depth(book.get("bids"))
    ask_depth = _side_depth(book.get("asks"))
    return min(bid_depth, ask_depth)


def hours_to_resolution(market: dict) -> float | None:
    """Parse market.endDate into hours-from-now. Returns None if unparseable."""
    end = market.get("endDate") or market.get("end_date")
    if not end:
        return None
    try:
        # Polymarket returns ISO-8601 strings, sometimes with 'Z', sometimes '+00:00'.
        iso = end.replace("Z", "+00:00")
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        delta = dt - datetime.now(timezone.utc)
        return delta.total_seconds() / 3600.0
    except Exception:
        return None


def scanner_filters_pass(market: dict, pm_cfg: dict, yes_prob: float) -> tuple[bool, str | None, dict]:
    """
    Apply Phase B scanner filters. Returns (ok, rejection_reason, context).
    Context carries fields the opportunity record will record (depth, hours).
    """
    ctx: dict = {"depth_usd": 0.0, "hours_to_resolution": None}

    # Edge: skip near-certainty markets where there's no meaningful room
    if yes_prob < 0.05 or yes_prob > 0.95:
        return False, "near_certainty", ctx

    # Volume floor
    min_volume = float(pm_cfg.get("min_volume_usd", 50000))
    volume = float(market.get("volume", 0) or 0)
    if volume < min_volume:
        return False, "volume", ctx

    # Time to resolution
    hrs = hours_to_resolution(market)
    ctx["hours_to_resolution"] = hrs
    min_hrs = float(pm_cfg.get("min_hours_to_resolution", 4))
    max_hrs = float(pm_cfg.get("max_hours_to_resolution", 168))
    if hrs is None or hrs < min_hrs or hrs > max_hrs:
        return False, "time", ctx

    # Orderbook depth — one network call per candidate; do this last so
    # earlier cheap rejections don't waste API calls.
    token_id = ""
    for token in market.get("tokens", []) or []:
        if str(token.get("outcome", "")).upper() == "YES":
            token_id = str(token.get("token_id", ""))
            break
    depth = fetch_orderbook_depth(token_id) if token_id else 0.0
    ctx["depth_usd"] = depth
    min_depth = float(pm_cfg.get("min_depth_usd", 500))
    if depth < min_depth:
        return False, "depth", ctx

    return True, None, ctx


def whale_concurrence_for_market(market: dict, side: str) -> bool:
    """
    Phase B signal: does any tracked whale currently hold `side` in this market?

    Conservative implementation — we check the Redis-cached whale set and,
    for each whale, query Polymarket's data API for recent positions. To
    stay cheap we only check up to a handful of top whales and cache
    the answer for 10 minutes per market.
    """
    whale_set = whale_tracker.get_whale_set(r)
    if not whale_set:
        return False

    condition_id = market.get("conditionId") or market.get("condition_id")
    if not condition_id:
        return False

    cache_key = f"polymarket:whale_concur:{condition_id}:{side}"
    cached = r.get(cache_key)
    if cached is not None:
        return cached == "1"

    # Limit to a small sample of whales per check to bound latency.
    sample = list(whale_set)[:20]
    concurrence = False
    for address in sample:
        try:
            resp = requests.get(
                "https://data-api.polymarket.com/positions",
                params={"user": address, "limit": 50},
                timeout=5,
            )
            if resp.status_code != 200:
                continue
            positions = resp.json() or []
            for pos in positions:
                pos_cond = pos.get("conditionId") or pos.get("condition_id")
                pos_outcome = str(pos.get("outcome", "")).upper()
                if pos_cond == condition_id and pos_outcome == side.upper():
                    concurrence = True
                    break
            if concurrence:
                break
        except Exception as e:
            logger.debug(f"whale position lookup failed for {address[:10]}: {e}")

    r.setex(cache_key, 600, "1" if concurrence else "0")
    return concurrence


def parse_market_odds(market: dict) -> tuple[float, float] | None:
    """
    Parse YES probability from CLOB token prices.
    Returns (yes_prob, no_prob) or None if unparseable.
    """
    tokens = market.get("tokens", [])
    yes_price = None
    no_price = None
    for token in tokens:
        outcome = str(token.get("outcome", "")).upper()
        price = float(token.get("price", 0) or 0)
        if outcome == "YES":
            yes_price = price
        elif outcome == "NO":
            no_price = price

    if yes_price is None:
        return None
    # Normalize: in Polymarket, price IS the probability (0.0–1.0)
    no_prob = 1.0 - yes_price if no_price is None else no_price
    return yes_price, no_prob


# ── Claude edge analysis ──────────────────────────────────────────────────────

def analyze_market_with_claude(market: dict, market_prob: float) -> dict | None:
    """
    Ask Claude to estimate the probability of YES for a Polymarket question.
    Returns {claude_prob, confidence, reasoning} or None on failure.
    """
    question = market.get("question", "Unknown question")
    resolution_date = market.get("endDate", "Unknown")
    volume = float(market.get("volume", 0) or 0)

    prompt = f"""You are analyzing a prediction market question to estimate the true probability.

Market question: "{question}"
Resolution date: {resolution_date}
Current market odds: YES = {market_prob:.1%}
Market volume: ${volume:,.0f}

Analyze this question carefully. Consider:
- Known facts and recent news relevant to this question
- Base rates for similar events
- Information asymmetry or bias in how markets price this

Respond with ONLY valid JSON:
{{
  "yes_probability": 0.65,
  "confidence": 0.75,
  "reasoning": "One concise sentence explaining your estimate",
  "key_factors": ["factor 1", "factor 2"]
}}

yes_probability: your estimate of P(YES) from 0.0 to 1.0
confidence: how confident you are in this estimate (0.0 = no idea, 1.0 = certain)
"""

    try:
        message = claude.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=400,
            messages=[{"role": "user", "content": prompt}],
        )
        text = message.content[0].text.strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        result = json.loads(text)
        result["claude_prob"] = float(result.pop("yes_probability", 0.5))
        return result
    except Exception as e:
        logger.warning(f"Claude analysis failed for market '{question[:50]}': {e}")
        return None


# ── Opportunity evaluation ────────────────────────────────────────────────────

def evaluate_opportunity(market: dict, claude_analysis: dict, market_prob: float,
                         pm_cfg: dict, filter_ctx: dict | None = None) -> dict | None:
    """
    Return an opportunity dict if this market meets the entry criteria.
    Uses fractional-Kelly sizing; hard-capped at max_per_market of portfolio.
    """
    min_edge = float(pm_cfg.get("min_edge_percent", 5.0)) / 100
    min_confidence = float(pm_cfg.get("min_confidence", 0.75))
    max_per_market_pct = float(str(pm_cfg.get("max_per_market", "3%")).replace("%", "")) / 100
    kelly_cap = float(pm_cfg.get("kelly_cap", 0.25))

    claude_prob = claude_analysis["claude_prob"]
    confidence = claude_analysis["confidence"]
    edge = abs(claude_prob - market_prob)

    if edge < min_edge or confidence < min_confidence:
        return None

    # Determine which side to bet and the price we'd be paying.
    if claude_prob > market_prob + min_edge:
        side = "YES"
        bet_price = market_prob                     # buying YES
        p_win = claude_prob
    else:
        side = "NO"
        bet_price = 1.0 - market_prob               # buying NO
        p_win = 1.0 - claude_prob

    portfolio_value = float(r.get("portfolio:estimated_value") or 10000)

    # Kelly sizing, capped by both the quarter-Kelly fraction AND the
    # per-market hard ceiling (max_per_market). A miscalibrated Claude
    # estimate can produce a large f_star; the per-market cap is the
    # backstop that keeps any single bet from dominating the book.
    kelly_amount = kelly_size(p_win=p_win, market_price=bet_price,
                              bankroll=portfolio_value, max_fraction=kelly_cap)
    per_market_cap = portfolio_value * max_per_market_pct
    amount_usd = min(kelly_amount, per_market_cap)
    if amount_usd <= 0:
        return None

    f_star_effective = amount_usd / portfolio_value if portfolio_value > 0 else 0.0
    kelly_fraction.observe(f_star_effective)

    concurrence = whale_concurrence_for_market(market, side)
    if concurrence:
        whale_concurrences.inc()

    filter_ctx = filter_ctx or {}
    return {
        "market_id": market.get("conditionId", ""),
        "question": market.get("question", ""),
        "side": side,
        "amount_usd": round(amount_usd, 2),
        "entry_price": round(bet_price, 4),
        "claude_prob": round(claude_prob, 4),
        "market_prob": round(market_prob, 4),
        "edge": round(edge, 4),
        "expected_gap": round(claude_prob - market_prob, 4),
        "confidence": round(confidence, 4),
        "kelly_fraction": round(f_star_effective, 4),
        "whale_concurrence": bool(concurrence),
        "depth_usd": round(float(filter_ctx.get("depth_usd", 0.0)), 2),
        "hours_to_resolution": round(filter_ctx.get("hours_to_resolution") or 0.0, 2),
        "claude_model": "claude-sonnet-4-6",
        "reasoning": claude_analysis.get("reasoning", ""),
        "resolution_date": market.get("endDate", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "simulation": SIMULATION_MODE,
    }


def publish_opportunity(opp: dict):
    """Write opportunity to Redis and SQLite."""
    # Hard guard: Phase B remains simulation-only. This function must never
    # be reachable from a live-order code path. Until a separate plan
    # explicitly enables live, all rows are marked simulation=1 regardless
    # of SIMULATION_MODE — see Phases A–C legal guardrails.
    simulation_flag = 1

    # Redis — for email digest and monitoring
    r.lpush("signals:polymarket:opportunities", json.dumps(opp))
    r.ltrim("signals:polymarket:opportunities", 0, 49)  # keep last 50
    r.setex(f"signals:polymarket:market:{opp['market_id']}", 86400 * 7, json.dumps(opp))

    # SQLite — includes Phase B columns (expected_gap, kelly_fraction,
    # whale_concurrence, claude_model, depth_usd, hours_to_resolution)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT INTO positions
            (market_id, question, side, amount_usd, entry_price, claude_prob,
             market_prob, opened_at, resolution_date, simulation,
             expected_gap, kelly_fraction, whale_concurrence,
             claude_model, depth_usd, hours_to_resolution)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        opp["market_id"], opp["question"], opp["side"], opp["amount_usd"],
        opp["entry_price"], opp["claude_prob"], opp["market_prob"],
        opp["timestamp"], opp.get("resolution_date", ""), simulation_flag,
        opp.get("expected_gap"), opp.get("kelly_fraction"),
        1 if opp.get("whale_concurrence") else 0,
        opp.get("claude_model", "claude-sonnet-4-6"),
        opp.get("depth_usd"), opp.get("hours_to_resolution"),
    ))
    conn.commit()
    conn.close()

    logger.info(
        f"[SIMULATION] Opportunity: {opp['side']} on '{opp['question'][:60]}...' "
        f"edge={opp['edge']:.1%}, kelly={opp.get('kelly_fraction', 0):.3f}, "
        f"confidence={opp['confidence']:.1%}, whale={opp.get('whale_concurrence', False)}"
    )
    opportunities.inc()


def check_position_timeouts():
    """Close positions that have exceeded resolution_timeout (90 days)."""
    conn = sqlite3.connect(DB_PATH)
    timeout_date = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()
    conn.execute("""
        UPDATE positions SET closed_at=?, outcome='timeout', pnl_usd=0
        WHERE closed_at IS NULL AND opened_at < ?
    """, (datetime.now(timezone.utc).isoformat(), timeout_date))
    conn.commit()

    open_count = conn.execute(
        "SELECT COUNT(*) FROM positions WHERE closed_at IS NULL"
    ).fetchone()[0]
    conn.close()
    positions_open.set(open_count)


# ── Main cycle ────────────────────────────────────────────────────────────────

def check_total_exposure(pm_cfg: dict) -> bool:
    """
    Return True if we're within the max portfolio allocation for prediction markets.
    """
    max_alloc_pct = float(str(pm_cfg.get("capital_allocation", "10%")).replace("%", "")) / 100
    portfolio_value = float(r.get("portfolio:estimated_value") or 10000)
    max_total = portfolio_value * max_alloc_pct

    conn = sqlite3.connect(DB_PATH)
    open_exposure = conn.execute(
        "SELECT COALESCE(SUM(amount_usd), 0) FROM positions WHERE closed_at IS NULL"
    ).fetchone()[0]
    conn.close()
    return float(open_exposure) < max_total


def run_polymarket_cycle():
    pm_cfg = load_pm_config()

    if not pm_cfg.get("enabled", False):
        logger.debug("Prediction markets disabled in strategy_config.yaml. Skipping.")
        return

    logger.info("Running Polymarket scan...")

    if not check_total_exposure(pm_cfg):
        logger.info("At max prediction market allocation. Skipping new positions.")
        return

    categories = pm_cfg.get("categories", ["crypto", "technology", "macro_economy"])
    markets = fetch_active_markets(categories)

    opps_found = 0
    for market in markets[:30]:  # Limit to 30 per cycle to control Claude API costs
        odds = parse_market_odds(market)
        if odds is None:
            rejects.labels(reason="unparseable_odds").inc()
            continue
        yes_prob, _ = odds

        # Phase B filters: depth, time-to-resolution, volume, near-certainty
        ok, reason, filter_ctx = scanner_filters_pass(market, pm_cfg, yes_prob)
        if not ok:
            rejects.labels(reason=reason or "unknown").inc()
            continue

        analysis = analyze_market_with_claude(market, yes_prob)
        if analysis is None:
            rejects.labels(reason="claude_error").inc()
            continue

        opp = evaluate_opportunity(market, analysis, yes_prob, pm_cfg, filter_ctx)
        if opp:
            publish_opportunity(opp)
            opps_found += 1
        else:
            rejects.labels(reason="edge_or_confidence").inc()

        time.sleep(1)  # Rate limit Claude API calls

    check_position_timeouts()

    # Log scan summary to SQLite
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO scans (scanned_at, markets_checked, opportunities_found) VALUES (?,?,?)",
        (datetime.now(timezone.utc).isoformat(), min(len(markets), 30), opps_found),
    )
    conn.commit()
    conn.close()

    logger.info(f"Polymarket scan complete. Checked {min(len(markets), 30)} markets, found {opps_found} opportunities.")


# ── Whale refresh worker (Phase A) ────────────────────────────────────────────

def whale_refresh_worker(stop_event: threading.Event):
    """
    Background loop that refreshes the whale set periodically.
    Runs independently of the market scan cycle; the scanner (Phase B) reads
    the whale set from Redis whenever it needs it.

    Refresh cadence comes from prediction_markets.whales.refresh_hours
    (default 24). The whale refresh is a no-op if POLYMARKET_SUBGRAPH_URL
    is unset, so enabling this loop is safe even before the subgraph URL
    is provisioned.
    """
    while not stop_event.is_set():
        pm_cfg = load_pm_config()
        whales_cfg = pm_cfg.get("whales", {}) or {}
        refresh_hours = float(whales_cfg.get("refresh_hours", 24))

        if not whales_cfg.get("enabled", True):
            logger.info("whale tracker disabled in config; sleeping %.1fh", refresh_hours)
        else:
            started = time.monotonic()
            try:
                with whale_refresh_duration.time():
                    summary = whale_tracker.refresh_whales(whales_cfg, redis_client=r)
                whales_tracked.set(summary.get("whales_count", 0))
                whale_events_seen.set(summary.get("events_seen", 0))
                whale_refresh_last_success.set(time.time())
            except Exception as e:
                whale_refresh_failures.inc()
                logger.error("whale refresh failed after %.1fs: %s",
                             time.monotonic() - started, e, exc_info=True)

        # Sleep in short increments so a stop_event can interrupt promptly.
        sleep_total = refresh_hours * 3600
        slept = 0.0
        while slept < sleep_total and not stop_event.is_set():
            time.sleep(min(60, sleep_total - slept))
            slept += 60


def exit_monitor_worker(stop_event: threading.Event):
    """
    Background loop that evaluates exit triggers on open simulated positions
    every 5 minutes. Noop when there are no open positions.
    """
    metrics = {
        "target_hit":   exits.labels(reason="target_hit"),
        "volume_exit":  exits.labels(reason="volume_exit"),
        "stale_thesis": exits.labels(reason="stale_thesis"),
    }
    while not stop_event.is_set():
        try:
            pm_cfg = load_pm_config()
            exit_cfg = (pm_cfg.get("exit") or {})
            exit_monitor.evaluate_exits(exit_cfg, r, metrics=metrics)
        except Exception as e:
            logger.error(f"exit monitor cycle error: {e}", exc_info=True)

        # Sleep 300s in 60s slices so stop_event is responsive.
        for _ in range(5):
            if stop_event.is_set():
                break
            time.sleep(60)


if __name__ == "__main__":
    logger.info(f"Polymarket Service started (simulation={SIMULATION_MODE})")

    init_db()

    # Start Prometheus metrics server
    start_http_server(9104)
    logger.info("Metrics server started on port 9104")

    pm_cfg = load_pm_config()
    if not pm_cfg.get("enabled", False):
        logger.info(
            "Prediction markets are DISABLED in strategy_config.yaml. "
            "Set prediction_markets.enabled: true to activate. "
            "Service will check every 30 minutes."
        )

    # Start the whale refresh background thread regardless of the
    # prediction_markets.enabled gate — whale analytics is a pure
    # read-only feed, useful even when the scanner is idle.
    stop_event = threading.Event()
    whale_thread = threading.Thread(
        target=whale_refresh_worker,
        args=(stop_event,),
        name="whale-refresh",
        daemon=True,
    )
    whale_thread.start()
    logger.info("whale refresh worker started")

    # Phase B: exit monitor thread — polls open simulated positions every
    # 5 minutes and closes them on target / volume / stale triggers.
    exit_thread = threading.Thread(
        target=exit_monitor_worker,
        args=(stop_event,),
        name="exit-monitor",
        daemon=True,
    )
    exit_thread.start()
    logger.info("exit monitor worker started")

    time.sleep(30)

    while True:
        try:
            run_polymarket_cycle()
        except Exception as e:
            logger.error(f"Polymarket cycle error: {e}", exc_info=True)

        time.sleep(1800)  # Run every 30 minutes
