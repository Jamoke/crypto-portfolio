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


def load_pm_config() -> dict:
    try:
        with open(STRATEGY_CONFIG) as f:
            cfg = yaml.safe_load(f)
        return cfg.get("strategies", {}).get("prediction_markets", {})
    except Exception as e:
        logger.error(f"Could not load strategy config: {e}")
        return {}


# ── Database ──────────────────────────────────────────────────────────────────

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
                         pm_cfg: dict) -> dict | None:
    """
    Return an opportunity dict if this market meets the entry criteria.
    """
    min_edge = float(pm_cfg.get("min_edge_percent", 5.0)) / 100
    min_confidence = float(pm_cfg.get("min_confidence", 0.75))
    max_per_market_pct = float(str(pm_cfg.get("max_per_market", "3%")).replace("%", "")) / 100

    claude_prob = claude_analysis["claude_prob"]
    confidence = claude_analysis["confidence"]
    edge = abs(claude_prob - market_prob)

    if edge < min_edge or confidence < min_confidence:
        return None

    # Determine which side to bet
    if claude_prob > market_prob + min_edge:
        side = "YES"
        bet_price = market_prob  # buy YES at current market price
    else:
        side = "NO"
        bet_price = 1.0 - market_prob

    portfolio_value = float(r.get("portfolio:estimated_value") or 10000)
    amount_usd = portfolio_value * max_per_market_pct

    return {
        "market_id": market.get("conditionId", ""),
        "question": market.get("question", ""),
        "side": side,
        "amount_usd": round(amount_usd, 2),
        "entry_price": round(bet_price, 4),
        "claude_prob": round(claude_prob, 4),
        "market_prob": round(market_prob, 4),
        "edge": round(edge, 4),
        "confidence": round(confidence, 4),
        "reasoning": claude_analysis.get("reasoning", ""),
        "resolution_date": market.get("endDate", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "simulation": SIMULATION_MODE,
    }


def publish_opportunity(opp: dict):
    """Write opportunity to Redis and SQLite."""
    # Redis — for email digest and monitoring
    r.lpush("signals:polymarket:opportunities", json.dumps(opp))
    r.ltrim("signals:polymarket:opportunities", 0, 49)  # keep last 50
    r.setex(f"signals:polymarket:market:{opp['market_id']}", 86400 * 7, json.dumps(opp))

    # SQLite
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT INTO positions
            (market_id, question, side, amount_usd, entry_price, claude_prob,
             market_prob, opened_at, resolution_date, simulation)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (
        opp["market_id"], opp["question"], opp["side"], opp["amount_usd"],
        opp["entry_price"], opp["claude_prob"], opp["market_prob"],
        opp["timestamp"], opp.get("resolution_date", ""), 1 if SIMULATION_MODE else 0,
    ))
    conn.commit()
    conn.close()

    mode = "SIMULATION" if SIMULATION_MODE else "LIVE"
    logger.info(
        f"[{mode}] Opportunity: {opp['side']} on '{opp['question'][:60]}...' "
        f"edge={opp['edge']:.1%}, confidence={opp['confidence']:.1%}"
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
            continue
        yes_prob, _ = odds

        # Skip near-certainty markets (not much edge possible)
        if yes_prob < 0.05 or yes_prob > 0.95:
            continue

        analysis = analyze_market_with_claude(market, yes_prob)
        if analysis is None:
            continue

        opp = evaluate_opportunity(market, analysis, yes_prob, pm_cfg)
        if opp:
            publish_opportunity(opp)
            opps_found += 1

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

    time.sleep(30)

    while True:
        try:
            run_polymarket_cycle()
        except Exception as e:
            logger.error(f"Polymarket cycle error: {e}", exc_info=True)

        time.sleep(1800)  # Run every 30 minutes
