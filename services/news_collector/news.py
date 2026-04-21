"""
News Collector Service
Polls crypto news APIs every 30 minutes and writes per-symbol news items
into Redis so the claude_analyst can explain price moves in context.

Source:
  - CryptoCompare News https://min-api.cryptocompare.com/       (100k req/mo free)

Previously this service also integrated CryptoPanic, but CryptoPanic retired
its free Developer API on April 1, 2026 — integration removed. If a
replacement source is added later (NewsData.io, Messari API once confirmed
to include API access under Coinbase One, etc.), slot it alongside the
existing CryptoCompare path; the Redis shape and Prometheus metrics are
source-agnostic.

Redis shape:
  signals:news:{SYMBOL}    — JSON list of {headline, source, url, published_at,
                             sentiment, body_summary} objects, last 20, TTL 48h.
  signals:news:last_refresh — ISO8601 timestamp of last successful poll.

Cardinality:
  symbol label is bounded by the asset_governor allowed list (~10-20 assets),
  source label is {cryptocompare}. Safe for Prometheus.

Design notes:
  - Symbols are pulled from asset_governor /allowed_symbols on each cycle so
    config changes propagate without a restart.
  - CryptoCompare returns a global news feed; we filter per-symbol by scanning
    the `categories` field (CryptoCompare's categorical tags) and falling back
    to title-string matching for uncategorised posts.
  - CryptoCompare doesn't score sentiment; `sentiment` field left as None.
    If/when a sentiment source returns, claude_analyst already consumes the
    field when present.
  - Headlines are deduplicated by URL within a symbol window so repeated
    aggregator scrapes don't flood Redis.
"""

import os
import json
import time
import logging
from datetime import datetime, timezone
from typing import Optional

import redis
import requests
from prometheus_client import Counter, Gauge, start_http_server

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Environment ───────────────────────────────────────────────────────────────
REDIS_URL              = os.environ.get("REDIS_URL", "redis://redis:6379")
CRYPTOCOMPARE_API_KEY  = os.environ.get("CRYPTOCOMPARE_API_KEY", "")
POLL_INTERVAL_SECONDS  = int(os.environ.get("NEWS_POLL_INTERVAL", "1800"))  # 30 min default
METRICS_PORT           = int(os.environ.get("METRICS_PORT", "9107"))
ASSET_GOVERNOR_URL     = os.environ.get("ASSET_GOVERNOR_URL", "http://asset_governor:8090")
FALLBACK_SYMBOLS       = ["BTC", "ETH", "ARB", "MATIC", "AAVE", "UNI", "MKR", "OP"]

r = redis.from_url(REDIS_URL, decode_responses=True)

# ── Prometheus metrics ────────────────────────────────────────────────────────
news_items_fetched_total = Counter(
    "news_items_fetched_total",
    "News items fetched per source+symbol (deduplicated within a cycle)",
    ["source", "symbol"],
)
news_fetch_failures_total = Counter(
    "news_fetch_failures_total",
    "News API fetch failures (network error, non-2xx, rate-limit)",
    ["source"],
)
news_last_success_timestamp = Gauge(
    "news_last_success_timestamp",
    "Unix timestamp of the last successful poll per source",
    ["source"],
)
news_items_in_redis = Gauge(
    "news_items_in_redis",
    "Current count of news items stored in Redis per symbol",
    ["symbol"],
)
news_collector_cycle_last_success_timestamp = Gauge(
    "news_collector_cycle_last_success_timestamp",
    "Unix timestamp of the last successful collector cycle",
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_tracked_symbols() -> list[str]:
    """Pull allowed symbol set from asset_governor; fall back on a static list."""
    try:
        resp = requests.get(f"{ASSET_GOVERNOR_URL}/allowed_symbols", timeout=5)
        syms = resp.json().get("symbols", []) or []
        # Drop stablecoins — news about them is noise for trading signals
        stables = {"USDC", "DAI", "USDT", "FRAX", "TUSD"}
        tradeable = [s.upper() for s in syms if s.upper() not in stables]
        if tradeable:
            return tradeable
    except Exception as e:
        logger.warning(f"asset_governor unreachable ({e}); using fallback symbols")
    return FALLBACK_SYMBOLS


def _write_symbol_news(symbol: str, items: list[dict]):
    """Merge new items with existing cache, dedupe by URL, keep newest 20."""
    if not items:
        return
    key = f"signals:news:{symbol}"
    try:
        existing_raw = r.get(key)
        existing = json.loads(existing_raw) if existing_raw else []
    except Exception:
        existing = []

    by_url = {e.get("url"): e for e in existing if e.get("url")}
    for it in items:
        url = it.get("url")
        if url and url not in by_url:
            by_url[url] = it

    merged = sorted(
        by_url.values(),
        key=lambda x: x.get("published_at", ""),
        reverse=True,
    )[:20]

    r.setex(key, 86400 * 2, json.dumps(merged))
    news_items_in_redis.labels(symbol=symbol).set(len(merged))


# ── CryptoCompare ─────────────────────────────────────────────────────────────

def fetch_cryptocompare(symbols: list[str]) -> dict[str, list[dict]]:
    """
    CryptoCompare /data/v2/news/ accepts a categories= filter. We fetch the
    global feed once (100k/mo budget makes this cheap) and bucket into symbols
    by scanning the returned `categories` field on each item.
    """
    out: dict[str, list[dict]] = {s: [] for s in symbols}
    try:
        headers = {}
        if CRYPTOCOMPARE_API_KEY:
            headers["authorization"] = f"Apikey {CRYPTOCOMPARE_API_KEY}"
        resp = requests.get(
            "https://min-api.cryptocompare.com/data/v2/news/",
            params={"lang": "EN"},
            headers=headers,
            timeout=15,
        )
        if resp.status_code != 200:
            logger.warning(f"cryptocompare non-200: {resp.status_code} body={resp.text[:200]}")
            news_fetch_failures_total.labels(source="cryptocompare").inc()
            return out
        data = resp.json()
    except Exception as e:
        logger.warning(f"cryptocompare fetch failed: {e}")
        news_fetch_failures_total.labels(source="cryptocompare").inc()
        return out

    symbol_upper = {s.upper() for s in symbols}
    for post in data.get("Data", []) or []:
        cats = (post.get("categories") or "").upper().split("|")
        cats_clean = {c.strip() for c in cats if c.strip()}
        matched = cats_clean & symbol_upper
        if not matched and post.get("body"):
            # Fall back to scanning title for symbol mentions — crude but catches
            # obvious cases like "AAVE drops 20%" even when uncategorised.
            title_upper = (post.get("title") or "").upper()
            matched = {s for s in symbol_upper if f" {s} " in f" {title_upper} "}
        if not matched:
            continue

        item = {
            "headline": post.get("title", ""),
            "source": (post.get("source_info") or {}).get("name") or post.get("source", "cryptocompare"),
            "url": post.get("url"),
            "published_at": datetime.fromtimestamp(
                int(post.get("published_on", 0) or 0), tz=timezone.utc
            ).isoformat() if post.get("published_on") else "",
            "sentiment": None,  # CryptoCompare doesn't score
            "provider": "cryptocompare",
            "body_summary": (post.get("body") or "")[:400] or None,
        }
        for sym in matched:
            out[sym].append(item)
            news_items_fetched_total.labels(source="cryptocompare", symbol=sym).inc()

    news_last_success_timestamp.labels(source="cryptocompare").set(time.time())
    return out


# ── Cycle ─────────────────────────────────────────────────────────────────────

def run_cycle():
    symbols = load_tracked_symbols()
    logger.info(f"cycle start: tracking {len(symbols)} symbols {symbols}")

    combined: dict[str, list[dict]] = {s: [] for s in symbols}
    for source_name, fn in (("cryptocompare", fetch_cryptocompare),):
        try:
            part = fn(symbols)
        except Exception as e:
            logger.exception(f"{source_name} unexpected failure: {e}")
            news_fetch_failures_total.labels(source=source_name).inc()
            continue
        for sym, items in part.items():
            combined.setdefault(sym, []).extend(items)

    total_written = 0
    for sym, items in combined.items():
        if items:
            _write_symbol_news(sym, items)
            total_written += len(items)

    r.setex("signals:news:last_refresh", 86400 * 2,
            datetime.now(timezone.utc).isoformat())
    news_collector_cycle_last_success_timestamp.set(time.time())
    logger.info(f"cycle done: wrote {total_written} items across {len(combined)} symbols")


if __name__ == "__main__":
    logger.info(
        f"News Collector started (cryptocompare="
        f"{'yes' if CRYPTOCOMPARE_API_KEY else 'unauthenticated'}, "
        f"interval={POLL_INTERVAL_SECONDS}s)"
    )
    start_http_server(METRICS_PORT)
    logger.info(f"Metrics server started on port {METRICS_PORT}")
    time.sleep(20)  # let redis + asset_governor come up

    while True:
        try:
            run_cycle()
        except Exception as e:
            logger.error(f"cycle error: {e}", exc_info=True)
        time.sleep(POLL_INTERVAL_SECONDS)
