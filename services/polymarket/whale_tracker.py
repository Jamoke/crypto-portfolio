"""
Whale tracker — Phase A of the Polymarket service extension.

Queries the Polymarket order-matching subgraph (Goldsky) for recent
OrderFilled events, pairs buy/sell legs per (maker, market, outcome),
computes realized PnL, and persists a ranked set of profitable wallets
to SQLite + Redis.

The whale set is the input to Phase B's scanner (whale_concurrence feature)
and the calibration harness in Phase C.

The Polymarket subgraph URL is configurable via env var
POLYMARKET_SUBGRAPH_URL. If unset, the refresh is a no-op (logs a warning);
the rest of the polymarket service keeps running normally.

No trading, no wallet keys. Read-only analytics.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

import redis

logger = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────────────────
SUBGRAPH_URL = os.environ.get(
    "POLYMARKET_SUBGRAPH_URL",
    # Public Polymarket orderbook subgraph on Goldsky — override via env if it rotates.
    "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.14/gn",
)
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")
DB_PATH = Path("/app/data/polymarket.db")

# GraphQL page size — subgraphs typically cap at 1000 per request.
PAGE_SIZE = 1000
# Upper bound on pages per refresh. 200 * 1000 = 200k events, plenty for 90d.
MAX_PAGES = 200
# Maximum time a single refresh may run (seconds). Avoids pinning the Pi.
REFRESH_TIMEOUT_S = 900


# ── Subgraph query ──────────────────────────────────────────────────────────
# OrderFilledEvent schema reference (as published by Polymarket subgraph).
# Field names are stable; if the subgraph is republished they may drift —
# ERR logs in fetch_events() will surface any mismatch quickly.
ORDER_FILLED_QUERY = """
query OrderFilled($first: Int!, $ts_gte: BigInt!, $id_gt: String!) {
  orderFilledEvents(
    first: $first
    orderBy: id
    orderDirection: asc
    where: { timestamp_gte: $ts_gte, id_gt: $id_gt }
  ) {
    id
    timestamp
    maker
    taker
    makerAssetId
    takerAssetId
    makerAmountFilled
    takerAmountFilled
    fee
  }
}
"""


def _gql_post(url: str, query: str, variables: dict) -> dict:
    """Minimal GraphQL POST — we avoid the `gql` lib runtime to keep the
    container slim; `requests` is already a pinned dependency."""
    import requests

    resp = requests.post(
        url,
        json={"query": query, "variables": variables},
        timeout=30,
        headers={"Content-Type": "application/json"},
    )
    if resp.status_code == 429:
        raise _RateLimited()
    resp.raise_for_status()
    body = resp.json()
    if "errors" in body:
        raise RuntimeError(f"subgraph errors: {body['errors']}")
    return body["data"]


class _RateLimited(Exception):
    pass


def fetch_events(lookback_days: int) -> list[dict]:
    """
    Stream OrderFilled events from the subgraph in ID-ordered pages.
    Returns a flat list of event dicts; empty list on total failure.
    """
    if not SUBGRAPH_URL:
        logger.warning("POLYMARKET_SUBGRAPH_URL not set — skipping whale refresh")
        return []

    ts_gte = int(time.time()) - (lookback_days * 86400)
    events: list[dict] = []
    id_cursor = ""
    started = time.monotonic()

    for page in range(MAX_PAGES):
        if time.monotonic() - started > REFRESH_TIMEOUT_S:
            logger.warning("whale refresh hit REFRESH_TIMEOUT_S, returning partial set")
            break
        try:
            data = _gql_post(
                SUBGRAPH_URL,
                ORDER_FILLED_QUERY,
                {"first": PAGE_SIZE, "ts_gte": str(ts_gte), "id_gt": id_cursor},
            )
        except _RateLimited:
            logger.info("subgraph rate-limited, backing off 10s")
            time.sleep(10)
            continue
        except Exception as e:
            logger.error(f"subgraph query failed on page {page}: {e}")
            # Return whatever we have — partial data is still useful for analytics.
            break

        batch = data.get("orderFilledEvents") or []
        if not batch:
            break
        events.extend(batch)
        id_cursor = batch[-1]["id"]
        if len(batch) < PAGE_SIZE:
            break

    logger.info(f"fetched {len(events)} OrderFilled events (lookback={lookback_days}d)")
    return events


# ── PnL pairing ─────────────────────────────────────────────────────────────

def _events_to_trades(events: list[dict]) -> list[dict]:
    """
    Normalize each event into a trade row with signed USDC direction.

    In Polymarket CLOB, one side of a match gives USDC and receives outcome
    tokens (a BUY), the other gives outcome tokens and receives USDC (a SELL).
    USDC asset IDs are zero/USDC contract; outcome asset IDs are non-zero.

    Heuristic: whichever side's asset ID == 0 (or is the USDC marker)
    represents USDC, so:
      - maker gives USDC  → maker is BUYING outcome tokens
      - maker gives token → maker is SELLING outcome tokens
    """
    USDC_MARKER = "0"  # Polymarket subgraph encodes USDC as asset id "0"
    rows: list[dict] = []
    for ev in events:
        maker = (ev.get("maker") or "").lower()
        if not maker:
            continue
        maker_asset = str(ev.get("makerAssetId") or "")
        taker_asset = str(ev.get("takerAssetId") or "")
        try:
            maker_amt = float(ev["makerAmountFilled"]) / 1e6   # USDC is 6 decimals
            taker_amt = float(ev["takerAmountFilled"]) / 1e6
        except (KeyError, TypeError, ValueError):
            continue

        if maker_asset == USDC_MARKER:
            # maker paid USDC → bought outcome tokens
            side = "BUY"
            token_id = taker_asset
            usdc_flow = -maker_amt       # USDC out
            tokens = taker_amt           # tokens received (6 decimal convention also)
        elif taker_asset == USDC_MARKER:
            # maker sold outcome tokens → received USDC
            side = "SELL"
            token_id = maker_asset
            usdc_flow = +taker_amt       # USDC in
            tokens = maker_amt
        else:
            # token-for-token matches (rare); skip for PnL accounting
            continue

        rows.append({
            "maker": maker,
            "token_id": token_id,
            "side": side,
            "usdc_flow": usdc_flow,
            "tokens": tokens,
            "timestamp": int(ev["timestamp"]),
        })
    return rows


def _aggregate_whales(trades: list[dict]) -> list[dict]:
    """
    Pair BUY/SELL legs per (maker, token_id) to realize PnL.
    Open (unmatched) legs do not contribute to realized PnL.

    We use FIFO cost-basis: first BUY is matched to first SELL, etc.
    Partial fills are averaged across the outstanding lot.
    """
    # Group (maker, token_id) → list of trades sorted by timestamp.
    by_key: dict[tuple[str, str], list[dict]] = {}
    for t in trades:
        by_key.setdefault((t["maker"], t["token_id"]), []).append(t)

    per_maker: dict[str, dict] = {}

    for (maker, _token), legs in by_key.items():
        legs.sort(key=lambda r: r["timestamp"])
        # FIFO lot queue: list of [tokens_remaining, cost_per_token]
        lots: list[list[float]] = []
        for leg in legs:
            rec = per_maker.setdefault(maker, {
                "maker": maker,
                "trades": 0,
                "wins": 0,
                "realized_pnl": 0.0,
                "markets_touched": set(),
                "first_ts": leg["timestamp"],
                "last_ts": leg["timestamp"],
                "hold_hours": 0.0,
                "closed_legs": 0,
            })
            rec["trades"] += 1
            rec["markets_touched"].add(leg["token_id"])
            rec["last_ts"] = max(rec["last_ts"], leg["timestamp"])

            if leg["side"] == "BUY":
                cost_per = (-leg["usdc_flow"]) / leg["tokens"] if leg["tokens"] > 0 else 0.0
                lots.append([leg["tokens"], cost_per])
            else:
                # SELL: drain lots FIFO
                tokens_remaining = leg["tokens"]
                proceeds_per = leg["usdc_flow"] / leg["tokens"] if leg["tokens"] > 0 else 0.0
                while tokens_remaining > 0 and lots:
                    lot_qty, lot_cost = lots[0]
                    take = min(lot_qty, tokens_remaining)
                    pnl = (proceeds_per - lot_cost) * take
                    rec["realized_pnl"] += pnl
                    rec["wins"] += 1 if pnl > 0 else 0
                    rec["closed_legs"] += 1
                    lot_qty -= take
                    tokens_remaining -= take
                    if lot_qty <= 1e-9:
                        lots.pop(0)
                    else:
                        lots[0][0] = lot_qty

    # Finalize per-maker rollups.
    whales: list[dict] = []
    for maker, rec in per_maker.items():
        trades = rec["trades"]
        closed = rec["closed_legs"]
        win_rate = (rec["wins"] / closed) if closed > 0 else 0.0
        whales.append({
            "address": maker,
            "trades": trades,
            "closed_legs": closed,
            "win_rate": round(win_rate, 4),
            "total_pnl": round(rec["realized_pnl"], 2),
            "markets_touched": len(rec["markets_touched"]),
            "avg_hold_hours": round((rec["last_ts"] - rec["first_ts"]) / 3600.0, 2),
            "last_trade_at": datetime.fromtimestamp(rec["last_ts"], tz=timezone.utc).isoformat(),
        })
    return whales


# ── Persistence ─────────────────────────────────────────────────────────────

def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS whales (
            address         TEXT PRIMARY KEY,
            trades          INTEGER NOT NULL,
            closed_legs     INTEGER NOT NULL,
            win_rate        REAL NOT NULL,
            total_pnl       REAL NOT NULL,
            markets_touched INTEGER NOT NULL,
            avg_hold_hours  REAL NOT NULL,
            last_trade_at   TEXT NOT NULL,
            updated_at      TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS whale_refresh_log (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            ran_at         TEXT NOT NULL,
            lookback_days  INTEGER NOT NULL,
            events_seen    INTEGER NOT NULL,
            whales_stored  INTEGER NOT NULL,
            duration_s     REAL NOT NULL
        )
    """)
    conn.commit()


def _filter_whales(whales: list[dict], cfg: dict) -> list[dict]:
    min_trades = int(cfg.get("min_trades", 100))
    min_win_rate = float(cfg.get("min_win_rate", 0.65))
    min_pnl = float(cfg.get("min_pnl_usd", 0.0))
    filtered = [
        w for w in whales
        if w["trades"] >= min_trades
        and w["win_rate"] >= min_win_rate
        and w["total_pnl"] >= min_pnl
    ]
    filtered.sort(key=lambda w: w["total_pnl"], reverse=True)
    top_n = int(cfg.get("top_n", 50))
    return filtered[:top_n]


def _write_whales(conn: sqlite3.Connection, whales: list[dict]) -> None:
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("DELETE FROM whales")
    conn.executemany("""
        INSERT INTO whales
            (address, trades, closed_legs, win_rate, total_pnl,
             markets_touched, avg_hold_hours, last_trade_at, updated_at)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, [
        (w["address"], w["trades"], w["closed_legs"], w["win_rate"], w["total_pnl"],
         w["markets_touched"], w["avg_hold_hours"], w["last_trade_at"], now)
        for w in whales
    ])
    conn.commit()


# ── Public entry point ──────────────────────────────────────────────────────

def refresh_whales(cfg: Optional[dict] = None, redis_client: Optional[redis.Redis] = None) -> dict:
    """
    Pull events, aggregate, filter, persist. Returns a summary dict suitable
    for Prometheus + logging. Safe to call from a background thread.

    cfg is the `prediction_markets.whales` block from strategy_config.yaml.
    """
    cfg = cfg or {}
    started = time.monotonic()
    lookback = int(cfg.get("lookback_days", 90))

    events = fetch_events(lookback)
    trades = _events_to_trades(events)
    all_whales = _aggregate_whales(trades)
    top_whales = _filter_whales(all_whales, cfg)

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        ensure_schema(conn)
        _write_whales(conn, top_whales)
        duration = time.monotonic() - started
        conn.execute("""
            INSERT INTO whale_refresh_log
                (ran_at, lookback_days, events_seen, whales_stored, duration_s)
            VALUES (?,?,?,?,?)
        """, (datetime.now(timezone.utc).isoformat(), lookback,
              len(events), len(top_whales), round(duration, 2)))
        conn.commit()
    finally:
        conn.close()

    # Publish top addresses to Redis for fast lookup by the scanner (Phase B).
    rc = redis_client or redis.from_url(REDIS_URL, decode_responses=True)
    summary = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "lookback_days": lookback,
        "events_seen": len(events),
        "whales_count": len(top_whales),
        "top": [
            {"address": w["address"], "pnl": w["total_pnl"],
             "trades": w["trades"], "win_rate": w["win_rate"]}
            for w in top_whales
        ],
    }
    rc.set("signals:polymarket:whales:summary", json.dumps(summary), ex=86400 * 2)
    # Also keep a plain set of addresses for O(1) membership tests in the scanner.
    if top_whales:
        rc.delete("signals:polymarket:whales:set")
        rc.sadd("signals:polymarket:whales:set", *[w["address"] for w in top_whales])
        rc.expire("signals:polymarket:whales:set", 86400 * 2)

    logger.info(
        f"whale refresh done: events={len(events)} whales={len(top_whales)} "
        f"duration={time.monotonic() - started:.1f}s"
    )
    return summary


def get_whale_set(redis_client: redis.Redis) -> set[str]:
    """Fast lookup helper for the scanner — returns the current whale address set."""
    try:
        return set(redis_client.smembers("signals:polymarket:whales:set") or [])
    except Exception as e:
        logger.warning(f"whale set lookup failed: {e}")
        return set()


if __name__ == "__main__":
    # Manual invocation: `python whale_tracker.py` for ad-hoc refreshes.
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    refresh_whales({"lookback_days": 90, "min_trades": 100, "min_win_rate": 0.65})
