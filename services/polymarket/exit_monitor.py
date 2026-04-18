"""
Exit monitor — Phase B of the Polymarket service extension.

For every open simulated position, apply three exit triggers on a 5-minute
cadence:

  1. target_hit    — current market price has moved 85% of the way from
                     entry price toward Claude's probability estimate
  2. volume_exit   — 10-minute volume delta is >= 3x the rolling 24h
                     10-minute average, indicating smart money re-pricing
  3. stale_thesis  — position has been open > 24h with price movement
                     within ±2% (thesis not materializing)

All trades remain simulation=1; this module never places an order on
Polymarket. It writes close records to SQLite and emits Prometheus
metrics for Grafana.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import redis
import requests

logger = logging.getLogger(__name__)

DB_PATH = Path("/app/data/polymarket.db")
POLYMARKET_CLOB_BASE = "https://clob.polymarket.com"

# Rolling-volume history — compact sorted sets per token, trimmed every tick.
VOLUME_HISTORY_TTL = 86400 * 2   # 48h worth of samples
VOLUME_TEN_MIN = 10 * 60
VOLUME_DAY = 24 * 3600


def _fetch_market(condition_id: str) -> dict | None:
    """Pull the current market snapshot by condition ID. Returns None on error."""
    try:
        resp = requests.get(
            f"{POLYMARKET_CLOB_BASE}/markets",
            params={"condition_ids": condition_id, "limit": 1},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json().get("data", [])
        return data[0] if data else None
    except Exception as e:
        logger.debug(f"market fetch failed for {condition_id[:16]}: {e}")
        return None


def _current_yes_price(market: dict) -> float | None:
    for token in market.get("tokens", []) or []:
        if str(token.get("outcome", "")).upper() == "YES":
            try:
                return float(token.get("price", 0) or 0)
            except (TypeError, ValueError):
                return None
    return None


def _token_id_for_side(market: dict, side: str) -> str | None:
    side_u = side.upper()
    for token in market.get("tokens", []) or []:
        if str(token.get("outcome", "")).upper() == side_u:
            return str(token.get("token_id") or "") or None
    return None


def _record_volume_sample(redis_client: redis.Redis, token_id: str, cum_volume: float) -> None:
    """Append a (timestamp, cumulative_volume) sample and trim the history."""
    key = f"polymarket:volume_history:{token_id}"
    now = int(time.time())
    # Member must be unique; combine ts + volume.
    member = f"{now}:{cum_volume}"
    redis_client.zadd(key, {member: now})
    # Trim anything older than the 48h TTL window.
    redis_client.zremrangebyscore(key, 0, now - VOLUME_HISTORY_TTL)
    redis_client.expire(key, VOLUME_HISTORY_TTL)


def _volume_spike_detected(redis_client: redis.Redis, token_id: str,
                           cum_volume: float, multiplier: float) -> bool:
    """
    Compare 10-minute volume delta to the rolling 24h average 10-minute delta.
    Returns True if 10m delta >= multiplier * 24h_avg_10m delta.
    """
    key = f"polymarket:volume_history:{token_id}"
    now = int(time.time())

    def _nearest_sample(score_floor: int) -> float | None:
        # We want the earliest sample at or after score_floor.
        entries = redis_client.zrangebyscore(key, score_floor, now, start=0, num=1)
        if not entries:
            return None
        try:
            return float(entries[0].rsplit(":", 1)[-1])
        except (TypeError, ValueError):
            return None

    ten_min_ago = _nearest_sample(now - VOLUME_TEN_MIN - 60)
    day_ago = _nearest_sample(now - VOLUME_DAY - 60)
    if ten_min_ago is None or day_ago is None:
        return False

    ten_min_delta = max(0.0, cum_volume - ten_min_ago)
    day_delta = max(0.0, cum_volume - day_ago)
    # 24h / 10m = 144 buckets; average per-bucket rate over the day
    day_avg_per_10m = day_delta / 144.0
    if day_avg_per_10m <= 0:
        return False
    return ten_min_delta >= (multiplier * day_avg_per_10m)


def _compute_pnl(side: str, entry_price: float, exit_price: float, amount_usd: float) -> float:
    """
    Simulated PnL from a CLOB position. For a BUY of YES at entry_price:
      shares = amount_usd / entry_price
      pnl = shares * (exit_price - entry_price)
    For a BUY of NO (which in Polymarket terms is buying the NO token at
    `1 - market_yes_prob`), same math applied to the NO token's price —
    the caller passes the correct directional prices.
    """
    if entry_price <= 0:
        return 0.0
    shares = amount_usd / entry_price
    return round(shares * (exit_price - entry_price), 2)


def _open_positions(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    return conn.execute(
        "SELECT * FROM positions WHERE closed_at IS NULL AND simulation=1"
    ).fetchall()


def _close_position(conn: sqlite3.Connection, row: sqlite3.Row,
                    exit_price: float, reason: str, pnl: float) -> None:
    conn.execute(
        """
        UPDATE positions
        SET closed_at=?, outcome=?, exit_price=?, exit_reason=?, pnl_usd=?
        WHERE id=?
        """,
        (
            datetime.now(timezone.utc).isoformat(),
            reason, exit_price, reason, pnl, row["id"],
        ),
    )
    conn.commit()


def evaluate_exits(exit_cfg: dict[str, Any], redis_client: redis.Redis,
                   metrics: dict[str, Any] | None = None) -> dict[str, int]:
    """
    Single-pass exit evaluation. Returns a summary dict of triggers fired.

    metrics (optional) is a dict with Prometheus objects to .inc() when a
    trigger fires — keyed by reason. The main polymarket.py wires these
    to the Counter it already exposes.
    """
    target_frac = float(exit_cfg.get("target_fraction", 0.85))
    spike_mult = float(exit_cfg.get("volume_spike_multiplier", 3.0))
    stale_hours = float(exit_cfg.get("stale_hours", 24))
    stale_tol = float(exit_cfg.get("stale_price_tolerance", 0.02))

    fired = {"target_hit": 0, "volume_exit": 0, "stale_thesis": 0}

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        rows = _open_positions(conn)
        for row in rows:
            market = _fetch_market(row["market_id"])
            if market is None:
                continue
            yes_price = _current_yes_price(market)
            if yes_price is None:
                continue

            # For a NO bet, our "current price" is (1 - yes_price).
            side = row["side"]
            current_price = yes_price if side == "YES" else (1.0 - yes_price)
            entry_price = float(row["entry_price"])
            expected_gap = float(row["expected_gap"] or 0.0)
            # For NO positions, expected_gap is computed as (claude_prob -
            # market_prob) where we bet NO because market_prob > claude_prob.
            # Convert to positional terms: expected upside in our favor.
            directional_gap = abs(expected_gap)

            # Volume tracking sample (uses our side's token_id)
            token_id = _token_id_for_side(market, side)
            cum_volume = float(market.get("volume", 0) or 0)
            if token_id:
                _record_volume_sample(redis_client, token_id, cum_volume)

            closed = False

            # Trigger 1: target hit — 85% of expected gap captured
            if directional_gap > 0:
                target_move = directional_gap * target_frac
                realized_move = current_price - entry_price
                if realized_move >= target_move:
                    pnl = _compute_pnl(side, entry_price, current_price, float(row["amount_usd"]))
                    _close_position(conn, row, current_price, "target_hit", pnl)
                    fired["target_hit"] += 1
                    if metrics: metrics["target_hit"].inc()
                    closed = True

            # Trigger 2: volume spike
            if not closed and token_id and _volume_spike_detected(
                redis_client, token_id, cum_volume, spike_mult
            ):
                pnl = _compute_pnl(side, entry_price, current_price, float(row["amount_usd"]))
                _close_position(conn, row, current_price, "volume_exit", pnl)
                fired["volume_exit"] += 1
                if metrics: metrics["volume_exit"].inc()
                closed = True

            # Trigger 3: stale thesis
            if not closed:
                try:
                    opened = datetime.fromisoformat(row["opened_at"].replace("Z", "+00:00"))
                except Exception:
                    opened = None
                if opened is not None:
                    if opened.tzinfo is None:
                        opened = opened.replace(tzinfo=timezone.utc)
                    hours_open = (datetime.now(timezone.utc) - opened).total_seconds() / 3600.0
                    if hours_open > stale_hours and abs(current_price - entry_price) < stale_tol:
                        pnl = _compute_pnl(side, entry_price, current_price,
                                           float(row["amount_usd"]))
                        _close_position(conn, row, current_price, "stale_thesis", pnl)
                        fired["stale_thesis"] += 1
                        if metrics: metrics["stale_thesis"].inc()
                        closed = True
    finally:
        conn.close()

    # Publish a short summary so the scanner / email digest can see it if
    # they want to (email digest is untouched in this plan).
    redis_client.set(
        "signals:polymarket:exit_summary_last",
        json.dumps({
            "ran_at": datetime.now(timezone.utc).isoformat(),
            "fired": fired,
        }),
        ex=86400,
    )

    total = sum(fired.values())
    if total:
        logger.info(f"exit monitor fired: {fired}")
    return fired


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    import os
    rc = redis.from_url(os.environ.get("REDIS_URL", "redis://redis:6379"), decode_responses=True)
    evaluate_exits(
        {"target_fraction": 0.85, "volume_spike_multiplier": 3.0,
         "stale_hours": 24, "stale_price_tolerance": 0.02},
        rc,
    )
