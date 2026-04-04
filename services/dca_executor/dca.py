"""
DCA Executor Service
Implements systematic Dollar-Cost Averaging into BTC and ETH.

Behavior:
  - On the configured day/time (default: Monday 09:00 local), publishes a DCA
    buy signal to Redis for each configured asset.
  - Dip acceleration: if BTC or ETH drops >8% in 24h, publishes a 2x-sized
    buy signal immediately (max 3 accelerations per month per asset).
  - In SIMULATION_MODE (default): signals are logged and written to Redis only.
  - In live mode: signals are consumed by defi_executor via Redis pub/sub.

Configuration is read from strategy_config.yaml (dca_accumulator block).
"""

import os
import json
import time
import logging
import yaml
import redis
import requests
from datetime import datetime, timezone, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Environment ───────────────────────────────────────────────────────────────
REDIS_URL       = os.environ.get("REDIS_URL", "redis://redis:6379")
SIMULATION_MODE = os.environ.get("SIMULATION_MODE", "true").lower() == "true"
TIMEZONE        = os.environ.get("TZ", "America/Los_Angeles")
COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "")

# ── Config ────────────────────────────────────────────────────────────────────
STRATEGY_CONFIG = Path("/app/config/strategy_config.yaml")

r = redis.from_url(REDIS_URL, decode_responses=True)


def load_dca_config() -> dict:
    try:
        with open(STRATEGY_CONFIG) as f:
            cfg = yaml.safe_load(f)
        return cfg.get("strategies", {}).get("dca_accumulator", {})
    except Exception as e:
        logger.error(f"Could not load strategy config: {e}")
        return {}


def get_portfolio_value() -> float:
    """Estimate total portfolio value from Redis (set by defi_executor or manually)."""
    val = r.get("portfolio:estimated_value")
    if val:
        return float(val)
    return 10000.0  # Default assumption when no data available


def calc_dca_amount_per_asset(dca_cfg: dict) -> float:
    """
    DCA allocation = configured % of portfolio / number of DCA assets.
    E.g. 15% of $10,000 portfolio / 2 assets = $750 per asset per week.
    """
    alloc_str = str(dca_cfg.get("capital_allocation", "15%")).replace("%", "")
    allocation_pct = float(alloc_str) / 100
    assets = dca_cfg.get("assets", ["BTC", "ETH"])
    portfolio_value = get_portfolio_value()
    return (portfolio_value * allocation_pct) / max(len(assets), 1)


def publish_dca_signal(symbol: str, amount_usd: float, reason: str = "scheduled"):
    """Write a DCA buy signal to Redis."""
    signal = {
        "symbol": symbol,
        "action": "buy",
        "amount_usd": round(amount_usd, 2),
        "reason": reason,
        "source": "dca_executor",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "simulation": SIMULATION_MODE,
    }
    key = f"signals:dca:{symbol}"
    r.setex(key, 86400 * 7, json.dumps(signal))           # keep for 7 days
    r.lpush("signals:dca:queue", json.dumps(signal))
    r.ltrim("signals:dca:queue", 0, 99)                    # keep last 100

    mode = "SIMULATION" if SIMULATION_MODE else "LIVE"
    logger.info(f"[{mode}] DCA signal: {symbol} ${amount_usd:.2f} ({reason})")


def fetch_price_change_24h(symbol: str) -> float | None:
    """Fetch 24h price change % from CoinGecko."""
    id_map = {"BTC": "bitcoin", "ETH": "ethereum"}
    coin_id = id_map.get(symbol)
    if not coin_id:
        return None

    headers = {}
    if COINGECKO_API_KEY:
        headers["x-cg-demo-api-key"] = COINGECKO_API_KEY

    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            params={"vs_currency": "usd", "ids": coin_id, "price_change_percentage": "24h"},
            headers=headers,
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if data:
            return data[0].get("price_change_percentage_24h_in_currency")
    except Exception as e:
        logger.warning(f"Could not fetch price change for {symbol}: {e}")
    return None


def check_dip_acceleration(dca_cfg: dict, base_amount_usd: float):
    """
    If BTC or ETH dropped more than the trigger threshold in 24h,
    emit an accelerated DCA signal (up to max_accelerations per month).
    """
    dip_cfg = dca_cfg.get("dip_acceleration", {})
    if not dip_cfg.get("enabled", True):
        return

    trigger_drop = float(str(dip_cfg.get("trigger_drop", "-8%")).replace("%", ""))
    multiplier = float(dip_cfg.get("multiplier", 2.0))
    max_monthly = int(dip_cfg.get("max_accelerations", 3))

    assets = dca_cfg.get("assets", ["BTC", "ETH"])
    for symbol in assets:
        # Check how many accelerations have fired this month
        month_key = f"dca:accelerations:{symbol}:{datetime.now().strftime('%Y-%m')}"
        count = int(r.get(month_key) or 0)
        if count >= max_monthly:
            logger.info(f"DCA dip acceleration: {symbol} hit monthly limit ({max_monthly})")
            continue

        change = fetch_price_change_24h(symbol)
        if change is None:
            continue

        if change < trigger_drop:
            accel_amount = base_amount_usd * multiplier
            publish_dca_signal(symbol, accel_amount, reason=f"dip_acceleration_{change:.1f}pct")
            r.incr(month_key)
            r.expire(month_key, 86400 * 35)  # Expire after ~5 weeks


def is_scheduled_time(dca_cfg: dict) -> bool:
    """
    Return True if now is the configured DCA day and hour (±30 min window).
    Uses local timezone from TZ env var.
    """
    schedule = dca_cfg.get("schedule", {})
    day_name = schedule.get("day", "monday").lower()
    time_str = schedule.get("time", "09:00")

    day_map = {
        "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
        "friday": 4, "saturday": 5, "sunday": 6,
    }
    target_weekday = day_map.get(day_name, 0)
    target_hour, target_minute = map(int, time_str.split(":"))

    try:
        tz = ZoneInfo(TIMEZONE)
    except Exception:
        tz = ZoneInfo("America/Los_Angeles")

    now = datetime.now(tz)
    if now.weekday() != target_weekday:
        return False

    # Within a 30-minute window of the target time
    target_dt = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
    delta = abs((now - target_dt).total_seconds())
    return delta <= 1800  # 30 minutes


def already_ran_today(symbol: str) -> bool:
    """Check if a scheduled DCA already ran today (prevents double-firing in 30min window)."""
    today = datetime.now().strftime("%Y-%m-%d")
    key = f"dca:last_scheduled:{symbol}:{today}"
    return r.exists(key) > 0


def mark_ran_today(symbol: str):
    today = datetime.now().strftime("%Y-%m-%d")
    key = f"dca:last_scheduled:{symbol}:{today}"
    r.setex(key, 86400 * 2, "1")


def run_dca_cycle():
    dca_cfg = load_dca_config()
    if not dca_cfg.get("enabled", True):
        return

    base_amount = calc_dca_amount_per_asset(dca_cfg)
    assets = dca_cfg.get("assets", ["BTC", "ETH"])

    # Scheduled DCA
    if is_scheduled_time(dca_cfg):
        for symbol in assets:
            if not already_ran_today(symbol):
                publish_dca_signal(symbol, base_amount, reason="scheduled_weekly")
                mark_ran_today(symbol)

    # Dip acceleration check (runs every cycle regardless of schedule)
    check_dip_acceleration(dca_cfg, base_amount)


if __name__ == "__main__":
    logger.info(f"DCA Executor started (simulation={SIMULATION_MODE})")
    time.sleep(20)  # Wait for Redis and other services

    while True:
        try:
            run_dca_cycle()
        except Exception as e:
            logger.error(f"DCA cycle error: {e}", exc_info=True)

        time.sleep(3600)  # Check every hour
