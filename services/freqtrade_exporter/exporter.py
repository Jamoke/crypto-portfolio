"""
Freqtrade Prometheus Exporter
Polls Freqtrade REST APIs for both the momentum and scalp bots,
then exposes the data as Prometheus metrics on port 9101.

Grafana scrapes this via Prometheus to populate the Strategy Breakdown
dashboard panels.

Metrics exported:
  freqtrade_open_trades{bot, pair}          - currently open trades
  freqtrade_trade_profit_pct{bot, pair}     - profit % on open trades
  freqtrade_daily_profit_usd{bot}           - today's realized profit in USD
  freqtrade_weekly_profit_usd{bot}          - this week's realized profit in USD
  freqtrade_total_trades{bot}               - total closed trades since start
  freqtrade_win_rate{bot}                   - win rate (0.0–1.0)
  freqtrade_balance_usd{bot}                - current bot wallet balance in USD
  freqtrade_scrape_errors_total{bot}        - number of failed scrape attempts
"""

import os
import time
import logging
import requests
from prometheus_client import (
    Gauge, Counter, start_http_server, REGISTRY
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Environment ───────────────────────────────────────────────────────────────
FREQTRADE_URL       = os.environ.get("FREQTRADE_URL", "http://freqtrade:8080")
FREQTRADE_SCALP_URL = os.environ.get("FREQTRADE_SCALP_URL", "http://freqtrade_scalp:8081")
FREQTRADE_USERNAME  = os.environ.get("FREQTRADE_USERNAME", "admin")
FREQTRADE_PASSWORD  = os.environ.get("FREQTRADE_PASSWORD", "")
SCRAPE_INTERVAL     = int(os.environ.get("SCRAPE_INTERVAL", 60))
PORT                = int(os.environ.get("PORT", 9101))

BOTS = {
    "momentum": FREQTRADE_URL,
    "scalp": FREQTRADE_SCALP_URL,
}

# ── Prometheus metrics ────────────────────────────────────────────────────────
open_trades = Gauge(
    "freqtrade_open_trades",
    "Number of currently open trades",
    ["bot", "pair"],
)
trade_profit_pct = Gauge(
    "freqtrade_trade_profit_pct",
    "Current profit percentage on an open trade",
    ["bot", "pair"],
)
daily_profit = Gauge(
    "freqtrade_daily_profit_usd",
    "Today's total realized profit in USD",
    ["bot"],
)
weekly_profit = Gauge(
    "freqtrade_weekly_profit_usd",
    "This week's total realized profit in USD",
    ["bot"],
)
total_trades = Gauge(
    "freqtrade_total_trades",
    "Total number of closed trades since bot start",
    ["bot"],
)
win_rate = Gauge(
    "freqtrade_win_rate",
    "Win rate (winning trades / total trades)",
    ["bot"],
)
balance_usd = Gauge(
    "freqtrade_balance_usd",
    "Current bot wallet balance in USD",
    ["bot"],
)
scrape_errors = Counter(
    "freqtrade_scrape_errors_total",
    "Number of failed Freqtrade API scrape attempts",
    ["bot"],
)


# ── Auth helpers ──────────────────────────────────────────────────────────────

_tokens: dict[str, str] = {}


def get_token(bot_name: str, base_url: str) -> str | None:
    """Authenticate with Freqtrade and cache the JWT token."""
    try:
        resp = requests.post(
            f"{base_url}/api/v1/token/login",
            auth=(FREQTRADE_USERNAME, FREQTRADE_PASSWORD),
            timeout=5,
        )
        if resp.status_code == 200:
            token = resp.json().get("access_token")
            _tokens[bot_name] = token
            return token
        logger.warning(f"Auth failed for {bot_name} ({base_url}): {resp.status_code}")
    except Exception as e:
        logger.warning(f"Cannot reach {bot_name} at {base_url}: {e}")
    return None


def ft_get(bot_name: str, base_url: str, path: str) -> dict | list | None:
    """Make an authenticated GET request to Freqtrade; re-auth on 401."""
    token = _tokens.get(bot_name) or get_token(bot_name, base_url)
    if not token:
        return None

    for attempt in range(2):
        try:
            resp = requests.get(
                f"{base_url}/api/v1{path}",
                headers={"Authorization": f"Bearer {token}"},
                timeout=8,
            )
            if resp.status_code == 401 and attempt == 0:
                # Token expired — re-authenticate
                token = get_token(bot_name, base_url)
                if token:
                    _tokens[bot_name] = token
                continue
            if resp.status_code == 200:
                return resp.json()
            logger.warning(f"{bot_name} GET {path} → {resp.status_code}")
            return None
        except Exception as e:
            logger.warning(f"{bot_name} GET {path} error: {e}")
            return None
    return None


# ── Scrape logic ──────────────────────────────────────────────────────────────

def scrape_bot(bot_name: str, base_url: str):
    """Scrape all metrics from one Freqtrade bot instance."""
    try:
        # ── Open trades ───────────────────────────────────────────────────────
        status = ft_get(bot_name, base_url, "/status")
        if isinstance(status, list):
            seen_pairs = set()
            for trade in status:
                pair = trade.get("pair", "unknown")
                profit = float(trade.get("profit_pct", 0) or 0)
                open_trades.labels(bot=bot_name, pair=pair).set(1)
                trade_profit_pct.labels(bot=bot_name, pair=pair).set(profit)
                seen_pairs.add(pair)
            # Remove gauges for pairs that are no longer open
            for sample in list(open_trades._metrics.keys()):
                s_bot, s_pair = sample
                if s_bot == bot_name and s_pair not in seen_pairs:
                    open_trades.remove(s_bot, s_pair)
                    trade_profit_pct.remove(s_bot, s_pair)

        # ── Profit summary ────────────────────────────────────────────────────
        profit_data = ft_get(bot_name, base_url, "/profit")
        if profit_data:
            daily_profit.labels(bot=bot_name).set(
                float(profit_data.get("profit_today_fiat", 0) or 0)
            )
            weekly_profit.labels(bot=bot_name).set(
                float(profit_data.get("profit_all_fiat", 0) or 0)
            )
            total_closed = int(profit_data.get("trade_count", 0) or 0)
            total_trades.labels(bot=bot_name).set(total_closed)
            wins = int(profit_data.get("winning_trades", 0) or 0)
            losses = int(profit_data.get("losing_trades", 0) or 0)
            if wins + losses > 0:
                win_rate.labels(bot=bot_name).set(wins / (wins + losses))

        # ── Balance ───────────────────────────────────────────────────────────
        balance_data = ft_get(bot_name, base_url, "/balance")
        if balance_data:
            # balance returns total in stake currency; approximate USD at face value for USDC
            total = float(balance_data.get("total", 0) or 0)
            balance_usd.labels(bot=bot_name).set(total)

    except Exception as e:
        logger.error(f"Scrape failed for {bot_name}: {e}", exc_info=True)
        scrape_errors.labels(bot=bot_name).inc()


def scrape_loop():
    """Main loop — scrape all bots every SCRAPE_INTERVAL seconds."""
    logger.info(f"Starting scrape loop (interval={SCRAPE_INTERVAL}s)")
    while True:
        for bot_name, base_url in BOTS.items():
            scrape_bot(bot_name, base_url)
        time.sleep(SCRAPE_INTERVAL)


if __name__ == "__main__":
    logger.info(f"Freqtrade Exporter starting on port {PORT}")
    start_http_server(PORT)

    # Wait for Freqtrade to be ready
    time.sleep(30)

    scrape_loop()
