"""
Freqtrade Prometheus Exporter
Polls Freqtrade REST APIs for the momentum, recovery, scalp, and (when running)
FreqAI bots, then exposes the data as Prometheus metrics on port 9101 AND
mirrors a small per-bot summary into Redis so the email_digest service can
render a uniform strategy table without re-querying Prometheus.

Grafana scrapes the /metrics endpoint via Prometheus.

Prometheus metrics exported (one row per bot):
  freqtrade_open_trades{bot, pair}          - currently open trades
  freqtrade_trade_profit_pct{bot, pair}     - profit % on open trades
  freqtrade_daily_profit_usd{bot}           - today's realized profit in USD
  freqtrade_weekly_profit_usd{bot}          - this week's realized profit in USD
  freqtrade_total_trades{bot}               - total closed trades since start
  freqtrade_win_rate{bot}                   - win rate (0.0–1.0)
  freqtrade_balance_usd{bot}                - current bot wallet balance in USD
  freqtrade_scrape_errors_total{bot}        - number of failed scrape attempts

Redis keys written (TTL 10 min — refreshed every scrape):
  freqtrade:{bot}:daily_pnl   - float, USD
  freqtrade:{bot}:win_rate    - float, 0.0-1.0
  freqtrade:{bot}:balance     - float, USD
  freqtrade:{bot}:status      - "running" | "unreachable"
  freqtrade:bots:active       - JSON list of bot names that scraped successfully
"""

import os
import json
import time
import logging
import requests
import redis
from prometheus_client import (
    Gauge, Counter, start_http_server, REGISTRY
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Environment ───────────────────────────────────────────────────────────────
REDIS_URL              = os.environ.get("REDIS_URL", "redis://redis:6379")
FREQTRADE_URL          = os.environ.get("FREQTRADE_URL",          "http://freqtrade:8080")
FREQTRADE_RECOVERY_URL = os.environ.get("FREQTRADE_RECOVERY_URL", "http://freqtrade_recovery:8083")
FREQTRADE_SCALP_URL    = os.environ.get("FREQTRADE_SCALP_URL",    "http://freqtrade_scalp:8081")
# FreqAI is profile-gated in docker-compose. Set FREQTRADE_FREQAI_URL=skip to
# keep it out of the rotation when the profile isn't running.
FREQTRADE_FREQAI_URL   = os.environ.get("FREQTRADE_FREQAI_URL",   "http://freqtrade_freqai:8082")
FREQTRADE_USERNAME     = os.environ.get("FREQTRADE_USERNAME", "admin")
FREQTRADE_PASSWORD     = os.environ.get("FREQTRADE_PASSWORD", "")
SCRAPE_INTERVAL        = int(os.environ.get("SCRAPE_INTERVAL", 60))
PORT                   = int(os.environ.get("PORT", 9101))

# Order matters for the email digest's strategy table render.
_candidate_bots = [
    ("momentum", FREQTRADE_URL),
    ("recovery", FREQTRADE_RECOVERY_URL),
    ("scalp",    FREQTRADE_SCALP_URL),
    ("freqai",   FREQTRADE_FREQAI_URL),
]
BOTS = {name: url for name, url in _candidate_bots if url and url.lower() != "skip"}

r = redis.from_url(REDIS_URL, decode_responses=True)

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
avg_trade_profit = Gauge(
    "freqtrade_avg_trade_profit_pct",
    "Average profit % across all closed trades",
    ["bot"],
)
best_trade_profit = Gauge(
    "freqtrade_best_trade_profit_pct",
    "Best single closed trade profit %",
    ["bot"],
)
worst_trade_profit = Gauge(
    "freqtrade_worst_trade_profit_pct",
    "Worst single closed trade profit %",
    ["bot"],
)
max_drawdown = Gauge(
    "freqtrade_max_drawdown_pct",
    "Maximum drawdown from peak balance",
    ["bot"],
)
open_trade_count = Gauge(
    "freqtrade_open_trade_count",
    "Number of currently open trades",
    ["bot"],
)
open_trade_unrealized_pnl = Gauge(
    "freqtrade_open_unrealized_pnl_pct",
    "Sum of unrealized P&L % across all open trades",
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

def _publish_redis_summary(bot_name: str, daily_pnl, win_rate_val, balance_val, reachable: bool):
    """
    Push a small per-bot summary into Redis with a 10-min TTL so digests can
    render the strategy table without scraping Prometheus. TTL ensures stale
    data drops out if the exporter goes down between cycles.
    """
    try:
        ttl = SCRAPE_INTERVAL * 10  # ~10 cycles of headroom
        if reachable:
            if daily_pnl is not None:
                r.setex(f"freqtrade:{bot_name}:daily_pnl", ttl, str(daily_pnl))
            if win_rate_val is not None:
                r.setex(f"freqtrade:{bot_name}:win_rate", ttl, str(win_rate_val))
            if balance_val is not None:
                r.setex(f"freqtrade:{bot_name}:balance", ttl, str(balance_val))
            r.setex(f"freqtrade:{bot_name}:status", ttl, "running")
        else:
            r.setex(f"freqtrade:{bot_name}:status", ttl, "unreachable")
    except Exception as e:
        logger.warning(f"Redis publish failed for {bot_name}: {e}")


def scrape_bot(bot_name: str, base_url: str):
    """Scrape all metrics from one Freqtrade bot instance.

    Returns True if at least one endpoint returned data, False if the bot
    appears unreachable (so the scrape loop can mark its Redis status).
    """
    reachable = False
    daily_pnl_val = None
    win_rate_val_local = None
    balance_val_local = None
    try:
        # ── Open trades ───────────────────────────────────────────────────────
        status = ft_get(bot_name, base_url, "/status")
        if status is not None:
            reachable = True
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
            reachable = True
            daily_pnl_val = float(profit_data.get("profit_today_fiat", 0) or 0)
            daily_profit.labels(bot=bot_name).set(daily_pnl_val)
            weekly_profit.labels(bot=bot_name).set(
                float(profit_data.get("profit_all_fiat", 0) or 0)
            )
            total_closed = int(profit_data.get("trade_count", 0) or 0)
            total_trades.labels(bot=bot_name).set(total_closed)
            wins = int(profit_data.get("winning_trades", 0) or 0)
            losses = int(profit_data.get("losing_trades", 0) or 0)
            if wins + losses > 0:
                win_rate_val_local = wins / (wins + losses)
                win_rate.labels(bot=bot_name).set(win_rate_val_local)

        # ── Balance ───────────────────────────────────────────────────────────
        balance_data = ft_get(bot_name, base_url, "/balance")
        if balance_data:
            reachable = True
            balance_val_local = float(balance_data.get("total", 0) or 0)
            balance_usd.labels(bot=bot_name).set(balance_val_local)

        # ── Open trade summary ────────────────────────────────────────────────
        if isinstance(status, list):
            open_trade_count.labels(bot=bot_name).set(len(status))
            unrealized = sum(float(t.get("profit_pct", 0) or 0) for t in status)
            open_trade_unrealized_pnl.labels(bot=bot_name).set(unrealized)
        else:
            open_trade_count.labels(bot=bot_name).set(0)
            open_trade_unrealized_pnl.labels(bot=bot_name).set(0)

        # ── Closed trade stats ────────────────────────────────────────────────
        trades_data = ft_get(bot_name, base_url, "/trades?limit=500&offset=0")
        if trades_data and isinstance(trades_data.get("trades"), list):
            closed = [
                t for t in trades_data["trades"]
                if not t.get("is_open", True)
            ]
            if closed:
                profits = [float(t.get("profit_pct", 0) or 0) * 100 for t in closed]
                avg_trade_profit.labels(bot=bot_name).set(
                    sum(profits) / len(profits)
                )
                best_trade_profit.labels(bot=bot_name).set(max(profits))
                worst_trade_profit.labels(bot=bot_name).set(min(profits))

                # Max drawdown: peak-to-trough on cumulative profit curve
                cum, peak, max_dd = 0.0, 0.0, 0.0
                for p in profits:
                    cum += p
                    peak = max(peak, cum)
                    dd = (peak - cum) / (peak + 1e-9)
                    max_dd = max(max_dd, dd)
                max_drawdown.labels(bot=bot_name).set(max_dd)

    except Exception as e:
        logger.error(f"Scrape failed for {bot_name}: {e}", exc_info=True)
        scrape_errors.labels(bot=bot_name).inc()

    _publish_redis_summary(
        bot_name,
        daily_pnl_val,
        win_rate_val_local,
        balance_val_local,
        reachable,
    )
    return reachable


def scrape_loop():
    """Main loop — scrape all bots every SCRAPE_INTERVAL seconds."""
    logger.info(f"Starting scrape loop (interval={SCRAPE_INTERVAL}s) bots={list(BOTS.keys())}")
    while True:
        active = []
        for bot_name, base_url in BOTS.items():
            if scrape_bot(bot_name, base_url):
                active.append(bot_name)
        # Tell downstream consumers (email digest) which bots responded this
        # cycle. TTL = 10 cycles so a single missed scrape doesn't blank it.
        try:
            r.setex("freqtrade:bots:active", SCRAPE_INTERVAL * 10, json.dumps(active))
        except Exception as e:
            logger.warning(f"Could not publish active bot list to Redis: {e}")
        logger.info(f"Scrape complete. Active: {active}. Next in {SCRAPE_INTERVAL}s.")
        time.sleep(SCRAPE_INTERVAL)


if __name__ == "__main__":
    logger.info(f"Freqtrade Exporter starting on port {PORT}")
    start_http_server(PORT)

    # Wait for Freqtrade to be ready
    time.sleep(30)

    scrape_loop()
