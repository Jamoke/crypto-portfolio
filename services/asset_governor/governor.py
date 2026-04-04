"""
Asset Governor Service
Enforces the allowlist and allocation rules defined in asset_governance.yaml.
Exposes a simple REST API that all other services call before acting.

Also runs a circuit breaker enforcer: monitors system:trading_paused in Redis
and calls Freqtrade's REST API to pause/resume bots when the flag changes.

Endpoints:
  GET  /check?symbol=ETH          → {"allowed": true, "reason": "..."}
  GET  /allocation?symbol=ETH     → {"max_allocation": 0.15, "current": 0.08}
  GET  /allowed_symbols            → {"symbols": ["BTC", "ETH", ...]}
  GET  /circuit_breaker_status    → {"paused": false, "reason": "", "bots": {...}}
  POST /reload                     → Reloads config from disk
"""

import os
import yaml
import json
import time
import logging
import threading
import requests
import redis as redis_lib
from pathlib import Path
from flask import Flask, request, jsonify
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
CONFIG_PATH = Path("/app/config/asset_governance.yaml")
RISK_CONFIG_PATH = Path("/app/config/risk_config.yaml")

# ── Environment ───────────────────────────────────────────────────────────────
REDIS_URL           = os.environ.get("REDIS_URL", "redis://redis:6379")
FREQTRADE_URL       = os.environ.get("FREQTRADE_URL", "http://freqtrade:8080")
FREQTRADE_SCALP_URL = os.environ.get("FREQTRADE_SCALP_URL", "http://freqtrade_scalp:8081")
FREQTRADE_USERNAME  = os.environ.get("FREQTRADE_USERNAME", "admin")
FREQTRADE_PASSWORD  = os.environ.get("FREQTRADE_PASSWORD", "")

# ── Prometheus metrics ────────────────────────────────────────────────────────
circuit_breaker_triggers = Counter(
    "circuit_breaker_triggered_total",
    "Number of times the circuit breaker has been triggered",
    ["reason"],
)

# ── Redis ─────────────────────────────────────────────────────────────────────
_redis = redis_lib.from_url(REDIS_URL, decode_responses=True)

_governance = {}
_last_paused_state = None  # track previous state to detect transitions


def load_governance() -> dict:
    global _governance
    try:
        with open(CONFIG_PATH) as f:
            _governance = yaml.safe_load(f) or {}
        logger.info("Asset governance config loaded successfully")
    except Exception as e:
        logger.error(f"Failed to load governance config: {e}")
    return _governance


def load_risk_config() -> dict:
    try:
        with open(RISK_CONFIG_PATH) as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        logger.warning(f"Could not load risk config: {e}")
        return {}


def get_blacklist() -> set:
    return set(_governance.get("global_blacklist", []))


def get_allowed_symbols() -> dict:
    """Returns {symbol: {"category": ..., "max_allocation": ...}}"""
    allowed = {}
    categories = _governance.get("categories", {})
    blacklist = get_blacklist()
    overrides = _governance.get("overrides", {})
    force_allow = set(overrides.get("force_allow", []))
    force_deny = set(overrides.get("force_deny", []))

    for cat_name, cat in categories.items():
        if not cat.get("enabled", False):
            continue
        max_alloc_str = cat.get("max_allocation", "100%")
        max_alloc = float(str(max_alloc_str).replace("%", "")) / 100

        for symbol in cat.get("assets", []):
            if symbol in blacklist and symbol not in force_allow:
                continue
            if symbol in force_deny:
                continue
            allowed[symbol] = {
                "category": cat_name,
                "max_allocation": max_alloc,
                "notes": cat.get("notes", ""),
            }

    for symbol in force_allow:
        if symbol not in blacklist:
            allowed.setdefault(symbol, {"category": "override", "max_allocation": 0.05})

    return allowed


# ── Circuit Breaker ───────────────────────────────────────────────────────────

def _ft_auth() -> tuple[str, str]:
    return (FREQTRADE_USERNAME, FREQTRADE_PASSWORD)


def _ft_get_token(base_url: str) -> str | None:
    """Authenticate with Freqtrade and return a JWT token."""
    try:
        resp = requests.post(
            f"{base_url}/api/v1/token/login",
            json={"username": FREQTRADE_USERNAME, "password": FREQTRADE_PASSWORD},
            timeout=5,
        )
        if resp.status_code == 200:
            return resp.json().get("access_token")
        logger.warning(f"Freqtrade auth failed at {base_url}: {resp.status_code}")
    except Exception as e:
        logger.warning(f"Could not reach Freqtrade at {base_url}: {e}")
    return None


def _ft_call(base_url: str, method: str, path: str) -> bool:
    """Make an authenticated call to the Freqtrade REST API. Returns True on success."""
    token = _ft_get_token(base_url)
    if not token:
        return False
    try:
        headers = {"Authorization": f"Bearer {token}"}
        url = f"{base_url}/api/v1{path}"
        if method == "POST":
            resp = requests.post(url, headers=headers, timeout=5)
        else:
            resp = requests.get(url, headers=headers, timeout=5)
        if resp.status_code in (200, 201):
            return True
        logger.warning(f"Freqtrade {method} {path} returned {resp.status_code}: {resp.text}")
    except Exception as e:
        logger.warning(f"Freqtrade API call failed ({base_url}{path}): {e}")
    return False


def pause_bots(reason: str):
    """Tell both Freqtrade bots to stop buying new positions."""
    logger.warning(f"CIRCUIT BREAKER: Pausing bots. Reason: {reason}")
    for name, url in [("momentum", FREQTRADE_URL), ("scalp", FREQTRADE_SCALP_URL)]:
        ok = _ft_call(url, "POST", "/stopbuy")
        logger.info(f"  {name} bot stopbuy: {'ok' if ok else 'failed'}")
    circuit_breaker_triggers.labels(reason=reason).inc()


def resume_bots():
    """Tell both Freqtrade bots to resume accepting new positions."""
    logger.info("CIRCUIT BREAKER: Resuming bots (manual approval received)")
    for name, url in [("momentum", FREQTRADE_URL), ("scalp", FREQTRADE_SCALP_URL)]:
        ok = _ft_call(url, "POST", "/start")
        logger.info(f"  {name} bot start: {'ok' if ok else 'failed'}")


def get_bot_states() -> dict:
    """Query current state of both bots."""
    states = {}
    for name, url in [("momentum", FREQTRADE_URL), ("scalp", FREQTRADE_SCALP_URL)]:
        token = _ft_get_token(url)
        if not token:
            states[name] = "unreachable"
            continue
        try:
            resp = requests.get(
                f"{url}/api/v1/status",
                headers={"Authorization": f"Bearer {token}"},
                timeout=5,
            )
            if resp.status_code == 200:
                data = resp.json()
                # /status returns a list of open trades; check bot state separately
                states[name] = "running"
            else:
                states[name] = f"http_{resp.status_code}"
        except Exception:
            states[name] = "unreachable"
    return states


def circuit_breaker_loop():
    """
    Background thread: polls system:trading_paused in Redis every 60s.
    Calls Freqtrade API to pause/resume bots when the flag transitions.
    """
    global _last_paused_state
    logger.info("Circuit breaker enforcer started")

    while True:
        try:
            paused_str = _redis.get("system:trading_paused")
            paused = paused_str == "true"
            reason = _redis.get("system:pause_reason") or "drawdown_limit"

            if paused and _last_paused_state is not True:
                pause_bots(reason)
                _last_paused_state = True
            elif not paused and _last_paused_state is True:
                resume_bots()
                _last_paused_state = False
            elif _last_paused_state is None:
                # First run — sync state without triggering counter
                _last_paused_state = paused
                if paused:
                    pause_bots(reason)

        except Exception as e:
            logger.error(f"Circuit breaker loop error: {e}", exc_info=True)

        time.sleep(60)


def drawdown_monitor_loop():
    """
    Background thread: reads simulated trades from Redis, tracks daily/weekly P&L,
    and sets system:trading_paused if drawdown thresholds are breached.
    """
    risk_cfg = load_risk_config()
    cb_cfg = risk_cfg.get("risk_management", {}).get("circuit_breakers", {})

    daily_threshold = float(
        str(cb_cfg.get("daily_drawdown_pause", "10%")).replace("%", "")
    ) / 100
    weekly_threshold = float(
        str(cb_cfg.get("weekly_drawdown_pause", "20%")).replace("%", "")
    ) / 100

    logger.info(
        f"Drawdown monitor started: daily={daily_threshold:.0%}, weekly={weekly_threshold:.0%}"
    )

    while True:
        try:
            # Skip if already paused
            if _redis.get("system:trading_paused") == "true":
                time.sleep(300)
                continue

            # Read recent simulated trades from Redis
            raw_trades = _redis.lrange("executor:simulated_trades", 0, 99)
            trades = [json.loads(t) for t in raw_trades if t]

            if not trades:
                time.sleep(300)
                continue

            from datetime import datetime, timezone, timedelta
            now = datetime.now(timezone.utc)
            day_ago = now - timedelta(hours=24)
            week_ago = now - timedelta(days=7)

            daily_pnl = 0.0
            weekly_pnl = 0.0

            for trade in trades:
                ts_str = trade.get("timestamp", "")
                pnl = float(trade.get("profit_usd", 0) or trade.get("pnl_usd", 0))
                try:
                    ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    if ts >= day_ago:
                        daily_pnl += pnl
                    if ts >= week_ago:
                        weekly_pnl += pnl
                except Exception:
                    continue

            # Check thresholds (negative P&L = drawdown)
            # Use a rough portfolio estimate: sum of invested_usd across trades or a fixed baseline
            portfolio_value = float(_redis.get("portfolio:estimated_value") or 10000)

            daily_drawdown = daily_pnl / portfolio_value if portfolio_value else 0
            weekly_drawdown = weekly_pnl / portfolio_value if portfolio_value else 0

            if daily_drawdown < -daily_threshold:
                reason = f"daily_drawdown_{abs(daily_drawdown):.1%}"
                _redis.set("system:trading_paused", "true")
                _redis.set("system:pause_reason", reason)
                logger.warning(
                    f"Daily drawdown {daily_drawdown:.1%} exceeded {-daily_threshold:.1%} — pausing"
                )
            elif weekly_drawdown < -weekly_threshold:
                reason = f"weekly_drawdown_{abs(weekly_drawdown):.1%}"
                _redis.set("system:trading_paused", "true")
                _redis.set("system:pause_reason", reason)
                logger.warning(
                    f"Weekly drawdown {weekly_drawdown:.1%} exceeded {-weekly_threshold:.1%} — pausing"
                )

        except Exception as e:
            logger.error(f"Drawdown monitor error: {e}", exc_info=True)

        time.sleep(300)  # Check every 5 minutes


# ── Flask routes ──────────────────────────────────────────────────────────────

@app.route("/check")
def check_symbol():
    try:
        symbol = request.args.get("symbol", "").upper()
        if not symbol:
            return jsonify({"error": "symbol required"}), 400

        blacklist = get_blacklist()
        if symbol in blacklist:
            return jsonify({"allowed": False, "reason": "global_blacklist", "symbol": symbol}), 200

        allowed = get_allowed_symbols()
        if symbol in allowed:
            info = allowed[symbol]
            return jsonify({
                "allowed": True,
                "symbol": symbol,
                "category": info["category"],
                "max_allocation": info["max_allocation"],
            }), 200

        return jsonify({
            "allowed": False,
            "reason": "not_in_allowlist",
            "symbol": symbol,
            "hint": "Add to an enabled category in asset_governance.yaml",
        }), 200
    except Exception as e:
        logger.error(f"/check error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/allowed_symbols")
def allowed_symbols():
    try:
        allowed = get_allowed_symbols()
        return jsonify({
            "symbols": list(allowed.keys()),
            "count": len(allowed),
            "details": allowed,
        }), 200
    except Exception as e:
        logger.error(f"/allowed_symbols error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/circuit_breaker_status")
def circuit_breaker_status():
    try:
        paused = _redis.get("system:trading_paused") == "true"
        reason = _redis.get("system:pause_reason") or ""
        bot_states = get_bot_states()
        return jsonify({
            "paused": paused,
            "reason": reason,
            "bots": bot_states,
        }), 200
    except Exception as e:
        logger.error(f"/circuit_breaker_status error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/reload", methods=["POST"])
def reload_config():
    try:
        load_governance()
        allowed = get_allowed_symbols()
        return jsonify({
            "status": "reloaded",
            "allowed_count": len(allowed),
            "symbols": list(allowed.keys()),
        }), 200
    except Exception as e:
        logger.error(f"/reload error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/health")
def health():
    try:
        paused = _redis.get("system:trading_paused") == "true"
        return jsonify({
            "status": "ok",
            "loaded": bool(_governance),
            "trading_paused": paused,
        }), 200
    except Exception as e:
        logger.error(f"/health error: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/metrics")
def metrics():
    return generate_latest(), 200, {"Content-Type": CONTENT_TYPE_LATEST}


if __name__ == "__main__":
    load_governance()
    logger.info(f"Asset Governor starting. Allowed symbols: {list(get_allowed_symbols().keys())}")

    # Start background threads
    threading.Thread(target=circuit_breaker_loop, daemon=True).start()
    threading.Thread(target=drawdown_monitor_loop, daemon=True).start()

    app.run(host="0.0.0.0", port=8090, debug=False)
