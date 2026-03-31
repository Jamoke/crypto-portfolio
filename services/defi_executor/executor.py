"""
DeFi Executor Service
Watches Redis for signals from Freqtrade + Claude Analyst.
In SIMULATION_MODE (Phase 1), logs what WOULD have been executed.
In live mode, submits transactions via 1inch on Polygon/Arbitrum.

SIMULATION_MODE=true is the default and safe for Phase 1.
Never set SIMULATION_MODE=false until Phase 3 testing is complete.
"""

import os
import json
import time
import logging
import requests
import redis
import yaml
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SIMULATION_MODE = os.environ.get("SIMULATION_MODE", "true").lower() == "true"
WALLET_ADDRESS = os.environ.get("WALLET_ADDRESS", "")
POLYGON_RPC_URL = os.environ.get("POLYGON_RPC_URL", "")
ARBITRUM_RPC_URL = os.environ.get("ARBITRUM_RPC_URL", "")
ONEINCH_API_KEY = os.environ.get("ONEINCH_API_KEY", "")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")

RISK_CONFIG = Path("/app/config/risk_config.yaml")
STRATEGY_CONFIG = Path("/app/config/strategy_config.yaml")

r = redis.from_url(REDIS_URL, decode_responses=True)


def load_risk_config() -> dict:
    try:
        with open(RISK_CONFIG) as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        logger.error(f"Could not load risk config: {e}")
        return {}


def check_asset_allowed(symbol: str) -> tuple[bool, str]:
    """Check with Asset Governor if this symbol can be traded."""
    try:
        resp = requests.get(
            f"http://asset_governor:8090/check",
            params={"symbol": symbol},
            timeout=5,
        )
        data = resp.json()
        return data.get("allowed", False), data.get("reason", "unknown")
    except Exception as e:
        logger.error(f"Asset Governor check failed: {e}")
        return False, "governor_unavailable"


def evaluate_signal(symbol: str, claude_signal: dict, momentum_signal: dict, risk: dict) -> Optional[dict]:
    """
    Combine Claude and momentum signals into an action decision.
    Returns None if no trade should be placed.
    """
    strategy_cfg = {}
    try:
        with open(STRATEGY_CONFIG) as f:
            cfg = yaml.safe_load(f) or {}
            strategy_cfg = cfg.get("strategies", {}).get("claude_alpha", {})
    except Exception:
        pass

    min_confidence = strategy_cfg.get("min_confidence", 0.70)
    min_strength = strategy_cfg.get("min_signal_strength", 0.40)
    require_confirmation = strategy_cfg.get("require_technical_confirmation", True)

    cs = claude_signal.get("signal_strength", 0)
    cc = claude_signal.get("confidence", 0)
    action = claude_signal.get("suggested_action", "hold")

    # Check minimums
    if cc < min_confidence:
        logger.debug(f"{symbol}: Claude confidence {cc} < threshold {min_confidence}")
        return None
    if abs(cs) < min_strength:
        logger.debug(f"{symbol}: Signal strength {cs} < threshold {min_strength}")
        return None

    # Technical confirmation check
    if require_confirmation and momentum_signal:
        m_enter = momentum_signal.get("enter_long", 0)
        m_exit = momentum_signal.get("exit_long", 0)
        if cs > 0 and not m_enter:
            logger.debug(f"{symbol}: Claude bullish but no Freqtrade entry signal")
            return None
        if cs < 0 and not m_exit:
            logger.debug(f"{symbol}: Claude bearish but no Freqtrade exit signal")
            return None

    max_alloc = risk.get("position_limits", {}).get("max_per_asset", 0.15)
    max_gas = risk.get("defi_specific", {}).get("max_gas_per_trade_usd", 15)

    return {
        "symbol": symbol,
        "action": action,
        "signal_strength": cs,
        "confidence": cc,
        "max_allocation": max_alloc,
        "max_gas_usd": max_gas,
        "reasoning": claude_signal.get("reasoning", ""),
        "decided_at": datetime.now(timezone.utc).isoformat(),
    }


def simulate_execution(decision: dict):
    """Log a simulated trade (SIMULATION_MODE=true). No real transactions."""
    log_entry = {
        "mode": "SIMULATION",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "decision": decision,
        "note": "Would have executed on-chain via 1inch. Set SIMULATION_MODE=false for live trading.",
    }
    logger.info(f"[SIM] WOULD EXECUTE: {decision['symbol']} {decision['action'].upper()} "
                f"(confidence={decision['confidence']:.0%}, strength={decision['signal_strength']:+.2f})")

    # Store simulated trades for digest reporting
    r.lpush("executor:simulated_trades", json.dumps(log_entry))
    r.ltrim("executor:simulated_trades", 0, 499)


def process_signals():
    """One pass: check all symbols with active signals."""
    risk = load_risk_config()

    # Get all Claude signals
    for key in r.scan_iter("claude:signals:*"):
        try:
            claude_signal = json.loads(r.get(key))
            symbol = claude_signal.get("symbol", "").upper()
            if not symbol:
                continue

            # Check allowlist
            allowed, reason = check_asset_allowed(symbol)
            if not allowed:
                logger.debug(f"{symbol}: blocked by Asset Governor ({reason})")
                continue

            # Get matching Freqtrade momentum signal
            momentum_key = f"signals:momentum:{symbol}_USDC"
            momentum_signal = {}
            raw = r.get(momentum_key)
            if raw:
                momentum_signal = json.loads(raw)

            # Decide
            decision = evaluate_signal(symbol, claude_signal, momentum_signal, risk)
            if decision is None:
                continue

            if SIMULATION_MODE:
                simulate_execution(decision)
            else:
                # Phase 3+: call live execution
                logger.warning("Live execution not yet implemented. Set SIMULATION_MODE=true.")

        except Exception as e:
            logger.error(f"Error processing signal {key}: {e}", exc_info=True)


if __name__ == "__main__":
    mode = "SIMULATION" if SIMULATION_MODE else "LIVE"
    logger.info(f"DeFi Executor started in {mode} mode")

    if not SIMULATION_MODE and not WALLET_ADDRESS:
        logger.error("WALLET_ADDRESS required for live mode. Exiting.")
        exit(1)

    if SIMULATION_MODE:
        logger.info("Running in SIMULATION mode. No real transactions will occur.")

    time.sleep(20)  # Wait for other services

    while True:
        try:
            process_signals()
        except Exception as e:
            logger.error(f"Signal processing error: {e}", exc_info=True)

        time.sleep(60)  # Check signals every minute
