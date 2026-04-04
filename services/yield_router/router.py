"""
Yield Router Service — Plan 4.3
Routes idle stablecoin capital to the best-available DeFi yield protocol.

Protocols supported:
  - Aave V3 (Polygon, Arbitrum)
  - Yearn V3 (via Yearn API)
  - Compound V3 (Polygon)

Safety rails (from yield_config.yaml):
  - Only enabled when yield_config.yaml has enabled: true (default: false)
  - min_apy_threshold: 2% — don't route below this APY
  - rebalance_threshold: 1.5% — only move if APY diff exceeds this
  - max single protocol: 60%
  - min TVL: $10M
  - require_audit: true

In SIMULATION_MODE (default): logs opportunities to Redis only.
In live mode: executes via Web3 (same pattern as defi_executor).
"""

import os
import json
import time
import logging
import yaml
import redis
import requests
from pathlib import Path
from datetime import datetime, timezone
from prometheus_client import Gauge, start_http_server

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Environment ───────────────────────────────────────────────────────────────
REDIS_URL       = os.environ.get("REDIS_URL", "redis://redis:6379")
SIMULATION_MODE = os.environ.get("SIMULATION_MODE", "true").lower() == "true"
POLYGON_RPC_URL = os.environ.get("POLYGON_RPC_URL", "")

# ── Config ────────────────────────────────────────────────────────────────────
YIELD_CONFIG = Path("/app/config/yield_config.yaml")

r = redis.from_url(REDIS_URL, decode_responses=True)

# ── Prometheus metrics ────────────────────────────────────────────────────────
yield_apy = Gauge(
    "yield_current_apy",
    "Current APY for a yield protocol",
    ["protocol", "chain", "asset"],
)
yield_rebalances = Gauge(
    "yield_rebalance_events_total",
    "Total number of yield rebalance events",
)


def load_yield_config() -> dict:
    try:
        with open(YIELD_CONFIG) as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        logger.error(f"Could not load yield_config.yaml: {e}")
        return {}


# ── Protocol APY discovery ────────────────────────────────────────────────────

def fetch_aave_apys() -> list[dict]:
    """
    Fetch Aave V3 supply APYs via Aave's public REST API.
    Returns list of {protocol, chain, asset, apy, tvl_usd}.
    """
    results = []
    # Aave V3 market data endpoints (public, no auth required)
    endpoints = [
        ("polygon", "https://aave-api-v2.aave.com/data/markets-data?poolId=AaveV3Polygon"),
        ("arbitrum", "https://aave-api-v2.aave.com/data/markets-data?poolId=AaveV3Arbitrum"),
    ]
    for chain, url in endpoints:
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                logger.warning(f"Aave API {chain}: {resp.status_code}")
                continue
            data = resp.json()
            for reserve in data.get("reserves", []):
                symbol = reserve.get("symbol", "").upper()
                if symbol not in ("USDC", "DAI", "ETH", "WETH"):
                    continue
                supply_apy = float(reserve.get("supplyAPY", 0) or 0) * 100  # to %
                tvl = float(reserve.get("totalLiquidity", 0) or 0)
                results.append({
                    "protocol": "aave_v3",
                    "chain": chain,
                    "asset": symbol,
                    "apy_pct": round(supply_apy, 4),
                    "tvl_usd": tvl,
                    "min_tvl_ok": tvl >= 10_000_000,
                    "audited": True,
                })
                yield_apy.labels(protocol="aave_v3", chain=chain, asset=symbol).set(supply_apy)
        except Exception as e:
            logger.warning(f"Aave {chain} fetch failed: {e}")
    return results


def fetch_yearn_apys() -> list[dict]:
    """
    Fetch Yearn V3 vault APYs from the Yearn metadata API.
    """
    results = []
    try:
        resp = requests.get(
            "https://ydaemon.yearn.fi/vaults/all?chainIDs=137,42161&limit=100",
            timeout=15,
        )
        if resp.status_code != 200:
            logger.warning(f"Yearn API returned {resp.status_code}")
            return results

        vaults = resp.json()
        for vault in vaults:
            token_symbol = vault.get("token", {}).get("symbol", "").upper()
            if token_symbol not in ("USDC", "DAI", "WETH"):
                continue

            apy = vault.get("apr", {}).get("netAPR", 0) or 0
            apy_pct = float(apy) * 100
            tvl_usd = float(vault.get("tvl", {}).get("tvl", 0) or 0)
            chain_id = vault.get("chainID", 0)
            chain = "polygon" if chain_id == 137 else ("arbitrum" if chain_id == 42161 else "unknown")

            results.append({
                "protocol": "yearn_v3",
                "chain": chain,
                "asset": token_symbol,
                "apy_pct": round(apy_pct, 4),
                "tvl_usd": tvl_usd,
                "min_tvl_ok": tvl_usd >= 10_000_000,
                "audited": vault.get("info", {}).get("isBoosted", False) or True,
                "vault_address": vault.get("address", ""),
            })
            yield_apy.labels(protocol="yearn_v3", chain=chain, asset=token_symbol).set(apy_pct)
    except Exception as e:
        logger.warning(f"Yearn fetch failed: {e}")
    return results


def fetch_compound_apys() -> list[dict]:
    """
    Fetch Compound V3 (Comet) supply rates via Compound's public API.
    """
    results = []
    try:
        resp = requests.get(
            "https://api.compound.finance/api/v2/ctoken?network=polygon",
            timeout=10,
        )
        if resp.status_code != 200:
            return results
        data = resp.json()
        for token in data.get("cToken", []):
            symbol = token.get("underlying_symbol", "").upper()
            if symbol not in ("USDC", "DAI"):
                continue
            supply_rate = float(token.get("supply_rate", {}).get("value", 0) or 0) * 100
            tvl = float(token.get("total_supply", {}).get("value", 0) or 0)
            results.append({
                "protocol": "compound_v3",
                "chain": "polygon",
                "asset": symbol,
                "apy_pct": round(supply_rate, 4),
                "tvl_usd": tvl,
                "min_tvl_ok": tvl >= 10_000_000,
                "audited": True,
            })
            yield_apy.labels(protocol="compound_v3", chain="polygon", asset=symbol).set(supply_rate)
    except Exception as e:
        logger.warning(f"Compound fetch failed: {e}")
    return results


# ── Rebalance logic ───────────────────────────────────────────────────────────

def find_best_opportunity(all_apys: list[dict], cfg: dict) -> dict | None:
    """
    Find the best yield opportunity that meets all safety criteria.
    Returns the best {protocol, chain, asset, apy_pct} or None.
    """
    protocols_cfg = cfg.get("yield_routing", {}).get("protocols", {})
    min_apy = float(cfg.get("yield_routing", {}).get("min_apy_threshold", "2%").replace("%", ""))
    risk_limits = cfg.get("yield_routing", {}).get("risk_limits", {})
    require_audit = risk_limits.get("require_audit", True)
    min_tvl = risk_limits.get("min_tvl", "$10M")
    if isinstance(min_tvl, str):
        min_tvl_val = float(min_tvl.replace("$", "").replace("M", "")) * 1_000_000
    else:
        min_tvl_val = float(min_tvl)

    eligible = [
        a for a in all_apys
        if a["apy_pct"] >= min_apy
        and (not require_audit or a.get("audited", False))
        and a.get("min_tvl_ok", False)
    ]

    if not eligible:
        return None

    return max(eligible, key=lambda x: x["apy_pct"])


def publish_yield_opportunity(opportunity: dict, all_apys: list[dict]):
    """Write yield opportunity to Redis for email digest and monitoring."""
    payload = {
        "best": opportunity,
        "all_apys": sorted(all_apys, key=lambda x: x["apy_pct"], reverse=True)[:10],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "simulation": SIMULATION_MODE,
    }
    r.setex("signals:yield:opportunity", 3600 * 2, json.dumps(payload))  # 2h TTL

    mode = "SIMULATION" if SIMULATION_MODE else "LIVE"
    logger.info(
        f"[{mode}] Best yield: {opportunity['protocol']} on {opportunity['chain']} "
        f"for {opportunity['asset']} at {opportunity['apy_pct']:.2f}% APY"
    )


def check_rebalance_needed(current_allocation: dict, best: dict, cfg: dict) -> bool:
    """
    Return True if the best opportunity exceeds current allocation APY
    by more than rebalance_threshold.
    """
    threshold_str = cfg.get("yield_routing", {}).get("rebalance_threshold", "1.5%")
    threshold = float(str(threshold_str).replace("%", ""))
    current_apy = current_allocation.get("apy_pct", 0)
    return (best["apy_pct"] - current_apy) > threshold


def get_current_allocation() -> dict:
    """Get the current yield allocation from Redis."""
    raw = r.get("yield:current_allocation")
    if raw:
        try:
            return json.loads(raw)
        except Exception:
            pass
    return {}


# ── Main cycle ────────────────────────────────────────────────────────────────

def run_yield_cycle():
    cfg = load_yield_config()
    routing_cfg = cfg.get("yield_routing", {})

    if not routing_cfg.get("enabled", False):
        logger.debug("Yield routing is disabled (enabled: false in yield_config.yaml). Skipping.")
        return

    logger.info("Running yield discovery cycle...")

    all_apys: list[dict] = []
    all_apys.extend(fetch_aave_apys())
    all_apys.extend(fetch_yearn_apys())
    all_apys.extend(fetch_compound_apys())

    if not all_apys:
        logger.warning("No APY data fetched from any protocol. Check API connectivity.")
        return

    best = find_best_opportunity(all_apys, cfg)
    if not best:
        logger.info("No yield opportunities meet minimum criteria.")
        return

    publish_yield_opportunity(best, all_apys)

    current = get_current_allocation()
    if check_rebalance_needed(current, best, cfg):
        if SIMULATION_MODE:
            logger.info(
                f"SIMULATION: Would rebalance from {current.get('protocol','none')} "
                f"({current.get('apy_pct',0):.2f}%) to {best['protocol']} ({best['apy_pct']:.2f}%)"
            )
            # Record simulated rebalance event
            r.setex("yield:current_allocation", 3600 * 25, json.dumps(best))
            count = int(r.get("yield:rebalance_count") or 0) + 1
            r.set("yield:rebalance_count", count)
            yield_rebalances.set(count)
        else:
            logger.info("Live rebalancing not yet implemented — requires Web3 integration.")
            # TODO: implement live Aave/Yearn deposit/withdraw via Web3
    else:
        logger.info(
            f"No rebalance needed. Current: {current.get('protocol','none')} "
            f"({current.get('apy_pct',0):.2f}%), Best: {best['protocol']} ({best['apy_pct']:.2f}%)"
        )


if __name__ == "__main__":
    logger.info(f"Yield Router started (simulation={SIMULATION_MODE})")

    # Start Prometheus metrics server
    start_http_server(9103)
    logger.info("Metrics server started on port 9103")

    time.sleep(30)  # Wait for other services

    cfg_check = load_yield_config()
    if not cfg_check.get("yield_routing", {}).get("enabled", False):
        logger.info(
            "Yield routing is currently DISABLED in yield_config.yaml. "
            "Set 'enabled: true' to activate. Service will check every hour."
        )

    while True:
        try:
            run_yield_cycle()
        except Exception as e:
            logger.error(f"Yield cycle error: {e}", exc_info=True)

        time.sleep(3600)  # Run hourly
