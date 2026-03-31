"""
Asset Governor Service
Enforces the allowlist and allocation rules defined in asset_governance.yaml.
Exposes a simple REST API that all other services call before acting.

Endpoints:
  GET  /check?symbol=ETH          → {"allowed": true, "reason": "..."}
  GET  /allocation?symbol=ETH     → {"max_allocation": 0.15, "current": 0.08}
  GET  /allowed_symbols            → {"symbols": ["BTC", "ETH", ...]}
  POST /reload                     → Reloads config from disk
"""

import os
import yaml
import logging
from pathlib import Path
from flask import Flask, request, jsonify

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
CONFIG_PATH = Path("/app/config/asset_governance.yaml")

_governance = {}


def load_governance() -> dict:
    global _governance
    try:
        with open(CONFIG_PATH) as f:
            _governance = yaml.safe_load(f) or {}
        logger.info("Asset governance config loaded successfully")
    except Exception as e:
        logger.error(f"Failed to load governance config: {e}")
    return _governance


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

    # Force-allow overrides everything
    for symbol in force_allow:
        if symbol not in blacklist:
            allowed.setdefault(symbol, {"category": "override", "max_allocation": 0.05})

    return allowed


@app.route("/check")
def check_symbol():
    symbol = request.args.get("symbol", "").upper()
    if not symbol:
        return jsonify({"error": "symbol required"}), 400

    blacklist = get_blacklist()
    if symbol in blacklist:
        return jsonify({"allowed": False, "reason": "global_blacklist", "symbol": symbol})

    allowed = get_allowed_symbols()
    if symbol in allowed:
        info = allowed[symbol]
        return jsonify({
            "allowed": True,
            "symbol": symbol,
            "category": info["category"],
            "max_allocation": info["max_allocation"],
        })

    return jsonify({
        "allowed": False,
        "reason": "not_in_allowlist",
        "symbol": symbol,
        "hint": "Add to an enabled category in asset_governance.yaml",
    })


@app.route("/allowed_symbols")
def allowed_symbols():
    allowed = get_allowed_symbols()
    return jsonify({
        "symbols": list(allowed.keys()),
        "count": len(allowed),
        "details": allowed,
    })


@app.route("/reload", methods=["POST"])
def reload_config():
    load_governance()
    allowed = get_allowed_symbols()
    return jsonify({
        "status": "reloaded",
        "allowed_count": len(allowed),
        "symbols": list(allowed.keys()),
    })


@app.route("/health")
def health():
    return jsonify({"status": "ok", "loaded": bool(_governance)})


if __name__ == "__main__":
    load_governance()
    logger.info(f"Asset Governor starting. Allowed symbols: {list(get_allowed_symbols().keys())}")
    app.run(host="0.0.0.0", port=8090, debug=False)
