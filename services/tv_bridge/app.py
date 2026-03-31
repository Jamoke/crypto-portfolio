"""
TradingView Webhook Bridge
Listens for incoming webhook alerts from TradingView and publishes
them to Redis so the strategy engine can consume them.

TradingView alert webhook URL format:
  http://YOUR_PI_IP:8443/webhook?secret=YOUR_SECRET

Expected JSON payload from TradingView Pine Script:
{
  "symbol": "ETHUSD",
  "action": "buy",          // "buy", "sell", or "neutral"
  "indicator": "supertrend",
  "timeframe": "4h",
  "price": 3200.50,
  "message": "Optional human-readable message"
}
"""

import os
import json
import hmac
import hashlib
import logging
import redis
from datetime import datetime
from flask import Flask, request, jsonify, abort

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)

WEBHOOK_SECRET = os.environ.get("TV_WEBHOOK_SECRET", "")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")
PORT = int(os.environ.get("WEBHOOK_PORT", 8443))

r = redis.from_url(REDIS_URL, decode_responses=True)


def validate_secret(provided_secret: str) -> bool:
    """Constant-time comparison to prevent timing attacks."""
    if not WEBHOOK_SECRET:
        logger.warning("No TV_WEBHOOK_SECRET set — accepting all webhooks (unsafe!)")
        return True
    return hmac.compare_digest(provided_secret or "", WEBHOOK_SECRET)


@app.route("/webhook", methods=["POST"])
def webhook():
    """Receive TradingView alert and publish to Redis."""

    # Validate secret
    secret = request.args.get("secret", "")
    if not validate_secret(secret):
        logger.warning(f"Rejected webhook with invalid secret from {request.remote_addr}")
        abort(403)

    # Parse payload
    try:
        payload = request.get_json(force=True)
        if not payload:
            abort(400)
    except Exception:
        abort(400)

    # Validate required fields
    required = ["symbol", "action", "indicator"]
    missing = [f for f in required if f not in payload]
    if missing:
        return jsonify({"error": f"Missing fields: {missing}"}), 400

    # Normalize action
    action = payload["action"].lower()
    if action not in ("buy", "sell", "neutral"):
        return jsonify({"error": "action must be buy, sell, or neutral"}), 400

    # Build signal record
    signal = {
        "symbol": payload["symbol"].upper(),
        "action": action,
        "indicator": payload.get("indicator", "unknown"),
        "timeframe": payload.get("timeframe", "unknown"),
        "price": payload.get("price"),
        "message": payload.get("message", ""),
        "received_at": datetime.utcnow().isoformat(),
        "source": "tradingview",
    }

    # Publish to Redis (expires in 4 hours per strategy_config default)
    symbol_key = signal["symbol"].replace("/", "_").replace(":", "_")
    redis_key = f"signals:tradingview:{symbol_key}"
    r.setex(redis_key, 14400, json.dumps(signal))  # 4h TTL

    # Also push to a list so consumers can process in order
    r.lpush("signals:tradingview:queue", json.dumps(signal))
    r.ltrim("signals:tradingview:queue", 0, 99)  # Keep last 100

    logger.info(f"TV signal received: {signal['symbol']} {action.upper()} via {signal['indicator']}")

    return jsonify({"status": "accepted", "signal": signal}), 200


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint."""
    try:
        r.ping()
        redis_ok = True
    except Exception:
        redis_ok = False

    return jsonify({
        "status": "ok" if redis_ok else "degraded",
        "redis": redis_ok,
        "timestamp": datetime.utcnow().isoformat(),
    })


@app.route("/signals", methods=["GET"])
def recent_signals():
    """Show last 10 received TradingView signals (for debugging)."""
    raw = r.lrange("signals:tradingview:queue", 0, 9)
    signals = [json.loads(s) for s in raw]
    return jsonify({"signals": signals, "count": len(signals)})


if __name__ == "__main__":
    logger.info(f"TradingView Bridge starting on port {PORT}")
    app.run(host="0.0.0.0", port=PORT, debug=False)
