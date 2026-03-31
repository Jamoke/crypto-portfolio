"""
Claude Analyst Service
Runs every 6 hours. Pulls market data, sends to Claude for analysis,
publishes structured signals back to Redis.

Phase 1: Basic analysis using CoinGecko data.
Later phases: Add LunarCrush sentiment, on-chain data, news feeds.
"""

import os
import json
import time
import logging
import requests
import redis
import anthropic
import yaml
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")
GOVERNANCE_CONFIG = Path("/app/config/asset_governance.yaml")
STRATEGY_CONFIG = Path("/app/config/strategy_config.yaml")

r = redis.from_url(REDIS_URL, decode_responses=True)
claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# CoinGecko symbol → ID mapping for common assets
COINGECKO_IDS = {
    "BTC": "bitcoin", "ETH": "ethereum", "ARB": "arbitrum",
    "OP": "optimism", "MATIC": "matic-network", "ZK": "zksync",
    "UNI": "uniswap", "AAVE": "aave", "MKR": "maker",
    "CRV": "curve-dao-token", "BAL": "balancer",
    "USDC": "usd-coin", "DAI": "dai",
}


def load_allowed_symbols() -> list[str]:
    """Ask the Asset Governor which symbols are allowed."""
    try:
        resp = requests.get("http://asset_governor:8090/allowed_symbols", timeout=5)
        return resp.json().get("symbols", [])
    except Exception as e:
        logger.warning(f"Could not reach Asset Governor: {e}. Using fallback list.")
        return ["BTC", "ETH", "ARB", "MATIC"]


def fetch_coingecko_data(symbols: list[str]) -> dict:
    """Fetch price + market data from CoinGecko."""
    ids = [COINGECKO_IDS[s] for s in symbols if s in COINGECKO_IDS]
    if not ids:
        return {}

    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": ",".join(ids),
        "order": "market_cap_desc",
        "price_change_percentage": "1h,24h,7d",
        "sparkline": False,
    }
    headers = {}
    if COINGECKO_API_KEY:
        headers["x-cg-demo-api-key"] = COINGECKO_API_KEY

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=10)
        if resp.status_code == 401:
            if not COINGECKO_API_KEY:
                logger.error(
                    "CoinGecko returned 401 Unauthorized. A free API key is now required. "
                    "Sign up at https://www.coingecko.com/api and add "
                    "COINGECKO_API_KEY=your_key to your .env file, then restart."
                )
            else:
                logger.error("CoinGecko 401: API key may be invalid. Check COINGECKO_API_KEY in .env.")
            return {}
        resp.raise_for_status()
        data = resp.json()
        # Index by symbol
        result = {}
        id_to_symbol = {v: k for k, v in COINGECKO_IDS.items()}
        for item in data:
            sym = id_to_symbol.get(item["id"])
            if sym:
                result[sym] = {
                    "price_usd": item["current_price"],
                    "market_cap_usd": item["market_cap"],
                    "volume_24h_usd": item["total_volume"],
                    "change_1h_pct": item.get("price_change_percentage_1h_in_currency"),
                    "change_24h_pct": item.get("price_change_percentage_24h_in_currency"),
                    "change_7d_pct": item.get("price_change_percentage_7d_in_currency"),
                    "ath_change_pct": item.get("ath_change_percentage"),
                }
        return result
    except Exception as e:
        logger.error(f"CoinGecko fetch failed: {e}")
        return {}


def get_tradingview_signals() -> list[dict]:
    """Pull recent TradingView signals from Redis."""
    raw = r.lrange("signals:tradingview:queue", 0, 19)
    return [json.loads(s) for s in raw]


def get_freqtrade_signals() -> dict:
    """Pull recent Freqtrade strategy signals from Redis."""
    signals = {}
    for key in r.scan_iter("signals:momentum:*"):
        try:
            signals[key] = json.loads(r.get(key))
        except Exception:
            pass
    return signals


def run_claude_analysis(market_data: dict, tv_signals: list, ft_signals: dict) -> dict:
    """Send data to Claude and get structured analysis back."""

    prompt = f"""You are a crypto market analyst for a DeFi portfolio system. Analyze the following data and provide trading signals.

## Market Data (as of {datetime.now(timezone.utc).isoformat()})
{json.dumps(market_data, indent=2)}

## TradingView Technical Signals (last 20)
{json.dumps(tv_signals, indent=2)}

## Freqtrade Strategy Signals
{json.dumps(ft_signals, indent=2)}

## Your Task
For each asset in the market data, provide a structured signal. Be specific and concise.

Respond with ONLY valid JSON in this exact format:
{{
  "timestamp": "ISO8601",
  "market_summary": "2-3 sentence overall market assessment",
  "signals": {{
    "BTC": {{
      "signal_strength": 0.6,
      "confidence": 0.75,
      "direction": "bullish",
      "reasoning": "One sentence max",
      "suggested_action": "accumulate",
      "risk_level": "medium"
    }}
  }},
  "macro_notes": "Any important macro factors to flag"
}}

signal_strength: -1.0 (strong sell) to +1.0 (strong buy)
confidence: 0.0 to 1.0
direction: "bullish", "bearish", or "neutral"
suggested_action: "buy", "sell", "hold", "accumulate", "reduce", or "avoid"
risk_level: "low", "medium", "high", or "extreme"
"""

    try:
        message = claude.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}],
        )
        text = message.content[0].text.strip()
        # Strip markdown code fences if present
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text)
    except json.JSONDecodeError as e:
        logger.error(f"Claude returned invalid JSON: {e}")
        return {"error": str(e)}
    except Exception as e:
        logger.error(f"Claude API call failed: {e}")
        return {"error": str(e)}


def publish_signals(analysis: dict):
    """Write Claude signals to Redis for other services."""
    if "error" in analysis:
        logger.error(f"Skipping signal publish due to analysis error: {analysis['error']}")
        return

    # Publish full analysis
    r.setex("claude:analysis:latest", 86400, json.dumps(analysis))

    # Publish individual signals
    for symbol, signal in analysis.get("signals", {}).items():
        key = f"claude:signals:{symbol}"
        signal["symbol"] = symbol
        signal["source"] = "claude_analyst"
        signal["timestamp"] = analysis.get("timestamp", datetime.utcnow().isoformat())
        r.setex(key, 21600, json.dumps(signal))  # 6h TTL

    logger.info(f"Published Claude analysis for {len(analysis.get('signals', {}))} symbols")


def run_analysis_cycle():
    """One full analysis cycle."""
    logger.info("Starting analysis cycle...")

    symbols = load_allowed_symbols()
    # Filter to just tradeable (non-stablecoin) assets
    stables = {"USDC", "DAI", "USDT"}
    tradeable = [s for s in symbols if s not in stables and s in COINGECKO_IDS]

    if not tradeable:
        logger.warning("No tradeable symbols found. Check asset_governance.yaml.")
        return

    logger.info(f"Analyzing: {tradeable}")

    market_data = fetch_coingecko_data(tradeable)
    tv_signals = get_tradingview_signals()
    ft_signals = get_freqtrade_signals()

    if not market_data and not tv_signals and not ft_signals:
        logger.warning(
            "No market data or signals available — skipping Claude API call to conserve credits. "
            "Check COINGECKO_API_KEY in .env and ensure other services are running."
        )
        return

    analysis = run_claude_analysis(market_data, tv_signals, ft_signals)
    publish_signals(analysis)

    logger.info(f"Analysis cycle complete. Market summary: {analysis.get('market_summary', 'N/A')}")


if __name__ == "__main__":
    logger.info("Claude Analyst Service started")

    # Wait for other services
    time.sleep(15)

    # Run immediately, then every 6 hours
    INTERVAL = 6 * 3600  # 6 hours

    while True:
        try:
            run_analysis_cycle()
        except Exception as e:
            logger.error(f"Analysis cycle failed: {e}", exc_info=True)

        logger.info(f"Next analysis in {INTERVAL // 3600} hours")
        time.sleep(INTERVAL)
