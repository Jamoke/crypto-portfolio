#!/bin/bash
# Run Freqtrade backtests for both strategies over the last 90 days.
# Results are written to freqtrade/user_data/backtest_results/ as JSON.
# The backtest_publisher service picks them up and sends them to Prometheus/Grafana.
#
# Usage:
#   ./scripts/run_backtest.sh              # last 90 days, both strategies
#   ./scripts/run_backtest.sh 180          # last 180 days
#   ./scripts/run_backtest.sh 90 momentum  # momentum only
#
# Run from the crypto-portfolio project root.

set -e

DAYS=${1:-90}
ONLY=${2:-"both"}   # "momentum", "scalp", or "both"

# Compute date range: from DAYS ago to today (UTC)
TIMERANGE_START=$(date -u -d "${DAYS} days ago" +%Y%m%d 2>/dev/null \
                  || date -u -v-${DAYS}d +%Y%m%d)   # macOS fallback
TIMERANGE="${TIMERANGE_START}-$(date -u +%Y%m%d)"

echo "========================================"
echo "  Freqtrade Backtest Runner"
echo "  Timerange: ${TIMERANGE} (last ${DAYS} days)"
echo "========================================"

# ── Download data for momentum pairs ──────────────────────────────────────────
echo ""
echo "▶ Downloading OHLCV data for momentum pairs (4h)..."
docker compose run --rm --no-deps freqtrade \
  download-data \
  --config /freqtrade/config.json \
  --timerange "${TIMERANGE}" \
  --timeframe 4h

# ── Download data for scalp/breakout pairs ─────────────────────────────────────
echo ""
echo "▶ Downloading OHLCV data for breakout pairs (4h)..."
docker compose run --rm --no-deps freqtrade_scalp \
  download-data \
  --config /freqtrade/config.json \
  --timerange "${TIMERANGE}" \
  --timeframe 4h

# ── Run MomentumStrategy backtest ─────────────────────────────────────────────
if [[ "${ONLY}" == "both" || "${ONLY}" == "momentum" ]]; then
  echo ""
  echo "▶ Running MomentumStrategy backtest..."
  docker compose run --rm --no-deps freqtrade \
    backtesting \
    --config /freqtrade/config.json \
    --strategy MomentumStrategy \
    --timerange "${TIMERANGE}" \
    --timeframe 4h \
    --export trades \
    --export-filename /freqtrade/user_data/backtest_results/momentum_backtest.json
  echo "✔ MomentumStrategy backtest complete"
fi

# ── Run ScalpStrategy (TrendBreakout) backtest ────────────────────────────────
if [[ "${ONLY}" == "both" || "${ONLY}" == "scalp" ]]; then
  echo ""
  echo "▶ Running ScalpStrategy (TrendBreakout) backtest..."
  docker compose run --rm --no-deps freqtrade_scalp \
    backtesting \
    --config /freqtrade/config.json \
    --strategy ScalpStrategy \
    --timerange "${TIMERANGE}" \
    --timeframe 4h \
    --export trades \
    --export-filename /freqtrade/user_data/backtest_results/scalp_backtest.json
  echo "✔ ScalpStrategy backtest complete"
fi

echo ""
echo "========================================"
echo "  Done. Results written to:"
echo "  freqtrade/user_data/backtest_results/"
echo ""
echo "  Grafana panels update within 60s as"
echo "  backtest_publisher picks up the files."
echo "========================================"
