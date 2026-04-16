#!/bin/bash
# Run Freqtrade backtests for both strategies over the last 90 days.
# Uses --entrypoint freqtrade to bypass entrypoint.sh (which is only for 'trade' mode).
#
# Usage (from project root):
#   bash scripts/run_backtest.sh          # last 90 days, both strategies
#   bash scripts/run_backtest.sh 180      # last 180 days
#   bash scripts/run_backtest.sh 90 momentum   # momentum only

set -e

DAYS=${1:-90}
ONLY=${2:-"both"}

# Date range: DAYS ago → today (UTC). Supports Linux and macOS.
if date -u -d "1 day ago" +%Y%m%d >/dev/null 2>&1; then
  START=$(date -u -d "${DAYS} days ago" +%Y%m%d)   # Linux (GNU date)
else
  START=$(date -u -v-${DAYS}d +%Y%m%d)              # macOS (BSD date)
fi
END=$(date -u +%Y%m%d)
TIMERANGE="${START}-${END}"

echo "========================================"
echo "  Freqtrade Backtest Runner"
echo "  Timerange: ${TIMERANGE} (last ${DAYS} days)"
echo "  Bypassing entrypoint.sh via --entrypoint freqtrade"
echo "========================================"
echo ""

# ── Download OHLCV data ───────────────────────────────────────────────────────
# Both bots share Gate.io but use different pair lists from their config files.

echo "▶ Downloading 4h data for momentum pairs..."
docker compose run --rm --no-deps \
  --entrypoint freqtrade \
  freqtrade \
  download-data \
  --config /freqtrade/config.json \
  --timerange "${TIMERANGE}" \
  --timeframe 4h
echo "✔ Momentum pair data downloaded"

echo ""
echo "▶ Downloading 4h data for breakout pairs..."
docker compose run --rm --no-deps \
  --entrypoint freqtrade \
  freqtrade_scalp \
  download-data \
  --config /freqtrade/config.json \
  --timerange "${TIMERANGE}" \
  --timeframe 4h
echo "✔ Breakout pair data downloaded"

# ── Run backtests ─────────────────────────────────────────────────────────────

if [[ "${ONLY}" == "both" || "${ONLY}" == "momentum" ]]; then
  echo ""
  echo "▶ Running MomentumStrategy backtest..."
  docker compose run --rm --no-deps \
    --entrypoint freqtrade \
    freqtrade \
    backtesting \
    --config /freqtrade/config.json \
    --strategy MomentumStrategy \
    --timerange "${TIMERANGE}" \
    --timeframe 4h \
    --export trades \
    --export-filename /freqtrade/user_data/backtest_results/momentum_backtest.json
  echo "✔ MomentumStrategy backtest complete"
fi

if [[ "${ONLY}" == "both" || "${ONLY}" == "scalp" ]]; then
  echo ""
  echo "▶ Running ScalpStrategy (TrendBreakout) backtest..."
  docker compose run --rm --no-deps \
    --entrypoint freqtrade \
    freqtrade_scalp \
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
echo "  Done. Checking results..."
ls -lh freqtrade/user_data/backtest_results/ 2>/dev/null || echo "  (no files yet)"
echo ""
echo "  Grafana Backtesting panels populate"
echo "  within 60s once files appear above."
echo "========================================"
