#!/bin/bash
# Run Freqtrade backtests over the last N days.
# Uses --entrypoint freqtrade to bypass entrypoint.sh (which is only for 'trade' mode).
#
# Usage (from project root):
#   bash scripts/run_backtest.sh               # last 90 days, all strategies
#   bash scripts/run_backtest.sh 180           # last 180 days, all strategies
#   bash scripts/run_backtest.sh 90 momentum   # momentum only
#   bash scripts/run_backtest.sh 90 recovery   # recovery only
#   bash scripts/run_backtest.sh 90 scalp      # scalp only

set -e

DAYS=${1:-90}
ONLY=${2:-"all"}

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
# Momentum and Recovery share the same pairs (config.json), one download covers both.
# Scalp uses its own pair list (config_scalp.json).

if [[ "${ONLY}" == "all" || "${ONLY}" == "momentum" || "${ONLY}" == "recovery" ]]; then
  echo "▶ Downloading 4h data for momentum/recovery pairs..."
  docker compose run --rm --no-deps \
    --entrypoint freqtrade \
    freqtrade \
    download-data \
    --config /freqtrade/config.json \
    --timerange "${TIMERANGE}" \
    --timeframe 4h
  echo "✔ Momentum/recovery pair data downloaded"
  echo ""
fi

if [[ "${ONLY}" == "all" || "${ONLY}" == "scalp" ]]; then
  echo "▶ Downloading 4h data for breakout pairs..."
  docker compose run --rm --no-deps \
    --entrypoint freqtrade \
    freqtrade_scalp \
    download-data \
    --config /freqtrade/config.json \
    --timerange "${TIMERANGE}" \
    --timeframe 4h
  echo "✔ Breakout pair data downloaded"
  echo ""
fi

# ── Run backtests ─────────────────────────────────────────────────────────────

if [[ "${ONLY}" == "all" || "${ONLY}" == "momentum" ]]; then
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
  echo ""
fi

if [[ "${ONLY}" == "all" || "${ONLY}" == "recovery" ]]; then
  echo "▶ Running RecoveryMomentumStrategy backtest..."
  docker compose run --rm --no-deps \
    --entrypoint freqtrade \
    freqtrade_recovery \
    backtesting \
    --config /freqtrade/config.json \
    --strategy RecoveryMomentumStrategy \
    --timerange "${TIMERANGE}" \
    --timeframe 4h \
    --export trades \
    --export-filename /freqtrade/user_data/backtest_results/recovery_backtest.json
  echo "✔ RecoveryMomentumStrategy backtest complete"
  echo ""
fi

if [[ "${ONLY}" == "all" || "${ONLY}" == "scalp" ]]; then
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
  echo ""
fi

echo ""
echo "========================================"
echo "  Done. Checking results..."
ls -lh freqtrade/user_data/backtest_results/ 2>/dev/null || echo "  (no files yet)"
echo ""
echo "  Grafana Backtesting panels populate"
echo "  within 60s once files appear above."
echo "========================================"
