#!/bin/bash
# Polymarket calibration runner — Phase C.
#
# Scores Claude's probability estimates against resolved Polymarket
# markets. Writes to services/polymarket/data/calibration.db (via the
# polymarket container's /app/data mount), which is then read by
# scripts/calibration_analysis.ipynb.
#
# Usage (from project root):
#   bash scripts/calibrate_polymarket.sh --dry-run --limit 5
#   bash scripts/calibrate_polymarket.sh --limit 25
#   bash scripts/calibrate_polymarket.sh --window-days 180 --limit 500
#   bash scripts/calibrate_polymarket.sh --lag-hours 6 --limit 100
#
# Forwards all flags to services/polymarket/calibrate.py. See that file
# for the full flag list.

set -e

cd "$(dirname "$0")/.."   # project root

# Guardrail: refuse to run if POLYMARKET_PRIVATE_KEY is set. Calibration
# is read-only; the key should never be reachable from this code path.
if [[ -n "${POLYMARKET_PRIVATE_KEY:-}" ]]; then
  echo "✗ POLYMARKET_PRIVATE_KEY is set in the shell environment."
  echo "  Calibration is read-only and must run without signing material."
  echo "  Unset the var (unset POLYMARKET_PRIVATE_KEY) and re-run."
  exit 2
fi

echo "========================================"
echo "  Polymarket Calibration Harness"
echo "  Mode: $([[ "$*" == *--dry-run* ]] && echo "DRY RUN" || echo "LIVE")"
echo "  DB:   services/polymarket/data/calibration.db"
echo "========================================"
echo ""

docker compose run --rm --no-deps \
  -e CALIBRATION_DB=/app/data/calibration.db \
  -e CLAUDE_CUTOFFS=/app/config/claude_cutoffs.yaml \
  --entrypoint python \
  polymarket \
  /app/calibrate.py "$@"

echo ""
echo "========================================"
echo "  Done. Open scripts/calibration_analysis.ipynb"
echo "  to render Brier scores + reliability diagram."
echo "========================================"
