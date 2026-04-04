#!/bin/sh
# Substitute ${VAR} patterns in freqtrade config from environment variables,
# write resolved config to /tmp/cfg.json, then exec freqtrade.
# Usage: entrypoint.sh <strategy> [extra freqtrade args...]
set -e

STRATEGY="${1:-MomentumStrategy}"
shift || true

python3 - <<'PYEOF'
import os, re, sys
src = "/freqtrade/config.json"
dst = "/tmp/cfg.json"
c = open(src).read()
c = re.sub(r'\$\{([A-Z0-9_]+)\}', lambda m: os.environ.get(m.group(1), m.group(0)), c)
open(dst, "w").write(c)
print(f"[entrypoint] Config written to {dst}", file=sys.stderr)
PYEOF

echo "[entrypoint] Starting freqtrade with strategy: $STRATEGY" >&2
exec freqtrade trade --config /tmp/cfg.json --strategy "$STRATEGY" --dry-run "$@"
