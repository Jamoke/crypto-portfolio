"""
Backtest Publisher
Reads Freqtrade backtest result JSON files and exposes key metrics
as Prometheus gauges so Grafana can display strategy evaluation data
without requiring live trades.

Rescans /results/ every 60s so new backtests appear automatically.

Metrics exposed on :9105/metrics:
  backtest_total_trades{strategy}
  backtest_win_rate{strategy}          0.0–1.0
  backtest_total_profit_pct{strategy}  total % gain on starting capital
  backtest_profit_factor{strategy}     gross wins / gross losses
  backtest_max_drawdown_pct{strategy}  0.0–1.0
  backtest_avg_profit_pct{strategy}    average profit % per trade
  backtest_best_trade_pct{strategy}
  backtest_worst_trade_pct{strategy}
  backtest_avg_duration_hours{strategy}
  backtest_sharpe{strategy}
  backtest_days_covered{strategy}      how many calendar days the backtest spans
  backtest_trade_profit_pct{strategy, pair, exit_reason, idx}  per-trade detail
  backtest_last_updated{strategy}      unix timestamp of last file read
"""

import os
import json
import time
import glob
import logging
import zipfile
from pathlib import Path
from datetime import datetime
from flask import Flask, Response
from prometheus_client import (
    Gauge, generate_latest, CONTENT_TYPE_LATEST, REGISTRY
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

RESULTS_DIR = Path(os.environ.get("RESULTS_DIR", "/results"))
SCAN_INTERVAL = int(os.environ.get("SCAN_INTERVAL", 60))
PORT = int(os.environ.get("PORT", 9105))

# ── Prometheus metrics ─────────────────────────────────────────────────────────
_L = ["strategy"]
_LP = ["strategy", "pair", "exit_reason", "idx"]

bt_total_trades     = Gauge("backtest_total_trades",      "Total trades in backtest",         _L)
bt_win_rate         = Gauge("backtest_win_rate",          "Win rate 0.0-1.0",                 _L)
bt_total_profit_pct = Gauge("backtest_total_profit_pct",  "Total profit % on starting capital",_L)
bt_profit_factor    = Gauge("backtest_profit_factor",     "Gross wins / gross losses",         _L)
bt_max_drawdown     = Gauge("backtest_max_drawdown_pct",  "Max drawdown 0.0-1.0",             _L)
bt_avg_profit       = Gauge("backtest_avg_profit_pct",    "Average profit % per trade",        _L)
bt_best_trade       = Gauge("backtest_best_trade_pct",    "Best single trade profit %",        _L)
bt_worst_trade      = Gauge("backtest_worst_trade_pct",   "Worst single trade profit %",       _L)
bt_avg_duration     = Gauge("backtest_avg_duration_hours","Avg trade duration in hours",       _L)
bt_sharpe           = Gauge("backtest_sharpe",            "Sharpe ratio",                      _L)
bt_days             = Gauge("backtest_days_covered",      "Calendar days the backtest spans",  _L)
bt_updated          = Gauge("backtest_last_updated",      "Unix timestamp of last file parse", _L)
bt_trade_profit     = Gauge("backtest_trade_profit_pct",  "Individual trade profit %",         _LP)


def _safe(val, default=0.0):
    try:
        return float(val) if val is not None else default
    except (TypeError, ValueError):
        return default


def parse_backtest_file(path: Path) -> dict | None:
    """
    Parse a Freqtrade backtest result file (.zip or .json).
    Freqtrade stable (2025+) writes results as compressed .zip files.
    Returns dict of {strategy_name: parsed_data} or None on error.
    """
    try:
        if path.suffix == ".zip":
            with zipfile.ZipFile(path) as zf:
                json_names = [n for n in zf.namelist() if n.endswith(".json")]
                if not json_names:
                    logger.warning(f"{path}: no JSON found inside zip")
                    return None
                raw = json.loads(zf.read(json_names[0]))
        else:
            raw = json.loads(path.read_text())
    except Exception as e:
        logger.warning(f"Cannot read {path}: {e}")
        return None

    # Freqtrade backtest JSON structure varies slightly by version.
    # The 'strategy' key maps strategy names to their results.
    strategies = raw.get("strategy") or raw.get("strategy_comparison")
    if not strategies:
        logger.warning(f"{path}: no 'strategy' key found — skipping")
        return None

    if isinstance(strategies, list):
        # strategy_comparison format — convert to dict
        strategies = {s["key"]: s for s in strategies}

    results = {}
    for name, data in strategies.items():
        metrics = data.get("results_metrics", data)  # some versions flatten this
        trades  = data.get("trades", [])

        wins   = int(_safe(metrics.get("winning_trades", metrics.get("wins", 0))))
        losses = int(_safe(metrics.get("losing_trades", metrics.get("losses", 0))))
        total  = wins + losses

        # Profit factor
        gross_profit = sum(_safe(t.get("profit_abs", 0)) for t in trades if _safe(t.get("profit_abs", 0)) > 0)
        gross_loss   = abs(sum(_safe(t.get("profit_abs", 0)) for t in trades if _safe(t.get("profit_abs", 0)) < 0))
        pf = gross_profit / gross_loss if gross_loss > 0 else 0.0

        # Average duration
        durations = []
        for t in trades:
            try:
                open_dt  = datetime.fromisoformat(str(t.get("open_date", "")).replace("Z", "+00:00"))
                close_dt = datetime.fromisoformat(str(t.get("close_date", "")).replace("Z", "+00:00"))
                durations.append((close_dt - open_dt).total_seconds() / 3600)
            except Exception:
                pass
        avg_dur = sum(durations) / len(durations) if durations else 0.0

        # Date range
        try:
            dates = [t.get("open_date", "") for t in trades if t.get("open_date")]
            if dates:
                first = datetime.fromisoformat(str(min(dates)).replace("Z", "+00:00"))
                last  = datetime.fromisoformat(str(max(dates)).replace("Z", "+00:00"))
                days  = max(1, (last - first).days)
            else:
                days = 0
        except Exception:
            days = 0

        profits = [_safe(t.get("profit_ratio", t.get("profit_pct", 0))) * 100 for t in trades]

        results[name] = {
            "total_trades":     total,
            "wins":             wins,
            "losses":           losses,
            "win_rate":         wins / total if total > 0 else 0.0,
            "total_profit_pct": _safe(metrics.get("profit_total_abs", metrics.get("profit_total", 0))),
            "profit_factor":    pf,
            "max_drawdown":     _safe(metrics.get("max_drawdown", metrics.get("max_drawdown_abs", 0))),
            "avg_profit_pct":   sum(profits) / len(profits) if profits else 0.0,
            "best_trade_pct":   max(profits) if profits else 0.0,
            "worst_trade_pct":  min(profits) if profits else 0.0,
            "avg_duration_hrs": avg_dur,
            "sharpe":           _safe(metrics.get("sharpe", 0)),
            "days_covered":     days,
            "trades":           trades,
        }
        logger.info(
            f"Parsed {name}: {total} trades, "
            f"win_rate={results[name]['win_rate']:.1%}, "
            f"avg_profit={results[name]['avg_profit_pct']:.2f}%"
        )

    return results


def publish_metrics(strategy: str, data: dict):
    """Write one strategy's backtest data to Prometheus gauges."""
    s = strategy
    bt_total_trades.labels(strategy=s).set(data["total_trades"])
    bt_win_rate.labels(strategy=s).set(data["win_rate"])
    bt_total_profit_pct.labels(strategy=s).set(data["total_profit_pct"])
    bt_profit_factor.labels(strategy=s).set(data["profit_factor"])
    bt_max_drawdown.labels(strategy=s).set(data["max_drawdown"])
    bt_avg_profit.labels(strategy=s).set(data["avg_profit_pct"])
    bt_best_trade.labels(strategy=s).set(data["best_trade_pct"])
    bt_worst_trade.labels(strategy=s).set(data["worst_trade_pct"])
    bt_avg_duration.labels(strategy=s).set(data["avg_duration_hrs"])
    bt_sharpe.labels(strategy=s).set(data["sharpe"])
    bt_days.labels(strategy=s).set(data["days_covered"])
    bt_updated.labels(strategy=s).set(time.time())

    # Per-trade profit — last 100 trades to limit label cardinality
    trades = data["trades"][-100:]
    for i, trade in enumerate(trades):
        profit = _safe(trade.get("profit_ratio", trade.get("profit_pct", 0))) * 100
        pair   = str(trade.get("pair", "unknown")).replace("/", "_")
        reason = str(trade.get("exit_reason", trade.get("sell_reason", "unknown")))
        bt_trade_profit.labels(strategy=s, pair=pair, exit_reason=reason, idx=str(i)).set(profit)


def scan_and_publish():
    """Find all backtest result files (.zip or .json) and publish their metrics."""
    files = [
        p for p in sorted(RESULTS_DIR.glob("*"))
        if p.suffix == ".zip" or (p.suffix == ".json" and not p.stem.endswith(".meta"))
    ]
    if not files:
        logger.info(f"No backtest result files found in {RESULTS_DIR}")
        return

    for path in sorted(files):
        logger.info(f"Processing {path.name}...")
        parsed = parse_backtest_file(path)
        if not parsed:
            continue
        for strategy, data in parsed.items():
            publish_metrics(strategy, data)


# ── Flask metrics server ───────────────────────────────────────────────────────
app = Flask("backtest_publisher")

@app.route("/metrics")
def metrics():
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)

@app.route("/health")
def health():
    return {"status": "ok", "results_dir": str(RESULTS_DIR)}, 200


def run_server():
    import logging as _log
    _log.getLogger("werkzeug").setLevel(_log.ERROR)
    app.run(host="0.0.0.0", port=PORT, debug=False)


if __name__ == "__main__":
    import threading
    logger.info(f"Backtest Publisher starting on :{PORT}")
    logger.info(f"Watching: {RESULTS_DIR}")

    threading.Thread(target=run_server, daemon=True).start()

    while True:
        try:
            scan_and_publish()
        except Exception as e:
            logger.error(f"Scan error: {e}", exc_info=True)
        time.sleep(SCAN_INTERVAL)
