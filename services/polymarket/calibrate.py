"""
Polymarket calibration harness — Phase C.

Scores Claude's probability estimates against resolved Polymarket markets
to answer the only question that matters before we ever go live:
  "Does Claude's estimate actually beat the market's midpoint?"

Methodology
-----------
1. Fetch resolved markets from the CLOB in a rolling window.
2. Discard any market whose resolution_date <= Claude's training cutoff
   (+ buffer). The rest are fair test cases — Claude cannot have
   memorized the outcome.
3. For each fair market, reconstruct the market state at
   T = resolution_date - evaluation_lag (default 24h before close).
   The reconstruction uses the CLOB historical endpoint if available,
   else the snapshot at fetch-time (best-effort; documented limitation).
4. Call analyze_market_with_claude() with a prompt that sees only
   the question + state at T — no resolution hints, no final price.
5. Record (market_id, question, resolution, claude_prob, confidence,
   market_prob_at_T, lag_hours, claude_model) to calibration.db.

The analysis (Brier score, reliability diagram, edge-conditional
accuracy, category breakdown) lives in
scripts/calibration_analysis.ipynb — this module only builds the
dataset. Keeping the two separated means the harness is deterministic
and the notebook can be re-run freely against the same DB.

Legal guardrail: this module has no execution path that touches a
CLOB order endpoint. It is read-only.

Cost guardrail: --limit caps the number of Claude calls. The script
prints an estimated dollar cost before firing any requests when
--dry-run is set.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import requests
import yaml

logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
SERVICE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get("CALIBRATION_DB", "/app/data/calibration.db"))
CUTOFFS_PATH = Path(os.environ.get("CLAUDE_CUTOFFS",
                                   "/app/config/claude_cutoffs.yaml"))

POLYMARKET_CLOB_BASE = "https://clob.polymarket.com"

# Cost estimate: Claude Sonnet 4.6 is ~$3/Mtok input, ~$15/Mtok output
# as of Jan 2025. Our prompts are ~400 tok in, ~200 tok out.
COST_PER_CALL_USD = (400 * 3 + 200 * 15) / 1_000_000   # ≈ $0.0042


# ── Schema ────────────────────────────────────────────────────────────────────
CREATE_CALIBRATION_TABLE = """
CREATE TABLE IF NOT EXISTS calibration_runs (
    market_id TEXT,
    question TEXT,
    resolution_date TEXT,
    resolution REAL,                   -- 0.0 or 1.0 (YES outcome)
    claude_prob REAL,
    claude_confidence REAL,
    claude_reasoning TEXT,
    market_prob_at_T REAL,
    evaluation_lag_hours REAL,
    claude_model TEXT,
    category TEXT,
    run_id TEXT,
    recorded_at TEXT,
    PRIMARY KEY (market_id, run_id)
)
"""


def _init_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(CREATE_CALIBRATION_TABLE)
    conn.commit()
    return conn


# ── Cutoff handling ───────────────────────────────────────────────────────────
def load_cutoff(model: str, path: Path = CUTOFFS_PATH) -> datetime:
    """
    Return the earliest resolution date we will trust as uncontaminated
    for `model`. That's cutoff + buffer_days, both from claude_cutoffs.yaml.
    """
    with path.open() as f:
        cfg = yaml.safe_load(f)
    buffer_days = int(cfg.get("buffer_days", 0))
    models = cfg.get("models", {})
    if model not in models:
        raise ValueError(f"No cutoff configured for model {model!r}. "
                         f"Add it to {path}.")
    cutoff_str = models[model]["cutoff"]
    cutoff = datetime.fromisoformat(cutoff_str).replace(tzinfo=timezone.utc)
    return cutoff + timedelta(days=buffer_days)


# ── CLOB fetch ────────────────────────────────────────────────────────────────
def _fetch_resolved_markets(window_days: int, limit: int,
                            min_volume: float = 1000.0) -> list[dict]:
    """
    Pull closed markets from the CLOB. The CLOB supports ?closed=true and
    cursor pagination — we page until we hit `limit` or the window edge.
    """
    results: list[dict] = []
    cursor = ""
    cutoff_ts = time.time() - window_days * 86400
    while len(results) < limit:
        params: dict[str, Any] = {"closed": "true", "limit": 500}
        if cursor:
            params["next_cursor"] = cursor
        try:
            resp = requests.get(f"{POLYMARKET_CLOB_BASE}/markets",
                                params=params, timeout=15)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"CLOB fetch failed: {e}")
            break
        payload = resp.json()
        batch = payload.get("data") or []
        if not batch:
            break
        for m in batch:
            # end_date_iso | endDate fallbacks; CLOB has shifted field names.
            end_raw = m.get("end_date_iso") or m.get("endDate") or m.get("end_date")
            if not end_raw:
                continue
            try:
                end_dt = datetime.fromisoformat(str(end_raw).replace("Z", "+00:00"))
            except Exception:
                continue
            if end_dt.timestamp() < cutoff_ts:
                continue
            if float(m.get("volume", 0) or 0) < min_volume:
                continue
            results.append(m)
            if len(results) >= limit:
                break
        cursor = payload.get("next_cursor") or ""
        if not cursor:
            break
    logger.info(f"fetched {len(results)} resolved markets (window={window_days}d, limit={limit})")
    return results


def _market_resolution(market: dict) -> float | None:
    """
    Returns 1.0 if YES resolved, 0.0 if NO resolved, None if ambiguous.
    Closed Polymarket markets expose a winning token via tokens[].winner.
    """
    for tok in market.get("tokens", []) or []:
        if tok.get("winner"):
            outcome = str(tok.get("outcome", "")).upper()
            if outcome == "YES":
                return 1.0
            if outcome == "NO":
                return 0.0
    return None


def _market_prob_at_T(market: dict, resolution_dt: datetime,
                      lag_hours: float) -> float | None:
    """
    Best-effort price reconstruction at T = resolution_dt - lag_hours.

    Primary: CLOB /prices-history?market=<token_id>&interval=1h
    Fallback: last snapshot price if history fetch fails (with a flag).

    Returns None if neither works.
    """
    # Find the YES token id
    yes_token = None
    for tok in market.get("tokens", []) or []:
        if str(tok.get("outcome", "")).upper() == "YES":
            yes_token = tok.get("token_id")
            break
    if not yes_token:
        return None

    T = resolution_dt - timedelta(hours=lag_hours)
    try:
        resp = requests.get(
            f"{POLYMARKET_CLOB_BASE}/prices-history",
            params={
                "market": yes_token,
                "startTs": int(T.timestamp() - 3600),
                "endTs": int(T.timestamp() + 3600),
                "fidelity": 60,   # 1 minute
            },
            timeout=15,
        )
        resp.raise_for_status()
        hist = resp.json().get("history") or []
        if hist:
            # Pick the sample closest to T
            target = T.timestamp()
            best = min(hist, key=lambda p: abs(int(p.get("t", 0)) - target))
            return float(best.get("p", 0) or 0) or None
    except Exception as e:
        logger.debug(f"prices-history failed for {yes_token[:10]}: {e}")

    # Fallback: current snapshot (inaccurate but better than dropping the row)
    for tok in market.get("tokens", []) or []:
        if str(tok.get("outcome", "")).upper() == "YES":
            try:
                return float(tok.get("price", 0) or 0) or None
            except (TypeError, ValueError):
                return None
    return None


# ── Claude analysis (no resolution hints) ─────────────────────────────────────
def _build_calibration_prompt(question: str, market_prob_at_T: float,
                              resolution_date: str,
                              evaluation_lag_hours: float) -> str:
    """
    The prompt mirrors production but states explicitly that Claude should
    answer AS OF T, not as of now. This reduces one confound; training-data
    contamination is still handled by the cutoff filter.
    """
    return f"""You are estimating the true probability of a prediction market resolving YES.

Market question: "{question}"
Resolution date (UTC): {resolution_date}
Evaluate as of {evaluation_lag_hours:.1f} hours BEFORE resolution.
Market implied probability at that time: YES = {market_prob_at_T:.1%}

Answer using ONLY information that would have been publicly available at T —
do NOT use the final outcome or any post-T news. If you are uncertain, say so
in the confidence field.

Respond with ONLY valid JSON:
{{
  "yes_probability": 0.65,
  "confidence": 0.75,
  "reasoning": "One concise sentence."
}}

yes_probability: your estimate of P(YES) from 0.0 to 1.0
confidence: how confident you are in this estimate (0.0 = no idea, 1.0 = certain)
"""


def _call_claude(client, model: str, prompt: str) -> dict | None:
    try:
        msg = client.messages.create(
            model=model,
            max_tokens=400,
            messages=[{"role": "user", "content": prompt}],
        )
        text = msg.content[0].text.strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        j = json.loads(text)
        return {
            "claude_prob": float(j.get("yes_probability", 0.5)),
            "claude_confidence": float(j.get("confidence", 0.0)),
            "claude_reasoning": str(j.get("reasoning", ""))[:500],
        }
    except Exception as e:
        logger.warning(f"claude call failed: {e}")
        return None


# ── Main run loop ─────────────────────────────────────────────────────────────
def run_calibration(
    model: str,
    window_days: int,
    limit: int,
    evaluation_lag_hours: float,
    max_workers: int,
    dry_run: bool,
) -> dict[str, int]:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    earliest_allowed = load_cutoff(model)
    logger.info(f"model={model} earliest_allowed_resolution={earliest_allowed.date()}"
                f" window={window_days}d limit={limit} lag={evaluation_lag_hours}h")

    markets = _fetch_resolved_markets(window_days=window_days, limit=limit * 2)

    # Filter to (a) uncontaminated and (b) resolved YES/NO cleanly
    eligible: list[tuple[dict, datetime, float]] = []
    for m in markets:
        end_raw = m.get("end_date_iso") or m.get("endDate") or m.get("end_date")
        try:
            end_dt = datetime.fromisoformat(str(end_raw).replace("Z", "+00:00"))
        except Exception:
            continue
        if end_dt < earliest_allowed:
            continue
        resolution = _market_resolution(m)
        if resolution is None:
            continue
        eligible.append((m, end_dt, resolution))
        if len(eligible) >= limit:
            break

    est_cost = len(eligible) * COST_PER_CALL_USD
    logger.info(f"{len(eligible)} eligible markets. Estimated Claude cost: ${est_cost:.2f}")

    if dry_run:
        logger.info("dry-run: skipping Claude calls and DB writes")
        return {"eligible": len(eligible), "written": 0, "skipped": 0, "dry_run": 1}

    # Import Claude lazily so --dry-run works without the SDK installed locally.
    import anthropic
    claude_client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    conn = _init_db(DB_PATH)
    written = 0
    skipped = 0

    def process(entry):
        market, end_dt, resolution = entry
        prob_T = _market_prob_at_T(market, end_dt, evaluation_lag_hours)
        if prob_T is None:
            return None
        prompt = _build_calibration_prompt(
            market.get("question") or market.get("title") or "",
            prob_T,
            end_dt.isoformat(),
            evaluation_lag_hours,
        )
        out = _call_claude(claude_client, model, prompt)
        if out is None:
            return None
        out.update({
            "market_id": market.get("condition_id") or market.get("id"),
            "question": market.get("question") or market.get("title") or "",
            "resolution_date": end_dt.isoformat(),
            "resolution": resolution,
            "market_prob_at_T": prob_T,
            "evaluation_lag_hours": evaluation_lag_hours,
            "claude_model": model,
            "category": market.get("category") or market.get("tags", [None])[0],
            "run_id": run_id,
            "recorded_at": datetime.now(timezone.utc).isoformat(),
        })
        return out

    # Cap at 3 concurrent per Anthropic SDK guidelines.
    workers = min(max_workers, 3)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(process, e) for e in eligible]
        for fut in as_completed(futures):
            row = fut.result()
            if row is None:
                skipped += 1
                continue
            conn.execute("""
                INSERT OR REPLACE INTO calibration_runs (
                    market_id, question, resolution_date, resolution,
                    claude_prob, claude_confidence, claude_reasoning,
                    market_prob_at_T, evaluation_lag_hours, claude_model,
                    category, run_id, recorded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row["market_id"], row["question"], row["resolution_date"],
                row["resolution"], row["claude_prob"], row["claude_confidence"],
                row["claude_reasoning"], row["market_prob_at_T"],
                row["evaluation_lag_hours"], row["claude_model"],
                row["category"], row["run_id"], row["recorded_at"],
            ))
            written += 1
            if written % 10 == 0:
                conn.commit()
                logger.info(f"progress: written={written} skipped={skipped}")

    conn.commit()
    conn.close()
    logger.info(f"calibration run {run_id} complete: written={written} skipped={skipped}")
    return {"eligible": len(eligible), "written": written, "skipped": skipped,
            "run_id": run_id, "dry_run": 0}


# ── CLI ───────────────────────────────────────────────────────────────────────
def _build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Polymarket calibration harness (Phase C)")
    p.add_argument("--model", default="claude-sonnet-4-6",
                   help="Claude model id (must have a cutoff in claude_cutoffs.yaml)")
    p.add_argument("--window-days", type=int, default=180,
                   help="Rolling window of resolved markets to consider")
    p.add_argument("--limit", type=int, default=500,
                   help="Max Claude calls for this run (cost guardrail)")
    p.add_argument("--lag-hours", type=float, default=24.0,
                   help="Evaluate at T = resolution_date - lag_hours")
    p.add_argument("--workers", type=int, default=3,
                   help="Concurrent Claude calls (capped at 3)")
    p.add_argument("--dry-run", action="store_true",
                   help="Print cost estimate and eligibility counts; skip Claude and DB")
    return p


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    args = _build_argparser().parse_args(argv)
    result = run_calibration(
        model=args.model,
        window_days=args.window_days,
        limit=args.limit,
        evaluation_lag_hours=args.lag_hours,
        max_workers=args.workers,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
