"""
Email Digest Service
Composes and sends a daily portfolio summary email.
Also monitors the inbox for approval replies and processes them.

Reply keywords (case-insensitive):
  APPROVE  — approve a pending governance change
  DENY     — deny a pending governance change
  RESUME   — resume trading after a circuit breaker pause
  PAUSE    — manually pause all trading
  SKIP     — skip a recommended action

Architecture:
  1. gather_portfolio_data()  — collects all data from Redis
  2. compose_digest_json()    — asks Claude to fill a structured JSON (not HTML)
  3. render_digest_html()     — renders a fixed Python HTML template from the JSON
  4. send_email()             — sends the final HTML email via SMTP

This split prevents Claude from hallucinating HTML tags and gives consistent
formatting across every digest.
"""

import os
import json
import time
import hmac
import hashlib
import logging
import smtplib
import redis
import anthropic
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
SMTP_HOST         = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT         = int(os.environ.get("SMTP_PORT", 587))
SMTP_USER         = os.environ.get("SMTP_USER", "")
SMTP_PASSWORD     = os.environ.get("SMTP_PASSWORD", "")
DIGEST_RECIPIENT  = os.environ.get("DIGEST_RECIPIENT", SMTP_USER)
DIGEST_HOUR       = int(os.environ.get("DIGEST_HOUR", 8))
GRAFANA_URL       = os.environ.get("GRAFANA_URL", "http://localhost:3000")

DIGEST_SECRET = os.environ.get("DIGEST_SECRET")
if not DIGEST_SECRET:
    raise ValueError(
        "DIGEST_SECRET env var must be set. "
        "Generate with: python3 -c \"import secrets; print(secrets.token_hex(32))\""
    )

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")

r = redis.from_url(REDIS_URL, decode_responses=True)
claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


def make_approval_token(action_id: str) -> str:
    """Generate a short HMAC token for email approval security."""
    return hmac.new(
        DIGEST_SECRET.encode(),
        action_id.encode(),
        hashlib.sha256,
    ).hexdigest()[:12]


# ── Data collection ───────────────────────────────────────────────────────────

def gather_portfolio_data() -> dict:
    """Collect current state from Redis across all services."""
    data = {}

    # Claude's latest analysis
    raw = r.get("claude:analysis:latest")
    if raw:
        data["claude_analysis"] = json.loads(raw)

    # Simulated trades (Phase 1) or real trades
    sim_trades = r.lrange("executor:simulated_trades", 0, 19)
    data["recent_sim_trades"] = [json.loads(t) for t in sim_trades]

    # TradingView signals
    tv_signals = r.lrange("signals:tradingview:queue", 0, 9)
    data["tv_signals"] = [json.loads(s) for s in tv_signals]

    # DCA signals
    dca_signals = r.lrange("signals:dca:queue", 0, 4)
    data["dca_signals"] = [json.loads(s) for s in dca_signals]

    # Pending governance actions
    pending = r.lrange("governance:pending_actions", 0, 4)
    data["pending_actions"] = [json.loads(a) for a in pending]

    # Circuit breaker status
    data["trading_paused"] = r.get("system:trading_paused") == "true"
    data["pause_reason"]   = r.get("system:pause_reason") or ""

    # Per-bot summary written by freqtrade_exporter every SCRAPE_INTERVAL.
    # Bots: momentum, recovery, scalp, freqai (freqai only when its profile is up).
    # If a key is missing, the exporter is down or the bot was unreachable —
    # we leave the value as None and let Claude / the fallback render N/A.
    bot_names = ["momentum", "recovery", "scalp", "freqai"]
    data["bots"] = []
    try:
        active_raw = r.get("freqtrade:bots:active")
        active_set = set(json.loads(active_raw)) if active_raw else set()
    except Exception:
        active_set = set()

    for name in bot_names:
        status = r.get(f"freqtrade:{name}:status")
        if status is None and name == "freqai":
            # FreqAI is profile-gated; skip silently when it isn't running.
            continue
        data["bots"].append({
            "name": name,
            "daily_pnl": _safe_float(r.get(f"freqtrade:{name}:daily_pnl")),
            "win_rate": _safe_float(r.get(f"freqtrade:{name}:win_rate")),
            "balance": _safe_float(r.get(f"freqtrade:{name}:balance")),
            "status": status or ("unreachable" if name in {"momentum", "recovery", "scalp"} else "off"),
            "in_active_set": name in active_set,
        })

    # Claude signal accuracy (latest per symbol from prediction feedback)
    accuracies = {}
    for key in r.scan_iter("claude:prediction:*"):
        try:
            parts = key.split(":")
            if len(parts) >= 3:
                sym = parts[2]
                if sym not in accuracies:
                    accuracies[sym] = []
        except Exception:
            pass
    data["signal_accuracies"] = accuracies  # populated incrementally — may be empty early on

    # Yield opportunities
    yield_opp = r.get("signals:yield:opportunity")
    data["yield_opportunity"] = json.loads(yield_opp) if yield_opp else None

    # Tax harvesting candidates from executor
    # Fetch from /positions via HTTP to get cost basis
    data["tax_summary"] = _fetch_tax_summary()

    # Polymarket opportunities (Phase B output — Kelly-sized simulated bets)
    poly_opps = r.lrange("signals:polymarket:opportunities", 0, 4)
    data["polymarket_opportunities"] = [json.loads(o) for o in poly_opps]

    # Polymarket whale roster summary (Phase A)
    whales_raw = r.get("signals:polymarket:whales:summary")
    data["polymarket_whales"] = json.loads(whales_raw) if whales_raw else None

    # Sanity ping on news collector — if last_refresh is stale, tell Claude so
    # it doesn't pretend to have news context when the service is down.
    data["news_last_refresh"] = r.get("signals:news:last_refresh")

    return data


def _safe_float(val) -> float | None:
    try:
        return float(val) if val is not None else None
    except Exception:
        return None


def _fetch_tax_summary() -> list[dict]:
    """Fetch open positions from defi_executor for unrealized gain estimation."""
    try:
        import requests as req
        resp = req.get("http://defi_executor:8091/positions", timeout=5)
        if resp.status_code == 200:
            positions = resp.json()
            # Get current prices from Redis
            result = []
            for pos in positions:
                sym = pos.get("symbol", "")
                avg_cost = pos.get("avg_cost_usd", 0)
                qty = pos.get("token_amount", 0)
                price_raw = r.get(f"claude:signals:{sym}")
                current_price = 0.0
                if price_raw:
                    current_price = json.loads(price_raw).get("price_usd", 0) or 0
                unrealized = (current_price - avg_cost) * qty if current_price else None
                result.append({
                    "symbol": sym,
                    "avg_cost_usd": avg_cost,
                    "quantity": qty,
                    "current_price_usd": current_price,
                    "unrealized_gain_usd": round(unrealized, 2) if unrealized is not None else None,
                })
            return sorted(result, key=lambda x: x.get("unrealized_gain_usd") or 0)
    except Exception as e:
        logger.debug(f"Could not fetch tax summary: {e}")
    return []


# ── Claude content generation ─────────────────────────────────────────────────

def compose_digest_json(portfolio_data: dict) -> dict:
    """
    Ask Claude to produce structured JSON content for the digest.
    Claude fills text/data only — no HTML generation.
    """
    analysis = portfolio_data.get("claude_analysis", {})
    sim_trades = portfolio_data.get("recent_sim_trades", [])
    pending_actions = portfolio_data.get("pending_actions", [])
    tax_data = portfolio_data.get("tax_summary", [])
    harvesting_candidates = [
        p for p in tax_data
        if p.get("unrealized_gain_usd") is not None and p["unrealized_gain_usd"] < -50
    ]

    poly_opps = portfolio_data.get("polymarket_opportunities", []) or []
    poly_whales = portfolio_data.get("polymarket_whales") or {}
    upstream_events = analysis.get("event_explanations", []) if analysis else []

    prompt = f"""You are composing a daily crypto portfolio digest. Return ONLY valid JSON (no markdown, no prose).

Style rules — enforce strictly:
- Bullets are TERSE: <= 90 chars, no filler ("it's worth noting", "as expected", "in light of"). Lead with the fact, not framing.
- No restating the section header. No closing summary sentences.
- Carry upstream event_explanations verbatim — do not paraphrase or expand.
- Never fabricate causes; if upstream said cause is unclear, repeat that.

Today: {datetime.now(timezone.utc).strftime('%A, %B %d, %Y %H:%M UTC')}
Trading mode: {"PAUSED — " + portfolio_data.get("pause_reason","") if portfolio_data.get("trading_paused") else "SIMULATION (dry-run, no real trades)"}
News feed last refresh: {portfolio_data.get("news_last_refresh") or "UNKNOWN — news_collector may be down"}

== Bot Performance (sim mode — values from freqtrade_exporter Redis mirror) ==
{json.dumps(portfolio_data.get("bots", []))}
Format daily_pnl as "$X.XX" (negative as "-$X.XX"), win_rate as "XX%" (multiply 0.0-1.0 by 100; "N/A" if null), balance as "$X.XX" (or "N/A").
Include EVERY bot in the strategy_table in this exact order: momentum, recovery, scalp, freqai. Skip a bot ONLY if its name is missing from the input array entirely.

== Claude Market Analysis (upstream from claude_analyst) ==
{json.dumps(analysis.get("signals", {})) if analysis else "No analysis available yet."}
Market summary: {analysis.get("market_summary", "N/A")}

== Event Explanations (from claude_analyst, for >=10% 24h moves) ==
{json.dumps(upstream_events) if upstream_events else "None."}

== Recent Simulated Trades ==
{json.dumps(sim_trades[:3]) if sim_trades else "No trades yet."}

== DCA Activity ==
{json.dumps(portfolio_data.get("dca_signals", [])[:3]) if portfolio_data.get("dca_signals") else "No DCA signals this period."}

== Polymarket Opportunities (top 3, simulation-only) ==
{json.dumps(poly_opps[:3]) if poly_opps else "No active opportunities."}

== Polymarket Whale Roster Summary ==
{json.dumps({"whales_count": poly_whales.get("whales_count"), "updated_at": poly_whales.get("updated_at"), "top_5": (poly_whales.get("top") or [])[:5]}) if poly_whales else "No whale data."}

== Tax / Financial ==
Harvesting candidates (losses > $50):
{json.dumps(harvesting_candidates[:5]) if harvesting_candidates else "None."}

All positions:
{json.dumps(tax_data[:6]) if tax_data else "No position data."}

== Pending Actions ==
{json.dumps(pending_actions) if pending_actions else "None."}

Respond with this exact JSON structure. Use null or [] when data is unavailable. Caps: summary_bullets <= 3, top_signals <= 5, reasoning_bullets <= 2 per signal, event_explanations bullets exactly 3, opportunities <= 3, tax_insights <= 3.

{{
  "summary_bullets": ["bullet 1", "bullet 2", "bullet 3"],
  "strategy_table": [
    {{"bot": "momentum", "daily_pnl": "$X.XX", "win_rate": "XX%", "balance": "$X.XX", "status": "running"}},
    {{"bot": "recovery", "daily_pnl": "$X.XX", "win_rate": "XX%", "balance": "$X.XX", "status": "running"}},
    {{"bot": "scalp",    "daily_pnl": "$X.XX", "win_rate": "XX%", "balance": "$X.XX", "status": "running"}}
  ],
  "top_signals": [
    {{"symbol": "ETH", "direction": "bullish", "confidence": "0.82", "action": "accumulate",
      "reasoning_bullets": ["evidence 1", "evidence 2"]}}
  ],
  "event_explanations": [
    {{"symbol": "AAVE", "move_pct": "-20.3%",
      "explanation_bullets": ["cited driver", "corroborating data", "confidence caveat"]}}
  ],
  "polymarket_section": {{
    "headline": "one sentence on current Polymarket state (or 'No opportunities this period')",
    "opportunities": [
      {{"question": "short form", "side": "YES", "edge_pct": "7.2%",
        "kelly_pct": "18%", "whale_concurrence": true, "amount_usd": "$120.00"}}
    ],
    "whale_note": "one sentence on whale roster (size, refresh age, concurrence rate)"
  }},
  "tax_insights": [
    {{"symbol": "MATIC", "unrealized_pnl": "-$34.20", "action": "Consider selling to harvest loss", "holding_period": "short-term"}}
  ],
  "dca_activity": "one sentence summary of DCA activity or 'No DCA events this period'",
  "system_health": "one sentence about system status, explicitly flagging stale news feed if news_last_refresh is UNKNOWN or older than 2 hours",
  "pending_actions_summary": "one sentence, or 'No pending actions'"
}}"""

    try:
        message = claude.messages.create(
            model="claude-sonnet-4-6",
            # 2500 fits the trimmed digest envelope. Was 4000 before the
            # 2026-04-26 trim; the new directives cap each section.
            max_tokens=2500,
            messages=[{"role": "user", "content": prompt}],
        )
        text = message.content[0].text.strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text)
    except json.JSONDecodeError as e:
        logger.error(f"Claude returned invalid JSON: {e}")
        return _fallback_digest_json(portfolio_data)
    except Exception as e:
        logger.error(f"Claude API call failed: {e}")
        return _fallback_digest_json(portfolio_data)


def _fmt_money(v):
    if v is None:
        return "N/A"
    return f"-${abs(v):,.2f}" if v < 0 else f"${v:,.2f}"


def _fmt_pct(v):
    if v is None:
        return "N/A"
    return f"{v * 100:.0f}%"


def _fallback_digest_json(portfolio_data: dict) -> dict:
    """Return a minimal JSON digest if Claude is unavailable.

    Strategy table is rendered from the live exporter mirror in Redis even
    on the fallback path so we still ship real numbers when only the Claude
    call failed.
    """
    paused = portfolio_data.get("trading_paused", False)
    bot_order = ["momentum", "recovery", "scalp", "freqai"]
    bots_by_name = {b["name"]: b for b in portfolio_data.get("bots", [])}
    strategy_table = []
    for name in bot_order:
        b = bots_by_name.get(name)
        if not b:
            continue
        strategy_table.append({
            "bot": name,
            "daily_pnl": _fmt_money(b.get("daily_pnl")),
            "win_rate": _fmt_pct(b.get("win_rate")),
            "balance": _fmt_money(b.get("balance")),
            "status": b.get("status") or "unknown",
        })
    if not strategy_table:
        strategy_table = [
            {"bot": "momentum", "daily_pnl": "N/A", "win_rate": "N/A", "balance": "N/A", "status": "unknown"},
            {"bot": "recovery", "daily_pnl": "N/A", "win_rate": "N/A", "balance": "N/A", "status": "unknown"},
            {"bot": "scalp",    "daily_pnl": "N/A", "win_rate": "N/A", "balance": "N/A", "status": "unknown"},
        ]
    return {
        "summary_bullets": [
            f"System mode: {'PAUSED' if paused else 'Simulation'}",
            "Claude API unavailable — using fallback digest",
            "Check system logs for details",
        ],
        "strategy_table": strategy_table,
        "top_signals": [],
        "event_explanations": [],
        "polymarket_section": {
            "headline": "Polymarket data unavailable (Claude fallback path)",
            "opportunities": [],
            "whale_note": "N/A",
        },
        "tax_insights": [],
        "dca_activity": "DCA data unavailable",
        "system_health": f"Trading {'paused: ' + portfolio_data.get('pause_reason','') if paused else 'running in simulation mode'}",
        "pending_actions_summary": f"{len(portfolio_data.get('pending_actions', []))} pending actions",
    }


# ── HTML template ─────────────────────────────────────────────────────────────

def render_digest_html(content: dict, pending_actions: list, date_str: str) -> str:
    """Render the digest HTML from a fixed Python template."""

    # Strategy table rows
    strategy_rows = ""
    for bot in content.get("strategy_table", []):
        status_lower = str(bot.get("status","")).lower()
        if "running" in status_lower:
            color = "#2ecc71"           # green — alive
        elif status_lower in {"off", "disabled"}:
            color = "#888"              # gray — intentionally not running (e.g. freqai profile off)
        else:
            color = "#e74c3c"           # red — unreachable / unknown
        strategy_rows += f"""
        <tr>
          <td style="padding:8px;border:1px solid #ddd"><strong>{bot.get("bot","").title()}</strong></td>
          <td style="padding:8px;border:1px solid #ddd">{bot.get("daily_pnl","N/A")}</td>
          <td style="padding:8px;border:1px solid #ddd">{bot.get("win_rate","N/A")}</td>
          <td style="padding:8px;border:1px solid #ddd">{bot.get("balance","N/A")}</td>
          <td style="padding:8px;border:1px solid #ddd;color:{color}">{bot.get("status","N/A")}</td>
        </tr>"""

    # Top signals — now renders either reasoning_bullets (new) or reasoning (legacy)
    signal_rows = ""
    for sig in content.get("top_signals", [])[:5]:
        dir_color = "#2ecc71" if sig.get("direction") == "bullish" else (
            "#e74c3c" if sig.get("direction") == "bearish" else "#f39c12"
        )
        bullets = sig.get("reasoning_bullets") or []
        if bullets:
            reasoning_html = "<ul style='margin:0;padding-left:16px;font-size:12px'>" + "".join(
                f"<li style='margin-bottom:3px'>{b}</li>" for b in bullets
            ) + "</ul>"
        else:
            reasoning_html = f"<span style='font-size:12px'>{sig.get('reasoning','')}</span>"
        signal_rows += f"""
        <tr>
          <td style="padding:8px;border:1px solid #ddd"><strong>{sig.get("symbol","")}</strong></td>
          <td style="padding:8px;border:1px solid #ddd;color:{dir_color}">{sig.get("direction","").upper()}</td>
          <td style="padding:8px;border:1px solid #ddd">{sig.get("confidence","")}</td>
          <td style="padding:8px;border:1px solid #ddd">{sig.get("action","")}</td>
          <td style="padding:8px;border:1px solid #ddd">{reasoning_html}</td>
        </tr>"""

    # Event explanations — for >=10% 24h movers
    event_cards = ""
    for evt in content.get("event_explanations", [])[:8]:
        move_pct = str(evt.get("move_pct", ""))
        move_color = "#e74c3c" if move_pct.strip().startswith("-") else "#2ecc71"
        bullets_html = "".join(
            f"<li style='margin-bottom:4px'>{b}</li>"
            for b in (evt.get("explanation_bullets") or [])
        )
        event_cards += f"""
        <div style="border:1px solid #eee;border-radius:6px;padding:12px;margin-bottom:10px;background:#fafbfc">
          <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:6px">
            <strong style="font-size:15px">{evt.get("symbol","")}</strong>
            <span style="color:{move_color};font-weight:bold">{move_pct}</span>
          </div>
          <ul style="margin:0;padding-left:18px;font-size:13px">{bullets_html}</ul>
        </div>"""

    # Polymarket section
    poly = content.get("polymarket_section") or {}
    poly_rows = ""
    for op in (poly.get("opportunities") or [])[:5]:
        concurrence_badge = (
            "<span style='background:#27ae60;color:white;padding:2px 6px;"
            "border-radius:3px;font-size:10px'>WHALES</span>"
            if op.get("whale_concurrence") else ""
        )
        poly_rows += f"""
        <tr>
          <td style="padding:8px;border:1px solid #ddd;font-size:12px">{op.get("question","")}</td>
          <td style="padding:8px;border:1px solid #ddd"><strong>{op.get("side","")}</strong></td>
          <td style="padding:8px;border:1px solid #ddd">{op.get("edge_pct","")}</td>
          <td style="padding:8px;border:1px solid #ddd">{op.get("kelly_pct","")}</td>
          <td style="padding:8px;border:1px solid #ddd">{op.get("amount_usd","")} {concurrence_badge}</td>
        </tr>"""

    # Tax insights
    tax_items = ""
    for t in content.get("tax_insights", [])[:5]:
        pnl = str(t.get("unrealized_pnl", ""))
        color = "#e74c3c" if "-" in pnl else "#2ecc71"
        tax_items += f"""
        <li style="margin-bottom:6px">
          <strong>{t.get("symbol","")}</strong>:
          <span style="color:{color}">{pnl}</span> —
          {t.get("action","")}
          <em style="font-size:11px;color:#888">({t.get("holding_period","")})</em>
        </li>"""

    # Summary bullets
    summary_items = "".join(
        f'<li style="margin-bottom:6px">{b}</li>'
        for b in content.get("summary_bullets", [])
    )

    # Pending actions with APPROVE/DENY tokens
    action_section = ""
    if pending_actions:
        action_section = "<h2 style='color:#e74c3c'>⚠️ Action Required</h2><ul>"
        for action in pending_actions:
            action_id = str(action.get("id", action.get("type", "unknown")))
            token = make_approval_token(action_id)
            action_section += f"""
            <li style="margin-bottom:12px">
              <strong>{action.get("type","Action")}</strong>: {action.get("description", json.dumps(action))}<br>
              <code style="background:#f4f4f4;padding:2px 6px;border-radius:3px">
                Reply: APPROVE {token} or DENY {token}
              </code>
            </li>"""
        action_section += "</ul>"

    grafana_url = os.environ.get("GRAFANA_URL", "http://localhost:3000")

    # Pre-compute conditional blocks to avoid nested f-strings (Python <3.12 limitation)
    if not signal_rows:
        signal_table_html = "<p style='color:#888;font-style:italic'>No signals available.</p>"
    else:
        signal_table_html = (
            "<table style='width:100%;border-collapse:collapse;font-size:13px'>"
            "<thead><tr style='background:#f4f4f4'>"
            "<th style='padding:8px;border:1px solid #ddd;text-align:left'>Symbol</th>"
            "<th style='padding:8px;border:1px solid #ddd;text-align:left'>Direction</th>"
            "<th style='padding:8px;border:1px solid #ddd;text-align:left'>Confidence</th>"
            "<th style='padding:8px;border:1px solid #ddd;text-align:left'>Action</th>"
            "<th style='padding:8px;border:1px solid #ddd;text-align:left'>Reasoning</th>"
            f"</tr></thead><tbody>{signal_rows}</tbody></table>"
        )

    if not tax_items:
        tax_section_html = ""
    else:
        tax_section_html = (
            "<div style='padding:0 16px 16px;background:#fff9f0;border-left:4px solid #f39c12;margin:0 16px 16px'>"
            "<h2 style='font-size:16px;margin-top:0'>Tax Insights &amp; Harvesting Opportunities</h2>"
            f"<ul style='margin:0;padding-left:20px'>{tax_items}</ul>"
            "<p style='font-size:11px;color:#888;margin-top:8px'>"
            "&#9888;&#65039; This is not tax advice. Consult a tax professional before acting on these insights."
            "</p></div>"
        )

    # Event-explanations section (only renders if there are notable movers)
    if not event_cards:
        events_section_html = ""
    else:
        events_section_html = (
            "<div style='padding:0 16px 16px'>"
            "<h2 style='font-size:16px;border-bottom:2px solid #eee;padding-bottom:8px'>"
            "Notable Moves — What We Know</h2>"
            f"{event_cards}"
            "</div>"
        )

    # Polymarket section (always renders; headline/whale_note explain empty state)
    poly_headline = poly.get("headline", "No Polymarket data available.")
    poly_whale_note = poly.get("whale_note", "")
    if poly_rows:
        poly_table_html = (
            "<table style='width:100%;border-collapse:collapse;font-size:13px'>"
            "<thead><tr style='background:#f4f4f4'>"
            "<th style='padding:8px;border:1px solid #ddd;text-align:left'>Market</th>"
            "<th style='padding:8px;border:1px solid #ddd;text-align:left'>Side</th>"
            "<th style='padding:8px;border:1px solid #ddd;text-align:left'>Edge</th>"
            "<th style='padding:8px;border:1px solid #ddd;text-align:left'>Kelly</th>"
            "<th style='padding:8px;border:1px solid #ddd;text-align:left'>Size</th>"
            f"</tr></thead><tbody>{poly_rows}</tbody></table>"
        )
    else:
        poly_table_html = "<p style='color:#888;font-style:italic;margin:0'>No opportunities passed the scanner filters this period.</p>"
    polymarket_section_html = (
        "<div style='padding:0 16px 16px'>"
        "<h2 style='font-size:16px;border-bottom:2px solid #eee;padding-bottom:8px'>"
        "Polymarket (Simulation)</h2>"
        f"<p style='margin:0 0 10px;font-size:13px'>{poly_headline}</p>"
        f"{poly_table_html}"
        f"<p style='margin:10px 0 0;font-size:12px;color:#666'><em>Whale roster:</em> {poly_whale_note}</p>"
        "</div>"
    )

    return f"""
<div style="font-family:Arial,sans-serif;max-width:700px;margin:0 auto;color:#333">

  <div style="background:#1a1a2e;color:white;padding:20px;border-radius:8px 8px 0 0">
    <h1 style="margin:0;font-size:20px">Crypto Portfolio Digest</h1>
    <p style="margin:4px 0 0;opacity:0.8;font-size:13px">{date_str}</p>
  </div>

  <div style="background:#f8f9fa;padding:16px;border-left:4px solid #3498db">
    <h2 style="margin:0 0 10px;font-size:16px">Summary</h2>
    <ul style="margin:0;padding-left:20px">{summary_items}</ul>
  </div>

  <div style="padding:16px">
    <h2 style="font-size:16px;border-bottom:2px solid #eee;padding-bottom:8px">Strategy Performance</h2>
    <table style="width:100%;border-collapse:collapse;font-size:14px">
      <thead>
        <tr style="background:#f4f4f4">
          <th style="padding:8px;border:1px solid #ddd;text-align:left">Bot</th>
          <th style="padding:8px;border:1px solid #ddd;text-align:left">Daily P&L</th>
          <th style="padding:8px;border:1px solid #ddd;text-align:left">Win Rate</th>
          <th style="padding:8px;border:1px solid #ddd;text-align:left">Balance</th>
          <th style="padding:8px;border:1px solid #ddd;text-align:left">Status</th>
        </tr>
      </thead>
      <tbody>{strategy_rows}</tbody>
    </table>
  </div>

  <div style="padding:0 16px 16px">
    <h2 style="font-size:16px;border-bottom:2px solid #eee;padding-bottom:8px">Market Signals</h2>
    {signal_table_html}
  </div>

  {events_section_html}

  {polymarket_section_html}

  <div style="padding:0 16px 16px">
    <h2 style="font-size:16px;border-bottom:2px solid #eee;padding-bottom:8px">DCA Activity</h2>
    <p style="margin:0">{content.get("dca_activity","N/A")}</p>
  </div>

  {tax_section_html}

  <div style="padding:0 16px 16px">
    <h2 style="font-size:16px;border-bottom:2px solid #eee;padding-bottom:8px">System Health</h2>
    <p style="margin:0">{content.get("system_health","N/A")}</p>
  </div>

  {action_section}

  <div style="background:#f4f4f4;padding:16px;border-radius:0 0 8px 8px;font-size:12px;color:#888">
    <p style="margin:0">
      <a href="{grafana_url}" style="color:#3498db">View Grafana Dashboard</a> &nbsp;|&nbsp;
      <a href="{grafana_url}/d/crypto-tax-analysis" style="color:#3498db">Tax Dashboard</a> &nbsp;|&nbsp;
      Crypto Portfolio System — Simulation Mode
    </p>
  </div>

</div>
"""


# ── Email delivery ────────────────────────────────────────────────────────────

def send_email(subject: str, html_body: str):
    """Send the digest email via SMTP."""
    if not SMTP_USER or not SMTP_PASSWORD:
        logger.warning("SMTP credentials not configured. Email not sent.")
        logger.info(f"Would have sent: {subject}")
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = SMTP_USER
    msg["To"]      = DIGEST_RECIPIENT

    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, DIGEST_RECIPIENT, msg.as_string())
        logger.info(f"Digest email sent to {DIGEST_RECIPIENT}")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")


def send_daily_digest():
    """Compile and send the daily digest."""
    logger.info("Composing daily digest...")

    portfolio_data = gather_portfolio_data()
    content_json   = compose_digest_json(portfolio_data)

    date_str = datetime.now(timezone.utc).strftime("%A, %B %d, %Y")
    html_body = render_digest_html(
        content_json,
        portfolio_data.get("pending_actions", []),
        date_str,
    )

    subject = f"[Crypto Portfolio] Daily Digest — {datetime.now(timezone.utc).strftime('%b %d')}"
    send_email(subject, html_body)


def check_and_send_digest():
    """Check if it's time to send the digest."""
    now = datetime.now()
    last_sent_key = "digest:last_sent_date"
    last_sent = r.get(last_sent_key)
    today = now.strftime("%Y-%m-%d")

    if now.hour >= DIGEST_HOUR and last_sent != today:
        send_daily_digest()
        r.set(last_sent_key, today)
        r.expire(last_sent_key, 86400 * 2)


if __name__ == "__main__":
    logger.info("Email Digest Service started")
    time.sleep(30)  # Wait for system startup

    while True:
        try:
            check_and_send_digest()
        except Exception as e:
            logger.error(f"Digest cycle error: {e}", exc_info=True)

        time.sleep(900)  # Check every 15 minutes
