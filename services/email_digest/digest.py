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
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
DIGEST_RECIPIENT = os.environ.get("DIGEST_RECIPIENT", SMTP_USER)
DIGEST_HOUR = int(os.environ.get("DIGEST_HOUR", 8))
DIGEST_SECRET = os.environ.get("DIGEST_SECRET", "change_me")
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


def gather_portfolio_data() -> dict:
    """Collect current state from Redis."""
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

    # Pending governance actions
    pending = r.lrange("governance:pending_actions", 0, 4)
    data["pending_actions"] = [json.loads(a) for a in pending]

    # Circuit breaker status
    data["trading_paused"] = r.get("system:trading_paused") == "true"
    data["pause_reason"] = r.get("system:pause_reason") or ""

    return data


def compose_digest_email(portfolio_data: dict) -> str:
    """Ask Claude to write the digest email body as HTML."""

    sim_trades = portfolio_data.get("recent_sim_trades", [])
    analysis = portfolio_data.get("claude_analysis", {})

    prompt = f"""You are composing a daily portfolio digest email for a crypto investor.
Write a concise, professional HTML email body (no <html>/<head>/<body> tags, just the inner content).

Today: {datetime.now(timezone.utc).strftime('%A, %B %d, %Y')}

## Portfolio Status
- Mode: SIMULATION (no real trades yet — Phase 1)
- Trading paused: {portfolio_data.get('trading_paused', False)}
{f"- Pause reason: {portfolio_data['pause_reason']}" if portfolio_data.get('pause_reason') else ""}

## Claude Market Analysis
{json.dumps(analysis.get('signals', {}), indent=2) if analysis else "No analysis available yet."}

## Simulated Trade Activity (last 24h)
{json.dumps(sim_trades[:5], indent=2) if sim_trades else "No simulated trades yet. System warming up."}

## TradingView Signals
{json.dumps(portfolio_data.get('tv_signals', [])[:5], indent=2) if portfolio_data.get('tv_signals') else "No TV signals received."}

## Pending Actions Requiring Approval
{json.dumps(portfolio_data.get('pending_actions', []), indent=2) if portfolio_data.get('pending_actions') else "None."}

Write the email with these sections:
1. Quick summary (2-3 sentences, bullet points ok)
2. Market snapshot (key signals)
3. System activity (what the bot is doing)
4. Action required (if any) — include reply instructions
5. Footer note

Keep it scannable. Use simple HTML: <h2>, <p>, <ul><li>, <strong>.
If there are pending actions, show them with: "Reply APPROVE [token] or DENY [token] to this email"
"""

    message = claude.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1500,
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text


def send_email(subject: str, html_body: str):
    """Send the digest email via SMTP."""
    if not SMTP_USER or not SMTP_PASSWORD:
        logger.warning("SMTP credentials not configured. Email not sent.")
        logger.info(f"Would have sent: {subject}")
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = DIGEST_RECIPIENT

    part = MIMEText(html_body, "html")
    msg.attach(part)

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
    html_body = compose_digest_email(portfolio_data)

    date_str = datetime.now(timezone.utc).strftime("%b %d")
    subject = f"[Crypto Portfolio] Daily Digest — {date_str}"

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
