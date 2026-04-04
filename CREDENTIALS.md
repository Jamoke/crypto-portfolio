# Credential Audit Guide

This file documents every secret used in this project: where it lives, its current state,
and whether it still needs to be set or rotated. Keep this file updated as new secrets are added.

**Never commit `.env` to git.** It is in `.gitignore`. Only `.env.example` is committed.

---

## Quick-start checklist

Before deploying, these credentials **must** be set in your `.env` file:

- [ ] `ANTHROPIC_API_KEY` — Claude API (required for analyst + email digest)
- [ ] `COINGECKO_API_KEY` — Market data (free tier now requires a key)
- [ ] `TV_WEBHOOK_SECRET` — TradingView webhook HMAC validation
- [ ] `SMTP_USER` / `SMTP_PASSWORD` — Email digest sending
- [ ] `DIGEST_RECIPIENT` — Where the daily digest is sent
- [ ] `DIGEST_SECRET` — HMAC key for email approval tokens
- [ ] `FREQTRADE_PASSWORD` — Freqtrade web UI login
- [ ] `FREQTRADE_JWT_SECRET` — Freqtrade API JWT signing key (momentum bot)
- [ ] `FREQTRADE_WS_TOKEN` — Freqtrade websocket token (momentum bot)
- [ ] `FREQTRADE_SCALP_JWT_SECRET` — JWT key for scalp bot
- [ ] `FREQTRADE_SCALP_WS_TOKEN` — WS token for scalp bot
- [ ] `GRAFANA_PASSWORD` — Grafana admin login

Phase 3 (live trading) additionally requires:
- [ ] `WALLET_ADDRESS`
- [ ] `WALLET_PRIVATE_KEY`
- [ ] `POLYGON_RPC_URL`
- [ ] `ARBITRUM_RPC_URL`

---

## Full credential inventory

### 1. Anthropic API Key

| Field | Value |
|-------|-------|
| Variable | `ANTHROPIC_API_KEY` |
| Location | `.env` |
| Used by | `claude_analyst`, `email_digest` |
| Current state | Placeholder `sk-ant-xxxx` in `.env.example` |
| **Action** | **Set in `.env` before starting services** |
| Notes | Get at console.anthropic.com. Charges per token — analyst runs every 6h. |

---

### 2. CoinGecko API Key

| Field | Value |
|-------|-------|
| Variable | `COINGECKO_API_KEY` |
| Location | `.env` |
| Used by | `claude_analyst` |
| Current state | Empty in `.env.example` |
| **Action** | **Required** — CoinGecko now requires a key even for the free tier |
| Notes | Sign up at coingecko.com/api. Free Demo key is sufficient. Without it, analyst skips market data fetch and Claude API is not called. |

---

### 3. LunarCrush API Key

| Field | Value |
|-------|-------|
| Variable | `LUNARCRUSH_API_KEY` |
| Location | `.env` |
| Used by | Nobody yet (Phase 5 plan) |
| Current state | Empty |
| **Action** | Optional — leave blank until Phase 5 |

---

### 4. Wallet Address

| Field | Value |
|-------|-------|
| Variable | `WALLET_ADDRESS` |
| Location | `.env` |
| Used by | `defi_executor` |
| Current state | Zero address placeholder |
| **Action** | Set for Phase 3 (live trading). Use a dedicated trading wallet, never cold storage. |

---

### 5. Wallet Private Key

| Field | Value |
|-------|-------|
| Variable | `WALLET_PRIVATE_KEY` |
| Location | `.env` only |
| Used by | `defi_executor` (Phase 3) |
| Current state | Empty |
| **Action** | Set only when activating live trading. Never commit `.env`. Never log this value. |
| Notes | Use a hardware wallet-derived hot wallet with limited funds. The executor explicitly avoids logging this variable. |

---

### 6. Polygon RPC URL

| Field | Value |
|-------|-------|
| Variable | `POLYGON_RPC_URL` |
| Location | `.env` |
| Used by | `defi_executor`, `yield_router` (Phase 3) |
| Current state | `https://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY` placeholder |
| **Action** | Set for Phase 3. Get a free key at alchemy.com |

---

### 7. Arbitrum RPC URL

| Field | Value |
|-------|-------|
| Variable | `ARBITRUM_RPC_URL` |
| Location | `.env` |
| Used by | `defi_executor`, `yield_router` (Phase 3) |
| Current state | `https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY` placeholder |
| **Action** | Set for Phase 3 |

---

### 8. 1inch API Key

| Field | Value |
|-------|-------|
| Variable | `ONEINCH_API_KEY` |
| Location | `.env` |
| Used by | `defi_executor` (Phase 3) |
| Current state | Empty |
| **Action** | Optional — DEX routing works without it but rate limits apply. Get free at portal.1inch.dev |

---

### 9. TradingView Webhook Secret

| Field | Value |
|-------|-------|
| Variable | `TV_WEBHOOK_SECRET` |
| Location | `.env` |
| Used by | `tv_bridge` |
| Current state | `replace_with_random_secret_string` placeholder |
| **Action** | **Must set before using TradingView alerts.** Generate with: `python3 -c "import secrets; print(secrets.token_hex(32))"` |
| Notes | Add the same secret to your TradingView alert webhook URL as a query param: `?secret=YOURSECRET` |

---

### 10. Telegram Bot Token

| Field | Value |
|-------|-------|
| Variable | `TELEGRAM_BOT_TOKEN` |
| Location | `.env` |
| Used by | Stubbed in Freqtrade configs (disabled) |
| Current state | Empty |
| **Action** | Optional — set when enabling Telegram integration. Create a bot via @BotFather. |

---

### 11. Telegram Chat ID

| Field | Value |
|-------|-------|
| Variable | `TELEGRAM_CHAT_ID` |
| Location | `.env` |
| Used by | Stubbed in Freqtrade configs (disabled) |
| Current state | Empty |
| **Action** | Optional — set alongside `TELEGRAM_BOT_TOKEN` |

---

### 12. SMTP User

| Field | Value |
|-------|-------|
| Variable | `SMTP_USER` |
| Location | `.env` |
| Used by | `email_digest`, `defi_executor` (trade notifications) |
| Current state | `your@gmail.com` placeholder |
| **Action** | **Must set for email digest to work** |

---

### 13. SMTP Password

| Field | Value |
|-------|-------|
| Variable | `SMTP_PASSWORD` |
| Location | `.env` |
| Used by | `email_digest`, `defi_executor` |
| Current state | `your_app_password_here` placeholder |
| **Action** | **Must set.** For Gmail: enable 2FA, then create a 16-character App Password at myaccount.google.com/apppasswords. Do NOT use your regular Gmail password. |

---

### 14. Digest Recipient

| Field | Value |
|-------|-------|
| Variable | `DIGEST_RECIPIENT` |
| Location | `.env` |
| Used by | `email_digest` |
| Current state | `your@gmail.com` placeholder |
| **Action** | Set to the email address that should receive daily digests |

---

### 15. Digest Secret (HMAC key)

| Field | Value |
|-------|-------|
| Variable | `DIGEST_SECRET` |
| Location | `.env` |
| Used by | `email_digest` |
| Current state | Placeholder in `.env.example`; was previously `"change_me"` as hardcoded fallback in `digest.py:38` (now removed) |
| **Action** | **Must set.** Generate with: `python3 -c "import secrets; print(secrets.token_hex(32))"` |
| Notes | Used to generate HMAC tokens in approval emails. If not set, the service now fails loudly at startup. |

---

### 16. Freqtrade Password

| Field | Value |
|-------|-------|
| Variable | `FREQTRADE_PASSWORD` |
| Location | `.env` + `freqtrade/config.json` via `${FREQTRADE_PASSWORD}` |
| Used by | Freqtrade web UI, `asset_governor` (circuit breaker) |
| Current state | `changeme_secure_password` placeholder |
| **Action** | **Must set before deployment** |

---

### 17. Grafana Password

| Field | Value |
|-------|-------|
| Variable | `GRAFANA_PASSWORD` |
| Location | `.env` |
| Used by | Grafana admin login |
| Current state | `changeme_secure_password` placeholder |
| **Action** | **Must set before deployment** |

---

### 18 & 19. Freqtrade JWT Secret + WS Token (momentum bot)

| Field | Value |
|-------|-------|
| Variables | `FREQTRADE_JWT_SECRET`, `FREQTRADE_WS_TOKEN` |
| Location | `.env` + `freqtrade/config.json` via `${...}` substitution |
| Used by | Freqtrade momentum bot API authentication |
| Current state | Was hardcoded as `"change_this_to_a_long_random_secret_key"` — now env-var-driven |
| **Action** | **Must set.** Generate: `python3 -c "import secrets; print(secrets.token_hex(32))"` for each |

---

### 20 & 21. Freqtrade JWT Secret + WS Token (scalp bot)

| Field | Value |
|-------|-------|
| Variables | `FREQTRADE_SCALP_JWT_SECRET`, `FREQTRADE_SCALP_WS_TOKEN` |
| Location | `.env` + `freqtrade/config_scalp.json` via `${...}` substitution |
| Used by | Freqtrade scalp bot API authentication |
| Current state | Was hardcoded — now env-var-driven |
| **Action** | **Must set.** Use different values from the momentum bot tokens. |

---

### 22. CORS Origin IP

| Field | Value |
|-------|-------|
| Location | `freqtrade/config.json:74`, `freqtrade/config_scalp.json:76` |
| Current state | `192.168.50.152` (hardcoded LAN IP) |
| **Action** | Update to your Raspberry Pi's actual LAN IP before deployment. Also add to `.env.example` as `PI_LOCAL_IP` and reference via `${PI_LOCAL_IP}` in configs. |

---

### 23. Polymarket API Key (future — Plan 4.5)

| Field | Value |
|-------|-------|
| Variable | `POLYMARKET_API_KEY` |
| Location | `.env` (not yet in `.env.example`) |
| Used by | `polymarket` service (not yet built) |
| Current state | Does not exist yet |
| **Action** | Add when implementing Plan 4.5. Get at docs.polymarket.com |

---

### 24. Polymarket Private Key (future — Plan 4.5)

| Field | Value |
|-------|-------|
| Variable | `POLYMARKET_PRIVATE_KEY` |
| Location | `.env` |
| Used by | `polymarket` service |
| Current state | Does not exist yet |
| **Action** | Can reuse the trading wallet private key, or use a dedicated Polygon wallet for Polymarket CLOB order signing |

---

## Generating secure secrets

For any token/secret/key where you need a random value:

```bash
# 32-byte hex string (recommended for JWT secrets, HMAC keys)
python3 -c "import secrets; print(secrets.token_hex(32))"

# URL-safe base64 (alternative)
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
```

---

## Verifying no secrets are hardcoded

Run this from the repo root after setup to confirm no placeholder values remain:

```bash
grep -rn "changeme\|change_this\|change_me\|YOUR_KEY\|xxxx\|placeholder" \
  --include="*.json" --include="*.yaml" --include="*.py" --include="*.sh" \
  crypto-portfolio/
```

Expected result: zero matches (excluding this file and `.env.example`).
