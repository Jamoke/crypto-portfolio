# Crypto Portfolio — Automated DeFi Investment System

> Phase 1: Signal generation + dry-run on Raspberry Pi 5

## Quick Start (after Pi setup)

```bash
# 1. Clone this repo onto your Pi
git clone https://github.com/YOUR_USERNAME/crypto-portfolio.git ~/crypto-portfolio
cd ~/crypto-portfolio

# 2. Copy and fill in your environment variables
cp .env.example .env
nano .env

# 3. Run the Pi setup script (first time only)
bash scripts/setup_pi.sh

# 4. Start all services
docker compose up -d

# 5. Watch logs
docker compose logs -f
```

## Access Points (on your local network)

| Service | URL | Credentials |
|---|---|---|
| Freqtrade UI | http://PI_IP:8080 | admin / (from .env) |
| Grafana Dashboard | http://PI_IP:3000 | admin / (from .env) |
| TV Webhook Endpoint | http://PI_IP:8443/webhook | secret in .env |
| Asset Governor API | http://PI_IP:8090 | (internal only) |

## Key Config Files

| File | Purpose |
|---|---|
| `config/asset_governance.yaml` | What assets can be traded |
| `config/strategy_config.yaml` | Strategy parameters |
| `config/risk_config.yaml` | Risk limits and circuit breakers |
| `config/yield_config.yaml` | DeFi yield routing (Phase 3) |

## Current Phase: Phase 1

- [x] Freqtrade running in dry-run mode
- [x] Claude analyst generating signals every 6h
- [x] TradingView webhook bridge
- [x] Asset governance enforcement
- [x] Email digest
- [ ] Wallet setup (next step)
- [ ] Telegram bot (next step)
- [ ] Live DeFi execution (Phase 3)

## Important Safety Notes

- `SIMULATION_MODE=true` in `.env` means **no real transactions**
- Never commit your `.env` file to git (it's in `.gitignore`)
- Never paste your private key anywhere in this system
- Wait for Phase 3 before setting `SIMULATION_MODE=false`

## TradingView Webhook Setup

In TradingView, create an alert with webhook URL:
```
http://YOUR_PI_IP:8443/webhook?secret=YOUR_TV_WEBHOOK_SECRET
```

Alert message body (JSON):
```json
{
  "symbol": "{{ticker}}",
  "action": "{{strategy.order.action}}",
  "indicator": "supertrend",
  "timeframe": "{{interval}}",
  "price": {{close}}
}
```
