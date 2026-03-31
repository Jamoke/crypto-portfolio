#!/bin/bash
# ============================================================
#  Raspberry Pi 5 Setup Script
#  Run this once on a fresh Pi OS installation.
#  Usage: bash scripts/setup_pi.sh
# ============================================================

set -e
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

info()    { echo -e "${GREEN}[INFO]${NC} $1"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $1"; }
error()   { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }
section() { echo -e "\n${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; echo -e "${GREEN} $1${NC}"; echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; }

section "1. System Update"
sudo apt-get update -qq && sudo apt-get upgrade -y -qq
info "System updated"

section "2. Install Dependencies"
sudo apt-get install -y -qq \
    git curl wget unzip \
    python3 python3-pip python3-venv \
    ca-certificates gnupg lsb-release \
    ufw fail2ban \
    htop net-tools
info "Dependencies installed"

section "3. Install Docker"
if command -v docker &>/dev/null; then
    info "Docker already installed: $(docker --version)"
else
    curl -fsSL https://get.docker.com | sudo bash
    sudo usermod -aG docker "$USER"
    info "Docker installed. You'll need to log out and back in for group permissions."
fi

section "4. Install Docker Compose (plugin)"
if docker compose version &>/dev/null; then
    info "Docker Compose already installed: $(docker compose version)"
else
    sudo apt-get install -y docker-compose-plugin
    info "Docker Compose installed"
fi

section "5. Configure Firewall"
sudo ufw --force enable
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow ssh
sudo ufw allow 3000/tcp comment 'Grafana'
sudo ufw allow 8080/tcp comment 'Freqtrade API'
sudo ufw allow 8443/tcp comment 'TradingView webhook'
# 8090 (Asset Governor) is internal only
info "Firewall configured"

section "6. System Performance Tweaks for Pi 5"
# Increase swap for memory-intensive operations
if [ "$(sudo swapon --show | wc -l)" -lt 2 ]; then
    sudo dphys-swapfile swapoff || true
    echo "CONF_SWAPSIZE=2048" | sudo tee /etc/dphys-swapfile > /dev/null
    sudo dphys-swapfile setup
    sudo dphys-swapfile swapon
    info "Swap increased to 2GB"
fi

# Enable GPU memory split (less GPU, more for system)
if ! grep -q "gpu_mem=16" /boot/config.txt 2>/dev/null; then
    echo "gpu_mem=16" | sudo tee -a /boot/config.txt > /dev/null
fi

section "7. Create Project Structure"
PROJECT_DIR="$HOME/crypto-portfolio"
if [ -d "$PROJECT_DIR" ]; then
    warn "Directory $PROJECT_DIR already exists"
else
    mkdir -p "$PROJECT_DIR"
    info "Created $PROJECT_DIR"
fi

section "8. Set Up .env File"
if [ ! -f "$PROJECT_DIR/.env" ]; then
    if [ -f "$PROJECT_DIR/.env.example" ]; then
        cp "$PROJECT_DIR/.env.example" "$PROJECT_DIR/.env"
        warn ".env file created from template. EDIT IT BEFORE STARTING:"
        warn "  nano $PROJECT_DIR/.env"
    else
        warn ".env.example not found. Clone the repo first."
    fi
else
    info ".env already exists"
fi

section "9. Install Cloudflare Tunnel (for TradingView webhooks)"
warn "Optional: Cloudflare Tunnel gives TradingView a public URL to reach your Pi."
warn "Skip this if you already have port forwarding or a static IP."
read -p "Install Cloudflare Tunnel? [y/N]: " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    curl -fsSL https://pkg.cloudflare.com/cloudflare-main.gpg | \
        sudo tee /usr/share/keyrings/cloudflare-main.gpg > /dev/null
    echo "deb [signed-by=/usr/share/keyrings/cloudflare-main.gpg] https://pkg.cloudflare.com/cloudflared any main" | \
        sudo tee /etc/apt/sources.list.d/cloudflared.list
    sudo apt-get update -qq && sudo apt-get install -y cloudflared
    info "cloudflared installed. Run 'cloudflared tunnel login' to set up."
fi

section "10. Auto-start on Boot"
COMPOSE_FILE="$PROJECT_DIR/docker-compose.yml"
SERVICE_FILE="/etc/systemd/system/crypto-portfolio.service"
sudo tee "$SERVICE_FILE" > /dev/null <<EOF
[Unit]
Description=Crypto Portfolio Services
Requires=docker.service
After=docker.service network-online.target
Wants=network-online.target

[Service]
WorkingDirectory=$PROJECT_DIR
ExecStart=/usr/bin/docker compose up
ExecStop=/usr/bin/docker compose down
Restart=on-failure
RestartSec=10
User=$USER

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable crypto-portfolio
info "Auto-start service configured (will start on next boot)"
warn "To start now: sudo systemctl start crypto-portfolio"
warn "Or use: cd $PROJECT_DIR && docker compose up -d"

section "Setup Complete!"
echo ""
echo "Next steps:"
echo "  1. Edit your .env file:    nano $HOME/crypto-portfolio/.env"
echo "  2. Start services:         cd $HOME/crypto-portfolio && docker compose up -d"
echo "  3. Check logs:             docker compose logs -f"
echo "  4. Open Grafana:           http://$(hostname -I | awk '{print $1}'):3000"
echo "  5. Open Freqtrade UI:      http://$(hostname -I | awk '{print $1}'):8080"
echo ""
warn "Remember: SIMULATION_MODE=true in .env until Phase 3. No real trades!"
