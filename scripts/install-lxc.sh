#!/usr/bin/env bash
# Setup helper for a Debian/Ubuntu LXC on pve-home (512MB-1GB RAM, zero project code runs here).
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/t3-boat-sentinel}"
SERVICE_USER="${SERVICE_USER:-$(whoami)}"

echo "==> Installing prerequisites (curl, unzip, ca-certificates)"
sudo apt-get update -y
sudo apt-get install -y curl unzip ca-certificates

if ! command -v bun >/dev/null 2>&1; then
  echo "==> Installing Bun"
  curl -fsSL https://bun.sh/install | bash
  export PATH="$HOME/.bun/bin:$PATH"
fi

if ! command -v boat >/dev/null 2>&1 && [ ! -x "$HOME/.ascii/bin/boat" ]; then
  echo "==> Installing Boat CLI"
  curl -fsSL https://boat.dev/install | sh
fi

if ! command -v cloudflared >/dev/null 2>&1; then
  echo "==> Installing cloudflared"
  ARCH="$(dpkg --print-architecture)"
  curl -fsSL "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-${ARCH}.deb" -o /tmp/cloudflared.deb
  sudo dpkg -i /tmp/cloudflared.deb
  rm -f /tmp/cloudflared.deb
fi

echo "==> Installing dependencies in $APP_DIR"
cd "$APP_DIR"
"$HOME/.bun/bin/bun" install --production

if [ ! -f "$APP_DIR/sentinel.yaml" ]; then
  cp "$APP_DIR/sentinel.example.yaml" "$APP_DIR/sentinel.yaml"
  echo "Created $APP_DIR/sentinel.yaml — edit domain, secrets, and projects before starting."
fi

echo "==> Creating systemd service /etc/systemd/system/t3-boat-sentinel.service"
sudo tee /etc/systemd/system/t3-boat-sentinel.service >/dev/null <<UNIT
[Unit]
Description=t3-boat-sentinel (T3 Code + Boat immutable sandbox orchestrator)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${SERVICE_USER}
WorkingDirectory=${APP_DIR}
Environment=PATH=${HOME}/.bun/bin:${HOME}/.ascii/bin:/usr/local/bin:/usr/bin:/bin
Environment=SENTINEL_CONFIG=${APP_DIR}/sentinel.yaml
ExecStart=${HOME}/.bun/bin/bun run src/index.ts serve
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
UNIT

sudo systemctl daemon-reload
sudo systemctl enable t3-boat-sentinel.service

echo ""
echo "Next steps:"
echo "  1. Sign in to Boat once:   boat onboard"
echo "  2. Edit config:            $APP_DIR/sentinel.yaml"
echo "  3. Configure cloudflared:  *.boat.<domain> -> http://127.0.0.1:8080"
echo "  4. Start sentinel:         sudo systemctl start t3-boat-sentinel"
echo "  5. Bake base snapshot:     bun run sentinel bake base"
echo "  6. Bake project snapshot:  bun run sentinel bake horizon-stack"
echo "  7. Pair slots once:        bun run sentinel pair boat-1"
