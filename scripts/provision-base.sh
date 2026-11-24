#!/usr/bin/env bash
# Runs inside a Boat VM (as `user`). Idempotent: used for golden snapshot baking and slot boot.
# Reads non-secret configuration from environment variables and secrets from `~/.t3-sentinel/boot-secrets.json`
# (uploaded via `boat scp` with mode 0600 — never passed on the command line!).
#
# Modes:
#   BAKE_CLEAN=1   Install/update t3, verify it starts & capture descriptor/session templates,
#                  then stop t3 and wipe ALL of `~/.t3/userdata` so the golden snapshot is spotless.
#   (default)      Write slot's permanent `environment-id` BEFORE t3 starts, start `t3code.service`
#                  and `t3-sentinel-watchdog.service`, optionally register `PROJECT_WORKDIR` via
#                  `t3 project add`, and print one JSON line with `{ environmentId, descriptor, authSession, upstreamToken?, t3Version }`.
set -euo pipefail

T3_VERSION="${T3_VERSION:-latest}"
T3_CHANNEL="${T3_CHANNEL:-stable}"
BAKE_CLEAN="${BAKE_CLEAN:-0}"
MINT_UPSTREAM_TOKEN="${MINT_UPSTREAM_TOKEN:-0}"

t3="$HOME/.local/bin/t3"
sentinel_dir="$HOME/.t3-sentinel"
userdata_dir="$HOME/.t3/userdata"
units="$HOME/.config/systemd/user"
mkdir -p "$sentinel_dir" "$units" "$userdata_dir"
chmod 700 "$sentinel_dir"

restart_t3=0
installed_ver="$("$t3" --version 2>/dev/null || true)"

if [ ! -x "$t3" ]; then
  if [ "$T3_VERSION" = "latest" ]; then
    curl -fsSL https://t3.codes/install.sh | T3CODE_CHANNEL="$T3_CHANNEL" sh >"$sentinel_dir/install.log" 2>&1
  else
    curl -fsSL https://t3.codes/install.sh | T3CODE_VERSION="$T3_VERSION" sh >"$sentinel_dir/install.log" 2>&1
  fi
  restart_t3=1
elif [ "$T3_VERSION" = "latest" ]; then
  "$t3" update --channel "$T3_CHANNEL" --yes >"$sentinel_dir/update.log" 2>&1
  new_ver="$("$t3" --version 2>/dev/null || true)"
  [ "$installed_ver" = "$new_ver" ] || restart_t3=1
elif [ "$installed_ver" != "t3 v$T3_VERSION" ]; then
  curl -fsSL https://t3.codes/install.sh | T3CODE_VERSION="$T3_VERSION" sh >"$sentinel_dir/install.log" 2>&1
  restart_t3=1
fi

wait_for_t3() {
  for _ in $(seq 1 120); do
    if curl -sf --max-time 3 -o /dev/null http://127.0.0.1:3773/.well-known/t3/environment; then
      return 0
    fi
    sleep 0.5
  done
  echo "T3 server did not start on :3773" >&2
  tail -n 40 "$userdata_dir/logs/boot-service.log" >&2 || true
  return 1
}

if [ "$BAKE_CLEAN" = "1" ]; then
  # Smoke-test t3 serve & capture real descriptor + auth session templates before wiping userdata
  "$t3" service install --allow-downgrade >/dev/null
  systemctl --user daemon-reload
  systemctl --user restart t3code.service
  wait_for_t3

  smoke_token="$(timeout 30 "$t3" auth session issue --token-only --ttl 1h --label bake-smoke)"
  descriptor="$(curl -sf --max-time 5 http://127.0.0.1:3773/.well-known/t3/environment)"
  auth_session="$(curl -sf --max-time 5 -H "Authorization: Bearer $smoke_token" http://127.0.0.1:3773/api/auth/session || echo '{}')"
  ver="$("$t3" --version 2>/dev/null || echo unknown)"

  # Stop services and wipe ALL of ~/.t3/userdata so no environment-id, sqlite DB, or tokens leak into snapshot
  systemctl --user stop t3code.service 2>/dev/null || true
  systemctl --user stop t3-sentinel-watchdog.service 2>/dev/null || true
  rm -rf "$userdata_dir" "$sentinel_dir/watchdog.json" "$sentinel_dir/boot-secrets.json" "$sentinel_dir/saw-thread" "$HOME/.klav-forward" "$units/t3code.service.d/klav-env.conf"
  mkdir -p "$userdata_dir"

  DESC="$descriptor" SESS="$auth_session" VER="$ver" node -e '
    console.log(JSON.stringify({
      t3Version: process.env.VER,
      descriptor: JSON.parse(process.env.DESC),
      authSession: JSON.parse(process.env.SESS || "{}"),
      cleaned: true,
    }));
  '
  exit 0
fi

# Write slot's permanent environment-id BEFORE t3 starts so HTTP descriptor AND WS subscribeServerConfig match!
if [ -n "${SLOT_ENV_ID:-}" ]; then
  if [ "$(cat "$userdata_dir/environment-id" 2>/dev/null || true)" != "$SLOT_ENV_ID" ]; then
    printf '%s\n' "$SLOT_ENV_ID" > "$userdata_dir/environment-id"
    restart_t3=1
  fi
fi

if [ -n "${SLOT_LABEL:-}" ]; then
  machine_info="PRETTY_HOSTNAME=$SLOT_LABEL"
  if [ "$(cat /etc/machine-info 2>/dev/null || true)" != "$machine_info" ]; then
    echo "$machine_info" | sudo tee /etc/machine-info >/dev/null || true
    restart_t3=1
  fi
fi

# Merge optional T3 settings template (providers, default model, runtime mode) & enable linked environments
settings="$userdata_dir/settings.json"
before_settings="$(cat "$settings" 2>/dev/null || true)"
node -e '
  const fs = require("fs");
  const target = process.argv[1];
  const tpl = process.argv[2];
  const cur = fs.existsSync(target) ? JSON.parse(fs.readFileSync(target, "utf8")) : {};
  const extra = fs.existsSync(tpl) ? JSON.parse(fs.readFileSync(tpl, "utf8")) : {};
  const merged = {
    ...cur,
    ...extra,
    providerInstances: { ...(cur.providerInstances || {}), ...(extra.providerInstances || {}) },
    linkedEnvironmentsEnabled: true,
  };
  fs.writeFileSync(target, JSON.stringify(merged, null, 2), { mode: 0o600 });
  if (fs.existsSync(tpl)) fs.unlinkSync(tpl);
' "$settings" "$sentinel_dir/t3-settings.json"
[ "$before_settings" = "$(cat "$settings")" ] || restart_t3=1

"$t3" service install --allow-downgrade >/dev/null
if [ -f "$HOME/.klav-forward/env.systemd" ]; then
  mkdir -p "$units/t3code.service.d"
  printf '[Service]\nEnvironmentFile=-%%h/.klav-forward/env.systemd\n' > "$units/t3code.service.d/klav-env.conf"
  restart_t3=1
fi
systemctl --user daemon-reload
systemctl --user is-active --quiet t3code.service || restart_t3=1
[ "$restart_t3" = "0" ] || systemctl --user restart t3code.service
wait_for_t3

# Optionally register project via official `t3 project add` CLI (fallback handled by sentinel if unavailable)
if [ -n "${PROJECT_WORKDIR:-}" ] && [ -n "${PROJECT_TITLE:-}" ]; then
  "$t3" project add "$PROJECT_WORKDIR" --title "$PROJECT_TITLE" >/dev/null 2>&1 || true
fi

# Configure & start watchdog using secrets uploaded to ~/.t3-sentinel/boot-secrets.json
if [ -f "$sentinel_dir/boot-secrets.json" ] && [ -f "$sentinel_dir/watchdog.mjs" ]; then
  watchdog_token="$("$t3" auth session issue --token-only --ttl 3650d --label t3-sentinel-watchdog --scope orchestration:read)"
  TOK="$watchdog_token" node -e '
    const fs = require("fs");
    const boot = JSON.parse(fs.readFileSync(process.argv[1], "utf8"));
    fs.writeFileSync(process.argv[2], JSON.stringify({ ...boot, t3Token: process.env.TOK }, null, 2), { mode: 0o600 });
    fs.unlinkSync(process.argv[1]);
  ' "$sentinel_dir/boot-secrets.json" "$sentinel_dir/watchdog.json"

  cat >"$units/t3-sentinel-watchdog.service" <<'UNIT'
[Unit]
Description=t3-boat-sentinel watchdog (reports idle/archive/heartbeats to sentinel)
After=t3code.service

[Service]
ExecStart=/bin/bash -lc 'exec node --no-warnings %h/.t3-sentinel/watchdog.mjs'
Restart=always
RestartSec=5
StandardOutput=append:%h/.t3-sentinel/watchdog.log
StandardError=append:%h/.t3-sentinel/watchdog.log

[Install]
WantedBy=default.target
UNIT
  systemctl --user daemon-reload
  systemctl --user enable t3-sentinel-watchdog.service >/dev/null 2>&1
  systemctl --user restart t3-sentinel-watchdog.service
fi

upstream_token=""
if [ "$MINT_UPSTREAM_TOKEN" = "1" ]; then
  upstream_token="$("$t3" auth session issue --token-only --ttl 3650d --label t3-sentinel)"
fi

descriptor="$(curl -sf http://127.0.0.1:3773/.well-known/t3/environment)"
auth_session="{}"
if [ -n "$upstream_token" ]; then
  auth_session="$(curl -sf -H "Authorization: Bearer $upstream_token" http://127.0.0.1:3773/api/auth/session || echo '{}')"
fi
ver="$("$t3" --version 2>/dev/null || echo unknown)"

DESC="$descriptor" SESS="$auth_session" TOK="$upstream_token" VER="$ver" node -e '
  const desc = JSON.parse(process.env.DESC);
  const out = {
    environmentId: desc.environmentId,
    descriptor: desc,
    authSession: JSON.parse(process.env.SESS || "{}"),
    t3Version: process.env.VER,
  };
  if (process.env.TOK) out.upstreamToken = process.env.TOK;
  console.log(JSON.stringify(out));
'
