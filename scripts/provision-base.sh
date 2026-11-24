#!/usr/bin/env bash
# Runs inside a Boat VM (as `user`). Idempotent: used for golden snapshot baking and slot boot.
# Reads non-secret configuration from environment variables and secrets from `~/.t3-sentinel/boot-secrets.json`
# (uploaded via `boat scp` with mode 0600 — never passed on the command line!).
#
# Modes:
#   BAKE_CLEAN=1   Install/update t3, verify it starts & capture descriptor/session templates,
#                  optionally install provider runtimes (T3_PREPARE_ACP_AGENTS / T3_INSTALL_PROVIDERS),
#                  then re-probe providers and wait until their on-disk status caches report
#                  `installed: true` (also for T3_VERIFY_ACP_AGENTS / T3_VERIFY_PROVIDERS, verify-only),
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
# t3code.service appends stdout to userdata/logs/boot-service.log; systemd fails (209/STDOUT) if the dir is missing
mkdir -p "$sentinel_dir" "$units" "$userdata_dir/logs"
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
  # If a t3-settings.json template was uploaded for base bake, install it temporarily so provider setup sees enabled providers
  if [ -f "$sentinel_dir/t3-settings.json" ]; then
    cp "$sentinel_dir/t3-settings.json" "$userdata_dir/settings.json"
    chmod 600 "$userdata_dir/settings.json"
  fi

  # Smoke-test t3 serve & capture real descriptor + auth session templates before wiping per-VM userdata
  "$t3" service install --allow-downgrade >/dev/null
  systemctl --user daemon-reload
  # Clear start-limit-hit (StartLimitBurst=5) left by boot attempts that ran before logs/ existed
  systemctl --user reset-failed t3code.service 2>/dev/null || true
  systemctl --user restart t3code.service
  wait_for_t3

  smoke_token="$(timeout 30 "$t3" auth session issue --token-only --ttl 1h --label bake-smoke)"
  descriptor="$(curl -sf --max-time 5 http://127.0.0.1:3773/.well-known/t3/environment)"
  auth_session="$(curl -sf --max-time 5 -H "Authorization: Bearer $smoke_token" http://127.0.0.1:3773/api/auth/session || echo '{}')"
  ver="$("$t3" --version 2>/dev/null || echo unknown)"

  # Optionally pre-prepare ACP Registry agents (e.g. devin) and managed provider runtimes (e.g. antigravity),
  # then make sure their provider status caches (`~/.t3/caches/<instanceId>.json`) say installed before the
  # snapshot is taken. T3 probes providers at boot, before the installs above, and writes `installed:false`;
  # new VMs hydrate from those files, so a stale cache in the snapshot shows "Not found" until a re-check.
  if [ -n "${T3_PREPARE_ACP_AGENTS:-}" ] || [ -n "${T3_INSTALL_PROVIDERS:-}" ] \
    || [ -n "${T3_VERIFY_ACP_AGENTS:-}" ] || [ -n "${T3_VERIFY_PROVIDERS:-}" ]; then
    js_runner="node"
    if command -v bun >/dev/null 2>&1; then js_runner="bun"; elif [ -x "$HOME/.bun/bin/bun" ]; then js_runner="$HOME/.bun/bin/bun"; fi
    SMOKE_TOK="$smoke_token" ACP_LIST="${T3_PREPARE_ACP_AGENTS:-}" PROV_LIST="${T3_INSTALL_PROVIDERS:-}" \
      VERIFY_ACP_LIST="${T3_VERIFY_ACP_AGENTS:-}" VERIFY_PROV_LIST="${T3_VERIFY_PROVIDERS:-}" \
      VERIFY_TIMEOUT_SECS="${T3_PROVIDER_VERIFY_TIMEOUT_SECS:-180}" \
      CACHE_DIR="$HOME/.t3/caches" SETTINGS_PATH="$userdata_dir/settings.json" "$js_runner" -e '
      (async () => {
      const fs = require("fs");
      const pathMod = require("path");
      const tok = process.env.SMOKE_TOK;
      const csv = (v) => (v || "").split(",").map(s => s.trim()).filter(Boolean);
      const acpAgents = csv(process.env.ACP_LIST);
      const installProvs = csv(process.env.PROV_LIST);
      const verifyAgents = [...new Set([...acpAgents, ...csv(process.env.VERIFY_ACP_LIST)])];
      const verifyProvs = [...new Set([...installProvs, ...csv(process.env.VERIFY_PROV_LIST)])];
      if (typeof WebSocket === "undefined") {
        console.error("[bake] WebSocket global unavailable in JS runtime; cannot install or verify providers");
        process.exit(1);
      }
      const tickRes = await fetch("http://127.0.0.1:3773/api/auth/websocket-ticket/rpc", {
        method: "POST",
        headers: { authorization: "Bearer " + tok },
      });
      if (!tickRes.ok) throw new Error("ws ticket failed: HTTP " + tickRes.status);
      const { ticket } = await tickRes.json();
      const ws = new WebSocket("ws://127.0.0.1:3773/ws?wsTicket=" + encodeURIComponent(ticket) + "&orchestrationProtocol=2");
      await new Promise((res, rej) => {
        ws.addEventListener("open", res);
        ws.addEventListener("error", () => rej(new Error("local t3 ws failed")));
      });
      let seq = 1;
      const waiters = new Map();
      const streamHandlers = new Map();
      ws.addEventListener("message", (ev) => {
        const raw = JSON.parse(String(ev.data));
        const frames = Array.isArray(raw) ? raw : [raw];
        for (const f of frames) {
          if (f._tag === "Ping") { ws.send(JSON.stringify({ _tag: "Pong" })); continue; }
          const id = String(f.requestId ?? "");
          if (f._tag === "Chunk" && streamHandlers.has(id)) {
            ws.send(JSON.stringify({ _tag: "Ack", requestId: f.requestId }));
            streamHandlers.get(id)(f.values || []);
          } else if (f._tag === "Exit" && waiters.has(id)) {
            const w = waiters.get(id);
            waiters.delete(id);
            w(f.exit);
          }
        }
      });
      function callUnary(tag, payload, timeoutMs = 600000) {
        const id = seq++;
        return new Promise((resolve, reject) => {
          const t = setTimeout(() => { waiters.delete(String(id)); reject(new Error(tag + " timed out")); }, timeoutMs);
          waiters.set(String(id), (exit) => {
            clearTimeout(t);
            if (exit && exit._tag === "Success") resolve(exit.value);
            else reject(new Error(tag + " failed: " + JSON.stringify(exit)));
          });
          ws.send(JSON.stringify({ _tag: "Request", id, tag, payload, headers: [] }));
        });
      }
      for (const agentId of acpAgents) {
        console.error("[bake] preparing ACP Registry agent: " + agentId + "...");
        const res = await callUnary("server.prepareAcpRegistryAgent", { agentId }, 300000);
        console.error("[bake] prepared ACP agent " + agentId + ": " + JSON.stringify(res));
      }
      for (const instanceId of installProvs) {
        console.error("[bake] installing managed provider runtime: " + instanceId + "...");
        const subId = seq++;
        let doneResolve, doneReject;
        const donePromise = new Promise((res, rej) => { doneResolve = res; doneReject = rej; });
        const t = setTimeout(() => doneReject(new Error("provider install timed out: " + instanceId)), 600000);
        streamHandlers.set(String(subId), (vals) => {
          for (const st of vals) {
            if (!st || typeof st !== "object") continue;
            console.error("[bake] provider " + instanceId + " phase=" + st.phase + (st.message ? " (" + st.message + ")" : ""));
            if (st.phase === "succeeded") { clearTimeout(t); doneResolve(st); }
            else if (st.phase === "failed" || st.phase === "cancelled") { clearTimeout(t); doneReject(new Error(st.error || st.message || st.phase)); }
          }
        });
        ws.send(JSON.stringify({ _tag: "Request", id: subId, tag: "provider.install.subscribe", payload: { instanceId }, headers: [] }));
        const startState = await callUnary("provider.install.start", { instanceId }, 120000);
        console.error("[bake] provider.install.start(" + instanceId + ") -> phase=" + startState?.phase);
        if (startState?.phase === "succeeded") { clearTimeout(t); doneResolve(startState); }
        else if (startState?.phase === "failed") { clearTimeout(t); doneReject(new Error(startState.error || "failed")); }
        await donePromise;
        ws.send(JSON.stringify({ _tag: "Interrupt", requestId: subId }));
        streamHandlers.delete(String(subId));
        console.error("[bake] provider runtime ready: " + instanceId);
      }

      // Instances whose status cache must say installed. ACP Registry agents map to the
      // providerInstances entry (driver acpRegistry, config.agentId) that references them.
      const expected = new Set(verifyProvs);
      let settings = {};
      try { settings = JSON.parse(fs.readFileSync(process.env.SETTINGS_PATH, "utf8")); } catch {}
      const instances = (settings && settings.providerInstances) || {};
      let configProviders = null;
      for (const agentId of verifyAgents) {
        const ids = Object.entries(instances)
          .filter(([, inst]) => inst && inst.driver === "acpRegistry" && inst.config && inst.config.source !== "local"
            && typeof inst.config.agentId === "string" && inst.config.agentId.trim() === agentId)
          .map(([id]) => id);
        if (ids.length === 0) {
          if (configProviders === null) {
            configProviders = await callUnary("server.getConfig", {}, 30000).then((c) => c.providers || []).catch(() => []);
          }
          const fallback = "acpRegistry_" + agentId;
          const known = configProviders.some((p) => p.instanceId === fallback && p.driver === "acpRegistry")
            || fs.existsSync(pathMod.join(process.env.CACHE_DIR, fallback + ".json"));
          if (known) ids.push(fallback);
        }
        if (ids.length === 0) {
          console.error("[bake] WARNING: no provider instance references ACP agent " + agentId + "; nothing to verify for it");
        }
        for (const id of ids) expected.add(id);
      }
      if (expected.size === 0) { ws.close(); process.exit(0); }

      const ids = [...expected];
      const deadline = Date.now() + Number(process.env.VERIFY_TIMEOUT_SECS) * 1000;
      const readCache = (id) => {
        try { return JSON.parse(fs.readFileSync(pathMod.join(process.env.CACHE_DIR, id + ".json"), "utf8")); }
        catch { return null; }
      };
      const pending = () => ids.filter((id) => { const c = readCache(id); return !(c && c.installed === true); });
      const remainingMs = () => Math.max(1000, deadline - Date.now());
      const note = (what) => (err) => console.error("[bake] " + what + " did not complete: " + (err && err.message ? err.message : err));

      // A full pass re-probes every live instance and rewrites the caches. It joins a pass already in
      // flight (possibly the boot probe from before the installs), so also refresh each instance directly.
      console.error("[bake] re-probing providers; waiting for caches of " + ids.join(", ") + " to report installed...");
      // Judge only after these first refreshes settle (or the deadline hits): files that already say
      // installed may be rewritten by a probe still running, and t3 must not be stopped mid-probe.
      const firstRefreshes = [callUnary("server.refreshProviders", {}, remainingMs()).catch(note("server.refreshProviders"))];
      const inFlight = new Set();
      const refreshOne = (id) => {
        if (inFlight.has(id)) return null;
        inFlight.add(id);
        return callUnary("server.refreshProviders", { instanceId: id }, remainingMs())
          .catch(note("server.refreshProviders(" + id + ")"))
          .finally(() => inFlight.delete(id));
      };
      for (const id of ids) firstRefreshes.push(refreshOne(id));
      let refreshed = false;
      Promise.allSettled(firstRefreshes).then(() => { refreshed = true; });

      let lastNudge = Date.now();
      while ((!refreshed || pending().length > 0) && Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, 2000));
        if (Date.now() - lastNudge >= 30000) {
          lastNudge = Date.now();
          pending().forEach(refreshOne);
        }
      }
      const stale = pending();
      for (const id of ids) {
        const c = readCache(id);
        console.error("[bake] provider cache " + id + ": " + (c
          ? "installed=" + c.installed + " enabled=" + c.enabled + " status=" + c.status + " checkedAt=" + c.checkedAt + (c.message ? " message=" + JSON.stringify(c.message) : "")
          : "missing"));
      }
      if (stale.length > 0) {
        console.error("[bake] provider status caches still not installed after " + process.env.VERIFY_TIMEOUT_SECS + "s: " + stale.join(", ") + "; refusing to snapshot");
        process.exit(1);
      }
      console.error("[bake] provider status caches verified: " + ids.join(", "));
      ws.close();
      process.exit(0);
      })().catch((err) => { console.error("[bake] provider pre-install error:", err); process.exit(1); });
    '
  fi

  # Stop services and wipe per-VM userdata (DB, environment-id, tokens, logs, worktrees, settings)
  # while preserving cached provider/ACP tool binaries (`providers/`, `cache/`, `tools/`, `acp-registry/`)
  systemctl --user stop t3code.service 2>/dev/null || true
  systemctl --user stop t3-sentinel-watchdog.service 2>/dev/null || true
  # Agent/probe helpers (e.g. Devin's background ACP checks) run in app-t3code.slice, outside t3code.service's cgroup
  systemctl --user stop app-t3code.slice 2>/dev/null || true
  # Unlinking a still-open DB fails with EBUSY on the sandbox filesystem; wait for holders to exit, name them if not
  for _ in $(seq 1 30); do grep -q "$userdata_dir/statev2" < <(ls -l /proc/[0-9]*/fd 2>/dev/null) || break; sleep 1; done
  for p in /proc/[0-9]*; do grep -q "$userdata_dir/statev2" < <(ls -l "$p/fd" 2>/dev/null) && echo "[bake] still holding statev2: $(tr "\0" " " < "$p/cmdline")" >&2; done
  rm -rf \
    "$userdata_dir/statev2.sqlite" "$userdata_dir/statev2.sqlite-wal" "$userdata_dir/statev2.sqlite-shm" \
    "$userdata_dir/server-owner.sqlite" "$userdata_dir/server-owner.sqlite-journal" \
    "$userdata_dir/environment-id" "$userdata_dir/settings.json" "$userdata_dir/keybindings.json" \
    "$userdata_dir/logs" "$userdata_dir/worktrees" "$userdata_dir/attachments" "$userdata_dir/auth" \
    "$sentinel_dir/watchdog.json" "$sentinel_dir/boot-secrets.json" "$sentinel_dir/t3-settings.json" "$sentinel_dir/saw-thread" \
    "$HOME/.klav-forward" "$units/t3code.service.d/klav-env.conf"
  # Keep logs/ so t3code.service can open its stdout log when VMs boot from this snapshot
  mkdir -p "$userdata_dir/logs"

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
if [ "$restart_t3" != "0" ]; then
  systemctl --user reset-failed t3code.service 2>/dev/null || true
  systemctl --user restart t3code.service
fi
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
