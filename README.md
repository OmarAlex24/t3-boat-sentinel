# t3-boat-sentinel

**1 chat = 1 disposable [Boat](https://boat.dev) cloud sandbox behind a single [T3 Code](https://t3.codes) Environment (Desktop & Mobile) — zero T3 forks, zero T3 Connect tunnels consumed, immutable golden snapshots, and zero-trust proxies.**

`t3-boat-sentinel` runs as a tiny always-on sentinel daemon (Proxmox LXC, Raspberry Pi, or any $4 VPS — **none of your project code runs on the sentinel**) behind a single wildcard Cloudflare Tunnel.

---

## Why this exists

| | Traditional single-VM setups | `t3-boat-sentinel` |
|---|---|---|
| **Environment UX** | Pair 1 environment per VM/slot | **1 single `Boat` Environment** — spawns up to `N` (e.g. 20) parallel Boat VMs on demand |
| **Sandbox isolation** | 1 persistent VM shared across chats | **1 chat = 1 fresh disposable VM** from an immutable golden snapshot (`env-<project>`) |
| **Golden snapshots** | Overwritten on sleep (accumulates worktrees & bloat) | **Strictly immutable** — chats never mutate snapshots; only clean `bake` updates them |
| **Settle / Archive a chat** | Keeps VM & disk around | **Auto-destroys only that chat's Boat VM ($0)** + revokes its ephemeral tokens & Klav sandbox |
| **Adding new repos** | Hand-edit YAML & restart | **Automatic** via T3 Code's native Clone UI, the web Dashboard (`+ Add GitHub Repo`), API, or MCP |
| **Live boot feedback** | Blind spinner or extra Mac CLI | **Native progress inside T3 Code UI** via `subscribeWorktreeSetup` (zero extra client install) |
| **Secrets & LLM keys** | Plaintext API keys inside VMs | **Zero-trust**: per-sandbox ephemeral `LlmProxy` tokens + optional [Klav](https://klav.dev) (`klav-proxy` + ephemeral Tailnet) |
| **T3 Connect tunnels** | Consumes 1 tunnel per VM | **0 T3 Connect tunnels** (all traffic rides your own wildcard Cloudflare Tunnel) |

---

## Architecture (Hub Mode — Recommended)

```text
T3 Code Desktop (Mac/Linux/Win) + T3 Mobile
   │  wss://boat.example.com       (paired ONCE forever — 1 Environment for all repos & chats!)
   │  https://boat-ctrl.example.com (web dashboard / MCP / API — optional Cloudflare Access)
   ▼
Sentinel host (Proxmox LXC / Docker — ~40MB RAM, zero project code runs here)
   ├── cloudflared (1 wildcard tunnel: *.example.com -> http://127.0.0.1:8080)
   ├── Hub RPC Multiplexer (`boat.example.com`)
   │     ├── Presents all configured & dynamically-added repos in ONE T3 Environment
   │     ├── Streams live VM boot progress directly into T3 UI (`subscribeWorktreeSetup`)
   │     ├── Spawns 1 fresh isolated Boat VM per new chat (`orchestration.launchThread`)
   │     ├── Rewrites projectIds & routes every thread/terminal/VCS/attachment RPC to its owning VM
   │     └── Auto-reaps each VM independently when its chat is settled or archived
   ├── Zero-Trust LLM Proxy (`boat-llm.example.com`)
   │     └── Per-VM ephemeral `t3llm_...` token valid ONLY while `phase === "running"` (401 on sleep/delete)
   └── Reaper (reconciles `boat list --all` every 5m so zero orphan VMs bill)
   ▼
Disposable Boat VMs (`boat new --type large --from env-<project>`)
   ├── t3code.service (official `t3 serve`, auto-pinned to your Desktop/Mobile version)
   ├── Optional Klav ephemeral sandbox (`klav-forward` over `tag:klav-sandbox` Tailnet — real API keys never enter VM)
   └── watchdog.mjs (0 npm deps)
         ├── Chat settled / archived -> notifies sentinel -> destroys ONLY that VM ($0) + revokes tokens
         └── Idle (no agents, no CPU builds) -> pauses VM (never mutates golden snapshots)
```

*(Classic fixed-slot mode `boat-1..N` is also still supported if you want dedicated named environments per slot.)*

---

## Features

- **1-Environment Hub Multiplexer (`hub:`):** Pair `https://boat.example.com` **once** on T3 Desktop and **once** on T3 Mobile. Every new chat automatically spawns its own isolated Boat VM (`defaultSize: large` / `default` / `medium`, configurable globally or per-project), runs completely in parallel (up to `hub.maxSandboxes`), and is destroyed independently when you **Settle** or **Archive** the thread.
- **Zero-YAML Repo Onboarding:** Add new repositories without touching `sentinel.yaml`:
  1. Directly in **T3 Code UI** (Clone repository → Hub intercepts `projectClone.start`, registers the repo, and bakes `env-<repo>` in the background with live clone progress!).
  2. From the **Dashboard** (`https://boat-ctrl.example.com` → `+ Add GitHub Repo` → enter `owner/repo`).
  3. Via **REST API** (`POST /api/projects`) or **MCP** (`sentinel_add_project`).
  If you start a chat before `env-<repo>` finishes baking, Sentinel automatically boots from `base-t3` and clones on first boot.
- **Native Live Boot Progress in T3 Code:** Streams real-time VM creation, Klav setup, T3 server startup, and connection stages straight into T3 Code's native setup progress panel (`subscribeWorktreeSetup`) — no separate Mac CLI required.
- **Immutable 2-Tier Golden Snapshots:** `base-t3` (Ubuntu + `t3` + watchdog + cached provider/ACP tool binaries + optional `klav-forward`) → `env-<project>` (always built cleanly from `base-t3`, smoke-tested, and wiped of per-VM DB/tokens while preserving pre-cached provider binaries). Supports daily UTC auto-bake (`baseBakeHourUtc` / `bakeHourUtc`).
- **Zero-Trust LLM Proxy (`llmProxy:`):** Forward Claude/OpenAI requests to a private LAN/Tailscale gateway (e.g., `cliproxy`) without ever letting Boat VMs join your LAN or see your real API key. Each running VM receives a unique `t3llm_...` token that immediately returns `401` the moment the VM sleeps or is destroyed.
- **Zero-Knowledge Secrets with [Klav](https://klav.dev) (`klav:`):** Supports Klav ADR 0036/0042 (`mode: proxy` with `klav-proxy` + ephemeral `tag:klav-sandbox` Tailscale + `klav-forward`) so agents can call external APIs (e.g. Northflank, GitHub, Cloudflare) without plaintext secrets ever entering the sandbox, revoking access automatically when the chat ends.
- **Auto-Delete on Settle or Archive:** Configure `deleteWhen: archived-or-settled` (or `archived`) and `archiveAction: delete` (or `stop`). After `archiveGraceSecs` (default 20s), the VM is permanently deleted ($0/hr idle, no standby pool required).
- **Built-in stdio MCP Server + Agent Skill:** Run `bun run sentinel mcp` and install [`skills/t3-boat-sentinel/SKILL.md`](skills/t3-boat-sentinel/SKILL.md) so personal assistants and coding agents can check status, add repos, bake snapshots, and manage sandboxes.

---

## Quickstart

### Option A: Proxmox LXC / Linux host (recommended, ~512MB RAM)

```bash
git clone https://github.com/OmarAlex24/t3-boat-sentinel /opt/t3-boat-sentinel
cd /opt/t3-boat-sentinel
bash scripts/install-lxc.sh

# 1. Authenticate Boat CLI once
boat onboard

# 2. Generate sentinel.yaml with a cryptographic controlSecret
bun run sentinel init
$EDITOR sentinel.yaml

# 3. Point a single Cloudflare Tunnel wildcard to http://127.0.0.1:8080:
#      *.example.com -> http://127.0.0.1:8080
#
#    Security note:
#    - Do NOT put Cloudflare Access on `boat.example.com` or `boat-llm.example.com`
#      (they use T3's OAuth/Bearer auth and per-VM ephemeral tokens).
#    - DO protect `boat-ctrl.example.com` with Cloudflare Access (Email OTP / GitHub),
#      and add ONE Bypass application for `boat-ctrl.example.com/internal/watchdog`
#      so Boat VMs can deliver per-sandbox signed watchdog events.

# 4. Start the daemon
sudo systemctl start t3-boat-sentinel
```

### Option B: Docker Compose

```bash
cp sentinel.example.yaml sentinel.yaml
# Set CONTROL_SECRET (and optional LLM_UPSTREAM_API_KEY / CLOUDFLARE_TUNNEL_TOKEN) in .env
docker compose run --rm sentinel boat onboard
docker compose up -d
# Or with bundled cloudflared sidecar:
docker compose --profile tunnel up -d
```

---

## First-Time Setup

1. **Bake your immutable base snapshot (`base-t3`):**
   ```bash
   bun run sentinel bake base
   ```

2. **Add & bake your first project (either in `sentinel.yaml` or directly from the Dashboard / API):**
   ```bash
   bun run sentinel bake my-app
   ```
   Or dynamically without touching YAML:
   ```bash
   curl -X POST https://boat-ctrl.example.com/api/projects \
     -H "Authorization: Bearer $CONTROL_SECRET" \
     -H "Content-Type: application/json" \
     -d '{"repo": "owner/my-app", "bake": true}'
   ```

3. **Pair your Hub ONCE in T3 Desktop and ONCE in T3 Mobile:**
   ```bash
   bun run sentinel pair hub
   ```
   Paste the link in **T3 Code → Settings → Environments → Add environment**.
   *(Generate a fresh pairing link for each device — each code is single-use and multiple concurrent pairing codes are supported.)*

4. **Code away:** Open your `Boat` environment in T3 Desktop or T3 Mobile, pick any repo, and send a message. Sentinel spawns a fresh Boat VM for that thread, streams live boot progress right inside T3, and destroys the VM automatically when you **Settle** or **Archive** the chat.

---

## CLI & MCP Usage

### CLI (talks to the running daemon)
```bash
bun run sentinel status
bun run sentinel pair hub               # pairing link for T3 Desktop / Mobile
bun run sentinel bake base              # refresh base-t3 golden snapshot
bun run sentinel bake my-app            # refresh env-my-app from base-t3
bun run sentinel reset hub:<sandboxId>  # destroy one Hub VM immediately
bun run sentinel reset hub              # destroy all live Hub VMs
bun run sentinel revoke hub             # revoke paired device tokens
```

### As an MCP server (Claude Code / Codex / T3 Code / Personal Assistants)
```json
{
  "mcpServers": {
    "t3-boat-sentinel": {
      "command": "bun",
      "args": ["run", "/path/to/t3-boat-sentinel/src/index.ts", "mcp"],
      "env": {
        "SENTINEL_URL": "https://boat-ctrl.example.com",
        "CONTROL_SECRET": "<your-controlSecret>"
      }
    }
  }
}
```

---

## Development

```bash
bun install
bun run typecheck
bun test
```

## License

[MIT](LICENSE). Not affiliated with or endorsed by T3 Code or Boat.
