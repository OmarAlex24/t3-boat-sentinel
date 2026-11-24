# t3-boat-sentinel

**1 chat = 1 disposable [Boat](https://boat.dev) cloud sandbox for official [T3 Code](https://t3.codes) (Desktop & Mobile) — zero forks, zero T3 Connect tunnels consumed, immutable golden snapshots.**

`t3-boat-sentinel` runs as a tiny always-on sentinel daemon (Proxmox LXC, Raspberry Pi, or any $4 VPS — **none of your project code runs on the sentinel**) behind a single wildcard Cloudflare Tunnel.

---

## Why this exists

| | `t3boat` | `t3-boat-sentinel` |
|---|---|---|
| **Sandbox model** | 1 persistent VPS-like VM shared across chats | **1 chat = 1 fresh disposable VM** from an immutable golden snapshot |
| **Golden snapshots** | Overwritten on every sleep (accumulates worktrees & artifacts) | **Strictly immutable** — chats never mutate snapshots; only clean `bake` updates them |
| **Archiving a chat** | Keeps VM & disk around | **Auto-stops & permanently deletes the Boat VM ($0)** after a 20s Undo grace period |
| **T3 Mobile wake** | Cannot wake sleeping VMs when laptop is closed | **24/7 wake from T3 Mobile** via always-on sentinel + Cloudflare Tunnel |
| **T3 Connect tunnels** | Consumes 1 T3 Connect tunnel per machine | **0 T3 Connect tunnels** (unlimited slots via your own wildcard Cloudflare Tunnel) |
| **T3 Code maintenance** | Inspects & caches internal T3 RPCs | **Transparent HTTP/WS proxy** + 4 stable T3 auth routes (resilient across T3 updates) |

---

## Architecture

```text
T3 Mobile / T3 Desktop
   │  wss://boat-1.boat.example.com   (paired ONCE forever)
   │  https://ctrl.boat.example.com   (mobile dashboard / iOS Shortcut)
   ▼
Sentinel host (Proxmox LXC / Docker — ~40MB RAM, zero project code runs here)
   ├── cloudflared (1 wildcard tunnel: *.boat.example.com -> 127.0.0.1:8080)
   ├── Slot Auth & Transparent HTTP/WS Proxy (permanent environmentId + pairing tokens)
   ├── Wake-on-command (passive sidebar probes never wake sleeping VMs)
   └── Reaper (reconciles `boat list --all` every 5m so zero orphan VMs bill)
   ▼
Disposable Boat VMs (spawned via `boat new --from env-<project>`)
   ├── t3code.service (auto-synced to your target T3 version/channel)
   └── watchdog.mjs (0 npm deps)
         ├── All chats archived/deleted -> notifies sentinel -> `boat delete --yes`
         └── Idle (no agents, no CPU builds) -> `boat stop self` (never touches snapshots)
```

---

## Features

- **Immutable 2-tier golden snapshots:** `base-t3` (Ubuntu + `t3` + watchdog + optional `klav` binary) → `env-<project>` (always built cleanly from `base-t3`, smoke-tested, and wiped of `~/.t3/userdata` before snapshotting). Supports daily UTC auto-bake (`baseBakeHourUtc` / `bakeHourUtc`).
- **Zero-knowledge secrets with [Klav](https://klav.dev):** Optionally provisions a scoped ephemeral Klav agent (`boat-<sandboxId>`) per disposable VM (`klav agent register` + non-interactive `approve --project ... --expires ...`), registers `klav mcp` inside the VM, and immediately revokes it (`klav agent revoke`) when the chat is archived or deleted. Plaintext `.env` injection from the sentinel host is also supported as a fallback.
- **Auto-register project & launch initial thread:** Automatically registers `project.workdir` on fresh VMs (`t3 project add` / `POST /api/projects/mutate`) so the repo appears immediately in T3 Mobile, and supports passing `prompt` to `sentinel_spawn` / `POST /api/spawn` to start a thread right away (`orchestration.launchThread`).
- **Zero re-pairing & clean cursor reset:** Sentinel writes each slot's permanent UUID into `~/.t3/userdata/environment-id` before `t3` starts, stores SHA-256 hashed per-device bearer tokens, and strips stale `resumeFrom` / `afterSequence` cursors when a fresh VM replaces an old one.
- **Auto-delete on archive:** Archive or delete all top-level chats from Mac or T3 Mobile → after `archiveGraceSecs` (default 20s), sentinel checks for dirty/unpushed git state, warns via `notifyUrl`, and deletes (or pauses, if `archiveAction: stop`) the VM automatically.
- **Async Job API & Single-Writer Daemon:** Long operations (`spawn`, `wake`, `bake`) run as async jobs (`202` + `GET /api/jobs/:id`) with `idempotencyKey` support and `idleTimeout: 255` so HTTP/MCP clients never time out. All CLI commands talk to the running daemon.
- **Built-in stdio MCP server + Agent Skill:** Run `bun run sentinel mcp` and install [`skills/t3-boat-sentinel/SKILL.md`](skills/t3-boat-sentinel/SKILL.md) so personal assistants and coding agents can spawn chats, poll jobs, bake snapshots, and manage slots directly.
- **Optional push notifications:** Sends alerts to any webhook (such as [ntfy.sh](https://ntfy.sh)) when an agent finishes, needs input, or a sandbox is deleted after archive.

---

## Quickstart

### Option A: Proxmox LXC / Linux host (recommended, ~512MB RAM)

```bash
git clone https://github.com/<your-user>/t3-boat-sentinel /opt/t3-boat-sentinel
cd /opt/t3-boat-sentinel
bash scripts/install-lxc.sh

# 1. Authenticate Boat CLI once
boat onboard

# 2. Generate sentinel.yaml with cryptographic secrets
bun run sentinel init
$EDITOR sentinel.yaml

# 3. Point a single Cloudflare Tunnel wildcard to http://127.0.0.1:8080:
#      *.boat.example.com -> http://127.0.0.1:8080
#
#    IMPORTANT: Do NOT put Cloudflare Access on `boat-1..N` (T3 Mobile authenticates
#    with T3's cryptographic bearer token). You CAN protect `ctrl.boat.example.com`
#    with Cloudflare Access — just add a Bypass rule for `/internal/watchdog`
#    (already authenticated with `watchdogSecret`).

# 4. Start the daemon
sudo systemctl start t3-boat-sentinel
```

### Option B: Docker Compose

```bash
cp sentinel.example.yaml sentinel.yaml
# Set CONTROL_SECRET, WATCHDOG_SECRET (and optional CLOUDFLARE_TUNNEL_TOKEN) in .env
docker compose run --rm sentinel boat onboard
docker compose up -d
# Or with bundled cloudflared sidecar:
docker compose --profile tunnel up -d
```

---

## First-time setup

1. **Bake your immutable base snapshot (`base-t3`):**
   ```bash
   bun run sentinel bake base
   ```
   *(To pre-authenticate `claude` / `codex` inside `base-t3`: spin up a temp VM with `boat new --from base-t3`, run `boat ssh <id>` → `claude auth login` / `codex login`, exit, then `boat stop <id> && boat snapshot <id> base-t3 && boat delete <id> --yes`.)*

2. **Bake each project's immutable snapshot (e.g. `my-app`):**
   ```bash
   bun run sentinel bake my-app
   ```

3. **Pair your slots ONCE in T3 Desktop and T3 Mobile:**
   ```bash
   bun run sentinel pair boat-1
   bun run sentinel pair boat-2
   ```
   Paste each URL in **T3 Code → Settings → Environments → Add environment**.

---

## Daily workflow

### From T3 Mobile (1-tap iOS Shortcut)
Create an iOS Shortcut with **Get Contents of URL**:
- **URL:** `https://ctrl.boat.example.com/api/spawn`
- **Method:** `POST`
- **Headers:** `Authorization: Bearer <your-controlSecret>`
- **Body (JSON):** `{"project": "my-app"}`

Tap the shortcut → Sentinel spawns a fresh VM from `env-my-app` into a free slot (e.g. `boat-2`) in ~10s → open `Boat 2` in T3 Mobile → code away. When done, **archive the chat** on Mac or Mobile → Sentinel deletes the VM automatically 20 seconds later.

### From CLI
```bash
bun run sentinel status
bun run sentinel spawn my-app          # picks a free slot
bun run sentinel wake boat-1
bun run sentinel sleep boat-1
bun run sentinel reset boat-1          # delete VM immediately
bun run sentinel revoke boat-1         # revoke paired tokens for lost devices
bun run sentinel bake my-app           # refresh immutable golden snapshot
```

### As an MCP server (Claude Code / Codex / T3 Code)
```json
{
  "mcpServers": {
    "t3-boat-sentinel": {
      "command": "bun",
      "args": ["run", "/path/to/t3-boat-sentinel/src/index.ts", "mcp"],
      "env": {
        "SENTINEL_URL": "https://ctrl.boat.example.com",
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
