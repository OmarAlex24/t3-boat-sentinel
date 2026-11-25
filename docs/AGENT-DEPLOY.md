# Agent Deployment Guide (`t3-boat-sentinel`)

> **Designed for coding agents (Claude Code, T3 Code, Codex, Antigravity).**
> Most users shouldn't wire tunnels, DNS, snapshots, and Cloudflare Access by hand — hand this file to your agent and let it deploy everything end-to-end while asking you only for credentials/approvals.

## One-Prompt Quickstart (for humans to paste to their agent)

```text
Read https://github.com/OmarAlex24/t3-boat-sentinel/blob/main/docs/AGENT-DEPLOY.md and deploy t3-boat-sentinel for me.
Ask me first whether I want:
(A) Self-hosted on my Proxmox LXC / VPS / Docker (~$0–$4/mo, full Boat CLI snapshots + private previews + optional LAN LLM proxy), or
(B) Serverless on Cloudflare Workers + Durable Objects ($5/mo Workers Paid, zero VPS to manage, HTTP sandbox providers like Daytona/E2B/Freestyle/Boat API).
Then ask only for the domain and any credentials needed, set up Cloudflare Access on the control plane, and verify everything end-to-end.
```

---

## Decision Matrix (Agent: confirm this with the user first)

| Hosting Target | Cost | Best For | Capabilities & Tradeoffs |
|---|---|---|---|
| **A. Proxmox LXC / $4 VPS / Docker** *(Recommended)* | **$0 extra** (existing host) or ~$4/mo | Boat CLI users, homelabs, Klav/LAN proxies | Full native `BoatProvider` (immutable 2-tier snapshots `base-t3 → env-*`, `pause/resume`, `--private` `_port_auth` cookie gate), zero-trust LAN `LlmProxy`, `klav-proxy`, 5-min bakes with zero timeouts. |
| **B. Cloudflare Workers + Durable Objects** *(No VPS required)* | **$5/mo** (Workers Paid) | Users with **zero** VPS/homelab who want 100% serverless | Runs `HubServer` + `ControlPlane` at Cloudflare's edge (no `cloudflared` tunnel, no Linux host). Requires an **HTTP-API sandbox provider** (e.g. `@opencoredev/sandbox-sdk` with Daytona, E2B, Freestyle, Upstash Box, Railway, or Boat's REST API) — cannot run local CLI binaries (`boat`/`klav`) or reach a private LAN without an inbound tunnel. |

---

## Path A: Agent Runbook — Self-Hosted (Proxmox LXC / VPS / Docker)

Execute these steps in order. **Never print or commit secrets.**

### 1. Gather user inputs
Ask the user for:
1. Target host (`ssh <host>` or local Docker).
2. Domain managed in Cloudflare (e.g. `example.com` → `boat.example.com`, `boat-ctrl.example.com`, optional `boat-llm.example.com`).
3. Sandbox provider: **Boat** (default) or an `@opencoredev/sandbox-sdk` adapter (`daytona`, `e2b`, `docker`, `freestyle`, `boxd`, `upstash`, `railway`).
4. First GitHub repo(s) to pre-bake (e.g. `owner/repo`) and desired default VM size (`default`, `medium`, `large`).
5. Optional: LLM gateway URL/key (`llmProxy`), Cloudflare API token (to automate Tunnel + Access), and email for Cloudflare Access.

### 2. Install & configure Sentinel on the host
```bash
git clone https://github.com/OmarAlex24/t3-boat-sentinel /opt/t3-boat-sentinel
cd /opt/t3-boat-sentinel
bash scripts/install-lxc.sh   # or use `docker compose --profile tunnel up -d`
bun run sentinel init         # creates sentinel.yaml (0600) with random controlSecret
```
- If using Boat, have the user complete `boat onboard` once on the host.
- If using `@opencoredev/sandbox-sdk`, run `bun add @opencoredev/sandbox-sdk` and set `provider: { type: sandbox-sdk, adapter: <name>, options: {} }` in `sentinel.yaml`.
- Recommended `sentinel.yaml` defaults:
  - `hub: { host: boat.<domain>, label: Boat, maxSandboxes: 20 }`
  - `slots: []`
  - `boat.hostVisibility: private` (enforces Boat's `_port_auth` cookie gate → external scanners get `403`)
  - `boat.defaultSize: large` (or `default` for lower cost)
  - `boat.deleteWhen: archived-or-settled`, `boat.archiveAction: delete`
  - `t3.version`: pin to match the user's T3 Desktop/Mobile version; `t3.channel`: `stable` or `nightly`.
  - Optional: `t3.prepareAcpAgents: [devin]`, `t3.installProviders: [antigravity]`.
  - Optional: `t3.persistProviderSessions: [antigravity]` keeps the user's Antigravity sign-in across chat VMs (stored 0600 under `<stateDir>/credentials/`, never in snapshots; `sentinel forget-session antigravity` removes it).
  - To turn on Devin and Antigravity, follow [Devin and Antigravity](../README.md#devin-and-antigravity) in the README: copy [`t3-settings.example.json`](../t3-settings.example.json) and set `t3.settingsFile` to its absolute path, re-bake base and then every project, and sign in from a running chat.

### 3. Wire Cloudflare Tunnel + Wildcard DNS
Create one Cloudflare Tunnel pointing `*.<domain>` → `http://127.0.0.1:8080` and add wildcard CNAME `*` → `<tunnel-id>.cfargotunnel.com` (proxied).

### 4. Lock Down the Control Plane with Cloudflare Access (MANDATORY)
Using Cloudflare Zero Trust API (or dashboard), create **two** Self-Hosted Access Applications:
1. **`boat-ctrl.<domain>`** → Policy **Allow**: user's email (`include: [{ email: { email } }]`).
2. **`boat-ctrl.<domain>/internal/watchdog`** → Policy **Bypass**: `Everyone` *(already authenticated with per-VM 256-bit `watchdogSecret`)*.

Do **NOT** put interactive Cloudflare Access on `boat.<domain>` (T3 Desktop/Mobile authenticate via OAuth pairing + 256-bit Bearer token) or `boat-llm.<domain>` (uses per-VM ephemeral `t3llm_...` tokens).

Then copy the `teamDomain` (from `GET /accounts/{id}/access/organizations` → `auth_domain`) and App 1's `aud` tag into `sentinel.yaml` for **double-lock origin RS256 verification**:
```yaml
security:
  cloudflareAccess:
    teamDomain: <team>.cloudflareaccess.com
    aud: <app-1-aud-tag>
    allowedEmails:
      - <user-email>
```

### 5. Bake Golden Snapshots, Pair & Verify
```bash
sudo systemctl restart t3-boat-sentinel
bun run sentinel bake base
bun run sentinel bake <first-project>
bun run sentinel pair hub    # give the single-use URL to the user for T3 Desktop, run again for T3 Mobile
```
Verify all security invariants before finishing:
- `GET https://boat-ctrl.<domain>/` → `302` redirect to Cloudflare Access login.
- `POST https://boat-ctrl.<domain>/internal/watchdog` (invalid Bearer) → `401` (NOT `302`!).
- `GET https://boat.<domain>/.well-known/t3/environment` → `200` JSON with `capabilities.projectCloneTracking: true`.
- `GET https://boat.<domain>/robots.txt` → `Disallow: /`.

---

## Path B: Agent Blueprint — Serverless on Cloudflare Workers (No VPS Required)

Use this path when the user **does not have a Proxmox host or VPS** and wants Sentinel hosted completely on **Cloudflare Workers ($5/mo Workers Paid)**.

### Why it works & exact architecture
`t3-boat-sentinel`'s core (`HubServer`, `ControlPlane`, `SlotAuth`, `AuthRateLimiter`, `CloudflareAccessVerifier`, `SandboxProvider`) is pure TypeScript using Web Standard APIs (`fetch`, `Request`, `Response`, `WebSocket`, `crypto.subtle`).

To deploy it serverless on Cloudflare Workers, the agent scaffolds a thin Workers adapter around Sentinel's modules:

```text
T3 Desktop / Mobile
   │  wss://boat.<domain>       -> Cloudflare Worker -> HubMultiplexerDO (Durable Object, WebSocket Hibernation)
   │  https://boat-ctrl.<domain> -> Cloudflare Access -> Cloudflare Worker -> HubMultiplexerDO (ControlPlane + SQLite state)
   ▼
HTTP Sandbox Provider (`@opencoredev/sandbox-sdk` or Boat HTTP API)
   ├── Daytona / E2B / Freestyle / Upstash Box / Railway / boxd / Boat API
   └── Disposable VMs running `t3 serve` + `watchdog.mjs` -> callback to `https://boat-ctrl.<domain>/internal/watchdog`
```

### Concrete mapping for the agent to implement
1. **Project scaffold:** Create a Wrangler project (`wrangler.jsonc`) with:
   - `compatibility_flags: ["nodejs_compat"]`
   - One SQLite-backed Durable Object binding: `HUB_DO` (`new_sqlite_classes: ["HubMultiplexerDO"]`)
   - Custom routes for `boat.<domain>/*` and `boat-ctrl.<domain>/*` (no `cloudflared` tunnel needed!).
   - Secrets stored via `wrangler secret put`: `CONTROL_SECRET`, provider API key (e.g. `DAYTONA_API_KEY` / `E2B_API_KEY` / `BOAT_API_KEY`).
2. **State persistence (`StateStore` → DO SQLite):**
   - Replace `fs.readFileSync/writeFileSync` in `src/state.ts` with `this.ctx.storage.sql` (one `kv` table storing `state_json`, atomic per DO turn).
3. **WebSocket multiplexing (`HubServer` → DO WebSocket Hibernation):**
   - Accept downstream T3 Desktop/Mobile sockets with `this.ctx.acceptWebSocket(server)` so **0 GB-s** are billed when idle (no active chats).
   - While a chat VM is running, the DO holds the upstream `HubVmBridge` WebSocket to that VM's `exposePort(3773)` URL (with any private `Cookie`/`Authorization` gate headers).
   - Use DO Alarms (`this.ctx.storage.setAlarm(...)`) for the 5-minute `reaperTick()` loop instead of `setInterval`.
4. **Provider selection (HTTP-only — no CLI!):**
   - Instantiate `SandboxSdkProvider` from `src/providers/sandbox-sdk.ts` with an HTTP-based adapter (`daytona`, `e2b`, `freestyle`, `upstash`, `railway`, `boxd`, or `boat` HTTP API from `@opencoredev/sandbox-sdk`).
   - Do **not** use `BoatProvider` (`src/providers/boat.ts`) on Workers since it spawns the `boat` CLI binary.
   - For long `bake` jobs (>30s), either use a provider with fast template builds or run the bake steps inside a **Cloudflare Workflow** (or chained DO Alarms) instead of holding a single HTTP request open.
5. **Security (identical to Path A):**
   - Reuse `src/security.ts` directly (`crypto.subtle` RS256 Cloudflare Access JWT verifier + `AuthRateLimiter` + security headers work natively on Workers!).
   - Create the same 2 Cloudflare Access apps (`boat-ctrl.<domain>` Allow user email + `boat-ctrl.<domain>/internal/watchdog` Bypass).

### Tradeoffs to explain clearly to the user before picking Path B
- **Pros:** Zero Linux servers to patch, zero `cloudflared` daemon, global Cloudflare edge, ~$5/mo flat for personal usage (128 MB DO awake ~8h/day ≈ 108k GB-s, well inside the 400k GB-s included in Workers Paid).
- **Cons:** Cannot directly reach a home LAN `cliproxy` or local `klav-proxy` (unless exposed via Cloudflare Tunnel + Service Token), and `sandbox-sdk`'s HTTP `boat()` adapter currently does not expose Boat's CLI named snapshots/resume (Daytona, Freestyle, boxd, or E2B are better fits on Workers if you want snapshots).
