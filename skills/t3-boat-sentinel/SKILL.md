---
name: t3-boat-sentinel
description: Manage disposable Boat cloud sandboxes for T3 Code via t3-boat-sentinel (inspect Hub sandboxes, add/bake GitHub repos dynamically, spawn/reset VMs, and refresh immutable golden snapshots). Use when asked to check Boat sandboxes, add a repo to Boat, bake snapshots, or destroy/reset a sandbox.
---

# t3-boat-sentinel Skill

Use this skill to orchestrate isolated, disposable Boat (`boat.dev`) cloud VMs managed by `t3-boat-sentinel`.

## Core Model
- **1 Environment → N parallel Boat VMs (Hub Mode):** T3 Desktop & Mobile pair once to a single `Boat` environment. Every new chat automatically spawns 1 fresh isolated Boat VM (`defaultSize`, e.g. `large`) from that project's immutable golden snapshot (`env-<project>`).
- **Strictly immutable golden snapshots (`base-t3`, `env-<project>`):** Chats never mutate snapshots. Only `sentinel_bake` (or `sentinel_add_project` with `bake: true`) updates them inside a throwaway VM.
- **Auto-cleanup ($0 idle):** Settling or archiving a chat automatically destroys ONLY that chat's Boat VM, invalidates its per-VM `LlmProxy` token, and revokes its ephemeral Klav sandbox after `archiveGraceSecs` (default 20s).

## MCP Tools (preferred when `t3-boat-sentinel` MCP is attached)

Long operations (`sentinel_add_project` with `bake: true`, `sentinel_bake`, `sentinel_spawn`, `sentinel_wake`) return `{ jobId, status: "running" }` immediately so MCP clients never time out. Poll `sentinel_job` until `status` is `"completed"` or `"failed"`.

| Tool | Behavior |
|---|---|
| `sentinel_status` | Read-only. Lists the Hub environment (active per-chat Boat VMs, thread counts, Klav agents, limits), any legacy slots, configured & dynamic projects, and golden snapshot freshness. Call first. |
| `sentinel_add_project` | Registers a GitHub repo (`owner/repo` or git URL) dynamically without editing `sentinel.yaml`, pushes it live into T3 Code Desktop & Mobile, and optionally bakes its immutable `env-<name>` snapshot in an async job. |
| `sentinel_bake` | Async job. Rebuilds an immutable golden snapshot (`"base"` or a project name) inside a throwaway VM. |
| `sentinel_job` | Read-only. Returns `{ status, result, error }` for a `jobId`. |
| `sentinel_reset` | **Destructive.** Permanently deletes a Boat VM and revokes its tokens/Klav sandbox. Pass `slot: "hub:<sandboxId>"` for one Hub VM, `slot: "hub"` for all Hub VMs, or a legacy slot name (`boat-1`). Confirm before deleting a VM with unpushed work. |
| `sentinel_spawn` | Async job (legacy fixed-slot mode). Spawns a fresh VM into a free fixed slot and optionally launches an initial `prompt`. |
| `sentinel_wake` / `sentinel_sleep` | Wake or pause a legacy fixed slot. |

## Standard Workflows

### 1. Inspect active Boat VMs & projects
1. Call `sentinel_status`.
2. Summarize active Hub sandboxes (`hub.sandboxes`: `sandboxId`, `project`, `phase`, `threadIds.length`, `klavAgent`), active/max capacity, and project snapshot freshness.

### 2. Add a new GitHub repo to Boat automatically
1. Call `sentinel_add_project({ repo: "owner/repo", bake: true })` (optionally pass `size` or `klavProfiles`).
2. Poll `sentinel_job({ jobId })` until `status === "completed"`.
3. Confirm the repo is live in T3 Code Desktop & Mobile and its immutable `env-<name>` snapshot is ready.

### 3. Refresh an immutable golden snapshot
1. Call `sentinel_bake({ target: "<project>" })` (or `target: "base"` first when updating `t3` / base tooling).
2. Poll `sentinel_job({ jobId })` until `status === "completed"` and report `{ snapshot, t3Version }`.
3. Existing active chats are unaffected; future chats spawn from the refreshed snapshot.

### 4. CLI fallback (on the sentinel host — all commands talk to the running daemon)
```bash
bun run sentinel status
bun run sentinel pair hub
bun run sentinel bake base
bun run sentinel bake <project>
bun run sentinel reset hub:<sandboxId>
bun run sentinel revoke hub
```
