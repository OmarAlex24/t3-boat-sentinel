---
name: t3-boat-sentinel
description: Manage disposable Boat cloud sandboxes for T3 Code via t3-boat-sentinel (spawn clean per-chat VMs, launch initial threads, wake/sleep/reset slots, and bake immutable golden snapshots). Use when asked to spin up a new Boat sandbox/chat, check slot status, reset a sandbox, or refresh project snapshots.
---

# t3-boat-sentinel Skill

Use this skill to orchestrate isolated, disposable Boat (`boat.dev`) cloud VMs managed by `t3-boat-sentinel`.

## Core Model
- **1 chat = 1 disposable Boat VM** running official `t3 serve`, assigned to a pre-paired fixed slot (`boat-1`, `boat-2`, ...).
- **Golden snapshots (`base-t3`, `env-<project>`) are strictly immutable during chat usage.** Never snapshot a dirty chat VM; only update golden snapshots via `sentinel_bake` (which always builds `env-<project>` from `base-t3`, smoke-tests `t3 serve`, and wipes `~/.t3/userdata` before snapshotting).
- **Auto-cleanup:** Archiving all top-level chats inside a slot automatically triggers VM deletion (or pause if `archiveAction: stop`) and revokes its ephemeral Klav agent credentials after `archiveGraceSecs` (default 20s).

## MCP Tools (preferred when `t3-boat-sentinel` MCP is attached)

Long operations (`sentinel_spawn`, `sentinel_wake`, `sentinel_bake`) return `{ jobId, status: "running" }` immediately so MCP clients never time out. Poll `sentinel_job` until `status` is `"completed"` or `"failed"`.

| Tool | Behavior |
|---|---|
| `sentinel_status` | Read-only. Lists slots (`phase`, `project`), projects, and golden snapshot freshness. Call first. |
| `sentinel_spawn` | Async job. Spawns a fresh VM from an immutable project snapshot into a free slot (refuses occupied slots unless `replace: true`), auto-registers the project in T3, and optionally starts an initial thread if `prompt` is given. Pass `idempotencyKey` to make retries safe. |
| `sentinel_job` | Read-only. Returns `{ status, result, error }` for a `jobId`. |
| `sentinel_wake` | Async job. Resumes a `sleeping` slot. |
| `sentinel_sleep` | Pauses a `running` slot without touching its golden snapshot. |
| `sentinel_reset` | **Destructive.** Permanently deletes the VM in a slot, revokes its Klav agent, and frees the slot. Confirm before resetting a slot with unpushed work. |
| `sentinel_bake` | Async job. Rebuilds an immutable golden snapshot (`"base"` or a project name) inside a throwaway VM. |

## Standard Workflows

### 1. Spawn a fresh chat for a project (e.g. from a personal assistant like Zumith)
1. Call `sentinel_status` to check configured projects and verify a free slot (`phase: "empty"`).
2. If no slot is free, report the occupied slots and ask before passing `replace: true` or calling `sentinel_reset`.
3. Call `sentinel_spawn` with `{ project: "<project>", idempotencyKey: "<unique-key>", prompt: "<task>", title: "<short title>" }`.
4. Poll `sentinel_job({ jobId })` until `status === "completed"`.
5. Report the `slot`, `label`, `url`, and `threadId` (plus any `launchError` if the VM booted cleanly but thread launch failed).

### 2. Refresh a project's immutable golden snapshot
1. Call `sentinel_bake({ target: "<project>" })` (or `target: "base"` first if updating `t3` / base tools).
2. Poll `sentinel_job({ jobId })` until `status === "completed"` and report `{ snapshot, t3Version }`.
3. Existing active chats are unaffected; future spawns use the refreshed immutable snapshot.

### 3. CLI fallback (on the sentinel host — all commands talk to the running daemon)
```bash
bun run sentinel status
bun run sentinel spawn <project> [slot]
bun run sentinel bake <base|project>
bun run sentinel pair <slot>
bun run sentinel revoke <slot> [clientId]
bun run sentinel reset <slot>
```
