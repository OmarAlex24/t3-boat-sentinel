# Contributing to t3-boat-sentinel

Contributions are welcome!

## Design principles
1. **Zero T3 Code forks & minimal protocol coupling:** Keep the proxy transparent for HTTP/WS traffic and avoid caching or re-implementing T3's internal RPC schema.
2. **Immutable golden snapshots:** Active chats, sleep cycles, and watchdogs must never overwrite named snapshots. Only explicit `bake` runs create or update snapshots.
3. **Fail-safe billing protection:** Always queue sandbox IDs for deletion before destructive operations so the reaper guarantees zero leaked VMs.

## Running checks locally

```bash
bun install
bun run typecheck
bash -n scripts/provision-base.sh
bash -n scripts/install-lxc.sh
node --check scripts/watchdog.mjs
bun test
```
