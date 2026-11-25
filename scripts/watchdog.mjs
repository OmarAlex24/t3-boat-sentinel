// Boat-side watchdog for t3-boat-sentinel (Node built-ins only, 0 npm deps).
//
// Design (sentinel owns stop/delete, watchdog reports + acts as dead-man safety net):
// 1. Persists `saw-thread` on disk so restarts never forget a chat existed.
// 2. Archive detection (`deleteWhen: "archived" | "archived-or-settled"`):
//    Once >=1 top-level thread existed and ALL top-level threads are archived/deleted
//    (or settled) with no running agents for `archiveGraceSecs`, keeps posting
//    `event: "all-archived"` (including dirty git check) on EVERY tick until sentinel
//    stops/deletes this VM. If sentinel is unreachable past `sleepAfterSecs`, falls back
//    to `boat stop self` so the VM never bills forever.
// 3. Idle detection: reports `event: "idle"` to sentinel (which runs `boat stop`).
//    If sentinel is unreachable or `inspectShell` fails for > `maxUnreachableSecs`,
//    falls back to `boat stop self` as a billing backstop.
//    Hosts without the `boat` CLI (Celesto) have no backstop: the VM keeps running until
//    sentinel is reachable again and stops it. That is logged once, not on every tick.
// 4. NEVER modifies or overwrites any saved snapshot!

import { execFile } from "node:child_process";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { availableParallelism, homedir } from "node:os";
import { delimiter, join } from "node:path";
import { DatabaseSync } from "node:sqlite";

const HOME = homedir();
const SENTINEL_DIR = join(HOME, ".t3-sentinel");
const CONFIG_PATH = join(SENTINEL_DIR, "watchdog.json");
const SAW_THREAD_PATH = join(SENTINEL_DIR, "saw-thread");
const DB_PATH = join(HOME, ".t3", "userdata", "statev2.sqlite");
const T3_ORIGIN = "http://127.0.0.1:3773";
const TICK_MS = 5_000;
const HEARTBEAT_EVERY_MS = 60_000;
const CPU_BUSY_CORES = 0.5;
const CPU_HOLD_MS = 30 * 60_000;
const MAX_SHELL_FAIL_MS = 20 * 60_000;
const LIVE_STATUSES = new Set(["queued", "preparing", "starting", "running", "waiting"]);
const SELF_SETTLED = new Set(["auth_refresh", "dynamic_tool_call"]);
const PERSON_EVENTS = [
  "thread.created",
  "thread.visit-recorded",
  "thread.visited",
  "thread.marked-unread",
  "thread.archived",
  "thread.unarchived",
  "thread.deleted",
  "thread.settled",
  "thread.pinned",
  "thread.unpinned",
  "thread.snoozed",
  "thread.unsnoozed",
];

const config = JSON.parse(readFileSync(CONFIG_PATH, "utf8"));
const log = (...args) => console.log(new Date().toISOString(), "[watchdog]", ...args);
// Lines that would otherwise repeat every tick while sentinel or T3 is down: at most one per key per window
const THROTTLE_MS = 5 * 60_000;
const lastLoggedAt = new Map();
const logThrottled = (key, ...args) => {
  const now = Date.now();
  const last = lastLoggedAt.get(key);
  if (last !== undefined && now - last < THROTTLE_MS) return;
  lastLoggedAt.set(key, now);
  log(...args);
};
// `boat stop self` needs the boat CLI, which only Boat VMs have
const BOAT_BIN = (() => {
  const ascii = join(HOME, ".ascii", "bin", "boat");
  if (existsSync(ascii)) return ascii;
  for (const dir of (process.env.PATH ?? "").split(delimiter)) {
    if (dir && existsSync(join(dir, "boat"))) return join(dir, "boat");
  }
  return null;
})();
let warnedNoBackstop = false;

let everHadTopLevelThread = existsSync(SAW_THREAD_PATH);
let allArchivedSince = null;
let lastActivityAt = Date.now();
let lastUsedAt = Date.now();
let lastTickAt = Date.now();
let lastHeartbeatAt = 0;
let shellFailingSince = null;
let wasAgentWorking = false;
let eventCursor = null;

const CPU_COUNT = availableParallelism();
let cpuPrev = readCpu();
let cpuLoad = 0;

function markSawThread() {
  if (!everHadTopLevelThread) {
    everHadTopLevelThread = true;
    try {
      writeFileSync(SAW_THREAD_PATH, new Date().toISOString(), { mode: 0o600 });
    } catch {
      // ignore
    }
  }
}

function readCpu() {
  const fields = readFileSync("/proc/stat", "utf8").split("\n")[0].trim().split(/\s+/).slice(1).map(Number);
  const idle = fields[3] + (fields[4] ?? 0);
  const total = fields.reduce((a, b) => a + b, 0);
  return { busy: total - idle, total };
}

function sampleCpu() {
  const now = readCpu();
  const total = now.total - cpuPrev.total;
  const usage = total > 0 ? ((now.busy - cpuPrev.busy) / total) * CPU_COUNT : 0;
  cpuPrev = now;
  cpuLoad = cpuLoad * 0.8 + usage * 0.2;
}

function waitsOnPerson(thread) {
  const req = thread.pendingRuntimeRequest;
  if (!req || req.blocking === false) return false;
  if (SELF_SETTLED.has(req.kind)) return false;
  if (req.userOnly) return false;
  const delegated = thread.lineage?.relationshipToParent === "subagent" || thread.delegatedFrom != null;
  return !(delegated && req.kind === "user_input");
}

function readDbActivity() {
  if (!existsSync(DB_PATH)) return { personActed: false, sawCreatedEvent: false };
  const db = new DatabaseSync(DB_PATH, { readOnly: true });
  try {
    let table = "orchestration_events";
    try {
      const hasV2 = db
        .prepare("SELECT 1 AS ok FROM sqlite_master WHERE type = 'table' AND name = 'orchestration_v2_events'")
        .get();
      if (hasV2?.ok) table = "orchestration_v2_events";
    } catch {
      // fall back to v1 table name
    }

    let sawCreatedEvent = false;
    try {
      const created = db
        .prepare(`SELECT 1 AS ok FROM ${table} WHERE event_type = 'thread.created' LIMIT 1`)
        .get();
      sawCreatedEvent = Boolean(created?.ok);
    } catch {
      // table may not exist yet
    }

    if (eventCursor === null) {
      try {
        eventCursor = db.prepare(`SELECT MAX(sequence) AS s FROM ${table}`).get()?.s ?? 0;
      } catch {
        eventCursor = 0;
      }
      return { personActed: false, sawCreatedEvent };
    }

    try {
      const placeholders = PERSON_EVENTS.map(() => "?").join(",");
      const row = db
        .prepare(
          `SELECT MAX(sequence) AS last, SUM(event_type IN (${placeholders})) AS person
           FROM ${table} WHERE sequence > ?`,
        )
        .get(...PERSON_EVENTS, eventCursor);
      eventCursor = row?.last ?? eventCursor;
      return { personActed: (row?.person ?? 0) > 0, sawCreatedEvent };
    } catch {
      return { personActed: false, sawCreatedEvent };
    }
  } finally {
    db.close();
  }
}

function isThreadSettled(t) {
  if (t.settled === true) return true;
  if (t.settledOverride === "settled") return true;
  if (t.settledOverride === "unsettled") return false;
  return t.settledAt != null;
}

async function inspectShell() {
  const res = await fetch(`${T3_ORIGIN}/api/orchestration/shell`, {
    headers: {
      authorization: `Bearer ${config.t3Token}`,
      "x-t3-orchestration-protocol": "2",
    },
    signal: AbortSignal.timeout(10_000),
  });
  if (!res.ok) throw new Error(`shell HTTP ${res.status}`);
  const body = await res.json();
  const threads = Array.isArray(body.threads) ? body.threads : [];

  const includeSettled = config.deleteWhen === "archived-or-settled";
  const activeTopLevel = threads.filter((t) => {
    if (t.lineage?.relationshipToParent === "subagent") return false;
    if (t.archivedAt != null || t.deletedAt != null) return false;
    if (includeSettled && isThreadSettled(t)) return false;
    return true;
  });

  const running = (t) => t.activeRunId != null || t.activityRunStatus != null || LIVE_STATUSES.has(t.status);
  const liveAgents = threads.filter((t) => {
    const hasBg = (t.pendingBackgroundTasks ?? []).some(
      (task) => !(task.taskId?.startsWith("pull-request-watch:") && task.awaited !== true),
    );
    return hasBg || (running(t) && !waitsOnPerson(t));
  });

  const waitingOnPerson = threads.filter((t) => running(t) && waitsOnPerson(t));
  return {
    activeTopLevelCount: activeTopLevel.length,
    liveAgentCount: liveAgents.length,
    waitingOnPersonCount: waitingOnPerson.length,
  };
}

function checkGitDirty() {
  return new Promise((resolve) => {
    const dir = config.projectWorkdir;
    if (!dir || !existsSync(join(dir, ".git"))) return resolve({ dirty: false, unpushed: false });
    execFile("git", ["-C", dir, "status", "--porcelain"], { timeout: 5_000 }, (err, stdout) => {
      const dirty = !err && stdout.trim().length > 0;
      execFile("git", ["-C", dir, "log", "@{u}..", "--oneline"], { timeout: 5_000 }, (err2, stdout2) => {
        const unpushed = !err2 && stdout2.trim().length > 0;
        resolve({ dirty, unpushed });
      });
    });
  });
}

async function notifySentinel(event, detail = {}) {
  if (!config.callbackUrl || !config.watchdogSecret) return false;
  try {
    const res = await fetch(config.callbackUrl, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${config.watchdogSecret}`,
      },
      body: JSON.stringify({
        slot: config.slot,
        sandboxId: config.sandboxId,
        event,
        detail,
        at: new Date().toISOString(),
      }),
      signal: AbortSignal.timeout(15_000),
    });
    return res.ok;
  } catch (err) {
    logThrottled(`callback:${event}`, `callback ${event} failed:`, err.message);
    return false;
  }
}

function stopSelfBackstop(reason) {
  if (!BOAT_BIN) {
    if (!warnedNoBackstop) {
      warnedNoBackstop = true;
      log(`no boat CLI on this host: cannot stop self (${reason}); the VM keeps running until sentinel stops it`);
    }
    return Promise.resolve();
  }
  logThrottled(`backstop:${reason}`, `backstop stopping self (${reason})`);
  return new Promise((resolve) => {
    execFile(BOAT_BIN, ["stop", "self"], { timeout: 60_000 }, (err) => {
      if (err) logThrottled("backstop-failed", "boat stop self failed:", err.message);
      resolve();
    });
  });
}

async function tick() {
  const now = Date.now();
  if (now - lastTickAt > 30_000) {
    log("resumed after sleep gap, resetting idle timer");
    lastActivityAt = now;
    lastUsedAt = now;
  }
  lastTickAt = now;

  sampleCpu();
  const { personActed, sawCreatedEvent } = readDbActivity();
  if (sawCreatedEvent) markSawThread();
  if (personActed) {
    lastActivityAt = now;
    lastUsedAt = now;
  }

  let shell;
  try {
    shell = await inspectShell();
    shellFailingSince = null;
  } catch (err) {
    logThrottled("shell-unavailable", "t3 shell unavailable:", err.message);
    shellFailingSince ??= now;
    // Billing backstop: if T3 server stays unreachable/incompatible for 20m, stop self
    if (now - shellFailingSince >= MAX_SHELL_FAIL_MS) {
      await notifySentinel("idle", { reason: "t3-unreachable" });
      await stopSelfBackstop("t3-unreachable");
    }
    return;
  }

  if (shell.activeTopLevelCount > 0) {
    markSawThread();
    allArchivedSince = null;
  }

  // Periodic heartbeat so sentinel knows VM is healthy and alive
  if (now - lastHeartbeatAt >= HEARTBEAT_EVERY_MS) {
    lastHeartbeatAt = now;
    void notifySentinel("heartbeat", {
      activeThreads: shell.activeTopLevelCount,
      liveAgents: shell.liveAgentCount,
      cpuLoad: Number(cpuLoad.toFixed(2)),
    });
  }

  const workingNow = shell.liveAgentCount > 0;
  if (wasAgentWorking && !workingNow) {
    const kind = shell.waitingOnPersonCount > 0 ? "needs-input" : "agent-done";
    void notifySentinel(kind, { waitingOnPerson: shell.waitingOnPersonCount });
  }
  wasAgentWorking = workingNow;

  // 1. All chats archived/deleted -> keep notifying sentinel every tick until VM is deleted/stopped
  if (everHadTopLevelThread && shell.activeTopLevelCount === 0 && shell.liveAgentCount === 0) {
    if (allArchivedSince === null) {
      allArchivedSince = now;
      log(`all chats archived/deleted; waiting ${config.archiveGraceSecs}s grace`);
    }
    if (now - allArchivedSince >= config.archiveGraceSecs * 1000) {
      const git = await checkGitDirty();
      const ok = await notifySentinel("all-archived", git);
      // If sentinel is unreachable past sleepAfterSecs, fall back to stopping self so VM never bills forever
      if (!ok && now - allArchivedSince >= config.sleepAfterSecs * 1000) {
        await stopSelfBackstop("all-archived-sentinel-unreachable");
      }
    }
    return;
  }

  // 2. Idle check -> ask sentinel to stop VM (with local backstop if sentinel is unreachable)
  const cpuBusy = cpuLoad > CPU_BUSY_CORES && now - lastUsedAt < CPU_HOLD_MS;
  if (workingNow || cpuBusy) {
    lastActivityAt = now;
    return;
  }

  const idleSecs = (now - lastActivityAt) / 1000;
  if (idleSecs >= config.sleepAfterSecs) {
    logThrottled("idle", `idle for ${Math.round(idleSecs)}s -> requesting sentinel to stop VM`);
    const ok = await notifySentinel("idle", { idleSecs: Math.round(idleSecs) });
    // If sentinel doesn't stop us within 60s (e.g. sentinel offline), stop self as safety net
    if (!ok || idleSecs >= config.sleepAfterSecs + 60) {
      await stopSelfBackstop("idle-backstop");
    }
  }
}

log(`started for slot=${config.slot} sandbox=${config.sandboxId} sleep=${config.sleepAfterSecs}s archiveGrace=${config.archiveGraceSecs}s deleteWhen=${config.deleteWhen}`);
setInterval(() => {
  void tick();
}, TICK_MS);
void tick();
