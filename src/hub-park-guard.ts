/**
 * Whether a VM may be parked (stopped because its chats are settled), from its thread rows
 * (`OrchestrationV2ThreadShell`, as its shell snapshot lists them, delegated children included).
 *
 * The predicates are the VM watchdog's `inspectShell` (`scripts/watchdog.mjs`, which runs on the VM
 * and cannot import this file): a VM parks only when it would report `all-archived` itself.
 */

const LIVE_STATUSES = new Set(["queued", "preparing", "starting", "running", "waiting"]);
/** Runtime requests T3 answers itself: they never wait on the person. */
const SELF_SETTLED = new Set(["auth_refresh", "dynamic_tool_call"]);

export type ParkBlocker = { threadId: string; reason: "running" | "not settled" };

/** A delegated or subagent chat: it lives inside its parent's chat, never on its own in the sidebar. */
export function isChildThreadRow(row: Record<string, unknown>): boolean {
  const lineage = row.lineage as { relationshipToParent?: unknown } | undefined;
  return lineage?.relationshipToParent === "subagent" || row.delegatedFrom != null;
}

/** Settled as T3 shows it: an explicit override wins over `settledAt`. */
export function isSettledThreadRow(row: Record<string, unknown>): boolean {
  if (row.settled === true) return true;
  if (row.settledOverride === "settled") return true;
  if (row.settledOverride === "unsettled") return false;
  return row.settledAt != null;
}

/** A run or turn is under way on it (the row's status, its active run, or the run's activity). */
export function isLiveThreadRow(row: Record<string, unknown>): boolean {
  return row.activeRunId != null || row.activityRunStatus != null || LIVE_STATUSES.has(String(row.status));
}

/** Blocked on a question only the person can answer (a VM like that idle-sleeps, as the watchdog does). */
export function waitsOnPerson(row: Record<string, unknown>): boolean {
  const req = row.pendingRuntimeRequest as { blocking?: unknown; kind?: unknown; userOnly?: unknown } | null | undefined;
  if (!req || req.blocking === false) return false;
  if (SELF_SETTLED.has(String(req.kind))) return false;
  if (req.userOnly) return false;
  const delegated = (row.lineage as { relationshipToParent?: unknown } | undefined)?.relationshipToParent === "subagent" || row.delegatedFrom != null;
  return !(delegated && req.kind === "user_input");
}

function hasPendingBackgroundWork(row: Record<string, unknown>): boolean {
  const tasks = Array.isArray(row.pendingBackgroundTasks) ? (row.pendingBackgroundTasks as Array<Record<string, unknown>>) : [];
  return tasks.some((task) => !(String(task.taskId ?? "").startsWith("pull-request-watch:") && task.awaited !== true));
}

/**
 * Why the VM holding these rows must not be parked, or null when it may: a chat (any, delegated
 * children included) has a live run or pending background work and is not just waiting on the
 * person, or an open top-level chat (not a subagent, archived or deleted) is not settled.
 */
export function parkBlocker(rows: Array<Record<string, unknown>>): ParkBlocker | null {
  for (const row of rows) {
    if (hasPendingBackgroundWork(row) || (isLiveThreadRow(row) && !waitsOnPerson(row))) {
      return { threadId: String(row.id ?? ""), reason: "running" };
    }
  }
  for (const row of rows) {
    if ((row.lineage as { relationshipToParent?: unknown } | undefined)?.relationshipToParent === "subagent") continue;
    if (row.archivedAt != null || row.deletedAt != null) continue;
    if (!isSettledThreadRow(row)) return { threadId: String(row.id ?? ""), reason: "not settled" };
  }
  return null;
}
