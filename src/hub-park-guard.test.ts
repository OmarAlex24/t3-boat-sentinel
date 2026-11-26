import { describe, expect, test } from "bun:test";
import { isChildThreadRow, parkBlocker } from "./hub-park-guard.ts";

const at = "2026-11-26T00:53:34.448Z";
const row = (id: string, extra: Record<string, unknown> = {}) => ({
  id,
  lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: id },
  status: "idle",
  activeRunId: null,
  activityRunStatus: null,
  settledAt: at,
  archivedAt: null,
  deletedAt: null,
  ...extra,
});
const child = (id: string, extra: Record<string, unknown> = {}) =>
  row(id, { lineage: { parentThreadId: "p", relationshipToParent: "subagent", rootThreadId: "p" }, ...extra });

describe("parkBlocker (when a VM may be parked)", () => {
  test("every open top-level chat settled and nothing live: parks", () => {
    expect(parkBlocker([row("p"), child("c", { settledAt: null }), row("gone", { settledAt: null, archivedAt: at })])).toBeNull();
  });

  test("a running parent blocks, whatever its children are", () => {
    expect(parkBlocker([row("p", { status: "running", activeRunId: "run-22", settledAt: null }), child("c")])).toEqual({
      threadId: "p",
      reason: "running",
    });
    // An active run alone is enough, even with a settled-looking status
    expect(parkBlocker([row("p", { activeRunId: "run-22" })])?.reason).toBe("running");
    expect(parkBlocker([row("p", { activityRunStatus: "starting" })])?.reason).toBe("running");
  });

  test("a running delegated child blocks too", () => {
    expect(parkBlocker([row("p"), child("c", { status: "running" })])).toEqual({ threadId: "c", reason: "running" });
  });

  test("pending background work blocks, except a pull-request watch nobody waits on", () => {
    expect(parkBlocker([row("p", { pendingBackgroundTasks: [{ taskId: "bash:1" }] })])?.reason).toBe("running");
    expect(parkBlocker([row("p", { pendingBackgroundTasks: [{ taskId: "pull-request-watch:42" }] })])).toBeNull();
    expect(parkBlocker([row("p", { pendingBackgroundTasks: [{ taskId: "pull-request-watch:42", awaited: true }] })])?.reason).toBe("running");
  });

  test("a chat waiting on the person does not count as running (as the watchdog)", () => {
    const waiting = row("p", { status: "waiting", activeRunId: "run-1", pendingRuntimeRequest: { kind: "user_input", blocking: true } });
    expect(parkBlocker([waiting])).toBeNull();
    // A delegated child's question is answered by its parent agent: it is still working
    const childAsks = child("c", { status: "waiting", activeRunId: "run-2", pendingRuntimeRequest: { kind: "user_input", blocking: true } });
    expect(parkBlocker([row("p"), childAsks])?.reason).toBe("running");
  });

  test("an open top-level chat not settled blocks; an unsettle override wins over settledAt", () => {
    expect(parkBlocker([row("p"), row("q", { settledAt: null })])).toEqual({ threadId: "q", reason: "not settled" });
    expect(parkBlocker([row("p", { settledOverride: "unsettled" })])).toEqual({ threadId: "p", reason: "not settled" });
    expect(parkBlocker([row("p", { settledAt: null, settledOverride: "settled" })])).toBeNull();
  });

  test("isChildThreadRow: subagent lineage or delegated", () => {
    expect(isChildThreadRow(child("c"))).toBe(true);
    expect(isChildThreadRow(row("d", { delegatedFrom: { threadId: "p" } }))).toBe(true);
    expect(isChildThreadRow(row("p"))).toBe(false);
  });
});
