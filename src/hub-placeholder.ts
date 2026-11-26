import { randomUUID } from "node:crypto";

/**
 * Thread records the Hub shows while a chat's VM boots: a shell thread (`orchestration.subscribeShell`)
 * and a thread projection (`orchestration.launchThread` result, `orchestration.subscribeThread`
 * snapshot). Their keys follow the T3 client's own decoders (`OrchestrationV2ThreadShell`,
 * `OrchestrationV2ThreadProjection`); a record the client cannot decode would break its whole
 * sidebar, so every required key is always present.
 *
 * A launch with a first message carries a run in `preparing`: the client then shows its native
 * "setting up" card (fed by `subscribeWorktreeSetup`) and holds further sends until the VM is up.
 * The client draws the timeline from `visibleTurnItems` (and retires its optimistic copy of the
 * message once `messages` holds its id), so the message is in both.
 *
 * VM state the user should see ("starting", "asleep", "waking", failures) is a `notification` turn
 * item with `source.kind: "system"`: the client draws it as a timeline row with the T3 icon, and an
 * alert icon when `outcome` is `failed`.
 */

export interface PlaceholderThread {
  shell: Record<string, unknown>;
  projection: Record<string, unknown>;
}

export interface PlaceholderInput {
  threadId: string;
  hubProjectId: string;
  title: string;
  modelSelection: Record<string, unknown>;
  providerInstanceId: string;
  runtimeMode: string;
  interactionMode: string;
  creationSource: string;
  branch: string | null;
  worktreePath: string | null;
  message: {
    messageId: string;
    text: string;
    attachments: unknown[];
    context?: unknown;
  } | null;
  now: string;
}

const CREATION_SOURCES = new Set(["web", "mobile", "mcp", "provider", "server"]);

function nonEmpty(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

/**
 * Reads what a placeholder needs from an `orchestration.launchThread` payload or a `thread.create`
 * command. Returns null when a required field is missing, so the caller keeps the awaited path
 * instead of showing a record the client would reject.
 */
export function placeholderInputFrom(
  kind: "launch" | "create",
  payload: Record<string, unknown>,
  threadId: string,
  hubProjectId: string,
  now = new Date().toISOString(),
): PlaceholderInput | null {
  const modelSelection =
    payload.modelSelection && typeof payload.modelSelection === "object"
      ? (payload.modelSelection as Record<string, unknown>)
      : null;
  const providerInstanceId = nonEmpty(modelSelection?.instanceId) ?? nonEmpty(modelSelection?.provider);
  const model = nonEmpty(modelSelection?.model);
  const title = nonEmpty(payload.title);
  const runtimeMode = nonEmpty(payload.runtimeMode);
  const interactionMode = nonEmpty(payload.interactionMode);
  if (!modelSelection || !providerInstanceId || !model || !title || !runtimeMode || !interactionMode) return null;

  const creationSource = CREATION_SOURCES.has(String(payload.creationSource)) ? String(payload.creationSource) : "web";
  let branch: string | null = null;
  let worktreePath: string | null = null;
  let message: PlaceholderInput["message"] = null;
  if (kind === "create") {
    branch = nonEmpty(payload.branch);
    worktreePath = nonEmpty(payload.worktreePath);
  } else {
    const strategy = (payload.workspaceStrategy ?? {}) as Record<string, unknown>;
    if (strategy.type === "existing_worktree") worktreePath = nonEmpty(strategy.worktreePath);
    if (strategy.type === "root" || strategy.type === "existing_worktree") branch = nonEmpty(strategy.branch);
    const initial = payload.initialMessage as Record<string, unknown> | undefined;
    if (initial && typeof initial.text === "string") {
      message = {
        messageId: nonEmpty(initial.messageId) ?? `msg_${randomUUID().replace(/-/g, "")}`,
        text: initial.text,
        attachments: Array.isArray(initial.attachments) ? initial.attachments : [],
        ...(initial.context !== undefined ? { context: initial.context } : {}),
      };
    }
  }
  return {
    threadId,
    hubProjectId,
    title,
    modelSelection,
    providerInstanceId,
    runtimeMode,
    interactionMode,
    creationSource,
    branch,
    worktreePath,
    message,
    now,
  };
}

export function buildPlaceholderThread(input: PlaceholderInput): PlaceholderThread {
  const { now, message } = input;
  const runId = message ? `run_hub_${randomUUID().replace(/-/g, "")}` : null;
  const creation = { createdBy: "user", creationSource: input.creationSource };
  const lineage = { parentThreadId: null, relationshipToParent: null, rootThreadId: input.threadId };
  const common = {
    ...creation,
    id: input.threadId,
    projectId: input.hubProjectId,
    title: input.title,
    providerInstanceId: input.providerInstanceId,
    modelSelection: input.modelSelection,
    runtimeMode: input.runtimeMode,
    interactionMode: input.interactionMode,
    branch: input.branch,
    worktreePath: input.worktreePath,
    lineage,
    forkedFrom: null,
    activeProviderThreadId: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    deletedAt: null,
  };

  const shell: Record<string, unknown> = {
    ...common,
    latestRunId: runId,
    latestRunRequestedAt: runId ? now : null,
    latestRunStartedAt: null,
    latestRunCompletedAt: null,
    activeRunId: runId,
    activityRunStartedAt: runId ? now : null,
    activityRunStatus: runId ? "preparing" : null,
    status: runId ? "preparing" : "idle",
    lastError: null,
    pendingRuntimeRequest: null,
    latestVisibleMessage: message ? { id: message.messageId, role: "user", text: message.text, updatedAt: now } : null,
    latestUserMessageAt: message ? now : null,
    latestUserAuthoredMessageAt: message ? now : null,
    hasActionableProposedPlan: false,
    itemCount: message ? 1 : 0,
    visibleItemCount: message ? 1 : 0,
    lastVisitedAt: now,
  };

  const runs = runId
    ? [
        {
          id: runId,
          threadId: input.threadId,
          ordinal: 1,
          providerInstanceId: input.providerInstanceId,
          modelSelection: input.modelSelection,
          providerThreadId: null,
          userMessageId: message!.messageId,
          rootNodeId: null,
          activeAttemptId: null,
          status: "preparing",
          requestedAt: now,
          startedAt: null,
          completedAt: null,
          checkpointId: null,
          contextHandoffId: null,
        },
      ]
    : [];
  const messages = message
    ? [
        {
          ...creation,
          id: message.messageId,
          threadId: input.threadId,
          runId,
          nodeId: null,
          role: "user",
          text: message.text,
          ...(message.context !== undefined ? { context: message.context } : {}),
          attachments: message.attachments,
          streaming: false,
          createdAt: now,
          updatedAt: now,
        },
      ]
    : [];

  const turnItems = message
    ? [
        {
          ...turnItemBase(input.threadId, `hub_msg_${message.messageId}`, 1, now),
          ...creation,
          runId,
          type: "user_message",
          status: "completed",
          completedAt: now,
          messageId: message.messageId,
          inputIntent: "turn_start",
          text: message.text,
          ...(message.context !== undefined ? { context: message.context } : {}),
          attachments: message.attachments,
        },
      ]
    : [];

  const projection: Record<string, unknown> = {
    thread: { ...common, lastVisitedAt: now },
    runs,
    attempts: [],
    nodes: [],
    subagents: [],
    providerSessions: [],
    providerThreads: [],
    providerTurns: [],
    runtimeRequests: [],
    messages,
    plans: [],
    turnItems,
    checkpointScopes: [],
    checkpoints: [],
    contextHandoffs: [],
    contextTransfers: [],
    visibleTurnItems: turnItems.map((item) => visibleRow(input.threadId, item)),
    updatedAt: now,
  };
  return {
    shell,
    projection: withStatusNotice(projection, input.threadId, {
      outcome: "updated",
      summary: "Starting a VM for this chat. Your message is sent as soon as it is ready.",
    }),
  };
}

function turnItemBase(threadId: string, id: string, ordinal: number, now: string): Record<string, unknown> {
  return {
    id,
    threadId,
    runId: null,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal,
    title: null,
    startedAt: now,
    completedAt: null,
    updatedAt: now,
  };
}

function visibleRow(threadId: string, item: Record<string, unknown>): Record<string, unknown> {
  return { position: item.ordinal, visibility: "synthetic", sourceThreadId: threadId, sourceItemId: item.id, item };
}

/** What the Hub tells the user about the chat's VM, shown as a timeline row. */
export interface StatusNotice {
  outcome: "updated" | "failed";
  summary: string;
  detail?: string;
}

const STATUS_NOTICE_PREFIX = "hub_vm_status_";

/**
 * The projection with the Hub's status row at the end of the timeline (replacing an earlier one).
 * `null` removes it.
 */
export function withStatusNotice(
  projection: Record<string, unknown>,
  threadId: string,
  notice: StatusNotice | null,
  now = new Date().toISOString(),
): Record<string, unknown> {
  const isNotice = (item: { id?: unknown } | undefined) => typeof item?.id === "string" && item.id.startsWith(STATUS_NOTICE_PREFIX);
  const turnItems = ((projection.turnItems as Array<Record<string, unknown>> | undefined) ?? []).filter((i) => !isNotice(i));
  const visible = ((projection.visibleTurnItems as Array<{ item?: { id?: unknown }; position?: unknown }> | undefined) ?? []).filter(
    (row) => !isNotice(row.item),
  );
  if (!notice) return { ...projection, turnItems, visibleTurnItems: visible };
  const last = Math.max(
    0,
    ...turnItems.map((i) => (typeof i.ordinal === "number" ? i.ordinal : 0)),
    ...visible.map((r) => (typeof r.position === "number" ? r.position : 0)),
  );
  const item: Record<string, unknown> = {
    ...turnItemBase(threadId, `${STATUS_NOTICE_PREFIX}${threadId}`, last + 1, now),
    type: "notification",
    status: notice.outcome === "failed" ? "failed" : "completed",
    completedAt: now,
    source: { kind: "system" },
    outcome: notice.outcome,
    summary: notice.summary,
    ...(notice.detail ? { detail: notice.detail } : {}),
  };
  return { ...projection, turnItems: [...turnItems, item], visibleTurnItems: [...visible, visibleRow(threadId, item)], updatedAt: now };
}

/**
 * A projection with no history, from a sidebar row: for a chat whose VM is asleep when the Hub has
 * no cached copy of it. Null when the row lacks what the client requires.
 */
export function projectionFromShell(shell: Record<string, unknown>): Record<string, unknown> | null {
  const title = nonEmpty(shell.title) ?? "Chat";
  if (typeof shell.id !== "string" || !shell.lineage || !shell.modelSelection || typeof shell.createdAt !== "string") return null;
  const thread: Record<string, unknown> = {};
  for (const key of APP_THREAD_KEYS) if (key in shell) thread[key] = shell[key];
  return {
    thread: { ...thread, title },
    runs: [],
    attempts: [],
    nodes: [],
    subagents: [],
    providerSessions: [],
    providerThreads: [],
    providerTurns: [],
    runtimeRequests: [],
    messages: [],
    plans: [],
    turnItems: [],
    checkpointScopes: [],
    checkpoints: [],
    contextHandoffs: [],
    contextTransfers: [],
    visibleTurnItems: [],
    updatedAt: shell.updatedAt ?? shell.createdAt,
  };
}

/** Keys a shell row shares with the projection's thread record. */
const APP_THREAD_KEYS = [
  "createdBy",
  "creationSource",
  "id",
  "projectId",
  "title",
  "providerInstanceId",
  "modelSelection",
  "runtimeMode",
  "interactionMode",
  "branch",
  "worktreePath",
  "linkedPullRequest",
  "pullRequests",
  "branchPullRequest",
  "issues",
  "activeProviderThreadId",
  "historyOrigin",
  "linkOrigin",
  "delegatedFrom",
  "workflowRun",
  "lineage",
  "forkedFrom",
  "createdAt",
  "updatedAt",
  "archivedAt",
  "settledOverride",
  "settledAt",
  "unsettledAt",
  "snoozedUntil",
  "snoozedAt",
  "limitRecovery",
  "pinnedAt",
  "autoSettleDisabledAt",
  "pinOrderKey",
  "activeOrderKey",
  "tags",
  "lastVisitedAt",
  "opens",
  "titleRegeneration",
  "lastTitleRegeneration",
  "deletedAt",
] as const;

/** The same records after the VM failed to start: the run ends `failed` and the shell carries the error. */
export function failPlaceholderThread(
  placeholder: PlaceholderThread,
  error: string,
  now = new Date().toISOString(),
): PlaceholderThread {
  const shell: Record<string, unknown> = {
    ...placeholder.shell,
    status: "failed",
    activeRunId: null,
    activityRunStatus: null,
    activityRunStartedAt: null,
    latestRunCompletedAt: placeholder.shell.latestRunId ? now : null,
    lastError: error,
    updatedAt: now,
  };
  const projection = placeholder.projection;
  const runs = (projection.runs as Array<Record<string, unknown>>).map((run) => ({
    ...run,
    status: "failed",
    completedAt: now,
  }));
  const threadId = String(shell.id);
  return {
    shell,
    projection: withStatusNotice(
      {
        ...projection,
        thread: { ...(projection.thread as Record<string, unknown>), updatedAt: now },
        runs,
      },
      threadId,
      { outcome: "failed", summary: "This chat's VM could not start.", detail: error },
      now,
    ),
  };
}

/**
 * A message the Hub holds for a chat whose VM is waking: the client's `message.dispatch` payload,
 * as sent (Hub project ids), with the queued run the Hub shows for it.
 */
export interface HeldTurn {
  runId: string;
  payload: Record<string, unknown>;
  requestedAt: string;
  /**
   * `sent` once the VM took it (kept until the chat's view moves to the VM); `failed` once the wake
   * failed or the VM did not take it: shown as a failed run, never delivered.
   */
  status: "queued" | "sent" | "failed";
}

/** True when a `message.dispatch` payload has what a queued message needs (id, text, attachments). */
export function isQueueableMessage(payload: Record<string, unknown>): boolean {
  return (
    payload.type === "message.dispatch" &&
    typeof payload.messageId === "string" &&
    payload.messageId !== "" &&
    typeof payload.text === "string" &&
    Array.isArray(payload.attachments) &&
    payload.notification === undefined &&
    payload.guestMessageId === undefined
  );
}

/**
 * The projection with the held messages as T3 shows a queued follow-up: each a `queued` run (next
 * ordinal, `queuePosition` in order), its user message (the client's own id, so its optimistic copy
 * reconciles) and a `user_message` turn item with `inputIntent: "queued_turn"`. Like T3, a queued
 * message's item is not in `visibleTurnItems` (the app lists it above the composer, with "Send now");
 * a failed one is (a failed run with its message in the timeline, so the user can resend it). A sent
 * one the projection does not have yet shows in the timeline as a completed run (the VM took it).
 */
export function withHeldTurns(
  projection: Record<string, unknown>,
  threadId: string,
  turns: HeldTurn[],
  now = new Date().toISOString(),
): Record<string, unknown> {
  const messages = (projection.messages as Array<Record<string, unknown>> | undefined) ?? [];
  const known = new Set(messages.map((m) => m.id));
  const shown = turns.filter((t) => !known.has(t.payload.messageId));
  if (shown.length === 0) return projection;

  const thread = (projection.thread ?? {}) as Record<string, unknown>;
  const runs = (projection.runs as Array<Record<string, unknown>> | undefined) ?? [];
  const turnItems = (projection.turnItems as Array<Record<string, unknown>> | undefined) ?? [];
  const visible = (projection.visibleTurnItems as Array<Record<string, unknown>> | undefined) ?? [];
  const ordinalOf = (v: unknown) => (typeof v === "number" ? v : 0);
  let runOrdinal = Math.max(0, ...runs.map((r) => ordinalOf(r.ordinal)));
  let itemOrdinal = Math.max(0, ...turnItems.map((i) => ordinalOf(i.ordinal)), ...visible.map((r) => ordinalOf(r.position)));
  let queuePosition = 0;

  const newRuns: Record<string, unknown>[] = [];
  const newMessages: Record<string, unknown>[] = [];
  const newItems: Record<string, unknown>[] = [];
  const newVisible: Record<string, unknown>[] = [];
  for (const turn of shown) {
    const p = turn.payload;
    const queued = turn.status === "queued";
    const runStatus = queued ? "queued" : turn.status === "sent" ? "completed" : "failed";
    const messageId = String(p.messageId);
    const modelSelection =
      p.modelSelection && typeof p.modelSelection === "object" ? (p.modelSelection as Record<string, unknown>) : thread.modelSelection;
    const providerInstanceId =
      nonEmpty((modelSelection as Record<string, unknown> | undefined)?.instanceId) ?? nonEmpty(thread.providerInstanceId) ?? "unknown";
    const creation = {
      createdBy: typeof p.createdBy === "string" ? p.createdBy : "user",
      creationSource: CREATION_SOURCES.has(String(p.creationSource)) ? String(p.creationSource) : "web",
    };
    const context = p.context !== undefined ? { context: p.context } : {};
    const attachments = Array.isArray(p.attachments) ? p.attachments : [];
    newRuns.push({
      id: turn.runId,
      threadId,
      ordinal: ++runOrdinal,
      providerInstanceId,
      modelSelection,
      providerThreadId: null,
      userMessageId: messageId,
      rootNodeId: null,
      activeAttemptId: null,
      status: runStatus,
      ...(queued ? { queuePosition: ++queuePosition } : {}),
      requestedAt: turn.requestedAt,
      startedAt: null,
      completedAt: queued ? null : now,
      checkpointId: null,
      contextHandoffId: null,
      ...(p.freshContext === true ? { freshContext: true } : {}),
    });
    newMessages.push({
      ...creation,
      id: messageId,
      threadId,
      runId: turn.runId,
      nodeId: null,
      role: "user",
      text: p.text,
      ...context,
      attachments,
      streaming: false,
      createdAt: turn.requestedAt,
      updatedAt: turn.requestedAt,
    });
    const item: Record<string, unknown> = {
      ...turnItemBase(threadId, `hub_msg_${messageId}`, ++itemOrdinal, turn.requestedAt),
      ...creation,
      runId: turn.runId,
      type: "user_message",
      status: queued ? "pending" : runStatus,
      startedAt: null,
      completedAt: queued ? null : now,
      messageId,
      inputIntent: "queued_turn",
      text: p.text,
      ...context,
      attachments,
    };
    newItems.push(item);
    if (!queued) newVisible.push(visibleRow(threadId, item));
  }
  return {
    ...projection,
    runs: [...runs, ...newRuns],
    messages: [...messages, ...newMessages],
    turnItems: [...turnItems, ...newItems],
    visibleTurnItems: [...visible, ...newVisible],
    updatedAt: now,
  };
}

/** Keys a restored row takes from a cached projection's `thread` (same meaning in both records). */
const ROW_KEYS_FROM_PROJECTION = [
  "createdBy",
  "creationSource",
  "title",
  "providerInstanceId",
  "modelSelection",
  "runtimeMode",
  "interactionMode",
  "branch",
  "worktreePath",
  "lineage",
  "forkedFrom",
  "createdAt",
  "updatedAt",
] as const;

/**
 * A sidebar row for a chat the Hub has no stored row for (its VM went to sleep before the Hub kept
 * rows), so a Hub restart does not drop it. Built from the chat's cached projection when there is
 * one, else from what the Hub knows (project defaults, the sandbox's times). Its VM's own row
 * replaces it once the VM runs again.
 */
export function restoredShellRow(input: {
  threadId: string;
  hubProjectId: string;
  providerInstanceId: string;
  model: string;
  /** When the chat was last active (ISO). */
  at: string;
  projectionThread?: Record<string, unknown> | null;
}): Record<string, unknown> {
  const { shell } = buildPlaceholderThread({
    threadId: input.threadId,
    hubProjectId: input.hubProjectId,
    title: "Sleeping chat",
    modelSelection: { instanceId: input.providerInstanceId, model: input.model },
    providerInstanceId: input.providerInstanceId,
    runtimeMode: "full-access",
    interactionMode: "default",
    creationSource: "web",
    branch: null,
    worktreePath: null,
    message: null,
    now: input.at,
  });
  const thread = input.projectionThread;
  if (thread && typeof thread === "object") {
    for (const key of ROW_KEYS_FROM_PROJECTION) {
      const value = thread[key];
      if (value === undefined) continue;
      if (value === null && shell[key] !== null) continue;
      if (value !== null && shell[key] !== null && typeof value !== typeof shell[key]) continue;
      shell[key] = value;
    }
  }
  shell.id = input.threadId;
  shell.projectId = input.hubProjectId;
  delete shell.lastVisitedAt;
  return shell;
}
