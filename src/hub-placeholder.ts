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
    turnItems: [],
    checkpointScopes: [],
    checkpoints: [],
    contextHandoffs: [],
    contextTransfers: [],
    visibleTurnItems: [],
    updatedAt: now,
  };
  return { shell, projection };
}

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
  return {
    shell,
    projection: {
      ...projection,
      thread: { ...(projection.thread as Record<string, unknown>), updatedAt: now },
      runs,
      updatedAt: now,
    },
  };
}
