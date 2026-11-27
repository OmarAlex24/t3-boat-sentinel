import { createHash, randomUUID } from "node:crypto";
import {
  accessRank,
  authenticateClient,
  extractBearer,
  isAuthMcpClientAccess,
  isInteractionMode,
  isRuntimeMode,
  PEER_LINK_RENEWAL_STEP_MS,
  PEER_LINK_TOKEN_TTL_SECS,
  type AuthMcpClientAccess,
  type ClientCredentialRecord,
  type ProviderInteractionMode,
  type RuntimeMode,
} from "./auth.ts";
import { DEFAULT_MAX_PARKED_SANDBOXES, DEFAULT_PARKED_TTL_HOURS, type ProjectConfig, type SentinelConfig } from "./config.ts";
import { requestOrigin } from "./hub-mcp-oauth.ts";
import { isChildThreadRow, vmCloseBlocker } from "./hub-park-guard.ts";
import { repositoryIdentityFor } from "./repository-identity.ts";
import { clientIpFromRequest, rateLimitedResponse, type AuthRateLimiter } from "./security.ts";
import {
  buildAutoProjectConfig,
  countsTowardHubLimit,
  type HubSandboxRecord,
  type HubThreadLinkOriginRecord,
  type StateStore,
} from "./state.ts";

export const MCP_PROTOCOL_VERSION = "2025-06-18";

const MODE_LIMIT_HEADER = "t3-mode-limit";
const LINK_EXPIRES_AT_HEADER = "t3-link-expires-at";
const SENDER_THREAD_HEADER = "t3-sender-thread";

const DEFAULT_WAIT_TIMEOUT_MS = 30_000;
const MAX_WAIT_TIMEOUT_MS = 45_000;
export const DEFAULT_DISPATCH_TIMEOUT_MS = 45_000;
const DISPATCH_CHECK_TIMEOUT_MS = 10_000;
const DEFAULT_PROVIDER_INSTANCE_ID = "claudeAgent";
const DEFAULT_MODEL_SLUG = "claude-sonnet-4-6";

export type HubSandboxPhase = "booting" | "cloning" | "starting_t3" | "running" | "sleeping" | "error" | "destroying";

export interface HubDelegatedFrom {
  environmentId: string;
  threadId: string;
  title: string;
}

export interface HubPendingLaunch {
  threadId: string;
  projectKey: string;
  projectId: string;
  title: string;
  message?: string;
  workspaceStrategy?: Record<string, unknown>;
  runtimeMode: RuntimeMode;
  interactionMode: ProviderInteractionMode;
  modelSelection: { instanceId: string; model: string; options?: unknown[] };
  clientRequestId?: string;
  createdAt: string;
  updatedAt: string;
  sandboxId: string | null;
  phase: HubSandboxPhase;
  step: string;
  error: string | null;
  queuedMessages: Array<{
    message: string;
    mode: string;
    clientRequestId?: string;
    freshContext?: boolean;
    outputSchema?: Record<string, unknown>;
    linkedSender?: HubDelegatedFrom;
  }>;
  cancelled?: boolean;
  linkOrigin?: HubThreadLinkOriginRecord;
  delegatedFrom?: HubDelegatedFrom;
  launchedRunId?: string | null;
}

export interface RepositoryIdentity {
  canonicalKey: string;
  locator: {
    source: "git-remote";
    remoteName: string;
    remoteUrl: string;
  };
  webUrl?: string;
  rootPath?: string;
  displayName?: string;
  provider?: "github" | "gitlab" | "bitbucket" | "azure-devops" | "codeberg" | "gitea" | "other";
  owner?: string;
  name?: string;
}

/** Who asked for a park (logged with it); an MCP settle also names the chat it settled. */
export interface HubParkRequest {
  trigger: string;
  /** The chat an MCP `t3_thread_organize` settle settled: it must be one of the VM's top-level chats. */
  settledThreadId?: string;
}

/** The MCP client that made a request (its paired credential + mode limits), for enforcement and logs. */
export interface HubMcpCaller {
  clientId: string;
  label: string;
  peerLink: boolean;
  access: AuthMcpClientAccess;
  readOnly: boolean;
  runtimeMode: RuntimeMode;
  interactionMode: ProviderInteractionMode;
  senderThread?: HubDelegatedFrom;
}

export interface HubMcpBridge {
  callVmUnary: (
    sandboxId: string,
    tag: string,
    payload: unknown,
    timeoutMs?: number,
  ) => Promise<{ _tag: "Success"; value: unknown } | { _tag: "Failure"; cause: unknown }>;
  getThreadShell: (threadId: string) => Record<string, unknown> | undefined;
  listThreadShells: () => Array<{ threadId: string; shell: Record<string, unknown>; sandboxId: string | null }>;
  getSandboxForThread: (threadId: string) => HubSandboxRecord | undefined;
  /** Cached full thread projection (populated before a VM sleeps/parks), if any. */
  getCachedProjection?: (threadId: string) => Record<string, unknown> | undefined;
  /** Cached Hub ServerConfig (contains harvested providers/models, works at 0 VMs). */
  getServerConfig?: () => Record<string, unknown>;
  /** Build a full T3 Project record for a registered Hub project. */
  buildProjectShell?: (projectKey: string, proj: ProjectConfig) => Record<string, unknown>;
  /** Next monotonic Hub orchestration sequence for `OrchestrationV2DispatchCommandResult`. */
  nextSequence?: () => number;
  wakeSandbox: (sandboxId: string) => Promise<HubSandboxRecord>;
  destroySandbox: (sandboxId: string, reason: string) => Promise<void>;
  sleepSandbox: (sandboxId: string, reason?: string) => Promise<void>;
  /** In-flight in-place resize metadata, while `t3_thread_wait` must keep waiting instead of reporting `interrupted`. */
  getUpsizeProgress?: (sandboxId: string) => { fromSize: string; toSize: string; trigger: string } | undefined;
  /**
   * Parks a VM whose chats are all settled (see `boat.settleAction`); true when it ends parked.
   * Refused while a chat on it is live or an open top-level chat is not settled.
   */
  parkSandbox: (sandboxId: string, request: HubParkRequest) => Promise<boolean>;
  /** The thread rows a running VM lists now (delegated children included), or null when it cannot be read. */
  readVmThreads: (sandboxId: string) => Promise<Array<Record<string, unknown>> | null>;
  projectKeyFromHubId: (hubProjectId: string) => string | null;
  ensureHubProjectId: (projectKey: string) => string;
  spawnSandboxForThread: (
    projectKey: string,
    threadId: string,
    onProgress?: (update: { sandboxId?: string | null; phase?: HubSandboxPhase; step: string }) => void,
  ) => Promise<HubSandboxRecord>;
  bindThreadToSandbox: (threadId: string, sandboxId: string) => void;
  bindWorktreeToSandbox: (worktreePath: string, sandboxId: string) => void;
  notifyProjectAdded: (projectKey: string) => void;
  startProjectBake: (projectKey: string) => void;
  /** Timeout for `orchestration.dispatchCommand` when sending a message (defaults to `DEFAULT_DISPATCH_TIMEOUT_MS`). */
  dispatchTimeoutMs?: () => number;
  providerName: () => string;
  anyRunningSandbox: () => HubSandboxRecord | undefined;
}

export class HubMcpToolError extends Error {
  constructor(
    public readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = "HubMcpToolError";
  }
}

interface JsonRpcRequest {
  jsonrpc?: string;
  id?: string | number | null;
  method?: string;
  params?: Record<string, unknown>;
}

interface McpSessionRecord {
  id: string;
  clientId: string;
  createdAt: number;
  lastUsedAt: number;
}

const SESSION_TTL_MS = 24 * 60 * 60_000;
const MAX_SESSIONS = 500;
const FAILED_LAUNCH_TTL_MS = 30 * 60_000;
const DESTROYED_THREAD_TTL_MS = 24 * 60 * 60_000;

const VALID_THREAD_STATUSES = new Set([
  "idle",
  "preparing",
  "queued",
  "starting",
  "running",
  "waiting",
  "completed",
  "interrupted",
  "failed",
  "cancelled",
  "rolled_back",
]);

const TERMINAL_RUN_STATUSES = new Set([
  "completed",
  "failed",
  "cancelled",
  "interrupted",
  "rolled_back",
]);

const ACTIVE_RUN_STATUSES = new Set(["preparing", "queued", "starting", "running", "waiting"]);

const VALID_TURN_ITEM_STATUSES = new Set([
  "idle",
  "pending",
  "running",
  "waiting",
  "completed",
  "failed",
  "cancelled",
  "interrupted",
]);

const VALID_ACTORS = new Set(["user", "agent", "system"]);
const VALID_CREATION_SOURCES = new Set(["web", "mobile", "mcp", "provider", "server"]);
const VALID_PR_SOURCES = new Set(["manual", "created", "agent", "stack", "stack-dismissed"]);
const PROVIDER_SLUG_PATTERN = /^[a-zA-Z][a-zA-Z0-9_-]*$/;
const FULL_CATALOG_DRIVERS = new Set(["codex", "claudeAgent", "muse", "grok"]);
const LISTED_MODEL_LIMIT = 8;

const MCP_TOOLS = [
  {
    name: "orchestrator_capabilities",
    description:
      "List providers, models, options, and runtime modes for delegation or thread launch (works with 0 VMs running).",
    inputSchema: {
      type: "object",
      properties: {
        search: { type: "string" },
      },
    },
  },
  {
    name: "t3_project_list",
    description: "List projects registered in the Sentinel Hub environment, including GitHub repositoryIdentity.",
    inputSchema: {
      type: "object",
      properties: {
        limit: { type: "number" },
        cursor: { type: "number" },
      },
    },
  },
  {
    name: "t3_project_read",
    description: "Read one registered project in Sentinel Hub.",
    inputSchema: {
      type: "object",
      properties: {
        projectId: { type: "string" },
      },
      required: ["projectId"],
    },
  },
  {
    name: "t3_thread_list",
    description: "List active and booting threads across the Sentinel Hub (works with 0 VMs running).",
    inputSchema: {
      type: "object",
      properties: {
        allProjects: { type: "boolean" },
        projectId: { type: "string" },
        limit: { type: "number" },
        cursor: { type: "number" },
        statuses: { type: "array", items: { type: "string" } },
        titleContains: { type: "string" },
        settled: { type: "boolean" },
        snoozed: { type: "boolean" },
        includeSubagents: { type: "boolean" },
      },
    },
  },
  {
    name: "t3_thread_read",
    description: "Read a thread's state, sandbox boot progress, messages or activity timeline, and recent runs.",
    inputSchema: {
      type: "object",
      properties: {
        threadId: { type: "string" },
        itemId: { type: "string" },
        textOffset: { type: "number" },
        view: { type: "string", enum: ["messages", "activity"] },
        limit: { type: "number" },
        maxCharsPerItem: { type: "number" },
        runLimit: { type: "number" },
        afterPosition: { type: "number" },
        beforePosition: { type: "number" },
      },
      required: ["threadId"],
    },
  },
  {
    name: "t3_thread_launch",
    description:
      "Launch a new isolated thread on its own Boat VM asynchronously (returns < 2s with status 'preparing' while VM boots).",
    inputSchema: {
      type: "object",
      properties: {
        projectId: { type: "string" },
        title: { type: "string" },
        generateTitle: { type: "boolean" },
        message: { type: "string" },
        workspaceStrategy: { type: "object" },
        runtimeMode: { type: "string", enum: ["approval-required", "auto-accept-edits", "auto", "full-access"] },
        interactionMode: { type: "string", enum: ["default", "plan"] },
        modelSelection: { type: "object" },
        outputSchema: { type: "object" },
        clientRequestId: { type: "string" },
        delegatedFrom: { type: "object" },
      },
      required: ["projectId"],
    },
  },
  {
    name: "t3_thread_send",
    description: "Send a message to an active or booting thread (queues automatically if its VM is still starting).",
    inputSchema: {
      type: "object",
      properties: {
        threadId: { type: "string" },
        message: { type: "string" },
        mode: { type: "string", enum: ["auto", "queue", "steer", "restart"] },
        freshContext: { type: "boolean" },
        outputSchema: { type: "object" },
        clientRequestId: { type: "string" },
      },
      required: ["threadId", "message"],
    },
  },
  {
    name: "t3_thread_wait",
    description:
      "Wait for a thread's run (or booting VM launch) to finish, bounded to 45s. Supports untilDelegatedWorkSettled and untilOutput.",
    inputSchema: {
      type: "object",
      properties: {
        threadId: { type: "string" },
        runId: { type: "string" },
        timeoutMs: { type: "number" },
        untilDelegatedWorkSettled: { type: "boolean" },
        untilOutput: { type: "boolean" },
      },
      required: ["threadId"],
    },
  },
  {
    name: "t3_thread_interrupt",
    description:
      "Interrupt a thread's active run or stop the thread completely (`stop: true`, including cancelling a booting VM launch).",
    inputSchema: {
      type: "object",
      properties: {
        threadId: { type: "string" },
        runId: { type: "string" },
        reason: { type: "string" },
        stop: { type: "boolean" },
        clientRequestId: { type: "string" },
      },
      required: ["threadId"],
    },
  },
  {
    name: "t3_thread_organize",
    description:
      "Organize a thread (settle, archive, pin, unpin, snooze, unsnooze, unsettle, unarchive, mark_unread). Archiving destroys the chat's VM for $0/hr idle; settling stops (parks) it and keeps the chat, unless boat.settleAction is delete.",
    inputSchema: {
      type: "object",
      properties: {
        threadId: { type: "string" },
        action: {
          type: "string",
          enum: ["settle", "archive", "pin", "unpin", "snooze", "unsnooze", "unsettle", "unarchive", "mark_unread"],
        },
        snoozedUntil: { type: "string" },
      },
      required: ["threadId", "action"],
    },
  },
  {
    name: "list_thread_pull_requests",
    description: "List pull requests linked to a thread.",
    inputSchema: {
      type: "object",
      properties: {
        threadId: { type: "string" },
      },
      required: ["threadId"],
    },
  },
  {
    name: "t3_pending_request",
    description: "List, read, or respond to pending user-input requests on a thread.",
    inputSchema: {
      type: "object",
      properties: {
        action: { type: "string", enum: ["list", "read", "respond"] },
        threadId: { type: "string" },
        requestId: { type: "string" },
        answers: { type: "object" },
      },
      required: ["action", "threadId"],
    },
  },
  {
    name: "t3_worktree_list",
    description: "List git branches/worktrees for a thread or project workspace (degrades cleanly when 0 VMs are running).",
    inputSchema: {
      type: "object",
      properties: {
        threadId: { type: "string" },
        query: { type: "string" },
        refKind: { type: "string", enum: ["all", "local", "remote"] },
        limit: { type: "number" },
        cursor: { type: "number" },
      },
    },
  },
  {
    name: "t3_thread_search",
    description: "Search active and booting threads by title, branch, or content.",
    inputSchema: {
      type: "object",
      properties: {
        query: { type: "string" },
        projectId: { type: "string" },
        limit: { type: "number" },
      },
      required: ["query"],
    },
  },
  {
    name: "ci_logs",
    description: "Read CI check logs for a pull request on a live thread sandbox.",
    inputSchema: {
      type: "object",
      properties: {
        threadId: { type: "string" },
        prNumber: { type: "number" },
        runId: { type: "string" },
      },
    },
  },
  {
    name: "sentinel_add_project",
    description:
      "Register a new GitHub repo in Sentinel Hub and kick off background snapshot baking (immediately launchable via base-t3 clone fallback).",
    inputSchema: {
      type: "object",
      properties: {
        repo: { type: "string" },
        name: { type: "string" },
        workdir: { type: "string" },
        size: { type: "string" },
        klavProfiles: { type: "array", items: { type: "string" } },
        klavTtl: { type: "string" },
        bake: { type: "boolean" },
      },
      required: ["repo"],
    },
  },
  {
    name: "sentinel_status",
    description: "Return live Sentinel Hub capacity, active sandboxes, and registered projects.",
    inputSchema: {
      type: "object",
      properties: {},
    },
  },
];

/**
 * Parse a GitHub repository reference (`owner/repo`, `https://github.com/owner/repo.git`,
 * `git@github.com:owner/repo.git`) into T3 Code's `RepositoryIdentity` object (including `locator`).
 */
export function parseGitHubRepositoryIdentity(repo?: string | null, rootPath?: string): RepositoryIdentity | null {
  if (!repo) return null;
  const raw = repo.trim();
  if (!raw) return null;
  const derived = repositoryIdentityFor(raw, rootPath ?? "");
  if (derived) return derived as unknown as RepositoryIdentity;

  const shortMatch = /^([A-Za-z0-9_.-]+)\/([A-Za-z0-9_.-]+?)(?:\.git)?$/.exec(raw);
  if (!shortMatch) return null;
  const owner = shortMatch[1];
  const name = shortMatch[2];
  const remoteUrl = `https://github.com/${owner}/${name}.git`;
  return {
    canonicalKey: `github.com/${owner.toLowerCase()}/${name.toLowerCase()}`,
    locator: { source: "git-remote", remoteName: "origin", remoteUrl },
    webUrl: `https://github.com/${owner}/${name}`,
    ...(rootPath ? { rootPath } : {}),
    displayName: `${owner}/${name}`,
    provider: "github",
    owner,
    name,
  };
}

/**
 * Streamable HTTP MCP server (`POST /mcp`, protocolVersion `2025-06-18`) embedded in `HubServer`.
 * Supports both native T3 Code Linked Environments (via MCP OAuth, `T3-Mode-Limit`, `linkFence`,
 * `t3_thread_wait`, `t3_thread_interrupt`, `orchestrator_capabilities`) and direct machine clients
 * (e.g. Cloudflare Workers / Zumith) with `$0/hr` idle operation when 0 Boat VMs are running.
 */
export class HubMcpHandler {
  private readonly sessions = new Map<string, McpSessionRecord>();
  private readonly pendingLaunches = new Map<string, HubPendingLaunch>();
  private readonly launchByRequestId = new Map<string, string>(); // scopedKey -> threadId
  private readonly sendByRequestId = new Map<
    string,
    { threadId: string; messageId: string; runId: string; status: string; delivery: string }
  >();
  private readonly interruptByRequestId = new Map<string, { threadId: string; runId: string | null; status: string }>();
  private readonly destroyedThreads = new Map<string, { destroyedAt: number; reason: string }>();
  private readonly threadWaiters = new Map<string, Set<() => void>>();

  constructor(
    private readonly config: SentinelConfig,
    private readonly state: StateStore,
    private readonly bridge: HubMcpBridge,
    private readonly rateLimiter?: AuthRateLimiter,
  ) {}

  /** Wake any in-flight `t3_thread_wait` calls watching `threadId`. */
  notifyThreadChanged(threadId: string): void {
    const waiters = this.threadWaiters.get(threadId);
    if (!waiters || waiters.size === 0) return;
    for (const wake of [...waiters]) wake();
  }

  /** Called by HubServer when a sandbox is destroyed so `/mcp` reports it cleanly and wakes waiters. */
  recordThreadsDestroyed(threadIds: string[], reason: string): void {
    const now = Date.now();
    for (const tid of threadIds) {
      this.destroyedThreads.set(tid, { destroyedAt: now, reason });
      const pending = this.pendingLaunches.get(tid);
      if (pending) {
        pending.cancelled = true;
        pending.phase = "destroying";
        this.pendingLaunches.delete(tid);
      }
      this.notifyThreadChanged(tid);
    }
    this.pruneExpiredState();
  }

  /** Number of currently booting launches not yet promoted into `state.hub.sandboxes`. */
  countBootingLaunches(): number {
    let count = 0;
    for (const launch of this.pendingLaunches.values()) {
      if (launch.phase === "booting" || launch.phase === "cloning" || launch.phase === "starting_t3") {
        if (!launch.sandboxId || !this.state.getHubSandbox(launch.sandboxId)) {
          count++;
        }
      }
    }
    return count;
  }

  async handleHttp(req: Request): Promise<Response> {
    if (req.method === "OPTIONS") {
      return withMcpCors(new Response(null, { status: 204 }));
    }

    const hub = this.state.ensureHub(
      this.config.hub?.label ?? "Boat",
      Object.keys(this.state.getAllProjects(this.config)),
    );
    const ip = clientIpFromRequest(req);

    if (this.rateLimiter) {
      const limit = this.rateLimiter.check("hub-mcp", ip, 15, 5 * 60_000);
      if (!limit.allowed) return withMcpCors(rateLimitedResponse(limit.retryAfterSecs));
    }

    const presentedToken = extractBearer(req.headers) !== null;
    const client = authenticateClient(hub.clients, req.headers, { allowMcpOnly: true });
    if (!client) {
      this.rateLimiter?.recordFailure("hub-mcp", ip, 5 * 60_000);
      const origin = requestOrigin(req, this.config.hub?.host);
      const resourceMetadata = `${origin}/.well-known/oauth-protected-resource/mcp`;
      const challengeParts = [`resource_metadata="${resourceMetadata}"`];
      if (presentedToken) challengeParts.push('error="invalid_token"');
      return withMcpCors(
        Response.json(
          { error: "invalid_mcp_credential", message: "A valid Bearer token is required for /mcp." },
          {
            status: 401,
            headers: {
              "cache-control": "no-store",
              "www-authenticate": `Bearer ${challengeParts.join(", ")}`,
            },
          },
        ),
      );
    }
    this.rateLimiter?.recordSuccess("hub-mcp", ip);

    const parsedCaller = this.buildCaller(client, req.headers);
    if (!parsedCaller.ok) {
      return withMcpCors(
        Response.json(
          {
            error: "invalid_mode_limit",
            message: "T3-Mode-Limit must be <runtimeMode>/<interactionMode>, e.g. auto/default.",
          },
          { status: 400, headers: { "cache-control": "no-store" } },
        ),
      );
    }
    const caller = parsedCaller.caller;
    const linkExpiresAt = this.maybeRenewPeerLink(client);

    if (req.method === "DELETE") {
      const sid = req.headers.get("mcp-session-id");
      if (sid) this.sessions.delete(sid);
      const headers: Record<string, string> = {};
      if (linkExpiresAt) headers[LINK_EXPIRES_AT_HEADER] = linkExpiresAt;
      return withMcpCors(new Response(null, { status: 204, headers }));
    }

    if (req.method !== "POST") {
      return withMcpCors(
        Response.json({ error: "method_not_allowed", message: "Use POST /mcp for Streamable HTTP MCP." }, { status: 405 }),
      );
    }

    let rawBody: unknown;
    try {
      rawBody = await req.json();
    } catch {
      return withMcpCors(
        Response.json(
          { jsonrpc: "2.0", id: null, error: { code: -32700, message: "Parse error: invalid JSON body" } },
          { status: 400 },
        ),
      );
    }

    const incomingSessionId = req.headers.get("mcp-session-id");
    if (incomingSessionId) {
      const existing = this.sessions.get(incomingSessionId);
      if (existing) existing.lastUsedAt = Date.now();
    }

    const isBatch = Array.isArray(rawBody);
    const messages = (isBatch ? rawBody : [rawBody]) as JsonRpcRequest[];
    if (messages.length === 0) {
      return withMcpCors(
        Response.json(
          { jsonrpc: "2.0", id: null, error: { code: -32600, message: "Invalid Request: empty batch" } },
          { status: 400 },
        ),
      );
    }

    let issuedSessionId: string | null = incomingSessionId;
    const responses: Array<Record<string, unknown>> = [];

    for (const msg of messages) {
      if (!msg || typeof msg !== "object" || typeof msg.method !== "string") {
        responses.push({
          jsonrpc: "2.0",
          id: msg && typeof msg === "object" && "id" in msg ? (msg.id ?? null) : null,
          error: { code: -32600, message: "Invalid JSON-RPC request" },
        });
        continue;
      }

      const hasId = msg.id !== undefined && msg.id !== null;
      if (msg.method === "notifications/initialized" || msg.method.startsWith("notifications/") || !hasId) {
        continue;
      }

      if (msg.method === "initialize") {
        const sid = this.createSession(client.id);
        issuedSessionId = sid;
        responses.push({
          jsonrpc: "2.0",
          id: msg.id,
          result: {
            protocolVersion: MCP_PROTOCOL_VERSION,
            capabilities: { tools: {} },
            serverInfo: {
              name: "t3-boat-sentinel-hub",
              version: "1.0.0",
            },
          },
        });
        continue;
      }

      if (msg.method === "ping") {
        responses.push({ jsonrpc: "2.0", id: msg.id, result: {} });
        continue;
      }

      if (msg.method === "tools/list") {
        responses.push({ jsonrpc: "2.0", id: msg.id, result: { tools: MCP_TOOLS } });
        continue;
      }

      if (msg.method === "tools/call") {
        const params = (msg.params ?? {}) as { name?: unknown; arguments?: unknown };
        const toolName = typeof params.name === "string" ? params.name : "";
        const toolArgs =
          params.arguments && typeof params.arguments === "object"
            ? (params.arguments as Record<string, unknown>)
            : {};
        const toolResult = await this.executeTool(toolName, toolArgs, caller);
        responses.push({ jsonrpc: "2.0", id: msg.id, result: toolResult });
        continue;
      }

      responses.push({
        jsonrpc: "2.0",
        id: msg.id,
        error: { code: -32601, message: `Method not found: ${msg.method}` },
      });
    }

    const headers: Record<string, string> = {
      "mcp-protocol-version": MCP_PROTOCOL_VERSION,
      "cache-control": "no-store",
    };
    if (issuedSessionId) {
      headers["mcp-session-id"] = issuedSessionId;
    }
    if (linkExpiresAt) {
      headers[LINK_EXPIRES_AT_HEADER] = linkExpiresAt;
    }

    if (responses.length === 0) {
      return withMcpCors(new Response(null, { status: 202, headers }));
    }

    return withMcpCors(
      Response.json(isBatch ? responses : responses[0], {
        status: 200,
        headers,
      }),
    );
  }

  private buildCaller(
    client: ClientCredentialRecord,
    headers: Headers,
  ): { ok: true; caller: HubMcpCaller } | { ok: false } {
    const approvedAccess: AuthMcpClientAccess = client.access ?? "full-access";
    const readOnly = approvedAccess === "read-only";
    let runtimeMode: RuntimeMode = readOnly ? "approval-required" : approvedAccess;
    let interactionMode: ProviderInteractionMode = "default";

    const limitHeader = headers.get(MODE_LIMIT_HEADER);
    if (limitHeader !== null) {
      const parts = limitHeader.trim().split("/");
      if (parts.length !== 2 || !isRuntimeMode(parts[0]) || !isInteractionMode(parts[1])) {
        return { ok: false };
      }
      const [askedRuntime, askedInteraction] = parts as [RuntimeMode, ProviderInteractionMode];
      if (accessRank(askedRuntime) < accessRank(runtimeMode)) runtimeMode = askedRuntime;
      if (askedInteraction === "plan") interactionMode = "plan";
    }

    let senderThread: HubDelegatedFrom | undefined;
    const senderHeader = headers.get(SENDER_THREAD_HEADER);
    if (senderHeader !== null && client.peerLink === true) {
      const parts = senderHeader.trim().split("/");
      if (parts.length === 3) {
        try {
          const environmentId = decodeURIComponent(parts[0]).trim();
          const threadId = decodeURIComponent(parts[1]).trim();
          const title = decodeURIComponent(parts[2]);
          if (environmentId && threadId) {
            senderThread = { environmentId, threadId, title };
          }
        } catch {
          // Malformed URI encoding only drops the sender label, matching T3 Code.
        }
      }
    }

    return {
      ok: true,
      caller: {
        clientId: client.id,
        label: client.label,
        peerLink: client.peerLink === true,
        access: approvedAccess,
        readOnly,
        runtimeMode,
        interactionMode,
        ...(senderThread ? { senderThread } : {}),
      },
    };
  }

  private maybeRenewPeerLink(client: ClientCredentialRecord): string | undefined {
    if (client.peerLink !== true) return undefined;
    const now = Date.now();
    const targetMs = now + PEER_LINK_TOKEN_TTL_SECS * 1000;
    const currentMs = client.expiresAt ? Date.parse(client.expiresAt) : 0;
    if (!Number.isFinite(currentMs) || targetMs - currentMs >= PEER_LINK_RENEWAL_STEP_MS) {
      const renewedIso = new Date(targetMs).toISOString();
      this.state.updateHub((h) => {
        const rec = h.clients.find((c) => c.id === client.id);
        if (rec) rec.expiresAt = renewedIso;
      });
      return renewedIso;
    }
    return client.expiresAt;
  }

  private assertCanWrite(caller: HubMcpCaller): void {
    if (caller.readOnly) {
      throw new HubMcpToolError(
        "capability_denied",
        "This tool changes the environment, and this MCP client was approved for read-only access.",
      );
    }
  }

  private isFencedLink(caller: HubMcpCaller): boolean {
    return caller.peerLink && caller.access !== "full-access";
  }

  private getThreadLinkOrigin(threadId: string): HubThreadLinkOriginRecord | undefined {
    const pending = this.pendingLaunches.get(threadId);
    if (pending?.linkOrigin) return pending.linkOrigin;
    const stored = this.state.getHub()?.threadLinkOrigins?.[threadId];
    if (stored) return stored;
    const shell = this.bridge.getThreadShell(threadId);
    const raw = shell?.linkOrigin as Record<string, unknown> | undefined;
    if (raw && typeof raw.sessionId === "string" && typeof raw.label === "string") {
      return {
        sessionId: raw.sessionId,
        label: raw.label,
        ...(isAuthMcpClientAccess(raw.access) ? { access: raw.access } : {}),
      };
    }
    return undefined;
  }

  private getThreadDelegatedFrom(threadId: string, rawThread?: Record<string, unknown>): HubDelegatedFrom | undefined {
    const raw = rawThread?.delegatedFrom as Record<string, unknown> | undefined;
    if (
      raw &&
      typeof raw.environmentId === "string" &&
      raw.environmentId.trim() &&
      typeof raw.threadId === "string" &&
      raw.threadId.trim() &&
      typeof raw.title === "string"
    ) {
      return { environmentId: raw.environmentId.trim(), threadId: raw.threadId.trim(), title: raw.title };
    }
    const pending = this.pendingLaunches.get(threadId);
    if (pending?.delegatedFrom) return pending.delegatedFrom;
    return this.state.getHub()?.threadLinkOrigins?.[threadId]?.delegatedFrom;
  }

  private assertSameLink(caller: HubMcpCaller, threadId: string): void {
    if (!this.isFencedLink(caller)) return;
    const origin = this.getThreadLinkOrigin(threadId);
    if (origin?.sessionId !== caller.clientId) {
      throw new HubMcpToolError(
        "capability_denied",
        `This work was started from ${caller.label} through a linked environment, so it may change only threads that link started. A link paired with full access is not limited this way.`,
      );
    }
  }

  private assertNotFencedLink(caller: HubMcpCaller, what: string): void {
    if (!this.isFencedLink(caller)) return;
    throw new HubMcpToolError(
      "capability_denied",
      `This work was started from ${caller.label} through a linked environment, so it cannot ${what}. A link paired with full access is not limited this way.`,
    );
  }

  private assertTargetWithinLimits(caller: HubMcpCaller, threadId: string): void {
    const pending = this.pendingLaunches.get(threadId);
    const shell = this.bridge.getThreadShell(threadId);
    const targetRuntime = pending?.runtimeMode ?? (isRuntimeMode(shell?.runtimeMode) ? shell.runtimeMode : undefined);
    const targetInteraction =
      pending?.interactionMode ??
      (isInteractionMode(shell?.interactionMode) ? shell.interactionMode : undefined);
    if (targetRuntime && accessRank(targetRuntime) > accessRank(caller.runtimeMode)) {
      throw new HubMcpToolError(
        "runtime_mode_escalation_denied",
        `Thread ${threadId} runs in ${targetRuntime} mode, above this caller's ${caller.runtimeMode} limit.`,
      );
    }
    if (targetInteraction === "default" && caller.interactionMode === "plan") {
      throw new HubMcpToolError(
        "interaction_mode_escalation_denied",
        `Thread ${threadId} runs in default interaction mode, above this caller's plan limit.`,
      );
    }
  }

  private createSession(clientId: string): string {
    const now = Date.now();
    this.pruneExpiredState();
    if (this.sessions.size >= MAX_SESSIONS) {
      const oldestKey = this.sessions.keys().next().value;
      if (oldestKey) this.sessions.delete(oldestKey);
    }
    const id = randomUUID();
    this.sessions.set(id, { id, clientId, createdAt: now, lastUsedAt: now });
    return id;
  }

  private pruneExpiredState(): void {
    const now = Date.now();
    for (const [sid, s] of this.sessions) {
      if (now - s.lastUsedAt > SESSION_TTL_MS) this.sessions.delete(sid);
    }
    for (const [tid, rec] of this.destroyedThreads) {
      if (now - rec.destroyedAt > DESTROYED_THREAD_TTL_MS) this.destroyedThreads.delete(tid);
    }
    for (const [tid, launch] of this.pendingLaunches) {
      if (launch.phase === "error" && now - Date.parse(launch.updatedAt) > FAILED_LAUNCH_TTL_MS) {
        this.pendingLaunches.delete(tid);
      }
    }
  }

  private async executeTool(
    name: string,
    args: Record<string, unknown>,
    caller: HubMcpCaller,
  ): Promise<Record<string, unknown>> {
    try {
      const data = await this.dispatchTool(name, args, caller);
      return {
        content: [{ type: "text", text: JSON.stringify(data) }],
        structuredContent: data,
      };
    } catch (err) {
      const code = err instanceof HubMcpToolError ? err.code : "orchestration_error";
      const message = err instanceof Error ? err.message : String(err);
      const failure = { _tag: "OrchestratorMcpFailure", code, message };
      return {
        isError: true,
        content: [{ type: "text", text: JSON.stringify(failure) }],
        structuredContent: failure,
      };
    }
  }

  private async dispatchTool(
    name: string,
    args: Record<string, unknown>,
    caller: HubMcpCaller,
  ): Promise<Record<string, unknown>> {
    switch (name) {
      case "orchestrator_capabilities":
        return this.toolCapabilities(args, caller);
      case "t3_project_list":
        return this.toolProjectList(args);
      case "t3_project_read":
        return this.toolProjectRead(args);
      case "t3_thread_list":
        return this.toolThreadList(args);
      case "t3_thread_read":
        return this.toolThreadRead(args);
      case "t3_thread_launch":
        return this.toolThreadLaunch(args, caller);
      case "t3_thread_send":
        return this.toolThreadSend(args, caller);
      case "t3_thread_wait":
        return this.toolThreadWait(args);
      case "t3_thread_interrupt":
        return this.toolThreadInterrupt(args, caller);
      case "t3_thread_organize":
        return this.toolThreadOrganize(args, caller);
      case "list_thread_pull_requests":
        return this.toolListThreadPullRequests(args);
      case "t3_pending_request":
        return this.toolPendingRequest(args, caller);
      case "t3_worktree_list":
        return this.toolWorktreeList(args);
      case "t3_thread_search":
        return this.toolThreadSearch(args);
      case "ci_logs":
        return this.toolCiLogs(args);
      case "sentinel_add_project":
        return this.toolAddProject(args, caller);
      case "sentinel_status":
        return this.toolSentinelStatus();
      default:
        throw new HubMcpToolError("invalid_request", `Unknown MCP tool "${name}".`);
    }
  }

  private threadLink(threadId: string): string {
    const hub = this.state.ensureHub(
      this.config.hub?.label ?? "Boat",
      Object.keys(this.state.getAllProjects(this.config)),
    );
    return `t3-thread://v2/${hub.environmentId}/${threadId}`;
  }

  private nextSequence(): number {
    return this.bridge.nextSequence ? this.bridge.nextSequence() : this.state.nextHubSequence();
  }

  private rawServerProviders(): Array<Record<string, unknown>> {
    const cfg = this.bridge.getServerConfig?.() ?? this.state.getHub()?.serverConfig;
    const list = cfg && typeof cfg === "object" ? (cfg as { providers?: unknown }).providers : undefined;
    if (Array.isArray(list) && list.length > 0) {
      return list.filter((p): p is Record<string, unknown> => Boolean(p && typeof p === "object"));
    }
    return [
      {
        instanceId: DEFAULT_PROVIDER_INSTANCE_ID,
        driver: DEFAULT_PROVIDER_INSTANCE_ID,
        displayName: "Claude",
        enabled: true,
        installed: true,
        status: "ready",
        auth: { status: "authenticated" },
        models: [
          { slug: "claude-opus-5-5", name: "Opus 5.5", isCustom: false },
          { slug: DEFAULT_MODEL_SLUG, name: "Sonnet 4.6", isCustom: false },
        ],
      },
    ];
  }

  private defaultModelSelectionForProject(projectKey: string): { instanceId: string; model: string } {
    const proj = this.state.getAllProjects(this.config)[projectKey];
    if (proj?.defaultProvider && proj?.defaultModel) {
      return { instanceId: proj.defaultProvider, model: proj.defaultModel };
    }
    const providers = this.rawServerProviders();
    for (const p of providers) {
      if (p.enabled === false || p.installed === false) continue;
      const authStatus = (p.auth as { status?: unknown } | undefined)?.status;
      if (authStatus === "unauthenticated") continue;
      const instanceId = validProviderSlug(p.instanceId);
      const models = Array.isArray(p.models) ? (p.models as Array<Record<string, unknown>>) : [];
      const firstModel = models.find((m) => typeof m?.slug === "string" && m.slug.trim())?.slug as string | undefined;
      if (instanceId && firstModel) return { instanceId, model: firstModel };
    }
    return { instanceId: DEFAULT_PROVIDER_INSTANCE_ID, model: DEFAULT_MODEL_SLUG };
  }

  private toolCapabilities(args: Record<string, unknown>, caller: HubMcpCaller): Record<string, unknown> {
    const searchRaw = typeof args.search === "string" ? args.search.trim().toLowerCase() : "";
    const searchTerms = searchRaw ? searchRaw.split(/\s+/).filter(Boolean) : [];
    const rawProviders = this.rawServerProviders();

    const providers = rawProviders
      .map((p) => {
        const instanceId = validProviderSlug(p.instanceId);
        const driverKind = validProviderSlug(p.driver ?? p.instanceId);
        if (!instanceId || !driverKind) return null;

        const constraints: string[] = [];
        if (p.enabled === false) {
          constraints.push("Provider instance is disabled.");
        } else {
          if (p.installed === false) constraints.push("Provider executable is not installed.");
          if (p.status === "error" || p.status === "disabled") {
            constraints.push(typeof p.message === "string" && p.message ? p.message : `Provider status is ${p.status}.`);
          }
          const authStatus = (p.auth as { status?: unknown } | undefined)?.status;
          if (authStatus === "unauthenticated") constraints.push("Provider is not authenticated.");
        }

        const rawModels = Array.isArray(p.models)
          ? (p.models as Array<Record<string, unknown>>).filter(
              (m) => m && typeof m.slug === "string" && m.slug.trim().length > 0,
            )
          : [];
        const displayName = typeof p.displayName === "string" ? p.displayName : null;
        const providerText = `${instanceId} ${driverKind} ${displayName ?? ""}`.toLowerCase();
        const providerWords = new Set([...providerText.split(/\s+/), ...providerText.split(/[^\p{L}\p{N}]+/u)]);
        const modelText = (m: Record<string, unknown>) =>
          `${String(m.slug ?? "")} ${String(m.name ?? "")} ${String(m.subProvider ?? "")}`.toLowerCase();
        const modelTerms = searchTerms.filter(
          (term) =>
            !providerWords.has(term) &&
            (!providerText.includes(term) || rawModels.some((m) => modelText(m).includes(term))),
        );

        let listed: Array<Record<string, unknown>>;
        let omitted: number;
        if (modelTerms.length > 0) {
          const matches = rawModels.filter((m) => {
            const text = modelText(m);
            return modelTerms.every((term) => text.includes(term));
          });
          listed = matches.slice(0, LISTED_MODEL_LIMIT);
          omitted = matches.length - listed.length;
        } else {
          const current = rawModels.filter((m) => m.isLegacy !== true);
          const shown = FULL_CATALOG_DRIVERS.has(driverKind) ? current : current.slice(0, LISTED_MODEL_LIMIT);
          const shownSet = new Set(shown);
          listed = rawModels.filter((m) => m.isCustom === true || shownSet.has(m));
          omitted = Math.max(0, rawModels.length - listed.length);
        }

        const models = listed.map((m) => {
          const id = String(m.slug).trim();
          const name = typeof m.name === "string" ? m.name.trim() : "";
          const caps = m.capabilities as { optionDescriptors?: unknown } | undefined;
          const descriptors = Array.isArray(caps?.optionDescriptors)
            ? (caps.optionDescriptors as Array<Record<string, unknown>>)
            : [];
          const options = descriptors
            .map((d) => mapOptionDescriptor(d))
            .filter((o): o is Record<string, unknown> => o !== null);
          return {
            id,
            ...(name && name.toLowerCase() !== id.toLowerCase() ? { label: name } : {}),
            ...(options.length > 0 ? { options } : {}),
          };
        });

        const runtimeModes = Array.isArray(p.supportedRuntimeModes)
          ? p.supportedRuntimeModes.filter(isRuntimeMode)
          : [];

        return {
          providerInstanceId: instanceId,
          driverKind,
          displayName,
          models,
          ...(omitted > 0 ? { omittedModels: omitted } : {}),
          ...(runtimeModes.length > 0 ? { runtimeModes } : {}),
          canRunChildTask: constraints.length === 0,
          canRunCrossProviderChildTask: constraints.length === 0,
          constraints,
        };
      })
      .filter((p): p is NonNullable<typeof p> => p !== null);

    return {
      parentThreadId: null,
      inheritedProviderInstanceId: null,
      inheritedModel: null,
      inheritedOptions: [],
      runtimeMode: caller.runtimeMode,
      interactionMode: caller.interactionMode,
      providers,
      features: {
        appOwnedSubagents: true,
        asyncPolling: true,
        cancellation: true,
        batchThreadCreation: true,
        threadManagement: true,
        incrementalThreadRead: true,
        scheduledTasks: false,
        maxBatchThreads: 20,
      },
    };
  }

  private buildRepositoryIdentity(projectKey: string, proj: ProjectConfig): RepositoryIdentity | null {
    const fromRepo = parseGitHubRepositoryIdentity(proj.repo, proj.workdir);
    if (fromRepo) return fromRepo;
    if (proj.bakeCmd) {
      const m = /(?:https?:\/\/(?:www\.)?github\.com\/|git@github\.com:)([A-Za-z0-9_.-]+)\/([A-Za-z0-9_.-]+?)(?:\.git)?(?:\s|$|["';&|])/i.exec(
        proj.bakeCmd,
      );
      if (m) {
        return parseGitHubRepositoryIdentity(`${m[1]}/${m[2]}`, proj.workdir);
      }
    }
    const cached = this.state.getHub()?.projectTemplates[projectKey]?.shell?.repositoryIdentity as
      | Record<string, unknown>
      | undefined;
    if (cached && typeof cached.canonicalKey === "string" && cached.canonicalKey.trim()) {
      const loc = cached.locator as Record<string, unknown> | undefined;
      if (loc && loc.source === "git-remote" && typeof loc.remoteName === "string" && typeof loc.remoteUrl === "string") {
        return cached as unknown as RepositoryIdentity;
      }
      if (typeof cached.owner === "string" && typeof cached.name === "string") {
        return parseGitHubRepositoryIdentity(`${cached.owner}/${cached.name}`, proj.workdir);
      }
    }
    return null;
  }

  private formatProjectRecord(key: string, proj: ProjectConfig): Record<string, unknown> {
    const now = new Date().toISOString();
    const id = this.bridge.ensureHubProjectId(key);
    const repoId = this.buildRepositoryIdentity(key, proj);
    const fromHub = this.bridge.buildProjectShell?.(key, proj);
    const base = fromHub && typeof fromHub === "object" ? fromHub : {};
    const rawScripts = Array.isArray(base.scripts) ? base.scripts : [];
    const defaultSel =
      base.defaultModelSelection && typeof base.defaultModelSelection === "object"
        ? normalizeModelSelection(base.defaultModelSelection)
        : proj.defaultProvider && proj.defaultModel
          ? { instanceId: proj.defaultProvider, model: proj.defaultModel }
          : null;

    return {
      ...base,
      id,
      title: typeof base.title === "string" && base.title.trim() ? base.title.trim() : key,
      workspaceRoot: proj.workdir,
      repositoryIdentity: repoId,
      defaultModelSelection: defaultSel,
      scripts: rawScripts,
      archivedAt: typeof base.archivedAt === "string" ? base.archivedAt : null,
      createdAt: validIsoDateTime(base.createdAt, now),
      updatedAt: validIsoDateTime(base.updatedAt, now),
      deletedAt: null,
    };
  }

  private toolProjectList(args: Record<string, unknown>): Record<string, unknown> {
    const allProjects = this.state.getAllProjects(this.config);
    const seenRepoKeys = new Set<string>();
    const projects: Array<Record<string, unknown>> = [];

    for (const [key, proj] of Object.entries(allProjects)) {
      if (proj.registerProject === false) continue;
      const formatted = this.formatProjectRecord(key, proj);
      const repoId = formatted.repositoryIdentity as RepositoryIdentity | null;
      if (repoId) {
        if (seenRepoKeys.has(repoId.canonicalKey)) continue;
        seenRepoKeys.add(repoId.canonicalKey);
      }
      projects.push(formatted);
    }

    const cursor = typeof args.cursor === "number" && args.cursor >= 0 ? Math.floor(args.cursor) : 0;
    const limit = typeof args.limit === "number" && args.limit > 0 ? Math.min(Math.floor(args.limit), 200) : 100;
    const slice = projects.slice(cursor, cursor + limit);
    const nextCursor = cursor + limit < projects.length ? cursor + limit : null;

    return {
      projects: slice,
      nextCursor,
    };
  }

  private toolProjectRead(args: Record<string, unknown>): Record<string, unknown> {
    const projectId = requireString(args.projectId, "projectId");
    const projectKey = this.bridge.projectKeyFromHubId(projectId);
    const proj = projectKey ? this.state.getAllProjects(this.config)[projectKey] : undefined;
    if (!projectKey || !proj) {
      throw new HubMcpToolError("invalid_request", `Project "${projectId}" was not found.`);
    }
    return this.formatProjectRecord(projectKey, proj);
  }

  private sandboxMcpView(sb: HubSandboxRecord): Record<string, unknown> {
    const upsizing = this.bridge.getUpsizeProgress?.(sb.sandboxId);
    const step = upsizing
      ? `Upsizing ${upsizing.fromSize} -> ${upsizing.toSize} (${upsizing.trigger})`
      : sb.phase === "running"
        ? "Running"
        : sb.phase === "sleeping"
          ? sb.sleepReason
            ? `Sleeping (${sb.sleepReason})`
            : sb.parkedAt
              ? "Parked"
              : "Sleeping"
          : sb.phase;
    return {
      sandboxId: sb.sandboxId,
      phase: sb.phase === "provisioning" ? "booting" : sb.phase,
      step,
      error: null,
      pauseReason: upsizing ? null : sb.phase === "sleeping" ? (sb.sleepReason ?? null) : null,
    };
  }

  private runStoppedByPause(status: unknown): boolean {
    return typeof status === "string" && (ACTIVE_RUN_STATUSES.has(status) || status === "interrupted");
  }

  private syntheticPauseNotice(threadId: string, sb: HubSandboxRecord, latestRunId: string): Record<string, unknown> | null {
    if (sb.phase !== "sleeping" || !sb.sleepReason || this.bridge.getUpsizeProgress?.(sb.sandboxId)) return null;
    const now = new Date().toISOString();
    const at = sb.sleepingSince ?? now;
    return {
      visibility: "local",
      sourceThreadId: threadId,
      itemId: `hub-pause-notice:${sb.sandboxId}:${at}`,
      runId: latestRunId,
      messageId: null,
      createdBy: null,
      creationSource: null,
      type: "assistant_message",
      status: "completed",
      title: "Sandbox paused",
      text:
        `[t3-boat-sentinel] This chat's Boat VM (${sb.sandboxId}) was paused while a turn was active: ${sb.sleepReason}. ` +
        "Its disk, worktree, and chat history are preserved — send a message to wake it and continue.",
      updatedAt: at,
    };
  }

  private formatShellThread(
    threadId: string,
    shell: Record<string, unknown>,
    sandboxId: string | null,
  ): Record<string, unknown> {
    const now = new Date().toISOString();
    const sb = sandboxId ? this.state.getHubSandbox(sandboxId) : undefined;
    const projectId = String(shell.projectId ?? sb?.hubProjectId ?? "unknown");
    const title = String(shell.title ?? "Untitled thread");
    const status = normalizeThreadStatus(shell);
    const createdAt = validIsoDateTime(shell.createdAt, sb?.createdAt ?? now);
    const updatedAt = validIsoDateTime(shell.updatedAt ?? shell.createdAt, sb?.lastActiveAt ?? createdAt);
    const { pullRequests, linkedPullRequest, openPullRequestCount } = extractPullRequests(projectId, shell);
    const pendingRequestCount =
      typeof shell.pendingRequestCount === "number"
        ? shell.pendingRequestCount
        : Array.isArray(shell.pendingRequests)
          ? shell.pendingRequests.length
          : shell.pendingRuntimeRequest
            ? 1
            : 0;
    const settled = Boolean(shell.settledOverride === "settled" || shell.settled === true || (!shell.settledOverride && shell.settledAt));
    const settledAt = settled && typeof shell.settledAt === "string" ? shell.settledAt : null;
    const snoozedUntil = typeof shell.snoozedUntil === "string" ? shell.snoozedUntil : null;
    const snoozed = snoozedUntil !== null && Date.parse(snoozedUntil) > Date.now();
    const modelSel = normalizeModelSelection(shell.modelSelection);
    const providerInstanceId =
      validProviderSlug(modelSel?.instanceId ?? shell.providerInstanceId) ?? DEFAULT_PROVIDER_INSTANCE_ID;
    const model = modelSel?.model ?? (typeof shell.model === "string" && shell.model ? shell.model : DEFAULT_MODEL_SLUG);
    const lineage = (shell.lineage && typeof shell.lineage === "object" ? shell.lineage : {}) as Record<string, unknown>;
    const parentThreadId = typeof lineage.parentThreadId === "string" ? lineage.parentThreadId : null;
    const relationshipToParent =
      lineage.relationshipToParent === "fork" || lineage.relationshipToParent === "subagent"
        ? lineage.relationshipToParent
        : null;
    const delegatedFrom = this.getThreadDelegatedFrom(threadId, shell);
    const latestRunId = typeof shell.latestRunId === "string" && shell.latestRunId ? shell.latestRunId : null;
    const itemCount =
      typeof shell.visibleItemCount === "number" && shell.visibleItemCount >= 0
        ? Math.floor(shell.visibleItemCount)
        : typeof shell.itemCount === "number" && shell.itemCount >= 0
          ? Math.floor(shell.itemCount)
          : 0;

    return {
      threadId,
      projectId,
      title,
      link: this.threadLink(threadId),
      createdBy: validActor(shell.createdBy, "user"),
      creationSource: validCreationSource(shell.creationSource, "web"),
      status,
      latestRunId,
      providerInstanceId,
      model,
      runtimeMode: isRuntimeMode(shell.runtimeMode) ? shell.runtimeMode : "full-access",
      interactionMode: isInteractionMode(shell.interactionMode) ? shell.interactionMode : "default",
      linkedPullRequest,
      pullRequests,
      pullRequestCount: pullRequests.length,
      openPullRequestCount,
      pendingRequestCount,
      settled,
      settledAt,
      snoozed,
      snoozedUntil: snoozed ? snoozedUntil : null,
      pinned: shell.pinnedAt != null,
      blockedOnThreadWait: shell.blockedOnThreadWait === true,
      pendingBackgroundTasks: Array.isArray(shell.pendingBackgroundTasks) ? shell.pendingBackgroundTasks : [],
      parentThreadId,
      relationshipToParent,
      ...(delegatedFrom ? { delegatedFrom } : {}),
      itemCount,
      createdAt,
      updatedAt,
      sandbox: sb ? this.sandboxMcpView(sb) : null,
    };
  }

  private formatPendingThread(launch: HubPendingLaunch): Record<string, unknown> {
    const status =
      launch.phase === "error"
        ? "failed"
        : launch.phase === "running"
          ? "starting"
          : "preparing";
    const syntheticRunId =
      launch.phase === "error" ? `run_hub_launch_${launch.threadId}` : (launch.launchedRunId ?? null);
    return {
      threadId: launch.threadId,
      projectId: launch.projectId,
      title: launch.title,
      link: this.threadLink(launch.threadId),
      createdBy: "agent",
      creationSource: "mcp",
      status,
      latestRunId: syntheticRunId,
      activeRunId: launch.phase === "error" ? null : syntheticRunId,
      providerInstanceId: launch.modelSelection.instanceId,
      model: launch.modelSelection.model,
      runtimeMode: launch.runtimeMode,
      interactionMode: launch.interactionMode,
      linkedPullRequest: null,
      pullRequests: [],
      pullRequestCount: 0,
      openPullRequestCount: 0,
      titleRegeneration: null,
      branch: null,
      worktreePath: null,
      parentThreadId: null,
      relationshipToParent: null,
      ...(launch.delegatedFrom ? { delegatedFrom: launch.delegatedFrom } : {}),
      runCount: syntheticRunId ? 1 : 0,
      itemCount: launch.phase === "error" ? 1 : 0,
      pendingRequestCount: 0,
      archived: false,
      settled: false,
      settledAt: null,
      snoozed: false,
      snoozedUntil: null,
      pinned: false,
      blockedOnThreadWait: false,
      pendingBackgroundTasks: [],
      createdAt: launch.createdAt,
      updatedAt: launch.updatedAt,
      sandbox: {
        sandboxId: launch.sandboxId,
        phase: launch.phase,
        step: launch.step,
        error: launch.error,
      },
    };
  }

  private toolThreadList(args: Record<string, unknown>): Record<string, unknown> {
    this.pruneExpiredState();
    const allProjects = args.allProjects === true;
    const filterProjectId =
      !allProjects && typeof args.projectId === "string" && args.projectId.trim() ? args.projectId.trim() : null;
    const filterStatuses = Array.isArray(args.statuses)
      ? new Set(args.statuses.filter((s): s is string => typeof s === "string"))
      : null;
    const filterSettled = typeof args.settled === "boolean" ? args.settled : null;
    const filterSnoozed = typeof args.snoozed === "boolean" ? args.snoozed : null;
    const filterTitle = typeof args.titleContains === "string" && args.titleContains.trim()
      ? args.titleContains.trim().toLowerCase()
      : null;
    const includeSubagents = args.includeSubagents !== false;

    const byId = new Map<string, Record<string, unknown>>();

    for (const entry of this.bridge.listThreadShells()) {
      byId.set(entry.threadId, this.formatShellThread(entry.threadId, entry.shell, entry.sandboxId));
    }

    for (const launch of this.pendingLaunches.values()) {
      if (launch.cancelled) continue;
      if (!byId.has(launch.threadId)) {
        byId.set(launch.threadId, this.formatPendingThread(launch));
      }
    }

    let threads = Array.from(byId.values());
    if (filterProjectId) {
      threads = threads.filter((t) => t.projectId === filterProjectId);
    }
    if (filterStatuses && filterStatuses.size > 0) {
      threads = threads.filter((t) => filterStatuses.has(String(t.status)));
    }
    if (filterSettled !== null) {
      threads = threads.filter((t) => Boolean(t.settled) === filterSettled);
    }
    if (filterSnoozed !== null) {
      threads = threads.filter((t) => Boolean(t.snoozed) === filterSnoozed);
    }
    if (filterTitle) {
      threads = threads.filter((t) => String(t.title ?? "").toLowerCase().includes(filterTitle));
    }
    if (!includeSubagents) {
      threads = threads.filter((t) => t.relationshipToParent !== "subagent");
    }

    threads.sort((a, b) => String(b.updatedAt ?? "").localeCompare(String(a.updatedAt ?? "")));

    const cursor = typeof args.cursor === "number" && args.cursor >= 0 ? Math.floor(args.cursor) : 0;
    const limit = typeof args.limit === "number" && args.limit > 0 ? Math.min(Math.floor(args.limit), 100) : 50;
    const slice = threads.slice(cursor, cursor + limit);
    const nextCursor = cursor + limit < threads.length ? cursor + limit : null;

    return {
      projectId: filterProjectId,
      currentThreadId: null,
      threads: slice,
      nextCursor,
      total: threads.length,
    };
  }

  private buildPendingReadResult(pending: HubPendingLaunch): Record<string, unknown> {
    const thread = this.formatPendingThread(pending);
    if (pending.phase === "error") {
      const runId = `run_hub_launch_${pending.threadId}`;
      const errorText = pending.error ?? "Sandbox failed to boot for this thread.";
      return {
        thread,
        recentRuns: [
          {
            runId,
            ordinal: 1,
            status: "failed",
            providerInstanceId: pending.modelSelection.instanceId,
            model: pending.modelSelection.model,
            executedModel: null,
            requestedAt: pending.createdAt,
            startedAt: pending.createdAt,
            completedAt: pending.updatedAt,
          },
        ],
        items: [
          {
            position: 0,
            visibility: "synthetic",
            sourceThreadId: pending.threadId,
            itemId: `hub_launch_err_${pending.threadId}`,
            runId,
            messageId: null,
            createdBy: "system",
            creationSource: "server",
            type: "error",
            status: "failed",
            title: "VM boot failed",
            text: errorText,
            textTruncated: false,
            nextTextOffset: null,
            updatedAt: pending.updatedAt,
          },
        ],
        nextPosition: 0,
        hasMore: false,
        earlierPosition: null,
      };
    }
    return {
      thread,
      items: [],
      recentRuns: [],
      nextPosition: null,
      hasMore: false,
      earlierPosition: null,
    };
  }

  private buildFallbackShellReadResult(
    threadId: string,
    shell: Record<string, unknown> | undefined,
    sb: HubSandboxRecord,
    opts?: { beforePosition: number | null; itemId: string | null },
  ): Record<string, unknown> {
    const now = new Date().toISOString();
    const base = shell
      ? this.formatShellThread(threadId, shell, sb.sandboxId)
      : {
          threadId,
          projectId: sb.hubProjectId,
          title: sb.phase === "sleeping" ? "Sleeping thread" : "Untitled thread",
          link: this.threadLink(threadId),
          createdBy: "user",
          creationSource: "web",
          status: "idle",
          latestRunId: null,
          providerInstanceId: DEFAULT_PROVIDER_INSTANCE_ID,
          model: DEFAULT_MODEL_SLUG,
          runtimeMode: "full-access",
          interactionMode: "default",
          linkedPullRequest: null,
          pullRequests: [],
          pullRequestCount: 0,
          openPullRequestCount: 0,
          pendingRequestCount: 0,
          settled: false,
          settledAt: null,
          snoozed: false,
          snoozedUntil: null,
          pinned: false,
          blockedOnThreadWait: false,
          pendingBackgroundTasks: [],
          parentThreadId: null,
          relationshipToParent: null,
          itemCount: 0,
          createdAt: sb.createdAt ?? now,
          updatedAt: sb.sleepingSince ?? sb.lastActiveAt ?? now,
          sandbox: this.sandboxMcpView(sb),
        };
    const shellStatus = shell ? normalizeThreadStatus(shell) : "idle";
    const latestRunId = typeof base.latestRunId === "string" && base.latestRunId ? base.latestRunId : null;
    const pausedMidRun =
      sb.phase === "sleeping" &&
      !this.bridge.getUpsizeProgress?.(sb.sandboxId) &&
      latestRunId !== null &&
      this.runStoppedByPause(shellStatus);
    const notice = pausedMidRun && latestRunId ? this.syntheticPauseNotice(threadId, sb, latestRunId) : null;
    const includeNotice = notice !== null && opts?.itemId == null && opts?.beforePosition == null;
    return {
      thread: {
        ...base,
        status: pausedMidRun ? "interrupted" : base.status,
        activeRunId: pausedMidRun ? null : typeof shell?.activeRunId === "string" ? shell.activeRunId : null,
        titleRegeneration: null,
        branch: typeof shell?.branch === "string" ? shell.branch : null,
        worktreePath: typeof shell?.worktreePath === "string" ? shell.worktreePath : null,
        runCount: base.latestRunId ? 1 : 0,
        archived: shell?.archivedAt != null,
      },
      items: includeNotice ? [{ position: 0, ...notice, textTruncated: false, nextTextOffset: null }] : [],
      recentRuns: [],
      nextPosition: includeNotice ? 0 : null,
      hasMore: false,
      earlierPosition: null,
    };
  }

  private async toolThreadRead(args: Record<string, unknown>): Promise<Record<string, unknown>> {
    const threadId = requireString(args.threadId, "threadId");
    const view = args.view === "activity" ? "activity" : "messages";
    const limit = typeof args.limit === "number" && args.limit > 0 ? Math.min(Math.floor(args.limit), 100) : 50;
    const maxChars =
      typeof args.maxCharsPerItem === "number" && args.maxCharsPerItem > 0
        ? Math.min(Math.floor(args.maxCharsPerItem), 20_000)
        : 20_000;
    const runLimit =
      typeof args.runLimit === "number" && args.runLimit > 0 ? Math.min(Math.floor(args.runLimit), 50) : 10;
    const afterPosition = typeof args.afterPosition === "number" ? Math.floor(args.afterPosition) : null;
    const beforePosition = typeof args.beforePosition === "number" ? Math.floor(args.beforePosition) : null;
    const itemId = typeof args.itemId === "string" && args.itemId.trim() ? args.itemId.trim() : null;
    const textOffset = typeof args.textOffset === "number" && args.textOffset >= 0 ? Math.floor(args.textOffset) : 0;

    const readOpts = { view, limit, maxChars, runLimit, afterPosition, beforePosition, itemId, textOffset } as const;

    const sb = this.bridge.getSandboxForThread(threadId);
    const pending = this.pendingLaunches.get(threadId);

    if ((!sb || sb.phase === "provisioning") && pending) {
      return this.buildPendingReadResult(pending);
    }

    if (sb && sb.phase === "running") {
      const res = await this.bridge.callVmUnary(sb.sandboxId, "orchestration.getThreadProjection", { threadId }, 12_000);
      if (res._tag === "Success" && res.value && typeof res.value === "object") {
        return this.mapProjectionToThreadRead(res.value as Record<string, unknown>, sb, readOpts);
      }
      if (pending && pending.phase !== "error") {
        return this.buildPendingReadResult(pending);
      }
      const cached = this.bridge.getCachedProjection?.(threadId);
      if (cached) {
        return this.mapProjectionToThreadRead(cached, sb, readOpts);
      }
      const shell = this.bridge.getThreadShell(threadId);
      if (shell) {
        return this.buildFallbackShellReadResult(threadId, shell, sb, readOpts);
      }
      throw new HubMcpToolError("orchestration_error", `Failed to read projection for thread "${threadId}".`);
    }

    if (sb && sb.phase === "sleeping") {
      const cached = this.bridge.getCachedProjection?.(threadId);
      if (cached) {
        return this.mapProjectionToThreadRead(cached, sb, readOpts);
      }
      const shell = this.bridge.getThreadShell(threadId);
      return this.buildFallbackShellReadResult(threadId, shell, sb, readOpts);
    }

    const destroyed = this.destroyedThreads.get(threadId);
    if (destroyed) {
      throw new HubMcpToolError(
        "sandbox_destroyed",
        `Sandbox for thread "${threadId}" was destroyed (${destroyed.reason}).`,
      );
    }

    throw new HubMcpToolError("thread_not_found", `Thread "${threadId}" was not found.`);
  }

  private mapProjectionToThreadRead(
    projection: Record<string, unknown>,
    sb: HubSandboxRecord,
    opts: {
      view: "messages" | "activity";
      limit: number;
      maxChars: number;
      runLimit: number;
      afterPosition: number | null;
      beforePosition: number | null;
      itemId: string | null;
      textOffset: number;
    },
  ): Record<string, unknown> {
    const now = new Date().toISOString();
    const rawThread = (projection.thread ?? {}) as Record<string, unknown>;
    const threadId = String(rawThread.id ?? rawThread.threadId ?? "");
    const rawRuns = Array.isArray(projection.runs) ? (projection.runs as Array<Record<string, unknown>>) : [];
    const rawMessages = Array.isArray(projection.messages)
      ? (projection.messages as Array<Record<string, unknown>>)
      : [];
    const rawTurnItems = Array.isArray(projection.turnItems)
      ? (projection.turnItems as Array<Record<string, unknown>>)
      : [];
    const rawVisibleItems = Array.isArray(projection.visibleTurnItems)
      ? (projection.visibleTurnItems as Array<Record<string, unknown>>)
      : [];
    const rawRequests = Array.isArray(projection.runtimeRequests)
      ? (projection.runtimeRequests as Array<Record<string, unknown>>)
      : [];
    const rawProviderTurns = Array.isArray(projection.providerTurns)
      ? (projection.providerTurns as Array<Record<string, unknown>>)
      : [];

    const pendingRequests = rawRequests.filter((r) => r && r.status === "pending");
    const pendingUserInput = pendingRequests.filter((r) => r.kind === "user_input");
    const { pullRequests, linkedPullRequest, openPullRequestCount } = extractPullRequests(
      sb.hubProjectId,
      projection,
      rawThread,
    );

    const sortedRunsDesc = [...rawRuns].sort((a, b) => {
      const ordA = typeof a.ordinal === "number" ? a.ordinal : 0;
      const ordB = typeof b.ordinal === "number" ? b.ordinal : 0;
      if (ordA !== ordB) return ordB - ordA;
      return String(b.requestedAt ?? b.startedAt ?? "").localeCompare(String(a.requestedAt ?? a.startedAt ?? ""));
    });
    const latestRun = sortedRunsDesc[0];
    const activeRun = sortedRunsDesc.find((r) => typeof r.status === "string" && ACTIVE_RUN_STATUSES.has(r.status));
    let status = normalizeThreadStatus(rawThread);
    if (pendingUserInput.length > 0) {
      status = "waiting";
    } else if (activeRun && typeof activeRun.status === "string" && VALID_THREAD_STATUSES.has(activeRun.status)) {
      status = activeRun.status;
    } else if (latestRun && typeof latestRun.status === "string" && VALID_THREAD_STATUSES.has(latestRun.status)) {
      status = latestRun.status;
    }
    const latestRunId = latestRun ? String(latestRun.id ?? latestRun.runId ?? "") || null : null;
    const pausedMidRun =
      sb.phase === "sleeping" &&
      !this.bridge.getUpsizeProgress?.(sb.sandboxId) &&
      latestRun !== undefined &&
      this.runStoppedByPause(latestRun.status);
    if (pausedMidRun) status = "interrupted";

    const threadModelSel = normalizeModelSelection(rawThread.modelSelection);
    const threadProviderId =
      validProviderSlug(threadModelSel?.instanceId ?? rawThread.providerInstanceId) ?? DEFAULT_PROVIDER_INSTANCE_ID;
    const threadModel = threadModelSel?.model ?? DEFAULT_MODEL_SLUG;
    const threadCreatedAt = validIsoDateTime(rawThread.createdAt, sb.createdAt ?? now);
    const threadUpdatedAt = validIsoDateTime(rawThread.updatedAt ?? rawThread.createdAt, sb.lastActiveAt ?? threadCreatedAt);

    // Map providerTurns by attemptId for executedModel
    const executedModelByAttempt = new Map<string, string>();
    for (const pt of rawProviderTurns) {
      if (typeof pt.attemptId === "string" && typeof pt.model === "string" && pt.model.trim()) {
        executedModelByAttempt.set(pt.attemptId, pt.model.trim());
      }
    }

    // Build ordered timeline items matching OrchestratorMcpThreadTimelineItem
    const messageById = new Map<string, Record<string, unknown>>();
    for (const m of rawMessages) {
      if (typeof m.id === "string") messageById.set(m.id, m);
    }

    interface TimelineEntry {
      itemId: string;
      visibility: "local" | "inherited" | "synthetic";
      sourceThreadId: string;
      subagentId?: string;
      runId: string | null;
      messageId: string | null;
      createdBy: "user" | "agent" | "system" | null;
      creationSource: "web" | "mobile" | "mcp" | "provider" | "server" | null;
      type: string;
      status: string;
      title: string | null;
      text: string | null;
      updatedAt: string;
      role?: string;
    }

    const fullTimeline: TimelineEntry[] = [];
    const buildEntryFromTurnItem = (
      ti: Record<string, unknown>,
      idx: number,
      visibility: "local" | "inherited" | "synthetic" = "local",
      sourceThreadId = threadId,
    ): TimelineEntry => {
      const msgId = typeof ti.messageId === "string" ? ti.messageId : null;
      const linkedMsg = msgId ? messageById.get(msgId) : undefined;
      const rawText = extractTurnItemText(ti, linkedMsg);
      const itemType = String(ti.type ?? "activity");
      const role =
        typeof linkedMsg?.role === "string"
          ? linkedMsg.role
          : itemType === "user_message"
            ? "user"
            : itemType === "assistant_message"
              ? "assistant"
              : undefined;
      const createdBy =
        linkedMsg && VALID_ACTORS.has(String(linkedMsg.createdBy))
          ? (String(linkedMsg.createdBy) as "user" | "agent" | "system")
          : VALID_ACTORS.has(String(ti.createdBy))
            ? (String(ti.createdBy) as "user" | "agent" | "system")
            : null;
      const creationSource =
        linkedMsg && VALID_CREATION_SOURCES.has(String(linkedMsg.creationSource))
          ? (String(linkedMsg.creationSource) as "web" | "mobile" | "mcp" | "provider" | "server")
          : VALID_CREATION_SOURCES.has(String(ti.creationSource))
            ? (String(ti.creationSource) as "web" | "mobile" | "mcp" | "provider" | "server")
            : null;
      return {
        itemId: typeof ti.id === "string" && ti.id ? ti.id : `item_${threadId}_${idx}`,
        visibility,
        sourceThreadId,
        ...(itemType === "subagent" && typeof ti.subagentId === "string" ? { subagentId: ti.subagentId } : {}),
        runId: typeof ti.runId === "string" && ti.runId ? ti.runId : null,
        messageId: msgId,
        createdBy,
        creationSource,
        type: itemType,
        status: validTurnItemStatus(ti.status, "completed"),
        title: typeof ti.title === "string" ? ti.title : null,
        text: rawText,
        updatedAt: validIsoDateTime(ti.updatedAt ?? linkedMsg?.updatedAt ?? ti.createdAt, threadUpdatedAt),
        ...(role ? { role } : {}),
      };
    };

    const sourceItems: Array<{
      ti: Record<string, unknown>;
      visibility: "local" | "inherited" | "synthetic";
      sourceThreadId: string;
    }> =
      rawVisibleItems.length > 0
        ? rawVisibleItems
            .filter((row) => row && row.item && typeof row.item === "object")
            .map((row) => ({
              ti: row.item as Record<string, unknown>,
              visibility:
                row.visibility === "inherited" || row.visibility === "synthetic" ? row.visibility : "local",
              sourceThreadId:
                typeof row.sourceThreadId === "string" && row.sourceThreadId ? row.sourceThreadId : threadId,
            }))
        : rawTurnItems.map((ti) => ({ ti, visibility: "local" as const, sourceThreadId: threadId }));

    if (opts.view === "activity" && sourceItems.length > 0) {
      for (let i = 0; i < sourceItems.length; i++) {
        const s = sourceItems[i];
        fullTimeline.push(buildEntryFromTurnItem(s.ti, i, s.visibility, s.sourceThreadId));
      }
    } else if (rawMessages.length > 0) {
      // Find matching turnItem id for each message if available
      const turnItemByMsgId = new Map<string, Record<string, unknown>>();
      for (const s of sourceItems) {
        if (typeof s.ti.messageId === "string") turnItemByMsgId.set(s.ti.messageId, s.ti);
      }
      for (let i = 0; i < rawMessages.length; i++) {
        const m = rawMessages[i];
        const role = typeof m.role === "string" ? m.role : "assistant";
        const msgId = typeof m.id === "string" && m.id ? m.id : `msg_${threadId}_${i}`;
        const linkedTi = turnItemByMsgId.get(msgId);
        const itemId =
          linkedTi && typeof linkedTi.id === "string" && linkedTi.id
            ? linkedTi.id
            : `turn-item:message:${msgId}`;
        fullTimeline.push({
          itemId,
          visibility: "local",
          sourceThreadId: threadId,
          runId: typeof m.runId === "string" && m.runId ? m.runId : null,
          messageId: msgId,
          createdBy: validActor(m.createdBy, role === "user" ? "user" : "agent"),
          creationSource: validCreationSource(m.creationSource, "provider"),
          type: role === "user" ? "user_message" : "assistant_message",
          status: m.streaming === true ? "running" : "completed",
          title: null,
          text: typeof m.text === "string" ? m.text : "",
          updatedAt: validIsoDateTime(m.updatedAt ?? m.createdAt, threadUpdatedAt),
          role,
        });
      }
    } else if (sourceItems.length > 0) {
      for (let i = 0; i < sourceItems.length; i++) {
        const s = sourceItems[i];
        if (s.ti.type === "user_message" || s.ti.type === "assistant_message" || s.ti.type === "error") {
          fullTimeline.push(buildEntryFromTurnItem(s.ti, i, s.visibility, s.sourceThreadId));
        }
      }
    }

    if (pausedMidRun && latestRunId && opts.itemId === null) {
      const notice = this.syntheticPauseNotice(threadId, sb, latestRunId);
      if (notice && !fullTimeline.some((entry) => entry.itemId === notice.itemId)) {
        fullTimeline.push(notice as unknown as TimelineEntry);
      }
    }

    const formatSliceItem = (entry: TimelineEntry, position: number, offset: number): Record<string, unknown> => {
      const fullText = entry.text;
      if (fullText === null) {
        return {
          position,
          ...entry,
          text: null,
          textTruncated: false,
          nextTextOffset: null,
        };
      }
      const end = textPageEnd(fullText, offset, opts.maxChars);
      const truncated = fullText.length > end;
      return {
        position,
        ...entry,
        text: fullText.slice(offset, end),
        textTruncated: truncated,
        nextTextOffset: truncated ? end : null,
      };
    };

    let page: Array<Record<string, unknown>>;
    let hasMore = false;
    let earlierPosition: number | null = null;
    let nextPosition: number | null = null;

    if (opts.itemId !== null) {
      // Single-item continuation read (used by RemoteDelegation.lastReply for replies > 20k chars)
      let matchIdx = fullTimeline.findIndex((e) => e.itemId === opts.itemId);
      // If view was messages and item lives in activity turnItems, check activity items too
      if (matchIdx === -1 && sourceItems.length > 0) {
        const tiIdx = sourceItems.findIndex((s) => s.ti.id === opts.itemId);
        if (tiIdx !== -1) {
          const entry = buildEntryFromTurnItem(
            sourceItems[tiIdx].ti,
            tiIdx,
            sourceItems[tiIdx].visibility,
            sourceItems[tiIdx].sourceThreadId,
          );
          page = [formatSliceItem(entry, tiIdx, opts.textOffset)];
          nextPosition = tiIdx;
        } else {
          page = [];
        }
      } else if (matchIdx !== -1) {
        page = [formatSliceItem(fullTimeline[matchIdx], matchIdx, opts.textOffset)];
        nextPosition = matchIdx;
      } else {
        page = [];
      }
    } else {
      const indexed = fullTimeline.map((entry, position) => ({ entry, position }));
      const newestFirst = opts.beforePosition !== null || opts.afterPosition === null;
      const filtered = indexed.filter(({ position }) => {
        if (opts.afterPosition !== null && position <= opts.afterPosition) return false;
        if (opts.beforePosition !== null && position >= opts.beforePosition) return false;
        return true;
      });
      if (newestFirst) {
        const start = Math.max(0, filtered.length - opts.limit);
        const slice = filtered.slice(start);
        page = slice.map(({ entry, position }) => formatSliceItem(entry, position, 0));
        earlierPosition = start > 0 && slice.length > 0 ? slice[0].position : null;
        hasMore =
          slice.length > 0 &&
          slice[slice.length - 1].position < fullTimeline.length - 1;
        nextPosition = slice.length > 0 ? slice[slice.length - 1].position : null;
      } else {
        const slice = filtered.slice(0, opts.limit);
        page = slice.map(({ entry, position }) => formatSliceItem(entry, position, 0));
        hasMore = filtered.length > slice.length;
        earlierPosition = slice.length > 0 && slice[0].position > 0 ? slice[0].position : null;
        nextPosition = slice.length > 0 ? slice[slice.length - 1].position : null;
      }
    }

    const recentRuns = sortedRunsDesc.slice(0, opts.runLimit).map((r, idx) => {
      const runId = String(r.id ?? r.runId ?? `run_${threadId}_${idx + 1}`);
      const runSel = normalizeModelSelection(r.modelSelection);
      const providerInstanceId =
        validProviderSlug(runSel?.instanceId ?? r.providerInstanceId) ?? threadProviderId;
      const model = runSel?.model ?? (typeof r.model === "string" && r.model ? r.model : threadModel);
      const ordinal =
        typeof r.ordinal === "number" && Number.isInteger(r.ordinal) && r.ordinal >= 1
          ? r.ordinal
          : Math.max(1, sortedRunsDesc.length - idx);
      const rawStatus =
        pausedMidRun && runId === latestRunId
          ? "interrupted"
          : typeof r.status === "string" && VALID_THREAD_STATUSES.has(r.status) && r.status !== "idle"
            ? r.status
            : "completed";
      const activeAttemptId = typeof r.activeAttemptId === "string" ? r.activeAttemptId : null;
      const executedModel =
        typeof r.executedModel === "string"
          ? r.executedModel
          : activeAttemptId
            ? (executedModelByAttempt.get(activeAttemptId) ?? null)
            : null;
      return {
        runId,
        id: runId,
        ordinal,
        status: rawStatus,
        providerInstanceId,
        model,
        executedModel,
        requestedAt: validIsoDateTime(r.requestedAt ?? r.startedAt, threadCreatedAt),
        startedAt: typeof r.startedAt === "string" ? r.startedAt : null,
        completedAt: typeof r.completedAt === "string" ? r.completedAt : null,
        ...(typeof r.error === "string" ? { error: r.error } : {}),
      };
    });

    const lineage = (rawThread.lineage && typeof rawThread.lineage === "object" ? rawThread.lineage : {}) as Record<
      string,
      unknown
    >;
    const parentThreadId = typeof lineage.parentThreadId === "string" ? lineage.parentThreadId : null;
    const relationshipToParent =
      lineage.relationshipToParent === "fork" || lineage.relationshipToParent === "subagent"
        ? lineage.relationshipToParent
        : null;
    const delegatedFrom = this.getThreadDelegatedFrom(threadId, rawThread);
    const settled = Boolean(
      rawThread.settledOverride === "settled" || (!rawThread.settledOverride && rawThread.settledAt),
    );
    const settledAt = settled && typeof rawThread.settledAt === "string" ? rawThread.settledAt : null;
    const snoozedUntil = typeof rawThread.snoozedUntil === "string" ? rawThread.snoozedUntil : null;
    const snoozed = snoozedUntil !== null && Date.parse(snoozedUntil) > Date.now();
    const rawTitleRegen = rawThread.titleRegeneration as { requestId?: unknown; startedAt?: unknown } | null | undefined;
    const titleRegeneration =
      rawTitleRegen && typeof rawTitleRegen.requestId === "string" && typeof rawTitleRegen.startedAt === "string"
        ? { requestId: rawTitleRegen.requestId, startedAt: rawTitleRegen.startedAt }
        : null;

    return {
      thread: {
        threadId,
        projectId: sb.hubProjectId,
        title: String(rawThread.title ?? "Untitled thread"),
        link: this.threadLink(threadId),
        createdBy: validActor(rawThread.createdBy, "user"),
        creationSource: validCreationSource(rawThread.creationSource, "web"),
        status,
        latestRunId,
        activeRunId: pausedMidRun ? null : activeRun ? String(activeRun.id ?? activeRun.runId ?? "") || null : null,
        providerInstanceId: threadProviderId,
        model: threadModel,
        runtimeMode: isRuntimeMode(rawThread.runtimeMode) ? rawThread.runtimeMode : "full-access",
        interactionMode: isInteractionMode(rawThread.interactionMode) ? rawThread.interactionMode : "default",
        linkedPullRequest,
        pullRequests,
        pullRequestCount: pullRequests.length,
        openPullRequestCount,
        titleRegeneration,
        branch: typeof rawThread.branch === "string" ? rawThread.branch : null,
        worktreePath: typeof rawThread.worktreePath === "string" ? rawThread.worktreePath : null,
        parentThreadId,
        relationshipToParent,
        ...(delegatedFrom ? { delegatedFrom } : {}),
        runCount: rawRuns.length,
        itemCount: fullTimeline.length,
        pendingRequestCount: pendingRequests.length,
        archived: rawThread.archivedAt != null,
        settled,
        settledAt,
        snoozed,
        snoozedUntil: snoozed ? snoozedUntil : null,
        pinned: rawThread.pinnedAt != null,
        blockedOnThreadWait: rawThread.blockedOnThreadWait === true,
        pendingBackgroundTasks: Array.isArray(rawThread.pendingBackgroundTasks) ? rawThread.pendingBackgroundTasks : [],
        createdAt: threadCreatedAt,
        updatedAt: threadUpdatedAt,
        sandbox: this.sandboxMcpView(sb),
      },
      items: page,
      recentRuns,
      nextPosition,
      hasMore,
      earlierPosition,
    };
  }

  private async toolThreadLaunch(args: Record<string, unknown>, caller: HubMcpCaller): Promise<Record<string, unknown>> {
    this.assertCanWrite(caller);
    const projectId = requireString(args.projectId, "projectId");
    const clientRequestId =
      typeof args.clientRequestId === "string" && args.clientRequestId.trim() ? args.clientRequestId.trim() : undefined;
    const scopedRequestKey = clientRequestId ? `${caller.clientId}:${clientRequestId}` : undefined;

    // 1. Validate requested runtimeMode & interactionMode against caller's ceiling
    let runtimeMode: RuntimeMode = caller.runtimeMode;
    if (args.runtimeMode !== undefined) {
      if (!isRuntimeMode(args.runtimeMode)) {
        throw new HubMcpToolError("invalid_request", `Invalid runtimeMode "${String(args.runtimeMode)}".`);
      }
      if (accessRank(args.runtimeMode) > accessRank(caller.runtimeMode)) {
        throw new HubMcpToolError(
          "runtime_mode_escalation_denied",
          `Requested runtimeMode "${args.runtimeMode}" exceeds this caller's "${caller.runtimeMode}" limit.`,
        );
      }
      runtimeMode = args.runtimeMode;
    }

    let interactionMode: ProviderInteractionMode = caller.interactionMode;
    if (args.interactionMode !== undefined) {
      if (!isInteractionMode(args.interactionMode)) {
        throw new HubMcpToolError("invalid_request", `Invalid interactionMode "${String(args.interactionMode)}".`);
      }
      if (args.interactionMode === "default" && caller.interactionMode === "plan") {
        throw new HubMcpToolError(
          "interaction_mode_escalation_denied",
          `Requested interactionMode "default" exceeds this caller's "plan" limit.`,
        );
      }
      interactionMode = args.interactionMode;
    }

    // 2. Idempotency check by (caller, clientRequestId) — never spawn duplicate VMs on retry!
    if (scopedRequestKey) {
      const existingThreadId = this.launchByRequestId.get(scopedRequestKey);
      if (existingThreadId) {
        const pending = this.pendingLaunches.get(existingThreadId);
        if (pending) {
          return {
            threadId: existingThreadId,
            projectId: pending.projectId,
            title: pending.title,
            modelSelection: pending.modelSelection,
            runId: pending.launchedRunId ?? null,
            status:
              pending.phase === "error"
                ? "failed"
                : pending.phase === "running"
                  ? "starting"
                  : pending.message
                    ? "preparing"
                    : null,
            link: this.threadLink(existingThreadId),
            sandbox: {
              sandboxId: pending.sandboxId,
              phase: pending.phase,
              step: pending.step,
              error: pending.error,
            },
          };
        }
        const existingShell = this.bridge.getThreadShell(existingThreadId);
        const existingSb = this.bridge.getSandboxForThread(existingThreadId);
        if (existingShell || existingSb) {
          const existingSel =
            normalizeModelSelection(existingShell?.modelSelection) ??
            normalizeModelSelection(args.modelSelection) ??
            this.defaultModelSelectionForProject(existingSb?.project ?? "");
          const shellStatus = existingShell ? normalizeThreadStatus(existingShell) : "starting";
          return {
            threadId: existingThreadId,
            projectId: existingSb?.hubProjectId ?? projectId,
            title: String(existingShell?.title ?? args.title ?? "Thread"),
            modelSelection: existingSel,
            runId: typeof existingShell?.latestRunId === "string" ? existingShell.latestRunId : null,
            status: shellStatus === "idle" ? null : shellStatus,
            link: this.threadLink(existingThreadId),
            sandbox: existingSb
              ? { sandboxId: existingSb.sandboxId, phase: existingSb.phase, step: "Running", error: null }
              : null,
          };
        }
      }
    }

    // 3. Validate projectId
    const projectKey = this.bridge.projectKeyFromHubId(projectId);
    if (!projectKey) {
      throw new HubMcpToolError("project_not_found", `Project "${projectId}" was not found in Sentinel Hub.`);
    }

    // 4. Validate capacity up front before returning
    const maxSandboxes = this.config.hub?.maxSandboxes ?? 10;
    const activeCount = this.state.listHubSandboxes().filter(countsTowardHubLimit).length + this.countBootingLaunches();
    if (activeCount >= maxSandboxes) {
      throw new HubMcpToolError(
        "capacity_exceeded",
        `Hub sandbox limit reached (${activeCount}/${maxSandboxes}). Settle or archive an existing thread first.`,
      );
    }

    // 5. Create thread record immediately and kick off async VM boot (< 50ms return!)
    const threadId = randomUUID();
    const message = typeof args.message === "string" && args.message.trim() ? args.message : undefined;
    const title =
      typeof args.title === "string" && args.title.trim()
        ? args.title.trim()
        : message
          ? message.slice(0, 72)
          : `${projectKey} task`;
    const workspaceStrategy =
      args.workspaceStrategy && typeof args.workspaceStrategy === "object"
        ? (args.workspaceStrategy as Record<string, unknown>)
        : undefined;
    const modelSelection =
      normalizeModelSelection(args.modelSelection) ?? this.defaultModelSelectionForProject(projectKey);

    let delegatedFrom: HubDelegatedFrom | undefined;
    if (caller.peerLink && args.delegatedFrom && typeof args.delegatedFrom === "object") {
      const df = args.delegatedFrom as Record<string, unknown>;
      if (
        typeof df.environmentId === "string" &&
        df.environmentId.trim() &&
        typeof df.threadId === "string" &&
        df.threadId.trim() &&
        typeof df.title === "string"
      ) {
        delegatedFrom = {
          environmentId: df.environmentId.trim(),
          threadId: df.threadId.trim(),
          title: df.title,
        };
      }
    }

    const linkOrigin: HubThreadLinkOriginRecord | undefined = caller.peerLink
      ? {
          sessionId: caller.clientId,
          label: caller.label,
          access: caller.access,
          ...(delegatedFrom ? { delegatedFrom } : {}),
        }
      : undefined;

    if (linkOrigin) {
      this.state.updateHub((h) => {
        h.threadLinkOrigins ??= {};
        h.threadLinkOrigins[threadId] = linkOrigin;
      });
    }

    const now = new Date().toISOString();
    const pending: HubPendingLaunch = {
      threadId,
      projectKey,
      projectId,
      title,
      message,
      workspaceStrategy,
      runtimeMode,
      interactionMode,
      modelSelection,
      clientRequestId,
      createdAt: now,
      updatedAt: now,
      sandboxId: null,
      phase: "booting",
      step: `Spawning isolated ${this.bridge.providerName()} VM for ${projectKey}...`,
      error: null,
      queuedMessages: [],
      ...(linkOrigin ? { linkOrigin } : {}),
      ...(delegatedFrom ? { delegatedFrom } : {}),
    };

    this.pendingLaunches.set(threadId, pending);
    if (scopedRequestKey) {
      this.launchByRequestId.set(scopedRequestKey, threadId);
    }

    void this.runAsyncLaunch(pending);

    return {
      threadId,
      projectId,
      title,
      modelSelection,
      runId: null,
      status: message ? "preparing" : null,
      link: this.threadLink(threadId),
      sandbox: {
        sandboxId: null,
        phase: pending.phase,
        step: pending.step,
        error: null,
      },
    };
  }

  private async runAsyncLaunch(pending: HubPendingLaunch): Promise<void> {
    try {
      const sb = await this.bridge.spawnSandboxForThread(pending.projectKey, pending.threadId, (update) => {
        if (pending.cancelled) return;
        if (update.sandboxId !== undefined) pending.sandboxId = update.sandboxId;
        if (update.phase) pending.phase = update.phase;
        pending.step = update.step;
        pending.updatedAt = new Date().toISOString();
      });

      if (pending.cancelled) {
        await this.bridge.destroySandbox(sb.sandboxId, "mcp-launch-cancelled").catch(() => {});
        return;
      }

      pending.sandboxId = sb.sandboxId;
      pending.phase = "starting_t3";
      pending.step = `Launching thread inside VM ${sb.sandboxId}...`;
      pending.updatedAt = new Date().toISOString();

      const strategy = pending.workspaceStrategy ?? { type: "root" };
      const launchPayload: Record<string, unknown> = {
        commandId: randomUUID(),
        creationSource: "mcp",
        threadId: pending.threadId,
        projectId: sb.vmProjectId,
        title: pending.title,
        runtimeMode: pending.runtimeMode,
        interactionMode: pending.interactionMode,
        workspaceStrategy: strategy,
        ...(pending.message ? { initialMessage: { text: pending.message, attachments: [] } } : {}),
        modelSelection: pending.modelSelection,
      };

      let exit = await this.bridge.callVmUnary(sb.sandboxId, "orchestration.launchThread", launchPayload, 120_000);
      if (exit._tag === "Failure" && strategy.type === "worktree" && !strategy.baseRef) {
        exit = await this.bridge.callVmUnary(
          sb.sandboxId,
          "orchestration.launchThread",
          { ...launchPayload, commandId: randomUUID(), workspaceStrategy: { type: "root" } },
          120_000,
        );
      }

      if (exit._tag === "Failure") {
        throw new Error(`orchestration.launchThread failed: ${formatRpcFailure(exit.cause)}`);
      }

      const val = (exit.value ?? {}) as Record<string, unknown>;
      const actualTid = typeof val.threadId === "string" ? val.threadId : pending.threadId;
      this.bridge.bindThreadToSandbox(actualTid, sb.sandboxId);
      const projObj = val.projection as
        | { thread?: { worktreePath?: string | null }; runs?: Array<{ id?: unknown }> }
        | undefined;
      if (projObj?.thread?.worktreePath) {
        this.bridge.bindWorktreeToSandbox(projObj.thread.worktreePath, sb.sandboxId);
      }
      const firstRunId = typeof projObj?.runs?.[0]?.id === "string" ? projObj.runs[0].id : null;
      pending.launchedRunId = firstRunId;

      pending.phase = "running";
      pending.step = "Running";
      pending.updatedAt = new Date().toISOString();

      while (pending.queuedMessages.length > 0 && !pending.cancelled) {
        const next = pending.queuedMessages.shift()!;
        await this.dispatchMessageToVm(
          sb.sandboxId,
          actualTid,
          next.message,
          next.mode,
          next.clientRequestId,
          next.freshContext,
          next.outputSchema,
          next.linkedSender,
        ).catch((err) => {
          console.error(`[hub-mcp] failed to flush queued message for ${actualTid}:`, err);
        });
      }

      if (!pending.cancelled) {
        this.pendingLaunches.delete(pending.threadId);
      }
      this.notifyThreadChanged(actualTid);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.error(`[hub-mcp] async launch failed for thread=${pending.threadId}:`, msg);
      pending.phase = "error";
      pending.step = "VM boot failed";
      pending.error = msg;
      pending.updatedAt = new Date().toISOString();
      this.notifyThreadChanged(pending.threadId);
    }
  }

  private async dispatchMessageToVm(
    sandboxId: string,
    threadId: string,
    message: string,
    mode: string,
    clientRequestId?: string,
    freshContext?: boolean,
    outputSchema?: Record<string, unknown>,
    linkedSender?: HubDelegatedFrom,
  ): Promise<{ messageId: string; runId: string; status: string; delivery: "started" | "queued" | "steered" | "restarted" }> {
    const messageId = clientRequestId
      ? `msg_${createHash("sha256").update(`${threadId}:${clientRequestId}`).digest("hex").slice(0, 32)}`
      : `msg_${randomUUID().replace(/-/g, "")}`;
    const deliveryIntent = mode === "steer" || mode === "restart" ? mode : "auto";
    const dispatchTimeoutMs = this.bridge.dispatchTimeoutMs?.() ?? DEFAULT_DISPATCH_TIMEOUT_MS;
    const checkTimeoutMs = Math.min(DISPATCH_CHECK_TIMEOUT_MS, Math.max(1_000, dispatchTimeoutMs));

    // Inspect current active run so we send the exact `dispatchMode` T3 Code requires.
    let activeRunId: string | null = null;
    const preShell = this.bridge.getThreadShell(threadId);
    if (typeof preShell?.activeRunId === "string" && preShell.activeRunId) {
      activeRunId = preShell.activeRunId;
    } else if (mode === "steer" || mode === "restart" || mode === "auto") {
      const preProj = await this.bridge.callVmUnary(sandboxId, "orchestration.getThreadProjection", { threadId }, 8_000);
      if (preProj._tag === "Success" && preProj.value && typeof preProj.value === "object") {
        const runs = Array.isArray((preProj.value as Record<string, unknown>).runs)
          ? ((preProj.value as Record<string, unknown>).runs as Array<Record<string, unknown>>)
          : [];
        const active = runs.find((r) => typeof r.status === "string" && ACTIVE_RUN_STATUSES.has(r.status));
        if (typeof active?.id === "string") activeRunId = active.id;
      }
    }

    if (mode === "steer" && !activeRunId) {
      throw new HubMcpToolError(
        "thread_not_sendable",
        `Thread ${threadId} has no active run to steer. Send with mode: "auto" or "queue" to start a new turn there.`,
      );
    }

    const canSteer = Boolean(activeRunId) && !freshContext && !outputSchema;
    let dispatchMode: Record<string, unknown>;
    let delivery: "started" | "queued" | "steered" | "restarted";
    if (mode === "steer") {
      dispatchMode = { type: "steer_active", targetRunId: activeRunId! };
      delivery = "steered";
    } else if (mode === "restart" && activeRunId) {
      dispatchMode = { type: "restart_active", targetRunId: activeRunId };
      delivery = "restarted";
    } else if (mode === "queue" && activeRunId) {
      dispatchMode = { type: "queue_after_active" };
      delivery = "queued";
    } else if (mode === "auto" && canSteer) {
      dispatchMode = { type: "steer_active", targetRunId: activeRunId! };
      delivery = "steered";
    } else if (activeRunId) {
      dispatchMode = { type: "queue_after_active" };
      delivery = "queued";
    } else {
      dispatchMode = { type: "start_immediately" };
      delivery = "started";
    }

    const makeCmd = (cmdId: string, dm: Record<string, unknown>): Record<string, unknown> => ({
      type: "message.dispatch",
      commandId: cmdId,
      threadId,
      messageId,
      text: message,
      attachments: [],
      deliveryIntent,
      dispatchMode: dm,
      createdBy: "agent",
      creationSource: "mcp",
      ...(freshContext ? { freshContext: true } : {}),
      ...(outputSchema ? { outputSchema } : {}),
      ...(linkedSender ? { linkedSender } : {}),
    });

    const extractResultFromProjection = (
      proj: Record<string, unknown>,
      curDelivery: "started" | "queued" | "steered" | "restarted",
    ): { runId: string; status: string; delivery: "started" | "queued" | "steered" | "restarted" } => {
      let runId = curDelivery === "steered" && activeRunId ? activeRunId : `run_${messageId}`;
      let status = curDelivery === "queued" ? "queued" : "starting";
      let resolvedDelivery = curDelivery;
      const runs = Array.isArray(proj.runs) ? (proj.runs as Array<Record<string, unknown>>) : [];
      const turnItems = Array.isArray(proj.turnItems) ? (proj.turnItems as Array<Record<string, unknown>>) : [];
      const matchedItem = turnItems.find((item) => item.messageId === messageId);
      const matched =
        runs.find((r) => r.userMessageId === messageId) ??
        (typeof matchedItem?.runId === "string" ? runs.find((r) => r.id === matchedItem.runId) : undefined) ??
        (curDelivery === "steered" && activeRunId ? runs.find((r) => r.id === activeRunId) : undefined) ??
        runs[runs.length - 1];
      if (matched && typeof matched.id === "string" && matched.id) {
        runId = matched.id;
        if (typeof matched.status === "string" && VALID_THREAD_STATUSES.has(matched.status) && matched.status !== "idle") {
          status = matched.status;
          if (status === "queued" && resolvedDelivery === "started") resolvedDelivery = "queued";
        }
      }
      return { runId, status, delivery: resolvedDelivery };
    };

    const checkMessageLanded = async (
      curDelivery: "started" | "queued" | "steered" | "restarted",
    ): Promise<{ runId: string; status: string; delivery: "started" | "queued" | "steered" | "restarted" } | null> => {
      const projExit = await this.bridge.callVmUnary(sandboxId, "orchestration.getThreadProjection", { threadId }, checkTimeoutMs);
      if (projExit._tag !== "Success" || !projExit.value || typeof projExit.value !== "object") return null;
      const proj = projExit.value as Record<string, unknown>;
      const messages = Array.isArray(proj.messages) ? (proj.messages as Array<Record<string, unknown>>) : [];
      const turnItems = Array.isArray(proj.turnItems) ? (proj.turnItems as Array<Record<string, unknown>>) : [];
      const runs = Array.isArray(proj.runs) ? (proj.runs as Array<Record<string, unknown>>) : [];
      const landed =
        messages.some((m) => m.id === messageId) ||
        turnItems.some((item) => item.messageId === messageId) ||
        runs.some((r) => r.userMessageId === messageId);
      if (!landed) return null;
      return extractResultFromProjection(proj, curDelivery);
    };

    type DispatchOutcome =
      | { ok: true; landed: { runId: string; status: string; delivery: "started" | "queued" | "steered" | "restarted" } | null }
      | { ok: false; transportFailure: boolean; cause: unknown };

    // On a transport failure (timeout/disconnect), the VM may still have committed the message (e.g.
    // slow SQLite COMMIT under load). Check the thread projection before retrying, and retry with the
    // exact same commandId so T3 replays its receipt without creating a duplicate message.
    const sendWithSafeRetry = async (
      cmdId: string,
      dm: Record<string, unknown>,
      curDelivery: "started" | "queued" | "steered" | "restarted",
    ): Promise<DispatchOutcome> => {
      const cmd = makeCmd(cmdId, dm);
      const first = await this.bridge.callVmUnary(sandboxId, "orchestration.dispatchCommand", cmd, dispatchTimeoutMs);
      if (first._tag === "Success") return { ok: true, landed: null };
      if (!isRpcTransportFailure(first.cause)) {
        return { ok: false, transportFailure: false, cause: first.cause };
      }

      const landedAfterFirst = await checkMessageLanded(curDelivery);
      if (landedAfterFirst) {
        return { ok: true, landed: landedAfterFirst };
      }

      const retry = await this.bridge.callVmUnary(sandboxId, "orchestration.dispatchCommand", cmd, dispatchTimeoutMs);
      if (retry._tag === "Success") return { ok: true, landed: null };
      if (!isRpcTransportFailure(retry.cause)) {
        return { ok: false, transportFailure: false, cause: retry.cause };
      }

      const landedAfterRetry = await checkMessageLanded(curDelivery);
      if (landedAfterRetry) {
        return { ok: true, landed: landedAfterRetry };
      }
      return { ok: false, transportFailure: true, cause: retry.cause };
    };

    const baseCmdId = clientRequestId ? `cmd_${clientRequestId}` : randomUUID();
    let outcome = await sendWithSafeRetry(baseCmdId, dispatchMode, delivery);
    // In `auto` mode, only fall back from `steer_active` to `queue_after_active` when the VM
    // explicitly rejected steering (e.g. run finished or provider can't steer). Never switch to a
    // different commandId after a timeout/transport failure, or a slow commit would deliver twice.
    if (!outcome.ok && !outcome.transportFailure && mode === "auto" && dispatchMode.type === "steer_active") {
      const alreadyLanded = await checkMessageLanded("steered");
      if (alreadyLanded) {
        outcome = { ok: true, landed: alreadyLanded };
      } else {
        delivery = "queued";
        outcome = await sendWithSafeRetry(`${baseCmdId}_q`, { type: "queue_after_active" }, delivery);
      }
    }
    if (!outcome.ok) {
      throw new HubMcpToolError("orchestration_error", `Failed to send message: ${formatRpcFailure(outcome.cause)}`);
    }

    // Read back the run id created/targeted by this message so RemoteDelegation.followUp tracks the exact run.
    let runId: string;
    let status: string;
    if (outcome.landed) {
      runId = outcome.landed.runId;
      status = outcome.landed.status;
      delivery = outcome.landed.delivery;
    } else {
      runId = delivery === "steered" && activeRunId ? activeRunId : `run_${messageId}`;
      status = delivery === "queued" ? "queued" : "starting";
      const postProj = await this.bridge.callVmUnary(sandboxId, "orchestration.getThreadProjection", { threadId }, 6_000);
      if (postProj._tag === "Success" && postProj.value && typeof postProj.value === "object") {
        const extracted = extractResultFromProjection(postProj.value as Record<string, unknown>, delivery);
        runId = extracted.runId;
        status = extracted.status;
        delivery = extracted.delivery;
      }
    }

    this.notifyThreadChanged(threadId);
    return { messageId, runId, status, delivery };
  }

  private async toolThreadSend(args: Record<string, unknown>, caller: HubMcpCaller): Promise<Record<string, unknown>> {
    this.assertCanWrite(caller);
    const threadId = requireString(args.threadId, "threadId");
    const message = requireString(args.message, "message");
    const mode = typeof args.mode === "string" && args.mode.trim() ? args.mode.trim() : "auto";
    const freshContext = args.freshContext === true;
    const outputSchema =
      args.outputSchema && typeof args.outputSchema === "object"
        ? (args.outputSchema as Record<string, unknown>)
        : undefined;
    const clientRequestId =
      typeof args.clientRequestId === "string" && args.clientRequestId.trim() ? args.clientRequestId.trim() : undefined;
    const scopedRequestKey = clientRequestId ? `${caller.clientId}:${clientRequestId}` : undefined;

    this.assertSameLink(caller, threadId);
    this.assertTargetWithinLimits(caller, threadId);

    if (scopedRequestKey) {
      const prev = this.sendByRequestId.get(scopedRequestKey);
      if (prev && prev.threadId === threadId) {
        return {
          accepted: true,
          deduplicated: true,
          threadId,
          messageId: prev.messageId,
          runId: prev.runId,
          status: prev.status,
          delivery: prev.delivery,
          link: this.threadLink(threadId),
        };
      }
    }

    const pending = this.pendingLaunches.get(threadId);
    if (pending) {
      if (pending.phase === "error") {
        throw new HubMcpToolError("orchestration_error", pending.error ?? "Sandbox failed to boot for this thread.");
      }
      if (pending.phase !== "running") {
        pending.queuedMessages.push({
          message,
          mode,
          clientRequestId,
          freshContext,
          outputSchema,
          linkedSender: caller.senderThread,
        });
        const queuedId = `queued_${randomUUID().replace(/-/g, "")}`;
        const queuedRunId = `run_queued_${queuedId}`;
        if (scopedRequestKey) {
          this.sendByRequestId.set(scopedRequestKey, {
            threadId,
            messageId: queuedId,
            runId: queuedRunId,
            status: "queued",
            delivery: "queued",
          });
        }
        return {
          accepted: true,
          queued: true,
          threadId,
          messageId: queuedId,
          runId: queuedRunId,
          status: "queued",
          delivery: "queued",
          link: this.threadLink(threadId),
        };
      }
    }

    let sb = this.bridge.getSandboxForThread(threadId);
    if (!sb) {
      const destroyed = this.destroyedThreads.get(threadId);
      if (destroyed) {
        throw new HubMcpToolError(
          "sandbox_destroyed",
          `Sandbox for thread "${threadId}" was already destroyed (${destroyed.reason}).`,
        );
      }
      throw new HubMcpToolError("thread_not_found", `Thread "${threadId}" was not found.`);
    }

    if (sb.phase === "sleeping") {
      sb = await this.bridge.wakeSandbox(sb.sandboxId);
    }
    if (sb.phase !== "running") {
      throw new HubMcpToolError("sandbox_destroyed", `Sandbox "${sb.sandboxId}" is ${sb.phase}.`);
    }

    const sent = await this.dispatchMessageToVm(
      sb.sandboxId,
      threadId,
      message,
      mode,
      clientRequestId,
      freshContext,
      outputSchema,
      caller.senderThread,
    );
    if (scopedRequestKey) {
      this.sendByRequestId.set(scopedRequestKey, {
        threadId,
        messageId: sent.messageId,
        runId: sent.runId,
        status: sent.status,
        delivery: sent.delivery,
      });
    }
    return {
      accepted: true,
      threadId,
      messageId: sent.messageId,
      runId: sent.runId,
      status: sent.status,
      delivery: sent.delivery,
      link: this.threadLink(threadId),
    };
  }

  private async toolThreadWait(args: Record<string, unknown>): Promise<Record<string, unknown>> {
    const threadId = requireString(args.threadId, "threadId");
    const requestedRunId = typeof args.runId === "string" && args.runId.trim() ? args.runId.trim() : null;
    const rawTimeout = typeof args.timeoutMs === "number" && Number.isFinite(args.timeoutMs) ? args.timeoutMs : DEFAULT_WAIT_TIMEOUT_MS;
    const timeoutMs = Math.min(MAX_WAIT_TIMEOUT_MS, Math.max(1, Math.floor(rawTimeout)));
    const untilDelegated = args.untilDelegatedWorkSettled === true;
    const untilOutput = args.untilOutput === true;

    const deadline = Date.now() + timeoutMs;
    const startedAt = Date.now();
    let lastStatus = "preparing";
    let lastRunId: string | null = requestedRunId;

    while (true) {
      const pending = this.pendingLaunches.get(threadId);
      const sb = this.bridge.getSandboxForThread(threadId);
      const upsizing = sb ? this.bridge.getUpsizeProgress?.(sb.sandboxId) : undefined;

      // 1. Thread is still booting its Boat VM
      if (pending && (!sb || sb.phase === "provisioning" || pending.phase !== "running")) {
        if (pending.phase === "error") {
          const failedRunId = `run_hub_launch_${threadId}`;
          return {
            threadId,
            runId: failedRunId,
            status: "failed",
            timedOut: false,
            ...(untilDelegated ? { resultRun: { runId: failedRunId, status: "failed" } } : {}),
          };
        }
        lastStatus = "preparing";
        lastRunId = pending.launchedRunId ?? null;
      } else if (sb && upsizing) {
        // An in-place resize pauses the VM, resumes it, then dispatches its auto-continue. Both the
        // sleeping window and the short post-wake window are preparation, never an interrupted run.
        lastStatus = "preparing";
        if (!lastRunId) {
          const shell = this.bridge.getThreadShell(threadId);
          lastRunId = typeof shell?.latestRunId === "string" && shell.latestRunId ? shell.latestRunId : null;
        }
      } else if (sb && sb.phase === "running") {
        // 2. Live running VM: inspect projection
        const res = await this.bridge.callVmUnary(sb.sandboxId, "orchestration.getThreadProjection", { threadId }, 10_000);
        if (res._tag === "Success" && res.value && typeof res.value === "object") {
          const evaluated = evaluateProjectionWait(
            res.value as Record<string, unknown>,
            threadId,
            requestedRunId,
            untilDelegated,
            untilOutput,
          );
          if (evaluated.error) {
            throw new HubMcpToolError(evaluated.error.code, evaluated.error.message);
          }
          if (evaluated.done) {
            return evaluated.result!;
          }
          lastStatus = evaluated.status;
          lastRunId = evaluated.runId;
        }
      } else if (sb && sb.phase === "sleeping") {
        // 3. Sleeping / parked VM: never wake just to wait; read cached projection or shell
        const cached = this.bridge.getCachedProjection?.(threadId);
        if (cached) {
          const evaluated = evaluateProjectionWait(cached, threadId, requestedRunId, untilDelegated, untilOutput);
          if (evaluated.error) {
            throw new HubMcpToolError(evaluated.error.code, evaluated.error.message);
          }
          if (evaluated.done) return evaluated.result!;
          // A VM that went to sleep mid-run has no live process left; report interrupted so callers don't hang.
          const interruptedRunId = evaluated.runId;
          return {
            threadId,
            runId: interruptedRunId,
            status: interruptedRunId ? "interrupted" : "idle",
            timedOut: false,
            ...(untilDelegated && interruptedRunId
              ? { resultRun: { runId: interruptedRunId, status: "interrupted" } }
              : {}),
          };
        }
        const shell = this.bridge.getThreadShell(threadId);
        const shellRunId = typeof shell?.latestRunId === "string" ? shell.latestRunId : null;
        const shellStatus = shell ? normalizeThreadStatus(shell) : "idle";
        const finalStatus = TERMINAL_RUN_STATUSES.has(shellStatus) || shellStatus === "idle" ? shellStatus : "interrupted";
        return {
          threadId,
          runId: shellRunId,
          status: finalStatus,
          timedOut: false,
          ...(untilDelegated && shellRunId && finalStatus !== "idle"
            ? { resultRun: { runId: shellRunId, status: finalStatus } }
            : {}),
        };
      } else {
        // 4. Destroyed or missing thread
        throw new HubMcpToolError("thread_not_found", `Thread "${threadId}" was not found.`);
      }

      const remaining = deadline - Date.now();
      if (remaining <= 0) {
        return {
          threadId,
          runId: lastRunId,
          status: lastStatus,
          timedOut: true,
        };
      }

      const pollDelay = Date.now() - startedAt < 5_000 ? 350 : 1_000;
      await this.waitForThreadSignal(threadId, Math.min(remaining, pollDelay));
    }
  }

  private waitForThreadSignal(threadId: string, ms: number): Promise<void> {
    if (ms <= 0) return Promise.resolve();
    return new Promise((resolve) => {
      let set = this.threadWaiters.get(threadId);
      if (!set) {
        set = new Set();
        this.threadWaiters.set(threadId, set);
      }
      const done = () => {
        clearTimeout(timer);
        set?.delete(done);
        if (set && set.size === 0) this.threadWaiters.delete(threadId);
        resolve();
      };
      set.add(done);
      const timer = setTimeout(done, ms);
    });
  }

  private async toolThreadInterrupt(
    args: Record<string, unknown>,
    caller: HubMcpCaller,
  ): Promise<Record<string, unknown>> {
    this.assertCanWrite(caller);
    const threadId = requireString(args.threadId, "threadId");
    const requestedRunId = typeof args.runId === "string" && args.runId.trim() ? args.runId.trim() : null;
    const stop = args.stop === true;
    const rawReason = typeof args.reason === "string" && args.reason.trim() ? args.reason.trim() : undefined;
    const reason = rawReason ? `Stopped by an agent: ${rawReason}` : "Stopped by an agent";
    const clientRequestId =
      typeof args.clientRequestId === "string" && args.clientRequestId.trim() ? args.clientRequestId.trim() : undefined;
    const scopedRequestKey = clientRequestId ? `${caller.clientId}:${clientRequestId}` : undefined;

    this.assertSameLink(caller, threadId);
    this.assertTargetWithinLimits(caller, threadId);

    if (scopedRequestKey) {
      const prev = this.interruptByRequestId.get(scopedRequestKey);
      if (prev && prev.threadId === threadId) return prev;
    }

    const remember = (res: { threadId: string; runId: string | null; status: string }) => {
      if (scopedRequestKey) this.interruptByRequestId.set(scopedRequestKey, res);
      return res;
    };

    // 1. Cancel a booting VM launch cleanly
    const pending = this.pendingLaunches.get(threadId);
    if (pending && pending.phase !== "running") {
      pending.cancelled = true;
      pending.phase = "destroying";
      this.pendingLaunches.delete(threadId);
      this.destroyedThreads.set(threadId, { destroyedAt: Date.now(), reason: "mcp-interrupt" });
      if (pending.sandboxId) {
        await this.bridge.destroySandbox(pending.sandboxId, "mcp-interrupt").catch(() => {});
      }
      this.notifyThreadChanged(threadId);
      return remember({ threadId, runId: pending.launchedRunId ?? null, status: "interrupt_requested" });
    }

    const sb = this.bridge.getSandboxForThread(threadId);
    if (!sb) {
      throw new HubMcpToolError("thread_not_found", `Thread "${threadId}" was not found.`);
    }

    if (sb.phase !== "running") {
      return remember({ threadId, runId: null, status: "no_active_run" });
    }

    // Read projection to know which run is active (or already terminal)
    const projRes = await this.bridge.callVmUnary(sb.sandboxId, "orchestration.getThreadProjection", { threadId }, 10_000);
    const runs =
      projRes._tag === "Success" && projRes.value && typeof projRes.value === "object"
        ? Array.isArray((projRes.value as Record<string, unknown>).runs)
          ? ((projRes.value as Record<string, unknown>).runs as Array<Record<string, unknown>>)
          : []
        : [];
    const sortedRunsDesc = [...runs].sort((a, b) => {
      const ordA = typeof a.ordinal === "number" ? a.ordinal : 0;
      const ordB = typeof b.ordinal === "number" ? b.ordinal : 0;
      return ordB - ordA;
    });
    const activeRun = sortedRunsDesc.find((r) => typeof r.status === "string" && ACTIVE_RUN_STATUSES.has(r.status));
    const activeRunId = typeof activeRun?.id === "string" ? activeRun.id : null;
    const commandId = clientRequestId ? `cmd_int_${clientRequestId}` : randomUUID();

    if (stop) {
      let exit = await this.bridge.callVmUnary(
        sb.sandboxId,
        "orchestration.dispatchCommand",
        { type: "thread.stop", commandId, threadId, reason },
        12_000,
      );
      // Fallback for older VMs that do not support `thread.stop`
      if (exit._tag === "Failure" && activeRunId) {
        exit = await this.bridge.callVmUnary(
          sb.sandboxId,
          "orchestration.dispatchCommand",
          { type: "run.interrupt", commandId: `${commandId}_fb`, threadId, runId: activeRunId, reason, holdQueue: true },
          12_000,
        );
      }
      if (exit._tag === "Failure" && activeRunId) {
        throw new HubMcpToolError("orchestration_error", `Unable to stop thread ${threadId}: ${formatRpcFailure(exit.cause)}`);
      }
      this.notifyThreadChanged(threadId);
      return remember({
        threadId,
        runId: activeRunId,
        status: activeRunId ? "interrupt_requested" : "no_active_run",
      });
    }

    const targetRun = requestedRunId
      ? sortedRunsDesc.find((r) => r.id === requestedRunId)
      : activeRun;

    if (!targetRun) {
      if (requestedRunId) {
        throw new HubMcpToolError("run_not_found", `Run "${requestedRunId}" was not found on thread "${threadId}".`);
      }
      return remember({ threadId, runId: null, status: "no_active_run" });
    }

    const targetRunId = String(targetRun.id);
    const targetStatus = String(targetRun.status ?? "");
    if (TERMINAL_RUN_STATUSES.has(targetStatus)) {
      return remember({ threadId, runId: targetRunId, status: targetStatus });
    }

    const exit = await this.bridge.callVmUnary(
      sb.sandboxId,
      "orchestration.dispatchCommand",
      { type: "run.interrupt", commandId, threadId, runId: targetRunId, reason },
      12_000,
    );
    if (exit._tag === "Failure") {
      throw new HubMcpToolError("orchestration_error", `Unable to interrupt thread ${threadId}: ${formatRpcFailure(exit.cause)}`);
    }
    this.notifyThreadChanged(threadId);
    return remember({ threadId, runId: targetRunId, status: "interrupt_requested" });
  }

  /** Each call is logged once (client, action, chat, VM, outcome): a settle may park a whole VM. */
  private async toolThreadOrganize(args: Record<string, unknown>, caller: HubMcpCaller): Promise<Record<string, unknown>> {
    const log = { action: String(args.action ?? ""), threadId: String(args.threadId ?? ""), sandboxId: "-", outcome: "error" };
    try {
      const result = await this.organizeThread(args, caller, log);
      if (log.outcome === "error") log.outcome = "ok";
      return result;
    } catch (err) {
      log.outcome = `error: ${err instanceof Error ? err.message : String(err)}`;
      throw err;
    } finally {
      console.log(
        `[hub-mcp] t3_thread_organize by ${JSON.stringify(caller.label)}: action=${log.action} thread=${log.threadId} sandbox=${log.sandboxId} -> ${log.outcome}`,
      );
    }
  }

  private async organizeThread(
    args: Record<string, unknown>,
    caller: HubMcpCaller,
    log: { sandboxId: string; outcome: string },
  ): Promise<Record<string, unknown>> {
    this.assertCanWrite(caller);
    const threadId = requireString(args.threadId, "threadId");
    const action = requireString(args.action, "action");
    const isDestructiveCleanup = action === "settle" || action === "archive";

    this.assertSameLink(caller, threadId);
    this.assertTargetWithinLimits(caller, threadId);

    const pending = this.pendingLaunches.get(threadId);
    if (pending && isDestructiveCleanup) {
      pending.cancelled = true;
      pending.phase = "destroying";
      this.pendingLaunches.delete(threadId);
      this.destroyedThreads.set(threadId, { destroyedAt: Date.now(), reason: `mcp-${action}` });
      if (pending.sandboxId) {
        log.sandboxId = pending.sandboxId;
        await this.bridge.destroySandbox(pending.sandboxId, `mcp-${action}`).catch(() => {});
      }
      this.notifyThreadChanged(threadId);
      log.outcome = "booting chat cancelled, VM destroyed";
      return { sequence: this.nextSequence(), ok: true, threadId, action, sandboxDestroyed: true };
    }

    const sb = this.bridge.getSandboxForThread(threadId);
    if (!sb) {
      if (this.destroyedThreads.has(threadId) && isDestructiveCleanup) {
        log.outcome = "VM already destroyed";
        return { sequence: this.nextSequence(), ok: true, threadId, action, sandboxDestroyed: true, alreadyDestroyed: true };
      }
      throw new HubMcpToolError("thread_not_found", `Thread "${threadId}" was not found.`);
    }
    log.sandboxId = sb.sandboxId;

    const commandId = randomUUID();
    let command: Record<string, unknown>;
    switch (action) {
      case "snooze": {
        const snoozedUntil = requireString(args.snoozedUntil, "snoozedUntil");
        command = { type: "thread.snooze", commandId, threadId, snoozedUntil };
        break;
      }
      case "unsnooze":
      case "unsettle":
        command = { type: `thread.${action}`, commandId, threadId, reason: "user" };
        break;
      case "mark_unread":
        command = { type: "thread.mark-unread", commandId, threadId };
        break;
      case "settle":
      case "archive":
      case "unarchive":
      case "pin":
      case "unpin":
        command = { type: `thread.${action}`, commandId, threadId };
        break;
      default:
        throw new HubMcpToolError("invalid_request", `Unsupported organize action "${action}".`);
    }

    let listedBefore: Array<Record<string, unknown>> | null = null;
    if (isDestructiveCleanup && sb.phase === "running") listedBefore = await this.bridge.readVmThreads(sb.sandboxId);

    let refused: string | null = null;
    if (sb.phase === "running") {
      const exit = await this.bridge.callVmUnary(sb.sandboxId, "orchestration.dispatchCommand", command, 10_000);
      if (exit._tag === "Failure") {
        if (!isDestructiveCleanup) {
          throw new HubMcpToolError("orchestration_error", `Organize failed: ${formatRpcFailure(exit.cause)}`);
        }
        refused = formatRpcFailure(exit.cause);
      }
    }

    let sandboxDestroyed = false;
    const parksOnSettle = this.config.boat.settleAction === "park" && this.config.boat.deleteWhen === "archived-or-settled";
    if (action === "settle" && parksOnSettle) {
      let sandboxParked = false;
      if (refused === null) {
        sandboxParked = await this.bridge.parkSandbox(sb.sandboxId, {
          trigger: `MCP t3_thread_organize settle of ${threadId} by ${JSON.stringify(caller.label)}`,
          settledThreadId: threadId,
        });
      }
      log.outcome = refused !== null ? `the VM refused the settle (${refused}); not parked` : sandboxParked ? "VM parked" : "settled; VM not parked";
      return { sequence: this.nextSequence(), ok: true, threadId, action, sandboxDestroyed, sandboxParked };
    }
    if (isDestructiveCleanup) {
      const trigger = `MCP t3_thread_organize ${action} of ${threadId} by ${JSON.stringify(caller.label)}`;
      const keep =
        sb.phase !== "running"
          ? "its VM is not running, so the command was not applied"
          : await this.vmCloseRefusal(sb.sandboxId, threadId, listedBefore, refused);
      if (keep !== null) {
        console.log(`[hub] not ${this.config.boat.archiveAction === "stop" ? "stopping" : "destroying"} ${sb.sandboxId}: ${keep} — trigger: ${trigger}`);
        log.outcome = refused !== null ? `the VM refused the ${action} (${refused}); VM kept` : `VM kept: ${keep}`;
        if (sb.phase !== "running") {
          throw new HubMcpToolError(
            "sandbox_not_running",
            `This chat's VM is ${sb.phase}, so the ${action} was not applied and the VM was kept. Close the chat from the T3 app, or wake it (send it a message) and try again.`,
          );
        }
      } else if (this.config.boat.archiveAction === "stop") {
        console.log(`[hub] stopping ${sb.sandboxId} (${sb.project}): no open or running chat left — trigger: ${trigger}`);
        await this.bridge.sleepSandbox(sb.sandboxId, "Paused via MCP");
        log.outcome = "VM stopped";
      } else {
        console.log(`[hub] destroying ${sb.sandboxId} (${sb.project}): no open or running chat left — trigger: ${trigger}`);
        await this.bridge.destroySandbox(sb.sandboxId, `mcp-${action}`);
        sandboxDestroyed = true;
        log.outcome = "VM destroyed";
      }
    }

    return { sequence: this.nextSequence(), ok: true, threadId, action, sandboxDestroyed };
  }

  private async vmCloseRefusal(
    sandboxId: string,
    threadId: string,
    listedBefore: Array<Record<string, unknown>> | null,
    refused: string | null,
  ): Promise<string | null> {
    if (refused !== null) return "the VM refused the command";
    if (!listedBefore) return "could not read its chats";
    const closedThreadRow = listedBefore.find((row) => row.id === threadId);
    if (!closedThreadRow) return `chat ${threadId} is not one of the chats it lists`;
    if (isChildThreadRow(closedThreadRow)) return `chat ${threadId} is a delegated chat`;
    const rows = await this.bridge.readVmThreads(sandboxId);
    if (!rows) return "could not read its chats";
    const blocker = vmCloseBlocker(rows, { settledIsClosed: this.config.boat.deleteWhen === "archived-or-settled" });
    return blocker ? `chat ${blocker.threadId} is ${blocker.reason}` : null;
  }

  private async toolListThreadPullRequests(args: Record<string, unknown>): Promise<Record<string, unknown>> {
    const threadId = requireString(args.threadId, "threadId");
    const pending = this.pendingLaunches.get(threadId);
    const sb = this.bridge.getSandboxForThread(threadId);

    if ((!sb || sb.phase === "provisioning") && pending) {
      return { pullRequests: [], chains: [] };
    }
    if (!sb) {
      if (this.destroyedThreads.has(threadId)) {
        throw new HubMcpToolError("sandbox_destroyed", `Sandbox for thread "${threadId}" was destroyed.`);
      }
      throw new HubMcpToolError("thread_not_found", `Thread "${threadId}" was not found.`);
    }

    if (sb.phase === "running") {
      const res = await this.bridge.callVmUnary(sb.sandboxId, "orchestration.getThreadProjection", { threadId }, 10_000);
      if (res._tag === "Success" && res.value && typeof res.value === "object") {
        const proj = res.value as Record<string, unknown>;
        const { pullRequests } = extractPullRequests(sb.hubProjectId, proj, (proj.thread ?? {}) as Record<string, unknown>);
        return { pullRequests, chains: [] };
      }
    }

    const shell = this.bridge.getThreadShell(threadId);
    return {
      pullRequests: shell ? extractPullRequests(sb.hubProjectId, shell).pullRequests : [],
      chains: [],
    };
  }

  private async toolPendingRequest(args: Record<string, unknown>, caller: HubMcpCaller): Promise<Record<string, unknown>> {
    const action = requireString(args.action, "action");
    const threadId = requireString(args.threadId, "threadId");
    if (action === "respond") {
      this.assertCanWrite(caller);
      this.assertSameLink(caller, threadId);
      this.assertTargetWithinLimits(caller, threadId);
    }

    const pending = this.pendingLaunches.get(threadId);
    const sb = this.bridge.getSandboxForThread(threadId);

    if ((!sb || sb.phase === "provisioning") && pending) {
      if (action === "list") return { requestIds: [] };
      throw new HubMcpToolError("invalid_request", `Thread "${threadId}" is still booting; no pending requests yet.`);
    }
    if (!sb) {
      if (this.destroyedThreads.has(threadId)) {
        throw new HubMcpToolError("sandbox_destroyed", `Sandbox for thread "${threadId}" was destroyed.`);
      }
      throw new HubMcpToolError("thread_not_found", `Thread "${threadId}" was not found.`);
    }
    if (sb.phase !== "running") {
      if (action === "list") return { requestIds: [] };
      throw new HubMcpToolError("sandbox_destroyed", `Sandbox "${sb.sandboxId}" is ${sb.phase}.`);
    }

    if (action === "list") {
      const res = await this.bridge.callVmUnary(sb.sandboxId, "orchestration.getThreadProjection", { threadId }, 10_000);
      if (res._tag === "Failure" || !res.value || typeof res.value !== "object") {
        return { requestIds: [] };
      }
      const proj = res.value as Record<string, unknown>;
      const reqs = Array.isArray(proj.runtimeRequests) ? (proj.runtimeRequests as Array<Record<string, unknown>>) : [];
      const requestIds = reqs
        .filter((r) => r && r.kind === "user_input" && r.status === "pending" && typeof r.id === "string")
        .map((r) => String(r.id));
      return { requestIds };
    }

    if (action === "read") {
      const requestId = requireString(args.requestId, "requestId");
      const res = await this.bridge.callVmUnary(sb.sandboxId, "orchestration.getThreadProjection", { threadId }, 10_000);
      if (res._tag === "Failure" || !res.value || typeof res.value !== "object") {
        throw new HubMcpToolError("invalid_request", `Unable to load pending request "${requestId}".`);
      }
      const proj = res.value as Record<string, unknown>;
      const items = Array.isArray(proj.turnItems) ? (proj.turnItems as Array<Record<string, unknown>>) : [];
      const item = items.find((i) => i && i.type === "user_input_request" && i.requestId === requestId);
      if (!item) {
        throw new HubMcpToolError("invalid_request", `Pending request "${requestId}" was not found.`);
      }
      return {
        requestId,
        questions: Array.isArray(item.questions) ? item.questions : [],
      };
    }

    if (action === "respond") {
      const requestId = requireString(args.requestId, "requestId");
      if (!args.answers || typeof args.answers !== "object") {
        throw new HubMcpToolError("invalid_request", "t3_pending_request respond requires `answers`.");
      }
      const exit = await this.bridge.callVmUnary(
        sb.sandboxId,
        "orchestration.dispatchCommand",
        {
          type: "runtime-request.respond",
          commandId: randomUUID(),
          threadId,
          requestId,
          answers: args.answers,
        },
        12_000,
      );
      if (exit._tag === "Failure") {
        throw new HubMcpToolError("orchestration_error", `Respond failed: ${formatRpcFailure(exit.cause)}`);
      }
      this.notifyThreadChanged(threadId);
      return { sequence: this.nextSequence(), ok: true, requestId };
    }

    throw new HubMcpToolError("invalid_request", `Unsupported action "${action}" for t3_pending_request.`);
  }

  private async toolWorktreeList(args: Record<string, unknown>): Promise<Record<string, unknown>> {
    const threadId = typeof args.threadId === "string" ? args.threadId : null;
    const sb = (threadId ? this.bridge.getSandboxForThread(threadId) : null) ?? this.bridge.anyRunningSandbox();
    if (!sb || sb.phase !== "running") {
      return { refs: [], branches: [], nextCursor: null };
    }
    const proj = this.state.getAllProjects(this.config)[sb.project];
    const res = await this.bridge.callVmUnary(
      sb.sandboxId,
      "vcs.listRefs",
      { cwd: proj?.workdir ?? "/home/user/workspace" },
      10_000,
    );
    if (res._tag === "Success" && res.value && typeof res.value === "object") {
      const val = res.value as Record<string, unknown>;
      const branches = Array.isArray(val.branches) ? val.branches : [];
      return { refs: branches, branches, nextCursor: val.nextCursor ?? null };
    }
    return { refs: [], branches: [], nextCursor: null };
  }

  private async toolThreadSearch(args: Record<string, unknown>): Promise<Record<string, unknown>> {
    const query = requireString(args.query, "query").toLowerCase();
    const projectId = typeof args.projectId === "string" && args.projectId.trim() ? args.projectId.trim() : null;
    const limit = typeof args.limit === "number" && args.limit > 0 ? Math.min(Math.floor(args.limit), 50) : 20;

    const list = this.toolThreadList({
      allProjects: !projectId,
      ...(projectId ? { projectId } : {}),
      includeSubagents: false,
      limit: 100,
    });
    const threads = (list.threads as Array<Record<string, unknown>>) ?? [];
    const matches = threads
      .filter((t) => {
        const hay = `${String(t.title ?? "")} ${String(t.threadId ?? "")} ${JSON.stringify(t.pullRequests ?? [])}`.toLowerCase();
        return hay.includes(query);
      })
      .slice(0, limit)
      .map((t) => ({
        threadId: String(t.threadId),
        projectId: String(t.projectId),
        title: String(t.title ?? ""),
        link: typeof t.link === "string" ? t.link : this.threadLink(String(t.threadId)),
        source: "title" as const,
      }));

    return { currentThreadId: null, matches };
  }

  private async toolCiLogs(args: Record<string, unknown>): Promise<Record<string, unknown>> {
    const threadId = typeof args.threadId === "string" ? args.threadId : null;
    const sb = (threadId ? this.bridge.getSandboxForThread(threadId) : null) ?? this.bridge.anyRunningSandbox();
    if (!sb || sb.phase !== "running") {
      return { logs: [], available: false, message: "No running sandbox available to fetch CI logs." };
    }
    const res = await this.bridge.callVmUnary(sb.sandboxId, "pullRequests.getCiLogs", args, 10_000);
    if (res._tag === "Success" && res.value && typeof res.value === "object") {
      return res.value as Record<string, unknown>;
    }
    return { logs: [], available: false, message: "CI logs RPC not available on sandbox." };
  }

  private toolAddProject(args: Record<string, unknown>, caller: HubMcpCaller): Record<string, unknown> {
    this.assertCanWrite(caller);
    this.assertNotFencedLink(caller, "change projects or environment settings");
    const repo = requireString(args.repo, "repo");
    const { name, project } = buildAutoProjectConfig({
      repo,
      name: typeof args.name === "string" ? args.name : undefined,
      workdir: typeof args.workdir === "string" ? args.workdir : undefined,
      size: typeof args.size === "string" ? args.size : undefined,
      klavProfiles: Array.isArray(args.klavProfiles)
        ? args.klavProfiles.filter((p): p is string => typeof p === "string")
        : undefined,
      klavTtl: typeof args.klavTtl === "string" ? args.klavTtl : undefined,
    });

    this.state.addDynamicProject(name, project);
    const projectId = this.bridge.ensureHubProjectId(name);
    this.bridge.notifyProjectAdded(name);

    const shouldBake = args.bake !== false;
    if (shouldBake) {
      this.bridge.startProjectBake(name);
    }

    return {
      ok: true,
      name,
      projectId,
      workdir: project.workdir,
      snapshot: project.snapshot,
      repositoryIdentity: this.buildRepositoryIdentity(name, project),
      baking: shouldBake,
      fallbackReady: true,
    };
  }

  private toolSentinelStatus(): Record<string, unknown> {
    const hub = this.state.ensureHub(
      this.config.hub?.label ?? "Boat",
      Object.keys(this.state.getAllProjects(this.config)),
    );
    const sandboxes = this.state.listHubSandboxes();
    return {
      ok: true,
      environmentId: hub.environmentId,
      label: hub.label,
      provider: this.bridge.providerName(),
      maxSandboxes: this.config.hub?.maxSandboxes ?? 10,
      maxParkedSandboxes: this.config.hub?.maxParkedSandboxes ?? DEFAULT_MAX_PARKED_SANDBOXES,
      parkedTtlHours: this.config.boat.parkedTtlHours ?? DEFAULT_PARKED_TTL_HOURS,
      activeSandboxes: sandboxes.filter(countsTowardHubLimit).length,
      parkedSandboxes: sandboxes.filter((s) => s.phase === "sleeping" && s.parkedAt).length,
      bootingLaunches: this.countBootingLaunches(),
      sandboxes: sandboxes.map((s) => ({
        sandboxId: s.sandboxId,
        project: s.project,
        phase: s.phase,
        threadIds: s.threadIds,
        parkedAt: s.parkedAt ?? null,
        createdAt: s.createdAt,
        lastHeartbeatAt: s.lastHeartbeatAt,
      })),
      projects: this.toolProjectList({}).projects,
    };
  }
}

function evaluateProjectionWait(
  projection: Record<string, unknown>,
  threadId: string,
  requestedRunId: string | null,
  untilDelegated: boolean,
  untilOutput: boolean,
):
  | { done: true; result: Record<string, unknown>; error?: undefined }
  | { done: false; status: string; runId: string | null; error?: undefined }
  | { done: false; error: { code: string; message: string } } {
  const rawThread = (projection.thread ?? {}) as Record<string, unknown>;
  const rawRuns = Array.isArray(projection.runs) ? (projection.runs as Array<Record<string, unknown>>) : [];
  const rawSubagents = Array.isArray(projection.subagents)
    ? (projection.subagents as Array<Record<string, unknown>>)
    : [];
  const rawRequests = Array.isArray(projection.runtimeRequests)
    ? (projection.runtimeRequests as Array<Record<string, unknown>>)
    : [];

  const sortedRunsDesc = [...rawRuns].sort((a, b) => {
    const ordA = typeof a.ordinal === "number" ? a.ordinal : 0;
    const ordB = typeof b.ordinal === "number" ? b.ordinal : 0;
    if (ordA !== ordB) return ordB - ordA;
    return String(b.requestedAt ?? b.startedAt ?? "").localeCompare(String(a.requestedAt ?? a.startedAt ?? ""));
  });

  if (sortedRunsDesc.length === 0) {
    if (requestedRunId) {
      return { done: false, error: { code: "run_not_found", message: `Run "${requestedRunId}" was not found.` } };
    }
    return {
      done: true,
      result: { threadId, runId: null, status: "idle", timedOut: false },
    };
  }

  const targetRun = requestedRunId
    ? sortedRunsDesc.find((r) => r.id === requestedRunId || r.runId === requestedRunId)
    : sortedRunsDesc[0];

  if (!targetRun) {
    return { done: false, error: { code: "run_not_found", message: `Run "${requestedRunId}" was not found.` } };
  }

  const targetRunId = String(targetRun.id ?? targetRun.runId);
  const targetOrdinal = typeof targetRun.ordinal === "number" ? targetRun.ordinal : 1;
  const rawStatus = typeof targetRun.status === "string" && VALID_THREAD_STATUSES.has(targetRun.status)
    ? targetRun.status
    : "running";
  const hasPendingInput = rawRequests.some((r) => r && r.kind === "user_input" && r.status === "pending");
  const effectiveStatus = hasPendingInput && !TERMINAL_RUN_STATUSES.has(rawStatus) ? "waiting" : rawStatus;

  const targetOutput = extractRunOutput(targetRun);
  if (!TERMINAL_RUN_STATUSES.has(rawStatus)) {
    return { done: false, status: effectiveStatus, runId: targetRunId };
  }
  if (untilOutput && targetOutput === undefined) {
    // Check if a later run submitted output or is still active
    const activeLater = sortedRunsDesc.find(
      (r) => (typeof r.ordinal === "number" ? r.ordinal : 0) > targetOrdinal && !TERMINAL_RUN_STATUSES.has(String(r.status)),
    );
    if (activeLater) {
      return { done: false, status: String(activeLater.status), runId: targetRunId };
    }
  }

  if (untilDelegated) {
    const activeLaterRun = sortedRunsDesc.find(
      (r) =>
        (typeof r.ordinal === "number" ? r.ordinal : 0) > targetOrdinal &&
        !TERMINAL_RUN_STATUSES.has(String(r.status)) &&
        !(r.status === "queued" && r.queueHeld === true),
    );
    if (activeLaterRun) {
      return { done: false, status: String(activeLaterRun.status), runId: targetRunId };
    }
    const openSubagent = rawSubagents.some(
      (s) =>
        s &&
        s.origin === "app_owned" &&
        s.result === null &&
        !TERMINAL_RUN_STATUSES.has(String(s.status ?? "")),
    );
    if (openSubagent) {
      return { done: false, status: "waiting", runId: targetRunId };
    }
    const pendingBg = Array.isArray(rawThread.pendingBackgroundTasks)
      ? (rawThread.pendingBackgroundTasks as Array<Record<string, unknown>>).some(
          (t) => t && (t.awaited === true || t.kind === "subagent" || t.kind === "background_task"),
        )
      : false;
    if (pendingBg) {
      return { done: false, status: "waiting", runId: targetRunId };
    }
  }

  const latestTerminal = sortedRunsDesc.find(
    (r) => TERMINAL_RUN_STATUSES.has(String(r.status)) && r.status !== "rolled_back",
  ) ?? targetRun;
  const latestTerminalId = String(latestTerminal.id ?? latestTerminal.runId ?? targetRunId);
  const latestTerminalStatus = String(latestTerminal.status ?? rawStatus);
  const finalOutput = extractRunOutput(latestTerminal) ?? targetOutput;

  return {
    done: true,
    result: {
      threadId,
      runId: targetRunId,
      status: rawStatus,
      timedOut: false,
      ...(untilDelegated ? { resultRun: { runId: latestTerminalId, status: latestTerminalStatus } } : {}),
      ...(finalOutput !== undefined ? { output: finalOutput } : {}),
    },
  };
}

function extractRunOutput(run: Record<string, unknown>): unknown {
  const out = run.output as { value?: unknown } | undefined;
  if (out && typeof out === "object" && "value" in out) return out.value;
  return undefined;
}

function mapOptionDescriptor(raw: Record<string, unknown>): Record<string, unknown> | null {
  if (typeof raw.id !== "string" || !raw.id.trim()) return null;
  if (raw.type === "boolean") {
    const def = typeof raw.defaultValue === "boolean" ? raw.defaultValue : undefined;
    return { id: raw.id, values: [false, true], ...(def !== undefined ? { default: def } : {}) };
  }
  if (raw.type === "select" && Array.isArray(raw.options)) {
    const values = (raw.options as Array<Record<string, unknown>>)
      .map((o) => (typeof o?.id === "string" ? o.id : null))
      .filter((v): v is string => v !== null);
    if (values.length === 0) return null;
    const def =
      typeof raw.defaultValue === "string"
        ? raw.defaultValue
        : typeof raw.defaultOptionId === "string"
          ? raw.defaultOptionId
          : undefined;
    return { id: raw.id, values, ...(def !== undefined ? { default: def } : {}) };
  }
  return null;
}

function validProviderSlug(val: unknown): string | null {
  if (typeof val !== "string") return null;
  const trimmed = val.trim();
  return trimmed && PROVIDER_SLUG_PATTERN.test(trimmed) ? trimmed : null;
}

function normalizeModelSelection(
  raw: unknown,
): { instanceId: string; model: string; options?: unknown[] } | null {
  if (!raw || typeof raw !== "object") return null;
  const obj = raw as Record<string, unknown>;
  const instanceId = validProviderSlug(obj.instanceId ?? obj.provider);
  const model = typeof obj.model === "string" && obj.model.trim() ? obj.model.trim() : null;
  if (!instanceId || !model) return null;
  return {
    instanceId,
    model,
    ...(Array.isArray(obj.options) ? { options: obj.options } : {}),
  };
}

function validIsoDateTime(val: unknown, fallback: string): string {
  if (typeof val === "string" && val.trim() && Number.isFinite(Date.parse(val))) {
    return val;
  }
  return fallback;
}

function validActor(val: unknown, fallback: "user" | "agent" | "system"): "user" | "agent" | "system" {
  return typeof val === "string" && VALID_ACTORS.has(val) ? (val as "user" | "agent" | "system") : fallback;
}

function validCreationSource(
  val: unknown,
  fallback: "web" | "mobile" | "mcp" | "provider" | "server",
): "web" | "mobile" | "mcp" | "provider" | "server" {
  return typeof val === "string" && VALID_CREATION_SOURCES.has(val)
    ? (val as "web" | "mobile" | "mcp" | "provider" | "server")
    : fallback;
}

function validTurnItemStatus(val: unknown, fallback: string): string {
  return typeof val === "string" && VALID_TURN_ITEM_STATUSES.has(val) ? val : fallback;
}

function textPageEnd(text: string, offset: number, maxChars: number): number {
  const end = offset + maxChars;
  if (end >= text.length) return end;
  const last = text.charCodeAt(end - 1);
  if (last < 0xd800 || last > 0xdbff) return end;
  return end - 1 > offset ? end - 1 : end + 1;
}

function extractTurnItemText(ti: Record<string, unknown>, linkedMsg?: Record<string, unknown>): string | null {
  if (typeof linkedMsg?.text === "string") return linkedMsg.text;
  if (typeof ti.text === "string") return ti.text;
  if (ti.type === "error" && ti.failure && typeof ti.failure === "object") {
    const msg = (ti.failure as { message?: unknown }).message;
    if (typeof msg === "string") return msg;
  }
  if (typeof ti.markdown === "string") return ti.markdown;
  if (typeof ti.summary === "string") {
    return typeof ti.detail === "string" ? `${ti.summary}\n${ti.detail}` : ti.summary;
  }
  if (typeof ti.message === "string") return ti.message;
  if (typeof ti.detail === "string") return ti.detail;
  if (ti.type === "command_execution" && typeof ti.input === "string") {
    return typeof ti.output === "string" ? `$ ${ti.input}\n${ti.output}` : `$ ${ti.input}`;
  }
  if (ti.type === "user_input_request" && Array.isArray(ti.questions)) {
    return JSON.stringify(ti.questions, null, 2);
  }
  if (ti.type === "subagent") {
    if (typeof ti.result === "string") return ti.result;
    if (typeof ti.progress === "string") return ti.progress;
    if (typeof ti.prompt === "string") return ti.prompt;
  }
  return ti.type ? JSON.stringify(ti) : null;
}

function requireString(val: unknown, field: string): string {
  if (typeof val !== "string" || !val.trim()) {
    throw new HubMcpToolError("invalid_request", `Missing required string field "${field}".`);
  }
  return val.trim();
}

function normalizeThreadStatus(obj: Record<string, unknown>): string {
  const candidates = [
    obj.activityRunStatus,
    obj.status,
    (obj.latestRun as Record<string, unknown> | undefined)?.status,
    (obj.activeRun as Record<string, unknown> | undefined)?.status,
  ];
  for (const c of candidates) {
    if (typeof c === "string" && VALID_THREAD_STATUSES.has(c)) return c;
  }
  return "idle";
}

function deriveRepoFromPrUrl(url: string | null): string | null {
  if (!url) return null;
  const m = /^https?:\/\/[^/]+\/([^/]+\/[^/]+)\/(?:pull|pulls|merge_requests)\/\d+/i.exec(url);
  return m ? m[1] : null;
}

function normalizePullRequest(raw: unknown): Record<string, unknown> | null {
  if (!raw || typeof raw !== "object") return null;
  const pr = raw as Record<string, unknown>;
  const url = typeof pr.url === "string" && pr.url.trim() ? pr.url.trim() : null;
  const number = typeof pr.number === "number" && Number.isInteger(pr.number) && pr.number >= 1 ? pr.number : null;
  if (!url || number === null) return null;
  const repository =
    typeof pr.repository === "string" && pr.repository.trim()
      ? pr.repository.trim()
      : deriveRepoFromPrUrl(url);
  if (!repository) return null;
  const snapshot = pr.snapshot as { state?: unknown } | null | undefined;
  const rawState = typeof snapshot?.state === "string" ? snapshot.state : typeof pr.state === "string" ? pr.state : null;
  const stateLower = rawState ? rawState.toLowerCase() : null;
  const state =
    stateLower === "open" || stateLower === "closed" || stateLower === "merged" ? stateLower : null;
  const source = typeof pr.source === "string" && VALID_PR_SOURCES.has(pr.source) ? pr.source : "agent";
  const watch = pr.watch as { startedAt?: unknown } | undefined;
  const watching = typeof pr.watching === "boolean" ? pr.watching : watch !== undefined;
  const watchStartedAt =
    typeof pr.watchStartedAt === "string"
      ? pr.watchStartedAt
      : typeof watch?.startedAt === "string"
        ? watch.startedAt
        : null;
  return {
    host: typeof pr.host === "string" && pr.host.trim() ? pr.host.trim() : "github.com",
    repository,
    number,
    url,
    source,
    state,
    watching,
    watchStartedAt,
  };
}

function extractPullRequests(
  projectId: string,
  ...objs: Array<Record<string, unknown> | undefined>
): {
  pullRequests: Array<Record<string, unknown>>;
  linkedPullRequest: Record<string, unknown> | null;
  openPullRequestCount: number;
} {
  const out: Array<Record<string, unknown>> = [];
  const seen = new Set<string>();
  const add = (raw: unknown) => {
    const pr = normalizePullRequest(raw);
    if (!pr) return;
    const key = String(pr.url);
    if (seen.has(key)) return;
    seen.add(key);
    out.push(pr);
  };
  for (const obj of objs) {
    if (!obj) continue;
    if (Array.isArray(obj.pullRequests)) {
      for (const item of obj.pullRequests) add(item);
    }
    if (Array.isArray(obj.pullRequestLinks)) {
      for (const item of obj.pullRequestLinks) add(item);
    }
    if (obj.linkedPullRequest) add(obj.linkedPullRequest);
    if (obj.branchPullRequest) add(obj.branchPullRequest);
  }
  const first = out[0];
  const linkedPullRequest = first
    ? {
        projectId,
        repository: String(first.repository),
        number: Number(first.number),
        url: String(first.url),
      }
    : null;
  const openPullRequestCount = out.filter((p) => p.state !== "merged" && p.state !== "closed").length;
  return { pullRequests: out, linkedPullRequest, openPullRequestCount };
}

/** True for Hub transport failures (timeout / disconnect / stopped VM), as opposed to a VM refusal. */
export function isRpcTransportFailure(cause: unknown): boolean {
  const reasons = Array.isArray(cause) ? cause : cause ? [cause] : [];
  return reasons.some((r) => {
    if (!r || typeof r !== "object") return false;
    const obj = r as Record<string, unknown>;
    if (obj._tag !== "Die" || typeof obj.defect !== "string") return false;
    return (
      obj.defect === "VM not connected" ||
      obj.defect === "Upstream VM disconnected" ||
      obj.defect === "Sandbox stopped" ||
      obj.defect.endsWith(" timed out")
    );
  });
}

/**
 * Text for a failed RPC Exit's `cause`. T3 Code 0.1.12+ (Effect 4) sends the cause as an array of
 * reasons (`Fail` with `error`, `Die` with `defect`, `Interrupt`); older VMs sent one object.
 */
export function formatRpcFailure(cause: unknown): string {
  if (cause === null || cause === undefined || cause === "") return "unknown error";
  if (Array.isArray(cause)) {
    const parts = [...new Set(cause.map(describeCauseReason).filter((p) => p !== ""))];
    return parts.length > 0 ? parts.join("; ") : "unknown error";
  }
  return describeCauseReason(cause) || "unknown error";
}

function describeCauseReason(reason: unknown): string {
  if (typeof reason === "string") return reason;
  if (!reason || typeof reason !== "object") return "";
  const obj = reason as Record<string, unknown>;
  if (obj._tag === "Interrupt") return "interrupted";
  if ("error" in obj) return describeErrorValue(obj.error);
  if ("defect" in obj) return describeErrorValue(obj.defect);
  return describeErrorValue(obj);
}

function describeErrorValue(value: unknown): string {
  if (typeof value === "string") return value;
  if (value && typeof value === "object") {
    const obj = value as Record<string, unknown>;
    for (const key of ["message", "detail", "reason"]) {
      const text = obj[key];
      if (typeof text === "string" && text.trim()) return text;
    }
  }
  try {
    return (JSON.stringify(value) ?? String(value)).slice(0, 300);
  } catch {
    return String(value);
  }
}

function withMcpCors(res: Response): Response {
  const headers = new Headers(res.headers);
  headers.set("access-control-allow-origin", "*");
  headers.set("access-control-allow-methods", "POST, DELETE, OPTIONS");
  headers.set(
    "access-control-allow-headers",
    "authorization, content-type, accept, mcp-protocol-version, mcp-session-id, t3-mode-limit, t3-sender-thread, cf-access-client-id, cf-access-client-secret",
  );
  headers.set("access-control-expose-headers", "mcp-session-id, mcp-protocol-version, t3-link-expires-at");
  headers.set("x-robots-tag", "noindex, nofollow, noarchive");
  return new Response(res.body, { status: res.status, statusText: res.statusText, headers });
}
