import { randomUUID } from "node:crypto";
import { authenticateClient } from "./auth.ts";
import type { ProjectConfig, SentinelConfig } from "./config.ts";
import { clientIpFromRequest, rateLimitedResponse, type AuthRateLimiter } from "./security.ts";
import { isChildThreadRow, vmCloseBlocker } from "./hub-park-guard.ts";
import { buildAutoProjectConfig, countsTowardHubLimit, type HubSandboxRecord, type StateStore } from "./state.ts";

export const MCP_PROTOCOL_VERSION = "2025-06-18";

export type HubSandboxPhase = "booting" | "cloning" | "starting_t3" | "running" | "sleeping" | "error" | "destroying";

export interface HubPendingLaunch {
  threadId: string;
  projectKey: string;
  projectId: string;
  title: string;
  message?: string;
  workspaceStrategy?: Record<string, unknown>;
  runtimeMode: string;
  modelSelection?: Record<string, unknown>;
  clientRequestId?: string;
  createdAt: string;
  updatedAt: string;
  sandboxId: string | null;
  phase: HubSandboxPhase;
  step: string;
  error: string | null;
  queuedMessages: Array<{ message: string; mode: string; clientRequestId?: string }>;
  cancelled?: boolean;
}

export interface RepositoryIdentity {
  canonicalKey: string;
  provider: "github";
  owner: string;
  name: string;
}

/** Who asked for a park (logged with it); an MCP settle also names the chat it settled. */
export interface HubParkRequest {
  trigger: string;
  /** The chat an MCP `t3_thread_organize` settle settled: it must be one of the VM's top-level chats. */
  settledThreadId?: string;
}

/** The MCP client that made a request (its paired credential's label), for the Hub's logs. */
export interface HubMcpCaller {
  label: string;
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
  wakeSandbox: (sandboxId: string) => Promise<HubSandboxRecord>;
  destroySandbox: (sandboxId: string, reason: string) => Promise<void>;
  sleepSandbox: (sandboxId: string) => Promise<void>;
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

const MCP_TOOLS = [
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
        settled: { type: "boolean" },
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
        message: { type: "string" },
        workspaceStrategy: { type: "object" },
        runtimeMode: { type: "string", enum: ["approval-required", "auto-accept-edits", "auto", "full-access"] },
        modelSelection: { type: "object" },
        clientRequestId: { type: "string" },
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
        clientRequestId: { type: "string" },
      },
      required: ["threadId", "message"],
    },
  },
  {
    name: "t3_thread_organize",
    description: "Organize a thread (settle, archive, pin, unpin, snooze, unsnooze, unsettle, unarchive, mark_unread). Archiving destroys the chat's VM for $0/hr idle; settling stops (parks) it and keeps the chat, unless boat.settleAction is delete.",
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
 * `git@github.com:owner/repo.git`) into T3 Code's `repositoryIdentity` object.
 */
export function parseGitHubRepositoryIdentity(repo?: string | null): RepositoryIdentity | null {
  if (!repo) return null;
  const raw = repo.trim();
  if (!raw) return null;
  let owner: string | null = null;
  let name: string | null = null;

  const httpsMatch = /^https?:\/\/(?:www\.)?github\.com\/([^/\s]+)\/([^/\s?#]+?)(?:\.git)?\/?$/i.exec(raw);
  const sshMatch = /^git@github\.com:([^/\s]+)\/([^/\s]+?)(?:\.git)?$/i.exec(raw);
  const shortMatch = /^([A-Za-z0-9_.-]+)\/([A-Za-z0-9_.-]+?)(?:\.git)?$/.exec(raw);

  if (httpsMatch) {
    owner = httpsMatch[1];
    name = httpsMatch[2];
  } else if (sshMatch) {
    owner = sshMatch[1];
    name = sshMatch[2];
  } else if (shortMatch) {
    owner = shortMatch[1];
    name = shortMatch[2];
  }
  if (!owner || !name) return null;
  return {
    canonicalKey: `github.com/${owner.toLowerCase()}/${name.toLowerCase()}`,
    provider: "github",
    owner,
    name,
  };
}

/**
 * Streamable HTTP MCP server (`POST /mcp`, protocolVersion `2025-06-18`) embedded in `HubServer`.
 * Designed for remote cloud clients (e.g. Cloudflare Workers / Zumith) with 15s HTTP timeouts
 * and `$0/hr` idle operation when 0 Boat VMs are running.
 */
export class HubMcpHandler {
  private readonly sessions = new Map<string, McpSessionRecord>();
  private readonly pendingLaunches = new Map<string, HubPendingLaunch>();
  private readonly launchByRequestId = new Map<string, string>(); // clientRequestId -> threadId
  private readonly sendByRequestId = new Map<string, { threadId: string; messageId: string }>();
  private readonly destroyedThreads = new Map<string, { destroyedAt: number; reason: string }>();

  constructor(
    private readonly config: SentinelConfig,
    private readonly state: StateStore,
    private readonly bridge: HubMcpBridge,
    private readonly rateLimiter?: AuthRateLimiter,
  ) {}

  /** Called by HubServer when a sandbox is destroyed so `/mcp` returns `sandbox_destroyed` cleanly. */
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
    }
    this.pruneExpiredState();
  }

  /** Number of currently booting launches not yet promoted into `state.hub.sandboxes`. */
  countBootingLaunches(): number {
    let count = 0;
    for (const launch of this.pendingLaunches.values()) {
      if (launch.phase === "booting" || launch.phase === "cloning" || launch.phase === "starting_t3") {
        // Don't double-count if sandboxId is already stored in HubState
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

    const client = authenticateClient(hub.clients, req.headers);
    if (!client) {
      this.rateLimiter?.recordFailure("hub-mcp", ip, 5 * 60_000);
      return withMcpCors(
        Response.json(
          { error: "invalid_mcp_credential", message: "A valid Bearer token is required for /mcp." },
          { status: 401, headers: { "www-authenticate": 'Bearer error="invalid_token"' } },
        ),
      );
    }
    this.rateLimiter?.recordSuccess("hub-mcp", ip);

    if (req.method === "DELETE") {
      const sid = req.headers.get("mcp-session-id");
      if (sid) this.sessions.delete(sid);
      return withMcpCors(new Response(null, { status: 204 }));
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
        // Notification — no JSON-RPC response body
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
        const toolResult = await this.executeTool(toolName, toolArgs, { label: client.label });
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

  private async dispatchTool(name: string, args: Record<string, unknown>, caller: HubMcpCaller): Promise<Record<string, unknown>> {
    switch (name) {
      case "t3_project_list":
        return this.toolProjectList(args);
      case "t3_thread_list":
        return this.toolThreadList(args);
      case "t3_thread_read":
        return this.toolThreadRead(args);
      case "t3_thread_launch":
        return this.toolThreadLaunch(args);
      case "t3_thread_send":
        return this.toolThreadSend(args);
      case "t3_thread_organize":
        return this.toolThreadOrganize(args, caller);
      case "list_thread_pull_requests":
        return this.toolListThreadPullRequests(args);
      case "t3_pending_request":
        return this.toolPendingRequest(args);
      case "t3_worktree_list":
        return this.toolWorktreeList(args);
      case "t3_thread_search":
        return this.toolThreadSearch(args);
      case "ci_logs":
        return this.toolCiLogs(args);
      case "sentinel_add_project":
        return this.toolAddProject(args);
      case "sentinel_status":
        return this.toolSentinelStatus();
      default:
        throw new HubMcpToolError("unknown_tool", `Unknown MCP tool "${name}".`);
    }
  }

  private threadLink(threadId: string): string {
    const hub = this.state.ensureHub(
      this.config.hub?.label ?? "Boat",
      Object.keys(this.state.getAllProjects(this.config)),
    );
    return `t3-thread://v2/${hub.environmentId}/${threadId}`;
  }

  private buildRepositoryIdentity(projectKey: string, proj: ProjectConfig): RepositoryIdentity | null {
    const fromRepo = parseGitHubRepositoryIdentity(proj.repo);
    if (fromRepo) return fromRepo;
    if (proj.bakeCmd) {
      const m = /(?:https?:\/\/(?:www\.)?github\.com\/|git@github\.com:)([A-Za-z0-9_.-]+)\/([A-Za-z0-9_.-]+?)(?:\.git)?(?:\s|$|["';&|])/i.exec(
        proj.bakeCmd,
      );
      if (m) {
        return parseGitHubRepositoryIdentity(`${m[1]}/${m[2]}`);
      }
    }
    const cached = this.state.getHub()?.projectTemplates[projectKey]?.shell?.repositoryIdentity as
      | Record<string, unknown>
      | undefined;
    if (
      cached &&
      cached.provider === "github" &&
      typeof cached.owner === "string" &&
      typeof cached.name === "string"
    ) {
      return {
        canonicalKey:
          typeof cached.canonicalKey === "string"
            ? cached.canonicalKey
            : `github.com/${cached.owner.toLowerCase()}/${cached.name.toLowerCase()}`,
        provider: "github",
        owner: cached.owner,
        name: cached.name,
      };
    }
    return null;
  }

  private toolProjectList(args: Record<string, unknown>): Record<string, unknown> {
    const allProjects = this.state.getAllProjects(this.config);
    const seenRepoKeys = new Set<string>();
    const projects: Array<Record<string, unknown>> = [];

    for (const [key, proj] of Object.entries(allProjects)) {
      if (proj.registerProject === false) continue;
      const repoId = this.buildRepositoryIdentity(key, proj);
      if (repoId) {
        if (seenRepoKeys.has(repoId.canonicalKey)) continue;
        seenRepoKeys.add(repoId.canonicalKey);
      }
      const id = this.bridge.ensureHubProjectId(key);
      projects.push({
        id,
        title: key,
        workspaceRoot: proj.workdir,
        repositoryIdentity: repoId,
        archivedAt: null,
        deletedAt: null,
      });
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

  private formatShellThread(
    threadId: string,
    shell: Record<string, unknown>,
    sandboxId: string | null,
  ): Record<string, unknown> {
    const sb = sandboxId ? this.state.getHubSandbox(sandboxId) : undefined;
    const projectId = String(shell.projectId ?? sb?.hubProjectId ?? "");
    const title = String(shell.title ?? "Untitled thread");
    const status = normalizeThreadStatus(shell);
    const updatedAt =
      typeof shell.updatedAt === "string"
        ? shell.updatedAt
        : typeof shell.createdAt === "string"
          ? shell.createdAt
          : new Date().toISOString();
    const pullRequests = extractPullRequestsFromObject(shell);
    const pendingRequestCount =
      typeof shell.pendingRequestCount === "number"
        ? shell.pendingRequestCount
        : Array.isArray(shell.pendingRequests)
          ? shell.pendingRequests.length
          : 0;
    const settled = Boolean(shell.settledAt || shell.settledOverride === "settled" || shell.settled === true);

    return {
      threadId,
      projectId,
      title,
      status,
      updatedAt,
      pullRequests,
      linkedPullRequest: pullRequests[0] ?? null,
      pendingRequestCount,
      settled,
      link: this.threadLink(threadId),
      sandbox: sb
        ? {
            sandboxId: sb.sandboxId,
            phase: sb.phase === "provisioning" ? "booting" : sb.phase,
            step: sb.phase === "running" ? "Running" : sb.phase === "sleeping" ? "Sleeping" : sb.phase,
            error: null,
          }
        : null,
    };
  }

  private formatPendingThread(launch: HubPendingLaunch): Record<string, unknown> {
    const status =
      launch.phase === "error"
        ? "failed"
        : launch.phase === "running"
          ? "starting"
          : "preparing";
    return {
      threadId: launch.threadId,
      projectId: launch.projectId,
      title: launch.title,
      status,
      updatedAt: launch.updatedAt,
      pullRequests: [],
      linkedPullRequest: null,
      pendingRequestCount: 0,
      settled: false,
      link: this.threadLink(launch.threadId),
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

    const byId = new Map<string, Record<string, unknown>>();

    // 1. Live threads from connected VMs
    for (const entry of this.bridge.listThreadShells()) {
      byId.set(entry.threadId, this.formatShellThread(entry.threadId, entry.shell, entry.sandboxId));
    }

    // 2. Booting / recently failed async launches not yet in shell map
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

    // Newest updatedAt first
    threads.sort((a, b) => String(b.updatedAt ?? "").localeCompare(String(a.updatedAt ?? "")));

    const cursor = typeof args.cursor === "number" && args.cursor >= 0 ? Math.floor(args.cursor) : 0;
    const limit = typeof args.limit === "number" && args.limit > 0 ? Math.min(Math.floor(args.limit), 200) : 50;
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

  private async toolThreadRead(args: Record<string, unknown>): Promise<Record<string, unknown>> {
    const threadId = requireString(args.threadId, "threadId");
    const view = args.view === "activity" ? "activity" : "messages";
    const limit = typeof args.limit === "number" && args.limit > 0 ? Math.min(Math.floor(args.limit), 100) : 50;
    const maxChars =
      typeof args.maxCharsPerItem === "number" && args.maxCharsPerItem > 0
        ? Math.min(Math.floor(args.maxCharsPerItem), 20_000)
        : 20_000;
    const runLimit =
      typeof args.runLimit === "number" && args.runLimit > 0 ? Math.min(Math.floor(args.runLimit), 50) : 5;
    const afterPosition = typeof args.afterPosition === "number" ? Math.floor(args.afterPosition) : -1;
    const beforePosition = typeof args.beforePosition === "number" ? Math.floor(args.beforePosition) : null;

    // 1. Check live sandbox first
    const sb = this.bridge.getSandboxForThread(threadId);
    const pending = this.pendingLaunches.get(threadId);

    // If still booting (no running sandbox yet, or launchThread hasn't landed in VM yet), return 200 immediately!
    if ((!sb || sb.phase === "provisioning") && pending) {
      return {
        thread: {
          ...this.formatPendingThread(pending),
          itemCount: 0,
        },
        items: [],
        recentRuns: [],
        nextPosition: null,
        hasMore: false,
      };
    }

    if (sb && sb.phase === "running") {
      const res = await this.bridge.callVmUnary(sb.sandboxId, "orchestration.getThreadProjection", { threadId }, 12_000);
      if (res._tag === "Success" && res.value && typeof res.value === "object") {
        return this.mapProjectionToThreadRead(
          res.value as Record<string, unknown>,
          sb,
          { view, limit, maxChars, runLimit, afterPosition, beforePosition },
        );
      }
      // If VM is up but thread creation is still in-flight (first ~1s after boot), return pending state cleanly
      if (pending && pending.phase !== "error") {
        return {
          thread: {
            ...this.formatPendingThread(pending),
            itemCount: 0,
          },
          items: [],
          recentRuns: [],
          nextPosition: null,
          hasMore: false,
        };
      }
      // Fall back to cached shell if projection call failed transiently
      const shell = this.bridge.getThreadShell(threadId);
      if (shell) {
        return {
          thread: {
            ...this.formatShellThread(threadId, shell, sb.sandboxId),
            itemCount: 0,
          },
          items: [],
          recentRuns: [],
          nextPosition: null,
          hasMore: false,
        };
      }
      throw new HubMcpToolError("orchestration_error", `Failed to read projection for thread "${threadId}".`);
    }

    if (sb && sb.phase === "sleeping") {
      const shell = this.bridge.getThreadShell(threadId);
      return {
        thread: shell
          ? { ...this.formatShellThread(threadId, shell, sb.sandboxId), itemCount: 0 }
          : {
              threadId,
              projectId: sb.hubProjectId,
              title: "Sleeping thread",
              status: "idle",
              updatedAt: sb.sleepingSince ?? sb.lastActiveAt,
              pullRequests: [],
              pendingRequestCount: 0,
              itemCount: 0,
              link: this.threadLink(threadId),
              sandbox: { sandboxId: sb.sandboxId, phase: "sleeping", step: "Sleeping", error: null },
            },
        items: [],
        recentRuns: [],
        nextPosition: null,
        hasMore: false,
      };
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
      afterPosition: number;
      beforePosition: number | null;
    },
  ): Record<string, unknown> {
    const rawThread = (projection.thread ?? {}) as Record<string, unknown>;
    const threadId = String(rawThread.id ?? rawThread.threadId ?? "");
    const rawRuns = Array.isArray(projection.runs) ? (projection.runs as Array<Record<string, unknown>>) : [];
    const rawMessages = Array.isArray(projection.messages)
      ? (projection.messages as Array<Record<string, unknown>>)
      : [];
    const rawTurnItems = Array.isArray(projection.turnItems)
      ? (projection.turnItems as Array<Record<string, unknown>>)
      : [];
    const rawRequests = Array.isArray(projection.runtimeRequests)
      ? (projection.runtimeRequests as Array<Record<string, unknown>>)
      : [];

    const pendingRequests = rawRequests.filter(
      (r) => r && r.kind === "user_input" && r.status === "pending",
    );
    const pullRequests = extractPullRequestsFromObject(projection, rawThread);

    // Derive thread status from live runs & pending requests
    const sortedRunsDesc = [...rawRuns].sort((a, b) => {
      const ordA = typeof a.ordinal === "number" ? a.ordinal : 0;
      const ordB = typeof b.ordinal === "number" ? b.ordinal : 0;
      if (ordA !== ordB) return ordB - ordA;
      return String(b.requestedAt ?? b.startedAt ?? "").localeCompare(String(a.requestedAt ?? a.startedAt ?? ""));
    });
    const latestRun = sortedRunsDesc[0];
    let status = normalizeThreadStatus(rawThread);
    if (pendingRequests.length > 0) {
      status = "waiting";
    } else if (latestRun && typeof latestRun.status === "string" && VALID_THREAD_STATUSES.has(latestRun.status)) {
      status = latestRun.status;
    }

    // Build ordered timeline items
    const messageById = new Map<string, Record<string, unknown>>();
    for (const m of rawMessages) {
      if (typeof m.id === "string") messageById.set(m.id, m);
    }

    const fullTimeline: Array<Record<string, unknown>> = [];
    if (opts.view === "activity" && rawTurnItems.length > 0) {
      for (const ti of rawTurnItems) {
        const msgId = typeof ti.messageId === "string" ? ti.messageId : null;
        const linkedMsg = msgId ? messageById.get(msgId) : undefined;
        const rawText =
          (typeof linkedMsg?.text === "string" && linkedMsg.text) ||
          (typeof ti.text === "string" && ti.text) ||
          (typeof ti.summary === "string" && ti.summary) ||
          (typeof ti.message === "string" && ti.message) ||
          (typeof ti.detail === "string" && ti.detail) ||
          (ti.type ? JSON.stringify(ti) : "");
        const role =
          typeof linkedMsg?.role === "string"
            ? linkedMsg.role
            : ti.type === "user_message"
              ? "user"
              : ti.type === "assistant_message"
                ? "assistant"
                : undefined;
        fullTimeline.push({
          type: String(ti.type ?? "activity"),
          ...(role ? { role } : {}),
          ...(typeof ti.runId === "string" ? { runId: ti.runId } : {}),
          text: rawText,
        });
      }
    } else {
      for (const m of rawMessages) {
        const role = typeof m.role === "string" ? m.role : "assistant";
        fullTimeline.push({
          type: role === "user" ? "user_message" : "assistant_message",
          role,
          ...(typeof m.runId === "string" ? { runId: m.runId } : {}),
          text: typeof m.text === "string" ? m.text : "",
        });
      }
    }

    const indexed = fullTimeline.map((item, position) => {
      const fullText = String(item.text ?? "");
      const truncated = fullText.length > opts.maxChars;
      return {
        position,
        ...item,
        text: truncated ? fullText.slice(0, opts.maxChars) : fullText,
        textTruncated: truncated,
      };
    });

    const filtered = indexed.filter((item) => {
      if (item.position <= opts.afterPosition) return false;
      if (opts.beforePosition !== null && item.position >= opts.beforePosition) return false;
      return true;
    });
    const page = filtered.slice(0, opts.limit);
    const hasMore = filtered.length > page.length;
    const nextPosition = page.length > 0 ? page[page.length - 1].position : null;

    const recentRuns = sortedRunsDesc.slice(0, opts.runLimit).map((r) => ({
      runId: String(r.id ?? r.runId ?? ""),
      id: String(r.id ?? r.runId ?? ""),
      status: String(r.status ?? "idle"),
      ...(typeof r.startedAt === "string" ? { startedAt: r.startedAt } : {}),
      ...(typeof r.completedAt === "string" ? { completedAt: r.completedAt } : {}),
      ...(typeof r.error === "string" ? { error: r.error } : {}),
    }));

    return {
      thread: {
        threadId,
        projectId: sb.hubProjectId,
        title: String(rawThread.title ?? "Untitled thread"),
        status,
        updatedAt:
          typeof rawThread.updatedAt === "string"
            ? rawThread.updatedAt
            : typeof rawThread.createdAt === "string"
              ? rawThread.createdAt
              : sb.lastActiveAt,
        pullRequests,
        linkedPullRequest: pullRequests[0] ?? null,
        pendingRequestCount: pendingRequests.length,
        itemCount: fullTimeline.length,
        settled: Boolean(rawThread.settledAt || rawThread.settledOverride === "settled"),
        link: this.threadLink(threadId),
        sandbox: {
          sandboxId: sb.sandboxId,
          phase: "running",
          step: "Running",
          error: null,
        },
      },
      items: page,
      recentRuns,
      nextPosition,
      hasMore,
    };
  }

  private async toolThreadLaunch(args: Record<string, unknown>): Promise<Record<string, unknown>> {
    const projectId = requireString(args.projectId, "projectId");
    const clientRequestId =
      typeof args.clientRequestId === "string" && args.clientRequestId.trim() ? args.clientRequestId.trim() : undefined;

    // 1. Idempotency check by clientRequestId — never spawn duplicate VMs on Worker retry!
    if (clientRequestId) {
      const existingThreadId = this.launchByRequestId.get(clientRequestId);
      if (existingThreadId) {
        const pending = this.pendingLaunches.get(existingThreadId);
        if (pending) {
          return {
            threadId: existingThreadId,
            projectId: pending.projectId,
            title: pending.title,
            status: pending.phase === "error" ? "failed" : pending.phase === "running" ? "starting" : "preparing",
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
          return {
            threadId: existingThreadId,
            projectId: existingSb?.hubProjectId ?? projectId,
            title: String(existingShell?.title ?? args.title ?? "Thread"),
            status: existingShell ? normalizeThreadStatus(existingShell) : "starting",
            link: this.threadLink(existingThreadId),
            sandbox: existingSb
              ? { sandboxId: existingSb.sandboxId, phase: existingSb.phase, step: "Running", error: null }
              : null,
          };
        }
      }
    }

    // 2. Validate projectId
    const projectKey = this.bridge.projectKeyFromHubId(projectId);
    if (!projectKey) {
      throw new HubMcpToolError("project_not_found", `Project "${projectId}" was not found in Sentinel Hub.`);
    }

    // 3. Validate capacity up front before returning
    const maxSandboxes = this.config.hub?.maxSandboxes ?? 10;
    const activeCount = this.state.listHubSandboxes().filter(countsTowardHubLimit).length + this.countBootingLaunches();
    if (activeCount >= maxSandboxes) {
      throw new HubMcpToolError(
        "capacity_exceeded",
        `Hub sandbox limit reached (${activeCount}/${maxSandboxes}). Settle or archive an existing thread first.`,
      );
    }

    // 4. Create thread record immediately and kick off async VM boot (< 50ms return for 15s Worker timeout!)
    const threadId = randomUUID();
    const message = typeof args.message === "string" && args.message.trim() ? args.message : undefined;
    const title =
      typeof args.title === "string" && args.title.trim()
        ? args.title.trim()
        : message
          ? message.slice(0, 72)
          : `${projectKey} task`;
    const runtimeMode =
      typeof args.runtimeMode === "string" && args.runtimeMode.trim() ? args.runtimeMode.trim() : "full-access";
    const workspaceStrategy =
      args.workspaceStrategy && typeof args.workspaceStrategy === "object"
        ? (args.workspaceStrategy as Record<string, unknown>)
        : undefined;
    const modelSelection =
      args.modelSelection && typeof args.modelSelection === "object"
        ? (args.modelSelection as Record<string, unknown>)
        : undefined;

    const now = new Date().toISOString();
    const pending: HubPendingLaunch = {
      threadId,
      projectKey,
      projectId,
      title,
      message,
      workspaceStrategy,
      runtimeMode,
      modelSelection,
      clientRequestId,
      createdAt: now,
      updatedAt: now,
      sandboxId: null,
      phase: "booting",
      step: `Spawning isolated ${this.bridge.providerName()} VM for ${projectKey}...`,
      error: null,
      queuedMessages: [],
    };

    this.pendingLaunches.set(threadId, pending);
    if (clientRequestId) {
      this.launchByRequestId.set(clientRequestId, threadId);
    }

    void this.runAsyncLaunch(pending);

    return {
      threadId,
      projectId,
      title,
      status: "preparing",
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

      const proj = this.state.getAllProjects(this.config)[pending.projectKey];
      const modelSelection =
        pending.modelSelection ??
        (proj?.defaultProvider && proj?.defaultModel
          ? { instanceId: proj.defaultProvider, model: proj.defaultModel }
          : undefined);

      const strategy = pending.workspaceStrategy ?? { type: "root" };
      const launchPayload: Record<string, unknown> = {
        commandId: randomUUID(),
        threadId: pending.threadId,
        projectId: sb.vmProjectId,
        title: pending.title,
        runtimeMode: pending.runtimeMode,
        interactionMode: "default",
        workspaceStrategy: strategy,
        ...(pending.message ? { initialMessage: { text: pending.message, attachments: [] } } : {}),
        ...(modelSelection ? { modelSelection } : {}),
      };

      let exit = await this.bridge.callVmUnary(sb.sandboxId, "orchestration.launchThread", launchPayload, 120_000);
      // If a requested worktree strategy without an explicit baseRef fails on a non-standard repo, fall back cleanly to root
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
      const projObj = val.projection as { thread?: { worktreePath?: string | null } } | undefined;
      if (projObj?.thread?.worktreePath) {
        this.bridge.bindWorktreeToSandbox(projObj.thread.worktreePath, sb.sandboxId);
      }

      pending.phase = "running";
      pending.step = "Running";
      pending.updatedAt = new Date().toISOString();

      // Drain any messages queued via `t3_thread_send` while the VM was booting
      while (pending.queuedMessages.length > 0 && !pending.cancelled) {
        const next = pending.queuedMessages.shift()!;
        await this.dispatchMessageToVm(sb.sandboxId, actualTid, next.message, next.mode, next.clientRequestId).catch(
          (err) => {
            console.error(`[hub-mcp] failed to flush queued message for ${actualTid}:`, err);
          },
        );
      }

      // Remove from pending map once VM is live (shell map + sandbox state own it now)
      if (!pending.cancelled) {
        this.pendingLaunches.delete(pending.threadId);
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.error(`[hub-mcp] async launch failed for thread=${pending.threadId}:`, msg);
      pending.phase = "error";
      pending.step = "VM boot failed";
      pending.error = msg;
      pending.updatedAt = new Date().toISOString();
    }
  }

  private async dispatchMessageToVm(
    sandboxId: string,
    threadId: string,
    message: string,
    mode: string,
    clientRequestId?: string,
  ): Promise<{ messageId: string; delivery: string }> {
    const messageId = `msg_${randomUUID().replace(/-/g, "")}`;
    const deliveryIntent = mode === "steer" || mode === "restart" ? mode : "auto";
    const exit = await this.bridge.callVmUnary(
      sandboxId,
      "orchestration.dispatchCommand",
      {
        type: "message.dispatch",
        commandId: clientRequestId ? `cmd_${clientRequestId}` : randomUUID(),
        threadId,
        messageId,
        text: message,
        attachments: [],
        deliveryIntent,
        createdBy: "agent",
        creationSource: "mcp",
      },
      12_000,
    );
    if (exit._tag === "Failure") {
      throw new HubMcpToolError("orchestration_error", `Failed to send message: ${formatRpcFailure(exit.cause)}`);
    }
    return { messageId, delivery: deliveryIntent };
  }

  private async toolThreadSend(args: Record<string, unknown>): Promise<Record<string, unknown>> {
    const threadId = requireString(args.threadId, "threadId");
    const message = requireString(args.message, "message");
    const mode = typeof args.mode === "string" && args.mode.trim() ? args.mode.trim() : "auto";
    const clientRequestId =
      typeof args.clientRequestId === "string" && args.clientRequestId.trim() ? args.clientRequestId.trim() : undefined;

    if (clientRequestId) {
      const prev = this.sendByRequestId.get(clientRequestId);
      if (prev && prev.threadId === threadId) {
        return {
          accepted: true,
          deduplicated: true,
          threadId,
          messageId: prev.messageId,
          delivery: mode,
          link: this.threadLink(threadId),
        };
      }
    }

    const pending = this.pendingLaunches.get(threadId);
    if (pending) {
      if (pending.phase === "error") {
        throw new HubMcpToolError("vm_boot_failed", pending.error ?? "Sandbox failed to boot for this thread.");
      }
      if (pending.phase !== "running") {
        pending.queuedMessages.push({ message, mode, clientRequestId });
        const queuedId = `queued_${randomUUID().replace(/-/g, "")}`;
        if (clientRequestId) this.sendByRequestId.set(clientRequestId, { threadId, messageId: queuedId });
        return {
          accepted: true,
          queued: true,
          threadId,
          messageId: queuedId,
          status: "preparing",
          delivery: "queued_until_boot",
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

    const sent = await this.dispatchMessageToVm(sb.sandboxId, threadId, message, mode, clientRequestId);
    if (clientRequestId) {
      this.sendByRequestId.set(clientRequestId, { threadId, messageId: sent.messageId });
    }
    return {
      accepted: true,
      threadId,
      messageId: sent.messageId,
      delivery: sent.delivery,
      link: this.threadLink(threadId),
    };
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
    const threadId = requireString(args.threadId, "threadId");
    const action = requireString(args.action, "action");
    const isDestructiveCleanup = action === "settle" || action === "archive";

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
      log.outcome = "booting chat cancelled, VM destroyed";
      return { ok: true, threadId, action, sandboxDestroyed: true };
    }

    const sb = this.bridge.getSandboxForThread(threadId);
    if (!sb) {
      if (this.destroyedThreads.has(threadId) && isDestructiveCleanup) {
        log.outcome = "VM already destroyed";
        return { ok: true, threadId, action, sandboxDestroyed: true, alreadyDestroyed: true };
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

    // A cleanup may close the whole VM: only for one of its top-level chats, read before the command
    // (once archived, the VM no longer lists it)
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
      // Like a settle in the app: the chat stays, and its VM is parked once nothing on it is open or
      // running. A settle the VM refused parks nothing; the Hub checks the rest with what the VM lists now
      let sandboxParked = false;
      if (refused === null) {
        sandboxParked = await this.bridge.parkSandbox(sb.sandboxId, {
          trigger: `MCP t3_thread_organize settle of ${threadId} by ${JSON.stringify(caller.label)}`,
          settledThreadId: threadId,
        });
      }
      log.outcome = refused !== null ? `the VM refused the settle (${refused}); not parked` : sandboxParked ? "VM parked" : "settled; VM not parked";
      return { ok: true, threadId, action, sandboxDestroyed, sandboxParked };
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
          // Nothing was applied: say so instead of a success (its chats cannot be read to close it safely)
          throw new HubMcpToolError(
            "sandbox_not_running",
            `This chat's VM is ${sb.phase}, so the ${action} was not applied and the VM was kept. Close the chat from the T3 app, or wake it (send it a message) and try again.`,
          );
        }
      } else if (this.config.boat.archiveAction === "stop") {
        console.log(`[hub] stopping ${sb.sandboxId} (${sb.project}): no open or running chat left — trigger: ${trigger}`);
        await this.bridge.sleepSandbox(sb.sandboxId);
        log.outcome = "VM stopped";
      } else {
        console.log(`[hub] destroying ${sb.sandboxId} (${sb.project}): no open or running chat left — trigger: ${trigger}`);
        await this.bridge.destroySandbox(sb.sandboxId, `mcp-${action}`);
        sandboxDestroyed = true;
        log.outcome = "VM destroyed";
      }
    }

    return { ok: true, threadId, action, sandboxDestroyed };
  }

  /**
   * Why a settle or archive over MCP must leave the chat's VM as it is, or null when the VM may be
   * stopped or deleted: the chat must be one of its top-level chats (closing a delegated chat never
   * closes its parent's VM), the VM must have taken the command, and what it lists afterwards must
   * hold no open top-level chat and nothing live. A VM whose chats cannot be read is kept: a deleted
   * VM cannot come back, and its watchdog (`all-archived`) still closes it once its chats are done.
   */
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
        const prs = extractPullRequestsFromObject(proj, (proj.thread ?? {}) as Record<string, unknown>);
        return { pullRequests: prs, chains: [] };
      }
    }

    const shell = this.bridge.getThreadShell(threadId);
    return {
      pullRequests: shell ? extractPullRequestsFromObject(shell) : [],
      chains: [],
    };
  }

  private async toolPendingRequest(args: Record<string, unknown>): Promise<Record<string, unknown>> {
    const action = requireString(args.action, "action");
    const threadId = requireString(args.threadId, "threadId");
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
      return { ok: true, requestId };
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
      limit: 200,
    });
    const threads = (list.threads as Array<Record<string, unknown>>) ?? [];
    const matches = threads
      .filter((t) => {
        const hay = `${String(t.title ?? "")} ${String(t.threadId ?? "")} ${JSON.stringify(t.pullRequests ?? [])}`.toLowerCase();
        return hay.includes(query);
      })
      .slice(0, limit);

    return { matches };
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

  private toolAddProject(args: Record<string, unknown>): Record<string, unknown> {
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

function requireString(val: unknown, field: string): string {
  if (typeof val !== "string" || !val.trim()) {
    throw new HubMcpToolError("invalid_request", `Missing required string field "${field}".`);
  }
  return val.trim();
}

function normalizeThreadStatus(obj: Record<string, unknown>): string {
  const candidates = [
    obj.status,
    (obj.latestRun as Record<string, unknown> | undefined)?.status,
    (obj.activeRun as Record<string, unknown> | undefined)?.status,
  ];
  for (const c of candidates) {
    if (typeof c === "string" && VALID_THREAD_STATUSES.has(c)) return c;
  }
  return "idle";
}

function normalizePullRequest(raw: unknown): Record<string, unknown> | null {
  if (!raw || typeof raw !== "object") return null;
  const pr = raw as Record<string, unknown>;
  const url = typeof pr.url === "string" ? pr.url : null;
  const number = typeof pr.number === "number" ? pr.number : null;
  if (!url && number === null) return null;
  const stateRaw = typeof pr.state === "string" ? pr.state.toLowerCase() : "open";
  const state = stateRaw === "merged" || stateRaw === "closed" ? stateRaw : "open";
  return {
    host: typeof pr.host === "string" ? pr.host : "github.com",
    ...(typeof pr.repository === "string" ? { repository: pr.repository } : {}),
    ...(number !== null ? { number } : {}),
    ...(url ? { url } : {}),
    state,
    ...(typeof pr.title === "string" ? { title: pr.title } : {}),
    ...(typeof pr.watching === "boolean" ? { watching: pr.watching } : {}),
  };
}

function extractPullRequestsFromObject(...objs: Array<Record<string, unknown> | undefined>): Array<Record<string, unknown>> {
  const out: Array<Record<string, unknown>> = [];
  const seen = new Set<string>();
  const add = (raw: unknown) => {
    const pr = normalizePullRequest(raw);
    if (!pr) return;
    const key = String(pr.url ?? `${pr.host}:${pr.number}`);
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
  return out;
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
  if ("error" in obj) return describeErrorValue(obj.error); // Fail
  if ("defect" in obj) return describeErrorValue(obj.defect); // Die
  return describeErrorValue(obj);
}

function describeErrorValue(value: unknown): string {
  if (typeof value === "string") return value;
  if (value && typeof value === "object") {
    const obj = value as Record<string, unknown>;
    // `detail`: ProviderSetupError; `reason`: ServerSelfUpdateError
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
    "authorization, content-type, accept, mcp-protocol-version, mcp-session-id, cf-access-client-id, cf-access-client-secret",
  );
  headers.set("access-control-expose-headers", "mcp-session-id, mcp-protocol-version");
  headers.set("x-robots-tag", "noindex, nofollow, noarchive");
  return new Response(res.body, { status: res.status, statusText: res.statusText, headers });
}
