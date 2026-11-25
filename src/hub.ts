import { randomUUID } from "node:crypto";
import type { ServerWebSocket } from "bun";
import {
  CLIENT_TOKEN_TTL_SECS,
  DEFAULT_T3_PERMISSIONS,
  DEFAULT_T3_SCOPES,
  SlotAuth,
  authenticateClient,
  hashToken,
  randomToken,
  type TicketUse,
} from "./auth.ts";
import {
  asSandboxProvider,
  hasCapability,
  type LegacyBoatClient,
  type SandboxProvider,
} from "./providers/types.ts";
import {
  ROBOTS_TXT,
  applySecurityHeaders,
  clientIpFromRequest,
  rateLimitedResponse,
  type AuthRateLimiter,
} from "./security.ts";
import type { ProjectConfig, SentinelConfig } from "./config.ts";
import { HubMcpHandler, type HubSandboxPhase } from "./hub-mcp.ts";
import {
  applySettingsOverlay,
  applySettingsUpdate,
  describeSettingsUpdate,
  forwardableSettingsUpdate,
  isEmptySettingsOverlay,
  isFullServerSettings,
  overlayReplayCalls,
  parseSettingsUpdate,
  providersWithInstanceEnabled,
  recordSettingsUpdate,
  rewriteProjectIdsAndKeys,
  VM_SETTINGS_PATH,
  type SettingsUpdateInput,
} from "./hub-settings.ts";
import type { KlavClient } from "./klav.ts";
import type { LifecycleManager, WatchdogEventPayload } from "./lifecycle.ts";
import { buildAutoProjectConfig, type HubSandboxRecord, type StateStore } from "./state.ts";

type SelfUpdateStage = "downloading" | "installing";
type SelfUpdateResult = { targetVersion: string; method: "boot-service" };
type SelfUpdateOutcome = { ok: true; result: SelfUpdateResult } | { ok: false; reason: string };

/** One self-update round, shared by every request for its version while it is queued or running. */
interface SelfUpdateRun {
  stage: SelfUpdateStage | null;
  readonly listeners: Set<(stage: SelfUpdateStage) => void>;
  done: Promise<SelfUpdateOutcome>;
}
import { ensureT3Project } from "./t3rpc.ts";

export interface HubWsData {
  kind: "hub";
  clientId: string;
  /** Streams this downstream client is subscribed to locally on the Hub: String(id) -> { rawId, tag, payload? }. */
  localStreams: Map<string, { rawId: string | number; tag: string; payload?: unknown }>;
  /** Active upstream-forwarded RPC requests owned by this client: String(clientReqId) -> { sandboxId, upReqId }. */
  upstreamRequests: Map<string, { sandboxId: string; upReqId: string }>;
}

interface RpcClientFrame {
  _tag: "Request" | "Ack" | "Interrupt" | "Ping" | "Pong" | string;
  id?: string | number;
  requestId?: string | number;
  tag?: string;
  payload?: unknown;
  headers?: unknown;
}

interface RpcServerFrame {
  _tag: "Chunk" | "Exit" | "Pong" | "Ping" | "Defect" | string;
  requestId?: string | number;
  values?: unknown[];
  exit?: { _tag: "Success"; value: unknown } | { _tag: "Failure"; cause: unknown };
  defect?: unknown;
}

interface PendingUpstreamCall {
  resolve?: (exit: { _tag: "Success"; value: unknown } | { _tag: "Failure"; cause: unknown }) => void;
  onChunk?: (values: unknown[]) => void;
  clientWs?: ServerWebSocket<HubWsData>;
  clientReqId?: string | number;
  vmProjectId?: string;
  hubProjectId?: string;
  tag?: string;
  /** Called with the upstream Exit before it is relayed to the client. */
  onExit?: (exit: { _tag: "Success"; value: unknown } | { _tag: "Failure"; cause: unknown }) => void;
  /** Called instead of failing the client's request when the VM connection is lost before the call ends. */
  onLost?: () => void;
}

interface StagedAttachment {
  id: string;
  name: string;
  mimeType: string;
  sizeBytes: number;
  bytes: Uint8Array | null;
  createdAt: number;
}

const HOP_BY_HOP = new Set([
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
  "host",
]);
const STRIP_RESPONSE_HEADERS = new Set([...HOP_BY_HOP, "content-encoding", "content-length"]);

const THREAD_ROUTED_PREFIXES = ["terminal."];
const THREAD_ROUTED_METHODS = new Set([
  "orchestration.subscribeThread",
  "orchestration.getThreadProjection",
  "orchestration.getTurnDiff",
  "orchestration.getFullThreadDiff",
  "orchestration.getTurnItem",
  "orchestration.stopWorkflow",
  "orchestration.forceStopThread",
  "orchestration.subscribeCommandOutput",
  "orchestration.searchThread",
  "orchestration.searchThreadStream",
  "orchestration.getThreadFamily",
  "orchestration.getWorktreeThreadIds",
  "orchestration.getWorkflowScript",
]);

const WORKSPACE_ROUTED_PREFIXES = ["vcs.", "git.", "projects.", "review."];

/** Sign-out RPCs (`{ instanceId }` payload); a successful one forgets the stored provider session. */
const PROVIDER_LOGOUT_METHODS = new Set(["provider.auth.logout", "server.logoutAcpRegistry"]);

/**
 * Provider setup streams the Settings → Providers panel opens on every visit. With no VM running
 * the Hub answers them with an idle snapshot; a VM bridge that opens later takes them over.
 */
const PROVIDER_SETUP_STREAMS = new Set(["provider.install.subscribe", "provider.auth.subscribe"]);
/** Provider setup commands whose error union is `ProviderSetupError | EnvironmentAuthorizationError`. */
const PROVIDER_SETUP_COMMANDS = new Set([
  "provider.auth.start",
  "provider.auth.respond",
  "provider.auth.complete",
  "provider.auth.cancel",
  "provider.auth.removeMethod",
  "provider.auth.logout",
  "provider.install.start",
  "provider.install.cancel",
  "provider.install.remove",
]);
const NO_VM_PROVIDER_SETUP_DETAIL =
  "Provider setup runs inside a Boat chat VM, and none is running. Start a thread on Boat, then try again.";

const KEEP_OPEN_SUBSCRIPTIONS = new Set([
  "subscribePreviewEvents",
  "scheduledTasks.subscribe",
  "subscribeDeviceState",
  "subscribeDiscoveredLocalServers",
  "subscribeResourceTelemetry",
  "pullRequests.subscribeRefreshes",
]);

interface WorktreeSetupStageState {
  id: "fetch" | "checkout" | "submodules" | "setup-script" | "agent";
  status: "pending" | "running" | "done" | "skipped" | "warning" | "failed";
  startedAt: string | null;
  endedAt: string | null;
  percent: number | null;
  detail: string | null;
  tail: string[];
}

interface WorktreeSetupSnapshotState {
  threadId: string;
  phase: "running" | "done" | "failed" | "cancelled";
  startedAt: string;
  endedAt: string | null;
  branch: string | null;
  baseRef: string | null;
  worktreePath: string | null;
  setupScript: null;
  stages: WorktreeSetupStageState[];
  error: string | null;
  sequence: number;
}

interface ProjectCloneSnapshotState {
  projectId: string;
  remoteUrl: string;
  destinationPath: string;
  repository: null;
  phase: "running" | "done" | "failed" | "cancelled";
  stage: "connecting" | "counting" | "receiving" | "resolving" | "checkout";
  percent: number | null;
  detail: string | null;
  error: string | null;
  startedAt: string;
  endedAt: string | null;
  sequence: number;
}

/**
 * Persistent WebSocket bridge from the Hub to a single live Boat VM's `t3 serve`.
 * - Subscribes to `orchestration.subscribeShell` and pushes merged deltas to the Hub.
 * - Multiplexes arbitrary unary and streaming RPC requests from any connected T3 client.
 * - Rewrites `vmProjectId <-> hubProjectId` transparently on every frame.
 */
class HubVmBridge {
  private ws: WebSocket | null = null;
  private closed = false;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private pingTimer: ReturnType<typeof setInterval> | null = null;
  private reqCounter = 0;
  private readonly pending = new Map<string, PendingUpstreamCall>();
  private readonly openWaiters: Array<{ resolve: () => void; reject: (err: Error) => void }> = [];
  private readonly shellReqId = "hub-shell";
  /** Persistent `subscribeServerConfig` stream: the VM publishes corrected provider status after boot on it. */
  private readonly configReqId = "hub-config";
  /** Latest `providerStatuses` list seen on the current connection; overrides the (possibly stale) boot snapshot. */
  private latestProviders: unknown[] | null = null;

  constructor(
    public sandbox: HubSandboxRecord,
    private readonly onShellItems: (sandboxId: string, items: unknown[]) => void,
    private readonly onUrlTokenIssued: (sandboxId: string, token: string) => void,
    private readonly onTemplatesHarvested: (
      sandbox: HubSandboxRecord,
      templates: { serverConfig?: Record<string, unknown>; lifecycleEvents?: unknown[]; vcsRefs?: unknown },
    ) => void,
    private readonly onProviderStatuses: (sandboxId: string, providers: unknown[]) => void = () => {},
    private readonly onOpen: (sandboxId: string) => void = () => {},
  ) {}

  start(): void {
    this.closed = false;
    this.connect();
  }

  stop(): void {
    this.closed = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
    for (const waiter of this.openWaiters.splice(0)) {
      waiter.reject(new Error("VM bridge stopped"));
    }
    if (this.pending.has(this.configReqId)) {
      this.sendRaw({ _tag: "Interrupt", requestId: this.configReqId });
    }
    for (const [upId, call] of this.pending) {
      this.pending.delete(upId);
      if (call.resolve) {
        call.resolve({ _tag: "Failure", cause: hubDefectCause("Sandbox stopped") });
      } else if (call.onLost && call.clientWs && call.clientReqId !== undefined) {
        call.clientWs.data.upstreamRequests.delete(String(call.clientReqId));
        call.onLost();
      } else if (call.clientWs && call.clientReqId) {
        sendRpcExitFailure(call.clientWs, call.clientReqId, "Sandbox stopped");
      }
    }
    if (this.ws) {
      try {
        this.ws.close(1000, "hub bridge closed");
      } catch {
        // ignore
      }
      this.ws = null;
    }
  }

  isOpen(): boolean {
    return this.ws !== null && this.ws.readyState === WebSocket.OPEN;
  }

  waitUntilOpen(timeoutMs = 20_000): Promise<void> {
    if (this.isOpen()) return Promise.resolve();
    if (this.closed) return Promise.reject(new Error("VM bridge closed"));
    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        const idx = this.openWaiters.findIndex((w) => w.resolve === wrappedResolve);
        if (idx >= 0) this.openWaiters.splice(idx, 1);
        reject(new Error(`Timed out connecting to Boat VM ${this.sandbox.sandboxId}`));
      }, timeoutMs);
      const wrappedResolve = () => {
        clearTimeout(timer);
        resolve();
      };
      const wrappedReject = (err: Error) => {
        clearTimeout(timer);
        reject(err);
      };
      this.openWaiters.push({ resolve: wrappedResolve, reject: wrappedReject });
    });
  }

  private connect(): void {
    if (this.closed || this.sandbox.phase !== "running") return;
    const wsBase = this.sandbox.upstreamUrl.replace(/^https:\/\//, "wss://").replace(/^http:\/\//, "ws://");
    const targetUrl = `${wsBase}/ws?orchestrationProtocol=2`;

    const ws = new WebSocket(targetUrl, {
      headers: { ...(this.sandbox.upstreamHeaders ?? {}), authorization: `Bearer ${this.sandbox.upstreamToken}` },
    } as unknown as string[]);
    this.ws = ws;

    ws.addEventListener("open", () => {
      if (this.ws !== ws) return;
      for (const waiter of this.openWaiters.splice(0)) {
        waiter.resolve();
      }

      // Subscribe to this VM's orchestration shell stream
      this.pending.set(this.shellReqId, {
        onChunk: (values) => {
          this.sendRaw({ _tag: "Ack", requestId: this.shellReqId });
          this.onShellItems(this.sandbox.sandboxId, values);
        },
      });
      this.sendRaw({
        _tag: "Request",
        id: this.shellReqId,
        tag: "orchestration.subscribeShell",
        payload: {},
        headers: [],
      });

      // Keep upstream connection warm
      if (this.pingTimer) clearInterval(this.pingTimer);
      this.pingTimer = setInterval(() => {
        if (this.isOpen()) this.sendRaw({ _tag: "Ping" });
      }, 25_000);

      // Follow this VM's ServerConfig stream for the life of the connection, and harvest
      // real ServerConfig / lifecycle / vcsRefs in background from its first snapshot.
      const firstConfig = this.subscribeServerConfig();
      void this.harvestTemplates(ws, firstConfig);

      this.onOpen(this.sandbox.sandboxId);
    });

    ws.addEventListener("message", (ev) => {
      if (this.ws !== ws || typeof ev.data !== "string") return;
      for (const frame of decodeRpcFrames<RpcServerFrame>(ev.data)) {
        this.handleServerFrame(frame);
      }
    });

    ws.addEventListener("close", () => {
      if (this.ws !== ws) return;
      this.ws = null;
      if (this.pingTimer) {
        clearInterval(this.pingTimer);
        this.pingTimer = null;
      }
      // Fail any in-flight calls on disconnect
      for (const [upId, call] of this.pending) {
        this.pending.delete(upId);
        if (upId === this.shellReqId) continue;
        if (upId === this.configReqId) {
          // Settle a still-waiting first-snapshot promise; reconnect re-subscribes
          call.resolve?.({ _tag: "Failure", cause: hubDefectCause("Upstream VM disconnected") });
          continue;
        }
        if (call.resolve) {
          call.resolve({ _tag: "Failure", cause: hubDefectCause("Upstream VM disconnected") });
        } else if (call.onLost && call.clientWs && call.clientReqId !== undefined) {
          call.clientWs.data.upstreamRequests.delete(String(call.clientReqId));
          call.onLost();
        } else if (call.clientWs && call.clientReqId) {
          sendRpcExitFailure(call.clientWs, call.clientReqId, "Upstream VM disconnected");
        }
      }
      if (!this.closed && this.sandbox.phase === "running") {
        this.reconnectTimer = setTimeout(() => this.connect(), 2_000);
      }
    });

    ws.addEventListener("error", () => {
      // close handler follows
    });
  }

  /**
   * Opens the persistent `subscribeServerConfig` stream on the current connection. Every chunk is
   * Acked. Resolves with the first `snapshot` config, or null if none arrives within 10s or the
   * stream ends first. Later `providerStatuses` items (T3 publishes the real check result after
   * booting from its on-disk cache) go to `onProviderStatuses`.
   */
  private subscribeServerConfig(): Promise<Record<string, unknown> | null> {
    this.latestProviders = null;
    return new Promise((resolve) => {
      let settled = false;
      const settle = (cfg: Record<string, unknown> | null) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        resolve(cfg);
      };
      const timer = setTimeout(() => settle(null), 10_000);
      this.pending.set(this.configReqId, {
        onChunk: (values) => {
          this.sendRaw({ _tag: "Ack", requestId: this.configReqId });
          for (const raw of values) {
            if (!raw || typeof raw !== "object") continue;
            const item = raw as { type?: unknown; config?: unknown; payload?: unknown };
            if (item.type === "snapshot" && item.config && typeof item.config === "object") {
              const config = item.config as Record<string, unknown>;
              if (!settled) {
                settle(config);
              } else if (Array.isArray(config.providers)) {
                this.handleProviderStatuses(config.providers);
              }
            } else if (item.type === "providerStatuses") {
              const providers = (item.payload as { providers?: unknown } | undefined)?.providers;
              if (Array.isArray(providers)) this.handleProviderStatuses(providers);
            }
          }
        },
        // Stream ended (Exit) or bridge stopped before a snapshot arrived
        resolve: () => settle(null),
      });
      this.sendRaw({
        _tag: "Request",
        id: this.configReqId,
        tag: "subscribeServerConfig",
        payload: { workspaceSnapshotsOnDemand: true },
        headers: [],
      });
    });
  }

  private handleProviderStatuses(providers: unknown[]): void {
    this.latestProviders = providers;
    this.onProviderStatuses(this.sandbox.sandboxId, providers);
  }

  private async harvestTemplates(
    ws: WebSocket,
    firstConfig: Promise<Record<string, unknown> | null>,
  ): Promise<void> {
    try {
      const templates: { serverConfig?: Record<string, unknown>; lifecycleEvents?: unknown[]; vcsRefs?: unknown } = {};
      const snapshot = await firstConfig;
      if (this.ws !== ws) return; // reconnected meanwhile; the new connection harvests again
      if (snapshot) {
        templates.serverConfig = snapshot;
      } else {
        const cfg = await this.callUnary("server.getConfig", {});
        if (cfg._tag === "Success" && cfg.value && typeof cfg.value === "object") {
          templates.serverConfig = cfg.value as Record<string, unknown>;
        }
      }
      // Capture first lifecycle event chunk
      const lifecycle = await this.captureFirstChunk("subscribeServerLifecycle", {});
      if (lifecycle && lifecycle.length > 0) {
        templates.lifecycleEvents = lifecycle;
      }
      if (this.ws !== ws) return;
      // A live status published while harvesting is newer than the boot snapshot
      if (templates.serverConfig && this.latestProviders) {
        templates.serverConfig = { ...templates.serverConfig, providers: this.latestProviders };
      }
      this.onTemplatesHarvested(this.sandbox, templates);
    } catch {
      // non-fatal background harvest
    }
  }

  private captureFirstChunk(tag: string, payload: unknown): Promise<unknown[] | null> {
    return new Promise((resolve) => {
      if (!this.isOpen()) return resolve(null);
      const upReqId = `hub-cap-${++this.reqCounter}`;
      const timer = setTimeout(() => {
        this.pending.delete(upReqId);
        this.sendRaw({ _tag: "Interrupt", requestId: upReqId });
        resolve(null);
      }, 10_000);
      this.pending.set(upReqId, {
        onChunk: (values) => {
          clearTimeout(timer);
          this.pending.delete(upReqId);
          this.sendRaw({ _tag: "Interrupt", requestId: upReqId });
          resolve(values);
        },
        resolve: () => {
          clearTimeout(timer);
          resolve(null);
        },
      });
      this.sendRaw({ _tag: "Request", id: upReqId, tag, payload, headers: [] });
    });
  }

  callUnary(
    tag: string,
    payload: unknown,
    timeoutMs = 60_000,
  ): Promise<{ _tag: "Success"; value: unknown } | { _tag: "Failure"; cause: unknown }> {
    return new Promise((resolve) => {
      if (!this.isOpen()) {
        return resolve({ _tag: "Failure", cause: hubDefectCause("VM not connected") });
      }
      const upReqId = `hub-u-${++this.reqCounter}`;
      const timer = setTimeout(() => {
        this.pending.delete(upReqId);
        this.sendRaw({ _tag: "Interrupt", requestId: upReqId });
        resolve({ _tag: "Failure", cause: hubDefectCause(`${tag} timed out`) });
      }, timeoutMs);
      this.pending.set(upReqId, {
        tag,
        resolve: (exit) => {
          clearTimeout(timer);
          resolve(exit);
        },
      });
      this.sendRaw({ _tag: "Request", id: upReqId, tag, payload, headers: [] });
    });
  }

  forwardToClient(
    clientWs: ServerWebSocket<HubWsData>,
    clientReqId: string | number,
    tag: string,
    payload: unknown,
    headers: unknown = [],
    onExit?: PendingUpstreamCall["onExit"],
    onLost?: PendingUpstreamCall["onLost"],
  ): string {
    const upReqId = `hub-f-${++this.reqCounter}`;
    const rewrittenPayload = rewriteProjectIds(payload, this.sandbox.hubProjectId, this.sandbox.vmProjectId);
    this.pending.set(upReqId, {
      clientWs,
      clientReqId,
      vmProjectId: this.sandbox.vmProjectId,
      hubProjectId: this.sandbox.hubProjectId,
      tag,
      onExit,
      onLost,
    });
    clientWs.data.upstreamRequests.set(String(clientReqId), { sandboxId: this.sandbox.sandboxId, upReqId });
    this.sendRaw({ _tag: "Request", id: upReqId, tag, payload: rewrittenPayload, headers });
    return upReqId;
  }

  sendAck(upReqId: string): void {
    this.sendRaw({ _tag: "Ack", requestId: upReqId });
  }

  sendInterrupt(upReqId: string): void {
    this.pending.delete(upReqId);
    this.sendRaw({ _tag: "Interrupt", requestId: upReqId });
  }

  private sendRaw(frame: Record<string, unknown>): void {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      try {
        this.ws.send(JSON.stringify(frame));
      } catch {
        // ignore send errors on closing socket
      }
    }
  }

  private handleServerFrame(frame: RpcServerFrame): void {
    if (frame._tag === "Ping") {
      this.sendRaw({ _tag: "Pong" });
      return;
    }
    if (frame._tag === "Pong") return;

    const reqId = frame.requestId !== undefined ? String(frame.requestId) : undefined;
    if (!reqId) return;
    const call = this.pending.get(reqId);
    if (!call) return;

    if (frame._tag === "Chunk" && Array.isArray(frame.values)) {
      if (call.onChunk) {
        call.onChunk(frame.values);
        return;
      }
      if (call.clientWs && call.clientReqId) {
        const rewritten =
          call.vmProjectId && call.hubProjectId
            ? rewriteProjectIds(frame.values, call.vmProjectId, call.hubProjectId)
            : frame.values;
        sendRpcFrame(call.clientWs, { _tag: "Chunk", requestId: call.clientReqId, values: rewritten });
      }
      return;
    }

    if (frame._tag === "Exit" && frame.exit) {
      this.pending.delete(reqId);
      if (call.resolve) {
        call.resolve(frame.exit);
        return;
      }
      if (call.onExit) {
        try {
          call.onExit(frame.exit);
        } catch (err) {
          console.warn("[hub] upstream exit hook failed:", err instanceof Error ? err.message : err);
        }
      }
      if (call.clientWs && call.clientReqId !== undefined) {
        call.clientWs.data.upstreamRequests.delete(String(call.clientReqId));
        let exit: { _tag: "Success"; value: unknown } | { _tag: "Failure"; cause: unknown } = frame.exit;
        if (exit._tag === "Success") {
          // Track asset/upload URL tokens issued by this VM so HTTP GET /api/assets & POST /api/attachments route back to it
          this.inspectIssuedTokens(call.tag, exit.value);
          if (call.vmProjectId && call.hubProjectId) {
            exit = {
              _tag: "Success",
              value: rewriteProjectIds(exit.value, call.vmProjectId, call.hubProjectId),
            };
          }
        }
        sendRpcFrame(call.clientWs, { _tag: "Exit", requestId: call.clientReqId, exit });
      }
    }
  }

  private inspectIssuedTokens(tag: string | undefined, value: unknown): void {
    for (const token of issuedUrlTokens(tag, value)) this.onUrlTokenIssued(this.sandbox.sandboxId, token);
  }
}

/** URL tokens in an upload or asset URL a VM issued, so `/api/assets/<token>` and uploads route back to it. */
export function issuedUrlTokens(tag: string | undefined, value: unknown): string[] {
  if (!value || typeof value !== "object") return [];
  const obj = value as Record<string, unknown>;
  const tokens: string[] = [];
  const urlStr =
    (typeof obj.relativeUrl === "string" && obj.relativeUrl) ||
    (typeof obj.uploadUrl === "string" && obj.uploadUrl) ||
    (typeof obj.url === "string" && obj.url) ||
    null;
  const m = urlStr ? /\/api\/(?:attachments\/upload|assets)\/([^/?#]+)/.exec(urlStr) : null;
  if (m?.[1]) tokens.push(m[1]);
  if (tag === "assets.createUrl" && typeof obj.token === "string") tokens.push(obj.token);
  return tokens;
}

/**
 * Single-Environment Hub Multiplexer (`https://boat.<domain>`):
 * Presents ONE T3 Code Environment containing all configured projects and dynamically spawns
 * 1 fresh, isolated Boat VM per chat thread (unlimited parallel sandboxes behind 1 Environment!).
 * Auto-destroys each VM when its thread is settled or archived.
 */
export class HubServer {
  private readonly auth = new SlotAuth();
  private readonly bridges = new Map<string, HubVmBridge>();
  private readonly clients = new Set<ServerWebSocket<HubWsData>>();
  private readonly threadToSandbox = new Map<string, string>();
  private readonly worktreeToSandbox = new Map<string, string>();
  private readonly threadShells = new Map<string, Record<string, unknown>>();
  private readonly inFlightSpawns = new Map<string, Promise<HubSandboxRecord>>();
  private readonly inFlightWakes = new Map<string, Promise<HubSandboxRecord>>();
  private readonly urlTokenToSandbox = new Map<string, string>();
  private readonly stagedAttachments = new Map<string, StagedAttachment>();
  private readonly stagedUploadTokens = new Map<string, string>(); // uploadToken -> attachmentId
  /** Attachment ids a VM issued when the Hub replayed a staged upload into it -> that VM. */
  private readonly attachmentToSandbox = new Map<string, string>();
  private readonly worktreeSetupSnapshots = new Map<string, WorktreeSetupSnapshotState>();
  private readonly worktreeSetupSubs = new Map<
    string,
    Set<{ ws: ServerWebSocket<HubWsData>; reqKey: string; reqId: string | number }>
  >();
  private readonly activeProjectClones = new Map<string, ProjectCloneSnapshotState>();
  private readonly destroyingSandboxes = new Set<string>();
  /** Self-update rounds queued or running, by target version: a request for one of them joins it. */
  private readonly selfUpdateRuns = new Map<string, SelfUpdateRun>();
  /** Tail of the self-update queue: rounds for different versions run one after another. */
  private selfUpdateQueue: Promise<unknown> = Promise.resolve();
  /** Settings updates and overlay replays run one at a time, in arrival order. */
  private settingsQueue: Promise<unknown> = Promise.resolve();
  /** Overlay replay of each VM's latest connection, awaited before a spawned or woken VM is used. */
  private readonly settingsReplays = new Map<string, Promise<void>>();
  /** A VM's settings after its replay on the current connection; newer than its boot snapshot. */
  private readonly replayedSettings = new Map<string, Record<string, unknown>>();

  private readonly boat: SandboxProvider;
  private readonly mcp: HubMcpHandler;

  constructor(
    private readonly config: SentinelConfig,
    private readonly state: StateStore,
    provider: SandboxProvider | LegacyBoatClient,
    private readonly lifecycle: LifecycleManager,
    private readonly klav?: KlavClient,
    private readonly rateLimiter?: AuthRateLimiter,
  ) {
    this.boat = asSandboxProvider(provider);
    this.mcp = new HubMcpHandler(
      config,
      state,
      {
        callVmUnary: async (sandboxId, tag, payload, timeoutMs) => {
          const sb = this.state.getHubSandbox(sandboxId);
          if (!sb || sb.phase !== "running") {
            return { _tag: "Failure", cause: hubDefectCause(`Sandbox ${sandboxId} is not running`) };
          }
          const bridge = await this.ensureBridgeOpen(sb);
          return bridge.callUnary(tag, payload, timeoutMs);
        },
        getThreadShell: (threadId) => this.threadShells.get(threadId),
        listThreadShells: () =>
          Array.from(this.threadShells.entries()).map(([threadId, shell]) => ({
            threadId,
            shell,
            sandboxId: this.threadToSandbox.get(threadId) ?? null,
          })),
        getSandboxForThread: (threadId) => {
          const sbId = this.threadToSandbox.get(threadId);
          return sbId ? this.state.getHubSandbox(sbId) : undefined;
        },
        wakeSandbox: (sandboxId) => this.wakeHubSandbox(sandboxId),
        destroySandbox: (sandboxId, reason) => this.destroyHubSandbox(sandboxId, reason),
        sleepSandbox: (sandboxId) => this.sleepHubSandbox(sandboxId),
        projectKeyFromHubId: (hubProjectId) => this.projectKeyFromHubId(hubProjectId),
        ensureHubProjectId: (projectKey) => {
          const hub = this.state.ensureHub(
            this.config.hub?.label ?? "Boat",
            Object.keys(this.state.getAllProjects(this.config)),
          );
          if (hub.projectIds[projectKey]) return hub.projectIds[projectKey];
          const id = randomUUID();
          this.state.updateHub((h) => {
            h.projectIds[projectKey] = id;
          });
          return id;
        },
        spawnSandboxForThread: (projectKey, threadId, onProgress) =>
          this.spawnSandboxForThread(projectKey, threadId, onProgress),
        bindThreadToSandbox: (threadId, sandboxId) => this.bindThreadToSandbox(threadId, sandboxId),
        bindWorktreeToSandbox: (worktreePath, sandboxId) => this.bindWorktreeToSandbox(worktreePath, sandboxId),
        notifyProjectAdded: (projectKey) => this.notifyProjectAdded(projectKey),
        startProjectBake: (projectKey) => {
          void this.lifecycle.bake(projectKey).catch((err) => {
            console.error(`[hub-mcp] background bake failed for "${projectKey}":`, err);
          });
        },
        providerName: () => this.boat.name,
        anyRunningSandbox: () => this.anyRunningSandbox(),
      },
      rateLimiter,
    );
    if (config.hub) {
      this.state.ensureHub(config.hub.label, Object.keys(this.state.getAllProjects(config)));
      this.restoreLiveSandboxes();
    }
  }

  private restoreLiveSandboxes(): void {
    for (const sb of this.state.listHubSandboxes()) {
      for (const tid of sb.threadIds) this.threadToSandbox.set(tid, sb.sandboxId);
      for (const wt of sb.worktreePaths) this.worktreeToSandbox.set(wt, sb.sandboxId);
      if (sb.phase === "running") {
        this.attachBridge(sb);
      }
    }
  }

  stop(): void {
    for (const bridge of this.bridges.values()) {
      bridge.stop();
    }
    this.bridges.clear();
    this.lifecycle.providerSessions.stop();
  }

  /**
   * For a sign-out of a synced provider instance, returns a hook that forgets the stored session
   * once the VM confirms it. A mere `unauthenticated` status never deletes it (it can be transient).
   */
  /**
   * Answers a provider setup stream with an idle snapshot and keeps it open, so the panel shows a
   * settled state (not "Checking…" or an error) while no VM runs. A bridge that opens later takes
   * the stream over (`adoptLocalProviderStreams`).
   */
  private serveProviderStreamLocally(
    ws: ServerWebSocket<HubWsData>,
    reqId: string | number,
    tag: string,
    payload: unknown,
  ): void {
    if (!this.clients.has(ws)) return;
    const instanceId = providerInstanceIdOf(payload);
    if (!instanceId) {
      sendRpcExitFailure(ws, reqId, `Invalid instanceId for ${tag}`);
      return;
    }
    ws.data.upstreamRequests.delete(String(reqId));
    ws.data.localStreams.set(String(reqId), { rawId: reqId, tag, payload });
    const providers = this.buildHubServerConfig().providers;
    sendRpcFrame(ws, { _tag: "Chunk", requestId: reqId, values: [idleProviderSetupState(tag, instanceId, providers)] });
  }

  /**
   * Forwards a provider setup stream to a VM. If that VM goes away, the stream moves to another open
   * VM, or falls back to the idle snapshot when none is open.
   */
  private forwardProviderStream(
    bridge: HubVmBridge,
    ws: ServerWebSocket<HubWsData>,
    reqId: string | number,
    tag: string,
    payload: unknown,
    headers: unknown,
  ): void {
    const lostSandboxId = bridge.sandbox.sandboxId;
    bridge.forwardToClient(ws, reqId, tag, payload, headers, undefined, () => {
      if (!this.clients.has(ws)) return;
      // A stopping bridge still reports open while it fails its calls, so skip it by id
      const next = Array.from(this.bridges.values()).find(
        (b) => b.sandbox.sandboxId !== lostSandboxId && b.sandbox.phase === "running" && b.isOpen(),
      );
      if (next) {
        this.forwardProviderStream(next, ws, reqId, tag, payload, headers);
      } else {
        this.serveProviderStreamLocally(ws, reqId, tag, payload);
      }
    });
  }

  /** Hands provider setup streams the Hub answered locally to a VM whose bridge just opened. */
  private adoptLocalProviderStreams(sandboxId: string): void {
    const bridge = this.bridges.get(sandboxId);
    if (!bridge?.isOpen()) return;
    for (const client of this.clients) {
      for (const [key, sub] of client.data.localStreams) {
        if (!PROVIDER_SETUP_STREAMS.has(sub.tag)) continue;
        client.data.localStreams.delete(key);
        this.forwardProviderStream(bridge, client, sub.rawId, sub.tag, sub.payload, []);
      }
    }
  }

  private signOutHook(tag: string, payload: unknown): PendingUpstreamCall["onExit"] {
    if (!PROVIDER_LOGOUT_METHODS.has(tag)) return undefined;
    const instanceId = (payload as { instanceId?: unknown } | null)?.instanceId;
    if (typeof instanceId !== "string" || !this.lifecycle.providerSessions.isEnabled(instanceId)) return undefined;
    return (exit) => {
      if (exit._tag !== "Success") return;
      this.lifecycle.providerSessions.forget(instanceId);
    };
  }

  createPairingLink(): { url: string; code: string; expiresAt: string } {
    if (!this.config.hub) throw new Error("Hub is not configured in sentinel.yaml");
    const { code, expiresAt } = this.auth.newPairingCode();
    const url = `https://${this.config.hub.host}/pair#token=${encodeURIComponent(code)}`;
    return { url, code, expiresAt };
  }

  /** Mint a long-lived machine Bearer token for headless MCP clients (e.g. Zumith). Stored hashed only. */
  mintClientToken(label = "machine"): { clientId: string; token: string; label: string; pairedAt: string } {
    if (!this.config.hub) throw new Error("Hub is not configured in sentinel.yaml");
    const token = randomToken(32);
    const clientId = randomUUID();
    const pairedAt = new Date().toISOString();
    const cleanLabel = label.trim() || "machine";
    this.state.updateHub((h) => {
      h.clients.push({
        id: clientId,
        tokenHash: hashToken(token),
        label: cleanLabel,
        pairedAt,
        lastSyncedSandboxId: null,
      });
    });
    console.log(`[hub] minted machine Bearer token for "${cleanLabel}" (clientId=${clientId})`);
    return { clientId, token, label: cleanLabel, pairedAt };
  }

  getAuth(): SlotAuth {
    return this.auth;
  }

  private buildDescriptor(): Record<string, unknown> {
    const hub = this.state.ensureHub(this.config.hub?.label ?? "Boat", Object.keys(this.state.getAllProjects(this.config)));
    const base = this.state.getTemplates().descriptor ?? {
      environmentId: hub.environmentId,
      label: hub.label,
      platform: { os: "linux", arch: "x64" },
      serverVersion: this.config.t3.version === "latest" ? "0.0.0" : this.config.t3.version,
      orchestrationProtocolVersion: 2,
      capabilities: {
        repositoryIdentity: true,
        connectionProbe: true,
        attachmentUploads: true,
        projectCloneTracking: true,
      },
    };
    const caps: Record<string, unknown> = {
      ...((base.capabilities as Record<string, unknown> | undefined) ?? {}),
      projectCloneTracking: true,
    };
    delete caps.accessTokenRenewal;
    return {
      ...base,
      environmentId: hub.environmentId,
      label: hub.label,
      capabilities: caps,
    };
  }

  private buildAuthSession(): Record<string, unknown> {
    const expiresAt = new Date(Date.now() + CLIENT_TOKEN_TTL_SECS * 1000).toISOString();
    const base = this.state.getTemplates().authSession ?? {
      authenticated: true,
      auth: {
        policy: "remote-reachable",
        bootstrapMethods: ["one-time-token"],
        sessionMethods: ["bearer-access-token"],
        sessionCookieName: "t3_session",
      },
      scopes: [...DEFAULT_T3_SCOPES],
      permissions: [...DEFAULT_T3_PERMISSIONS],
      sessionMethod: "bearer-access-token",
    };
    const authDesc =
      base.auth && typeof base.auth === "object"
        ? { ...(base.auth as Record<string, unknown>), sessionMethods: ["bearer-access-token"] }
        : {
            policy: "remote-reachable",
            bootstrapMethods: ["one-time-token"],
            sessionMethods: ["bearer-access-token"],
            sessionCookieName: "t3_session",
          };
    return {
      ...base,
      authenticated: true,
      auth: authDesc,
      scopes: Array.isArray(base.scopes) && base.scopes.length > 0 ? base.scopes : [...DEFAULT_T3_SCOPES],
      permissions:
        Array.isArray(base.permissions) && base.permissions.length > 0
          ? base.permissions
          : [...DEFAULT_T3_PERMISSIONS],
      sessionMethod: "bearer-access-token",
      expiresAt,
    };
  }

  private buildHubServerConfig(): Record<string, unknown> {
    const hub = this.state.ensureHub(this.config.hub?.label ?? "Boat", Object.keys(this.state.getAllProjects(this.config)));
    const desc = this.buildDescriptor();
    const cached = hub.serverConfig;
    if (cached && typeof cached === "object") {
      const cachedAuth =
        cached.auth && typeof cached.auth === "object"
          ? {
              ...(cached.auth as Record<string, unknown>),
              policy: "remote-reachable",
              sessionMethods: ["bearer-access-token"],
            }
          : {
              policy: "remote-reachable",
              bootstrapMethods: ["one-time-token"],
              sessionMethods: ["bearer-access-token"],
              sessionCookieName: "t3_session",
            };
      return {
        ...cached,
        environment: desc,
        auth: cachedAuth,
      };
    }
    // Fallback before first VM/bake harvests the real ServerConfig
    return {
      environment: desc,
      auth: {
        policy: "remote-reachable",
        bootstrapMethods: ["one-time-token"],
        sessionMethods: ["bearer-access-token"],
        sessionCookieName: "t3_session",
      },
      cwd: "/home/user/workspace",
      keybindingsConfigPath: "/home/user/.t3/userdata/keybindings.json",
      keybindings: [],
      issues: [],
      providers: [
        {
          instanceId: "claudeAgent",
          driver: "claudeAgent",
          enabled: true,
          installed: true,
          version: "2.0.0",
          status: "ready",
          auth: { status: "authenticated" },
          checkedAt: new Date().toISOString(),
          models: [
            { slug: "claude-opus-5-5", name: "Opus 5.5", isCustom: false },
            { slug: "claude-sonnet-4-6", name: "Sonnet 4.6", isCustom: false },
          ],
          slashCommands: [],
          skills: [],
        },
      ],
      availableEditors: [],
      settings: {},
    };
  }

  private buildProjectShell(key: string, proj: ProjectConfig): Record<string, unknown> {
    const hub = this.state.ensureHub(this.config.hub?.label ?? "Boat", Object.keys(this.state.getAllProjects(this.config)));
    const now = new Date().toISOString();
    const projectId = hub.projectIds[key] ?? randomUUID();
    if (!hub.projectIds[key]) {
      this.state.updateHub((h) => {
        h.projectIds[key] = projectId;
      });
    }
    const cached = hub.projectTemplates[key]?.shell;
    if (cached && typeof cached === "object") {
      return {
        ...cached,
        id: projectId,
        title: key,
        workspaceRoot: proj.workdir,
      };
    }
    return {
      id: projectId,
      title: key,
      workspaceRoot: proj.workdir,
      defaultModelSelection: null,
      scripts: [],
      createdAt: now,
      updatedAt: now,
      deletedAt: null,
    };
  }

  private buildHubProjects(): Record<string, unknown>[] {
    const allProjects = this.state.getAllProjects(this.config);
    const out: Record<string, unknown>[] = [];
    for (const [key, proj] of Object.entries(allProjects)) {
      if (proj.registerProject === false) continue;
      out.push(this.buildProjectShell(key, proj));
    }
    return out;
  }

  /** Push a newly registered project live into all connected T3 Desktop & Mobile clients. */
  notifyProjectAdded(projectKey: string): void {
    const proj = this.state.getAllProjects(this.config)[projectKey];
    if (!proj || proj.registerProject === false) return;
    const shell = this.buildProjectShell(projectKey, proj);
    this.broadcastShellItem({
      kind: "project.updated",
      sequence: this.state.nextHubSequence(),
      project: shell,
    });
  }

  /** Notify all connected T3 Desktop & Mobile clients that a dynamic project was removed. */
  notifyProjectRemoved(projectId: string): void {
    this.broadcastShellItem({
      kind: "project.removed",
      sequence: this.state.nextHubSequence(),
      projectId,
    });
  }

  private buildMergedShellSnapshot(): Record<string, unknown> {
    const hub = this.state.ensureHub(this.config.hub?.label ?? "Boat", Object.keys(this.state.getAllProjects(this.config)));
    return {
      schemaVersion: 2,
      snapshotSequence: hub.sequence,
      projects: this.buildHubProjects(),
      threads: Array.from(this.threadShells.values()),
      archivedThreads: [],
    };
  }

  async handleHttp(req: Request, server: Bun.Server<HubWsData>): Promise<Response | undefined> {
    if (req.method === "OPTIONS") {
      return withCors(new Response(null, { status: 204 }));
    }

    const url = new URL(req.url);
    const path = url.pathname;
    if (req.method === "GET" && path === "/robots.txt") {
      return applySecurityHeaders(new Response(ROBOTS_TXT, { headers: { "content-type": "text/plain; charset=utf-8" } }));
    }
    if (req.method === "GET" && path === "/health") {
      const activeSandboxes = this.state.listHubSandboxes().filter((s) => s.phase !== "destroying").length;
      return withCors(
        Response.json({
          ok: true,
          provider: this.boat.name,
          activeSandboxes,
          maxSandboxes: this.config.hub?.maxSandboxes ?? 10,
        }),
      );
    }
    if (path === "/mcp" || path === "/mcp/") {
      return this.mcp.handleHttp(req);
    }
    const ip = clientIpFromRequest(req);
    const hub = this.state.ensureHub(this.config.hub?.label ?? "Boat", Object.keys(this.state.getAllProjects(this.config)));

    // 1. Discovery descriptor
    if (req.method === "GET" && path === "/.well-known/t3/environment") {
      return withCors(Response.json(this.buildDescriptor()));
    }

    // 2. Pairing token exchange (rate-limited against brute-force guessing)
    if (req.method === "POST" && path === "/oauth/token") {
      if (this.rateLimiter) {
        const limit = this.rateLimiter.check("hub-pair", ip, 5, 15 * 60_000);
        if (!limit.allowed) return withCors(rateLimitedResponse(limit.retryAfterSecs));
      }
      const form = new URLSearchParams(await req.text());
      const code = form.get("subject_token") ?? "";
      if (!this.auth.redeemPairingCode(code)) {
        this.rateLimiter?.recordFailure("hub-pair", ip, 15 * 60_000);
        return withCors(Response.json({ error: "invalid_grant" }, { status: 400 }));
      }
      this.rateLimiter?.recordSuccess("hub-pair", ip);
      const token = randomToken(32);
      const label = form.get("client_label")?.trim() || form.get("client_device_type")?.trim() || "T3 client";
      this.state.updateHub((h) => {
        h.clients.push({
          id: randomUUID(),
          tokenHash: hashToken(token),
          label,
          pairedAt: new Date().toISOString(),
          lastSyncedSandboxId: null,
        });
      });
      const session = this.buildAuthSession();
      const scopes = (session.scopes as string[]).join(" ");
      console.log(`[hub] paired new T3 client (${label})`);
      return withCors(
        Response.json({
          access_token: token,
          issued_token_type: "urn:ietf:params:oauth:token-type:access_token",
          token_type: "Bearer",
          expires_in: CLIENT_TOKEN_TTL_SECS,
          scope: scopes,
        }),
      );
    }

    // 3. Session probe
    if (req.method === "GET" && path === "/api/auth/session") {
      if (!authenticateClient(hub.clients, req.headers)) {
        return withCors(Response.json({ error: "unauthorized" }, { status: 401 }));
      }
      return withCors(Response.json(this.buildAuthSession()));
    }

    // 4. WebSocket tickets
    const ticketMatch = /^\/api\/auth\/websocket-ticket(?:\/(rpc|media))?$/.exec(path);
    if (req.method === "POST" && ticketMatch) {
      const client = authenticateClient(hub.clients, req.headers);
      if (!client) return withCors(Response.json({ error: "unauthorized" }, { status: 401 }));
      const use = (ticketMatch[1] ?? "rpc") as TicketUse;
      return withCors(Response.json(this.auth.issueTicket(use, client.id)));
    }

    // 5. Traces no-op
    if (req.method === "POST" && path === "/api/observability/v1/traces") {
      return withCors(new Response(null, { status: 200 }));
    }

    // 6. Merged HTTP shell snapshot
    if (req.method === "GET" && path === "/api/orchestration/shell") {
      if (!authenticateClient(hub.clients, req.headers)) {
        return withCors(Response.json({ error: "unauthorized" }, { status: 401 }));
      }
      return withCors(Response.json(this.buildMergedShellSnapshot()));
    }

    // 7. Attachment upload route (staged locally if pre-spawn, else proxied to owning VM)
    if (req.method === "POST" && path.startsWith("/api/attachments/upload/")) {
      const uploadToken = path.slice("/api/attachments/upload/".length);
      const stagedId = this.stagedUploadTokens.get(uploadToken);
      if (stagedId) {
        const entry = this.stagedAttachments.get(stagedId);
        if (!entry) return withCors(new Response("Not Found", { status: 404 }));
        entry.bytes = new Uint8Array(await req.arrayBuffer());
        this.stagedUploadTokens.delete(uploadToken);
        return withCors(new Response(null, { status: 204 }));
      }
      const sandboxId = this.urlTokenToSandbox.get(uploadToken);
      const sb = sandboxId ? this.state.getHubSandbox(sandboxId) : undefined;
      if (sb && sb.phase === "running") {
        return this.proxyHttpToSandbox(sb, req, url, true);
      }
      return withCors(new Response("Not Found", { status: 404 }));
    }

    // 8. Asset GET route (proxied to owning VM)
    if ((req.method === "GET" || req.method === "HEAD") && path.startsWith("/api/assets/")) {
      const suffix = path.slice("/api/assets/".length);
      const token = suffix.split("/")[0] ?? "";
      const sandboxId = this.urlTokenToSandbox.get(token);
      const sb = sandboxId ? this.state.getHubSandbox(sandboxId) : this.anyRunningSandbox();
      if (sb && sb.phase === "running") {
        return this.proxyHttpToSandbox(sb, req, url, true);
      }
      return withCors(new Response("Not Found", { status: 404 }));
    }

    // 9. WebSocket upgrade
    if (req.headers.get("upgrade")?.toLowerCase() === "websocket") {
      const isMedia = path.startsWith("/api/device-hub/") || path.startsWith("/api/preview-stream/");
      const ticketUse: TicketUse = isMedia ? "media" : "rpc";
      const wsTicket = url.searchParams.get("wsTicket");
      const ticketClientId = wsTicket ? this.auth.checkTicket(wsTicket, ticketUse) : null;
      const bearerClient = ticketClientId ? null : authenticateClient(hub.clients, req.headers);
      const clientId = ticketClientId ?? bearerClient?.id ?? null;
      if (!clientId) {
        return withCors(Response.json({ error: "unauthorized" }, { status: 401 }));
      }
      const upgraded = server.upgrade(req, {
        data: {
          kind: "hub",
          clientId,
          localStreams: new Map(),
          upstreamRequests: new Map(),
        },
      });
      if (!upgraded) return withCors(new Response("WebSocket upgrade failed", { status: 400 }));
      return undefined;
    }

    // 10. Other authenticated HTTP requests -> forward to any running sandbox if available
    if (!authenticateClient(hub.clients, req.headers)) {
      return withCors(Response.json({ error: "unauthorized" }, { status: 401 }));
    }
    const fallback = this.anyRunningSandbox();
    if (fallback) {
      return this.proxyHttpToSandbox(fallback, req, url, false);
    }
    return withCors(Response.json({ error: "not_found" }, { status: 404 }));
  }

  onWsOpen(ws: ServerWebSocket<HubWsData>): void {
    this.clients.add(ws);
    console.log(`[hub-ws] client connected (clientId=${ws.data.clientId}, total=${this.clients.size})`);
  }

  onWsClose(ws: ServerWebSocket<HubWsData>, code?: number, reason?: string): void {
    this.clients.delete(ws);
    console.log(`[hub-ws] client disconnected (clientId=${ws.data.clientId}, code=${code ?? 0}, reason=${reason ?? ""})`);
    for (const [, ref] of ws.data.upstreamRequests) {
      this.bridges.get(ref.sandboxId)?.sendInterrupt(ref.upReqId);
    }
    this.removeWorktreeSetupSubsForWs(ws);
    ws.data.upstreamRequests.clear();
    ws.data.localStreams.clear();
  }

  onWsMessage(ws: ServerWebSocket<HubWsData>, msg: string | Buffer): void {
    const raw = typeof msg === "string" ? msg : new TextDecoder().decode(msg);
    const frames = decodeRpcFrames<RpcClientFrame>(raw);
    if (frames.length === 0) {
      console.warn(`[hub-ws] unparsed frame (${raw.length}B): ${raw.slice(0, 120)}`);
      return;
    }
    for (const frame of frames) {
      if (frame._tag === "Request") {
        console.log(`[hub-ws] req id=${JSON.stringify(frame.id)} (${typeof frame.id}) tag=${frame.tag}`);
      }
      void this.handleClientFrame(ws, frame).catch((err) => {
        console.error("[hub] client frame error:", err);
      });
    }
  }

  private async handleClientFrame(ws: ServerWebSocket<HubWsData>, frame: RpcClientFrame): Promise<void> {
    if (frame._tag === "Ping") {
      sendRpcFrame(ws, { _tag: "Pong" });
      return;
    }
    if (frame._tag === "Pong") return;

    if (frame._tag === "Ack") {
      const reqKey = frame.requestId !== undefined ? String(frame.requestId) : "";
      const ref = ws.data.upstreamRequests.get(reqKey);
      if (ref) {
        this.bridges.get(ref.sandboxId)?.sendAck(ref.upReqId);
      }
      return;
    }

    if (frame._tag === "Interrupt") {
      const rawReqId = frame.requestId ?? "";
      const reqKey = String(rawReqId);
      ws.data.localStreams.delete(reqKey);
      this.removeWorktreeSetupSub(ws, reqKey);
      const ref = ws.data.upstreamRequests.get(reqKey);
      if (ref) {
        ws.data.upstreamRequests.delete(reqKey);
        this.bridges.get(ref.sandboxId)?.sendInterrupt(ref.upReqId);
      }
      // A real T3 server ends an interrupted request with an Interrupt cause
      sendRpcFrame(ws, {
        _tag: "Exit",
        requestId: rawReqId,
        exit: { _tag: "Failure", cause: [{ _tag: "Interrupt", fiberId: null }] },
      });
      return;
    }

    if (frame._tag !== "Request" || frame.id === undefined || !frame.tag) return;
    const reqId = frame.id;
    const reqKey = String(frame.id);
    const tag = frame.tag;
    const payload = frame.payload;
    const headers = frame.headers ?? [];

    // 1. Environment-level Hub RPCs
    if (tag === "server.getConfig") {
      sendRpcExitSuccess(ws, reqId, this.buildHubServerConfig());
      return;
    }
    if (tag === "subscribeServerConfig") {
      ws.data.localStreams.set(reqKey, { rawId: reqId, tag });
      sendRpcFrame(ws, {
        _tag: "Chunk",
        requestId: reqId,
        values: [{ version: 1, type: "snapshot", config: this.buildHubServerConfig() }],
      });
      return;
    }
    if (tag === "subscribeServerLifecycle") {
      ws.data.localStreams.set(reqKey, { rawId: reqId, tag });
      const hub = this.state.getHub();
      const desc = this.buildDescriptor();
      const events =
        hub?.lifecycleEvents && hub.lifecycleEvents.length > 0
          ? hub.lifecycleEvents.map((ev) => {
              if (ev && typeof ev === "object") {
                const obj = ev as Record<string, unknown>;
                const p = obj.payload && typeof obj.payload === "object" ? (obj.payload as Record<string, unknown>) : null;
                if (p && "environment" in p) {
                  return { ...obj, payload: { ...p, environment: desc } };
                }
              }
              return ev;
            })
          : [
              {
                version: 1,
                sequence: 1,
                type: "ready",
                payload: { at: new Date().toISOString(), environment: desc },
              },
            ];
      sendRpcFrame(ws, { _tag: "Chunk", requestId: reqId, values: events });
      return;
    }
    if (tag === "subscribeAuthAccess") {
      ws.data.localStreams.set(reqKey, { rawId: reqId, tag });
      const hub = this.state.getHub();
      sendRpcFrame(ws, {
        _tag: "Chunk",
        requestId: reqId,
        values: [
          {
            revision: 1,
            sessions: (hub?.clients ?? []).map((c) => ({
              sessionId: c.id,
              subject: c.label,
              method: "bearer-access-token",
              connected: true,
              createdAt: c.pairedAt,
            })),
          },
        ],
      });
      return;
    }
    if (tag === "orchestration.subscribeShell") {
      ws.data.localStreams.set(reqKey, { rawId: reqId, tag });
      const snapshot = this.buildMergedShellSnapshot();
      const items: unknown[] = [{ kind: "snapshot", snapshot }];
      if ((payload as Record<string, unknown> | undefined)?.requestCompletionMarker) {
        items.push({ kind: "synchronized" });
      }
      sendRpcFrame(ws, { _tag: "Chunk", requestId: reqId, values: items });
      return;
    }
    if (tag === "orchestration.getArchivedShellSnapshot") {
      const hub = this.state.ensureHub();
      sendRpcExitSuccess(ws, reqId, {
        schemaVersion: 2,
        snapshotSequence: hub.sequence,
        projects: this.buildHubProjects(),
        threads: [],
      });
      return;
    }
    if (tag === "orchestration.getOlderSettledThreads") {
      sendRpcExitSuccess(ws, reqId, { threads: [], nextCursor: null });
      return;
    }
    if (tag === "subscribeTerminalMetadata") {
      ws.data.localStreams.set(reqKey, { rawId: reqId, tag });
      sendRpcFrame(ws, { _tag: "Chunk", requestId: reqId, values: [{ kind: "snapshot", threads: [] }] });
      return;
    }
    if (tag === "subscribeWorktreeSetup") {
      const tid = extractThreadId(payload);
      ws.data.localStreams.set(reqKey, { rawId: reqId, tag });
      if (tid) {
        let subs = this.worktreeSetupSubs.get(tid);
        if (!subs) {
          subs = new Set();
          this.worktreeSetupSubs.set(tid, subs);
        }
        subs.add({ ws, reqKey, reqId });
        sendRpcFrame(ws, {
          _tag: "Chunk",
          requestId: reqId,
          values: [this.worktreeSetupSnapshots.get(tid) ?? null],
        });
      }
      return;
    }
    if (tag === "worktreeSetup.cancel") {
      sendRpcExitSuccess(ws, reqId, { cancelled: false });
      return;
    }
    if (tag === "subscribeProjectClones") {
      ws.data.localStreams.set(reqKey, { rawId: reqId, tag });
      sendRpcFrame(ws, {
        _tag: "Chunk",
        requestId: reqId,
        values: [Array.from(this.activeProjectClones.values())],
      });
      return;
    }
    if (tag === "projectClone.start") {
      await this.handleProjectCloneStart(ws, reqId, (payload ?? {}) as Record<string, unknown>);
      return;
    }
    if (tag === "projectClone.cancel") {
      const pid = String((payload as Record<string, unknown> | undefined)?.projectId ?? "");
      const existing = this.activeProjectClones.get(pid);
      if (existing) {
        this.activeProjectClones.delete(pid);
        this.broadcastProjectClones();
        sendRpcExitSuccess(ws, reqId, { applied: true });
      } else {
        sendRpcExitSuccess(ws, reqId, { applied: false });
      }
      return;
    }
    if (tag === "projectClone.retry") {
      const pid = String((payload as Record<string, unknown> | undefined)?.projectId ?? "");
      const projKey = this.projectKeyFromHubId(pid);
      const existing = this.activeProjectClones.get(pid);
      if (projKey && existing) {
        this.startBackgroundProjectBake(pid, projKey, existing.remoteUrl, existing.destinationPath);
        sendRpcExitSuccess(ws, reqId, { applied: true });
      } else {
        sendRpcExitSuccess(ws, reqId, { applied: false });
      }
      return;
    }
    if (tag === "subscribeVcsStatus" || tag === "subscribeVcsBranch") {
      await this.handleWorkspaceRpc(ws, reqId, tag, payload, headers);
      return;
    }
    if (KEEP_OPEN_SUBSCRIPTIONS.has(tag)) {
      ws.data.localStreams.set(reqKey, { rawId: reqId, tag });
      const live = this.anyRunningSandbox();
      const bridge = live ? this.bridges.get(live.sandboxId) : undefined;
      if (bridge?.isOpen()) {
        bridge.forwardToClient(ws, reqId, tag, payload, headers);
      }
      return;
    }
    if (tag === "server.reportClientActivity" || tag === "preview.reportProfiles" || tag === "preview.reportStatus") {
      sendRpcExitSuccess(ws, reqId, undefined);
      return;
    }
    if (tag === "server.refreshProviders") {
      const forwarded = await this.forwardRefreshProviders(payload);
      if (forwarded) {
        sendRpcExitSuccess(ws, reqId, forwarded);
        return;
      }
      const cfg = this.buildHubServerConfig();
      sendRpcExitSuccess(ws, reqId, { providers: cfg.providers ?? [] });
      return;
    }
    if (tag === "server.updateSettings") {
      await this.handleUpdateSettings(ws, reqId, payload);
      return;
    }
    if (tag === "server.getSettings") {
      const settings = this.buildHubServerConfig().settings;
      if (isFullServerSettings(settings)) {
        sendRpcExitSuccess(ws, reqId, settings);
        return;
      }
      // No harvested settings yet: fall through and ask a VM
    }
    if (tag === "server.updateServer" || tag === "server.updateServerWithProgress") {
      await this.handleServerSelfUpdate(ws, reqId, tag, (payload ?? {}) as Record<string, unknown>);
      return;
    }
    if (tag === "server.probe") {
      const live = this.anyRunningSandbox();
      const bridge = live ? this.bridges.get(live.sandboxId) : undefined;
      if (bridge?.isOpen()) {
        bridge.forwardToClient(ws, reqId, tag, payload, headers);
      } else {
        sendRpcExitSuccess(ws, reqId, { ok: true, serverVersion: this.buildDescriptor().serverVersion });
      }
      return;
    }

    // 2. Attachment upload URL creation (stage locally if no VM is targeted yet)
    if (tag === "attachments.createUploadUrl") {
      const p = (payload ?? {}) as { name?: string; mimeType?: string; sizeBytes?: number };
      const id = `att_${randomUUID().replace(/-/g, "")}`;
      const uploadToken = randomToken(24);
      this.stagedAttachments.set(id, {
        id,
        name: p.name ?? "attachment",
        mimeType: p.mimeType ?? "application/octet-stream",
        sizeBytes: Number(p.sizeBytes ?? 0),
        bytes: null,
        createdAt: Date.now(),
      });
      this.stagedUploadTokens.set(uploadToken, id);
      const relativeUrl = `/api/attachments/upload/${uploadToken}`;
      sendRpcExitSuccess(ws, reqId, {
        attachmentId: id,
        relativeUrl,
        uploadUrl: `https://${this.config.hub!.host}${relativeUrl}`,
      });
      return;
    }

    // 3. Thread launch -> SPAWN A FRESH BOAT VM PER CHAT!
    if (tag === "orchestration.launchThread") {
      await this.handleLaunchThread(ws, reqId, payload as Record<string, unknown>, headers);
      return;
    }

    // 4. Orchestration command dispatch (spawn on `thread.create`, else route to thread's VM)
    if (tag === "orchestration.dispatchCommand") {
      await this.handleDispatchCommand(ws, reqId, payload as Record<string, unknown>, headers);
      return;
    }

    // 5. Thread-scoped RPCs (`orchestration.subscribeThread`, `terminal.*`, etc.)
    if (THREAD_ROUTED_METHODS.has(tag) || THREAD_ROUTED_PREFIXES.some((p) => tag.startsWith(p))) {
      const tid = extractThreadId(payload);
      if (!tid) {
        sendRpcExitFailure(ws, reqId, `Missing threadId for ${tag}`);
        return;
      }
      try {
        const sb = await this.resolveSandboxForThread(tid);
        const bridge = await this.ensureBridgeOpen(sb);
        bridge.forwardToClient(ws, reqId, tag, payload, headers);
      } catch (err) {
        sendRpcExitFailure(ws, reqId, err instanceof Error ? err.message : String(err));
      }
      return;
    }

    // 6. Workspace/CWD-scoped RPCs (`vcs.*`, `git.*`, `projects.*`, `review.*`)
    if (WORKSPACE_ROUTED_PREFIXES.some((p) => tag.startsWith(p))) {
      await this.handleWorkspaceRpc(ws, reqId, tag, payload, headers);
      return;
    }

    // 7. Any other RPC that names a chat (`secrets.answerRequest`, `assets.createUrl` with
    //    `resource.threadId`, ...) goes to that chat's VM only, never to another chat's VM
    const chatThreadId = chatThreadIdOf(tag, payload);
    if (chatThreadId) {
      await this.forwardToThreadVm(ws, reqId, tag, payload, headers, chatThreadId);
      return;
    }

    // 8. Asset URLs that name no chat: an uploaded attachment, or a path under a workspace (`cwd`)
    if (tag === "assets.createUrl" && (await this.handleAssetUrlWithoutThread(ws, reqId, payload, headers))) {
      return;
    }

    // 9. Generic fallback -> forward to any running VM if available, else return clean empty/error
    const live = this.anyRunningSandbox();
    if (live) {
      try {
        const bridge = await this.ensureBridgeOpen(live);
        if (PROVIDER_SETUP_STREAMS.has(tag)) {
          this.forwardProviderStream(bridge, ws, reqId, tag, payload, headers);
        } else {
          bridge.forwardToClient(ws, reqId, tag, payload, headers, this.signOutHook(tag, payload));
        }
        return;
      } catch {
        // fall through
      }
    }
    if (PROVIDER_SETUP_STREAMS.has(tag)) {
      this.serveProviderStreamLocally(ws, reqId, tag, payload);
      return;
    }
    if (PROVIDER_SETUP_COMMANDS.has(tag)) {
      const instanceId = providerInstanceIdOf(payload);
      if (instanceId) {
        // Typed, so the panel shows this text instead of a generic "Provider setup failed."
        sendRpcExitFail(ws, reqId, {
          _tag: "ProviderSetupError",
          instanceId,
          operation: tag.replace(/^provider\./, ""),
          detail: NO_VM_PROVIDER_SETUP_DETAIL,
        });
        return;
      }
    }
    if (tag.startsWith("pullRequests.") || tag.startsWith("peerLinks.") || tag.startsWith("sourceControl.")) {
      sendRpcExitSuccess(ws, reqId, {});
      return;
    }
    sendRpcExitFailure(ws, reqId, `No active Boat sandbox to handle ${tag}; start a thread first.`);
  }

  private async handleLaunchThread(
    ws: ServerWebSocket<HubWsData>,
    reqId: string | number,
    payload: Record<string, unknown>,
    headers: unknown,
  ): Promise<void> {
    try {
      const hubProjectId = String(payload.projectId ?? "");
      const threadId = typeof payload.threadId === "string" && payload.threadId ? payload.threadId : randomUUID();
      const reuse = payload.reuseExistingThread === true;

      let sb: HubSandboxRecord;
      if (reuse && this.threadToSandbox.has(threadId)) {
        sb = await this.resolveSandboxForThread(threadId);
      } else {
        const projectKey = this.projectKeyFromHubId(hubProjectId);
        if (!projectKey) {
          sendRpcExitFailure(ws, reqId, `Unknown projectId "${hubProjectId}"`);
          return;
        }
        sb = await this.spawnSandboxForThread(projectKey, threadId);
      }

      const bridge = await this.ensureBridgeOpen(sb);
      const nextPayload = await this.replayStagedAttachmentsInPayload(sb, bridge, {
        ...payload,
        threadId,
        projectId: sb.vmProjectId,
      });

      const exit = await bridge.callUnary("orchestration.launchThread", nextPayload, 120_000);
      if (exit._tag === "Success") {
        const val = rewriteProjectIds(exit.value, sb.vmProjectId, sb.hubProjectId) as Record<string, unknown>;
        const actualTid = typeof val.threadId === "string" ? val.threadId : threadId;
        this.bindThreadToSandbox(actualTid, sb.sandboxId);
        // Extract worktreePath if created
        const proj = val.projection as { thread?: { worktreePath?: string | null } } | undefined;
        if (proj?.thread?.worktreePath) {
          this.bindWorktreeToSandbox(proj.thread.worktreePath, sb.sandboxId);
        }
        sendRpcExitSuccess(ws, reqId, { ...val, sequence: this.state.nextHubSequence() });
      } else {
        sendRpcFrame(ws, { _tag: "Exit", requestId: reqId, exit });
      }
    } catch (err) {
      sendRpcExitFailure(ws, reqId, err instanceof Error ? err.message : String(err));
    }
  }

  private async handleDispatchCommand(
    ws: ServerWebSocket<HubWsData>,
    reqId: string | number,
    payload: Record<string, unknown>,
    headers: unknown,
  ): Promise<void> {
    try {
      const cmdType = String(payload.type ?? "");
      if (cmdType === "thread.create") {
        const hubProjectId = String(payload.projectId ?? "");
        const threadId = String(payload.threadId ?? randomUUID());
        const projectKey = this.projectKeyFromHubId(hubProjectId);
        if (!projectKey) {
          sendRpcExitFailure(ws, reqId, `Unknown projectId "${hubProjectId}"`);
          return;
        }
        const sb = await this.spawnSandboxForThread(projectKey, threadId);
        const bridge = await this.ensureBridgeOpen(sb);
        const forwarded = rewriteProjectIds(payload, sb.hubProjectId, sb.vmProjectId);
        const exit = await bridge.callUnary("orchestration.dispatchCommand", forwarded, 60_000);
        if (exit._tag === "Success") {
          sendRpcExitSuccess(ws, reqId, { sequence: this.state.nextHubSequence() });
        } else {
          sendRpcFrame(ws, { _tag: "Exit", requestId: reqId, exit });
        }
        return;
      }

      const tid = extractThreadId(payload);
      if (!tid) {
        sendRpcExitFailure(ws, reqId, `Missing threadId for command "${cmdType}"`);
        return;
      }
      const sb = await this.resolveSandboxForThread(tid);
      const bridge = await this.ensureBridgeOpen(sb);
      const nextPayload = await this.replayStagedAttachmentsInPayload(
        sb,
        bridge,
        rewriteProjectIds(payload, sb.hubProjectId, sb.vmProjectId) as Record<string, unknown>,
      );
      const exit = await bridge.callUnary("orchestration.dispatchCommand", nextPayload, 60_000);
      if (exit._tag === "Success") {
        sendRpcExitSuccess(ws, reqId, { sequence: this.state.nextHubSequence() });
      } else {
        sendRpcFrame(ws, { _tag: "Exit", requestId: reqId, exit });
      }
    } catch (err) {
      sendRpcExitFailure(ws, reqId, err instanceof Error ? err.message : String(err));
    }
  }

  private async handleWorkspaceRpc(
    ws: ServerWebSocket<HubWsData>,
    reqId: string | number,
    tag: string,
    payload: unknown,
    headers: unknown,
  ): Promise<void> {
    const obj = (payload && typeof payload === "object" ? payload : {}) as Record<string, unknown>;
    const tid = extractThreadId(obj);
    const cwd = typeof obj.cwd === "string" ? obj.cwd : typeof obj.workspaceRoot === "string" ? obj.workspaceRoot : null;
    const hubProjId = typeof obj.projectId === "string" ? obj.projectId : null;

    let sb: HubSandboxRecord | undefined;
    if (tid && this.threadToSandbox.has(tid)) {
      try {
        sb = await this.resolveSandboxForThread(tid);
      } catch {
        sb = undefined;
      }
    }
    if (!sb && cwd) {
      sb = this.findSandboxByCwd(cwd);
    }
    if (!sb && hubProjId) {
      const key = this.projectKeyFromHubId(hubProjId);
      if (key) sb = this.anyRunningSandboxForProject(key);
    }

    if (sb) {
      try {
        const bridge = await this.ensureBridgeOpen(sb);
        bridge.forwardToClient(ws, reqId, tag, payload, headers);
        return;
      } catch {
        // fall through to cached fallback
      }
    }

    // Zero running VMs for this project -> return clean cached fallback for composer calls
    const projKey =
      (hubProjId ? this.projectKeyFromHubId(hubProjId) : null) ??
      (cwd ? this.projectKeyFromWorkdir(cwd) : null);

    if (tag === "vcs.listRefs") {
      const cached = projKey ? this.state.getHub()?.projectTemplates[projKey]?.vcsRefs : undefined;
      if (cached) {
        sendRpcExitSuccess(ws, reqId, cached);
        return;
      }
      sendRpcExitSuccess(ws, reqId, {
        isRepo: true,
        hasOriginRemote: true,
        nextCursor: null,
        branches: [
          { name: "main", current: true, isDefault: true, isRemote: false, worktreePath: null },
          { name: "origin/main", current: false, isDefault: true, isRemote: true, remoteName: "origin", worktreePath: null },
        ],
      });
      return;
    }

    if (tag === "projects.listEntries" || tag === "projects.searchEntries") {
      sendRpcExitSuccess(ws, reqId, { entries: [], truncated: false });
      return;
    }
    if (tag === "projects.listIgnoredFiles") {
      sendRpcExitSuccess(ws, reqId, { paths: [] });
      return;
    }
    if (tag === "subscribeVcsStatus" || tag === "subscribeVcsBranch") {
      // Keep VCS status/branch subscriptions open quietly when zero VMs are running for this project
      ws.data.localStreams.set(String(reqId), { rawId: reqId, tag });
      return;
    }

    sendRpcExitFailure(ws, reqId, `No running sandbox for ${tag}`);
  }

  /** True while a chat has a VM: bound to a live record, or still spawning. */
  private threadHasVm(threadId: string): boolean {
    if (this.inFlightSpawns.has(threadId)) return true;
    const sandboxId = this.threadToSandbox.get(threadId);
    if (!sandboxId || this.destroyingSandboxes.has(sandboxId)) return false;
    const sb = this.state.getHubSandbox(sandboxId);
    return !!sb && sb.phase !== "destroying";
  }

  /**
   * Forwards a chat-scoped RPC to that chat's VM. Chats are disposable: once a chat's VM is gone,
   * so are its files and pending requests, and the request fails here instead of reaching another
   * chat's VM (which must never see, say, a secret the user typed for this chat).
   */
  private async forwardToThreadVm(
    ws: ServerWebSocket<HubWsData>,
    reqId: string | number,
    tag: string,
    payload: unknown,
    headers: unknown,
    threadId: string,
  ): Promise<void> {
    if (!this.threadHasVm(threadId)) {
      if (tag === "preview.list") {
        // Asked for every open chat, drafts included: a chat with no VM has no previews
        sendRpcExitSuccess(ws, reqId, { sessions: [], serverEpoch: "boat-hub", revision: 0 });
        return;
      }
      if (tag === "orchestration.subscribePromptSuggestion") {
        // A chat with no VM has nothing to suggest; keep the stream open and quiet
        ws.data.localStreams.set(String(reqId), { rawId: reqId, tag });
        return;
      }
      const typed = threadGoneError(tag, payload);
      if (typed) sendRpcExitFail(ws, reqId, typed);
      else sendRpcExitFailure(ws, reqId, THREAD_VM_GONE_DETAIL);
      return;
    }
    try {
      const sb = await this.resolveSandboxForThread(threadId);
      const bridge = await this.ensureBridgeOpen(sb);
      bridge.forwardToClient(ws, reqId, tag, payload, headers);
    } catch (err) {
      // Waking or reaching the VM failed: transient, so untyped, and still never another VM
      sendRpcExitFailure(ws, reqId, err instanceof Error ? err.message : String(err));
    }
  }

  /**
   * `assets.createUrl` for a resource that names no chat. An `attachment` goes to the VM the Hub
   * uploaded it to, else each running VM is asked in turn; a `cwd` goes to the VM holding that
   * workspace. Returns false to leave the request to the generic fallback.
   */
  private async handleAssetUrlWithoutThread(
    ws: ServerWebSocket<HubWsData>,
    reqId: string | number,
    payload: unknown,
    headers: unknown,
  ): Promise<boolean> {
    const resource = (payload as { resource?: unknown } | null)?.resource;
    if (!resource || typeof resource !== "object") return false;
    const res = resource as Record<string, unknown>;

    if (res._tag === "attachment" && typeof res.attachmentId === "string") {
      await this.createAttachmentAssetUrl(ws, reqId, payload, headers, res, res.attachmentId);
      return true;
    }
    if (typeof res.cwd === "string") {
      const sb = this.findSandboxByCwd(res.cwd);
      if (!sb) return false;
      try {
        const bridge = await this.ensureBridgeOpen(sb);
        bridge.forwardToClient(ws, reqId, "assets.createUrl", payload, headers);
      } catch (err) {
        sendRpcExitFailure(ws, reqId, err instanceof Error ? err.message : String(err));
      }
      return true;
    }
    return false;
  }

  private async createAttachmentAssetUrl(
    ws: ServerWebSocket<HubWsData>,
    reqId: string | number,
    payload: unknown,
    headers: unknown,
    resource: Record<string, unknown>,
    attachmentId: string,
  ): Promise<void> {
    const notFound = { _tag: "AssetAttachmentNotFoundError", resource };
    const ownerId = this.attachmentToSandbox.get(attachmentId);
    const owner = ownerId ? this.state.getHubSandbox(ownerId) : undefined;
    if (owner?.phase === "sleeping") {
      // Like a chat's own images: an idle VM is woken, not skipped
      try {
        const bridge = await this.ensureBridgeOpen(await this.wakeHubSandbox(owner.sandboxId));
        bridge.forwardToClient(ws, reqId, "assets.createUrl", payload, headers);
      } catch (err) {
        sendRpcExitFailure(ws, reqId, err instanceof Error ? err.message : String(err));
      }
      return;
    }
    const running = this.state
      .listHubSandboxes()
      .filter((s) => s.phase === "running" && !this.destroyingSandboxes.has(s.sandboxId));
    const candidates = ownerId ? running.filter((s) => s.sandboxId === ownerId) : running;
    if (candidates.length === 0) {
      sendRpcExitFail(ws, reqId, notFound);
      return;
    }
    if (candidates.length === 1) {
      try {
        const bridge = await this.ensureBridgeOpen(candidates[0]);
        bridge.forwardToClient(ws, reqId, "assets.createUrl", payload, headers);
      } catch (err) {
        sendRpcExitFailure(ws, reqId, err instanceof Error ? err.message : String(err));
      }
      return;
    }
    // The Hub lost track of it (restart): ask each VM; only the one holding the file signs a URL
    for (const sb of candidates) {
      const bridge = this.bridges.get(sb.sandboxId);
      if (!bridge?.isOpen()) continue;
      const exit = await bridge.callUnary("assets.createUrl", payload, 15_000);
      if (exit._tag !== "Success") continue;
      for (const token of issuedUrlTokens("assets.createUrl", exit.value)) this.urlTokenToSandbox.set(token, sb.sandboxId);
      this.attachmentToSandbox.set(attachmentId, sb.sandboxId);
      sendRpcExitSuccess(ws, reqId, rewriteProjectIds(exit.value, sb.vmProjectId, sb.hubProjectId));
      return;
    }
    sendRpcExitFail(ws, reqId, notFound);
  }

  private async replayStagedAttachmentsInPayload(
    sb: HubSandboxRecord,
    bridge: HubVmBridge,
    payload: Record<string, unknown>,
  ): Promise<Record<string, unknown>> {
    const msg =
      (payload.initialMessage as Record<string, unknown> | undefined) ??
      (Array.isArray(payload.attachments) ? payload : undefined);
    if (!msg || !Array.isArray(msg.attachments) || msg.attachments.length === 0) return payload;

    const remapped = new Map<string, string>();
    const nextAttachments: unknown[] = [];

    for (const rawAtt of msg.attachments) {
      if (!rawAtt || typeof rawAtt !== "object") {
        nextAttachments.push(rawAtt);
        continue;
      }
      const att = rawAtt as Record<string, unknown>;
      const id = typeof att.id === "string" ? att.id : "";
      const staged = this.stagedAttachments.get(id);
      if (!staged || !staged.bytes) {
        nextAttachments.push(att);
        continue;
      }
      const created = await bridge.callUnary("attachments.createUploadUrl", {
        name: staged.name,
        mimeType: staged.mimeType,
        sizeBytes: staged.bytes.byteLength,
      });
      if (created._tag === "Success" && created.value && typeof created.value === "object") {
        const res = created.value as { attachmentId?: string; relativeUrl?: string };
        if (res.attachmentId && res.relativeUrl) {
          await fetch(`${sb.upstreamUrl}${res.relativeUrl}`, {
            method: "POST",
            headers: {
              ...(sb.upstreamHeaders ?? {}),
              "content-type": staged.mimeType,
              "content-length": String(staged.bytes.byteLength),
            },
            body: Buffer.from(staged.bytes),
          });
          remapped.set(id, res.attachmentId);
          this.attachmentToSandbox.set(res.attachmentId, sb.sandboxId);
          this.stagedAttachments.delete(id);
          nextAttachments.push({ ...att, id: res.attachmentId });
          continue;
        }
      }
      nextAttachments.push(att);
    }

    if (remapped.size === 0) return payload;
    let serialized = JSON.stringify({ ...payload, ...(payload.initialMessage ? { initialMessage: { ...msg, attachments: nextAttachments } } : { attachments: nextAttachments }) });
    for (const [oldId, newId] of remapped) {
      serialized = serialized.replaceAll(oldId, newId);
    }
    return JSON.parse(serialized) as Record<string, unknown>;
  }

  /** Spawn a brand-new Boat VM dedicated to a single chat thread. */
  async spawnSandboxForThread(
    projectKey: string,
    threadId: string,
    onProgress?: (update: { sandboxId?: string | null; phase?: HubSandboxPhase; step: string }) => void,
  ): Promise<HubSandboxRecord> {
    const existingPromise = this.inFlightSpawns.get(threadId);
    if (existingPromise) return existingPromise;

    const existingSbId = this.threadToSandbox.get(threadId);
    if (existingSbId) {
      const sb = this.state.getHubSandbox(existingSbId);
      if (sb && sb.phase === "running") return sb;
    }

    const promise = this.doSpawnSandboxForThread(projectKey, threadId, onProgress).finally(() => {
      this.inFlightSpawns.delete(threadId);
    });
    this.inFlightSpawns.set(threadId, promise);
    return promise;
  }

  private async doSpawnSandboxForThread(
    projectKey: string,
    threadId: string,
    onProgress?: (update: { sandboxId?: string | null; phase?: HubSandboxPhase; step: string }) => void,
  ): Promise<HubSandboxRecord> {
    const allProjects = this.state.getAllProjects(this.config);
    const project = allProjects[projectKey];
    if (!project) throw new Error(`Unknown project "${projectKey}"`);
    const maxSandboxes = this.config.hub?.maxSandboxes ?? 10;
    const activeCount = this.state.listHubSandboxes().filter((s) => s.phase !== "destroying").length;
    if (activeCount >= maxSandboxes) {
      throw new Error(`Hub sandbox limit reached (${activeCount}/${maxSandboxes}). Settle or archive an existing chat first.`);
    }

    const supportsSnapshots = hasCapability(this.boat, "snapshots") && typeof this.boat.listSnapshots === "function";
    let snapshotToUse: string | undefined = supportsSnapshots ? project.snapshot : undefined;
    let effectiveProject: ProjectConfig = project;
    let fallbackCloning = false;
    if (supportsSnapshots) {
      const snapshots = await this.boat.listSnapshots!();
      if (!snapshots.includes(project.snapshot)) {
        if (snapshots.includes(this.config.boat.baseSnapshot) && project.bakeCmd) {
          snapshotToUse = this.config.boat.baseSnapshot;
          fallbackCloning = true;
          effectiveProject = {
            ...project,
            bootCmd: project.bootCmd ? `${project.bakeCmd}\n${project.bootCmd}` : project.bakeCmd,
          };
        } else {
          throw new Error(`Golden snapshot "${project.snapshot}" does not exist yet. Run \`sentinel bake ${projectKey}\` first.`);
        }
      }
    } else if (project.bakeCmd) {
      fallbackCloning = true;
      effectiveProject = {
        ...project,
        bootCmd: project.bootCmd ? `${project.bakeCmd}\n${project.bootCmd}` : project.bakeCmd,
      };
    }

    const hub = this.state.ensureHub(this.config.hub?.label ?? "Boat", Object.keys(allProjects));
    const hubProjectId = hub.projectIds[projectKey] ?? randomUUID();
    const size = project.size ?? this.config.boat.defaultSize;
    const shortThread = threadId.replace(/[^a-z0-9]/gi, "").slice(0, 6).toLowerCase() || "chat";
    const sandboxName = `${this.config.boat.sandboxPrefix}hub-${shortThread}-${Date.now().toString(36)}`;
    const watchdogSecret = randomToken(32);
    const llmToken = this.config.llmProxy ? `t3llm_${randomToken(24)}` : null;

    let sandboxId: string | null = null;
    let klavAgentName: string | null = null;
    const setupStartedAt = new Date().toISOString();
    let setupSeq = 0;
    const emitStage = (
      activeStage: "fetch" | "checkout" | "setup-script" | "agent" | "done" | "failed",
      detail: string,
      percent: number,
      errorMsg: string | null = null,
    ) => {
      const now = new Date().toISOString();
      const order: Array<"fetch" | "checkout" | "submodules" | "setup-script" | "agent"> = [
        "fetch",
        "checkout",
        "submodules",
        "setup-script",
        "agent",
      ];
      const activeIdx =
        activeStage === "done"
          ? order.length
          : activeStage === "failed"
            ? Math.max(0, order.indexOf("setup-script"))
            : order.indexOf(activeStage);
      const stages: WorktreeSetupStageState[] = order.map((id, idx) => {
        if (id === "submodules") {
          return {
            id,
            status: activeIdx > idx ? "skipped" : "pending",
            startedAt: null,
            endedAt: activeIdx > idx ? now : null,
            percent: null,
            detail: null,
            tail: [],
          };
        }
        if (activeStage === "done" || idx < activeIdx) {
          return {
            id,
            status: "done",
            startedAt: setupStartedAt,
            endedAt: now,
            percent: 100,
            detail: idx === activeIdx - 1 ? detail.slice(0, 200) : null,
            tail: [],
          };
        }
        if (activeStage === "failed" && idx === activeIdx) {
          return {
            id,
            status: "failed",
            startedAt: setupStartedAt,
            endedAt: now,
            percent,
            detail: detail.slice(0, 200),
            tail: errorMsg ? [errorMsg.slice(0, 400)] : [],
          };
        }
        if (idx === activeIdx) {
          return {
            id,
            status: "running",
            startedAt: now,
            endedAt: null,
            percent,
            detail: detail.slice(0, 200),
            tail: [detail.slice(0, 400)],
          };
        }
        return {
          id,
          status: "pending",
          startedAt: null,
          endedAt: null,
          percent: null,
          detail: null,
          tail: [],
        };
      });
      this.publishWorktreeSetup(threadId, {
        threadId,
        phase: activeStage === "done" ? "done" : activeStage === "failed" ? "failed" : "running",
        startedAt: setupStartedAt,
        endedAt: activeStage === "done" || activeStage === "failed" ? now : null,
        branch: null,
        baseRef: snapshotToUse ?? null,
        worktreePath: effectiveProject.workdir,
        setupScript: null,
        stages,
        error: errorMsg ? errorMsg.slice(0, 1000) : null,
        sequence: ++setupSeq,
      });
    };

    try {
      const notifyProgress = (phase: HubSandboxPhase, step: string) => {
        onProgress?.({ sandboxId, phase, step });
      };
      emitStage("fetch", `Creating ${this.boat.name} VM (${size}) from ${snapshotToUse ?? "base image"}...`, 15);
      notifyProgress("booting", `Creating ${this.boat.name} VM (${size}) from ${snapshotToUse ?? "base image"}...`);
      console.log(
        `[hub] spawning fresh VM ${sandboxName} via ${this.boat.name} for project="${projectKey}" thread=${threadId} (snapshot=${snapshotToUse ?? "base-image"})`,
      );
      sandboxId = await this.boat.create({ name: sandboxName, size, fromSnapshot: snapshotToUse });
      this.lifecycle.trackInFlightSandbox(sandboxId);

      if (effectiveProject.klav && this.klav) {
        emitStage("checkout", `VM ${sandboxId} created — attaching zero-trust Klav proxy...`, 40);
        notifyProgress("booting", `VM ${sandboxId} created — attaching zero-trust Klav proxy...`);
        const prov = await this.klav.provisionForSandbox(this.boat, sandboxId, effectiveProject.klav);
        klavAgentName = prov.agentName;
      } else {
        emitStage("checkout", `VM ${sandboxId} created — preparing workspace...`, 40);
        notifyProgress("booting", `VM ${sandboxId} created — preparing workspace...`);
      }

      emitStage(
        "setup-script",
        fallbackCloning
          ? `Cloning ${projectKey} & starting T3 Code inside ${sandboxId}...`
          : `Starting T3 Code server & watchdog inside ${sandboxId}...`,
        68,
      );
      notifyProgress(
        fallbackCloning ? "cloning" : "starting_t3",
        fallbackCloning
          ? `Cloning ${projectKey} & starting T3 Code inside ${sandboxId}...`
          : `Starting T3 Code server & watchdog inside ${sandboxId}...`,
      );
      const provisioned = await this.lifecycle.provisionStandaloneVm({
        sandboxId,
        watchdogSlotKey: `hub:${sandboxId}`,
        environmentId: hub.environmentId,
        label: hub.label,
        projectName: projectKey,
        project: effectiveProject,
        watchdogSecret,
        llmToken,
        mintToken: true,
        injectProviderSessions: true,
        installProviderRuntimes: !supportsSnapshots,
      });

      if (!provisioned.upstreamToken) {
        throw new Error(`T3 server inside ${sandboxId} returned no upstreamToken`);
      }

      emitStage("agent", `Connecting live stream to VM ${sandboxId}...`, 90);
      const endpoint = await this.boat.exposePort(sandboxId, 3773, {
        visibility: this.config.boat.hostVisibility,
      });
      const vmProjectId = await ensureT3Project(
        endpoint.url,
        provisioned.upstreamToken,
        projectKey,
        effectiveProject.workdir,
        endpoint.headers,
      );
      const now = new Date().toISOString();

      const record: HubSandboxRecord = {
        sandboxId,
        project: projectKey,
        hubProjectId,
        vmProjectId,
        snapshot: snapshotToUse ?? "base-image",
        size,
        upstreamUrl: endpoint.url,
        upstreamHeaders: endpoint.headers ?? null,
        upstreamToken: provisioned.upstreamToken,
        watchdogSecret,
        llmToken,
        klavAgentName,
        threadIds: [threadId],
        worktreePaths: [],
        phase: "running",
        createdAt: now,
        lastActiveAt: now,
        lastHeartbeatAt: now,
        sleepingSince: null,
      };

      const injected = provisioned.injectedProviderSessions ?? [];
      // The VM already holds the stored copy: its first `authenticated` report needs no re-read
      this.lifecycle.providerSessions.markInjected(sandboxId, injected);
      this.state.setHubSandbox(record);
      this.threadToSandbox.set(threadId, sandboxId);
      this.attachBridge(record);
      await this.bridges.get(sandboxId)!.waitUntilOpen(20_000);
      await this.settingsReplayDone(sandboxId);
      // T3 may still report the status it probed before the sign-in was placed; re-check in the background
      for (const instanceId of injected) this.refreshInjectedProvider(sandboxId, instanceId);

      emitStage("done", `Boat VM ${sandboxId} (${size}) ready`, 100);
      console.log(`[hub] sandbox ${sandboxId} ready for thread=${threadId} (project=${projectKey})`);
      return record;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      emitStage("failed", `Boat VM boot failed`, 100, msg);
      if (sandboxId) {
        this.state.removeHubSandbox(sandboxId);
        this.state.queueDelete(sandboxId, "hub-failed-spawn", klavAgentName);
        void this.lifecycle.reaperTick();
      } else if (klavAgentName && this.klav) {
        await this.klav.revokeForSandbox(klavAgentName).catch(() => {});
      }
      throw err;
    } finally {
      if (sandboxId) {
        this.lifecycle.untrackInFlightSandbox(sandboxId);
      }
    }
  }

  private async resolveSandboxForThread(threadId: string): Promise<HubSandboxRecord> {
    const inFlight = this.inFlightSpawns.get(threadId);
    if (inFlight) return inFlight;

    const sandboxId = this.threadToSandbox.get(threadId);
    if (!sandboxId) {
      throw new Error(`No Boat sandbox found for thread "${threadId}"`);
    }
    const sb = this.state.getHubSandbox(sandboxId);
    if (!sb) {
      this.threadToSandbox.delete(threadId);
      throw new Error(`Sandbox "${sandboxId}" no longer exists`);
    }
    if (sb.phase === "sleeping") {
      return this.wakeHubSandbox(sandboxId);
    }
    if (sb.phase !== "running") {
      throw new Error(`Sandbox "${sandboxId}" is ${sb.phase}`);
    }
    return sb;
  }

  async wakeHubSandbox(sandboxId: string): Promise<HubSandboxRecord> {
    const existing = this.inFlightWakes.get(sandboxId);
    if (existing) return existing;

    const promise = (async () => {
      const sb = this.state.getHubSandbox(sandboxId);
      if (!sb) throw new Error(`Unknown hub sandbox "${sandboxId}"`);
      if (sb.phase === "running") return sb;

      if (!hasCapability(this.boat, "pause") || !this.boat.resume) {
        throw new Error(`Provider "${this.boat.name}" does not support pause/resume`);
      }
      console.log(`[hub] waking sleeping sandbox ${sandboxId} (${sb.project})`);
      const res = await this.boat.resume(sandboxId, sb.size);
      if (res === "data_expired") {
        await this.destroyHubSandbox(sandboxId, "data-expired");
        throw new Error("Sandbox disk expired after 14 days stopped");
      }

      const project = this.state.getAllProjects(this.config)[sb.project];
      let nextKlavAgent = sb.klavAgentName;
      if (this.config.klav?.mode === "proxy" && project?.klav && this.klav) {
        if (sb.klavAgentName) await this.klav.revokeForSandbox(sb.klavAgentName).catch(() => {});
        const prov = await this.klav.provisionForSandbox(this.boat, sandboxId, project.klav);
        nextKlavAgent = prov.agentName;
      }

      const llmToken = this.config.llmProxy ? `t3llm_${randomToken(24)}` : null;
      if (llmToken) {
        // Re-inject fresh ephemeral llmToken on wake
        await this.lifecycle.provisionStandaloneVm({
          sandboxId,
          watchdogSlotKey: `hub:${sandboxId}`,
          environmentId: this.state.ensureHub().environmentId,
          label: this.state.ensureHub().label,
          projectName: sb.project,
          project,
          watchdogSecret: sb.watchdogSecret,
          llmToken,
          mintToken: false,
        });
      }

      const endpoint = await this.boat.exposePort(sandboxId, 3773, {
        visibility: this.config.boat.hostVisibility,
      });
      const now = new Date().toISOString();
      const updated = this.state.updateHubSandbox(sandboxId, (s) => {
        s.upstreamUrl = endpoint.url;
        s.upstreamHeaders = endpoint.headers ?? null;
        s.llmToken = llmToken;
        s.klavAgentName = nextKlavAgent;
        s.phase = "running";
        s.lastActiveAt = now;
        s.lastHeartbeatAt = now;
        s.sleepingSince = null;
      })!;

      this.attachBridge(updated);
      await this.bridges.get(sandboxId)!.waitUntilOpen(20_000);
      await this.settingsReplayDone(sandboxId);
      return updated;
    })().finally(() => {
      this.inFlightWakes.delete(sandboxId);
    });

    this.inFlightWakes.set(sandboxId, promise);
    return promise;
  }

  async sleepHubSandbox(sandboxId: string): Promise<void> {
    const sb = this.state.getHubSandbox(sandboxId);
    if (!sb || sb.phase !== "running") return;
    if (!hasCapability(this.boat, "pause") || !this.boat.pause) {
      console.warn(`[hub] provider "${this.boat.name}" does not support pause; skipping idle sleep for ${sandboxId}`);
      return;
    }
    console.log(`[hub] pausing idle sandbox ${sandboxId} (${sb.project})`);
    this.bridges.get(sandboxId)?.stop();
    this.bridges.delete(sandboxId);
    await this.boat.pause(sandboxId);
    this.state.updateHubSandbox(sandboxId, (s) => {
      s.phase = "sleeping";
      s.llmToken = null; // immediately revoke LLM token while sleeping
      s.sleepingSince = new Date().toISOString();
    });
  }

  async destroyHubSandbox(sandboxId: string, reason: string): Promise<void> {
    const sb = this.state.getHubSandbox(sandboxId);
    if (!sb || this.destroyingSandboxes.has(sandboxId)) return;
    this.destroyingSandboxes.add(sandboxId);
    try {
      await this.doDestroyHubSandbox(sb, reason);
    } finally {
      this.destroyingSandboxes.delete(sandboxId);
    }
  }

  private async doDestroyHubSandbox(sb: HubSandboxRecord, reason: string): Promise<void> {
    const sandboxId = sb.sandboxId;
    console.log(`[hub] destroying sandbox ${sandboxId} (project=${sb.project}, reason=${reason})`);

    // Best effort, bounded (~10s): the CLI may have refreshed the token since it was captured
    if (sb.phase === "running") {
      await this.lifecycle.providerSessions.pullBeforeDestroy(sandboxId).catch(() => {});
    }
    this.lifecycle.providerSessions.forgetSandbox(sandboxId);

    this.bridges.get(sandboxId)?.stop();
    this.bridges.delete(sandboxId);
    this.settingsReplays.delete(sandboxId);
    this.replayedSettings.delete(sandboxId);

    // Threads may have been bound while the pull ran
    const current = this.state.getHubSandbox(sandboxId) ?? sb;
    const threadIds = [...current.threadIds];
    const worktreePaths = [...current.worktreePaths];
    const klavAgent = current.klavAgentName;

    this.state.removeHubSandbox(sandboxId);
    this.mcp.recordThreadsDestroyed(threadIds, reason);
    for (const tid of threadIds) {
      this.threadToSandbox.delete(tid);
      this.threadShells.delete(tid);
      this.broadcastShellItem({
        kind: "thread.removed",
        sequence: this.state.nextHubSequence(),
        location: "active",
        threadId: tid,
      });
    }
    for (const wt of worktreePaths) {
      this.worktreeToSandbox.delete(wt);
    }
    for (const [attachmentId, owner] of this.attachmentToSandbox) {
      if (owner === sandboxId) this.attachmentToSandbox.delete(attachmentId);
    }

    if (klavAgent && this.klav) {
      await this.klav.revokeForSandbox(klavAgent).catch((err) => {
        console.warn(`[hub] Klav revoke failed for ${klavAgent}:`, err);
      });
    }

    this.state.queueDelete(sandboxId, `hub:${reason}`, klavAgent);
    await this.boat.delete(sandboxId).catch((err) => {
      console.warn(`[hub] delete failed for ${sandboxId} (queued for reaper):`, err);
    });
    void this.lifecycle.reaperTick();
  }

  async handleWatchdogEvent(payload: WatchdogEventPayload): Promise<void> {
    const sb = this.state.getHubSandbox(payload.sandboxId);
    if (!sb) return;

    switch (payload.event) {
      case "heartbeat":
        this.state.updateHubSandbox(sb.sandboxId, (s) => {
          s.lastHeartbeatAt = new Date().toISOString();
        });
        break;

      case "all-archived":
        if (this.config.boat.archiveAction === "stop") {
          await this.sleepHubSandbox(sb.sandboxId);
        } else {
          await this.destroyHubSandbox(sb.sandboxId, "chat-settled-or-archived");
        }
        break;

      case "idle":
        await this.sleepHubSandbox(sb.sandboxId);
        break;
    }
  }

  private attachBridge(sb: HubSandboxRecord): void {
    this.bridges.get(sb.sandboxId)?.stop();
    const bridge = new HubVmBridge(
      sb,
      (sandboxId, items) => this.handleVmShellItems(sandboxId, items),
      (sandboxId, token) => this.urlTokenToSandbox.set(token, sandboxId),
      (sandbox, templates) => this.handleTemplatesHarvested(sandbox, templates),
      (sandboxId, providers) => this.handleVmProviderStatuses(sandboxId, providers),
      (sandboxId) => {
        this.adoptLocalProviderStreams(sandboxId);
        this.replaySettingsOverlay(sandboxId);
      },
    );
    this.bridges.set(sb.sandboxId, bridge);
    bridge.start();
  }

  private async ensureBridgeOpen(sb: HubSandboxRecord): Promise<HubVmBridge> {
    let bridge = this.bridges.get(sb.sandboxId);
    if (!bridge) {
      this.attachBridge(sb);
      bridge = this.bridges.get(sb.sandboxId)!;
    }
    await bridge.waitUntilOpen(20_000);
    return bridge;
  }

  private handleVmShellItems(sandboxId: string, items: unknown[]): void {
    const sb = this.state.getHubSandbox(sandboxId);
    if (!sb) return;

    for (const raw of items) {
      if (!raw || typeof raw !== "object") continue;
      const item = raw as Record<string, unknown>;

      if (item.kind === "snapshot" && item.snapshot && typeof item.snapshot === "object") {
        const snap = item.snapshot as {
          projects?: Array<Record<string, unknown>>;
          threads?: Array<Record<string, unknown>>;
        };
        const vmProj = snap.projects?.find((p) => p.id === sb.vmProjectId);
        if (vmProj) {
          this.state.updateHub((h) => {
            h.projectTemplates[sb.project] = {
              ...(h.projectTemplates[sb.project] ?? {}),
              shell: { ...vmProj, id: sb.hubProjectId },
            };
          });
        }
        for (const t of snap.threads ?? []) {
          const rewritten = rewriteProjectIds(t, sb.vmProjectId, sb.hubProjectId) as Record<string, unknown>;
          const tid = String(rewritten.id ?? "");
          if (!tid) continue;
          const isNew = !this.threadShells.has(tid);
          this.threadShells.set(tid, rewritten);
          this.bindThreadToSandbox(tid, sandboxId);
          if (typeof rewritten.worktreePath === "string" && rewritten.worktreePath) {
            this.bindWorktreeToSandbox(rewritten.worktreePath, sandboxId);
          }
          if (isNew) {
            this.broadcastShellItem({
              kind: "thread.updated",
              sequence: this.state.nextHubSequence(),
              location: "active",
              thread: rewritten,
            });
          }
        }
      } else if (item.kind === "thread.updated" && item.thread && typeof item.thread === "object") {
        const rewritten = rewriteProjectIds(item.thread, sb.vmProjectId, sb.hubProjectId) as Record<string, unknown>;
        const tid = String(rewritten.id ?? "");
        if (!tid) continue;
        this.threadShells.set(tid, rewritten);
        this.bindThreadToSandbox(tid, sandboxId);
        if (typeof rewritten.worktreePath === "string" && rewritten.worktreePath) {
          this.bindWorktreeToSandbox(rewritten.worktreePath, sandboxId);
        }
        this.broadcastShellItem({
          kind: "thread.updated",
          sequence: this.state.nextHubSequence(),
          location: item.location ?? "active",
          thread: rewritten,
        });
      } else if (item.kind === "thread.removed" && typeof item.threadId === "string") {
        this.threadShells.delete(item.threadId);
        this.broadcastShellItem({
          kind: "thread.removed",
          sequence: this.state.nextHubSequence(),
          location: item.location ?? "active",
          threadId: item.threadId,
        });
      }
    }
  }

  /** Sends the Hub's current provider list to every client subscribed to `subscribeServerConfig`. */
  private broadcastProviderStatuses(): void {
    const cfg = this.buildHubServerConfig();
    for (const client of this.clients) {
      for (const [, sub] of client.data.localStreams) {
        if (sub.tag === "subscribeServerConfig") {
          sendRpcFrame(client, {
            _tag: "Chunk",
            requestId: sub.rawId,
            values: [{ version: 1, type: "providerStatuses", payload: { providers: cfg.providers } }],
          });
        }
      }
    }
  }

  /**
   * Replaces only `serverConfig.providers` in Hub state and broadcasts it. No-op when unchanged or
   * when no config was harvested yet (the bridge folds live statuses into its first harvest).
   * With several VMs the last writer wins.
   */
  private applyProviderStatuses(providers: unknown[]): boolean {
    const current = this.state.getHub()?.serverConfig;
    if (!current || typeof current !== "object") return false;
    if (JSON.stringify(current.providers) === JSON.stringify(providers)) return false;
    this.state.updateHub((h) => {
      if (h.serverConfig && typeof h.serverConfig === "object") {
        h.serverConfig = { ...h.serverConfig, providers };
      }
    });
    this.broadcastProviderStatuses();
    return true;
  }

  private handleVmProviderStatuses(sandboxId: string, providers: unknown[]): void {
    const sb = this.state.getHubSandbox(sandboxId);
    if (!sb) return;
    this.applyProviderStatuses(providers);
    this.observeProviderSessions(sb, providers);
  }

  /** Feeds a booted VM's provider list to the session store (capture on `authenticated`). */
  private observeProviderSessions(sb: HubSandboxRecord, providers: unknown[]): void {
    // Records only exist once provisioning finished; skip VMs that are not live
    if (sb.phase !== "running" || this.destroyingSandboxes.has(sb.sandboxId)) return;
    this.lifecycle.providerSessions.observe(sb.sandboxId, providers, sb.createdAt);
  }

  /** Re-checks one provider on the VM it was injected into and caches the answer. Never throws. */
  private refreshInjectedProvider(sandboxId: string, instanceId: string): void {
    const bridge = this.bridges.get(sandboxId);
    if (!bridge) return;
    const input = { instanceId };
    void bridge
      .callUnary("server.refreshProviders", input, 150_000)
      .then((exit) => {
        if (exit._tag !== "Success" || !exit.value || typeof exit.value !== "object") {
          console.warn(`[sessions] refresh of ${instanceId} on ${sandboxId} after injection did not succeed`);
          return;
        }
        if (!this.state.getHubSandbox(sandboxId)) return;
        const value = exit.value as Record<string, unknown>;
        this.cacheRefreshedProviders(input, value);
        if (Array.isArray(value.providers)) {
          const sb = this.state.getHubSandbox(sandboxId);
          if (sb) this.observeProviderSessions(sb, value.providers);
        }
      })
      .catch(() => {});
  }

  /**
   * Forwards `server.refreshProviders` to a live VM and caches its answer. Returns null with no
   * open VM bridge or when the forward fails, so the caller replies from the Hub cache.
   */
  private async forwardRefreshProviders(payload: unknown): Promise<Record<string, unknown> | null> {
    const bridge = this.state
      .listHubSandboxes()
      .filter((s) => s.phase === "running")
      .map((s) => this.bridges.get(s.sandboxId))
      .find((b) => b?.isOpen());
    if (!bridge) return null;
    const input = payload && typeof payload === "object" ? (payload as Record<string, unknown>) : {};
    // Antigravity validation alone can take ~90s on a VM
    const exit = await bridge.callUnary("server.refreshProviders", input, 150_000);
    if (exit._tag !== "Success" || !exit.value || typeof exit.value !== "object") return null;
    const value = exit.value as Record<string, unknown>;
    this.cacheRefreshedProviders(input, value);
    return value;
  }

  /** Merges a `server.refreshProviders` reply into the Hub's cached provider list. */
  private cacheRefreshedProviders(input: Record<string, unknown>, value: Record<string, unknown>): void {
    const replied = value.providers;
    // An empty reply (omitProviders / modelCatalogOnly) and per-cwd snapshots are not the machine list
    if (Array.isArray(replied) && replied.length > 0 && input.cwd === undefined) {
      const cached = this.state.getHub()?.serverConfig?.providers;
      let next: unknown[] = replied;
      if (typeof input.instanceId === "string" && Array.isArray(cached)) {
        const byId = new Map<unknown, unknown>();
        for (const p of replied) byId.set((p as { instanceId?: unknown } | null)?.instanceId, p);
        next = cached.map((p) => {
          const id = (p as { instanceId?: unknown } | null)?.instanceId;
          if (!byId.has(id)) return p;
          const replacement = byId.get(id);
          byId.delete(id);
          return replacement;
        });
        next.push(...byId.values());
      }
      this.applyProviderStatuses(next);
    }
  }

  private handleTemplatesHarvested(
    sandbox: HubSandboxRecord,
    templates: { serverConfig?: Record<string, unknown>; lifecycleEvents?: unknown[]; vcsRefs?: unknown },
  ): void {
    let changed = false;
    this.state.updateHub((h) => {
      if (templates.serverConfig) {
        const replayed = this.replayedSettings.get(sandbox.sandboxId);
        h.serverConfig = replayed
          ? { ...templates.serverConfig, settings: replayed }
          : this.withSettingsOverlay(templates.serverConfig, h.settingsOverlay);
        changed = true;
      }
      if (templates.lifecycleEvents && templates.lifecycleEvents.length > 0) {
        h.lifecycleEvents = templates.lifecycleEvents;
        changed = true;
      }
    });
    if (changed && templates.serverConfig) {
      // Push updated real ServerConfig to any already-connected clients
      this.broadcastProviderStatuses();
      const settings = this.state.getHub()?.serverConfig?.settings;
      if (isFullServerSettings(settings)) this.broadcastSettings(settings);
    }
    // The first snapshot is not sent to onProviderStatuses; a VM may already be signed in (e.g. after a Hub restart)
    const providers = templates.serverConfig?.providers;
    const live = this.state.getHubSandbox(sandbox.sandboxId);
    if (live && Array.isArray(providers)) this.observeProviderSessions(live, providers);
    // Also harvest vcsRefs for this project if missing
    const project = this.state.getAllProjects(this.config)[sandbox.project];
    const bridge = this.bridges.get(sandbox.sandboxId);
    if (project && bridge?.isOpen() && !this.state.getHub()?.projectTemplates[sandbox.project]?.vcsRefs) {
      void bridge.callUnary("vcs.listRefs", { cwd: project.workdir }).then((res) => {
        if (res._tag === "Success" && res.value) {
          this.state.updateHub((h) => {
            h.projectTemplates[sandbox.project] = {
              ...(h.projectTemplates[sandbox.project] ?? {}),
              vcsRefs: res.value,
            };
          });
        }
      });
    }
  }

  /**
   * A VM's settings snapshot with the user's overlay applied. The snapshot can predate the overlay
   * replay (boot cache, or a VM resumed from an old disk), so it never overwrites newer changes.
   */
  private withSettingsOverlay(
    serverConfig: Record<string, unknown>,
    overlay: Parameters<typeof applySettingsOverlay>[1],
  ): Record<string, unknown> {
    if (isEmptySettingsOverlay(overlay) || !isFullServerSettings(serverConfig.settings)) return serverConfig;
    return { ...serverConfig, settings: applySettingsOverlay(serverConfig.settings, overlay) };
  }

  private handleUpdateSettings(ws: ServerWebSocket<HubWsData>, reqId: string | number, payload: unknown): Promise<void> {
    const run = this.settingsQueue.then(() => this.runUpdateSettings(ws, reqId, payload));
    this.settingsQueue = run.catch(() => {});
    return run.catch((err) => {
      console.error(`[hub] settings update failed: ${err instanceof Error ? err.name : "error"}`);
      sendRpcExitFailure(ws, reqId, "Settings update failed");
    });
  }

  /**
   * `server.updateSettings`: forwards the update to every running VM and answers with the first
   * VM's result (T3 applied it there). With no VM, applies it to the Hub's copy. Either way it is
   * recorded in the overlay that new VMs replay, and pushed to `subscribeServerConfig` clients.
   */
  private async runUpdateSettings(ws: ServerWebSocket<HubWsData>, reqId: string | number, payload: unknown): Promise<void> {
    const parsed = parseSettingsUpdate(payload);
    if (!parsed.ok) {
      // A T3 server answers a payload its schema rejects with a defect, not a typed error
      sendRpcExitFailure(ws, reqId, parsed.message);
      return;
    }
    const input = parsed.value;
    const current = this.buildHubServerConfig().settings;
    const mutation = input.providerInstanceMutation;
    if (
      mutation?.operation === "create" &&
      isFullServerSettings(current) &&
      Object.hasOwn(current.providerInstances as Record<string, unknown>, mutation.instanceId)
    ) {
      sendRpcExitFail(ws, reqId, {
        _tag: "ServerSettingsError",
        settingsPath: VM_SETTINGS_PATH,
        operation: "create-provider-instance",
        providerInstanceId: mutation.instanceId,
      });
      return;
    }

    const summary = describeSettingsUpdate(input);
    const forwarded = forwardableSettingsUpdate(input);
    const bridges = this.openRunningBridges();
    // Sent to every VM before awaiting any, so each VM sees updates in the order the Hub received them
    const calls = bridges.map((b) => ({
      bridge: b,
      exit: b.callUnary(
        "server.updateSettings",
        rewriteProjectIdsAndKeys(forwarded, b.sandbox.hubProjectId, b.sandbox.vmProjectId),
        15_000,
      ),
    }));
    for (const { bridge, exit } of calls.slice(1)) {
      void exit.then((res) => {
        if (res._tag !== "Success") console.warn(`[hub] settings update ${summary} not applied on ${bridge.sandbox.sandboxId}`);
      });
    }

    let settings: Record<string, unknown> | null = null;
    const primary = calls[0];
    if (primary) {
      const res = await primary.exit;
      if (res._tag === "Success" && isFullServerSettings(res.value)) {
        settings = rewriteProjectIdsAndKeys(res.value, primary.bridge.sandbox.vmProjectId, primary.bridge.sandbox.hubProjectId) as Record<
          string,
          unknown
        >;
      } else if (res._tag === "Failure" && !isHubTransportFailure(res.cause)) {
        // The VM's T3 rejected it: pass its error through unchanged and record nothing
        console.warn(`[hub] settings update ${summary} rejected by ${primary.bridge.sandbox.sandboxId}`);
        sendRpcFrame(ws, { _tag: "Exit", requestId: reqId, exit: { _tag: "Failure", cause: res.cause } });
        return;
      }
    }
    const applyLocally = settings === null;
    if (applyLocally) {
      if (!isFullServerSettings(current)) {
        // Nothing to apply the patch to before any VM has booted once
        sendRpcExitFail(ws, reqId, { _tag: "ServerSettingsError", settingsPath: VM_SETTINGS_PATH, operation: "read-file" });
        return;
      }
      settings = applySettingsUpdate(current, input);
    }

    const before = isFullServerSettings(current) ? current : {};
    this.state.updateHub((h) => {
      h.settingsOverlay = recordSettingsUpdate(h.settingsOverlay, input, before);
      if (h.serverConfig && typeof h.serverConfig === "object") {
        h.serverConfig = { ...h.serverConfig, settings };
      }
    });
    console.log(`[hub] settings update ${summary} applied (${applyLocally ? "Hub copy only, no VM" : `${calls.length} VM(s)`})`);
    if (applyLocally) {
      // No VM reports provider status: show the switch's effect right away, as T3 does
      const providers = providersWithInstanceEnabled(this.state.getHub()?.serverConfig?.providers, mutation);
      if (providers) this.applyProviderStatuses(providers);
    }
    this.broadcastSettings(settings!);
    sendRpcExitSuccess(ws, reqId, settings);
  }

  /** Sends `settingsUpdated` to every client subscribed to `subscribeServerConfig`, as T3 does. */
  private broadcastSettings(settings: Record<string, unknown>): void {
    for (const client of this.clients) {
      for (const [, sub] of client.data.localStreams) {
        if (sub.tag === "subscribeServerConfig") {
          sendRpcFrame(client, {
            _tag: "Chunk",
            requestId: sub.rawId,
            values: [{ version: 1, type: "settingsUpdated", payload: { settings } }],
          });
        }
      }
    }
  }

  private openRunningBridges(): HubVmBridge[] {
    return this.state
      .listHubSandboxes()
      .filter((s) => s.phase === "running" && !this.destroyingSandboxes.has(s.sandboxId))
      .map((s) => this.bridges.get(s.sandboxId))
      .filter((b): b is HubVmBridge => !!b?.isOpen());
  }

  /**
   * Replays the settings overlay on a VM whose bridge just opened (spawn, resume, reconnect or Hub
   * restart). Queued with settings updates so a replay never lands after a newer change.
   */
  private replaySettingsOverlay(sandboxId: string): void {
    this.replayedSettings.delete(sandboxId);
    const calls: SettingsUpdateInput[] = overlayReplayCalls(this.state.getHub()?.settingsOverlay);
    if (calls.length === 0) {
      this.settingsReplays.delete(sandboxId);
      return;
    }
    const run = this.settingsQueue.then(async () => {
      const bridge = this.bridges.get(sandboxId);
      if (!bridge?.isOpen()) return;
      // Re-read under the queue: an update may have landed since the bridge opened
      const latest = overlayReplayCalls(this.state.getHub()?.settingsOverlay);
      const exits = latest.map((call) =>
        bridge.callUnary(
          "server.updateSettings",
          rewriteProjectIdsAndKeys(call, bridge.sandbox.hubProjectId, bridge.sandbox.vmProjectId),
          15_000,
        ),
      );
      const results = await Promise.all(exits);
      const failed = results.filter((r) => r._tag !== "Success").length;
      if (failed > 0) console.warn(`[hub] settings replay on ${sandboxId}: ${failed} of ${results.length} call(s) failed`);
      else console.log(`[hub] settings replay on ${sandboxId}: ${results.length} call(s) applied`);
      // The last reply holds every replayed change as the VM's T3 applied it (redacted by T3)
      const last = results.filter((r) => r._tag === "Success").at(-1);
      if (last?._tag !== "Success" || !isFullServerSettings(last.value) || this.bridges.get(sandboxId) !== bridge) return;
      const settings = rewriteProjectIdsAndKeys(last.value, bridge.sandbox.vmProjectId, bridge.sandbox.hubProjectId) as Record<string, unknown>;
      this.replayedSettings.set(sandboxId, settings);
      // A harvest that lands later uses this too, instead of the VM's pre-replay snapshot
      this.state.updateHub((h) => {
        if (h.serverConfig && typeof h.serverConfig === "object") h.serverConfig = { ...h.serverConfig, settings };
      });
      this.broadcastSettings(settings);
    });
    this.settingsQueue = run.catch(() => {});
    this.settingsReplays.set(
      sandboxId,
      run.catch(() => {}),
    );
  }

  /** Waits (bounded) for a VM's overlay replay so the user never sees it with stale settings. */
  private async settingsReplayDone(sandboxId: string): Promise<void> {
    const replay = this.settingsReplays.get(sandboxId);
    if (!replay) return;
    let timer: ReturnType<typeof setTimeout> | undefined;
    await Promise.race([replay, new Promise<void>((r) => (timer = setTimeout(r, 30_000)))]);
    clearTimeout(timer);
  }

  private broadcastShellItem(item: Record<string, unknown>): void {
    for (const client of this.clients) {
      for (const [, sub] of client.data.localStreams) {
        if (sub.tag === "orchestration.subscribeShell") {
          sendRpcFrame(client, { _tag: "Chunk", requestId: sub.rawId, values: [item] });
        }
      }
    }
  }

  private bindThreadToSandbox(threadId: string, sandboxId: string): void {
    this.threadToSandbox.set(threadId, sandboxId);
    this.state.updateHubSandbox(sandboxId, (s) => {
      if (!s.threadIds.includes(threadId)) s.threadIds.push(threadId);
    });
  }

  private bindWorktreeToSandbox(worktreePath: string, sandboxId: string): void {
    this.worktreeToSandbox.set(worktreePath, sandboxId);
    this.state.updateHubSandbox(sandboxId, (s) => {
      if (!s.worktreePaths.includes(worktreePath)) s.worktreePaths.push(worktreePath);
    });
  }

  private projectKeyFromHubId(hubProjectId: string): string | null {
    const hub = this.state.getHub();
    if (!hub) return null;
    for (const [k, v] of Object.entries(hub.projectIds)) {
      if (v === hubProjectId) return k;
    }
    return null;
  }

  private projectKeyFromWorkdir(cwd: string): string | null {
    for (const [k, p] of Object.entries(this.state.getAllProjects(this.config))) {
      if (cwd === p.workdir || cwd.startsWith(`${p.workdir}/`)) return k;
    }
    return null;
  }

  private publishWorktreeSetup(threadId: string, snapshot: WorktreeSetupSnapshotState): void {
    this.worktreeSetupSnapshots.set(threadId, snapshot);
    const subs = this.worktreeSetupSubs.get(threadId);
    if (subs) {
      for (const sub of subs) {
        sendRpcFrame(sub.ws, { _tag: "Chunk", requestId: sub.reqId, values: [snapshot] });
      }
    }
    if (snapshot.phase === "done" || snapshot.phase === "failed") {
      setTimeout(() => {
        if (this.worktreeSetupSnapshots.get(threadId)?.sequence === snapshot.sequence) {
          this.worktreeSetupSnapshots.delete(threadId);
        }
      }, 30_000);
    }
  }

  private removeWorktreeSetupSub(ws: ServerWebSocket<HubWsData>, reqKey: string): void {
    for (const [tid, subs] of this.worktreeSetupSubs) {
      for (const sub of subs) {
        if (sub.ws === ws && sub.reqKey === reqKey) {
          subs.delete(sub);
        }
      }
      if (subs.size === 0) this.worktreeSetupSubs.delete(tid);
    }
  }

  private removeWorktreeSetupSubsForWs(ws: ServerWebSocket<HubWsData>): void {
    for (const [tid, subs] of this.worktreeSetupSubs) {
      for (const sub of subs) {
        if (sub.ws === ws) subs.delete(sub);
      }
      if (subs.size === 0) this.worktreeSetupSubs.delete(tid);
    }
  }

  private broadcastProjectClones(): void {
    const list = Array.from(this.activeProjectClones.values());
    for (const client of this.clients) {
      for (const [, sub] of client.data.localStreams) {
        if (sub.tag === "subscribeProjectClones") {
          sendRpcFrame(client, { _tag: "Chunk", requestId: sub.rawId, values: [list] });
        }
      }
    }
  }

  private async handleServerSelfUpdate(
    ws: ServerWebSocket<HubWsData>,
    reqId: string | number,
    tag: "server.updateServer" | "server.updateServerWithProgress",
    payload: Record<string, unknown>,
  ): Promise<void> {
    const targetVersion =
      typeof payload.targetVersion === "string" && payload.targetVersion.trim()
        ? payload.targetVersion.trim()
        : this.config.t3.version;
    const channel =
      payload.channel === "nightly" || payload.channel === "stable"
        ? payload.channel
        : targetVersion.includes("nightly")
          ? "nightly"
          : this.config.t3.channel;
    const isStream = tag === "server.updateServerWithProgress";
    const sendProgress = (stage: SelfUpdateStage) => {
      if (isStream) {
        sendRpcFrame(ws, { _tag: "Chunk", requestId: reqId, values: [{ type: "progress", stage }] });
      }
    };

    // A request for a version already queued or running joins that round and gets its outcome
    const joined = this.selfUpdateRuns.get(targetVersion);
    if (joined) console.log(`[hub] T3 UI requested server update -> ${targetVersion}; joining the round in progress`);
    const run = joined ?? this.startSelfUpdateRun(targetVersion, channel);
    if (run.stage) sendProgress(run.stage);
    run.listeners.add(sendProgress);
    const outcome = await run.done;
    run.listeners.delete(sendProgress);

    if (!outcome.ok) {
      sendRpcExitFail(ws, reqId, { _tag: "ServerSelfUpdateError", reason: outcome.reason });
    } else if (isStream) {
      sendRpcFrame(ws, { _tag: "Chunk", requestId: reqId, values: [{ type: "complete", result: outcome.result }] });
      sendRpcExitSuccess(ws, reqId, undefined);
    } else {
      sendRpcExitSuccess(ws, reqId, outcome.result);
    }
  }

  /** Queues a self-update round behind any other version's round; registered before the first await so duplicates join it. */
  private startSelfUpdateRun(targetVersion: string, channel: "stable" | "nightly"): SelfUpdateRun {
    const run: SelfUpdateRun = { stage: null, listeners: new Set(), done: Promise.resolve({ ok: false, reason: "not started" }) };
    const setStage = (stage: SelfUpdateStage) => {
      run.stage = stage;
      for (const listener of run.listeners) listener(stage);
    };
    run.done = this.selfUpdateQueue
      .then(() => this.runSelfUpdate(targetVersion, channel, setStage))
      .then(
        (result): SelfUpdateOutcome => ({ ok: true, result }),
        (err): SelfUpdateOutcome => {
          // ServerSelfUpdateError.reason must be non-empty
          const reason = (err instanceof Error ? err.message : String(err)).trim() || "Golden snapshot rebake failed";
          console.warn(`[hub] server update to T3 ${targetVersion} failed: ${reason}`);
          return { ok: false, reason };
        },
      )
      .finally(() => this.selfUpdateRuns.delete(targetVersion));
    this.selfUpdateRuns.set(targetVersion, run);
    this.selfUpdateQueue = run.done;
    return run;
  }

  private async runSelfUpdate(
    targetVersion: string,
    channel: "stable" | "nightly",
    setStage: (stage: SelfUpdateStage) => void,
  ): Promise<SelfUpdateResult> {
    const result = { targetVersion, method: "boot-service" as const };
    if (this.isSelfUpdateBaked(targetVersion)) {
      console.log(`[hub] T3 UI requested server update -> ${targetVersion}; every golden snapshot already has it`);
      this.applyServerVersion(targetVersion, channel);
      return result;
    }

    console.log(`[hub] T3 UI requested server update -> ${targetVersion} (${channel}); rebaking golden snapshots`);
    setStage("downloading");
    await this.lifecycle.bake("base", { t3Version: targetVersion, t3Channel: channel });
    const bakes: Record<string, string> = {};
    const recordBake = (target: string) => {
      const bakedAt = this.state.getBake(target)?.bakedAt;
      if (bakedAt) bakes[target] = bakedAt;
    };
    recordBake("base");

    // Update in-memory config so the project bakes and future boots use the new version
    this.config.t3.version = targetVersion;
    this.config.t3.channel = channel;

    setStage("installing");
    for (const name of this.selfUpdateProjectTargets()) {
      await this.lifecycle.bake(name, { t3Version: targetVersion, t3Channel: channel });
      recordBake(name);
    }

    this.applyServerVersion(targetVersion, channel);
    // Only a whole round marks the version as baked, so a partial failure is retried next time
    this.state.updateHub((h) => {
      h.selfUpdate = { version: targetVersion, channel, completedAt: new Date().toISOString(), bakes };
    });
    console.log(`[hub] golden snapshots updated to T3 ${targetVersion}`);
    return result;
  }

  /** Projects whose own snapshot a self-update rebakes (the rest boot from base-t3). */
  private selfUpdateProjectTargets(): string[] {
    return Object.entries(this.state.getAllProjects(this.config))
      .filter(([, proj]) => proj.snapshot !== this.config.boat.baseSnapshot)
      .map(([name]) => name);
  }

  /** True when the last whole round baked this version and no target was rebaked or added since. */
  private isSelfUpdateBaked(targetVersion: string): boolean {
    const marker = this.state.getHub()?.selfUpdate;
    if (!marker || marker.version !== targetVersion) return false;
    return ["base", ...this.selfUpdateProjectTargets()].every((target) => {
      const bakedAt = marker.bakes[target];
      return bakedAt !== undefined && this.state.getBake(target)?.bakedAt === bakedAt;
    });
  }

  /** Makes the descriptor (and so the `subscribeServerLifecycle` replay) report this version. */
  private applyServerVersion(targetVersion: string, channel: "stable" | "nightly"): void {
    this.config.t3.version = targetVersion;
    this.config.t3.channel = channel;
    const prevTpl = this.state.getTemplates().descriptor;
    if (prevTpl && prevTpl.serverVersion !== targetVersion) {
      this.state.setTemplates({ ...prevTpl, serverVersion: targetVersion }, null);
    }
  }

  private async handleProjectCloneStart(
    ws: ServerWebSocket<HubWsData>,
    reqId: string | number,
    payload: Record<string, unknown>,
  ): Promise<void> {
    try {
      const projectId = typeof payload.projectId === "string" && payload.projectId ? payload.projectId : randomUUID();
      const title = typeof payload.title === "string" ? payload.title.trim() : "";
      const remoteUrlInput = typeof payload.remoteUrl === "string" ? payload.remoteUrl.trim() : "";
      const repositoryInput = typeof payload.repository === "string" ? payload.repository.trim() : "";
      const destinationPath = typeof payload.destinationPath === "string" ? payload.destinationPath.trim() : "";
      const repoInput = remoteUrlInput || repositoryInput;
      if (!repoInput) {
        sendRpcExitFailure(ws, reqId, "projectClone.start requires remoteUrl or repository");
        return;
      }

      const { name, project } = buildAutoProjectConfig({
        repo: repoInput,
        name: title || undefined,
        workdir: destinationPath || undefined,
      });

      this.state.addDynamicProject(name, project, projectId);
      this.notifyProjectAdded(name);
      this.startBackgroundProjectBake(projectId, name, project.repo ?? repoInput, project.workdir);

      console.log(`[hub] auto-registered repo "${name}" (${project.repo}) via T3 projectClone.start -> baking ${project.snapshot}`);
      sendRpcExitSuccess(ws, reqId, {
        projectId,
        cwd: project.workdir,
        remoteUrl: project.repo ?? repoInput,
        repository: null,
      });
    } catch (err) {
      sendRpcExitFailure(ws, reqId, err instanceof Error ? err.message : String(err));
    }
  }

  private startBackgroundProjectBake(
    projectId: string,
    projectKey: string,
    remoteUrl: string,
    destinationPath: string,
  ): void {
    const startedAt = new Date().toISOString();
    let seq = 1;
    this.activeProjectClones.set(projectId, {
      projectId,
      remoteUrl,
      destinationPath,
      repository: null,
      phase: "running",
      stage: "receiving",
      percent: 25,
      detail: `Baking immutable Boat snapshot env-${projectKey}...`,
      error: null,
      startedAt,
      endedAt: null,
      sequence: seq,
    });
    this.broadcastProjectClones();

    void this.lifecycle
      .bake(projectKey)
      .then((res) => {
        this.activeProjectClones.set(projectId, {
          projectId,
          remoteUrl,
          destinationPath,
          repository: null,
          phase: "done",
          stage: "checkout",
          percent: 100,
          detail: `Snapshot ${res.snapshot} ready`,
          error: null,
          startedAt,
          endedAt: new Date().toISOString(),
          sequence: ++seq,
        });
        this.broadcastProjectClones();
        setTimeout(() => {
          if (this.activeProjectClones.get(projectId)?.phase === "done") {
            this.activeProjectClones.delete(projectId);
            this.broadcastProjectClones();
          }
        }, 5_000);
      })
      .catch((err) => {
        const msg = err instanceof Error ? err.message : String(err);
        console.error(`[hub] background bake failed for "${projectKey}":`, msg);
        this.activeProjectClones.set(projectId, {
          projectId,
          remoteUrl,
          destinationPath,
          repository: null,
          phase: "failed",
          stage: "receiving",
          percent: null,
          detail: null,
          error: msg.slice(0, 1000),
          startedAt,
          endedAt: new Date().toISOString(),
          sequence: ++seq,
        });
        this.broadcastProjectClones();
      });
  }

  private findSandboxByCwd(cwd: string): HubSandboxRecord | undefined {
    // 1. Exact or prefix match on a thread's worktreePath (100% deterministic for parallel chats!)
    for (const [wt, sbId] of this.worktreeToSandbox) {
      if (cwd === wt || cwd.startsWith(`${wt}/`)) {
        const sb = this.state.getHubSandbox(sbId);
        if (sb && sb.phase === "running") return sb;
      }
    }
    // 2. Fallback to any running sandbox of the project matching this workdir
    const key = this.projectKeyFromWorkdir(cwd);
    return key ? this.anyRunningSandboxForProject(key) : undefined;
  }

  private anyRunningSandboxForProject(projectKey: string): HubSandboxRecord | undefined {
    return this.state.listHubSandboxes().find((s) => s.project === projectKey && s.phase === "running");
  }

  private anyRunningSandbox(): HubSandboxRecord | undefined {
    return this.state.listHubSandboxes().find((s) => s.phase === "running");
  }

  private async proxyHttpToSandbox(
    sb: HubSandboxRecord,
    req: Request,
    url: URL,
    isUrlTokenRoute: boolean,
  ): Promise<Response> {
    const upstreamUrl = `${sb.upstreamUrl}${url.pathname}${url.search}`;
    const headers = new Headers();
    for (const [k, v] of req.headers) {
      const lower = k.toLowerCase();
      if (HOP_BY_HOP.has(lower) || lower === "authorization") continue;
      headers.set(k, v);
    }
    if (sb.upstreamHeaders) {
      for (const [hk, hv] of Object.entries(sb.upstreamHeaders)) {
        headers.set(hk, hv);
      }
    }
    if (!isUrlTokenRoute) {
      headers.set("authorization", `Bearer ${sb.upstreamToken}`);
    }
    try {
      const res = await fetch(upstreamUrl, {
        method: req.method,
        headers,
        body: req.method === "GET" || req.method === "HEAD" ? undefined : req.body,
        redirect: "manual",
        signal: AbortSignal.timeout(60_000),
      });
      const outHeaders = new Headers();
      for (const [k, v] of res.headers) {
        if (!STRIP_RESPONSE_HEADERS.has(k.toLowerCase())) outHeaders.set(k, v);
      }
      return withCors(new Response(res.body, { status: res.status, headers: outHeaders }));
    } catch (err) {
      console.error(`[hub] HTTP proxy error to ${sb.sandboxId}:`, err);
      return withCors(Response.json({ error: "bad_gateway" }, { status: 502 }));
    }
  }
}

export function decodeRpcFrames<T>(raw: string): T[] {
  try {
    const parsed = JSON.parse(raw) as unknown;
    return (Array.isArray(parsed) ? parsed : [parsed]) as T[];
  } catch {
    return [];
  }
}

function sendRpcFrame(ws: ServerWebSocket<HubWsData>, frame: Record<string, unknown>): void {
  try {
    ws.sendText(JSON.stringify(frame));
  } catch {
    // client already closed
  }
}

function sendRpcExitSuccess(ws: ServerWebSocket<HubWsData>, requestId: string | number, value: unknown): void {
  sendRpcFrame(ws, {
    _tag: "Exit",
    requestId,
    // JSON drops an undefined key, and the client's Exit codec requires `value` (null for void and stream exits)
    exit: { _tag: "Success", value: value === undefined ? null : value },
  });
}

/**
 * One reason of an Effect 4 `Cause` as `RpcServer` encodes it: T3 Code 0.1.12+ sends a failure's
 * cause as an array of these. A `Fail` error is decoded with the RPC's own error schema, so a
 * Hub-only error type is rejected; a `Die` defect accepts any string.
 */
export type RpcCauseReason =
  | { _tag: "Fail"; error: Record<string, unknown> }
  | { _tag: "Die"; defect: unknown }
  | { _tag: "Interrupt"; fiberId: number | null };

/** Cause for a failure whose error is in no RPC's error union, as a real T3 server sends for an unknown tag. */
export function hubDefectCause(message: string): RpcCauseReason[] {
  return [{ _tag: "Die", defect: message }];
}

/** True for the Hub's own "could not reach the VM" failures, as opposed to an error the VM sent. */
function isHubTransportFailure(cause: unknown): boolean {
  const reasons = Array.isArray(cause) ? (cause as RpcCauseReason[]) : [];
  return reasons.some(
    (r) =>
      r._tag === "Die" &&
      typeof r.defect === "string" &&
      (["VM not connected", "Upstream VM disconnected", "Sandbox stopped"].includes(r.defect) || r.defect.endsWith(" timed out")),
  );
}

/** Ends a request with an untyped failure (`Die`): the client decodes it for every RPC. */
function sendRpcExitFailure(ws: ServerWebSocket<HubWsData>, requestId: string | number, message: string): void {
  sendRpcFrame(ws, {
    _tag: "Exit",
    requestId,
    exit: { _tag: "Failure", cause: hubDefectCause(message) },
  });
}

/** Ends a request with a typed failure; `error._tag` must be in that RPC's error union. */
function sendRpcExitFail(ws: ServerWebSocket<HubWsData>, requestId: string | number, error: Record<string, unknown>): void {
  sendRpcFrame(ws, {
    _tag: "Exit",
    requestId,
    exit: { _tag: "Failure", cause: [{ _tag: "Fail", error }] satisfies RpcCauseReason[] },
  });
}

const PROVIDER_SLUG_PATTERN = /^[a-zA-Z][a-zA-Z0-9_-]{0,63}$/;

/** The `instanceId` of a provider setup payload, if it is a valid provider slug. */
function providerInstanceIdOf(payload: unknown): string | null {
  const id = (payload as { instanceId?: unknown } | null)?.instanceId;
  return typeof id === "string" && PROVIDER_SLUG_PATTERN.test(id) ? id : null;
}

/**
 * The state a T3 server reports for a provider with no setup in progress: `ProviderAuthState` for
 * `provider.auth.subscribe`, `ProviderInstallState` for `provider.install.subscribe`. Fields
 * only a VM knows (runtime version, sign-in methods) are left unknown.
 */
export function idleProviderSetupState(tag: string, instanceId: string, providers: unknown): Record<string, unknown> {
  if (tag === "provider.auth.subscribe") {
    return {
      instanceId,
      phase: "idle",
      flowId: null,
      authorizationUrl: null,
      expiresAt: null,
      message: null,
      interaction: null,
    };
  }
  const cached = Array.isArray(providers)
    ? (providers as Array<{ instanceId?: unknown; driver?: unknown } | null>).find((p) => p?.instanceId === instanceId)
    : undefined;
  const driver = typeof cached?.driver === "string" && PROVIDER_SLUG_PATTERN.test(cached.driver) ? cached.driver : instanceId;
  return {
    driver,
    operationId: null,
    phase: "idle",
    downloadedBytes: 0,
    totalBytes: null,
    version: null,
    installedVersion: null,
    canRemove: false,
    message: null,
  };
}

function extractThreadId(payload: unknown): string | null {
  if (!payload || typeof payload !== "object") return null;
  const obj = payload as Record<string, unknown>;
  for (const key of ["threadId", "targetThreadId", "sourceThreadId", "parentThreadId"]) {
    if (typeof obj[key] === "string" && obj[key]) return obj[key] as string;
  }
  // `assets.createUrl` nests it: `{ resource: { _tag: "media-file", threadId, path } }`
  const resource = obj.resource as Record<string, unknown> | null | undefined;
  if (resource && typeof resource === "object" && typeof resource.threadId === "string" && resource.threadId) {
    return resource.threadId;
  }
  return null;
}

/** RPCs whose `threadId` is not a chat: a pull request review thread, or a thread in a linked environment. */
const NON_CHAT_THREAD_ID_PREFIXES = ["pullRequests."];
const NON_CHAT_THREAD_ID_METHODS = new Set(["peerLinks.subscribeThread", "peerLinks.threadItem", "peerLinks.threadHistoryPage"]);

/** The chat a request belongs to, for RPCs without an explicit route; null for one that names no chat. */
export function chatThreadIdOf(tag: string, payload: unknown): string | null {
  if (NON_CHAT_THREAD_ID_METHODS.has(tag) || NON_CHAT_THREAD_ID_PREFIXES.some((p) => tag.startsWith(p))) return null;
  return extractThreadId(payload);
}

const THREAD_VM_GONE_DETAIL =
  "This chat's Boat VM no longer exists. Chats on Boat are disposable: their files and pending requests went with the VM.";

/**
 * The typed error (in that RPC's own error union, per T3 Code 0.1.13 contracts) for a chat-scoped
 * request whose VM is gone, or null when the union has no fitting member (the caller sends `Die`).
 */
export function threadGoneError(tag: string, payload: unknown): Record<string, unknown> | null {
  const p = (payload && typeof payload === "object" ? payload : {}) as Record<string, unknown>;
  switch (tag) {
    case "assets.createUrl":
      // Echoes the client's own resource, so it decodes as `AssetResource`
      return p.resource && typeof p.resource === "object"
        ? { _tag: "AssetWorkspaceContextNotFoundError", resource: p.resource }
        : null;
    case "secrets.answerRequest":
      return { _tag: "SecretRequestError", reason: "not_found" };
    case "agentFeedback.answerRequest":
      return { _tag: "AgentFeedbackRequestError", reason: "not_found" };
    case "peerLinks.answerRequest":
      return { _tag: "PeerLinkRequestError", reason: "not_found" };
    case "guestShare.create":
      return { _tag: "GuestShareCreateError", reason: "thread_unavailable" };
    case "provider.mcpSignIn.start":
    case "provider.mcpSignIn.complete":
      return typeof p.server === "string" ? { _tag: "ProviderMcpSignInError", server: p.server, detail: THREAD_VM_GONE_DETAIL } : null;
    case "provider.uploadFeedback":
      return typeof p.threadId === "string" ? { _tag: "ProviderUploadFeedbackError", threadId: p.threadId } : null;
    case "workspace.addTaskFolderRepository":
      return { _tag: "TaskFolderError", operation: "add-repository", detail: THREAD_VM_GONE_DETAIL };
    case "scheduledTasks.upsert":
      return { _tag: "ScheduledTaskError", message: THREAD_VM_GONE_DETAIL };
    case "assets.persistChatAttachments":
      return { _tag: "PersistChatAttachmentsError", message: THREAD_VM_GONE_DETAIL };
  }
  if (tag.startsWith("mcpApps.") && typeof p.threadId === "string") {
    return { _tag: "McpAppRequestError", threadId: p.threadId, reason: "session-stopped" };
  }
  return null;
}

export function rewriteProjectIds(val: unknown, fromId: string, toId: string): unknown {
  if (!fromId || !toId || fromId === toId || val === null || val === undefined) return val;
  if (typeof val === "string") return val === fromId ? toId : val;
  if (Array.isArray(val)) return val.map((item) => rewriteProjectIds(item, fromId, toId));
  if (typeof val === "object") {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(val as Record<string, unknown>)) {
      out[k] = rewriteProjectIds(v, fromId, toId);
    }
    return out;
  }
  return val;
}

function withCors(res: Response): Response {
  const headers = new Headers(res.headers);
  headers.set("access-control-allow-origin", "*");
  headers.set("access-control-allow-methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS");
  headers.set(
    "access-control-allow-headers",
    "authorization, b3, traceparent, content-type, dpop, x-t3-orchestration-protocol",
  );
  headers.set("access-control-max-age", "600");
  return applySecurityHeaders(new Response(res.body, { status: res.status, statusText: res.statusText, headers }));
}
