import { randomUUID } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
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
import type { ProjectConfig, ProjectKlavConfig, SentinelConfig } from "./config.ts";
import { HubMcpHandler, formatRpcFailure, type HubSandboxPhase } from "./hub-mcp.ts";
import { HubShellStore } from "./hub-shell-store.ts";
import { repositoryIdentityFor } from "./repository-identity.ts";
import { defaultBranchOfRefs, fallbackVcsRefs, isVcsRefsResult, templateVcsRefs } from "./vcs-refs.ts";
import {
  ATTACHMENT_UPLOAD_ROUTE_PREFIX,
  ATTACHMENT_UPLOAD_URL_TTL_MS,
  STAGED_ATTACHMENT_MAX_AGE_MS,
  parseUploadRequest,
  replayUploadRequest,
  stagedAssetHeaders,
  stagedAttachmentId,
  type StagedAttachment,
} from "./hub-attachments.ts";
import {
  buildPlaceholderThread,
  failPlaceholderThread,
  placeholderInputFrom,
  projectionFromShell,
  restoredShellRow,
  withStatusNotice,
  type PlaceholderInput,
  type StatusNotice,
} from "./hub-placeholder.ts";
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
import { releaseVmForwarder, type KlavClient } from "./klav.ts";
import {
  HubWakeCooldownError,
  redactForLog,
  wakeBackoffMs,
  type HubWakeEvent,
  type HubWakeListener,
} from "./hub-wake.ts";
import type { LifecycleManager, WatchdogEventPayload } from "./lifecycle.ts";
import {
  buildAutoProjectConfig,
  type HubPendingLaunchRecord,
  type HubSandboxRecord,
  type HubWakeFailure,
  type StateStore,
} from "./state.ts";

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

/**
 * A chat the Hub answered at once while its VM boots. The Hub serves the thread's placeholder until
 * the thread exists on the VM, then hands its streams over; commands for it wait on `ready`.
 */
interface PendingThreadLaunch {
  record: HubPendingLaunchRecord;
  /** The VM's shell already lists the thread (a launch resumed after a Hub restart): don't send it again. */
  seenOnVm: boolean;
  /** Set when the chat is archived or deleted while booting: the VM is destroyed once it exists. */
  cancelled: boolean;
  /** Settles when the thread exists on its VM; rejects when the launch fails or is cancelled. */
  ready: Promise<HubSandboxRecord>;
  resolveReady: (sb: HubSandboxRecord) => void;
  rejectReady: (err: Error) => void;
}

/** A thread stream the Hub serves itself while the thread's VM is booting, asleep or waking. */
interface LocalThreadSub {
  ws: ServerWebSocket<HubWsData>;
  reqId: string | number;
  payload: unknown;
  headers: unknown;
  /** False when the Hub sent only `synchronized` (the client keeps its own cached copy). */
  servedSnapshot: boolean;
}

/** Commands that only record what the user did; answered at once and sent once the VM is up. */
const BOOKKEEPING_COMMANDS = new Set([
  "thread.visit",
  "thread.mark-unread",
  "thread.pin",
  "thread.unpin",
  "thread.pin.reorder",
  "thread.active.reorder",
  "thread.tag.set",
]);

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
/** What a sleeping chat shows about its VM's wake; `error` is the redacted failure reason. */
type WakeStatus = { phase: "waking" } | { phase: "failed"; error: string; retryAfter: string | null };

/** A wake that failed; its message is already redacted and safe to send to a client. */
class HubWakeFailedError extends Error {
  override name = "HubWakeFailedError";
}

export class HubServer {
  private readonly auth = new SlotAuth();
  private readonly bridges = new Map<string, HubVmBridge>();
  private readonly clients = new Set<ServerWebSocket<HubWsData>>();
  private readonly threadToSandbox = new Map<string, string>();
  private readonly worktreeToSandbox = new Map<string, string>();
  /** Each active chat's sidebar row; stored on disk (`shellStore`) while the chat has a sandbox. */
  private readonly threadShells = new Map<string, Record<string, unknown>>();
  private readonly shellStore: HubShellStore;
  private readonly inFlightSpawns = new Map<string, Promise<HubSandboxRecord>>();
  private readonly inFlightWakes = new Map<string, Promise<HubSandboxRecord>>();
  private readonly inFlightSleeps = new Map<string, Promise<void>>();
  private readonly wakeListeners = new Set<HubWakeListener>();
  /** Pause retries for VMs a failed wake left running. */
  private readonly pauseRetries = new Map<string, ReturnType<typeof setTimeout>>();
  /** First cooldown after a failed wake; doubles per consecutive failure up to `wakeBackoffMaxMs`. */
  wakeBackoffBaseMs = 30_000;
  wakeBackoffMaxMs = 15 * 60_000;
  pauseRetryMs = 60_000;
  /** How long a leftover `klav-forward` gets after TERM before KILL. */
  forwarderTermTimeoutSecs = 10;
  /** How long a woken VM's bridge may take to open. */
  wakeBridgeTimeoutMs = 20_000;
  /** Chats answered before their VM was ready (booting or failed), by thread id. */
  private readonly pendingThreads = new Map<string, PendingThreadLaunch>();
  /** Thread streams the Hub serves while the thread's VM is not up, by thread id. */
  private readonly localThreadSubs = new Map<string, LocalThreadSub[]>();
  /** A sleeping VM being woken, or whose last wake failed (with the reason), by sandbox id. */
  private readonly wakeStatus = new Map<string, WakeStatus>();
  /** Each thread's projection, read from its VM before the VM went to sleep (also on disk). */
  private readonly projectionCache = new Map<string, Record<string, unknown>>();
  private readonly urlTokenToSandbox = new Map<string, string>();
  private readonly stagedAttachments = new Map<string, StagedAttachment>();
  private readonly stagedUploadTokens = new Map<string, string>(); // uploadToken -> attachmentId
  /** Asset URL tokens the Hub signed for a staged upload (the client checks an upload before sending) -> attachment id. */
  private readonly stagedAssetTokens = new Map<string, string>();
  /**
   * Staged uploads a chat answered before its VM is up still has to send. The app deletes its uploads
   * right after a send succeeds, which for such a chat is before the Hub replays them into the VM.
   */
  private readonly pinnedStagedAttachments = new Set<string>();
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
    this.shellStore = new HubShellStore(config.stateDir, (threadId) => this.keepsStoredShell(threadId));
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
    // What sleeping chats show follows the wake transitions: waking, then handed to the VM or why it failed
    this.onWakeTransition((e) => {
      if (e.type === "wake-start") this.onWakeStatus(e.sandboxId, { phase: "waking" });
      else if (e.type === "wake-success") this.onWakeStatus(e.sandboxId, null);
      else this.onWakeStatus(e.sandboxId, { phase: "failed", error: e.reason, retryAfter: e.retryAfter });
    });
    if (config.hub) {
      this.state.ensureHub(config.hub.label, Object.keys(this.state.getAllProjects(config)));
      this.restoreLiveSandboxes();
      this.restorePendingLaunches();
    }
  }

  private restoreLiveSandboxes(): void {
    for (const sb of this.state.listHubSandboxes()) {
      for (const tid of sb.threadIds) this.threadToSandbox.set(tid, sb.sandboxId);
      for (const wt of sb.worktreePaths) this.worktreeToSandbox.set(wt, sb.sandboxId);
    }
    this.restoreThreadShells();
    for (const sb of this.state.listHubSandboxes()) {
      if (sb.phase === "running") {
        this.attachBridge(sb);
      }
    }
  }

  /**
   * After a restart: every chat whose VM still exists (asleep, booting or running) keeps its sidebar
   * row until its VM reports its own. A chat with no stored row (its VM slept before the Hub kept
   * rows) gets one built from its cached projection, else from the project's defaults. Rows of
   * chats whose sandbox is gone are deleted.
   */
  private restoreThreadShells(): void {
    let restored = 0;
    let rebuilt = 0;
    for (const sb of this.state.listHubSandboxes()) {
      if (sb.phase === "destroying") continue;
      const project = this.state.getAllProjects(this.config)[sb.project];
      for (const threadId of sb.threadIds) {
        let row = this.shellStore.load(threadId);
        if (row) {
          restored++;
        } else {
          const cached = this.readProjectionCache(threadId);
          row = restoredShellRow({
            threadId,
            hubProjectId: sb.hubProjectId,
            providerInstanceId: project?.defaultProvider ?? "claudeAgent",
            model: project?.defaultModel ?? "claude-opus-5-5",
            at: sb.sleepingSince ?? sb.lastActiveAt ?? sb.createdAt,
            projectionThread: (cached?.thread as Record<string, unknown> | undefined) ?? null,
          });
          this.shellStore.save(threadId, row);
          rebuilt++;
        }
        this.threadShells.set(threadId, row);
      }
    }
    for (const threadId of this.shellStore.storedThreadIds()) {
      if (!this.keepsStoredShell(threadId)) this.shellStore.remove(threadId);
    }
    if (restored + rebuilt > 0) console.log(`[hub] restored ${restored} chat row(s) from disk, rebuilt ${rebuilt} without one`);
  }

  /** A row is kept on disk only while its chat is bound to a sandbox that still exists. */
  private keepsStoredShell(threadId: string): boolean {
    if (!this.threadShells.has(threadId)) return false;
    const sandboxId = this.threadToSandbox.get(threadId);
    const sb = sandboxId ? this.state.getHubSandbox(sandboxId) : undefined;
    return !!sb && sb.phase !== "destroying" && sb.threadIds.includes(threadId);
  }

  private setThreadShell(threadId: string, row: Record<string, unknown>): void {
    this.threadShells.set(threadId, row);
    this.shellStore.save(threadId, row);
  }

  private deleteThreadShell(threadId: string): void {
    this.threadShells.delete(threadId);
    this.shellStore.remove(threadId);
  }

  stop(): void {
    this.shellStore.flush();
    for (const timer of this.pauseRetries.values()) clearTimeout(timer);
    this.pauseRetries.clear();
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

  /**
   * Forwards an environment stream the Hub keeps open (automations, previews...). When its VM goes
   * away (sleep, destroy) the stream moves to another open VM, or waits quietly for the next one,
   * instead of failing with "The environment request failed."
   */
  private forwardKeepOpenStream(
    bridge: HubVmBridge,
    ws: ServerWebSocket<HubWsData>,
    reqId: string | number,
    tag: string,
    payload: unknown,
    headers: unknown,
  ): void {
    const lostSandboxId = bridge.sandbox.sandboxId;
    bridge.forwardToClient(ws, reqId, tag, payload, headers, undefined, () => {
      if (!this.clients.has(ws) || !ws.data.localStreams.has(String(reqId))) return;
      const next = Array.from(this.bridges.values()).find(
        (b) => b.sandbox.sandboxId !== lostSandboxId && b.sandbox.phase === "running" && b.isOpen(),
      );
      if (next) this.forwardKeepOpenStream(next, ws, reqId, tag, payload, headers);
    });
  }

  /** Gives streams that were waiting for a VM to one whose bridge just opened. */
  private adoptKeepOpenStreams(sandboxId: string): void {
    const bridge = this.bridges.get(sandboxId);
    if (!bridge?.isOpen()) return;
    for (const client of this.clients) {
      for (const [key, sub] of client.data.localStreams) {
        if (!KEEP_OPEN_SUBSCRIPTIONS.has(sub.tag) || client.data.upstreamRequests.has(key)) continue;
        this.forwardKeepOpenStream(bridge, client, sub.rawId, sub.tag, sub.payload ?? {}, []);
      }
    }
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

  /** `subscribeAuthAccess` snapshot: the Hub's paired clients as bearer-token sessions; no pairing links. */
  private buildAuthAccessSnapshot(currentClientId: string): Record<string, unknown> {
    const hub = this.state.getHub();
    const connectedIds = new Set(Array.from(this.clients, (c) => c.data.clientId));
    const session = this.buildAuthSession();
    return {
      version: 1,
      revision: hub?.clients.length ?? 0,
      type: "snapshot",
      payload: {
        pairingLinks: [],
        clientSessions: (hub?.clients ?? []).map((c) => ({
          sessionId: c.id,
          subject: c.label,
          scopes: session.scopes,
          permissions: session.permissions,
          method: "bearer-access-token",
          client: { label: c.label, deviceType: "unknown" },
          issuedAt: c.pairedAt,
          expiresAt: session.expiresAt, // rolling, as `/api/auth/session` reports it
          lastConnectedAt: null,
          connected: connectedIds.has(c.id),
          current: c.id === currentClientId,
        })),
      },
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
      // Required by the client's ServerConfig; the Hub exports no telemetry
      observability: {
        logsDirectoryPath: "/home/user/.t3/userdata/logs",
        localTracingEnabled: false,
        otlpTracesEnabled: false,
        otlpMetricsEnabled: false,
        otlpLogsEnabled: false,
      },
      // Every ServerSettings key has a decoding default; a VM's real settings replace these
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
    // The app offers "Run on Boat" for a local project whose repository identity matches; a VM's
    // captured identity wins, else it is derived from the project's repo like a T3 server does
    const derivedIdentity = proj.repo ? repositoryIdentityFor(proj.repo, proj.workdir) : null;
    const cached = hub.projectTemplates[key]?.shell;
    if (cached && typeof cached === "object") {
      const captured = (cached as { repositoryIdentity?: unknown }).repositoryIdentity;
      return {
        ...cached,
        id: projectId,
        title: key,
        workspaceRoot: proj.workdir,
        ...(!captured && derivedIdentity ? { repositoryIdentity: derivedIdentity } : {}),
      };
    }
    return {
      id: projectId,
      title: key,
      workspaceRoot: proj.workdir,
      ...(derivedIdentity ? { repositoryIdentity: derivedIdentity } : {}),
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
    if (req.method === "POST" && path.startsWith(ATTACHMENT_UPLOAD_ROUTE_PREFIX)) {
      const uploadToken = path.slice(ATTACHMENT_UPLOAD_ROUTE_PREFIX.length);
      const stagedId = this.stagedUploadTokens.get(uploadToken);
      if (stagedId) return withCors(await this.receiveStagedUpload(req, uploadToken, stagedId));
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
      const stagedAssetId = this.stagedAssetTokens.get(token);
      if (stagedAssetId) {
        const staged = this.stagedAttachments.get(stagedAssetId);
        if (!staged?.bytes) return withCors(new Response("Not Found", { status: 404 }));
        return withCors(new Response(req.method === "HEAD" ? null : Buffer.from(staged.bytes), { headers: stagedAssetHeaders(staged) }));
      }
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
    // Thread snapshots go to the thread's own VM. Another VM answers 404, and the client then treats
    // the thread as deleted; a 503 makes it fall back to the socket, which the Hub serves while booting.
    const threadRoute = /^\/api\/orchestration\/threads\/([^/]+)/.exec(path);
    if (threadRoute) {
      const tid = decodeURIComponent(threadRoute[1]);
      const ownerId = this.pendingThreads.has(tid) ? undefined : this.threadToSandbox.get(tid);
      const owner = ownerId ? this.state.getHubSandbox(ownerId) : undefined;
      if (owner?.phase === "running") return this.proxyHttpToSandbox(owner, req, url, false);
      if (this.pendingThreads.has(tid) || owner) {
        return withCors(Response.json({ error: "thread_starting" }, { status: 503 }));
      }
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
      sendRpcFrame(ws, { _tag: "Chunk", requestId: reqId, values: [this.buildAuthAccessSnapshot(ws.data.clientId)] });
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
      // The merged snapshot already lists every thread (VM shells are read without `omitOlderSettled`)
      sendRpcExitSuccess(ws, reqId, { snapshotSequence: this.state.ensureHub().sequence, threads: [], counts: [] });
      return;
    }
    if (tag === "subscribeTerminalMetadata") {
      ws.data.localStreams.set(reqKey, { rawId: reqId, tag });
      sendRpcFrame(ws, { _tag: "Chunk", requestId: reqId, values: [{ type: "snapshot", terminals: [] }] });
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
      ws.data.localStreams.set(reqKey, { rawId: reqId, tag, payload });
      const live = this.anyRunningSandbox();
      const bridge = live ? this.bridges.get(live.sandboxId) : undefined;
      if (bridge?.isOpen()) {
        this.forwardKeepOpenStream(bridge, ws, reqId, tag, payload, headers);
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
      const upload = parseUploadRequest(payload);
      if (!upload) {
        // A T3 server rejects a payload its schema does not accept with a defect
        sendRpcExitFailure(ws, reqId, "Invalid attachments.createUploadUrl payload");
        return;
      }
      this.sweepStagedAttachments();
      const now = Date.now();
      const id = stagedAttachmentId(upload.type, upload.name);
      const uploadToken = randomToken(24);
      const expiresAt = now + ATTACHMENT_UPLOAD_URL_TTL_MS;
      this.stagedAttachments.set(id, { id, ...upload, bytes: null, createdAt: now, expiresAt });
      this.stagedUploadTokens.set(uploadToken, id);
      sendRpcExitSuccess(ws, reqId, { attachmentId: id, relativeUrl: `${ATTACHMENT_UPLOAD_ROUTE_PREFIX}${uploadToken}`, expiresAt });
      return;
    }
    if (tag === "attachments.delete") {
      // A staged upload is the Hub's own: drop it here (the client discards drafts this way)
      const attachmentId = String((payload as { attachmentId?: unknown } | null)?.attachmentId ?? "");
      if (this.stagedAttachments.has(attachmentId)) {
        // One a booting chat still has to send is kept until it reaches the VM (or the chat closes)
        if (!this.pinnedStagedAttachments.has(attachmentId)) this.dropStagedAttachment(attachmentId);
        sendRpcExitSuccess(ws, reqId, null);
        return;
      }
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
      const servable = tag === "orchestration.subscribeThread" || tag === "orchestration.getThreadProjection";
      const pending = this.pendingThreads.get(tid);
      if (pending && servable) {
        this.serveLocalThread(ws, reqId, tag, payload, pending.record.projection);
        return;
      }
      if (!pending && servable && this.serveSleepingThread(ws, reqId, tag, payload, tid)) return;
      if (tag === "orchestration.getThreadFamily" && (pending || this.sleepingSandboxForThread(tid))) {
        // Opening the chat asks for its family; answer from the sidebar row instead of waiting for the VM
        const shell = this.threadShells.get(tid);
        if (shell) {
          sendRpcExitSuccess(ws, reqId, { snapshotSequence: this.state.ensureHub().sequence, threads: [shell] });
          return;
        }
      }
      try {
        const sb = await this.resolveSandboxForThread(tid);
        const bridge = await this.ensureBridgeOpen(sb);
        // The Hub's placeholder snapshot is sequence 0: a VM must answer such a client with a full snapshot
        bridge.forwardToClient(ws, reqId, tag, tag === "orchestration.subscribeThread" ? withoutZeroCursor(payload) : payload, headers);
      } catch (err) {
        this.sendTypedFailure(ws, reqId, tag, payload, err instanceof Error ? err.message : String(err));
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
    if (tag === "peerLinks.list" || tag === "peerLinks.names") {
      // No VM: no linked environments to list
      sendRpcExitSuccess(ws, reqId, { links: [] });
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

      const pending = this.pendingThreads.get(threadId);
      if (pending && !reuse) {
        // The client re-sends a launch after a socket loss: answer with the thread the Hub shows
        sendRpcExitSuccess(ws, reqId, { threadId, projection: pending.record.projection, resumed: false });
        return;
      }
      if (!pending && !this.threadToSandbox.has(threadId)) {
        const projectKey = this.projectKeyFromHubId(hubProjectId);
        if (!projectKey) {
          this.sendTypedFailure(ws, reqId, "orchestration.launchThread", payload, `Unknown projectId "${hubProjectId}"`);
          return;
        }
        const input = placeholderInputFrom("launch", payload, threadId, hubProjectId);
        // Without the fields a placeholder needs, keep the awaited launch below
        if (input) {
          const limit = this.capacityError();
          if (limit) {
            this.sendTypedFailure(ws, reqId, "orchestration.launchThread", payload, limit);
            return;
          }
          const started = this.startPendingLaunch("launch", projectKey, { ...payload, threadId }, input);
          // No `sequence`: the client then waits for the first message by id, which the placeholder holds
          sendRpcExitSuccess(ws, reqId, { threadId, projection: started.pending.record.projection, resumed: false });
          return;
        }
      }

      let sb: HubSandboxRecord;
      if (reuse && (this.threadToSandbox.has(threadId) || this.pendingThreads.has(threadId))) {
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
      this.sendTypedFailure(ws, reqId, "orchestration.launchThread", payload, err instanceof Error ? err.message : String(err));
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
        if (this.pendingThreads.has(threadId)) {
          sendRpcExitSuccess(ws, reqId, { sequence: this.state.ensureHub().sequence });
          return;
        }
        const input = this.threadToSandbox.has(threadId)
          ? null
          : placeholderInputFrom("create", payload, threadId, hubProjectId);
        if (input) {
          const limit = this.capacityError();
          if (limit) {
            this.sendTypedFailure(ws, reqId, "orchestration.dispatchCommand", payload, limit);
            return;
          }
          const started = this.startPendingLaunch("create", projectKey, { ...payload, threadId }, input);
          sendRpcExitSuccess(ws, reqId, { sequence: started.sequence });
          return;
        }
        const sb = await this.spawnSandboxForThread(projectKey, threadId);
        const bridge = await this.ensureBridgeOpen(sb);
        const forwarded = await this.replayStagedAttachmentsInPayload(
          sb,
          bridge,
          rewriteProjectIds(payload, sb.hubProjectId, sb.vmProjectId) as Record<string, unknown>,
        );
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
      const pending = this.pendingThreads.get(tid);
      if (pending && (cmdType === "thread.archive" || cmdType === "thread.delete")) {
        sendRpcExitSuccess(ws, reqId, { sequence: this.cancelPendingLaunch(pending, cmdType) });
        return;
      }
      if (BOOKKEEPING_COMMANDS.has(cmdType) && !this.threadVmIsUp(tid)) {
        // Answered now so opening a booting or sleeping chat raises no slow-request warning; the VM
        // gets it once up (for a sleeping chat this starts the wake, as opening it always did)
        sendRpcExitSuccess(ws, reqId, { sequence: this.state.ensureHub().sequence });
        void this.forwardWhenUp(tid, payload);
        return;
      }
      // Other commands for a booting chat wait for its VM (resolveSandboxForThread)
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
      this.sendTypedFailure(ws, reqId, "orchestration.dispatchCommand", payload, err instanceof Error ? err.message : String(err));
    }
  }

  // ── Chats answered before their VM is ready ───────────────────────────────────────────────────

  /**
   * Shows a new chat at once (shell item before the client's reply) and boots its VM in the
   * background. The spawn starts synchronously, so it counts toward the sandbox limit right away.
   */
  private startPendingLaunch(
    kind: HubPendingLaunchRecord["kind"],
    projectKey: string,
    payload: Record<string, unknown>,
    input: PlaceholderInput,
  ): { pending: PendingThreadLaunch; sequence: number } {
    const placeholder = buildPlaceholderThread(input);
    const pending = this.trackPendingLaunch({
      threadId: input.threadId,
      projectKey,
      hubProjectId: input.hubProjectId,
      kind,
      payload,
      shell: placeholder.shell,
      projection: placeholder.projection,
      phase: "booting",
      error: null,
      createdAt: input.now,
    });
    this.persistPendingLaunch(pending.record);
    for (const id of this.stagedIdsIn(payload)) this.pinnedStagedAttachments.add(id);
    const sequence = this.state.nextHubSequence();
    this.broadcastShellItem({ kind: "thread.updated", sequence, location: "active", thread: placeholder.shell });
    console.log(`[hub] chat ${input.threadId} answered at once; booting its VM in the background (project=${projectKey})`);
    void this.runPendingLaunch(pending);
    return { pending, sequence };
  }

  private trackPendingLaunch(record: HubPendingLaunchRecord): PendingThreadLaunch {
    let resolveReady!: (sb: HubSandboxRecord) => void;
    let rejectReady!: (err: Error) => void;
    const ready = new Promise<HubSandboxRecord>((resolve, reject) => {
      resolveReady = resolve;
      rejectReady = reject;
    });
    ready.catch(() => {});
    const pending: PendingThreadLaunch = { record, seenOnVm: false, cancelled: false, ready, resolveReady, rejectReady };
    if (record.phase === "failed") rejectReady(new Error(record.error ?? "This chat's VM failed to start"));
    this.pendingThreads.set(record.threadId, pending);
    this.setThreadShell(record.threadId, record.shell);
    return pending;
  }

  private persistPendingLaunch(record: HubPendingLaunchRecord): void {
    this.state.updateHub((h) => {
      h.pendingLaunches = { ...(h.pendingLaunches ?? {}), [record.threadId]: record };
    });
  }

  private forgetPendingLaunch(threadId: string): void {
    this.pendingThreads.delete(threadId);
    if (!this.state.getHub()?.pendingLaunches?.[threadId]) return;
    this.state.updateHub((h) => {
      const { [threadId]: _gone, ...rest } = h.pendingLaunches ?? {};
      if (Object.keys(rest).length > 0) h.pendingLaunches = rest;
      else delete h.pendingLaunches;
    });
  }

  /** After a Hub restart: failed chats stay in the sidebar, booting ones start their launch again. */
  private restorePendingLaunches(): void {
    for (const record of Object.values(this.state.getHub()?.pendingLaunches ?? {})) {
      const pending = this.trackPendingLaunch(record);
      if (record.phase === "booting") {
        console.log(`[hub] resuming the launch of chat ${record.threadId}, interrupted by a Hub restart`);
        void this.runPendingLaunch(pending);
      }
    }
  }

  /** Sandboxes that exist or are being created; an MCP or chat spawn counts from its first step. */
  private activeSandboxCount(): number {
    const live = this.state.listHubSandboxes().filter((s) => s.phase !== "destroying").length;
    let booting = 0;
    for (const tid of this.inFlightSpawns.keys()) {
      if (!this.threadToSandbox.has(tid)) booting++;
    }
    return live + booting;
  }

  private capacityError(): string | null {
    const max = this.config.hub?.maxSandboxes ?? 10;
    const active = this.activeSandboxCount();
    return active >= max ? `Hub sandbox limit reached (${active}/${max}). Settle or archive an existing chat first.` : null;
  }

  /** Boots the chat's VM, creates the thread there, then hands the client's streams over to it. */
  private async runPendingLaunch(pending: PendingThreadLaunch): Promise<void> {
    const { threadId, projectKey, kind } = pending.record;
    const startedAt = Date.now();
    let sb: HubSandboxRecord | null = null;
    try {
      sb = await this.spawnSandboxForThread(projectKey, threadId);
      if (pending.cancelled) return await this.discardCancelledLaunch(sb, threadId);
      const forwardStartedAt = Date.now();
      const bridge = await this.ensureBridgeOpen(sb);
      // A VM reused after a Hub restart replays the settings overlay on this connection first
      await this.settingsReplayDone(sb.sandboxId);
      let exit: { _tag: "Success"; value: unknown } | { _tag: "Failure"; cause: unknown };
      if (pending.seenOnVm) {
        exit = { _tag: "Success", value: null };
      } else if (kind === "launch") {
        const payload = await this.replayStagedAttachmentsInPayload(sb, bridge, {
          ...pending.record.payload,
          threadId,
          projectId: sb.vmProjectId,
        });
        exit = await bridge.callUnary("orchestration.launchThread", payload, 120_000);
      } else {
        const payload = await this.replayStagedAttachmentsInPayload(
          sb,
          bridge,
          rewriteProjectIds({ ...pending.record.payload, threadId }, sb.hubProjectId, sb.vmProjectId) as Record<string, unknown>,
        );
        exit = await bridge.callUnary("orchestration.dispatchCommand", payload, 60_000);
      }
      if (exit._tag !== "Success") throw new Error(`The chat's VM could not start it: ${formatRpcFailure(exit.cause)}`);
      if (pending.cancelled) return await this.discardCancelledLaunch(sb, threadId);

      this.bindThreadToSandbox(threadId, sb.sandboxId);
      const value = rewriteProjectIds(exit.value, sb.vmProjectId, sb.hubProjectId) as {
        projection?: { thread?: { worktreePath?: string | null } };
      } | null;
      if (value?.projection?.thread?.worktreePath) this.bindWorktreeToSandbox(value.projection.thread.worktreePath, sb.sandboxId);
      this.forgetPendingLaunch(threadId);
      console.log(
        `[hub] chat ${threadId} started on ${sb.sandboxId}: forward=${Date.now() - forwardStartedAt}ms, ${Date.now() - startedAt}ms after the client's request`,
      );
      pending.resolveReady(sb);
      this.handOffLocalThreadSubs(threadId, sb);
    } catch (err) {
      if (pending.cancelled) {
        if (sb) await this.discardCancelledLaunch(sb, threadId);
        return;
      }
      this.failPendingLaunch(pending, err instanceof Error ? err.message : String(err), sb);
    }
  }

  /** Archive or delete while booting: drop the chat now; its VM is destroyed as soon as it exists. */
  private cancelPendingLaunch(pending: PendingThreadLaunch, command: string): number {
    const { threadId } = pending.record;
    pending.cancelled = true;
    for (const id of this.stagedIdsIn(pending.record.payload)) this.dropStagedAttachment(id);
    this.forgetPendingLaunch(threadId);
    this.deleteThreadShell(threadId);
    this.localThreadSubs.delete(threadId);
    pending.rejectReady(new Error("This chat was closed before its VM finished starting"));
    const sequence = this.state.nextHubSequence();
    this.broadcastShellItem({ kind: "thread.removed", sequence, location: "active", threadId });
    console.log(`[hub] chat ${threadId} closed (${command}) while ${pending.record.phase === "failed" ? "failed" : "booting"}`);
    return sequence;
  }

  private async discardCancelledLaunch(sb: HubSandboxRecord, threadId: string): Promise<void> {
    console.log(`[hub] destroying ${sb.sandboxId}: chat ${threadId} was closed while its VM booted`);
    await this.destroyHubSandbox(sb.sandboxId, "chat-closed-while-booting").catch((err) => {
      console.warn(`[hub] could not destroy ${sb.sandboxId}:`, err);
    });
  }

  /** Shows the error in the chat (setup card, run, sidebar) and destroys a VM the launch left behind. */
  private failPendingLaunch(pending: PendingThreadLaunch, message: string, sb: HubSandboxRecord | null): void {
    const { threadId } = pending.record;
    console.error(`[hub] chat ${threadId} failed to start: ${message}`);
    for (const id of this.stagedIdsIn(pending.record.payload)) this.dropStagedAttachment(id);
    const failed = failPlaceholderThread({ shell: pending.record.shell, projection: pending.record.projection }, message);
    pending.record = { ...pending.record, ...failed, phase: "failed", error: message };
    this.persistPendingLaunch(pending.record);
    // Setup card first, while the client still follows it (it stops once the run is no longer preparing)
    this.markWorktreeSetupFailed(threadId, message);
    this.setThreadShell(threadId, failed.shell);
    this.broadcastShellItem({
      kind: "thread.updated",
      sequence: this.state.nextHubSequence(),
      location: "active",
      thread: failed.shell,
    });
    this.pushLocalThreadSnapshot(threadId, failed.projection);
    pending.rejectReady(new Error(message));

    // Unbind first: destroying a VM removes the threads bound to it, and this chat must stay
    this.threadToSandbox.delete(threadId);
    const sandboxId = sb?.sandboxId ?? null;
    if (sandboxId && this.state.getHubSandbox(sandboxId)) {
      this.state.updateHubSandbox(sandboxId, (s) => {
        s.threadIds = s.threadIds.filter((t) => t !== threadId);
      });
      void this.destroyHubSandbox(sandboxId, "chat-launch-failed").catch(() => {});
    }
  }

  private markWorktreeSetupFailed(threadId: string, message: string): void {
    const snap = this.worktreeSetupSnapshots.get(threadId);
    if (!snap || snap.phase === "failed") return;
    const now = new Date().toISOString();
    this.publishWorktreeSetup(threadId, {
      ...snap,
      phase: "failed",
      endedAt: now,
      error: message.slice(0, 1000),
      stages: snap.stages.map((stage) =>
        stage.id === "agent" || stage.status === "running"
          ? { ...stage, status: "failed", endedAt: now, detail: message.slice(0, 200), tail: [message.slice(0, 400)] }
          : stage,
      ),
      sequence: snap.sequence + 1,
    });
  }

  /** Serves a thread stream or projection read from a Hub-built projection. */
  private serveLocalThread(
    ws: ServerWebSocket<HubWsData>,
    reqId: string | number,
    tag: string,
    payload: unknown,
    projection: Record<string, unknown> | null,
  ): void {
    if (tag === "orchestration.getThreadProjection") {
      if (projection) sendRpcExitSuccess(ws, reqId, projection);
      else this.sendTypedFailure(ws, reqId, tag, payload, "This chat's VM is asleep and the Hub has no copy of it yet.");
      return;
    }
    const tid = extractThreadId(payload)!;
    ws.data.localStreams.set(String(reqId), { rawId: reqId, tag, payload });
    const subs = this.localThreadSubs.get(tid) ?? [];
    subs.push({ ws, reqId, payload, headers: [], servedSnapshot: projection !== null });
    this.localThreadSubs.set(tid, subs);
    // Sequence 0 marks the Hub's copy: the hand-off asks the VM for a full snapshot
    const values: unknown[] = projection ? [{ kind: "snapshot", snapshotSequence: 0, projection }] : [];
    if (projection || (payload as { requestCompletionMarker?: unknown } | null)?.requestCompletionMarker) {
      values.push({ kind: "synchronized" });
    }
    if (values.length > 0) sendRpcFrame(ws, { _tag: "Chunk", requestId: reqId, values });
  }

  private liveLocalThreadSubs(threadId: string): LocalThreadSub[] {
    const subs = (this.localThreadSubs.get(threadId) ?? []).filter(
      (sub) =>
        this.clients.has(sub.ws) && sub.ws.data.localStreams.get(String(sub.reqId))?.tag === "orchestration.subscribeThread",
    );
    if (subs.length > 0) this.localThreadSubs.set(threadId, subs);
    else this.localThreadSubs.delete(threadId);
    return subs;
  }

  /**
   * Sends a newer Hub-built projection to the thread's local streams. With `onlyServed`, a stream
   * that kept the client's own copy (got only `synchronized`) is left alone, so a projection without
   * history never replaces it.
   */
  private pushLocalThreadSnapshot(threadId: string, projection: Record<string, unknown>, onlyServed = false): void {
    for (const sub of this.liveLocalThreadSubs(threadId)) {
      if (onlyServed && !sub.servedSnapshot) continue;
      sub.servedSnapshot = true;
      sendRpcFrame(sub.ws, {
        _tag: "Chunk",
        requestId: sub.reqId,
        values: [{ kind: "snapshot", snapshotSequence: 0, projection }],
      });
    }
  }

  /** Moves a thread's local streams to its VM. A stream that got the Hub's copy asks for a full snapshot. */
  private handOffLocalThreadSubs(threadId: string, sb: HubSandboxRecord): void {
    const subs = this.liveLocalThreadSubs(threadId);
    this.localThreadSubs.delete(threadId);
    if (subs.length === 0) return;
    const bridge = this.bridges.get(sb.sandboxId);
    for (const sub of subs) {
      sub.ws.data.localStreams.delete(String(sub.reqId));
      if (!bridge?.isOpen()) {
        sendRpcExitFailure(sub.ws, sub.reqId, "The chat's VM disconnected; reconnecting");
        continue;
      }
      let payload = sub.payload;
      if (sub.servedSnapshot) {
        const { afterSequence: _hubCopyCursor, ...rest } = (sub.payload ?? {}) as Record<string, unknown>;
        payload = rest;
      }
      bridge.forwardToClient(sub.ws, sub.reqId, "orchestration.subscribeThread", payload, sub.headers);
    }
  }

  /** True when the thread's VM is running (commands for it go straight through). */
  private threadVmIsUp(threadId: string): boolean {
    if (this.pendingThreads.has(threadId)) return false;
    const sbId = this.threadToSandbox.get(threadId);
    const sb = sbId ? this.state.getHubSandbox(sbId) : undefined;
    return !sb || sb.phase === "running";
  }

  /** Sends a command the Hub already answered once the thread's VM is up; dropped if it never comes up. */
  private async forwardWhenUp(threadId: string, payload: Record<string, unknown>): Promise<void> {
    try {
      const sb = await this.resolveSandboxForThread(threadId);
      const bridge = await this.ensureBridgeOpen(sb);
      const exit = await bridge.callUnary(
        "orchestration.dispatchCommand",
        rewriteProjectIds(payload, sb.hubProjectId, sb.vmProjectId),
        60_000,
      );
      if (exit._tag !== "Success") console.warn(`[hub] deferred ${String(payload.type)} for ${threadId} failed: ${formatRpcFailure(exit.cause)}`);
    } catch {
      // The chat failed, was closed, or its VM could not wake; the user sees that in the chat
    }
  }

  /**
   * Ends a request with the RPC's typed error when it has one with a message, so the app shows the
   * reason (a `Die` reads "The environment request failed.").
   */
  private sendTypedFailure(
    ws: ServerWebSocket<HubWsData>,
    reqId: string | number,
    tag: string,
    payload: unknown,
    message: string,
  ): void {
    const p = (payload && typeof payload === "object" ? payload : {}) as Record<string, unknown>;
    const threadId = extractThreadId(p);
    const commandId = typeof p.commandId === "string" && p.commandId ? p.commandId : null;
    if ((tag === "orchestration.subscribeThread" || tag === "orchestration.getThreadProjection") && threadId) {
      sendRpcExitFail(ws, reqId, { _tag: "OrchestrationV2GetThreadProjectionError", threadId, message });
    } else if (tag === "orchestration.dispatchCommand" && commandId && typeof p.type === "string") {
      sendRpcExitFail(ws, reqId, { _tag: "OrchestrationV2DispatchCommandError", commandId, commandType: p.type, message });
    } else if (tag === "orchestration.launchThread" && commandId && typeof p.projectId === "string" && p.projectId) {
      sendRpcExitFail(ws, reqId, { _tag: "OrchestrationV2ThreadLaunchError", commandId, projectId: p.projectId, message });
    } else {
      sendRpcExitFailure(ws, reqId, message);
    }
  }

  // ── Chats whose VM is asleep ─────────────────────────────────────────────────────────────────

  /** The sleeping (or waking) sandbox a thread belongs to, if any. */
  private sleepingSandboxForThread(threadId: string): HubSandboxRecord | undefined {
    const sbId = this.threadToSandbox.get(threadId);
    const sb = sbId ? this.state.getHubSandbox(sbId) : undefined;
    if (!sb) return undefined;
    return sb.phase === "sleeping" || this.inFlightWakes.has(sb.sandboxId) ? sb : undefined;
  }

  private sleepNotice(sandboxId: string): StatusNotice {
    // After a Hub restart only the stored failure is left
    const stored = this.state.getHubSandbox(sandboxId)?.wakeFailure;
    const status: WakeStatus | undefined =
      this.wakeStatus.get(sandboxId) ??
      (stored ? { phase: "failed", error: stored.reason, retryAfter: stored.retryAfter } : undefined);
    if (status?.phase === "waking") {
      return { outcome: "updated", summary: "Waking this chat's VM. Messages you send are delivered once it is up." };
    }
    if (status?.phase === "failed") {
      const coolingDown = status.retryAfter !== null && Date.parse(status.retryAfter) > Date.now();
      return {
        outcome: "failed",
        summary: coolingDown
          ? `This chat's VM could not wake. The next try is allowed after ${status.retryAfter}; open the chat or send a message then.`
          : "This chat's VM could not wake. Open the chat again or send a message to retry.",
        detail: status.error,
      };
    }
    return { outcome: "updated", summary: "This chat's VM is asleep. Opening the chat or sending a message wakes it." };
  }

  /** The Hub's view of a sleeping chat: its cached projection (or its sidebar row) with a status row. */
  private sleepingThreadView(threadId: string, sandboxId: string): Record<string, unknown> | null {
    const cached = this.readProjectionCache(threadId);
    const shell = this.threadShells.get(threadId);
    const base = cached ?? (shell ? projectionFromShell(shell) : null);
    return base ? withStatusNotice(base, threadId, this.sleepNotice(sandboxId)) : null;
  }

  /**
   * Serves a sleeping chat's thread view at once and starts waking its VM; the stream moves to the VM
   * once it is up. False when the Hub knows nothing of the thread (then the request waits for the wake).
   */
  private serveSleepingThread(
    ws: ServerWebSocket<HubWsData>,
    reqId: string | number,
    tag: string,
    payload: unknown,
    threadId: string,
  ): boolean {
    const sb = this.sleepingSandboxForThread(threadId);
    if (!sb) return false;
    let view: Record<string, unknown> | null = null;
    if (this.readProjectionCache(threadId)) {
      view = this.sleepingThreadView(threadId, sb.sandboxId);
    } else {
      // No copy of the history: a client holding its own keeps it (only `synchronized`); one with
      // nothing gets the sidebar row with the status, so it never stays on "Syncing messages..."
      const afterSequence = (payload as { afterSequence?: unknown } | null)?.afterSequence;
      const clientHasCopy = typeof afterSequence === "number" && afterSequence > 0;
      if (!clientHasCopy || tag === "orchestration.getThreadProjection") view = this.sleepingThreadView(threadId, sb.sandboxId);
      if (!view && !clientHasCopy) return false;
    }
    this.serveLocalThread(ws, reqId, tag, payload, view);
    if (!this.inFlightWakes.has(sb.sandboxId)) {
      void this.wakeHubSandbox(sb.sandboxId).catch(() => {});
    }
    return true;
  }

  /** Wake start, success and failure: update what sleeping chats show, and hand them over on success. */
  private onWakeStatus(sandboxId: string, status: WakeStatus | null): void {
    const sb = this.state.getHubSandbox(sandboxId);
    if (status && sb) this.wakeStatus.set(sandboxId, status);
    else this.wakeStatus.delete(sandboxId);
    if (!sb) return;
    for (const threadId of sb.threadIds) {
      if (!status) {
        this.handOffLocalThreadSubs(threadId, sb);
        continue;
      }
      const subs = this.liveLocalThreadSubs(threadId);
      if (subs.length === 0) continue;
      const view = this.sleepingThreadView(threadId, sandboxId);
      if (view) this.pushLocalThreadSnapshot(threadId, view, !this.readProjectionCache(threadId));
    }
  }

  private projectionCacheFile(threadId: string): string {
    return join(this.config.stateDir, "hub-thread-cache", `${encodeURIComponent(threadId)}.json`);
  }

  private readProjectionCache(threadId: string): Record<string, unknown> | null {
    const hit = this.projectionCache.get(threadId);
    if (hit) return hit;
    const file = this.projectionCacheFile(threadId);
    if (!existsSync(file)) return null;
    try {
      const projection = JSON.parse(readFileSync(file, "utf8")) as Record<string, unknown>;
      this.projectionCache.set(threadId, projection);
      return projection;
    } catch {
      return null;
    }
  }

  /** Before a VM sleeps: keep a copy of each of its threads, so the Hub can show them while it sleeps. */
  private async cacheThreadProjections(sb: HubSandboxRecord): Promise<void> {
    const bridge = this.bridges.get(sb.sandboxId);
    if (!bridge?.isOpen()) return;
    await Promise.all(
      sb.threadIds.map(async (threadId) => {
        const exit = await bridge.callUnary("orchestration.getThreadProjection", { threadId }, 10_000);
        if (exit._tag !== "Success" || !exit.value || typeof exit.value !== "object") {
          // An older copy must never replace the newer one the client holds
          this.dropProjectionCache(threadId);
          return;
        }
        const projection = rewriteProjectIds(exit.value, sb.vmProjectId, sb.hubProjectId) as Record<string, unknown>;
        this.projectionCache.set(threadId, projection);
        try {
          const file = this.projectionCacheFile(threadId);
          mkdirSync(join(this.config.stateDir, "hub-thread-cache"), { recursive: true, mode: 0o700 });
          writeFileSync(file, JSON.stringify(projection), { mode: 0o600 });
        } catch (err) {
          console.warn(`[hub] could not save thread ${threadId} for its sleeping VM:`, err instanceof Error ? err.message : err);
        }
      }),
    );
  }

  private dropProjectionCache(threadId: string): void {
    this.projectionCache.delete(threadId);
    rmSync(this.projectionCacheFile(threadId), { force: true });
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
    if (tid && this.threadToSandbox.has(tid) && !this.pendingThreads.has(tid)) {
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
      const template = projKey ? this.state.getHub()?.projectTemplates[projKey] : undefined;
      // A template stored by an older Hub may no longer have the shape the app decodes
      if (isVcsRefsResult(template?.vcsRefs)) {
        sendRpcExitSuccess(ws, reqId, template.vcsRefs);
        return;
      }
      const project = projKey ? this.state.getAllProjects(this.config)[projKey] : undefined;
      sendRpcExitSuccess(ws, reqId, fallbackVcsRefs(template?.defaultBranch ?? null, !!project?.repo));
      return;
    }

    if (tag === "projects.listEntries" || tag === "projects.searchEntries") {
      sendRpcExitSuccess(ws, reqId, { entries: [], truncated: false });
      return;
    }
    if (tag === "projects.listIgnoredFiles") {
      sendRpcExitSuccess(ws, reqId, { files: [], truncated: false });
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
    // A chat answered before its VM is up (booting, or resumed after a Hub restart) is not gone:
    // its requests wait for the VM through resolveSandboxForThread. A failed start has no VM.
    if (this.pendingThreads.get(threadId)?.record.phase === "booting") return true;
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
    const staged = this.stagedAttachments.get(attachmentId);
    if (staged) {
      // The client checks a finished upload before sending it; one still staged lives on the Hub
      if (!staged.bytes) {
        sendRpcExitFail(ws, reqId, notFound);
        return;
      }
      const token = randomToken(24);
      this.stagedAssetTokens.set(token, attachmentId);
      sendRpcExitSuccess(ws, reqId, {
        relativeUrl: `/api/assets/${token}/${encodeURIComponent(staged.name)}`,
        expiresAt: Date.now() + ATTACHMENT_UPLOAD_URL_TTL_MS,
      });
      return;
    }
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

  /** The client's `POST` of a staged upload's bytes; answered like T3's upload route. */
  private async receiveStagedUpload(req: Request, uploadToken: string, stagedId: string): Promise<Response> {
    const entry = this.stagedAttachments.get(stagedId);
    if (!entry || entry.expiresAt <= Date.now()) {
      this.stagedUploadTokens.delete(uploadToken);
      return new Response("Not Found", { status: 404 });
    }
    const contentLength = req.headers.get("content-length");
    if (contentLength !== null && Number(contentLength) !== entry.sizeBytes) {
      return new Response("Content-Length must match the upload size.", { status: 400 });
    }
    const bytes = new Uint8Array(await req.arrayBuffer());
    if (bytes.byteLength !== entry.sizeBytes) {
      return new Response(`Body was ${bytes.byteLength} bytes, expected ${entry.sizeBytes}.`, { status: 400 });
    }
    entry.bytes = bytes;
    this.stagedUploadTokens.delete(uploadToken);
    return new Response(null, { status: 204 });
  }

  /** Staged uploads a send payload (launch, `thread.create`, `message.dispatch`) refers to. */
  private stagedIdsIn(payload: Record<string, unknown>): string[] {
    const message = payload.initialMessage as { attachments?: unknown } | undefined;
    const lists = [message?.attachments, payload.attachments].filter(Array.isArray) as unknown[][];
    return lists
      .flat()
      .map((att) => (att && typeof att === "object" ? (att as { id?: unknown }).id : undefined))
      .filter((id): id is string => typeof id === "string" && this.stagedAttachments.has(id));
  }

  private dropStagedAttachment(attachmentId: string): void {
    this.pinnedStagedAttachments.delete(attachmentId);
    this.stagedAttachments.delete(attachmentId);
    for (const map of [this.stagedUploadTokens, this.stagedAssetTokens]) {
      for (const [token, id] of map) if (id === attachmentId) map.delete(token);
    }
  }

  /** Forgets staged uploads never sent, after a day as T3 does (and unfinished ones once their URL expired). */
  private sweepStagedAttachments(): void {
    const now = Date.now();
    for (const staged of this.stagedAttachments.values()) {
      if (this.pinnedStagedAttachments.has(staged.id)) continue;
      const stale = staged.bytes ? now - staged.createdAt > STAGED_ATTACHMENT_MAX_AGE_MS : staged.expiresAt <= now;
      if (stale) this.dropStagedAttachment(staged.id);
    }
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
      // Same request the client made (type included, so a file is stored as a file), then the bytes
      const created = await bridge.callUnary("attachments.createUploadUrl", replayUploadRequest(staged));
      const res = (created._tag === "Success" ? created.value : null) as { attachmentId?: string; relativeUrl?: string } | null;
      if (!res?.attachmentId || !res.relativeUrl) {
        const reason = created._tag === "Success" ? "no upload URL" : formatRpcFailure(created.cause);
        throw new Error(`Attachment '${staged.name}' could not be sent to the chat's VM: ${reason}`);
      }
      const upload = await fetch(`${sb.upstreamUrl}${res.relativeUrl}`, {
        method: "POST",
        headers: {
          ...(sb.upstreamHeaders ?? {}),
          "content-type": staged.mimeType,
          "content-length": String(staged.bytes.byteLength),
        },
        body: Buffer.from(staged.bytes),
      });
      await upload.body?.cancel();
      if (!upload.ok) {
        throw new Error(`Attachment '${staged.name}' could not be sent to the chat's VM: upload rejected (${upload.status})`);
      }
      remapped.set(id, res.attachmentId);
      this.attachmentToSandbox.set(res.attachmentId, sb.sandboxId);
      this.dropStagedAttachment(id);
      nextAttachments.push({ ...att, id: res.attachmentId });
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

    // Per-phase boot times, logged when the VM is ready or fails
    const phaseMs: Record<string, number> = {};
    const spawnStartedAt = Date.now();
    let phaseStartedAt = spawnStartedAt;
    const lap = (phase: string) => {
      const now = Date.now();
      phaseMs[phase] = now - phaseStartedAt;
      phaseStartedAt = now;
    };
    const timings = () =>
      `${Object.entries(phaseMs)
        .map(([k, v]) => `${k}=${v}ms`)
        .join(" ")} total=${Date.now() - spawnStartedAt}ms`;

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
      lap("prepare");
      sandboxId = await this.boat.create({ name: sandboxName, size, fromSnapshot: snapshotToUse });
      this.lifecycle.trackInFlightSandbox(sandboxId);
      lap("create");

      if (effectiveProject.klav && this.klav) {
        emitStage("checkout", `VM ${sandboxId} created — attaching zero-trust Klav proxy...`, 40);
        notifyProgress("booting", `VM ${sandboxId} created — attaching zero-trust Klav proxy...`);
        const prov = await this.klav.provisionForSandbox(this.boat, sandboxId, effectiveProject.klav);
        klavAgentName = prov.agentName;
        lap("klav");
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
        // A fallback clone is already fresh; a project snapshot is as old as its bake
        syncRepoToRemote: !fallbackCloning && project.syncToRemoteOnStart !== false,
      });

      lap("provision");
      if (provisioned.repoSyncMs !== undefined) {
        // Sync to origin runs inside provisioning, before T3 starts: report it as its own phase
        phaseMs.provision -= provisioned.repoSyncMs;
        phaseMs.sync = provisioned.repoSyncMs;
      }
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
      lap("expose");
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
      lap("bridge");
      await this.settingsReplayDone(sandboxId);
      lap("settings");
      // T3 may still report the status it probed before the sign-in was placed; re-check in the background
      for (const instanceId of injected) this.refreshInjectedProvider(sandboxId, instanceId);

      const sync = provisioned.repoSync;
      emitStage(
        "done",
        sync?.status === "synced"
          ? `Boat VM ${sandboxId} (${size}) ready on origin/${sync.branch} (${sync.to.slice(0, 7)})`
          : sync?.status === "failed"
            ? `Boat VM ${sandboxId} (${size}) ready, but the repo could not be updated from origin (${sync.reason}): this chat starts on the snapshot's code`
            : `Boat VM ${sandboxId} (${size}) ready`,
        100,
      );
      console.log(`[hub] sandbox ${sandboxId} ready for thread=${threadId} (project=${projectKey}): ${timings()}`);
      return record;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.warn(`[hub] VM boot for thread=${threadId} failed after ${timings()}`);
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
    // A chat answered before its VM was ready: wait until the thread exists there (or fail with its error)
    const pending = this.pendingThreads.get(threadId);
    if (pending) await pending.ready;

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
    // Mid-sleep the record still says running, but its bridge and Klav sandbox are gone: wake it again
    if (sb.phase === "sleeping" || this.inFlightSleeps.has(sandboxId)) {
      return this.wakeHubSandbox(sandboxId);
    }
    if (sb.phase !== "running") {
      throw new Error(`Sandbox "${sandboxId}" is ${sb.phase}`);
    }
    return sb;
  }

  /** Listens to wake transitions (start, success, failure) of sleeping chat VMs; returns an unsubscribe. */
  onWakeTransition(listener: HubWakeListener): () => void {
    this.wakeListeners.add(listener);
    return () => this.wakeListeners.delete(listener);
  }

  private emitWake(event: HubWakeEvent): void {
    for (const listener of this.wakeListeners) {
      try {
        listener(event);
      } catch (err) {
        console.warn(`[hub] wake listener failed: ${redactForLog(err)}`);
      }
    }
  }

  /** Klav settings of a project whose VMs get a fresh proxy sandbox on every wake; `null` otherwise. */
  private klavProxyProject(projectName: string): ProjectKlavConfig | null {
    if (this.config.klav?.mode !== "proxy" || !this.klav) return null;
    return this.state.getAllProjects(this.config)[projectName]?.klav ?? null;
  }

  async wakeHubSandbox(sandboxId: string): Promise<HubSandboxRecord> {
    const existing = this.inFlightWakes.get(sandboxId);
    if (existing) return existing;

    const sb = this.state.getHubSandbox(sandboxId);
    if (!sb) throw new Error(`Unknown hub sandbox "${sandboxId}"`);
    if (sb.phase === "running") return sb;
    // During the cooldown after a failed wake: no resume, no Klav sandbox, no log line
    const failure = sb.wakeFailure;
    if (failure && Date.parse(failure.retryAfter) > Date.now()) {
      throw new HubWakeCooldownError(sandboxId, failure.step, failure.reason, failure.retryAfter);
    }

    const promise = this.doWakeHubSandbox(sandboxId).finally(() => {
      this.inFlightWakes.delete(sandboxId);
    });
    this.inFlightWakes.set(sandboxId, promise);
    return promise;
  }

  private async doWakeHubSandbox(sandboxId: string): Promise<HubSandboxRecord> {
    // A sleep still releasing Klav or pausing must finish before the resume
    await this.inFlightSleeps.get(sandboxId)?.catch(() => {});
    const sb = this.state.getHubSandbox(sandboxId);
    if (!sb) throw new Error(`Unknown hub sandbox "${sandboxId}"`);
    if (sb.phase === "running") return sb;

    if (!hasCapability(this.boat, "pause") || !this.boat.resume) {
      throw new Error(`Provider "${this.boat.name}" does not support pause/resume`);
    }

    this.clearPauseRetry(sandboxId);
    this.emitWake({ type: "wake-start", sandboxId, threadIds: [...sb.threadIds] });
    console.log(`[hub] waking sleeping sandbox ${sandboxId} (${sb.project})`);
    let step = "resume";
    let resumed = false;
    let newKlavSandbox: string | null = null;
    try {
      const res = await this.boat.resume(sandboxId, sb.size);
      if (res === "data_expired") {
        const threadIds = [...sb.threadIds];
        await this.destroyHubSandbox(sandboxId, "data-expired");
        const reason = "Sandbox disk expired after 14 days stopped";
        console.warn(`[hub] wake of sandbox ${sandboxId} failed at step resume: ${reason}; sandbox destroyed`);
        this.emitWake({ type: "wake-failure", sandboxId, threadIds, step, reason, retryAfter: null, attempts: 1 });
        throw new HubWakeFailedError(reason);
      }
      resumed = true;

      const project = this.state.getAllProjects(this.config)[sb.project];
      const klavProject = this.klavProxyProject(sb.project);
      let nextKlavAgent = sb.klavAgentName;
      if (klavProject && this.klav) {
        // Left by a pause the Hub did not make (watchdog backstop, reaper) or by an older Hub
        if (sb.klavAgentName) {
          step = "klav-revoke-previous";
          const previous = sb.klavAgentName;
          await this.klav.revokeForSandbox(previous).catch((err) => {
            console.warn(`[hub] Klav revoke of previous sandbox failed for ${sandboxId}: ${redactForLog(err)}`);
          });
          this.state.updateHubSandbox(sandboxId, (s) => {
            if (s.klavAgentName === previous) s.klavAgentName = null;
          });
        }
        // Before `klav sandbox start`, so a port that stays taken mints nothing
        step = "klav-forward-cleanup";
        await releaseVmForwarder(this.boat, sandboxId, { termTimeoutSecs: this.forwarderTermTimeoutSecs });
        step = "klav-provision";
        const prov = await this.klav.provisionForSandbox(this.boat, sandboxId, klavProject);
        newKlavSandbox = prov.agentName;
        nextKlavAgent = prov.agentName;
      }

      const llmToken = this.config.llmProxy ? `t3llm_${randomToken(24)}` : null;
      if (llmToken) {
        step = "provision-vm";
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

      step = "expose-port";
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
        s.wakeFailure = null;
      })!;

      step = "bridge";
      this.attachBridge(updated);
      await this.bridges.get(sandboxId)!.waitUntilOpen(this.wakeBridgeTimeoutMs);
      step = "settings-replay";
      await this.settingsReplayDone(sandboxId);
      this.emitWake({ type: "wake-success", sandboxId, threadIds: [...updated.threadIds] });
      return this.state.getHubSandbox(sandboxId) ?? updated;
    } catch (err) {
      if (err instanceof HubWakeFailedError) throw err;
      const failure = await this.rollBackFailedWake(sandboxId, { step, resumed, newKlavSandbox, err });
      throw new HubWakeFailedError(
        `Waking chat VM ${sandboxId} failed at ${failure.step} (${failure.reason}); next try after ${failure.retryAfter}`,
      );
    }
  }

  /**
   * Undoes a wake that failed after it started: no bridge, no Klav sandbox, no LLM token, the VM paused
   * again (or a pause retry scheduled), and a failure record that holds off the next wake.
   */
  private async rollBackFailedWake(
    sandboxId: string,
    ctx: { step: string; resumed: boolean; newKlavSandbox: string | null; err: unknown },
  ): Promise<HubWakeFailure> {
    const reason = redactForLog(ctx.err);
    this.bridges.get(sandboxId)?.stop();
    this.bridges.delete(sandboxId);

    if (ctx.newKlavSandbox && this.klav) {
      await releaseVmForwarder(this.boat, sandboxId, { termTimeoutSecs: this.forwarderTermTimeoutSecs }).catch(() => {});
      await this.klav.revokeForSandbox(ctx.newKlavSandbox).catch((err) => {
        console.warn(`[hub] Klav revoke after failed wake failed for ${sandboxId}: ${redactForLog(err)}`);
      });
    }

    // A resume that threw may still have started the VM (or a previous failure left it running)
    const previous = this.state.getHubSandbox(sandboxId)?.wakeFailure;
    let needsPause = ctx.resumed || previous?.vmPaused === false;
    if (!needsPause) {
      const info = await this.boat.info(sandboxId).catch(() => null);
      needsPause = info?.state === "running";
    }
    let vmPaused = !needsPause;
    if (needsPause && this.boat.pause) {
      try {
        await this.boat.pause(sandboxId);
        vmPaused = true;
      } catch {
        vmPaused = false;
      }
    }

    const nowMs = Date.now();
    const before = this.state.getHubSandbox(sandboxId);
    const attempts = (before?.wakeFailure?.attempts ?? 0) + 1;
    const failure: HubWakeFailure = {
      step: ctx.step,
      reason,
      at: new Date(nowMs).toISOString(),
      attempts,
      retryAfter: new Date(nowMs + wakeBackoffMs(attempts, this.wakeBackoffBaseMs, this.wakeBackoffMaxMs)).toISOString(),
      vmPaused,
    };
    const klavProxy = before ? this.klavProxyProject(before.project) !== null : false;
    const updated = this.state.updateHubSandbox(sandboxId, (s) => {
      s.phase = "sleeping";
      s.llmToken = null;
      if (klavProxy || s.klavAgentName === ctx.newKlavSandbox) s.klavAgentName = null;
      s.sleepingSince = s.sleepingSince ?? failure.at;
      s.wakeFailure = failure;
    });

    console.warn(
      `[hub] wake of sandbox ${sandboxId} failed at step ${failure.step} (attempt ${attempts}, VM ${vmPaused ? "paused" : "still running, pause retry scheduled"}, next try after ${failure.retryAfter}): ${reason}`,
    );
    if (!vmPaused) this.schedulePauseRetry(sandboxId);
    this.emitWake({
      type: "wake-failure",
      sandboxId,
      threadIds: [...(updated?.threadIds ?? [])],
      step: failure.step,
      reason,
      retryAfter: failure.retryAfter,
      attempts,
    });
    return failure;
  }

  /** Pauses a VM a failed wake left running; retries on its own until it is paused or woken. */
  private schedulePauseRetry(sandboxId: string, delayMs = this.pauseRetryMs): void {
    this.clearPauseRetry(sandboxId);
    const timer = setTimeout(() => {
      this.pauseRetries.delete(sandboxId);
      void this.retryPauseAfterFailedWake(sandboxId);
    }, delayMs);
    (timer as { unref?: () => void }).unref?.();
    this.pauseRetries.set(sandboxId, timer);
  }

  private clearPauseRetry(sandboxId: string): void {
    const timer = this.pauseRetries.get(sandboxId);
    if (timer) clearTimeout(timer);
    this.pauseRetries.delete(sandboxId);
  }

  private async retryPauseAfterFailedWake(sandboxId: string): Promise<void> {
    const sb = this.state.getHubSandbox(sandboxId);
    if (!sb || sb.phase !== "sleeping" || sb.wakeFailure?.vmPaused !== false || !this.boat.pause) return;
    if (this.inFlightWakes.has(sandboxId)) return;
    try {
      await this.boat.pause(sandboxId);
      this.state.updateHubSandbox(sandboxId, (s) => {
        if (s.wakeFailure) s.wakeFailure.vmPaused = true;
      });
      console.log(`[hub] paused sandbox ${sandboxId} left running by a failed wake`);
    } catch (err) {
      console.warn(`[hub] pause retry failed for ${sandboxId}: ${redactForLog(err)}`);
      this.schedulePauseRetry(sandboxId);
    }
  }

  async sleepHubSandbox(sandboxId: string): Promise<void> {
    const sb = this.state.getHubSandbox(sandboxId);
    if (!sb) return;
    if (sb.phase === "sleeping" && sb.wakeFailure?.vmPaused === false) {
      return this.retryPauseAfterFailedWake(sandboxId);
    }
    if (sb.phase !== "running") return;
    if (!hasCapability(this.boat, "pause") || !this.boat.pause) {
      console.warn(`[hub] provider "${this.boat.name}" does not support pause; skipping idle sleep for ${sandboxId}`);
      return;
    }
    if (this.inFlightWakes.has(sandboxId)) return;
    const existing = this.inFlightSleeps.get(sandboxId);
    if (existing) return existing;
    const promise = this.doSleepHubSandbox(sandboxId).finally(() => {
      this.inFlightSleeps.delete(sandboxId);
    });
    this.inFlightSleeps.set(sandboxId, promise);
    return promise;
  }

  private async doSleepHubSandbox(sandboxId: string): Promise<void> {
    const sb = this.state.getHubSandbox(sandboxId);
    if (!sb || sb.phase !== "running" || !this.boat.pause) return;
    console.log(`[hub] pausing idle sandbox ${sandboxId} (${sb.project})`);
    await this.cacheThreadProjections(sb).catch(() => {});
    this.bridges.get(sandboxId)?.stop();
    this.bridges.delete(sandboxId);

    // No live credentials in a sleeping VM: stop the forwarder while its control socket still answers,
    // then end the Klav sandbox on the issuer side. The wake provisions a fresh one.
    if (this.klavProxyProject(sb.project) && this.klav) {
      await releaseVmForwarder(this.boat, sandboxId, { termTimeoutSecs: this.forwarderTermTimeoutSecs }).catch((err) => {
        console.warn(`[hub] klav-forward release before sleep failed for ${sandboxId}: ${redactForLog(err)}`);
      });
      const klavSandbox = sb.klavAgentName;
      if (klavSandbox) {
        // On failure the name stays, so the wake's revoke of a previous sandbox retries it
        const revoked = await this.klav.revokeForSandbox(klavSandbox).then(
          () => true,
          (err) => {
            console.warn(`[hub] Klav revoke before sleep failed for ${sandboxId}: ${redactForLog(err)}`);
            return false;
          },
        );
        if (revoked) {
          this.state.updateHubSandbox(sandboxId, (s) => {
            if (s.klavAgentName === klavSandbox) s.klavAgentName = null;
          });
        }
      }
    }

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
    this.wakeStatus.delete(sandboxId);

    // Threads may have been bound while the pull ran
    const current = this.state.getHubSandbox(sandboxId) ?? sb;
    const threadIds = [...current.threadIds];
    const worktreePaths = [...current.worktreePaths];
    const klavAgent = current.klavAgentName;

    this.state.removeHubSandbox(sandboxId);
    this.mcp.recordThreadsDestroyed(threadIds, reason);
    for (const tid of threadIds) {
      this.threadToSandbox.delete(tid);
      this.deleteThreadShell(tid);
      this.localThreadSubs.delete(tid);
      this.dropProjectionCache(tid);
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
        this.adoptKeepOpenStreams(sandboxId);
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
        const listed = new Set<string>();
        for (const t of snap.threads ?? []) {
          const rewritten = rewriteProjectIds(t, sb.vmProjectId, sb.hubProjectId) as Record<string, unknown>;
          const tid = String(rewritten.id ?? "");
          if (!tid) continue;
          listed.add(tid);
          if (this.skipVmShellItem(tid)) continue;
          const previous = this.threadShells.get(tid);
          this.bindThreadToSandbox(tid, sandboxId);
          this.setThreadShell(tid, rewritten);
          if (typeof rewritten.worktreePath === "string" && rewritten.worktreePath) {
            this.bindWorktreeToSandbox(rewritten.worktreePath, sandboxId);
          }
          // New, or a row the Hub restored while the VM was down: the VM's live row wins
          if (!previous || JSON.stringify(previous) !== JSON.stringify(rewritten)) {
            this.broadcastShellItem({
              kind: "thread.updated",
              sequence: this.state.nextHubSequence(),
              location: "active",
              thread: rewritten,
            });
          }
        }
        // The snapshot lists every active thread of this VM (the bridge asks without `omitOlderSettled`
        // or `omitIdleSubagents`): a chat it no longer lists was archived or deleted while the Hub
        // was not listening. A trimmed snapshot (`olderSettled` set) proves nothing, so it removes none.
        const complete = !("olderSettled" in (item.snapshot as Record<string, unknown>));
        for (const tid of complete ? (this.state.getHubSandbox(sandboxId)?.threadIds ?? []) : []) {
          if (listed.has(tid) || this.pendingThreads.has(tid) || !this.threadShells.has(tid)) continue;
          this.deleteThreadShell(tid);
          this.broadcastShellItem({ kind: "thread.removed", sequence: this.state.nextHubSequence(), location: "active", threadId: tid });
        }
      } else if (item.kind === "thread.updated" && item.thread && typeof item.thread === "object") {
        const rewritten = rewriteProjectIds(item.thread, sb.vmProjectId, sb.hubProjectId) as Record<string, unknown>;
        const tid = String(rewritten.id ?? "");
        if (!tid || this.skipVmShellItem(tid)) continue;
        this.bindThreadToSandbox(tid, sandboxId);
        // An archived thread leaves the active list (the merged snapshot's `threads`)
        if (item.location === "archive") this.deleteThreadShell(tid);
        else this.setThreadShell(tid, rewritten);
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
        if (item.location !== "archive") this.deleteThreadShell(item.threadId);
        this.broadcastShellItem({
          kind: "thread.removed",
          sequence: this.state.nextHubSequence(),
          location: item.location ?? "active",
          threadId: item.threadId,
        });
      }
    }
  }

  /**
   * A failed chat keeps the Hub's row even if its VM (being destroyed) still reports the thread.
   * A booting chat the VM already lists is not launched again.
   */
  private skipVmShellItem(threadId: string): boolean {
    const pending = this.pendingThreads.get(threadId);
    if (!pending) return false;
    if (pending.record.phase === "failed") return true;
    pending.seenOnVm = true;
    return false;
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
    // Refresh this project's refs from the VM's own T3 (newer than a bake's), keeping what every chat shares
    const project = this.state.getAllProjects(this.config)[sandbox.project];
    const bridge = this.bridges.get(sandbox.sandboxId);
    if (project && bridge?.isOpen()) {
      void bridge.callUnary("vcs.listRefs", { cwd: project.workdir }).then((res) => {
        const vcsRefs = res._tag === "Success" ? templateVcsRefs(res.value) : null;
        if (vcsRefs) {
          const defaultBranch = defaultBranchOfRefs(vcsRefs);
          this.state.updateHub((h) => {
            h.projectTemplates[sandbox.project] = {
              ...(h.projectTemplates[sandbox.project] ?? {}),
              vcsRefs,
              ...(defaultBranch ? { defaultBranch } : {}),
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
    // A row the Hub showed before the binding (a booting chat) is stored once the chat has its VM
    const row = this.threadShells.get(threadId);
    if (row) this.shellStore.save(threadId, row);
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

/** Drops `afterSequence: 0`, the Hub placeholder's cursor, so a VM answers with a full snapshot. */
function withoutZeroCursor(payload: unknown): unknown {
  if (!payload || typeof payload !== "object") return payload;
  const { afterSequence, ...rest } = payload as Record<string, unknown>;
  return afterSequence === 0 ? rest : payload;
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
