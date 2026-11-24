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
import type { SentinelConfig } from "./config.ts";
import type { LifecycleManager } from "./lifecycle.ts";
import type { SlotRecord, StateStore } from "./state.ts";

export interface WsData {
  slotName: string;
  targetPathAndQuery: string;
  upstream: WebSocket | null;
  pendingFrames: Array<string | Uint8Array>;
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

/** Bun's fetch transparently decompresses responses; drop encoding/length so clients never hit ZlibError. */
const STRIP_RESPONSE_HEADERS = new Set([...HOP_BY_HOP, "content-encoding", "content-length"]);

const URL_TOKEN_PREFIXES = ["/api/attachments/upload/", "/api/assets/"];

export class SlotProxy {
  private readonly authBySlot = new Map<string, SlotAuth>();

  constructor(
    private readonly config: SentinelConfig,
    private readonly state: StateStore,
    private readonly lifecycle: LifecycleManager,
  ) {
    for (const s of config.slots) {
      this.authBySlot.set(s.name, new SlotAuth());
    }
  }

  getSlotAuth(slotName: string): SlotAuth {
    let auth = this.authBySlot.get(slotName);
    if (!auth) {
      auth = new SlotAuth();
      this.authBySlot.set(slotName, auth);
    }
    return auth;
  }

  createPairingLink(slotName: string): { url: string; code: string; expiresAt: string } {
    const slot = this.state.getSlot(slotName);
    if (!slot) throw new Error(`Unknown slot "${slotName}"`);
    const { code, expiresAt } = this.getSlotAuth(slotName).newPairingCode();
    const url = `https://${slotName}.${this.config.domain}/pair#token=${encodeURIComponent(code)}`;
    return { url, code, expiresAt };
  }

  resolveSlotFromHost(hostHeader: string | null): string | null {
    if (!hostHeader) return null;
    const host = hostHeader.split(":")[0].toLowerCase();
    const suffix = `.${this.config.domain.toLowerCase()}`;
    if (!host.endsWith(suffix)) return null;
    const sub = host.slice(0, -suffix.length);
    if (sub.includes(".")) return null;
    return this.state.getSlot(sub) ? sub : null;
  }

  private buildDescriptor(slot: SlotRecord): Record<string, unknown> {
    const base = slot.descriptor ?? this.state.getTemplates().descriptor ?? {
      environmentId: slot.environmentId,
      label: slot.label,
      platform: { os: "linux", arch: "x64" },
      serverVersion: this.config.t3.version === "latest" ? "0.0.0" : this.config.t3.version,
      orchestrationProtocolVersion: 2,
      capabilities: {
        repositoryIdentity: true,
        connectionProbe: true,
        attachmentUploads: true,
      },
    };

    // Strip `accessTokenRenewal` so T3 Mobile / Desktop pair with standard Bearer tokens instead of DPoP
    const caps = { ...((base.capabilities as Record<string, unknown> | undefined) ?? {}) };
    delete caps.accessTokenRenewal;

    return {
      ...base,
      environmentId: slot.environmentId,
      label: slot.label,
      capabilities: caps,
    };
  }

  private buildAuthSession(slot: SlotRecord): Record<string, unknown> {
    const expiresAt = new Date(Date.now() + CLIENT_TOKEN_TTL_SECS * 1000).toISOString();
    const base = slot.authSession ?? this.state.getTemplates().authSession ?? {
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
        ? {
            ...(base.auth as Record<string, unknown>),
            sessionMethods: ["bearer-access-token"],
          }
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

  async handleHttp(
    slotName: string,
    req: Request,
    server: Bun.Server<WsData>,
  ): Promise<Response | undefined> {
    if (req.method === "OPTIONS") {
      return withCors(new Response(null, { status: 204 }));
    }

    const url = new URL(req.url);
    const path = url.pathname;
    const slot = this.state.getSlot(slotName)!;
    const auth = this.getSlotAuth(slotName);

    // 1. Schema-valid discovery descriptor (never wakes VM; removes DPoP trigger)
    if (req.method === "GET" && path === "/.well-known/t3/environment") {
      return withCors(Response.json(this.buildDescriptor(slot)));
    }

    // 2. One-time pairing code exchange -> 10-year Bearer token (stored as SHA-256 hash)
    if (req.method === "POST" && path === "/oauth/token") {
      const text = await req.text();
      const form = new URLSearchParams(text);
      const code = form.get("subject_token") ?? "";
      if (!auth.redeemPairingCode(code)) {
        return withCors(Response.json({ error: "invalid_grant" }, { status: 400 }));
      }
      const token = randomToken(32);
      const label = form.get("client_label")?.trim() || form.get("client_device_type")?.trim() || "T3 client";
      this.state.updateSlot(slotName, (s) => {
        s.clients.push({
          id: randomUUID(),
          tokenHash: hashToken(token),
          label,
          pairedAt: new Date().toISOString(),
          lastSyncedSandboxId: null,
        });
      });

      const session = this.buildAuthSession(slot);
      const scopes = (session.scopes as string[]).join(" ");
      console.log(`[auth] paired new T3 client (${label}) to slot "${slotName}"`);
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

    // 3. Schema-valid session probe (never wakes VM)
    if (req.method === "GET" && path === "/api/auth/session") {
      if (!authenticateClient(slot.clients, req.headers)) {
        return withCors(Response.json({ error: "unauthorized" }, { status: 401 }));
      }
      return withCors(Response.json(this.buildAuthSession(slot)));
    }

    // 4. Short-lived WebSocket tickets (supports both /websocket-ticket and /websocket-ticket/:use)
    const ticketMatch = /^\/api\/auth\/websocket-ticket(?:\/(rpc|media))?$/.exec(path);
    if (req.method === "POST" && ticketMatch) {
      const client = authenticateClient(slot.clients, req.headers);
      if (!client) {
        return withCors(Response.json({ error: "unauthorized" }, { status: 401 }));
      }
      const use = (ticketMatch[1] ?? "rpc") as TicketUse;
      return withCors(Response.json(auth.issueTicket(use, client.id)));
    }

    // 5. Periodic renderer traces — swallow silently so they never wake sleeping VMs
    if (req.method === "POST" && path === "/api/observability/v1/traces") {
      return withCors(new Response(null, { status: 200 }));
    }

    // 6. WebSocket upgrade (/ws and /api/device-hub/*, /api/preview-stream/*, /ws/*)
    if (req.headers.get("upgrade")?.toLowerCase() === "websocket") {
      return this.handleWsUpgrade(slotName, req, url, server);
    }

    // 7. General HTTP passthrough
    const isUrlTokenRoute = URL_TOKEN_PREFIXES.some((p) => path.startsWith(p));
    const client = isUrlTokenRoute ? null : authenticateClient(slot.clients, req.headers);
    if (!isUrlTokenRoute && !client) {
      return withCors(Response.json({ error: "unauthorized" }, { status: 401 }));
    }

    let liveSlot = slot;
    if (liveSlot.phase === "sleeping" && this.config.wakePolicy === "on-connect") {
      try {
        liveSlot = await this.lifecycle.wake(slotName);
      } catch {
        return withCors(Response.json({ error: "wake_failed", slot: slotName }, { status: 503 }));
      }
    }

    if (liveSlot.phase !== "running" || !liveSlot.upstreamUrl || !liveSlot.upstreamToken) {
      return withCors(
        Response.json(
          { error: "sandbox_unavailable", phase: liveSlot.phase, slot: slotName },
          { status: 503 },
        ),
      );
    }

    // Strip stale `resumeFrom` on /api/orchestration/shell if this client hasn't synced to the new VM yet!
    const forwardUrl = new URL(url.toString());
    if (
      client &&
      path === "/api/orchestration/shell" &&
      liveSlot.sandboxId &&
      client.lastSyncedSandboxId !== liveSlot.sandboxId
    ) {
      forwardUrl.searchParams.delete("resumeFrom");
      this.markClientSynced(slotName, client.id, liveSlot.sandboxId);
    }

    return this.forwardHttp(liveSlot, req, forwardUrl, isUrlTokenRoute);
  }

  private async handleWsUpgrade(
    slotName: string,
    req: Request,
    url: URL,
    server: Bun.Server<WsData>,
  ): Promise<Response | undefined> {
    let slot = this.state.getSlot(slotName)!;
    const auth = this.getSlotAuth(slotName);
    const isMedia = url.pathname.startsWith("/api/device-hub/") || url.pathname.startsWith("/api/preview-stream/");
    const ticketUse: TicketUse = isMedia ? "media" : "rpc";

    const wsTicket = url.searchParams.get("wsTicket");
    const ticketClientId = wsTicket ? auth.checkTicket(wsTicket, ticketUse) : null;
    const bearerClient = ticketClientId ? null : authenticateClient(slot.clients, req.headers);
    const clientId = ticketClientId ?? bearerClient?.id ?? null;

    if (!clientId) {
      return withCors(Response.json({ error: "unauthorized" }, { status: 401 }));
    }

    // Empty slots (archived/deleted) NEVER boot automatically on passive reconnects
    if (slot.phase === "empty" || slot.phase === "destroying" || slot.phase === "provisioning") {
      return withCors(new Response("Slot has no active sandbox", { status: 503 }));
    }

    // Sleeping slots: in `manual` mode (or media streams), return 503 so sidebar never wakes VM
    if (slot.phase === "sleeping" || slot.phase === "waking") {
      if (this.config.wakePolicy === "manual" || isMedia) {
        return withCors(new Response("Sandbox is sleeping", { status: 503 }));
      }
      try {
        slot = await this.lifecycle.wake(slotName);
      } catch {
        return withCors(new Response("Sandbox wake failed", { status: 503 }));
      }
    }

    const upstreamQuery = new URLSearchParams(url.searchParams);
    upstreamQuery.delete("wsTicket");

    // Strip stale `afterSequence` on first WS connect to a freshly spawned VM so client fetches a full snapshot
    const clientRecord = slot.clients.find((c) => c.id === clientId);
    if (clientRecord && slot.sandboxId && clientRecord.lastSyncedSandboxId !== slot.sandboxId) {
      upstreamQuery.delete("afterSequence");
      this.markClientSynced(slotName, clientId, slot.sandboxId);
    }

    const qs = upstreamQuery.toString();
    const targetPathAndQuery = `${url.pathname}${qs ? `?${qs}` : ""}`;

    const upgraded = server.upgrade(req, {
      data: {
        slotName,
        targetPathAndQuery,
        upstream: null,
        pendingFrames: [],
      },
    });
    if (!upgraded) {
      return withCors(new Response("WebSocket upgrade failed", { status: 400 }));
    }
    return undefined;
  }

  private markClientSynced(slotName: string, clientId: string, sandboxId: string): void {
    this.state.updateSlot(slotName, (s) => {
      const c = s.clients.find((entry) => entry.id === clientId);
      if (c) c.lastSyncedSandboxId = sandboxId;
    });
  }

  wsHandlers(): Bun.WebSocketHandler<WsData> {
    return {
      open: (ws: ServerWebSocket<WsData>) => {
        this.connectUpstream(ws);
      },

      message: (ws: ServerWebSocket<WsData>, msg: string | Buffer) => {
        const upstream = ws.data.upstream;
        if (upstream && upstream.readyState === WebSocket.OPEN) {
          upstream.send(msg);
        } else {
          ws.data.pendingFrames.push(typeof msg === "string" ? msg : new Uint8Array(msg));
        }
      },

      close: (ws: ServerWebSocket<WsData>, code: number, reason: string) => {
        const upstream = ws.data.upstream;
        if (upstream && (upstream.readyState === WebSocket.OPEN || upstream.readyState === WebSocket.CONNECTING)) {
          try {
            upstream.close(safeCloseCode(code), reason.slice(0, 120));
          } catch {
            upstream.close();
          }
        }
        ws.data.upstream = null;
      },
    };
  }

  private connectUpstream(ws: ServerWebSocket<WsData>): void {
    const slot = this.state.getSlot(ws.data.slotName);
    if (!slot?.upstreamUrl || !slot.upstreamToken) {
      ws.close(1013, "Sandbox unavailable");
      return;
    }

    const wsBase = slot.upstreamUrl.replace(/^https:\/\//, "wss://").replace(/^http:\/\//, "ws://");
    const targetUrl = `${wsBase}${ws.data.targetPathAndQuery}`;

    const upstream = new WebSocket(targetUrl, {
      headers: { authorization: `Bearer ${slot.upstreamToken}` },
    } as unknown as string[]);
    ws.data.upstream = upstream;

    upstream.addEventListener("open", () => {
      for (const frame of ws.data.pendingFrames) {
        upstream.send(frame);
      }
      ws.data.pendingFrames = [];
    });

    upstream.addEventListener("message", (ev) => {
      try {
        if (typeof ev.data === "string") {
          ws.sendText(ev.data);
        } else if (ev.data instanceof ArrayBuffer) {
          ws.sendBinary(new Uint8Array(ev.data));
        } else if (ArrayBuffer.isView(ev.data)) {
          ws.sendBinary(new Uint8Array(ev.data.buffer, ev.data.byteOffset, ev.data.byteLength));
        }
      } catch {
        // client already closed
      }
    });

    upstream.addEventListener("close", (ev) => {
      try {
        ws.close(safeCloseCode(ev.code), ev.reason?.slice(0, 120) ?? "");
      } catch {
        // already closed
      }
    });

    upstream.addEventListener("error", () => {
      try {
        ws.close(1011, "Upstream error");
      } catch {
        // already closed
      }
    });
  }

  private async forwardHttp(
    slot: SlotRecord,
    req: Request,
    url: URL,
    isUrlTokenRoute: boolean,
  ): Promise<Response> {
    const upstreamUrl = `${slot.upstreamUrl}${url.pathname}${url.search}`;
    const headers = new Headers();
    for (const [k, v] of req.headers) {
      const lower = k.toLowerCase();
      if (HOP_BY_HOP.has(lower) || lower === "authorization") continue;
      headers.set(k, v);
    }
    if (!isUrlTokenRoute && slot.upstreamToken) {
      headers.set("authorization", `Bearer ${slot.upstreamToken}`);
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
        // Drop content-encoding and content-length since Bun transparently decompresses fetch bodies
        if (!STRIP_RESPONSE_HEADERS.has(k.toLowerCase())) outHeaders.set(k, v);
      }
      return withCors(new Response(res.body, { status: res.status, headers: outHeaders }));
    } catch (err) {
      console.error(`[proxy] HTTP forward failed for ${slot.name}:`, err);
      return withCors(Response.json({ error: "bad_gateway" }, { status: 502 }));
    }
  }
}

function safeCloseCode(code: number): number {
  if (
    code === 1000 ||
    (code >= 3000 && code <= 4999) ||
    (code >= 1001 && code <= 1014 && code !== 1004 && code !== 1005 && code !== 1006)
  ) {
    return code;
  }
  return 1000;
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
  return new Response(res.body, { status: res.status, statusText: res.statusText, headers });
}
