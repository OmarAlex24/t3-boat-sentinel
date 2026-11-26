import { createHash, createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import type { ServerWebSocket } from "bun";
import type { ExposedEndpoint } from "./providers/types.ts";
import { safeCloseCode } from "./proxy.ts";
import { ROBOTS_TXT, applySecurityHeaders } from "./security.ts";

export const PREVIEW_BOOTSTRAP_PARAM = "__t3p";
export const PREVIEW_EDGE_PARAM = "__t3e";
export const PREVIEW_COOKIE_NAME = "__t3_preview";
export const PREVIEW_BOOTSTRAP_PATH = "/api/preview-port/bootstrap";

const T3_RPC_PORT = 3773;
const DEFAULT_BOOTSTRAP_TTL_MS = 15 * 60_000;
const DEFAULT_SESSION_TTL_MS = 8 * 60 * 60_000;
const MAX_PENDING_BYTES = 1024 * 1024;

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
const RESERVED_SET_COOKIES = new Set([PREVIEW_COOKIE_NAME, "__t3_edge", "_port_auth"]);

export interface SealedEdgePayload {
  u: string;
  h?: Record<string, string>;
  s: string;
  p: number;
  host: string;
  exp: number;
}

export interface HubPortGatewayOptions {
  domain: string;
  signingSecret: string;
  exposePort: (sandboxId: string, port: number) => Promise<ExposedEndpoint>;
  isSandboxRunning: (sandboxId: string) => boolean;
  /** Optional Hub hostname (e.g. "boat.example.com") used to host single-domain Edge Worker bootstrap redirects without colliding with `p*-*.<domain>` Worker routes. */
  bootstrapHost?: string;
  edgeWorkerHost?: string;
  edgeWorkerSecret?: string;
  bootstrapTtlMs?: number;
  sessionTtlMs?: number;
  secureCookies?: boolean;
}

export interface PortGatewayWsData {
  kind: "port-gateway";
  sandboxId: string;
  port: number;
  targetUrl: string;
  upstreamHeaders: Record<string, string>;
  protocols: string[];
  upstream: WebSocket | null;
  pending: Array<string | Uint8Array>;
  closed: boolean;
}

interface SignedPreviewPayload {
  s: string;
  p: number;
  exp: number;
  /** Per-sandbox live epoch (session tokens only): cleared on invalidateSandbox so old cookies never survive sleep/wake. */
  e?: string;
}

interface EndpointEntry {
  generation: number;
  endpoint?: ExposedEndpoint;
  promise?: Promise<ExposedEndpoint>;
}

export function parseLoopbackPreviewUrl(rawUrl: string): { port: number; pathAndQuery: string } | null {
  let url: URL;
  try {
    url = new URL(rawUrl);
  } catch {
    return null;
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") return null;
  if (!isLoopbackHostname(url.hostname)) return null;

  const portText = explicitPortText(rawUrl);
  if (!portText || !/^\d+$/.test(portText)) return null;
  const port = Number(portText);
  if (!isValidPreviewPort(port)) return null;

  return { port, pathAndQuery: `${url.pathname}${url.search}${url.hash}` || "/" };
}

export function sandboxSlug(sandboxId: string): string {
  return createHash("sha256").update(sandboxId).digest("hex").slice(0, 10);
}

export function parseGatewayHost(
  hostHeader: string | null,
  domain: string,
): { port: number; slug: string; hostname: string } | null {
  if (!hostHeader || !domain) return null;
  const hostname = hostHeader.trim().replace(/:\d+$/, "").toLowerCase();
  const escapedDomain = domain.toLowerCase().replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const match = new RegExp(`^p(\\d+)-([a-z0-9]+)\\.${escapedDomain}$`, "i").exec(hostname);
  if (!match) return null;
  const port = Number(match[1]);
  if (!isValidPreviewPort(port)) return null;
  return { port, slug: match[2].toLowerCase(), hostname };
}

export async function sealEdgeTarget(payload: SealedEdgePayload, secret: string): Promise<string> {
  const key = await edgeKey(secret);
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const plaintext = new TextEncoder().encode(JSON.stringify(payload));
  const ciphertext = new Uint8Array(await crypto.subtle.encrypt({ name: "AES-GCM", iv }, key, plaintext));
  return `v1.${bytesToBase64Url(iv)}.${bytesToBase64Url(ciphertext)}`;
}

export async function unsealEdgeTarget(
  token: string,
  secret: string,
  nowMs = Date.now(),
): Promise<SealedEdgePayload | null> {
  try {
    const parts = token.split(".");
    if (parts.length !== 3 || parts[0] !== "v1") return null;
    const iv = base64UrlToBytes(parts[1]);
    const ciphertext = base64UrlToBytes(parts[2]);
    if (iv.byteLength !== 12 || ciphertext.byteLength < 16) return null;
    const key = await edgeKey(secret);
    const plaintext = await crypto.subtle.decrypt({ name: "AES-GCM", iv }, key, ciphertext);
    const parsed: unknown = JSON.parse(new TextDecoder().decode(plaintext));
    if (!isSealedEdgePayload(parsed)) return null;
    if (parsed.exp <= nowMs) return null;
    return parsed;
  } catch {
    return null;
  }
}

export class HubPortGateway {
  private readonly domain: string;
  private readonly hmacKey: Buffer;
  private readonly exposePort: HubPortGatewayOptions["exposePort"];
  private readonly isSandboxRunning: HubPortGatewayOptions["isSandboxRunning"];
  private readonly bootstrapHost?: string;
  private readonly edgeWorkerHost?: string;
  private readonly edgeWorkerSecret?: string;
  private readonly bootstrapTtlMs: number;
  private readonly sessionTtlMs: number;
  private readonly secureCookies: boolean;

  private readonly slugToSandbox = new Map<string, string>();
  private readonly sessionEpochs = new Map<string, string>();
  private readonly endpoints = new Map<string, EndpointEntry>();
  private readonly generations = new Map<string, number>();
  private readonly socketsBySandbox = new Map<string, Set<ServerWebSocket<PortGatewayWsData>>>();
  private readonly pendingBytes = new WeakMap<ServerWebSocket<PortGatewayWsData>, number>();

  constructor(options: HubPortGatewayOptions) {
    this.domain = options.domain;
    // Derive a dedicated preview HMAC subkey rather than reusing raw controlSecret directly.
    this.hmacKey = createHmac("sha256", options.signingSecret).update("t3-preview-gateway-v1").digest();
    this.exposePort = options.exposePort;
    this.isSandboxRunning = options.isSandboxRunning;
    this.bootstrapHost = options.bootstrapHost?.trim().replace(/^https?:\/\//i, "").replace(/\/+$/, "").toLowerCase();
    this.edgeWorkerHost = options.edgeWorkerHost;
    this.edgeWorkerSecret = options.edgeWorkerSecret;
    this.bootstrapTtlMs = options.bootstrapTtlMs ?? DEFAULT_BOOTSTRAP_TTL_MS;
    this.sessionTtlMs = options.sessionTtlMs ?? DEFAULT_SESSION_TTL_MS;
    this.secureCookies = options.secureCookies ?? true;
  }

  buildPreviewUrl(sandboxId: string, port: number, pathAndQuery = "/"): string {
    assertValidPreviewPort(port);
    const slug = sandboxSlug(sandboxId);
    this.slugToSandbox.set(slug, sandboxId);
    if (!this.sessionEpochs.has(sandboxId)) this.sessionEpochs.set(sandboxId, randomBytes(8).toString("hex"));
    const token = this.mintToken({ s: sandboxId, p: port, exp: Date.now() + this.bootstrapTtlMs }, "bootstrap");
    const normalized = normalizePathAndQuery(pathAndQuery);
    // When the Edge Worker runs on the same domain (`*.<domain>/*`), route the initial __t3p
    // bootstrap through Sentinel's hub host so the Worker never intercepts Sentinel's own bootstrap.
    if (
      this.edgeWorkerHost &&
      this.edgeWorkerSecret &&
      this.bootstrapHost &&
      this.edgeWorkerHost.toLowerCase() === this.domain.toLowerCase()
    ) {
      const hashIndex = normalized.indexOf("#");
      const targetPath = hashIndex >= 0 ? normalized.slice(0, hashIndex) : normalized;
      const hash = hashIndex >= 0 ? normalized.slice(hashIndex) : "";
      const params = new URLSearchParams({
        p: String(port),
        s: slug,
        to: safePathname(targetPath),
        [PREVIEW_BOOTSTRAP_PARAM]: token,
      });
      return `https://${this.bootstrapHost}${PREVIEW_BOOTSTRAP_PATH}?${params.toString()}${hash}`;
    }
    const pathWithToken = appendQueryParam(normalized, PREVIEW_BOOTSTRAP_PARAM, token);
    return `https://p${port}-${slug}.${this.domain}${pathWithToken}`;
  }

  /**
   * Rewrites a loopback dev-server URL OR refreshes a stale gateway/bootstrap URL belonging to
   * `sandboxId` with a fresh `__t3p` token (e.g. when a user clicks a discovered server >15 min later).
   */
  refreshPreviewUrl(rawUrl: string, sandboxId: string): string | null {
    const loopback = parseLoopbackPreviewUrl(rawUrl);
    if (loopback) return this.buildPreviewUrl(sandboxId, loopback.port, loopback.pathAndQuery);
    let parsed: URL;
    try {
      parsed = new URL(rawUrl);
    } catch {
      return null;
    }
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") return null;
    const expectedSlug = sandboxSlug(sandboxId);
    const gatewayHost =
      parseGatewayHost(parsed.host, this.domain) ??
      (this.edgeWorkerHost ? parseGatewayHost(parsed.host, this.edgeWorkerHost) : null);
    if (gatewayHost && gatewayHost.slug === expectedSlug) {
      return this.buildPreviewUrl(sandboxId, gatewayHost.port, stripGatewayParams(parsed, true));
    }
    if (
      this.bootstrapHost &&
      parsed.hostname.toLowerCase() === this.bootstrapHost &&
      parsed.pathname === PREVIEW_BOOTSTRAP_PATH &&
      (parsed.searchParams.get("s") ?? "").toLowerCase() === expectedSlug
    ) {
      const portText = parsed.searchParams.get("p") ?? "";
      const port = /^\d+$/.test(portText) ? Number(portText) : NaN;
      const rawTo = parsed.searchParams.get("to") ?? "/";
      if (isValidPreviewPort(port) && rawTo.startsWith("/") && !rawTo.startsWith("//")) {
        return this.buildPreviewUrl(sandboxId, port, `${rawTo}${parsed.hash}`);
      }
    }
    return null;
  }

  /**
   * Handles single-domain Edge Worker bootstrap requests served on Sentinel's hub host
   * (`GET /api/preview-port/bootstrap?p=<port>&s=<slug>&to=<path>&__t3p=<token>`).
   */
  async handleHubBootstrap(req: Request): Promise<Response> {
    if (req.method !== "GET" && req.method !== "HEAD") {
      return gatewayResponse("Method Not Allowed", 405);
    }
    if (!this.edgeWorkerHost || !this.edgeWorkerSecret) {
      return gatewayResponse("Not found", 404);
    }
    const url = new URL(req.url);
    const portText = url.searchParams.get("p") ?? "";
    const slug = (url.searchParams.get("s") ?? "").toLowerCase();
    const rawTo = url.searchParams.get("to") ?? "/";
    const bootstrapToken = url.searchParams.get(PREVIEW_BOOTSTRAP_PARAM);
    if (!/^\d+$/.test(portText) || !/^[a-z0-9]+$/.test(slug) || !bootstrapToken) {
      return gatewayResponse("Unauthorized", 401);
    }
    const port = Number(portText);
    if (!isValidPreviewPort(port)) return gatewayResponse("Unauthorized", 401);
    if (!rawTo.startsWith("/") || rawTo.startsWith("//") || rawTo.includes("\\") || /[\r\n]/.test(rawTo)) {
      return gatewayResponse("Bad Request", 400);
    }

    const sandboxId = this.slugToSandbox.get(slug);
    if (!sandboxId) return gatewayResponse("Not found", 404);
    if (!this.isSandboxRunning(sandboxId)) return gatewayResponse("Sandbox is not running", 503);

    const bootstrap = this.verifyToken(bootstrapToken, "bootstrap");
    if (!bootstrap || bootstrap.s !== sandboxId || bootstrap.p !== port) {
      return gatewayResponse("Unauthorized", 401);
    }

    let endpoint: ExposedEndpoint;
    try {
      endpoint = await this.getOrExposeEndpoint(sandboxId, port);
    } catch {
      return gatewayResponse("Failed to expose sandbox port", 502);
    }
    const edgeHostname = `p${port}-${slug}.${this.edgeWorkerHost}`;
    const sealed = await sealEdgeTarget(
      {
        u: endpoint.url,
        ...(endpoint.headers ? { h: endpoint.headers } : {}),
        s: sandboxId,
        p: port,
        host: edgeHostname,
        exp: Date.now() + this.sessionTtlMs,
      },
      this.edgeWorkerSecret,
    );
    const edgePath = appendQueryParam(rawTo, PREVIEW_EDGE_PARAM, sealed);
    return redirectResponse(`https://${edgeHostname}${edgePath}`);
  }

  resolveHost(hostHeader: string | null): { port: number; slug: string; hostname: string } | null {
    return parseGatewayHost(hostHeader, this.domain);
  }

  async handleHttp(req: Request, server: Bun.Server<any>): Promise<Response | undefined> {
    const url = new URL(req.url);
    if (req.method === "GET" && url.pathname === "/robots.txt") {
      return applySecurityHeaders(
        new Response(ROBOTS_TXT, { headers: { "content-type": "text/plain; charset=utf-8" } }),
      );
    }

    const gatewayHost = this.resolveHost(req.headers.get("host"));
    if (!gatewayHost) return gatewayResponse("Not found", 404);
    const sandboxId = this.slugToSandbox.get(gatewayHost.slug);
    if (!sandboxId) return gatewayResponse("Not found", 404);
    if (!this.isSandboxRunning(sandboxId)) return gatewayResponse("Sandbox is not running", 503);

    const bootstrapToken = url.searchParams.get(PREVIEW_BOOTSTRAP_PARAM);
    if ((req.method === "GET" || req.method === "HEAD") && bootstrapToken !== null) {
      const cleanPathAndQuery = stripGatewayParams(url, true);
      const bootstrap = this.verifyToken(bootstrapToken, "bootstrap");
      if (bootstrap && bootstrap.s === sandboxId && bootstrap.p === gatewayHost.port) {
        if (this.edgeWorkerHost && this.edgeWorkerSecret) {
          let endpoint: ExposedEndpoint;
          try {
            endpoint = await this.getOrExposeEndpoint(sandboxId, gatewayHost.port);
          } catch {
            return gatewayResponse("Failed to expose sandbox port", 502);
          }
          const edgeHostname = `p${gatewayHost.port}-${gatewayHost.slug}.${this.edgeWorkerHost}`;
          const sealed = await sealEdgeTarget(
            {
              u: endpoint.url,
              ...(endpoint.headers ? { h: endpoint.headers } : {}),
              s: sandboxId,
              p: gatewayHost.port,
              host: edgeHostname,
              exp: Date.now() + this.sessionTtlMs,
            },
            this.edgeWorkerSecret,
          );
          const edgePath = appendQueryParam(cleanPathAndQuery, PREVIEW_EDGE_PARAM, sealed);
          return redirectResponse(`https://${edgeHostname}${edgePath}`);
        }

        const exp = Date.now() + this.sessionTtlMs;
        let epoch = this.sessionEpochs.get(sandboxId);
        if (!epoch) {
          epoch = randomBytes(8).toString("hex");
          this.sessionEpochs.set(sandboxId, epoch);
        }
        const cookie = this.mintToken({ s: sandboxId, p: gatewayHost.port, exp, e: epoch }, "session");
        const maxAgeSecs = Math.max(1, Math.ceil(this.sessionTtlMs / 1000));
        const cookieAttrs = this.secureCookies ? "SameSite=None; Secure" : "SameSite=Lax";
        return redirectResponse(cleanPathAndQuery, {
          "set-cookie": `${PREVIEW_COOKIE_NAME}=${cookie}; Path=/; HttpOnly; ${cookieAttrs}; Max-Age=${maxAgeSecs}`,
        });
      }

      // Expired/stale __t3p param: if the browser already holds a valid session cookie, strip __t3p and continue.
      const existingSession = this.verifySessionCookie(req.headers.get("cookie"), sandboxId, gatewayHost.port);
      if (existingSession) {
        return redirectResponse(cleanPathAndQuery);
      }
      return gatewayResponse("Unauthorized", 401);
    }

    if (!this.verifySessionCookie(req.headers.get("cookie"), sandboxId, gatewayHost.port)) {
      return gatewayResponse("Unauthorized", 401);
    }

    let endpoint: ExposedEndpoint;
    try {
      endpoint = await this.getOrExposeEndpoint(sandboxId, gatewayHost.port);
    } catch {
      return gatewayResponse("Failed to expose sandbox port", 502);
    }

    if (req.headers.get("upgrade")?.toLowerCase() === "websocket") {
      return this.upgradeWebSocket(req, url, server, endpoint, sandboxId, gatewayHost);
    }
    return this.forwardHttp(req, url, endpoint, gatewayHost);
  }

  wsHandlers(): Bun.WebSocketHandler<PortGatewayWsData> {
    return {
      open: (ws) => {
        this.connectUpstream(ws);
      },
      message: (ws, msg) => {
        const upstream = ws.data.upstream;
        if (upstream && upstream.readyState === WebSocket.OPEN) {
          upstream.send(msg);
          return;
        }
        if (ws.data.closed) return;
        const chunk = typeof msg === "string" ? msg : new Uint8Array(msg);
        const size = typeof chunk === "string" ? Buffer.byteLength(chunk) : chunk.byteLength;
        const pending = (this.pendingBytes.get(ws) ?? 0) + size;
        if (pending > MAX_PENDING_BYTES) {
          closeClient(ws, 1009, "Message too large");
          return;
        }
        this.pendingBytes.set(ws, pending);
        ws.data.pending.push(chunk);
      },
      close: (ws, code, reason) => {
        ws.data.closed = true;
        this.untrackSocket(ws);
        this.pendingBytes.delete(ws);
        ws.data.pending = [];
        const upstream = ws.data.upstream;
        ws.data.upstream = null;
        if (upstream && (upstream.readyState === WebSocket.OPEN || upstream.readyState === WebSocket.CONNECTING)) {
          try {
            upstream.close(safeCloseCode(code), reason.slice(0, 120));
          } catch {
            upstream.close();
          }
        }
      },
    };
  }

  invalidateSandbox(sandboxId: string): void {
    this.generations.set(sandboxId, this.generation(sandboxId) + 1);
    this.sessionEpochs.delete(sandboxId);
    for (const key of Array.from(this.endpoints.keys())) {
      if (key.startsWith(`${sandboxId}:`)) this.endpoints.delete(key);
    }
    for (const [slug, mapped] of Array.from(this.slugToSandbox)) {
      if (mapped === sandboxId) this.slugToSandbox.delete(slug);
    }
    this.closeSandboxSockets(sandboxId, 1001, "Sandbox stopped");
  }

  closeAll(): void {
    for (const sandboxId of Array.from(this.socketsBySandbox.keys())) {
      this.closeSandboxSockets(sandboxId, 1001, "Server shutting down");
    }
  }

  exposedCount(sandboxId?: string): number {
    if (!sandboxId) return this.endpoints.size;
    let count = 0;
    for (const key of this.endpoints.keys()) {
      if (key.startsWith(`${sandboxId}:`)) count++;
    }
    return count;
  }

  private upgradeWebSocket(
    req: Request,
    url: URL,
    server: Bun.Server<any>,
    endpoint: ExposedEndpoint,
    sandboxId: string,
    gatewayHost: { port: number; slug: string; hostname: string },
  ): Response | undefined {
    const upstreamUrl = parseEndpointUrl(endpoint.url);
    if (!upstreamUrl) return gatewayResponse("Bad exposed endpoint", 502);
    const wsBase = endpoint.url.replace(/^https:\/\//i, "wss://").replace(/^http:\/\//i, "ws://");
    const pathAndQuery = stripGatewayParams(url, false);
    const protocols = parseWebSocketProtocols(req.headers.get("sec-websocket-protocol"));
    const upgraded = (server as unknown as Bun.Server<PortGatewayWsData>).upgrade(req, {
      ...(protocols.length > 0 ? { headers: { "sec-websocket-protocol": protocols[0] } } : {}),
      data: {
        kind: "port-gateway",
        sandboxId,
        port: gatewayHost.port,
        targetUrl: `${wsBase}${pathAndQuery}`,
        upstreamHeaders: this.buildWsHeaders(req, endpoint, upstreamUrl, gatewayHost.hostname),
        protocols,
        upstream: null,
        pending: [],
        closed: false,
      } satisfies PortGatewayWsData,
    });
    if (!upgraded) return gatewayResponse("WebSocket upgrade failed", 400);
    return undefined;
  }

  private async forwardHttp(
    req: Request,
    url: URL,
    endpoint: ExposedEndpoint,
    gatewayHost: { port: number; slug: string; hostname: string },
  ): Promise<Response> {
    const upstreamUrl = parseEndpointUrl(endpoint.url);
    if (!upstreamUrl) return gatewayResponse("Bad exposed endpoint", 502);
    const targetUrl = `${endpoint.url}${stripGatewayParams(url, false)}`;
    const headers = this.buildHttpHeaders(req, endpoint, upstreamUrl, gatewayHost.hostname);

    try {
      const res = await fetch(targetUrl, {
        method: req.method,
        headers,
        body: req.method === "GET" || req.method === "HEAD" ? undefined : req.body,
        redirect: "manual",
        signal: AbortSignal.timeout(60_000),
      });
      const outHeaders = copyResponseHeaders(res.headers);
      const location = outHeaders.get("location");
      if (location) outHeaders.set("location", rewriteLocation(location, gatewayHost, upstreamUrl));
      outHeaders.set("x-robots-tag", "noindex, nofollow, noarchive");
      return new Response(res.body, { status: res.status, statusText: res.statusText, headers: outHeaders });
    } catch (err) {
      console.error(`[port-gateway] HTTP forward failed for ${gatewayHost.hostname}:`, err);
      return gatewayResponse("Bad gateway", 502);
    }
  }

  private buildHttpHeaders(
    req: Request,
    endpoint: ExposedEndpoint,
    upstreamUrl: URL,
    previewHostname: string,
  ): Headers {
    const headers = new Headers();
    const connectionTokens = connectionHeaderTokens(req.headers.get("connection"));
    for (const [key, value] of req.headers) {
      const lower = key.toLowerCase();
      if (HOP_BY_HOP.has(lower) || connectionTokens.has(lower) || lower === "cookie" || lower.startsWith("sec-websocket-")) {
        continue;
      }
      headers.set(key, value);
    }

    const appCookie = stripCookie(req.headers.get("cookie"), PREVIEW_COOKIE_NAME);
    const endpointCookie = mergeEndpointHeaders(headers, endpoint.headers);
    const cookie = joinCookies(endpointCookie, appCookie);
    if (cookie) headers.set("cookie", cookie);

    headers.set("host", upstreamUrl.host);
    headers.set("x-forwarded-host", previewHostname);
    headers.set("x-forwarded-proto", "https");
    const origin = req.headers.get("origin");
    if (origin && originMatchesHost(origin, previewHostname)) headers.set("origin", upstreamUrl.origin);
    return headers;
  }

  private buildWsHeaders(
    req: Request,
    endpoint: ExposedEndpoint,
    upstreamUrl: URL,
    previewHostname: string,
  ): Record<string, string> {
    const headers: Record<string, string> = {};
    const appCookie = stripCookie(req.headers.get("cookie"), PREVIEW_COOKIE_NAME);
    let endpointCookie: string | null = null;
    for (const [key, value] of Object.entries(endpoint.headers ?? {})) {
      if (key.toLowerCase() === "cookie") endpointCookie = value;
      else if (key.toLowerCase() !== "host") setHeader(headers, key, value);
    }
    const cookie = joinCookies(endpointCookie, appCookie);
    if (cookie) setHeader(headers, "cookie", cookie);
    setHeader(headers, "x-forwarded-host", previewHostname);
    setHeader(headers, "x-forwarded-proto", "https");
    const origin = req.headers.get("origin");
    if (origin) setHeader(headers, "origin", originMatchesHost(origin, previewHostname) ? upstreamUrl.origin : origin);
    return headers;
  }

  private connectUpstream(ws: ServerWebSocket<PortGatewayWsData>): void {
    this.trackSocket(ws);
    let upstream: WebSocket;
    try {
      upstream = new WebSocket(ws.data.targetUrl, {
        headers: ws.data.upstreamHeaders,
        ...(ws.data.protocols.length > 0 ? { protocols: ws.data.protocols } : {}),
      } as unknown as string[]);
    } catch {
      closeClient(ws, 1011, "Upstream error");
      return;
    }
    upstream.binaryType = "arraybuffer";
    ws.data.upstream = upstream;
    let opened = false;

    upstream.addEventListener("open", () => {
      if (ws.data.upstream !== upstream) return;
      opened = true;
      for (const chunk of ws.data.pending) upstream.send(chunk);
      ws.data.pending = [];
      this.pendingBytes.delete(ws);
    });

    upstream.addEventListener("message", (ev) => {
      if (ws.data.upstream !== upstream || ws.data.closed) return;
      const data: unknown = ev.data;
      try {
        if (typeof data === "string") {
          ws.sendText(data);
        } else if (data instanceof ArrayBuffer) {
          ws.sendBinary(new Uint8Array(data));
        } else if (ArrayBuffer.isView(data)) {
          ws.sendBinary(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
        }
      } catch {
        // client already closed
      }
    });

    upstream.addEventListener("close", (ev) => {
      if (ws.data.upstream !== upstream) return;
      ws.data.upstream = null;
      if (ws.data.closed) return;
      if (opened && ev.code !== 1006) closeClient(ws, safeCloseCode(ev.code), ev.reason ?? "");
      else closeClient(ws, 1013, "Upstream unavailable");
    });

    upstream.addEventListener("error", () => {
      // the close handler follows
    });
  }

  private getOrExposeEndpoint(sandboxId: string, port: number): Promise<ExposedEndpoint> {
    const key = `${sandboxId}:${port}`;
    const existing = this.endpoints.get(key);
    if (existing?.endpoint && !endpointExpired(existing.endpoint)) return Promise.resolve(existing.endpoint);
    if (existing?.promise) return existing.promise;

    const generation = this.generation(sandboxId);
    const entry: EndpointEntry = { generation };
    this.endpoints.set(key, entry);
    entry.promise = Promise.resolve()
      .then(() => this.exposePort(sandboxId, port))
      .then((endpoint) => {
        const current = this.endpoints.get(key);
        if (current !== entry || generation !== this.generation(sandboxId)) {
          throw new Error("Sandbox endpoint was invalidated");
        }
        entry.endpoint = endpoint;
        return endpoint;
      })
      .catch((err) => {
        if (this.endpoints.get(key) === entry) this.endpoints.delete(key);
        throw err;
      })
      .finally(() => {
        if (this.endpoints.get(key) === entry) entry.promise = undefined;
      });
    return entry.promise;
  }

  private mintToken(payload: SignedPreviewPayload, purpose: "bootstrap" | "session"): string {
    const payloadB64 = bytesToBase64Url(new TextEncoder().encode(JSON.stringify(payload)));
    return `${payloadB64}.${this.signature(payloadB64, purpose)}`;
  }

  private verifyToken(token: string, purpose: "bootstrap" | "session"): SignedPreviewPayload | null {
    const parts = token.split(".");
    if (parts.length !== 2 || !parts[0] || !parts[1]) return null;
    const expected = this.signature(parts[0], purpose);
    const actualBuf = Buffer.from(parts[1]);
    const expectedBuf = Buffer.from(expected);
    if (actualBuf.length !== expectedBuf.length || !timingSafeEqual(actualBuf, expectedBuf)) return null;
    try {
      const parsed: unknown = JSON.parse(new TextDecoder().decode(base64UrlToBytes(parts[0])));
      if (!isSignedPreviewPayload(parsed)) return null;
      if (parsed.exp <= Date.now()) return null;
      return parsed;
    } catch {
      return null;
    }
  }

  private verifySessionCookie(cookieHeader: string | null, sandboxId: string, port: number): boolean {
    const sessionToken = extractCookie(cookieHeader, PREVIEW_COOKIE_NAME);
    const session = sessionToken ? this.verifyToken(sessionToken, "session") : null;
    if (!session || session.s !== sandboxId || session.p !== port) return false;
    const liveEpoch = this.sessionEpochs.get(sandboxId);
    return typeof session.e === "string" && liveEpoch !== undefined && session.e === liveEpoch;
  }

  private signature(payloadB64: string, purpose: "bootstrap" | "session"): string {
    return createHmac("sha256", this.hmacKey).update(`${purpose}.${payloadB64}`).digest("base64url");
  }

  private generation(sandboxId: string): number {
    return this.generations.get(sandboxId) ?? 0;
  }

  private trackSocket(ws: ServerWebSocket<PortGatewayWsData>): void {
    const sandboxId = ws.data.sandboxId;
    let set = this.socketsBySandbox.get(sandboxId);
    if (!set) this.socketsBySandbox.set(sandboxId, (set = new Set()));
    set.add(ws);
  }

  private untrackSocket(ws: ServerWebSocket<PortGatewayWsData>): void {
    const set = this.socketsBySandbox.get(ws.data.sandboxId);
    set?.delete(ws);
    if (set && set.size === 0) this.socketsBySandbox.delete(ws.data.sandboxId);
  }

  private closeSandboxSockets(sandboxId: string, code: number, reason: string): void {
    const sockets = this.socketsBySandbox.get(sandboxId);
    if (!sockets) return;
    this.socketsBySandbox.delete(sandboxId);
    for (const ws of sockets) closeClient(ws, code, reason);
  }
}

function endpointExpired(endpoint: ExposedEndpoint): boolean {
  if (!endpoint.expiresAt) return false;
  const expiresAt = Date.parse(endpoint.expiresAt);
  return Number.isFinite(expiresAt) && expiresAt <= Date.now();
}

function isValidPreviewPort(port: number): boolean {
  return Number.isInteger(port) && port >= 1 && port <= 65535 && port !== T3_RPC_PORT;
}

function assertValidPreviewPort(port: number): void {
  if (!isValidPreviewPort(port)) throw new Error(`Invalid preview port ${port}`);
}

function isLoopbackHostname(hostname: string): boolean {
  const host = hostname.toLowerCase();
  return host === "localhost" || host === "127.0.0.1" || host === "0.0.0.0" || host === "[::1]" || host === "::1";
}

function explicitPortText(rawUrl: string): string | null {
  const match = /^[a-zA-Z][a-zA-Z\d+.-]*:\/\/([^/?#]*)/.exec(rawUrl);
  if (!match) return null;
  let authority = match[1];
  const at = authority.lastIndexOf("@");
  if (at >= 0) authority = authority.slice(at + 1);
  if (authority.startsWith("[")) {
    const close = authority.indexOf("]");
    if (close < 0) return null;
    const rest = authority.slice(close + 1);
    return rest.startsWith(":") ? rest.slice(1) : null;
  }
  const colon = authority.lastIndexOf(":");
  return colon >= 0 ? authority.slice(colon + 1) : null;
}

function normalizePathAndQuery(pathAndQuery: string): string {
  if (!pathAndQuery) return "/";
  return pathAndQuery.startsWith("/") ? pathAndQuery : `/${pathAndQuery}`;
}

function appendQueryParam(pathAndQuery: string, name: string, value: string): string {
  const stripped = stripQueryParamFromPathAndQuery(pathAndQuery, name);
  const hashIndex = stripped.indexOf("#");
  const beforeHash = hashIndex >= 0 ? stripped.slice(0, hashIndex) : stripped;
  const hash = hashIndex >= 0 ? stripped.slice(hashIndex) : "";
  const separator = !beforeHash.includes("?") ? "?" : beforeHash.endsWith("?") || beforeHash.endsWith("&") ? "" : "&";
  return `${beforeHash}${separator}${name}=${encodeURIComponent(value)}${hash}`;
}

function stripQueryParamFromPathAndQuery(pathAndQuery: string, name: string): string {
  const hashIndex = pathAndQuery.indexOf("#");
  const beforeHash = hashIndex >= 0 ? pathAndQuery.slice(0, hashIndex) : pathAndQuery;
  const hash = hashIndex >= 0 ? pathAndQuery.slice(hashIndex) : "";
  const queryIndex = beforeHash.indexOf("?");
  if (queryIndex < 0) return pathAndQuery;
  const path = beforeHash.slice(0, queryIndex);
  const rawQuery = beforeHash.slice(queryIndex + 1);
  const parts = rawQuery.split("&").filter((part) => queryPartKey(part) !== name);
  const query = parts.join("&");
  return `${path}${query ? `?${query}` : ""}${hash}`;
}

function queryPartKey(part: string): string {
  const eq = part.indexOf("=");
  const raw = eq >= 0 ? part.slice(0, eq) : part;
  try {
    return decodeURIComponent(raw.replace(/\+/g, "%20"));
  } catch {
    return raw;
  }
}

function safePathname(pathname: string): string {
  const collapsed = pathname.replace(/\\/g, "/").replace(/^\/+/, "");
  return `/${collapsed}`;
}

function stripGatewayParams(url: URL, includeHash: boolean): string {
  const original = `${safePathname(url.pathname)}${url.search}${includeHash ? url.hash : ""}`;
  return stripQueryParamFromPathAndQuery(
    stripQueryParamFromPathAndQuery(original, PREVIEW_BOOTSTRAP_PARAM),
    PREVIEW_EDGE_PARAM,
  );
}

function extractCookie(cookieHeader: string | null, name: string): string | null {
  if (!cookieHeader) return null;
  for (const part of cookieHeader.split(";")) {
    const eq = part.indexOf("=");
    if (eq <= 0) continue;
    if (part.slice(0, eq).trim() === name) return part.slice(eq + 1).trim();
  }
  return null;
}

function stripCookie(cookieHeader: string | null, name: string): string | null {
  if (!cookieHeader) return null;
  const parts = cookieHeader.split(";").filter((part) => {
    const eq = part.indexOf("=");
    if (eq <= 0) return true;
    return part.slice(0, eq).trim() !== name;
  });
  const stripped = parts.map((part) => part.trim()).filter(Boolean).join("; ");
  return stripped || null;
}

function joinCookies(...cookies: Array<string | null>): string | null {
  const joined = cookies.filter((cookie): cookie is string => !!cookie).join("; ");
  return joined || null;
}

function mergeEndpointHeaders(headers: Headers, endpointHeaders?: Record<string, string>): string | null {
  let endpointCookie: string | null = null;
  for (const [key, value] of Object.entries(endpointHeaders ?? {})) {
    const lower = key.toLowerCase();
    if (lower === "cookie") endpointCookie = value;
    else if (lower !== "host") headers.set(key, value);
  }
  return endpointCookie;
}

function setHeader(headers: Record<string, string>, key: string, value: string): void {
  const existing = Object.keys(headers).find((candidate) => candidate.toLowerCase() === key.toLowerCase());
  if (existing && existing !== key) delete headers[existing];
  headers[key] = value;
}

function connectionHeaderTokens(header: string | null): Set<string> {
  const out = new Set<string>();
  if (!header) return out;
  for (const token of header.split(",")) {
    const normalized = token.trim().toLowerCase();
    if (normalized) out.add(normalized);
  }
  return out;
}

function sanitizeUpstreamSetCookie(raw: string): string | null {
  const parts = raw.split(";");
  const first = parts[0]?.trim() ?? "";
  const eq = first.indexOf("=");
  if (eq <= 0) return null;
  const name = first.slice(0, eq).trim().toLowerCase();
  if (RESERVED_SET_COOKIES.has(name)) return null;
  const attrs = parts
    .slice(1)
    .map((part) => part.trim())
    .filter((part) => part.length > 0 && !/^domain\s*=/i.test(part));
  return [first, ...attrs].join("; ");
}

function copyResponseHeaders(headersIn: Headers): Headers {
  const out = new Headers();
  const connectionTokens = connectionHeaderTokens(headersIn.get("connection"));
  let fallbackSetCookie: string | null = null;
  for (const [key, value] of headersIn) {
    const lower = key.toLowerCase();
    if (lower === "set-cookie") {
      fallbackSetCookie = value;
      continue;
    }
    if (STRIP_RESPONSE_HEADERS.has(lower) || connectionTokens.has(lower)) continue;
    out.set(key, value);
  }
  const getSetCookie = (headersIn as Headers & { getSetCookie?: () => string[] }).getSetCookie;
  const setCookies = typeof getSetCookie === "function" ? getSetCookie.call(headersIn) : [];
  if (setCookies.length > 0) {
    for (const cookie of setCookies) {
      const sanitized = sanitizeUpstreamSetCookie(cookie);
      if (sanitized) out.append("set-cookie", sanitized);
    }
  } else if (fallbackSetCookie) {
    const sanitized = sanitizeUpstreamSetCookie(fallbackSetCookie);
    if (sanitized) out.set("set-cookie", sanitized);
  }
  return out;
}

function rewriteLocation(
  location: string,
  gatewayHost: { port: number; slug: string; hostname: string },
  upstreamUrl: URL,
): string {
  let target: URL;
  try {
    target = new URL(location);
  } catch {
    return location;
  }
  const matchesUpstream = target.origin === upstreamUrl.origin;
  const matchesLoopback = isLoopbackHostname(target.hostname) && urlPort(target) === gatewayHost.port;
  if (!matchesUpstream && !matchesLoopback) return location;
  return `https://${gatewayHost.hostname}${target.pathname}${target.search}${target.hash}`;
}

function urlPort(url: URL): number | null {
  if (url.port) return Number(url.port);
  if (url.protocol === "http:") return 80;
  if (url.protocol === "https:") return 443;
  return null;
}

function originMatchesHost(origin: string, hostname: string): boolean {
  try {
    return new URL(origin).host.toLowerCase() === hostname.toLowerCase();
  } catch {
    return false;
  }
}

function parseEndpointUrl(rawUrl: string): URL | null {
  try {
    const url = new URL(rawUrl);
    if (url.protocol !== "http:" && url.protocol !== "https:") return null;
    return url;
  } catch {
    return null;
  }
}

function parseWebSocketProtocols(header: string | null): string[] {
  if (!header) return [];
  return header.split(",").map((part) => part.trim()).filter(Boolean);
}

function gatewayResponse(body: string, status: number): Response {
  return applySecurityHeaders(new Response(body, { status }));
}

function redirectResponse(location: string, extraHeaders: Record<string, string> = {}): Response {
  const headers = new Headers({ location });
  for (const [key, value] of Object.entries(extraHeaders)) headers.set(key, value);
  return applySecurityHeaders(new Response(null, { status: 302, headers }));
}

function closeClient(ws: ServerWebSocket<PortGatewayWsData>, code: number, reason: string): void {
  try {
    ws.close(code, reason.slice(0, 120));
  } catch {
    // already closed
  }
}

function isSignedPreviewPayload(value: unknown): value is SignedPreviewPayload {
  if (typeof value !== "object" || value === null) return false;
  const payload = value as Partial<SignedPreviewPayload>;
  return (
    typeof payload.s === "string" &&
    typeof payload.p === "number" &&
    isValidPreviewPort(payload.p) &&
    typeof payload.exp === "number" &&
    Number.isFinite(payload.exp)
  );
}

function isSealedEdgePayload(value: unknown): value is SealedEdgePayload {
  if (typeof value !== "object" || value === null) return false;
  const payload = value as Partial<SealedEdgePayload>;
  if (
    typeof payload.u !== "string" ||
    typeof payload.s !== "string" ||
    typeof payload.host !== "string" ||
    typeof payload.p !== "number" ||
    !isValidPreviewPort(payload.p) ||
    typeof payload.exp !== "number" ||
    !Number.isFinite(payload.exp)
  ) {
    return false;
  }
  if (payload.h !== undefined) {
    if (typeof payload.h !== "object" || payload.h === null) return false;
    for (const value of Object.values(payload.h)) {
      if (typeof value !== "string") return false;
    }
  }
  return true;
}

async function edgeKey(secret: string): Promise<CryptoKey> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(secret));
  return crypto.subtle.importKey("raw", digest, { name: "AES-GCM" }, false, ["encrypt", "decrypt"]);
}

function bytesToBase64Url(bytes: Uint8Array): string {
  let binary = "";
  const chunkSize = 0x8000;
  for (let offset = 0; offset < bytes.length; offset += chunkSize) {
    binary += String.fromCharCode(...bytes.subarray(offset, offset + chunkSize));
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function base64UrlToBytes(input: string): Uint8Array<ArrayBuffer> {
  if (!/^[A-Za-z0-9_-]*$/.test(input)) throw new Error("invalid base64url");
  const base64 = input.replace(/-/g, "+").replace(/_/g, "/") + "=".repeat((4 - (input.length % 4)) % 4);
  const binary = atob(base64);
  const out = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) out[i] = binary.charCodeAt(i);
  return out;
}
