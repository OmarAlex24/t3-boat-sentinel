import type { ServerWebSocket } from "bun";
import type { MediaSandbox } from "./hub-media.ts";
import { safeCloseCode } from "./proxy.ts";

/**
 * Native T3 Desktop preview tunnel (`/api/preview/tunnel?port=<port>`).
 *
 * When T3 Desktop renders a remote environment's `http://localhost:<port>/` inside its local
 * Electron `<webview>` (or handles "Open on this device" / port forwarding), its local SOCKS5
 * proxy opens raw TCP-over-WebSocket connections to `wss://<hub>/api/preview/tunnel?wsTicket=...&port=<port>`.
 *
 * Because the Hub multiplexes N Boat VMs behind one environment host and T3's tunnel URL carries
 * only `port` (never `threadId`), the Hub resolves an ordered list of candidate running VMs
 * (preferring the client's active VM and VMs discovered to listen on `port`) and dials their
 * `/api/preview/tunnel?port=<port>` endpoint BEFORE upgrading the client socket:
 *
 *   - First VM whose upstream WebSocket accepts the upgrade wins; the Hub upgrades the client
 *     socket and pipes raw binary/text frames bidirectionally.
 *   - If a VM refuses the port (non-101 / `x-t3-tunnel-port-refused`), the Hub tries the next
 *     candidate VM seamlessly.
 *   - If no running VM accepts `port` (or `port` is invalid/reserved, e.g. 3773), the Hub answers
 *     with `x-t3-tunnel-port-refused: 1` so T3 Desktop knows nothing is listening there.
 */

export const PREVIEW_TUNNEL_PATH = "/api/preview/tunnel";
export const PREVIEW_TUNNEL_PORT_REFUSED_HEADER = "x-t3-tunnel-port-refused";
export const PREVIEW_TUNNEL_RESERVED_PORT = 3773;

const DEFAULT_DIAL_TIMEOUT_MS = 5_000;
const DEFAULT_TOTAL_DIAL_TIMEOUT_MS = 12_000;
const MAX_EARLY_UPSTREAM_BYTES = 1024 * 1024;
const CLIENT_PING_MS = 25_000;

export interface TunnelUpstreamConnection {
  readonly sandboxId: string;
  attach(ws: ServerWebSocket<HubTunnelWsData>): void;
  send(msg: string | Buffer | Uint8Array): void;
  close(code?: number, reason?: string): void;
  abort(): void;
}

export interface HubTunnelWsData {
  kind: "hub-tunnel";
  clientId: string;
  sandboxId: string;
  port: number;
  connection: TunnelUpstreamConnection;
  closed: boolean;
  pingTimer: ReturnType<typeof setInterval> | null;
}

export type TunnelPortResult =
  | { ok: true; port: number }
  | { ok: false; status: number; message: string };

export function parseTunnelPort(raw: string | null): TunnelPortResult {
  if (!raw || !/^\d+$/.test(raw)) {
    return { ok: false, status: 400, message: "Invalid port" };
  }
  const port = Number(raw);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    return { ok: false, status: 400, message: "Invalid port" };
  }
  if (port === PREVIEW_TUNNEL_RESERVED_PORT) {
    return { ok: false, status: 403, message: `Port ${PREVIEW_TUNNEL_RESERVED_PORT} is reserved` };
  }
  return { ok: true, port };
}

export function buildUpstreamTunnelUrl(sandbox: MediaSandbox, port: number): string {
  const wsBase = sandbox.upstreamUrl
    .replace(/^https:\/\//i, "wss://")
    .replace(/^http:\/\//i, "ws://")
    .replace(/\/+$/, "");
  return `${wsBase}${PREVIEW_TUNNEL_PATH}?port=${port}`;
}

function shortReason(reason: string): string {
  return reason.slice(0, 120);
}

function closeClient(ws: ServerWebSocket<HubTunnelWsData>, code: number, reason: string): void {
  try {
    ws.close(safeCloseCode(code), shortReason(reason));
  } catch {
    // already closed
  }
}

function portRefusedResponse(status: number, message: string): Response {
  return new Response(message, {
    status,
    headers: { [PREVIEW_TUNNEL_PORT_REFUSED_HEADER]: "1" },
  });
}

function unrefTimer(timer: ReturnType<typeof setTimeout>): void {
  (timer as { unref?: () => void }).unref?.();
}

type DialResult = { ok: true; connection: TunnelUpstreamConnection } | { ok: false };

function dialUpstreamTunnel(
  sandbox: MediaSandbox,
  port: number,
  timeoutMs: number,
  signal?: AbortSignal,
): Promise<DialResult> {
  if (signal?.aborted || timeoutMs <= 0) return Promise.resolve({ ok: false });

  return new Promise<DialResult>((resolve) => {
    const targetUrl = buildUpstreamTunnelUrl(sandbox, port);
    const headers: Record<string, string> = {
      ...(sandbox.upstreamHeaders ?? {}),
      authorization: `Bearer ${sandbox.upstreamToken}`,
    };

    let upstream: WebSocket;
    try {
      upstream = new WebSocket(targetUrl, { headers } as unknown as string[]);
    } catch {
      resolve({ ok: false });
      return;
    }
    upstream.binaryType = "arraybuffer";

    let settled = false;
    let opened = false;
    let sink: ServerWebSocket<HubTunnelWsData> | null = null;
    const earlyBuffer: Array<string | Uint8Array> = [];
    let earlyBytes = 0;
    let earlyClosed: { code: number; reason: string } | null = null;

    const cleanupDialGuards = () => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
    };

    const failBeforeOpen = () => {
      if (settled) return;
      settled = true;
      cleanupDialGuards();
      try {
        if (upstream.readyState === WebSocket.OPEN || upstream.readyState === WebSocket.CONNECTING) {
          upstream.close();
        }
      } catch {
        // ignore close errors
      }
      resolve({ ok: false });
    };

    const onAbort = () => failBeforeOpen();
    signal?.addEventListener("abort", onAbort, { once: true });

    const timer = setTimeout(() => failBeforeOpen(), timeoutMs);
    unrefTimer(timer);

    const connection: TunnelUpstreamConnection = {
      sandboxId: sandbox.sandboxId,
      attach(ws) {
        sink = ws;
        while (earlyBuffer.length > 0 && !ws.data.closed) {
          const item = earlyBuffer.shift()!;
          try {
            if (typeof item === "string") ws.sendText(item);
            else ws.sendBinary(item);
          } catch {
            break;
          }
        }
        earlyBytes = 0;
        if (earlyClosed && !ws.data.closed) {
          closeClient(ws, earlyClosed.code, earlyClosed.reason);
        }
      },
      send(msg) {
        if (upstream.readyState !== WebSocket.OPEN) return;
        try {
          upstream.send(msg);
        } catch {
          // close event will follow
        }
      },
      close(code = 1000, reason = "") {
        sink = null;
        earlyBuffer.length = 0;
        earlyBytes = 0;
        if (upstream.readyState === WebSocket.OPEN || upstream.readyState === WebSocket.CONNECTING) {
          try {
            upstream.close(safeCloseCode(code), shortReason(reason));
          } catch {
            try {
              upstream.close();
            } catch {
              // ignore
            }
          }
        }
      },
      abort() {
        connection.close(1000, "Client upgrade failed");
      },
    };

    upstream.addEventListener("open", () => {
      if (settled) {
        connection.abort();
        return;
      }
      settled = true;
      opened = true;
      cleanupDialGuards();
      resolve({ ok: true, connection });
    });

    upstream.addEventListener("message", (ev) => {
      if (!opened) return;
      const data = ev.data;
      if (sink && !sink.data.closed) {
        try {
          if (typeof data === "string") sink.sendText(data);
          else if (data instanceof ArrayBuffer) sink.sendBinary(new Uint8Array(data));
          else if (ArrayBuffer.isView(data)) {
            sink.sendBinary(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
          }
        } catch {
          connection.close(1011, "Client send failed");
        }
        return;
      }
      let buffered: string | Uint8Array | null = null;
      let size = 0;
      if (typeof data === "string") {
        buffered = data;
        size = Buffer.byteLength(data);
      } else if (data instanceof ArrayBuffer) {
        buffered = new Uint8Array(data);
        size = buffered.byteLength;
      } else if (ArrayBuffer.isView(data)) {
        buffered = new Uint8Array(data.buffer, data.byteOffset, data.byteLength).slice();
        size = buffered.byteLength;
      }
      if (!buffered) return;
      earlyBytes += size;
      if (earlyBytes > MAX_EARLY_UPSTREAM_BYTES) {
        earlyClosed = { code: 1009, reason: "Upstream buffer overflow" };
        connection.close(1009, "Upstream buffer overflow");
        return;
      }
      earlyBuffer.push(buffered);
    });

    upstream.addEventListener("close", (ev) => {
      if (!opened) {
        failBeforeOpen();
        return;
      }
      const code = safeCloseCode(ev.code || 1000);
      const reason = ev.reason || "";
      if (sink && !sink.data.closed) {
        closeClient(sink, code, reason);
      } else {
        earlyClosed = { code, reason };
      }
    });

    upstream.addEventListener("error", () => {
      if (!opened) {
        failBeforeOpen();
        return;
      }
      if (sink && !sink.data.closed) {
        closeClient(sink, 1011, "Upstream tunnel error");
      } else if (!earlyClosed) {
        earlyClosed = { code: 1011, reason: "Upstream tunnel error" };
      }
    });
  });
}

export interface HubTunnelProxyOptions {
  dialTimeoutMs?: number;
  totalDialTimeoutMs?: number;
  onConnected?: (clientId: string, port: number, sandboxId: string) => void;
}

/**
 * Races multiple candidate VMs in parallel and returns the first one that accepts the tunnel.
 * Every losing candidate that opens later is immediately aborted so no upstream sockets leak.
 */
function raceDialCandidates(
  candidates: MediaSandbox[],
  port: number,
  timeoutMs: number,
  signal?: AbortSignal,
): Promise<{ ok: true; sandbox: MediaSandbox; connection: TunnelUpstreamConnection } | { ok: false }> {
  if (candidates.length === 0 || signal?.aborted || timeoutMs <= 0) {
    return Promise.resolve({ ok: false });
  }
  return new Promise((resolve) => {
    const raceAbort = new AbortController();
    const onParentAbort = () => raceAbort.abort();
    signal?.addEventListener("abort", onParentAbort, { once: true });

    let settled = false;
    let remaining = candidates.length;

    const finish = (
      result: { ok: true; sandbox: MediaSandbox; connection: TunnelUpstreamConnection } | { ok: false },
    ) => {
      if (settled) return;
      settled = true;
      signal?.removeEventListener("abort", onParentAbort);
      raceAbort.abort();
      resolve(result);
    };

    for (const candidate of candidates) {
      void dialUpstreamTunnel(candidate, port, timeoutMs, raceAbort.signal).then((attempt) => {
        if (attempt.ok) {
          if (settled) {
            attempt.connection.abort();
            return;
          }
          finish({ ok: true, sandbox: candidate, connection: attempt.connection });
          return;
        }
        remaining--;
        if (remaining === 0) finish({ ok: false });
      });
    }
  });
}

export class HubTunnelProxy {
  private readonly bySandbox = new Map<string, Set<ServerWebSocket<HubTunnelWsData>>>();
  private readonly dialTimeoutMs: number;
  private readonly totalDialTimeoutMs: number;
  private readonly onConnected?: (clientId: string, port: number, sandboxId: string) => void;

  constructor(
    private readonly resolveCandidates: (clientId: string, port: number) => MediaSandbox[],
    opts: HubTunnelProxyOptions = {},
  ) {
    this.dialTimeoutMs = opts.dialTimeoutMs ?? DEFAULT_DIAL_TIMEOUT_MS;
    this.totalDialTimeoutMs = opts.totalDialTimeoutMs ?? DEFAULT_TOTAL_DIAL_TIMEOUT_MS;
    this.onConnected = opts.onConnected;
  }

  async handleUpgrade(
    req: Request,
    url: URL,
    server: Bun.Server<any>,
    clientId: string,
  ): Promise<Response | undefined> {
    if (req.headers.get("upgrade")?.toLowerCase() !== "websocket") {
      return new Response("Upgrade Required", { status: 426 });
    }
    const parsedPort = parseTunnelPort(url.searchParams.get("port"));
    if (!parsedPort.ok) {
      return portRefusedResponse(parsedPort.status, parsedPort.message);
    }
    const port = parsedPort.port;
    const candidates = this.resolveCandidates(clientId, port);
    if (candidates.length === 0) {
      return portRefusedResponse(502, `No running sandbox available for port ${port}`);
    }

    const deadline = Date.now() + this.totalDialTimeoutMs;
    const [primary, ...fallbacks] = candidates;

    let winner: { sandbox: MediaSandbox; connection: TunnelUpstreamConnection } | null = null;
    if (primary && !req.signal?.aborted) {
      const remainingMs = deadline - Date.now();
      const first = await dialUpstreamTunnel(primary, port, Math.min(this.dialTimeoutMs, remainingMs), req.signal);
      if (first.ok) {
        winner = { sandbox: primary, connection: first.connection };
      }
    }

    if (!winner && fallbacks.length > 0 && !req.signal?.aborted) {
      const remainingMs = deadline - Date.now();
      const raced = await raceDialCandidates(fallbacks, port, Math.min(this.dialTimeoutMs, remainingMs), req.signal);
      if (raced.ok) {
        winner = { sandbox: raced.sandbox, connection: raced.connection };
      }
    }

    if (!winner) {
      return portRefusedResponse(502, `Port ${port} is not reachable on any running sandbox`);
    }

    this.onConnected?.(clientId, port, winner.sandbox.sandboxId);

    const upgraded = (server as unknown as Bun.Server<HubTunnelWsData>).upgrade(req, {
      data: {
        kind: "hub-tunnel",
        clientId,
        sandboxId: winner.sandbox.sandboxId,
        port,
        connection: winner.connection,
        closed: false,
        pingTimer: null,
      } satisfies HubTunnelWsData,
    });
    if (!upgraded) {
      winner.connection.abort();
      return new Response("WebSocket upgrade failed", { status: 400 });
    }
    return undefined;
  }

  open(ws: ServerWebSocket<HubTunnelWsData>): void {
    this.track(ws);
    const timer = setInterval(() => {
      if (ws.data.closed) return;
      try {
        ws.ping();
      } catch {
        // close event will clean up
      }
    }, CLIENT_PING_MS);
    unrefTimer(timer);
    ws.data.pingTimer = timer;
    ws.data.connection.attach(ws);
  }

  message(ws: ServerWebSocket<HubTunnelWsData>, msg: string | Buffer): void {
    if (ws.data.closed) return;
    ws.data.connection.send(msg);
  }

  close(ws: ServerWebSocket<HubTunnelWsData>, code?: number, reason?: string): void {
    ws.data.closed = true;
    this.untrack(ws);
    if (ws.data.pingTimer) {
      clearInterval(ws.data.pingTimer);
      ws.data.pingTimer = null;
    }
    ws.data.connection.close(code, reason);
  }

  closeSandbox(sandboxId: string, code = 1001, reason = "Sandbox stopped"): void {
    const sockets = this.bySandbox.get(sandboxId);
    if (!sockets) return;
    this.bySandbox.delete(sandboxId);
    for (const ws of sockets) closeClient(ws, code, reason);
  }

  closeAll(code = 1001, reason = "Server shutting down"): void {
    for (const sandboxId of Array.from(this.bySandbox.keys())) {
      this.closeSandbox(sandboxId, code, reason);
    }
  }

  count(sandboxId?: string): number {
    if (sandboxId) return this.bySandbox.get(sandboxId)?.size ?? 0;
    let total = 0;
    for (const set of this.bySandbox.values()) total += set.size;
    return total;
  }

  private track(ws: ServerWebSocket<HubTunnelWsData>): void {
    let set = this.bySandbox.get(ws.data.sandboxId);
    if (!set) {
      set = new Set();
      this.bySandbox.set(ws.data.sandboxId, set);
    }
    set.add(ws);
  }

  private untrack(ws: ServerWebSocket<HubTunnelWsData>): void {
    const set = this.bySandbox.get(ws.data.sandboxId);
    if (!set) return;
    set.delete(ws);
    if (set.size === 0) this.bySandbox.delete(ws.data.sandboxId);
  }
}
