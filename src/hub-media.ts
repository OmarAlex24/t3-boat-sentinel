import type { ServerWebSocket } from "bun";
import { safeCloseCode } from "./proxy.ts";

/**
 * Media sockets: the server-browser preview stream (`/api/preview-stream/ws?threadId&tabId`) and
 * the device hub (`/api/device-hub/*`). The T3 app opens them with a `media` wsTicket the Hub
 * minted; the Hub checks it, then pipes the socket to the VM that runs the chat, authenticated
 * with that VM's own Bearer token (the VM checks a query `wsTicket` before the header, so the
 * Hub's ticket must not reach it).
 *
 * The preview client reads close codes: 4404 is "tab gone" and stops for good; 1008, 4401 and a
 * failed handshake are "unauthorized" and also stop; anything else reconnects with backoff. So
 * only a bad ticket is refused before the upgrade, and every other failure is a close code.
 */

export const PREVIEW_STREAM_PREFIX = "/api/preview-stream/";
export const DEVICE_HUB_PREFIX = "/api/device-hub/";
/** The chat (or its tab) is gone: the preview client stops reconnecting. */
export const MEDIA_GONE_CLOSE = 4404;
/** Try again later: the client reconnects with backoff (up to 10 s). */
export const MEDIA_RETRY_CLOSE = 1013;

/** Client messages (acks, input) held while the VM socket opens; past this the client is dropped. */
const MAX_PENDING_BYTES = 1024 * 1024;
/** A static page sends no frames: ping the VM socket so proxies on the way keep it open. */
const UPSTREAM_PING_MS = 25_000;

export interface HubMediaWsData {
  kind: "hub-media";
  clientId: string;
  /** Path and query for the VM, without the Hub's `wsTicket`. */
  upstreamPathAndQuery: string;
  /** The chat a preview stream belongs to; null for the device hub (no chat in its URLs). */
  threadId: string | null;
  sandboxId: string | null;
  upstream: WebSocket | null;
  pending: Array<string | Uint8Array>;
  pendingBytes: number;
  closed: boolean;
  pingTimer: ReturnType<typeof setInterval> | null;
}

export interface MediaSandbox {
  sandboxId: string;
  upstreamUrl: string;
  upstreamToken: string;
  upstreamHeaders?: Record<string, string> | null;
}

export type MediaTarget = { ok: true; sandbox: MediaSandbox } | { ok: false; code: number; reason: string };

export function isMediaPath(path: string): boolean {
  return path.startsWith(PREVIEW_STREAM_PREFIX) || path.startsWith(DEVICE_HUB_PREFIX);
}

/** The chat a media URL names: preview-stream URLs (ws, download, upload) carry `threadId`. */
export function mediaThreadId(url: URL): string | null {
  if (!url.pathname.startsWith(PREVIEW_STREAM_PREFIX)) return null;
  return url.searchParams.get("threadId") || null;
}

/** The URL to send the VM: the Hub's `wsTicket` removed, everything else as the client sent it. */
export function withoutHubTicket(url: URL): URL {
  const out = new URL(url.toString());
  out.searchParams.delete("wsTicket");
  return out;
}

export function newMediaWsData(clientId: string, url: URL): HubMediaWsData {
  const upstream = withoutHubTicket(url);
  return {
    kind: "hub-media",
    clientId,
    upstreamPathAndQuery: `${upstream.pathname}${upstream.search}`,
    threadId: mediaThreadId(url),
    sandboxId: null,
    upstream: null,
    pending: [],
    pendingBytes: 0,
    closed: false,
    pingTimer: null,
  };
}

/** A close reason must fit in 123 bytes. */
function shortReason(reason: string): string {
  return reason.length > 120 ? `${reason.slice(0, 117)}...` : reason;
}

function closeClient(ws: ServerWebSocket<HubMediaWsData>, code: number, reason: string): void {
  try {
    ws.close(code, shortReason(reason));
  } catch {
    // already closed
  }
}

/**
 * Pipes media sockets to their VM, message for message: frames are not re-encoded, and the
 * preview's ack (one frame in flight per viewer) passes through untouched, so the VM's own flow
 * control spans the Hub.
 */
export class HubMediaProxy {
  private readonly bySandbox = new Map<string, Set<ServerWebSocket<HubMediaWsData>>>();

  constructor(private readonly resolveTarget: (data: HubMediaWsData) => Promise<MediaTarget>) {}

  open(ws: ServerWebSocket<HubMediaWsData>): void {
    void this.connect(ws);
  }

  message(ws: ServerWebSocket<HubMediaWsData>, msg: string | Buffer): void {
    const upstream = ws.data.upstream;
    if (upstream && upstream.readyState === WebSocket.OPEN) {
      upstream.send(msg);
      return;
    }
    const chunk = typeof msg === "string" ? msg : new Uint8Array(msg);
    ws.data.pendingBytes += typeof chunk === "string" ? chunk.length : chunk.byteLength;
    if (ws.data.pendingBytes > MAX_PENDING_BYTES) {
      closeClient(ws, MEDIA_RETRY_CLOSE, "The chat's VM did not answer in time");
      return;
    }
    ws.data.pending.push(chunk);
  }

  close(ws: ServerWebSocket<HubMediaWsData>, code?: number, reason?: string): void {
    ws.data.closed = true;
    this.untrack(ws);
    this.stopPing(ws);
    const upstream = ws.data.upstream;
    ws.data.upstream = null;
    ws.data.pending = [];
    if (upstream && (upstream.readyState === WebSocket.OPEN || upstream.readyState === WebSocket.CONNECTING)) {
      try {
        upstream.close(safeCloseCode(code ?? 1000), (reason ?? "").slice(0, 120));
      } catch {
        upstream.close();
      }
    }
  }

  /** Closes every media socket on a VM that went away (asleep, failed wake, destroyed). */
  closeSandbox(sandboxId: string, code: number, reason: string): void {
    const sockets = this.bySandbox.get(sandboxId);
    if (!sockets) return;
    this.bySandbox.delete(sandboxId);
    for (const ws of sockets) closeClient(ws, code, reason);
  }

  closeAll(code: number, reason: string): void {
    for (const sandboxId of Array.from(this.bySandbox.keys())) this.closeSandbox(sandboxId, code, reason);
  }

  /** Open media sockets on a VM (all VMs when omitted). */
  count(sandboxId?: string): number {
    if (sandboxId) return this.bySandbox.get(sandboxId)?.size ?? 0;
    let n = 0;
    for (const set of this.bySandbox.values()) n += set.size;
    return n;
  }

  private async connect(ws: ServerWebSocket<HubMediaWsData>): Promise<void> {
    let target: MediaTarget;
    try {
      target = await this.resolveTarget(ws.data);
    } catch (err) {
      target = { ok: false, code: MEDIA_RETRY_CLOSE, reason: err instanceof Error ? err.message : String(err) };
    }
    if (ws.data.closed) return;
    if (!target.ok) {
      closeClient(ws, target.code, target.reason);
      return;
    }
    const sb = target.sandbox;
    ws.data.sandboxId = sb.sandboxId;
    let set = this.bySandbox.get(sb.sandboxId);
    if (!set) this.bySandbox.set(sb.sandboxId, (set = new Set()));
    set.add(ws);

    const wsBase = sb.upstreamUrl.replace(/^https:\/\//, "wss://").replace(/^http:\/\//, "ws://");
    const upstream = new WebSocket(`${wsBase}${ws.data.upstreamPathAndQuery}`, {
      headers: { ...(sb.upstreamHeaders ?? {}), authorization: `Bearer ${sb.upstreamToken}` },
    } as unknown as string[]);
    upstream.binaryType = "arraybuffer";
    ws.data.upstream = upstream;
    let opened = false;

    upstream.addEventListener("open", () => {
      if (ws.data.upstream !== upstream) return;
      opened = true;
      for (const chunk of ws.data.pending) upstream.send(chunk);
      ws.data.pending = [];
      ws.data.pendingBytes = 0;
      ws.data.pingTimer = setInterval(() => {
        if (upstream.readyState === WebSocket.OPEN) (upstream as unknown as { ping?: () => void }).ping?.();
      }, UPSTREAM_PING_MS);
    });

    upstream.addEventListener("message", (ev) => {
      if (ws.data.upstream !== upstream) return;
      try {
        const data: unknown = ev.data;
        if (typeof data === "string") ws.sendText(data);
        else if (data instanceof ArrayBuffer) ws.sendBinary(new Uint8Array(data));
        else if (ArrayBuffer.isView(data)) ws.sendBinary(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
      } catch {
        // client already closed
      }
    });

    upstream.addEventListener("close", (ev) => {
      if (ws.data.upstream !== upstream) return;
      ws.data.upstream = null;
      this.stopPing(ws);
      this.untrack(ws);
      if (ws.data.closed) return;
      // The VM's own code passes through (4404 tab gone, 4503 host setup, 1012 reconnect...)
      if (opened && ev.code !== 1006) closeClient(ws, safeCloseCode(ev.code), ev.reason ?? "");
      else closeClient(ws, MEDIA_RETRY_CLOSE, opened ? "Lost the chat's VM" : "Could not reach the chat's VM");
    });

    upstream.addEventListener("error", () => {
      // the close handler follows
    });
  }

  private untrack(ws: ServerWebSocket<HubMediaWsData>): void {
    const sandboxId = ws.data.sandboxId;
    if (!sandboxId) return;
    const set = this.bySandbox.get(sandboxId);
    set?.delete(ws);
    if (set && set.size === 0) this.bySandbox.delete(sandboxId);
  }

  private stopPing(ws: ServerWebSocket<HubMediaWsData>): void {
    if (ws.data.pingTimer) clearInterval(ws.data.pingTimer);
    ws.data.pingTimer = null;
  }
}
