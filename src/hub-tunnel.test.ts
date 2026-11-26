import { afterEach, describe, expect, test } from "bun:test";
import {
  HubTunnelProxy,
  PREVIEW_TUNNEL_PATH,
  PREVIEW_TUNNEL_PORT_REFUSED_HEADER,
  buildUpstreamTunnelUrl,
  parseTunnelPort,
  type HubTunnelWsData,
} from "./hub-tunnel.ts";
import type { MediaSandbox } from "./hub-media.ts";

const cleanups: Array<() => void> = [];

afterEach(() => {
  while (cleanups.length > 0) {
    try {
      cleanups.pop()!();
    } catch {
      // ignore cleanup errors
    }
  }
});

interface MockVmTunnel {
  sandbox: MediaSandbox;
  connections: Array<{ port: string | null; authorization: string | null; extraHeader: string | null; wsTicket: string | null }>;
  stop: () => void;
}

function startMockVmTunnel(
  sandboxId: string,
  opts: {
    openPorts: number[];
    token?: string;
    upstreamHeaders?: Record<string, string>;
    bannerOnOpen?: Uint8Array | string;
  },
): MockVmTunnel {
  const token = opts.token ?? `tok-${sandboxId}`;
  const connections: MockVmTunnel["connections"] = [];
  const server = Bun.serve<{ port: number }>({
    port: 0,
    hostname: "127.0.0.1",
    fetch(req, srv) {
      const url = new URL(req.url);
      if (url.pathname !== PREVIEW_TUNNEL_PATH) return new Response("Not Found", { status: 404 });
      connections.push({
        port: url.searchParams.get("port"),
        authorization: req.headers.get("authorization"),
        extraHeader: req.headers.get("x-boat-gate"),
        wsTicket: url.searchParams.get("wsTicket"),
      });
      if (req.headers.get("authorization") !== `Bearer ${token}`) {
        return new Response("Unauthorized", { status: 401 });
      }
      const port = Number(url.searchParams.get("port") ?? "0");
      if (!opts.openPorts.includes(port)) {
        return new Response("Connection refused", {
          status: 502,
          headers: { [PREVIEW_TUNNEL_PORT_REFUSED_HEADER]: "1" },
        });
      }
      if (srv.upgrade(req, { data: { port } })) return undefined;
      return new Response("Upgrade failed", { status: 400 });
    },
    websocket: {
      open(ws) {
        if (opts.bannerOnOpen !== undefined) {
          if (typeof opts.bannerOnOpen === "string") ws.sendText(opts.bannerOnOpen);
          else ws.sendBinary(opts.bannerOnOpen);
        }
      },
      message(ws, msg) {
        if (typeof msg === "string") {
          ws.sendText(`${sandboxId}:${ws.data.port}:${msg}`);
        } else {
          const prefix = Buffer.from(`${sandboxId}:${ws.data.port}:`);
          ws.sendBinary(Buffer.concat([prefix, Buffer.from(msg)]));
        }
      },
    },
  });
  cleanups.push(() => server.stop(true));
  return {
    sandbox: {
      sandboxId,
      upstreamUrl: `http://127.0.0.1:${server.port}`,
      upstreamToken: token,
      upstreamHeaders: opts.upstreamHeaders ?? null,
    },
    connections,
    stop: () => server.stop(true),
  };
}

function startHubTunnelServer(proxy: HubTunnelProxy): { baseWs: string; baseHttp: string } {
  const server = Bun.serve<HubTunnelWsData>({
    port: 0,
    hostname: "127.0.0.1",
    fetch(req, srv) {
      const url = new URL(req.url);
      if (url.pathname === PREVIEW_TUNNEL_PATH) {
        return proxy.handleUpgrade(req, url, srv, "client-1");
      }
      return new Response("Not Found", { status: 404 });
    },
    websocket: {
      open: (ws) => proxy.open(ws),
      message: (ws, msg) => proxy.message(ws, msg),
      close: (ws, code, reason) => proxy.close(ws, code, reason),
    },
  });
  cleanups.push(() => {
    proxy.closeAll();
    server.stop(true);
  });
  return {
    baseWs: `ws://127.0.0.1:${server.port}`,
    baseHttp: `http://127.0.0.1:${server.port}`,
  };
}

async function openClientWs(url: string): Promise<{
  ws: WebSocket;
  nextMessage: () => Promise<string | Uint8Array>;
  closed: Promise<{ code: number; reason: string }>;
}> {
  const ws = new WebSocket(url);
  ws.binaryType = "arraybuffer";
  cleanups.push(() => {
    if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) ws.close();
  });
  const queue: Array<string | Uint8Array> = [];
  const waiters: Array<(msg: string | Uint8Array) => void> = [];
  ws.addEventListener("message", (ev) => {
    const val = typeof ev.data === "string" ? ev.data : new Uint8Array(ev.data as ArrayBuffer);
    const waiter = waiters.shift();
    if (waiter) waiter(val);
    else queue.push(val);
  });
  const closed = new Promise<{ code: number; reason: string }>((resolve) => {
    ws.addEventListener("close", (ev) => resolve({ code: ev.code, reason: ev.reason }));
  });
  await new Promise<void>((resolve, reject) => {
    ws.addEventListener("open", () => resolve(), { once: true });
    ws.addEventListener("error", () => reject(new Error(`WebSocket failed to open: ${url}`)), { once: true });
  });
  return {
    ws,
    nextMessage: () =>
      queue.length > 0
        ? Promise.resolve(queue.shift()!)
        : new Promise<string | Uint8Array>((resolve) => waiters.push(resolve)),
    closed,
  };
}

describe("HubTunnelProxy helpers", () => {
  test("parseTunnelPort validates port range and rejects T3 control port 3773", () => {
    expect(parseTunnelPort("5173")).toEqual({ ok: true, port: 5173 });
    expect(parseTunnelPort("1")).toEqual({ ok: true, port: 1 });
    expect(parseTunnelPort("65535")).toEqual({ ok: true, port: 65535 });
    expect(parseTunnelPort("3773")).toEqual({ ok: false, status: 403, message: "Port 3773 is reserved" });
    expect(parseTunnelPort(null).ok).toBe(false);
    expect(parseTunnelPort("0").ok).toBe(false);
    expect(parseTunnelPort("65536").ok).toBe(false);
    expect(parseTunnelPort("abc").ok).toBe(false);
  });

  test("buildUpstreamTunnelUrl converts scheme and keeps only port query", () => {
    expect(
      buildUpstreamTunnelUrl({ sandboxId: "sb-1", upstreamUrl: "https://vm.example.com/", upstreamToken: "t" }, 5173),
    ).toBe("wss://vm.example.com/api/preview/tunnel?port=5173");
    expect(
      buildUpstreamTunnelUrl({ sandboxId: "sb-1", upstreamUrl: "http://127.0.0.1:4100", upstreamToken: "t" }, 3000),
    ).toBe("ws://127.0.0.1:4100/api/preview/tunnel?port=3000");
  });
});

describe("HubTunnelProxy", () => {
  test("falls through candidate VMs until one accepts the port, strips wsTicket, and pipes binary + text", async () => {
    const vm1 = startMockVmTunnel("sb-1", { openPorts: [3000] });
    const vm2 = startMockVmTunnel("sb-2", { openPorts: [5173], upstreamHeaders: { "x-boat-gate": "gate-2" } });
    const proxy = new HubTunnelProxy(() => [vm1.sandbox, vm2.sandbox]);
    const { baseWs } = startHubTunnelServer(proxy);

    const client = await openClientWs(`${baseWs}${PREVIEW_TUNNEL_PATH}?wsTicket=hub-ticket-secret&port=5173`);
    client.ws.send(new Uint8Array([1, 2, 3, 4]));
    const binReply = await client.nextMessage();
    expect(binReply).toBeInstanceOf(Uint8Array);
    expect(Buffer.from(binReply as Uint8Array).toString("latin1")).toBe("sb-2:5173:\x01\x02\x03\x04");

    client.ws.send("GET / HTTP/1.1");
    expect(await client.nextMessage()).toBe("sb-2:5173:GET / HTTP/1.1");

    expect(vm1.connections).toHaveLength(1);
    expect(vm2.connections).toEqual([
      { port: "5173", authorization: "Bearer tok-sb-2", extraHeader: "gate-2", wsTicket: null },
    ]);
    expect(proxy.count("sb-2")).toBe(1);
    expect(proxy.count("sb-1")).toBe(0);
  });

  test("returns x-t3-tunnel-port-refused when no VM listens, when port 3773 is requested, or on non-WS requests", async () => {
    const vm1 = startMockVmTunnel("sb-1", { openPorts: [3000] });
    const proxy = new HubTunnelProxy(() => [vm1.sandbox]);
    const { baseHttp } = startHubTunnelServer(proxy);

    const wsHeaders = {
      upgrade: "websocket",
      connection: "Upgrade",
      "sec-websocket-key": "dGhlIHNhbXBsZSBub25jZQ==",
      "sec-websocket-version": "13",
    };

    const refused = await fetch(`${baseHttp}${PREVIEW_TUNNEL_PATH}?port=5173`, { headers: wsHeaders });
    expect(refused.status).toBe(502);
    expect(refused.headers.get(PREVIEW_TUNNEL_PORT_REFUSED_HEADER)).toBe("1");

    const reserved = await fetch(`${baseHttp}${PREVIEW_TUNNEL_PATH}?port=3773`, { headers: wsHeaders });
    expect(reserved.status).toBe(403);
    expect(reserved.headers.get(PREVIEW_TUNNEL_PORT_REFUSED_HEADER)).toBe("1");
    expect(vm1.connections).toHaveLength(1); // 3773 never dialed vm1

    const plainHttp = await fetch(`${baseHttp}${PREVIEW_TUNNEL_PATH}?port=3000`);
    expect(plainHttp.status).toBe(426);
  });

  test("delivers early upstream frames and closes only the invalidated sandbox's sockets", async () => {
    const vm1 = startMockVmTunnel("sb-1", { openPorts: [5173], bannerOnOpen: new Uint8Array([0x53, 0x53, 0x48]) });
    const vm2 = startMockVmTunnel("sb-2", { openPorts: [3000] });
    const proxy = new HubTunnelProxy((_cid, port) => (port === 5173 ? [vm1.sandbox] : [vm2.sandbox]));
    const { baseWs } = startHubTunnelServer(proxy);

    const c1 = await openClientWs(`${baseWs}${PREVIEW_TUNNEL_PATH}?port=5173`);
    const banner = await c1.nextMessage();
    expect(Buffer.from(banner as Uint8Array).toString("utf8")).toBe("SSH");

    const c2 = await openClientWs(`${baseWs}${PREVIEW_TUNNEL_PATH}?port=3000`);
    expect(proxy.count()).toBe(2);

    proxy.closeSandbox("sb-1", 1001, "VM slept");
    const c1Closed = await c1.closed;
    expect([1000, 1001]).toContain(c1Closed.code);
    expect(proxy.count("sb-1")).toBe(0);
    expect(proxy.count("sb-2")).toBe(1);

    c2.ws.send("ping");
    expect(await c2.nextMessage()).toBe("sb-2:3000:ping");
  });
});
