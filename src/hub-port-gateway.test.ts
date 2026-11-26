import { afterEach, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import type { ExposedEndpoint } from "./providers/types.ts";
import {
  HubPortGateway,
  PREVIEW_BOOTSTRAP_PARAM,
  PREVIEW_COOKIE_NAME,
  PREVIEW_EDGE_PARAM,
  parseGatewayHost,
  parseLoopbackPreviewUrl,
  sandboxSlug,
  sealEdgeTarget,
  unsealEdgeTarget,
} from "./hub-port-gateway.ts";

const DOMAIN = "preview.test";
const SECRET = "control-secret-for-tests";
const EDGE_SECRET = "edge-shared-secret-for-tests";
const SANDBOX = "sb-preview-test";
const PORT = 5173;

const cleanups: Array<() => void> = [];

afterEach(() => {
  while (cleanups.length > 0) {
    try {
      cleanups.pop()!();
    } catch {
      // best-effort test cleanup
    }
  }
});

async function until(cond: () => boolean, timeoutMs = 3_000): Promise<void> {
  const start = Date.now();
  while (!cond()) {
    if (Date.now() - start > timeoutMs) throw new Error("condition not met in time");
    await Bun.sleep(5);
  }
}

function previewHost(sandboxId = SANDBOX, port = PORT): string {
  return `p${port}-${sandboxSlug(sandboxId)}.${DOMAIN}`;
}

function startUpstream(fetchHandler: (req: Request) => Response | Promise<Response>): Bun.Server<unknown> {
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch: fetchHandler,
  });
  cleanups.push(() => server.stop(true));
  return server;
}

function makeGateway(opts: {
  endpoint: () => ExposedEndpoint;
  running?: () => boolean;
  expose?: (sandboxId: string, port: number) => Promise<ExposedEndpoint>;
  bootstrapTtlMs?: number;
  sessionTtlMs?: number;
  secureCookies?: boolean;
  edgeWorkerHost?: string;
  edgeWorkerSecret?: string;
}): { gateway: HubPortGateway; exposeCalls: Array<{ sandboxId: string; port: number }> } {
  const exposeCalls: Array<{ sandboxId: string; port: number }> = [];
  const gateway = new HubPortGateway({
    domain: DOMAIN,
    signingSecret: SECRET,
    isSandboxRunning: opts.running ?? (() => true),
    exposePort:
      opts.expose ??
      (async (sandboxId, port) => {
        exposeCalls.push({ sandboxId, port });
        return opts.endpoint();
      }),
    ...(opts.bootstrapTtlMs !== undefined ? { bootstrapTtlMs: opts.bootstrapTtlMs } : {}),
    ...(opts.sessionTtlMs !== undefined ? { sessionTtlMs: opts.sessionTtlMs } : {}),
    ...(opts.secureCookies !== undefined ? { secureCookies: opts.secureCookies } : {}),
    ...(opts.edgeWorkerHost ? { edgeWorkerHost: opts.edgeWorkerHost } : {}),
    ...(opts.edgeWorkerSecret ? { edgeWorkerSecret: opts.edgeWorkerSecret } : {}),
  });
  return { gateway, exposeCalls };
}

function startGateway(gateway: HubPortGateway): Bun.Server<unknown> {
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(req, srv) {
      return (await gateway.handleHttp(req, srv)) ?? new Response("unhandled", { status: 500 });
    },
    websocket: gateway.wsHandlers(),
  });
  cleanups.push(() => server.stop(true));
  return server;
}

function fetchGateway(
  server: Bun.Server<unknown>,
  host: string,
  pathAndQuery: string,
  init: RequestInit = {},
): Promise<Response> {
  const headers = new Headers(init.headers);
  headers.set("host", host);
  return fetch(`http://127.0.0.1:${server.port}${pathAndQuery}`, {
    ...init,
    headers,
    redirect: "manual",
  });
}

function tokenFromPreviewUrl(previewUrl: string): string {
  const token = new URL(previewUrl).searchParams.get(PREVIEW_BOOTSTRAP_PARAM);
  if (!token) throw new Error("preview URL has no bootstrap token");
  return token;
}

function sessionCookieFrom(setCookie: string | null): string {
  if (!setCookie) throw new Error("response did not set a session cookie");
  return setCookie.split(";")[0];
}

async function bootstrapSession(
  gateway: HubPortGateway,
  server: Bun.Server<unknown>,
  sandboxId = SANDBOX,
  port = PORT,
  pathAndQuery = "/",
): Promise<{ host: string; cookie: string; location: string | null; response: Response }> {
  const preview = new URL(gateway.buildPreviewUrl(sandboxId, port, pathAndQuery));
  const host = preview.host;
  const response = await fetchGateway(server, host, `${preview.pathname}${preview.search}`);
  const setCookie = response.headers.get("set-cookie");
  return {
    host,
    cookie: setCookie ? sessionCookieFrom(setCookie) : "",
    location: response.headers.get("location"),
    response,
  };
}

function tamperToken(token: string): string {
  const last = token.at(-1) ?? "A";
  return `${token.slice(0, -1)}${last === "A" ? "B" : "A"}`;
}

describe("port gateway pure helpers", () => {
  test("parseLoopbackPreviewUrl accepts loopback dev-server URLs and preserves path, query, and hash", () => {
    expect(parseLoopbackPreviewUrl("http://localhost:5173")).toEqual({ port: 5173, pathAndQuery: "/" });
    expect(parseLoopbackPreviewUrl("http://LOCALHOST:5173/app?x=1#frag")).toEqual({
      port: 5173,
      pathAndQuery: "/app?x=1#frag",
    });
    expect(parseLoopbackPreviewUrl("https://127.0.0.1:4443/secure")).toEqual({ port: 4443, pathAndQuery: "/secure" });
    expect(parseLoopbackPreviewUrl("http://0.0.0.0:3000/vite")).toEqual({ port: 3000, pathAndQuery: "/vite" });
    expect(parseLoopbackPreviewUrl("http://[::1]:8080/ipv6?x=1")).toEqual({ port: 8080, pathAndQuery: "/ipv6?x=1" });
    expect(parseLoopbackPreviewUrl("http://localhost:80/default-explicit")).toEqual({
      port: 80,
      pathAndQuery: "/default-explicit",
    });
  });

  test("parseLoopbackPreviewUrl rejects non-loopback URLs, missing or invalid ports, and T3's RPC port", () => {
    const invalid = [
      "http://localhost/",
      "http://localhost:0/",
      "http://localhost:65536/",
      "http://localhost:3773/",
      "https://example.com:5173/",
      "http://10.0.0.8:5173/",
      "ftp://localhost:5173/",
      "not-a-url",
    ];
    for (const raw of invalid) expect(parseLoopbackPreviewUrl(raw)).toBeNull();
  });

  test("sandboxSlug is a deterministic DNS-safe SHA-256 prefix", () => {
    expect(sandboxSlug("sandbox-abc")).toBe(createHash("sha256").update("sandbox-abc").digest("hex").slice(0, 10));
    expect(sandboxSlug("sandbox-abc")).toMatch(/^[0-9a-f]{10}$/);
    expect(sandboxSlug("sandbox-abc")).toBe(sandboxSlug("sandbox-abc"));
  });

  test("parseGatewayHost accepts only single-level preview subdomains with valid ports", () => {
    expect(parseGatewayHost("p5173-ab12.example.com", "example.com")).toEqual({
      port: 5173,
      slug: "ab12",
      hostname: "p5173-ab12.example.com",
    });
    expect(parseGatewayHost("P5173-ABC.EXAMPLE.COM:443", "example.com")).toEqual({
      port: 5173,
      slug: "abc",
      hostname: "p5173-abc.example.com",
    });
    expect(parseGatewayHost("p443-ab12.preview.example.com", "preview.example.com")?.port).toBe(443);

    const invalid = [
      "p0-ab12.example.com",
      "p65536-ab12.example.com",
      "p3773-ab12.example.com",
      "foo.p5173-ab12.example.com",
      "p5173-ab12.extra.example.com",
      "p5173-a_b.example.com",
      "p5173-.example.com",
      "p-ab12.example.com",
      "p5173-ab12.example.org",
    ];
    for (const host of invalid) expect(parseGatewayHost(host, "example.com")).toBeNull();
    expect(parseGatewayHost(null, "example.com")).toBeNull();
  });
});

describe("lazy exposure and HTTP proxying", () => {
  test("buildPreviewUrl is synchronous and lazy; concurrent authenticated requests share one exposePort call", async () => {
    const upstreamPaths: string[] = [];
    const upstream = startUpstream((req) => {
      const url = new URL(req.url);
      upstreamPaths.push(`${url.pathname}${url.search}`);
      return new Response(`ok:${url.pathname}`);
    });

    let releaseExpose: () => void = () => {};
    const exposeGate = new Promise<void>((resolve) => {
      releaseExpose = resolve;
    });
    const exposeCalls: Array<{ sandboxId: string; port: number }> = [];
    const { gateway } = makeGateway({
      endpoint: () => ({ url: `http://127.0.0.1:${upstream.port}` }),
      expose: async (sandboxId, port) => {
        exposeCalls.push({ sandboxId, port });
        await exposeGate;
        return { url: `http://127.0.0.1:${upstream.port}` };
      },
    });
    const server = startGateway(gateway);

    const previewUrl = gateway.buildPreviewUrl(SANDBOX, PORT, "/index.html?x=1#app");
    expect(previewUrl.startsWith(`https://${previewHost()}/index.html?x=1&${PREVIEW_BOOTSTRAP_PARAM}=`)).toBe(true);
    expect(previewUrl.indexOf(PREVIEW_BOOTSTRAP_PARAM)).toBeLessThan(previewUrl.indexOf("#app"));
    expect(exposeCalls).toHaveLength(0);

    const preview = new URL(previewUrl);
    const bootstrap = await fetchGateway(server, preview.host, `${preview.pathname}${preview.search}`);
    expect(bootstrap.status).toBe(302);
    expect(exposeCalls).toHaveLength(0);
    const cookie = sessionCookieFrom(bootstrap.headers.get("set-cookie"));

    const responsesPromise = Promise.all(
      Array.from({ length: 5 }, (_, i) =>
        fetchGateway(server, preview.host, `/asset-${i}.js?v=${i}`, { headers: { cookie } }),
      ),
    );
    await until(() => exposeCalls.length === 1);
    releaseExpose();
    const responses = await responsesPromise;

    expect(exposeCalls).toEqual([{ sandboxId: SANDBOX, port: PORT }]);
    expect(responses.map((res) => res.status)).toEqual([200, 200, 200, 200, 200]);
    expect(upstreamPaths).toHaveLength(5);
    expect(gateway.exposedCount()).toBe(1);
    expect(gateway.exposedCount(SANDBOX)).toBe(1);

    gateway.invalidateSandbox(SANDBOX);
    expect(gateway.exposedCount()).toBe(0);
  });

  test("invalidateSandbox drops an in-flight expose promise before it can populate the cache", async () => {
    const upstream = startUpstream(() => new Response("upstream"));
    let releaseExpose: () => void = () => {};
    const exposeGate = new Promise<void>((resolve) => {
      releaseExpose = resolve;
    });
    const exposeCalls: Array<{ sandboxId: string; port: number }> = [];
    const { gateway } = makeGateway({
      endpoint: () => ({ url: `http://127.0.0.1:${upstream.port}` }),
      expose: async (sandboxId, port) => {
        exposeCalls.push({ sandboxId, port });
        await exposeGate;
        return { url: `http://127.0.0.1:${upstream.port}` };
      },
    });
    const server = startGateway(gateway);
    const session = await bootstrapSession(gateway, server);
    expect(session.response.status).toBe(302);
    expect(exposeCalls).toHaveLength(0);

    const responsePromise = fetchGateway(server, session.host, "/", { headers: { cookie: session.cookie } });
    await until(() => exposeCalls.length === 1);
    gateway.invalidateSandbox(SANDBOX);
    expect(gateway.exposedCount()).toBe(0);
    releaseExpose();

    const res = await responsePromise;
    expect(res.status).toBe(502);
    expect(gateway.exposedCount()).toBe(0);
    const after = await fetchGateway(server, session.host, "/", { headers: { cookie: session.cookie } });
    expect(after.status).toBe(404);
  });

  test("bootstrap strips the token, mints a hardened session cookie, and forwarding merges cookies correctly", async () => {
    const seen: Array<Record<string, string | null>> = [];
    const upstream = startUpstream(async (req) => {
      seen.push({
        cookie: req.headers.get("cookie"),
        host: req.headers.get("host"),
        forwardedHost: req.headers.get("x-forwarded-host"),
        forwardedProto: req.headers.get("x-forwarded-proto"),
        origin: req.headers.get("origin"),
      });
      if (req.method === "POST") return new Response(`post:${await req.text()}`);
      return new Response("hello app", {
        headers: { "content-type": "text/plain", "set-cookie": "app=server; Path=/; HttpOnly" },
      });
    });
    const { gateway } = makeGateway({
      endpoint: () => ({ url: `http://127.0.0.1:${upstream.port}`, headers: { cookie: "boat-gate=secret" } }),
    });
    const server = startGateway(gateway);

    const { host, cookie, location, response } = await bootstrapSession(gateway, server, SANDBOX, PORT, "/app?x=1");
    expect(response.status).toBe(302);
    expect(location).toBe("/app?x=1");
    expect(location).not.toContain(PREVIEW_BOOTSTRAP_PARAM);

    const setCookie = response.headers.get("set-cookie")!;
    expect(setCookie).toContain(`${PREVIEW_COOKIE_NAME}=`);
    expect(setCookie).toContain("Path=/");
    expect(setCookie).toContain("HttpOnly");
    expect(setCookie).toContain("SameSite=None");
    expect(setCookie).toContain("Max-Age=28800");
    expect(setCookie).toContain("Secure");

    const res = await fetchGateway(server, host, "/app?x=1", {
      headers: { cookie: `${cookie}; app=client`, origin: `https://${host}` },
    });
    expect(res.status).toBe(200);
    expect(await res.text()).toBe("hello app");
    expect(res.headers.get("x-robots-tag")).toBe("noindex, nofollow, noarchive");
    expect(res.headers.get("x-frame-options")).toBeNull();
    expect(res.headers.get("content-security-policy")).toBeNull();
    expect(res.headers.get("set-cookie")).toContain("app=server");

    expect(seen).toHaveLength(1);
    expect(seen[0].cookie).toContain("app=client");
    expect(seen[0].cookie).toContain("boat-gate=secret");
    expect(seen[0].cookie).not.toContain(PREVIEW_COOKIE_NAME);
    expect(seen[0].host).toBe(`127.0.0.1:${upstream.port}`);
    expect(seen[0].forwardedHost).toBe(host);
    expect(seen[0].forwardedProto).toBe("https");
    expect(seen[0].origin).toBe(`http://127.0.0.1:${upstream.port}`);

    const post = await fetchGateway(server, host, "/submit", {
      method: "POST",
      headers: { cookie: `${cookie}; app=client`, "content-type": "text/plain" },
      body: "stream-body",
    });
    expect(post.status).toBe(200);
    expect(await post.text()).toBe("post:stream-body");
    expect(seen).toHaveLength(2);
    expect(seen[1].cookie).toContain("app=client");
    expect(seen[1].cookie).toContain("boat-gate=secret");
    expect(seen[1].cookie).not.toContain(PREVIEW_COOKIE_NAME);
  });

  test("robots.txt is served without a preview session", async () => {
    const upstream = startUpstream(() => new Response("upstream"));
    const { gateway } = makeGateway({ endpoint: () => ({ url: `http://127.0.0.1:${upstream.port}` }) });
    const server = startGateway(gateway);
    const res = await fetchGateway(server, previewHost(), "/robots.txt");
    expect(res.status).toBe(200);
    expect(await res.text()).toContain("Disallow: /");
    expect(res.headers.get("x-robots-tag")).toBe("noindex, nofollow, noarchive");
  });
});

describe("zero-trust preview authentication", () => {
  test("invalid, tampered, expired, wrong-port, and wrong-sandbox bootstrap tokens are rejected", async () => {
    const upstream = startUpstream(() => new Response("upstream"));
    const { gateway } = makeGateway({ endpoint: () => ({ url: `http://127.0.0.1:${upstream.port}` }) });
    const server = startGateway(gateway);
    const host = previewHost();

    const goodToken = tokenFromPreviewUrl(gateway.buildPreviewUrl(SANDBOX, PORT));
    const wrongPortToken = tokenFromPreviewUrl(gateway.buildPreviewUrl(SANDBOX, 9999));
    const wrongSandboxToken = tokenFromPreviewUrl(gateway.buildPreviewUrl("sb-other", PORT));

    const expiring = makeGateway({
      endpoint: () => ({ url: `http://127.0.0.1:${upstream.port}` }),
      bootstrapTtlMs: -1_000,
    }).gateway;
    const expiredToken = tokenFromPreviewUrl(expiring.buildPreviewUrl(SANDBOX, PORT));

    const invalidTokens = [
      "not-a-token",
      tamperToken(goodToken),
      expiredToken,
      wrongPortToken,
      wrongSandboxToken,
    ];
    for (const token of invalidTokens) {
      const res = await fetchGateway(server, host, `/?${PREVIEW_BOOTSTRAP_PARAM}=${encodeURIComponent(token)}`);
      expect(res.status).toBe(401);
    }
  });

  test("missing, invalid, tampered, expired, wrong-port, and wrong-sandbox session cookies are rejected", async () => {
    const upstream = startUpstream(() => new Response("upstream"));
    const { gateway } = makeGateway({ endpoint: () => ({ url: `http://127.0.0.1:${upstream.port}` }) });
    const server = startGateway(gateway);
    const host = previewHost();
    gateway.buildPreviewUrl(SANDBOX, PORT);

    const missing = await fetchGateway(server, host, "/");
    expect(missing.status).toBe(401);

    const session = await bootstrapSession(gateway, server);
    expect(session.response.status).toBe(302);
    const goodCookie = session.cookie;
    expect(goodCookie).toContain(`${PREVIEW_COOKIE_NAME}=`);

    const invalidCookies = [
      `${PREVIEW_COOKIE_NAME}=not-a-token`,
      tamperToken(goodCookie),
    ];
    for (const cookie of invalidCookies) {
      const res = await fetchGateway(server, host, "/", { headers: { cookie } });
      expect(res.status).toBe(401);
    }

    const wrongPort = await bootstrapSession(gateway, server, SANDBOX, 9999);
    expect(wrongPort.response.status).toBe(302);
    const wrongPortRes = await fetchGateway(server, host, "/", { headers: { cookie: wrongPort.cookie } });
    expect(wrongPortRes.status).toBe(401);

    const wrongSandbox = await bootstrapSession(gateway, server, "sb-other", PORT);
    expect(wrongSandbox.response.status).toBe(302);
    const wrongSandboxRes = await fetchGateway(server, host, "/", { headers: { cookie: wrongSandbox.cookie } });
    expect(wrongSandboxRes.status).toBe(401);

    const expiredGatewayRig = makeGateway({
      endpoint: () => ({ url: `http://127.0.0.1:${upstream.port}` }),
      sessionTtlMs: -1_000,
    });
    const expiredServer = startGateway(expiredGatewayRig.gateway);
    const expired = await bootstrapSession(expiredGatewayRig.gateway, expiredServer);
    expect(expired.response.status).toBe(302);
    const expiredRes = await fetchGateway(expiredServer, expired.host, "/", { headers: { cookie: expired.cookie } });
    expect(expiredRes.status).toBe(401);
  });

  test("a stopped or sleeping sandbox returns 503 before exposing its port", async () => {
    let running = true;
    const upstream = startUpstream(() => new Response("upstream"));
    const { gateway, exposeCalls } = makeGateway({
      endpoint: () => ({ url: `http://127.0.0.1:${upstream.port}` }),
      running: () => running,
    });
    const server = startGateway(gateway);
    const preview = new URL(gateway.buildPreviewUrl(SANDBOX, PORT));
    running = false;
    const res = await fetchGateway(server, preview.host, `${preview.pathname}${preview.search}`);
    expect(res.status).toBe(503);
    expect(await res.text()).toContain("Sandbox is not running");
    expect(exposeCalls).toHaveLength(0);
  });
});

describe("redirect rewriting", () => {
  test("loopback and upstream-origin Location headers are rewritten to the public preview origin", async () => {
    let redirectTarget = "";
    const upstream = startUpstream((req) => {
      const url = new URL(req.url);
      if (url.pathname === "/go") {
        return new Response(null, { status: 302, headers: { location: redirectTarget } });
      }
      return new Response("ok");
    });
    const { gateway } = makeGateway({ endpoint: () => ({ url: `http://127.0.0.1:${upstream.port}` }) });
    const server = startGateway(gateway);
    const session = await bootstrapSession(gateway, server);
    expect(session.response.status).toBe(302);

    const cases: Array<{ input: string; expected: string }> = [
      { input: "http://localhost:5173/login", expected: `https://${session.host}/login` },
      { input: "http://127.0.0.1:5173/deep?x=1#frag", expected: `https://${session.host}/deep?x=1#frag` },
      { input: "http://0.0.0.0:5173/zero", expected: `https://${session.host}/zero` },
      { input: `http://127.0.0.1:${upstream.port}/upstream`, expected: `https://${session.host}/upstream` },
      { input: "https://example.com/external", expected: "https://example.com/external" },
    ];

    for (const { input, expected } of cases) {
      redirectTarget = input;
      const res = await fetchGateway(server, session.host, "/go", { headers: { cookie: session.cookie } });
      expect(res.status).toBe(302);
      expect(res.headers.get("location")).toBe(expected);
    }
  });
});

describe("WebSocket HMR proxying", () => {
  test("forwards vite-hmr subprotocols, text and binary frames, and disconnects immediately on invalidation", async () => {
    let upstreamSocket: Bun.ServerWebSocket<unknown> | null = null;
    let upstreamClose: { code: number; reason: string } | null = null;
    const upstreamHandshake: Record<string, string | null> = {};
    const upstream = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch(req, srv) {
        upstreamHandshake.protocol = req.headers.get("sec-websocket-protocol");
        upstreamHandshake.cookie = req.headers.get("cookie");
        upstreamHandshake.origin = req.headers.get("origin");
        return srv.upgrade(req) ? undefined : new Response("upgrade failed", { status: 400 });
      },
      websocket: {
        open(ws) {
          upstreamSocket = ws;
        },
        message(ws, msg) {
          if (typeof msg === "string") ws.sendText(msg);
          else ws.sendBinary(msg);
        },
        close(_ws, code, reason) {
          upstreamClose = { code, reason };
        },
      },
    });
    cleanups.push(() => upstream.stop(true));

    const { gateway, exposeCalls } = makeGateway({
      endpoint: () => ({ url: `http://127.0.0.1:${upstream.port}`, headers: { cookie: "boat-gate=secret" } }),
    });
    const server = startGateway(gateway);
    const session = await bootstrapSession(gateway, server);
    expect(session.response.status).toBe(302);
    expect(exposeCalls).toHaveLength(0);

    const ws = new WebSocket(`ws://127.0.0.1:${server.port}/hmr`, {
      headers: {
        host: session.host,
        cookie: `${session.cookie}; app=client`,
        origin: `https://${session.host}`,
      },
      protocols: ["vite-hmr"],
    } as unknown as string[]);
    ws.binaryType = "arraybuffer";
    cleanups.push(() => ws.close());

    const closePromise = new Promise<{ code: number; reason: string }>((resolve) => {
      ws.addEventListener("close", (ev) => resolve({ code: ev.code, reason: ev.reason }));
    });
    await new Promise<void>((resolve, reject) => {
      ws.addEventListener("open", () => resolve());
      ws.addEventListener("error", () => reject(new Error("client websocket failed")));
    });
    await until(() => upstreamSocket !== null);

    expect(exposeCalls).toEqual([{ sandboxId: SANDBOX, port: PORT }]);
    expect(upstreamHandshake.protocol).toContain("vite-hmr");
    expect(ws.protocol).toBe("vite-hmr");
    expect(upstreamHandshake.cookie).toContain("boat-gate=secret");
    expect(upstreamHandshake.cookie).toContain("app=client");
    expect(upstreamHandshake.cookie).not.toContain(PREVIEW_COOKIE_NAME);
    expect(upstreamHandshake.origin).toBe(`http://127.0.0.1:${upstream.port}`);

    const nextMessage = () =>
      new Promise<string | Uint8Array>((resolve) => {
        ws.addEventListener(
          "message",
          (ev) => {
            resolve(typeof ev.data === "string" ? ev.data : new Uint8Array(ev.data as ArrayBuffer));
          },
          { once: true },
        );
      });

    const textReply = nextMessage();
    ws.send("hmr-update");
    expect(await textReply).toBe("hmr-update");

    const binaryReply = nextMessage();
    ws.send(new Uint8Array([1, 2, 3, 255]));
    expect(Array.from((await binaryReply) as Uint8Array)).toEqual([1, 2, 3, 255]);

    gateway.invalidateSandbox(SANDBOX);
    const closed = await closePromise;
    expect([1000, 1001]).toContain(closed.code);
    expect(closed.reason).toBe("Sandbox stopped");
    await until(() => upstreamClose !== null);
    expect(upstreamClose!.code).toBe(1001);

    const afterInvalidate = await fetchGateway(server, session.host, "/", { headers: { cookie: session.cookie } });
    expect(afterInvalidate.status).toBe(404);
  });
});

describe("Cloudflare edge worker handoff and AES-256-GCM sealing", () => {
  test("sealEdgeTarget and unsealEdgeTarget round-trip payloads and reject tampering or expiry", async () => {
    const payload = {
      u: "https://sandbox-123.upstream.example.com",
      h: { cookie: "gate=secret", authorization: "Bearer token" },
      s: "sandbox-123",
      p: 5173,
      host: "p5173-0123456789.edge.example.com",
      exp: 2_000_000_000_000,
    };
    const sealed = await sealEdgeTarget(payload, EDGE_SECRET);
    expect(sealed).toMatch(/^v1\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/);
    expect(await unsealEdgeTarget(sealed, EDGE_SECRET, 1_000)).toEqual(payload);

    const parts = sealed.split(".");
    const first = parts[2][0] ?? "A";
    const tamperedCiphertext = `${first === "A" ? "B" : "A"}${parts[2].slice(1)}`;
    expect(await unsealEdgeTarget(`${parts[0]}.${parts[1]}.${tamperedCiphertext}`, EDGE_SECRET, 1_000)).toBeNull();
    expect(await unsealEdgeTarget(sealed, "wrong-secret", 1_000)).toBeNull();
    expect(await unsealEdgeTarget(sealed, EDGE_SECRET, payload.exp)).toBeNull();
    expect(await unsealEdgeTarget("v1.bad", EDGE_SECRET, 1_000)).toBeNull();
  });

  test("bootstrap redirects to the edge worker with a sealed target when edge mode is configured", async () => {
    const upstream = startUpstream(() => new Response("upstream"));
    const endpoint = {
      url: `http://127.0.0.1:${upstream.port}`,
      headers: { cookie: "boat-gate=secret" },
    };
    const { gateway, exposeCalls } = makeGateway({
      endpoint: () => endpoint,
      edgeWorkerHost: "edge.example.com",
      edgeWorkerSecret: EDGE_SECRET,
    });
    const server = startGateway(gateway);

    const preview = new URL(gateway.buildPreviewUrl(SANDBOX, PORT, "/app?x=1"));
    expect(exposeCalls).toHaveLength(0);
    const res = await fetchGateway(server, preview.host, `${preview.pathname}${preview.search}`);
    expect(res.status).toBe(302);
    expect(exposeCalls).toEqual([{ sandboxId: SANDBOX, port: PORT }]);
    expect(res.headers.get("set-cookie")).toBeNull();

    const location = res.headers.get("location");
    expect(location).toBeTruthy();
    const redirect = new URL(location!);
    const slug = sandboxSlug(SANDBOX);
    expect(redirect.protocol).toBe("https:");
    expect(redirect.host).toBe(`p${PORT}-${slug}.edge.example.com`);
    expect(redirect.pathname).toBe("/app");
    expect(redirect.searchParams.get("x")).toBe("1");
    expect(redirect.searchParams.get(PREVIEW_BOOTSTRAP_PARAM)).toBeNull();
    const sealed = redirect.searchParams.get(PREVIEW_EDGE_PARAM);
    expect(sealed).toBeTruthy();

    const unsealed = await unsealEdgeTarget(sealed!, EDGE_SECRET);
    expect(unsealed).not.toBeNull();
    expect(unsealed!.u).toBe(endpoint.url);
    expect(unsealed!.h).toEqual(endpoint.headers);
    expect(unsealed!.s).toBe(SANDBOX);
    expect(unsealed!.p).toBe(PORT);
    expect(unsealed!.host).toBe(`p${PORT}-${slug}.edge.example.com`);
    expect(unsealed!.exp).toBeGreaterThan(Date.now());
  });

  test("single-domain edge mode routes __t3p bootstrap via hub.host and redirects cleanly to p<port>-<slug>.<domain>", async () => {
    const upstream = startUpstream(() => new Response("upstream"));
    const endpoint = {
      url: `http://127.0.0.1:${upstream.port}`,
      headers: { cookie: "boat-gate=secret" },
    };
    const exposeCalls: Array<{ sandboxId: string; port: number }> = [];
    const gateway = new HubPortGateway({
      domain: "example.com",
      bootstrapHost: "boat.example.com",
      signingSecret: SECRET,
      exposePort: async (sandboxId, port) => {
        exposeCalls.push({ sandboxId, port });
        return endpoint;
      },
      isSandboxRunning: () => true,
      edgeWorkerHost: "example.com",
      edgeWorkerSecret: EDGE_SECRET,
      secureCookies: false,
    });

    const preview = new URL(gateway.buildPreviewUrl(SANDBOX, PORT, "/app?x=1#section"));
    expect(exposeCalls).toHaveLength(0);
    expect(preview.host).toBe("boat.example.com");
    expect(preview.pathname).toBe("/api/preview-port/bootstrap");
    expect(preview.hash).toBe("#section");

    const res = await gateway.handleHubBootstrap(new Request(preview.toString()));
    expect(res.status).toBe(302);
    expect(exposeCalls).toEqual([{ sandboxId: SANDBOX, port: PORT }]);

    const redirect = new URL(res.headers.get("location")!);
    const slug = sandboxSlug(SANDBOX);
    expect(redirect.host).toBe(`p${PORT}-${slug}.example.com`);
    expect(redirect.pathname).toBe("/app");
    expect(redirect.searchParams.get("x")).toBe("1");
    const sealed = redirect.searchParams.get(PREVIEW_EDGE_PARAM)!;
    const unsealed = await unsealEdgeTarget(sealed, EDGE_SECRET);
    expect(unsealed?.host).toBe(`p${PORT}-${slug}.example.com`);
    expect(unsealed?.u).toBe(endpoint.url);

    // Open-redirect guard on `to`
    const badTo = new URL(preview.toString());
    badTo.searchParams.set("to", "//evil.example/steal");
    expect((await gateway.handleHubBootstrap(new Request(badTo.toString()))).status).toBe(400);
  });

  test("security hardening: open-redirect // collapse, stale __t3p cookie fallback, Set-Cookie Domain strip, and session epoch revocation", async () => {
    const upstream = startUpstream(() => {
      const headers = new Headers();
      headers.append("set-cookie", "app=val; Path=/; Domain=preview.test; HttpOnly");
      headers.append("set-cookie", `${PREVIEW_COOKIE_NAME}=hijack; Path=/`);
      headers.append("set-cookie", "_port_auth=leak; Path=/");
      return new Response("ok", { headers });
    });
    const { gateway } = makeGateway({
      endpoint: () => ({ url: `http://127.0.0.1:${upstream.port}` }),
    });
    const server = startGateway(gateway);

    // 1. Open-redirect //evil.com collapse + percent-encoded param strip
    const bootUrl = new URL(gateway.buildPreviewUrl(SANDBOX, PORT, "/"));
    const token = bootUrl.searchParams.get(PREVIEW_BOOTSTRAP_PARAM)!;
    const slashRes = await fetchGateway(server, bootUrl.host, `//evil.example/x?%5F%5Ft3p=${encodeURIComponent(token)}`);
    expect(slashRes.status).toBe(302);
    expect(slashRes.headers.get("location")).toBe("/evil.example/x");
    const cookie = slashRes.headers.get("set-cookie")!.split(";")[0]!;

    // 2. Stale/expired __t3p falls back to valid session cookie (302 clean redirect, not 401)
    const staleRes = await fetchGateway(server, bootUrl.host, `/dash?${PREVIEW_BOOTSTRAP_PARAM}=expired.bad`, {
      headers: { cookie },
    });
    expect(staleRes.status).toBe(302);
    expect(staleRes.headers.get("location")).toBe("/dash");

    // 3. Upstream Set-Cookie Domain is stripped and reserved cookies are dropped
    const appRes = await fetchGateway(server, bootUrl.host, "/dash", { headers: { cookie } });
    expect(appRes.status).toBe(200);
    expect(appRes.headers.getSetCookie()).toEqual(["app=val; Path=/; HttpOnly"]);

    // 4. Refreshing a stale gateway URL mints a fresh bootstrap token
    const refreshed = gateway.refreshPreviewUrl(`https://${bootUrl.host}/dash?${PREVIEW_BOOTSTRAP_PARAM}=old`, SANDBOX);
    expect(refreshed).toBeTruthy();
    expect(new URL(refreshed!).searchParams.get(PREVIEW_BOOTSTRAP_PARAM)).not.toBe("old");

    // 5. Invalidate + re-register (sleep -> wake) revokes old 8h session cookie via epoch rotation
    gateway.invalidateSandbox(SANDBOX);
    gateway.buildPreviewUrl(SANDBOX, PORT, "/");
    const afterWake = await fetchGateway(server, bootUrl.host, "/dash", { headers: { cookie } });
    expect(afterWake.status).toBe(401);
  });
});
