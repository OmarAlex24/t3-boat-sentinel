import { afterAll, beforeAll, beforeEach, describe, expect, test } from "bun:test";
import worker, { sealEdgeTarget, unsealEdgeTarget, type PreviewEdgeEnv, type SealedEdgePayload } from "./index.ts";

const SECRET = "preview-edge-test-secret-at-least-32-bytes";
const PREVIEW_HOST = "p5173-a1b2c3d4e5.example.com";
const PREVIEW_ORIGIN = `https://${PREVIEW_HOST}`;

interface SeenUpstreamRequest {
  method: string;
  url: string;
  cookie: string | null;
  gateHeader: string | null;
  host: string | null;
  origin: string | null;
  contentType: string | null;
  upgrade: string | null;
  websocketProtocol: string | null;
  websocketExtensions: string | null;
  forwardedHost: string | null;
  forwardedProto: string | null;
}

let upstreamServer: ReturnType<typeof Bun.serve>;
let seenUpstream: SeenUpstreamRequest[];

function upstreamBase(): string {
  return `http://127.0.0.1:${upstreamServer.port}`;
}

function recordUpstream(req: Request): SeenUpstreamRequest {
  return {
    method: req.method,
    url: req.url,
    cookie: req.headers.get("cookie"),
    gateHeader: req.headers.get("x-boat-gate"),
    host: req.headers.get("host"),
    origin: req.headers.get("origin"),
    contentType: req.headers.get("content-type"),
    upgrade: req.headers.get("upgrade"),
    websocketProtocol: req.headers.get("sec-websocket-protocol"),
    websocketExtensions: req.headers.get("sec-websocket-extensions"),
    forwardedHost: req.headers.get("x-forwarded-host"),
    forwardedProto: req.headers.get("x-forwarded-proto"),
  };
}

beforeAll(() => {
  upstreamServer = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(req) {
      seenUpstream.push(recordUpstream(req));
      const url = new URL(req.url);
      if (url.pathname === "/redirect") {
        const location = url.searchParams.get("to") ?? "http://localhost:5173/dash";
        return new Response(null, { status: 302, headers: { location } });
      }
      if (url.pathname === "/echo") {
        const body = await req.text();
        return new Response(body || "upstream-ok", { headers: { "content-type": "text/plain" } });
      }
      return new Response("upstream-default", { headers: { "content-type": "text/plain" } });
    },
  });
});

beforeEach(() => {
  seenUpstream = [];
});

afterAll(() => {
  upstreamServer.stop(true);
});

function testEnv(overrides: Partial<PreviewEdgeEnv> = {}): PreviewEdgeEnv {
  return {
    PREVIEW_EDGE_SECRET: SECRET,
    ALLOW_HTTP_UPSTREAM: "1",
    ...overrides,
  };
}

function basePayload(overrides: Partial<SealedEdgePayload> = {}): SealedEdgePayload {
  return {
    u: upstreamBase(),
    h: {
      cookie: "_port_auth=boat-gate",
      "x-boat-gate": "yes",
    },
    s: "sandbox-a1b2c3d4e5",
    p: 5173,
    host: PREVIEW_HOST,
    exp: Date.now() + 60_000,
    ...overrides,
  };
}

async function tokenFor(overrides: Partial<SealedEdgePayload> = {}): Promise<{ payload: SealedEdgePayload; token: string }> {
  const payload = basePayload(overrides);
  return { payload, token: await sealEdgeTarget(payload, SECRET) };
}

async function expectUnauthorized(response: Response): Promise<void> {
  expect(response.status).toBe(401);
  expect(await response.text()).toBe("Unauthorized");
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(response.headers.get("content-type")).toContain("text/plain");
  expect(response.headers.get("x-robots-tag")).toBe("noindex, nofollow, noarchive");
}

describe("sealed edge tokens", () => {
  test("sealEdgeTarget and unsealEdgeTarget round-trip the exact payload", async () => {
    const { payload, token } = await tokenFor();
    expect(token.startsWith("v1.")).toBe(true);
    expect(token.split(".")).toHaveLength(3);
    expect(await unsealEdgeTarget(token, SECRET)).toEqual(payload);
    expect(await unsealEdgeTarget(token, "wrong-secret")).toBeNull();
  });
});

describe("unauthenticated endpoints and bootstrap", () => {
  test("/robots.txt disallows indexing without requiring authentication", async () => {
    const response = await worker.fetch(new Request(`${PREVIEW_ORIGIN}/robots.txt`), testEnv());
    expect(response.status).toBe(200);
    expect(await response.text()).toBe("User-agent: *\nDisallow: /\n");
    expect(response.headers.get("x-robots-tag")).toBe("noindex, nofollow, noarchive");
  });

  test("valid ?__t3e bootstrap strips the token, redirects, and sets a hardened cookie", async () => {
    const { token } = await tokenFor();
    const response = await worker.fetch(
      new Request(`${PREVIEW_ORIGIN}/some/path?foo=bar&encoded=a%2Fb%26c&plus=a+b&__t3e=${token}&x=1`),
      testEnv(),
    );

    expect(response.status).toBe(302);
    const location = response.headers.get("location");
    expect(location).toBe(`${PREVIEW_ORIGIN}/some/path?foo=bar&encoded=a%2Fb%26c&plus=a+b&x=1`);
    expect(location).not.toContain("__t3e");

    const setCookie = response.headers.get("set-cookie");
    expect(setCookie).toContain(`__t3_edge=${token}`);
    expect(setCookie).toContain("Path=/");
    expect(setCookie).toContain("HttpOnly");
    expect(setCookie).toContain("Secure");
    expect(setCookie).toContain("SameSite=None");
    const maxAge = Number(/(?:^|;\s*)Max-Age=(\d+)/.exec(setCookie ?? "")?.[1]);
    expect(maxAge).toBeGreaterThan(0);
    expect(maxAge).toBeLessThanOrEqual(60);
    expect(response.headers.get("x-robots-tag")).toBe("noindex, nofollow, noarchive");
  });

  test("strips Domain from upstream Set-Cookie and drops reserved gateway/gate cookies", async () => {
    const srv = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch() {
        const headers = new Headers();
        headers.append("set-cookie", "app_sid=ok; Path=/; Domain=example.com; HttpOnly");
        headers.append("set-cookie", "__t3_edge=hijack; Path=/");
        headers.append("set-cookie", "_port_auth=leaked; Path=/");
        return new Response("ok", { headers });
      },
    });
    try {
      const { token } = await tokenFor({ u: `http://127.0.0.1:${srv.port}` });
      const response = await worker.fetch(
        new Request(`${PREVIEW_ORIGIN}/`, { headers: { cookie: `__t3_edge=${token}` } }),
        testEnv(),
      );
      expect(response.status).toBe(200);
      const setCookies = response.headers.getSetCookie();
      expect(setCookies).toEqual(["app_sid=ok; Path=/; HttpOnly"]);
    } finally {
      srv.stop(true);
    }
  });

  test("wrong-host replay is rejected", async () => {
    const { token } = await tokenFor({ host: "p5173-otherhost99.example.com" });
    const response = await worker.fetch(new Request(`${PREVIEW_ORIGIN}/?__t3e=${token}`), testEnv());
    await expectUnauthorized(response);
  });

  test("tampered token is rejected", async () => {
    const { token } = await tokenFor();
    const parts = token.split(".");
    // Flip a middle character of the ciphertext (never the last char, whose trailing padding bits are ignored by base64url decode)
    const ct = parts[2];
    const mid = Math.floor(ct.length / 2);
    const flipped = ct[mid] === "A" ? "B" : "A";
    const tampered = `${parts[0]}.${parts[1]}.${ct.slice(0, mid)}${flipped}${ct.slice(mid + 1)}`;
    const response = await worker.fetch(new Request(`${PREVIEW_ORIGIN}/?__t3e=${tampered}`), testEnv());
    await expectUnauthorized(response);
  });

  test("stale __t3e falls back to a valid edge cookie and collapses // open-redirect paths", async () => {
    const { token: validToken } = await tokenFor();
    const { token: expiredToken } = await tokenFor({ exp: Date.now() - 1 });
    const response = await worker.fetch(
      new Request(`${PREVIEW_ORIGIN}//evil.example/path?__t3e=${expiredToken}&keep=1`, {
        headers: { cookie: `__t3_edge=${validToken}` },
      }),
      testEnv(),
    );
    expect(response.status).toBe(302);
    expect(response.headers.get("location")).toBe(`${PREVIEW_ORIGIN}/evil.example/path?keep=1`);
  });

  test("non-preview hostnames pass through to origin instead of 401", async () => {
    const originalFetch = globalThis.fetch;
    let passedUrl = "";
    globalThis.fetch = (async (input: RequestInfo | URL) => {
      passedUrl = input instanceof Request ? input.url : String(input);
      return new Response("hub-origin", { status: 200 });
    }) as typeof fetch;
    try {
      const response = await worker.fetch(new Request("https://boat.example.com/.well-known/t3/environment"), testEnv());
      expect(response.status).toBe(200);
      expect(await response.text()).toBe("hub-origin");
      expect(passedUrl).toBe("https://boat.example.com/.well-known/t3/environment");
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("expired token is rejected", async () => {
    const { token } = await tokenFor({ exp: Date.now() - 1 });
    const response = await worker.fetch(new Request(`${PREVIEW_ORIGIN}/?__t3e=${token}`), testEnv());
    await expectUnauthorized(response);
  });

  test("http upstream is rejected when ALLOW_HTTP_UPSTREAM is unset, while https remains valid", async () => {
    const { token } = await tokenFor();
    const response = await worker.fetch(
      new Request(`${PREVIEW_ORIGIN}/?__t3e=${token}`),
      testEnv({ ALLOW_HTTP_UPSTREAM: undefined }),
    );
    await expectUnauthorized(response);

    const { token: httpsToken } = await tokenFor({ u: "https://upstream.boat.example" });
    const httpsResponse = await worker.fetch(
      new Request(`${PREVIEW_ORIGIN}/?__t3e=${httpsToken}`),
      testEnv({ ALLOW_HTTP_UPSTREAM: undefined }),
    );
    expect(httpsResponse.status).toBe(302);
  });

  test("missing or invalid edge cookie is rejected before proxying", async () => {
    const missing = await worker.fetch(new Request(`${PREVIEW_ORIGIN}/echo`), testEnv());
    await expectUnauthorized(missing);

    const invalid = await worker.fetch(
      new Request(`${PREVIEW_ORIGIN}/echo`, { headers: { cookie: "__t3_edge=not-a-token" } }),
      testEnv(),
    );
    await expectUnauthorized(invalid);
    expect(seenUpstream).toHaveLength(0);
  });
});

describe("authenticated HTTP proxying", () => {
  test("merges upstream gate and app cookies, strips __t3_edge, and rewrites localhost redirects", async () => {
    const { token } = await tokenFor();
    const response = await worker.fetch(
      new Request(`${PREVIEW_ORIGIN}/redirect?to=${encodeURIComponent("http://localhost:5173/dash")}`, {
        headers: {
          cookie: `app_session=abc123; __t3_edge=${token}; theme=dark`,
          origin: PREVIEW_ORIGIN,
        },
      }),
      testEnv(),
    );

    expect(response.status).toBe(302);
    expect(response.headers.get("location")).toBe(`${PREVIEW_ORIGIN}/dash`);
    expect(response.headers.get("x-robots-tag")).toBe("noindex, nofollow, noarchive");

    expect(seenUpstream).toHaveLength(1);
    const upstream = seenUpstream[0];
    expect(upstream.cookie).toBe("_port_auth=boat-gate; app_session=abc123; theme=dark");
    expect(upstream.cookie).not.toContain("__t3_edge");
    expect(upstream.gateHeader).toBe("yes");
    expect(upstream.forwardedHost).toBe(PREVIEW_HOST);
    expect(upstream.forwardedProto).toBe("https");
    expect(upstream.origin).toBe(upstreamBase());
    expect(new URL(upstream.url).pathname).toBe("/redirect");
  });

  test("forwards POST bodies, methods, and content type", async () => {
    const { token } = await tokenFor();
    const response = await worker.fetch(
      new Request(`${PREVIEW_ORIGIN}/echo`, {
        method: "POST",
        headers: { cookie: `__t3_edge=${token}`, "content-type": "application/json" },
        body: JSON.stringify({ ok: true }),
      }),
      testEnv(),
    );

    expect(response.status).toBe(200);
    expect(await response.text()).toBe('{"ok":true}');
    expect(seenUpstream).toHaveLength(1);
    expect(seenUpstream[0].method).toBe("POST");
    expect(seenUpstream[0].contentType).toBe("application/json");
  });

  test("rewrites redirects to 127.0.0.1, 0.0.0.0, and the upstream origin", async () => {
    const locations = [
      "http://127.0.0.1:5173/loopback",
      "http://0.0.0.0:5173/wildcard",
      `${upstreamBase()}/upstream`,
    ];

    for (const location of locations) {
      seenUpstream = [];
      const { token } = await tokenFor();
      const response = await worker.fetch(
        new Request(`${PREVIEW_ORIGIN}/redirect?to=${encodeURIComponent(location)}`, {
          headers: { cookie: `__t3_edge=${token}` },
        }),
        testEnv(),
      );
      expect(response.status).toBe(302);
      expect(response.headers.get("location")).toBe(
        `${PREVIEW_ORIGIN}${new URL(location).pathname}`,
      );
    }
  });

  test("does not rewrite redirects to unrelated external origins", async () => {
    const external = "https://example.org/external";
    const { token } = await tokenFor();
    const response = await worker.fetch(
      new Request(`${PREVIEW_ORIGIN}/redirect?to=${encodeURIComponent(external)}`, {
        headers: { cookie: `__t3_edge=${token}` },
      }),
      testEnv(),
    );
    expect(response.status).toBe(302);
    expect(response.headers.get("location")).toBe(external);
  });
});

describe("WebSocket proxying", () => {
  test("forwards Upgrade, vite-hmr subprotocol, extensions, and upstream gate cookie", async () => {
    const { token } = await tokenFor();
    const response = await worker.fetch(
      new Request(`${PREVIEW_ORIGIN}/hmr`, {
        headers: {
          cookie: `__t3_edge=${token}; app_session=ws-app`,
          connection: "Upgrade",
          upgrade: "websocket",
          "sec-websocket-key": "dGhlIHNhbXBsZSBub25jZQ==",
          "sec-websocket-version": "13",
          "sec-websocket-protocol": "vite-hmr",
          "sec-websocket-extensions": "permessage-deflate; client_max_window_bits",
          origin: PREVIEW_ORIGIN,
        },
      }),
      testEnv(),
    );

    expect(response.status).toBe(200);
    expect(seenUpstream).toHaveLength(1);
    const upstream = seenUpstream[0];
    expect(upstream.upgrade).toBe("websocket");
    expect(upstream.websocketProtocol).toBe("vite-hmr");
    expect(upstream.websocketExtensions).toBe("permessage-deflate; client_max_window_bits");
    expect(upstream.cookie).toBe("_port_auth=boat-gate; app_session=ws-app");
    expect(upstream.cookie).not.toContain("__t3_edge");
    expect(upstream.origin).toBe(upstreamBase());
  });
});
