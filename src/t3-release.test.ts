import { describe, expect, test } from "bun:test";
import { T3ReleaseResolver, t3TagPattern } from "./t3-release.ts";

type Call = { url: string; headers: Record<string, string> };

function fakeGithub(routes: (url: string) => Response | Promise<Response>) {
  const calls: Call[] = [];
  const fetch = async (url: string, init: RequestInit) => {
    calls.push({ url, headers: { ...(init.headers as Record<string, string>) } });
    return routes(url);
  };
  return { calls, fetch };
}

const json = (body: unknown, status = 200, headers: Record<string, string> = {}) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json", ...headers } });

const RELEASES_PAGE_1 = [
  { tag_name: "v0.1.13-nightly.20261125.5300", draft: true },
  { tag_name: "v0.1.12-preview.20261124.7", draft: false },
  { tag_name: "v0.1.12-nightly.20261124.5209", draft: false },
  { tag_name: "v0.1.12-nightly.20261124.5204", draft: false },
  { tag_name: "v0.1.11", draft: false },
];

describe("t3TagPattern", () => {
  test("matches the installer's tags per channel", () => {
    expect(t3TagPattern("stable").exec("v0.1.11")?.[1]).toBe("0.1.11");
    expect(t3TagPattern("stable").exec("v0.1.12-nightly.20261124.5209")).toBeNull();
    expect(t3TagPattern("nightly").exec("v0.1.12-nightly.20261124.5209")?.[1]).toBe("0.1.12-nightly.20261124.5209");
    expect(t3TagPattern("nightly").exec("v0.1.12-preview.20261124.7")).toBeNull();
    expect(t3TagPattern("nightly").exec("v0.1.11")).toBeNull();
    expect(t3TagPattern("preview").exec("v0.1.12-preview.20261124.7")?.[1]).toBe("0.1.12-preview.20261124.7");
    expect(() => t3TagPattern("beta")).toThrow(/channel/);
  });
});

describe("T3ReleaseResolver", () => {
  test("passes exact versions through without a lookup", async () => {
    const gh = fakeGithub(() => json([]));
    const resolver = new T3ReleaseResolver({ fetch: gh.fetch, env: {} });
    expect(await resolver.resolve("0.1.10", "nightly")).toBe("0.1.10");
    expect(gh.calls).toHaveLength(0);
  });

  test("nightly: first non-draft nightly tag in API order, with the token when set", async () => {
    const gh = fakeGithub(() => json(RELEASES_PAGE_1));
    const resolver = new T3ReleaseResolver({ fetch: gh.fetch, env: { GITHUB_TOKEN: "ghp_test" } });
    expect(await resolver.resolve("latest", "nightly")).toBe("0.1.12-nightly.20261124.5209");
    expect(gh.calls).toHaveLength(1);
    expect(gh.calls[0].url).toBe("https://api.github.com/repos/pingdotgg/t3code/releases?per_page=100&page=1");
    expect(gh.calls[0].headers.Authorization).toBe("Bearer ghp_test");
  });

  test("no Authorization header without GITHUB_TOKEN", async () => {
    const gh = fakeGithub(() => json(RELEASES_PAGE_1));
    const resolver = new T3ReleaseResolver({ fetch: gh.fetch, env: {} });
    expect(await resolver.resolve("latest", "preview")).toBe("0.1.12-preview.20261124.7");
    expect(gh.calls[0].headers.Authorization).toBeUndefined();
  });

  test("pages until a match and stops at an empty page", async () => {
    const gh = fakeGithub((url) => {
      const page = Number(new URL(url).searchParams.get("page"));
      if (page === 1) return json([{ tag_name: "v0.1.11" }]);
      if (page === 2) return json([{ tag_name: "v0.1.10-nightly.20261001.100" }]);
      return json([]);
    });
    const resolver = new T3ReleaseResolver({ fetch: gh.fetch, env: {}, log: () => {} });
    expect(await resolver.resolve("latest", "nightly")).toBe("0.1.10-nightly.20261001.100");
    expect(gh.calls).toHaveLength(2);

    const none = fakeGithub((url) => (new URL(url).searchParams.get("page") === "1" ? json([{ tag_name: "v0.1.11" }]) : json([])));
    const logs: string[] = [];
    const r2 = new T3ReleaseResolver({ fetch: none.fetch, env: {}, log: (m) => logs.push(m) });
    expect(await r2.resolve("latest", "nightly")).toBe("latest");
    expect(none.calls).toHaveLength(2);
    expect(logs[0]).toContain("no nightly release found");
  });

  test("stable reads github.com/releases/latest, not the API", async () => {
    const gh = fakeGithub(() => json({ tag_name: "v0.1.11" }));
    const resolver = new T3ReleaseResolver({ fetch: gh.fetch, env: { GITHUB_TOKEN: "ghp_test" } });
    expect(await resolver.resolve("latest", "stable")).toBe("0.1.11");
    expect(gh.calls[0].url).toBe("https://github.com/pingdotgg/t3code/releases/latest");
    expect(gh.calls[0].headers.Accept).toBe("application/json");
    expect(gh.calls[0].headers.Authorization).toBeUndefined();
  });

  test("caches per channel for the TTL and shares one in-flight lookup", async () => {
    let now = 1_000_000;
    let release: (r: Response) => void = () => {};
    const gh = fakeGithub(() => new Promise<Response>((res) => (release = res)));
    const resolver = new T3ReleaseResolver({ fetch: gh.fetch, env: {}, now: () => now, ttlMs: 15 * 60_000 });

    const concurrent = [resolver.resolve("latest", "nightly"), resolver.resolve("latest", "nightly"), resolver.resolve("latest", "nightly")];
    await Bun.sleep(0);
    expect(gh.calls).toHaveLength(1);
    release(json(RELEASES_PAGE_1));
    expect(await Promise.all(concurrent)).toEqual(Array(3).fill("0.1.12-nightly.20261124.5209"));

    now += 14 * 60_000;
    expect(await resolver.resolve("latest", "nightly")).toBe("0.1.12-nightly.20261124.5209");
    expect(gh.calls).toHaveLength(1);

    now += 2 * 60_000;
    const again = resolver.resolve("latest", "nightly");
    await Bun.sleep(0);
    expect(gh.calls).toHaveLength(2);
    release(json([{ tag_name: "v0.1.12-nightly.20261125.5300" }]));
    expect(await again).toBe("0.1.12-nightly.20261125.5300");
  });

  test("falls back to latest on errors and retries on the next call", async () => {
    let fail = true;
    const gh = fakeGithub(() =>
      fail ? json({ message: "API rate limit exceeded" }, 403, { "x-ratelimit-remaining": "0" }) : json(RELEASES_PAGE_1),
    );
    const logs: string[] = [];
    const resolver = new T3ReleaseResolver({ fetch: gh.fetch, env: {}, log: (m) => logs.push(m) });
    expect(await resolver.resolve("latest", "nightly")).toBe("latest");
    expect(logs[0]).toContain("HTTP 403 (rate limited)");

    fail = false;
    expect(await resolver.resolve("latest", "nightly")).toBe("0.1.12-nightly.20261124.5209");

    const down = new T3ReleaseResolver({
      fetch: async () => {
        throw new Error("ECONNREFUSED");
      },
      env: {},
      log: (m) => logs.push(m),
    });
    expect(await down.resolve("latest", "stable")).toBe("latest");
    expect(logs.at(-1)).toContain("ECONNREFUSED");
  });
});
