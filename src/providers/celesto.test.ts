import { afterEach, describe, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { CelestoProvider } from "./celesto.ts";

interface Call {
  method: string;
  path: string;
  headers: Record<string, string>;
  body: Record<string, unknown> | undefined;
}

interface FakeComputer {
  id: string;
  name: string;
  status: string;
  /** Statuses to return on the next GETs before settling on `status`. */
  pending?: string[];
}

/** In-memory Celesto API, plus a tiny emulation of the shell commands the provider sends. */
class FakeCelesto {
  calls: Call[] = [];
  computers = new Map<string, FakeComputer>();
  byKey = new Map<string, string>();
  files = new Map<string, string>();
  /** Called on every POST /computers before it is handled (e.g. to inspect the registry). */
  onCreate?: (call: Call) => void;
  /** Fail the next N create POSTs at the network level AFTER the server created the computer. */
  dropCreateResponses = 0;
  deleteConflicts = 0;
  execHandler: (cmd: string) => { exit_code: number; stdout?: string; stderr?: string } | Response | undefined = () =>
    undefined;
  private seq = 1;

  fetch = async (url: string, init: RequestInit): Promise<Response> => {
    const u = new URL(url);
    const path = u.pathname.replace(/^\/v1/, "");
    const headers = Object.fromEntries(Object.entries((init.headers ?? {}) as Record<string, string>));
    const body = init.body ? (JSON.parse(String(init.body)) as Record<string, unknown>) : undefined;
    const call: Call = { method: init.method ?? "GET", path, headers, body };
    this.calls.push(call);
    const json = (status: number, data: unknown) =>
      new Response(JSON.stringify(data), { status, headers: { "content-type": "application/json" } });

    if (call.method === "POST" && path === "/computers") {
      this.onCreate?.(call);
      const key = headers["Idempotency-Key"];
      if (!key) return json(400, { error: { code: "idempotency_key_required" } });
      let id = this.byKey.get(key);
      if (!id) {
        id = `cmp_new${this.seq++}`;
        this.byKey.set(key, id);
        this.computers.set(id, { id, name: `watt-${id}`, status: "running", pending: ["creating"] });
      }
      if (this.dropCreateResponses > 0) {
        this.dropCreateResponses--;
        throw new TypeError("fetch failed: socket hang up");
      }
      return json(201, { id, name: `watt-${id}`, status: "creating", size_id: body?.size_id });
    }
    if (call.method === "GET" && path === "/computers") {
      return json(200, { computers: [...this.computers.values()].map((c) => ({ id: c.id, name: c.name, status: c.status })) });
    }
    const m = path.match(/^\/computers\/([^/]+)(?:\/(.*))?$/);
    if (!m) return json(404, { error: "not found" });
    const c = this.computers.get(m[1]);
    const sub = m[2];
    if (!c) return json(404, { error: { code: "not_found" } });
    if (!sub && call.method === "GET") {
      const status = c.pending?.length ? c.pending.shift()! : c.status;
      return json(200, { id: c.id, name: c.name, status });
    }
    if (!sub && call.method === "DELETE") {
      if (this.deleteConflicts > 0) {
        this.deleteConflicts--;
        return json(409, { error: { code: "computer_busy" } });
      }
      this.computers.delete(c.id);
      return json(200, { id: c.id, status: "deleting" });
    }
    if (sub === "stop") {
      c.status = "stopped";
      c.pending = ["stopping"];
      return json(200, { id: c.id, status: "stopping" });
    }
    if (sub === "start") {
      c.status = "running";
      c.pending = ["starting"];
      return json(200, { id: c.id, status: "starting" });
    }
    if (sub === "published-ports") {
      if (call.method === "GET") return json(200, []);
      return json(201, { port: body?.port, url: "https://p-abcd.celesto.dev/", status: "published" });
    }
    if (sub === "exec") {
      const cmd = String(body?.command ?? "");
      if (cmd.length > 10_000) return json(422, { detail: [{ type: "string_too_long", loc: ["body", "command"], msg: "too long", input: cmd }] });
      if (Number(body?.timeout) > 300) return json(422, { detail: [{ type: "less_than_equal", loc: ["body", "timeout"], msg: "<= 300" }] });
      const custom = this.execHandler(cmd);
      if (custom instanceof Response) return custom;
      if (custom) return json(200, { stdout: "", stderr: "", ...custom });
      return json(200, this.emulate(cmd));
    }
    return json(404, {});
  };

  /** Emulates just enough of the provider's plumbing commands to reassemble uploaded files. */
  private emulate(cmd: string): { exit_code: number; stdout: string; stderr: string } {
    const ok = { exit_code: 0, stdout: "", stderr: "" };
    let mm = cmd.match(/^: > (\S+) && chmod 600 \S+$/);
    if (mm) {
      this.files.set(mm[1], "");
      return ok;
    }
    mm = cmd.match(/^printf '%s' '([A-Za-z0-9+/=]*)' >> (\S+)$/);
    if (mm) {
      this.files.set(mm[2], (this.files.get(mm[2]) ?? "") + mm[1]);
      return ok;
    }
    mm = cmd.match(/base64 -d (\S+) > (?:'([^']+)'|(\S+))/);
    if (mm) {
      const decoded = Buffer.from(this.files.get(mm[1]) ?? "", "base64").toString("utf8");
      const target = mm[2] ?? mm[3];
      this.files.set(target, decoded);
      const mv = cmd.match(/mv -f '([^']+)' '([^']+)'/);
      if (mv) {
        this.files.set(mv[2], this.files.get(mv[1]) ?? "");
        this.files.delete(mv[1]);
      }
    }
    return ok;
  }

  execCommands(): string[] {
    return this.calls.filter((c) => c.path.endsWith("/exec")).map((c) => String(c.body?.command));
  }
}

const dirs: string[] = [];
afterEach(() => {
  while (dirs.length) rmSync(dirs.pop()!, { recursive: true, force: true });
});

function setup(opts: { user?: string } = {}) {
  const stateDir = mkdtempSync(join(tmpdir(), "celesto-test-"));
  dirs.push(stateDir);
  const fake = new FakeCelesto();
  // A computer not in this Sentinel's registry, for exec/upload tests that address it directly
  fake.computers.set("cmp_1", { id: "cmp_1", name: "watt-preexisting", status: "running" });
  const provider = new CelestoProvider({
    apiKey: "cel_test_key",
    stateDir,
    fetch: fake.fetch,
    sleep: async () => {},
    pollIntervalMs: 1,
    ...(opts.user ? { user: opts.user } : {}),
  });
  return { fake, provider, stateDir, registryPath: join(stateDir, "celesto-computers.json") };
}

describe("CelestoProvider", () => {
  test("declares pause only; a missing API key fails on the first API call, not at construction", async () => {
    const { provider } = setup();
    expect([...provider.capabilities]).toEqual(["pause"]);
    expect("download" in provider).toBe(false);

    const stateDir = mkdtempSync(join(tmpdir(), "t3s-celesto-nokey-"));
    dirs.push(stateDir);
    const calls: string[] = [];
    const keyless = new CelestoProvider({
      apiKey: "  ",
      stateDir,
      fetch: async (url) => {
        calls.push(url);
        return new Response("{}");
      },
    });
    await expect(keyless.info("c1")).rejects.toThrow(/CELESTO_API_KEY/);
    await expect(keyless.create({ name: "t3s-hub-x", size: "default" })).rejects.toThrow(/CELESTO_API_KEY/);
    expect(calls).toHaveLength(0);
    // create refused before recording anything to recover later
    expect(keyless.registryEntries()).toEqual([]);
  });

  test("create prepares /home/user for the run user as root, and deletes the computer if that fails", async () => {
    const { fake, provider } = setup();
    const id = await provider.create({ name: "t3s-hub-home-1", size: "default" });
    const execs = fake.execCommands();
    expect(execs).toEqual(["mkdir -p /home/user && chown ohm: /home/user && chmod 755 /home/user"]);
    // Sent raw (as root), not wrapped in sudo -u ohm
    expect(execs[0]).not.toContain("sudo");
    expect(provider.registryEntries().map((e) => e.id)).toEqual([id]);

    fake.execHandler = () => ({ exit_code: 1, stderr: "chown: invalid user" });
    await expect(provider.create({ name: "t3s-hub-home-2", size: "default" })).rejects.toThrow(/chown: invalid user/);
    const failedId = fake.calls.filter((c) => c.method === "DELETE").map((c) => c.path);
    expect(failedId).toHaveLength(1);
    expect(failedId[0]).not.toContain(id);
  });

  test("create sends Idempotency-Key + size_id and never cpus or RAM, then waits for running", async () => {
    const { fake, provider } = setup();
    const id = await provider.create({ name: "t3s-hub-abc-1", size: "default" });
    const id2 = await provider.create({ name: "t3s-hub-def-2", size: "small" });
    const posts = fake.calls.filter((c) => c.method === "POST" && c.path === "/computers");
    expect(posts).toHaveLength(2);
    for (const p of posts) {
      expect(p.headers["Idempotency-Key"]).toMatch(/^[0-9a-f-]{36}$/);
      expect(Object.keys(p.body!).sort()).toEqual(["size_id", "template_id"]);
      for (const banned of ["cpus", "vcpus", "ram_mb", "disk_size_mb", "name", "labels", "size"]) {
        expect(p.body).not.toHaveProperty(banned);
      }
      expect(p.headers.Authorization).toBe("Bearer cel_test_key");
    }
    expect(posts[0].body).toEqual({ template_id: "coding-agent", size_id: "standard" });
    expect(posts[1].body!.size_id).toBe("small");
    expect(posts[0].headers["Idempotency-Key"]).not.toBe(posts[1].headers["Idempotency-Key"]);
    // It polled past `creating`
    expect(fake.calls.some((c) => c.method === "GET" && c.path === `/computers/${id}`)).toBe(true);
    expect(id).not.toBe(id2);
    expect(provider.resolveSize("large")).toBe("large");
    expect(provider.resolveSize("medium")).toBe("large");
  });

  test("records the registry entry before the POST and retries a lost response with the SAME key", async () => {
    const { fake, provider, registryPath } = setup();
    const seenAtPost: Array<Array<{ idempotencyKey: string; intendedName: string; id?: string }>> = [];
    fake.onCreate = () => {
      seenAtPost.push(JSON.parse(readFileSync(registryPath, "utf8")).computers);
    };
    fake.dropCreateResponses = 1;
    const id = await provider.create({ name: "t3s-boat-1-xyz", size: "default" });

    const posts = fake.calls.filter((c) => c.method === "POST" && c.path === "/computers");
    expect(posts).toHaveLength(2);
    expect(posts[0].headers["Idempotency-Key"]).toBe(posts[1].headers["Idempotency-Key"]);
    // Only one computer exists server-side
    expect(fake.byKey.size).toBe(1);
    // Before the first POST the entry existed, without an id
    expect(seenAtPost[0]).toHaveLength(1);
    expect(seenAtPost[0][0].intendedName).toBe("t3s-boat-1-xyz");
    expect(seenAtPost[0][0].idempotencyKey).toBe(posts[0].headers["Idempotency-Key"]);
    expect(seenAtPost[0][0].id).toBeUndefined();
    // After: the id is recorded, and the file is private
    const reg = provider.registryEntries();
    expect(reg).toHaveLength(1);
    expect(reg[0].id).toBe(id);
    if (process.platform !== "win32") {
      const { statSync } = await import("node:fs");
      expect(statSync(registryPath).mode & 0o777).toBe(0o600);
    }
  });

  test("a definitive 4xx create removes the registry entry", async () => {
    const { fake, provider, stateDir } = setup();
    const origFetch = fake.fetch;
    const failing = new CelestoProvider({
      apiKey: "k",
      stateDir,
      sleep: async () => {},
      pollIntervalMs: 1,
      fetch: async (url, init) => {
        if (init.method === "POST" && url.endsWith("/computers")) {
          return new Response(JSON.stringify({ error: { code: "resource_overrides_unavailable" } }), { status: 422 });
        }
        return origFetch(url, init);
      },
    });
    await expect(failing.create({ name: "t3s-x", size: "default" })).rejects.toThrow(/HTTP 422.*resource_overrides_unavailable/);
    expect(provider.registryEntries()).toHaveLength(0);
  });

  test("list() returns only owned computers, under their Sentinel names, and drops gone ones", async () => {
    const { fake, provider } = setup();
    fake.computers.set("cmp_foreign", { id: "cmp_foreign", name: "users-own-box", status: "running" });
    const id = await provider.create({ name: "t3s-hub-abc-1", size: "default" });
    const gone = await provider.create({ name: "t3s-hub-old-2", size: "default" });
    fake.computers.delete(gone); // deleted out of band

    const listed = await provider.list();
    expect(listed).toEqual([{ id, name: "t3s-hub-abc-1", state: "running", stoppedAt: null }]);
    expect(provider.registryEntries().map((e) => e.id)).toEqual([id]);
    expect((await provider.info(id)).name).toBe("t3s-hub-abc-1");
  });

  test("list() hides a computer while its create() is in progress, then shows it", async () => {
    const { fake, stateDir } = setup();
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    let waiting!: () => void;
    const blocked = new Promise<void>((r) => (waiting = r));
    // Every sleep (the poll between status reads) parks until the gate opens
    const provider = new CelestoProvider({
      apiKey: "cel_test_key",
      stateDir,
      fetch: fake.fetch,
      sleep: async () => {
        waiting();
        await gate;
      },
      pollIntervalMs: 1,
    });

    const creating = provider.create({ name: "t3s-hub-race-1", size: "default" });
    await blocked;
    // The POST is done and the id recorded, but create() is still waiting for `running`
    const [entry] = provider.registryEntries();
    expect(entry.id).toMatch(/^cmp_new/);
    expect(fake.computers.get(entry.id!)?.pending).toEqual([]);
    expect(await provider.list()).toEqual([]);

    release();
    const id = await creating;
    expect(id).toBe(entry.id!);
    expect(await provider.list()).toEqual([{ id, name: "t3s-hub-race-1", state: "running", stoppedAt: null }]);
  });

  test("a create() that fails after the POST shows up in list() again, so the reaper can delete it", async () => {
    const { fake, stateDir } = setup();
    const provider = new CelestoProvider({
      apiKey: "cel_test_key",
      stateDir,
      sleep: async () => {},
      pollIntervalMs: 1,
      fetch: async (url, init) => {
        const res = await fake.fetch(url, init);
        // The new computer breaks while booting
        if (init.method === "POST" && url.endsWith("/computers")) {
          const { id } = (await res.clone().json()) as { id: string };
          fake.computers.set(id, { id, name: `watt-${id}`, status: "error" });
        }
        return res;
      },
    });
    await expect(provider.create({ name: "t3s-hub-broken-1", size: "default" })).rejects.toThrow(/is error/);
    expect((await provider.list()).map((c) => c.name)).toEqual(["t3s-hub-broken-1"]);
  });

  test("list() throws on a corrupt registry instead of reporting nothing", async () => {
    const { provider, registryPath } = setup();
    writeFileSync(registryPath, "{not json");
    await expect(provider.list()).rejects.toThrow();
  });

  test("delete retries 409 while creating, then drops the registry entry", async () => {
    const { fake, provider } = setup();
    const id = await provider.create({ name: "t3s-hub-abc-1", size: "default" });
    fake.deleteConflicts = 2;
    await provider.delete(id);
    expect(fake.calls.filter((c) => c.method === "DELETE")).toHaveLength(3);
    expect(fake.computers.has(id)).toBe(false);
    expect(provider.registryEntries()).toHaveLength(0);
    // Deleting an already-gone computer is a no-op
    await provider.delete(id);
  });

  test("exec runs as the user in a bash login shell from its home", async () => {
    const { fake, provider } = setup();
    fake.execHandler = () => ({ exit_code: 0, stdout: "hi\n" });
    const r = await provider.exec("cmp_1", "echo 'hi'", 30);
    expect(r).toEqual({ exitCode: 0, stdout: "hi\n", stderr: "" });
    const [cmd] = fake.execCommands();
    expect(cmd).toBe(`cd /home/ohm && exec sudo -n -u ohm -H bash -lc 'echo '\\''hi'\\'''`);
    expect(fake.calls.at(-1)!.body!.timeout).toBe(30);
  });

  test("execOk throws with a stderr tail and never echoes the command", async () => {
    const { fake, provider } = setup();
    fake.execHandler = () => ({ exit_code: 3, stderr: `${"x".repeat(1000)}boom` });
    const err = await provider.execOk("cmp_1", "echo SECRET_VALUE", 30).catch((e: Error) => e);
    expect(String(err)).toMatch(/exit 3\): x+boom$/);
    expect(String(err)).not.toContain("SECRET_VALUE");

    fake.execHandler = (cmd) =>
      new Response(JSON.stringify({ detail: [{ type: "string_too_long", msg: "too long", input: cmd }] }), { status: 422 });
    const err2 = await provider.exec("cmp_1", "echo SECRET_VALUE", 30).catch((e: Error) => e);
    expect(String(err2)).toContain("string_too_long");
    expect(String(err2)).not.toContain("SECRET_VALUE");
  });

  test("uploads are chunked under 10,000 chars, expand ~ and land owned by the user with mode 600", async () => {
    const { fake, provider, stateDir } = setup();
    const local = join(stateDir, "big.txt");
    const content = Array.from({ length: 3000 }, (_, i) => `line ${i} ${"z".repeat(i % 37)}`).join("\n");
    writeFileSync(local, content);
    await provider.upload("cmp_1", local, "~/.t3-sentinel/boot-secrets.json");

    const cmds = fake.execCommands();
    expect(cmds.length).toBeGreaterThan(5);
    for (const c of cmds) expect(c.length).toBeLessThan(10_000);
    expect(fake.files.get("/home/ohm/.t3-sentinel/boot-secrets.json")).toBe(content);
    const final = cmds.find((c) => c.includes("base64 -d"))!;
    expect(final).toContain("chmod 600");
    expect(final).toContain("chown ohm:");
    expect(final).toContain(`mkdir -p "$parent"`);
    expect(final).toMatch(/mv -f '\/home\/ohm\/\.t3-sentinel\/boot-secrets\.json\.t3s-up\.[0-9a-f]+' '\/home\/ohm\/\.t3-sentinel\/boot-secrets\.json'/);
    // Staging is cleaned up
    expect(cmds.at(-1)).toMatch(/^rm -rf \/tmp\/t3s-up-[0-9a-f]+$/);
  });

  test("a long script is uploaded as a file and then run as the user", async () => {
    const { fake, provider } = setup();
    const script = `echo start\n${"true # padding\n".repeat(1500)}echo end`;
    expect(script.length).toBeGreaterThan(20_000);
    fake.execHandler = (cmd) => (cmd.includes("bash -l /tmp/t3s-exec-") ? { exit_code: 0, stdout: "start\nend\n" } : undefined);
    const r = await provider.exec("cmp_1", script, 120);
    expect(r.stdout).toBe("start\nend\n");
    const cmds = fake.execCommands();
    for (const c of cmds) expect(c.length).toBeLessThan(10_000);
    const runIdx = cmds.findIndex((c) => c.includes("bash -l /tmp/t3s-exec-"));
    expect(runIdx).toBeGreaterThan(0);
    expect(cmds[runIdx]).toMatch(/^cd \/home\/ohm && sudo -n -u ohm -H bash -l \/tmp\/t3s-exec-[0-9a-f]+\/script\.sh$/);
    const dir = cmds[runIdx].match(/(\/tmp\/t3s-exec-[0-9a-f]+)/)![1];
    expect(fake.files.get(`${dir}/script.sh`)).toBe(script);
    expect(cmds.at(-1)).toBe(`rm -rf ${dir}`);
  });

  test("a timeout over 300 s detaches the command and polls its rc file", async () => {
    const { fake, provider } = setup();
    let polls = 0;
    fake.execHandler = (cmd) => {
      if (cmd.startsWith("cat ") && cmd.endsWith("/rc 2>/dev/null")) {
        polls++;
        return polls < 3 ? { exit_code: 1 } : { exit_code: 0, stdout: "0\n" };
      }
      if (cmd.startsWith("tail -c 1048576")) return { exit_code: 0, stdout: '{"upstreamToken":"t"}\n' };
      if (cmd.startsWith("tail -c 65536")) return { exit_code: 0, stdout: "warn\n" };
      return undefined;
    };
    const r = await provider.exec("cmp_1", "T3_INSTALL_PROVIDERS=antigravity bash ~/.t3-sentinel/provision-base.sh", 900);
    expect(r).toEqual({ exitCode: 0, stdout: '{"upstreamToken":"t"}\n', stderr: "warn\n" });
    expect(polls).toBe(3);
    const cmds = fake.execCommands();
    const detach = cmds.find((c) => c.startsWith("nohup setsid timeout -k 10 900 bash -c "));
    expect(detach).toBeDefined();
    expect(detach).toContain("sudo -n -u ohm -H bash -l /tmp/t3s-exec-");
    expect(detach).toMatch(/<\/dev\/null >\/dev\/null 2>&1 &$/);
    for (const c of fake.calls.filter((x) => x.path.endsWith("/exec"))) {
      expect(Number(c.body!.timeout)).toBeLessThanOrEqual(300);
      expect(String(c.body!.command).length).toBeLessThan(10_000);
    }
  });

  test("pause stops and waits; resume starts and needs the provision marker + live supervisor", async () => {
    const { fake, provider } = setup();
    const id = await provider.create({ name: "t3s-hub-abc-1", size: "default" });
    await provider.pause(id);
    expect(fake.calls.some((c) => c.method === "POST" && c.path === `/computers/${id}/stop`)).toBe(true);
    expect(fake.computers.get(id)!.status).toBe("stopped");

    let markerPresent = false;
    let checkOutput: { exit_code: number; stdout?: string } | null = null;
    fake.execHandler = (cmd) =>
      cmd.includes("test -f ~/.t3-sentinel/provisioned") || cmd.includes("[ ! -f ~/.t3-sentinel/provisioned ]")
        ? (checkOutput ?? { exit_code: 0, stdout: markerPresent ? "t3s-resume:ok\n" : "t3s-resume:missing\n" })
        : undefined;
    expect(await provider.resume(id, "default")).toBe("data_expired");
    expect(fake.calls.some((c) => c.method === "POST" && c.path === `/computers/${id}/start`)).toBe(true);

    await provider.pause(id);
    markerPresent = true;
    expect(await provider.resume(id, "default")).toBe("resumed");
    const check = fake.execCommands().at(-1)!;
    expect(check).toContain("sudo -n -u ohm -H bash -lc");
    expect(check).toContain("t3-supervisor.pid");

    // No verdict (e.g. the exec failed oddly) never reads as expired: the wake fails and can retry
    checkOutput = { exit_code: 1, stdout: "" };
    await expect(provider.resume(id, "default")).rejects.toThrow(/no verdict/);
    // Provisioned disk but no live supervisor after a stop: resumable (the Hub re-provisions)
    checkOutput = { exit_code: 0, stdout: "t3s-resume:dead\n" };
    expect(await provider.resume(id, "default")).toBe("resumed");

    fake.computers.delete(id);
    expect(await provider.resume(id, "default")).toBe("data_expired");
  });

  test("list() recovers an interrupted create with the same key and size, so the reaper can see it", async () => {
    const { fake, provider, registryPath } = setup();
    const key = "11111111-2222-3333-4444-555555555555";
    // A computer the server created, whose 201 this Sentinel never saw (crash after the POST)
    fake.byKey.set(key, "cmp_lost");
    fake.computers.set("cmp_lost", { id: "cmp_lost", name: "watt-lost", status: "running" });
    writeFileSync(
      registryPath,
      JSON.stringify({
        computers: [
          { idempotencyKey: key, intendedName: "t3s-hub-lost-1", sizeId: "large", createdAt: new Date(Date.now() - 120_000).toISOString() },
        ],
      }),
      { mode: 0o600 },
    );
    const listed = await provider.list();
    const post = fake.calls.find((c) => c.method === "POST" && c.path === "/computers")!;
    expect(post.headers["Idempotency-Key"]).toBe(key);
    expect(post.body!.size_id).toBe("large");
    expect(listed).toEqual([{ id: "cmp_lost", name: "t3s-hub-lost-1", state: "running", stoppedAt: null }]);
    expect(provider.registryEntries()[0]).toMatchObject({ idempotencyKey: key, id: "cmp_lost", recoveryAttempts: 1 });
  });

  test("a 409 on create keeps the registry entry (the key may still be in flight)", async () => {
    const { fake, provider, stateDir } = setup();
    const busy = new CelestoProvider({
      apiKey: "k",
      stateDir,
      sleep: async () => {},
      pollIntervalMs: 1,
      fetch: async (url, init) =>
        init.method === "POST" && url.endsWith("/computers")
          ? new Response(JSON.stringify({ error: { code: "idempotency_conflict" } }), { status: 409 })
          : fake.fetch(url, init),
    });
    await expect(busy.create({ name: "t3s-busy", size: "default" })).rejects.toThrow(/HTTP 409/);
    expect(provider.registryEntries()).toHaveLength(1);
    expect(provider.registryEntries()[0]).toMatchObject({ intendedName: "t3s-busy", sizeId: "standard" });
  });

  test("exposePort publishes once and returns the public URL without headers", async () => {
    const { fake, provider } = setup();
    const ep = await provider.exposePort("cmp_1", 3773, { visibility: "private" });
    expect(ep).toEqual({ url: "https://p-abcd.celesto.dev" });
    const post = fake.calls.find((c) => c.method === "POST" && c.path.endsWith("/published-ports"))!;
    expect(post.body).toEqual({ port: 3773 });
  });
});
