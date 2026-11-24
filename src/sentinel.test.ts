import { afterEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { gzipSync } from "node:zlib";
import { SlotAuth, hashToken } from "./auth.ts";
import type { BoatClient, BoatExecResult, BoatSandbox, ResumeResult } from "./boat.ts";
import { expandEnvPlaceholders, type SentinelConfig } from "./config.ts";
import { createSentinelServer, type SentinelServerHandle } from "./index.ts";
import type { KlavClient } from "./klav.ts";
import { LifecycleManager } from "./lifecycle.ts";
import { handleMcpRequest } from "./mcp.ts";
import { StateStore } from "./state.ts";

class MockBoat implements BoatClient {
  sandboxes = new Map<string, BoatSandbox>();
  snapshots = new Set<string>();
  created: Array<{ name: string; size: string; from?: string }> = [];
  deleted: string[] = [];
  stopped: string[] = [];
  resumed: string[] = [];
  savedSnapshots: Array<{ id: string; name: string }> = [];
  execScripts: string[] = [];
  upstreamBaseUrl: string | null = null;
  onBeforeBakeStop?: (tempId: string) => Promise<void>;
  private seq = 1;

  async create(name: string, size: string, fromSnapshot?: string): Promise<string> {
    const id = `sb-${this.seq++}`;
    this.created.push({ name, size, from: fromSnapshot });
    this.sandboxes.set(id, { id, name, state: "running", stoppedAt: null });
    return id;
  }

  async info(id: string): Promise<BoatSandbox> {
    const sb = this.sandboxes.get(id);
    if (!sb) throw new Error(`sandbox ${id} not found`);
    return sb;
  }

  async list(): Promise<BoatSandbox[]> {
    return Array.from(this.sandboxes.values());
  }

  async resume(id: string): Promise<ResumeResult> {
    const sb = this.sandboxes.get(id);
    if (!sb) return "data_expired";
    this.resumed.push(id);
    sb.state = "running";
    sb.stoppedAt = null;
    return "resumed";
  }

  async stop(id: string): Promise<void> {
    this.stopped.push(id);
    const sb = this.sandboxes.get(id);
    if (sb) {
      sb.state = "stopped";
      sb.stoppedAt = new Date().toISOString();
    }
  }

  async delete(id: string): Promise<void> {
    this.deleted.push(id);
    this.sandboxes.delete(id);
  }

  async saveSnapshot(id: string, name: string): Promise<void> {
    this.savedSnapshots.push({ id, name });
    this.snapshots.add(name);
  }

  async namedSnapshots(): Promise<string[]> {
    return Array.from(this.snapshots);
  }

  async hostPublic(id: string): Promise<string> {
    return this.upstreamBaseUrl ?? `https://${id}.boat.test`;
  }

  async exec(id: string, script: string): Promise<BoatExecResult> {
    this.execScripts.push(script);
    if (script.includes("BAKE_CLEAN=1")) {
      if (this.onBeforeBakeStop) await this.onBeforeBakeStop(id);
      return {
        exitCode: 0,
        stdout: JSON.stringify({
          t3Version: "t3 v0.1.0",
          descriptor: {
            environmentId: "temp-id",
            label: "temp",
            platform: { os: "linux", arch: "x64" },
            serverVersion: "0.1.0",
            orchestrationProtocolVersion: 2,
            capabilities: { repositoryIdentity: true, accessTokenRenewal: true },
          },
          authSession: {
            authenticated: true,
            auth: {
              policy: "remote-reachable",
              bootstrapMethods: ["one-time-token"],
              sessionMethods: ["bearer-access-token", "dpop-access-token"],
              sessionCookieName: "t3_session",
            },
            scopes: ["orchestration:read", "orchestration:operate"],
            permissions: ["orchestration:read", "orchestration:operate"],
          },
          cleaned: true,
        }),
        stderr: "",
      };
    }
    if (script.includes("provision-base.sh")) {
      return {
        exitCode: 0,
        stdout: JSON.stringify({
          environmentId: "written-slot-env",
          descriptor: {
            environmentId: "written-slot-env",
            label: "Boat 1",
            platform: { os: "linux", arch: "x64" },
            serverVersion: "0.1.0",
            orchestrationProtocolVersion: 2,
            capabilities: { repositoryIdentity: true, accessTokenRenewal: true },
          },
          authSession: {
            authenticated: true,
            auth: {
              policy: "remote-reachable",
              bootstrapMethods: ["one-time-token"],
              sessionMethods: ["bearer-access-token"],
              sessionCookieName: "t3_session",
            },
            scopes: ["orchestration:read", "orchestration:operate"],
            permissions: ["orchestration:read", "orchestration:operate"],
          },
          upstreamToken: "upstream-secret-token",
          t3Version: "t3 v0.1.0",
        }),
        stderr: "",
      };
    }
    return { exitCode: 0, stdout: "", stderr: "" };
  }

  async execOk(id: string, script: string): Promise<string> {
    const r = await this.exec(id, script);
    if (r.exitCode !== 0) throw new Error(r.stderr);
    return r.stdout;
  }

  async upload(): Promise<void> {}
}

const tempDirs: string[] = [];
const cleanups: Array<() => void> = [];

function makeTestConfig(port = 0): SentinelConfig {
  const dir = mkdtempSync(join(tmpdir(), "t3-sentinel-test-"));
  tempDirs.push(dir);
  return {
    domain: "boat.example.com",
    controlHost: "ctrl.boat.example.com",
    listenHost: "127.0.0.1",
    listenPort: port,
    stateDir: dir,
    controlSecret: "ctrl-secret",
    t3: { version: "0.1.0", channel: "stable", autoUpdateOnWake: false },
    boat: {
      defaultSize: "default",
      baseSnapshot: "base-t3",
      sleepAfterSecs: 600,
      archiveGraceSecs: 20,
      deleteWhen: "archived",
      archiveAction: "delete",
      reaperIntervalSecs: 300,
      maxSleepingHours: 0,
      sandboxPrefix: "t3s-",
      injectFiles: [],
    },
    wakePolicy: "manual",
    slots: [
      { name: "boat-1", label: "Boat 1" },
      { name: "boat-2", label: "Boat 2" },
    ],
    projects: {
      "my-app": {
        snapshot: "env-my-app",
        workdir: "/home/user/my-app",
        defaultProvider: "claudeAgent",
        defaultModel: "claude-sonnet-5-5",
        bakeCmd: "git pull && pnpm install",
        bootCmd: "git fetch origin",
      },
    },
  };
}

afterEach(() => {
  while (cleanups.length > 0) cleanups.pop()!();
  while (tempDirs.length > 0) rmSync(tempDirs.pop()!, { recursive: true, force: true });
});

describe("config & auth primitives", () => {
  test("expands ${VAR} placeholders while preserving unset variables", () => {
    const out = expandEnvPlaceholders("a: ${FOO}\nb: ${BAR}", { FOO: "secret-1" });
    expect(out).toBe("a: secret-1\nb: ${BAR}");
  });

  test("pairing code is single-use and tickets enforce rpc vs media use", () => {
    const auth = new SlotAuth();
    const { code } = auth.newPairingCode();
    expect(auth.redeemPairingCode("wrong")).toBe(false);
    expect(auth.redeemPairingCode(code)).toBe(true);
    expect(auth.redeemPairingCode(code)).toBe(false);

    const rpc = auth.issueTicket("rpc", "client-a");
    expect(auth.checkTicket(rpc.ticket, "media")).toBeNull();
    expect(auth.checkTicket(rpc.ticket, "rpc")).toBe("client-a");
    expect(auth.checkTicket(rpc.ticket, "rpc")).toBeNull();

    const media = auth.issueTicket("media", "client-a");
    expect(auth.checkTicket(media.ticket, "media")).toBe("client-a");
    expect(auth.checkTicket(media.ticket, "media")).toBe("client-a");
  });
});

describe("LifecycleManager hardening", () => {
  test("reaper never kills an in-flight bake VM; env-* always bakes from base-t3", async () => {
    const config = makeTestConfig();
    const state = new StateStore(config.stateDir, config);
    const boat = new MockBoat();
    const lifecycle = new LifecycleManager(config, state, boat);

    // Run reaperTick right in the middle of baking base-t3!
    boat.onBeforeBakeStop = async (tempId) => {
      await lifecycle.reaperTick();
      expect(boat.sandboxes.has(tempId)).toBe(true);
      expect(boat.deleted).not.toContain(tempId);
    };

    await lifecycle.bake("base");
    boat.onBeforeBakeStop = undefined;
    expect(boat.sandboxes.size).toBe(0);

    // Bake project twice — BOTH times must build from base-t3 (not from stale env-my-app)
    await lifecycle.bake("my-app");
    await lifecycle.bake("my-app");
    expect(boat.created[1].from).toBe("base-t3");
    expect(boat.created[2].from).toBe("base-t3");
    expect(state.getTemplates().descriptor).not.toBeNull();
  });

  test("concurrent spawns reserve distinct slots and never clobber an occupied slot unless replace=true", async () => {
    const config = makeTestConfig();
    const state = new StateStore(config.stateDir, config);
    const boat = new MockBoat();
    boat.snapshots.add("env-my-app");
    const lifecycle = new LifecycleManager(config, state, boat);

    const [a, b] = await Promise.all([lifecycle.spawn("my-app"), lifecycle.spawn("my-app")]);
    expect(new Set([a.slot.name, b.slot.name])).toEqual(new Set(["boat-1", "boat-2"]));

    // Third spawn with no free slots fails with descriptive list
    await expect(lifecycle.spawn("my-app")).rejects.toThrow(/No free slots available/);
    // Targeting occupied slot without replace=true fails
    await expect(lifecycle.spawn("my-app", { slot: "boat-1" })).rejects.toThrow(/occupied/);
  });

  test("failed initial prompt keeps the healthy VM alive and returns launchError", async () => {
    const upstreamServer = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      async fetch(req, srv) {
        const url = new URL(req.url);
        if (req.headers.get("upgrade")?.toLowerCase() === "websocket") {
          srv.upgrade(req);
          return undefined;
        }
        if (url.pathname === "/api/orchestration/shell") return Response.json({ projects: [], threads: [] });
        if (url.pathname === "/api/projects/mutate") return Response.json({ ok: true });
        return new Response("ok");
      },
      websocket: {
        message(ws, msg) {
          const parsed = JSON.parse(String(msg)) as { id: string };
          ws.send(JSON.stringify({ _tag: "Exit", requestId: parsed.id, exit: { _tag: "Failure", cause: "bad model" } }));
        },
      },
    });
    cleanups.push(() => upstreamServer.stop(true));

    const config = makeTestConfig();
    const state = new StateStore(config.stateDir, config);
    const boat = new MockBoat();
    boat.snapshots.add("env-my-app");
    boat.upstreamBaseUrl = `http://127.0.0.1:${upstreamServer.port}`;
    const lifecycle = new LifecycleManager(config, state, boat);

    const res = await lifecycle.spawn("my-app", { prompt: "Hello" });
    expect(res.slot.phase).toBe("running");
    expect(res.threadId).toBeNull();
    expect(res.launchError).toContain("orchestration.launchThread failed");
    expect(boat.sandboxes.has(res.slot.sandboxId!)).toBe(true);
  });

  test("provisions Klav agent, passes SLOT_ENV_ID (never secrets on CLI), and revokes Klav + deletes VM on archive", async () => {
    const config = makeTestConfig();
    config.projects["my-app"].klav = { project: "Work/my-app", expires: "24h" };

    const boat = new MockBoat();
    boat.snapshots.add("env-my-app");

    const revoked: string[] = [];
    const mockKlav: KlavClient = {
      async provisionForSandbox(_b, sandboxId, proj) {
        return { agentName: `boat-${sandboxId}`, project: proj.project ?? (proj.profiles ?? []).join(",") };
      },
      async revokeForSandbox(agentName) {
        revoked.push(agentName);
      },
    };

    const state = new StateStore(config.stateDir, config);
    const stableEnvId = state.getSlot("boat-1")!.environmentId;
    state.updateSlot("boat-1", (s) =>
      s.clients.push({
        id: "dev-1",
        tokenHash: hashToken("mobile-tok"),
        label: "iPhone",
        pairedAt: new Date().toISOString(),
        lastSyncedSandboxId: null,
      }),
    );

    const lifecycle = new LifecycleManager(config, state, boat, mockKlav);
    const res = await lifecycle.spawn("my-app");
    const sandboxId = res.slot.sandboxId!;

    // Verify SLOT_ENV_ID was passed and watchdogSecret was NOT on the CLI
    const provExec = boat.execScripts.find((s) => s.includes("provision-base.sh"))!;
    expect(provExec).toContain(`SLOT_ENV_ID='${stableEnvId}'`);
    expect(provExec).not.toContain("WATCHDOG_SECRET=");
    expect(res.slot.watchdogSecret).toBeTruthy();

    // Idle event causes sentinel itself to call boat.stop
    await lifecycle.handleWatchdogEvent({ slot: "boat-1", sandboxId, event: "idle" });
    expect(boat.stopped).toContain(sandboxId);
    expect(state.getSlot("boat-1")!.phase).toBe("sleeping");

    // Archive event deletes VM, revokes Klav agent, and preserves hashed client credentials + environmentId
    await lifecycle.handleWatchdogEvent({ slot: "boat-1", sandboxId, event: "all-archived" });
    expect(boat.deleted).toContain(sandboxId);
    expect(revoked).toEqual([`boat-${sandboxId}`]);
    const after = state.getSlot("boat-1")!;
    expect(after.phase).toBe("empty");
    expect(after.environmentId).toBe(stableEnvId);
    expect(after.clients.length).toBe(1);
    expect(boat.savedSnapshots.length).toBe(0);
  });
});

describe("End-to-end HTTP & WebSocket proxy + ControlPlane + Job API", () => {
  test("serves schema-valid descriptor without DPoP, handles gzip without ZlibError, strips stale cursors on VM swap, and runs async jobs", async () => {
    const seenShellQueries: string[] = [];
    const seenWsQueries: string[] = [];

    const upstreamServer = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch(req, srv) {
        const url = new URL(req.url);
        if (req.headers.get("upgrade")?.toLowerCase() === "websocket") {
          seenWsQueries.push(url.search);
          srv.upgrade(req);
          return undefined;
        }
        if (url.pathname === "/api/orchestration/shell") {
          seenShellQueries.push(url.search);
          // Return real gzipped JSON just like T3 Code does!
          const gz = gzipSync(Buffer.from(JSON.stringify({ projects: [], threads: [], sequence: 1 })));
          return new Response(gz, {
            headers: {
              "content-type": "application/json",
              "content-encoding": "gzip",
              "content-length": String(gz.byteLength),
            },
          });
        }
        if (url.pathname === "/api/projects/mutate") return Response.json({ ok: true });
        return new Response("ok");
      },
      websocket: {
        message(ws, msg) {
          ws.send(`echo:${String(msg)}`);
        },
      },
    });
    cleanups.push(() => upstreamServer.stop(true));

    const boat = new MockBoat();
    boat.snapshots.add("env-my-app");
    boat.upstreamBaseUrl = `http://127.0.0.1:${upstreamServer.port}`;
    const config = makeTestConfig(0);
    const handle: SentinelServerHandle = createSentinelServer(config, boat, { startBackgroundLoops: false });
    cleanups.push(() => handle.stop());
    const base = `http://127.0.0.1:${handle.server.port}`;

    // 1. Control API rejects query-string ?token= (Bearer header only!)
    const badQueryAuth = await fetch(`${base}/api/status?token=ctrl-secret`, {
      headers: { host: "ctrl.boat.example.com" },
    });
    expect(badQueryAuth.status).toBe(401);

    // 2. Pair BEFORE first spawn: descriptor has struct platform, no accessTokenRenewal, non-empty scope
    const pairRes = await fetch(`${base}/api/pair`, {
      method: "POST",
      headers: { host: "ctrl.boat.example.com", authorization: "Bearer ctrl-secret", "content-type": "application/json" },
      body: JSON.stringify({ slot: "boat-1" }),
    });
    const { code } = (await pairRes.json()) as { code: string };

    const descRes = await fetch(`${base}/.well-known/t3/environment`, {
      headers: { host: "boat-1.boat.example.com" },
    });
    const desc = (await descRes.json()) as {
      platform: { os: string; arch: string };
      serverVersion: string;
      capabilities: Record<string, unknown>;
    };
    expect(desc.platform).toEqual({ os: "linux", arch: "x64" });
    expect(desc.serverVersion.length).toBeGreaterThan(0);
    expect(desc.capabilities.accessTokenRenewal).toBeUndefined();

    const tokenRes = await fetch(`${base}/oauth/token`, {
      method: "POST",
      headers: { host: "boat-1.boat.example.com", "content-type": "application/x-www-form-urlencoded" },
      body: `subject_token=${encodeURIComponent(code)}&client_label=iPhone`,
    });
    const tokenBody = (await tokenRes.json()) as { access_token: string; scope: string; token_type: string };
    expect(tokenBody.token_type).toBe("Bearer");
    expect(tokenBody.scope.length).toBeGreaterThan(0);
    const clientToken = tokenBody.access_token;

    // Session includes auth descriptor & permissions
    const sessRes = await fetch(`${base}/api/auth/session`, {
      headers: { host: "boat-1.boat.example.com", authorization: `Bearer ${clientToken}` },
    });
    const sess = (await sessRes.json()) as { authenticated: boolean; auth: { policy: string }; permissions: string[] };
    expect(sess.authenticated).toBe(true);
    expect(sess.auth.policy).toBe("remote-reachable");
    expect(sess.permissions.length).toBeGreaterThan(0);

    // 3. Async spawn job with idempotencyKey returns 202 and deduplicates retries
    const job1 = await fetch(`${base}/api/spawn`, {
      method: "POST",
      headers: { host: "ctrl.boat.example.com", authorization: "Bearer ctrl-secret", "content-type": "application/json" },
      body: JSON.stringify({ project: "my-app", slot: "boat-1", async: true, idempotencyKey: "req-1" }),
    });
    expect(job1.status).toBe(202);
    const { jobId } = (await job1.json()) as { jobId: string };

    const jobDup = await fetch(`${base}/api/spawn`, {
      method: "POST",
      headers: { host: "ctrl.boat.example.com", authorization: "Bearer ctrl-secret", "content-type": "application/json" },
      body: JSON.stringify({ project: "my-app", slot: "boat-1", async: true, idempotencyKey: "req-1" }),
    });
    expect(((await jobDup.json()) as { jobId: string }).jobId).toBe(jobId);

    const finished = await handle.jobs.wait(jobId, 10_000);
    expect(finished.status).toBe("completed");

    // 4. Gzipped /api/orchestration/shell decodes cleanly AND strips stale resumeFrom on first connect!
    const shellRes = await fetch(`${base}/api/orchestration/shell?resumeFrom=99`, {
      headers: { host: "boat-1.boat.example.com", authorization: `Bearer ${clientToken}` },
    });
    expect(shellRes.status).toBe(200);
    expect(shellRes.headers.get("content-encoding")).toBeNull();
    const shellJson = (await shellRes.json()) as { sequence: number };
    expect(shellJson.sequence).toBe(1);
    expect(seenShellQueries.at(-1)).toBe("");

    // 5. Bare /api/auth/websocket-ticket (no /rpc suffix) works and WS relays cleanly
    const tRes = await fetch(`${base}/api/auth/websocket-ticket`, {
      method: "POST",
      headers: { host: "boat-1.boat.example.com", authorization: `Bearer ${clientToken}` },
    });
    const { ticket } = (await tRes.json()) as { ticket: string };

    const wsReply = await new Promise<string>((resolve, reject) => {
      const ws = new WebSocket(`ws://127.0.0.1:${handle.server.port}/ws?wsTicket=${ticket}`, {
        headers: { host: "boat-1.boat.example.com" },
      } as unknown as string[]);
      ws.addEventListener("open", () => ws.send("hi"));
      ws.addEventListener("message", (ev) => {
        ws.close();
        resolve(String(ev.data));
      });
      ws.addEventListener("error", () => reject(new Error("ws error")));
    });
    expect(wsReply).toBe("echo:hi");
    expect(seenWsQueries.at(-1)).toBe("");

    // 6. Per-sandbox watchdog secret rejects wrong secret and accepts slot's unique secret
    const badWd = await fetch(`${base}/internal/watchdog`, {
      method: "POST",
      headers: { host: "ctrl.boat.example.com", authorization: "Bearer wrong-secret", "content-type": "application/json" },
      body: JSON.stringify({ slot: "boat-1", sandboxId: handle.state.getSlot("boat-1")!.sandboxId, event: "heartbeat" }),
    });
    expect(badWd.status).toBe(401);

    const goodWd = await fetch(`${base}/internal/watchdog`, {
      method: "POST",
      headers: {
        host: "ctrl.boat.example.com",
        authorization: `Bearer ${handle.state.getSlot("boat-1")!.watchdogSecret}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({ slot: "boat-1", sandboxId: handle.state.getSlot("boat-1")!.sandboxId, event: "heartbeat" }),
    });
    expect(goodWd.status).toBe(200);
  });

  test("LlmProxy swaps per-sandbox ephemeral token -> real upstream key only while running, and rejects sleeping/deleted VMs with 401", async () => {
    const seenUpstreamKeys: Array<string | null> = [];
    const mockLlmUpstream = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch(req) {
        seenUpstreamKeys.push(req.headers.get("x-api-key"));
        return Response.json({ ok: true, model: "claude-opus-5-5" });
      },
    });
    cleanups.push(() => mockLlmUpstream.stop(true));

    const boat = new MockBoat();
    boat.snapshots.add("env-my-app");
    const config = makeTestConfig(0);
    config.llmProxy = {
      host: "boat-llm.boat.example.com",
      upstreamUrl: `http://127.0.0.1:${mockLlmUpstream.port}`,
      upstreamApiKey: "real-secret-upstream-key",
    };

    const handle = createSentinelServer(config, boat, { startBackgroundLoops: false });
    cleanups.push(() => handle.stop());
    const base = `http://127.0.0.1:${handle.server.port}`;

    const { slot } = await handle.lifecycle.spawn("my-app", { slot: "boat-1" });
    const ephemeral = slot.llmToken!;
    expect(ephemeral).toStartWith("t3llm_");
    expect(ephemeral).not.toBe("real-secret-upstream-key");

    // While running: ephemeral token works and upstream receives real key
    const okRes = await fetch(`${base}/v1/messages`, {
      method: "POST",
      headers: { host: "boat-llm.boat.example.com", "x-api-key": ephemeral },
      body: JSON.stringify({ model: "claude-opus-5-5" }),
    });
    expect(okRes.status).toBe(200);
    expect(seenUpstreamKeys).toEqual(["real-secret-upstream-key"]);

    // Once sleeping: same ephemeral token is IMMEDIATELY rejected with 401!
    await handle.lifecycle.sleep("boat-1");
    const sleepRes = await fetch(`${base}/v1/messages`, {
      method: "POST",
      headers: { host: "boat-llm.boat.example.com", "x-api-key": ephemeral },
      body: "{}",
    });
    expect(sleepRes.status).toBe(401);

    // Once deleted: still 401 and slot.llmToken is wiped
    await handle.lifecycle.destroy("boat-1", "test");
    expect(handle.state.getSlot("boat-1")!.llmToken).toBeNull();
    const deadRes = await fetch(`${base}/v1/messages`, {
      method: "POST",
      headers: { host: "boat-llm.boat.example.com", "x-api-key": ephemeral },
      body: "{}",
    });
    expect(deadRes.status).toBe(401);
  });
});

describe("MCP server handlers", () => {
  test("handles ping, initialize, tools/list with annotations, and async sentinel_spawn + sentinel_job", async () => {
    const calls: Array<{ path: string; body?: Record<string, unknown> }> = [];
    const api = {
      async call(path: string, body?: Record<string, unknown>) {
        calls.push({ path, body });
        return { ok: true, jobId: "job-123", status: "running" };
      },
    };

    const ping = await handleMcpRequest({ jsonrpc: "2.0", id: 0, method: "ping" }, api);
    expect((ping as { result: Record<string, unknown> }).result).toEqual({});

    const list = await handleMcpRequest({ jsonrpc: "2.0", id: 1, method: "tools/list" }, api);
    const tools = (list as { result: { tools: Array<{ name: string; annotations?: { destructiveHint?: boolean } }> } }).result
      .tools;
    expect(tools.find((t) => t.name === "sentinel_reset")?.annotations?.destructiveHint).toBe(true);
    expect(tools.some((t) => t.name === "sentinel_job")).toBe(true);

    await handleMcpRequest(
      {
        jsonrpc: "2.0",
        id: 2,
        method: "tools/call",
        params: {
          name: "sentinel_spawn",
          arguments: { project: "my-app", idempotencyKey: "k1", prompt: "Fix bug" },
        },
      },
      api,
    );
    expect(calls[0]).toEqual({
      path: "/api/spawn",
      body: { project: "my-app", async: true, idempotencyKey: "k1", prompt: "Fix bug" },
    });
  });
});

describe("Single-Environment Hub Multiplexer (1 env -> N parallel Boat VMs)", () => {
  const cleanups: Array<() => void> = [];
  afterEach(() => {
    while (cleanups.length) cleanups.pop()!();
  });

  function startFakeVmT3Server(vmTag: string) {
    const seenMethods: Array<{ tag: string; payload: unknown }> = [];
    const vmProjectId = `vm-proj-${vmTag}`;
    const server = Bun.serve({
      port: 0,
      hostname: "127.0.0.1",
      fetch(req, srv) {
        const url = new URL(req.url);
        if (req.headers.get("upgrade")?.toLowerCase() === "websocket") {
          srv.upgrade(req);
          return undefined;
        }
        if (url.pathname === "/api/orchestration/shell") {
          return Response.json({
            projects: [{ id: vmProjectId, title: "my-app", workspaceRoot: "/home/user/my-app" }],
            threads: [],
          });
        }
        return Response.json({ ok: true, vmTag });
      },
      websocket: {
        message(ws, raw) {
          const frame = JSON.parse(String(raw)) as { _tag: string; id?: string; tag?: string; payload?: Record<string, unknown> };
          if (frame._tag === "Ping") {
            ws.sendText(JSON.stringify({ _tag: "Pong" }));
            return;
          }
          if (frame._tag !== "Request" || !frame.id || !frame.tag) return;
          seenMethods.push({ tag: frame.tag, payload: frame.payload });

          if (frame.tag === "orchestration.subscribeShell") {
            ws.sendText(
              JSON.stringify({
                _tag: "Chunk",
                requestId: frame.id,
                values: [
                  {
                    kind: "snapshot",
                    snapshot: {
                      schemaVersion: 2,
                      snapshotSequence: 1,
                      projects: [{ id: vmProjectId, title: "my-app", workspaceRoot: "/home/user/my-app" }],
                      threads: [],
                      archivedThreads: [],
                    },
                  },
                ],
              }),
            );
            return;
          }
          if (frame.tag === "server.getConfig") {
            ws.sendText(
              JSON.stringify({
                _tag: "Exit",
                requestId: frame.id,
                exit: {
                  _tag: "Success",
                  value: { providers: [{ instanceId: "claudeAgent", status: "ready" }], settings: {} },
                },
              }),
            );
            return;
          }
          if (frame.tag === "subscribeServerLifecycle") {
            ws.sendText(
              JSON.stringify({
                _tag: "Chunk",
                requestId: frame.id,
                values: [{ version: 1, sequence: 1, type: "ready", payload: {} }],
              }),
            );
            return;
          }
          if (frame.tag === "orchestration.launchThread") {
            const p = frame.payload ?? {};
            const threadId = String(p.threadId);
            const threadShell = {
              id: threadId,
              projectId: vmProjectId,
              title: String(p.title ?? "Chat"),
              worktreePath: `/home/user/.t3/worktrees/my-app/${vmTag}-${threadId}`,
              status: "running",
            };
            ws.sendText(
              JSON.stringify({
                _tag: "Exit",
                requestId: frame.id,
                exit: {
                  _tag: "Success",
                  value: {
                    threadId,
                    projection: { thread: threadShell },
                    resumed: false,
                    sequence: 2,
                  },
                },
              }),
            );
            // Also emit live shell delta
            ws.sendText(
              JSON.stringify({
                _tag: "Chunk",
                requestId: "hub-shell",
                values: [{ kind: "thread.updated", sequence: 2, location: "active", thread: threadShell }],
              }),
            );
            return;
          }
          if (frame.tag === "terminal.open") {
            ws.sendText(
              JSON.stringify({
                _tag: "Exit",
                requestId: frame.id,
                exit: { _tag: "Success", value: { terminalId: `term-${vmTag}`, vmTag } },
              }),
            );
            return;
          }
          ws.sendText(
            JSON.stringify({
              _tag: "Exit",
              requestId: frame.id,
              exit: { _tag: "Success", value: { vmTag, tag: frame.tag } },
            }),
          );
        },
      },
    });
    return { server, url: `http://127.0.0.1:${server.port}`, seenMethods, vmProjectId };
  }

  test("spawns 1 fresh Boat VM per chat behind 1 single Environment, routes RPCs strictly to owning VM, and destroys only the settled VM", async () => {
    const dir = mkdtempSync(join(tmpdir(), "t3-sentinel-hub-"));
    cleanups.push(() => rmSync(dir, { recursive: true, force: true }));

    const vm1 = startFakeVmT3Server("vm1");
    const vm2 = startFakeVmT3Server("vm2");
    cleanups.push(() => vm1.server.stop(true));
    cleanups.push(() => vm2.server.stop(true));

    const fakeLlmUpstream = Bun.serve({
      port: 0,
      hostname: "127.0.0.1",
      fetch() {
        return Response.json({ ok: true });
      },
    });
    cleanups.push(() => fakeLlmUpstream.stop(true));

    const boat = new MockBoat();
    boat.snapshots.add("base-t3");
    boat.snapshots.add("env-my-app");
    const origHostPublic = boat.hostPublic.bind(boat);
    boat.hostPublic = async (id: string) => {
      if (id === "sb-1") return vm1.url;
      if (id === "sb-2") return vm2.url;
      return origHostPublic(id);
    };

    const cfg: SentinelConfig = {
      ...makeTestConfig(0),
      stateDir: dir,
      hub: { host: "boat.boat.example.com", label: "Boat", maxSandboxes: 5 },
      llmProxy: {
        host: "boat-llm.boat.example.com",
        upstreamUrl: `http://127.0.0.1:${fakeLlmUpstream.port}`,
        upstreamApiKey: "real-secret-key",
      },
    };

    const handle = createSentinelServer(cfg, boat, { startBackgroundLoops: false });
    cleanups.push(() => handle.stop());
    const base = `http://127.0.0.1:${handle.server.port}`;

    // 1. Pair ONCE to the single unified Hub environment
    const pairRes = await fetch(`${base}/api/pair`, {
      method: "POST",
      headers: { host: cfg.controlHost, authorization: `Bearer ${cfg.controlSecret}`, "content-type": "application/json" },
      body: JSON.stringify({ slot: "hub" }),
    });
    const pairData = (await pairRes.json()) as { code: string; url: string };
    expect(pairData.url).toContain("https://boat.boat.example.com/pair#token=");

    const tokenRes = await fetch(`${base}/oauth/token`, {
      method: "POST",
      headers: { host: "boat.boat.example.com", "content-type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({ subject_token: pairData.code, client_label: "Mac" }),
    });
    const { access_token: bearer } = (await tokenRes.json()) as { access_token: string };
    expect(bearer).toBeTruthy();

    const ticketRes = await fetch(`${base}/api/auth/websocket-ticket/rpc`, {
      method: "POST",
      headers: { host: "boat.boat.example.com", authorization: `Bearer ${bearer}` },
    });
    const { ticket } = (await ticketRes.json()) as { ticket: string };

    // 2. Open ONE WebSocket to the unified Hub
    const ws = new WebSocket(`ws://127.0.0.1:${handle.server.port}/ws?wsTicket=${ticket}&orchestrationProtocol=2`, {
      headers: { host: "boat.boat.example.com" },
    } as unknown as string[]);
    cleanups.push(() => ws.close());

    const inbox: Array<Record<string, unknown>> = [];
    const waiters: Array<() => void> = [];
    await new Promise<void>((resolve, reject) => {
      ws.addEventListener("open", () => resolve());
      ws.addEventListener("error", () => reject(new Error("hub ws failed")));
      ws.addEventListener("message", (ev) => {
        inbox.push(JSON.parse(String(ev.data)) as Record<string, unknown>);
        for (const w of waiters.splice(0)) w();
      });
    });

    const waitForFrame = async (pred: (f: Record<string, unknown>) => boolean, timeoutMs = 5_000) => {
      const start = Date.now();
      while (Date.now() - start < timeoutMs) {
        const idx = inbox.findIndex(pred);
        if (idx >= 0) return inbox.splice(idx, 1)[0];
        await new Promise<void>((r) => {
          waiters.push(r);
          setTimeout(r, 50);
        });
      }
      throw new Error(`Timed out waiting for WS frame; inbox=${JSON.stringify(inbox)}`);
    };

    // 3. Subscribe to shell with 0 VMs running -> gets permanent hubProjectId
    ws.send(JSON.stringify({ _tag: "Request", id: "shell-1", tag: "orchestration.subscribeShell", payload: { requestCompletionMarker: true }, headers: [] }));
    const shellChunk = await waitForFrame((f) => f._tag === "Chunk" && f.requestId === "shell-1");
    const snapItem = (shellChunk.values as Array<Record<string, unknown>>)[0];
    expect(snapItem.kind).toBe("snapshot");
    const projects = ((snapItem.snapshot as Record<string, unknown>).projects as Array<Record<string, unknown>>);
    expect(projects.length).toBe(1);
    const hubProjectId = String(projects[0].id);

    // 4. Launch TWO parallel chats on the SAME Hub -> spawns sb-1 and sb-2!
    ws.send(JSON.stringify({
      _tag: "Request",
      id: "launch-a",
      tag: "orchestration.launchThread",
      payload: { threadId: "thread-A", projectId: hubProjectId, title: "Chat A", workspaceStrategy: { type: "root" } },
      headers: [],
    }));
    ws.send(JSON.stringify({
      _tag: "Request",
      id: "launch-b",
      tag: "orchestration.launchThread",
      payload: { threadId: "thread-B", projectId: hubProjectId, title: "Chat B", workspaceStrategy: { type: "root" } },
      headers: [],
    }));

    const exitA = await waitForFrame((f) => f._tag === "Exit" && f.requestId === "launch-a");
    const exitB = await waitForFrame((f) => f._tag === "Exit" && f.requestId === "launch-b");
    expect((exitA.exit as Record<string, unknown>)._tag).toBe("Success");
    expect((exitB.exit as Record<string, unknown>)._tag).toBe("Success");
    expect(handle.state.listHubSandboxes().length).toBe(2);

    // Both projectIds were rewritten back to hubProjectId!
    const projA = (((exitA.exit as { value: { projection: { thread: { projectId: string } } } }).value).projection.thread.projectId);
    const projB = (((exitB.exit as { value: { projection: { thread: { projectId: string } } } }).value).projection.thread.projectId);
    expect(projA).toBe(hubProjectId);
    expect(projB).toBe(hubProjectId);

    // 5. Thread-scoped terminal.open routes strictly to each thread's own VM!
    ws.send(JSON.stringify({ _tag: "Request", id: "term-a", tag: "terminal.open", payload: { threadId: "thread-A" }, headers: [] }));
    ws.send(JSON.stringify({ _tag: "Request", id: "term-b", tag: "terminal.open", payload: { threadId: "thread-B" }, headers: [] }));
    const termA = await waitForFrame((f) => f._tag === "Exit" && f.requestId === "term-a");
    const termB = await waitForFrame((f) => f._tag === "Exit" && f.requestId === "term-b");
    const vmTagA = ((termA.exit as { value: { vmTag: string } }).value).vmTag;
    const vmTagB = ((termB.exit as { value: { vmTag: string } }).value).vmTag;
    expect(new Set([vmTagA, vmTagB])).toEqual(new Set(["vm1", "vm2"]));

    // 6. Verify LlmProxy accepts sb-1's ephemeral token, then destroy ONLY sb-1 via watchdog `all-archived`
    const sb1 = handle.state.getHubSandbox("sb-1")!;
    const sb2 = handle.state.getHubSandbox("sb-2")!;
    expect(sb1.llmToken).toBeTruthy();
    const okLlm = await fetch(`${base}/v1/messages`, {
      method: "POST",
      headers: { host: "boat-llm.boat.example.com", "x-api-key": sb1.llmToken! },
      body: "{}",
    });
    expect(okLlm.status).toBe(200);

    const wdRes = await fetch(`${base}/internal/watchdog`, {
      method: "POST",
      headers: { host: cfg.controlHost, authorization: `Bearer ${sb1.watchdogSecret}`, "content-type": "application/json" },
      body: JSON.stringify({ slot: "hub:sb-1", sandboxId: "sb-1", event: "all-archived", at: new Date().toISOString() }),
    });
    expect(wdRes.status).toBe(200);

    // Wait for thread.removed broadcast on Hub WS
    const removedChunk = await waitForFrame(
      (f) => f._tag === "Chunk" && f.requestId === "shell-1" && Array.isArray(f.values) && (f.values[0] as Record<string, unknown>)?.kind === "thread.removed",
    );
    expect(removedChunk).toBeTruthy();
    expect(handle.state.getHubSandbox("sb-1")).toBeUndefined();
    expect(handle.state.getHubSandbox("sb-2")?.phase).toBe("running");

    // sb-1's LLM token is immediately revoked (401), while sb-2's still returns 200!
    const deadLlm = await fetch(`${base}/v1/messages`, {
      method: "POST",
      headers: { host: "boat-llm.boat.example.com", "x-api-key": sb1.llmToken! },
      body: "{}",
    });
    expect(deadLlm.status).toBe(401);
    const stillAliveLlm = await fetch(`${base}/v1/messages`, {
      method: "POST",
      headers: { host: "boat-llm.boat.example.com", "x-api-key": sb2.llmToken! },
      body: "{}",
    });
    expect(stillAliveLlm.status).toBe(200);
  });
});
