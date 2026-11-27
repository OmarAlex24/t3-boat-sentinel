import { afterEach, describe, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { BoatExecResult, BoatSandbox, ResumeResult } from "./boat.ts";
import { loadConfig, resolveAutoUpsizeConfig, type SentinelConfig } from "./config.ts";
import { createSentinelServer, type SentinelServerHandle } from "./index.ts";
import { StateStore, type HubSandboxRecord } from "./state.ts";
import { evaluateWatchdogOverload } from "./watchdog-overload.ts";

const cleanups: Array<() => void | Promise<void>> = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!();
});

function tempDir(prefix = "t3-auto-upsize-"): string {
  const dir = mkdtempSync(join(tmpdir(), prefix));
  cleanups.push(() => rmSync(dir, { recursive: true, force: true }));
  return dir;
}

function makeConfig(stateDir: string, patch?: (cfg: SentinelConfig) => void): SentinelConfig {
  const cfg: SentinelConfig = {
    domain: "boat.example.com",
    controlHost: "sentinel.boat.example.com",
    controlSecret: "test-secret-0123456789abcdef",
    listenHost: "127.0.0.1",
    listenPort: 0,
    stateDir,
    hub: { host: "boat.boat.example.com", label: "Boat", maxSandboxes: 4 },
    boat: {
      baseSnapshot: "base-t3",
      defaultSize: "large",
      sleepAfterSecs: 1800,
      archiveGraceSecs: 60,
      deleteWhen: "archived-or-settled",
      archiveAction: "delete",
      settleAction: "park",
      reaperIntervalSecs: 600,
      maxSleepingHours: 0,
      sandboxPrefix: "t3-",
      hostVisibility: "public",
      injectFiles: [],
      autoUpsize: { to: "xlarge", sustainSecs: 5 },
    },
    t3: { version: "0.1.14", channel: "stable", autoUpdateOnWake: false },
    projects: {
      "my-app": {
        repo: "https://github.com/example/my-app.git",
        snapshot: "env-my-app",
        workdir: "/home/user/my-app",
      },
    },
    wakePolicy: "manual",
    slots: [],
  };
  patch?.(cfg);
  return cfg;
}

function yamlConfig(extraBoat = "", extraProject = ""): string {
  return `
domain: example.com
controlSecret: test-secret-0123456789abcdef
listenPort: 0
stateDir: ./data
hub:
  host: boat.example.com
  label: Boat
  maxSandboxes: 2
t3:
  version: latest
  channel: stable
  autoUpdateOnWake: false
boat:
  defaultSize: large
  baseSnapshot: base-t3
  sleepAfterSecs: 600
  archiveGraceSecs: 20
  deleteWhen: archived-or-settled
  archiveAction: delete
  settleAction: park
  reaperIntervalSecs: 300
  maxSleepingHours: 0
  sandboxPrefix: t3s-
  injectFiles: []
${extraBoat}
wakePolicy: manual
slots: []
projects:
  my-app:
    snapshot: env-my-app
    workdir: /home/user/my-app
${extraProject}
`;
}

function loadYaml(extraBoat = "", extraProject = ""): SentinelConfig {
  const dir = tempDir("t3-auto-upsize-config-");
  const file = join(dir, "sentinel.yaml");
  writeFileSync(file, yamlConfig(extraBoat, extraProject));
  return loadConfig(file);
}

describe("autoUpsize config", () => {
  test("resolveAutoUpsizeConfig applies defaults, global values, per-repo overrides, and per-repo false", () => {
    const dir = tempDir();
    const cfg = makeConfig(dir, (c) => {
      c.boat.autoUpsize = {
        to: "xlarge",
        minAvailableMemPct: 12,
        minLoadPerCpu: 1.5,
        sustainSecs: 30,
        onOomKill: false,
        autoContinue: false,
      };
    });
    expect(resolveAutoUpsizeConfig(cfg)).toEqual({
      to: "xlarge",
      minAvailableMemPct: 12,
      minLoadPerCpu: 1.5,
      sustainSecs: 30,
      onOomKill: false,
      autoContinue: false,
    });

    const project = { ...cfg.projects["my-app"], autoUpsize: { to: "xxlarge", sustainSecs: 45 } };
    expect(resolveAutoUpsizeConfig(cfg, project)).toEqual({
      to: "xxlarge",
      minAvailableMemPct: 15,
      minLoadPerCpu: 2,
      sustainSecs: 45,
      onOomKill: true,
      autoContinue: true,
    });
    expect(resolveAutoUpsizeConfig(cfg, { ...cfg.projects["my-app"], autoUpsize: false })).toBeNull();

    cfg.boat.autoUpsize = undefined;
    expect(resolveAutoUpsizeConfig(cfg, project)).toEqual({
      to: "xxlarge",
      minAvailableMemPct: 15,
      minLoadPerCpu: 2,
      sustainSecs: 45,
      onOomKill: true,
      autoContinue: true,
    });
  });

  test("loadConfig accepts global and per-project autoUpsize and rejects invalid values", () => {
    const good = loadYaml(
      `  autoUpsize:\n    to: xlarge\n    minAvailableMemPct: 20\n    minLoadPerCpu: 1.25\n    sustainSecs: 10\n    onOomKill: false\n    autoContinue: false`,
      `    autoUpsize:\n      to: xxlarge\n      sustainSecs: 15`,
    );
    expect(good.boat.autoUpsize).toMatchObject({ to: "xlarge", minAvailableMemPct: 20, sustainSecs: 10 });
    expect(good.projects["my-app"].autoUpsize).toMatchObject({ to: "xxlarge", sustainSecs: 15 });
    expect(resolveAutoUpsizeConfig(good, good.projects["my-app"])).toMatchObject({ to: "xxlarge", sustainSecs: 15 });

    const invalidGlobalValues = [
      `  autoUpsize:\n    to: ""`,
      `  autoUpsize:\n    to: xlarge\n    minAvailableMemPct: 0`,
      `  autoUpsize:\n    to: xlarge\n    minAvailableMemPct: 100`,
      `  autoUpsize:\n    to: xlarge\n    minLoadPerCpu: 0`,
      `  autoUpsize:\n    to: xlarge\n    sustainSecs: 4`,
    ];
    for (const extra of invalidGlobalValues) {
      expect(() => loadYaml(extra)).toThrow(/autoUpsize/);
    }
    expect(() => loadYaml("", `    autoUpsize:\n      sustainSecs: 10`)).toThrow(/projects\.my-app\.autoUpsize\.to/);
    expect(() => loadYaml("", `    autoUpsize: true`)).toThrow(/projects\.my-app\.autoUpsize/);
  });
});

describe("watchdog overload evaluator", () => {
  const cfg = { minAvailableMemPct: 15, minLoadPerCpu: 2, sustainSecs: 10, onOomKill: true };
  const input = (patch: Record<string, unknown> = {}) => ({
    now: 100_000,
    cpus: 4,
    memTotalKb: 16 * 1024 * 1024,
    memAvailableKb: 1024 * 1024,
    load1: 8,
    oomKill: 7,
    baselineOomKill: 7,
    shellFailingSince: null,
    overloadedSince: null,
    ...patch,
  });

  test("OOM kill delta triggers immediately, while the startup baseline does not", () => {
    const baseline = evaluateWatchdogOverload(cfg, input({ baselineOomKill: null }));
    expect(baseline.baselineOomKill).toBe(7);
    expect(baseline.trigger).toBeNull();

    const triggered = evaluateWatchdogOverload(cfg, input({ oomKill: 9, baselineOomKill: 7 }));
    expect(triggered.trigger?.reason).toBe("oom-kill");
    expect(triggered.trigger?.detail).toMatchObject({ reason: "oom-kill", oomKillsDelta: 2, memAvailPct: 6.25, cpus: 4, load1: 8 });
    expect(evaluateWatchdogOverload({ ...cfg, onOomKill: false }, input({ oomKill: 9 })).trigger).toBeNull();
  });

  test("sustained memory pressure with high load triggers only after sustainSecs", () => {
    const first = evaluateWatchdogOverload(cfg, input({ overloadedSince: null }));
    expect(first.pressure).toBe(true);
    expect(first.overloadedSince).toBe(100_000);
    expect(first.trigger).toBeNull();

    const early = evaluateWatchdogOverload(cfg, input({ now: 109_999, overloadedSince: first.overloadedSince }));
    expect(early.trigger).toBeNull();
    const due = evaluateWatchdogOverload(cfg, input({ now: 110_000, overloadedSince: first.overloadedSince }));
    expect(due.trigger?.reason).toBe("sustained-pressure");
    expect(due.trigger?.detail).toMatchObject({ sustainedSecs: 10, memAvailMb: 1024, load1: 8, cpus: 4 });
  });

  test("low memory plus an unresponsive shell qualifies even without high load; recovery resets the timer", () => {
    const failing = evaluateWatchdogOverload(cfg, input({ load1: 0.2, shellFailingSince: 90_000, overloadedSince: 95_000, now: 105_000 }));
    expect(failing.trigger?.reason).toBe("shell-unresponsive-low-mem");

    const quiet = evaluateWatchdogOverload(cfg, input({ memAvailableKb: 12 * 1024 * 1024, overloadedSince: 90_000 }));
    expect(quiet.pressure).toBe(false);
    expect(quiet.overloadedSince).toBeNull();
    expect(evaluateWatchdogOverload(cfg, input({ memTotalKb: null, memAvailableKb: null })).pressure).toBe(false);
  });

  test("scripts/watchdog.mjs stays self-contained and checks overload before the shell-failure return", () => {
    const script = readFileSync(join(import.meta.dir, "..", "scripts", "watchdog.mjs"), "utf8");
    const imports = [...script.matchAll(/^import .* from "([^"]+)";$/gm)].map((m) => m[1]);
    expect(imports.length).toBeGreaterThan(0);
    expect(imports.every((spec) => spec.startsWith("node:"))).toBe(true);
    expect(script).toContain("/proc/meminfo");
    expect(script).toContain("/proc/loadavg");
    expect(script).toContain("/proc/vmstat");
    expect(script).toContain("baselineOomKill");
    const tickStart = script.indexOf("lastTickAt = now;");
    const firstOverloadCheck = script.indexOf("await checkOverload(now);", tickStart);
    expect(firstOverloadCheck).toBeGreaterThan(tickStart);
    expect(firstOverloadCheck).toBeLessThan(script.indexOf("readDbActivity();", tickStart));
    const shellFailure = script.indexOf("shellFailingSince ??= now;");
    const overloadCheck = script.indexOf("await checkOverload(now);", shellFailure);
    const earlyReturn = script.indexOf("return;", shellFailure);
    expect(shellFailure).toBeGreaterThan(-1);
    expect(overloadCheck).toBeGreaterThan(shellFailure);
    expect(overloadCheck).toBeLessThan(earlyReturn);
  });
});

class MiniBoatClient {
  sandboxes = new Map<string, BoatSandbox>();
  stopped: string[] = [];
  resumed: Array<{ id: string; size: string }> = [];
  execScripts: string[] = [];
  resumeGate: Promise<void> | null = null;

  constructor(id: string) {
    this.sandboxes.set(id, { id, name: `test-${id}`, state: "running", stoppedAt: null });
  }
  async create(name: string, _size: string, _fromSnapshot?: string): Promise<string> {
    const id = `sb-${this.sandboxes.size + 1}`;
    this.sandboxes.set(id, { id, name, state: "running", stoppedAt: null });
    return id;
  }
  async info(id: string): Promise<BoatSandbox> {
    const sb = this.sandboxes.get(id);
    if (!sb) throw new Error(`sandbox ${id} not found`);
    return sb;
  }
  async list(): Promise<BoatSandbox[]> {
    return [...this.sandboxes.values()];
  }
  async resume(id: string, size: string): Promise<ResumeResult> {
    if (this.resumeGate) await this.resumeGate;
    const sb = this.sandboxes.get(id);
    if (!sb) return "data_expired";
    this.resumed.push({ id, size });
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
    this.sandboxes.delete(id);
  }
  async saveSnapshot(): Promise<void> {}
  async namedSnapshots(): Promise<string[]> {
    return ["base-t3", "env-my-app"];
  }
  async hostPublic(id: string): Promise<string> {
    return this.urls.get(id) ?? `http://127.0.0.1:1`;
  }
  urls = new Map<string, string>();
  async exec(_id: string, script: string): Promise<BoatExecResult> {
    this.execScripts.push(script);
    return { exitCode: 0, stdout: "{}", stderr: "" };
  }
  async execOk(id: string, script: string): Promise<string> {
    const res = await this.exec(id, script);
    if (res.exitCode !== 0) throw new Error(res.stderr);
    return res.stdout;
  }
  uploads: Array<{ remotePath: string; content: string | null }> = [];
  async upload(_id: string, localPath: string, remotePath: string): Promise<void> {
    let content: string | null = null;
    try {
      content = readFileSync(localPath, "utf8");
    } catch {}
    this.uploads.push({ remotePath, content });
  }
}

function startFakeVm() {
  const dispatches: Array<Record<string, unknown>> = [];
  const failDispatches = new Set<string>();
  const threads: Array<Record<string, unknown>> = [];
  const server = Bun.serve({
    port: 0,
    hostname: "127.0.0.1",
    fetch(req, srv) {
      if (req.headers.get("upgrade")?.toLowerCase() === "websocket") {
        return srv.upgrade(req) ? undefined : new Response("upgrade failed", { status: 500 });
      }
      return Response.json({ ok: true });
    },
    websocket: {
      message(ws, raw) {
        const frame = JSON.parse(String(raw)) as { _tag?: string; id?: string | number; tag?: string; payload?: Record<string, unknown> };
        if (frame._tag === "Ping") {
          ws.send(JSON.stringify({ _tag: "Pong" }));
          return;
        }
        if (frame._tag !== "Request" || !frame.tag) return;
        const id = frame.id!;
        if (frame.tag === "orchestration.subscribeShell") {
          ws.send(JSON.stringify({
            _tag: "Chunk",
            requestId: id,
            values: [{ kind: "snapshot", snapshot: { schemaVersion: 2, snapshotSequence: 1, projects: [], threads, archivedThreads: [] } }],
          }));
          return;
        }
        if (frame.tag === "subscribeServerConfig") {
          ws.send(JSON.stringify({
            _tag: "Chunk",
            requestId: id,
            values: [{ type: "snapshot", config: { providers: [], settings: {} } }],
          }));
          return;
        }
        if (frame.tag === "subscribeServerLifecycle") {
          ws.send(JSON.stringify({ _tag: "Chunk", requestId: id, values: [{ version: 1, sequence: 1, type: "ready", payload: {} }] }));
          return;
        }
        if (frame.tag === "orchestration.dispatchCommand" && frame.payload?.type === "message.dispatch") {
          dispatches.push(frame.payload);
          if (failDispatches.has(String(frame.payload.threadId))) {
            ws.send(JSON.stringify({
              _tag: "Exit",
              requestId: id,
              exit: { _tag: "Failure", cause: [{ _tag: "Fail", error: { _tag: "DispatchRejectedError", message: "injected dispatch failure" } }] },
            }));
          } else {
            ws.send(JSON.stringify({ _tag: "Exit", requestId: id, exit: { _tag: "Success", value: { sequence: 2, runId: "run-continuation", status: "running" } } }));
          }
          return;
        }
        ws.send(JSON.stringify({ _tag: "Exit", requestId: id, exit: { _tag: "Success", value: { ok: true } } }));
      },
    },
  });
  cleanups.push(() => server.stop(true));
  return { server, url: `http://127.0.0.1:${server.port}`, dispatches, failDispatches };
}

function threadRow(threadId: string, projectId: string, live: boolean, extra: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    id: threadId,
    projectId,
    title: threadId,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    status: live ? "running" : "idle",
    activeRunId: live ? `run-${threadId}` : null,
    activityRunStatus: null,
    pendingRuntimeRequest: null,
    pendingBackgroundTasks: [],
    archivedAt: null,
    deletedAt: null,
    settledAt: null,
    ...extra,
  };
}

function seedHubSandbox(handle: SentinelServerHandle, cfg: SentinelConfig, vmUrl: string, sandboxId = "sb-1"): HubSandboxRecord {
  const hub = handle.state.ensureHub(cfg.hub?.label ?? "Boat", Object.keys(cfg.projects));
  const now = new Date().toISOString();
  const record: HubSandboxRecord = {
    sandboxId,
    project: "my-app",
    hubProjectId: hub.projectIds["my-app"],
    vmProjectId: "vm-proj-my-app",
    snapshot: "env-my-app",
    size: "large",
    upstreamUrl: vmUrl,
    upstreamHeaders: null,
    upstreamToken: "upstream-token",
    watchdogSecret: "watchdog-secret",
    llmToken: null,
    klavAgentName: null,
    threadIds: [],
    worktreePaths: [],
    phase: "running",
    createdAt: now,
    lastActiveAt: now,
    lastHeartbeatAt: now,
    sleepingSince: null,
  };
  handle.state.setHubSandbox(record);
  return record;
}

function seedThread(handle: SentinelServerHandle, sandboxId: string, row: Record<string, unknown>): void {
  const hub = handle.hub as unknown as {
    threadToSandbox: Map<string, string>;
    threadShells: Map<string, Record<string, unknown>>;
  };
  const threadId = String(row.id);
  hub.threadToSandbox.set(threadId, sandboxId);
  hub.threadShells.set(threadId, row);
  handle.state.updateHubSandbox(sandboxId, (s) => {
    if (!s.threadIds.includes(threadId)) s.threadIds.push(threadId);
  });
}

async function setupHub(patch?: (cfg: SentinelConfig) => void) {
  const dir = tempDir();
  const vm = startFakeVm();
  const boat = new MiniBoatClient("sb-1");
  boat.urls.set("sb-1", vm.url);
  const cfg = makeConfig(dir, patch);
  const handle = createSentinelServer(cfg, boat, { startBackgroundLoops: false });
  cleanups.push(() => handle.stop());
  seedHubSandbox(handle, cfg, vm.url, "sb-1");
  return { dir, vm, boat, cfg, handle };
}

async function waitFor(cond: () => boolean, timeoutMs = 2_000): Promise<void> {
  const started = Date.now();
  while (!cond()) {
    if (Date.now() - started > timeoutMs) throw new Error("condition not met in time");
    await Bun.sleep(10);
  }
}

const overloaded = (sandboxId = "sb-1") => ({
  slot: `hub:${sandboxId}`,
  sandboxId,
  event: "overloaded" as const,
  detail: { reason: "oom-kill", oomKillsDelta: 1 },
});

describe("watchdog autoUpsize propagation", () => {
  test("wake writes resolved thresholds to watchdog boot secrets and omits them at target size", async () => {
    const { handle, boat } = await setupHub();
    handle.state.updateHubSandbox("sb-1", (s) => {
      s.phase = "sleeping";
    });
    await handle.hub!.wakeHubSandbox("sb-1");

    const bootSecrets = () =>
      boat.uploads
        .filter((u) => u.remotePath === "~/.t3-sentinel/boot-secrets.json" && u.content)
        .map((u) => JSON.parse(u.content!) as Record<string, unknown>);
    expect(bootSecrets().at(-1)?.autoUpsize).toEqual({
      to: "xlarge",
      minAvailableMemPct: 15,
      minLoadPerCpu: 2,
      sustainSecs: 5,
      onOomKill: true,
      autoContinue: true,
    });

    handle.state.updateHubSandbox("sb-1", (s) => {
      s.size = "xlarge";
      s.phase = "sleeping";
    });
    await handle.hub!.wakeHubSandbox("sb-1");
    expect(bootSecrets().at(-1)).not.toHaveProperty("autoUpsize");
  });
});

describe("Hub automatic sandbox upsize", () => {
  test("overloaded event stops, persists the new size, resumes the same sandbox, and continues only interrupted top-level chats", async () => {
    const { handle, boat, vm, cfg, dir } = await setupHub();
    const projectId = handle.state.getHub()!.projectIds["my-app"];
    seedThread(handle, "sb-1", threadRow("thread-live", projectId, true));
    seedThread(handle, "sb-1", threadRow("thread-idle", projectId, false));
    seedThread(handle, "sb-1", threadRow("thread-archived", projectId, true, { archivedAt: new Date().toISOString() }));
    seedThread(handle, "sb-1", threadRow("thread-child", projectId, true, {
      lineage: { parentThreadId: "thread-live", relationshipToParent: "subagent", rootThreadId: "thread-live" },
    }));

    await handle.hub!.handleWatchdogEvent(overloaded());

    expect(boat.stopped).toEqual(["sb-1"]);
    expect(boat.resumed).toEqual([{ id: "sb-1", size: "xlarge" }]);
    const record = handle.state.getHubSandbox("sb-1")!;
    expect(record).toMatchObject({ size: "xlarge", upsizedFrom: "large", phase: "running" });
    expect(typeof record.upsizedAt).toBe("string");

    const continuations = vm.dispatches.filter((d) => d.type === "message.dispatch");
    expect(continuations).toHaveLength(1);
    const sent = continuations[0];
    expect(sent.threadId).toBe("thread-live");
    expect(sent.commandId).toBe(`cmd_upsize_continue_sb-1_thread-live_${record.upsizedAt}`);
    expect(sent.messageId).toBe(`msg_upsize_continue_sb-1_thread-live_${record.upsizedAt}`);
    expect(sent.dispatchMode).toEqual({ type: "start_immediately" });
    expect(sent.deliveryIntent).toBe("auto");
    expect(sent.createdBy).toBe("agent");
    expect(sent.creationSource).toBe("mcp");
    expect(String(sent.text)).toContain("large");
    expect(String(sent.text)).toContain("xlarge");
    expect(String(sent.text)).toContain("git status");

    // State normalization after a Hub restart keeps the upsize provenance and guard.
    const reloaded = new StateStore(dir, cfg);
    expect(reloaded.getHubSandbox("sb-1")).toMatchObject({ size: "xlarge", upsizedFrom: "large", upsizedAt: record.upsizedAt });
  });

  test("autoContinue false performs the resize without dispatching a continuation", async () => {
    const { handle, boat, vm } = await setupHub((cfg) => {
      cfg.boat.autoUpsize = { to: "xlarge", autoContinue: false, sustainSecs: 5 };
    });
    seedThread(handle, "sb-1", threadRow("thread-live", handle.state.getHub()!.projectIds["my-app"], true));
    await handle.hub!.handleWatchdogEvent(overloaded());
    expect(boat.resumed).toEqual([{ id: "sb-1", size: "xlarge" }]);
    expect(vm.dispatches.filter((d) => d.type === "message.dispatch")).toEqual([]);
  });

  test("a failing continuation dispatch is recorded while other chats still continue", async () => {
    const { handle, vm } = await setupHub();
    const projectId = handle.state.getHub()!.projectIds["my-app"];
    seedThread(handle, "sb-1", threadRow("thread-a", projectId, true));
    seedThread(handle, "sb-1", threadRow("thread-b", projectId, true));
    vm.failDispatches.add("thread-b");

    const result = await handle.hub!.upsizeHubSandbox("sb-1", "xlarge", { trigger: "test" });
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.continuedThreadIds).toEqual(["thread-a"]);
      expect(Object.keys(result.continueErrors)).toEqual(["thread-b"]);
      expect(result.continueErrors["thread-b"]).toContain("injected dispatch failure");
    }
    expect(vm.dispatches.map((d) => d.threadId).sort()).toEqual(["thread-a", "thread-b"]);
  });

  test("per-repo autoUpsize false ignores overloaded events even when enabled globally", async () => {
    const { handle, boat } = await setupHub((cfg) => {
      cfg.projects["my-app"].autoUpsize = false;
    });
    await handle.hub!.handleWatchdogEvent(overloaded());
    expect(boat.stopped).toEqual([]);
    expect(boat.resumed).toEqual([]);
    expect(handle.state.getHubSandbox("sb-1")!.size).toBe("large");
  });

  test("at most one automatic upsize: a second overloaded event is skipped", async () => {
    const { handle, boat } = await setupHub();
    await handle.hub!.handleWatchdogEvent(overloaded());
    await handle.hub!.handleWatchdogEvent(overloaded());
    expect(boat.stopped).toEqual(["sb-1"]);
    expect(boat.resumed).toEqual([{ id: "sb-1", size: "xlarge" }]);
  });

  test("simultaneous overloaded events share one in-flight resize", async () => {
    const { handle, boat } = await setupHub();
    await Promise.all([handle.hub!.handleWatchdogEvent(overloaded()), handle.hub!.handleWatchdogEvent(overloaded())]);
    expect(boat.stopped).toEqual(["sb-1"]);
    expect(boat.resumed).toEqual([{ id: "sb-1", size: "xlarge" }]);
  });

  test("direct upsize skips cleanly when already at target size or already upsized without force", async () => {
    const { handle, boat } = await setupHub();
    const first = await handle.hub!.upsizeHubSandbox("sb-1", "large", { trigger: "test" });
    expect(first).toMatchObject({ ok: false, skipped: "already-target-size" });
    const second = await handle.hub!.upsizeHubSandbox("sb-1", "xlarge", { trigger: "test" });
    expect(second.ok).toBe(true);
    const third = await handle.hub!.upsizeHubSandbox("sb-1", "xxlarge", { trigger: "test" });
    expect(third).toMatchObject({ ok: false, skipped: "already-upsized" });
    expect(boat.resumed).toEqual([{ id: "sb-1", size: "xlarge" }]);
  });
});

describe("manual Control API resize", () => {
  test("POST /api/hub/sandboxes/:id/resize validates input, returns 404, resizes, and force allows another resize", async () => {
    const { handle, boat, cfg } = await setupHub();
    const base = `http://127.0.0.1:${handle.server.port}`;
    const post = (sandboxId: string, body: unknown) =>
      fetch(`${base}/api/hub/sandboxes/${encodeURIComponent(sandboxId)}/resize`, {
        method: "POST",
        headers: { host: cfg.controlHost, authorization: `Bearer ${cfg.controlSecret}`, "content-type": "application/json" },
        body: JSON.stringify(body),
      });

    const missing = await post("sb-1", {});
    expect(missing.status).toBe(400);
    const badFlag = await post("sb-1", { size: "xlarge", autoContinue: "yes" });
    expect(badFlag.status).toBe(400);
    const badJson = await fetch(`${base}/api/hub/sandboxes/sb-1/resize`, {
      method: "POST",
      headers: { host: cfg.controlHost, authorization: `Bearer ${cfg.controlSecret}`, "content-type": "application/json" },
      body: "{not-json",
    });
    expect(badJson.status).toBe(400);
    const unknown = await post("missing-sb", { size: "xlarge" });
    expect(unknown.status).toBe(404);
    const unauthorized = await fetch(`${base}/api/hub/sandboxes/sb-1/resize`, {
      method: "POST",
      headers: { host: cfg.controlHost, "content-type": "application/json" },
      body: JSON.stringify({ size: "xlarge" }),
    });
    expect(unauthorized.status).toBe(401);

    const first = await post("sb-1", { size: "xlarge", autoContinue: false });
    expect(first.status).toBe(200);
    expect(await first.json()).toMatchObject({ ok: true, previousSize: "large", targetSize: "xlarge", continuedThreadIds: [] });
    expect(boat.resumed).toEqual([{ id: "sb-1", size: "xlarge" }]);

    // force:true in the Control API bypasses the one-automatic-upsize guard.
    const second = await post("sb-1", { size: "xxlarge", autoContinue: false });
    expect(second.status).toBe(200);
    expect(await second.json()).toMatchObject({ ok: true, previousSize: "xlarge", targetSize: "xxlarge" });
    expect(boat.stopped).toEqual(["sb-1", "sb-1"]);
    expect(boat.resumed).toEqual([
      { id: "sb-1", size: "xlarge" },
      { id: "sb-1", size: "xxlarge" },
    ]);
    expect(handle.state.getHubSandbox("sb-1")).toMatchObject({ size: "xxlarge", upsizedFrom: "xlarge" });
  });
});

describe("Hub sandbox pause reasons", () => {
  test("persist idle, watchdog-backstop, external-stop and settled-park reasons, and clear them on wake", async () => {
    const { handle, boat, cfg, dir } = await setupHub();
    const projectId = handle.state.getHub()!.projectIds["my-app"];
    const row = threadRow("thread-rest", projectId, false);
    seedThread(handle, "sb-1", row);

    await handle.hub!.handleWatchdogEvent({ slot: "hub:sb-1", sandboxId: "sb-1", event: "idle" });
    expect(handle.state.getHubSandbox("sb-1")?.sleepReason).toBe("Paused after inactivity (idle timeout)");
    const reloaded = new StateStore(dir, cfg);
    expect(reloaded.getHubSandbox("sb-1")?.sleepReason).toBe("Paused after inactivity (idle timeout)");

    await handle.hub!.wakeHubSandbox("sb-1");
    expect(handle.state.getHubSandbox("sb-1")?.sleepReason ?? null).toBeNull();

    await handle.hub!.handleWatchdogEvent({
      slot: "hub:sb-1",
      sandboxId: "sb-1",
      event: "idle",
      detail: { reason: "t3-unreachable" },
    });
    expect(handle.state.getHubSandbox("sb-1")?.sleepReason).toBe(
      "T3 server inside the VM became unreachable (watchdog backstop)",
    );

    await handle.hub!.wakeHubSandbox("sb-1");
    expect(handle.state.getHubSandbox("sb-1")?.sleepReason ?? null).toBeNull();

    const remote = boat.sandboxes.get("sb-1")!;
    remote.state = "stopped";
    remote.stoppedAt = new Date().toISOString();
    await handle.lifecycle.reaperTick();
    expect(handle.state.getHubSandbox("sb-1")?.sleepReason).toBe("Stopped externally in Boat (or by watchdog backstop)");

    row.settledAt = new Date().toISOString();
    expect(await handle.hub!.parkHubSandbox("sb-1", { trigger: "watchdog all-archived" })).toBe(true);
    expect(handle.state.getHubSandbox("sb-1")).toMatchObject({
      parkedAt: expect.any(String),
      sleepReason: "Settled (all chats settled; VM parked)",
    });

    await handle.hub!.wakeHubSandbox("sb-1");
    expect(handle.state.getHubSandbox("sb-1")?.sleepReason ?? null).toBeNull();
  });

  test("an in-flight upsize records the scaling reason and replaces the sleeping notice while the VM resumes", async () => {
    const { handle, boat } = await setupHub();
    seedThread(handle, "sb-1", threadRow("thread-scale", handle.state.getHub()!.projectIds["my-app"], false));
    let releaseResume!: () => void;
    boat.resumeGate = new Promise<void>((resolve) => {
      releaseResume = resolve;
    });

    const upsize = handle.hub!.upsizeHubSandbox("sb-1", "xlarge", { trigger: "test-trigger", autoContinue: false });
    await waitFor(() => handle.state.getHubSandbox("sb-1")?.phase === "sleeping");

    expect(handle.state.getHubSandbox("sb-1")?.sleepReason).toBe("Scaling VM from large to xlarge (test-trigger)");
    const notice = (
      handle.hub as unknown as { sleepNotice(sandboxId: string): { summary: string; detail?: string } }
    ).sleepNotice("sb-1");
    expect(notice.summary).toBe("Scaling this chat's VM from large to xlarge…");
    expect(notice.detail).toBe("Trigger: test-trigger. The same disk, worktree, and chat history will resume automatically.");

    releaseResume();
    expect((await upsize).ok).toBe(true);
    expect(handle.state.getHubSandbox("sb-1")?.sleepReason ?? null).toBeNull();
  });
});
