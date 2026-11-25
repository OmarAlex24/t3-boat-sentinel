import { afterEach, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { gzipSync } from "node:zlib";
import { SlotAuth, hashToken } from "./auth.ts";
import type { BoatClient, BoatExecResult, BoatSandbox, ResumeResult } from "./boat.ts";
import { expandEnvPlaceholders, loadConfig, type SentinelConfig } from "./config.ts";
import { createSentinelServer, type SentinelServerHandle } from "./index.ts";
import type { KlavClient } from "./klav.ts";
import { LifecycleManager } from "./lifecycle.ts";
import { formatRpcFailure } from "./hub-mcp.ts";
import { buildPlaceholderThread, failPlaceholderThread, placeholderInputFrom } from "./hub-placeholder.ts";
import {
  applySettingsOverlay,
  applySettingsUpdate,
  overlayReplayCalls,
  parseSettingsUpdate,
  recordSettingsUpdate,
  type SettingsUpdateInput,
} from "./hub-settings.ts";
import { handleMcpRequest } from "./mcp.ts";
import { CelestoProvider, SandboxSdkProvider, createSandboxProvider, type ExternalSdkProviderLike } from "./providers/index.ts";
import { asSandboxProvider, type ProviderCapability, type SandboxProvider } from "./providers/types.ts";
import { ProviderSessionStore } from "./provider-sessions.ts";
import { StateStore } from "./state.ts";
import { T3ReleaseResolver } from "./t3-release.ts";

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
  /** While set, `create` waits on it (a VM that is still booting). */
  createGate: Promise<void> | null = null;
  /** When set, `create` fails with it. */
  createError: Error | null = null;
  /** While set, `resume` waits on it (a VM that is still waking). */
  resumeGate: Promise<void> | null = null;
  /** When set, `resume` fails with it. */
  resumeError: Error | null = null;
  private seq = 1;

  async create(name: string, size: string, fromSnapshot?: string): Promise<string> {
    if (this.createGate) await this.createGate;
    if (this.createError) throw this.createError;
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
    if (this.resumeGate) await this.resumeGate;
    if (this.resumeError) throw this.resumeError;
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

  downloads: Array<{ id: string; remotePath: string; localPath: string }> = [];
  async download(id: string, remotePath: string, localPath: string): Promise<void> {
    this.downloads.push({ id, remotePath, localPath });
    throw new Error("no such file");
  }
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

describe("Celesto provider config", () => {
  function writeYaml(body: string): string {
    const dir = mkdtempSync(join(tmpdir(), "t3-sentinel-celesto-cfg-"));
    tempDirs.push(dir);
    const path = join(dir, "sentinel.yaml");
    writeFileSync(path, `domain: example.com\ncontrolSecret: s\nstateDir: ${dir}\nslots: [{ name: boat-1 }]\n${body}`);
    return path;
  }

  test("reads the API key from config or CELESTO_API_KEY; without either, config still loads", () => {
    const prev = process.env.CELESTO_API_KEY;
    try {
      delete process.env.CELESTO_API_KEY;
      expect(loadConfig(writeYaml("provider:\n  type: celesto\n")).provider).toEqual({ type: "celesto", apiKey: "" });
      // An unset ${VAR} placeholder counts as missing
      const unset = loadConfig(writeYaml("provider:\n  type: celesto\n  apiKey: ${CELESTO_KEY_UNSET_X}\n"));
      expect(unset.provider).toEqual({ type: "celesto", apiKey: "" });
      expect(createSandboxProvider(unset)).toBeInstanceOf(CelestoProvider);

      process.env.CELESTO_API_KEY = "cel_from_env";
      const fromEnv = loadConfig(writeYaml("provider:\n  type: celesto\n  user: ohm\n  sizeMap: { medium: standard }\n"));
      expect(fromEnv.provider).toEqual({ type: "celesto", apiKey: "cel_from_env", user: "ohm", sizeMap: { medium: "standard" } });
      const provider = createSandboxProvider(fromEnv);
      expect(provider).toBeInstanceOf(CelestoProvider);
      expect((provider as CelestoProvider).resolveSize("medium")).toBe("standard");

      const explicit = loadConfig(writeYaml("provider:\n  type: celesto\n  apiKey: cel_explicit\n"));
      expect(explicit.provider).toMatchObject({ type: "celesto", apiKey: "cel_explicit" });
    } finally {
      if (prev === undefined) delete process.env.CELESTO_API_KEY;
      else process.env.CELESTO_API_KEY = prev;
    }
  });

  test("CLI: daemon-only commands work without a Celesto key; serve refuses to start", async () => {
    const seen: string[] = [];
    const daemon = Bun.serve({
      port: 0,
      hostname: "127.0.0.1",
      async fetch(req) {
        seen.push(new URL(req.url).pathname);
        return Response.json({ ok: true });
      },
    });
    try {
      const cfgPath = writeYaml("provider:\n  type: celesto\n");
      const env: Record<string, string> = { ...(process.env as Record<string, string>), SENTINEL_CONFIG: cfgPath, SENTINEL_URL: `http://127.0.0.1:${daemon.port}` };
      delete env.CELESTO_API_KEY;
      // cwd outside the repo so Bun cannot auto-load a .env that carries CELESTO_API_KEY
      const cli = async (...args: string[]) => {
        const proc = Bun.spawn([process.execPath, join(import.meta.dir, "index.ts"), ...args], {
          cwd: dirname(cfgPath),
          env,
          stdout: "pipe",
          stderr: "pipe",
        });
        // A `serve` that did start would never exit on its own
        const timer = setTimeout(() => proc.kill(), 10_000);
        try {
          const [code, stderr] = await Promise.all([proc.exited, new Response(proc.stderr).text()]);
          return { code, stderr };
        } finally {
          clearTimeout(timer);
        }
      };
      for (const args of [["token", "hub", "x"], ["revoke", "hub"], ["forget-session", "antigravity"]]) {
        const res = await cli(...args);
        expect({ args, code: res.code, stderr: res.stderr }).toEqual({ args, code: 0, stderr: "" });
      }
      expect(seen).toEqual(["/api/token", "/api/revoke", "/api/forget-session"]);

      const serve = await cli("serve");
      expect(serve.code).not.toBe(0);
      expect(serve.stderr).toContain("CELESTO_API_KEY");
    } finally {
      daemon.stop(true);
    }
  });
});

describe("LifecycleManager hardening", () => {
  test("providers without snapshots or download: runtimes install at boot and stored sign-ins are never uploaded", async () => {
    const vm = Bun.serve({
      port: 0,
      hostname: "127.0.0.1",
      fetch() {
        return Response.json({ projects: [{ id: "vm-p1", title: "my-app", workspaceRoot: "/home/user/my-app" }], threads: [] });
      },
    });
    cleanups.push(() => vm.stop(true));

    const run = async (opts: { snapshots: boolean; download: boolean }) => {
      const base = makeTestConfig(0);
      const cfg: SentinelConfig = {
        ...base,
        t3: { ...base.t3, persistProviderSessions: ["antigravity"], prepareAcpAgents: ["devin"], installProviders: ["antigravity"] },
      };
      mkdirSync(join(cfg.stateDir, "credentials", "antigravity"), { recursive: true, mode: 0o700 });
      writeFileSync(join(cfg.stateDir, "credentials", "antigravity", "antigravity-oauth-token"), "ya29.SECRET", { mode: 0o600 });

      const boat = new MockBoat();
      boat.snapshots.add("env-my-app");
      boat.upstreamBaseUrl = `http://127.0.0.1:${vm.port}`;
      const legacy = asSandboxProvider(boat);
      const uploads: string[] = [];
      const execs: Array<{ script: string; timeout?: number }> = [];
      const provider: SandboxProvider = {
        ...legacy,
        name: "test-provider",
        capabilities: new Set<ProviderCapability>(opts.snapshots ? ["pause", "snapshots"] : ["pause"]),
        ...(opts.snapshots ? {} : { listSnapshots: undefined, saveSnapshot: undefined }),
        download: opts.download ? legacy.download : undefined,
        upload: async (_id, _local, remote) => {
          uploads.push(remote);
        },
        exec: (id, script, timeout) => {
          execs.push({ script, timeout });
          return legacy.exec(id, script, timeout);
        },
        execOk: (id, script, timeout) => {
          execs.push({ script, timeout });
          return legacy.execOk(id, script, timeout);
        },
      };
      const state = new StateStore(cfg.stateDir, cfg);
      const lifecycle = new LifecycleManager(cfg, state, provider);
      const res = await lifecycle.spawn("my-app", { slot: "boat-1" });
      expect(res.slot.phase).toBe("running");
      const provision = execs.find((e) => e.script.includes("bash ~/.t3-sentinel/provision-base.sh"))!;
      return { uploads, provision };
    };

    // Celesto-like: no snapshots, no download
    const celestoLike = await run({ snapshots: false, download: false });
    expect(celestoLike.uploads.some((r) => r.includes("provider-sessions"))).toBe(false);
    expect(celestoLike.provision.script).toContain("T3_PREPARE_ACP_AGENTS='devin'");
    expect(celestoLike.provision.script).toContain("T3_INSTALL_PROVIDERS='antigravity'");
    expect(celestoLike.provision.timeout).toBe(900);

    // Boat-like: snapshots + download keep today's behavior
    const boatLike = await run({ snapshots: true, download: true });
    expect(boatLike.uploads.some((r) => r.includes("provider-sessions"))).toBe(true);
    expect(boatLike.provision.script).not.toContain("T3_INSTALL_PROVIDERS");
    expect(boatLike.provision.script).not.toContain("T3_PREPARE_ACP_AGENTS");
    expect(boatLike.provision.timeout).toBe(300);
  });

  test("t3.version latest: exact version for providers without snapshots, unchanged with snapshots", async () => {
    const vm = Bun.serve({
      port: 0,
      hostname: "127.0.0.1",
      fetch() {
        return Response.json({ projects: [{ id: "vm-p1", title: "my-app", workspaceRoot: "/home/user/my-app" }], threads: [] });
      },
    });
    cleanups.push(() => vm.stop(true));

    const run = async (opts: { snapshots: boolean; githubStatus?: number }) => {
      const base = makeTestConfig(0);
      const cfg: SentinelConfig = { ...base, t3: { ...base.t3, version: "latest", channel: "nightly" } };
      const boat = new MockBoat();
      boat.snapshots.add("env-my-app");
      boat.upstreamBaseUrl = `http://127.0.0.1:${vm.port}`;
      const legacy = asSandboxProvider(boat);
      const scripts: string[] = [];
      const provider: SandboxProvider = {
        ...legacy,
        name: "test-provider",
        capabilities: new Set<ProviderCapability>(opts.snapshots ? ["pause", "snapshots"] : ["pause"]),
        ...(opts.snapshots ? {} : { listSnapshots: undefined, saveSnapshot: undefined }),
        execOk: (id, script, timeout) => {
          scripts.push(script);
          return legacy.execOk(id, script, timeout);
        },
      };
      const githubCalls: string[] = [];
      const t3Releases = new T3ReleaseResolver({
        env: {},
        log: () => {},
        fetch: async (url) => {
          githubCalls.push(url);
          if (opts.githubStatus) return new Response("{}", { status: opts.githubStatus });
          return Response.json([{ tag_name: "v0.1.11" }, { tag_name: "v0.1.12-nightly.20261124.5209" }]);
        },
      });
      const state = new StateStore(cfg.stateDir, cfg);
      const lifecycle = new LifecycleManager(cfg, state, provider, undefined, { t3Releases });
      await lifecycle.spawn("my-app", { slot: "boat-1" });
      const provision = scripts.find((s) => s.includes("bash ~/.t3-sentinel/provision-base.sh"))!;
      return { provision, githubCalls };
    };

    const celestoLike = await run({ snapshots: false });
    expect(celestoLike.provision).toContain("T3_VERSION='0.1.12-nightly.20261124.5209'");
    expect(celestoLike.githubCalls).toHaveLength(1);

    const boatLike = await run({ snapshots: true });
    expect(boatLike.provision).toContain("T3_VERSION='latest'");
    expect(boatLike.githubCalls).toHaveLength(0);

    // Lookup failure: the VM falls back to the installer's own channel lookup
    const fallback = await run({ snapshots: false, githubStatus: 403 });
    expect(fallback.provision).toContain("T3_VERSION='latest'");
    expect(fallback.provision).toContain("T3_CHANNEL='nightly'");
  });

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

  test("base bakes install and verify provider runtimes; project bakes upload settings and verify only", async () => {
    const config = makeTestConfig();
    const settingsFile = join(config.stateDir, "t3-settings.json");
    writeFileSync(settingsFile, JSON.stringify({ providerInstances: {} }));
    config.t3 = { ...config.t3, settingsFile, prepareAcpAgents: ["devin"], installProviders: ["antigravity"] };
    const state = new StateStore(config.stateDir, config);
    const boat = new MockBoat();
    const uploads: Array<{ local: string; remote: string }> = [];
    boat.upload = async (...args: unknown[]) => {
      uploads.push({ local: String(args[1]), remote: String(args[2]) });
    };
    const lifecycle = new LifecycleManager(config, state, boat);
    const bakeScript = () => boat.execScripts.filter((s) => s.includes("BAKE_CLEAN=1")).at(-1) ?? "";
    const settingsUploads = () => uploads.filter((u) => u.remote === "~/.t3-sentinel/t3-settings.json");

    await lifecycle.bake("base");
    expect(bakeScript()).toContain("T3_PREPARE_ACP_AGENTS='devin'");
    expect(bakeScript()).toContain("T3_INSTALL_PROVIDERS='antigravity'");
    expect(bakeScript()).not.toContain("T3_VERIFY_");
    expect(settingsUploads().map((u) => u.local)).toEqual([settingsFile]);

    await lifecycle.bake("my-app");
    expect(bakeScript()).toContain("T3_VERIFY_ACP_AGENTS='devin'");
    expect(bakeScript()).toContain("T3_VERIFY_PROVIDERS='antigravity'");
    expect(bakeScript()).not.toContain("T3_PREPARE_ACP_AGENTS");
    expect(bakeScript()).not.toContain("T3_INSTALL_PROVIDERS");
    expect(settingsUploads().map((u) => u.local)).toEqual([settingsFile, settingsFile]);
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

  function startFakeVmT3Server(
    vmTag: string,
    opts: {
      /** Answer `subscribeServerConfig` with this snapshot and keep the stream open (default: plain Exit). */
      configSnapshot?: Record<string, unknown>;
      /** Answer `server.refreshProviders` with these providers. */
      refreshProviders?: unknown[];
      /** Apply `server.updateSettings` to `configSnapshot.settings` and answer with the result, like T3. */
      applySettings?: boolean;
      /** Fail `orchestration.launchThread` with this message. */
      rejectLaunch?: string;
      /** With `rejectLaunch`: report the thread on the shell stream just after the failure. */
      lateShellItemAfterReject?: boolean;
      /** Fail `orchestration.getThreadProjection` (the Hub then has no copy of a sleeping chat). */
      failProjection?: boolean;
    } = {},
  ) {
    let vmSettings = (opts.configSnapshot?.settings ?? null) as Record<string, unknown> | null;
    const seenMethods: Array<{ tag: string; payload: unknown }> = [];
    const acks: string[] = [];
    const vmProjectId = `vm-proj-${vmTag}`;
    let configStream: { ws: { sendText(data: string): unknown }; requestId: string } | null = null;
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
          if (frame._tag === "Ack") {
            acks.push(String((frame as { requestId?: unknown }).requestId));
            return;
          }
          if (frame._tag !== "Request" || !frame.id || !frame.tag) return;
          seenMethods.push({ tag: frame.tag, payload: frame.payload });

          if (frame.tag === "subscribeServerConfig" && opts.configSnapshot) {
            configStream = { ws, requestId: frame.id };
            ws.sendText(
              JSON.stringify({
                _tag: "Chunk",
                requestId: frame.id,
                values: [{ version: 1, type: "snapshot", config: opts.configSnapshot }],
              }),
            );
            return;
          }
          if (frame.tag === "provider.install.subscribe" || frame.tag === "provider.auth.subscribe") {
            // Like a real VM: one snapshot, stream stays open
            ws.sendText(
              JSON.stringify({
                _tag: "Chunk",
                requestId: frame.id,
                values: [{ instanceId: frame.payload?.instanceId, phase: "idle", fromVm: vmTag }],
              }),
            );
            return;
          }
          if (frame.tag === "server.updateSettings" && opts.applySettings && vmSettings) {
            const parsed = parseSettingsUpdate(frame.payload);
            if (!parsed.ok) {
              ws.sendText(JSON.stringify({ _tag: "Exit", requestId: frame.id, exit: { _tag: "Failure", cause: [{ _tag: "Die", defect: parsed.message }] } }));
              return;
            }
            // Like T3: the patch, then the instance mutation replacing the whole instance
            vmSettings = applySettingsUpdate(vmSettings, { patch: parsed.value.patch });
            const m = parsed.value.providerInstanceMutation;
            if (m) {
              const instances = { ...(vmSettings.providerInstances as Record<string, unknown>) };
              if (m.operation === "remove") delete instances[m.instanceId];
              else instances[m.instanceId] = m.instance;
              vmSettings = { ...vmSettings, providerInstances: instances };
            }
            ws.sendText(JSON.stringify({ _tag: "Exit", requestId: frame.id, exit: { _tag: "Success", value: vmSettings } }));
            return;
          }
          if (frame.tag === "server.refreshProviders" && opts.refreshProviders) {
            ws.sendText(
              JSON.stringify({
                _tag: "Exit",
                requestId: frame.id,
                exit: { _tag: "Success", value: { providers: opts.refreshProviders } },
              }),
            );
            return;
          }

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
          if (frame.tag === "orchestration.launchThread" && opts.rejectLaunch) {
            ws.sendText(
              JSON.stringify({
                _tag: "Exit",
                requestId: frame.id,
                exit: { _tag: "Failure", cause: [{ _tag: "Die", defect: opts.rejectLaunch }] },
              }),
            );
            if (opts.lateShellItemAfterReject) {
              const thread = { id: String(frame.payload?.threadId), projectId: vmProjectId, title: "Half made", status: "idle" };
              setTimeout(() => {
                ws.sendText(
                  JSON.stringify({
                    _tag: "Chunk",
                    requestId: "hub-shell",
                    values: [{ kind: "thread.updated", sequence: 3, location: "active", thread }],
                  }),
                );
              }, 20);
            }
            return;
          }
          if (frame.tag === "scheduledTasks.subscribe") {
            ws.sendText(JSON.stringify({ _tag: "Chunk", requestId: frame.id, values: [{ tasks: [] }] }));
            return;
          }
          if (frame.tag === "orchestration.getThreadProjection" && opts.failProjection) {
            ws.sendText(
              JSON.stringify({ _tag: "Exit", requestId: frame.id, exit: { _tag: "Failure", cause: [{ _tag: "Die", defect: "busy" }] } }),
            );
            return;
          }
          if (frame.tag === "orchestration.getThreadProjection") {
            const threadId = String(frame.payload?.threadId);
            ws.sendText(
              JSON.stringify({
                _tag: "Exit",
                requestId: frame.id,
                exit: {
                  _tag: "Success",
                  value: {
                    thread: { id: threadId, projectId: vmProjectId, title: "Cached chat" },
                    runs: [],
                    messages: [{ id: `vm-msg-${threadId}`, role: "user", text: "What we said before" }],
                    turnItems: [],
                    visibleTurnItems: [],
                  },
                },
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
    const pushConfigItems = (values: unknown[]) => {
      if (!configStream) throw new Error("no open subscribeServerConfig stream on fake VM");
      configStream.ws.sendText(JSON.stringify({ _tag: "Chunk", requestId: configStream.requestId, values }));
    };
    return {
      server,
      url: `http://127.0.0.1:${server.port}`,
      seenMethods,
      acks,
      vmProjectId,
      pushConfigItems,
      getSettings: () => vmSettings,
    };
  }

  /** Pairs with the Hub and opens one client WebSocket; returns helpers to send and await frames. */
  async function connectHubClient(handle: SentinelServerHandle, cfg: SentinelConfig) {
    const base = `http://127.0.0.1:${handle.server.port}`;
    const pairRes = await fetch(`${base}/api/pair`, {
      method: "POST",
      headers: { host: cfg.controlHost, authorization: `Bearer ${cfg.controlSecret}`, "content-type": "application/json" },
      body: JSON.stringify({ slot: "hub" }),
    });
    const { code } = (await pairRes.json()) as { code: string };
    const tokRes = await fetch(`${base}/oauth/token`, {
      method: "POST",
      headers: { host: cfg.hub!.host, "content-type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({ subject_token: code, client_label: "Mac" }),
    });
    const { access_token: bearer } = (await tokRes.json()) as { access_token: string };
    const tickRes = await fetch(`${base}/api/auth/websocket-ticket/rpc`, {
      method: "POST",
      headers: { host: cfg.hub!.host, authorization: `Bearer ${bearer}` },
    });
    const { ticket } = (await tickRes.json()) as { ticket: string };

    const ws = new WebSocket(`ws://127.0.0.1:${handle.server.port}/ws?wsTicket=${ticket}&orchestrationProtocol=2`, {
      headers: { host: cfg.hub!.host },
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
          setTimeout(r, 40);
        });
      }
      throw new Error(`Timed out waiting for frame; inbox=${JSON.stringify(inbox)}`);
    };
    const request = (id: string | number, tag: string, payload: unknown = {}) =>
      ws.send(JSON.stringify({ _tag: "Request", id, tag, payload, headers: [] }));
    return { ws, inbox, waitForFrame, request, bearer, base };
  }

  type ProviderStub = { instanceId: string; installed?: boolean };
  const providerById = (providers: unknown, id: string) =>
    (providers as ProviderStub[] | undefined)?.find((p) => p.instanceId === id);
  const providerStatusesChunk = (requestId: string | number, id: string, installed: boolean) =>
    (f: Record<string, unknown>) =>
      f._tag === "Chunk" &&
      f.requestId === requestId &&
      (f.values as Array<{ type?: string; payload?: { providers?: unknown } }>)?.some(
        (v) => v.type === "providerStatuses" && providerById(v.payload?.providers, id)?.installed === installed,
      );

  async function startHubWithVm(
    vm: ReturnType<typeof startFakeVmT3Server>,
    dirPrefix: string,
    opts: { persistProviderSessions?: string[]; beforeStart?: (dir: string) => void; maxSandboxes?: number } = {},
  ) {
    const dir = mkdtempSync(join(tmpdir(), dirPrefix));
    cleanups.push(() => rmSync(dir, { recursive: true, force: true }));
    const boat = new MockBoat();
    boat.snapshots.add("base-t3");
    boat.snapshots.add("env-my-app");
    boat.upstreamBaseUrl = vm.url;
    const base = makeTestConfig(0);
    const cfg: SentinelConfig = {
      ...base,
      stateDir: dir,
      t3: { ...base.t3, ...(opts.persistProviderSessions ? { persistProviderSessions: opts.persistProviderSessions } : {}) },
      hub: { host: "boat.boat.example.com", label: "Boat", maxSandboxes: opts.maxSandboxes ?? 5 },
    };
    opts.beforeStart?.(dir);
    const handle = createSentinelServer(cfg, boat, { startBackgroundLoops: false });
    handle.lifecycle.providerSessions.debounceMs = 10;
    cleanups.push(() => handle.stop());
    return { cfg, handle, boat, dir };
  }

  /** Opens the client's shell stream ("shell") and returns the Hub project id. */
  async function subscribeHubShell(client: Awaited<ReturnType<typeof connectHubClient>>): Promise<string> {
    client.request("shell", "orchestration.subscribeShell");
    const shellSnap = await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "shell");
    return String(
      (((shellSnap.values as Array<Record<string, unknown>>)[0].snapshot as Record<string, unknown>).projects as Array<
        Record<string, unknown>
      >)[0].id,
    );
  }

  /** An `orchestration.launchThread` payload shaped like the T3 app's first send in a new chat. */
  function clientLaunchPayload(threadId: string, projectId: string, text = "Hello", title = "Providers") {
    return {
      commandId: `cmd-${threadId}`,
      creationSource: "web",
      threadId,
      projectId,
      title,
      generateTitle: true,
      modelSelection: { instanceId: "claudeAgent", model: "claude-opus-5-5" },
      runtimeMode: "full-access",
      interactionMode: "default",
      workspaceStrategy: { type: "root" },
      initialMessage: { messageId: `msg-${threadId}`, text, attachments: [] },
    };
  }

  /** A shell chunk on the client's "shell" stream carrying `thread.updated` for this thread. */
  const shellThreadUpdate =
    (threadId: string, pred: (thread: Record<string, unknown>) => boolean = () => true) =>
    (f: Record<string, unknown>) =>
      f._tag === "Chunk" &&
      f.requestId === "shell" &&
      (f.values as Array<{ kind?: string; thread?: Record<string, unknown> }>)?.some(
        (v) => v.kind === "thread.updated" && v.thread?.id === threadId && pred(v.thread),
      );

  /** Launches a chat like the app does and waits until the thread exists on its VM. */
  async function launchThreadOnHub(client: Awaited<ReturnType<typeof connectHubClient>>, threadId: string) {
    const hubProjectId = await subscribeHubShell(client);
    client.request(`launch-${threadId}`, "orchestration.launchThread", clientLaunchPayload(threadId, hubProjectId));
    const exit = await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === `launch-${threadId}`);
    expect((exit.exit as Record<string, unknown>)._tag).toBe("Success");
    // The Hub answers at once; the VM's own shell update says the thread exists there
    await client.waitForFrame(shellThreadUpdate(threadId, (t) => t.status === "running"));
  }

  describe("new chats open at once while their VM boots", () => {
    type HubClient = Awaited<ReturnType<typeof connectHubClient>>;
    const exitOf = (f: Record<string, unknown>) =>
      f.exit as { _tag: string; value?: Record<string, unknown>; cause?: unknown };
    const until = async (cond: () => boolean, timeoutMs = 5_000) => {
      const start = Date.now();
      while (!cond()) {
        if (Date.now() - start > timeoutMs) throw new Error("condition not met in time");
        await Bun.sleep(20);
      }
    };
    /** A VM create that waits until `release()`. */
    const holdCreate = (boat: MockBoat) => {
      let release!: () => void;
      boat.createGate = new Promise<void>((r) => (release = r));
      return () => {
        boat.createGate = null;
        release();
      };
    };
    const httpThreadSnapshot = (client: HubClient, cfg: SentinelConfig, threadId: string) =>
      fetch(`${client.base}/api/orchestration/threads/${threadId}/bounded`, {
        headers: { host: cfg.hub!.host, authorization: `Bearer ${client.bearer}` },
      });

    test("replies to launchThread at once with a placeholder chat, before the VM exists", async () => {
      const vm = startFakeVmT3Server("vm-instant");
      cleanups.push(() => vm.server.stop(true));
      const { cfg, handle, boat } = await startHubWithVm(vm, "t3-sentinel-hub-instant-");
      const release = holdCreate(boat);
      cleanups.push(release);
      const client = await connectHubClient(handle, cfg);
      const hubProjectId = await subscribeHubShell(client);

      const sentAt = Date.now();
      client.request("launch", "orchestration.launchThread", clientLaunchPayload("thread-fast", hubProjectId, "Fix the flaky test"));
      await until(() => client.inbox.some((f) => f._tag === "Exit" && f.requestId === "launch"), 2_000);
      expect(Date.now() - sentAt).toBeLessThan(1_000);
      // The sidebar row arrives before the reply, so the app can open the chat right away
      const rowIdx = client.inbox.findIndex(shellThreadUpdate("thread-fast", (t) => t.status === "preparing"));
      const exitIdx = client.inbox.findIndex((f) => f._tag === "Exit" && f.requestId === "launch");
      expect(rowIdx).toBeGreaterThanOrEqual(0);
      expect(rowIdx).toBeLessThan(exitIdx);

      const exit = exitOf(client.inbox[exitIdx]);
      expect(exit._tag).toBe("Success");
      expect(exit.value!.threadId).toBe("thread-fast");
      expect(exit.value!.resumed).toBe(false);
      expect("sequence" in exit.value!).toBe(false);
      const projection = exit.value!.projection as {
        thread: { id: string; projectId: string };
        runs: Array<{ status: string; userMessageId: string }>;
        messages: Array<{ id: string; text: string }>;
      };
      expect(projection.thread).toMatchObject({ id: "thread-fast", projectId: hubProjectId });
      expect(projection.runs).toMatchObject([{ status: "preparing", userMessageId: "msg-thread-fast" }]);
      expect(projection.messages).toMatchObject([{ id: "msg-thread-fast", text: "Fix the flaky test" }]);
      expect(boat.created).toHaveLength(0);
      expect(vm.seenMethods.some((m) => m.tag === "orchestration.launchThread")).toBe(false);

      // The thread view is served by the Hub until the VM is up
      client.request("sub", "orchestration.subscribeThread", { threadId: "thread-fast", requestCompletionMarker: true });
      const sub = await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "sub");
      const [snap, synced] = sub.values as Array<{ kind: string; snapshotSequence?: number; projection?: unknown }>;
      expect(snap).toMatchObject({ kind: "snapshot", snapshotSequence: 0, projection: { thread: { id: "thread-fast" } } });
      expect(synced.kind).toBe("synchronized");
      // Reads the app sends on opening a chat are answered, not held until the VM is up
      client.request("family", "orchestration.getThreadFamily", { threadId: "thread-fast" });
      const family = exitOf(await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "family", 1_000));
      expect((family.value!.threads as Array<{ id: string }>).map((t) => t.id)).toEqual(["thread-fast"]);
      client.request("visit", "orchestration.dispatchCommand", {
        type: "thread.visit",
        commandId: "cmd-visit-fast",
        threadId: "thread-fast",
        visitedAt: new Date().toISOString(),
      });
      expect(exitOf(await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "visit", 1_000))._tag).toBe("Success");
      // HTTP snapshots answer 503, never another VM's 404 (the app would mark the chat deleted)
      expect((await httpThreadSnapshot(client, cfg, "thread-fast")).status).toBe(503);
      // A re-sent launch (the app re-sends after a socket loss) gets the same chat, no second VM
      client.request("launch-again", "orchestration.launchThread", clientLaunchPayload("thread-fast", hubProjectId, "Fix the flaky test"));
      const again = await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "launch-again");
      expect(exitOf(again).value!.threadId).toBe("thread-fast");
      // A new subscriber (another window, or after a reconnect) sees the chat in the shell snapshot
      client.request("shell-2", "orchestration.subscribeShell");
      const shell2 = await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "shell-2");
      const threads = ((shell2.values as Array<{ snapshot: { threads: Array<{ id: string }> } }>)[0].snapshot.threads);
      expect(threads.map((t) => t.id)).toContain("thread-fast");

      release();
      await client.waitForFrame(shellThreadUpdate("thread-fast", (t) => t.status === "running"));
      expect(boat.created).toHaveLength(1);
      expect(handle.state.getHub()!.pendingLaunches).toBeUndefined();
    });

    test("delivers the first message and messages sent while booting once the VM is ready", async () => {
      const vm = startFakeVmT3Server("vm-queue");
      cleanups.push(() => vm.server.stop(true));
      const { cfg, handle, boat } = await startHubWithVm(vm, "t3-sentinel-hub-queue-");
      const release = holdCreate(boat);
      cleanups.push(release);
      const client = await connectHubClient(handle, cfg);
      const hubProjectId = await subscribeHubShell(client);

      client.request("setup", "subscribeWorktreeSetup", { threadId: "thread-q" });
      client.request("launch", "orchestration.launchThread", clientLaunchPayload("thread-q", hubProjectId, "Write the migration"));
      expect(exitOf(await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "launch"))._tag).toBe("Success");
      // The client holds the placeholder (sequence 0) and subscribes with that cursor
      client.request("sub", "orchestration.subscribeThread", { threadId: "thread-q", afterSequence: 0 });
      await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "sub");
      client.request("msg-2", "orchestration.dispatchCommand", {
        type: "message.dispatch",
        commandId: "cmd-2",
        threadId: "thread-q",
        messageId: "msg-2",
        text: "Also add a test",
        attachments: [],
      });
      await Bun.sleep(100);
      // Held, not failed, while the VM boots
      expect(client.inbox.some((f) => f._tag === "Exit" && f.requestId === "msg-2")).toBe(false);

      release();
      const msgExit = await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "msg-2");
      expect(exitOf(msgExit)._tag).toBe("Success");
      await client.waitForFrame(shellThreadUpdate("thread-q", (t) => t.status === "running"));

      const tags = vm.seenMethods.map((m) => m.tag);
      const launch = vm.seenMethods.find((m) => m.tag === "orchestration.launchThread")!.payload as Record<string, unknown>;
      expect(launch).toMatchObject({
        threadId: "thread-q",
        projectId: vm.vmProjectId,
        commandId: "cmd-thread-q",
        initialMessage: { messageId: "msg-thread-q", text: "Write the migration" },
      });
      const queuedIdx = vm.seenMethods.findIndex(
        (m) => m.tag === "orchestration.dispatchCommand" && (m.payload as { messageId?: string }).messageId === "msg-2",
      );
      expect(tags.indexOf("orchestration.launchThread")).toBeLessThan(queuedIdx);
      // The thread stream moved to the VM without the placeholder's cursor, so it gets a full snapshot
      await until(() => vm.seenMethods.some((m) => m.tag === "orchestration.subscribeThread"));
      const forwardedSub = vm.seenMethods.find((m) => m.tag === "orchestration.subscribeThread")!.payload as Record<string, unknown>;
      expect(forwardedSub.threadId).toBe("thread-q");
      expect("afterSequence" in forwardedSub).toBe(false);
      // The setup card finished
      await client.waitForFrame(
        (f) => f._tag === "Chunk" && f.requestId === "setup" && (f.values as Array<{ phase?: string }>)[0]?.phase === "done",
      );
      // HTTP snapshots now go to the chat's own VM
      expect((await httpThreadSnapshot(client, cfg, "thread-q")).status).toBe(200);
    });

    test("shows a VM spawn failure in the chat instead of dropping it", async () => {
      const vm = startFakeVmT3Server("vm-fail");
      cleanups.push(() => vm.server.stop(true));
      const { cfg, handle, boat } = await startHubWithVm(vm, "t3-sentinel-hub-fail-");
      boat.createError = new Error("no capacity in region eu-1");
      const client = await connectHubClient(handle, cfg);
      const hubProjectId = await subscribeHubShell(client);

      client.request("setup", "subscribeWorktreeSetup", { threadId: "thread-x" });
      client.request("launch", "orchestration.launchThread", clientLaunchPayload("thread-x", hubProjectId));
      expect(exitOf(await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "launch"))._tag).toBe("Success");

      const failedRow = await client.waitForFrame(shellThreadUpdate("thread-x", (t) => t.status === "failed"));
      const row = (failedRow.values as Array<{ thread: Record<string, unknown> }>)[0].thread;
      expect(String(row.lastError)).toContain("no capacity in region eu-1");
      const setupFailed = await client.waitForFrame(
        (f) => f._tag === "Chunk" && f.requestId === "setup" && (f.values as Array<{ phase?: string }>)[0]?.phase === "failed",
      );
      expect(String((setupFailed.values as Array<{ error: string }>)[0].error)).toContain("no capacity in region eu-1");

      // The chat stays: in the shell, in its thread view (run failed), and in state for a Hub restart
      expect(client.inbox.some((f) => f._tag === "Chunk" && (f.values as Array<{ kind?: string } | null>)?.some((v) => v?.kind === "thread.removed"))).toBe(false);
      client.request("sub", "orchestration.subscribeThread", { threadId: "thread-x" });
      const sub = await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "sub");
      const projection = (sub.values as Array<{ projection: { runs: Array<{ status: string }> } }>)[0].projection;
      expect(projection.runs[0].status).toBe("failed");
      expect(handle.state.getHub()!.pendingLaunches!["thread-x"]).toMatchObject({ phase: "failed" });
      // Sending to it fails with the reason
      client.request("msg", "orchestration.dispatchCommand", {
        type: "message.dispatch",
        commandId: "cmd-x2",
        threadId: "thread-x",
        messageId: "msg-x2",
        text: "retry?",
        attachments: [],
      });
      const msgExit = exitOf(await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "msg"));
      expect(msgExit._tag).toBe("Failure");
      expect(JSON.stringify(msgExit.cause)).toContain("no capacity in region eu-1");
      // Archiving the failed chat removes it
      client.request("archive", "orchestration.dispatchCommand", { type: "thread.archive", commandId: "cmd-arch", threadId: "thread-x" });
      expect(exitOf(await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "archive"))._tag).toBe("Success");
      expect(handle.state.getHub()!.pendingLaunches).toBeUndefined();
    });

    test("keeps the chat and destroys the VM when the VM rejects the launch", async () => {
      const vm = startFakeVmT3Server("vm-reject", { rejectLaunch: "workspace is not a git repository" });
      cleanups.push(() => vm.server.stop(true));
      const { cfg, handle, boat } = await startHubWithVm(vm, "t3-sentinel-hub-reject-");
      const client = await connectHubClient(handle, cfg);
      const hubProjectId = await subscribeHubShell(client);

      client.request("launch", "orchestration.launchThread", clientLaunchPayload("thread-r", hubProjectId));
      expect(exitOf(await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "launch"))._tag).toBe("Success");
      const failedRow = await client.waitForFrame(shellThreadUpdate("thread-r", (t) => t.status === "failed"));
      expect(String((failedRow.values as Array<{ thread: { lastError: string } }>)[0].thread.lastError)).toContain(
        "workspace is not a git repository",
      );
      await until(() => boat.deleted.length === 1);
      expect(handle.state.listHubSandboxes()).toHaveLength(0);
      // Destroying the VM did not remove the failed chat
      expect(client.inbox.some((f) => f._tag === "Chunk" && (f.values as Array<{ kind?: string } | null>)?.some((v) => v?.kind === "thread.removed"))).toBe(false);
    });

    test("archiving a chat while its VM boots cancels the launch and destroys the VM", async () => {
      const vm = startFakeVmT3Server("vm-archive");
      cleanups.push(() => vm.server.stop(true));
      const { cfg, handle, boat } = await startHubWithVm(vm, "t3-sentinel-hub-archive-");
      const release = holdCreate(boat);
      cleanups.push(release);
      const client = await connectHubClient(handle, cfg);
      const hubProjectId = await subscribeHubShell(client);

      client.request("launch", "orchestration.launchThread", clientLaunchPayload("thread-a", hubProjectId));
      expect(exitOf(await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "launch"))._tag).toBe("Success");
      client.request("archive", "orchestration.dispatchCommand", { type: "thread.archive", commandId: "cmd-arch", threadId: "thread-a" });
      expect(exitOf(await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "archive"))._tag).toBe("Success");
      const removed = await client.waitForFrame(
        (f) =>
          f._tag === "Chunk" &&
          f.requestId === "shell" &&
          (f.values as Array<{ kind?: string; threadId?: string }>).some((v) => v.kind === "thread.removed" && v.threadId === "thread-a"),
      );
      expect(removed).toBeTruthy();
      expect(handle.state.getHub()!.pendingLaunches).toBeUndefined();

      release();
      await until(() => boat.deleted.length === 1);
      expect(boat.created).toHaveLength(1);
      expect(vm.seenMethods.some((m) => m.tag === "orchestration.launchThread")).toBe(false);
      expect(handle.state.listHubSandboxes()).toHaveLength(0);
    });

    test("counts booting chats toward maxSandboxes and refuses the launch over the limit", async () => {
      const vm = startFakeVmT3Server("vm-cap");
      cleanups.push(() => vm.server.stop(true));
      const { cfg, handle, boat } = await startHubWithVm(vm, "t3-sentinel-hub-cap-", { maxSandboxes: 1 });
      const release = holdCreate(boat);
      cleanups.push(release);
      const client = await connectHubClient(handle, cfg);
      const hubProjectId = await subscribeHubShell(client);

      client.request("launch-1", "orchestration.launchThread", clientLaunchPayload("thread-c1", hubProjectId));
      expect(exitOf(await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "launch-1"))._tag).toBe("Success");
      client.request("launch-2", "orchestration.launchThread", clientLaunchPayload("thread-c2", hubProjectId));
      const second = exitOf(await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "launch-2"));
      // A failed request lets the app restore the draft; no row was added for it
      expect(second._tag).toBe("Failure");
      expect(JSON.stringify(second.cause)).toContain("Hub sandbox limit reached (1/1)");
      expect(client.inbox.some(shellThreadUpdate("thread-c2"))).toBe(false);
      release();
      await client.waitForFrame(shellThreadUpdate("thread-c1", (t) => t.status === "running"));
    });

    test("a Hub restart while a VM boots resumes the launch", async () => {
      const vm = startFakeVmT3Server("vm-restart");
      cleanups.push(() => vm.server.stop(true));
      const { cfg, handle, boat } = await startHubWithVm(vm, "t3-sentinel-hub-restart-");
      holdCreate(boat); // this VM never finishes: the Hub goes down first
      const client = await connectHubClient(handle, cfg);
      const hubProjectId = await subscribeHubShell(client);
      client.request("launch", "orchestration.launchThread", clientLaunchPayload("thread-s", hubProjectId, "Survive restarts"));
      expect(exitOf(await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "launch"))._tag).toBe("Success");
      expect(handle.state.getHub()!.pendingLaunches!["thread-s"]).toMatchObject({ phase: "booting" });
      handle.stop();

      const boat2 = new MockBoat();
      boat2.snapshots.add("base-t3");
      boat2.snapshots.add("env-my-app");
      boat2.upstreamBaseUrl = vm.url;
      const handle2 = createSentinelServer(cfg, boat2, { startBackgroundLoops: false });
      cleanups.push(() => handle2.stop());
      const client2 = await connectHubClient(handle2, cfg);
      // Restored in the shell, then started on a fresh VM with the original first message
      await until(() => vm.seenMethods.some((m) => m.tag === "orchestration.launchThread"));
      const launch = vm.seenMethods.find((m) => m.tag === "orchestration.launchThread")!.payload as Record<string, unknown>;
      expect(launch).toMatchObject({ threadId: "thread-s", initialMessage: { text: "Survive restarts" } });
      expect(boat2.created).toHaveLength(1);
      await until(() => handle2.state.getHub()!.pendingLaunches === undefined);
      client2.request("shell", "orchestration.subscribeShell");
      const shell = await client2.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "shell");
      const threads = (shell.values as Array<{ snapshot: { threads: Array<{ id: string }> } }>)[0].snapshot.threads;
      expect(threads.map((t) => t.id)).toContain("thread-s");
    });

    test("a failed chat stays even if its VM reports the thread after the failure", async () => {
      const vm = startFakeVmT3Server("vm-late", { rejectLaunch: "launch rejected", lateShellItemAfterReject: true });
      cleanups.push(() => vm.server.stop(true));
      const { cfg, handle, boat } = await startHubWithVm(vm, "t3-sentinel-hub-late-");
      const client = await connectHubClient(handle, cfg);
      const hubProjectId = await subscribeHubShell(client);

      client.request("launch", "orchestration.launchThread", clientLaunchPayload("thread-late", hubProjectId));
      await client.waitForFrame(shellThreadUpdate("thread-late", (t) => t.status === "failed"));
      await until(() => boat.deleted.length === 1);
      await Bun.sleep(100);
      const removed = client.inbox.some(
        (f) =>
          f._tag === "Chunk" &&
          (f.values as Array<{ kind?: string; threadId?: string } | null>)?.some((v) => v?.kind === "thread.removed" && v.threadId === "thread-late"),
      );
      expect(removed).toBe(false);
      client.request("shell-2", "orchestration.subscribeShell");
      const shell = await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "shell-2");
      const row = (shell.values as Array<{ snapshot: { threads: Array<{ id: string; status: string }> } }>)[0].snapshot.threads.find(
        (t) => t.id === "thread-late",
      );
      expect(row?.status).toBe("failed");
    });
  });

  describe("chats whose VM is asleep", () => {
    type HubClient = Awaited<ReturnType<typeof connectHubClient>>;
    const until = async (cond: () => boolean, timeoutMs = 5_000) => {
      const start = Date.now();
      while (!cond()) {
        if (Date.now() - start > timeoutMs) throw new Error("condition not met in time");
        await Bun.sleep(20);
      }
    };
    type Row = { item: { type: string; summary?: string; outcome?: string; detail?: string } };
    const noticeOf = (f: Record<string, unknown>) => {
      const snap = (f.values as Array<{ kind: string; projection?: { visibleTurnItems: Row[] } }>).find((v) => v.kind === "snapshot");
      return snap?.projection?.visibleTurnItems.find((r) => r.item.type === "notification")?.item;
    };
    const threadChunk = (reqId: string, pred: (notice: Row["item"] | undefined, f: Record<string, unknown>) => boolean) =>
      (f: Record<string, unknown>) => f._tag === "Chunk" && f.requestId === reqId && pred(noticeOf(f), f);

    async function sleepingChat(prefix: string, vmOpts: Parameters<typeof startFakeVmT3Server>[1] = {}) {
      const vm = startFakeVmT3Server(prefix, vmOpts);
      cleanups.push(() => vm.server.stop(true));
      const hub = await startHubWithVm(vm, `t3-sentinel-hub-${prefix}-`);
      const client: HubClient = await connectHubClient(hub.handle, hub.cfg);
      await launchThreadOnHub(client, "thread-zz");
      const sandboxId = hub.handle.state.listHubSandboxes()[0].sandboxId;
      await hub.handle.hub!.sleepHubSandbox(sandboxId);
      expect(hub.handle.state.getHubSandbox(sandboxId)!.phase).toBe("sleeping");
      return { vm, client, sandboxId, ...hub };
    }

    test("opening a sleeping chat shows its history at once, says it is waking, then hands over to the VM", async () => {
      const { vm, client, boat, sandboxId, dir } = await sleepingChat("vm-sleep");
      expect(existsSync(join(dir, "hub-thread-cache", "thread-zz.json"))).toBe(true);
      let release!: () => void;
      boat.resumeGate = new Promise<void>((r) => (release = r));
      cleanups.push(() => release());
      const seenBefore = vm.seenMethods.length;

      // The app reopens the chat with its cached cursor, and records the visit
      client.request("sub", "orchestration.subscribeThread", { threadId: "thread-zz", afterSequence: 7, requestCompletionMarker: true });
      client.request("visit", "orchestration.dispatchCommand", {
        type: "thread.visit",
        commandId: "cmd-visit",
        threadId: "thread-zz",
        visitedAt: new Date().toISOString(),
      });
      const first = await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "sub");
      const snap = (first.values as Array<{ kind: string; projection?: { messages: Array<{ text: string }> } }>)[0];
      expect(snap.kind).toBe("snapshot");
      expect(snap.projection!.messages[0].text).toBe("What we said before");
      expect((first.values as Array<{ kind: string }>).some((v) => v.kind === "synchronized")).toBe(true);
      // Answered at once, not after the wake
      const visit = await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "visit", 1_000);
      expect((visit.exit as { _tag: string })._tag).toBe("Success");
      await client.waitForFrame(threadChunk("sub", (n) => n?.summary?.startsWith("Waking this chat's VM") === true), 2_000);
      expect(boat.resumed).toHaveLength(0);

      release();
      await until(() => vm.seenMethods.slice(seenBefore).some((m) => m.tag === "orchestration.subscribeThread"));
      const handedOff = vm.seenMethods.slice(seenBefore).find((m) => m.tag === "orchestration.subscribeThread")!.payload as Record<string, unknown>;
      // It got the Hub's copy, so the VM sends a full snapshot
      expect("afterSequence" in handedOff).toBe(false);
      expect(boat.resumed).toEqual([sandboxId]);
      await until(() =>
        vm.seenMethods
          .slice(seenBefore)
          .some((m) => m.tag === "orchestration.dispatchCommand" && (m.payload as { type?: string }).type === "thread.visit"),
      );
    });

    test("with no Hub copy, a client's own cached history is kept, not replaced by an empty view", async () => {
      const { client, boat, dir } = await sleepingChat("vm-nocache", { failProjection: true });
      expect(existsSync(join(dir, "hub-thread-cache", "thread-zz.json"))).toBe(false);
      let release!: () => void;
      boat.resumeGate = new Promise<void>((r) => (release = r));
      cleanups.push(() => release());

      client.request("sub", "orchestration.subscribeThread", { threadId: "thread-zz", afterSequence: 7, requestCompletionMarker: true });
      const first = await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "sub");
      expect(first.values).toEqual([{ kind: "synchronized" }]);
      // The wake starts and its status is not pushed over the client's copy
      await Bun.sleep(150);
      expect(client.inbox.some((f) => f._tag === "Chunk" && f.requestId === "sub")).toBe(false);
      release();
    });

    test("a failed wake says why in the chat, and commands fail with a readable reason", async () => {
      const { client, boat } = await sleepingChat("vm-wakefail");
      boat.resumeError = new Error("klav-forward is not listening");

      client.request("sub", "orchestration.subscribeThread", { threadId: "thread-zz" });
      const failed = await client.waitForFrame(threadChunk("sub", (n) => n?.outcome === "failed"));
      const notice = noticeOf(failed)!;
      expect(notice.summary).toContain("could not wake");
      expect(notice.detail).toContain("klav-forward is not listening");

      client.request("msg", "orchestration.dispatchCommand", {
        type: "message.dispatch",
        commandId: "cmd-m",
        threadId: "thread-zz",
        messageId: "m-1",
        text: "hello?",
        attachments: [],
      });
      const exit = (await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "msg")).exit as {
        _tag: string;
        cause: Array<{ _tag: string; error?: Record<string, unknown> }>;
      };
      expect(exit._tag).toBe("Failure");
      expect(exit.cause[0]).toMatchObject({
        _tag: "Fail",
        error: { _tag: "OrchestrationV2DispatchCommandError", commandId: "cmd-m", commandType: "message.dispatch" },
      });
      expect(String(exit.cause[0].error!.message)).toContain("klav-forward is not listening");
    });

    test("automations follow the VMs: no failure when the VM sleeps, back on the next VM that opens", async () => {
      const { vm, client, handle, sandboxId } = await sleepingChat("vm-auto");
      // Nothing running: the stream waits quietly
      client.request("tasks", "scheduledTasks.subscribe", {});
      await Bun.sleep(100);
      expect(client.inbox.some((f) => f.requestId === "tasks")).toBe(false);
      const before = vm.seenMethods.filter((m) => m.tag === "scheduledTasks.subscribe").length;
      await handle.hub!.wakeHubSandbox(sandboxId);
      await until(() => vm.seenMethods.filter((m) => m.tag === "scheduledTasks.subscribe").length === before + 1);
      await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "tasks");
      // The VM sleeps again: the stream is not failed
      await handle.hub!.sleepHubSandbox(sandboxId);
      await Bun.sleep(100);
      expect(client.inbox.some((f) => f._tag === "Exit" && f.requestId === "tasks")).toBe(false);
    });
  });

  test("follows live providerStatuses from a VM after its stale boot snapshot and pushes them to clients", async () => {
    const staleSnapshot = {
      providers: [
        { instanceId: "claudeAgent", driver: "claudeAgent", enabled: true, installed: true },
        { instanceId: "antigravity", driver: "antigravity", enabled: true, installed: false, message: "Antigravity ACP runtime is not installed" },
      ],
      settings: { theme: "dark" },
      keybindings: [],
    };
    const vm = startFakeVmT3Server("vm-prov", { configSnapshot: staleSnapshot });
    cleanups.push(() => vm.server.stop(true));
    const { cfg, handle } = await startHubWithVm(vm, "t3-sentinel-hub-prov-");
    const client = await connectHubClient(handle, cfg);

    client.request("cfg", "subscribeServerConfig", { workspaceSnapshotsOnDemand: true });
    await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "cfg");

    await launchThreadOnHub(client, "thread-prov");

    // Boot snapshot from the VM's stale on-disk cache reaches the Hub first
    await client.waitForFrame(providerStatusesChunk("cfg", "antigravity", false));
    expect(providerById(handle.state.getHub()?.serverConfig?.providers, "antigravity")?.installed).toBe(false);
    const sub = vm.seenMethods.find((m) => m.tag === "subscribeServerConfig");
    expect(sub?.payload).toEqual({ workspaceSnapshotsOnDemand: true });

    // The VM's real check then publishes the corrected status on the same stream
    vm.pushConfigItems([
      {
        version: 1,
        type: "providerStatuses",
        payload: {
          providers: [
            { instanceId: "claudeAgent", driver: "claudeAgent", enabled: true, installed: true },
            { instanceId: "antigravity", driver: "antigravity", enabled: true, installed: true },
          ],
        },
      },
    ]);
    await client.waitForFrame(providerStatusesChunk("cfg", "antigravity", true));
    const hubCfg = handle.state.getHub()?.serverConfig;
    expect(providerById(hubCfg?.providers, "antigravity")?.installed).toBe(true);
    // Only providers were replaced; the rest of the harvested config is untouched
    expect(hubCfg?.settings).toEqual({ theme: "dark" });
    // The Hub Acks every chunk of the persistent stream and did not interrupt it
    expect(vm.acks.filter((id) => id === "hub-config").length).toBeGreaterThanOrEqual(2);

    // An identical status is not re-broadcast
    const before = client.inbox.length;
    vm.pushConfigItems([
      {
        version: 1,
        type: "providerStatuses",
        payload: { providers: hubCfg?.providers },
      },
    ]);
    await new Promise((r) => setTimeout(r, 200));
    expect(client.inbox.slice(before).some((f) => f._tag === "Chunk" && f.requestId === "cfg")).toBe(false);
  });

  test("forwards server.refreshProviders to a live VM, replies with its result and caches it", async () => {
    const refreshed = [
      { instanceId: "claudeAgent", driver: "claudeAgent", enabled: true, installed: true },
      { instanceId: "antigravity", driver: "antigravity", enabled: true, installed: true },
      { instanceId: "acpRegistry_devin", driver: "acpRegistry", enabled: true, installed: true },
    ];
    const vm = startFakeVmT3Server("vm-refresh", {
      configSnapshot: {
        providers: [{ instanceId: "antigravity", driver: "antigravity", enabled: true, installed: false }],
        settings: {},
      },
      refreshProviders: refreshed,
    });
    cleanups.push(() => vm.server.stop(true));
    const { cfg, handle } = await startHubWithVm(vm, "t3-sentinel-hub-refresh-");
    const client = await connectHubClient(handle, cfg);

    client.request("cfg", "subscribeServerConfig", {});
    await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "cfg");
    await launchThreadOnHub(client, "thread-refresh");
    await client.waitForFrame(providerStatusesChunk("cfg", "antigravity", false));

    client.request("refresh", "server.refreshProviders", { fresh: true });
    const exit = await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "refresh");
    const result = exit.exit as { _tag: string; value: { providers: unknown[] } };
    expect(result._tag).toBe("Success");
    expect(result.value.providers).toEqual(refreshed);
    expect(vm.seenMethods.find((m) => m.tag === "server.refreshProviders")?.payload).toEqual({ fresh: true });

    expect(handle.state.getHub()?.serverConfig?.providers).toEqual(refreshed);
    await client.waitForFrame(providerStatusesChunk("cfg", "acpRegistry_devin", true));
  });

  test("Hub-built failures use the Effect 4 wire shape: an array cause, Die for Hub errors, value:null on void success", async () => {
    const vm = startFakeVmT3Server("vm-wire");
    cleanups.push(() => vm.server.stop(true));
    const { cfg, handle } = await startHubWithVm(vm, "t3-sentinel-hub-wire-");
    const client = await connectHubClient(handle, cfg);

    // No VM running: the generic fallback fails with a Die whose defect is the text (like a real
    // T3 server's "Unknown request tag"); a Fail with a Hub-only error type would not decode.
    client.request("unknown", "no.such.method", {});
    const unknown = await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "unknown");
    expect(unknown.exit).toEqual({
      _tag: "Failure",
      cause: [{ _tag: "Die", defect: "No active Boat sandbox to handle no.such.method; start a thread first." }],
    });

    // Void success keeps the `value` key (the client's Exit codec requires it)
    client.request("activity", "server.reportClientActivity", {});
    const activity = await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "activity");
    expect(activity.exit).toEqual({ _tag: "Success", value: null });
    expect("value" in (activity.exit as Record<string, unknown>)).toBe(true);

    // An interrupted request ends with an Interrupt cause, as a real T3 server ends it
    client.request("life", "subscribeServerLifecycle", {});
    await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "life");
    client.ws.send(JSON.stringify({ _tag: "Interrupt", requestId: "life" }));
    const interrupted = await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "life");
    expect(interrupted.exit).toEqual({ _tag: "Failure", cause: [{ _tag: "Interrupt", fiberId: null }] });
  });

  test("provider setup streams with no VM: idle snapshot kept open, handed to a VM when one starts, idle again when it goes", async () => {
    const vm = startFakeVmT3Server("vm-setup");
    cleanups.push(() => vm.server.stop(true));
    const { cfg, handle } = await startHubWithVm(vm, "t3-sentinel-hub-setup-");
    handle.state.updateHub((h) => {
      h.serverConfig = {
        providers: [{ instanceId: "antigravity", driver: "antigravity", enabled: true, installed: true }],
        settings: {},
      };
    });
    const client = await connectHubClient(handle, cfg);

    client.request("inst", "provider.install.subscribe", { instanceId: "antigravity" });
    client.request("auth", "provider.auth.subscribe", { instanceId: "antigravity" });
    const inst = await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "inst");
    expect((inst.values as unknown[])[0]).toEqual({
      driver: "antigravity",
      operationId: null,
      phase: "idle",
      downloadedBytes: 0,
      totalBytes: null,
      version: null,
      installedVersion: null,
      canRemove: false,
      message: null,
    });
    const auth = await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "auth");
    expect((auth.values as unknown[])[0]).toEqual({
      instanceId: "antigravity",
      phase: "idle",
      flowId: null,
      authorizationUrl: null,
      expiresAt: null,
      message: null,
      interaction: null,
    });

    // Setup commands fail with the RPC's own error type, so the panel can show the text
    client.request("start", "provider.install.start", { instanceId: "antigravity" });
    const start = await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "start");
    const startCause = (start.exit as { cause: Array<{ _tag: string; error: Record<string, unknown> }> }).cause;
    expect(startCause).toHaveLength(1);
    expect(startCause[0]._tag).toBe("Fail");
    expect(startCause[0].error).toMatchObject({ _tag: "ProviderSetupError", instanceId: "antigravity", operation: "install.start" });
    expect(String(startCause[0].error.detail)).toContain("Start a thread on Boat");

    // A payload with no valid instanceId gets a Die, like the real server's decode failure
    client.request("bad", "provider.install.subscribe", {});
    const bad = await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "bad");
    expect((bad.exit as { cause: Array<{ _tag: string }> }).cause[0]._tag).toBe("Die");

    // The idle streams stay open
    await new Promise((r) => setTimeout(r, 100));
    expect(client.inbox.some((f) => f._tag === "Exit" && (f.requestId === "inst" || f.requestId === "auth"))).toBe(false);

    // A VM starts: its bridge takes both streams over on the same request ids
    await launchThreadOnHub(client, "thread-setup");
    const fromVmInst = await client.waitForFrame(
      (f) => f._tag === "Chunk" && f.requestId === "inst" && (f.values as Array<{ fromVm?: string }>)[0]?.fromVm === "vm-setup",
    );
    expect((fromVmInst.values as Array<{ instanceId?: string }>)[0].instanceId).toBe("antigravity");
    await client.waitForFrame(
      (f) => f._tag === "Chunk" && f.requestId === "auth" && (f.values as Array<{ fromVm?: string }>)[0]?.fromVm === "vm-setup",
    );
    expect(vm.seenMethods.filter((m) => m.tag === "provider.install.subscribe").map((m) => m.payload)).toEqual([
      { instanceId: "antigravity" },
    ]);

    // The VM goes away: the streams fall back to the idle snapshot instead of a red error
    vm.server.stop(true);
    const backToIdle = await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "inst");
    expect((backToIdle.values as Array<Record<string, unknown>>)[0]).toMatchObject({ driver: "antigravity", phase: "idle" });
    await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "auth");
    expect(client.inbox.some((f) => f._tag === "Exit" && (f.requestId === "inst" || f.requestId === "auth"))).toBe(false);
  });

  test("provider setup streams move to another running VM when theirs goes away", async () => {
    const vm = startFakeVmT3Server("vm-setup-2");
    cleanups.push(() => vm.server.stop(true));
    const { cfg, handle } = await startHubWithVm(vm, "t3-sentinel-hub-setup-2-");
    const client = await connectHubClient(handle, cfg);
    const fromVm = (id: string) => (f: Record<string, unknown>) =>
      f._tag === "Chunk" && f.requestId === id && (f.values as Array<{ fromVm?: string }>)[0]?.fromVm === "vm-setup-2";

    await launchThreadOnHub(client, "thread-a");
    const sandboxA = handle.state.listHubSandboxes()[0].sandboxId;
    client.request("inst", "provider.install.subscribe", { instanceId: "antigravity" });
    await client.waitForFrame(fromVm("inst"));
    // Second chat: a second VM (launchThreadOnHub's "shell" subscription is already open)
    client.request("launch-b", "orchestration.launchThread", {
      threadId: "thread-b",
      projectId: handle.state.getHub()!.projectIds["my-app"],
      title: "Second",
      workspaceStrategy: { type: "root" },
    });
    const launchB = await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "launch-b");
    expect((launchB.exit as { _tag: string })._tag).toBe("Success");
    expect(handle.state.listHubSandboxes().filter((s) => s.phase === "running")).toHaveLength(2);

    await handle.hub!.destroyHubSandbox(sandboxA, "test");
    // Re-subscribed on the other VM (both fake VMs share one server here), not answered idle by the Hub
    await client.waitForFrame(fromVm("inst"));
    expect(vm.seenMethods.filter((m) => m.tag === "provider.install.subscribe")).toHaveLength(2);
    expect(client.inbox.some((f) => f.requestId === "inst")).toBe(false);
  });

  test("server.refreshProviders with no live VM returns the Hub's cached providers", async () => {
    const dir = mkdtempSync(join(tmpdir(), "t3-sentinel-hub-refresh-cache-"));
    cleanups.push(() => rmSync(dir, { recursive: true, force: true }));
    const boat = new MockBoat();
    boat.snapshots.add("base-t3");
    const cfg: SentinelConfig = {
      ...makeTestConfig(0),
      stateDir: dir,
      hub: { host: "boat.boat.example.com", label: "Boat", maxSandboxes: 5 },
    };
    const handle = createSentinelServer(cfg, boat, { startBackgroundLoops: false });
    cleanups.push(() => handle.stop());
    const cached = [{ instanceId: "antigravity", driver: "antigravity", enabled: true, installed: true }];
    handle.state.ensureHub("Boat", Object.keys(handle.state.getAllProjects(cfg)));
    handle.state.updateHub((h) => {
      h.serverConfig = { providers: cached, settings: {} };
    });

    const client = await connectHubClient(handle, cfg);
    client.request("refresh", "server.refreshProviders", {});
    const exit = await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "refresh");
    const result = exit.exit as { _tag: string; value: { providers: unknown[] } };
    expect(result._tag).toBe("Success");
    expect(result.value.providers).toEqual(cached);
    expect(handle.state.listHubSandboxes().length).toBe(0);
  });

  describe("server.updateSettings (patch, providerInstanceMutation, overlay)", () => {
    const SECRET = "sk-test-do-not-log-4242";
    const baseSettings = () => ({
      favorites: null,
      providerModelPreferences: null,
      providerInstances: {
        claudeAgent: { driver: "claudeAgent", enabled: true },
        opencode: { driver: "opencode", enabled: true },
      },
      fastWorktreeCheckout: false,
      worktreesDirectory: "",
      previousWorktreesDirectories: [],
      storageCleanup: { logsAfterDays: 30, worktreeAfterDays: 7 },
      cloudSpeechCredentials: { openai: "" },
    });
    const baseProviders = () => [
      { instanceId: "claudeAgent", driver: "claudeAgent", enabled: true, installed: true, status: "ready" },
      { instanceId: "opencode", driver: "opencode", enabled: true, installed: true, status: "ready" },
    ];
    const FAVORITE = { setModelFavorites: [{ provider: "claudeAgent", model: "claude-opus-4-6", favorite: true }] };
    const DISABLE_OPENCODE = {
      patch: {},
      providerInstanceMutation: { operation: "upsert", instanceId: "opencode", instance: { driver: "opencode", enabled: false } },
    };
    const configChunk = (requestId: string, type: string, pred: (payload: Record<string, unknown>) => boolean = () => true) =>
      (f: Record<string, unknown>) =>
        f._tag === "Chunk" &&
        f.requestId === requestId &&
        (f.values as Array<{ type?: string; payload?: Record<string, unknown> }>)?.some((v) => v.type === type && pred(v.payload ?? {}));
    const opencodeOf = (settings: unknown) =>
      ((settings as { providerInstances?: Record<string, unknown> } | undefined)?.providerInstances ?? {}).opencode;
    const updateExit = async (client: Awaited<ReturnType<typeof connectHubClient>>, id: string, payload: unknown) => {
      client.request(id, "server.updateSettings", payload);
      const exit = await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === id);
      return exit.exit as { _tag: string; value?: Record<string, unknown>; cause?: unknown };
    };

    test("with no VM: applies the patch with T3's semantics, notifies other clients, fails like T3, logs no values, and persists the overlay", async () => {
      const vm = startFakeVmT3Server("vm-settings-local");
      cleanups.push(() => vm.server.stop(true));
      const { cfg, handle } = await startHubWithVm(vm, "t3-sentinel-hub-settings-local-");
      handle.state.updateHub((h) => {
        h.serverConfig = { providers: baseProviders(), settings: baseSettings() };
      });
      const logged: string[] = [];
      const origLog = console.log;
      const origWarn = console.warn;
      console.log = (...args: unknown[]) => void logged.push(args.map(String).join(" "));
      console.warn = (...args: unknown[]) => void logged.push(args.map(String).join(" "));
      cleanups.push(() => {
        console.log = origLog;
        console.warn = origWarn;
      });

      const a = await connectHubClient(handle, cfg);
      const b = await connectHubClient(handle, cfg);
      b.request("cfg", "subscribeServerConfig", {});
      await b.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "cfg");

      // Favorite a model: setModelFavorites adds to the list, and the change reaches the other client
      const fav = await updateExit(a, "fav", { patch: FAVORITE });
      expect(fav._tag).toBe("Success");
      expect(fav.value?.favorites).toEqual([{ provider: "claudeAgent", model: "claude-opus-4-6" }]);
      expect(fav.value?.fastWorktreeCheckout).toBe(false);
      const pushed = await b.waitForFrame(configChunk("cfg", "settingsUpdated"));
      const pushedSettings = (pushed.values as Array<{ payload: { settings: Record<string, unknown> } }>)[0].payload.settings;
      expect(pushedSettings.favorites).toEqual(fav.value?.favorites);

      // Turn OpenCode off: the instance is replaced and the cached provider shows disabled at once
      const off = await updateExit(a, "off", DISABLE_OPENCODE);
      expect(off._tag).toBe("Success");
      expect(opencodeOf(off.value)).toEqual({ driver: "opencode", enabled: false });
      expect(off.value?.favorites).toEqual([{ provider: "claudeAgent", model: "claude-opus-4-6" }]);
      await b.waitForFrame(
        configChunk("cfg", "providerStatuses", (p) => {
          const oc = providerById(p.providers, "opencode") as { enabled?: boolean; status?: string } | undefined;
          return oc?.enabled === false && oc.status === "disabled";
        }),
      );

      // Deep-merged keys keep their siblings
      const merged = await updateExit(a, "merge", { patch: { fastWorktreeCheckout: true, storageCleanup: { logsAfterDays: 14 } } });
      expect(merged.value?.storageCleanup).toEqual({ logsAfterDays: 14, worktreeAfterDays: 7 });

      // `create` of an existing instance fails with the typed error a T3 server sends
      const dup = await updateExit(a, "dup", {
        patch: {},
        providerInstanceMutation: { operation: "create", instanceId: "opencode", instance: { driver: "opencode" } },
      });
      expect(dup).toEqual({
        _tag: "Failure",
        cause: [
          {
            _tag: "Fail",
            error: {
              _tag: "ServerSettingsError",
              settingsPath: "/home/user/.t3/userdata/settings.json",
              operation: "create-provider-instance",
              providerInstanceId: "opencode",
            },
          },
        ],
      });
      // The old flat payload is rejected like a T3 server's decoder rejects it
      const flat = await updateExit(a, "flat", { fastWorktreeCheckout: true });
      expect(flat).toEqual({ _tag: "Failure", cause: [{ _tag: "Die", defect: 'Missing key\n  at ["patch"]' }] });

      // A credential is replayed to VMs but never enters the Hub's served copy or the logs
      const secret = await updateExit(a, "secret", { patch: { cloudSpeechCredentials: { openai: SECRET } } });
      expect(secret._tag).toBe("Success");
      expect(JSON.stringify(secret.value)).not.toContain(SECRET);
      expect(JSON.stringify(handle.state.getHub()?.serverConfig)).not.toContain(SECRET);
      expect(logged.some((l) => l.includes(SECRET))).toBe(false);
      expect(logged.some((l) => l.includes("settings update keys=[setModelFavorites]"))).toBe(true);

      a.request("get", "server.getSettings", {});
      const got = await a.waitForFrame((f) => f._tag === "Exit" && f.requestId === "get");
      expect((got.exit as { value: Record<string, unknown> }).value.favorites).toEqual(fav.value?.favorites);

      // The overlay survives a Hub restart
      const reloaded = new StateStore(cfg.stateDir, cfg).getHub()?.settingsOverlay;
      expect(reloaded?.patch.setModelFavorites).toEqual(FAVORITE.setModelFavorites);
      expect(reloaded?.patch.fastWorktreeCheckout).toBe(true);
      expect(reloaded?.providerInstances.opencode).toEqual({ operation: "upsert", instance: { driver: "opencode", enabled: false } });
    });

    test("forwards to running VMs, answers with the VM's result, and follows the VM's provider statuses", async () => {
      const vm = startFakeVmT3Server("vm-settings-fwd", {
        configSnapshot: { providers: baseProviders(), settings: baseSettings() },
        applySettings: true,
      });
      cleanups.push(() => vm.server.stop(true));
      const { cfg, handle } = await startHubWithVm(vm, "t3-sentinel-hub-settings-fwd-");
      const client = await connectHubClient(handle, cfg);
      client.request("cfg", "subscribeServerConfig", {});
      await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "cfg");
      await launchThreadOnHub(client, "thread-settings-fwd");
      await client.waitForFrame(configChunk("cfg", "settingsUpdated"));

      const off = await updateExit(client, "off", DISABLE_OPENCODE);
      expect(off._tag).toBe("Success");
      expect(off.value).toEqual(vm.getSettings()!);
      expect(opencodeOf(vm.getSettings())).toEqual({ driver: "opencode", enabled: false });
      const forwarded = vm.seenMethods.filter((m) => m.tag === "server.updateSettings");
      expect(forwarded.at(-1)?.payload).toEqual(DISABLE_OPENCODE);
      await client.waitForFrame(configChunk("cfg", "settingsUpdated", (p) => opencodeOf(p.settings) !== undefined));

      // A create is sent to VMs as an upsert (the Hub checked it against its own copy)
      const created = await updateExit(client, "create", {
        patch: {},
        providerInstanceMutation: { operation: "create", instanceId: "opencode_work", instance: { driver: "opencode", enabled: true } },
      });
      expect(created._tag).toBe("Success");
      expect((vm.seenMethods.at(-1)?.payload as SettingsUpdateInput).providerInstanceMutation?.operation).toBe("upsert");

      // The VM then publishes the provider list it computes; the Hub caches and forwards it
      vm.pushConfigItems([
        {
          version: 1,
          type: "providerStatuses",
          payload: { providers: [baseProviders()[0], { ...baseProviders()[1], enabled: false, status: "disabled" }] },
        },
      ]);
      await client.waitForFrame(
        configChunk("cfg", "providerStatuses", (p) => (providerById(p.providers, "opencode") as { status?: string })?.status === "disabled"),
      );
      expect((providerById(handle.state.getHub()?.serverConfig?.providers, "opencode") as { enabled?: boolean })?.enabled).toBe(false);
      expect(opencodeOf(handle.state.getHub()?.serverConfig?.settings)).toEqual({ driver: "opencode", enabled: false });
    });

    test("replays the overlay to a new VM before its thread launches, and its stale snapshot does not overwrite it", async () => {
      const withConfig = (binaryPath: string) => {
        const settings = baseSettings();
        settings.providerInstances.opencode = { driver: "opencode", enabled: true, config: { binaryPath } } as never;
        return settings;
      };
      const vm = startFakeVmT3Server("vm-settings-replay", {
        // Booted from a snapshot baked before the user's changes
        configSnapshot: { providers: baseProviders(), settings: { ...withConfig("/old"), bootMarker: "vm-boot" } },
        applySettings: true,
      });
      cleanups.push(() => vm.server.stop(true));
      const { cfg, handle } = await startHubWithVm(vm, "t3-sentinel-hub-settings-replay-");
      handle.state.updateHub((h) => {
        h.serverConfig = { providers: baseProviders(), settings: withConfig("/old") };
        h.lifecycleEvents = null;
      });
      const client = await connectHubClient(handle, cfg);
      expect((await updateExit(client, "fav", { patch: FAVORITE }))._tag).toBe("Success");
      const newInstance = { driver: "opencode", enabled: false, config: { binaryPath: "/new" } };
      const off = await updateExit(client, "off", {
        patch: {},
        providerInstanceMutation: { operation: "upsert", instanceId: "opencode", instance: newInstance },
      });
      expect(off._tag).toBe("Success");
      // With no VM, instance config stays as the Hub last saw it from a VM (it can hold secrets)
      expect(opencodeOf(off.value)).toEqual({ driver: "opencode", enabled: false, config: { binaryPath: "/old" } });
      expect(handle.state.listHubSandboxes().length).toBe(0);

      await launchThreadOnHub(client, "thread-settings-replay");
      const tags = vm.seenMethods.map((m) => m.tag);
      expect(vm.seenMethods.filter((m) => m.tag === "server.updateSettings").length).toBe(2);
      expect(tags.indexOf("server.updateSettings")).toBeLessThan(tags.indexOf("orchestration.launchThread"));
      expect(vm.getSettings()?.favorites).toEqual([{ provider: "claudeAgent", model: "claude-opus-4-6" }]);
      expect(opencodeOf(vm.getSettings())).toEqual(newInstance);

      // Wait for the harvest of the VM's boot snapshot (favorites null, OpenCode on, old config)
      const deadline = Date.now() + 5_000;
      while (!handle.state.getHub()?.lifecycleEvents && Date.now() < deadline) await new Promise((r) => setTimeout(r, 20));
      expect(handle.state.getHub()?.lifecycleEvents).not.toBeNull();
      const hubSettings = handle.state.getHub()?.serverConfig?.settings as Record<string, unknown>;
      expect(hubSettings.bootMarker).toBe("vm-boot");
      expect(hubSettings.favorites).toEqual([{ provider: "claudeAgent", model: "claude-opus-4-6" }]);
      // The Hub copy is the VM's reply to the replay, so it has the config the user saved
      expect(opencodeOf(hubSettings)).toEqual(newInstance);
    });
  });

  describe("persisted provider sessions (Antigravity sign-in sync)", () => {
    const AG_HASH = createHash("sha256").update("antigravity").digest("hex");
    const AG_STAGED = `~/.t3-sentinel/provider-sessions/.t3/userdata/providers/antigravity/${AG_HASH}/antigravity-acp/antigravity-oauth-token`;
    const agToken = (access: string, expiry: string) =>
      JSON.stringify({ token: { access_token: access, refresh_token: "1//refresh-SECRET", expiry } });
    const agProviders = (status: string) => [
      { instanceId: "claudeAgent", driver: "claudeAgent", enabled: true, installed: true },
      { instanceId: "antigravity", driver: "antigravity", enabled: true, installed: true, auth: { status } },
    ];
    const AG_VM_PATH = `~/.t3/userdata/providers/antigravity/${AG_HASH}/antigravity-acp/antigravity-oauth-token`;
    const storedPath = (dir: string) => join(dir, "credentials", "antigravity", "antigravity-oauth-token");
    /** The capture's exec only lists files ("<name> <bytes>"); contents come over `download`. */
    const isCredentialListing = (script: string) => script.includes("antigravity-oauth-token") && script.includes("wc -c");
    const listingFor = (content: string | null) =>
      content ? `antigravity-oauth-token ${Buffer.byteLength(content)}\n` : "";

    /** Serves `vmToken.current` as the VM's credential file and counts listings (one per capture). */
    function serveVmToken(boat: MockBoat, vmToken: { current: string | null }) {
      const reads = { count: 0 };
      const original = boat.exec.bind(boat);
      boat.exec = async (id: string, script: string) => {
        if (isCredentialListing(script)) {
          reads.count++;
          expect(script).toContain(`providers/antigravity/${AG_HASH}/antigravity-acp`);
          return { exitCode: 0, stdout: listingFor(vmToken.current), stderr: "" };
        }
        return original(id, script);
      };
      boat.download = async (_id: string, remotePath: string, localPath: string) => {
        expect(remotePath).toBe(AG_VM_PATH);
        if (vmToken.current === null) throw new Error("no such file");
        writeFileSync(localPath, vmToken.current);
      };
      return reads;
    }

    async function waitFor(pred: () => boolean, timeoutMs = 5_000) {
      const start = Date.now();
      while (!pred()) {
        if (Date.now() - start > timeoutMs) throw new Error("condition not met in time");
        await new Promise((r) => setTimeout(r, 20));
      }
    }

    test("captures an authenticated sign-in once at 0600, keeps it on unauthenticated, pulls before teardown, never into state.json", async () => {
      const vm = startFakeVmT3Server("vm-ag-capture", {
        configSnapshot: { providers: agProviders("authenticated"), settings: {} },
      });
      cleanups.push(() => vm.server.stop(true));
      const { cfg, handle, boat, dir } = await startHubWithVm(vm, "t3-sentinel-ag-capture-", {
        persistProviderSessions: ["antigravity"],
      });
      const vmToken = { current: agToken("ya29.FIRST-SECRET", "2026-11-25T10:00:00.123456789-06:00") };
      const reads = serveVmToken(boat, vmToken);
      const client = await connectHubClient(handle, cfg);
      client.request("cfg", "subscribeServerConfig", {});
      await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "cfg");
      await launchThreadOnHub(client, "thread-ag-capture");

      await waitFor(() => existsSync(storedPath(dir)));
      await handle.lifecycle.providerSessions.settle();
      expect(reads.count).toBe(1);
      expect(readFileSync(storedPath(dir), "utf8")).toBe(vmToken.current!);
      expect(statSync(storedPath(dir)).mode & 0o777).toBe(0o600);
      expect(statSync(join(dir, "credentials", "antigravity")).mode & 0o777).toBe(0o700);
      expect(statSync(join(dir, "credentials")).mode & 0o777).toBe(0o700);

      // Repeated authenticated statuses do not re-download
      vm.pushConfigItems([{ version: 1, type: "providerStatuses", payload: { providers: agProviders("authenticated") } }]);
      vm.pushConfigItems([{ version: 1, type: "providerStatuses", payload: { providers: agProviders("authenticated") } }]);
      await new Promise((r) => setTimeout(r, 150));
      await handle.lifecycle.providerSessions.settle();
      expect(reads.count).toBe(1);

      // A transient unauthenticated status keeps the stored copy
      vm.pushConfigItems([{ version: 1, type: "providerStatuses", payload: { providers: agProviders("unauthenticated") } }]);
      await new Promise((r) => setTimeout(r, 150));
      await handle.lifecycle.providerSessions.settle();
      expect(existsSync(storedPath(dir))).toBe(true);

      // Signed in again: captured again
      vm.pushConfigItems([{ version: 1, type: "providerStatuses", payload: { providers: agProviders("authenticated") } }]);
      await waitFor(() => reads.count === 2);
      await handle.lifecycle.providerSessions.settle();

      // The CLI refreshed its token: teardown pulls the newer copy before the VM is deleted
      vmToken.current = agToken("ya29.REFRESHED-SECRET", "2026-11-25T18:00:00Z");
      const sandboxId = handle.state.listHubSandboxes()[0].sandboxId;
      await handle.hub!.destroyHubSandbox(sandboxId, "test");
      expect(reads.count).toBe(3);
      expect(readFileSync(storedPath(dir), "utf8")).toBe(vmToken.current);
      expect(boat.deleted).toContain(sandboxId);

      const stateJson = readFileSync(join(dir, "state.json"), "utf8");
      expect(stateJson).not.toContain("SECRET");
    });

    test("an older token never overwrites a newer one; unparsable expiry falls back to last write wins", async () => {
      const dir = mkdtempSync(join(tmpdir(), "t3-sentinel-ag-expiry-"));
      cleanups.push(() => rmSync(dir, { recursive: true, force: true }));
      const base = makeTestConfig(0);
      const cfg: SentinelConfig = { ...base, stateDir: dir, t3: { ...base.t3, persistProviderSessions: ["antigravity"] } };
      let vmContent = "";
      const provider = {
        name: "fake",
        capabilities: new Set(),
        exec: async () => ({ exitCode: 0, stdout: listingFor(vmContent), stderr: "" }),
        download: async (_id: string, _remote: string, local: string) => writeFileSync(local, vmContent),
      } as unknown as SandboxProvider;
      const store = new ProviderSessionStore(cfg, provider);

      const newer = agToken("ya29.NEWER", "2026-12-01T00:00:00Z");
      vmContent = newer;
      await store.capture("vm-new", "antigravity");
      expect(readFileSync(storedPath(dir), "utf8")).toBe(newer);

      vmContent = agToken("ya29.OLDER", "2026-11-01T00:00:00.000000001-06:00");
      await store.capture("vm-old", "antigravity");
      expect(readFileSync(storedPath(dir), "utf8")).toBe(newer);

      const unparsable = agToken("ya29.NO-EXPIRY", "not-a-date");
      vmContent = unparsable;
      await store.capture("vm-other", "antigravity");
      expect(readFileSync(storedPath(dir), "utf8")).toBe(unparsable);
      expect(statSync(storedPath(dir)).mode & 0o777).toBe(0o600);
    });

    test("a new chat VM gets the stored sign-in staged before provision-base runs, then re-checks the provider", async () => {
      const vm = startFakeVmT3Server("vm-ag-inject", {
        configSnapshot: { providers: agProviders("authenticated"), settings: {} },
        refreshProviders: agProviders("authenticated"),
      });
      cleanups.push(() => vm.server.stop(true));
      const stored = agToken("ya29.STORED-SECRET", "2026-12-01T00:00:00Z");
      const { cfg, handle, boat, dir } = await startHubWithVm(vm, "t3-sentinel-ag-inject-", {
        persistProviderSessions: ["antigravity"],
        beforeStart: (d) => {
          mkdirSync(join(d, "credentials", "antigravity"), { recursive: true, mode: 0o700 });
          writeFileSync(storedPath(d), stored, { mode: 0o600 });
        },
      });
      const reads = serveVmToken(boat, { current: stored });
      const order: string[] = [];
      const uploads: Array<{ local: string; remote: string }> = [];
      boat.upload = async (...args: unknown[]) => {
        uploads.push({ local: String(args[1]), remote: String(args[2]) });
        order.push(`upload:${String(args[2])}`);
      };
      const originalExec = boat.exec;
      boat.exec = async (id: string, script: string) => {
        if (script.includes("provision-base.sh")) order.push("provision");
        return originalExec(id, script);
      };

      const client = await connectHubClient(handle, cfg);
      await launchThreadOnHub(client, "thread-ag-inject");

      const credUpload = uploads.find((u) => u.remote === AG_STAGED);
      expect(credUpload?.local).toBe(storedPath(dir));
      expect(boat.execScripts.some((sc) => sc.includes(`mkdir -p ${AG_STAGED.slice(0, AG_STAGED.lastIndexOf("/"))}`))).toBe(true);
      expect(order.indexOf(`upload:${AG_STAGED}`)).toBeLessThan(order.indexOf("provision"));
      await waitFor(() => vm.seenMethods.some((m) => m.tag === "server.refreshProviders"));
      expect(vm.seenMethods.find((m) => m.tag === "server.refreshProviders")?.payload).toEqual({ instanceId: "antigravity" });
      // The VM already holds the stored copy: no read-back on its first authenticated report
      await new Promise((r) => setTimeout(r, 150));
      await handle.lifecycle.providerSessions.settle();
      expect(reads.count).toBe(0);
    });

    test("a failed credential upload never blocks the VM launch", async () => {
      const vm = startFakeVmT3Server("vm-ag-inject-fail");
      cleanups.push(() => vm.server.stop(true));
      const { cfg, handle, boat } = await startHubWithVm(vm, "t3-sentinel-ag-inject-fail-", {
        persistProviderSessions: ["antigravity"],
        beforeStart: (d) => {
          mkdirSync(join(d, "credentials", "antigravity"), { recursive: true });
          writeFileSync(storedPath(d), agToken("ya29.X", "2026-12-01T00:00:00Z"), { mode: 0o600 });
        },
      });
      boat.upload = async (...args: unknown[]) => {
        if (String(args[2]).includes("provider-sessions")) throw new Error("scp failed");
      };
      const client = await connectHubClient(handle, cfg);
      await launchThreadOnHub(client, "thread-ag-inject-fail");
      expect(handle.state.listHubSandboxes()[0]?.phase).toBe("running");
      expect(boat.execScripts.some((sc) => sc.includes("rm -rf ~/.t3-sentinel/provider-sessions"))).toBe(true);
    });

    test("a failed read is retried; a hung teardown pull never delays the destroy past its bound", async () => {
      const vm = startFakeVmT3Server("vm-ag-hang", {
        configSnapshot: { providers: agProviders("authenticated"), settings: {} },
      });
      cleanups.push(() => vm.server.stop(true));
      const { cfg, handle, boat, dir } = await startHubWithVm(vm, "t3-sentinel-ag-hang-", {
        persistProviderSessions: ["antigravity"],
      });
      let mode: "fail" | "ok" | "hang" = "fail";
      let attempts = 0;
      const original = boat.exec.bind(boat);
      const token = agToken("ya29.RETRY-SECRET", "2026-12-01T00:00:00Z");
      boat.exec = async (id: string, script: string) => {
        if (!isCredentialListing(script)) return original(id, script);
        attempts++;
        if (mode === "fail") return { exitCode: 1, stdout: "", stderr: "boom" };
        return { exitCode: 0, stdout: listingFor(token), stderr: "" };
      };
      // The hung teardown pull is the file copy itself
      boat.download = async (_id: string, _remote: string, local: string) => {
        if (mode === "hang") return new Promise<never>(() => {});
        writeFileSync(local, token);
      };
      const client = await connectHubClient(handle, cfg);
      await launchThreadOnHub(client, "thread-ag-hang");
      await waitFor(() => attempts === 1);
      await handle.lifecycle.providerSessions.settle();
      expect(existsSync(storedPath(dir))).toBe(false);

      mode = "ok";
      vm.pushConfigItems([{ version: 1, type: "providerStatuses", payload: { providers: agProviders("authenticated") } }]);
      await waitFor(() => existsSync(storedPath(dir)));
      expect(attempts).toBe(2);

      mode = "hang";
      handle.lifecycle.providerSessions.teardownTimeoutMs = 50;
      const sandboxId = handle.state.listHubSandboxes()[0].sandboxId;
      const started = Date.now();
      await handle.hub!.destroyHubSandbox(sandboxId, "test");
      expect(Date.now() - started).toBeLessThan(2_000);
      expect(attempts).toBe(3);
      expect(boat.deleted).toContain(sandboxId);
    });

    test("credential bytes never travel through exec output: capture and the teardown pull list with exec and copy with download", async () => {
      const vm = startFakeVmT3Server("vm-ag-noexec", {
        configSnapshot: { providers: agProviders("authenticated"), settings: {} },
      });
      cleanups.push(() => vm.server.stop(true));
      const { cfg, handle, boat, dir } = await startHubWithVm(vm, "t3-sentinel-ag-noexec-", {
        persistProviderSessions: ["antigravity"],
      });
      // A fixture home stands in for the VM: credential scripts run for real against it
      const home = mkdtempSync(join(tmpdir(), "t3-sentinel-ag-vmhome-"));
      cleanups.push(() => rmSync(home, { recursive: true, force: true }));
      const vmTokenPath = join(home, AG_VM_PATH.slice(2));
      mkdirSync(dirname(vmTokenPath), { recursive: true });
      // The legacy file is a credential too: both must come over download
      writeFileSync(join(dirname(vmTokenPath), "acp_token.json"), '{"legacy":"LEGACY-SECRET"}');
      let token = agToken("ya29.NOEXEC-SECRET", "2026-11-25T10:00:00Z");
      writeFileSync(vmTokenPath, token);
      const tokens = [token];

      const execs: Array<{ script: string; stdout: string; stderr: string }> = [];
      const original = boat.exec.bind(boat);
      boat.exec = async (id: string, script: string) => {
        let res: { exitCode: number | null; stdout: string; stderr: string };
        if (script.includes("antigravity-acp")) {
          const p = Bun.spawnSync(["bash", "-c", script], { env: { ...process.env, HOME: home } });
          res = { exitCode: p.exitCode, stdout: p.stdout.toString(), stderr: p.stderr.toString() };
        } else {
          res = await original(id, script);
        }
        execs.push({ script, stdout: res.stdout, stderr: res.stderr });
        return res;
      };
      const downloads: Array<{ remotePath: string; localPath: string; mode: number }> = [];
      boat.download = async (_id: string, remotePath: string, localPath: string) => {
        downloads.push({ remotePath, localPath, mode: statSync(localPath).mode & 0o777 });
        writeFileSync(localPath, readFileSync(join(home, remotePath.replace(/^~\//, ""))));
      };

      const client = await connectHubClient(handle, cfg);
      client.request("cfg", "subscribeServerConfig", {});
      await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "cfg");
      await launchThreadOnHub(client, "thread-ag-noexec");
      await waitFor(() => existsSync(storedPath(dir)));
      await handle.lifecycle.providerSessions.settle();
      expect(readFileSync(storedPath(dir), "utf8")).toBe(token);
      expect(readFileSync(join(dir, "credentials", "antigravity", "acp_token.json"), "utf8")).toContain("LEGACY-SECRET");

      token = agToken("ya29.REFRESHED-NOEXEC-SECRET", "2026-11-25T18:00:00Z");
      tokens.push(token);
      writeFileSync(vmTokenPath, token);
      await handle.hub!.destroyHubSandbox(handle.state.listHubSandboxes()[0].sandboxId, "test");
      expect(readFileSync(storedPath(dir), "utf8")).toBe(token);

      // Two captures (sign-in, teardown pull), two files each, all over download
      const credScripts = execs.filter((e) => /antigravity-oauth-token|acp_token/.test(e.script));
      expect(credScripts.length).toBe(2);
      expect(downloads.length).toBe(4);
      for (const e of credScripts) {
        expect(e.script).not.toMatch(/\b(base64|cat|head|tail|xxd|od|dd|less|more|strings|openssl)\b/);
        expect(e.stdout).toMatch(/^(?:(?:antigravity-oauth-token|acp_token\.json) \d+\n)+$/);
      }
      for (const e of execs) {
        const seen = `${e.script}\n${e.stdout}\n${e.stderr}`;
        expect(seen).not.toContain("SECRET");
        for (const t of tokens) expect(seen).not.toContain(Buffer.from(t).toString("base64").slice(0, 24));
      }
      // Temp files: created 0600 inside the 0700 credentials dir before the copy, deleted afterwards
      for (const d of downloads) {
        expect(dirname(d.localPath)).toBe(join(dir, "credentials"));
        expect(d.mode).toBe(0o600);
        expect(existsSync(d.localPath)).toBe(false);
      }
      expect(readdirSync(join(dir, "credentials")).filter((n) => n.endsWith(".tmp"))).toEqual([]);
    });

    test("a provider without a file download never captures (no exec), warning once", async () => {
      const dir = mkdtempSync(join(tmpdir(), "t3-sentinel-ag-nodl-"));
      cleanups.push(() => rmSync(dir, { recursive: true, force: true }));
      const base = makeTestConfig(0);
      const cfg: SentinelConfig = { ...base, stateDir: dir, t3: { ...base.t3, persistProviderSessions: ["antigravity"] } };
      let execs = 0;
      const provider = {
        name: "fake-sdk",
        capabilities: new Set(),
        exec: async () => {
          execs++;
          return { exitCode: 0, stdout: "", stderr: "" };
        },
      } as unknown as SandboxProvider;
      const store = new ProviderSessionStore(cfg, provider);
      const warnings: string[] = [];
      const warn = console.warn;
      console.warn = (...args: unknown[]) => void warnings.push(args.map(String).join(" "));
      try {
        await store.capture("vm-a", "antigravity");
        store.markInjected("vm-b", ["antigravity"]);
        await store.pullBeforeDestroy("vm-b");
      } finally {
        console.warn = warn;
      }
      expect(execs).toBe(0);
      expect(existsSync(storedPath(dir))).toBe(false);
      expect(warnings.filter((w) => w.includes("no file download")).length).toBe(1);
    });

    test("a successful sign-out forwarded by the Hub deletes the stored copy; forget-session API validates input", async () => {
      const vm = startFakeVmT3Server("vm-ag-logout", {
        configSnapshot: { providers: agProviders("authenticated"), settings: {} },
      });
      cleanups.push(() => vm.server.stop(true));
      const { cfg, handle, boat, dir } = await startHubWithVm(vm, "t3-sentinel-ag-logout-", {
        persistProviderSessions: ["antigravity"],
        beforeStart: (d) => {
          mkdirSync(join(d, "credentials", "antigravity"), { recursive: true });
          writeFileSync(storedPath(d), agToken("ya29.X", "2026-12-01T00:00:00Z"), { mode: 0o600 });
        },
      });
      const reads = serveVmToken(boat, { current: agToken("ya29.X", "2026-12-01T00:00:00Z") });
      const client = await connectHubClient(handle, cfg);
      await launchThreadOnHub(client, "thread-ag-logout");
      expect(existsSync(storedPath(dir))).toBe(true);

      client.request("logout", "provider.auth.logout", { instanceId: "antigravity" });
      const exit = await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "logout");
      expect((exit.exit as { _tag: string })._tag).toBe("Success");
      expect(vm.seenMethods.some((m) => m.tag === "provider.auth.logout")).toBe(true);
      expect(existsSync(storedPath(dir))).toBe(false);

      // The VM still reports authenticated for a moment: it must not re-store the signed-out token
      vm.pushConfigItems([{ version: 1, type: "providerStatuses", payload: { providers: agProviders("authenticated") } }]);
      await new Promise((r) => setTimeout(r, 150));
      await handle.lifecycle.providerSessions.settle();
      expect(existsSync(storedPath(dir))).toBe(false);
      expect(reads.count).toBe(0);

      // After a sentinel restart, VMs created before the sign-out are still not re-captured
      const restarted = new ProviderSessionStore(cfg, handle.lifecycle.getProvider());
      restarted.debounceMs = 10;
      const sb = handle.state.listHubSandboxes()[0];
      restarted.observe(sb.sandboxId, agProviders("authenticated"), sb.createdAt);
      await restarted.settle();
      expect(existsSync(storedPath(dir))).toBe(false);
      expect(reads.count).toBe(0);
      expect(statSync(join(dir, "credentials", "antigravity.signed-out-at")).mode & 0o777).toBe(0o600);

      const base = `http://127.0.0.1:${handle.server.port}`;
      const call = (body: unknown) =>
        fetch(`${base}/api/forget-session`, {
          method: "POST",
          headers: { host: cfg.controlHost, authorization: `Bearer ${cfg.controlSecret}`, "content-type": "application/json" },
          body: JSON.stringify(body),
        });
      const ok = await call({ instanceId: "antigravity" });
      expect(await ok.json()).toEqual({ ok: true, instanceId: "antigravity", removed: false });
      const bad = await call({ instanceId: "../etc" });
      expect(bad.status).toBe(400);
    });
  });

  describe("persisted provider sessions (Devin sign-in sync)", () => {
    const DEVIN_ID = "acpRegistry_devin";
    const FLAG_HASH = createHash("sha256").update(JSON.stringify(DEVIN_ID)).digest("hex");
    const CREDS_VM = ".local/share/devin/credentials.toml";
    const FLAG_VM = `.t3/caches/acp-auth-${FLAG_HASH}.json`;
    const devinCreds = (key: string) =>
      `api_key = "${key}"\napi_server_url = "https://server.example"\ndevin_api_url = "https://api.example"\n`;
    const devinFlag = JSON.stringify({ binding: "b".repeat(43), authenticated: true });
    const devinProviders = (status: string) => [
      { instanceId: "claudeAgent", driver: "claudeAgent", enabled: true, installed: true },
      { instanceId: DEVIN_ID, driver: "acpRegistry", enabled: true, installed: true, auth: { status } },
    ];
    const storedDir = (dir: string) => join(dir, "credentials", DEVIN_ID);

    /** A fixture home stands in for the VM: listings run for real against it, files come over `download`. */
    function fixtureVm(files: Record<string, string>) {
      const home = mkdtempSync(join(tmpdir(), "t3-sentinel-devin-vmhome-"));
      cleanups.push(() => rmSync(home, { recursive: true, force: true }));
      for (const [rel, content] of Object.entries(files)) {
        mkdirSync(dirname(join(home, rel)), { recursive: true });
        writeFileSync(join(home, rel), content);
      }
      const execs: Array<{ script: string; stdout: string; stderr: string }> = [];
      const downloads: string[] = [];
      const exec = async (_id: string, script: string) => {
        const p = Bun.spawnSync(["bash", "-c", script], { env: { ...process.env, HOME: home } });
        const res = { exitCode: p.exitCode, stdout: p.stdout.toString(), stderr: p.stderr.toString() };
        execs.push({ script, ...res });
        return res;
      };
      const download = async (_id: string, remotePath: string, localPath: string) => {
        downloads.push(remotePath);
        writeFileSync(localPath, readFileSync(join(home, remotePath.replace(/^~\//, ""))));
      };
      return { home, execs, downloads, exec, download };
    }

    function storeWith(opts: { settings?: unknown; files: Record<string, string> }) {
      const dir = mkdtempSync(join(tmpdir(), "t3-sentinel-devin-"));
      cleanups.push(() => rmSync(dir, { recursive: true, force: true }));
      const base = makeTestConfig(0);
      let settingsFile: string | undefined;
      if (opts.settings) {
        settingsFile = join(dir, "t3-settings.json");
        writeFileSync(settingsFile, JSON.stringify(opts.settings));
      }
      const cfg: SentinelConfig = {
        ...base,
        stateDir: dir,
        t3: { ...base.t3, settingsFile, persistProviderSessions: [DEVIN_ID] },
      };
      const vm = fixtureVm(opts.files);
      const provider = { name: "fake", capabilities: new Set(), exec: vm.exec, download: vm.download } as unknown as SandboxProvider;
      return { dir, cfg, vm, store: new ProviderSessionStore(cfg, provider) };
    }

    const devinSettings = (config: Record<string, unknown>) => ({
      providerInstances: { [DEVIN_ID]: { driver: "acpRegistry", enabled: true, config } },
    });

    test("captures credentials.toml and T3's sign-in marker over download (names only in exec), stages both, forgets both", async () => {
      const { dir, vm, store } = storeWith({
        settings: devinSettings({ agentId: "devin", distribution: "auto" }),
        files: { [CREDS_VM]: devinCreds("cog_FIRST-SECRET"), [FLAG_VM]: devinFlag },
      });
      await store.capture("vm-1", DEVIN_ID);

      expect(readFileSync(join(storedDir(dir), "credentials.toml"), "utf8")).toBe(devinCreds("cog_FIRST-SECRET"));
      expect(readFileSync(join(storedDir(dir), "acp-auth.json"), "utf8")).toBe(devinFlag);
      expect(statSync(join(storedDir(dir), "credentials.toml")).mode & 0o777).toBe(0o600);
      expect(statSync(storedDir(dir)).mode & 0o777).toBe(0o700);
      expect(vm.downloads.sort()).toEqual([`~/${CREDS_VM}`, `~/${FLAG_VM}`].sort());
      expect(vm.execs.length).toBe(1);
      expect(vm.execs[0].stdout).toMatch(/^(?:(?:credentials\.toml|acp-auth\.json) \d+\n){2}$/);
      expect(vm.execs[0].script).not.toMatch(/\b(base64|cat|head|tail|xxd|od|dd|less|more|strings|openssl)\b/);
      for (const e of vm.execs) expect(`${e.script}\n${e.stdout}\n${e.stderr}`).not.toContain("SECRET");

      // Not an expiring token: a later capture with different bytes replaces it
      writeFileSync(join(vm.home, CREDS_VM), devinCreds("cog_SECOND-SECRET"));
      await store.capture("vm-2", DEVIN_ID);
      expect(readFileSync(join(storedDir(dir), "credentials.toml"), "utf8")).toBe(devinCreds("cog_SECOND-SECRET"));

      // New VMs get both files at the same paths under $HOME
      const staged = store.stagedCredentials();
      expect(staged.map((s) => s.remotePath).sort()).toEqual(
        [`~/.t3-sentinel/provider-sessions/${CREDS_VM}`, `~/.t3-sentinel/provider-sessions/${FLAG_VM}`].sort(),
      );
      expect(staged.find((s) => s.remotePath.endsWith("credentials.toml"))?.remoteDir).toBe(
        "~/.t3-sentinel/provider-sessions/.local/share/devin",
      );
      expect(store.listStored()).toEqual([DEVIN_ID]);

      expect(store.forget(DEVIN_ID)).toEqual({ removed: true });
      expect(existsSync(storedDir(dir))).toBe(false);
      expect(store.stagedCredentials()).toEqual([]);
    });

    test("the sign-in marker alone is never stored or staged", async () => {
      const { dir, vm, store } = storeWith({
        settings: devinSettings({ agentId: "devin" }),
        files: { [FLAG_VM]: devinFlag },
      });
      await store.capture("vm-1", DEVIN_ID);
      expect(vm.downloads).toEqual([]);
      expect(existsSync(storedDir(dir))).toBe(false);

      // A leftover marker without credentials (e.g. a partial copy) is not staged into new VMs
      mkdirSync(storedDir(dir), { recursive: true, mode: 0o700 });
      writeFileSync(join(storedDir(dir), "acp-auth.json"), devinFlag, { mode: 0o600 });
      expect(store.stagedCredentials()).toEqual([]);
      expect(store.listStored()).toEqual([]);
    });

    test("a marker that fails to copy never blocks storing credentials.toml", async () => {
      const { dir, vm, store } = storeWith({
        settings: devinSettings({ agentId: "devin" }),
        files: { [CREDS_VM]: devinCreds("cog_ONLY-SECRET"), [FLAG_VM]: devinFlag },
      });
      const download = vm.download;
      (store as unknown as { boat: { download: typeof download } }).boat.download = async (id, remote, local) => {
        if (remote.includes("acp-auth-")) throw new Error("file changed during copy");
        return download(id, remote, local);
      };
      await store.capture("vm-1", DEVIN_ID);
      expect(readFileSync(join(storedDir(dir), "credentials.toml"), "utf8")).toBe(devinCreds("cog_ONLY-SECRET"));
      expect(existsSync(join(storedDir(dir), "acp-auth.json"))).toBe(false);
      expect(store.stagedCredentials().map((c) => c.remotePath)).toEqual([`~/.t3-sentinel/provider-sessions/${CREDS_VM}`]);
    });

    test("only the devin ACP agent is supported: other registry agents and local agents are skipped without exec", async () => {
      for (const config of [{ agentId: "cline" }, { agentId: "devin", source: "local", commandPath: "/usr/bin/devin" }]) {
        const { dir, vm, store } = storeWith({
          settings: devinSettings(config),
          files: { [CREDS_VM]: devinCreds("cog_X-SECRET") },
        });
        const warn = console.warn;
        const warnings: string[] = [];
        console.warn = (...args: unknown[]) => void warnings.push(args.map(String).join(" "));
        try {
          await store.capture("vm-1", DEVIN_ID);
        } finally {
          console.warn = warn;
        }
        expect(vm.execs.length).toBe(0);
        expect(existsSync(storedDir(dir))).toBe(false);
        expect(warnings.some((w) => w.includes("is not supported"))).toBe(true);
      }
    });

    test("Hub: captures on an authenticated report, stages into the next VM before provision-base, a sign-out deletes it", async () => {
      // No settings file: the driver comes from the VM's provider status, the agent from T3's default instance ID
      const vm = startFakeVmT3Server("vm-devin", {
        configSnapshot: { providers: devinProviders("authenticated"), settings: {} },
      });
      cleanups.push(() => vm.server.stop(true));
      const { cfg, handle, boat, dir } = await startHubWithVm(vm, "t3-sentinel-devin-hub-", {
        persistProviderSessions: [DEVIN_ID],
      });
      const fixture = fixtureVm({ [CREDS_VM]: devinCreds("cog_HUB-SECRET"), [FLAG_VM]: devinFlag });
      const original = boat.exec.bind(boat);
      boat.exec = async (id: string, script: string) =>
        script.includes("credentials.toml") && script.includes("wc -c") ? fixture.exec(id, script) : original(id, script);
      boat.download = fixture.download;

      const client = await connectHubClient(handle, cfg);
      client.request("cfg", "subscribeServerConfig", {});
      await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "cfg");
      await launchThreadOnHub(client, "thread-devin-capture");
      const start = Date.now();
      while (!existsSync(join(storedDir(dir), "credentials.toml"))) {
        if (Date.now() - start > 5_000) throw new Error("Devin credentials were not captured");
        await new Promise((r) => setTimeout(r, 20));
      }
      await handle.lifecycle.providerSessions.settle();
      expect(readFileSync(join(storedDir(dir), "credentials.toml"), "utf8")).toBe(devinCreds("cog_HUB-SECRET"));
      expect(existsSync(join(storedDir(dir), "acp-auth.json"))).toBe(true);
      expect(readFileSync(join(dir, "state.json"), "utf8")).not.toContain("SECRET");

      // A fresh VM gets both files staged before provision-base.sh runs
      const order: string[] = [];
      boat.upload = async (...args: unknown[]) => void order.push(`upload:${String(args[2])}`);
      const execWithOrder = boat.exec;
      boat.exec = async (id: string, script: string) => {
        if (script.includes("provision-base.sh")) order.push("provision");
        return execWithOrder(id, script);
      };
      const res = await handle.lifecycle.provisionStandaloneVm({
        sandboxId: "sb-next",
        watchdogSlotKey: "slot",
        environmentId: "env",
        label: "Boat",
        projectName: "my-app",
        project: undefined,
        watchdogSecret: "w",
        llmToken: null,
        mintToken: false,
        injectProviderSessions: true,
      });
      expect(res.injectedProviderSessions).toEqual([DEVIN_ID]);
      const credsUpload = order.indexOf(`upload:~/.t3-sentinel/provider-sessions/${CREDS_VM}`);
      expect(credsUpload).toBeGreaterThanOrEqual(0);
      expect(order.indexOf(`upload:~/.t3-sentinel/provider-sessions/${FLAG_VM}`)).toBeGreaterThanOrEqual(0);
      expect(credsUpload).toBeLessThan(order.indexOf("provision"));

      // Signing out of Devin in T3 (ACP registry logout) deletes the stored copy
      client.request("logout", "server.logoutAcpRegistry", { instanceId: DEVIN_ID });
      const exit = await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "logout");
      expect((exit.exit as { _tag: string })._tag).toBe("Success");
      expect(existsSync(storedDir(dir))).toBe(false);
    });

    test("provision-base.sh installs staged files at the same path under $HOME; the bake wipes and refuses Devin sign-ins", () => {
      const script = readFileSync(join(import.meta.dir, "..", "scripts", "provision-base.sh"), "utf8");
      const home = mkdtempSync(join(tmpdir(), "t3-sentinel-devin-install-"));
      cleanups.push(() => rmSync(home, { recursive: true, force: true }));
      const staging = join(home, ".t3-sentinel", "provider-sessions");
      const stage = (rel: string, content: string) => {
        mkdirSync(dirname(join(staging, rel)), { recursive: true });
        writeFileSync(join(staging, rel), content);
      };
      const agRel = `.t3/userdata/providers/antigravity/${"a".repeat(64)}/antigravity-acp/antigravity-oauth-token`;
      stage(CREDS_VM, devinCreds("cog_INSTALL"));
      stage(FLAG_VM, devinFlag);
      stage(agRel, "ya29.INSTALL");
      // Layout staged by a sentinel from before this change (relative to ~/.t3/userdata)
      const legacyRel = `providers/antigravity/${"b".repeat(64)}/antigravity-acp/antigravity-oauth-token`;
      stage(legacyRel, "ya29.LEGACY");

      const install = script.slice(script.indexOf('staged_sessions="$sentinel_dir/provider-sessions"'));
      const loop = install.slice(0, install.indexOf("\nfi\n") + 4);
      const run = Bun.spawnSync(
        ["bash", "-c", `set -euo pipefail; sentinel_dir="$HOME/.t3-sentinel"; userdata_dir="$HOME/.t3/userdata"; restart_t3=0\n${loop}\necho "restart=$restart_t3"`],
        { env: { ...process.env, HOME: home } },
      );
      expect(run.stderr.toString()).toBe("");
      expect(run.stdout.toString().trim()).toBe("restart=1");
      for (const [rel, content] of [
        [CREDS_VM, devinCreds("cog_INSTALL")],
        [FLAG_VM, devinFlag],
        [agRel, "ya29.INSTALL"],
        [`.t3/userdata/${legacyRel}`, "ya29.LEGACY"],
      ]) {
        expect(readFileSync(join(home, rel), "utf8")).toBe(content);
        expect(statSync(join(home, rel)).mode & 0o777).toBe(0o600);
      }
      expect(existsSync(staging)).toBe(false);
      expect(existsSync(join(home, "providers"))).toBe(false);

      // Bake: the wipe covers Devin's credentials and T3's ACP sign-in markers...
      const wipe = script.slice(script.indexOf("wipe_userdata() {"), script.indexOf("\n  }\n", script.indexOf("wipe_userdata() {")));
      expect(wipe).toContain('"$HOME/.local/share/devin/credentials.toml"');
      expect(wipe).toContain('"$HOME"/.t3/caches/acp-auth-*.json');
      // ...and the leak check finds them, without flagging Antigravity's own acp-auth-state.json
      const leakStart = script.indexOf('leaked="$({');
      const leak = script.slice(leakStart, script.indexOf('} 2>/dev/null || true)"', leakStart) + '} 2>/dev/null || true)"'.length);
      mkdirSync(join(home, ".t3/userdata/providers/antigravity/x/antigravity-acp/cache"), { recursive: true });
      writeFileSync(join(home, ".t3/userdata/providers/antigravity/x/antigravity-acp/cache/acp-auth-state.json"), "{}");
      const check = () =>
        Bun.spawnSync(["bash", "-c", `sentinel_dir="$HOME/.t3-sentinel"; userdata_dir="$HOME/.t3/userdata"\n${leak}\nprintf '%s' "$leaked"`], {
          env: { ...process.env, HOME: home },
        })
          .stdout.toString()
          .split("\n")
          .filter(Boolean)
          .map((p) => p.slice(home.length + 1))
          .sort();
      const legacyInstalled = `.t3/userdata/${legacyRel}`;
      expect(check()).toEqual([CREDS_VM, FLAG_VM, agRel, legacyInstalled].sort());
      for (const rel of [CREDS_VM, FLAG_VM, agRel, legacyInstalled]) rmSync(join(home, rel));
      expect(check()).toEqual([]);
    });
  });

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

  test("streams live subscribeWorktreeSetup boot progress, handles dynamic repos via POST /api/projects & T3 projectClone.start, and keeps subscribeVcsStatus open cleanly", async () => {
    const dir = mkdtempSync(join(tmpdir(), "t3-sentinel-hub-dyn-"));
    cleanups.push(() => rmSync(dir, { recursive: true, force: true }));

    const vm1 = startFakeVmT3Server("vm1");
    cleanups.push(() => vm1.server.stop(true));

    const boat = new MockBoat();
    boat.snapshots.add("base-t3");
    boat.snapshots.add("env-my-app");
    boat.upstreamBaseUrl = vm1.url;

    const cfg: SentinelConfig = {
      ...makeTestConfig(0),
      stateDir: dir,
      hub: { host: "boat.boat.example.com", label: "Boat", maxSandboxes: 5 },
    };

    const handle = createSentinelServer(cfg, boat, { startBackgroundLoops: false });
    cleanups.push(() => handle.stop());
    const base = `http://127.0.0.1:${handle.server.port}`;

    // Pair & connect WS
    const pairRes = await fetch(`${base}/api/pair`, {
      method: "POST",
      headers: { host: cfg.controlHost, authorization: `Bearer ${cfg.controlSecret}`, "content-type": "application/json" },
      body: JSON.stringify({ slot: "hub" }),
    });
    const { code } = (await pairRes.json()) as { code: string };
    const tokRes = await fetch(`${base}/oauth/token`, {
      method: "POST",
      headers: { host: "boat.boat.example.com", "content-type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({ subject_token: code, client_label: "Mac" }),
    });
    const { access_token: bearer } = (await tokRes.json()) as { access_token: string };
    const tickRes = await fetch(`${base}/api/auth/websocket-ticket/rpc`, {
      method: "POST",
      headers: { host: "boat.boat.example.com", authorization: `Bearer ${bearer}` },
    });
    const { ticket } = (await tickRes.json()) as { ticket: string };

    const ws = new WebSocket(`ws://127.0.0.1:${handle.server.port}/ws?wsTicket=${ticket}&orchestrationProtocol=2`, {
      headers: { host: "boat.boat.example.com" },
    } as unknown as string[]);
    cleanups.push(() => ws.close());

    const inbox: Array<Record<string, unknown>> = [];
    const waiters: Array<() => void> = [];
    await new Promise<void>((resolve, reject) => {
      ws.addEventListener("open", () => resolve());
      ws.addEventListener("error", () => reject(new Error("ws error")));
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
          setTimeout(r, 40);
        });
      }
      throw new Error(`Timed out waiting for frame; inbox=${JSON.stringify(inbox)}`);
    };

    // Subscribe to shell, project clones, and VCS status with 0 VMs running
    ws.send(JSON.stringify({ _tag: "Request", id: 1, tag: "orchestration.subscribeShell", payload: {}, headers: [] }));
    const shellSnap = await waitForFrame((f) => f._tag === "Chunk" && f.requestId === 1);
    const hubProjectId = String((((shellSnap.values as Array<Record<string, unknown>>)[0].snapshot as Record<string, unknown>).projects as Array<Record<string, unknown>>)[0].id);

    ws.send(JSON.stringify({ _tag: "Request", id: 2, tag: "subscribeProjectClones", payload: {}, headers: [] }));
    const initClones = await waitForFrame((f) => f._tag === "Chunk" && f.requestId === 2);
    expect((initClones.values as unknown[])[0]).toEqual([]);

    // subscribeVcsStatus with 0 running VMs should NOT fail!
    ws.send(JSON.stringify({ _tag: "Request", id: 3, tag: "subscribeVcsStatus", payload: { cwd: "/home/user/my-app" }, headers: [] }));

    // Subscribe to worktree setup progress for thread-progress BEFORE launching
    ws.send(JSON.stringify({ _tag: "Request", id: 4, tag: "subscribeWorktreeSetup", payload: { threadId: "thread-progress" }, headers: [] }));
    const initSetup = await waitForFrame((f) => f._tag === "Chunk" && f.requestId === 4);
    expect((initSetup.values as unknown[])[0]).toBeNull();

    // Launch thread -> should emit running stages AND final `done` WorktreeSetupSnapshot
    ws.send(JSON.stringify({
      _tag: "Request",
      id: 5,
      tag: "orchestration.launchThread",
      payload: { threadId: "thread-progress", projectId: hubProjectId, title: "Progress Test", workspaceStrategy: { type: "root" } },
      headers: [],
    }));
    const launchExit = await waitForFrame((f) => f._tag === "Exit" && f.requestId === 5);
    expect((launchExit.exit as Record<string, unknown>)._tag).toBe("Success");
    const setupDone = await waitForFrame(
      (f) => f._tag === "Chunk" && f.requestId === 4 && (f.values as Array<{ phase?: string }>)?.[0]?.phase === "done",
    );
    expect(setupDone).toBeTruthy();

    // Verify subscribeVcsStatus never errored with an Exit Failure
    expect(inbox.some((f) => f._tag === "Exit" && f.requestId === 3)).toBe(false);

    // Add a repo dynamically via Dashboard/API (`POST /api/projects`) -> broadcasts `project.updated` live!
    const addRes = await fetch(`${base}/api/projects`, {
      method: "POST",
      headers: { host: cfg.controlHost, authorization: `Bearer ${cfg.controlSecret}`, "content-type": "application/json" },
      body: JSON.stringify({ repo: "OmarAlex24/awesome-repo", size: "large", bake: false }),
    });
    expect(addRes.status).toBe(200);
    const addedBody = (await addRes.json()) as { ok: boolean; name: string; project: { snapshot: string; workdir: string } };
    expect(addedBody.name).toBe("awesome-repo");
    expect(addedBody.project.snapshot).toBe("env-awesome-repo");

    const projUpdated = await waitForFrame(
      (f) =>
        f._tag === "Chunk" &&
        f.requestId === 1 &&
        (f.values as Array<{ kind?: string; project?: { title?: string } }>)?.[0]?.kind === "project.updated" &&
        (f.values as Array<{ project?: { title?: string } }>)[0].project?.title === "awesome-repo",
    );
    expect(projUpdated).toBeTruthy();

    // Delete dynamic project -> broadcasts `project.removed` live!
    const delRes = await fetch(`${base}/api/projects/awesome-repo`, {
      method: "DELETE",
      headers: { host: cfg.controlHost, authorization: `Bearer ${cfg.controlSecret}` },
    });
    expect(delRes.status).toBe(200);
    const projRemoved = await waitForFrame(
      (f) => f._tag === "Chunk" && f.requestId === 1 && (f.values as Array<{ kind?: string }>)?.[0]?.kind === "project.removed",
    );
    expect(projRemoved).toBeTruthy();

    // Add a repo natively via T3 Code UI (`projectClone.start`) -> auto-registers & bakes snapshot!
    ws.send(JSON.stringify({
      _tag: "Request",
      id: 6,
      tag: "projectClone.start",
      payload: {
        projectId: "proj-cloned-1",
        title: "cloned-app",
        createdAt: new Date().toISOString(),
        remoteUrl: "https://github.com/OmarAlex24/cloned-app.git",
        destinationPath: "/home/user/cloned-app",
      },
      headers: [],
    }));
    const cloneExit = await waitForFrame((f) => f._tag === "Exit" && f.requestId === 6);
    expect((cloneExit.exit as Record<string, unknown>)._tag).toBe("Success");
    const cloneDone = await waitForFrame(
      (f) =>
        f._tag === "Chunk" &&
        f.requestId === 2 &&
        Array.isArray((f.values as unknown[])?.[0]) &&
        ((f.values as Array<Array<{ projectId?: string; phase?: string }>>)[0]).some(
          (c) => c.projectId === "proj-cloned-1" && c.phase === "done",
        ),
    );
    expect(cloneDone).toBeTruthy();
    expect(boat.snapshots.has("env-cloned-app")).toBe(true);
  });

  test("modular SandboxSdkProvider works without snapshots, forwards private preview Cookie headers, and degrades pause gracefully", async () => {
    const dir = mkdtempSync(join(tmpdir(), "t3-sentinel-sdk-"));
    cleanups.push(() => rmSync(dir, { recursive: true, force: true }));

    const seenCookies: Array<string | null> = [];
    const upstream = Bun.serve({
      port: 0,
      hostname: "127.0.0.1",
      fetch(req) {
        seenCookies.push(req.headers.get("cookie"));
        const url = new URL(req.url);
        if (url.pathname === "/api/orchestration/shell") {
          return Response.json({ projects: [{ id: "vm-p1", title: "my-app", workspaceRoot: "/home/user/my-app" }], threads: [] });
        }
        return Response.json({ ok: true });
      },
    });
    cleanups.push(() => upstream.stop(true));

    const createdInputs: Array<Record<string, unknown>> = [];
    const execCommands: string[] = [];
    const uploadedPaths: string[] = [];
    const fakeSdk: ExternalSdkProviderLike = {
      name: "fake-daytona",
      capabilities: [], // no snapshots, no pause!
      async create(input) {
        createdInputs.push(input);
        return { id: "sdk-box-1" };
      },
      async list() {
        return [{ id: "sdk-box-1", name: "t3s-boat-1", state: "running" }];
      },
      async destroy() {},
      async exec(_id, input) {
        execCommands.push(input.command);
        if (input.command.includes("provision-base.sh")) {
          return {
            exitCode: 0,
            stdout: JSON.stringify({
              environmentId: "sdk-env",
              descriptor: { environmentId: "sdk-env", label: "Boat 1", platform: { os: "linux", arch: "x64" }, serverVersion: "0.1.0", orchestrationProtocolVersion: 2, capabilities: {} },
              authSession: { authenticated: true, scopes: ["orchestration:read"], permissions: ["orchestration:read"] },
              upstreamToken: "sdk-upstream-tok",
              t3Version: "t3 v0.1.0",
            }),
            stderr: "",
          };
        }
        return { exitCode: 0, stdout: "", stderr: "" };
      },
      files: {
        async write(_id, path) {
          uploadedPaths.push(path);
        },
      },
      async previewUrl() {
        return { url: `http://127.0.0.1:${upstream.port}`, headers: { cookie: "boat_gate=secret123" } };
      },
    };

    const provider = new SandboxSdkProvider({ adapter: "fake-daytona" }, fakeSdk);
    expect(provider.capabilities.has("snapshots")).toBe(false);
    expect(provider.capabilities.has("pause")).toBe(false);

    const cfg: SentinelConfig = { ...makeTestConfig(0), stateDir: dir };
    const handle = createSentinelServer(cfg, provider, { startBackgroundLoops: false });
    cleanups.push(() => handle.stop());

    // Baking on a snapshot-less provider throws a clear capability error
    await expect(handle.lifecycle.bake("my-app")).rejects.toThrow(/does not support named snapshots/);

    // Spawning works cleanly without snapshots (runs bakeCmd on boot!) and sends private preview Cookie header!
    const res = await handle.lifecycle.spawn("my-app", { slot: "boat-1" });
    expect(res.slot.phase).toBe("running");
    expect(res.slot.upstreamHeaders).toEqual({ cookie: "boat_gate=secret123" });
    expect(createdInputs[0].snapshot).toBeUndefined();
    expect(execCommands.some((c) => c.includes("pnpm install"))).toBe(true);
    expect(uploadedPaths.some((p) => p.includes("boot-secrets.json"))).toBe(true);
    expect(seenCookies).toContain("boat_gate=secret123");

    // Sleep degrades cleanly without crashing when provider has no pause capability
    const afterSleep = await handle.lifecycle.sleep("boat-1");
    expect(afterSleep.phase).toBe("running");
  });

  test("security hardening: robots.txt, anti-indexing/CSP headers, /oauth/token rate limiting (429), and origin Cloudflare Access gate", async () => {
    const dir = mkdtempSync(join(tmpdir(), "t3-sentinel-sec-"));
    cleanups.push(() => rmSync(dir, { recursive: true, force: true }));

    const boat = new MockBoat();
    const cfg: SentinelConfig = {
      ...makeTestConfig(0),
      stateDir: dir,
      hub: { host: "boat.boat.example.com", label: "Boat", maxSandboxes: 5 },
      security: {
        cloudflareAccess: {
          teamDomain: "example.cloudflareaccess.com",
          aud: "test-aud-tag",
        },
      },
    };

    const handle = createSentinelServer(cfg, boat, { startBackgroundLoops: false });
    cleanups.push(() => handle.stop());
    const base = `http://127.0.0.1:${handle.server.port}`;

    // 1. robots.txt + X-Robots-Tag on Hub & Control
    const robots = await fetch(`${base}/robots.txt`, { headers: { host: "boat.boat.example.com" } });
    expect(robots.status).toBe(200);
    expect(await robots.text()).toContain("Disallow: /");
    expect(robots.headers.get("x-robots-tag")).toContain("noindex");

    // 2. Control plane blocks public host without Cf-Access-Jwt-Assertion (403), while loopback CLI still works!
    const blockedDash = await fetch(`${base}/`, { headers: { host: cfg.controlHost } });
    expect(blockedDash.status).toBe(403);
    const localDash = await fetch(`${base}/`, { headers: { host: "127.0.0.1" } });
    expect(localDash.status).toBe(200);
    expect(localDash.headers.get("x-frame-options")).toBe("DENY");
    expect(localDash.headers.get("content-security-policy")).toContain("frame-ancestors 'none'");

    // /health returns minimal { ok: true } without exposing slot count
    const health = await fetch(`${base}/health`, { headers: { host: cfg.controlHost } });
    expect(await health.json()).toEqual({ ok: true });

    // 3. Brute-forcing /oauth/token locks out IP with 429 after 5 bad guesses
    for (let i = 0; i < 5; i++) {
      const bad = await fetch(`${base}/oauth/token`, {
        method: "POST",
        headers: {
          host: "boat.boat.example.com",
          "cf-connecting-ip": "203.0.113.99",
          "content-type": "application/x-www-form-urlencoded",
        },
        body: "subject_token=wrong-guess",
      });
      expect(bad.status).toBe(400);
    }
    const locked = await fetch(`${base}/oauth/token`, {
      method: "POST",
      headers: {
        host: "boat.boat.example.com",
        "cf-connecting-ip": "203.0.113.99",
        "content-type": "application/x-www-form-urlencoded",
      },
      body: "subject_token=wrong-guess-6",
    });
    expect(locked.status).toBe(429);
    expect(locked.headers.get("retry-after")).toBeTruthy();
  });

  test("BoatProvider private preview exchanges ?_token=... into clean base URL + _port_auth Cookie header", async () => {
    const gateServer = Bun.serve({
      port: 0,
      hostname: "127.0.0.1",
      fetch(req) {
        const url = new URL(req.url);
        if (url.searchParams.get("_token") === "secret-gate-token") {
          return new Response(null, {
            status: 302,
            headers: {
              location: "/",
              "set-cookie": "_port_auth=signed-cookie-val; Path=/; HttpOnly; Secure; Expires=Wed, 25 Nov 2026 00:00:00 GMT",
            },
          });
        }
        return new Response("Forbidden", { status: 403 });
      },
    });
    cleanups.push(() => gateServer.stop(true));

    const { BoatProvider } = await import("./providers/boat.ts");
    const bp = new BoatProvider({ hostVisibility: "private" });
    // Stub private CLI runner to return Boat's exact `--private --localhost --json` shape
    (bp as unknown as { run: (args: string[]) => Promise<string> }).run = async (args: string[]) => {
      expect(args).toContain("--private");
      expect(args).toContain("--localhost");
      return JSON.stringify({
        access: "private",
        isProtected: true,
        localhost: true,
        port: 3773,
        url: `http://127.0.0.1:${gateServer.port}?_token=secret-gate-token`,
      });
    };

    const ep = await bp.exposePort("bx_test", 3773);
    expect(ep.url).toBe(`http://127.0.0.1:${gateServer.port}`);
    expect(ep.headers).toEqual({ cookie: "_port_auth=signed-cookie-val" });
  });

  test("reaperTick never kills an in-flight Hub VM while it is still provisioning", async () => {
    const dir = mkdtempSync(join(tmpdir(), "t3-sentinel-reaper-hub-"));
    cleanups.push(() => rmSync(dir, { recursive: true, force: true }));

    const vm = startFakeVmT3Server("vm-reaper");
    cleanups.push(() => vm.server.stop(true));

    const boat = new MockBoat();
    boat.snapshots.add("env-my-app");
    boat.upstreamBaseUrl = vm.url;

    const cfg: SentinelConfig = {
      ...makeTestConfig(0),
      stateDir: dir,
      hub: { host: "boat.boat.example.com", label: "Boat", maxSandboxes: 5 },
    };
    const handle = createSentinelServer(cfg, boat, { startBackgroundLoops: false });
    cleanups.push(() => handle.stop());

    // Fire reaperTick RIGHT in the middle of provisioning the Hub VM (before setHubSandbox is called!)
    const origExec = boat.exec.bind(boat);
    let reaperRanDuringSpawn = false;
    boat.exec = async (id: string, script: string) => {
      if (!reaperRanDuringSpawn && script.includes("provision-base.sh")) {
        reaperRanDuringSpawn = true;
        await handle.lifecycle.reaperTick();
        expect(boat.deleted).not.toContain(id);
        expect(boat.sandboxes.has(id)).toBe(true);
      }
      return origExec(id, script);
    };

    const sb = await handle.hub!.spawnSandboxForThread("my-app", "thread-reaper-safe");
    expect(reaperRanDuringSpawn).toBe(true);
    expect(sb.phase).toBe("running");
    expect(boat.deleted).not.toContain(sb.sandboxId);
  });

  test("server.updateServerWithProgress rebakes base-t3 + project snapshots and streams progress to T3 Desktop", async () => {
    const dir = mkdtempSync(join(tmpdir(), "t3-sentinel-selfupd-"));
    cleanups.push(() => rmSync(dir, { recursive: true, force: true }));

    const boat = new MockBoat();
    const cfg: SentinelConfig = {
      ...makeTestConfig(0),
      stateDir: dir,
      hub: { host: "boat.boat.example.com", label: "Boat", maxSandboxes: 5 },
    };
    const handle = createSentinelServer(cfg, boat, { startBackgroundLoops: false });
    cleanups.push(() => handle.stop());
    const base = `http://127.0.0.1:${handle.server.port}`;

    const pairRes = await fetch(`${base}/api/pair`, {
      method: "POST",
      headers: { host: cfg.controlHost, authorization: `Bearer ${cfg.controlSecret}`, "content-type": "application/json" },
      body: JSON.stringify({ slot: "hub" }),
    });
    const { code } = (await pairRes.json()) as { code: string };
    const tokRes = await fetch(`${base}/oauth/token`, {
      method: "POST",
      headers: { host: "boat.boat.example.com", "content-type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({ subject_token: code, client_label: "Mac" }),
    });
    const { access_token: bearer } = (await tokRes.json()) as { access_token: string };
    const tickRes = await fetch(`${base}/api/auth/websocket-ticket/rpc`, {
      method: "POST",
      headers: { host: "boat.boat.example.com", authorization: `Bearer ${bearer}` },
    });
    const { ticket } = (await tickRes.json()) as { ticket: string };

    const ws = new WebSocket(`ws://127.0.0.1:${handle.server.port}/ws?wsTicket=${ticket}&orchestrationProtocol=2`, {
      headers: { host: "boat.boat.example.com" },
    } as unknown as string[]);
    cleanups.push(() => ws.close());

    const frames: Array<Record<string, unknown>> = [];
    await new Promise<void>((resolve, reject) => {
      ws.addEventListener("open", () => {
        ws.send(
          JSON.stringify({
            _tag: "Request",
            id: 77,
            tag: "server.updateServerWithProgress",
            payload: { targetVersion: "0.1.12-nightly.20261124.5204", channel: "nightly" },
            headers: [],
          }),
        );
      });
      ws.addEventListener("error", () => reject(new Error("ws error")));
      ws.addEventListener("message", (ev) => {
        const parsed = JSON.parse(String(ev.data)) as Record<string, unknown>;
        frames.push(parsed);
        if (parsed._tag === "Exit" && parsed.requestId === 77) resolve();
      });
    });

    const chunkVals = frames
      .filter((f) => f._tag === "Chunk" && f.requestId === 77)
      .flatMap((f) => (f.values as Array<Record<string, unknown>>) ?? []);
    expect(chunkVals.map((v) => v.type)).toEqual(["progress", "progress", "complete"]);
    expect(boat.snapshots.has("base-t3")).toBe(true);
    expect(boat.snapshots.has("env-my-app")).toBe(true);
    expect(boat.execScripts.some((s) => s.includes("T3_VERSION='0.1.12-nightly.20261124.5204'"))).toBe(true);

    const desc = (await (await fetch(`${base}/.well-known/t3/environment`, { headers: { host: "boat.boat.example.com" } })).json()) as {
      serverVersion: string;
    };
    expect(desc.serverVersion).toBe("0.1.12-nightly.20261124.5204");
  });

  /** Wraps the Hub's `lifecycle.bake` so a test can hold bakes, count them and fail one. */
  function gateBakes(handle: SentinelServerHandle) {
    const original = handle.lifecycle.bake.bind(handle.lifecycle);
    const calls: Array<{ target: string; version?: string }> = [];
    let release: () => void = () => {};
    let gate: Promise<void> = Promise.resolve();
    const failOnce = new Set<string>();
    handle.lifecycle.bake = async (target, opts) => {
      calls.push({ target, version: opts?.t3Version });
      await gate;
      if (failOnce.delete(target)) throw new Error(`bake of ${target} failed`);
      return original(target, opts);
    };
    return {
      calls,
      failOnce,
      hold() {
        gate = new Promise<void>((r) => (release = r));
      },
      release: () => release(),
    };
  }

  async function lifecycleServerVersion(client: Awaited<ReturnType<typeof connectHubClient>>, id: string) {
    client.request(id, "subscribeServerLifecycle", {});
    const chunk = await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === id);
    const ready = (chunk.values as Array<{ type: string; payload: { environment: { serverVersion: string } } }>).find(
      (v) => v.type === "ready",
    );
    return ready?.payload.environment.serverVersion;
  }

  test("server update: a duplicate request joins the round in flight and an already-baked version returns at once", async () => {
    const vm = startFakeVmT3Server("vm-upd");
    cleanups.push(() => vm.server.stop(true));
    const { cfg, handle } = await startHubWithVm(vm, "t3-sentinel-hub-upd-join-");
    const bakes = gateBakes(handle);
    const client = await connectHubClient(handle, cfg);
    const target = "0.1.12-nightly.20261125.5230";

    bakes.hold();
    client.request("unary", "server.updateServer", { targetVersion: target });
    client.request("stream", "server.updateServerWithProgress", { targetVersion: target, channel: "nightly" });
    await new Promise((r) => setTimeout(r, 150));
    bakes.release();

    const unary = await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "unary", 15_000);
    expect(unary.exit).toEqual({ _tag: "Success", value: { targetVersion: target, method: "boot-service" } });
    const streamExit = await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "stream", 15_000);
    expect(streamExit.exit).toEqual({ _tag: "Success", value: null });
    const streamValues = client.inbox
      .filter((f) => f._tag === "Chunk" && f.requestId === "stream")
      .flatMap((f) => f.values as Array<Record<string, unknown>>);
    expect(streamValues.at(-1)).toEqual({ type: "complete", result: { targetVersion: target, method: "boot-service" } });
    expect(streamValues.filter((v) => v.type === "progress").map((v) => v.stage)).toContain("installing");

    // One round for both requests: base-t3 once, env-my-app once
    expect(bakes.calls).toEqual([
      { target: "base", version: target },
      { target: "my-app", version: target },
    ]);
    expect(handle.state.getHub()?.selfUpdate?.version).toBe(target);
    expect(await lifecycleServerVersion(client, "life-1")).toBe(target);

    // The app's queued second update for the same version: success at once, nothing rebaked
    client.request("again", "server.updateServerWithProgress", { targetVersion: target });
    const again = await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "again");
    expect(again.exit).toEqual({ _tag: "Success", value: null });
    expect(bakes.calls).toHaveLength(2);
    expect(await lifecycleServerVersion(client, "life-2")).toBe(target);

    // The marker survives a restart (StateStore.load rebuilds the hub record field by field)
    const reloaded = new StateStore(cfg.stateDir, cfg);
    expect(reloaded.getHub()?.selfUpdate).toEqual(handle.state.getHub()?.selfUpdate);

    // A later rebake of any target (e.g. a scheduled one) invalidates the marker
    await handle.lifecycle.bake("my-app");
    client.request("after-rebake", "server.updateServer", { targetVersion: target });
    await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "after-rebake", 15_000);
    expect(bakes.calls.filter((c) => c.target === "base")).toHaveLength(2);
  });

  test("server update: a different version waits for the round in flight, then runs", async () => {
    const vm = startFakeVmT3Server("vm-upd-seq");
    cleanups.push(() => vm.server.stop(true));
    const { cfg, handle } = await startHubWithVm(vm, "t3-sentinel-hub-upd-seq-");
    const bakes = gateBakes(handle);
    const client = await connectHubClient(handle, cfg);
    const first = "0.1.12-nightly.20261125.5230";
    const second = "0.1.13-nightly.20261125.5245";

    bakes.hold();
    client.request("first", "server.updateServer", { targetVersion: first });
    client.request("second", "server.updateServer", { targetVersion: second });
    client.request("second-dup", "server.updateServer", { targetVersion: second });
    await new Promise((r) => setTimeout(r, 150));
    // Only the first round has started
    expect(bakes.calls).toEqual([{ target: "base", version: first }]);
    bakes.release();

    for (const id of ["first", "second", "second-dup"]) {
      const exit = await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === id, 15_000);
      expect((exit.exit as { _tag: string })._tag).toBe("Success");
    }
    expect(bakes.calls.map((c) => `${c.target}@${c.version}`)).toEqual([
      `base@${first}`,
      `my-app@${first}`,
      `base@${second}`,
      `my-app@${second}`,
    ]);
    expect(handle.state.getHub()?.selfUpdate?.version).toBe(second);
    expect(await lifecycleServerVersion(client, "life")).toBe(second);
  });

  test("server update: a partial failure sends a typed error and is retried in full next time", async () => {
    const vm = startFakeVmT3Server("vm-upd-fail");
    cleanups.push(() => vm.server.stop(true));
    const { cfg, handle } = await startHubWithVm(vm, "t3-sentinel-hub-upd-fail-");
    const bakes = gateBakes(handle);
    const client = await connectHubClient(handle, cfg);
    const target = "0.1.12-nightly.20261125.5230";

    bakes.failOnce.add("my-app");
    client.request("fails", "server.updateServerWithProgress", { targetVersion: target });
    const failed = await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "fails", 15_000);
    expect(failed.exit).toEqual({
      _tag: "Failure",
      cause: [{ _tag: "Fail", error: { _tag: "ServerSelfUpdateError", reason: "bake of my-app failed" } }],
    });
    // base-t3 was rebaked, but the round did not finish: no marker
    expect(handle.state.getHub()?.selfUpdate ?? null).toBeNull();

    client.request("retry", "server.updateServer", { targetVersion: target });
    const retry = await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "retry", 15_000);
    expect((retry.exit as { _tag: string })._tag).toBe("Success");
    expect(bakes.calls.map((c) => c.target)).toEqual(["base", "my-app", "base", "my-app"]);
    expect(handle.state.getHub()?.selfUpdate?.version).toBe(target);
  });

  test("Streamable HTTP /mcp: machine token, 0-VM idle reads, async <2s launch, queued sends, pending requests, and settle -> $0/hr destroy", async () => {
    const dir = mkdtempSync(join(tmpdir(), "t3-sentinel-hub-mcp-"));
    cleanups.push(() => rmSync(dir, { recursive: true, force: true }));

    // Rich mock VM T3 server supporting launchThread, getThreadProjection, dispatchCommand, vcs.listRefs
    const dispatchedCommands: Array<Record<string, unknown>> = [];
    const launchedPayloads: Array<Record<string, unknown>> = [];
    const vmMessages: Array<Record<string, unknown>> = [];
    let vmThreadId = "";
    let pendingRequestStatus = "pending";
    const vmProjectId = "vm-proj-mcp";

    const vmServer = Bun.serve({
      port: 0,
      hostname: "127.0.0.1",
      fetch(req, srv) {
        const url = new URL(req.url);
        if (url.pathname === "/api/orchestration/shell") {
          return Response.json({
            projects: [{ id: vmProjectId, title: "my-app", workspaceRoot: "/home/user/my-app" }],
            threads: [],
          });
        }
        if (req.headers.get("upgrade")?.toLowerCase() === "websocket") {
          srv.upgrade(req);
          return undefined;
        }
        return Response.json({ ok: true });
      },
      websocket: {
        message(ws, raw) {
          const frame = JSON.parse(String(raw)) as { _tag?: string; id?: string; tag?: string; payload?: Record<string, unknown> };
          if (frame._tag === "Ping") {
            ws.sendText(JSON.stringify({ _tag: "Pong" }));
            return;
          }
          if (frame._tag !== "Request" || !frame.id) return;
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
          if (frame.tag === "orchestration.launchThread") {
            const p = frame.payload ?? {};
            launchedPayloads.push(p);
            vmThreadId = String(p.threadId);
            const initMsg = p.initialMessage as { text?: string } | undefined;
            if (initMsg?.text) {
              vmMessages.push({ id: "m-init", role: "user", runId: "run-1", text: initMsg.text });
              vmMessages.push({ id: "m-reply", role: "assistant", runId: "run-1", text: "Working on it!" });
            }
            const threadShell = {
              id: vmThreadId,
              projectId: vmProjectId,
              title: String(p.title ?? "MCP Task"),
              status: "running",
              updatedAt: "2026-11-24T12:00:00.000Z",
              pullRequests: [
                {
                  host: "github.com",
                  repository: "OmarAlex24/my-app",
                  number: 42,
                  url: "https://github.com/OmarAlex24/my-app/pull/42",
                  state: "open",
                  title: "Fix bug",
                },
              ],
            };
            ws.sendText(
              JSON.stringify({
                _tag: "Exit",
                requestId: frame.id,
                exit: { _tag: "Success", value: { threadId: vmThreadId, projection: { thread: threadShell }, sequence: 2 } },
              }),
            );
            ws.sendText(
              JSON.stringify({
                _tag: "Chunk",
                requestId: "hub-shell",
                values: [{ kind: "thread.updated", sequence: 2, location: "active", thread: threadShell }],
              }),
            );
            return;
          }
          if (frame.tag === "orchestration.dispatchCommand") {
            const p = frame.payload ?? {};
            dispatchedCommands.push(p);
            if (p.type === "message.dispatch" && typeof p.text === "string") {
              vmMessages.push({ id: String(p.messageId ?? "m-extra"), role: "user", runId: "run-2", text: p.text });
            }
            if (p.type === "runtime-request.respond") {
              pendingRequestStatus = "resolved";
            }
            ws.sendText(
              JSON.stringify({
                _tag: "Exit",
                requestId: frame.id,
                exit: { _tag: "Success", value: { sequence: 3 } },
              }),
            );
            return;
          }
          if (frame.tag === "orchestration.getThreadProjection") {
            ws.sendText(
              JSON.stringify({
                _tag: "Exit",
                requestId: frame.id,
                exit: {
                  _tag: "Success",
                  value: {
                    thread: {
                      id: vmThreadId,
                      projectId: vmProjectId,
                      title: "Autonomous PR",
                      status: "running",
                      updatedAt: "2026-11-24T12:01:00.000Z",
                      pullRequests: [
                        {
                          host: "github.com",
                          repository: "OmarAlex24/my-app",
                          number: 42,
                          url: "https://github.com/OmarAlex24/my-app/pull/42",
                          state: "open",
                          title: "Fix bug",
                        },
                      ],
                    },
                    messages: vmMessages,
                    turnItems: [
                      ...vmMessages.map((m) => ({
                        type: m.role === "user" ? "user_message" : "assistant_message",
                        messageId: m.id,
                        runId: m.runId,
                      })),
                      {
                        type: "user_input_request",
                        requestId: "req-q1",
                        questions: [{ id: "q1", question: "Ship it?", options: [{ label: "Yes" }, { label: "No" }] }],
                      },
                    ],
                    runs: [{ id: "run-1", ordinal: 1, status: "running", startedAt: "2026-11-24T12:00:00.000Z" }],
                    runtimeRequests: [{ id: "req-q1", kind: "user_input", status: pendingRequestStatus }],
                  },
                },
              }),
            );
            return;
          }
          if (frame.tag === "vcs.listRefs") {
            ws.sendText(
              JSON.stringify({
                _tag: "Exit",
                requestId: frame.id,
                exit: { _tag: "Success", value: { branches: [{ name: "main", current: true }], nextCursor: null } },
              }),
            );
            return;
          }
          ws.sendText(JSON.stringify({ _tag: "Exit", requestId: frame.id, exit: { _tag: "Success", value: {} } }));
        },
      },
    });
    cleanups.push(() => vmServer.stop(true));

    const boat = new MockBoat();
    boat.snapshots.add("base-t3");
    boat.snapshots.add("env-my-app");
    boat.upstreamBaseUrl = `http://127.0.0.1:${vmServer.port}`;

    // Gate VM creation so we can verify async < 2s launch & mid-boot reads/queued sends!
    let releaseBoot!: () => void;
    const bootGate = new Promise<void>((r) => {
      releaseBoot = r;
    });
    const origCreate = boat.create.bind(boat);
    boat.create = async (name: string, size: string, fromSnapshot?: string) => {
      await bootGate;
      return origCreate(name, size, fromSnapshot);
    };

    const cfg: SentinelConfig = {
      ...makeTestConfig(0),
      stateDir: dir,
      hub: { host: "boat.boat.example.com", label: "Boat", maxSandboxes: 2 },
      projects: {
        "my-app": {
          repo: "https://github.com/OmarAlex24/my-app.git",
          snapshot: "env-my-app",
          workdir: "/home/user/my-app",
          defaultProvider: "claudeAgent",
          defaultModel: "claude-sonnet-5-5",
          bakeCmd: "git pull",
          bootCmd: "git fetch origin",
        },
      },
    };
    const handle = createSentinelServer(cfg, boat, { startBackgroundLoops: false });
    cleanups.push(() => handle.stop());
    const base = `http://127.0.0.1:${handle.server.port}`;
    const hubEnvId = handle.state.ensureHub().environmentId;

    // 1. GET /health on Hub works without auth
    const healthRes = await fetch(`${base}/health`, { headers: { host: "boat.boat.example.com" } });
    expect(healthRes.status).toBe(200);
    expect(await healthRes.json()).toEqual({ ok: true, provider: "boat-legacy", activeSandboxes: 0, maxSandboxes: 2 });

    // 2. Unauthenticated POST /mcp -> 401
    const unauth = await fetch(`${base}/mcp`, {
      method: "POST",
      headers: { host: "boat.boat.example.com", "content-type": "application/json" },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "initialize", params: {} }),
    });
    expect(unauth.status).toBe(401);

    // 3. Mint machine Bearer token via Control Plane POST /api/token (stored hashed only)
    const tokRes = await fetch(`${base}/api/token`, {
      method: "POST",
      headers: { host: cfg.controlHost, authorization: `Bearer ${cfg.controlSecret}`, "content-type": "application/json" },
      body: JSON.stringify({ slot: "hub", label: "zumith-worker" }),
    });
    expect(tokRes.status).toBe(200);
    const tokBody = (await tokRes.json()) as { ok: boolean; token: string; clientId: string; mcpUrl: string };
    expect(tokBody.ok).toBe(true);
    expect(tokBody.mcpUrl).toBe("https://boat.boat.example.com/mcp");
    expect(handle.state.getHub()!.clients.some((c) => c.id === tokBody.clientId && c.tokenHash === hashToken(tokBody.token))).toBe(true);

    let sessionId: string | null = null;
    const mcpRpc = async (method: string, params: Record<string, unknown> = {}, id: number | null = 1) => {
      const res = await fetch(`${base}/mcp`, {
        method: "POST",
        headers: {
          host: "boat.boat.example.com",
          authorization: `Bearer ${tokBody.token}`,
          "content-type": "application/json",
          accept: "application/json, text/event-stream",
          "mcp-protocol-version": "2025-06-18",
          ...(sessionId ? { "mcp-session-id": sessionId } : {}),
        },
        body: JSON.stringify(id === null ? { jsonrpc: "2.0", method, params } : { jsonrpc: "2.0", id, method, params }),
      });
      const sid = res.headers.get("mcp-session-id");
      if (sid) sessionId = sid;
      return { res, body: res.status === 202 ? null : ((await res.json()) as Record<string, unknown>) };
    };
    const callTool = async (name: string, args: Record<string, unknown> = {}) => {
      const { res, body } = await mcpRpc("tools/call", { name, arguments: args });
      expect(res.status).toBe(200);
      return body!.result as { isError?: boolean; structuredContent: Record<string, unknown>; content: Array<{ type: string; text: string }> };
    };

    // 4. Streamable HTTP handshake
    const init = await mcpRpc("initialize", { protocolVersion: "2025-06-18", capabilities: {}, clientInfo: { name: "zumith", version: "1.0" } });
    expect(init.res.status).toBe(200);
    expect(sessionId).toBeTruthy();
    expect((init.body!.result as { protocolVersion: string }).protocolVersion).toBe("2025-06-18");

    const notif = await mcpRpc("notifications/initialized", {}, null);
    expect(notif.res.status).toBe(202);

    const toolsList = await mcpRpc("tools/list");
    const toolNames = ((toolsList.body!.result as { tools: Array<{ name: string }> }).tools).map((t) => t.name);
    expect(toolNames).toContain("t3_project_list");
    expect(toolNames).toContain("t3_thread_launch");
    expect(toolNames).toContain("t3_thread_organize");
    expect(toolNames).toContain("sentinel_add_project");

    // 5. 0-VM idle reads ($0/hr): t3_project_list + t3_thread_list + t3_worktree_list
    const projRes = await callTool("t3_project_list");
    const projects = projRes.structuredContent.projects as Array<{
      id: string;
      title: string;
      repositoryIdentity: { provider: string; owner: string; name: string; canonicalKey: string };
    }>;
    expect(projects).toHaveLength(1);
    expect(projects[0].repositoryIdentity).toEqual({
      provider: "github",
      owner: "OmarAlex24",
      name: "my-app",
      canonicalKey: "github.com/omaralex24/my-app",
    });
    const hubProjectId = projects[0].id;

    const emptyThreads = await callTool("t3_thread_list", { allProjects: true });
    expect(emptyThreads.structuredContent.threads).toEqual([]);
    const emptyWorktrees = await callTool("t3_worktree_list");
    expect(emptyWorktrees.structuredContent.refs).toEqual([]);

    // 6. Structured error on unknown projectId
    const badProj = await callTool("t3_thread_launch", { projectId: "does-not-exist", title: "Nope" });
    expect(badProj.isError).toBe(true);
    expect(badProj.structuredContent.code).toBe("project_not_found");

    // 7. Async < 2s t3_thread_launch while VM boot is paused + clientRequestId deduplication!
    const t0 = Date.now();
    const launch1 = await callTool("t3_thread_launch", {
      projectId: hubProjectId,
      title: "Autonomous PR",
      message: "Implement feature X",
      workspaceStrategy: { type: "worktree", baseRef: "origin/main" },
      runtimeMode: "full-access",
      clientRequestId: "zumith-req-1",
    });
    expect(Date.now() - t0).toBeLessThan(2_000);
    expect(launch1.isError).toBeUndefined();
    const launchedTid = String(launch1.structuredContent.threadId);
    expect(launch1.structuredContent.status).toBe("preparing");
    expect(launch1.structuredContent.link).toBe(`t3-thread://v2/${hubEnvId}/${launchedTid}`);

    // Idempotent retry returns SAME threadId, never creates 2nd VM
    const launchDup = await callTool("t3_thread_launch", {
      projectId: hubProjectId,
      title: "Autonomous PR",
      message: "Implement feature X",
      clientRequestId: "zumith-req-1",
    });
    expect(launchDup.structuredContent.threadId).toBe(launchedTid);

    // 8. Mid-boot reads return 200 with status: "preparing" & sandbox progress; sends queue cleanly!
    const midRead = await callTool("t3_thread_read", { threadId: launchedTid, limit: 1, maxCharsPerItem: 1, runLimit: 1 });
    expect(midRead.isError).toBeUndefined();
    const midThread = midRead.structuredContent.thread as { status: string; link: string; sandbox: { phase: string } };
    expect(midThread.status).toBe("preparing");
    expect(midThread.link).toBe(`t3-thread://v2/${hubEnvId}/${launchedTid}`);
    expect(midThread.sandbox.phase).toBe("booting");

    const midList = await callTool("t3_thread_list", { allProjects: true });
    expect((midList.structuredContent.threads as Array<{ threadId: string }>)[0].threadId).toBe(launchedTid);

    const queuedSend = await callTool("t3_thread_send", {
      threadId: launchedTid,
      message: "Also add unit tests",
      mode: "auto",
      clientRequestId: "send-1",
    });
    expect(queuedSend.structuredContent.accepted).toBe(true);
    expect(queuedSend.structuredContent.queued).toBe(true);

    // 9. Release VM boot gate and wait for background launch + queued message flush!
    releaseBoot();
    const deadline = Date.now() + 5_000;
    while (Date.now() < deadline && dispatchedCommands.length === 0) {
      await Bun.sleep(25);
    }
    expect(boat.created).toHaveLength(1);
    expect(launchedPayloads).toHaveLength(1);
    expect(launchedPayloads[0].runtimeMode).toBe("full-access");
    expect(launchedPayloads[0].workspaceStrategy).toEqual({ type: "worktree", baseRef: "origin/main" });
    expect(dispatchedCommands.some((c) => c.type === "message.dispatch" && c.text === "Also add unit tests")).toBe(true);

    // 10. Live reads: t3_thread_read (messages + incremental afterPosition), PRs, pending requests, respond!
    const liveRead = await callTool("t3_thread_read", { threadId: launchedTid, view: "messages", limit: 10, maxCharsPerItem: 100 });
    const liveThread = liveRead.structuredContent.thread as {
      projectId: string;
      status: string;
      pendingRequestCount: number;
      link: string;
      sandbox: { phase: string; sandboxId: string };
    };
    expect(liveThread.projectId).toBe(hubProjectId);
    expect(liveThread.status).toBe("waiting"); // because pending user_input request exists!
    expect(liveThread.pendingRequestCount).toBe(1);
    expect(liveThread.sandbox.phase).toBe("running");
    expect((liveRead.structuredContent.items as Array<{ text: string }>).map((i) => i.text)).toEqual([
      "Implement feature X",
      "Working on it!",
      "Also add unit tests",
    ]);

    const prs = await callTool("list_thread_pull_requests", { threadId: launchedTid });
    expect((prs.structuredContent.pullRequests as Array<{ number: number }>)[0].number).toBe(42);

    const pendList = await callTool("t3_pending_request", { action: "list", threadId: launchedTid });
    expect(pendList.structuredContent.requestIds).toEqual(["req-q1"]);

    const pendRead = await callTool("t3_pending_request", { action: "read", threadId: launchedTid, requestId: "req-q1" });
    expect((pendRead.structuredContent.questions as Array<{ id: string }>)[0].id).toBe("q1");

    const pendResp = await callTool("t3_pending_request", {
      action: "respond",
      threadId: launchedTid,
      requestId: "req-q1",
      answers: { q1: "Yes" },
    });
    expect(pendResp.structuredContent.ok).toBe(true);
    expect(dispatchedCommands.some((c) => c.type === "runtime-request.respond" && c.requestId === "req-q1")).toBe(true);

    // 11. Settle via t3_thread_organize -> dispatches thread.settle AND destroys Boat VM ($0/hr)!
    const settled = await callTool("t3_thread_organize", { threadId: launchedTid, action: "settle" });
    expect(settled.structuredContent.ok).toBe(true);
    expect(settled.structuredContent.sandboxDestroyed).toBe(true);
    expect(boat.deleted).toContain(liveThread.sandbox.sandboxId);

    // 12. Sending to destroyed thread returns structured `sandbox_destroyed` error
    const afterDestroy = await callTool("t3_thread_send", { threadId: launchedTid, message: "Hello?" });
    expect(afterDestroy.isError).toBe(true);
    expect(afterDestroy.structuredContent.code).toBe("sandbox_destroyed");

    // 13. Dynamic repo onboarding via sentinel_add_project
    const added = await callTool("sentinel_add_project", { repo: "OmarAlex24/new-svc", bake: false });
    expect(added.structuredContent.ok).toBe(true);
    expect(added.structuredContent.fallbackReady).toBe(true);
    expect((added.structuredContent.repositoryIdentity as { owner: string; name: string }).name).toBe("new-svc");
  });
});

describe("formatRpcFailure", () => {
  test("reads Effect 4 array causes from 0.1.12+ VMs", () => {
    expect(formatRpcFailure([{ _tag: "Die", defect: "Unknown request tag: x" }])).toBe("Unknown request tag: x");
    expect(formatRpcFailure([{ _tag: "Die", defect: { name: "Error", message: "boom" } }])).toBe("boom");
    expect(
      formatRpcFailure([
        { _tag: "Fail", error: { _tag: "ProviderSetupError", instanceId: "antigravity", operation: "subscribe", detail: "No provider" } },
      ]),
    ).toBe("No provider");
    expect(formatRpcFailure([{ _tag: "Fail", error: { _tag: "ServerSelfUpdateError", reason: "bake failed" } }])).toBe(
      "bake failed",
    );
    expect(formatRpcFailure([{ _tag: "Interrupt", fiberId: 3 }])).toBe("interrupted");
    expect(
      formatRpcFailure([
        { _tag: "Fail", error: { message: "first" } },
        { _tag: "Die", defect: "second" },
        { _tag: "Die", defect: "second" },
      ]),
    ).toBe("first; second");
    expect(formatRpcFailure([])).toBe("unknown error");
  });

  test("still reads strings and the older single-object cause", () => {
    expect(formatRpcFailure("plain text")).toBe("plain text");
    expect(formatRpcFailure({ _tag: "Fail", error: { _tag: "HubError", message: "legacy" } })).toBe("legacy");
    expect(formatRpcFailure({ _tag: "Die", defect: "legacy defect" })).toBe("legacy defect");
    expect(formatRpcFailure({ message: "bare" })).toBe("bare");
    expect(formatRpcFailure(null)).toBe("unknown error");
    expect(formatRpcFailure({ _tag: "Fail", error: { code: 7 } })).toBe('{"code":7}');
  });
});

describe("hub settings overlay", () => {
  const settings = () => ({
    favorites: [{ provider: "codex", model: "gpt-5" }],
    providerModelPreferences: null,
    providerInstances: { opencode: { driver: "opencode", enabled: true }, codex: { driver: "codex", enabled: true } },
    previousWorktreesDirectories: [],
    worktreesDirectory: "",
  });
  const parse = (payload: unknown) => {
    const p = parseSettingsUpdate(payload);
    if (!p.ok) throw new Error(p.message);
    return p.value;
  };

  test("composes changes: later favorites per model win, an absolute list supersedes, orders stay per provider", () => {
    let overlay = recordSettingsUpdate(null, parse({ patch: { setModelFavorites: [{ provider: "a", model: "1", favorite: true }] } }), settings());
    overlay = recordSettingsUpdate(overlay, parse({ patch: { setModelFavorites: [{ provider: "a", model: "1", favorite: false }] } }), settings());
    expect(overlay.patch.setModelFavorites).toEqual([{ provider: "a", model: "1", favorite: false }]);
    overlay = recordSettingsUpdate(overlay, parse({ patch: { setProviderModelOrder: { provider: "a", modelOrder: ["1"] } } }), settings());
    overlay = recordSettingsUpdate(overlay, parse({ patch: { setProviderModelOrder: { provider: "b", modelOrder: ["2"] } } }), settings());
    expect(overlay.modelOrders).toEqual({ a: ["1"], b: ["2"] });
    overlay = recordSettingsUpdate(overlay, parse({ patch: { favorites: [{ provider: "z", model: "9" }] } }), settings());
    expect(overlay.patch.favorites).toEqual([{ provider: "z", model: "9" }]);
    expect(overlay.patch.setModelFavorites).toBeUndefined();
    const applied = applySettingsOverlay(settings(), overlay);
    expect(applied.favorites).toEqual([{ provider: "z", model: "9" }]);
    expect(applied.providerModelPreferences).toEqual({ a: { hiddenModels: [], modelOrder: ["1"] }, b: { hiddenModels: [], modelOrder: ["2"] } });
  });

  test("providerInstanceMutation: create is recorded as upsert, remove is kept, a whole map becomes per-instance changes", () => {
    let overlay = recordSettingsUpdate(
      null,
      parse({ patch: {}, providerInstanceMutation: { operation: "create", instanceId: "oc2", instance: { driver: "opencode" } } }),
      settings(),
    );
    expect(overlay.providerInstances.oc2).toEqual({ operation: "upsert", instance: { driver: "opencode" } });
    overlay = recordSettingsUpdate(overlay, parse({ patch: {}, providerInstanceMutation: { operation: "remove", instanceId: "codex" } }), settings());
    expect(overlay.providerInstances.codex).toEqual({ operation: "remove" });
    overlay = recordSettingsUpdate(overlay, parse({ patch: { providerInstances: { opencode: { driver: "opencode", enabled: false } } } }), settings());
    // codex and oc2 are left out of the whole map: removed, as T3 replaces the map
    expect(overlay.providerInstances).toEqual({
      oc2: { operation: "remove" },
      codex: { operation: "remove" },
      opencode: { operation: "upsert", instance: { driver: "opencode", enabled: false } },
    });
    const calls = overlayReplayCalls(overlay);
    expect(calls.every((c) => c.providerInstanceMutation?.operation !== "create")).toBe(true);
    // An instance added later by t3-settings.json that the user never touched is kept
    const withNew = { ...settings(), providerInstances: { ...settings().providerInstances, devin: { driver: "acpRegistry" } } };
    expect(Object.keys(applySettingsOverlay(withNew, overlay).providerInstances as object).sort()).toEqual(["devin", "opencode"]);
  });

  test("a later change that echoes a redacted secret keeps the raw secret recorded earlier", () => {
    const env = (value: string, redacted = false) => [{ name: "OPENCODE_KEY", value, sensitive: true, ...(redacted ? { valueRedacted: true } : {}) }];
    const upsert = (instance: Record<string, unknown>) =>
      parse({ patch: {}, providerInstanceMutation: { operation: "upsert", instanceId: "opencode", instance } });
    let overlay = recordSettingsUpdate(null, upsert({ driver: "opencode", enabled: true, environment: env("sk-env-raw") }), settings());
    // What the app sends when the switch is turned off: T3's redacted view of the instance
    overlay = recordSettingsUpdate(overlay, upsert({ driver: "opencode", enabled: false, environment: env("", true) }), settings());
    expect(overlay.providerInstances.opencode).toEqual({
      operation: "upsert",
      instance: { driver: "opencode", enabled: false, environment: env("sk-env-raw") },
    });
    overlay = recordSettingsUpdate(overlay, parse({ patch: { cloudSpeechCredentials: { openai: "sk-voice-raw" } } }), settings());
    overlay = recordSettingsUpdate(overlay, parse({ patch: { cloudSpeechCredentials: { openai: "\u2022\u2022\u2022\u2022\u2022\u2022", xai: "x" } } }), settings());
    expect(overlay.patch.cloudSpeechCredentials).toEqual({ openai: "sk-voice-raw", xai: "x" });
  });

  test("replaying is idempotent, and secrets and instance config never enter the Hub copy", () => {
    const withConfig = { ...settings(), providerInstances: { opencode: { driver: "opencode", enabled: true, config: { apiKey: "\u2022\u2022\u2022" } } } };
    let overlay = recordSettingsUpdate(null, parse({ patch: { cloudSpeechCredentials: { openai: "sk-raw" }, fastWorktreeCheckout: true } }), withConfig);
    overlay = recordSettingsUpdate(
      overlay,
      parse({
        patch: {},
        providerInstanceMutation: { operation: "upsert", instanceId: "opencode", instance: { driver: "opencode", enabled: false, config: { apiKey: "sk-typed" } } },
      }),
      withConfig,
    );
    const once = applySettingsOverlay(withConfig, overlay);
    expect(applySettingsOverlay(once, overlay)).toEqual(once);
    expect(JSON.stringify(once)).not.toContain("sk-");
    expect(once.providerInstances).toEqual({ opencode: { driver: "opencode", enabled: false, config: { apiKey: "\u2022\u2022\u2022" } } });
    // The VM still gets the user's raw values
    expect(JSON.stringify(overlayReplayCalls(overlay))).toContain("sk-raw");
  });
});

describe("hub placeholder thread", () => {
  // Required keys of the T3 client's decoders (0.1.13-nightly.20261125.5245): a record missing one
  // fails to decode, and a bad shell row breaks the whole sidebar.
  const SHELL_KEYS = [
    "createdBy", "creationSource", "id", "projectId", "title", "providerInstanceId", "modelSelection", "runtimeMode",
    "interactionMode", "branch", "worktreePath", "lineage", "forkedFrom", "activeProviderThreadId", "latestRunId",
    "activeRunId", "status", "pendingRuntimeRequest", "latestVisibleMessage", "latestUserMessageAt",
    "hasActionableProposedPlan", "itemCount", "visibleItemCount", "createdAt", "updatedAt", "archivedAt",
    "settledOverride", "settledAt", "deletedAt",
  ];
  const PROJECTION_KEYS = [
    "thread", "runs", "attempts", "nodes", "subagents", "providerSessions", "providerThreads", "providerTurns",
    "runtimeRequests", "messages", "plans", "turnItems", "checkpointScopes", "checkpoints", "contextHandoffs",
    "contextTransfers", "visibleTurnItems", "updatedAt",
  ];
  const payload = {
    commandId: "c1",
    threadId: "t1",
    projectId: "p1",
    title: "Hello",
    modelSelection: { instanceId: "claudeAgent", model: "claude-opus-5-5" },
    runtimeMode: "full-access",
    interactionMode: "default",
    workspaceStrategy: { type: "root" },
    initialMessage: { messageId: "m1", text: "hi", attachments: [] },
  };

  test("has every key the client requires, before and after a failure", () => {
    const placeholder = buildPlaceholderThread(placeholderInputFrom("launch", payload, "t1", "p1")!);
    for (const record of [placeholder, failPlaceholderThread(placeholder, "boom")]) {
      for (const key of SHELL_KEYS) expect(Object.keys(record.shell)).toContain(key);
      for (const key of PROJECTION_KEYS) expect(Object.keys(record.projection)).toContain(key);
    }
  });

  test("shows the first message as a timeline row and the starting state as a system notice", () => {
    const { projection, shell } = buildPlaceholderThread(placeholderInputFrom("launch", payload, "t1", "p1")!);
    const rows = projection.visibleTurnItems as Array<{ item: Record<string, unknown> }>;
    expect(rows.map((r) => r.item.type)).toEqual(["user_message", "notification"]);
    expect(rows[0].item).toMatchObject({ messageId: "m1", text: "hi" });
    expect(rows[1].item).toMatchObject({ source: { kind: "system" }, outcome: "updated" });
    expect(shell).toMatchObject({ status: "preparing", activityRunStatus: "preparing" });
    const failed = failPlaceholderThread({ shell, projection }, "no capacity");
    const failedRows = failed.projection.visibleTurnItems as Array<{ item: Record<string, unknown> }>;
    expect(failedRows.filter((r) => r.item.type === "notification")).toHaveLength(1);
    expect(failedRows.at(-1)!.item).toMatchObject({ outcome: "failed", detail: "no capacity" });
    expect(failed.shell).toMatchObject({ status: "failed", lastError: "no capacity" });
  });

  test("falls back to the awaited launch when the payload lacks what a placeholder needs", () => {
    const { modelSelection: _m, ...withoutModel } = payload;
    expect(placeholderInputFrom("launch", withoutModel, "t1", "p1")).toBeNull();
  });
});
