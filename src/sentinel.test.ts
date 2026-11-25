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
import {
  forwarderProbeScript,
  forwarderStopScript,
  judgeForwarder,
  parseForwarderProbe,
  type ForwarderFacts,
  type KlavClient,
} from "./klav.ts";
import { HubWakeCooldownError, redactForLog, wakeBackoffMs } from "./hub-wake.ts";
import { LifecycleManager } from "./lifecycle.ts";
import { formatRpcFailure } from "./hub-mcp.ts";
import { buildPlaceholderThread, failPlaceholderThread, placeholderInputFrom } from "./hub-placeholder.ts";
import { chatThreadIdOf, threadGoneError } from "./hub.ts";
import { defaultBranchOfRefs, fallbackVcsRefs, templateVcsRefs } from "./vcs-refs.ts";
import { normalizeGitRemoteUrl, repositoryIdentityFor } from "./repository-identity.ts";
import { REPO_SYNC_MARKER, buildRepoSyncScript, parseRepoSyncOutput } from "./repo-sync.ts";
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
import { ProviderSetupRouter, ProviderStatusBook } from "./hub-providers.ts";
import { StateStore, buildAutoProjectConfig, normalizeProjectWorkdir } from "./state.ts";
import { BAKE_STEPS, bakeProgress, formatElapsed, type BakeStep } from "./bake-progress.ts";
import { T3ReleaseResolver } from "./t3-release.ts";
import { ATTACHMENT_UPLOAD_URL_TTL_MS, parseUploadRequest } from "./hub-attachments.ts";
import { loadT3Contracts } from "./testing/t3-contracts.ts";
import { HubMediaProxy, PREVIEW_ACK, newMediaWsData, type HubMediaWsData } from "./hub-media.ts";

/**
 * The installed T3 app's own RPC schemas (`T3_APP_ASAR`, else the macOS Nightly app), or null when
 * no app is installed (CI): contract decodes then skip, the plain assertions next to them still run.
 */
const t3Contracts = await loadT3Contracts();
if (t3Contracts) console.log(`[contracts] decoding with the T3 app's schemas, version ${t3Contracts.version}`);
else console.log("[contracts] no T3 app bundle found: contract decodes skipped");

/** The exec that runs provisioning (the bundle's place script only names the file). */
const RUN_PROVISION = "bash ~/.t3-sentinel/provision-base.sh";

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
  /** Answers an exec before the defaults; return undefined to fall through. */
  execHook?: (id: string, script: string) => Promise<BoatExecResult> | undefined;
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
    const hooked = this.execHook?.(id, script);
    if (hooked) return hooked;
    if (script.includes("t3s_place()")) {
      if (!this.vmHome) return { exitCode: 0, stdout: "", stderr: "" };
      // The real place script, run by bash against the fake VM's home
      const proc = Bun.spawnSync(["bash", "-c", script], { env: { ...process.env, HOME: this.vmHome } });
      return { exitCode: proc.exitCode, stdout: proc.stdout.toString(), stderr: proc.stderr.toString() };
    }
    if (script.includes(REPO_SYNC_MARKER)) {
      // A VM whose workdir is not a checkout, like most fixtures here
      return { exitCode: 0, stdout: JSON.stringify({ status: "skipped", reason: "not-a-repository" }), stderr: "" };
    }
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
    if (script.includes(RUN_PROVISION)) {
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

  /** Every upload with the bytes sent, read at upload time (local staging is removed right after). */
  uploads: Array<{ id: string; local: string; remote: string; bytes: Uint8Array }> = [];
  /** When set, uploads land under this dir as the VM's home and the bundle's place scripts run there in bash. */
  vmHome: string | null = null;
  /** Artificial latency of each upload (a `boat scp` takes ~4s in production). */
  uploadDelayMs = 0;
  /** When set, `upload` fails with it. */
  uploadError: Error | null = null;

  /** A remote path inside the fake VM (`~/` and relative paths resolve against `vmHome`). */
  vmPath(remote: string): string {
    const home = this.vmHome!;
    if (remote.startsWith("~/")) return join(home, remote.slice(2));
    if (remote.startsWith("/")) {
      if (!remote.startsWith(home)) throw new Error(`fake VM path outside its home: ${remote}`);
      return remote;
    }
    return join(home, remote);
  }

  async upload(id: string, localPath: string, remotePath: string): Promise<void> {
    const bytes = existsSync(localPath) ? new Uint8Array(readFileSync(localPath)) : new Uint8Array();
    this.uploads.push({ id, local: localPath, remote: remotePath, bytes });
    if (this.uploadDelayMs) await Bun.sleep(this.uploadDelayMs);
    if (this.uploadError) throw this.uploadError;
    if (this.vmHome) {
      const dest = this.vmPath(remotePath);
      mkdirSync(dirname(dest), { recursive: true });
      writeFileSync(dest, bytes, { mode: 0o600 });
    }
  }

  downloads: Array<{ id: string; remotePath: string; localPath: string }> = [];
  async download(id: string, remotePath: string, localPath: string): Promise<void> {
    this.downloads.push({ id, remotePath, localPath });
    throw new Error("no such file");
  }
}

const tempDirs: string[] = [];
const cleanups: Array<() => void> = [];

/** A fresh dir standing in for a VM's home (see `MockBoat.vmHome`). */
function fakeVmHome(): string {
  const dir = mkdtempSync(join(tmpdir(), "t3-sentinel-vmhome-"));
  tempDirs.push(dir);
  return dir;
}

/** The chat a request a fake VM received names, top-level or in `resource`. */
function extractThreadIdForTest(payload: unknown): string | undefined {
  const p = (payload ?? {}) as { threadId?: string; resource?: { threadId?: string } };
  return p.threadId ?? p.resource?.threadId;
}

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
      settleAction: "park",
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
      return { uploads, provision, execs };
    };

    // Celesto-like: no snapshots, no download
    const celestoLike = await run({ snapshots: false, download: false });
    expect(celestoLike.uploads.some((r) => r.includes("provider-sessions"))).toBe(false);
    expect(celestoLike.provision.script).toContain("T3_PREPARE_ACP_AGENTS='devin'");
    expect(celestoLike.provision.script).toContain("T3_INSTALL_PROVIDERS='antigravity'");
    expect(celestoLike.provision.timeout).toBe(900);

    // Boat-like: snapshots + download keep today's behavior
    const boatLike = await run({ snapshots: true, download: true });
    // One archive upload; the place script puts the stored sign-in under the staging dir
    expect(boatLike.uploads).toHaveLength(1);
    expect(boatLike.execs.some((e) => e.script.includes("t3s_place()") && e.script.includes("provider-sessions"))).toBe(true);
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

  test("project bakes record the project as the bake VM's T3 reports it: shell row, refs and default branch", async () => {
    const config = makeTestConfig();
    const state = new StateStore(config.stateDir, config);
    state.ensureHub("Boat", Object.keys(state.getAllProjects(config)));
    const boat = new MockBoat();
    const workdir = "/home/user/my-app";
    const identity = repositoryIdentityFor("git@github.com:acme/my-app.git", workdir);
    const projectShell = {
      id: "vm-project-1",
      title: "my-app",
      workspaceRoot: workdir,
      repositoryIdentity: identity,
      defaultModelSelection: null,
      scripts: [],
      createdAt: "2026-11-25T00:00:00.000Z",
      updatedAt: "2026-11-25T00:00:00.000Z",
      deletedAt: null,
    };
    const vcsRefs = {
      isRepo: true,
      hasPrimaryRemote: true,
      nextCursor: null,
      totalCount: 3,
      refs: [
        { name: "master", current: true, isDefault: true, isRemote: false, worktreePath: null },
        { name: "release", current: false, isDefault: false, isRemote: false, worktreePath: null },
        { name: "origin/master", current: false, isDefault: true, isRemote: true, remoteName: "origin", worktreePath: null },
      ],
    };
    boat.execHook = (_id, script) =>
      script.includes("BAKE_CLEAN=1") && script.includes("BAKE_PROJECT_WORKDIR=")
        ? Promise.resolve({ exitCode: 0, stdout: JSON.stringify({ t3Version: "t3 v0.1.0", projectShell, vcsRefs, defaultBranch: "master", cleaned: true }), stderr: "" })
        : undefined;
    const lifecycle = new LifecycleManager(config, state, boat);
    const bakeScript = () => boat.execScripts.filter((s) => s.includes("BAKE_CLEAN=1")).at(-1) ?? "";

    await lifecycle.bake("base");
    expect(bakeScript()).not.toContain("BAKE_PROJECT_WORKDIR");
    expect(state.getHub()!.projectTemplates["my-app"]).toBeUndefined();

    await lifecycle.bake("my-app");
    expect(bakeScript()).toContain(`BAKE_PROJECT_WORKDIR='${workdir}'`);
    // A clean clone's refs: the bake's local default branch is kept, other local branches are not
    expect(state.getHub()!.projectTemplates["my-app"]).toEqual({ shell: projectShell, vcsRefs: templateVcsRefs(vcsRefs), defaultBranch: "master" });
    expect(((state.getHub()!.projectTemplates["my-app"]!.vcsRefs as { refs: Array<{ name: string }> }).refs).map((r) => r.name)).toEqual([
      "master",
      "origin/master",
    ]);
    expect(defaultBranchOfRefs(vcsRefs)).toBe("master");
    if (t3Contracts) {
      expect(t3Contracts.decodeWith("OrchestrationProjectShell", projectShell)).toEqual({ ok: true });
      expect(t3Contracts.decodeSuccess("vcs.listRefs", vcsRefs)).toEqual({ ok: true });
    }
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
    const provExec = boat.execScripts.find((s) => s.includes(RUN_PROVISION))!;
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
      /** Attachment ids this VM holds: `assets.createUrl` for any other attachment fails as T3 does. */
      attachments?: string[];
      /**
       * Run a provider sign-in like T3: `provider.auth.start` answers `waiting` with a flow,
       * `provider.auth.complete` answers `succeeded` and publishes it on the open `provider.auth.subscribe` streams.
       */
      signIn?: boolean;
    } = {},
  ) {
    let assetSeq = 0;
    let uploadSeq = 0;
    /** Upload URLs this VM issued, and the bytes clients (the Hub) posted to them. */
    const uploadUrls = new Map<string, { attachmentId: string; payload: Record<string, unknown> }>();
    const uploads: Array<{ attachmentId: string; payload: Record<string, unknown>; contentType: string | null; bytes: Uint8Array }> = [];
    /** Active threads this VM holds, as rows its shell snapshot lists (like a real VM after a reconnect). */
    const threads = new Map<string, Record<string, unknown>>();
    let vmSettings = (opts.configSnapshot?.settings ?? null) as Record<string, unknown> | null;
    const seenMethods: Array<{ tag: string; payload: unknown }> = [];
    const acks: string[] = [];
    const vmProjectId = `vm-proj-${vmTag}`;
    /** This VM's project as T3 lists it in a shell snapshot (every key `OrchestrationProjectShell` requires). */
    const vmProjectRow = {
      id: vmProjectId,
      title: "my-app",
      workspaceRoot: "/home/user/my-app",
      defaultModelSelection: null,
      scripts: [],
      createdAt: "2026-11-01T00:00:00.000Z",
      updatedAt: "2026-11-01T00:00:00.000Z",
      deletedAt: null,
    };
    let configStream: { ws: { sendText(data: string): unknown }; requestId: string } | null = null;
    let shellStream: { ws: { sendText(data: string): unknown }; requestId: string } | null = null;
    const authStreams: Array<{ ws: { sendText(data: string): unknown }; requestId: string }> = [];
    let flowSeq = 0;
    /** With `signIn`: the last sign-in state, which a new `provider.auth.subscribe` gets first (like T3). */
    let authState: Record<string, unknown> | null = null;
    // Preview media, like T3's /api/preview-stream: who connected (and how), and what they sent
    const mediaConnections: Array<{ path: string; query: Record<string, string>; authorization: string | null }> = [];
    const mediaMessages: string[] = [];
    const previewEventStreams: Array<{ ws: { sendText(data: string): unknown }; requestId: string }> = [];
    const server = Bun.serve({
      port: 0,
      hostname: "127.0.0.1",
      fetch(req, srv) {
        const url = new URL(req.url);
        if (url.pathname.startsWith("/api/preview-stream/") || url.pathname.startsWith("/api/device-hub/")) {
          mediaConnections.push({
            path: url.pathname,
            query: Object.fromEntries(url.searchParams),
            authorization: req.headers.get("authorization"),
          });
          if (req.headers.get("upgrade")?.toLowerCase() === "websocket") {
            (srv as unknown as Bun.Server<{ media: boolean }>).upgrade(req, { data: { media: true } });
            return undefined;
          }
          return Response.json({ vmTag, media: url.pathname });
        }
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
        if (req.method === "POST" && url.pathname.startsWith("/api/attachments/upload/")) {
          const issued = uploadUrls.get(url.pathname.slice("/api/attachments/upload/".length));
          if (!issued) return new Response("Not Found", { status: 404 });
          return req.arrayBuffer().then((buf) => {
            uploads.push({ ...issued, contentType: req.headers.get("content-type"), bytes: new Uint8Array(buf) });
            return new Response(null, { status: 204 });
          });
        }
        return Response.json({ ok: true, vmTag });
      },
      websocket: {
        open(ws) {
          // A preview stream starts with a JPEG frame; this one names the VM that drew it
          if ((ws.data as { media?: boolean } | undefined)?.media) ws.sendBinary(new TextEncoder().encode(`frame-from-${vmTag}`));
        },
        message(ws, raw) {
          if ((ws.data as { media?: boolean } | undefined)?.media) {
            const text = typeof raw === "string" ? raw : new TextDecoder().decode(raw);
            mediaMessages.push(text);
            const msg = JSON.parse(text) as { type?: string };
            if (msg.type === "ack") ws.sendText(JSON.stringify({ type: "control", acked: vmTag }));
            if (msg.type === "host-setup") ws.close(4503, JSON.stringify({ need: "libraries", command: "t3 browser setup" }));
            return;
          }
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
          if (opts.signIn && (frame.tag === "provider.auth.start" || frame.tag === "provider.auth.complete")) {
            const done = frame.tag === "provider.auth.complete";
            if (!done) flowSeq++;
            const state = {
              instanceId: frame.payload?.instanceId,
              phase: done ? "succeeded" : "waiting",
              flowId: `flow-${vmTag}-${flowSeq}`,
              authorizationUrl: null,
              expiresAt: null,
              message: null,
            };
            authState = state;
            ws.sendText(JSON.stringify({ _tag: "Exit", requestId: frame.id, exit: { _tag: "Success", value: state } }));
            for (const s of authStreams) {
              s.ws.sendText(JSON.stringify({ _tag: "Chunk", requestId: s.requestId, values: [state] }));
            }
            return;
          }
          if (frame.tag === "provider.install.subscribe" || frame.tag === "provider.auth.subscribe") {
            if (frame.tag === "provider.auth.subscribe") authStreams.push({ ws, requestId: frame.id });
            const current = frame.tag === "provider.auth.subscribe" && authState ? authState : null;
            // Like a real VM: one snapshot, stream stays open
            ws.sendText(
              JSON.stringify({
                _tag: "Chunk",
                requestId: frame.id,
                values: [current ?? { instanceId: frame.payload?.instanceId, phase: "idle", fromVm: vmTag }],
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
            shellStream = { ws, requestId: frame.id };
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
                      projects: [vmProjectRow],
                      threads: [...threads.values()],
                      archivedThreads: [],
                    },
                  },
                ],
              }),
            );
            // As T3 0.1.13 does: a second snapshot that only resolves the projects' repository identity.
            // Its `threads` is empty and the app keeps the threads it already has.
            ws.sendText(
              JSON.stringify({
                _tag: "Chunk",
                requestId: frame.id,
                values: [
                  {
                    kind: "snapshot",
                    snapshot: { schemaVersion: 2, snapshotSequence: 1, projects: [vmProjectRow], threads: [], archivedThreads: [] },
                    resolvedRepositoryIdentityRoots: [vmProjectRow.workspaceRoot],
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
          if (frame.tag === "subscribePreviewEvents") {
            previewEventStreams.push({ ws, requestId: frame.id });
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
                  value: vmThreadProjection(threadId, vmProjectId),
                },
              }),
            );
            return;
          }
          if (frame.tag === "orchestration.launchThread") {
            const p = frame.payload ?? {};
            const threadId = String(p.threadId);
            const threadShell = vmThreadRow(threadId, vmProjectId, String(p.title ?? "Chat"), `/home/user/.t3/worktrees/my-app/${vmTag}-${threadId}`);
            threads.set(threadId, threadShell);
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
          if (frame.tag === "assets.createUrl") {
            const resource = (frame.payload?.resource ?? {}) as Record<string, unknown>;
            const exit =
              resource._tag === "attachment" && !(opts.attachments ?? []).includes(String(resource.attachmentId))
                ? { _tag: "Failure", cause: [{ _tag: "Fail", error: { _tag: "AssetAttachmentNotFoundError", resource } }] }
                : { _tag: "Success", value: { relativeUrl: `/api/assets/tok-${vmTag}-${++assetSeq}/image.png`, expiresAt: 1 } };
            ws.sendText(JSON.stringify({ _tag: "Exit", requestId: frame.id, exit }));
            return;
          }
          if (frame.tag === "attachments.createUploadUrl") {
            const n = ++uploadSeq;
            const attachmentId = `vm-${vmTag}-att-${n}`;
            uploadUrls.set(`vmtok-${n}`, { attachmentId, payload: frame.payload ?? {} });
            ws.sendText(
              JSON.stringify({
                _tag: "Exit",
                requestId: frame.id,
                exit: { _tag: "Success", value: { attachmentId, relativeUrl: `/api/attachments/upload/vmtok-${n}`, expiresAt: Date.now() + 600_000 } },
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
    /** Publishes a thread row change on the Hub's shell stream, like T3 when a turn starts or ends. */
    const pushThreadRow = (threadId: string, patch: Record<string, unknown>) => {
      if (!shellStream) throw new Error("no open orchestration.subscribeShell stream on fake VM");
      const row = { ...threads.get(threadId)!, ...patch };
      threads.set(threadId, row);
      shellStream.ws.sendText(
        JSON.stringify({
          _tag: "Chunk",
          requestId: shellStream.requestId,
          values: [{ kind: "thread.updated", sequence: 10, location: "active", thread: row }],
        }),
      );
    };
    const pushPreviewEvent = (event: Record<string, unknown>) => {
      for (const stream of previewEventStreams) {
        stream.ws.sendText(JSON.stringify({ _tag: "Chunk", requestId: stream.requestId, values: [event] }));
      }
    };
    return {
      server,
      url: `http://127.0.0.1:${server.port}`,
      seenMethods,
      acks,
      vmProjectId,
      pushConfigItems,
      pushThreadRow,
      /** Sets the sign-in state a new `provider.auth.subscribe` gets first (with `signIn`). */
      setAuthState: (state: Record<string, unknown>) => {
        authState = state;
      },
      getSettings: () => vmSettings,
      uploads,
      threads,
      mediaConnections,
      mediaMessages,
      previewEventStreams,
      pushPreviewEvent,
    };
  }

  /** A thread row as a T3 VM lists it (every key `OrchestrationV2ThreadShell` requires), running. */
  function vmThreadRow(threadId: string, projectId: string, title: string, worktreePath: string | null): Record<string, unknown> {
    const input = placeholderInputFrom(
      "launch",
      { title, modelSelection: { instanceId: "claudeAgent", model: "claude-opus-5-5" }, runtimeMode: "full-access", interactionMode: "default" },
      threadId,
      projectId,
    )!;
    const { lastVisitedAt: _visited, ...row } = buildPlaceholderThread(input).shell;
    return { ...row, worktreePath, status: "running" };
  }

  /** A settled thread's projection as a T3 VM answers `getThreadProjection`: one earlier user message. */
  function vmThreadProjection(threadId: string, projectId: string): Record<string, unknown> {
    const input = placeholderInputFrom(
      "launch",
      {
        title: "Cached chat",
        modelSelection: { instanceId: "claudeAgent", model: "claude-opus-5-5" },
        runtimeMode: "full-access",
        interactionMode: "default",
        initialMessage: { messageId: `vm-msg-${threadId}`, text: "What we said before", attachments: [] },
      },
      threadId,
      projectId,
    )!;
    const { projection } = buildPlaceholderThread(input);
    const settled = <T extends Record<string, unknown>>(rows: T[]) => rows.map((r) => ({ ...r, runId: null }));
    const turnItems = settled(projection.turnItems as Record<string, unknown>[]).filter((i) => i.type === "user_message");
    const visible = (projection.visibleTurnItems as Array<{ item: Record<string, unknown> }>)
      .filter((r) => r.item.type === "user_message")
      .map((r) => ({ ...r, item: { ...r.item, runId: null } }));
    return { ...projection, runs: [], messages: settled(projection.messages as Record<string, unknown>[]), turnItems, visibleTurnItems: visible };
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
    opts: {
      persistProviderSessions?: string[];
      beforeStart?: (dir: string) => void;
      patchConfig?: (cfg: SentinelConfig) => SentinelConfig;
      maxSandboxes?: number;
      klav?: KlavClient;
    } = {},
  ) {
    const dir = mkdtempSync(join(tmpdir(), dirPrefix));
    cleanups.push(() => rmSync(dir, { recursive: true, force: true }));
    const boat = new MockBoat();
    boat.snapshots.add("base-t3");
    boat.snapshots.add("env-my-app");
    boat.upstreamBaseUrl = vm.url;
    const base = makeTestConfig(0);
    const unpatched: SentinelConfig = {
      ...base,
      stateDir: dir,
      t3: { ...base.t3, ...(opts.persistProviderSessions ? { persistProviderSessions: opts.persistProviderSessions } : {}) },
      hub: { host: "boat.boat.example.com", label: "Boat", maxSandboxes: opts.maxSandboxes ?? 5 },
    };
    const cfg = opts.patchConfig ? opts.patchConfig(unpatched) : unpatched;
    opts.beforeStart?.(dir);
    const handle = createSentinelServer(cfg, boat, { startBackgroundLoops: false, ...(opts.klav ? { klav: opts.klav } : {}) });
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

    test("settling a chat whose VM failed to start closes it instead of returning the start error", async () => {
      const vm = startFakeVmT3Server("vm-settle-failed");
      cleanups.push(() => vm.server.stop(true));
      const { cfg, handle, boat } = await startHubWithVm(vm, "t3-sentinel-hub-settle-failed-");
      boat.create = async () => {
        throw new Error("boat new failed: not found (404)");
      };
      const client = await connectHubClient(handle, cfg);
      const hubProjectId = await subscribeHubShell(client);

      client.request("launch", "orchestration.launchThread", clientLaunchPayload("thread-s", hubProjectId));
      expect(exitOf(await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "launch"))._tag).toBe("Success");
      await client.waitForFrame(shellThreadUpdate("thread-s", (t) => t.status === "failed"));

      client.request("settle", "orchestration.dispatchCommand", { type: "thread.settle", commandId: "cmd-settle", threadId: "thread-s" });
      expect(exitOf(await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "settle"))._tag).toBe("Success");
      const removed = await client.waitForFrame(
        (f) =>
          f._tag === "Chunk" &&
          f.requestId === "shell" &&
          (f.values as Array<{ kind?: string; threadId?: string }>).some((v) => v.kind === "thread.removed" && v.threadId === "thread-s"),
      );
      expect(removed).toBeTruthy();
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

    test("chat-scoped RPCs for a booting chat wait for its VM instead of failing as gone; the boot logs its sync phase", async () => {
      const vm = startFakeVmT3Server("vm-scoped");
      cleanups.push(() => vm.server.stop(true));
      const { cfg, handle, boat } = await startHubWithVm(vm, "t3-sentinel-hub-scoped-");
      boat.execHook = (_id, script) =>
        script.includes(REPO_SYNC_MARKER)
          ? Promise.resolve({
              exitCode: 0,
              stdout: JSON.stringify({ status: "synced", reason: "", from: "a".repeat(40), to: "b".repeat(40), branch: "main", discarded: 0 }),
              stderr: "",
            })
          : undefined;
      const release = holdCreate(boat);
      cleanups.push(release);
      const logs: string[] = [];
      const origLog = console.log;
      console.log = (...args: unknown[]) => {
        logs.push(args.map(String).join(" "));
        origLog(...args);
      };
      cleanups.push(() => (console.log = origLog));
      const client = await connectHubClient(handle, cfg);
      const hubProjectId = await subscribeHubShell(client);

      client.request("launch", "orchestration.launchThread", clientLaunchPayload("thread-sc", hubProjectId));
      expect(exitOf(await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "launch"))._tag).toBe("Success");
      client.request("secret", "secrets.answerRequest", { threadId: "thread-sc", turnItemId: "ti-1", answer: { type: "decline" } });
      await Bun.sleep(100);
      // Not the "VM gone" error: it waits for the VM
      expect(client.inbox.some((f) => f._tag === "Exit" && f.requestId === "secret")).toBe(false);

      release();
      expect(exitOf(await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "secret"))._tag).toBe("Success");
      const secretIdx = vm.seenMethods.findIndex((m) => m.tag === "secrets.answerRequest");
      expect(secretIdx).toBeGreaterThan(vm.seenMethods.findIndex((m) => m.tag === "orchestration.launchThread"));
      // Sync-to-remote still runs in the background boot, before T3 starts, and is its own timed phase
      expect(boat.execScripts.some((s) => s.includes(REPO_SYNC_MARKER))).toBe(true);
      const ready = logs.find((l) => l.includes("ready for thread=thread-sc"));
      expect(ready).toMatch(/create=\d+ms upload=\d+ms sync=\d+ms provision=\d+ms .*total=\d+ms/);
    });

    test("a chat-scoped RPC for a sleeping chat wakes its VM; a chat whose start failed gets the gone error", async () => {
      const vm = startFakeVmT3Server("vm-scoped-sleep");
      cleanups.push(() => vm.server.stop(true));
      const { cfg, handle, boat } = await startHubWithVm(vm, "t3-sentinel-hub-scoped-sleep-");
      const client = await connectHubClient(handle, cfg);
      await launchThreadOnHub(client, "thread-ss");
      const sandboxId = handle.state.listHubSandboxes()[0].sandboxId;
      await handle.hub!.sleepHubSandbox(sandboxId);

      client.request("secret", "secrets.answerRequest", { threadId: "thread-ss", turnItemId: "ti-1", answer: { type: "decline" } });
      expect(exitOf(await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "secret"))._tag).toBe("Success");
      expect(boat.resumed).toEqual([sandboxId]);
      expect(vm.seenMethods.some((m) => m.tag === "secrets.answerRequest")).toBe(true);

      boat.createError = new Error("no capacity");
      const hubProjectId = handle.state.getHubSandbox(sandboxId)!.hubProjectId;
      client.request("launch-f", "orchestration.launchThread", clientLaunchPayload("thread-sf", hubProjectId));
      await client.waitForFrame(shellThreadUpdate("thread-sf", (t) => t.status === "failed"));
      client.request("secret-f", "secrets.answerRequest", { threadId: "thread-sf", turnItemId: "ti-2", answer: { type: "decline" } });
      const gone = exitOf(await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "secret-f")) as unknown as {
        _tag: string;
        cause: Array<{ error?: { _tag: string } }>;
      };
      expect(gone._tag).toBe("Failure");
      expect(gone.cause[0].error?._tag).toBe("SecretRequestError");
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

  describe("attachments staged by the Hub (the app's upload flow)", () => {
    const UPLOAD_URL_PATTERN = /^\/api\/attachments\/upload\/(?!\.{1,2}$)[\w.-]+$/;
    type HubClient = Awaited<ReturnType<typeof connectHubClient>>;
    const exitOf = async (client: HubClient, id: string) =>
      (await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === id)) as Record<string, unknown> & {
        exit: { _tag: string; value?: any; cause?: any };
      };
    /** Decodes a frame with the app's schema for `tag` (when the app is installed). */
    const expectDecodes = (tag: string, frame: Record<string, unknown>) => {
      if (t3Contracts) expect(t3Contracts.decodeFrame(tag, frame)).toEqual([{ ok: true }]);
    };

    /**
     * One composer file, as the app sends it (attachmentUploadQueue + utils chunks): mint an upload URL,
     * XHR `POST` the bytes with `Content-Type: <mimeType>` (any 2xx is success), then check the upload
     * with `assets.createUrl({ _tag: "attachment" })` before sending the message.
     */
    async function uploadLikeTheApp(
      client: HubClient,
      cfg: SentinelConfig,
      key: string,
      input: { type?: "image" | "file"; name: string; mimeType: string },
      bytes: Uint8Array<ArrayBuffer>,
    ): Promise<string> {
      const before = Date.now();
      client.request(`mint-${key}`, "attachments.createUploadUrl", { ...input, sizeBytes: bytes.byteLength });
      const mint = await exitOf(client, `mint-${key}`);
      expect(mint.exit._tag).toBe("Success");
      expectDecodes("attachments.createUploadUrl", mint);
      const { attachmentId, relativeUrl, expiresAt } = mint.exit.value as { attachmentId: string; relativeUrl: string; expiresAt: number };
      expect(relativeUrl).toMatch(UPLOAD_URL_PATTERN);
      expect(expiresAt).toBeGreaterThanOrEqual(before + ATTACHMENT_UPLOAD_URL_TTL_MS);
      expect(expiresAt).toBeLessThanOrEqual(Date.now() + ATTACHMENT_UPLOAD_URL_TTL_MS);

      const posted = await fetch(`${client.base}${relativeUrl}`, {
        method: "POST",
        headers: { host: cfg.hub!.host, "content-type": input.mimeType },
        body: bytes,
      });
      expect(posted.status).toBe(204);

      client.request(`verify-${key}`, "assets.createUrl", { resource: { _tag: "attachment", attachmentId } });
      const verify = await exitOf(client, `verify-${key}`);
      expect(verify.exit._tag).toBe("Success");
      expectDecodes("assets.createUrl", verify);
      const asset = await fetch(`${client.base}${(verify.exit.value as { relativeUrl: string }).relativeUrl}`, { headers: { host: cfg.hub!.host } });
      expect(asset.headers.get("content-type")).toBe(input.mimeType);
      expect(new Uint8Array(await asset.arrayBuffer())).toEqual(bytes);
      return attachmentId;
    }

    test("the upload URL result decodes with the app's contract; ids, names and the upload route follow T3", async () => {
      if (t3Contracts) {
        expect(t3Contracts.exported("ATTACHMENT_UPLOAD_URL_TTL_MS")).toBe(ATTACHMENT_UPLOAD_URL_TTL_MS);
        // The answer the Hub gave before this fix: rejected by the app, hence "failed to upload"
        const old = { attachmentId: "att_1", relativeUrl: "/api/attachments/upload/tok", uploadUrl: "https://boat.example.com/api/attachments/upload/tok" };
        expect(t3Contracts.decodeSuccess("attachments.createUploadUrl", old).error).toContain("expiresAt");
      }
      // T3's input rules: type defaults to image; a file that is an SVG is stored with a .svg name
      expect(parseUploadRequest({ name: "image.png", mimeType: "image/png", sizeBytes: 3 })).toMatchObject({ type: "image", name: "image.png" });
      expect(parseUploadRequest({ type: "file", name: "diagram", mimeType: "image/svg+xml", sizeBytes: 3 })?.name).toBe("diagram.svg");
      expect(parseUploadRequest({ type: "file", name: "logo", mimeType: "application/octet-stream", sizeBytes: 3 })?.name).toBe("logo");
      expect(parseUploadRequest({ type: "video", name: "a", mimeType: "x/y", sizeBytes: 3 })).toBeNull();
      expect(parseUploadRequest({ name: "a.png", mimeType: "image/png", sizeBytes: 0 })).toBeNull();

      const vm = startFakeVmT3Server("vm1");
      cleanups.push(() => vm.server.stop(true));
      const { cfg, handle } = await startHubWithVm(vm, "t3-sentinel-hub-att-contract-");
      const client = await connectHubClient(handle, cfg);
      const mint = async (key: string, payload: Record<string, unknown>) => {
        client.request(key, "attachments.createUploadUrl", payload);
        const frame = await exitOf(client, key);
        expectDecodes("attachments.createUploadUrl", frame);
        return frame.exit;
      };
      expect((await mint("img", { name: "image.png", mimeType: "image/png", sizeBytes: 4 })).value.attachmentId).toMatch(
        /^pending-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
      );
      expect((await mint("pdf", { type: "file", name: "report.pdf", mimeType: "application/pdf", sizeBytes: 4 })).value.attachmentId).toEndWith("-pdf");
      expect((await mint("svg", { type: "file", name: "diagram", mimeType: "image/svg+xml", sizeBytes: 4 })).value.attachmentId).toEndWith("-svg");
      expect((await mint("bin", { type: "file", name: "Makefile", mimeType: "text/plain", sizeBytes: 4 })).value.attachmentId).toEndWith("-bin");
      // A payload T3's schema rejects gets a defect, like T3
      const bad = await mint("bad", { type: "file", name: "", mimeType: "text/plain", sizeBytes: 4 });
      expect(bad.cause[0]._tag).toBe("Die");

      // The upload route answers like T3's: 400 for a size that differs, 204 once, then 404
      const url = (await mint("once", { name: "image.png", mimeType: "image/png", sizeBytes: 4 })).value.relativeUrl as string;
      const post = (body: Uint8Array<ArrayBuffer>) =>
        fetch(`${client.base}${url}`, { method: "POST", headers: { host: cfg.hub!.host, "content-type": "image/png" }, body });
      expect((await post(new Uint8Array(3))).status).toBe(400);
      expect((await post(new Uint8Array(4))).status).toBe(204);
      expect((await post(new Uint8Array(4))).status).toBe(404);

      // Removing a staged upload from the composer drops it on the Hub (void success is null on the wire)
      const discard = (await mint("discard", { name: "image.png", mimeType: "image/png", sizeBytes: 4 })).value;
      client.request("del", "attachments.delete", { attachmentId: discard.attachmentId });
      const del = await exitOf(client, "del");
      expect(del.exit).toEqual({ _tag: "Success", value: null });
      expectDecodes("attachments.delete", del);
      const late = await fetch(`${client.base}${discard.relativeUrl}`, { method: "POST", headers: { host: cfg.hub!.host }, body: new Uint8Array(4) });
      expect(late.status).toBe(404);
      // An upload that never finished is not there yet
      client.request("unfinished", "assets.createUrl", { resource: { _tag: "attachment", attachmentId: (await mint("u", { name: "a.png", mimeType: "image/png", sizeBytes: 4 })).value.attachmentId } });
      const unfinished = await exitOf(client, "unfinished");
      expect(unfinished.exit.cause[0].error._tag).toBe("AssetAttachmentNotFoundError");
      expectDecodes("assets.createUrl", unfinished);
    });

    test("an image in a new chat and a file in a later message reach the chat's VM with their type, bytes and the VM's ids", async () => {
      const vm = startFakeVmT3Server("vm1");
      cleanups.push(() => vm.server.stop(true));
      const { cfg, handle } = await startHubWithVm(vm, "t3-sentinel-hub-att-e2e-");
      const client = await connectHubClient(handle, cfg);
      const hubProjectId = await subscribeHubShell(client);

      // 1. An image attached to the first message of a new chat, uploaded before any VM exists
      const png = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 1, 2, 3]);
      const imageId = await uploadLikeTheApp(client, cfg, "img", { name: "image.png", mimeType: "image/png" }, png);
      const image = { type: "image", id: imageId, name: "image.png", mimeType: "image/png", sizeBytes: png.byteLength };
      const launch = clientLaunchPayload("thread-att", hubProjectId, "What is in this image?");
      launch.initialMessage.attachments = [image] as never[];
      if (t3Contracts) expect(t3Contracts.decodePayload("orchestration.launchThread", launch)).toEqual({ ok: true });
      client.request("launch", "orchestration.launchThread", launch);
      expect((await exitOf(client, "launch")).exit._tag).toBe("Success");
      await client.waitForFrame(shellThreadUpdate("thread-att", (t) => t.status === "running"));

      expect(vm.uploads).toHaveLength(1);
      expect(vm.uploads[0]).toMatchObject({ attachmentId: "vm-vm1-att-1", contentType: "image/png", bytes: png });
      expect(vm.uploads[0].payload).toEqual({ name: "image.png", mimeType: "image/png", sizeBytes: png.byteLength });
      const vmLaunch = vm.seenMethods.find((m) => m.tag === "orchestration.launchThread")!.payload as typeof launch;
      expect(vmLaunch.initialMessage.attachments).toEqual([{ ...image, id: "vm-vm1-att-1" }] as never[]);

      // 2. A file attached to a later message in the same chat
      const pdf = new TextEncoder().encode("%PDF-1.7 hello");
      const fileId = await uploadLikeTheApp(client, cfg, "pdf", { type: "file", name: "report.pdf", mimeType: "application/pdf" }, pdf);
      expect(fileId).toEndWith("-pdf");
      const file = { type: "file", id: fileId, name: "report.pdf", mimeType: "application/pdf", sizeBytes: pdf.byteLength };
      const command = {
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        dispatchMode: { type: "start_immediately" },
        commandId: "cmd-file",
        threadId: "thread-att",
        messageId: "msg-file",
        text: "And this report?",
        attachments: [file],
      };
      if (t3Contracts) expect(t3Contracts.decodePayload("orchestration.dispatchCommand", command)).toEqual({ ok: true });
      client.request("send-file", "orchestration.dispatchCommand", command);
      const sent = await exitOf(client, "send-file");
      expect(sent.exit._tag).toBe("Success");
      expectDecodes("orchestration.dispatchCommand", sent);

      expect(vm.uploads).toHaveLength(2);
      // `type: "file"` reaches the VM, so it stores a file (an image-only request would be rejected)
      expect(vm.uploads[1].payload).toEqual({ type: "file", name: "report.pdf", mimeType: "application/pdf", sizeBytes: pdf.byteLength });
      expect(vm.uploads[1]).toMatchObject({ attachmentId: "vm-vm1-att-2", contentType: "application/pdf", bytes: pdf });
      const vmDispatch = vm.seenMethods.filter((m) => m.tag === "orchestration.dispatchCommand").at(-1)!.payload as typeof command;
      expect(vmDispatch.attachments).toEqual([{ ...file, id: "vm-vm1-att-2" }]);

      // 3. A VM that rejects the bytes fails the send with the reason, instead of passing an id the VM lacks
      const again = await uploadLikeTheApp(client, cfg, "again", { type: "file", name: "notes.txt", mimeType: "text/plain" }, new TextEncoder().encode("hi"));
      vm.uploads.length = 0;
      const realFetch = globalThis.fetch;
      globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) =>
        String(input).includes("/api/attachments/upload/vmtok-") ? new Response("nope", { status: 400 }) : realFetch(input, init)) as typeof fetch;
      try {
        client.request("send-bad", "orchestration.dispatchCommand", {
          ...command,
          commandId: "cmd-bad",
          messageId: "msg-bad",
          attachments: [{ type: "file", id: again, name: "notes.txt", mimeType: "text/plain", sizeBytes: 2 }],
        });
        const failed = await exitOf(client, "send-bad");
        expect(failed.exit.cause[0].error).toMatchObject({ _tag: "OrchestrationV2DispatchCommandError", commandId: "cmd-bad" });
        expect(failed.exit.cause[0].error.message).toContain("Attachment 'notes.txt' could not be sent to the chat's VM");
        expectDecodes("orchestration.dispatchCommand", failed);
      } finally {
        globalThis.fetch = realFetch;
      }
    });

    test("the app's delete right after sending to a new chat does not drop an upload its booting VM still needs", async () => {
      const vm = startFakeVmT3Server("vm1");
      cleanups.push(() => vm.server.stop(true));
      const { cfg, handle, boat } = await startHubWithVm(vm, "t3-sentinel-hub-att-pinned-");
      const client = await connectHubClient(handle, cfg);
      const hubProjectId = await subscribeHubShell(client);
      const png = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 9, 9]);
      const imageId = await uploadLikeTheApp(client, cfg, "img", { name: "image.png", mimeType: "image/png" }, png);

      let release!: () => void;
      boat.createGate = new Promise<void>((r) => (release = r));
      cleanups.push(() => release());
      const launch = clientLaunchPayload("thread-pin", hubProjectId, "Look");
      launch.initialMessage.attachments = [{ type: "image", id: imageId, name: "image.png", mimeType: "image/png", sizeBytes: png.byteLength }] as never[];
      client.request("launch", "orchestration.launchThread", launch);
      expect((await exitOf(client, "launch")).exit._tag).toBe("Success");
      // What the app does once the send succeeded (attachmentUploadQueue: release the composer's uploads)
      client.request("del", "attachments.delete", { attachmentId: imageId });
      expect((await exitOf(client, "del")).exit).toEqual({ _tag: "Success", value: null });

      boat.createGate = null;
      release();
      await client.waitForFrame(shellThreadUpdate("thread-pin", (t) => t.status === "running"));
      expect(vm.uploads).toHaveLength(1);
      expect(vm.uploads[0].bytes).toEqual(png);
      const vmLaunch = vm.seenMethods.find((m) => m.tag === "orchestration.launchThread")!.payload as typeof launch;
      expect((vmLaunch.initialMessage.attachments[0] as { id: string }).id).toBe("vm-vm1-att-1");
      // Sent, so it is gone from the Hub now
      client.request("gone", "assets.createUrl", { resource: { _tag: "attachment", attachmentId: imageId } });
      expect((await exitOf(client, "gone")).exit._tag).toBe("Failure");
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

    test("a Hub restart keeps every chat whose VM still exists in the sidebar; the VM's live row wins once it runs", async () => {
      // One fake VM per sandbox, as in production (each lists only its own threads)
      const vms = ["vm-r1", "vm-r2", "vm-r3", "vm-r4"].map((tag) => startFakeVmT3Server(tag));
      for (const vm of vms) cleanups.push(() => vm.server.stop(true));
      const { cfg, handle, boat, dir } = await startHubWithVm(vms[0], "t3-sentinel-hub-sidebar-restart-");
      boat.hostPublic = async (id: string) => vms[Number(id.slice("sb-".length)) - 1].url;
      const client: HubClient = await connectHubClient(handle, cfg);
      const hubProjectId = await subscribeHubShell(client);
      const chats = ["thread-run", "thread-sleep", "thread-cached", "thread-legacy"];
      for (const threadId of chats) {
        client.request(`launch-${threadId}`, "orchestration.launchThread", clientLaunchPayload(threadId, hubProjectId, "Hi", `Chat ${threadId}`));
        await client.waitForFrame(shellThreadUpdate(threadId, (t) => t.status === "running"));
      }
      const sandboxOf = (threadId: string) => handle.state.listHubSandboxes().find((sb) => sb.threadIds.includes(threadId))!.sandboxId;
      for (const threadId of ["thread-sleep", "thread-cached", "thread-legacy"]) await handle.hub!.sleepHubSandbox(sandboxOf(threadId));
      handle.stop();

      const rows = join(dir, "hub-thread-shells");
      expect(readdirSync(rows).sort()).toEqual(chats.map((t) => `${t}.json`).sort());
      // Two chats whose VMs slept before the Hub kept rows: one with a cached projection, one with nothing
      rmSync(join(rows, "thread-cached.json"));
      rmSync(join(rows, "thread-legacy.json"));
      rmSync(join(dir, "hub-thread-cache", "thread-legacy.json"));
      // A row left behind for a chat whose sandbox is gone
      writeFileSync(join(rows, "thread-ghost.json"), JSON.stringify({ id: "thread-ghost", title: "Ghost" }));

      const handle2 = createSentinelServer(cfg, boat, { startBackgroundLoops: false });
      cleanups.push(() => handle2.stop());
      const client2: HubClient = await connectHubClient(handle2, cfg);
      client2.request("shell", "orchestration.subscribeShell", { requestCompletionMarker: true });
      const shell = await client2.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "shell");
      const listed = (shell.values as Array<{ snapshot?: { threads: Array<Record<string, unknown>> } }>)[0].snapshot!.threads;
      // The whole merged snapshot decodes as the app's shell stream item (a bad row breaks the whole sidebar)
      if (t3Contracts) expect(t3Contracts.decodeFrame("orchestration.subscribeShell", shell)).toEqual([{ ok: true }, { ok: true }]);
      const byId = new Map(listed.map((t) => [String(t.id), t]));
      expect([...byId.keys()].sort()).toEqual([...chats].sort());
      expect(byId.get("thread-sleep")).toMatchObject({ title: "Chat thread-sleep", projectId: hubProjectId, status: "running" });
      expect(byId.get("thread-cached")).toMatchObject({ title: "Cached chat", projectId: hubProjectId });
      expect(byId.get("thread-legacy")).toMatchObject({ title: "Sleeping chat", projectId: hubProjectId, status: "idle" });
      await until(() => !existsSync(join(rows, "thread-ghost.json")));
      await until(() => existsSync(join(rows, "thread-legacy.json")));

      // Opening a rebuilt chat works like before the restart: the Hub's view at once, and its VM wakes,
      // whose live row then replaces the rebuilt one
      client2.request("legacy-view", "orchestration.getThreadProjection", { threadId: "thread-legacy" });
      const view = await client2.waitForFrame((f) => f._tag === "Exit" && f.requestId === "legacy-view");
      expect((view.exit as { _tag: string })._tag).toBe("Success");
      if (t3Contracts) expect(t3Contracts.decodeFrame("orchestration.getThreadProjection", view)).toEqual([{ ok: true }]);
      await client2.waitForFrame(shellThreadUpdate("thread-legacy", (t) => t.title === "Chat thread-legacy"));

      // The VM renamed the chat while the Hub was down: once it runs, its live row replaces the stored one
      const sleepVm = vms[Number(sandboxOf("thread-sleep").slice(3)) - 1];
      sleepVm.threads.set("thread-sleep", { ...sleepVm.threads.get("thread-sleep")!, title: "Renamed on the VM" });
      await handle2.hub!.wakeHubSandbox(sandboxOf("thread-sleep"));
      await client2.waitForFrame(shellThreadUpdate("thread-sleep", (t) => t.title === "Renamed on the VM"));
      await until(() => readFileSync(join(rows, "thread-sleep.json"), "utf8").includes("Renamed on the VM"));

      // A chat its VM no longer lists (archived while the Hub did not listen) leaves the sidebar on wake
      const legacyVm = vms[Number(sandboxOf("thread-legacy").slice(3)) - 1];
      await until(() => handle2.state.getHubSandbox(sandboxOf("thread-legacy"))?.phase === "running");
      await handle2.hub!.sleepHubSandbox(sandboxOf("thread-legacy"));
      legacyVm.threads.delete("thread-legacy");
      await handle2.hub!.wakeHubSandbox(sandboxOf("thread-legacy"));
      await client2.waitForFrame(
        (f) =>
          f._tag === "Chunk" &&
          f.requestId === "shell" &&
          (f.values as Array<{ kind?: string; threadId?: string }>).some((v) => v.kind === "thread.removed" && v.threadId === "thread-legacy"),
      );
      await until(() => !existsSync(join(rows, "thread-legacy.json")));

      // A destroyed sandbox takes its chat's row with it
      await handle2.hub!.destroyHubSandbox(sandboxOf("thread-cached"), "test");
      await until(() => !existsSync(join(rows, "thread-cached.json")));
      handle2.stop();
      expect(readdirSync(rows).sort()).toEqual(["thread-run.json", "thread-sleep.json"]);
    }, 20_000);

    test("an archived chat leaves the active list and its stored row", async () => {
      const { vm, handle, client, dir } = await (async () => {
        const vm = startFakeVmT3Server("vm-archive");
        cleanups.push(() => vm.server.stop(true));
        const hub = await startHubWithVm(vm, "t3-sentinel-hub-archive-row-");
        const client: HubClient = await connectHubClient(hub.handle, hub.cfg);
        await launchThreadOnHub(client, "thread-arch");
        return { vm, client, ...hub };
      })();
      await until(() => existsSync(join(dir, "hub-thread-shells", "thread-arch.json")));
      const bridgeShell = (values: unknown[]) =>
        (handle.hub as unknown as { handleVmShellItems(id: string, items: unknown[]): void }).handleVmShellItems("sb-1", values);
      const row = { ...vm.threads.get("thread-arch")!, archivedAt: new Date().toISOString() };
      bridgeShell([{ kind: "thread.removed", sequence: 5, location: "active", threadId: "thread-arch" }]);
      bridgeShell([{ kind: "thread.updated", sequence: 6, location: "archive", thread: row }]);
      const archived = await client.waitForFrame(
        (f) => f._tag === "Chunk" && f.requestId === "shell" && (f.values as Array<{ location?: string }>).some((v) => v.location === "archive"),
      );
      if (t3Contracts) for (const r of t3Contracts.decodeFrame("orchestration.subscribeShell", archived)) expect(r).toEqual({ ok: true });
      client.request("shell2", "orchestration.subscribeShell");
      const shell = await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "shell2");
      expect((shell.values as Array<{ snapshot: { threads: unknown[] } }>)[0].snapshot.threads).toEqual([]);
      await until(() => !existsSync(join(dir, "hub-thread-shells", "thread-arch.json")));
    });

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

    test("during the backoff after a failed wake, chat requests fail at once with the redacted reason and never resume", async () => {
      const { client, boat, handle, sandboxId } = await sleepingChat("vm-backoff");
      let resumeCalls = 0;
      const origResume = boat.resume.bind(boat);
      boat.resume = async (id: string) => {
        resumeCalls++;
        return origResume(id);
      };
      boat.resumeError = new Error("boat resume failed: KLAV_SANDBOX=ksb1_onetimepackagesecret capacity unavailable");

      client.request("sub", "orchestration.subscribeThread", { threadId: "thread-zz" });
      const failed = await client.waitForFrame(threadChunk("sub", (n) => n?.outcome === "failed"));
      const notice = noticeOf(failed)!;
      expect(notice.detail).toContain("capacity unavailable");
      expect(notice.detail).not.toContain("ksb1_onetimepackagesecret");
      expect(resumeCalls).toBe(1);
      const retryAfter = handle.state.getHubSandbox(sandboxId)!.wakeFailure!.retryAfter;
      expect(Date.parse(retryAfter)).toBeGreaterThan(Date.now());

      // A chat-scoped command and a re-opened chat during the cooldown: a clear failure at once, no resume
      const started = Date.now();
      client.request("msg", "orchestration.dispatchCommand", {
        type: "message.dispatch",
        commandId: "cmd-b",
        threadId: "thread-zz",
        messageId: "m-b",
        text: "still there?",
        attachments: [],
      });
      client.request("term", "terminal.open", { threadId: "thread-zz", terminalId: "t-1" });
      client.request("sub2", "orchestration.subscribeThread", { threadId: "thread-zz" });
      const msg = (await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "msg", 1_000)).exit as {
        _tag: string;
        cause: Array<{ error?: Record<string, unknown> }>;
      };
      expect(msg._tag).toBe("Failure");
      expect(String(msg.cause[0].error!.message)).toContain(`next try after ${retryAfter}`);
      expect(String(msg.cause[0].error!.message)).not.toContain("ksb1_onetimepackagesecret");
      const term = (await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "term", 1_000)).exit as { _tag: string };
      expect(term._tag).toBe("Failure");
      const reopened = noticeOf(await client.waitForFrame(threadChunk("sub2", (n) => n?.outcome === "failed"), 1_000))!;
      expect(reopened.summary).toContain(`next try is allowed after ${retryAfter}`);
      expect(reopened.detail).toContain("capacity unavailable");
      expect(Date.now() - started).toBeLessThan(1_000);
      await Bun.sleep(50);
      expect(resumeCalls).toBe(1);
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

  describe("settled chats park their VM", () => {
    type HubClient = Awaited<ReturnType<typeof connectHubClient>>;
    const until = async (cond: () => boolean, timeoutMs = 5_000) => {
      const start = Date.now();
      while (!cond()) {
        if (Date.now() - start > timeoutMs) throw new Error("condition not met in time");
        await Bun.sleep(20);
      }
    };
    const DAY_MS = 24 * 3600_000;
    const settledConfig = (cfg: SentinelConfig): SentinelConfig => ({ ...cfg, boat: { ...cfg.boat, deleteWhen: "archived-or-settled" } });
    type Exit = { _tag: string; cause?: Array<{ _tag: string; error?: { _tag: string; message?: string }; defect?: unknown }> };
    const exitOf = (f: Record<string, unknown>) => f.exit as Exit;
    /** The text of a failed Exit: a typed error's message, or a defect. */
    const failureText = (f: Record<string, unknown>) => {
      const reason = exitOf(f).cause?.[0];
      return String(reason?.error?.message ?? reason?.defect ?? "");
    };
    type Row = { item: { type: string; summary?: string } };
    const snapshotOf = (f: Record<string, unknown>) =>
      (f.values as Array<{ kind: string; projection?: { messages: Array<{ text: string }>; visibleTurnItems: Row[] } }>).find(
        (v) => v.kind === "snapshot",
      )?.projection;
    const noticeOf = (f: Record<string, unknown>) => snapshotOf(f)?.visibleTurnItems.find((r) => r.item.type === "notification")?.item;
    const shellThreads = async (client: HubClient, reqId: string) => {
      client.request(reqId, "orchestration.subscribeShell");
      const shell = await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === reqId);
      return (shell.values as Array<{ snapshot: { threads: Array<Record<string, unknown>> } }>)[0].snapshot.threads;
    };
    const dispatch = (client: HubClient, reqId: string, threadId: string, type: string, extra: Record<string, unknown> = {}) =>
      client.request(reqId, "orchestration.dispatchCommand", { type, commandId: `cmd-${reqId}`, threadId, ...extra });
    const exitFrame = (client: HubClient, reqId: string, timeoutMs?: number) =>
      client.waitForFrame((f) => f._tag === "Exit" && f.requestId === reqId, timeoutMs);
    const hubProjectIdOf = (handle: SentinelServerHandle) => handle.state.getHub()!.projectIds["my-app"];

    /**
     * One chat on its own VM, settled by the user: the VM's row carries `settledAt` and, after the
     * grace, its watchdog reports `all-archived` (all top-level chats archived, deleted or settled).
     */
    async function parkedChat(prefix: string, opts: { maxSandboxes?: number; vms?: number; settleAction?: "park" | "delete" } = {}) {
      const vms = Array.from({ length: opts.vms ?? 1 }, (_, i) => startFakeVmT3Server(`${prefix}-${i + 1}`));
      for (const vm of vms) cleanups.push(() => vm.server.stop(true));
      const hub = await startHubWithVm(vms[0], `t3-sentinel-hub-${prefix}-`, {
        maxSandboxes: opts.maxSandboxes,
        patchConfig: (cfg) => {
          const settled = settledConfig(cfg);
          return { ...settled, boat: { ...settled.boat, settleAction: opts.settleAction ?? "park" } };
        },
      });
      // One fake VM per sandbox, as in production (each lists only its own threads)
      hub.boat.hostPublic = async (id: string) => vms[Number(id.slice("sb-".length)) - 1].url;
      const client: HubClient = await connectHubClient(hub.handle, hub.cfg);
      await launchThreadOnHub(client, "thread-p");
      const sandboxId = hub.handle.state.listHubSandboxes()[0].sandboxId;
      const vm = vms[0];
      const settledRow = { ...vm.threads.get("thread-p")!, status: "idle", settledAt: new Date().toISOString() };
      vm.threads.set("thread-p", settledRow);
      (hub.handle.hub as unknown as { handleVmShellItems(id: string, items: unknown[]): void }).handleVmShellItems(sandboxId, [
        { kind: "thread.updated", sequence: 5, location: "active", thread: settledRow },
      ]);
      await client.waitForFrame(shellThreadUpdate("thread-p", (t) => typeof t.settledAt === "string"));
      await hub.handle.hub!.handleWatchdogEvent({ slot: `hub:${sandboxId}`, sandboxId, event: "all-archived" });
      return { vms, vm, client, sandboxId, ...hub };
    }

    /** Launches another chat (on the next VM) like the app's first send, and waits for its VM. */
    async function launchAnother(client: HubClient, handle: SentinelServerHandle, threadId: string) {
      client.request(`launch-${threadId}`, "orchestration.launchThread", clientLaunchPayload(threadId, hubProjectIdOf(handle)));
      return exitFrame(client, `launch-${threadId}`);
    }

    test("settling every chat on a VM parks it: stopped, not deleted, still listed as settled", async () => {
      const { boat, handle, client, sandboxId, dir } = await parkedChat("park-settle");
      expect(boat.deleted).toEqual([]);
      expect(boat.stopped).toEqual([sandboxId]);
      const record = handle.state.getHubSandbox(sandboxId)!;
      expect(record.phase).toBe("sleeping");
      expect(typeof record.parkedAt).toBe("string");
      expect(record.llmToken).toBeNull();
      // History cached before the stop, row kept on disk
      expect(existsSync(join(dir, "hub-thread-cache", "thread-p.json"))).toBe(true);
      await until(() => existsSync(join(dir, "hub-thread-shells", "thread-p.json")));
      const listed = await shellThreads(client, "shell2");
      expect(listed.map((t) => t.id)).toEqual(["thread-p"]);
      expect(typeof listed[0].settledAt).toBe("string");
      // The watchdog keeps reporting while it runs; a repeat changes nothing
      await handle.hub!.handleWatchdogEvent({ slot: `hub:${sandboxId}`, sandboxId, event: "all-archived" });
      expect(boat.deleted).toEqual([]);
      expect(boat.stopped).toEqual([sandboxId]);
    });

    test("settleAction delete keeps deleting a settled chat's VM", async () => {
      const { boat, handle, sandboxId } = await parkedChat("park-delete-config", { settleAction: "delete" });
      expect(boat.deleted).toEqual([sandboxId]);
      expect(handle.state.getHubSandbox(sandboxId)).toBeUndefined();
    });

    test("parked VMs do not count toward maxSandboxes; a parked chat cannot wake while the limit is full", async () => {
      const { boat, handle, client, sandboxId } = await parkedChat("park-limit", { maxSandboxes: 1, vms: 2 });
      // The only place is free again: a new chat starts
      const launched = await launchAnother(client, handle, "thread-q");
      expect(exitOf(launched)._tag).toBe("Success");
      await client.waitForFrame(shellThreadUpdate("thread-q", (t) => t.status === "running"));
      expect(handle.state.getHubSandbox(sandboxId)?.parkedAt).toBeTruthy();
      expect(handle.state.listHubSandboxes()).toHaveLength(2);

      // Now the limit is full: continuing the parked chat fails with a clear reason, no wake, no backoff
      dispatch(client, "unsettle-full", "thread-p", "thread.unsettle", { reason: "user" });
      const refused = await exitFrame(client, "unsettle-full");
      expect(exitOf(refused)._tag).toBe("Failure");
      expect(failureText(refused)).toContain("Hub sandbox limit reached (1/1)");
      if (t3Contracts) expect(t3Contracts.decodeFrame("orchestration.dispatchCommand", refused)).toEqual([{ ok: true }]);
      expect(boat.resumed).toEqual([]);
      const record = handle.state.getHubSandbox(sandboxId)!;
      expect(record.parkedAt).toBeTruthy();
      expect(record.wakeFailure ?? null).toBeNull();
    });

    test("opening a parked chat serves its cached history and never wakes its VM", async () => {
      const { boat, handle, client, vm, sandboxId } = await parkedChat("park-open");
      const seenBefore = vm.seenMethods.length;
      const worktree = String(vm.threads.get("thread-p")!.worktreePath);
      // What the app sends when a chat is opened
      client.request("sub", "orchestration.subscribeThread", { threadId: "thread-p", afterSequence: 7, requestCompletionMarker: true });
      client.request("proj", "orchestration.getThreadProjection", { threadId: "thread-p" });
      client.request("family", "orchestration.getThreadFamily", { threadId: "thread-p" });
      dispatch(client, "visit", "thread-p", "thread.visit", { visitedAt: new Date().toISOString() });
      client.request("diff", "orchestration.getFullThreadDiff", { threadId: "thread-p", toTurnCount: 1 });
      client.request("refs", "vcs.listRefs", { cwd: worktree });
      client.request("vcs", "subscribeVcsStatus", { cwd: worktree });
      client.request("previews", "preview.list", { threadId: "thread-p" });

      const sub = await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "sub");
      expect(snapshotOf(sub)!.messages[0].text).toBe("What we said before");
      expect(noticeOf(sub)?.summary).toStartWith("This chat is settled and its VM is parked");
      if (t3Contracts) for (const r of t3Contracts.decodeFrame("orchestration.subscribeThread", sub)) expect(r).toEqual({ ok: true });
      const proj = await exitFrame(client, "proj");
      expect(exitOf(proj)._tag).toBe("Success");
      if (t3Contracts) expect(t3Contracts.decodeFrame("orchestration.getThreadProjection", proj)).toEqual([{ ok: true }]);
      const family = await exitFrame(client, "family");
      expect(((family.exit as { value: { threads: Array<{ id: string }> } }).value.threads)[0].id).toBe("thread-p");
      if (t3Contracts) expect(t3Contracts.decodeFrame("orchestration.getThreadFamily", family)).toEqual([{ ok: true }]);
      expect(exitOf(await exitFrame(client, "visit"))._tag).toBe("Success");
      const diff = await exitFrame(client, "diff");
      expect(exitOf(diff)._tag).toBe("Failure");
      if (t3Contracts) expect(t3Contracts.decodeFrame("orchestration.getFullThreadDiff", diff)).toEqual([{ ok: true }]);
      const refs = await exitFrame(client, "refs");
      expect(exitOf(refs)._tag).toBe("Success");
      if (t3Contracts) expect(t3Contracts.decodeFrame("vcs.listRefs", refs)).toEqual([{ ok: true }]);
      const previews = await exitFrame(client, "previews");
      expect(exitOf(previews)._tag).toBe("Success");

      await Bun.sleep(300);
      expect(boat.resumed).toEqual([]);
      // Nothing reached the VM (besides the project refs its bridge asked for when it first opened)
      const harvest = (m: { tag: string; payload: unknown }) => m.tag === "vcs.listRefs" && (m.payload as { cwd?: unknown }).cwd === "/home/user/my-app";
      expect(vm.seenMethods.slice(seenBefore).filter((m) => !harvest(m)).map((m) => m.tag)).toEqual([]);
      expect(client.inbox.some((f) => f.requestId === "vcs" && f._tag === "Exit")).toBe(false);
      expect(handle.state.getHubSandbox(sandboxId)).toMatchObject({ phase: "sleeping" });
      expect(handle.state.getHubSandbox(sandboxId)?.parkedAt).toBeTruthy();
    });

    test("unsettling a parked chat wakes its VM as it was and forwards the command; it counts again", async () => {
      const { boat, handle, client, vm, sandboxId } = await parkedChat("park-unsettle");
      client.request("sub", "orchestration.subscribeThread", { threadId: "thread-p", requestCompletionMarker: true });
      await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "sub");

      dispatch(client, "unsettle", "thread-p", "thread.unsettle", { reason: "user" });
      const exit = await exitFrame(client, "unsettle", 10_000);
      expect(exitOf(exit)._tag).toBe("Success");
      expect(boat.resumed).toEqual([sandboxId]);
      expect(boat.created).toHaveLength(1);
      const forwarded = vm.seenMethods.find(
        (m) => m.tag === "orchestration.dispatchCommand" && (m.payload as { type?: string }).type === "thread.unsettle",
      );
      expect(forwarded?.payload).toMatchObject({ threadId: "thread-p", reason: "user" });
      const record = handle.state.getHubSandbox(sandboxId)!;
      expect(record.phase).toBe("running");
      expect(record.parkedAt ?? null).toBeNull();
      // The open chat moved to its VM
      await until(() => vm.seenMethods.some((m) => m.tag === "orchestration.subscribeThread"));

      // Its watchdog may still report the pre-park state once: the woken VM is not parked again
      await handle.hub!.handleWatchdogEvent({ slot: `hub:${sandboxId}`, sandboxId, event: "all-archived" });
      expect(handle.state.getHubSandbox(sandboxId)?.phase).toBe("running");
      expect(boat.stopped).toEqual([sandboxId]);
      expect(boat.deleted).toEqual([]);
    });

    for (const command of ["thread.archive", "thread.delete"]) {
      test(`${command} of a parked chat deletes its VM without waking it`, async () => {
        const { boat, handle, client, sandboxId, dir } = await parkedChat(`park-${command.slice(7)}`);
        expect(boat.deleted).toEqual([]);
        dispatch(client, "close", "thread-p", command);
        const exit = await exitFrame(client, "close");
        expect(exitOf(exit)._tag).toBe("Success");
        if (t3Contracts) expect(t3Contracts.decodeFrame("orchestration.dispatchCommand", exit)).toEqual([{ ok: true }]);
        const removed = await client.waitForFrame(
          (f) =>
            f._tag === "Chunk" &&
            f.requestId === "shell" &&
            (f.values as Array<{ kind?: string; threadId?: string }>).some((v) => v.kind === "thread.removed" && v.threadId === "thread-p"),
        );
        if (t3Contracts) for (const r of t3Contracts.decodeFrame("orchestration.subscribeShell", removed)) expect(r).toEqual({ ok: true });
        expect(boat.resumed).toEqual([]);
        expect(boat.deleted).toEqual([sandboxId]);
        expect(handle.state.getHubSandbox(sandboxId)).toBeUndefined();
        await until(() => !existsSync(join(dir, "hub-thread-shells", "thread-p.json")));
        expect(existsSync(join(dir, "hub-thread-cache", "thread-p.json"))).toBe(false);
      });
    }

    test("a Hub restart keeps a parked VM parked: listed, read without waking, not counted", async () => {
      const { boat, handle, cfg, sandboxId } = await parkedChat("park-restart", { maxSandboxes: 1, vms: 2 });
      handle.stop();

      const handle2 = createSentinelServer(cfg, boat, { startBackgroundLoops: false });
      cleanups.push(() => handle2.stop());
      expect(handle2.state.getHubSandbox(sandboxId)).toMatchObject({ phase: "sleeping" });
      expect(handle2.state.getHubSandbox(sandboxId)?.parkedAt).toBeTruthy();
      const client2: HubClient = await connectHubClient(handle2, cfg);
      expect((await shellThreads(client2, "shell")).map((t) => t.id)).toEqual(["thread-p"]);
      client2.request("sub", "orchestration.subscribeThread", { threadId: "thread-p", requestCompletionMarker: true });
      const sub = await client2.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "sub");
      expect(snapshotOf(sub)!.messages[0].text).toBe("What we said before");
      expect(noticeOf(sub)?.summary).toStartWith("This chat is settled");
      const launched = await launchAnother(client2, handle2, "thread-q");
      expect(exitOf(launched)._tag).toBe("Success");
      await client2.waitForFrame(shellThreadUpdate("thread-q", (t) => t.status === "running"));
      expect(boat.resumed).toEqual([]);
    });

    test("after 14 days parked the chat is read only: listed, not counted, and it refuses to continue", async () => {
      const { boat, handle, client, cfg, sandboxId, dir } = await parkedChat("park-expire", { maxSandboxes: 1, vms: 2 });
      // Not yet: 13 days
      handle.state.updateHubSandbox(sandboxId, (s) => {
        s.sleepingSince = new Date(Date.now() - 13 * DAY_MS).toISOString();
      });
      expect(await handle.hub!.expireParkedSandboxes()).toBe(0);
      handle.state.updateHubSandbox(sandboxId, (s) => {
        s.sleepingSince = new Date(Date.now() - 14 * DAY_MS - 60_000).toISOString();
      });
      client.request("sub", "orchestration.subscribeThread", { threadId: "thread-p", requestCompletionMarker: true });
      await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "sub");
      expect(await handle.hub!.expireParkedSandboxes()).toBe(1);
      // The open chat is told at once
      const told = await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "sub" && !!noticeOf(f)?.summary?.includes("expired"));
      expect(noticeOf(told)?.summary).toContain("expired after 14 days settled");
      // The Boat sandbox is deleted; no named snapshot is made
      expect(boat.deleted).toEqual([sandboxId]);
      expect(boat.savedSnapshots).toEqual([]);
      expect(handle.state.getHubSandbox(sandboxId)).toBeUndefined();
      expect(handle.state.getHub()?.expiredThreads?.["thread-p"]).toMatchObject({ sandboxId, reason: "parked-14-days" });

      // Still listed with its history
      expect((await shellThreads(client, "shell2")).map((t) => t.id)).toEqual(["thread-p"]);
      client.request("proj", "orchestration.getThreadProjection", { threadId: "thread-p" });
      const proj = await exitFrame(client, "proj");
      expect((proj.exit as { value: { messages: Array<{ text: string }> } }).value.messages[0].text).toBe("What we said before");
      if (t3Contracts) expect(t3Contracts.decodeFrame("orchestration.getThreadProjection", proj)).toEqual([{ ok: true }]);

      // Continuing it fails with a clear notice, never a wake
      dispatch(client, "unsettle", "thread-p", "thread.unsettle", { reason: "user" });
      const refused = await exitFrame(client, "unsettle");
      expect(failureText(refused)).toContain("expired after 14 days settled");
      if (t3Contracts) expect(t3Contracts.decodeFrame("orchestration.dispatchCommand", refused)).toEqual([{ ok: true }]);
      dispatch(client, "send", "thread-p", "thread.turn.start", { message: { messageId: "m2", text: "Again", attachments: [] } });
      expect(failureText(await exitFrame(client, "send"))).toContain("expired after 14 days settled");
      expect(boat.resumed).toEqual([]);

      // Counts toward nothing: the only place takes a new chat
      const launched = await launchAnother(client, handle, "thread-q");
      expect(exitOf(launched)._tag).toBe("Success");
      await client.waitForFrame(shellThreadUpdate("thread-q", (t) => t.status === "running"));

      // A restart keeps it listed and read only
      handle.stop();
      const handle2 = createSentinelServer(cfg, boat, { startBackgroundLoops: false });
      cleanups.push(() => handle2.stop());
      const client2: HubClient = await connectHubClient(handle2, cfg);
      expect((await shellThreads(client2, "shell")).map((t) => t.id).sort()).toEqual(["thread-p", "thread-q"]);
      await Bun.sleep(400);
      expect(existsSync(join(dir, "hub-thread-shells", "thread-p.json"))).toBe(true);
      dispatch(client2, "unsettle2", "thread-p", "thread.unsettle", { reason: "user" });
      expect(failureText(await exitFrame(client2, "unsettle2"))).toContain("expired after 14 days settled");

      // Archiving it is the only way out of the sidebar
      dispatch(client2, "archive", "thread-p", "thread.archive");
      expect(exitOf(await exitFrame(client2, "archive"))._tag).toBe("Success");
      expect((await shellThreads(client2, "shell3")).map((t) => t.id)).toEqual(["thread-q"]);
      expect(handle2.state.getHub()?.expiredThreads?.["thread-p"]).toBeUndefined();
    });

    test("a parked VM whose disk Boat dropped early turns read only on wake, without retrying", async () => {
      const { boat, handle, client, sandboxId } = await parkedChat("park-dropped");
      boat.sandboxes.delete(sandboxId);
      dispatch(client, "unsettle", "thread-p", "thread.unsettle", { reason: "user" });
      const refused = await exitFrame(client, "unsettle");
      expect(exitOf(refused)._tag).toBe("Failure");
      expect(failureText(refused)).toContain("expired after 14 days settled");
      expect(handle.state.getHubSandbox(sandboxId)).toBeUndefined();
      expect(handle.state.getHub()?.expiredThreads?.["thread-p"]).toMatchObject({ reason: "data-expired" });
      expect((await shellThreads(client, "shell2")).map((t) => t.id)).toEqual(["thread-p"]);
      // A second try is answered by the Hub: no resume, no backoff loop
      dispatch(client, "unsettle2", "thread-p", "thread.unsettle", { reason: "user" });
      expect(failureText(await exitFrame(client, "unsettle2"))).toContain("expired after 14 days settled");
    });

    test("the reaper turns a parked VM Boat no longer lists into a read-only chat", async () => {
      const { boat, handle, client, sandboxId } = await parkedChat("park-reaped");
      boat.sandboxes.delete(sandboxId);
      await handle.lifecycle.reaperTick();
      expect(handle.state.getHubSandbox(sandboxId)).toBeUndefined();
      expect(handle.state.getHub()?.expiredThreads?.["thread-p"]).toMatchObject({ reason: "gone-remotely" });
      expect((await shellThreads(client, "shell2")).map((t) => t.id)).toEqual(["thread-p"]);
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

  describe("provider status and sign-in across chat VMs", () => {
    const agModels = [{ slug: "gemini-3-pro", name: "Gemini 3 Pro", isCustom: false, capabilities: null }];
    const claude = { instanceId: "claudeAgent", driver: "claudeAgent", enabled: true, installed: true, status: "ready", auth: { status: "authenticated" } };
    /** Antigravity on a VM where a session already ran: signed in, with its models. */
    const agConfirmed = {
      instanceId: "antigravity",
      driver: "antigravity",
      enabled: true,
      installed: true,
      status: "ready",
      auth: { status: "authenticated", type: "oauth-personal", label: "Google account" },
      models: agModels,
    };
    /** Antigravity on a fresh VM: T3 has not checked the Google account yet. */
    const agUnchecked = {
      instanceId: "antigravity",
      driver: "antigravity",
      enabled: true,
      installed: true,
      status: "warning",
      auth: { status: "unknown" },
      message: "Antigravity is installed. Google account access is not checked yet.",
      models: [],
    };
    const agSignedOut = { ...agUnchecked, auth: { status: "unauthenticated" }, message: "Sign in with Google to use Antigravity." };
    type AgStub = { auth?: { status?: string }; models?: unknown[] };
    const hubAg = (handle: SentinelServerHandle) =>
      providerById(handle.state.getHub()?.serverConfig?.providers, "antigravity") as (ProviderStub & AgStub) | undefined;
    const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
    async function until(cond: () => boolean, timeoutMs = 5_000) {
      const start = Date.now();
      while (!cond()) {
        if (Date.now() - start > timeoutMs) throw new Error("condition not met in time");
        await sleep(20);
      }
    }
    const authTags = (vm: ReturnType<typeof startFakeVmT3Server>) =>
      vm.seenMethods.filter((m) => m.tag.startsWith("provider.auth.")).map((m) => m.tag);

    /** A Hub with two chats, each on its own fake VM: thread-a on sb-1 (vm-a), thread-b on sb-2 (vm-b). */
    async function twoChatVms(
      dirPrefix: string,
      optsA: Parameters<typeof startFakeVmT3Server>[1] = {},
      optsB: Parameters<typeof startFakeVmT3Server>[1] = {},
    ) {
      const vmA = startFakeVmT3Server("vm-a", optsA);
      const vmB = startFakeVmT3Server("vm-b", optsB);
      cleanups.push(() => vmA.server.stop(true));
      cleanups.push(() => vmB.server.stop(true));
      const { cfg, handle, boat } = await startHubWithVm(vmA, dirPrefix);
      boat.hostPublic = async (id: string) => (id === "sb-2" ? vmB.url : vmA.url);
      const client = await connectHubClient(handle, cfg);
      client.request("cfg", "subscribeServerConfig", {});
      await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "cfg");
      await launchThreadOnHub(client, "thread-a");
      const hubProjectId = handle.state.getHub()!.projectIds["my-app"];
      client.request("launch-b", "orchestration.launchThread", clientLaunchPayload("thread-b", hubProjectId));
      const launchB = await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "launch-b");
      expect((launchB.exit as { _tag: string })._tag).toBe("Success");
      // The Hub answers at once; wait for the VM's own row, as `launchThreadOnHub` does
      await until(() => vmB.threads.has("thread-b") && handle.state.getHubSandbox("sb-2")?.phase === "running");
      await client.waitForFrame(shellThreadUpdate("thread-b", (t) => t.worktreePath !== null && t.status === "running"));
      expect(handle.state.listHubSandboxes().map((s) => s.sandboxId)).toEqual(["sb-1", "sb-2"]);
      return { vmA, vmB, handle, client };
    }

    /** A thread row that runs (or ran) a turn on Antigravity. */
    const onAntigravity = (status: string) => ({
      status,
      providerInstanceId: "antigravity",
      modelSelection: { instanceId: "antigravity", model: "gemini-3-pro" },
    });
    async function setRow(
      client: Awaited<ReturnType<typeof connectHubClient>>,
      vm: ReturnType<typeof startFakeVmT3Server>,
      threadId: string,
      patch: Record<string, unknown>,
    ) {
      vm.pushThreadRow(threadId, patch);
      await client.waitForFrame(shellThreadUpdate(threadId, (t) => t.status === patch.status && t.providerInstanceId === (patch.providerInstanceId ?? t.providerInstanceId)));
    }

    test("a fresh VM's 'not checked yet' report keeps another VM's signed-in status and models; it wins once that VM is gone", async () => {
      const { vmB, handle, client } = await twoChatVms(
        "t3-sentinel-prov-merge-",
        { configSnapshot: { providers: [claude, agConfirmed], settings: {} } },
        { configSnapshot: { providers: [claude, agUnchecked], settings: {} } },
      );
      // vm-b's boot snapshot was harvested (it is the second VM); the Hub still shows vm-a's sign-in
      await until(() => vmB.seenMethods.some((m) => m.tag === "subscribeServerLifecycle"));
      await sleep(100);
      expect(hubAg(handle)?.auth?.status).toBe("authenticated");
      expect(hubAg(handle)?.models).toEqual(agModels);

      // A later live report from vm-b says the same: nothing changes, nothing is broadcast
      const before = client.inbox.length;
      vmB.pushConfigItems([{ version: 1, type: "providerStatuses", payload: { providers: [claude, agUnchecked] } }]);
      await sleep(150);
      expect(hubAg(handle)?.auth?.status).toBe("authenticated");
      expect(hubAg(handle)?.models).toEqual(agModels);
      const sent = client.inbox
        .slice(before)
        .filter((f) => f._tag === "Chunk" && f.requestId === "cfg")
        .flatMap((f) => f.values as Array<{ payload?: { providers?: unknown } }>)
        .map((v) => providerById(v.payload?.providers, "antigravity") as AgStub | undefined);
      expect(sent.every((p) => p === undefined || p.auth?.status === "authenticated")).toBe(true);

      // The confirming VM goes away: vm-b's own report is all that is left, and it wins
      await handle.hub!.destroyHubSandbox("sb-1", "test");
      vmB.pushConfigItems([{ version: 1, type: "providerStatuses", payload: { providers: [claude, agUnchecked] } }]);
      await until(() => hubAg(handle)?.auth?.status === "unknown");
      expect(hubAg(handle)?.models).toEqual([]);
    });

    test("a real sign-out still updates the status, and no other VM's older sign-in vouches for it afterwards", async () => {
      const { vmA, vmB, handle, client } = await twoChatVms(
        "t3-sentinel-prov-signout-",
        { configSnapshot: { providers: [claude, agConfirmed], settings: {} } },
        { configSnapshot: { providers: [claude, agUnchecked], settings: {} } },
      );
      await until(() => vmB.seenMethods.some((m) => m.tag === "subscribeServerLifecycle"));
      await sleep(100);
      // Starts from what only the merge can hold: vm-b unchecked, the Hub still signed in from vm-a
      expect(hubAg(handle)?.auth?.status).toBe("authenticated");

      client.request("logout", "provider.auth.logout", { instanceId: "antigravity" });
      const logout = await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "logout");
      expect((logout.exit as { _tag: string })._tag).toBe("Success");

      // After the sign-out, vm-a's earlier sign-in no longer replaces vm-b's report
      vmB.pushConfigItems([{ version: 1, type: "providerStatuses", payload: { providers: [claude, agUnchecked] } }]);
      await until(() => hubAg(handle)?.auth?.status === "unknown");
      // The signed-out VM reports it: shown as reported, never replaced
      vmA.pushConfigItems([{ version: 1, type: "providerStatuses", payload: { providers: [claude, agSignedOut] } }]);
      await until(() => hubAg(handle)?.auth?.status === "unauthenticated");
      await client.waitForFrame(
        (f) =>
          f._tag === "Chunk" &&
          f.requestId === "cfg" &&
          (f.values as Array<{ payload?: { providers?: unknown } }>).some(
            (v) => (providerById(v.payload?.providers, "antigravity") as AgStub | undefined)?.auth?.status === "unauthenticated",
          ),
      );
    });

    test("provider.auth.start avoids a VM whose chat runs a turn on that provider, and the rest of the flow sticks to the chosen VM", async () => {
      const { vmA, vmB, client } = await twoChatVms("t3-sentinel-prov-route-");
      // thread-a (vm-a, the oldest VM) runs an Antigravity turn; thread-b (vm-b) is idle
      await setRow(client, vmA, "thread-a", onAntigravity("running"));
      await setRow(client, vmB, "thread-b", { status: "idle" });

      client.request("auth", "provider.auth.subscribe", { instanceId: "antigravity" });
      await client.waitForFrame(
        (f) => f._tag === "Chunk" && f.requestId === "auth" && (f.values as Array<{ fromVm?: string }>)[0]?.fromVm === "vm-b",
      );
      client.request("start", "provider.auth.start", { instanceId: "antigravity" });
      const start = await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "start");
      expect((start.exit as { value?: { vmTag?: string } }).value?.vmTag).toBe("vm-b");

      // Loads flip: vm-a is now idle (a fresh pick would prefer it), vm-b runs a turn on another provider
      await setRow(client, vmA, "thread-a", { status: "idle" });
      await setRow(client, vmB, "thread-b", { status: "running" });
      client.request("respond", "provider.auth.respond", {
        instanceId: "antigravity",
        flowId: "flow-1",
        interactionId: "i-1",
        response: { type: "text", text: "1" },
      });
      client.request("complete", "provider.auth.complete", { instanceId: "antigravity", flowId: "flow-1", callbackUrl: "http://localhost/cb" });
      client.request("cancel", "provider.auth.cancel", { instanceId: "antigravity", flowId: "flow-1" });
      for (const id of ["respond", "complete", "cancel"]) {
        const exit = await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === id);
        expect((exit.exit as { value?: { vmTag?: string } }).value?.vmTag).toBe("vm-b");
      }
      expect(authTags(vmA)).toEqual([]);
      expect(authTags(vmB)).toEqual([
        "provider.auth.subscribe",
        "provider.auth.start",
        "provider.auth.respond",
        "provider.auth.complete",
        "provider.auth.cancel",
      ]);

      // A new start while vm-b runs a turn on Antigravity moves the flow to vm-a, and the panel's stream follows it
      await setRow(client, vmB, "thread-b", onAntigravity("running"));
      client.request("start-2", "provider.auth.start", { instanceId: "antigravity" });
      const start2 = await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "start-2");
      expect((start2.exit as { value?: { vmTag?: string } }).value?.vmTag).toBe("vm-a");
      await client.waitForFrame(
        (f) => f._tag === "Chunk" && f.requestId === "auth" && (f.values as Array<{ fromVm?: string }>)[0]?.fromVm === "vm-a",
      );
      expect(authTags(vmA)).toEqual(["provider.auth.subscribe", "provider.auth.start"]);
    });

    test("with every VM running a turn on the provider, sign-in fails with a typed ProviderSetupError and reaches no VM", async () => {
      const { vmA, vmB, client } = await twoChatVms("t3-sentinel-prov-busy-");
      await setRow(client, vmA, "thread-a", onAntigravity("running"));
      await setRow(client, vmB, "thread-b", onAntigravity("waiting"));

      // Reading setup state never interrupts anything: the panel's stream still opens
      client.request("auth", "provider.auth.subscribe", { instanceId: "antigravity" });
      await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "auth");

      client.request("start", "provider.auth.start", { instanceId: "antigravity" });
      client.request("install", "provider.install.start", { instanceId: "antigravity" });
      for (const [id, operation] of [
        ["start", "auth.start"],
        ["install", "install.start"],
      ] as const) {
        const frame = await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === id);
        const cause = (frame.exit as { _tag: string; cause: Array<{ _tag: string; error: Record<string, unknown> }> }).cause;
        expect((frame.exit as { _tag: string })._tag).toBe("Failure");
        expect(cause).toHaveLength(1);
        expect(cause[0]._tag).toBe("Fail");
        expect(cause[0].error).toMatchObject({ _tag: "ProviderSetupError", instanceId: "antigravity", operation });
        expect(String(cause[0].error.detail)).toContain("would interrupt a running chat");
        expect(String(cause[0].error.detail)).toContain("from a new chat");
        if (t3Contracts) expect(t3Contracts.decodeExit(`provider.${operation}`, frame.exit)).toEqual({ ok: true });
      }
      const commands = [...vmA.seenMethods, ...vmB.seenMethods].filter((m) => m.tag === "provider.auth.start" || m.tag === "provider.install.start");
      expect(commands).toEqual([]);
    });
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
      if (t3Contracts) expect(t3Contracts.decodeExit("server.updateSettings", dup)).toEqual({ ok: true });
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
      boat.vmHome = fakeVmHome();

      const client = await connectHubClient(handle, cfg);
      await launchThreadOnHub(client, "thread-ag-inject");

      // Placed by the bundle into the VM's staging dir, 0600, before provision-base.sh installs it
      const landed = boat.vmPath(AG_STAGED);
      expect(readFileSync(landed, "utf8")).toBe(stored);
      expect(statSync(landed).mode & 0o777).toBe(0o600);
      const placeAt = boat.execScripts.findIndex((sc) => sc.includes("t3s_place()"));
      expect(placeAt).toBeGreaterThanOrEqual(0);
      expect(placeAt).toBeLessThan(boat.execScripts.findIndex((sc) => sc.includes(RUN_PROVISION)));
      expect(boat.uploads).toHaveLength(1);
      await waitFor(() => vm.seenMethods.some((m) => m.tag === "server.refreshProviders"));
      // `refreshModels` opens a session, the only check that confirms the injected sign-in
      expect(vm.seenMethods.find((m) => m.tag === "server.refreshProviders")?.payload).toEqual({
        instanceId: "antigravity",
        refreshModels: true,
      });
      if (t3Contracts) {
        expect(t3Contracts.decodePayload("server.refreshProviders", { instanceId: "antigravity", refreshModels: true })).toEqual({ ok: true });
      }
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
      // The staging dir cannot be created in the VM (a file is in its way): the sign-in cannot be placed
      boat.vmHome = fakeVmHome();
      mkdirSync(join(boat.vmHome, ".t3-sentinel"), { recursive: true, mode: 0o700 });
      writeFileSync(join(boat.vmHome, ".t3-sentinel", "provider-sessions"), "in the way");
      const marked: string[][] = [];
      const sessions = handle.lifecycle.providerSessions;
      const markInjected = sessions.markInjected.bind(sessions);
      sessions.markInjected = (id, ids) => {
        marked.push([...ids]);
        markInjected(id, ids);
      };
      const client = await connectHubClient(handle, cfg);
      await launchThreadOnHub(client, "thread-ag-inject-fail");
      expect(handle.state.listHubSandboxes()[0]?.phase).toBe("running");
      // Nothing staged is left behind, the rest of the bundle landed, and the VM boots signed out
      expect(existsSync(join(boat.vmHome, ".t3-sentinel", "provider-sessions"))).toBe(false);
      expect(existsSync(join(boat.vmHome, ".t3-sentinel", "boot-secrets.json"))).toBe(true);
      expect(marked).toEqual([[]]);
    });

    test("a re-sign-in on a VM that was already signed in stores the new session at once, also after a sign-out", async () => {
      const vm = startFakeVmT3Server("vm-ag-resign", {
        configSnapshot: { providers: agProviders("authenticated"), settings: {} },
        signIn: true,
      });
      cleanups.push(() => vm.server.stop(true));
      const { cfg, handle, boat, dir } = await startHubWithVm(vm, "t3-sentinel-ag-resign-", {
        persistProviderSessions: ["antigravity"],
      });
      const first = agToken("ya29.FIRST-SECRET", "2026-11-25T10:00:00Z");
      const vmToken = { current: first as string | null };
      const reads = serveVmToken(boat, vmToken);
      const client = await connectHubClient(handle, cfg);
      await launchThreadOnHub(client, "thread-ag-resign");
      await waitFor(() => existsSync(storedPath(dir)) && readFileSync(storedPath(dir), "utf8") === first);
      await handle.lifecycle.providerSessions.settle();
      expect(reads.count).toBe(1);

      /** Signs in through the Settings panel's flow; the VM keeps reporting `authenticated` throughout. */
      const signIn = async (n: number) => {
        client.request(`auth-${n}`, "provider.auth.subscribe", { instanceId: "antigravity" });
        await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === `auth-${n}`);
        client.request(`start-${n}`, "provider.auth.start", { instanceId: "antigravity" });
        const start = await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === `start-${n}`);
        const flowId = (start.exit as { value: { flowId: string } }).value.flowId;
        client.request(`complete-${n}`, "provider.auth.complete", { instanceId: "antigravity", flowId, callbackUrl: "http://localhost/cb" });
        const complete = await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === `complete-${n}`);
        expect((complete.exit as { value: { phase: string } }).value.phase).toBe("succeeded");
        if (t3Contracts) expect(t3Contracts.decodeExit("provider.auth.complete", complete.exit)).toEqual({ ok: true });
        // The succeeded state also arrives on the panel's stream: still one capture per flow
        await client.waitForFrame(
          (f) => f._tag === "Chunk" && f.requestId === `auth-${n}` && (f.values as Array<{ phase?: string }>)[0]?.phase === "succeeded",
        );
      };

      // The same VM, signed in again (another account, a fresh token): stored without waiting for a teardown
      const second = agToken("ya29.SECOND-SECRET", "2026-11-26T10:00:00Z");
      vmToken.current = second;
      await signIn(1);
      await waitFor(() => readFileSync(storedPath(dir), "utf8") === second);
      await handle.lifecycle.providerSessions.settle();
      expect(reads.count).toBe(2);

      // A Hub sign-out forgets the stored copy and stops this VM's old token from coming back...
      client.request("logout", "provider.auth.logout", { instanceId: "antigravity" });
      await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "logout");
      expect(existsSync(storedPath(dir))).toBe(false);
      // A panel visit replays the VM's last state: a `succeeded` flow this Hub never saw (it restarted
      // since). Not a new sign-in: nothing stored, the sign-out holds
      vm.setAuthState({
        instanceId: "antigravity",
        phase: "succeeded",
        flowId: "flow-before-hub-restart",
        authorizationUrl: null,
        expiresAt: null,
        message: null,
      });
      client.request("replay", "provider.auth.subscribe", { instanceId: "antigravity" });
      const replay = await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "replay");
      expect((replay.values as Array<{ phase?: string }>)[0]?.phase).toBe("succeeded");
      await new Promise((r) => setTimeout(r, 150));
      await handle.lifecycle.providerSessions.settle();
      expect(existsSync(storedPath(dir))).toBe(false);
      expect(reads.count).toBe(2);
      // ...but signing in on it again is a new sign-in, and it is stored
      const third = agToken("ya29.THIRD-SECRET", "2026-11-27T10:00:00Z");
      vmToken.current = third;
      await signIn(2);
      await waitFor(() => existsSync(storedPath(dir)) && readFileSync(storedPath(dir), "utf8") === third);
      await handle.lifecycle.providerSessions.settle();
      expect(reads.count).toBe(3);
      expect(statSync(storedPath(dir)).mode & 0o777).toBe(0o600);
      expect(readFileSync(join(dir, "state.json"), "utf8")).not.toContain("SECRET");
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
      boat.vmHome = fakeVmHome();
      boat.uploads.length = 0;
      const execsBefore = boat.execScripts.length;
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
      expect(boat.uploads).toHaveLength(1);
      expect(readFileSync(boat.vmPath(`~/.t3-sentinel/provider-sessions/${CREDS_VM}`), "utf8")).toBe(devinCreds("cog_HUB-SECRET"));
      expect(existsSync(boat.vmPath(`~/.t3-sentinel/provider-sessions/${FLAG_VM}`))).toBe(true);
      const newExecs = boat.execScripts.slice(execsBefore);
      expect(newExecs.findIndex((sc) => sc.includes("t3s_place()"))).toBeLessThan(newExecs.findIndex((sc) => sc.includes(RUN_PROVISION)));

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

  describe("a new chat VM gets its files in one upload while Klav sets up in parallel", () => {
    const CANARY = {
      signIn: "ya29.CANARY-SIGNIN-7f3a",
      injected: "ghp_CANARY-INJECT-91bd",
      env: "API_KEY=CANARY-ENV-c04e",
      settings: '{"canary":"CANARY-T3SETTINGS-5e21"}',
    };

    /** A Klav that takes `delayMs` to set up, with the times it ran. */
    class SlowKlav implements KlavClient {
      started = 0;
      finished = 0;
      revoked: string[] = [];
      constructor(private readonly delayMs: number) {}
      async provisionForSandbox(_b: unknown, sandboxId: string) {
        this.started = Date.now();
        await Bun.sleep(this.delayMs);
        this.finished = Date.now();
        return { agentName: `ksb-${sandboxId}`, project: "dev" };
      }
      async revokeForSandbox(handle: string) {
        this.revoked.push(handle);
      }
    }

    /** A Hub whose project uses every kind of provisioning file, each carrying a recognizable fake secret. */
    async function startFullHub(prefix: string, opts: { klav?: KlavClient } = {}) {
      const vm = startFakeVmT3Server(prefix);
      cleanups.push(() => vm.server.stop(true));
      const vmHome = fakeVmHome();
      const local = mkdtempSync(join(tmpdir(), "t3-sentinel-bundle-src-"));
      tempDirs.push(local);
      const ghHosts = join(local, "hosts.yml");
      writeFileSync(ghHosts, CANARY.injected, { mode: 0o600 });
      const envFile = join(local, "app.env");
      writeFileSync(envFile, CANARY.env, { mode: 0o600 });
      const settingsFile = join(local, "t3-settings.json");
      writeFileSync(settingsFile, CANARY.settings, { mode: 0o600 });
      const envTarget = join(vmHome, "my-app", ".env");
      const started = await startHubWithVm(vm, `t3-sentinel-${prefix}-`, {
        persistProviderSessions: ["antigravity"],
        ...(opts.klav ? { klav: opts.klav } : {}),
        beforeStart: (d) => {
          mkdirSync(join(d, "credentials", "antigravity"), { recursive: true, mode: 0o700 });
          writeFileSync(join(d, "credentials", "antigravity", "antigravity-oauth-token"), CANARY.signIn, { mode: 0o600 });
        },
        patchConfig: (c) => ({
          ...c,
          llmProxy: { host: "boat-llm.boat.example.com", upstreamUrl: "http://127.0.0.1:1", upstreamApiKey: "k" },
          t3: { ...c.t3, settingsFile },
          boat: { ...c.boat, injectFiles: [{ from: ghHosts, to: "~/.config/gh/hosts.yml", mode: "640" }] },
          ...(opts.klav
            ? { klav: { mode: "proxy" as const, proxyPort: 18790, defaultTtl: "8h", defaultAllow: [], defaultDeny: [], defaultExpires: "8h" } }
            : {}),
          projects: {
            ...c.projects,
            "my-app": { ...c.projects["my-app"]!, envFile, envTarget, ...(opts.klav ? { klav: { profiles: ["dev"] } } : {}) },
          },
        }),
      });
      started.boat.vmHome = vmHome;
      return { ...started, vm, vmHome, envTarget };
    }

    /** Every file under `dir`, relative. */
    const filesUnder = (dir: string): string[] =>
      readdirSync(dir, { recursive: true, withFileTypes: true })
        .filter((e) => e.isFile())
        .map((e) => join(e.parentPath, e.name).slice(dir.length + 1))
        .sort();

    test("one upload carries every file; no exec script ever holds a secret", async () => {
      const { cfg, handle, boat } = await startFullHub("bundle-one");
      const client = await connectHubClient(handle, cfg);
      await launchThreadOnHub(client, "thread-bundle-one");

      // Before: a separate scp per file (scripts, sign-in, boot secrets, Claude settings, T3 settings, injected file, .env)
      expect(boat.uploads).toHaveLength(1);
      const record = handle.state.listHubSandboxes()[0]!;
      const secrets = [CANARY.signIn, CANARY.injected, CANARY.env, CANARY.settings, record.watchdogSecret, record.llmToken!];
      const archive = Buffer.from(boat.uploads[0]!.bytes).toString("latin1");
      for (const secret of secrets) {
        // The secret did travel, as a file transfer…
        expect(archive).toContain(secret);
        // …and never inside a command
        for (const script of boat.execScripts) expect(script).not.toContain(secret);
      }
    });

    test("every file lands at its path with its mode; the .env after the repo sync; no staging is left", async () => {
      const { cfg, handle, boat, vmHome, envTarget } = await startFullHub("bundle-paths");
      boat.execHook = (_id, script) =>
        script.includes(REPO_SYNC_MARKER)
          ? Promise.resolve({
              exitCode: 0,
              stdout: JSON.stringify({ status: "synced", reason: "", from: "a".repeat(40), to: "b".repeat(40), branch: "main", discarded: 0 }),
              stderr: "",
            })
          : undefined;
      // A file the snapshot already holds keeps its mode on overwrite, like an scp; an injected file's mode is forced
      mkdirSync(join(vmHome, ".config", "gh"), { recursive: true });
      writeFileSync(join(vmHome, ".config", "gh", "hosts.yml"), "old", { mode: 0o644 });
      const client = await connectHubClient(handle, cfg);
      await launchThreadOnHub(client, "thread-bundle-paths");

      const mode = (p: string) => statSync(p).mode & 0o777;
      const at = (p: string) => join(vmHome, p);
      const record = handle.state.listHubSandboxes()[0]!;
      const scriptMode = (name: string) => statSync(join(import.meta.dir, "..", "scripts", name)).mode & 0o777;

      expect(readFileSync(at(".t3-sentinel/provision-base.sh"), "utf8")).toBe(
        readFileSync(join(import.meta.dir, "..", "scripts", "provision-base.sh"), "utf8"),
      );
      expect(mode(at(".t3-sentinel/provision-base.sh"))).toBe(scriptMode("provision-base.sh"));
      expect(mode(at(".t3-sentinel/watchdog.mjs"))).toBe(scriptMode("watchdog.mjs"));
      expect(mode(at(".t3-sentinel"))).toBe(0o700);

      const bootSecrets = JSON.parse(readFileSync(at(".t3-sentinel/boot-secrets.json"), "utf8"));
      expect(bootSecrets).toMatchObject({ sandboxId: "sb-1", watchdogSecret: record.watchdogSecret });
      expect(mode(at(".t3-sentinel/boot-secrets.json"))).toBe(0o600);

      const claude = JSON.parse(readFileSync(at(".claude/settings.json"), "utf8"));
      expect(claude.env.ANTHROPIC_AUTH_TOKEN).toBe(record.llmToken);
      expect(mode(at(".claude/settings.json"))).toBe(0o600);

      expect(readFileSync(at(".t3-sentinel/t3-settings.json"), "utf8")).toBe(CANARY.settings);
      expect(mode(at(".t3-sentinel/t3-settings.json"))).toBe(0o600);

      expect(readFileSync(at(".config/gh/hosts.yml"), "utf8")).toBe(CANARY.injected);
      expect(mode(at(".config/gh/hosts.yml"))).toBe(0o640);

      const staged = filesUnder(at(".t3-sentinel/provider-sessions"));
      expect(staged).toHaveLength(1);
      expect(staged[0]).toEndWith("antigravity-oauth-token");
      expect(readFileSync(at(`.t3-sentinel/provider-sessions/${staged[0]}`), "utf8")).toBe(CANARY.signIn);
      expect(mode(at(`.t3-sentinel/provider-sessions/${staged[0]}`))).toBe(0o600);

      expect(readFileSync(envTarget, "utf8")).toBe(CANARY.env);
      expect(mode(envTarget)).toBe(0o600);
      // The .env is placed by its own exec after the sync (which may clean the tree), before bootCmd and T3
      const syncAt = boat.execScripts.findIndex((sc) => sc.includes(REPO_SYNC_MARKER));
      const envAt = boat.execScripts.findIndex((sc, i) => i > syncAt && sc.includes("t3s_place()"));
      expect(syncAt).toBeGreaterThan(boat.execScripts.findIndex((sc) => sc.includes("t3s_place()")));
      expect(envAt).toBeGreaterThan(syncAt);
      expect(envAt).toBeLessThan(boat.execScripts.indexOf("git fetch origin"));
      expect(envAt).toBeLessThan(boat.execScripts.findIndex((sc) => sc.includes(RUN_PROVISION)));

      // Only what provision-base.sh expects stays in the staging dir: no archive, extraction dir or deferred copy
      expect(readdirSync(at(".t3-sentinel")).sort()).toEqual(
        ["boot-secrets.json", "provider-sessions", "provision-base.sh", "t3-settings.json", "watchdog.mjs"].sort(),
      );
    });

    test("Klav sets up while the files upload: the boot takes less than both in a row, and T3 starts after Klav", async () => {
      const DELAY = 400;
      const klav = new SlowKlav(DELAY);
      const { cfg, handle, boat } = await startFullHub("bundle-klav", { klav });
      boat.uploadDelayMs = DELAY;
      let uploadStarted = 0;
      let uploadFinished = 0;
      let provisionAt = 0;
      const upload = boat.upload.bind(boat);
      boat.upload = async (id: string, l: string, r: string) => {
        uploadStarted = Date.now();
        await upload(id, l, r);
        uploadFinished = Date.now();
      };
      boat.execHook = (_id, script) => {
        if (script.includes(RUN_PROVISION)) provisionAt = Date.now();
        return undefined;
      };
      const logs: string[] = [];
      const origLog = console.log;
      console.log = (...args: unknown[]) => {
        logs.push(args.map(String).join(" "));
        origLog(...args);
      };
      cleanups.push(() => (console.log = origLog));
      const client = await connectHubClient(handle, cfg);
      await launchThreadOnHub(client, "thread-bundle-klav");

      // Both ran at the same time…
      expect(uploadStarted).toBeLessThan(klav.finished);
      expect(klav.started).toBeLessThan(uploadFinished);
      // …so getting both done took less than their sum
      expect(Math.max(klav.finished, uploadFinished) - Math.min(klav.started, uploadStarted)).toBeLessThan(2 * DELAY - 100);
      // T3 starts only once Klav's environment is in place
      expect(provisionAt).toBeGreaterThanOrEqual(klav.finished);
      expect(handle.state.listHubSandboxes()[0]?.klavAgentName).toBe("ksb-sb-1");
      const ready = logs.find((l) => l.includes("ready for thread=thread-bundle-klav"))!;
      expect(ready).toMatch(/ upload=\d+ms .*klav=\d+ms\(parallel\) total=\d+ms/);
    });

    test("an upload that fails while Klav is still setting up revokes the Klav agent and queues the VM for deletion", async () => {
      const klav = new SlowKlav(300);
      const { cfg, handle, boat } = await startFullHub("bundle-fail", { klav });
      let uploadFailedAt = 0;
      boat.uploadError = new Error("scp: connection reset");
      const upload = boat.upload.bind(boat);
      boat.upload = async (id: string, l: string, r: string) => {
        try {
          await upload(id, l, r);
        } finally {
          uploadFailedAt = Date.now();
        }
      };
      const client = await connectHubClient(handle, cfg);
      const hubProjectId = await subscribeHubShell(client);
      client.request("launch", "orchestration.launchThread", clientLaunchPayload("thread-bundle-fail", hubProjectId));
      await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "launch");

      const deadline = Date.now() + 5_000;
      while (!(klav.revoked.includes("ksb-sb-1") && boat.deleted.includes("sb-1"))) {
        if (Date.now() > deadline) throw new Error("Klav agent was not revoked / VM not deleted");
        await Bun.sleep(20);
      }
      // The upload failed first, while Klav was still running
      expect(uploadFailedAt).toBeGreaterThan(0);
      expect(uploadFailedAt).toBeLessThan(klav.finished);
      expect(handle.state.listHubSandboxes()).toHaveLength(0);
      expect(boat.execScripts.some((sc) => sc.includes(RUN_PROVISION))).toBe(false);
    });
  });

  test("a new chat VM syncs its repo before .env, bootCmd and T3; a resumed VM is never synced", async () => {
    const vm = startFakeVmT3Server("vm-sync");
    cleanups.push(() => vm.server.stop(true));
    const { cfg, handle, boat } = await startHubWithVm(vm, "t3-sentinel-hub-sync-", {
      // With the LLM proxy, waking a VM re-enters provisioning: the one wake path that could sync
      patchConfig: (c) => ({ ...c, llmProxy: { host: "boat-llm.boat.example.com", upstreamUrl: "http://127.0.0.1:1", upstreamApiKey: "k" } }),
    });
    boat.execHook = (_id, script) =>
      script.includes(REPO_SYNC_MARKER)
        ? Promise.resolve({
            exitCode: 0,
            stdout: JSON.stringify({ status: "synced", reason: "", from: "a".repeat(40), to: "b".repeat(40), branch: "main", discarded: 2 }),
            stderr: "",
          })
        : undefined;
    const client = await connectHubClient(handle, cfg);
    client.request("setup", "subscribeWorktreeSetup", { threadId: "thread-sync" });
    await launchThreadOnHub(client, "thread-sync");

    const syncAt = boat.execScripts.findIndex((s) => s.includes(REPO_SYNC_MARKER));
    expect(syncAt).toBeGreaterThanOrEqual(0);
    expect(boat.execScripts.filter((s) => s.includes(REPO_SYNC_MARKER)).length).toBe(1);
    expect(syncAt).toBeLessThan(boat.execScripts.indexOf("git fetch origin")); // the project's bootCmd
    expect(syncAt).toBeLessThan(boat.execScripts.findIndex((s) => s.includes(RUN_PROVISION)));
    expect(boat.execScripts[syncAt]).toContain("'/home/user/my-app'");
    // The chat's setup panel says which commit it starts on
    const done = await client.waitForFrame(
      (f) => f._tag === "Chunk" && f.requestId === "setup" && JSON.stringify(f.values).includes('"phase":"done"'),
    );
    expect(JSON.stringify(done.values)).toContain("ready on origin/main (bbbbbbb)");

    await handle.hub!.sleepHubSandbox("sb-1");
    expect(handle.state.getHubSandbox("sb-1")?.phase).toBe("sleeping");
    const provisionsBefore = boat.execScripts.filter((s) => s.includes(RUN_PROVISION)).length;
    await handle.hub!.wakeHubSandbox("sb-1");
    expect(boat.execScripts.filter((s) => s.includes(RUN_PROVISION)).length).toBe(provisionsBefore + 1);
    expect(boat.execScripts.filter((s) => s.includes(REPO_SYNC_MARKER)).length).toBe(1);
  });

  test("a sync that hangs past its timeout does not hold the chat: the VM boots on the snapshot's code", async () => {
    const vm = startFakeVmT3Server("vm-sync-hang");
    cleanups.push(() => vm.server.stop(true));
    const { cfg, handle, boat } = await startHubWithVm(vm, "t3-sentinel-hub-sync-hang-");
    handle.lifecycle.repoSyncTimeoutMs = 300;
    boat.execHook = (_id, script) => (script.includes(REPO_SYNC_MARKER) ? new Promise<BoatExecResult>(() => {}) : undefined);
    const client = await connectHubClient(handle, cfg);
    client.request("setup", "subscribeWorktreeSetup", { threadId: "thread-hang" });
    const started = Date.now();
    await launchThreadOnHub(client, "thread-hang");
    expect(Date.now() - started).toBeLessThan(4_000);
    expect(handle.state.getHubSandbox("sb-1")?.phase).toBe("running");
    expect(boat.execScripts.some((s) => s.includes(RUN_PROVISION))).toBe(true);
    const done = await client.waitForFrame(
      (f) => f._tag === "Chunk" && f.requestId === "setup" && JSON.stringify(f.values).includes('"phase":"done"'),
    );
    expect(JSON.stringify(done.values)).toContain("could not be updated from origin (timeout)");
  });

  test("no sync when the project opts out, or when the VM clones the repo itself (no project snapshot)", async () => {
    const vm = startFakeVmT3Server("vm-sync-off");
    cleanups.push(() => vm.server.stop(true));
    const off = await startHubWithVm(vm, "t3-sentinel-hub-sync-off-", {
      patchConfig: (c) => ({ ...c, projects: { "my-app": { ...c.projects["my-app"], syncToRemoteOnStart: false } } }),
    });
    await off.handle.hub!.spawnSandboxForThread("my-app", "thread-off");
    expect(off.boat.execScripts.some((s) => s.includes(REPO_SYNC_MARKER))).toBe(false);

    const clone = await startHubWithVm(vm, "t3-sentinel-hub-sync-clone-");
    clone.boat.snapshots.delete("env-my-app"); // falls back to base-t3 + bakeCmd at boot
    await clone.handle.hub!.spawnSandboxForThread("my-app", "thread-clone");
    expect(clone.boat.created[0].from).toBe("base-t3");
    expect(clone.boat.execScripts.some((s) => s.includes(REPO_SYNC_MARKER))).toBe(false);
  });

  test("chat-scoped RPCs with no explicit route reach only their chat's VM; a gone VM gets a typed error, never another VM", async () => {
    const vm1 = startFakeVmT3Server("vm1");
    const vm2 = startFakeVmT3Server("vm2", { attachments: ["att-on-vm2"] });
    cleanups.push(() => vm1.server.stop(true));
    cleanups.push(() => vm2.server.stop(true));
    const { cfg, handle, boat } = await startHubWithVm(vm1, "t3-sentinel-hub-chat-routing-");
    boat.hostPublic = async (id: string) => (id === "sb-2" ? vm2.url : vm1.url);
    const client = await connectHubClient(handle, cfg);
    // Launched in order: thread-A -> sb-1 (vm1, also the VM the generic fallback picks), thread-B -> sb-2 (vm2)
    await launchThreadOnHub(client, "thread-A");
    const hubProjectId = handle.state.getHubSandbox("sb-1")!.hubProjectId;
    client.request("launch-thread-B", "orchestration.launchThread", { threadId: "thread-B", projectId: hubProjectId, title: "B", workspaceStrategy: { type: "root" } });
    expect(((await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "launch-thread-B")).exit as Record<string, unknown>)._tag).toBe("Success");
    expect(handle.state.getHubSandbox("sb-1")?.threadIds).toEqual(["thread-A"]);
    expect(handle.state.getHubSandbox("sb-2")?.threadIds).toEqual(["thread-B"]);
    const seen = (vm: ReturnType<typeof startFakeVmT3Server>, tag: string) =>
      vm.seenMethods.filter((m) => m.tag === tag).map((m) => extractThreadIdForTest(m.payload));
    const exitOf = async (id: string) => (await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === id)).exit as Record<string, unknown>;
    const hubBase = `http://127.0.0.1:${handle.server.port}`;

    // 1. assets.createUrl nests the chat in `resource.threadId`: each chat's image is signed by its own VM
    client.request("asset-b", "assets.createUrl", { resource: { _tag: "media-file", threadId: "thread-B", path: "/home/user/.t3/userdata/providers/antigravity/h/image.png" } });
    client.request("asset-a", "assets.createUrl", { resource: { _tag: "tool-output-image", threadId: "thread-A", itemId: "item-1", index: 0 } });
    const assetB = await exitOf("asset-b");
    const assetA = await exitOf("asset-a");
    expect((assetB.value as { relativeUrl: string }).relativeUrl).toStartWith("/api/assets/tok-vm2-");
    expect((assetA.value as { relativeUrl: string }).relativeUrl).toStartWith("/api/assets/tok-vm1-");
    expect(seen(vm1, "assets.createUrl")).toEqual(["thread-A"]);
    expect(seen(vm2, "assets.createUrl")).toEqual(["thread-B"]);
    // ...and the token it issued still routes the image GET back to that VM
    const imgB = await fetch(`${hubBase}${(assetB.value as { relativeUrl: string }).relativeUrl}`, { headers: { host: cfg.hub!.host } });
    expect(((await imgB.json()) as { vmTag: string }).vmTag).toBe("vm2");

    // 2. A secret typed into thread-B's card goes to vm2 only, and thread-A's to vm1 only
    client.request("secret-b", "secrets.answerRequest", { threadId: "thread-B", turnItemId: "ti-b", answer: { type: "save", secret: "s3cret-b" } });
    client.request("secret-a", "secrets.answerRequest", { threadId: "thread-A", turnItemId: "ti-a", answer: { type: "decline" } });
    expect((await exitOf("secret-b"))._tag).toBe("Success");
    expect((await exitOf("secret-a"))._tag).toBe("Success");
    expect(seen(vm1, "secrets.answerRequest")).toEqual(["thread-A"]);
    expect(seen(vm2, "secrets.answerRequest")).toEqual(["thread-B"]);
    expect(JSON.stringify(vm1.seenMethods)).not.toContain("s3cret-b");

    // 3. An attachment the Hub can't place: each VM is asked in turn; the one holding it signs the URL
    client.request("att", "assets.createUrl", { resource: { _tag: "attachment", attachmentId: "att-on-vm2", disposition: "inline" } });
    const att = await exitOf("att");
    expect((att.value as { relativeUrl: string }).relativeUrl).toStartWith("/api/assets/tok-vm2-");
    const attGet = await fetch(`${hubBase}${(att.value as { relativeUrl: string }).relativeUrl}`, { headers: { host: cfg.hub!.host } });
    expect(((await attGet.json()) as { vmTag: string }).vmTag).toBe("vm2");
    client.request("att-missing", "assets.createUrl", { resource: { _tag: "attachment", attachmentId: "att-nowhere" } });
    expect((await exitOf("att-missing")).cause).toEqual([
      { _tag: "Fail", error: { _tag: "AssetAttachmentNotFoundError", resource: { _tag: "attachment", attachmentId: "att-nowhere" } } },
    ]);

    // 4. A pull request review thread id is not a chat: still served by any VM
    client.request("pr", "pullRequests.threadComments", { cwd: "/home/user/my-app", number: 1, threadId: "PRRT_kw" });
    expect((await exitOf("pr"))._tag).toBe("Success");

    // 5. thread-B's VM goes away; vm1 keeps running and must see none of thread-B's requests
    await handle.hub!.destroyHubSandbox("sb-2", "test");
    const vm1Before = vm1.seenMethods.length;
    const mediaB = { _tag: "media-file", threadId: "thread-B", path: "/home/user/.t3/userdata/providers/antigravity/h/image.png" };
    client.request("gone-asset", "assets.createUrl", { resource: mediaB });
    client.request("gone-secret", "secrets.answerRequest", { threadId: "thread-B", turnItemId: "ti-b", answer: { type: "save", secret: "s3cret-late" } });
    client.request("gone-peer", "peerLinks.answerRequest", { threadId: "thread-B", turnItemId: "ti-p", answer: { type: "decline" } });
    client.request("gone-preview", "preview.open", { threadId: "thread-B" });
    client.request("gone-list", "preview.list", { threadId: "thread-B" });
    expect((await exitOf("gone-asset")).cause).toEqual([{ _tag: "Fail", error: { _tag: "AssetWorkspaceContextNotFoundError", resource: mediaB } }]);
    expect((await exitOf("gone-secret")).cause).toEqual([{ _tag: "Fail", error: { _tag: "SecretRequestError", reason: "not_found" } }]);
    expect((await exitOf("gone-peer")).cause).toEqual([{ _tag: "Fail", error: { _tag: "PeerLinkRequestError", reason: "not_found" } }]);
    const previewCause = (await exitOf("gone-preview")).cause as Array<{ _tag: string; defect?: string }>;
    expect(previewCause[0]._tag).toBe("Die");
    expect(previewCause[0].defect).toContain("no longer exists");
    expect((await exitOf("gone-list")).value).toEqual({ sessions: [], serverEpoch: "boat-hub", revision: 0 });
    expect(JSON.stringify(vm1.seenMethods.slice(vm1Before))).not.toContain("thread-B");
    expect(JSON.stringify(vm1.seenMethods)).not.toContain("s3cret-late");
  });

  describe("cloning a repo bakes its Boat snapshot (projectClone.*)", () => {
    const until = async (cond: () => boolean, timeoutMs = 5_000) => {
      const start = Date.now();
      while (!cond()) {
        if (Date.now() - start > timeoutMs) throw new Error("timed out");
        await Bun.sleep(10);
      }
    };
    type CloneSnap = {
      projectId: string;
      phase: string;
      stage: string;
      percent: number | null;
      detail: string | null;
      error: string | null;
      sequence: number;
    };
    /** Every list the "clones" stream pushed, oldest first (read in place, never spliced). */
    const cloneLists = (client: Awaited<ReturnType<typeof connectHubClient>>) =>
      client.inbox
        .filter((f) => f._tag === "Chunk" && f.requestId === "clones")
        .map((f) => ((f.values as CloneSnap[][])[0] ?? []) as CloneSnap[]);
    /** This clone's snapshots as T3 saw them, oldest first. */
    const snapsOf = (client: Awaited<ReturnType<typeof connectHubClient>>, projectId: string) =>
      cloneLists(client).flatMap((list) => list.filter((c) => c.projectId === projectId));
    const lastOf = (client: Awaited<ReturnType<typeof connectHubClient>>, projectId: string) => {
      const lists = cloneLists(client);
      return lists.length ? lists[lists.length - 1].find((c) => c.projectId === projectId) ?? null : undefined;
    };
    const startClone = (client: Awaited<ReturnType<typeof connectHubClient>>, id: string, projectId: string, name: string) =>
      client.request(id, "projectClone.start", {
        projectId,
        title: name,
        createdAt: new Date().toISOString(),
        remoteUrl: `https://github.com/acme/${name}.git`,
        destinationPath: `~/${name}`,
      });

    test("reports each bake step with its elapsed time, a rising integer percent and a monotonic sequence", async () => {
      const vm = startFakeVmT3Server("vm-clone-progress");
      cleanups.push(() => vm.server.stop(true));
      const { cfg, handle, boat } = await startHubWithVm(vm, "t3-sentinel-hub-clone-progress-");
      handle.hub!.projectCloneTickMs = 15;
      const client = await connectHubClient(handle, cfg);
      client.request("clones", "subscribeProjectClones", {});
      await until(() => cloneLists(client).length > 0);

      // Hold the provider verification step open so the ticker refreshes it a few times
      let release!: () => void;
      const gate = new Promise<void>((r) => (release = r));
      cleanups.push(() => release());
      boat.onBeforeBakeStop = () => gate;
      const projectId = "33333333-2222-4333-8444-555555555555";
      startClone(client, "start", projectId, "widget");
      const exit = await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "start");
      expect((exit.exit as Record<string, unknown>)._tag).toBe("Success");

      const verifying = () => snapsOf(client, projectId).filter((c) => c.detail?.startsWith("Verifying T3 and providers"));
      await until(() => verifying().length >= 3);
      release();
      await until(() => lastOf(client, projectId)?.phase === "done");

      const snaps = snapsOf(client, projectId);
      // Honest step names, in bake order
      const labels = [
        "Creating a temporary Boat VM",
        "Uploading setup scripts",
        "Cloning and installing dependencies on a temporary Boat VM",
        "Verifying T3 and providers",
        "Stopping the temporary VM",
        "Saving the Boat snapshot",
      ];
      const firstSeen = labels.map((l) => snaps.findIndex((c) => c.detail?.startsWith(`${l} · `)));
      expect(firstSeen.every((i) => i >= 0)).toBe(true);
      expect([...firstSeen].sort((a, b) => a - b)).toEqual(firstSeen);
      expect(snaps[snaps.length - 1]).toMatchObject({ phase: "done", stage: "checkout", percent: 100 });
      // Integer, rising percent; sequence only ever goes up; detail fits T3's 200 chars
      const percents = snaps.map((c) => c.percent);
      expect(percents.every((p) => Number.isInteger(p) && p! >= 0 && p! <= 100)).toBe(true);
      expect([...percents].sort((a, b) => a! - b!)).toEqual(percents);
      const seqs = snaps.map((c) => c.sequence);
      expect(seqs.every((q, i) => i === 0 || q > seqs[i - 1])).toBe(true);
      expect(snaps.every((c) => (c.detail ?? "").length <= 200)).toBe(true);
      expect(verifying().every((c) => c.stage === "resolving" && / · \d+s$/.test(c.detail!))).toBe(true);
      if (t3Contracts) {
        for (const list of cloneLists(client)) {
          expect(t3Contracts.decodeSuccess("subscribeProjectClones", list)).toEqual({ ok: true });
        }
      }
      expect(boat.snapshots.has("env-widget")).toBe(true);
    }, 15_000);

    test("a failed bake shows the step it failed on and stops refreshing", async () => {
      const vm = startFakeVmT3Server("vm-clone-fail");
      cleanups.push(() => vm.server.stop(true));
      const { cfg, handle, boat } = await startHubWithVm(vm, "t3-sentinel-hub-clone-fail-");
      handle.hub!.projectCloneTickMs = 15;
      const client = await connectHubClient(handle, cfg);
      client.request("clones", "subscribeProjectClones", {});
      await until(() => cloneLists(client).length > 0);
      boat.execHook = (_id, script) =>
        script.includes("git clone") ? Promise.resolve({ exitCode: 1, stdout: "", stderr: "repository not found" }) : undefined;
      const projectId = "44444444-2222-4333-8444-555555555555";
      startClone(client, "start", projectId, "missing");
      await until(() => lastOf(client, projectId)?.phase === "failed");
      expect(lastOf(client, projectId)).toMatchObject({ stage: "receiving", percent: null, error: expect.stringContaining("repository not found") });
      const pushed = cloneLists(client).length;
      await Bun.sleep(80);
      expect(cloneLists(client).length).toBe(pushed);
      // The temp VM never outlives the failed bake
      expect(boat.sandboxes.size).toBe(0);
    }, 15_000);
  });

  describe("preview streams reach the chat's own VM", () => {
    type HubClient = Awaited<ReturnType<typeof connectHubClient>>;
    const until = async (cond: () => boolean, timeoutMs = 5_000) => {
      const start = Date.now();
      while (!cond()) {
        if (Date.now() - start > timeoutMs) throw new Error("condition not met in time");
        await Bun.sleep(10);
      }
    };

    async function mediaTicket(client: HubClient, cfg: SentinelConfig): Promise<string> {
      const res = await fetch(`${client.base}/api/auth/websocket-ticket/media`, {
        method: "POST",
        headers: { host: cfg.hub!.host, authorization: `Bearer ${client.bearer}` },
      });
      return ((await res.json()) as { ticket: string }).ticket;
    }

    /** Opens a media socket on the Hub like the app's preview pane, and records what it gets. */
    async function openMedia(handle: SentinelServerHandle, cfg: SentinelConfig, pathAndQuery: string) {
      const ws = new WebSocket(`ws://127.0.0.1:${handle.server.port}${pathAndQuery}`, {
        headers: { host: cfg.hub!.host },
      } as unknown as string[]);
      ws.binaryType = "arraybuffer";
      cleanups.push(() => ws.close());
      const messages: string[] = [];
      const binary: string[] = [];
      let closed: { code: number; reason: string } | null = null;
      ws.addEventListener("message", (ev) => {
        if (typeof ev.data === "string") messages.push(ev.data);
        else binary.push(new TextDecoder().decode(ev.data as ArrayBuffer));
      });
      ws.addEventListener("close", (ev) => (closed = { code: ev.code, reason: ev.reason }));
      await new Promise<void>((resolve) => {
        ws.addEventListener("open", () => resolve());
        ws.addEventListener("close", () => resolve());
      });
      return { ws, messages, binary, closed: () => closed as { code: number; reason: string } | null };
    }

    /** Two chats on two VMs: thread-A on sb-1 (vm1, also the generic fallback), thread-B on sb-2 (vm2). */
    async function twoChats(prefix: string) {
      const vm1 = startFakeVmT3Server(`${prefix}1`);
      const vm2 = startFakeVmT3Server(`${prefix}2`);
      cleanups.push(() => vm1.server.stop(true));
      cleanups.push(() => vm2.server.stop(true));
      const hub = await startHubWithVm(vm1, `t3-sentinel-hub-${prefix}-`);
      hub.boat.hostPublic = async (id: string) => (id === "sb-2" ? vm2.url : vm1.url);
      const client = await connectHubClient(hub.handle, hub.cfg);
      await launchThreadOnHub(client, "thread-A");
      const hubProjectId = hub.handle.state.getHubSandbox("sb-1")!.hubProjectId;
      client.request("launch-thread-B", "orchestration.launchThread", { threadId: "thread-B", projectId: hubProjectId, title: "B", workspaceStrategy: { type: "root" } });
      await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "launch-thread-B");
      expect(hub.handle.state.getHubSandbox("sb-2")?.threadIds).toEqual(["thread-B"]);
      return { vm1, vm2, client, ...hub };
    }

    test("a preview opened in chat B streams from VM B only, both ways, and ends with 4404 once B's VM is gone", async () => {
      const { vm1, vm2, client, cfg, handle } = await twoChats("vm-pv");
      const hub = handle.hub!;
      const exitOf = async (id: string) => (await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === id)).exit as Record<string, unknown>;

      // Preview RPCs name the chat: B's go to vm2 only, even though vm1 is the generic fallback
      client.request("open-b", "preview.open", { threadId: "thread-B", url: "http://localhost:4321/" });
      client.request("status-b", "preview.reportStatus", { threadId: "thread-B", tabId: "tab-1", navStatus: { _tag: "Idle" }, canGoBack: false, canGoForward: false });
      expect((await exitOf("open-b"))._tag).toBe("Success");
      expect((await exitOf("status-b"))._tag).toBe("Success");
      expect(vm2.seenMethods.filter((m) => m.tag.startsWith("preview.")).map((m) => m.tag)).toEqual(["preview.open", "preview.reportStatus"]);
      expect(vm1.seenMethods.some((m) => m.tag.startsWith("preview."))).toBe(false);
      // The localhost URL is the VM's own, so it goes through untouched
      expect((vm2.seenMethods.find((m) => m.tag === "preview.open")!.payload as { url: string }).url).toBe("http://localhost:4321/");

      // Preview events: each VM publishes its own chats', and the app gets all of them on one stream
      client.request("pev", "subscribePreviewEvents", {});
      await until(() => vm1.previewEventStreams.length === 1 && vm2.previewEventStreams.length === 1);
      const opened = { type: "opened", threadId: "thread-B", tabId: "tab-1", createdAt: "now", serverEpoch: "epoch-vm2", revision: 1, snapshot: { threadId: "thread-B", tabId: "tab-1" } };
      vm2.pushPreviewEvent(opened);
      const chunk = await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "pev");
      expect(chunk.values).toEqual([opened]);
      await until(() => vm2.acks.some((id) => id.startsWith("hub-s-")));

      // The pane's media socket: through the Hub to vm2, with vm2's own token and not the Hub's ticket
      const ticket = await mediaTicket(client, cfg);
      const pane = await openMedia(handle, cfg, `/api/preview-stream/ws?threadId=thread-B&tabId=tab-1&maxWidth=800&maxHeight=600&wsTicket=${ticket}`);
      await until(() => pane.binary.length > 0);
      expect(pane.binary).toEqual([`frame-from-vm-pv2`]);
      expect(vm2.mediaConnections).toHaveLength(1);
      expect(vm2.mediaConnections[0]).toEqual({
        path: "/api/preview-stream/ws",
        query: { threadId: "thread-B", tabId: "tab-1", maxWidth: "800", maxHeight: "600" },
        authorization: `Bearer ${handle.state.getHubSandbox("sb-2")!.upstreamToken}`,
      });
      expect(vm1.mediaConnections).toHaveLength(0);
      expect(hub.mediaSocketCount("sb-2")).toBe(1);
      // The Hub acks vm2's frame itself, and vm2's answer comes back; the client's own ack stays at the Hub
      await until(() => pane.messages.length > 0);
      expect(vm2.mediaMessages).toEqual([JSON.stringify({ type: "ack" })]);
      expect(JSON.parse(pane.messages[0])).toEqual({ type: "control", acked: "vm-pv2" });
      pane.ws.send(JSON.stringify({ type: "ack" }));
      pane.ws.send(JSON.stringify({ type: "takeControl" }));
      await until(() => vm2.mediaMessages.length > 1);
      expect(vm2.mediaMessages).toEqual([JSON.stringify({ type: "ack" }), JSON.stringify({ type: "takeControl" })]);

      // A download from that tab goes to vm2 too, without the Hub's ticket
      const dl = await fetch(`${client.base}/api/preview-stream/download?threadId=thread-B&tabId=tab-1&id=d1&wsTicket=${ticket}`, { headers: { host: cfg.hub!.host } });
      expect(await dl.json()).toEqual({ vmTag: "vm-pv2", media: "/api/preview-stream/download" });
      expect(vm2.mediaConnections[1].query).toEqual({ threadId: "thread-B", tabId: "tab-1", id: "d1" });
      // No ticket, no stream
      const noTicket = await fetch(`${client.base}/api/preview-stream/ws?threadId=thread-B&tabId=tab-1`, { headers: { host: cfg.hub!.host, upgrade: "websocket", connection: "Upgrade", "sec-websocket-key": "dGhlIHNhbXBsZSBub25jZQ==", "sec-websocket-version": "13" } });
      expect(noTicket.status).toBe(401);

      // The VM's own close code reaches the app unchanged (4503: the browser needs host setup)
      const setup = await openMedia(handle, cfg, `/api/preview-stream/ws?threadId=thread-B&tabId=tab-2&maxWidth=800&maxHeight=600&wsTicket=${ticket}`);
      await until(() => setup.binary.length > 0);
      setup.ws.send(JSON.stringify({ type: "host-setup" }));
      await until(() => setup.closed() !== null);
      expect(setup.closed()!.code).toBe(4503);
      expect(JSON.parse(setup.closed()!.reason)).toEqual({ need: "libraries", command: "t3 browser setup" });

      // B's VM goes away: its open stream ends with 4404 (tab gone), and so does a new one; vm1 sees nothing
      await hub.destroyHubSandbox("sb-2", "test");
      await until(() => pane.closed() !== null);
      expect(pane.closed()!.code).toBe(4404);
      const late = await openMedia(handle, cfg, `/api/preview-stream/ws?threadId=thread-B&tabId=tab-1&maxWidth=800&maxHeight=600&wsTicket=${ticket}`);
      await until(() => late.closed() !== null);
      expect(late.closed()!.code).toBe(4404);
      const lateDl = await fetch(`${client.base}/api/preview-stream/download?threadId=thread-B&tabId=tab-1&id=d1&wsTicket=${ticket}`, { headers: { host: cfg.hub!.host } });
      expect(lateDl.status).toBe(404);
      expect(vm1.mediaConnections).toHaveLength(0);
      expect(hub.mediaSocketCount()).toBe(0);

      // Chat A still streams from vm1, and its preview events still arrive
      const paneA = await openMedia(handle, cfg, `/api/preview-stream/ws?threadId=thread-A&tabId=tab-9&maxWidth=800&maxHeight=600&wsTicket=${ticket}`);
      await until(() => paneA.binary.length > 0);
      expect(paneA.binary).toEqual(["frame-from-vm-pv1"]);
      vm1.pushPreviewEvent({ ...opened, threadId: "thread-A", serverEpoch: "epoch-vm1" });
      const chunkA = await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "pev");
      expect((chunkA.values as Array<{ threadId: string }>)[0].threadId).toBe("thread-A");

      // Closing the event stream ends it on the VMs
      client.ws.send(JSON.stringify({ _tag: "Interrupt", requestId: "pev" }));
      await exitOf("pev");
      const before = vm1.previewEventStreams.length;
      vm1.pushPreviewEvent({ ...opened, threadId: "thread-A" });
      await Bun.sleep(50);
      expect(client.inbox.some((f) => f._tag === "Chunk" && f.requestId === "pev")).toBe(false);
      expect(vm1.previewEventStreams.length).toBe(before);
    });

    test("a preview of a sleeping chat is told to retry (1013) and does not wake the VM; once awake it streams", async () => {
      const vm = startFakeVmT3Server("vm-pv-sleep");
      cleanups.push(() => vm.server.stop(true));
      const { cfg, handle, boat } = await startHubWithVm(vm, "t3-sentinel-hub-pv-sleep-");
      const client = await connectHubClient(handle, cfg);
      await launchThreadOnHub(client, "thread-zz");
      const sandboxId = handle.state.listHubSandboxes()[0].sandboxId;
      const ticket = await mediaTicket(client, cfg);
      const path = `/api/preview-stream/ws?threadId=thread-zz&tabId=tab-1&maxWidth=800&maxHeight=600&wsTicket=${ticket}`;

      // A stream open when the VM goes to sleep is ended (a paused VM never closes it itself)
      const pane = await openMedia(handle, cfg, path);
      await until(() => pane.binary.length > 0);
      await handle.hub!.sleepHubSandbox(sandboxId);
      await until(() => pane.closed() !== null);
      expect(pane.closed()!.code).toBe(1013);

      // Reconnecting while asleep: retry later, and no wake
      const asleep = await openMedia(handle, cfg, path);
      await until(() => asleep.closed() !== null);
      expect(asleep.closed()!.code).toBe(1013);
      expect(asleep.closed()!.reason).toContain("asleep");
      expect(boat.resumed).toHaveLength(0);
      expect(vm.mediaConnections.filter((c) => c.path === "/api/preview-stream/ws")).toHaveLength(1);

      // The preview RPC wakes the chat, and the next reconnect streams again
      client.request("list", "preview.list", { threadId: "thread-zz" });
      await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "list");
      expect(boat.resumed).toEqual([sandboxId]);
      const awake = await openMedia(handle, cfg, path);
      await until(() => awake.binary.length > 0);
      expect(awake.binary).toEqual(["frame-from-vm-pv-sleep"]);
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

    // T3's "Add to another machine" sends `~/repo`: it lands in the VM user's home, never in a dir named `~`
    ws.send(JSON.stringify({
      _tag: "Request",
      id: 7,
      tag: "projectClone.start",
      payload: {
        projectId: "proj-tilde-1",
        title: "tilde-app",
        createdAt: new Date().toISOString(),
        remoteUrl: "https://github.com/OmarAlex24/tilde-app.git",
        destinationPath: "~/tilde-app/",
      },
      headers: [],
    }));
    const tildeExit = await waitForFrame((f) => f._tag === "Exit" && f.requestId === 7);
    expect((tildeExit.exit as { _tag: string; value: { cwd: string } })._tag).toBe("Success");
    expect((tildeExit.exit as { value: { cwd: string } }).value.cwd).toBe("/home/user/tilde-app");
    const tildeProject = handle.state.getAllProjects(cfg)["tilde-app"];
    expect(tildeProject.workdir).toBe("/home/user/tilde-app");
    expect(tildeProject.bakeCmd).toContain("git clone 'https://github.com/OmarAlex24/tilde-app.git' '/home/user/tilde-app'");
    expect(tildeProject.bootCmd).toContain("'/home/user/tilde-app'");
    expect(`${tildeProject.bakeCmd}\n${tildeProject.bootCmd}`).not.toContain("~");
    await waitForFrame(
      (f) =>
        f._tag === "Chunk" &&
        f.requestId === 2 &&
        ((f.values as Array<Array<{ projectId?: string; phase?: string; destinationPath?: string }>>)[0] ?? []).some(
          (c) => c.projectId === "proj-tilde-1" && c.phase === "done" && c.destinationPath === "/home/user/tilde-app",
        ),
    );
    expect(boat.execScripts.some((s) => s.includes("'/home/user/tilde-app'"))).toBe(true);

    // A relative folder is refused, and nothing is registered
    ws.send(JSON.stringify({
      _tag: "Request",
      id: 8,
      tag: "projectClone.start",
      payload: {
        projectId: "proj-relative-1",
        title: "relative-app",
        createdAt: new Date().toISOString(),
        remoteUrl: "https://github.com/OmarAlex24/relative-app.git",
        destinationPath: "relative-app",
      },
      headers: [],
    }));
    const relExit = await waitForFrame((f) => f._tag === "Exit" && f.requestId === 8);
    expect((relExit.exit as Record<string, unknown>)._tag).toBe("Failure");
    expect(JSON.stringify(relExit.exit)).toContain("absolute path");
    expect(handle.state.getAllProjects(cfg)["relative-app"]).toBeUndefined();
  });

  test("bake progress: integer percent that rises within and across steps, honest step names, short detail", () => {
    const steps: BakeStep[] = ["create", "upload", "install", "verify", "stop", "save"];
    let prev = -1;
    for (const step of steps) {
      for (const ms of [0, 1_000, 10_000, 60_000, 300_000, 3_600_000, 86_400_000]) {
        const p = bakeProgress(step, ms);
        expect(Number.isInteger(p.percent)).toBe(true);
        expect(p.percent).toBeGreaterThanOrEqual(prev);
        expect(p.percent).toBeLessThan(100);
        expect(p.detail.length).toBeLessThanOrEqual(200);
        expect(p.detail.startsWith(`${BAKE_STEPS[step].label} · `)).toBe(true);
        prev = p.percent;
      }
    }
    // A step never claims the next step's share, however long it runs
    expect(bakeProgress("install", 86_400_000).percent).toBeLessThan(BAKE_STEPS.verify.from);
    expect(bakeProgress("install", 80_000)).toMatchObject({ stage: "receiving", detail: "Cloning and installing dependencies on a temporary Boat VM · 1m 20s" });
    expect(formatElapsed(45_400)).toBe("45s");
    expect(formatElapsed(3_725_000)).toBe("1h 2m");
  });

  test("project workdirs expand ~ to the Boat VM home and refuse relative or other-user paths", () => {
    expect(normalizeProjectWorkdir("~")).toBe("/home/user");
    expect(normalizeProjectWorkdir("~/foo")).toBe("/home/user/foo");
    expect(normalizeProjectWorkdir(" ~/foo/bar/ ")).toBe("/home/user/foo/bar");
    expect(normalizeProjectWorkdir("/srv//app/")).toBe("/srv/app");
    expect(() => normalizeProjectWorkdir("foo")).toThrow(/absolute path/);
    expect(() => normalizeProjectWorkdir("./foo")).toThrow(/absolute path/);
    expect(() => normalizeProjectWorkdir("~bob/foo")).toThrow(/only ~/);
    expect(() => normalizeProjectWorkdir("/")).toThrow(/root/);
    expect(() => normalizeProjectWorkdir("~/a/../b")).toThrow(/not allowed/);

    const { project } = buildAutoProjectConfig({ repo: "acme/foo", workdir: "~/foo" });
    expect(project.workdir).toBe("/home/user/foo");
    expect(project.bakeCmd).toContain("'/home/user/foo'");
    expect(project.bootCmd).toContain("'/home/user/foo'");
    expect(`${project.bakeCmd}\n${project.bootCmd}`).not.toContain("~");
    expect(buildAutoProjectConfig({ repo: "acme/foo" }).project.workdir).toBe("/home/user/foo");
    expect(() => buildAutoProjectConfig({ repo: "acme/foo", workdir: "foo" })).toThrow(/absolute path/);
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
        if (input.command.includes(RUN_PROVISION)) {
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
      if (!reaperRanDuringSpawn && script.includes(RUN_PROVISION)) {
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

  test("reaperTick never kills a Hub VM that exists while create() has not returned its ID yet", async () => {
    const dir = mkdtempSync(join(tmpdir(), "t3-sentinel-reaper-create-"));
    cleanups.push(() => rmSync(dir, { recursive: true, force: true }));

    const vm = startFakeVmT3Server("vm-reaper-create");
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

    // `boat new` lists the VM under its prefixed name while it is still starting; the reaper ticks then
    const origCreate = boat.create.bind(boat);
    let createdId = "";
    boat.create = async (name: string, size: string, fromSnapshot?: string) => {
      createdId = await origCreate(name, size, fromSnapshot);
      await handle.lifecycle.reaperTick();
      return createdId;
    };

    const sb = await handle.hub!.spawnSandboxForThread("my-app", "thread-reaper-create");
    expect(createdId).not.toBe("");
    expect(boat.deleted).not.toContain(createdId);
    expect(sb.sandboxId).toBe(createdId);
    expect(sb.phase).toBe("running");
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

  describe("sleep and wake with Klav proxy mode", () => {
    /** Forwarder the fake VM reports; `null` fields mean "absent". */
    type FakeForwarder = { listening: boolean; statePid: number | null; comm: string | null; owns: boolean; downExit: number };

    class KlavVmBoat extends MockBoat {
      forwarder: FakeForwarder = { listening: false, statePid: null, comm: null, owns: false, downExit: 0 };
      stopScripts: string[] = [];
      failHostPublic = false;
      constructor(readonly log: string[]) {
        super();
      }
      override async exec(id: string, script: string): Promise<BoatExecResult> {
        if (script.includes("t3s:klav-forward-probe")) {
          this.log.push("vm:klav-forward-down");
          const f = this.forwarder;
          const lines = [`down_exit=${f.downExit}`, `listening=${f.listening ? 1 : 0}`, `state_pid=${f.statePid ?? ""}`];
          if (f.statePid !== null && f.comm !== null) lines.push(`comm=${f.comm}`, `owns_listener=${f.owns ? 1 : 0}`);
          return { exitCode: 0, stdout: `${lines.join("\n")}\n`, stderr: "" };
        }
        if (script.includes("t3s:klav-forward-stop")) {
          this.stopScripts.push(script);
          this.log.push(`vm:stop-forwarder:${script.match(/^pid=(\d+)$/m)?.[1]}`);
          this.forwarder = { listening: false, statePid: null, comm: null, owns: false, downExit: 0 };
          return { exitCode: 0, stdout: "stop=term\n", stderr: "" };
        }
        return super.exec(id, script);
      }
      override async stop(id: string): Promise<void> {
        this.log.push("pause");
        return super.stop(id);
      }
      override async resume(id: string): Promise<ResumeResult> {
        this.log.push("resume");
        return super.resume(id);
      }
      override async hostPublic(id: string): Promise<string> {
        if (this.failHostPublic) throw new Error("boat port failed for https://sb.boat.test/x?_token=t3s_secretportauthvalue");
        return super.hostPublic(id);
      }
    }

    class FakeKlav implements KlavClient {
      seq = 0;
      failProvision: string | null = null;
      constructor(readonly log: string[]) {}
      async provisionForSandbox() {
        this.log.push("klav:start");
        if (this.failProvision) throw new Error(this.failProvision);
        const agentName = `ksb-new-${++this.seq}`;
        return { agentName, project: "dev" };
      }
      async revokeForSandbox(handle: string) {
        this.log.push(`klav:stop:${handle}`);
      }
    }

    async function startKlavHub() {
      const vm = startFakeVmT3Server("klav");
      cleanups.push(() => vm.server.stop(true));
      const dir = mkdtempSync(join(tmpdir(), "t3-sentinel-klav-wake-"));
      cleanups.push(() => rmSync(dir, { recursive: true, force: true }));
      const log: string[] = [];
      const boat = new KlavVmBoat(log);
      boat.upstreamBaseUrl = vm.url;
      boat.sandboxes.set("sb-k", { id: "sb-k", name: "t3s-k", state: "running", stoppedAt: null });
      const klav = new FakeKlav(log);
      const base = makeTestConfig(0);
      const cfg: SentinelConfig = {
        ...base,
        stateDir: dir,
        hub: { host: "boat.boat.example.com", label: "Boat", maxSandboxes: 5 },
        klav: { mode: "proxy", proxyPort: 18790, defaultTtl: "8h", defaultAllow: [], defaultDeny: [], defaultExpires: "8h" },
        projects: { "my-app": { ...base.projects["my-app"]!, klav: { profiles: ["dev"] } } },
      };
      const handle = createSentinelServer(cfg, boat, { startBackgroundLoops: false, klav });
      cleanups.push(() => handle.stop());
      const hub = handle.hub!;
      hub.wakeBackoffBaseMs = 60_000;
      hub.forwarderTermTimeoutSecs = 1;
      const now = new Date().toISOString();
      handle.state.setHubSandbox({
        sandboxId: "sb-k",
        project: "my-app",
        hubProjectId: "hub-proj",
        vmProjectId: vm.vmProjectId,
        snapshot: "env-my-app",
        size: "default",
        upstreamUrl: vm.url,
        upstreamHeaders: null,
        upstreamToken: "upstream-secret-token",
        watchdogSecret: "wd-secret",
        llmToken: null,
        klavAgentName: "ksb-old",
        threadIds: ["thread-k"],
        worktreePaths: [],
        phase: "running",
        createdAt: now,
        lastActiveAt: now,
        lastHeartbeatAt: now,
        sleepingSince: null,
      });
      const events: Array<{ type: string; step?: string; reason?: string }> = [];
      hub.onWakeTransition((e) => events.push(e));
      return { boat, klav, hub, handle, log, events };
    }

    /** Captures console output for the duration of `fn`. */
    async function captureLogs<T>(fn: () => Promise<T>): Promise<{ lines: string[]; result: T | undefined; error: unknown }> {
      const lines: string[] = [];
      const orig = { log: console.log, warn: console.warn, error: console.error };
      const sink = (...args: unknown[]) => lines.push(args.map((a) => (a instanceof Error ? a.message : String(a))).join(" "));
      console.log = sink;
      console.warn = sink;
      console.error = sink;
      try {
        return { lines, result: await fn(), error: undefined };
      } catch (error) {
        return { lines, result: undefined, error };
      } finally {
        Object.assign(console, orig);
      }
    }

    test("sleep stops klav-forward and ends the Klav sandbox before pausing", async () => {
      const { hub, handle, log } = await startKlavHub();
      await hub.sleepHubSandbox("sb-k");
      expect(log).toEqual(["vm:klav-forward-down", "klav:stop:ksb-old", "pause"]);
      const sb = handle.state.getHubSandbox("sb-k")!;
      expect(sb.phase).toBe("sleeping");
      expect(sb.klavAgentName).toBeNull();
      expect(sb.llmToken).toBeNull();
    });

    test("concurrent idle events pause once", async () => {
      const { hub, log } = await startKlavHub();
      await Promise.all([hub.sleepHubSandbox("sb-k"), hub.sleepHubSandbox("sb-k")]);
      expect(log.filter((l) => l === "pause")).toHaveLength(1);
      expect(log.filter((l) => l === "klav:stop:ksb-old")).toHaveLength(1);
    });

    test("wake provisions a fresh Klav sandbox after the forwarder is released", async () => {
      const { hub, handle, log, events } = await startKlavHub();
      await hub.sleepHubSandbox("sb-k");
      log.length = 0;
      const [a, b] = await Promise.all([hub.wakeHubSandbox("sb-k"), hub.wakeHubSandbox("sb-k")]);
      expect(a).toBe(b);
      expect(log).toEqual(["resume", "vm:klav-forward-down", "klav:start"]);
      const sb = handle.state.getHubSandbox("sb-k")!;
      expect(sb.phase).toBe("running");
      expect(sb.klavAgentName).toBe("ksb-new-1");
      expect(sb.wakeFailure ?? null).toBeNull();
      expect(events.map((e) => e.type)).toEqual(["wake-start", "wake-success"]);
    });

    test("wake ends a Klav sandbox left by a pause the Hub did not make", async () => {
      const { hub, handle, log } = await startKlavHub();
      handle.state.updateHubSandbox("sb-k", (s) => {
        s.phase = "sleeping";
        s.sleepingSince = new Date().toISOString();
      });
      await hub.wakeHubSandbox("sb-k");
      expect(log).toEqual(["resume", "klav:stop:ksb-old", "vm:klav-forward-down", "klav:start"]);
      expect(handle.state.getHubSandbox("sb-k")!.klavAgentName).toBe("ksb-new-1");
    });

    test("a leftover forwarder `down` cannot reach is stopped only when verified as klav-forward on 8790", async () => {
      const { hub, handle, boat, log } = await startKlavHub();
      await hub.sleepHubSandbox("sb-k");
      log.length = 0;
      // What production hit: `down` exits 1, the old forwarder still answers on 8790
      boat.forwarder = { listening: true, statePid: 4242, comm: "klav-forward", owns: true, downExit: 1 };
      await hub.wakeHubSandbox("sb-k");
      expect(log).toEqual(["resume", "vm:klav-forward-down", "vm:stop-forwarder:4242", "klav:start"]);
      expect(boat.stopScripts[0]).toContain('kill -TERM "$pid"');
      expect(boat.stopScripts[0]).toContain('kill -KILL "$pid"');
      expect(boat.stopScripts[0]).toContain('!= "klav-forward"');
      expect(handle.state.getHubSandbox("sb-k")!.phase).toBe("running");
    });

    test("a process on 8790 that is not the recorded klav-forward is never stopped, and no Klav sandbox is minted", async () => {
      const { hub, handle, boat, log, events } = await startKlavHub();
      await hub.sleepHubSandbox("sb-k");
      log.length = 0;
      boat.forwarder = { listening: true, statePid: 4242, comm: "node", owns: true, downExit: 1 };
      await expect(hub.wakeHubSandbox("sb-k")).rejects.toThrow(/klav-forward-cleanup/);
      expect(boat.stopScripts).toHaveLength(0);
      expect(log).toEqual(["resume", "vm:klav-forward-down", "pause"]);
      const sb = handle.state.getHubSandbox("sb-k")!;
      expect(sb.phase).toBe("sleeping");
      expect(sb.wakeFailure?.step).toBe("klav-forward-cleanup");
      expect(sb.wakeFailure?.vmPaused).toBe(true);
      expect(events.map((e) => e.type)).toEqual(["wake-start", "wake-failure"]);
    });

    test("judgeForwarder stops only a verified klav-forward that owns 127.0.0.1:8790", () => {
      const f = (over: Partial<ForwarderFacts>): ForwarderFacts => ({
        downExit: 1,
        listening: true,
        statePid: 4242,
        comm: "klav-forward",
        pidOwnsListener: true,
        ...over,
      });
      expect(judgeForwarder(f({}))).toEqual({ action: "stop", pid: 4242 });
      expect(judgeForwarder(f({ listening: false, statePid: null, comm: null }))).toEqual({ action: "none" });
      expect(judgeForwarder(f({ comm: "node" })).action).toBe("refuse");
      expect(judgeForwarder(f({ pidOwnsListener: false })).action).toBe("refuse");
      expect(judgeForwarder(f({ statePid: null, comm: null })).action).toBe("refuse");
      expect(judgeForwarder(f({ statePid: 1 })).action).toBe("refuse");
      expect(() => forwarderStopScript(1)).toThrow();
      expect(parseForwarderProbe("down_exit=1\nlistening=1\nstate_pid=77\ncomm=klav-forward\nowns_listener=1\n")).toEqual({
        downExit: 1,
        listening: true,
        statePid: 77,
        comm: "klav-forward",
        pidOwnsListener: true,
      });
      // The probe only prints facts: never the state file, the package or `klav-forward env`
      const probe = forwarderProbeScript();
      expect(probe).toContain("klav-forward down >/dev/null 2>&1");
      expect(probe).not.toContain("klav-forward env");
      expect(probe).not.toMatch(/cat "\$HOME\/\.klav-forward\/state\.json"/);
      expect(probe).toContain("0100007F:2256");
    });

    test("a failed wake backs off: no resume and no Klav sandbox during the cooldown, the VM is paused again", async () => {
      const { hub, handle, boat, log, events } = await startKlavHub();
      await hub.sleepHubSandbox("sb-k");
      log.length = 0;
      boat.failHostPublic = true;
      await expect(hub.wakeHubSandbox("sb-k")).rejects.toThrow(/expose-port/);
      // The new Klav sandbox is ended and the VM goes back to sleep
      expect(log).toEqual(["resume", "vm:klav-forward-down", "klav:start", "vm:klav-forward-down", "klav:stop:ksb-new-1", "pause"]);
      const sb = handle.state.getHubSandbox("sb-k")!;
      expect(sb.phase).toBe("sleeping");
      expect(sb.klavAgentName).toBeNull();
      expect(sb.wakeFailure).toMatchObject({ step: "expose-port", attempts: 1, vmPaused: true });

      log.length = 0;
      for (let i = 0; i < 5; i++) {
        await expect(hub.wakeHubSandbox("sb-k")).rejects.toBeInstanceOf(HubWakeCooldownError);
      }
      expect(log).toEqual([]);
      expect(events.filter((e) => e.type === "wake-start")).toHaveLength(1);

      // After the cooldown the next wake runs and clears the failure
      boat.failHostPublic = false;
      handle.state.updateHubSandbox("sb-k", (s) => {
        s.wakeFailure!.retryAfter = new Date(Date.now() - 1).toISOString();
      });
      await hub.wakeHubSandbox("sb-k");
      expect(log).toEqual(["resume", "vm:klav-forward-down", "klav:start"]);
      expect(handle.state.getHubSandbox("sb-k")!.wakeFailure).toBeNull();
    });

    test("the backoff doubles per consecutive failure up to its cap", () => {
      expect(wakeBackoffMs(1, 30_000, 900_000)).toBe(30_000);
      expect(wakeBackoffMs(2, 30_000, 900_000)).toBe(60_000);
      expect(wakeBackoffMs(4, 30_000, 900_000)).toBe(240_000);
      expect(wakeBackoffMs(40, 30_000, 900_000)).toBe(900_000);
    });

    test("a failed wake logs one line with the step and a reason free of secrets", async () => {
      const { hub, handle, klav, events } = await startKlavHub();
      await hub.sleepHubSandbox("sb-k");
      klav.failProvision =
        "remote command failed (exit 1): KLAV_SANDBOX=ksb1_onetimepackagevalue123 export OPENAI_BASE_URL='http://127.0.0.1:8790/v1?token=fwdsecret' " +
        "Authorization: Bearer t3llm_abcdefghijklmnopqrstuvwxyz0123 klav-forward: cannot reach it through its control socket";
      const { lines, error } = await captureLogs(() => hub.wakeHubSandbox("sb-k"));
      expect(error).toBeInstanceOf(Error);
      const failureLines = lines.filter((l) => l.includes("wake of sandbox"));
      expect(failureLines).toHaveLength(1);
      expect(failureLines[0]).toContain("step klav-provision");
      expect(failureLines[0]).toContain("control socket");
      const everything = [...lines, String((error as Error).message), JSON.stringify(events), JSON.stringify(handle.state.getHubSandbox("sb-k"))];
      for (const secret of ["ksb1_onetimepackagevalue123", "fwdsecret", "t3llm_abcdefghijklmnopqrstuvwxyz0123", "/v1?token"]) {
        for (const text of everything) expect(text).not.toContain(secret);
      }
      // Cooldown rejections log nothing
      const again = await captureLogs(() => hub.wakeHubSandbox("sb-k"));
      expect(again.error).toBeInstanceOf(HubWakeCooldownError);
      expect(again.lines).toEqual([]);
    });

    test("redactForLog drops tokens, env values, URL paths and long opaque strings", () => {
      const out = redactForLog(
        new Error("failed: ANTHROPIC_AUTH_TOKEN=abc https://h.example.com/p/secret?x=1 ksb1_zzz tskey-auth-abc token: hunter2 " + "A".repeat(40)),
      );
      for (const leak of ["abc", "/p/secret", "x=1", "ksb1_zzz", "tskey-auth", "hunter2", "A".repeat(40)]) {
        expect(out).not.toContain(leak);
      }
      expect(out).toContain("https://h.example.com");
      expect(redactForLog('{"token":"tok-123","api_key": "k-456"}')).not.toMatch(/tok-123|k-456/);
      expect(out.length).toBeLessThanOrEqual(241);
    });

    test("a failure after the VM is marked running rolls back: bridge stopped, Klav sandbox ended, token dropped, VM paused", async () => {
      const { hub, handle, boat, log, events } = await startKlavHub();
      await hub.sleepHubSandbox("sb-k");
      log.length = 0;
      hub.wakeBridgeTimeoutMs = 150;
      boat.upstreamBaseUrl = "http://127.0.0.1:9"; // nothing answers: the bridge never opens
      await expect(hub.wakeHubSandbox("sb-k")).rejects.toThrow(/at bridge/);
      expect(log).toEqual(["resume", "vm:klav-forward-down", "klav:start", "vm:klav-forward-down", "klav:stop:ksb-new-1", "pause"]);
      const sb = handle.state.getHubSandbox("sb-k")!;
      expect(sb).toMatchObject({ phase: "sleeping", klavAgentName: null, llmToken: null });
      expect(sb.wakeFailure).toMatchObject({ step: "bridge", vmPaused: true });
      expect(events.map((e) => e.type)).toEqual(["wake-start", "wake-failure"]);
    });

    test("a revoke that fails before sleep keeps the name, and the wake ends that sandbox", async () => {
      const { hub, handle, klav, log } = await startKlavHub();
      const origRevoke = klav.revokeForSandbox.bind(klav);
      klav.revokeForSandbox = async (h: string) => {
        await origRevoke(h);
        throw new Error("klav sandbox stop failed");
      };
      await captureLogs(() => hub.sleepHubSandbox("sb-k"));
      expect(handle.state.getHubSandbox("sb-k")!.klavAgentName).toBe("ksb-old");
      klav.revokeForSandbox = origRevoke;
      log.length = 0;
      await hub.wakeHubSandbox("sb-k");
      expect(log).toEqual(["resume", "klav:stop:ksb-old", "vm:klav-forward-down", "klav:start"]);
    });

    test("a resume that throws on a VM a failed wake left running still counts it as running", async () => {
      const { hub, handle, boat } = await startKlavHub();
      await hub.sleepHubSandbox("sb-k");
      hub.pauseRetryMs = 60_000;
      boat.failHostPublic = true;
      boat.stop = async () => {
        throw new Error("boat stop failed");
      };
      await captureLogs(() => hub.wakeHubSandbox("sb-k"));
      expect(handle.state.getHubSandbox("sb-k")!.wakeFailure?.vmPaused).toBe(false);
      handle.state.updateHubSandbox("sb-k", (s) => {
        s.wakeFailure!.retryAfter = new Date(Date.now() - 1).toISOString();
      });
      boat.resume = async () => {
        throw new Error("sandbox is already running");
      };
      const { error } = await captureLogs(() => hub.wakeHubSandbox("sb-k"));
      expect(String(error)).toContain("at resume");
      expect(handle.state.getHubSandbox("sb-k")!.wakeFailure).toMatchObject({ step: "resume", attempts: 2, vmPaused: false });
    });

    test("a VM the failed wake could not pause again is paused by a retry", async () => {
      const { hub, handle, boat, log } = await startKlavHub();
      await hub.sleepHubSandbox("sb-k");
      hub.pauseRetryMs = 20;
      boat.failHostPublic = true;
      const origStop = boat.stop.bind(boat);
      let failPause = true;
      boat.stop = async (id: string) => {
        if (failPause) throw new Error("boat stop failed");
        return origStop(id);
      };
      await expect(hub.wakeHubSandbox("sb-k")).rejects.toThrow();
      expect(handle.state.getHubSandbox("sb-k")!.wakeFailure?.vmPaused).toBe(false);
      failPause = false;
      log.length = 0;
      await Bun.sleep(80);
      expect(log).toEqual(["pause"]);
      expect(handle.state.getHubSandbox("sb-k")!.wakeFailure?.vmPaused).toBe(true);
    });

    test("providers without pause keep running: sleep makes no Klav or forwarder call", async () => {
      const log: string[] = [];
      const inner = asSandboxProvider(new MockBoat());
      const noPause: SandboxProvider = { ...inner, name: "no-pause", capabilities: new Set<ProviderCapability>(), pause: undefined, resume: undefined };
      const dir = mkdtempSync(join(tmpdir(), "t3-sentinel-klav-nopause-"));
      cleanups.push(() => rmSync(dir, { recursive: true, force: true }));
      const base = makeTestConfig(0);
      const cfg: SentinelConfig = {
        ...base,
        stateDir: dir,
        hub: { host: "boat.boat.example.com", label: "Boat", maxSandboxes: 5 },
        klav: { mode: "proxy", proxyPort: 18790, defaultTtl: "8h", defaultAllow: [], defaultDeny: [], defaultExpires: "8h" },
        projects: { "my-app": { ...base.projects["my-app"]!, klav: { profiles: ["dev"] } } },
      };
      const handle = createSentinelServer(cfg, noPause, { startBackgroundLoops: false, klav: new FakeKlav(log) });
      cleanups.push(() => handle.stop());
      const now = new Date().toISOString();
      handle.state.setHubSandbox({
        sandboxId: "sb-np", project: "my-app", hubProjectId: "p", vmProjectId: "v", snapshot: "base-image", size: "default",
        upstreamUrl: "http://127.0.0.1:1", upstreamToken: "t", watchdogSecret: "w", llmToken: null, klavAgentName: "ksb-live",
        threadIds: [], worktreePaths: [], phase: "running", createdAt: now, lastActiveAt: now, lastHeartbeatAt: now, sleepingSince: null,
      });
      await captureLogs(() => handle.hub!.sleepHubSandbox("sb-np"));
      expect(log).toEqual([]);
      expect(handle.state.getHubSandbox("sb-np")!.klavAgentName).toBe("ksb-live");
      expect(handle.state.getHubSandbox("sb-np")!.phase).toBe("running");
    });
  });

  describe("audit: answers the Hub builds itself decode with the T3 app's own schemas", () => {
    // Every RPC value, stream item, typed error and HTTP body the Hub synthesizes (instead of
    // forwarding a VM's answer), sent the way the app sends the request and decoded with the
    // installed app's own schemas (exit schema, stream item schema, HTTP endpoint schemas).
    type HubClient = Awaited<ReturnType<typeof connectHubClient>>;
    const until = async (cond: () => boolean, timeoutMs = 5_000) => {
      const start = Date.now();
      while (!cond()) {
        if (Date.now() - start > timeoutMs) throw new Error("condition not met in time");
        await Bun.sleep(20);
      }
    };
    const testWithApp = t3Contracts ? test : test.skip;
    type Failure = { scenario: string; tag: string; error: string; value: unknown };
    const cwd = "/home/user/my-app";

    /** Sends RPCs as the app does and decodes every frame the Hub sends back for them. */
    function auditor(client: HubClient, scenario: string) {
      let seq = 0;
      const failures: Failure[] = [];
      const checked: string[] = [];
      const tags = new Map<string, string>();
      const check = (tag: string, frame: Record<string, unknown>) => {
        checked.push(tag);
        if (!t3Contracts) return;
        const values = frame._tag === "Chunk" ? (frame.values as unknown[]) : [frame.exit];
        t3Contracts.decodeFrame(tag, frame).forEach((r, i) => {
          if (!r.ok) failures.push({ scenario, tag, error: r.error ?? "", value: values[i] });
        });
      };
      /** One request and its first frame; `silent` for a stream the Hub keeps open with nothing to send. */
      const call = async (tag: string, payload: unknown = {}, opts: { silent?: boolean } = {}) => {
        const id = `audit-${scenario}-${++seq}`;
        tags.set(id, tag);
        client.request(id, tag, payload);
        if (opts.silent) {
          await Bun.sleep(30);
          expect(client.inbox.some((f) => f.requestId === id)).toBe(false);
          return null;
        }
        const frame = await client.waitForFrame((f) => f.requestId === id, 15_000);
        check(tag, frame);
        return frame as Record<string, unknown> & { exit?: { _tag: string; value?: any; cause?: any }; values?: any[] };
      };
      /** Frames an open stream received since (e.g. shell broadcasts). */
      const drain = (tag: string, requestId: string) => {
        for (let i = 0; i < client.inbox.length; ) {
          if (client.inbox[i].requestId === requestId) check(tag, client.inbox.splice(i, 1)[0]);
          else i++;
        }
      };
      /** Later frames of every stream this auditor opened (pushes after the first frame). */
      const drainAll = () => {
        for (let i = 0; i < client.inbox.length; ) {
          const tag = tags.get(String(client.inbox[i].requestId));
          if (tag) check(tag, client.inbox.splice(i, 1)[0]);
          else i++;
        }
      };
      return { call, drain, drainAll, check, failures, checked };
    }
    const report = (failures: Failure[]) =>
      failures.map((f) => `${f.scenario} ${f.tag}: ${f.error.replace(/\s+/g, " ").slice(0, 400)} <- ${JSON.stringify(f.value)?.slice(0, 300)}`);

    test("Boat projects: repository identity and refs from the repo URL, the bake, or a chat VM's T3", async () => {
      const vm = startFakeVmT3Server("vm-audit-repo");
      cleanups.push(() => vm.server.stop(true));
      const repo = "https://github.com/OmarAlex24/webservice_panel.git";
      const { cfg, handle } = await startHubWithVm(vm, "t3-sentinel-hub-audit-repo-", {
        patchConfig: (c) => ({ ...c, projects: { ...c.projects, "my-app": { ...c.projects["my-app"], repo } } }),
      });
      const client = await connectHubClient(handle, cfg);
      const a = auditor(client, "repo");
      const shellProject = async () => {
        const shell = await a.call("orchestration.subscribeShell", {});
        expect(shell!.values![0].kind).toBe("snapshot");
        return (shell!.values![0].snapshot.projects as Array<Record<string, any>>).find((p) => p.workspaceRoot === cwd)!;
      };

      // No template: the identity a T3 server derives from this remote, so the app offers "Run on → Boat"
      // for a local checkout of the same repo (matched by canonicalKey, lower case)
      const derived = await shellProject();
      expect(derived.repositoryIdentity).toEqual({
        canonicalKey: "github.com/omaralex24/webservice_panel",
        locator: { source: "git-remote", remoteName: "origin", remoteUrl: repo },
        rootPath: cwd,
        displayName: "OmarAlex24/webservice_panel",
        provider: "github",
        owner: "OmarAlex24",
        name: "webservice_panel",
      });
      expect(derived.repositoryIdentity.canonicalKey).toBe(normalizeGitRemoteUrl("git@github.com:OmarAlex24/webservice_panel.git"));

      // No refs listed yet: an empty, schema-valid list rather than a guessed `main`
      const none = await a.call("vcs.listRefs", { cwd });
      expect(none!.exit!.value).toEqual({ isRepo: true, hasPrimaryRemote: true, nextCursor: null, totalCount: 0, refs: [] });

      // Refs an older Hub stored in a shape the app no longer decodes are not served
      handle.state.updateHub((h) => {
        h.projectTemplates["my-app"] = { vcsRefs: { branches: [{ name: "main" }] } };
      });
      expect((await a.call("vcs.listRefs", { cwd }))!.exit!.value.refs).toEqual([]);

      // The bake recorded the default branch (`origin/HEAD`): that branch, locally and on origin
      handle.state.updateHub((h) => {
        h.projectTemplates["my-app"] = { defaultBranch: "master" };
      });
      const fromBranch = await a.call("vcs.listRefs", { cwd });
      expect(fromBranch!.exit!.value.refs.map((r: { name: string; isDefault: boolean }) => [r.name, r.isDefault])).toEqual([
        ["master", true],
        ["origin/master", true],
      ]);

      // Captured by the bake or a chat VM: the VM's own answers, as they are
      const capturedRefs = {
        isRepo: true,
        hasPrimaryRemote: true,
        nextCursor: null,
        totalCount: 2,
        refs: [
          { name: "master", current: true, isDefault: true, isRemote: false, worktreePath: null },
          { name: "origin/master", current: false, isDefault: true, isRemote: true, remoteName: "origin", worktreePath: null },
        ],
      };
      const capturedIdentity = repositoryIdentityFor("git@github.com:OmarAlex24/webservice_panel.git", cwd);
      handle.state.updateHub((h) => {
        h.projectTemplates["my-app"] = {
          shell: { ...derived, id: "vm-project", title: "webservice_panel", repositoryIdentity: capturedIdentity },
          vcsRefs: capturedRefs,
          defaultBranch: "master",
        };
      });
      expect((await a.call("vcs.listRefs", { cwd }))!.exit!.value).toEqual(capturedRefs);
      const captured = await shellProject();
      expect(captured).toMatchObject({ id: derived.id, title: "my-app", workspaceRoot: cwd, repositoryIdentity: capturedIdentity });
      if (t3Contracts) {
        expect(t3Contracts.decodeWith("OrchestrationProjectShell", derived)).toEqual({ ok: true });
        expect(t3Contracts.decodeWith("OrchestrationProjectShell", captured)).toEqual({ ok: true });
      }
      expect(report(a.failures)).toEqual([]);
    }, 20_000);

    test("environment RPCs with no VM: server config, lifecycle, auth access, shell, terminals, clones, workspace fallbacks", async () => {
      const vm = startFakeVmT3Server("vm-audit-env");
      cleanups.push(() => vm.server.stop(true));
      const { cfg, handle } = await startHubWithVm(vm, "t3-sentinel-hub-audit-env-");
      const client = await connectHubClient(handle, cfg);
      const other = await connectHubClient(handle, cfg); // a second paired client, connected
      void other;
      const a = auditor(client, "no-vm");
      const hubProjectId = await subscribeHubShell(client);

      const config = await a.call("server.getConfig");
      expect(config!.exit!.value.observability).toMatchObject({ logsDirectoryPath: expect.any(String), localTracingEnabled: false });
      await a.call("subscribeServerConfig");
      await a.call("subscribeServerLifecycle");
      const access = await a.call("subscribeAuthAccess");
      const accessSnap = access!.values![0];
      expect(accessSnap).toMatchObject({ version: 1, type: "snapshot", payload: { pairingLinks: [] } });
      expect(accessSnap.payload.clientSessions).toHaveLength(2);
      expect(accessSnap.payload.clientSessions.filter((c: { current: boolean }) => c.current)).toHaveLength(1);
      expect(accessSnap.payload.clientSessions.every((c: { connected: boolean }) => c.connected)).toBe(true);
      await a.call("orchestration.subscribeShell", { requestCompletionMarker: true });
      await a.call("orchestration.getArchivedShellSnapshot", {});
      const older = await a.call("orchestration.getOlderSettledThreads", { before: { settledAt: new Date().toISOString() }, limit: 10 });
      expect(older!.exit!.value).toEqual({ snapshotSequence: expect.any(Number), threads: [], counts: [] });
      const terminals = await a.call("subscribeTerminalMetadata", {});
      expect(terminals!.values).toEqual([{ type: "snapshot", terminals: [] }]);
      await a.call("subscribeWorktreeSetup", { threadId: "thread-none" });
      await a.call("worktreeSetup.cancel", { threadId: "thread-none" });
      await a.call("subscribeProjectClones", {});
      await a.call("projectClone.cancel", { projectId: hubProjectId });
      await a.call("projectClone.retry", { projectId: hubProjectId });
      await a.call("server.reportClientActivity", {});
      await a.call("preview.reportProfiles", {});
      await a.call("preview.reportStatus", {});
      await a.call("server.refreshProviders", {});
      await a.call("server.getSettings", {});
      await a.call("server.probe", {});
      await a.call("server.updateSettings", { patch: {} });
      // No bake or VM has listed this project's refs, and it has no repo: no refs, never a guessed `main`
      const refs = await a.call("vcs.listRefs", { cwd });
      expect(refs!.exit!.value).toEqual({ isRepo: true, hasPrimaryRemote: false, nextCursor: null, totalCount: 0, refs: [] });
      await a.call("projects.listEntries", { cwd });
      await a.call("projects.searchEntries", { cwd, query: "a", limit: 10 });
      expect((await a.call("projects.listIgnoredFiles", { cwd }))!.exit!.value).toEqual({ files: [], truncated: false });
      await a.call("subscribeVcsStatus", { cwd }, { silent: true });
      await a.call("provider.install.subscribe", { instanceId: "claudeAgent" });
      await a.call("provider.auth.subscribe", { instanceId: "claudeAgent" });
      await a.call("provider.auth.start", { instanceId: "claudeAgent" });
      await a.call("preview.list", { threadId: "thread-none" });
      await a.call("orchestration.subscribePromptSuggestion", { threadId: "thread-none" }, { silent: true });
      // Pull requests, peer links and source control with no VM: empty link lists, else a readable defect
      expect((await a.call("peerLinks.list", {}))!.exit!.value).toEqual({ links: [] });
      expect((await a.call("peerLinks.names", {}))!.exit!.value).toEqual({ links: [] });
      const noVmTags = (t3Contracts?.rpcTags ?? ["pullRequests.list", "sourceControl.starred", "peerLinks.link"]).filter(
        (t) => /^(pullRequests|peerLinks|sourceControl)\./.test(t) && t !== "peerLinks.list" && t !== "peerLinks.names",
      );
      for (const tag of noVmTags) {
        if (t3Contracts?.isStream(tag)) continue;
        const frame = await a.call(tag, {});
        expect(frame!.exit!.cause[0]._tag).toBe("Die");
      }
      // An interrupted stream ends with an Interrupt cause
      client.request("int-1", "subscribeServerConfig", {});
      await client.waitForFrame((f) => f.requestId === "int-1");
      client.ws.send(JSON.stringify({ _tag: "Interrupt", requestId: "int-1" }));
      a.check("subscribeServerConfig", await client.waitForFrame((f) => f.requestId === "int-1" && f._tag === "Exit"));
      a.drain("orchestration.subscribeShell", "shell");
      expect(report(a.failures)).toEqual([]);
    }, 20_000);

    test("chats answered by the Hub: booting, failed, asleep; launch, thread views, commands, setup card, shell rows", async () => {
      const vm = startFakeVmT3Server("vm-audit-chat");
      cleanups.push(() => vm.server.stop(true));
      const { cfg, handle, boat } = await startHubWithVm(vm, "t3-sentinel-hub-audit-chat-");
      const client = await connectHubClient(handle, cfg);
      const a = auditor(client, "booting");
      client.request("shell", "orchestration.subscribeShell");
      const first = await client.waitForFrame((f) => f._tag === "Chunk" && f.requestId === "shell");
      a.check("orchestration.subscribeShell", first);
      const hubProjectId = String((first.values as Array<{ snapshot: { projects: Array<{ id: string }> } }>)[0].snapshot.projects[0].id);

      let release!: () => void;
      boat.createGate = new Promise<void>((r) => (release = r));
      cleanups.push(() => release());
      await a.call("orchestration.launchThread", clientLaunchPayload("thread-b", hubProjectId, "Hello"));
      await a.call("orchestration.subscribeThread", { threadId: "thread-b", requestCompletionMarker: true });
      await a.call("orchestration.getThreadProjection", { threadId: "thread-b" });
      await a.call("orchestration.getThreadFamily", { threadId: "thread-b" });
      await a.call("orchestration.dispatchCommand", { type: "thread.visit", commandId: "cmd-v", threadId: "thread-b", visitedAt: new Date().toISOString() });
      await a.call("subscribeWorktreeSetup", { threadId: "thread-b" });
      await a.call("orchestration.launchThread", clientLaunchPayload("thread-b", hubProjectId, "Hello")); // re-sent after a socket loss
      // Its start fails: failed row, failed setup card, failed run in the thread view
      boat.createError = new Error("no capacity");
      release();
      await until(() => handle.state.getHub()?.pendingLaunches?.["thread-b"]?.phase === "failed");
      await Bun.sleep(50);
      a.drain("orchestration.subscribeShell", "shell");
      a.drainAll(); // the thread view's failed snapshot, the failed setup card
      await a.call("orchestration.getThreadProjection", { threadId: "thread-b" });
      await a.call("subscribeWorktreeSetup", { threadId: "thread-b" });
      boat.createError = null;
      // Typed failures: a launch into an unknown project, a dispatch the Hub cannot route
      await a.call("orchestration.launchThread", { ...clientLaunchPayload("thread-x", "no-such-project", "Hi"), commandId: "cmd-x" });
      // A cloned repository becomes a project (result, clone snapshot, project row)
      const clone = await a.call("projectClone.start", {
        projectId: "11111111-2222-4333-8444-555555555555",
        remoteUrl: "https://github.com/acme/widget.git",
        destinationPath: "/home/user/widget",
        title: "widget",
      });
      expect(clone!.exit!._tag).toBe("Success");
      await a.call("subscribeProjectClones", {});
      await Bun.sleep(50);
      a.drain("orchestration.subscribeShell", "shell");
      a.drainAll(); // clone progress pushes
      expect(report(a.failures)).toEqual([]);

      // A sleeping chat (its own Hub, so the project is the VM's), with and without a cached copy
      const sleepy = await startHubWithVm(vm, "t3-sentinel-hub-audit-sleep-");
      const client2 = await connectHubClient(sleepy.handle, sleepy.cfg);
      const s = auditor(client2, "sleeping");
      await launchThreadOnHub(client2, "thread-s");
      const sbId = sleepy.handle.state.listHubSandboxes().find((x) => x.threadIds.includes("thread-s"))!.sandboxId;
      await sleepy.handle.hub!.sleepHubSandbox(sbId);
      sleepy.boat.resumeGate = new Promise<void>(() => {});
      await s.call("orchestration.getThreadProjection", { threadId: "thread-s" });
      await s.call("orchestration.getThreadFamily", { threadId: "thread-s" });
      await s.call("orchestration.subscribeThread", { threadId: "thread-s" });
      await s.call("orchestration.dispatchCommand", { type: "thread.visit", commandId: "cmd-v2", threadId: "thread-s", visitedAt: new Date().toISOString() });
      s.drain("orchestration.subscribeShell", "shell");
      s.drainAll();
      expect(report(s.failures)).toEqual([]);

      const vmNoCopy = startFakeVmT3Server("vm-audit-nocopy", { failProjection: true });
      cleanups.push(() => vmNoCopy.server.stop(true));
      const bare = await startHubWithVm(vmNoCopy, "t3-sentinel-hub-audit-nocopy-");
      const client3 = await connectHubClient(bare.handle, bare.cfg);
      const n = auditor(client3, "no-copy");
      await launchThreadOnHub(client3, "thread-n");
      await bare.handle.hub!.sleepHubSandbox(bare.handle.state.listHubSandboxes()[0].sandboxId);
      let resume!: () => void;
      bare.boat.resumeGate = new Promise<void>((r) => (resume = r));
      // Its sidebar row stands in for the projection while it wakes; a wake failure says why
      await n.call("orchestration.getThreadProjection", { threadId: "thread-n" });
      bare.boat.resumeError = new Error("resume refused");
      resume();
      await bare.handle.hub!.wakeHubSandbox(bare.handle.state.listHubSandboxes()[0].sandboxId).catch(() => {});
      await n.call("orchestration.getThreadProjection", { threadId: "thread-n" });
      await n.call("orchestration.dispatchCommand", {
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        dispatchMode: { type: "start_immediately" },
        commandId: "cmd-n",
        threadId: "thread-n",
        messageId: "msg-n",
        text: "hi",
        attachments: [],
      });
      n.drainAll();
      expect(report(n.failures)).toEqual([]);
    }, 20_000);

    test("server self-update: progress items, the result and the typed failure", async () => {
      const vm = startFakeVmT3Server("vm-audit-upd");
      cleanups.push(() => vm.server.stop(true));
      const { cfg, handle } = await startHubWithVm(vm, "t3-sentinel-hub-audit-upd-");
      const bakes = gateBakes(handle);
      const client = await connectHubClient(handle, cfg);
      const a = auditor(client, "self-update");
      const target = "0.1.13-nightly.20261125.5245";
      bakes.failOnce.add("my-app");
      const failed = await a.call("server.updateServer", { targetVersion: target });
      expect(failed!.exit!.cause[0].error._tag).toBe("ServerSelfUpdateError");
      client.request("progress", "server.updateServerWithProgress", { targetVersion: target });
      const done = await client.waitForFrame((f) => f._tag === "Exit" && f.requestId === "progress", 15_000);
      const chunks = client.inbox.filter((f) => f.requestId === "progress");
      for (const f of chunks) a.check("server.updateServerWithProgress", f);
      a.check("server.updateServerWithProgress", done);
      expect(chunks.flatMap((f) => f.values as Array<{ type: string }>).map((v) => v.type)).toContain("complete");
      expect(done.exit).toEqual({ _tag: "Success", value: null });
      expect(report(a.failures)).toEqual([]);
    }, 20_000);

    testWithApp("typed errors the Hub builds for a chat whose VM is gone", () => {
      const resource = { _tag: "media-file", threadId: "thread-gone", path: "/home/user/x.png" };
      const cases: Array<[string, Record<string, unknown>]> = [
        ["assets.createUrl", { resource }],
        ["secrets.answerRequest", { threadId: "thread-gone" }],
        ["agentFeedback.answerRequest", { threadId: "thread-gone" }],
        ["peerLinks.answerRequest", { threadId: "thread-gone" }],
        ["guestShare.create", { threadId: "thread-gone" }],
        ["provider.mcpSignIn.start", { threadId: "thread-gone", server: "linear" }],
        ["provider.mcpSignIn.complete", { threadId: "thread-gone", server: "linear" }],
        ["provider.uploadFeedback", { threadId: "thread-gone" }],
        ["workspace.addTaskFolderRepository", { threadId: "thread-gone" }],
        ["scheduledTasks.upsert", { threadId: "thread-gone" }],
        ["assets.persistChatAttachments", { threadId: "thread-gone" }],
        ...t3Contracts!.rpcTags.filter((t) => t.startsWith("mcpApps.")).map((t): [string, Record<string, unknown>] => [t, { threadId: "thread-gone" }]),
      ];
      const failures = cases.flatMap(([tag, payload]) => {
        const error = threadGoneError(tag, payload);
        expect(error).not.toBeNull();
        const r = t3Contracts!.decodeError(tag, error);
        return r.ok ? [] : [`${tag}: ${r.error}`];
      });
      expect(failures).toEqual([]);
    });

    testWithApp("HTTP endpoints the Hub answers itself: descriptor, token, session, tickets, shell", async () => {
      const vm = startFakeVmT3Server("vm-audit-http");
      cleanups.push(() => vm.server.stop(true));
      const { cfg, handle } = await startHubWithVm(vm, "t3-sentinel-hub-audit-http-");
      const base = `http://127.0.0.1:${handle.server.port}`;
      const failures: string[] = [];
      const decode = async (group: string, endpoint: string, res: Response) => {
        expect(res.status).toBe(200);
        const r = t3Contracts!.decodeHttp(group, endpoint, await res.json());
        if (!r.ok) failures.push(`${group}.${endpoint}: ${r.error?.replace(/\s+/g, " ").slice(0, 400)}`);
      };
      const pair = await fetch(`${base}/api/pair`, {
        method: "POST",
        headers: { host: cfg.controlHost, authorization: `Bearer ${cfg.controlSecret}`, "content-type": "application/json" },
        body: JSON.stringify({ slot: "hub" }),
      });
      const { code } = (await pair.json()) as { code: string };
      const token = await fetch(`${base}/oauth/token`, {
        method: "POST",
        headers: { host: cfg.hub!.host, "content-type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({ subject_token: code, client_label: "Mac" }),
      });
      const tokenBody = (await token.clone().json()) as { access_token: string };
      await decode("auth", "token", token);
      const auth = { host: cfg.hub!.host, authorization: `Bearer ${tokenBody.access_token}` };
      await decode("metadata", "descriptor", await fetch(`${base}/.well-known/t3/environment`, { headers: { host: cfg.hub!.host } }));
      await decode("auth", "session", await fetch(`${base}/api/auth/session`, { headers: auth }));
      await decode("auth", "webSocketTicket", await fetch(`${base}/api/auth/websocket-ticket`, { method: "POST", headers: auth }));
      await decode("auth", "webSocketTicketFor", await fetch(`${base}/api/auth/websocket-ticket/rpc`, { method: "POST", headers: auth }));
      await decode("orchestration", "shellSnapshot", await fetch(`${base}/api/orchestration/shell`, { headers: auth }));
      expect(failures).toEqual([]);
    });
  });
});

describe("provider status merge and setup routing (hub-providers)", () => {
  const models = [{ slug: "m", name: "M", isCustom: false, capabilities: null }];
  const ag = (patch: Record<string, unknown>) => ({ instanceId: "antigravity", enabled: true, installed: true, ...patch });
  const confirmed = ag({ status: "ready", auth: { status: "authenticated" }, models });
  const unchecked = ag({ status: "warning", auth: { status: "unknown" }, models: [] });
  const live = () => true;

  test("only a less informed entry is replaced, and only by another live VM's confirmed entry", () => {
    const book = new ProviderStatusBook();
    book.record("vm-1", [confirmed]);
    expect(book.merge("vm-2", [unchecked], live)).toEqual([confirmed]);
    // Real changes pass through: signed out, uninstalled, disabled
    for (const real of [ag({ auth: { status: "unauthenticated" }, models: [] }), ag({ installed: false }), ag({ enabled: false })]) {
      expect(book.merge("vm-2", [real], live)).toEqual([real]);
    }
    // An entry that is itself confirmed (ready with models, auth not checked) is kept as reported
    const readyUnknown = ag({ status: "ready", auth: { status: "unknown" }, models });
    expect(book.merge("vm-2", [readyUnknown], live)).toEqual([readyUnknown]);
    // A VM never vouches against itself, nor once it is gone or signed out
    const own = new ProviderStatusBook();
    own.record("vm-1", [confirmed]);
    expect(own.merge("vm-1", [unchecked], live)).toEqual([unchecked]);
    own.record("vm-1", [confirmed]);
    expect(own.merge("vm-2", [unchecked], (id) => id !== "vm-1")).toEqual([unchecked]);
    own.forgetInstance("antigravity");
    expect(own.merge("vm-2", [unchecked], live)).toEqual([unchecked]);
  });

  test("setup routing: safe VM first, idle VM preferred, sticky flow, busy only for commands", () => {
    const router = new ProviderSetupRouter();
    const loads: Record<string, { onInstance: boolean; any: boolean }> = {
      a: { onInstance: true, any: true },
      b: { onInstance: false, any: true },
      c: { onInstance: false, any: false },
    };
    const load = (id: string) => loads[id];
    expect(router.route("c1", "antigravity", "provider.auth.start", ["a", "b", "c"], load, false)).toEqual({ kind: "vm", sandboxId: "c" });
    loads.c = { onInstance: true, any: true };
    // The flow continues where it started; a new start moves off a busy VM
    expect(router.route("c1", "antigravity", "provider.auth.complete", ["a", "b", "c"], load, false)).toEqual({ kind: "vm", sandboxId: "c" });
    expect(router.route("c1", "antigravity", "provider.auth.start", ["a", "b", "c"], load, false)).toEqual({ kind: "vm", sandboxId: "b" });
    loads.b = { onInstance: true, any: true };
    expect(router.route("c2", "antigravity", "provider.auth.start", ["a", "b", "c"], load, false)).toEqual({ kind: "busy" });
    expect(router.route("c2", "antigravity", "provider.auth.subscribe", ["a", "b", "c"], load, true)).toEqual({ kind: "vm", sandboxId: "a" });
    expect(router.route("c2", "antigravity", "provider.auth.start", [], load, false)).toEqual({ kind: "none" });
  });
});

describe("chat routing helpers", () => {
  test("finds the chat top-level or in an asset resource, and skips ids that are not chats", () => {
    expect(chatThreadIdOf("secrets.answerRequest", { threadId: "t1", turnItemId: "i" })).toBe("t1");
    expect(chatThreadIdOf("assets.createUrl", { resource: { _tag: "media-file", threadId: "t2", path: "a.png" } })).toBe("t2");
    expect(chatThreadIdOf("assets.createUrl", { resource: { _tag: "draft-workspace-file", cwd: "/w", path: "a.png" } })).toBeNull();
    expect(chatThreadIdOf("assets.createUrl", { resource: { _tag: "attachment", attachmentId: "x" } })).toBeNull();
    expect(chatThreadIdOf("pullRequests.replyToThread", { threadId: "PRRT_1", body: "ok" })).toBeNull();
    expect(chatThreadIdOf("peerLinks.subscribeThread", { environmentId: "e", threadId: "t3" })).toBeNull();
    expect(chatThreadIdOf("peerLinks.answerRequest", { threadId: "t4", turnItemId: "i", answer: { type: "decline" } })).toBe("t4");
  });

  test("gone-VM errors are members of each RPC's own error union", () => {
    expect(threadGoneError("agentFeedback.answerRequest", { threadId: "t" })).toEqual({ _tag: "AgentFeedbackRequestError", reason: "not_found" });
    expect(threadGoneError("guestShare.create", { threadId: "t" })).toEqual({ _tag: "GuestShareCreateError", reason: "thread_unavailable" });
    expect(threadGoneError("mcpApps.callTool", { threadId: "t", itemId: "i" })).toEqual({ _tag: "McpAppRequestError", threadId: "t", reason: "session-stopped" });
    expect(threadGoneError("provider.mcpSignIn.start", { threadId: "t", server: "linear" })?._tag).toBe("ProviderMcpSignInError");
    expect(threadGoneError("provider.uploadFeedback", { threadId: "t" })).toEqual({ _tag: "ProviderUploadFeedbackError", threadId: "t" });
    // No fitting member: the caller sends an untyped Die
    expect(threadGoneError("device.open", { threadId: "t" })).toBeNull();
  });
});

describe("repo sync to remote (fresh chat VMs)", () => {
  // Real git, isolated from the machine's config
  const gitEnv = {
    ...process.env,
    GIT_CONFIG_GLOBAL: "/dev/null",
    GIT_CONFIG_SYSTEM: "/dev/null",
    GIT_CONFIG_NOSYSTEM: "1",
    GIT_AUTHOR_NAME: "t",
    GIT_AUTHOR_EMAIL: "t@example.com",
    GIT_COMMITTER_NAME: "t",
    GIT_COMMITTER_EMAIL: "t@example.com",
  };
  const git = (cwd: string, ...args: string[]) => {
    const r = Bun.spawnSync(["git", ...args], { cwd, env: gitEnv });
    if (r.exitCode !== 0) throw new Error(`git ${args.join(" ")}: ${r.stderr.toString()}`);
    return r.stdout.toString().trim();
  };
  const runSync = (workdir: string, keep: string[] = []) => {
    const r = Bun.spawnSync(["bash", "-c", buildRepoSyncScript(workdir, 30, keep)], { env: gitEnv });
    return { result: parseRepoSyncOutput(r.stdout.toString()), raw: r.stdout.toString() + r.stderr.toString() };
  };

  /** An origin with `main`, a pusher clone, and a "snapshot" checkout baked from it. */
  function setupRepos() {
    const root = mkdtempSync(join(tmpdir(), "t3-sentinel-sync-"));
    tempDirs.push(root);
    const origin = join(root, "origin.git");
    git(root, "init", "-q", "--bare", "-b", "main", origin);
    const pusher = join(root, "pusher");
    git(root, "clone", "-q", origin, pusher);
    git(pusher, "checkout", "-q", "-b", "main");
    writeFileSync(join(pusher, "app.ts"), "v1\n");
    writeFileSync(join(pusher, ".gitignore"), "node_modules/\n");
    writeFileSync(join(pusher, "bun.lock"), "lock-v1\n");
    git(pusher, "add", ".");
    git(pusher, "commit", "-q", "-m", "v1");
    git(pusher, "push", "-q", "origin", "main");
    const work = join(root, "work");
    git(root, "clone", "-q", origin, work);
    const pushNew = (content: string) => {
      writeFileSync(join(pusher, "app.ts"), content);
      git(pusher, "commit", "-q", "-am", content.trim());
      git(pusher, "push", "-q", "origin", "main");
      return git(pusher, "rev-parse", "HEAD");
    };
    return { root, origin, work, pushNew };
  }

  test("a dirty snapshot is reset to origin's default branch; ignored caches and sentinel's files survive", () => {
    const { work, pushNew } = setupRepos();
    const baked = git(work, "rev-parse", "HEAD");
    // What a bake leaves: a rewritten lockfile, a stray file, an installed cache
    writeFileSync(join(work, "bun.lock"), "lock-rewritten-by-install\n");
    writeFileSync(join(work, "stray.log"), "x\n");
    mkdirSync(join(work, "node_modules", "pkg"), { recursive: true });
    writeFileSync(join(work, "node_modules", "pkg", "index.js"), "cached\n");
    writeFileSync(join(work, "sentinel-placed.json"), "{}\n");
    const latest = pushNew("v2\n");

    const { result, raw } = runSync(work, [join(work, "sentinel-placed.json")]);
    // bun.lock and stray.log; the kept file is not counted
    expect(result).toEqual({ status: "synced", from: baked, to: latest, branch: "main", discarded: 2 });
    expect(readFileSync(join(work, "app.ts"), "utf8")).toBe("v2\n");
    expect(readFileSync(join(work, "bun.lock"), "utf8")).toBe("lock-v1\n");
    expect(existsSync(join(work, "stray.log"))).toBe(false);
    expect(existsSync(join(work, "node_modules", "pkg", "index.js"))).toBe(true);
    expect(existsSync(join(work, "sentinel-placed.json"))).toBe(true);
    expect(git(work, "status", "--porcelain")).toBe("?? sentinel-placed.json");
    // Only SHAs, a branch and a count: no paths, no remote URL
    expect(raw).not.toContain(work);
    expect(raw).not.toContain("origin.git");
  });

  test("a diverged snapshot on another branch ends on origin's default branch, tracking it", () => {
    const { work, pushNew } = setupRepos();
    git(work, "checkout", "-q", "-b", "bake-branch");
    writeFileSync(join(work, "app.ts"), "local\n");
    git(work, "commit", "-q", "-am", "local-only");
    const local = git(work, "rev-parse", "HEAD");
    const latest = pushNew("v3\n");

    const { result } = runSync(work);
    expect(result).toEqual({ status: "synced", from: local, to: latest, branch: "main", discarded: 0 });
    expect(git(work, "rev-parse", "--abbrev-ref", "HEAD")).toBe("main");
    expect(git(work, "rev-parse", "--abbrev-ref", "main@{upstream}")).toBe("origin/main");
  });

  test("skips what is not a fresh checkout of a remote, and reports a failed fetch without the URL", () => {
    const { root, work } = setupRepos();
    expect(runSync(join(root, "missing")).result).toEqual({ status: "skipped", reason: "no-workdir" });
    const plain = join(root, "plain");
    mkdirSync(plain);
    expect(runSync(plain).result).toEqual({ status: "skipped", reason: "not-a-repository" });
    mkdirSync(join(work, "sub"));
    expect(runSync(join(work, "sub")).result).toEqual({ status: "skipped", reason: "not-repo-root" });
    const local = join(root, "local-only");
    git(root, "init", "-q", "-b", "main", local);
    expect(runSync(local).result).toEqual({ status: "skipped", reason: "no-origin" });

    git(work, "remote", "set-url", "origin", "https://x-access-token:ghs_secret@127.0.0.1:1/nope.git");
    const before = git(work, "rev-parse", "HEAD");
    writeFileSync(join(work, "stray.log"), "x\n");
    const failed = runSync(work);
    expect(failed.result).toEqual({ status: "failed", reason: "fetch-failed" });
    expect(failed.raw).not.toContain("ghs_secret");
    // Nothing was touched
    expect(git(work, "rev-parse", "HEAD")).toBe(before);
    expect(existsSync(join(work, "stray.log"))).toBe(true);
  });

  test("unexpected output is a failure, never a throw", () => {
    expect(parseRepoSyncOutput("")).toEqual({ status: "failed", reason: "no-result" });
    expect(parseRepoSyncOutput("{not json")).toEqual({ status: "failed", reason: "no-result" });
    expect(parseRepoSyncOutput(JSON.stringify({ status: "synced", to: "zz", branch: "main" }))).toEqual({ status: "failed", reason: "no-result" });
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

describe("repository identity of Boat projects (T3's RepositoryIdentityResolver)", () => {
  const ssh = "git@github.com:OmarAlex24/webservice_panel.git";
  const https = "https://github.com/OmarAlex24/webservice_panel.git";

  test("an HTTPS `.git` remote has the canonicalKey T3 computes for the SSH remote of the same repo", () => {
    const identity = repositoryIdentityFor(https, "/home/user/webservice-panel")!;
    expect(identity.canonicalKey).toBe("github.com/omaralex24/webservice_panel");
    expect(identity.canonicalKey).toBe(normalizeGitRemoteUrl(ssh));
    expect(repositoryIdentityFor(ssh, "/home/user/webservice-panel")!.canonicalKey).toBe(identity.canonicalKey);
    expect(identity).toMatchObject({ provider: "github", owner: "OmarAlex24", name: "webservice_panel", displayName: "OmarAlex24/webservice_panel" });
    expect(repositoryIdentityFor("https://gitlab.example.com/group/sub/app.git", "/w")).toMatchObject({
      canonicalKey: "gitlab.example.com/group/sub/app",
      provider: "unknown",
      owner: "group",
      name: "app",
    });
    expect(repositoryIdentityFor("/srv/git/local-only", "/w")).toBeNull();
  });

  test("a remote's credentials never reach the identity", () => {
    const identity = repositoryIdentityFor("https://x-access-token:SECRET-tok@github.com/acme/app.git?private_token=SECRET-q", "/w")!;
    expect(identity.locator).toEqual({ source: "git-remote", remoteName: "origin", remoteUrl: "https://github.com/acme/app.git" });
    expect(JSON.stringify(identity)).not.toContain("SECRET");
    expect(identity.canonicalKey).toBe("github.com/acme/app");
  });

  test("the fallback refs never guess a branch, and decode with the app's schema", () => {
    expect(fallbackVcsRefs(null, true)).toEqual({ isRepo: true, hasPrimaryRemote: true, nextCursor: null, totalCount: 0, refs: [] });
    expect((fallbackVcsRefs("master", true).refs as Array<{ name: string }>).map((r) => r.name)).toEqual(["master", "origin/master"]);
    expect((fallbackVcsRefs("trunk", false).refs as Array<{ name: string }>).map((r) => r.name)).toEqual(["trunk"]);
    expect(defaultBranchOfRefs(fallbackVcsRefs("master", true))).toBe("master");
    // A chat VM's refs as a template: no chat branches, worktrees or other chat's current branch
    const vmRefs = {
      isRepo: true,
      hasPrimaryRemote: true,
      nextCursor: 100,
      totalCount: 4,
      refs: [
        { name: "t3/feature", current: true, isDefault: false, isRemote: false, worktreePath: "/home/user/.t3/worktrees/x" },
        { name: "master", current: false, isDefault: true, isRemote: false, worktreePath: "/home/user/app" },
        { name: "origin/master", current: false, isDefault: true, isRemote: true, remoteName: "origin", worktreePath: null },
        { name: "origin/dev", current: false, isDefault: false, isRemote: true, remoteName: "origin", worktreePath: null },
      ],
    };
    const template = templateVcsRefs(vmRefs)!;
    expect(template).toMatchObject({ nextCursor: null, totalCount: 3 });
    expect((template.refs as Array<Record<string, unknown>>).map((r) => [r.name, r.current, r.worktreePath])).toEqual([
      ["master", true, null],
      ["origin/master", false, null],
      ["origin/dev", false, null],
    ]);
    expect(templateVcsRefs({ branches: ["main"] })).toBeNull();
    if (t3Contracts) expect(t3Contracts.decodeSuccess("vcs.listRefs", template)).toEqual({ ok: true });
    if (!t3Contracts) return;
    for (const refs of [fallbackVcsRefs(null, true), fallbackVcsRefs("master", true), fallbackVcsRefs("trunk", false)]) {
      expect(t3Contracts.decodeSuccess("vcs.listRefs", refs)).toEqual({ ok: true });
    }
  });

  (t3Contracts ? test : test.skip)("canonicalKey and remote parsing match the T3 app's own functions", () => {
    const t3Normalize = t3Contracts!.exported("normalizeGitRemoteUrl") as (url: string) => string;
    const remotes = [
      ssh,
      https,
      "https://github.com/OmarAlex24/webservice_panel",
      "https://github.com/OmarAlex24/webservice_panel/",
      "ssh://git@github.com/OmarAlex24/webservice_panel.git",
      "ssh://git@github.com:22/OmarAlex24/webservice_panel.git",
      "git://github.com/OmarAlex24/webservice_panel.git",
      "https://x-access-token:tok@github.com/OmarAlex24/webservice_panel.git",
      "git@gitlab.com:group/sub/project.git",
      "https://gitlab.example.com:8443/group/sub/project.git",
      "git@github.com:Acme/My%20Repo.git",
      "https://github.com/Acme/My%20Repo",
      "git@[::1]:acme/app.git",
      "/srv/git/local-only",
      "C:/repos/app",
    ];
    for (const remote of remotes) expect([remote, normalizeGitRemoteUrl(remote)]).toEqual([remote, t3Normalize(remote)]);
    // Production: the local "webservices" checkout and the Boat project "webservice-panel"
    expect(t3Normalize(ssh)).toBe(repositoryIdentityFor(https, "/home/user/webservice-panel")!.canonicalKey as string);
    expect(t3Contracts!.decodeWith("OrchestrationProjectShell", {
      id: "p1",
      title: "webservice-panel",
      workspaceRoot: "/home/user/webservice-panel",
      repositoryIdentity: repositoryIdentityFor(https, "/home/user/webservice-panel"),
      defaultModelSelection: null,
      scripts: [],
      createdAt: "2026-11-25T00:00:00.000Z",
      updatedAt: "2026-11-25T00:00:00.000Z",
      deletedAt: null,
    })).toEqual({ ok: true });
  });
});

describe("preview frame flow control at the Hub (hub-media)", () => {
  const until = async (cond: () => boolean, timeoutMs = 3_000) => {
    const start = Date.now();
    while (!cond()) {
      if (Date.now() - start > timeoutMs) throw new Error("condition not met in time");
      await Bun.sleep(5);
    }
  };
  const text = (data: unknown) => (typeof data === "string" ? data : new TextDecoder().decode(data as ArrayBuffer));
  const frameBytes = (label: string) => new TextEncoder().encode(label);

  /**
   * A fake VM media server, a Hub running only `HubMediaProxy`, and a client socket on the Hub.
   * `vmOneWayMs` delays everything the VM sends and receives (half the Hub<->VM round trip);
   * `onVmOpen` / `onVmMessage` script the VM. `flowControl: false` is the old pass-through.
   */
  async function mediaRig(opts: {
    path?: string;
    vmOneWayMs?: number;
    flowControl?: boolean;
    onVmOpen?: (send: (data: string | Uint8Array) => void) => void;
    onVmMessage?: (msg: string, send: (data: string | Uint8Array) => void) => void;
  } = {}) {
    const vmOneWay = opts.vmOneWayMs ?? 0;
    const vmReceived: string[] = [];
    let vmSocket: Bun.ServerWebSocket<unknown> | null = null;
    const later = (fn: () => void) => (vmOneWay > 0 ? setTimeout(fn, vmOneWay) : fn());
    const vmSend = (data: string | Uint8Array) =>
      later(() => {
        if (!vmSocket) return;
        if (typeof data === "string") vmSocket.sendText(data);
        else vmSocket.sendBinary(data);
      });
    const vm = Bun.serve({
      port: 0,
      hostname: "127.0.0.1",
      fetch(req, srv) {
        return srv.upgrade(req) ? undefined : new Response("no", { status: 400 });
      },
      websocket: {
        open(ws) {
          vmSocket = ws;
          opts.onVmOpen?.(vmSend);
        },
        message(_ws, raw) {
          const msg = text(raw);
          later(() => {
            vmReceived.push(msg);
            opts.onVmMessage?.(msg, vmSend);
          });
        },
      },
    });
    cleanups.push(() => vm.stop(true));
    const proxy = new HubMediaProxy(async () => ({
      ok: true,
      sandbox: { sandboxId: "sb-fc", upstreamUrl: `http://127.0.0.1:${vm.port}`, upstreamToken: "vm-token" },
    }));
    const hub = Bun.serve<HubMediaWsData, never>({
      port: 0,
      hostname: "127.0.0.1",
      fetch(req, srv) {
        const data = newMediaWsData("client-1", new URL(req.url));
        if (opts.flowControl === false) data.frames = null;
        return srv.upgrade(req, { data }) ? undefined : new Response("no", { status: 400 });
      },
      websocket: {
        open: (ws) => proxy.open(ws),
        message: (ws, msg) => proxy.message(ws, msg),
        close: (ws, code, reason) => proxy.close(ws, code, reason),
      },
    });
    cleanups.push(() => hub.stop(true));
    const path = opts.path ?? "/api/preview-stream/ws?threadId=t1&tabId=tab-1&maxWidth=800&maxHeight=600&wsTicket=hub-ticket";
    const client = new WebSocket(`ws://127.0.0.1:${hub.port}${path}`);
    client.binaryType = "arraybuffer";
    cleanups.push(() => client.close());
    const received: Array<{ kind: "frame" | "text"; body: string }> = [];
    client.addEventListener("message", (ev) => {
      received.push({ kind: typeof ev.data === "string" ? "text" : "frame", body: text(ev.data) });
    });
    await new Promise<void>((resolve) => client.addEventListener("open", () => resolve()));
    await until(() => vmSocket !== null && proxy.count("sb-fc") === 1);
    await Bun.sleep(10);
    const vmClose = (code: number, reason: string) => vmSocket?.close(code, reason);
    return { vmSend, vmClose, vmReceived, client, received, proxy, frames: () => received.filter((m) => m.kind === "frame").map((m) => m.body) };
  }

  test("the VM gets an ack for each frame before the client has acked any", async () => {
    let sent = 0;
    const rig = await mediaRig({
      onVmOpen: (send) => send(frameBytes(`f${++sent}`)),
      // The VM's one-in-flight: the next frame goes out on the ack
      onVmMessage: (msg, send) => {
        if (msg === PREVIEW_ACK && sent < 3) send(frameBytes(`f${++sent}`));
      },
    });
    await until(() => rig.vmReceived.length === 3);
    expect(rig.vmReceived).toEqual([PREVIEW_ACK, PREVIEW_ACK, PREVIEW_ACK]);
    // The client acked nothing, so it has the first frame only
    await Bun.sleep(30);
    expect(rig.frames()).toEqual(["f1"]);
  });

  test("a slow client gets the newest frame after its ack; the frames in between are dropped", async () => {
    const rig = await mediaRig();
    rig.vmSend(frameBytes("f1"));
    await until(() => rig.frames().length === 1 && rig.vmReceived.length === 1);
    for (const label of ["f2", "f3", "f4"]) rig.vmSend(frameBytes(label));
    await until(() => rig.vmReceived.length === 4);
    await Bun.sleep(30);
    expect(rig.frames()).toEqual(["f1"]);
    rig.client.send(PREVIEW_ACK);
    await until(() => rig.frames().length === 2);
    expect(rig.frames()).toEqual(["f1", "f4"]);
    // Nothing is held any more: another ack sends nothing, and the next frame goes straight out
    rig.client.send(PREVIEW_ACK);
    await Bun.sleep(30);
    expect(rig.frames()).toEqual(["f1", "f4"]);
    rig.vmSend(frameBytes("f5"));
    await until(() => rig.frames().length === 3);
    expect(rig.frames()).toEqual(["f1", "f4", "f5"]);
  });

  test("client acks never reach the VM, double acks and binary acks included", async () => {
    const rig = await mediaRig();
    rig.client.send(PREVIEW_ACK);
    rig.client.send(PREVIEW_ACK);
    rig.client.send(new TextEncoder().encode(PREVIEW_ACK));
    rig.client.send(JSON.stringify({ type: "mouse", action: "move" }));
    await until(() => rig.vmReceived.length > 0);
    await Bun.sleep(30);
    expect(rig.vmReceived).toEqual([JSON.stringify({ type: "mouse", action: "move" })]);
    // One frame, three client acks: the VM sees exactly the Hub's one ack
    rig.vmSend(frameBytes("f1"));
    await until(() => rig.frames().length === 1);
    for (let i = 0; i < 3; i++) rig.client.send(PREVIEW_ACK);
    await Bun.sleep(50);
    expect(rig.vmReceived).toEqual([JSON.stringify({ type: "mouse", action: "move" }), PREVIEW_ACK]);
  });

  test("control and input pass through unchanged and in order, past a held frame", async () => {
    const rig = await mediaRig();
    const input1 = JSON.stringify({ type: "key", key: "a" });
    const input2 = JSON.stringify({ type: "takeControl" });
    rig.client.send(input1);
    rig.client.send(PREVIEW_ACK);
    rig.client.send(input2);
    rig.client.send("not json");
    await until(() => rig.vmReceived.length === 3);
    expect(rig.vmReceived).toEqual([input1, input2, "not json"]);

    const viewport = JSON.stringify({ type: "viewport", width: 800, height: 600 });
    const control = JSON.stringify({ type: "control", canOperate: true, controller: "you", generation: 2, dialog: null });
    rig.vmSend(frameBytes("f1"));
    rig.vmSend(frameBytes("f2"));
    rig.vmSend(viewport);
    rig.vmSend(control);
    await until(() => rig.received.length === 3);
    await Bun.sleep(30);
    // f2 waits for the client's ack; the control messages do not
    expect(rig.received).toEqual([
      { kind: "frame", body: "f1" },
      { kind: "text", body: viewport },
      { kind: "text", body: control },
    ]);
    rig.client.send(PREVIEW_ACK);
    await until(() => rig.received.length === 4);
    expect(rig.received[3]).toEqual({ kind: "frame", body: "f2" });
    expect(rig.vmReceived.slice(3)).toEqual([PREVIEW_ACK, PREVIEW_ACK]);
  });

  test("the device hub stays a plain pipe: binary messages and acks pass through", async () => {
    const rig = await mediaRig({ path: "/api/device-hub/ws?device=d1" });
    rig.vmSend(frameBytes("d1"));
    rig.vmSend(frameBytes("d2"));
    await until(() => rig.frames().length === 2);
    rig.client.send(PREVIEW_ACK);
    await until(() => rig.vmReceived.length === 1);
    expect(rig.vmReceived).toEqual([PREVIEW_ACK]);
    expect(rig.frames()).toEqual(["d1", "d2"]);
  });

  test("a held frame is dropped when the VM closes, and the VM's close code reaches the client", async () => {
    const rig = await mediaRig();
    let closed = null as number | null;
    rig.client.addEventListener("close", (ev) => (closed = ev.code));
    rig.vmSend(frameBytes("f1"));
    rig.vmSend(frameBytes("f2"));
    await until(() => rig.vmReceived.length === 2);
    rig.vmClose(4404, "tab closed");
    await until(() => closed !== null);
    expect(closed).toBe(4404);
    expect(rig.frames()).toEqual(["f1"]);
  });

  /**
   * Latency simulation: a VM with T3's real one-in-flight logic, a page that draws a new frame
   * every 16 ms, 170 ms Hub<->VM and 100 ms client<->Hub round trips, and a client that acks each
   * frame on arrival like the app. Before = the old pass-through, after = flow control at the Hub.
   */
  test("latency simulation: frames per second before and after", async () => {
    const VM_ONE_WAY = 85;
    const CLIENT_ONE_WAY = 50;
    const WARMUP_MS = 600;
    const WINDOW_MS = 3_000;
    async function run(flowControl: boolean) {
      // The VM: T3's one-in-flight with a newer frame superseding the pending one
      let inFlight = false;
      let pending: Uint8Array | null = null;
      let seq = 0;
      let send: (data: Uint8Array) => void = () => {};
      const sendFrame = (frame: Uint8Array) => {
        inFlight = true;
        send(frame);
      };
      const timer = setInterval(() => {
        const frame = frameBytes(`${++seq}:${Date.now()}`);
        if (!inFlight) sendFrame(frame);
        else pending = frame;
      }, 16);
      cleanups.push(() => clearInterval(timer));
      const rig = await mediaRig({
        flowControl,
        vmOneWayMs: VM_ONE_WAY,
        onVmOpen: (vmSend) => {
          send = vmSend;
        },
        onVmMessage: (msg) => {
          if (msg !== PREVIEW_ACK) return;
          inFlight = false;
          const next = pending;
          pending = null;
          if (next) sendFrame(next);
        },
      });
      // The client: each frame lands CLIENT_ONE_WAY after the Hub sent it, and its ack takes as long back
      const arrivals: Array<{ at: number; age: number }> = [];
      rig.client.addEventListener("message", (ev) => {
        if (typeof ev.data === "string") return;
        const producedAt = Number(text(ev.data).split(":")[1]);
        setTimeout(() => {
          const at = Date.now();
          arrivals.push({ at, age: at - producedAt });
          setTimeout(() => rig.client.send(PREVIEW_ACK), CLIENT_ONE_WAY);
        }, CLIENT_ONE_WAY);
      });
      const start = Date.now();
      await Bun.sleep(WARMUP_MS + WINDOW_MS);
      clearInterval(timer);
      const window = arrivals.filter((a) => a.at >= start + WARMUP_MS && a.at < start + WARMUP_MS + WINDOW_MS);
      const ages = window.map((a) => a.age).sort((x, y) => x - y);
      rig.client.close();
      return { fps: window.length / (WINDOW_MS / 1000), medianAgeMs: ages[Math.floor(ages.length / 2)] ?? NaN };
    }
    const before = await run(false);
    const after = await run(true);
    console.log(
      `[preview flow sim] 170 ms Hub<->VM, 100 ms client<->Hub: before ${before.fps.toFixed(1)} fps (median frame age ${before.medianAgeMs} ms), after ${after.fps.toFixed(1)} fps (median frame age ${after.medianAgeMs} ms)`,
    );
    expect(before.fps).toBeLessThan(4.5);
    expect(after.fps).toBeGreaterThan(before.fps * 1.25);
  }, 20_000);
});
