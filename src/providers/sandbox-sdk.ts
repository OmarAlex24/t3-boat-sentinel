import { readFileSync } from "node:fs";
import type {
  CreateSandboxOptions,
  ExecResult,
  ExposedEndpoint,
  ProviderCapability,
  ResumeResult,
  SandboxInfo,
  SandboxProvider,
} from "./types.ts";

/**
 * Minimal structural shape of an `@opencoredev/sandbox-sdk` Provider / Sandbox handle
 * so `t3-boat-sentinel` stays zero-dependency when users only run Boat, while allowing
 * any of the 14+ Sandbox SDK adapters (E2B, Daytona, Docker, Vercel, Upstash Box,
 * Railway, Freestyle, boxd, Superserve, Sandboxer, Luxor, ...) to plug in via config!
 */
export interface ExternalSdkProviderLike {
  name: string;
  capabilities?: Iterable<string> | ReadonlySet<string>;
  create(input: Record<string, unknown>): Promise<{ id: string } | string>;
  connect?(id: string): Promise<ExternalSdkSandboxLike>;
  list?(labels?: Record<string, string>): Promise<Array<{ id: string; name?: string; status?: string; state?: string }>>;
  destroy?(id: string): Promise<void>;
  delete?(id: string): Promise<void>;
  exec?(id: string, input: { command: string; timeoutMs?: number }): Promise<{ exitCode?: number | null; stdout?: string; stderr?: string }>;
  files?: {
    write(id: string, path: string, content: Uint8Array | string, opts?: { mode?: number }): Promise<void>;
  };
  previewUrl?(id: string, port: number): Promise<{ url: string; headers?: Record<string, string>; expiresAt?: string | number }>;
  pause?(id: string): Promise<void>;
  resume?(id: string): Promise<void>;
  snapshot?(id: string, opts?: { name?: string }): Promise<{ id?: string; name?: string } | string | void>;
  listSnapshots?(): Promise<Array<string | { name?: string; id?: string }>>;
}

export interface ExternalSdkSandboxLike {
  id: string;
  run?(cmd: string, opts?: { timeoutMs?: number }): Promise<{ exitCode?: number | null; stdout?: string; stderr?: string }>;
  files?: {
    write(path: string, content: Uint8Array | string, opts?: { mode?: number }): Promise<void>;
  };
  preview?(port: number, opts?: { waitMs?: number }): Promise<{ url: string; headers?: Record<string, string>; expiresAt?: string | number }>;
  pause?(): Promise<void>;
  resume?(): Promise<void>;
  destroy?(): Promise<void>;
  snapshot?(opts?: { name?: string }): Promise<{ id?: string; name?: string } | string | void>;
}

export interface SandboxSdkProviderConfig {
  /** Adapter subpath inside `@opencoredev/sandbox-sdk/<adapter>` (e.g. "daytona", "e2b", "docker", "upstash", "freestyle", "boxd"). */
  adapter: string;
  /** Optional options object passed to the adapter factory (e.g. `{ image: "...", region: "..." }`). */
  options?: Record<string, unknown>;
  /** Optional mapping from Sentinel size names ("default", "medium", "large") to provider size specs. */
  sizeMap?: Record<string, unknown>;
  /** Label key/value applied to all sandboxes created by Sentinel so `list()` scopes strictly to Sentinel sandboxes. */
  sentinelLabel?: string;
}

/**
 * Generic bridge adapting any `@opencoredev/sandbox-sdk` provider to Sentinel's `SandboxProvider`.
 *
 * Usage in `sentinel.yaml`:
 * ```yaml
 * provider:
 *   type: sandbox-sdk
 *   adapter: daytona   # or e2b, docker, vercel, upstash, railway, freestyle, boxd, ...
 *   options: {}
 * ```
 * Install `@opencoredev/sandbox-sdk` (`bun add @opencoredev/sandbox-sdk`) when using this provider.
 */
export class SandboxSdkProvider implements SandboxProvider {
  readonly name: string;
  readonly capabilities: ReadonlySet<ProviderCapability>;
  private sdkProvider: ExternalSdkProviderLike | null = null;
  private initPromise: Promise<ExternalSdkProviderLike> | null = null;
  private readonly handles = new Map<string, ExternalSdkSandboxLike>();
  private readonly snapshotIds = new Map<string, string>();

  constructor(
    private readonly cfg: SandboxSdkProviderConfig,
    preloaded?: ExternalSdkProviderLike,
  ) {
    this.name = `sandbox-sdk:${cfg.adapter}`;
    const caps = new Set<ProviderCapability>(["private-preview"]);
    if (preloaded) {
      this.sdkProvider = preloaded;
      const rawCaps = new Set(Array.from(preloaded.capabilities ?? []));
      if (rawCaps.has("pause") || typeof preloaded.pause === "function") caps.add("pause");
      if (rawCaps.has("snapshot") || rawCaps.has("snapshots") || typeof preloaded.snapshot === "function") {
        caps.add("snapshots");
      }
    }
    this.capabilities = caps;
  }

  private async ensureSdk(): Promise<ExternalSdkProviderLike> {
    if (this.sdkProvider) return this.sdkProvider;
    if (!this.initPromise) {
      this.initPromise = (async () => {
        const modName = `@opencoredev/sandbox-sdk/${this.cfg.adapter}`;
        let mod: Record<string, unknown>;
        try {
          mod = (await import(modName)) as Record<string, unknown>;
        } catch (err) {
          throw new Error(
            `Failed to load "${modName}". Install it with \`bun add @opencoredev/sandbox-sdk\`. (${err instanceof Error ? err.message : String(err)})`,
          );
        }
        const factory = mod[this.cfg.adapter] ?? mod.default;
        if (typeof factory !== "function") {
          throw new Error(`Module "${modName}" does not export factory "${this.cfg.adapter}" or default`);
        }
        const instance = (factory as (opts?: Record<string, unknown>) => ExternalSdkProviderLike)(this.cfg.options);
        const mutableCaps = this.capabilities as Set<ProviderCapability>;
        const rawCaps = new Set(Array.from(instance.capabilities ?? []));
        if (rawCaps.has("pause") || typeof instance.pause === "function") mutableCaps.add("pause");
        if (rawCaps.has("snapshot") || rawCaps.has("snapshots") || typeof instance.snapshot === "function") {
          mutableCaps.add("snapshots");
        }
        this.sdkProvider = instance;
        return instance;
      })();
    }
    return this.initPromise;
  }

  private async getHandle(id: string): Promise<ExternalSdkSandboxLike | null> {
    const cached = this.handles.get(id);
    if (cached) return cached;
    const sdk = await this.ensureSdk();
    if (typeof sdk.connect === "function") {
      const handle = await sdk.connect(id);
      this.handles.set(id, handle);
      return handle;
    }
    return null;
  }

  async create(opts: CreateSandboxOptions): Promise<string> {
    const sdk = await this.ensureSdk();
    const mappedSize = this.cfg.sizeMap?.[opts.size] ?? opts.size;
    const snapRef = opts.fromSnapshot ? (this.snapshotIds.get(opts.fromSnapshot) ?? opts.fromSnapshot) : undefined;
    const created = await sdk.create({
      name: opts.name,
      key: opts.name,
      size: mappedSize,
      ...(snapRef ? { snapshot: snapRef } : {}),
      labels: {
        "t3-sentinel": this.cfg.sentinelLabel ?? "1",
        "t3-sentinel-name": opts.name,
        ...(opts.labels ?? {}),
      },
    });
    return typeof created === "string" ? created : created.id;
  }

  async info(id: string): Promise<SandboxInfo> {
    const all = await this.list();
    const found = all.find((s) => s.id === id);
    if (!found) throw new Error(`Sandbox "${id}" not found on provider ${this.name}`);
    return found;
  }

  async list(): Promise<SandboxInfo[]> {
    const sdk = await this.ensureSdk();
    if (typeof sdk.list !== "function") return [];
    const raw = await sdk.list({ "t3-sentinel": this.cfg.sentinelLabel ?? "1" });
    return raw.map((item) => ({
      id: item.id,
      name: item.name,
      state: normalizeState(item.state ?? item.status ?? "running"),
      stoppedAt: null,
    }));
  }

  async delete(id: string): Promise<void> {
    const sdk = await this.ensureSdk();
    this.handles.delete(id);
    if (typeof sdk.destroy === "function") {
      await sdk.destroy(id);
      return;
    }
    if (typeof sdk.delete === "function") {
      await sdk.delete(id);
      return;
    }
    const handle = await this.getHandle(id);
    if (handle?.destroy) {
      await handle.destroy();
      return;
    }
    throw new Error(`Provider ${this.name} does not implement destroy/delete`);
  }

  async exposePort(id: string, port: number): Promise<ExposedEndpoint> {
    const sdk = await this.ensureSdk();
    let res: { url: string; headers?: Record<string, string>; expiresAt?: string | number } | undefined;
    if (typeof sdk.previewUrl === "function") {
      res = await sdk.previewUrl(id, port);
    } else {
      const handle = await this.getHandle(id);
      if (handle?.preview) {
        res = await handle.preview(port, { waitMs: 10_000 });
      }
    }
    if (!res?.url) {
      throw new Error(`Provider ${this.name} returned no preview URL for port ${port}`);
    }
    return {
      url: res.url.replace(/\/+$/, ""),
      ...(res.headers && Object.keys(res.headers).length > 0 ? { headers: res.headers } : {}),
      ...(res.expiresAt !== undefined
        ? { expiresAt: typeof res.expiresAt === "number" ? new Date(res.expiresAt).toISOString() : res.expiresAt }
        : {}),
    };
  }

  async exec(id: string, script: string, timeoutSecs = 120): Promise<ExecResult> {
    const sdk = await this.ensureSdk();
    const cmd = `bash -lc ${shellQuote(script)}`;
    if (typeof sdk.exec === "function") {
      const r = await sdk.exec(id, { command: cmd, timeoutMs: timeoutSecs * 1000 });
      return {
        exitCode: r.exitCode ?? 0,
        stdout: r.stdout ?? "",
        stderr: r.stderr ?? "",
      };
    }
    const handle = await this.getHandle(id);
    if (handle?.run) {
      const r = await handle.run(cmd, { timeoutMs: timeoutSecs * 1000 });
      return {
        exitCode: r.exitCode ?? 0,
        stdout: r.stdout ?? "",
        stderr: r.stderr ?? "",
      };
    }
    throw new Error(`Provider ${this.name} does not implement exec/run`);
  }

  async execOk(id: string, script: string, timeoutSecs = 120): Promise<string> {
    const r = await this.exec(id, script, timeoutSecs);
    if (r.exitCode !== 0) {
      throw new Error(`remote command failed (exit ${r.exitCode}): ${(r.stderr.trim() || r.stdout.trim()).slice(-600)}`);
    }
    return r.stdout;
  }

  async upload(id: string, localPath: string, remotePath: string): Promise<void> {
    const bytes = new Uint8Array(readFileSync(localPath));
    const sdk = await this.ensureSdk();
    const resolvedPath = remotePath.startsWith("~/") ? `/home/user/${remotePath.slice(2)}` : remotePath;
    if (sdk.files?.write) {
      await sdk.files.write(id, resolvedPath, bytes, { mode: 0o600 });
      return;
    }
    const handle = await this.getHandle(id);
    if (handle?.files?.write) {
      await handle.files.write(resolvedPath, bytes, { mode: 0o600 });
      return;
    }
    // Safe base64 fallback over exec when provider does not expose a native file upload API
    const b64 = Buffer.from(bytes).toString("base64");
    await this.execOk(
      id,
      `mkdir -p "$(dirname ${shellQuote(resolvedPath)})" && printf '%s' ${shellQuote(b64)} | base64 -d > ${shellQuote(resolvedPath)} && chmod 600 ${shellQuote(resolvedPath)}`,
      120,
    );
  }

  // No `download`: the SDK surface used here has no file read known to keep contents out of the
  // provider's command logs, so features that pull files from a VM (provider session capture) stay off.

  async pause(id: string): Promise<void> {
    const sdk = await this.ensureSdk();
    if (typeof sdk.pause === "function") {
      await sdk.pause(id);
      return;
    }
    const handle = await this.getHandle(id);
    if (handle?.pause) {
      await handle.pause();
      return;
    }
    throw new Error(`Provider ${this.name} does not support pause`);
  }

  async resume(id: string): Promise<ResumeResult> {
    const sdk = await this.ensureSdk();
    if (typeof sdk.resume === "function") {
      await sdk.resume(id);
      return "resumed";
    }
    const handle = await this.getHandle(id);
    if (handle?.resume) {
      await handle.resume();
      return "resumed";
    }
    throw new Error(`Provider ${this.name} does not support resume`);
  }

  async saveSnapshot(id: string, name: string): Promise<void> {
    const sdk = await this.ensureSdk();
    let res: { id?: string; name?: string } | string | void;
    if (typeof sdk.snapshot === "function") {
      res = await sdk.snapshot(id, { name });
    } else {
      const handle = await this.getHandle(id);
      if (handle?.snapshot) {
        res = await handle.snapshot({ name });
      } else {
        throw new Error(`Provider ${this.name} does not support snapshots`);
      }
    }
    const snapId = typeof res === "string" ? res : (res?.id ?? res?.name ?? name);
    this.snapshotIds.set(name, snapId);
  }

  async listSnapshots(): Promise<string[]> {
    const sdk = await this.ensureSdk();
    const out = new Set<string>(this.snapshotIds.keys());
    if (typeof sdk.listSnapshots === "function") {
      const remote = await sdk.listSnapshots();
      for (const item of remote) {
        const label = typeof item === "string" ? item : (item.name ?? item.id);
        if (label) out.add(label);
      }
    }
    return Array.from(out);
  }
}

function normalizeState(raw: string): string {
  const s = raw.toLowerCase();
  if (s === "paused" || s === "suspended" || s === "archived") return "stopped";
  return s;
}

function shellQuote(s: string): string {
  return `'${s.replace(/'/g, `'\\''`)}'`;
}
