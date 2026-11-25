export type ProviderCapability = "pause" | "snapshots" | "private-preview";

export interface SandboxInfo {
  id: string;
  name?: string;
  state: string;
  stoppedAt?: string | null;
}

export interface ExecResult {
  exitCode: number | null;
  stdout: string;
  stderr: string;
}

export type ResumeResult = "resumed" | "data_expired";

export interface ExposedEndpoint {
  /** Base HTTPS/HTTP URL (no trailing slash) reaching the exposed sandbox port. */
  url: string;
  /** Optional provider gate headers (e.g. Boat/Sandboxer Cookie, Upstash Bearer) sent on every upstream HTTP & WS request. */
  headers?: Record<string, string>;
  /** Optional ISO-8601 expiry timestamp if the provider issues short-lived preview URLs/cookies. */
  expiresAt?: string;
}

export interface CommandOptions {
  signal?: AbortSignal;
}

export interface CreateSandboxOptions {
  name: string;
  size: string;
  fromSnapshot?: string;
  labels?: Record<string, string>;
}

/**
 * Provider-agnostic sandbox contract (inspired by `@opencoredev/sandbox-sdk`).
 *
 * Implement this interface to plug any cloud VM / microVM / container platform
 * (Boat, E2B, Daytona, Docker, Freestyle, boxd, Upstash Box, Railway, etc.) into
 * `t3-boat-sentinel` without changing Hub, Proxy, Lifecycle, or Klav orchestration.
 *
 * Capability-gated methods (`pause`/`resume`, `saveSnapshot`/`listSnapshots`) are
 * optional and declared in `capabilities` so Sentinel degrades gracefully on providers
 * that do not support snapshots or pause/resume.
 */
export interface SandboxProvider {
  readonly name: string;
  readonly capabilities: ReadonlySet<ProviderCapability>;

  create(opts: CreateSandboxOptions): Promise<string>;
  info(id: string): Promise<SandboxInfo>;
  list(): Promise<SandboxInfo[]>;
  delete(id: string): Promise<void>;

  exposePort(
    id: string,
    port: number,
    opts?: { visibility?: "private" | "public" },
  ): Promise<ExposedEndpoint>;

  /** `opts.signal` stops waiting on the command; providers that can also stop it remotely or locally do so. */
  exec(id: string, script: string, timeoutSecs?: number, opts?: CommandOptions): Promise<ExecResult>;
  execOk(id: string, script: string, timeoutSecs?: number, opts?: CommandOptions): Promise<string>;
  upload(id: string, localPath: string, remotePath: string, opts?: CommandOptions): Promise<void>;
  /**
   * Copies one file out of the sandbox by file transfer, never through command output (which
   * providers such as Boat keep in their command history). Optional: providers without a
   * transfer that keeps contents out of logs leave it undefined, and callers must degrade.
   */
  download?(id: string, remotePath: string, localPath: string, timeoutSecs?: number): Promise<void>;

  /** Present when `capabilities.has("pause")`. */
  pause?(id: string): Promise<void>;
  /** Present when `capabilities.has("pause")`. */
  resume?(id: string, size: string): Promise<ResumeResult>;

  /** Present when `capabilities.has("snapshots")`. */
  saveSnapshot?(id: string, name: string): Promise<void>;
  /** Present when `capabilities.has("snapshots")`. */
  listSnapshots?(): Promise<string[]>;
}

export function hasCapability(provider: SandboxProvider, cap: ProviderCapability): boolean {
  return provider.capabilities.has(cap);
}

/**
 * Legacy BoatClient shape preserved for backwards compatibility with existing callers and tests.
 */
export interface LegacyBoatClient {
  create(name: string, size: string, fromSnapshot?: string): Promise<string>;
  info(id: string): Promise<SandboxInfo>;
  list(): Promise<SandboxInfo[]>;
  resume(id: string, size: string): Promise<ResumeResult>;
  stop(id: string): Promise<void>;
  delete(id: string): Promise<void>;
  saveSnapshot(id: string, name: string): Promise<void>;
  namedSnapshots(): Promise<string[]>;
  hostPublic(id: string, port: number): Promise<string>;
  exec(id: string, script: string, timeoutSecs?: number, opts?: CommandOptions): Promise<ExecResult>;
  execOk(id: string, script: string, timeoutSecs?: number, opts?: CommandOptions): Promise<string>;
  upload(id: string, localPath: string, remotePath: string, opts?: CommandOptions): Promise<void>;
  download?(id: string, remotePath: string, localPath: string, timeoutSecs?: number): Promise<void>;
}

export function isSandboxProvider(value: SandboxProvider | LegacyBoatClient): value is SandboxProvider {
  return "capabilities" in value && "exposePort" in value;
}

/** Adapt a legacy `BoatClient` mock/implementation into a full `SandboxProvider`. */
export function asSandboxProvider(client: SandboxProvider | LegacyBoatClient): SandboxProvider {
  if (isSandboxProvider(client)) return client;
  return {
    name: "boat-legacy",
    capabilities: new Set<ProviderCapability>(["pause", "snapshots"]),
    create: (opts) => client.create(opts.name, opts.size, opts.fromSnapshot),
    info: (id) => client.info(id),
    list: () => client.list(),
    delete: (id) => client.delete(id),
    exposePort: async (id, port) => ({ url: await client.hostPublic(id, port) }),
    exec: (id, script, timeoutSecs, opts) => client.exec(id, script, timeoutSecs, opts),
    execOk: (id, script, timeoutSecs, opts) => client.execOk(id, script, timeoutSecs, opts),
    upload: (id, localPath, remotePath, opts) => client.upload(id, localPath, remotePath, opts),
    ...(typeof client.download === "function"
      ? {
          download: (id: string, remotePath: string, localPath: string, timeoutSecs?: number) =>
            client.download!(id, remotePath, localPath, timeoutSecs),
        }
      : {}),
    pause: (id) => client.stop(id),
    resume: (id, size) => client.resume(id, size),
    saveSnapshot: (id, name) => client.saveSnapshot(id, name),
    listSnapshots: () => client.namedSnapshots(),
  };
}
