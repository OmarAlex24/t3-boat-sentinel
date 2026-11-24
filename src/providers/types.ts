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

  exec(id: string, script: string, timeoutSecs?: number): Promise<ExecResult>;
  execOk(id: string, script: string, timeoutSecs?: number): Promise<string>;
  upload(id: string, localPath: string, remotePath: string): Promise<void>;

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
  exec(id: string, script: string, timeoutSecs?: number): Promise<ExecResult>;
  execOk(id: string, script: string, timeoutSecs?: number): Promise<string>;
  upload(id: string, localPath: string, remotePath: string): Promise<void>;
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
    exec: (id, script, timeoutSecs) => client.exec(id, script, timeoutSecs),
    execOk: (id, script, timeoutSecs) => client.execOk(id, script, timeoutSecs),
    upload: (id, localPath, remotePath) => client.upload(id, localPath, remotePath),
    pause: (id) => client.stop(id),
    resume: (id, size) => client.resume(id, size),
    saveSnapshot: (id, name) => client.saveSnapshot(id, name),
    listSnapshots: () => client.namedSnapshots(),
  };
}
