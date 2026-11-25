import { createHash, randomBytes } from "node:crypto";
import {
  chmodSync,
  closeSync,
  existsSync,
  mkdirSync,
  openSync,
  readFileSync,
  renameSync,
  rmSync,
  statSync,
  unlinkSync,
  writeSync,
} from "node:fs";
import { join } from "node:path";
import type { SentinelConfig } from "./config.ts";
import type { SandboxProvider } from "./providers/types.ts";

/**
 * Persists provider sign-ins (e.g. Antigravity's Google OAuth token) across disposable chat VMs.
 *
 * - Capture: when a VM reports an instance listed in `t3.persistProviderSessions` as authenticated,
 *   `exec` lists which credential files exist (names and byte counts only), each file is copied out
 *   with the provider's `download` (`boat scp`), and it is stored on the sentinel host under
 *   `<stateDir>/credentials/<instanceId>/<file>` (dir 0700, file 0600, atomic rename). Contents never
 *   travel through `exec` output: Boat keeps every command's stdout/stderr in `boat history`.
 * - Inject: fresh VMs get the stored files staged into `~/.t3-sentinel/provider-sessions/` before
 *   `provision-base.sh` runs; the script installs them (0600) under `~/.t3/userdata/` and restarts t3.
 * - Forget: sign-out through the Hub or `sentinel forget-session` deletes the stored copy.
 *
 * Credential contents never reach state.json, logs, errors or API responses.
 */

/** Instance IDs become directory names and shell arguments. */
export const PROVIDER_INSTANCE_ID_RE = /^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/;

/** Remote root for staged credentials inside a VM; mirrors the layout under `~/.t3/userdata/`. */
export const STAGED_SESSIONS_DIR = "~/.t3-sentinel/provider-sessions";

const MAX_CREDENTIAL_BYTES = 64 * 1024;
const FILE_NAME_RE = /^[A-Za-z0-9._-]+$/;

interface CredentialFileSpec {
  name: string;
  /** Returns an ISO expiry to compare tokens by, or null when the content does not parse. */
  expiry?: (content: Buffer) => string | null;
}

interface DriverCredentialSpec {
  /** Directory relative to `~/.t3/userdata/` that holds the credential files for one instance. */
  userdataDir: (instanceId: string) => string;
  files: CredentialFileSpec[];
}

function sha256Hex(data: string | Buffer): string {
  return createHash("sha256").update(data).digest("hex");
}

/** `{ token: { access_token, refresh_token?, expiry? } }` written by the Antigravity 1.5 CLI. */
function antigravityTokenExpiry(content: Buffer): string | null {
  try {
    const parsed = JSON.parse(content.toString("utf8")) as { token?: { access_token?: unknown; expiry?: unknown } };
    const token = parsed?.token;
    if (!token || typeof token.access_token !== "string" || token.access_token.length === 0) return null;
    return typeof token.expiry === "string" ? token.expiry : null;
  } catch {
    return null;
  }
}

/**
 * Credential locations per T3 provider driver. Add a driver here to make it syncable.
 * Antigravity: T3 hashes the instance ID (sha256 hex) into the profile directory and, for
 * `oauth-personal`, forces the CLI to keep its sign-in in a file instead of the OS keyring.
 */
export const DRIVER_CREDENTIALS: Record<string, DriverCredentialSpec> = {
  antigravity: {
    userdataDir: (instanceId) => `providers/antigravity/${sha256Hex(instanceId)}/antigravity-acp`,
    files: [
      { name: "antigravity-oauth-token", expiry: antigravityTokenExpiry },
      // Legacy name the 1.5 CLI no longer writes; still a credential if present
      { name: "acp_token.json" },
    ],
  },
};

export interface StagedCredential {
  instanceId: string;
  localPath: string;
  /** Remote staging path (under `~/.t3-sentinel/provider-sessions/`). */
  remotePath: string;
  /** Remote staging directory to create before uploading. */
  remoteDir: string;
}

interface ObservedProvider {
  instanceId?: unknown;
  driver?: unknown;
  enabled?: unknown;
  auth?: { status?: unknown } | null;
}

export class ProviderSessionStore {
  /** Delay between an `authenticated` status and the capture; lets a burst of status updates settle. */
  debounceMs = 3_000;
  /** Upper bound for the pre-teardown pull. */
  teardownTimeoutMs = 10_000;

  private readonly baseDir: string;
  private readonly enabled: Set<string>;
  private readonly settingsDrivers = new Map<string, string>();
  private readonly observedDrivers = new Map<string, string>();
  /** `${sandboxId}\0${instanceId}` -> last auth status seen from that VM. */
  private readonly lastStatus = new Map<string, string>();
  /** VMs whose copy must not be captured again after a sign-out until they re-authenticate. */
  private readonly suppressed = new Set<string>();
  private readonly timers = new Map<string, ReturnType<typeof setTimeout>>();
  private readonly generation = new Map<string, number>();
  private readonly warnedDrivers = new Set<string>();
  private warnedNoDownload = false;
  /** In-flight captures, so tests and teardown can await them. */
  private readonly inFlight = new Set<Promise<void>>();

  constructor(
    private readonly config: SentinelConfig,
    private readonly boat: SandboxProvider,
  ) {
    this.baseDir = join(config.stateDir, "credentials");
    this.enabled = new Set(
      (config.t3.persistProviderSessions ?? []).filter((id) => PROVIDER_INSTANCE_ID_RE.test(id)),
    );
    if (config.t3.settingsFile && existsSync(config.t3.settingsFile)) {
      try {
        const settings = JSON.parse(readFileSync(config.t3.settingsFile, "utf8")) as {
          providerInstances?: Record<string, { driver?: unknown }>;
        };
        for (const [id, inst] of Object.entries(settings.providerInstances ?? {})) {
          if (typeof inst?.driver === "string") this.settingsDrivers.set(id, inst.driver);
        }
      } catch {
        console.warn("[sessions] could not read providerInstances from t3.settingsFile; assuming driver = instance ID");
      }
    }
  }

  isEnabled(instanceId: string): boolean {
    return this.enabled.has(instanceId);
  }

  enabledInstances(): string[] {
    return [...this.enabled];
  }

  private specFor(instanceId: string): DriverCredentialSpec | null {
    const driver = this.settingsDrivers.get(instanceId) ?? this.observedDrivers.get(instanceId) ?? instanceId;
    const spec = DRIVER_CREDENTIALS[driver];
    if (!spec && !this.warnedDrivers.has(instanceId)) {
      this.warnedDrivers.add(instanceId);
      console.warn(`[sessions] persistProviderSessions: driver "${driver}" of instance "${instanceId}" is not supported; skipping`);
    }
    return spec ?? null;
  }

  private instanceDir(instanceId: string): string {
    return join(this.baseDir, instanceId);
  }

  /** Non-secret marker with the time of the last sign-out; kept outside the instance dir. */
  private forgottenMarker(instanceId: string): string {
    return join(this.baseDir, `${instanceId}.signed-out-at`);
  }

  /** True when the VM was created before the instance's last sign-out (it may still hold that token). */
  private signedOutBefore(instanceId: string, sandboxCreatedAt?: string): boolean {
    if (!sandboxCreatedAt) return false;
    let at: number;
    try {
      at = Date.parse(readFileSync(this.forgottenMarker(instanceId), "utf8").trim());
    } catch {
      return false;
    }
    const created = Date.parse(sandboxCreatedAt);
    return !Number.isNaN(at) && !Number.isNaN(created) && created <= at;
  }

  /** False (with a one-time warning) when the provider cannot copy files out without exec output. */
  private canDownload(): boolean {
    if (typeof this.boat.download === "function") return true;
    if (!this.warnedNoDownload) {
      this.warnedNoDownload = true;
      console.warn(
        `[sessions] provider ${this.boat.name} has no file download; provider sign-ins will not be captured from its VMs`,
      );
    }
    return false;
  }

  /** Lists stored instance IDs (names only, never contents). */
  listStored(): string[] {
    return this.enabledInstances().filter((id) => this.storedFiles(id).length > 0);
  }

  private storedFiles(instanceId: string): Array<{ spec: CredentialFileSpec; path: string }> {
    const spec = this.specFor(instanceId);
    if (!spec) return [];
    const dir = this.instanceDir(instanceId);
    return spec.files.map((f) => ({ spec: f, path: join(dir, f.name) })).filter((f) => existsSync(f.path));
  }

  /** Stored credentials to stage into a fresh VM before `provision-base.sh` runs. */
  stagedCredentials(): StagedCredential[] {
    const out: StagedCredential[] = [];
    for (const instanceId of this.enabledInstances()) {
      const spec = this.specFor(instanceId);
      if (!spec) continue;
      const rel = spec.userdataDir(instanceId);
      for (const f of this.storedFiles(instanceId)) {
        out.push({
          instanceId,
          localPath: f.path,
          remoteDir: `${STAGED_SESSIONS_DIR}/${rel}`,
          remotePath: `${STAGED_SESSIONS_DIR}/${rel}/${f.spec.name}`,
        });
      }
    }
    return out;
  }

  /** Marks a VM as already holding the stored copy, so its first `authenticated` report is not re-read. */
  markInjected(sandboxId: string, instanceIds: Iterable<string>): void {
    for (const id of instanceIds) this.lastStatus.set(key(sandboxId, id), "authenticated");
  }

  /**
   * Feeds one VM's provider list. Only call for VMs that finished booting. `sandboxCreatedAt` lets a
   * sign-out survive a sentinel restart: VMs created before it are not re-captured until they sign in again.
   */
  observe(sandboxId: string, providers: unknown[], sandboxCreatedAt?: string): void {
    if (this.enabled.size === 0) return;
    for (const raw of providers) {
      if (!raw || typeof raw !== "object") continue;
      const p = raw as ObservedProvider;
      if (typeof p.instanceId !== "string" || !this.enabled.has(p.instanceId)) continue;
      const instanceId = p.instanceId;
      if (typeof p.driver === "string") this.observedDrivers.set(instanceId, p.driver);
      const status = p.enabled === false ? "disabled" : typeof p.auth?.status === "string" ? p.auth.status : "unknown";
      const k = key(sandboxId, instanceId);
      const prev = this.lastStatus.get(k);
      if (prev === undefined && this.signedOutBefore(instanceId, sandboxCreatedAt)) this.suppressed.add(k);
      this.lastStatus.set(k, status);
      if (status !== "authenticated") {
        // A fresh sign-in on this VM after a sign-out may be captured again
        this.suppressed.delete(k);
        continue;
      }
      if (prev === "authenticated" || this.suppressed.has(k)) continue;
      this.scheduleCapture(sandboxId, instanceId);
    }
  }

  private scheduleCapture(sandboxId: string, instanceId: string): void {
    const k = key(sandboxId, instanceId);
    const existing = this.timers.get(k);
    if (existing) clearTimeout(existing);
    this.timers.set(
      k,
      setTimeout(() => {
        this.timers.delete(k);
        this.track(this.capture(sandboxId, instanceId));
      }, this.debounceMs),
    );
  }

  private track(p: Promise<void>): Promise<void> {
    this.inFlight.add(p);
    void p.finally(() => this.inFlight.delete(p));
    return p;
  }

  /** Resolves once no capture is pending or running (for tests and orderly shutdown). */
  async settle(): Promise<void> {
    while (this.timers.size > 0 || this.inFlight.size > 0) {
      await Promise.allSettled([...this.inFlight]);
      if (this.timers.size > 0) await Bun.sleep(Math.min(this.debounceMs, 50));
    }
  }

  /**
   * Copies the instance's credential files out of a VM and stores them. `exec` only learns which
   * files exist and their sizes; the bytes come over `download` (file transfer), never through
   * command output. Never throws; failures are logged as constant strings.
   */
  async capture(sandboxId: string, instanceId: string, timeoutSecs = 30): Promise<void> {
    const spec = this.specFor(instanceId);
    if (!spec || this.suppressed.has(key(sandboxId, instanceId)) || !this.canDownload()) return;
    const gen = this.generation.get(instanceId) ?? 0;
    const rel = spec.userdataDir(instanceId);
    const names = spec.files.map((f) => f.name).filter((n) => FILE_NAME_RE.test(n));
    // Prints "<name> <bytes>" per existing file; `wc -c` outputs only the count
    const listing =
      `cd "$HOME/.t3/userdata/${rel}" 2>/dev/null || exit 0; ` +
      `for f in ${names.join(" ")}; do if [ -f "$f" ]; then printf '%s %s\\n' "$f" $(wc -c <"$f"); fi; done; true`;

    // A failed read is retried on the VM's next `authenticated` report
    const allowRetry = () => this.lastStatus.delete(key(sandboxId, instanceId));
    let present: string[];
    try {
      const res = await this.boat.exec(sandboxId, listing, timeoutSecs);
      if (res.exitCode !== 0) {
        console.warn(`[sessions] could not list ${instanceId} credentials on ${sandboxId} (exit ${res.exitCode})`);
        allowRetry();
        return;
      }
      present = parseListing(res.stdout, names);
    } catch {
      console.warn(`[sessions] could not list ${instanceId} credentials on ${sandboxId}`);
      allowRetry();
      return;
    }
    if (present.length === 0) {
      console.log(`[sessions] ${sandboxId} reports ${instanceId} signed in but holds no credential file; nothing stored`);
      allowRetry();
      return;
    }

    const found = new Map<string, Buffer>();
    for (const name of present) {
      const content = await this.downloadOne(sandboxId, `~/.t3/userdata/${rel}/${name}`, timeoutSecs);
      if (!content) {
        console.warn(`[sessions] could not copy ${instanceId} ${name} from ${sandboxId}`);
        allowRetry();
        return;
      }
      found.set(name, content);
    }
    // A sign-out (or forget) happened while reading: drop this copy
    if ((this.generation.get(instanceId) ?? 0) !== gen || this.suppressed.has(key(sandboxId, instanceId))) return;

    for (const fileSpec of spec.files) {
      const content = found.get(fileSpec.name);
      if (content) this.store(instanceId, fileSpec, content, sandboxId);
    }
  }

  /**
   * Downloads one file into a 0600 temp file inside the 0700 credentials dir, reads it into memory
   * and always deletes the temp file. Returns null on any failure or an empty/oversized file.
   */
  private async downloadOne(sandboxId: string, remotePath: string, timeoutSecs: number): Promise<Buffer | null> {
    let tmp: string | null = null;
    try {
      mkdirSync(this.baseDir, { recursive: true, mode: 0o700 });
      chmodSync(this.baseDir, 0o700);
      tmp = join(this.baseDir, `.download-${randomBytes(6).toString("hex")}.tmp`);
      closeSync(openSync(tmp, "wx", 0o600));
      chmodSync(tmp, 0o600);
      await this.boat.download!(sandboxId, remotePath, tmp, timeoutSecs);
      chmodSync(tmp, 0o600);
      const size = statSync(tmp).size;
      if (size === 0 || size > MAX_CREDENTIAL_BYTES) return null;
      return readFileSync(tmp);
    } catch {
      return null;
    } finally {
      if (tmp) rmSync(tmp, { force: true });
    }
  }

  private store(instanceId: string, fileSpec: CredentialFileSpec, content: Buffer, sandboxId: string): void {
    const dir = this.instanceDir(instanceId);
    const target = join(dir, fileSpec.name);
    if (existsSync(target)) {
      let current: Buffer | null = null;
      try {
        current = readFileSync(target);
      } catch {
        current = null;
      }
      if (current) {
        if (sha256Hex(current) === sha256Hex(content)) return;
        if (fileSpec.expiry) {
          const curExp = Date.parse(fileSpec.expiry(current) ?? "");
          const newExp = Date.parse(fileSpec.expiry(content) ?? "");
          if (!Number.isNaN(curExp) && !Number.isNaN(newExp) && newExp < curExp) {
            console.log(`[sessions] kept stored ${instanceId} ${fileSpec.name}: ${sandboxId} holds an older token`);
            return;
          }
        }
      }
    }
    try {
      writeAtomic0600(this.baseDir, dir, target, content);
      console.log(`[sessions] stored ${instanceId} ${fileSpec.name} from ${sandboxId}`);
    } catch (err) {
      const code = (err as NodeJS.ErrnoException | undefined)?.code ?? "unknown";
      console.warn(`[sessions] failed to store ${instanceId} ${fileSpec.name} (${code})`);
    }
  }

  /** Last status this VM reported for the instance. */
  lastStatusOf(sandboxId: string, instanceId: string): string | undefined {
    return this.lastStatus.get(key(sandboxId, instanceId));
  }

  /**
   * Best-effort pull before a VM is destroyed (the CLI may have refreshed its token). Bounded by
   * `teardownTimeoutMs`; never throws.
   */
  async pullBeforeDestroy(sandboxId: string): Promise<void> {
    const ids = this.enabledInstances().filter((id) => {
      const k = key(sandboxId, id);
      return this.lastStatus.get(k) === "authenticated" && !this.suppressed.has(k);
    });
    if (ids.length === 0 || !this.canDownload()) return;
    for (const id of ids) {
      const t = this.timers.get(key(sandboxId, id));
      if (t) {
        clearTimeout(t);
        this.timers.delete(key(sandboxId, id));
      }
    }
    const timeoutSecs = Math.max(1, Math.ceil(this.teardownTimeoutMs / 1000));
    const work = Promise.allSettled(ids.map((id) => this.track(this.capture(sandboxId, id, timeoutSecs))));
    let timer: ReturnType<typeof setTimeout> | undefined;
    await Promise.race([work, new Promise<void>((r) => (timer = setTimeout(r, this.teardownTimeoutMs)))]);
    if (timer) clearTimeout(timer);
  }

  /** Drops all tracking for a VM that is gone. */
  forgetSandbox(sandboxId: string): void {
    const prefix = `${sandboxId}\0`;
    for (const [k, t] of this.timers) {
      if (k.startsWith(prefix)) {
        clearTimeout(t);
        this.timers.delete(k);
      }
    }
    for (const k of [...this.lastStatus.keys()]) if (k.startsWith(prefix)) this.lastStatus.delete(k);
    for (const k of [...this.suppressed]) if (k.startsWith(prefix)) this.suppressed.delete(k);
  }

  /**
   * Deletes the stored copy (sign-out or `forget-session`). VMs that still hold the token are
   * suppressed until they report a non-authenticated status and sign in again.
   */
  forget(instanceId: string): { removed: boolean } {
    if (!PROVIDER_INSTANCE_ID_RE.test(instanceId)) throw new Error("Invalid provider instance ID");
    this.generation.set(instanceId, (this.generation.get(instanceId) ?? 0) + 1);
    const suffix = `\0${instanceId}`;
    for (const [k, t] of this.timers) {
      if (k.endsWith(suffix)) {
        clearTimeout(t);
        this.timers.delete(k);
      }
    }
    for (const [k, status] of this.lastStatus) {
      if (k.endsWith(suffix) && status === "authenticated") this.suppressed.add(k);
    }
    const dir = this.instanceDir(instanceId);
    const removed = existsSync(dir);
    rmSync(dir, { recursive: true, force: true });
    try {
      mkdirSync(this.baseDir, { recursive: true, mode: 0o700 });
      chmodSync(this.baseDir, 0o700);
      writeAtomic0600(this.baseDir, this.baseDir, this.forgottenMarker(instanceId), Buffer.from(new Date().toISOString()));
    } catch {
      console.warn(`[sessions] could not record the ${instanceId} sign-out time; a restart may re-capture it from older VMs`);
    }
    if (removed) console.log(`[sessions] forgot stored ${instanceId} session`);
    return { removed };
  }

  stop(): void {
    for (const t of this.timers.values()) clearTimeout(t);
    this.timers.clear();
  }
}

/** Names from a "<name> <bytes>" listing that are expected credential files of a plausible size. */
function parseListing(stdout: string, names: string[]): string[] {
  const out: string[] = [];
  for (const line of stdout.split(/\r?\n/)) {
    const [name, bytes] = line.trim().split(/\s+/);
    if (!name || !names.includes(name) || out.includes(name)) continue;
    const size = Number(bytes);
    if (!Number.isInteger(size) || size <= 0 || size > MAX_CREDENTIAL_BYTES) continue;
    out.push(name);
  }
  return out;
}

function key(sandboxId: string, instanceId: string): string {
  return `${sandboxId}\0${instanceId}`;
}

function writeAtomic0600(baseDir: string, dir: string, target: string, content: Buffer): void {
  mkdirSync(dir, { recursive: true, mode: 0o700 });
  // mkdir's mode is subject to umask; enforce it
  chmodSync(baseDir, 0o700);
  chmodSync(dir, 0o700);
  const tmp = join(dir, `.${randomBytes(6).toString("hex")}.tmp`);
  const fd = openSync(tmp, "wx", 0o600);
  try {
    writeSync(fd, content);
  } finally {
    closeSync(fd);
  }
  try {
    chmodSync(tmp, 0o600);
    renameSync(tmp, target);
  } catch (err) {
    try {
      unlinkSync(tmp);
    } catch {
      // ignore
    }
    throw err;
  }
}
