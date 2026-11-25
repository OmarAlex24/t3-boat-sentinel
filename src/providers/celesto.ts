import { randomBytes, randomUUID } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import type {
  CreateSandboxOptions,
  ExecResult,
  ExposedEndpoint,
  ProviderCapability,
  ResumeResult,
  SandboxInfo,
  SandboxProvider,
} from "./types.ts";

export const CELESTO_DEFAULT_BASE_URL = "https://api.celesto.ai/v1";
export const CELESTO_DEFAULT_TEMPLATE = "coding-agent";
export const CELESTO_DEFAULT_USER = "ohm";

/** Celesto rejects `command` over 10,000 chars (422 string_too_long); keep a margin. */
export const CELESTO_MAX_COMMAND_CHARS = 9_500;
/** Base64 characters appended per upload exec (well under the command limit with the wrapper). */
export const CELESTO_UPLOAD_CHUNK_CHARS = 9_000;
/** Celesto rejects an exec `timeout` above 300 seconds. */
export const CELESTO_MAX_EXEC_TIMEOUT_SECS = 300;

/** Raw Celesto size ids (`size_id`), passed through unchanged. */
const CELESTO_SIZE_IDS = new Set(["nano", "small", "standard", "large"]);
/**
 * Sentinel / Boat size names -> Celesto `size_id`. `standard` is 2 vCPU / 4 GB (Boat `default`),
 * `large` is 4 vCPU / 16 GB. Celesto has nothing between them, so `medium` rounds up.
 */
const DEFAULT_SIZE_MAP: Record<string, string> = {
  default: "standard",
  medium: "large",
  large: "large",
  xlarge: "large",
};

/** File the provider's `resume` checks; `provision-base.sh` writes it in supervisor mode. */
export const CELESTO_PROVISION_MARKER = "~/.t3-sentinel/provisioned";
const SUPERVISOR_PIDFILE = "~/.t3-sentinel/t3-supervisor.pid";

export interface CelestoProviderOptions {
  apiKey: string;
  /** Directory for the ownership registry (`celesto-computers.json`, 0600). */
  stateDir: string;
  baseUrl?: string;
  templateId?: string;
  /** User T3 and every `exec` run as (Claude Code refuses to skip permissions as root). */
  user?: string;
  /** Extra Sentinel size name -> Celesto `size_id` mappings. */
  sizeMap?: Record<string, string>;
  /** Injected for tests. */
  fetch?: (input: string, init: RequestInit) => Promise<Response>;
  /** Injected for tests. */
  sleep?: (ms: number) => Promise<void>;
  /** Poll interval while waiting for status changes and detached execs. */
  pollIntervalMs?: number;
}

interface RegistryEntry {
  idempotencyKey: string;
  /** Sentinel name (e.g. `t3s-hub-abc-xyz`); Celesto ignores `name`, so the reaper matches on this. */
  intendedName: string;
  createdAt: string;
  id?: string;
  /** Recovery POSTs made from `list()` for an entry whose create result was never seen. */
  recoveryAttempts?: number;
}

interface CelestoComputer {
  id: string;
  name?: string;
  status?: string;
  stopped_at?: string | null;
  last_error?: unknown;
}

class CelestoHttpError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
  }
}

/**
 * Celesto (`celesto.ai`) cloud computers over the REST API with plain `fetch`.
 *
 * - No snapshots: every chat VM installs T3 at boot (`provision-base.sh` supervisor mode, since
 *   the image has no systemd).
 * - `pause` stops the computer; disk and running processes survive `start`.
 * - Published ports are public: T3's bearer auth is the only gate.
 * - Celesto ignores computer names and labels, so ownership lives in a local registry and
 *   `list()` returns only computers this Sentinel created, under their Sentinel names.
 * - No `download`: file contents would have to travel through exec output.
 */
export class CelestoProvider implements SandboxProvider {
  readonly name = "celesto";
  readonly capabilities: ReadonlySet<ProviderCapability> = new Set<ProviderCapability>(["pause"]);

  private readonly apiKey: string;
  private readonly baseUrl: string;
  private readonly templateId: string;
  readonly user: string;
  readonly home: string;
  private readonly sizeMap: Record<string, string>;
  private readonly registryPath: string;
  private readonly fetchImpl: (input: string, init: RequestInit) => Promise<Response>;
  private readonly sleep: (ms: number) => Promise<void>;
  private readonly pollMs: number;
  private warnedPrivate = false;
  /** Serializes registry read-modify-write cycles inside this process. */
  private registryLock: Promise<unknown> = Promise.resolve();
  /** Idempotency keys with a create call in progress (never recovered from `list()`). */
  private readonly creating = new Set<string>();

  constructor(opts: CelestoProviderOptions) {
    if (!opts.apiKey || opts.apiKey.trim().length === 0) {
      throw new Error("Celesto provider needs an API key: set provider.apiKey or the CELESTO_API_KEY environment variable");
    }
    this.apiKey = opts.apiKey.trim();
    this.baseUrl = (opts.baseUrl ?? CELESTO_DEFAULT_BASE_URL).replace(/\/+$/, "");
    this.templateId = opts.templateId ?? CELESTO_DEFAULT_TEMPLATE;
    this.user = opts.user ?? CELESTO_DEFAULT_USER;
    if (!/^[a-z_][a-z0-9_-]{0,31}$/.test(this.user)) {
      throw new Error(`Invalid Celesto run user "${this.user}"`);
    }
    this.home = this.user === "root" ? "/root" : `/home/${this.user}`;
    this.sizeMap = { ...DEFAULT_SIZE_MAP, ...(opts.sizeMap ?? {}) };
    this.registryPath = join(opts.stateDir, "celesto-computers.json");
    this.fetchImpl = opts.fetch ?? ((input, init) => fetch(input, init));
    this.sleep = opts.sleep ?? ((ms) => Bun.sleep(ms));
    this.pollMs = opts.pollIntervalMs ?? 2_000;
  }

  // ---------------------------------------------------------------- HTTP

  private async request<T>(
    method: string,
    path: string,
    opts: { body?: unknown; headers?: Record<string, string>; timeoutMs?: number; redactBody?: boolean } = {},
  ): Promise<{ status: number; data: T }> {
    const timeoutMs = opts.timeoutMs ?? 30_000;
    const init: RequestInit & { timeout?: number | boolean } = {
      method,
      headers: {
        Authorization: `Bearer ${this.apiKey}`,
        Accept: "application/json",
        ...(opts.body !== undefined ? { "Content-Type": "application/json" } : {}),
        ...(opts.headers ?? {}),
      },
      ...(opts.body !== undefined ? { body: JSON.stringify(opts.body) } : {}),
      signal: AbortSignal.timeout(timeoutMs),
      // Bun's socket idle timeout defaults to 5 minutes; a 300 s exec can sit silent that long.
      timeout: false,
    };
    let res: Response;
    try {
      res = await this.fetchImpl(`${this.baseUrl}${path}`, init);
    } catch (err) {
      const reason = err instanceof Error ? err.name === "TimeoutError" ? "timed out" : err.message : String(err);
      // Never include the request body: exec commands and upload chunks can carry boot secrets
      throw new Error(`celesto ${method} ${path} failed: ${reason}`);
    }
    const text = await res.text();
    let data: unknown = undefined;
    if (text) {
      try {
        data = JSON.parse(text);
      } catch {
        data = undefined;
      }
    }
    if (!res.ok) {
      const detail = describeError(data, opts.redactBody ? "" : text);
      throw new CelestoHttpError(`celesto ${method} ${path} failed: HTTP ${res.status}${detail ? ` (${detail})` : ""}`, res.status);
    }
    return { status: res.status, data: data as T };
  }

  // ------------------------------------------------------------ Registry

  private readRegistry(): RegistryEntry[] {
    if (!existsSync(this.registryPath)) return [];
    // Throw on a corrupt or unreadable registry: returning [] would make the reaper treat every
    // owned computer as gone and wipe live slots.
    const raw = readFileSync(this.registryPath, "utf8");
    const parsed = JSON.parse(raw) as { computers?: RegistryEntry[] };
    if (!parsed || !Array.isArray(parsed.computers)) {
      throw new Error(`Celesto registry ${this.registryPath} is malformed`);
    }
    return parsed.computers;
  }

  private writeRegistry(entries: RegistryEntry[]): void {
    mkdirSync(dirname(this.registryPath), { recursive: true });
    const tmp = `${this.registryPath}.${process.pid}.${randomBytes(4).toString("hex")}.tmp`;
    writeFileSync(tmp, JSON.stringify({ computers: entries }, null, 2), { mode: 0o600 });
    renameSync(tmp, this.registryPath);
  }

  private mutateRegistry<T>(fn: (entries: RegistryEntry[]) => T): Promise<T> {
    const run = this.registryLock.then(() => {
      const entries = this.readRegistry();
      const out = fn(entries);
      this.writeRegistry(entries);
      return out;
    });
    this.registryLock = run.catch(() => {});
    return run;
  }

  /** Registry snapshot (for tests and diagnostics). */
  registryEntries(): RegistryEntry[] {
    return this.readRegistry().map((e) => ({ ...e }));
  }

  // --------------------------------------------------------- Lifecycle

  resolveSize(size: string): string {
    if (CELESTO_SIZE_IDS.has(size)) return size;
    return this.sizeMap[size] ?? size;
  }

  async create(opts: CreateSandboxOptions): Promise<string> {
    if (opts.fromSnapshot) {
      throw new Error("Celesto provider does not support snapshots");
    }
    const idempotencyKey = randomUUID();
    // Recorded BEFORE the POST so a crash mid-create still leaves a key to recover the id with
    await this.mutateRegistry((entries) => {
      entries.push({ idempotencyKey, intendedName: opts.name, createdAt: new Date().toISOString() });
    });
    this.creating.add(idempotencyKey);
    let id: string;
    try {
      id = await this.postCreate(idempotencyKey, this.resolveSize(opts.size));
    } catch (err) {
      if (err instanceof CelestoHttpError && err.status >= 400 && err.status < 500 && err.status !== 408 && err.status !== 429) {
        // Definitive rejection: nothing was created
        await this.mutateRegistry((entries) => {
          const i = entries.findIndex((e) => e.idempotencyKey === idempotencyKey);
          if (i >= 0) entries.splice(i, 1);
        });
      }
      // Otherwise the result is unknown; the entry stays so `list()` can recover the id later
      throw err;
    } finally {
      this.creating.delete(idempotencyKey);
    }
    await this.waitForStatus(id, ["running"], 180_000, `start ${opts.name}`);
    return id;
  }

  /** POST /computers, retrying transient failures with the SAME key, and records the id. */
  private async postCreate(idempotencyKey: string, sizeId: string, attempts = 4): Promise<string> {
    let lastErr: unknown;
    for (let attempt = 1; attempt <= attempts; attempt++) {
      try {
        const res = await this.request<CelestoComputer>("POST", "/computers", {
          body: { template_id: this.templateId, size_id: sizeId },
          headers: { "Idempotency-Key": idempotencyKey },
          timeoutMs: 60_000,
        });
        const id = res.data?.id;
        if (!id) throw new Error("celesto POST /computers returned no id");
        await this.mutateRegistry((entries) => {
          const entry = entries.find((e) => e.idempotencyKey === idempotencyKey);
          if (entry) entry.id = id;
        });
        return id;
      } catch (err) {
        lastErr = err;
        const status = err instanceof CelestoHttpError ? err.status : 0;
        const transient = status === 0 || status === 408 || status === 429 || status >= 500;
        if (!transient || attempt === attempts) break;
        await this.sleep(Math.min(1_000 * 2 ** (attempt - 1), 8_000));
      }
    }
    throw lastErr;
  }

  async info(id: string): Promise<SandboxInfo> {
    const res = await this.request<CelestoComputer>("GET", `/computers/${encodeURIComponent(id)}`);
    const entry = this.readRegistry().find((e) => e.id === id);
    return toInfo(res.data, entry?.intendedName);
  }

  /** Only computers this Sentinel created, named with their Sentinel names. */
  async list(): Promise<SandboxInfo[]> {
    await this.recoverPendingCreates();
    const entries = this.readRegistry().filter((e) => e.id);
    if (entries.length === 0) return [];
    const res = await this.request<{ computers?: CelestoComputer[] }>("GET", "/computers");
    const remote = new Map((res.data?.computers ?? []).map((c) => [c.id, c]));
    const out: SandboxInfo[] = [];
    const gone: string[] = [];
    for (const entry of entries) {
      let computer = remote.get(entry.id!);
      if (!computer) {
        // Not in the listing (it may paginate): confirm with a direct read before calling it gone
        try {
          computer = (await this.request<CelestoComputer>("GET", `/computers/${encodeURIComponent(entry.id!)}`)).data;
        } catch (err) {
          if (err instanceof CelestoHttpError && err.status === 404) {
            gone.push(entry.id!);
            continue;
          }
          throw err;
        }
      }
      if (computer.status === "deleted") {
        gone.push(entry.id!);
        continue;
      }
      out.push(toInfo(computer, entry.intendedName));
    }
    if (gone.length > 0) {
      await this.mutateRegistry((all) => {
        for (let i = all.length - 1; i >= 0; i--) {
          if (all[i].id && gone.includes(all[i].id!)) all.splice(i, 1);
        }
      });
    }
    return out;
  }

  /**
   * Entries whose create result was never seen (crash or network failure after the POST):
   * replay the POST with the same key to learn the id. The computer then shows up in `list()`
   * under its Sentinel name and, owned by no slot, is reaped as an orphan.
   */
  private async recoverPendingCreates(): Promise<void> {
    const stale = this.readRegistry().filter(
      (e) => !e.id && !this.creating.has(e.idempotencyKey) && Date.now() - Date.parse(e.createdAt) > 60_000,
    );
    for (const entry of stale) {
      if ((entry.recoveryAttempts ?? 0) >= 3) {
        console.warn(`[celesto] giving up on recovering create for ${entry.intendedName}`);
        await this.mutateRegistry((all) => {
          const i = all.findIndex((e) => e.idempotencyKey === entry.idempotencyKey);
          if (i >= 0) all.splice(i, 1);
        });
        continue;
      }
      await this.mutateRegistry((all) => {
        const e = all.find((x) => x.idempotencyKey === entry.idempotencyKey);
        if (e) e.recoveryAttempts = (e.recoveryAttempts ?? 0) + 1;
      });
      try {
        await this.postCreate(entry.idempotencyKey, this.resolveSize("default"), 1);
        console.warn(`[celesto] recovered computer for interrupted create of ${entry.intendedName}`);
      } catch (err) {
        if (err instanceof CelestoHttpError && err.status >= 400 && err.status < 500 && err.status !== 429) {
          await this.mutateRegistry((all) => {
            const i = all.findIndex((e) => e.idempotencyKey === entry.idempotencyKey);
            if (i >= 0) all.splice(i, 1);
          });
        }
      }
    }
  }

  async delete(id: string): Promise<void> {
    let conflicts = 0;
    const maxConflicts = this.pollsFor(90_000);
    for (;;) {
      try {
        await this.request("DELETE", `/computers/${encodeURIComponent(id)}`);
        break;
      } catch (err) {
        if (err instanceof CelestoHttpError && err.status === 404) break;
        // 409 while the computer is still `creating` (or mid-transition): retry
        if (err instanceof CelestoHttpError && err.status === 409 && conflicts++ < maxConflicts) {
          await this.sleep(this.pollMs);
          continue;
        }
        throw err;
      }
    }
    // Drop the registry entry once the computer is gone; if it lingers in `deleting`, `list()` cleans up later
    for (let poll = 0; poll < this.pollsFor(60_000); poll++) {
      let status: string | undefined;
      try {
        status = (await this.request<CelestoComputer>("GET", `/computers/${encodeURIComponent(id)}`)).data?.status;
      } catch (err) {
        if (err instanceof CelestoHttpError && err.status === 404) status = "deleted";
        else break;
      }
      if (status === "deleted") {
        await this.mutateRegistry((entries) => {
          const i = entries.findIndex((e) => e.id === id);
          if (i >= 0) entries.splice(i, 1);
        });
        return;
      }
      await this.sleep(this.pollMs);
    }
  }

  async pause(id: string): Promise<void> {
    const current = (await this.request<CelestoComputer>("GET", `/computers/${encodeURIComponent(id)}`)).data?.status;
    if (current !== "stopped" && current !== "stopping") {
      await this.request("POST", `/computers/${encodeURIComponent(id)}/stop`, { timeoutMs: 60_000 });
    }
    await this.waitForStatus(id, ["stopped"], 180_000, `stop ${id}`);
  }

  async resume(id: string, _size: string): Promise<ResumeResult> {
    let status: string | undefined;
    try {
      status = (await this.request<CelestoComputer>("GET", `/computers/${encodeURIComponent(id)}`)).data?.status;
    } catch (err) {
      if (err instanceof CelestoHttpError && err.status === 404) return "data_expired";
      throw err;
    }
    if (status === "deleted" || status === "deleting" || status === "error") return "data_expired";
    if (status !== "running") {
      if (status === "stopping") await this.waitForStatus(id, ["stopped"], 180_000, `stop ${id}`);
      if (status !== "starting" && status !== "restoring") {
        await this.request("POST", `/computers/${encodeURIComponent(id)}/start`, { timeoutMs: 60_000 });
      }
      await this.waitForStatus(id, ["running"], 300_000, `start ${id}`);
    }
    // Resumable only when this VM was provisioned and its T3 supervisor survived the stop
    const check = await this.exec(
      id,
      `test -f ${CELESTO_PROVISION_MARKER} && pid="$(cat ${SUPERVISOR_PIDFILE} 2>/dev/null)" && [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null`,
      30,
    );
    return check.exitCode === 0 ? "resumed" : "data_expired";
  }

  /** Poll count covering `ms` (counted, not clock-based, so tests with a fake sleep stay fast). */
  private pollsFor(ms: number, pollMs = this.pollMs): number {
    return Math.max(1, Math.ceil(ms / Math.max(1, pollMs)));
  }

  private async waitForStatus(id: string, want: string[], timeoutMs: number, what: string): Promise<void> {
    const maxPolls = this.pollsFor(timeoutMs);
    let last: string | undefined;
    for (let poll = 0; ; poll++) {
      const c = (await this.request<CelestoComputer>("GET", `/computers/${encodeURIComponent(id)}`)).data;
      last = c?.status;
      if (last && want.includes(last)) return;
      if (last === "error" || last === "deleted" || last === "deleting") {
        throw new Error(`celesto computer ${id} is ${last} (waiting to ${what})`);
      }
      if (poll >= maxPolls) break;
      await this.sleep(this.pollMs);
    }
    throw new Error(`Timed out waiting for celesto computer ${id} to ${what} (last status ${last ?? "unknown"})`);
  }

  async exposePort(id: string, port: number, opts?: { visibility?: "private" | "public" }): Promise<ExposedEndpoint> {
    if (opts?.visibility === "private" && !this.warnedPrivate) {
      this.warnedPrivate = true;
      console.warn("[celesto] published ports are public; T3's bearer auth is the only gate");
    }
    const path = `/computers/${encodeURIComponent(id)}/published-ports`;
    const findExisting = async (): Promise<string | null> => {
      const res = await this.request<unknown>("GET", path);
      const items = Array.isArray(res.data)
        ? res.data
        : ((res.data as { published_ports?: unknown[]; ports?: unknown[] })?.published_ports ??
          (res.data as { ports?: unknown[] })?.ports ??
          []);
      const hit = (items as Array<{ port?: number; url?: string }>).find((p) => Number(p?.port) === port && p?.url);
      return hit?.url ?? null;
    };
    let url = await findExisting();
    if (!url) {
      try {
        const res = await this.request<{ url?: string }>("POST", path, { body: { port } });
        url = res.data?.url ?? null;
      } catch (err) {
        if (!(err instanceof CelestoHttpError && err.status === 409)) throw err;
      }
      url ??= await findExisting();
    }
    if (!url) throw new Error(`celesto published no URL for port ${port} on ${id}`);
    return { url: url.replace(/\/+$/, "") };
  }

  // -------------------------------------------------------------- Exec

  /** Wraps a script to run as the configured user in a bash login shell from its home (like `boat exec`). */
  private asUser(script: string): string {
    if (this.user === "root") return `cd /root && exec bash -lc ${shellQuote(script)}`;
    return `cd ${this.home} && exec sudo -n -u ${this.user} -H bash -lc ${shellQuote(script)}`;
  }

  private async rawExec(id: string, command: string, timeoutSecs: number): Promise<ExecResult> {
    if (command.length >= 10_000) {
      throw new Error(`internal: celesto exec command too long (${command.length} chars)`);
    }
    const timeout = Math.max(1, Math.min(CELESTO_MAX_EXEC_TIMEOUT_SECS, Math.ceil(timeoutSecs)));
    const res = await this.request<{ exit_code?: number | null; stdout?: string; stderr?: string }>(
      "POST",
      `/computers/${encodeURIComponent(id)}/exec`,
      { body: { command, timeout }, timeoutMs: (timeout + 60) * 1000, redactBody: true },
    );
    return {
      exitCode: res.data?.exit_code ?? null,
      stdout: res.data?.stdout ?? "",
      stderr: res.data?.stderr ?? "",
    };
  }

  /** Root exec that must succeed (internal plumbing). */
  private async rootOk(id: string, command: string, timeoutSecs = 60): Promise<string> {
    const r = await this.rawExec(id, command, timeoutSecs);
    if (r.exitCode !== 0) {
      throw new Error(`remote command failed (exit ${r.exitCode}): ${(r.stderr.trim() || r.stdout.trim()).slice(-600)}`);
    }
    return r.stdout;
  }

  async exec(id: string, script: string, timeoutSecs = 120): Promise<ExecResult> {
    const wrapped = this.asUser(script);
    if (timeoutSecs <= CELESTO_MAX_EXEC_TIMEOUT_SECS && wrapped.length <= CELESTO_MAX_COMMAND_CHARS) {
      return this.rawExec(id, wrapped, timeoutSecs);
    }
    // Too long for one request, or longer than the API's 300 s cap: stage the script as a file
    const dir = `/tmp/t3s-exec-${randomBytes(8).toString("hex")}`;
    await this.rootOk(id, `umask 077 && mkdir -p ${dir} && chown ${this.user}: ${dir}`);
    try {
      await this.writeChunked(id, `${dir}/script.b64`, Buffer.from(script, "utf8").toString("base64"));
      await this.rootOk(
        id,
        `base64 -d ${dir}/script.b64 > ${dir}/script.sh && rm -f ${dir}/script.b64 && chown ${this.user}: ${dir}/script.sh && chmod 600 ${dir}/script.sh`,
      );
      const run = this.user === "root"
        ? `cd /root && bash -l ${dir}/script.sh`
        : `cd ${this.home} && sudo -n -u ${this.user} -H bash -l ${dir}/script.sh`;
      if (timeoutSecs <= CELESTO_MAX_EXEC_TIMEOUT_SECS) {
        return await this.rawExec(id, run, timeoutSecs);
      }
      return await this.detachedExec(id, dir, run, timeoutSecs);
    } finally {
      await this.rawExec(id, `rm -rf ${dir}`, 30).catch(() => {});
    }
  }

  /** Runs `run` detached (survives the exec call), then polls its rc file until done or the deadline. */
  private async detachedExec(id: string, dir: string, run: string, timeoutSecs: number): Promise<ExecResult> {
    const inner = `${run} >${dir}/stdout 2>${dir}/stderr </dev/null; echo $? > ${dir}/rc.tmp; mv ${dir}/rc.tmp ${dir}/rc`;
    await this.rootOk(
      id,
      `nohup setsid timeout -k 10 ${Math.ceil(timeoutSecs)} bash -c ${shellQuote(inner)} </dev/null >/dev/null 2>&1 &`,
    );
    const pollMs = Math.max(this.pollMs, 3_000);
    const maxPolls = this.pollsFor((timeoutSecs + 30) * 1000, pollMs);
    for (let poll = 0; ; poll++) {
      const probe = await this.rawExec(id, `cat ${dir}/rc 2>/dev/null`, 30);
      const rcText = probe.stdout.trim();
      if (probe.exitCode === 0 && /^\d+$/.test(rcText)) {
        // Output comes back in pieces that fit a response comfortably
        const stdout = await this.rootOk(id, `tail -c 1048576 ${dir}/stdout 2>/dev/null || true`, 60);
        const stderr = await this.rootOk(id, `tail -c 65536 ${dir}/stderr 2>/dev/null || true`, 60);
        return { exitCode: Number(rcText), stdout, stderr };
      }
      if (poll >= maxPolls) {
        const stderr = await this.rawExec(id, `tail -c 4096 ${dir}/stderr 2>/dev/null`, 30).then((r) => r.stdout).catch(() => "");
        return { exitCode: null, stdout: "", stderr: `${stderr}\n[celesto] command timed out after ${timeoutSecs}s` };
      }
      await this.sleep(pollMs);
    }
  }

  async execOk(id: string, script: string, timeoutSecs = 120): Promise<string> {
    const r = await this.exec(id, script, timeoutSecs);
    if (r.exitCode !== 0) {
      const detail = (r.stderr.trim() || r.stdout.trim()).slice(-600);
      throw new Error(`remote command failed (exit ${r.exitCode}): ${detail}`);
    }
    return r.stdout;
  }

  /** Appends base64 text to `remoteFile` (as root) in chunks that keep each exec under the limit. */
  private async writeChunked(id: string, remoteFile: string, b64: string): Promise<void> {
    await this.rootOk(id, `: > ${remoteFile} && chmod 600 ${remoteFile}`);
    for (let i = 0; i < b64.length; i += CELESTO_UPLOAD_CHUNK_CHARS) {
      const chunk = b64.slice(i, i + CELESTO_UPLOAD_CHUNK_CHARS);
      await this.rootOk(id, `printf '%s' '${chunk}' >> ${remoteFile}`);
    }
  }

  private resolveRemotePath(remotePath: string): string {
    if (remotePath === "~") return this.home;
    if (remotePath.startsWith("~/")) return `${this.home}/${remotePath.slice(2)}`;
    if (!remotePath.startsWith("/")) return `${this.home}/${remotePath}`;
    return remotePath;
  }

  /**
   * Copies a local file into the computer. The bytes travel base64-encoded inside exec request
   * bodies (Celesto has no file-transfer API), so treat uploads as visible to Celesto.
   */
  async upload(id: string, localPath: string, remotePath: string): Promise<void> {
    const dest = this.resolveRemotePath(remotePath);
    const b64 = readFileSync(localPath).toString("base64");
    const staging = `/tmp/t3s-up-${randomBytes(8).toString("hex")}`;
    await this.rootOk(id, `umask 077 && mkdir -p ${staging}`);
    try {
      await this.writeChunked(id, `${staging}/data.b64`, b64);
      const q = shellQuote(dest);
      const tmp = shellQuote(`${dest}.t3s-up.${randomBytes(4).toString("hex")}`);
      const u = this.user;
      await this.rootOk(
        id,
        [
          `parent="$(dirname ${q})"`,
          // Create missing parents as the run user when it can (keeps ~ owned by it), else as root
          `if [ ! -d "$parent" ]; then sudo -n -u ${u} mkdir -p "$parent" 2>/dev/null || mkdir -p "$parent"; fi`,
          `base64 -d ${staging}/data.b64 > ${tmp}`,
          `chmod 600 ${tmp}`,
          `chown ${u}: ${tmp}`,
          `mv -f ${tmp} ${q}`,
        ].join(" && "),
      );
    } finally {
      await this.rawExec(id, `rm -rf ${staging}`, 30).catch(() => {});
    }
  }
}

function toInfo(c: CelestoComputer, intendedName?: string): SandboxInfo {
  return {
    id: c.id,
    name: intendedName ?? c.name,
    state: normalizeStatus(c.status),
    stoppedAt: c.stopped_at ?? null,
  };
}

/** The reaper acts on "running" and "stopped" only; other statuses pass through as-is. */
function normalizeStatus(status: string | undefined): string {
  if (!status) return "unknown";
  return status;
}

/** Short error detail without echoing request input (422 bodies can repeat the command). */
function describeError(data: unknown, fallbackText: string): string {
  if (data && typeof data === "object") {
    const d = data as Record<string, unknown>;
    const parts: string[] = [];
    const pick = (v: unknown) => (typeof v === "string" ? v.slice(0, 200) : undefined);
    const err = d.error;
    if (typeof err === "string") parts.push(err.slice(0, 200));
    else if (err && typeof err === "object") {
      const e = err as Record<string, unknown>;
      for (const k of ["code", "type", "message"]) {
        const v = pick(e[k]);
        if (v) parts.push(v);
      }
    }
    for (const k of ["code", "message"]) {
      const v = pick(d[k]);
      if (v) parts.push(v);
    }
    if (typeof d.detail === "string") parts.push(d.detail.slice(0, 200));
    else if (Array.isArray(d.detail)) {
      for (const item of d.detail.slice(0, 3)) {
        if (item && typeof item === "object") {
          const it = item as Record<string, unknown>;
          const loc = Array.isArray(it.loc) ? it.loc.join(".") : "";
          parts.push([pick(it.type), loc, pick(it.msg)].filter(Boolean).join(" "));
        }
      }
    }
    if (parts.length > 0) return [...new Set(parts)].join("; ");
    return "";
  }
  return fallbackText.slice(0, 200);
}

function shellQuote(s: string): string {
  return `'${s.replace(/'/g, `'\\''`)}'`;
}
