import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { randomUUID } from "node:crypto";
import type { ClientCredentialRecord } from "./auth.ts";
import type { ProjectConfig, SentinelConfig } from "./config.ts";
import { normalizeSettingsOverlay, type HubSettingsOverlay } from "./hub-settings.ts";

export type SlotPhase = "empty" | "provisioning" | "running" | "waking" | "sleeping" | "destroying";

export interface SlotRecord {
  name: string;
  label: string;
  /** Stable UUID environmentId written into Boat's ~/.t3/userdata/environment-id on every spawn. */
  environmentId: string;
  /** Incremented on every fresh VM spawn so clients never reuse stale orchestration cursors. */
  generation: number;
  /** Hashed per-device bearer credentials (never plaintext tokens on disk). */
  clients: ClientCredentialRecord[];
  /** Current Boat VM state for this slot (null when empty). */
  sandboxId: string | null;
  /** Per-sandbox secret for watchdog -> sentinel callbacks (unique per VM, never shared). */
  watchdogSecret: string | null;
  /** Per-sandbox ephemeral token for sentinel's LLM proxy (active ONLY while phase === "running"). */
  llmToken: string | null;
  project: string | null;
  snapshot: string | null;
  size: string;
  upstreamUrl: string | null;
  upstreamHeaders?: Record<string, string> | null;
  upstreamToken: string | null;
  klavAgentName: string | null;
  t3ProjectId: string | null;
  phase: SlotPhase;
  lastError: string | null;
  createdAt: string | null;
  lastActiveAt: string | null;
  lastHeartbeatAt: string | null;
  sleepingSince: string | null;
  descriptor: Record<string, unknown> | null;
  authSession: Record<string, unknown> | null;
}

export interface PendingDelete {
  sandboxId: string;
  klavAgentName?: string | null;
  reason: string;
  queuedAt: string;
}

export interface BakeRecord {
  snapshot: string;
  t3Version: string;
  bakedAt: string;
}

export interface HubSandboxRecord {
  sandboxId: string;
  project: string;
  hubProjectId: string;
  vmProjectId: string;
  snapshot: string;
  size: string;
  upstreamUrl: string;
  upstreamHeaders?: Record<string, string> | null;
  upstreamToken: string;
  watchdogSecret: string;
  llmToken: string | null;
  klavAgentName: string | null;
  threadIds: string[];
  worktreePaths: string[];
  phase: "provisioning" | "running" | "sleeping" | "destroying";
  createdAt: string;
  lastActiveAt: string;
  lastHeartbeatAt: string | null;
  sleepingSince: string | null;
  /** Last failed wake of this sleeping VM; cleared by the next successful wake. Holds no secrets. */
  wakeFailure?: HubWakeFailure | null;
  /**
   * Set while the VM is parked: stopped because every chat on it is settled (none open). A parked VM
   * does not count toward `hub.maxSandboxes`, and reading its chats never wakes it. Cleared by the
   * next successful wake and by an idle sleep.
   */
  parkedAt?: string | null;
}

/** True when the sandbox takes a place under `hub.maxSandboxes`: it exists and is not parked. */
export function countsTowardHubLimit(sb: HubSandboxRecord): boolean {
  if (sb.phase === "destroying") return false;
  return !(sb.phase === "sleeping" && sb.parkedAt);
}

/**
 * A settled chat whose parked VM is gone (Boat drops a stopped VM's disk after 14 days). Its sidebar
 * row and cached history stay on the Hub, read only; it has no sandbox and counts toward nothing.
 */
export interface HubExpiredThreadRecord {
  threadId: string;
  project: string;
  hubProjectId: string;
  /** The sandbox the chat ran on (deleted). */
  sandboxId: string;
  parkedAt: string | null;
  expiredAt: string;
  /** `parked-14-days`, `data-expired` (Boat dropped it early) or `gone-remotely`. */
  reason: string;
}

export interface HubWakeFailure {
  /** Wake step that failed (`resume`, `klav-forward-cleanup`, `klav-provision`, `provision-vm`, `bridge`...). */
  step: string;
  /** Redacted reason: never a token, the Klav package, `klav-forward env` output or a URL with secrets. */
  reason: string;
  at: string;
  /** Consecutive failed wakes; sets the backoff. */
  attempts: number;
  /** No new wake (no resume, no Klav sandbox) starts before this time. */
  retryAfter: string;
  /** False when the VM was resumed and could not be paused again; the Hub retries the pause. */
  vmPaused: boolean;
}

export interface HubProjectTemplate {
  shell?: Record<string, unknown>;
  /** `vcs.listRefs` for the project's workdir, from a VM's own T3 (a chat VM, or the project's bake). */
  vcsRefs?: unknown;
  /** The repo's default branch (`origin/HEAD`), recorded at bake time or read from `vcsRefs`. */
  defaultBranch?: string;
}

/** The last `server.updateServer` round that rebaked every golden snapshot (base + each project's own). */
export interface HubSelfUpdateRecord {
  version: string;
  channel: "stable" | "nightly";
  completedAt: string;
  /** `bakedAt` of each bake target's snapshot when the round finished; a later rebake of any target invalidates it. */
  bakes: Record<string, string>;
}

/**
 * A chat the Hub already answered while its VM is still booting (or failed to boot). Kept so the
 * chat survives a Hub restart: a booting launch is started again, a failed one stays in the sidebar
 * with its error until the user archives or deletes it.
 */
export interface HubPendingLaunchRecord {
  threadId: string;
  projectKey: string;
  hubProjectId: string;
  /** `launch`: `orchestration.launchThread`; `create`: an `orchestration.dispatchCommand` `thread.create`. */
  kind: "launch" | "create";
  /** The client's request payload, with Hub project ids. May hold the user's first message. */
  payload: Record<string, unknown>;
  shell: Record<string, unknown>;
  projection: Record<string, unknown>;
  phase: "booting" | "failed";
  error: string | null;
  createdAt: string;
}

/**
 * Messages a chat got while its VM was asleep or waking, held by the Hub (shown as queued runs) and
 * sent to the VM in order once it is up. Kept so a Hub restart resumes the wake and the delivery.
 */
export interface HubHeldTurnsRecord {
  threadId: string;
  sandboxId: string;
  /** Oldest first: the client's `message.dispatch` payloads (Hub project ids) and the runs the Hub shows. */
  turns: Array<{
    runId: string;
    payload: Record<string, unknown>;
    requestedAt: string;
    status: "queued" | "failed";
  }>;
  /** The redacted reason of the wake that failed them, if any. */
  error: string | null;
  createdAt: string;
}

export interface HubStateRecord {
  environmentId: string;
  label: string;
  sequence: number;
  clients: ClientCredentialRecord[];
  /** Permanent UUID per configured project key (e.g. "horizon-stack" -> "<uuid>"). */
  projectIds: Record<string, string>;
  /** Active Boat sandboxes managed by the Hub, keyed by sandboxId. */
  sandboxes: Record<string, HubSandboxRecord>;
  /** Cached real T3 templates captured from live `t3 serve` (bake or first VM boot). */
  serverConfig: Record<string, unknown> | null;
  lifecycleEvents: unknown[] | null;
  projectTemplates: Record<string, HubProjectTemplate>;
  /** Written only after a whole self-update round succeeds, so a partial failure is retried. */
  selfUpdate: HubSelfUpdateRecord | null;
  /**
   * The user's settings changes made through the Hub (`server.updateSettings`), replayed on every
   * VM after spawn, resume and reconnect. May hold credentials the user typed; never logged.
   */
  settingsOverlay: HubSettingsOverlay | null;
  /** Chats answered before their VM was ready, by thread id (see `HubPendingLaunchRecord`). */
  pendingLaunches?: Record<string, HubPendingLaunchRecord>;
  /** Read-only settled chats whose parked VM expired, by thread id (see `HubExpiredThreadRecord`). */
  expiredThreads?: Record<string, HubExpiredThreadRecord>;
  /** Messages held for chats whose VM is waking, by thread id (see `HubHeldTurnsRecord`). */
  heldTurns?: Record<string, HubHeldTurnsRecord>;
}

export interface PersistedState {
  version: 2;
  /** Captured real T3 descriptor & session templates from last bake so never-spawned slots serve valid schemas. */
  templateDescriptor: Record<string, unknown> | null;
  templateAuthSession: Record<string, unknown> | null;
  slots: Record<string, SlotRecord>;
  hub?: HubStateRecord;
  /** Projects added dynamically via Dashboard / API / T3 Code `projectClone.start` (merged with sentinel.yaml). */
  dynamicProjects?: Record<string, ProjectConfig>;
  pendingDeletes: PendingDelete[];
  bakes: Record<string, BakeRecord>;
}

export class StateStore {
  private readonly filePath: string;
  private data: PersistedState;

  constructor(stateDir: string, config: SentinelConfig) {
    mkdirSync(stateDir, { recursive: true });
    this.filePath = join(stateDir, "state.json");
    this.data = this.load(config);
    this.save();
  }

  private load(config: SentinelConfig): PersistedState {
    let existing: Partial<PersistedState> = {};
    if (existsSync(this.filePath)) {
      try {
        existing = JSON.parse(readFileSync(this.filePath, "utf8")) as Partial<PersistedState>;
      } catch (err) {
        console.warn(`[state] failed to parse ${this.filePath}, starting fresh:`, err);
      }
    }

    const slots: Record<string, SlotRecord> = {};
    for (const s of config.slots) {
      const prev = existing.slots?.[s.name];
      const phase: SlotPhase =
        prev?.sandboxId && prev.phase !== "empty" && prev.phase !== "destroying"
          ? prev.phase === "provisioning" || prev.phase === "waking"
            ? "sleeping"
            : prev.phase
          : "empty";
      slots[s.name] = {
        name: s.name,
        label: s.label ?? s.name,
        environmentId: prev?.environmentId ?? randomUUID(),
        generation: typeof prev?.generation === "number" ? prev.generation : 0,
        clients: Array.isArray(prev?.clients) ? prev.clients : [],
        sandboxId: prev?.sandboxId ?? null,
        watchdogSecret: prev?.watchdogSecret ?? null,
        llmToken: prev?.llmToken ?? null,
        project: prev?.project ?? null,
        snapshot: prev?.snapshot ?? null,
        size: s.size ?? prev?.size ?? config.boat.defaultSize,
        upstreamUrl: prev?.upstreamUrl ?? null,
        upstreamToken: prev?.upstreamToken ?? null,
        klavAgentName: prev?.klavAgentName ?? null,
        t3ProjectId: prev?.t3ProjectId ?? null,
        phase,
        lastError: prev?.lastError ?? null,
        createdAt: prev?.createdAt ?? null,
        lastActiveAt: prev?.lastActiveAt ?? null,
        lastHeartbeatAt: prev?.lastHeartbeatAt ?? null,
        sleepingSince: prev?.sleepingSince ?? null,
        descriptor: prev?.descriptor ?? null,
        authSession: prev?.authSession ?? null,
      };
    }

    let hub: HubStateRecord | undefined = existing.hub;
    if (config.hub || hub) {
      const prevHub = existing.hub;
      const projectIds: Record<string, string> = { ...(prevHub?.projectIds ?? {}) };
      for (const key of [
        ...Object.keys(existing.dynamicProjects ?? {}),
        ...Object.keys(config.projects),
      ]) {
        if (!projectIds[key]) projectIds[key] = randomUUID();
      }
      const sandboxes: Record<string, HubSandboxRecord> = {};
      for (const [id, sb] of Object.entries(prevHub?.sandboxes ?? {})) {
        if (!sb || sb.phase === "destroying") continue;
        sandboxes[id] = {
          ...sb,
          phase: sb.phase === "provisioning" ? "sleeping" : sb.phase,
          threadIds: Array.isArray(sb.threadIds) ? sb.threadIds : [],
          worktreePaths: Array.isArray(sb.worktreePaths) ? sb.worktreePaths : [],
        };
      }
      hub = {
        environmentId: prevHub?.environmentId ?? randomUUID(),
        label: config.hub?.label ?? prevHub?.label ?? "Boat",
        sequence: typeof prevHub?.sequence === "number" ? prevHub.sequence : 1,
        clients: Array.isArray(prevHub?.clients) ? prevHub.clients : [],
        projectIds,
        sandboxes,
        serverConfig: prevHub?.serverConfig ?? null,
        lifecycleEvents: Array.isArray(prevHub?.lifecycleEvents) ? prevHub.lifecycleEvents : null,
        projectTemplates: prevHub?.projectTemplates ?? {},
        selfUpdate: prevHub?.selfUpdate ?? null,
        settingsOverlay: normalizeSettingsOverlay(prevHub?.settingsOverlay),
        ...(prevHub?.pendingLaunches && Object.keys(prevHub.pendingLaunches).length > 0
          ? { pendingLaunches: prevHub.pendingLaunches }
          : {}),
        ...(prevHub?.expiredThreads && Object.keys(prevHub.expiredThreads).length > 0
          ? { expiredThreads: prevHub.expiredThreads }
          : {}),
        ...(prevHub?.heldTurns && Object.keys(prevHub.heldTurns).length > 0 ? { heldTurns: prevHub.heldTurns } : {}),
      };
    }

    return {
      version: 2,
      templateDescriptor: existing.templateDescriptor ?? null,
      templateAuthSession: existing.templateAuthSession ?? null,
      slots,
      ...(hub ? { hub } : {}),
      dynamicProjects: existing.dynamicProjects ?? {},
      pendingDeletes: Array.isArray(existing.pendingDeletes) ? existing.pendingDeletes : [],
      bakes: existing.bakes ?? {},
    };
  }

  save(): void {
    const tmp = `${this.filePath}.tmp`;
    writeFileSync(tmp, JSON.stringify(this.data, null, 2), { mode: 0o600 });
    renameSync(tmp, this.filePath);
  }

  getSlot(name: string): SlotRecord | undefined {
    return this.data.slots[name];
  }

  listSlots(): SlotRecord[] {
    return Object.values(this.data.slots);
  }

  updateSlot(name: string, fn: (slot: SlotRecord) => void): SlotRecord {
    const slot = this.data.slots[name];
    if (!slot) throw new Error(`Unknown slot "${name}"`);
    fn(slot);
    this.save();
    return slot;
  }

  setTemplates(descriptor: Record<string, unknown> | null, authSession: Record<string, unknown> | null): void {
    if (descriptor) this.data.templateDescriptor = descriptor;
    if (authSession) this.data.templateAuthSession = authSession;
    this.save();
  }

  getTemplates(): { descriptor: Record<string, unknown> | null; authSession: Record<string, unknown> | null } {
    return {
      descriptor: this.data.templateDescriptor,
      authSession: this.data.templateAuthSession,
    };
  }

  queueDelete(sandboxId: string, reason: string, klavAgentName?: string | null): void {
    if (this.data.pendingDeletes.some((d) => d.sandboxId === sandboxId)) return;
    this.data.pendingDeletes.push({
      sandboxId,
      klavAgentName: klavAgentName ?? null,
      reason,
      queuedAt: new Date().toISOString(),
    });
    this.save();
  }

  listPendingDeletes(): PendingDelete[] {
    return [...this.data.pendingDeletes];
  }

  removePendingDelete(sandboxId: string): void {
    const before = this.data.pendingDeletes.length;
    this.data.pendingDeletes = this.data.pendingDeletes.filter((d) => d.sandboxId !== sandboxId);
    if (this.data.pendingDeletes.length !== before) this.save();
  }

  setBake(target: string, record: BakeRecord): void {
    this.data.bakes[target] = record;
    this.save();
  }

  getBake(target: string): BakeRecord | null {
    return this.data.bakes[target] ?? null;
  }

  getHub(): HubStateRecord | undefined {
    return this.data.hub;
  }

  ensureHub(label = "Boat", projectKeys: string[] = []): HubStateRecord {
    if (!this.data.hub) {
      const projectIds: Record<string, string> = {};
      for (const k of projectKeys) projectIds[k] = randomUUID();
      this.data.hub = {
        environmentId: randomUUID(),
        label,
        sequence: 1,
        clients: [],
        projectIds,
        sandboxes: {},
        serverConfig: null,
        lifecycleEvents: null,
        projectTemplates: {},
        selfUpdate: null,
        settingsOverlay: null,
      };
      this.save();
    }
    return this.data.hub;
  }

  updateHub(fn: (hub: HubStateRecord) => void): HubStateRecord {
    const hub = this.ensureHub();
    fn(hub);
    this.save();
    return hub;
  }

  nextHubSequence(): number {
    const hub = this.ensureHub();
    hub.sequence += 1;
    this.save();
    return hub.sequence;
  }

  getHubSandbox(sandboxId: string): HubSandboxRecord | undefined {
    return this.data.hub?.sandboxes[sandboxId];
  }

  listHubSandboxes(): HubSandboxRecord[] {
    return Object.values(this.data.hub?.sandboxes ?? {});
  }

  setHubSandbox(record: HubSandboxRecord): void {
    const hub = this.ensureHub();
    hub.sandboxes[record.sandboxId] = record;
    this.save();
  }

  updateHubSandbox(sandboxId: string, fn: (sb: HubSandboxRecord) => void): HubSandboxRecord | undefined {
    const sb = this.data.hub?.sandboxes[sandboxId];
    if (!sb) return undefined;
    fn(sb);
    this.save();
    return sb;
  }

  removeHubSandbox(sandboxId: string): void {
    if (this.data.hub?.sandboxes[sandboxId]) {
      delete this.data.hub.sandboxes[sandboxId];
      this.save();
    }
  }

  getAllProjects(config: SentinelConfig): Record<string, ProjectConfig> {
    return { ...(this.data.dynamicProjects ?? {}), ...config.projects };
  }

  addDynamicProject(name: string, project: ProjectConfig, hubProjectId?: string): void {
    this.data.dynamicProjects ??= {};
    this.data.dynamicProjects[name] = project;
    if (this.data.hub) {
      this.data.hub.projectIds[name] = hubProjectId ?? this.data.hub.projectIds[name] ?? randomUUID();
    }
    this.save();
  }

  removeDynamicProject(name: string): string | null {
    if (!this.data.dynamicProjects?.[name]) return null;
    delete this.data.dynamicProjects[name];
    const removedHubId = this.data.hub?.projectIds[name] ?? null;
    if (this.data.hub?.projectIds[name]) {
      delete this.data.hub.projectIds[name];
    }
    this.save();
    return removedHubId;
  }
}

export interface AutoProjectInput {
  repo: string;
  name?: string;
  workdir?: string;
  size?: string;
  klavProfiles?: string[];
  klavTtl?: string;
  defaultProvider?: string;
  defaultModel?: string;
}

export function normalizeGitRepo(input: string): { remoteUrl: string; derivedName: string } {
  const raw = input.trim();
  if (!raw) throw new Error("Repository URL or owner/repo is required");
  let remoteUrl = raw;
  if (/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(raw)) {
    remoteUrl = `https://github.com/${raw}.git`;
  }
  const lastSeg = remoteUrl.replace(/\/+$/, "").split(/[/:]/).pop() ?? "repo";
  const derivedName =
    lastSeg
      .replace(/\.git$/i, "")
      .toLowerCase()
      .replace(/[^a-z0-9-]+/g, "-")
      .replace(/^-+|-+$/g, "") || "repo";
  return { remoteUrl, derivedName };
}

/** Home of the `user` account every Boat VM runs T3 as. */
export const BOAT_VM_HOME = "/home/user";

/**
 * The absolute path a project lives at inside its Boat VMs. Clients such as T3's "Add to another machine"
 * send `~/repo`; the workdir is single-quoted into shell commands, where `~` would not expand, so it is
 * expanded here against the VM user's home. Relative paths and other users' homes (`~bob/...`) are refused.
 */
export function normalizeProjectWorkdir(input: string): string {
  const raw = input.trim();
  let path: string;
  if (raw === "~" || raw.startsWith("~/")) {
    path = `${BOAT_VM_HOME}${raw.slice(1)}`;
  } else if (raw.startsWith("~")) {
    throw new Error(`Invalid project folder "${raw}": only ~ (the Boat VM user's home) can be expanded`);
  } else if (raw.startsWith("/")) {
    path = raw;
  } else {
    throw new Error(`Invalid project folder "${raw}": use an absolute path or one under ~/`);
  }
  path = path.replace(/\/{2,}/g, "/").replace(/\/+$/, "");
  if (!path) throw new Error(`Invalid project folder "${raw}": the filesystem root cannot be a project folder`);
  if (path.split("/").some((seg) => seg === "." || seg === "..")) {
    throw new Error(`Invalid project folder "${raw}": "." and ".." are not allowed`);
  }
  return path;
}

export function buildAutoProjectConfig(input: AutoProjectInput): { name: string; project: ProjectConfig } {
  const { remoteUrl, derivedName } = normalizeGitRepo(input.repo);
  const name = (input.name?.trim() || derivedName)
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, "-")
    .replace(/^-+|-+$/g, "");
  if (!name || name === "base") {
    throw new Error(`Invalid project name "${name || input.name}"`);
  }
  const workdir = input.workdir?.trim() ? normalizeProjectWorkdir(input.workdir) : `${BOAT_VM_HOME}/${name}`;
  const qUrl = `'${remoteUrl.replace(/'/g, `'\\''`)}'`;
  const qDir = `'${workdir.replace(/'/g, `'\\''`)}'`;
  const bakeCmd = [
    "set -e",
    `if [ ! -d ${qDir}/.git ]; then git clone ${qUrl} ${qDir}; fi`,
    `cd ${qDir}`,
    "git fetch --all --prune",
    'DEFAULT_BRANCH=$(git symbolic-ref --short HEAD 2>/dev/null || echo main)',
    'git reset --hard "origin/$DEFAULT_BRANCH" || true',
    "if [ -f pnpm-lock.yaml ] && command -v pnpm >/dev/null 2>&1; then pnpm install --frozen-lockfile || pnpm install;",
    "elif ( [ -f bun.lock ] || [ -f bun.lockb ] ) && command -v bun >/dev/null 2>&1; then bun install --frozen-lockfile || bun install;",
    "elif [ -f package-lock.json ] && command -v npm >/dev/null 2>&1; then npm ci || npm install;",
    "elif [ -f Cargo.toml ] && command -v cargo >/dev/null 2>&1; then cargo fetch || true;",
    "fi",
  ].join("\n");
  const bootCmd = `if [ -d ${qDir}/.git ]; then cd ${qDir} && git fetch origin --prune || true; fi`;
  const project: ProjectConfig = {
    snapshot: `env-${name}`,
    repo: remoteUrl,
    workdir,
    bakeCmd,
    bootCmd,
    ...(input.size ? { size: input.size } : {}),
    ...(input.defaultProvider ? { defaultProvider: input.defaultProvider } : {}),
    ...(input.defaultModel ? { defaultModel: input.defaultModel } : {}),
    ...(input.klavProfiles && input.klavProfiles.length > 0
      ? { klav: { profiles: input.klavProfiles, ...(input.klavTtl ? { ttl: input.klavTtl } : {}) } }
      : {}),
  };
  return { name, project };
}
