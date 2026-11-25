/**
 * Provider status and provider setup across the Hub's chat VMs.
 *
 * Every chat VM runs its own T3 server and reports its own provider list, but the app sees one
 * environment with one list. These helpers keep that list from flapping between VMs, and keep
 * provider sign-in (which restarts the provider on the VM that runs it) away from running chats.
 */

/** Shell thread statuses (`OrchestrationV2ShellThreadStatus`) whose run still holds its provider session. */
export const ACTIVE_RUN_STATUSES: ReadonlySet<string> = new Set(["preparing", "queued", "starting", "running", "waiting"]);

/**
 * Setup RPCs that continue a flow already started on one VM (`flowId` / `operationId` live there).
 * They follow the client's sticky VM even when a chat on it is now running.
 */
const FLOW_CONTINUATION_COMMANDS: ReadonlySet<string> = new Set([
  "provider.auth.respond",
  "provider.auth.complete",
  "provider.auth.cancel",
  "provider.install.cancel",
]);

interface ProviderEntry {
  instanceId?: unknown;
  enabled?: unknown;
  installed?: unknown;
  status?: unknown;
  auth?: { status?: unknown } | null;
  models?: unknown;
}

function entryOf(raw: unknown): (ProviderEntry & { instanceId: string }) | null {
  if (!raw || typeof raw !== "object") return null;
  const p = raw as ProviderEntry;
  return typeof p.instanceId === "string" ? (p as ProviderEntry & { instanceId: string }) : null;
}

function modelCount(p: ProviderEntry): number {
  return Array.isArray(p.models) ? p.models.length : 0;
}

/** A VM confirmed this provider works: signed in or ready, with models to pick. */
export function isConfirmedProvider(p: ProviderEntry): boolean {
  return p.enabled !== false && p.installed !== false && modelCount(p) > 0 && (p.auth?.status === "authenticated" || p.status === "ready");
}

/**
 * A report that knows less than a confirmed one: enabled and installed, not reported signed out,
 * not confirmed itself, and either its sign-in is not checked yet (`auth.status: "unknown"`) or it
 * has no models. A fresh VM reports Antigravity like this until a session starts on it.
 */
export function isLessInformedProvider(p: ProviderEntry): boolean {
  if (p.enabled === false || p.installed === false || isConfirmedProvider(p)) return false;
  const auth = p.auth?.status;
  if (auth === "unauthenticated") return false;
  return auth !== "authenticated" || modelCount(p) === 0;
}

/**
 * The latest provider report of each VM, per instance, and the merge that turns one VM's report
 * into the Hub's list.
 *
 * Rule: an entry a VM reports less informed (`isLessInformedProvider`) is replaced by the latest
 * entry of another running VM that confirmed that instance (`isConfirmedProvider`). Everything
 * else passes through as reported, so real changes win: a sign-out (the VM reports
 * `unauthenticated`, and `forgetInstance` drops every voucher), a disable, an install or removal
 * (`installed` flips), a failed sign-in check. A VM never vouches against itself: its own newer
 * report replaces its older one. A VM that goes away (`forgetSandbox`) stops vouching, so the next
 * report wins again.
 */
export class ProviderStatusBook {
  private readonly byVm = new Map<string, Map<string, Record<string, unknown>>>();

  /** Stores a VM's report. A partial list (one instance refreshed) updates only those instances. */
  record(sandboxId: string, providers: unknown[]): void {
    let entries = this.byVm.get(sandboxId);
    if (!entries) {
      entries = new Map();
      this.byVm.set(sandboxId, entries);
    }
    for (const raw of providers) {
      const p = entryOf(raw);
      if (p) entries.set(p.instanceId, raw as Record<string, unknown>);
    }
  }

  /**
   * Records a VM's report and returns it with less informed entries replaced as described above.
   * Returns the same array when nothing is replaced.
   */
  merge(sandboxId: string, providers: unknown[], isLive: (sandboxId: string) => boolean): unknown[] {
    this.record(sandboxId, providers);
    let changed = false;
    const merged = providers.map((raw) => {
      const p = entryOf(raw);
      if (!p || !isLessInformedProvider(p)) return raw;
      const voucher = this.voucherFor(p.instanceId, sandboxId, isLive);
      if (!voucher) return raw;
      changed = true;
      return voucher;
    });
    return changed ? merged : providers;
  }

  private voucherFor(
    instanceId: string,
    reporter: string,
    isLive: (sandboxId: string) => boolean,
  ): Record<string, unknown> | null {
    for (const [sandboxId, entries] of this.byVm) {
      if (sandboxId === reporter || !isLive(sandboxId)) continue;
      const entry = entries.get(instanceId);
      if (entry && isConfirmedProvider(entry)) return entry;
    }
    return null;
  }

  /** A sign-out: no VM vouches for the instance until it reports it again. */
  forgetInstance(instanceId: string): void {
    for (const entries of this.byVm.values()) entries.delete(instanceId);
  }

  forgetSandbox(sandboxId: string): void {
    this.byVm.delete(sandboxId);
  }
}

/** Whether a VM's chats hold a running turn: on one provider instance, or on any. */
export interface VmRunLoad {
  onInstance: boolean;
  any: boolean;
}

export type ProviderSetupRoute =
  | { kind: "vm"; sandboxId: string }
  /** Every running VM has a chat running a turn on this instance: the command must not run. */
  | { kind: "busy" }
  /** No running VM. */
  | { kind: "none" };

/**
 * Picks the VM that runs a client's provider setup for one instance, and keeps it: the auth flow
 * (`subscribe`, `start`, `respond`, `complete`, `cancel`) lives on one VM, so every call of the
 * flow must reach it. Sticky per paired client (`clientId` survives a reconnect) and instance.
 *
 * - A command that can restart the provider (`start`, `logout`, `install.*`...) never goes to a VM
 *   with a chat running a turn on that instance; it moves to another VM, or gets `busy`.
 * - A flow continuation (`respond`, `complete`, `cancel`) follows the sticky VM.
 * - A setup stream (`*.subscribe`) only reads state: it follows the sticky VM, else the best VM,
 *   else any running VM, and never gets `busy`.
 * - Among safe VMs, one with no running turn at all wins, then the oldest.
 */
export class ProviderSetupRouter {
  private readonly sticky = new Map<string, string>();

  private key(clientId: string, instanceId: string): string {
    return `${clientId}\0${instanceId}`;
  }

  stickyVm(clientId: string, instanceId: string): string | undefined {
    return this.sticky.get(this.key(clientId, instanceId));
  }

  bind(clientId: string, instanceId: string, sandboxId: string): void {
    this.sticky.set(this.key(clientId, instanceId), sandboxId);
  }

  /**
   * `running` lists the running VMs, oldest first; `load` says what runs on each. The choice is
   * recorded as the client's sticky VM before it returns.
   */
  route(
    clientId: string,
    instanceId: string,
    tag: string,
    running: string[],
    load: (sandboxId: string) => VmRunLoad,
    isStream: boolean,
  ): ProviderSetupRoute {
    if (running.length === 0) return { kind: "none" };
    const current = this.stickyVm(clientId, instanceId);
    const stickyLive = current !== undefined && running.includes(current);
    if (stickyLive && (isStream || FLOW_CONTINUATION_COMMANDS.has(tag) || !load(current).onInstance)) {
      return { kind: "vm", sandboxId: current };
    }
    const safe = running.filter((id) => !load(id).onInstance);
    const best = safe.find((id) => !load(id).any) ?? safe[0];
    const chosen = best ?? (isStream ? running[0] : undefined);
    if (!chosen) return { kind: "busy" };
    this.bind(clientId, instanceId, chosen);
    return { kind: "vm", sandboxId: chosen };
  }

  forgetSandbox(sandboxId: string): void {
    for (const [k, id] of this.sticky) if (id === sandboxId) this.sticky.delete(k);
  }
}

/** `detail` of the `ProviderSetupError` a setup command gets when every VM runs a turn on that provider. */
export function providerSetupBusyDetail(tag: string): string {
  const action = tag.startsWith("provider.install.")
    ? "Changing this provider's installation now"
    : tag === "provider.auth.logout"
      ? "Signing out now"
      : tag === "provider.auth.removeMethod"
        ? "Removing a sign-in method now"
        : "Signing in now";
  return `${action} would interrupt a running chat: every Boat VM has a chat running a turn on this provider. Try again when one finishes, or from a new chat.`;
}

/** Instance and flow of a `ProviderAuthState` that reports a finished sign-in, else null. */
export function succeededSignIn(value: unknown): { instanceId: string; flowId: string | null } | null {
  if (!value || typeof value !== "object") return null;
  const v = value as { instanceId?: unknown; phase?: unknown; flowId?: unknown };
  if (v.phase !== "succeeded" || typeof v.instanceId !== "string") return null;
  return { instanceId: v.instanceId, flowId: typeof v.flowId === "string" ? v.flowId : null };
}

/**
 * Run load of one shell thread row on a provider instance. A run is active while `status` (or
 * `activityRunStatus`) says so; it uses the instance when the row's `providerInstanceId` or its
 * `modelSelection.instanceId` names it.
 */
export function threadRunLoad(row: Record<string, unknown>, instanceId: string): VmRunLoad {
  const active =
    (typeof row.status === "string" && ACTIVE_RUN_STATUSES.has(row.status)) ||
    (typeof row.activityRunStatus === "string" && row.activityRunStatus.length > 0);
  if (!active) return { onInstance: false, any: false };
  const selection = row.modelSelection as { instanceId?: unknown } | null | undefined;
  return { onInstance: row.providerInstanceId === instanceId || selection?.instanceId === instanceId, any: true };
}
