/**
 * `server.updateSettings` for the Hub: T3 Code's settings patch semantics, and the persistent
 * overlay of the user's settings changes that the Hub replays on every VM.
 *
 * Ported from T3 Code 0.1.13 (`applyServerSettingsPatch` and `applyProviderInstanceMutation` in
 * the server bundle). The wire contract is
 * `server.updateSettings { patch: ServerSettingsPatch, providerInstanceMutation?: ProviderInstanceMutation }`,
 * which succeeds with the full `ServerSettings` and fails with `ServerSettingsError`.
 *
 * The Hub never computes settings that contain a secret. A running VM's reply is authoritative and
 * already redacted by T3. When the Hub applies a patch itself (no VM, or a stale VM snapshot), keys
 * that can carry secrets keep the Hub's current (redacted) value until a VM answers.
 */

export type ProviderInstanceMutation =
  | { operation: "create" | "upsert"; instanceId: string; instance: Record<string, unknown> }
  | { operation: "remove"; instanceId: string };

export interface SettingsUpdateInput {
  patch: Record<string, unknown>;
  providerInstanceMutation?: ProviderInstanceMutation;
}

type InstanceOp = { operation: "upsert"; instance: Record<string, unknown> } | { operation: "remove" };

/**
 * The user's settings changes made through the Hub, compacted so it stays bounded. Applying it is
 * idempotent, so it can be replayed on any VM at any time.
 */
export interface HubSettingsOverlay {
  version: 1;
  /** One composed `ServerSettingsPatch` (without `setProviderModelOrder` and `providerInstances`). */
  patch: Record<string, unknown>;
  /** `setProviderModelOrder` per provider: a patch holds only one. */
  modelOrders: Record<string, string[]>;
  /** Last change per provider instance, as an upsert of the whole instance or a removal. */
  providerInstances: Record<string, InstanceOp>;
  updatedAt: string | null;
}

/** Where a VM keeps its settings; reported in `ServerSettingsError.settingsPath`. */
export const VM_SETTINGS_PATH = "/home/user/.t3/userdata/settings.json";

/** Top-level keys whose values can hold secrets: never applied to the Hub's own copy. */
const SECRET_BEARING_KEYS = new Set([
  "cloudSpeechCredentials",
  "secretProviders",
  "sourceControlHosts",
  "usageLimitSources",
  "textGenerators",
  "deviceHosts",
  "easDeviceHosts",
  "simlockDeviceHosts",
]);
/** Maps of entries by id where `null` deletes an entry (`mergeSettingsEntries`). */
const ENTRY_KEYS = new Set(["usageLimitSources", "textGenerators", "secretProviders", "usagePriceOverrides", "usageModelAliases"]);
/** Keys a patch replaces whole instead of deep-merging. */
const REPLACE_KEYS = new Set([
  "defaultModelSelection",
  "fallbackModelSelection",
  "speechPostProcessingModelSelection",
  "defaultProjectScripts",
  "migrateModelPreferences",
]);
/** Pre-`projectSettingsOverrides` per-project maps; T3 translates them, they are not merged. */
const LEGACY_PROJECT_KEYS = new Set(["projectAgentBrowserAccessOverrides", "projectAutoPullOverrides", "projectScriptOverrides"]);

type Obj = Record<string, unknown>;

function isObject(value: unknown): value is Obj {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/** T3's `deepMerge`: objects merge key by key, anything else replaces; `undefined` is skipped. */
export function deepMerge(current: unknown, patch: unknown): unknown {
  if (!isObject(current) || !isObject(patch)) return patch;
  const next: Obj = { ...current };
  for (const [key, value] of Object.entries(patch)) {
    if (value === undefined) continue;
    const existing = next[key];
    next[key] = isObject(existing) && isObject(value) ? deepMerge(existing, value) : value;
  }
  return next;
}

function mergeEntries(current: unknown, patch: Obj): Obj {
  const next: Obj = { ...(isObject(current) ? current : {}) };
  for (const [id, entry] of Object.entries(patch)) {
    if (entry === null) delete next[id];
    else next[id] = entry;
  }
  return next;
}

const favoriteKey = (entry: unknown) => {
  const e = (entry ?? {}) as { provider?: unknown; model?: unknown };
  return `${String(e.provider)}\u0000${String(e.model)}`;
};

function applyFavoriteChanges(initial: unknown, changes: unknown[]): Obj[] {
  let favorites = Array.isArray(initial) ? ([...initial] as Obj[]) : [];
  for (const change of changes as Array<{ provider: unknown; model: unknown; favorite: unknown }>) {
    const exists = favorites.some((f) => f.provider === change.provider && f.model === change.model);
    if (change.favorite) {
      if (!exists) favorites.push({ provider: change.provider, model: change.model });
    } else {
      favorites = favorites.filter((f) => f.provider !== change.provider || f.model !== change.model);
    }
  }
  return favorites;
}

function applyHiddenChanges(initial: unknown, changes: unknown[]): Obj {
  const updated: Obj = { ...(isObject(initial) ? initial : {}) };
  for (const change of changes as Array<{ provider: string; model: string; hidden: unknown }>) {
    const prefs = (isObject(updated[change.provider]) ? updated[change.provider] : { hiddenModels: [], modelOrder: [] }) as Obj;
    const hiddenModels = (Array.isArray(prefs.hiddenModels) ? prefs.hiddenModels : []).filter((m) => m !== change.model);
    if (change.hidden) hiddenModels.push(change.model);
    updated[change.provider] = { ...prefs, hiddenModels };
  }
  return updated;
}

function applyModelOrder(initial: unknown, order: { provider: string; modelOrder: string[] }): Obj {
  const prefs = isObject(initial) ? initial : {};
  const current = (isObject(prefs[order.provider]) ? prefs[order.provider] : { hiddenModels: [], modelOrder: [] }) as Obj;
  return { ...prefs, [order.provider]: { ...current, modelOrder: [...order.modelOrder] } };
}

/**
 * Keeps an instance's secret-bearing parts (`config`, `environment`) from the Hub's current copy,
 * so a locally applied upsert never puts a secret the user typed into the Hub's settings.
 */
function hubSafeInstance(instance: Obj, previous: unknown): Obj {
  const { config: _config, environment: _environment, ...rest } = instance;
  const prev = isObject(previous) ? previous : {};
  return {
    ...rest,
    ...("config" in prev ? { config: prev.config } : {}),
    ...("environment" in prev ? { environment: prev.environment } : {}),
  };
}

/**
 * Applies a `ServerSettingsPatch` like T3's `applyServerSettingsPatch`, to the Hub's own copy.
 * Secret-bearing keys are skipped, and the derived background-activity, idle-compaction and legacy
 * per-project fields are not recomputed: a VM's reply replaces this copy whenever one answers.
 */
export function applyServerSettingsPatch(current: Obj, patch: Obj): Obj {
  const {
    setModelFavorites,
    setModelsHidden,
    setProviderModelOrder,
    favorites: favoritesPatch,
    providerModelPreferences: prefsPatch,
    migrateModelPreferences,
    worktreeCleanup,
    backgroundActivity,
    providerInstances,
    projectSettingsOverrides,
    textGenerationModelSelection,
    ...rest
  } = patch;
  const migrate = isObject(migrateModelPreferences) ? migrateModelPreferences : {};

  const initialFavorites = favoritesPatch ?? current.favorites ?? migrate.favorites ?? null;
  const favorites = Array.isArray(setModelFavorites) ? applyFavoriteChanges(initialFavorites, setModelFavorites) : initialFavorites;

  let prefs: unknown = prefsPatch ?? current.providerModelPreferences ?? migrate.providerModelPreferences ?? null;
  if (Array.isArray(setModelsHidden) && setModelsHidden.length > 0) prefs = applyHiddenChanges(prefs, setModelsHidden);
  if (isObject(setProviderModelOrder)) prefs = applyModelOrder(prefs, setProviderModelOrder as { provider: string; modelOrder: string[] });

  const merged: Obj = {};
  for (const [key, value] of Object.entries(rest)) {
    if (value === undefined || SECRET_BEARING_KEYS.has(key) || LEGACY_PROJECT_KEYS.has(key)) continue;
    merged[key] = value;
  }
  const next = deepMerge(current, Object.fromEntries(Object.entries(merged).filter(([k]) => !REPLACE_KEYS.has(k) && !ENTRY_KEYS.has(k)))) as Obj;
  for (const [key, value] of Object.entries(merged)) {
    if (REPLACE_KEYS.has(key)) next[key] = value;
    else if (ENTRY_KEYS.has(key) && isObject(value)) next[key] = mergeEntries(current[key], value);
  }
  delete next.migrateModelPreferences;
  next.favorites = favorites;
  next.providerModelPreferences = prefs;

  if (worktreeCleanup !== undefined) {
    const cleanup = worktreeCleanup as { mode?: unknown; rules?: unknown } | null;
    if (cleanup?.mode === "custom") {
      const storage = (isObject(next.storageCleanup) ? next.storageCleanup : {}) as Obj;
      const prev = current.worktreeCleanup as { mode?: unknown; rules?: unknown } | null | undefined;
      next.worktreeCleanup = {
        mode: "custom",
        rules: {
          worktreeKeepWhen: storage.worktreeKeepWhen,
          worktreeAfterDays: storage.worktreeAfterDays,
          worktreeSettledAfterDays: storage.worktreeSettledAfterDays,
          worktreeOnMerge: storage.worktreeOnMerge,
          worktreeOnDelete: storage.worktreeOnDelete,
          worktreeUnchanged: storage.worktreeUnchanged,
          ...(prev?.mode === "custom" && isObject(prev.rules) ? prev.rules : {}),
          ...(isObject(cleanup.rules) ? cleanup.rules : {}),
        },
      };
    } else {
      next.worktreeCleanup = cleanup;
    }
  }
  if (isObject(backgroundActivity)) {
    next.backgroundActivity = {
      ...(deepMerge(current.backgroundActivity ?? {}, backgroundActivity) as Obj),
      ...(backgroundActivity.overrides !== undefined ? { overrides: backgroundActivity.overrides } : {}),
    };
  }
  if (isObject(providerInstances)) {
    const prev = isObject(current.providerInstances) ? current.providerInstances : {};
    next.providerInstances = Object.fromEntries(
      Object.entries(providerInstances).map(([id, inst]) => [id, isObject(inst) ? hubSafeInstance(inst, prev[id]) : inst]),
    );
  }
  if (
    typeof rest.worktreesDirectory === "string" &&
    rest.worktreesDirectory !== current.worktreesDirectory &&
    Array.isArray(current.previousWorktreesDirectories)
  ) {
    const dir = rest.worktreesDirectory;
    const prevDir = current.worktreesDirectory;
    const previous = current.previousWorktreesDirectories as unknown[];
    next.previousWorktreesDirectories = [
      ...previous.filter((d) => d !== dir),
      ...(typeof prevDir === "string" && prevDir !== "" && !previous.includes(prevDir) ? [prevDir] : []),
    ];
  }
  if (isObject(projectSettingsOverrides)) {
    const stored = isObject(current.projectSettingsOverrides) ? current.projectSettingsOverrides : {};
    const out: Obj = { ...stored };
    for (const [projectId, entry] of Object.entries(projectSettingsOverrides)) {
      if (entry === null) {
        delete out[projectId];
        continue;
      }
      const prev = isObject(stored[projectId]) ? (stored[projectId] as Obj) : {};
      // Per-project provider instances can hold secrets: keep the stored ones
      const { providerInstances: _pi, ...fields } = isObject(entry) ? entry : {};
      out[projectId] = { ...fields, ...("providerInstances" in prev ? { providerInstances: prev.providerInstances } : {}) };
    }
    next.projectSettingsOverrides = Object.fromEntries(Object.entries(out).filter(([, e]) => isObject(e) && Object.keys(e).length > 0));
  }
  if (isObject(textGenerationModelSelection)) {
    const sel = textGenerationModelSelection;
    const prev = (isObject(current.textGenerationModelSelection) ? current.textGenerationModelSelection : {}) as Obj;
    const replace = sel.instanceId !== undefined || sel.model !== undefined;
    const options = replace ? sel.options : mergeOptionsById(prev.options, sel.options);
    next.textGenerationModelSelection = {
      instanceId: sel.instanceId ?? prev.instanceId,
      model: sel.model ?? prev.model,
      ...(options !== undefined ? { options } : {}),
    };
  }
  return next;
}

function mergeOptionsById(current: unknown, patch: unknown): unknown[] | undefined {
  if (patch === undefined) return Array.isArray(current) ? [...current] : undefined;
  if (!Array.isArray(patch) || patch.length === 0) return undefined;
  const merged = new Map<unknown, unknown>((Array.isArray(current) ? (current as Obj[]) : []).map((s) => [s.id, s.value]));
  for (const s of patch as Obj[]) merged.set(s.id, s.value);
  return [...merged.entries()].map(([id, value]) => ({ id, value }));
}

/** T3's `applyProviderInstanceMutation`, keeping the Hub copy free of the instance's secrets. */
export function applyProviderInstanceMutation(settings: Obj, mutation: ProviderInstanceMutation): Obj {
  const providerInstances: Obj = { ...(isObject(settings.providerInstances) ? settings.providerInstances : {}) };
  if (mutation.operation === "remove") delete providerInstances[mutation.instanceId];
  else providerInstances[mutation.instanceId] = hubSafeInstance(mutation.instance, providerInstances[mutation.instanceId]);
  return { ...settings, providerInstances };
}

/** Applies one update to the Hub's copy of the settings. */
export function applySettingsUpdate(settings: Obj, input: SettingsUpdateInput): Obj {
  const patched = applyServerSettingsPatch(settings, input.patch);
  return input.providerInstanceMutation ? applyProviderInstanceMutation(patched, input.providerInstanceMutation) : patched;
}

/** True when `settings` looks like a full `ServerSettings` the client can decode. */
export function isFullServerSettings(settings: unknown): settings is Obj {
  return isObject(settings) && isObject(settings.providerInstances);
}

const INSTANCE_ID_PATTERN = /^[a-zA-Z][a-zA-Z0-9_-]{0,63}$/;

/**
 * Reads a `server.updateSettings` payload. On a malformed one, returns the message a T3 server's
 * decoder gives (`Missing key\n  at ["patch"]`), which the Hub sends as a `Die`, as T3 does.
 */
export function parseSettingsUpdate(payload: unknown): { ok: true; value: SettingsUpdateInput } | { ok: false; message: string } {
  if (!isObject(payload)) return { ok: false, message: "Expected object" };
  if (!("patch" in payload)) return { ok: false, message: 'Missing key\n  at ["patch"]' };
  if (!isObject(payload.patch)) return { ok: false, message: 'Expected object\n  at ["patch"]' };
  const value: SettingsUpdateInput = { patch: payload.patch };
  const raw = payload.providerInstanceMutation;
  if (raw !== undefined) {
    const at = '\n  at ["providerInstanceMutation"]';
    if (!isObject(raw)) return { ok: false, message: `Expected object${at}` };
    if (typeof raw.instanceId !== "string" || !INSTANCE_ID_PATTERN.test(raw.instanceId)) {
      return { ok: false, message: `Invalid instanceId${at}["instanceId"]` };
    }
    if (raw.operation === "remove") {
      value.providerInstanceMutation = { operation: "remove", instanceId: raw.instanceId };
    } else if (raw.operation === "create" || raw.operation === "upsert") {
      if (!isObject(raw.instance) || typeof raw.instance.driver !== "string") {
        return { ok: false, message: `Expected a provider instance with a driver${at}["instance"]` };
      }
      value.providerInstanceMutation = { operation: raw.operation, instanceId: raw.instanceId, instance: raw.instance };
    } else {
      return { ok: false, message: `Expected "create" | "upsert" | "remove"${at}["operation"]` };
    }
  }
  return { ok: true, value };
}

/** A log line for an update: key names and the instance id only, never a value. */
export function describeSettingsUpdate(input: SettingsUpdateInput): string {
  const keys = Object.keys(input.patch).filter((k) => input.patch[k] !== undefined);
  const m = input.providerInstanceMutation;
  return `keys=[${keys.join(",")}]${m ? ` mutation=${m.operation}:${m.instanceId}` : ""}`;
}

export function emptySettingsOverlay(): HubSettingsOverlay {
  return { version: 1, patch: {}, modelOrders: {}, providerInstances: {}, updatedAt: null };
}

export function isEmptySettingsOverlay(overlay: HubSettingsOverlay | null | undefined): boolean {
  return (
    !overlay ||
    (Object.keys(overlay.patch).length === 0 &&
      Object.keys(overlay.modelOrders).length === 0 &&
      Object.keys(overlay.providerInstances).length === 0)
  );
}

/** Reads a persisted overlay; anything unrecognised becomes an empty one. */
export function normalizeSettingsOverlay(raw: unknown): HubSettingsOverlay | null {
  if (!isObject(raw) || raw.version !== 1) return null;
  return {
    version: 1,
    patch: isObject(raw.patch) ? raw.patch : {},
    modelOrders: isObject(raw.modelOrders) ? (raw.modelOrders as Record<string, string[]>) : {},
    providerInstances: isObject(raw.providerInstances) ? (raw.providerInstances as Record<string, InstanceOp>) : {},
    updatedAt: typeof raw.updatedAt === "string" ? raw.updatedAt : null,
  };
}

/** Composes a keyed delta list (`setModelFavorites`, `setModelsHidden`): the later change per model wins. */
function composeDeltas(earlier: unknown, later: unknown[]): unknown[] {
  const byKey = new Map<string, unknown>();
  for (const change of Array.isArray(earlier) ? earlier : []) byKey.set(favoriteKey(change), change);
  for (const change of later) {
    byKey.delete(favoriteKey(change));
    byKey.set(favoriteKey(change), change);
  }
  return [...byKey.values()];
}

/**
 * Records one accepted update in the overlay. `current` is the Hub's settings before the update,
 * used only to turn a whole `providerInstances` map into per-instance changes.
 */
export function recordSettingsUpdate(
  overlay: HubSettingsOverlay | null | undefined,
  input: SettingsUpdateInput,
  current: Obj,
  now = new Date().toISOString(),
): HubSettingsOverlay {
  const base = overlay ?? emptySettingsOverlay();
  const acc: Obj = { ...base.patch };
  const modelOrders = { ...base.modelOrders };
  const instances = { ...base.providerInstances };

  for (const [key, value] of Object.entries(input.patch)) {
    if (value === undefined) continue;
    switch (key) {
      case "favorites":
        acc.favorites = value;
        delete acc.setModelFavorites;
        break;
      case "setModelFavorites":
        // Within a patch T3 applies these after `favorites`; handled below
        break;
      case "providerModelPreferences":
        acc.providerModelPreferences = value;
        delete acc.setModelsHidden;
        for (const p of Object.keys(modelOrders)) delete modelOrders[p];
        break;
      case "setModelsHidden":
      case "setProviderModelOrder":
        break;
      case "providerInstances": {
        // A whole map replaces every instance: record each, and remove the ones it leaves out
        const map = isObject(value) ? value : {};
        const existing = new Set([
          ...(isObject(current.providerInstances) ? Object.keys(current.providerInstances) : []),
          ...Object.keys(instances).filter((id) => instances[id]?.operation === "upsert"),
        ]);
        for (const id of existing) if (!(id in map)) instances[id] = { operation: "remove" };
        for (const [id, inst] of Object.entries(map)) if (isObject(inst)) instances[id] = { operation: "upsert", instance: inst };
        break;
      }
      case "sourceControlHosts": {
        const out: Obj = { ...(isObject(acc.sourceControlHosts) ? acc.sourceControlHosts : {}) };
        for (const [kind, fields] of Object.entries(isObject(value) ? value : {})) {
          const saved = isObject(out[kind]) ? (out[kind] as Obj) : {};
          const f = isObject(fields) ? fields : {};
          const tokens = isObject(f.tokens) && Object.keys(f.tokens).length > 0 ? { tokens: { ...(isObject(saved.tokens) ? saved.tokens : {}), ...f.tokens } } : {};
          out[kind] = { ...saved, ...f, ...tokens };
        }
        acc.sourceControlHosts = out;
        break;
      }
      case "projectSettingsOverrides": {
        const out: Obj = { ...(isObject(acc.projectSettingsOverrides) ? acc.projectSettingsOverrides : {}) };
        for (const [projectId, entry] of Object.entries(isObject(value) ? value : {})) {
          const prev = out[projectId];
          out[projectId] =
            isObject(entry) && !("providerInstances" in entry) && isObject(prev) && "providerInstances" in prev
              ? { ...entry, providerInstances: prev.providerInstances }
              : entry;
        }
        acc.projectSettingsOverrides = out;
        break;
      }
      case "textGenerationModelSelection": {
        const sel = isObject(value) ? value : {};
        const prev = isObject(acc.textGenerationModelSelection) ? acc.textGenerationModelSelection : undefined;
        const replaces = sel.instanceId !== undefined || sel.model !== undefined;
        acc.textGenerationModelSelection =
          replaces || !prev ? sel : { ...prev, ...(sel.options !== undefined ? { options: mergeOptionsById(prev.options, sel.options) ?? [] } : {}) };
        break;
      }
      case "worktreeCleanup": {
        const prev = acc.worktreeCleanup as { mode?: unknown; rules?: unknown } | undefined;
        const next = value as { mode?: unknown; rules?: unknown } | null;
        acc.worktreeCleanup =
          next?.mode === "custom" && prev?.mode === "custom"
            ? { mode: "custom", rules: { ...(isObject(prev.rules) ? prev.rules : {}), ...(isObject(next.rules) ? next.rules : {}) } }
            : next;
        break;
      }
      case "backgroundActivity": {
        const prev = acc.backgroundActivity;
        const next = isObject(value) ? value : {};
        acc.backgroundActivity = {
          ...(deepMerge(isObject(prev) ? prev : {}, next) as Obj),
          ...(next.overrides !== undefined ? { overrides: next.overrides } : {}),
        };
        break;
      }
      default:
        if (ENTRY_KEYS.has(key) || LEGACY_PROJECT_KEYS.has(key)) {
          acc[key] = { ...(isObject(acc[key]) ? acc[key] : {}), ...(isObject(value) ? value : {}) };
        } else if (REPLACE_KEYS.has(key)) {
          acc[key] = value;
        } else {
          acc[key] = key in acc ? deepMerge(acc[key], value) : value;
        }
    }
  }
  if (Array.isArray(input.patch.setModelFavorites)) {
    acc.setModelFavorites = composeDeltas(acc.setModelFavorites, input.patch.setModelFavorites);
  }
  if (Array.isArray(input.patch.setModelsHidden)) {
    acc.setModelsHidden = composeDeltas(acc.setModelsHidden, input.patch.setModelsHidden);
  }
  const order = input.patch.setProviderModelOrder;
  if (isObject(order) && typeof order.provider === "string" && Array.isArray(order.modelOrder)) {
    modelOrders[order.provider] = order.modelOrder as string[];
  }
  const m = input.providerInstanceMutation;
  if (m) {
    instances[m.instanceId] = m.operation === "remove" ? { operation: "remove" } : { operation: "upsert", instance: m.instance };
  }
  return { version: 1, patch: acc, modelOrders, providerInstances: instances, updatedAt: now };
}

/** The `server.updateSettings` payloads that replay an overlay, in order. */
export function overlayReplayCalls(overlay: HubSettingsOverlay | null | undefined): SettingsUpdateInput[] {
  if (!overlay || isEmptySettingsOverlay(overlay)) return [];
  const calls: SettingsUpdateInput[] = [];
  if (Object.keys(overlay.patch).length > 0) calls.push({ patch: { ...overlay.patch } });
  for (const [provider, modelOrder] of Object.entries(overlay.modelOrders)) {
    calls.push({ patch: { setProviderModelOrder: { provider, modelOrder } } });
  }
  for (const [instanceId, op] of Object.entries(overlay.providerInstances)) {
    calls.push({
      patch: {},
      providerInstanceMutation:
        op.operation === "remove" ? { operation: "remove", instanceId } : { operation: "upsert", instanceId, instance: op.instance },
    });
  }
  return calls;
}

/** Applies the overlay to a settings snapshot (for example a VM's boot snapshot) for the Hub's copy. */
export function applySettingsOverlay(settings: Obj, overlay: HubSettingsOverlay | null | undefined): Obj {
  return overlayReplayCalls(overlay).reduce((acc, call) => applySettingsUpdate(acc, call), settings);
}

/** A `create` becomes an `upsert` on VMs: the Hub already checked it against its own copy. */
export function forwardableSettingsUpdate(input: SettingsUpdateInput): SettingsUpdateInput {
  const m = input.providerInstanceMutation;
  if (m?.operation !== "create") return input;
  return { ...input, providerInstanceMutation: { ...m, operation: "upsert" } };
}

/** Rewrites a project id wherever it appears, as a value or as a map key (`projectSettingsOverrides`). */
export function rewriteProjectIdsAndKeys(val: unknown, fromId: string | undefined, toId: string | undefined): unknown {
  if (!fromId || !toId || fromId === toId || val === null || val === undefined) return val;
  if (typeof val === "string") return val === fromId ? toId : val;
  if (Array.isArray(val)) return val.map((item) => rewriteProjectIdsAndKeys(item, fromId, toId));
  if (typeof val === "object") {
    const out: Obj = {};
    for (const [k, v] of Object.entries(val as Obj)) out[k === fromId ? toId : k] = rewriteProjectIdsAndKeys(v, fromId, toId);
    return out;
  }
  return val;
}

/**
 * The cached provider status after a provider instance is turned on or off with no VM to ask, as a
 * T3 server reports it right away: `disabled` when off, `warning` (being checked) when back on.
 */
export function providersWithInstanceEnabled(providers: unknown, mutation: ProviderInstanceMutation | undefined): unknown[] | null {
  if (!Array.isArray(providers) || !mutation || mutation.operation === "remove") return null;
  const enabled = mutation.instance.enabled;
  if (typeof enabled !== "boolean") return null;
  let changed = false;
  const next = providers.map((p) => {
    if (!isObject(p) || p.instanceId !== mutation.instanceId || p.enabled === enabled) return p;
    changed = true;
    return { ...p, enabled, status: enabled ? (p.status === "disabled" ? "warning" : p.status) : "disabled" };
  });
  return changed ? next : null;
}
