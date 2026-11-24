import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { randomUUID } from "node:crypto";
import type { ClientCredentialRecord } from "./auth.ts";
import type { SentinelConfig } from "./config.ts";

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

export interface PersistedState {
  version: 2;
  /** Captured real T3 descriptor & session templates from last bake so never-spawned slots serve valid schemas. */
  templateDescriptor: Record<string, unknown> | null;
  templateAuthSession: Record<string, unknown> | null;
  slots: Record<string, SlotRecord>;
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

    return {
      version: 2,
      templateDescriptor: existing.templateDescriptor ?? null,
      templateAuthSession: existing.templateAuthSession ?? null,
      slots,
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
}
