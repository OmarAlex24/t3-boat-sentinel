import { existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { randomToken } from "./auth.ts";
import {
  asSandboxProvider,
  hasCapability,
  type LegacyBoatClient,
  type SandboxProvider,
} from "./providers/types.ts";
import type { ProjectConfig, SentinelConfig } from "./config.ts";
import type { KlavClient } from "./klav.ts";
import { ProviderSessionStore, STAGED_SESSIONS_DIR } from "./provider-sessions.ts";
import { buildRepoSyncScript, describeRepoSync, parseRepoSyncOutput, type RepoSyncResult } from "./repo-sync.ts";
import type { SlotRecord, StateStore } from "./state.ts";
import { T3ReleaseResolver } from "./t3-release.ts";
import { ensureT3Project, launchT3Thread, type LaunchThreadOptions } from "./t3rpc.ts";
import { templateVcsRefs } from "./vcs-refs.ts";

const SCRIPTS_DIR = join(dirname(fileURLToPath(import.meta.url)), "..", "scripts");
const PROVISION_SCRIPT = join(SCRIPTS_DIR, "provision-base.sh");
const WATCHDOG_SCRIPT = join(SCRIPTS_DIR, "watchdog.mjs");

export interface WatchdogEventPayload {
  slot: string;
  sandboxId: string;
  event: "all-archived" | "idle" | "heartbeat" | "agent-done" | "needs-input";
  detail?: Record<string, unknown>;
}

export interface SpawnOptions {
  slot?: string;
  /** Must be true to destroy and replace an already-occupied slot. Default false. */
  replace?: boolean;
  /** Optional initial thread prompt — lets personal assistants (e.g. Zumith) start a chat immediately. */
  prompt?: string;
  title?: string;
  provider?: string;
  model?: string;
  runtimeMode?: LaunchThreadOptions["runtimeMode"];
}

export interface SpawnResult {
  slot: SlotRecord;
  projectId: string | null;
  threadId: string | null;
  launchError: string | null;
}

export class LifecycleManager {
  /** Single-flight per-slot locks for state transitions. */
  private locks = new Map<string, Promise<unknown>>();
  /** Synchronously reserved slot names so concurrent spawns never race on pickFreeSlot(). */
  private reservedSlots = new Set<string>();
  /** Temporary VM ids owned by active bakes or in-flight Hub spawns so reaperTick() never kills a booting VM. */
  private inFlightBakes = new Set<string>();
  private inFlightSpawns = new Set<string>();
  private reaperTimer: ReturnType<typeof setInterval> | null = null;
  private warnedNoSessionInject = false;
  /** How long a fresh chat VM may spend moving its checkout to the remote before booting anyway. */
  repoSyncTimeoutMs = 40_000;
  private bakeTimer: ReturnType<typeof setInterval> | null = null;

  private readonly boat: SandboxProvider;
  /** Provider sign-ins synced across chat VMs (`t3.persistProviderSessions`). */
  readonly providerSessions: ProviderSessionStore;
  /** Exact T3 versions for providers without snapshots (`t3.version: latest`). */
  private readonly t3Releases: T3ReleaseResolver;

  constructor(
    private readonly config: SentinelConfig,
    private readonly state: StateStore,
    provider: SandboxProvider | LegacyBoatClient,
    private readonly klav?: KlavClient,
    opts: { t3Releases?: T3ReleaseResolver } = {},
  ) {
    this.boat = asSandboxProvider(provider);
    this.providerSessions = new ProviderSessionStore(config, this.boat);
    this.t3Releases = opts.t3Releases ?? new T3ReleaseResolver();
  }

  /**
   * `T3_VERSION` for `provision-base.sh`. Providers with snapshots get `t3.version` unchanged (the
   * baked image already carries T3). Providers without them install T3 on every VM, so `latest`
   * becomes an exact version looked up here once, keeping the VM off the rate-limited GitHub API.
   */
  async provisionT3Version(): Promise<string> {
    const version = this.config.t3.version;
    const supportsSnapshots = hasCapability(this.boat, "snapshots") && typeof this.boat.listSnapshots === "function";
    if (supportsSnapshots || version !== "latest") return version;
    return this.t3Releases.resolve(version, this.config.t3.channel);
  }

  getProvider(): SandboxProvider {
    return this.boat;
  }

  /** Protect a newly created Hub VM from orphan reaping while it is still provisioning. */
  trackInFlightSandbox(sandboxId: string): void {
    this.inFlightSpawns.add(sandboxId);
  }

  untrackInFlightSandbox(sandboxId: string): void {
    this.inFlightSpawns.delete(sandboxId);
  }

  startBackgroundLoops(): void {
    if (!this.reaperTimer) {
      this.reaperTimer = setInterval(() => {
        void this.reaperTick().catch((e) => console.error("[reaper]", e));
      }, this.config.boat.reaperIntervalSecs * 1000);
      setTimeout(() => {
        void this.reaperTick().catch((e) => console.error("[reaper]", e));
      }, 10_000);
    }

    if (!this.bakeTimer) {
      this.bakeTimer = setInterval(() => {
        void this.scheduledBakeTick().catch((e) => console.error("[bake-cron]", e));
      }, 15 * 60_000);
    }
  }

  stopBackgroundLoops(): void {
    if (this.reaperTimer) clearInterval(this.reaperTimer);
    if (this.bakeTimer) clearInterval(this.bakeTimer);
    this.reaperTimer = null;
    this.bakeTimer = null;
    this.providerSessions.stop();
  }

  private withSlotLock<T>(slotName: string, fn: () => Promise<T>): Promise<T> {
    const prev = this.locks.get(slotName) ?? Promise.resolve();
    const next = prev.catch(() => {}).then(fn);
    const tracked = next.finally(() => {
      if (this.locks.get(slotName) === tracked) {
        this.locks.delete(slotName);
      }
    });
    this.locks.set(slotName, tracked);
    return next;
  }

  /** Synchronously reserve a free (or explicitly replaced) slot before any await. */
  private reserveSlot(requestedSlot?: string, replace = false): string {
    if (requestedSlot) {
      const slot = this.state.getSlot(requestedSlot);
      if (!slot) throw new Error(`Unknown slot "${requestedSlot}"`);
      if (this.reservedSlots.has(requestedSlot)) {
        throw new Error(`Slot "${requestedSlot}" is currently being provisioned`);
      }
      if (slot.phase !== "empty" && !replace) {
        throw new Error(
          `Slot "${requestedSlot}" is occupied (${slot.phase}, project=${slot.project ?? "none"}). Pass replace=true or reset it first.`,
        );
      }
      this.reservedSlots.add(requestedSlot);
      return requestedSlot;
    }

    const free = this.state.listSlots().find((s) => s.phase === "empty" && !this.reservedSlots.has(s.name));
    if (!free) {
      const summary = this.state
        .listSlots()
        .map((s) => `${s.name}:${s.phase}(${s.project ?? "-"})`)
        .join(", ");
      throw new Error(`No free slots available [${summary}]. Archive a chat or reset a slot first.`);
    }
    this.reservedSlots.add(free.name);
    return free.name;
  }

  /**
   * Spawn a brand-new disposable Boat VM for `projectName` from its immutable golden snapshot.
   * - Synchronously reserves the slot to prevent concurrent spawns from colliding.
   * - Verifies the immutable snapshot exists (never silently rebuilds `base` on error).
   * - Writes the slot's permanent `environmentId` into `~/.t3/userdata/environment-id` BEFORE `t3` starts.
   * - Provisions a per-sandbox `watchdogSecret` (uploaded via scp) and optional ephemeral Klav agent.
   * - Auto-registers `project.workdir` in T3 and optionally launches the initial prompt (non-fatal if prompt fails).
   */
  async spawn(projectName: string, requestedSlotOrOpts?: string | SpawnOptions): Promise<SpawnResult> {
    const opts: SpawnOptions =
      typeof requestedSlotOrOpts === "string" ? { slot: requestedSlotOrOpts } : (requestedSlotOrOpts ?? {});
    const allProjects = this.state.getAllProjects(this.config);
    const project = allProjects[projectName];
    if (!project) {
      throw new Error(`Unknown project "${projectName}". Available: ${Object.keys(allProjects).join(", ")}`);
    }

    // Reserve slot synchronously BEFORE any async call so concurrent spawns cannot pick the same slot
    const slotName = this.reserveSlot(opts.slot, opts.replace === true);

    try {
      const supportsSnapshots = hasCapability(this.boat, "snapshots") && typeof this.boat.listSnapshots === "function";
      let snapshotToUse: string | undefined = supportsSnapshots ? project.snapshot : undefined;
      let effectiveProject: ProjectConfig = project;
      if (supportsSnapshots) {
        // Fail fast if snapshot is missing (do NOT swallow listing errors!)
        const snapshots = await this.boat.listSnapshots!();
        if (!snapshots.includes(project.snapshot)) {
          const bakeTarget = project.snapshot === this.config.boat.baseSnapshot ? "base" : projectName;
          throw new Error(
            `Golden snapshot "${project.snapshot}" does not exist yet. Run \`sentinel bake ${bakeTarget}\` (or POST /api/bake) first.`,
          );
        }
      } else if (project.bakeCmd) {
        effectiveProject = {
          ...project,
          bootCmd: project.bootCmd ? `${project.bakeCmd}\n${project.bootCmd}` : project.bakeCmd,
        };
      }

      return await this.withSlotLock(slotName, async () => {
        const current = this.state.getSlot(slotName)!;
        if (current.sandboxId) {
          if (!opts.replace) {
            throw new Error(`Slot "${slotName}" is already occupied (${current.phase})`);
          }
          await this.destroyUnlocked(slotName, "replaced-by-spawn");
        }

        const size = project.size ?? current.size ?? this.config.boat.defaultSize;
        const sandboxName = `${this.config.boat.sandboxPrefix}${slotName}-${Date.now().toString(36)}`;
        const perSandboxWatchdogSecret = randomToken(32);
        const perSandboxLlmToken = this.config.llmProxy ? `t3llm_${randomToken(24)}` : null;

        this.state.updateSlot(slotName, (s) => {
          s.phase = "provisioning";
          s.project = projectName;
          s.snapshot = snapshotToUse ?? null;
          s.size = size;
          s.lastError = null;
        });

        let sandboxId: string | null = null;
        let klavAgentName: string | null = null;

        try {
          console.log(
            `[lifecycle] spawning ${slotName} via ${this.boat.name} (snapshot=${snapshotToUse ?? "base-image"}, size=${size})`,
          );
          sandboxId = await this.boat.create({ name: sandboxName, size, fromSnapshot: snapshotToUse });

          if (effectiveProject.klav && this.klav) {
            const prov = await this.klav.provisionForSandbox(this.boat, sandboxId, effectiveProject.klav);
            klavAgentName = prov.agentName;
          }

          await this.provisionRunningVm(
            slotName,
            sandboxId,
            effectiveProject,
            perSandboxWatchdogSecret,
            perSandboxLlmToken,
            true,
            true,
            !supportsSnapshots,
          );

          const endpoint = await this.boat.exposePort(sandboxId, 3773, {
            visibility: this.config.boat.hostVisibility,
          });
          const now = new Date().toISOString();

          const live = this.state.updateSlot(slotName, (s) => {
            s.sandboxId = sandboxId;
            s.watchdogSecret = perSandboxWatchdogSecret;
            s.llmToken = perSandboxLlmToken;
            s.klavAgentName = klavAgentName;
            s.upstreamUrl = endpoint.url;
            s.upstreamHeaders = endpoint.headers ?? null;
            s.generation += 1;
            s.phase = "running";
            s.createdAt = now;
            s.lastActiveAt = now;
            s.lastHeartbeatAt = now;
            s.sleepingSince = null;
            s.lastError = null;
          });

          // Auto-register project via HTTP fallback if CLI `t3 project add` wasn't available
          let projectId: string | null = null;
          if (effectiveProject.registerProject !== false && live.upstreamToken) {
            try {
              projectId = await ensureT3Project(
                endpoint.url,
                live.upstreamToken,
                projectName,
                effectiveProject.workdir,
                endpoint.headers,
              );
              this.state.updateSlot(slotName, (s) => {
                s.t3ProjectId = projectId;
              });
            } catch (err) {
              console.warn(`[lifecycle] non-fatal T3 project auto-register failed for ${slotName}:`, err);
            }
          }

          // Launch initial prompt in its own try/catch so a prompt error NEVER deletes a healthy VM
          let threadId: string | null = null;
          let launchError: string | null = null;
          if (opts.prompt) {
            if (!projectId || !live.upstreamToken) {
              launchError = "Could not register T3 project before launching prompt";
            } else {
              try {
                const launched = await launchT3Thread(
                  endpoint.url,
                  live.upstreamToken,
                  projectId,
                  {
                    prompt: opts.prompt,
                    title: opts.title,
                    provider: opts.provider ?? effectiveProject.defaultProvider,
                    model: opts.model ?? effectiveProject.defaultModel,
                    runtimeMode: opts.runtimeMode,
                  },
                  endpoint.headers,
                );
                threadId = launched.threadId;
              } catch (err) {
                launchError = err instanceof Error ? err.message : String(err);
                console.warn(`[lifecycle] initial prompt launch failed on healthy VM ${sandboxId}:`, launchError);
              }
            }
          }

          console.log(`[lifecycle] ${slotName} ready (sandbox=${sandboxId}, project=${projectName})`);
          return {
            slot: this.state.getSlot(slotName)!,
            projectId,
            threadId,
            launchError,
          };
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          console.error(`[lifecycle] spawn failed for ${slotName}:`, msg);
          if (sandboxId) {
            this.state.queueDelete(sandboxId, "failed-spawn", klavAgentName);
            void this.reaperTick();
          } else if (klavAgentName && this.klav) {
            await this.klav.revokeForSandbox(klavAgentName).catch(() => {});
          }
          this.state.updateSlot(slotName, (s) => {
            s.sandboxId = null;
            s.watchdogSecret = null;
            s.llmToken = null;
            s.klavAgentName = null;
            s.t3ProjectId = null;
            s.upstreamUrl = null;
            s.upstreamToken = null;
            s.phase = "empty";
            s.lastError = msg;
          });
          throw err;
        }
      });
    } finally {
      this.reservedSlots.delete(slotName);
    }
  }

  /** Wake a sleeping slot VM (`boat resume`) and refresh its public URL. */
  async wake(slotName: string): Promise<SlotRecord> {
    const slot = this.state.getSlot(slotName);
    if (!slot) throw new Error(`Unknown slot "${slotName}"`);
    if (slot.phase === "running" && slot.sandboxId && slot.upstreamUrl) return slot;

    return this.withSlotLock(slotName, async () => {
      const current = this.state.getSlot(slotName)!;
      if (current.phase === "running" && current.sandboxId && current.upstreamUrl) return current;
      if (!current.sandboxId) {
        throw new Error(`Slot "${slotName}" is empty (no active sandbox). Spawn a project first.`);
      }

      const sandboxId = current.sandboxId;
      this.state.updateSlot(slotName, (s) => {
        s.phase = "waking";
        s.lastError = null;
      });

      try {
        if (!hasCapability(this.boat, "pause") || !this.boat.resume) {
          throw new Error(`Provider "${this.boat.name}" does not support pause/resume`);
        }
        console.log(`[lifecycle] waking ${slotName} (sandbox=${sandboxId})`);
        const res = await this.boat.resume(sandboxId, current.size);
        if (res === "data_expired") {
          console.warn(`[lifecycle] sandbox ${sandboxId} disk expired (>14d stopped); resetting slot ${slotName}`);
          if (current.klavAgentName && this.klav) {
            await this.klav.revokeForSandbox(current.klavAgentName).catch(() => {});
          }
          this.state.updateSlot(slotName, (s) => {
            s.sandboxId = null;
            s.watchdogSecret = null;
            s.llmToken = null;
            s.klavAgentName = null;
            s.t3ProjectId = null;
            s.upstreamUrl = null;
            s.upstreamToken = null;
            s.project = null;
            s.snapshot = null;
            s.phase = "empty";
            s.sleepingSince = null;
            s.lastError = "Sandbox disk expired after 14 days stopped";
          });
          throw new Error("Sandbox disk expired; slot reset to empty");
        }

        const project = current.project ? this.state.getAllProjects(this.config)[current.project] : undefined;
        let nextKlavAgent = current.klavAgentName;
        if (this.config.klav?.mode === "proxy" && project?.klav && this.klav) {
          if (current.klavAgentName) {
            await this.klav.revokeForSandbox(current.klavAgentName).catch(() => {});
          }
          const prov = await this.klav.provisionForSandbox(this.boat, sandboxId, project.klav);
          nextKlavAgent = prov.agentName;
        }

        if (this.config.t3.autoUpdateOnWake || !current.upstreamToken) {
          const wdSecret = current.watchdogSecret ?? randomToken(32);
          await this.provisionRunningVm(slotName, sandboxId, project, wdSecret, current.llmToken, !current.upstreamToken);
        }

        const endpoint = await this.boat.exposePort(sandboxId, 3773, {
          visibility: this.config.boat.hostVisibility,
        });
        const now = new Date().toISOString();
        const updated = this.state.updateSlot(slotName, (s) => {
          s.upstreamUrl = endpoint.url;
          s.upstreamHeaders = endpoint.headers ?? null;
          s.klavAgentName = nextKlavAgent;
          s.phase = "running";
          s.lastActiveAt = now;
          s.lastHeartbeatAt = now;
          s.sleepingSince = null;
          s.lastError = null;
        });
        console.log(`[lifecycle] ${slotName} awake`);
        return updated;
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        this.state.updateSlot(slotName, (s) => {
          if (s.sandboxId === sandboxId) {
            s.phase = "sleeping";
            s.lastError = msg;
          }
        });
        throw err;
      }
    });
  }

  /** Sentinel-owned stop: pauses the disposable VM without modifying any saved snapshot. */
  async sleep(slotName: string): Promise<SlotRecord> {
    return this.withSlotLock(slotName, async () => {
      const slot = this.state.getSlot(slotName);
      if (!slot) throw new Error(`Unknown slot "${slotName}"`);
      if (!slot.sandboxId) return slot;

      if (!hasCapability(this.boat, "pause") || !this.boat.pause) {
        console.warn(`[lifecycle] provider "${this.boat.name}" does not support pause; leaving ${slotName} running`);
        return slot;
      }

      console.log(`[lifecycle] pausing ${slotName} (sandbox=${slot.sandboxId})`);
      await this.boat.pause(slot.sandboxId);
      return this.state.updateSlot(slotName, (s) => {
        s.phase = "sleeping";
        s.sleepingSince = new Date().toISOString();
      });
    });
  }

  /**
   * Permanently delete the disposable VM behind `slotName`, revoke its ephemeral Klav agent,
   * and return the slot to `empty` while preserving `environmentId` and hashed client credentials.
   * Never touches any immutable golden snapshot!
   */
  async destroy(slotName: string, reason = "manual-reset"): Promise<SlotRecord> {
    return this.withSlotLock(slotName, () => this.destroyUnlocked(slotName, reason));
  }

  private async destroyUnlocked(slotName: string, reason: string): Promise<SlotRecord> {
    const slot = this.state.getSlot(slotName);
    if (!slot) throw new Error(`Unknown slot "${slotName}"`);
    const sandboxId = slot.sandboxId;
    const klavAgentName = slot.klavAgentName;

    if (!sandboxId) {
      return this.state.updateSlot(slotName, (s) => {
        s.phase = "empty";
        s.watchdogSecret = null;
        s.project = null;
        s.snapshot = null;
        s.t3ProjectId = null;
        s.klavAgentName = null;
        s.upstreamUrl = null;
        s.upstreamToken = null;
        s.sleepingSince = null;
      });
    }

    console.log(`[lifecycle] destroying ${slotName} sandbox=${sandboxId} (reason=${reason})`);
    this.state.queueDelete(sandboxId, `${slotName}:${reason}`, klavAgentName);
    this.state.updateSlot(slotName, (s) => {
      s.phase = "destroying";
    });

    if (klavAgentName && this.klav) {
      await this.klav.revokeForSandbox(klavAgentName).catch((err) => {
        console.warn(`[lifecycle] Klav revoke failed for ${klavAgentName} (reaper will retry):`, err);
      });
    }

    try {
      await this.boat.delete(sandboxId);
    } catch (err) {
      console.warn(`[lifecycle] direct delete of ${sandboxId} failed (reaper will retry):`, err);
    }

    const updated = this.state.updateSlot(slotName, (s) => {
      s.sandboxId = null;
      s.watchdogSecret = null;
      s.llmToken = null;
      s.klavAgentName = null;
      s.t3ProjectId = null;
      s.project = null;
      s.snapshot = null;
      s.upstreamUrl = null;
      s.upstreamHeaders = null;
      s.upstreamToken = null;
      s.phase = "empty";
      s.createdAt = null;
      s.lastHeartbeatAt = null;
      s.sleepingSince = null;
      s.lastError = null;
    });

    void this.reaperTick();
    return updated;
  }

  async handleWatchdogEvent(payload: WatchdogEventPayload): Promise<void> {
    const slot = this.state.getSlot(payload.slot);
    if (!slot || slot.sandboxId !== payload.sandboxId) {
      console.log(`[watchdog-cb] ignoring stale event ${payload.event} for ${payload.slot}/${payload.sandboxId}`);
      return;
    }

    switch (payload.event) {
      case "heartbeat":
        this.state.updateSlot(payload.slot, (s) => {
          if (s.sandboxId === payload.sandboxId) {
            s.lastHeartbeatAt = new Date().toISOString();
          }
        });
        break;

      case "all-archived": {
        const dirty = Boolean(payload.detail?.dirty);
        const unpushed = Boolean(payload.detail?.unpushed);
        const gitWarning =
          dirty || unpushed
            ? ` (Warning: ${[dirty && "uncommitted changes", unpushed && "unpushed commits"].filter(Boolean).join(" & ")})`
            : "";

        if (this.config.boat.archiveAction === "stop") {
          console.log(`[watchdog-cb] all chats archived in ${payload.slot} -> pausing VM ${payload.sandboxId}`);
          await this.sleep(payload.slot);
          await this.sendNotify(`T3 Boat: ${slot.label}`, `Chat archived — sandbox paused${gitWarning}.`);
        } else {
          console.log(`[watchdog-cb] all chats archived in ${payload.slot} -> destroying VM ${payload.sandboxId}`);
          await this.destroy(payload.slot, "chat-archived");
          await this.sendNotify(`T3 Boat: ${slot.label}`, `Chat archived — sandbox deleted ($0)${gitWarning}.`);
        }
        break;
      }

      case "idle":
        console.log(`[watchdog-cb] ${payload.slot} reported idle -> sentinel stopping VM ${payload.sandboxId}`);
        await this.sleep(payload.slot);
        break;

      case "agent-done":
        await this.sendNotify(`T3 Boat: ${slot.label}`, `Agent finished turn in ${slot.project ?? slot.name}.`);
        break;

      case "needs-input":
        await this.sendNotify(`T3 Boat: ${slot.label}`, `Agent is waiting for approval or input.`);
        break;
    }
  }

  /**
   * Build or cleanly refresh an immutable golden snapshot (`"base"` or a configured project name).
   * - Always builds `env-*` from `base-t3` so base tooling & T3 updates always propagate cleanly.
   * - Keeps temp VM out of `pendingDeletes` until `finally` so the reaper never kills a live bake.
   * - Runs a live T3 smoke check & captures real descriptor/session templates before wiping `~/.t3/userdata`.
   */
  async bake(
    target: string,
    opts?: { t3Version?: string; t3Channel?: "stable" | "nightly" },
  ): Promise<{ snapshot: string; t3Version: string }> {
    if (!hasCapability(this.boat, "snapshots") || !this.boat.saveSnapshot || !this.boat.listSnapshots) {
      throw new Error(`Provider "${this.boat.name}" does not support named snapshots (bake is unavailable).`);
    }
    const effectiveT3Version = opts?.t3Version?.trim() || this.config.t3.version;
    const effectiveT3Channel = opts?.t3Channel ?? this.config.t3.channel;
    const allProjects = this.state.getAllProjects(this.config);
    const isBase = target === "base" || target === this.config.boat.baseSnapshot;
    const project = isBase ? undefined : allProjects[target];
    if (!isBase && !project) {
      throw new Error(`Unknown bake target "${target}". Use "base" or one of: ${Object.keys(allProjects).join(", ")}`);
    }

    const existingSnapshots = await this.boat.listSnapshots();
    const snapshotName = isBase ? this.config.boat.baseSnapshot : project!.snapshot;
    const size = project?.size ?? this.config.boat.defaultSize;

    // Always build project snapshots from base-t3 so base OS/t3/klav updates always propagate
    let fromSnapshot: string | undefined;
    if (isBase) {
      if (this.config.boat.baseFrom && existingSnapshots.includes(this.config.boat.baseFrom)) {
        fromSnapshot = this.config.boat.baseFrom;
      } else if (existingSnapshots.includes(this.config.boat.baseSnapshot)) {
        fromSnapshot = this.config.boat.baseSnapshot;
      }
    } else if (existingSnapshots.includes(this.config.boat.baseSnapshot)) {
      fromSnapshot = this.config.boat.baseSnapshot;
    }

    const tempName = `${this.config.boat.sandboxPrefix}bake-${target}-${Date.now().toString(36)}`;
    console.log(`[bake] creating temp VM ${tempName} (from=${fromSnapshot ?? "fresh-ubuntu"}) -> target snapshot "${snapshotName}"`);
    const tempId = await this.boat.create({ name: tempName, size, fromSnapshot });
    this.inFlightBakes.add(tempId);

    try {
      await this.boat.execOk(tempId, "mkdir -p ~/.t3-sentinel && chmod 700 ~/.t3-sentinel", 30);
      await this.boat.upload(tempId, PROVISION_SCRIPT, "~/.t3-sentinel/provision-base.sh");
      await this.boat.upload(tempId, WATCHDOG_SCRIPT, "~/.t3-sentinel/watchdog.mjs");

      if (isBase && this.config.klav?.forwardBinaryPath && existsSync(this.config.klav.forwardBinaryPath)) {
        await this.boat.upload(tempId, this.config.klav.forwardBinaryPath, "/tmp/klav-forward-bin");
        await this.boat.execOk(tempId, "sudo install -m 755 -o root -g root /tmp/klav-forward-bin /usr/local/bin/klav-forward && rm -f /tmp/klav-forward-bin", 30);
      }
      if (isBase && this.config.klav?.tailnetUpScriptPath && existsSync(this.config.klav.tailnetUpScriptPath)) {
        await this.boat.upload(tempId, this.config.klav.tailnetUpScriptPath, "/tmp/klav-tailnet-up");
        await this.boat.execOk(tempId, "sudo install -m 755 -o root -g root /tmp/klav-tailnet-up /usr/local/sbin/klav-tailnet-up && rm -f /tmp/klav-tailnet-up", 30);
      }
      if (isBase && this.config.klav?.linuxBinaryPath && existsSync(this.config.klav.linuxBinaryPath)) {
        await this.boat.upload(tempId, this.config.klav.linuxBinaryPath, "/tmp/klav-bin");
        await this.boat.execOk(tempId, "sudo install -m 755 /tmp/klav-bin /usr/local/bin/klav && rm -f /tmp/klav-bin", 30);
      }
      if (isBase && this.config.klav?.skillPath && existsSync(this.config.klav.skillPath)) {
        await this.boat.execOk(tempId, "mkdir -p ~/.claude/skills/klav", 30);
        await this.boat.upload(tempId, this.config.klav.skillPath, "~/.claude/skills/klav/SKILL.md");
      }

      if (isBase && this.config.boat.baseBakeCmd) {
        console.log(`[bake] running baseBakeCmd...`);
        await this.boat.execOk(tempId, this.config.boat.baseBakeCmd, 600);
      }

      if (project?.bakeCmd) {
        console.log(`[bake] running bakeCmd for ${target}...`);
        await this.boat.execOk(tempId, project.bakeCmd, 900);
      }

      // Upload the settings template for every bake so T3 runs with the real providerInstances: base bakes need it
      // for `prepareAcpAgents` / `installProviders`, and project bakes would otherwise re-probe without those
      // instances and leave the provider status caches inherited from base-t3 stale in the new snapshot.
      if (this.config.t3.settingsFile && existsSync(this.config.t3.settingsFile)) {
        await this.boat.upload(tempId, this.config.t3.settingsFile, "~/.t3-sentinel/t3-settings.json");
      }

      // Ensure zero ephemeral Klav keys ever leak into a golden snapshot
      await this.boat.execOk(tempId, "rm -rf ~/.config/klav/agents", 30);

      // Update t3, smoke-test t3 serve, optionally pre-prepare ACP agents & provider runtimes, capture templates, then wipe per-VM userdata
      const bakeEnv = [
        `T3_VERSION=${shellQuote(effectiveT3Version)}`,
        `T3_CHANNEL=${shellQuote(effectiveT3Channel)}`,
        `BAKE_CLEAN=1`,
      ];
      // Project bakes also report the project's refs and default branch (see provision-base.sh)
      if (project) bakeEnv.push(`BAKE_PROJECT_WORKDIR=${shellQuote(project.workdir)}`);
      // Base bakes install the runtimes; project bakes (built from base-t3) only verify them. Either way the
      // script waits until the provider status caches report installed before the snapshot is taken.
      const acpAgents = this.config.t3.prepareAcpAgents ?? [];
      const providers = this.config.t3.installProviders ?? [];
      if (acpAgents.length > 0) {
        const name = isBase ? "T3_PREPARE_ACP_AGENTS" : "T3_VERIFY_ACP_AGENTS";
        bakeEnv.push(`${name}=${shellQuote(acpAgents.join(","))}`);
      }
      if (providers.length > 0) {
        const name = isBase ? "T3_INSTALL_PROVIDERS" : "T3_VERIFY_PROVIDERS";
        bakeEnv.push(`${name}=${shellQuote(providers.join(","))}`);
      }
      const hasProviderWork = acpAgents.length > 0 || providers.length > 0;
      // Verification itself can take up to ~180s (Antigravity validation alone up to 90s)
      const bakeProvisionTimeout = hasProviderWork ? (isBase ? 1200 : 600) : 300;
      const out = await this.boat.execOk(
        tempId,
        `${bakeEnv.join(" ")} bash ~/.t3-sentinel/provision-base.sh`,
        bakeProvisionTimeout,
      );
      const parsed = lastJsonLine<{
        t3Version?: string;
        descriptor?: Record<string, unknown>;
        authSession?: Record<string, unknown>;
        projectShell?: Record<string, unknown>;
        vcsRefs?: unknown;
        defaultBranch?: string;
      }>(out);

      if (parsed.descriptor || parsed.authSession) {
        this.state.setTemplates(parsed.descriptor ?? null, parsed.authSession ?? null);
      }
      // The project as the bake VM's T3 reported it: the Hub's no-VM answers start from these
      if (!isBase && this.state.getHub() && (parsed.projectShell || parsed.vcsRefs || parsed.defaultBranch)) {
        this.state.updateHub((h) => {
          h.projectTemplates[target] = {
            ...(h.projectTemplates[target] ?? {}),
            ...(parsed.projectShell && typeof parsed.projectShell === "object" ? { shell: parsed.projectShell } : {}),
            ...(templateVcsRefs(parsed.vcsRefs) ? { vcsRefs: templateVcsRefs(parsed.vcsRefs)! } : {}),
            ...(parsed.defaultBranch ? { defaultBranch: parsed.defaultBranch } : {}),
          };
        });
      }

      if (hasCapability(this.boat, "pause") && this.boat.pause) {
        await this.boat.pause(tempId);
        // `boat stop` transitions through `archiving` before reaching `stopped`; wait until snapshotable.
        await this.waitUntilStopped(tempId, 180_000);
      }
      console.log(`[bake] saving immutable snapshot "${snapshotName}"...`);
      await this.boat.saveSnapshot(tempId, snapshotName);

      const t3Version = parsed.t3Version ?? "unknown";
      this.state.setBake(target, { snapshot: snapshotName, t3Version, bakedAt: new Date().toISOString() });
      console.log(`[bake] snapshot "${snapshotName}" ready (${t3Version})`);
      return { snapshot: snapshotName, t3Version };
    } finally {
      this.inFlightBakes.delete(tempId);
      this.state.queueDelete(tempId, `bake-temp:${target}`);
      console.log(`[bake] deleting temporary VM ${tempId}`);
      await this.boat.delete(tempId).catch((e) => console.warn(`[bake] cleanup delete deferred to reaper:`, e));
      void this.reaperTick();
    }
  }

  async reaperTick(): Promise<void> {
    let liveList;
    try {
      liveList = await this.boat.list();
    } catch (err) {
      console.warn("[reaper] boat list failed:", err);
      return;
    }

    const liveIds = new Set(liveList.map((s) => s.id));
    const liveById = new Map(liveList.map((s) => [s.id, s]));

    // 1. Drain pending delete queue (skip anything still actively baking!)
    for (const pending of this.state.listPendingDeletes()) {
      if (this.inFlightBakes.has(pending.sandboxId)) continue;
      if (!liveIds.has(pending.sandboxId)) {
        this.state.removePendingDelete(pending.sandboxId);
        console.log(`[reaper] confirmed gone: ${pending.sandboxId}`);
      } else {
        if (pending.klavAgentName && this.klav) {
          await this.klav.revokeForSandbox(pending.klavAgentName).catch(() => {});
        }
        console.log(`[reaper] retrying delete of ${pending.sandboxId} (${pending.reason})`);
        await this.boat.delete(pending.sandboxId).catch((e) => console.warn(`[reaper] delete retry failed:`, e));
      }
    }

    // 2. Reconcile slot states in BOTH directions & enforce optional maxSleepingHours
    const activeIds = new Set<string>();
    for (const hubSb of this.state.listHubSandboxes()) {
      activeIds.add(hubSb.sandboxId);
      const remote = liveById.get(hubSb.sandboxId);
      if (!remote && hubSb.phase !== "provisioning") {
        console.warn(`[reaper] hub sandbox ${hubSb.sandboxId} disappeared remotely -> removing`);
        if (hubSb.klavAgentName && this.klav) {
          await this.klav.revokeForSandbox(hubSb.klavAgentName).catch(() => {});
        }
        this.state.removeHubSandbox(hubSb.sandboxId);
      } else if (remote && remote.state === "stopped" && hubSb.phase === "running") {
        // Stopped outside the Hub (watchdog backstop): end its Klav proxy sandbox too; the wake mints a new one
        // On failure the name stays, so the wake's revoke of a previous sandbox retries it
        const endKlav =
          this.config.klav?.mode === "proxy" &&
          !!this.klav &&
          !!hubSb.klavAgentName &&
          (await this.klav.revokeForSandbox(hubSb.klavAgentName).then(
            () => true,
            () => false,
          ));
        this.state.updateHubSandbox(hubSb.sandboxId, (s) => {
          s.phase = "sleeping";
          s.llmToken = null;
          if (endKlav) s.klavAgentName = null;
          s.sleepingSince = remote.stoppedAt ?? new Date().toISOString();
        });
      }
    }
    const maxSleepMs = this.config.boat.maxSleepingHours > 0 ? this.config.boat.maxSleepingHours * 3600_000 : 0;

    for (const slot of this.state.listSlots()) {
      if (!slot.sandboxId) continue;
      activeIds.add(slot.sandboxId);

      const remote = liveById.get(slot.sandboxId);
      if (!remote && slot.phase !== "provisioning") {
        console.warn(`[reaper] slot ${slot.name} sandbox ${slot.sandboxId} disappeared remotely -> resetting to empty`);
        if (slot.klavAgentName && this.klav) {
          await this.klav.revokeForSandbox(slot.klavAgentName).catch(() => {});
        }
        this.state.updateSlot(slot.name, (s) => {
          s.sandboxId = null;
          s.watchdogSecret = null;
          s.klavAgentName = null;
          s.t3ProjectId = null;
          s.upstreamUrl = null;
          s.upstreamToken = null;
          s.project = null;
          s.snapshot = null;
          s.phase = "empty";
          s.sleepingSince = null;
        });
        continue;
      }

      if (remote && remote.state === "stopped" && slot.phase === "running") {
        this.state.updateSlot(slot.name, (s) => {
          s.phase = "sleeping";
          s.sleepingSince = remote.stoppedAt ?? new Date().toISOString();
        });
      } else if (remote && remote.state === "running" && slot.phase === "sleeping") {
        // Reconcile the reverse direction: if VM is still running despite local "sleeping" state, fix phase!
        this.state.updateSlot(slot.name, (s) => {
          s.phase = "running";
          s.sleepingSince = null;
        });
      }

      if (maxSleepMs > 0 && slot.phase === "sleeping" && slot.sleepingSince) {
        const asleepFor = Date.now() - Date.parse(slot.sleepingSince);
        if (asleepFor > maxSleepMs) {
          console.log(`[reaper] ${slot.name} slept >${this.config.boat.maxSleepingHours}h -> destroying`);
          await this.destroy(slot.name, "max-sleeping-hours");
        }
      }
    }

    // 3. Kill orphan sandboxes matching our prefix (never touch in-flight bakes, in-flight Hub spawns, or active slots)
    for (const sb of liveList) {
      if (!sb.name?.startsWith(this.config.boat.sandboxPrefix)) continue;
      if (activeIds.has(sb.id) || this.inFlightBakes.has(sb.id) || this.inFlightSpawns.has(sb.id)) continue;
      if (this.state.listPendingDeletes().some((d) => d.sandboxId === sb.id)) continue;
      if (this.locks.size > 0 || this.reservedSlots.size > 0 || this.inFlightSpawns.size > 0) continue;
      console.warn(`[reaper] found orphan sentinel sandbox ${sb.id} (${sb.name}) -> queueing delete`);
      this.state.queueDelete(sb.id, "orphan");
      await this.boat.delete(sb.id).catch((e) => console.warn("[reaper] orphan delete failed:", e));
    }
  }

  private async scheduledBakeTick(): Promise<void> {
    const now = new Date();
    const hourUtc = now.getUTCHours();
    const todayKey = now.toISOString().slice(0, 10);

    if (this.config.boat.baseBakeHourUtc !== undefined && this.config.boat.baseBakeHourUtc === hourUtc) {
      const last = this.state.getBake("base")?.bakedAt;
      if (!last || !last.startsWith(todayKey)) {
        console.log("[bake-cron] running scheduled daily bake for base snapshot");
        await this.bake("base");
      }
    }

    for (const [name, proj] of Object.entries(this.state.getAllProjects(this.config))) {
      if (proj.bakeHourUtc === undefined || proj.bakeHourUtc !== hourUtc) continue;
      const last = this.state.getBake(name)?.bakedAt;
      if (last && last.startsWith(todayKey)) continue;
      console.log(`[bake-cron] running scheduled daily bake for "${name}"`);
      await this.bake(name);
    }
  }

  /**
   * Reusable VM provisioning for both Slot mode and Hub mode:
   * uploads scripts, watchdog secrets, ephemeral LLM proxy token, T3 settings, optional files/.env,
   * runs `provision-base.sh`, and returns `{ descriptor, authSession, upstreamToken }`.
   */
  async provisionStandaloneVm(opts: {
    sandboxId: string;
    watchdogSlotKey: string;
    environmentId: string;
    label: string;
    projectName: string;
    project: ProjectConfig | undefined;
    watchdogSecret: string;
    llmToken: string | null;
    mintToken: boolean;
    /**
     * Fresh VMs only: stage stored provider sign-ins (`t3.persistProviderSessions`) so T3 starts
     * signed in. Never set on wake, where the VM's own copy may be newer.
     */
    injectProviderSessions?: boolean;
    /**
     * Fresh VMs on providers without snapshots: install `t3.prepareAcpAgents` / `t3.installProviders`
     * at boot, since no baked base image carries those runtimes.
     */
    installProviderRuntimes?: boolean;
    /**
     * Fresh VMs from a project snapshot only: move `project.workdir` to the remote's default branch
     * (discarding what the bake left), before `envFile`, `bootCmd` and T3. Never on wake.
     */
    syncRepoToRemote?: boolean;
  }): Promise<{
    descriptor?: Record<string, unknown>;
    authSession?: Record<string, unknown>;
    upstreamToken?: string;
    /** Instance IDs whose stored sign-in was staged into this VM. */
    injectedProviderSessions?: string[];
    /** Outcome of `syncRepoToRemote`, when it ran. */
    repoSync?: RepoSyncResult;
    /** How long `syncRepoToRemote` took, when it ran. */
    repoSyncMs?: number;
  }> {
    const { sandboxId, watchdogSlotKey, environmentId, label, projectName, project, watchdogSecret, llmToken, mintToken } = opts;
    // Stored sign-ins are only staged on providers with a file transfer (`download`): elsewhere the
    // upload would carry the token inside the provider's exec request bodies
    const canStageSessions = typeof this.boat.download === "function";
    if (opts.injectProviderSessions && !canStageSessions && this.providerSessions.enabledInstances().length > 0) {
      if (!this.warnedNoSessionInject) {
        this.warnedNoSessionInject = true;
        console.warn(
          `[sessions] provider ${this.boat.name} has no file download; stored provider sign-ins are not copied into its VMs`,
        );
      }
    }
    const staged = opts.injectProviderSessions && canStageSessions ? this.providerSessions.stagedCredentials() : [];
    const stagingDirs = [...new Set(staged.map((c) => c.remoteDir))];
    // Staging dirs live under the 0700 ~/.t3-sentinel; provision-base.sh installs the files 0600 and removes them
    const mkdirs = stagingDirs.map((d) => ` && mkdir -p ${d}`).join("");
    await this.boat.execOk(sandboxId, `mkdir -p ~/.t3-sentinel && chmod 700 ~/.t3-sentinel${mkdirs}`, 30);
    await this.boat.upload(sandboxId, PROVISION_SCRIPT, "~/.t3-sentinel/provision-base.sh");
    await this.boat.upload(sandboxId, WATCHDOG_SCRIPT, "~/.t3-sentinel/watchdog.mjs");

    const injected = new Set<string>();
    for (const cred of staged) {
      try {
        await this.boat.upload(sandboxId, cred.localPath, cred.remotePath);
        injected.add(cred.instanceId);
      } catch {
        // Never block a launch on a missing sign-in: drop anything staged (no partial files) and boot signed out
        console.warn(`[sessions] could not copy stored ${cred.instanceId} session into ${sandboxId}; continuing signed out`);
        injected.clear();
        await this.boat.exec(sandboxId, `rm -rf ${STAGED_SESSIONS_DIR}`, 30).catch(() => {});
        break;
      }
    }

    const tmpDir = mkdtempSync(join(tmpdir(), "t3-sentinel-boot-"));
    try {
      const bootSecretsPath = join(tmpDir, "boot-secrets.json");
      writeFileSync(
        bootSecretsPath,
        JSON.stringify(
          {
            slot: watchdogSlotKey,
            sandboxId,
            callbackUrl: `https://${this.config.controlHost}/internal/watchdog`,
            watchdogSecret,
            sleepAfterSecs: this.config.boat.sleepAfterSecs,
            archiveGraceSecs: this.config.boat.archiveGraceSecs,
            deleteWhen: this.config.boat.deleteWhen,
            projectWorkdir: project?.workdir ?? null,
          },
          null,
          2,
        ),
        { mode: 0o600 },
      );
      await this.boat.upload(sandboxId, bootSecretsPath, "~/.t3-sentinel/boot-secrets.json");

      if (this.config.llmProxy && llmToken) {
        const claudeSettingsPath = join(tmpDir, "claude-settings.json");
        writeFileSync(
          claudeSettingsPath,
          JSON.stringify(
            {
              env: {
                CLAUDE_CODE_DISABLE_TERMINAL_TITLE: "1",
                CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS: "1",
                ANTHROPIC_BASE_URL: `https://${this.config.llmProxy.host}`,
                ANTHROPIC_AUTH_TOKEN: llmToken,
              },
              skipDangerousModePermissionPrompt: true,
            },
            null,
            2,
          ),
          { mode: 0o600 },
        );
        await this.boat.execOk(sandboxId, "mkdir -p ~/.claude", 30);
        await this.boat.upload(sandboxId, claudeSettingsPath, "~/.claude/settings.json");
        await this.boat.execOk(sandboxId, "chmod 600 ~/.claude/settings.json", 30);
      }
    } finally {
      rmSync(tmpDir, { recursive: true, force: true });
    }

    if (this.config.t3.settingsFile && existsSync(this.config.t3.settingsFile)) {
      await this.boat.upload(sandboxId, this.config.t3.settingsFile, "~/.t3-sentinel/t3-settings.json");
    }

    for (const file of this.config.boat.injectFiles) {
      if (!existsSync(file.from)) continue;
      const remoteDir = file.to.includes("/") ? file.to.slice(0, file.to.lastIndexOf("/")) : ".";
      await this.boat.execOk(sandboxId, `mkdir -p ${remoteDir}`, 30);
      await this.boat.upload(sandboxId, file.from, file.to);
      await this.boat.execOk(sandboxId, `chmod ${file.mode ?? "600"} ${file.to}`, 30);
    }

    // After `injectFiles` (which may carry git credentials), before the `.env` and `bootCmd` that use the tree
    const repoSyncStartedAt = Date.now();
    const repoSync =
      opts.syncRepoToRemote && project ? await this.syncRepoToRemote(sandboxId, projectName, project) : undefined;
    const repoSyncMs = Date.now() - repoSyncStartedAt;

    if (project?.envFile && existsSync(project.envFile)) {
      const target = project.envTarget ?? `${project.workdir}/.env`;
      await this.boat.upload(sandboxId, project.envFile, target);
    }

    if (project?.bootCmd) {
      await this.boat.execOk(sandboxId, project.bootCmd, 180);
    }

    const envVars = [
      `T3_VERSION=${shellQuote(await this.provisionT3Version())}`,
      `T3_CHANNEL=${shellQuote(this.config.t3.channel)}`,
      `SLOT_ENV_ID=${shellQuote(environmentId)}`,
      `SLOT_LABEL=${shellQuote(label)}`,
      `MINT_UPSTREAM_TOKEN=${mintToken ? "1" : "0"}`,
    ];
    const bootAcpAgents = opts.installProviderRuntimes ? (this.config.t3.prepareAcpAgents ?? []) : [];
    const bootProviders = opts.installProviderRuntimes ? (this.config.t3.installProviders ?? []) : [];
    if (bootAcpAgents.length > 0) envVars.push(`T3_PREPARE_ACP_AGENTS=${shellQuote(bootAcpAgents.join(","))}`);
    if (bootProviders.length > 0) envVars.push(`T3_INSTALL_PROVIDERS=${shellQuote(bootProviders.join(","))}`);
    if (project && project.registerProject !== false) {
      envVars.push(`PROJECT_WORKDIR=${shellQuote(project.workdir)}`);
      envVars.push(`PROJECT_TITLE=${shellQuote(projectName)}`);
    }

    // Runtime installs at boot can take minutes (Antigravity validation alone up to 90s)
    const provisionTimeout = bootAcpAgents.length > 0 || bootProviders.length > 0 ? 900 : 300;
    const out = await this.boat.execOk(sandboxId, `${envVars.join(" ")} bash ~/.t3-sentinel/provision-base.sh`, provisionTimeout);
    const parsed = lastJsonLine<{
      descriptor?: Record<string, unknown>;
      authSession?: Record<string, unknown>;
      upstreamToken?: string;
    }>(out);

    if (parsed.descriptor || parsed.authSession) {
      this.state.setTemplates(parsed.descriptor ?? null, parsed.authSession ?? null);
    }
    if (injected.size > 0) {
      console.log(`[sessions] copied stored ${[...injected].join(", ")} session(s) into ${sandboxId}`);
    }
    return { ...parsed, injectedProviderSessions: [...injected], ...(repoSync ? { repoSync, repoSyncMs } : {}) };
  }

  /** Bounded and non-fatal: a failure or timeout boots the chat on the snapshot's code, with one log line. */
  private async syncRepoToRemote(sandboxId: string, projectName: string, project: ProjectConfig): Promise<RepoSyncResult> {
    const ms = Math.max(1, this.repoSyncTimeoutMs);
    // The script stops changing the tree 5s before the Hub stops waiting
    const script = buildRepoSyncScript(
      project.workdir,
      Math.max(1, Math.floor(ms / 1000) - 5),
      this.config.boat.injectFiles.map((f) => f.to),
    );
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timedOut = new Promise<RepoSyncResult>((resolve) => {
      timer = setTimeout(() => resolve({ status: "failed", reason: "timeout" }), ms);
    });
    const run = this.boat
      .exec(sandboxId, script, Math.ceil(ms / 1000))
      .then((r): RepoSyncResult => (r.exitCode === 0 ? parseRepoSyncOutput(r.stdout) : { status: "failed", reason: "exec-failed" }))
      // Never log the provider's message: it can echo the script or git's output
      .catch((): RepoSyncResult => ({ status: "failed", reason: "exec-failed" }));
    const result = await Promise.race([run, timedOut]);
    clearTimeout(timer);
    const line = describeRepoSync(projectName, sandboxId, result);
    if (result.status === "failed") console.warn(line);
    else console.log(line);
    return result;
  }

  private async provisionRunningVm(
    slotName: string,
    sandboxId: string,
    project: ProjectConfig | undefined,
    watchdogSecret: string,
    llmToken: string | null,
    mintToken: boolean,
    injectProviderSessions = false,
    installProviderRuntimes = false,
  ): Promise<void> {
    const slot = this.state.getSlot(slotName)!;
    const parsed = await this.provisionStandaloneVm({
      sandboxId,
      watchdogSlotKey: slotName,
      environmentId: slot.environmentId,
      label: slot.label,
      projectName: slot.project ?? project?.snapshot ?? slotName,
      project,
      watchdogSecret,
      llmToken,
      mintToken,
      injectProviderSessions,
      installProviderRuntimes,
    });

    this.state.updateSlot(slotName, (s) => {
      if (parsed.upstreamToken) s.upstreamToken = parsed.upstreamToken;
      if (parsed.descriptor) {
        s.descriptor = {
          ...parsed.descriptor,
          environmentId: s.environmentId,
          label: s.label,
        };
      }
      if (parsed.authSession && Object.keys(parsed.authSession).length > 0) {
        s.authSession = parsed.authSession;
      }
    });
  }

  private async waitUntilStopped(sandboxId: string, timeoutMs: number): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const info = await this.boat.info(sandboxId);
      if (info.state === "stopped" || info.state === "archived") return;
      await Bun.sleep(2_000);
    }
    throw new Error(`Timed out waiting for sandbox ${sandboxId} to finish stopping before snapshot`);
  }

  private async sendNotify(title: string, message: string): Promise<void> {
    if (!this.config.notifyUrl) return;
    try {
      await fetch(this.config.notifyUrl, {
        method: "POST",
        headers: { Title: title },
        body: message,
        signal: AbortSignal.timeout(8_000),
      });
    } catch {
      // non-fatal
    }
  }
}

function shellQuote(s: string): string {
  return `'${s.replace(/'/g, `'\\''`)}'`;
}

function lastJsonLine<T>(stdout: string): T {
  const lines = stdout.split(/\r?\n/);
  for (let i = lines.length - 1; i >= 0; i--) {
    const trimmed = lines[i].trimStart();
    if (trimmed.startsWith("{")) return JSON.parse(trimmed) as T;
  }
  return {} as T;
}
