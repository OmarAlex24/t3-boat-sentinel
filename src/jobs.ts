import { randomUUID } from "node:crypto";

export type JobStatus = "running" | "completed" | "failed";

export interface JobRecord {
  id: string;
  kind: "spawn" | "bake" | "wake" | "reset";
  status: JobStatus;
  idempotencyKey?: string;
  createdAt: string;
  finishedAt?: string;
  result?: unknown;
  error?: string;
}

/**
 * In-memory async job tracker with idempotency keys so long-running Boat operations
 * (spawn ~15-30s, bake ~1-5m) never hit HTTP / Cloudflare / MCP 10s-60s request timeouts.
 */
export class JobManager {
  private readonly jobs = new Map<string, JobRecord>();
  private readonly byIdempotencyKey = new Map<string, string>();
  private readonly maxHistory = 200;

  start(
    kind: JobRecord["kind"],
    fn: () => Promise<unknown>,
    idempotencyKey?: string,
  ): JobRecord {
    if (idempotencyKey) {
      const existingId = this.byIdempotencyKey.get(`${kind}:${idempotencyKey}`);
      const existing = existingId ? this.jobs.get(existingId) : undefined;
      if (existing && existing.status !== "failed") {
        return existing;
      }
    }

    const id = randomUUID();
    const record: JobRecord = {
      id,
      kind,
      status: "running",
      idempotencyKey,
      createdAt: new Date().toISOString(),
    };
    this.jobs.set(id, record);
    if (idempotencyKey) {
      this.byIdempotencyKey.set(`${kind}:${idempotencyKey}`, id);
    }
    this.prune();

    void fn()
      .then((result) => {
        record.status = "completed";
        record.result = result;
        record.finishedAt = new Date().toISOString();
      })
      .catch((err) => {
        record.status = "failed";
        record.error = err instanceof Error ? err.message : String(err);
        record.finishedAt = new Date().toISOString();
      });

    return record;
  }

  get(id: string): JobRecord | undefined {
    return this.jobs.get(id);
  }

  list(): JobRecord[] {
    return Array.from(this.jobs.values()).reverse();
  }

  async wait(id: string, timeoutMs = 120_000): Promise<JobRecord> {
    const start = Date.now();
    while (true) {
      const job = this.jobs.get(id);
      if (!job) throw new Error(`Unknown job "${id}"`);
      if (job.status !== "running") return job;
      if (Date.now() - start >= timeoutMs) return job;
      await Bun.sleep(250);
    }
  }

  private prune(): void {
    if (this.jobs.size <= this.maxHistory) return;
    for (const [id, job] of this.jobs) {
      if (this.jobs.size <= this.maxHistory) break;
      if (job.status !== "running") {
        this.jobs.delete(id);
        if (job.idempotencyKey) {
          this.byIdempotencyKey.delete(`${job.kind}:${job.idempotencyKey}`);
        }
      }
    }
  }
}
