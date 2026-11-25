import { existsSync, mkdirSync, readdirSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";

/**
 * Each chat's sidebar row (`OrchestrationV2ThreadShell`), one file per thread under
 * `<stateDir>/hub-thread-shells/`, so a Hub restart still lists chats whose VM is asleep or
 * booting. Writes are batched (`flushDelayMs`) and skipped when the row did not change; `flush()`
 * writes what is pending at once (the Hub calls it on stop).
 *
 * Whether a row may be kept is asked at write time (`shouldKeep`): only threads bound to a sandbox
 * that still exists are kept, so a row never outlives its VM.
 */
export class HubShellStore {
  flushDelayMs = 250;
  private readonly dir: string;
  /** What each file holds, so an unchanged row is not written again. */
  private readonly written = new Map<string, string>();
  private readonly dirty = new Map<string, Record<string, unknown> | null>();
  private timer: ReturnType<typeof setTimeout> | null = null;

  constructor(
    stateDir: string,
    private readonly shouldKeep: (threadId: string) => boolean,
  ) {
    this.dir = join(stateDir, "hub-thread-shells");
  }

  private file(threadId: string): string {
    return join(this.dir, `${encodeURIComponent(threadId)}.json`);
  }

  load(threadId: string): Record<string, unknown> | null {
    const file = this.file(threadId);
    if (!existsSync(file)) return null;
    try {
      const text = readFileSync(file, "utf8");
      const row = JSON.parse(text) as unknown;
      if (!row || typeof row !== "object" || (row as { id?: unknown }).id !== threadId) return null;
      this.written.set(threadId, text);
      return row as Record<string, unknown>;
    } catch {
      return null;
    }
  }

  /** Thread ids with a stored row. */
  storedThreadIds(): string[] {
    if (!existsSync(this.dir)) return [];
    return readdirSync(this.dir)
      .filter((name) => name.endsWith(".json"))
      .map((name) => decodeURIComponent(name.slice(0, -".json".length)));
  }

  save(threadId: string, row: Record<string, unknown>): void {
    this.dirty.set(threadId, row);
    this.schedule();
  }

  remove(threadId: string): void {
    this.dirty.set(threadId, null);
    this.schedule();
  }

  private schedule(): void {
    if (this.timer) return;
    this.timer = setTimeout(() => this.flush(), this.flushDelayMs);
  }

  flush(): void {
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    const batch = [...this.dirty];
    this.dirty.clear();
    for (const [threadId, row] of batch) {
      try {
        if (row && this.shouldKeep(threadId)) this.write(threadId, row);
        else this.delete(threadId);
      } catch (err) {
        console.warn(`[hub] could not save the sidebar row of ${threadId}:`, err instanceof Error ? err.message : err);
      }
    }
  }

  private write(threadId: string, row: Record<string, unknown>): void {
    const text = JSON.stringify(row);
    if (this.written.get(threadId) === text) return;
    mkdirSync(this.dir, { recursive: true, mode: 0o700 });
    const file = this.file(threadId);
    const tmp = `${file}.tmp`;
    writeFileSync(tmp, text, { mode: 0o600 });
    renameSync(tmp, file);
    this.written.set(threadId, text);
  }

  private delete(threadId: string): void {
    this.written.delete(threadId);
    rmSync(this.file(threadId), { force: true });
  }
}
