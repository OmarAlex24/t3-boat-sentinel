import { existsSync, mkdirSync, readdirSync, readFileSync, renameSync, rmSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import type { StagedAttachment } from "./hub-attachments.ts";

/** Bytes kept on disk for staged uploads, at most (a message whose upload does not fit is not kept across a restart). */
export const STAGED_STORE_MAX_BYTES = 512 * 1024 * 1024;

type StagedMeta = Omit<StagedAttachment, "bytes" | "expiresAt">;

/**
 * The bytes of staged uploads a message still has to send (a message held while its VM wakes, or the
 * first message of a chat whose VM boots), under `<stateDir>/hub-staged-attachments/`: the messages
 * themselves are kept in `state.json`, so a Hub restart must keep what they attach too. One `.bin`
 * (the bytes) and one `.json` (name, type, size) per upload; directory 0700, files 0600, each written
 * to a temporary file then renamed. The Hub removes a copy once no waiting message refers to it.
 */
export class HubStagedAttachmentStore {
  maxBytes = STAGED_STORE_MAX_BYTES;
  private readonly dir: string;

  constructor(stateDir: string) {
    this.dir = join(stateDir, "hub-staged-attachments");
  }

  private base(id: string): string {
    return join(this.dir, encodeURIComponent(id));
  }

  has(id: string): boolean {
    return existsSync(`${this.base(id)}.bin`) && existsSync(`${this.base(id)}.json`);
  }

  /** Keeps a copy of an uploaded staged attachment; false when it is not kept (no bytes yet, or no room). */
  save(staged: StagedAttachment): boolean {
    if (!staged.bytes || this.has(staged.id)) return !!staged.bytes;
    if (this.usedBytes() + staged.bytes.byteLength > this.maxBytes) {
      console.warn(`[hub] not keeping staged attachment ${staged.id} on disk: the store is full (${this.maxBytes} bytes)`);
      return false;
    }
    const meta: StagedMeta = {
      id: staged.id,
      type: staged.type,
      name: staged.name,
      mimeType: staged.mimeType,
      sizeBytes: staged.sizeBytes,
      createdAt: staged.createdAt,
    };
    try {
      mkdirSync(this.dir, { recursive: true, mode: 0o700 });
      const base = this.base(staged.id);
      writeFileSync(`${base}.bin.tmp`, staged.bytes, { mode: 0o600 });
      renameSync(`${base}.bin.tmp`, `${base}.bin`);
      // The metadata last: a copy without it is incomplete and never loaded
      writeFileSync(`${base}.json.tmp`, JSON.stringify(meta), { mode: 0o600 });
      renameSync(`${base}.json.tmp`, `${base}.json`);
      return true;
    } catch (err) {
      console.warn(`[hub] could not keep staged attachment ${staged.id} on disk:`, err instanceof Error ? err.message : err);
      return false;
    }
  }

  /** The stored copy, or null when there is none (or it cannot be read). */
  load(id: string): StagedAttachment | null {
    try {
      const meta = JSON.parse(readFileSync(`${this.base(id)}.json`, "utf8")) as StagedMeta;
      if (meta.id !== id) return null;
      const bytes = new Uint8Array(readFileSync(`${this.base(id)}.bin`));
      if (bytes.byteLength !== meta.sizeBytes) return null;
      return { ...meta, bytes, expiresAt: 0 };
    } catch {
      return null;
    }
  }

  remove(id: string): void {
    const base = this.base(id);
    for (const suffix of [".bin", ".json", ".bin.tmp", ".json.tmp"]) rmSync(`${base}${suffix}`, { force: true });
  }

  /** Ids with a file on disk (complete or not). */
  storedIds(): string[] {
    if (!existsSync(this.dir)) return [];
    const ids = new Set<string>();
    for (const name of readdirSync(this.dir)) {
      const m = /^(.*)\.(bin|json)(\.tmp)?$/.exec(name);
      if (m) ids.add(decodeURIComponent(m[1]));
    }
    return [...ids];
  }

  /** When a stored copy was made (epoch ms), or null. */
  createdAt(id: string): number | null {
    try {
      return (JSON.parse(readFileSync(`${this.base(id)}.json`, "utf8")) as StagedMeta).createdAt;
    } catch {
      return null;
    }
  }

  private usedBytes(): number {
    if (!existsSync(this.dir)) return 0;
    let total = 0;
    for (const name of readdirSync(this.dir)) {
      try {
        total += statSync(join(this.dir, name)).size;
      } catch {
        // removed meanwhile
      }
    }
    return total;
  }
}
