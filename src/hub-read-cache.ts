/**
 * What a sleeping chat's panels showed when its VM went to sleep: the answers to the read-only RPCs
 * the app sends when it opens a chat (git status and branch, the review diff, the refs, a file the
 * panel reads), and the attachments its history shows. Payload and result shapes follow the T3
 * contracts (T3 Code 0.1.13-nightly.20261125.5245, `WsRpcGroup`):
 *
 * - `subscribeVcsStatus` `{ cwd, includeRemote? }` streams `VcsStatusStreamEvent`; only its
 *   `snapshot` event stands alone (`localUpdated` and `remoteUpdated` patch one).
 * - `subscribeVcsBranch` `{ cwd }` streams `VcsBranchStatus`.
 * - `review.getDiffPreview` `{ cwd, ...options }` answers `ReviewDiffPreviewResult`. A `trackedOnly`
 *   read may answer a `completionToken` the app then sends back for the complete read.
 * - `vcs.listRefs` `{ cwd, query?, cursor?, ... }` answers `VcsListRefsResult`.
 * - `projects.readFile` `{ cwd, relativePath, allowMissing? }` answers `ProjectReadFileResult`.
 *
 * Files live under the state dir next to the projection cache (`hub-thread-cache/`): one JSON file
 * per chat in `hub-thread-reads/`, its attachments in `hub-thread-assets/`. Files are 0600 and
 * directories 0700.
 */
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";

/** Read-only RPCs whose answers a sleeping chat is served from its cache. */
export const CACHED_READ_TAGS = new Set([
  "subscribeVcsStatus",
  "subscribeVcsBranch",
  "review.getDiffPreview",
  "vcs.listRefs",
  "projects.readFile",
]);
/** Of those, the streams: the cache holds their latest value. */
export const CACHED_READ_STREAMS = new Set(["subscribeVcsStatus", "subscribeVcsBranch"]);
/**
 * Distinct requests remembered per chat and RPC (a diff per file, a file read, a refs search); the least
 * recently asked of that RPC go first, so many file reads never push out the git panel's streams.
 */
export const MAX_RECORDED_READS_PER_TAG = 16;

/** Default cap on one cached answer, in bytes of JSON. */
export const DEFAULT_MAX_CACHED_READ_BYTES = 1024 * 1024;
/** Default cap on one chat's cached attachments, in bytes. */
export const DEFAULT_MAX_CACHED_ASSET_BYTES = 25 * 1024 * 1024;

/** A request as it is recorded and keyed. A diff's completion token belongs to one past read, so it is left out. */
export function recordedPayload(tag: string, payload: unknown): Record<string, unknown> {
  const obj = { ...((payload && typeof payload === "object" ? payload : {}) as Record<string, unknown>) };
  if (tag === "review.getDiffPreview") delete obj.completionToken;
  return obj;
}

/**
 * The request a recorded read is replayed with at sleep. A diff preview is read whole (no `trackedOnly`):
 * the cached answer is then complete and carries no completion token, so a sleeping chat's app never
 * needs a second read the VM would have to answer.
 */
export function replayPayload(tag: string, payload: unknown): Record<string, unknown> {
  const obj = recordedPayload(tag, payload);
  if (tag === "review.getDiffPreview") delete obj.trackedOnly;
  return obj;
}

function stableJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  if (value && typeof value === "object") {
    const obj = value as Record<string, unknown>;
    const keys = Object.keys(obj)
      .filter((k) => obj[k] !== undefined)
      .sort();
    return `{${keys.map((k) => `${JSON.stringify(k)}:${stableJson(obj[k])}`).join(",")}}`;
  }
  return JSON.stringify(value) ?? "null";
}

/** The cache key of a request: its tag and its recorded payload, key order aside. */
export function readKey(tag: string, payload: unknown): string {
  return `${tag} ${stableJson(recordedPayload(tag, payload))}`;
}

/** A stream value the app can be served on its own: any branch status, a status snapshot. */
export function isStandaloneStreamValue(tag: string, value: unknown): boolean {
  if (!value || typeof value !== "object") return false;
  if (tag === "subscribeVcsStatus") return (value as { _tag?: unknown })._tag === "snapshot";
  return true;
}

export interface AttachmentRef {
  id: string;
  name: string;
  mimeType: string;
  sizeBytes: number;
}

/** Every `ChatAttachment` a thread projection lists (messages, turn items...), once each. */
export function attachmentsIn(projection: unknown): AttachmentRef[] {
  const found = new Map<string, AttachmentRef>();
  const visit = (node: unknown, depth: number) => {
    if (depth > 12 || !node || typeof node !== "object") return;
    if (Array.isArray(node)) {
      for (const item of node) visit(item, depth + 1);
      return;
    }
    for (const [key, value] of Object.entries(node as Record<string, unknown>)) {
      if (key === "attachments" && Array.isArray(value)) {
        for (const att of value) {
          const a = (att && typeof att === "object" ? att : {}) as Record<string, unknown>;
          if (typeof a.id !== "string" || !a.id || found.has(a.id)) continue;
          if (typeof a.name !== "string" || typeof a.mimeType !== "string" || typeof a.sizeBytes !== "number") continue;
          found.set(a.id, { id: a.id, name: a.name, mimeType: a.mimeType, sizeBytes: a.sizeBytes });
        }
      } else {
        visit(value, depth + 1);
      }
    }
  };
  visit(projection, 0);
  return [...found.values()];
}

export interface CachedRead {
  tag: string;
  value: unknown;
}

export interface CachedAsset {
  name: string;
  mimeType: string;
  /** File name under the chat's assets directory. */
  file: string;
}

export interface ThreadReads {
  /** When the VM was asked (epoch ms), just before it slept. */
  savedAt: number;
  entries: Record<string, CachedRead>;
  attachments: Record<string, CachedAsset>;
}

/** The cached reads of chats whose VM sleeps, on disk and in memory. */
export class ThreadReadStore {
  private readonly cache = new Map<string, ThreadReads>();
  /** Attachment id -> chat, for every chat loaded so far. */
  private readonly attachmentOwners = new Map<string, string>();
  private scannedDisk = false;

  constructor(private readonly stateDir: string) {}

  private get readsDir(): string {
    return join(this.stateDir, "hub-thread-reads");
  }

  private get assetsRoot(): string {
    return join(this.stateDir, "hub-thread-assets");
  }

  private readsFile(threadId: string): string {
    return join(this.readsDir, `${encodeURIComponent(threadId)}.json`);
  }

  /** A prefix keeps an id such as `..` from naming a directory outside the root. */
  private assetsDir(threadId: string): string {
    return join(this.assetsRoot, `t-${encodeURIComponent(threadId)}`);
  }

  /** The file an attachment is kept in: a hash, since the id comes from the VM. */
  static assetFileName(attachmentId: string): string {
    return createHash("sha256").update(attachmentId).digest("hex").slice(0, 40);
  }

  get(threadId: string): ThreadReads | null {
    const hit = this.cache.get(threadId);
    if (hit) return hit;
    const file = this.readsFile(threadId);
    if (!existsSync(file)) return null;
    try {
      const reads = JSON.parse(readFileSync(file, "utf8")) as ThreadReads;
      if (!reads || typeof reads.entries !== "object" || typeof reads.attachments !== "object") return null;
      this.remember(threadId, reads);
      return reads;
    } catch {
      return null;
    }
  }

  private remember(threadId: string, reads: ThreadReads): void {
    this.cache.set(threadId, reads);
    for (const id of Object.keys(reads.attachments)) this.attachmentOwners.set(id, threadId);
  }

  /** Writes an attachment's bytes for a chat; returns its file name. */
  writeAsset(threadId: string, attachmentId: string, bytes: Uint8Array): string {
    mkdirSync(this.assetsRoot, { recursive: true, mode: 0o700 });
    mkdirSync(this.assetsDir(threadId), { recursive: true, mode: 0o700 });
    const file = ThreadReadStore.assetFileName(attachmentId);
    writeFileSync(join(this.assetsDir(threadId), file), bytes, { mode: 0o600 });
    return file;
  }

  assetPath(threadId: string, asset: CachedAsset): string {
    return join(this.assetsDir(threadId), asset.file);
  }

  save(threadId: string, reads: ThreadReads): void {
    this.remember(threadId, reads);
    mkdirSync(this.readsDir, { recursive: true, mode: 0o700 });
    writeFileSync(this.readsFile(threadId), JSON.stringify(reads), { mode: 0o600 });
  }

  /** Forgets a chat's cached reads and attachments, in memory and on disk. */
  discard(threadId: string): void {
    const reads = this.cache.get(threadId);
    this.cache.delete(threadId);
    if (reads) {
      for (const id of Object.keys(reads.attachments)) {
        if (this.attachmentOwners.get(id) === threadId) this.attachmentOwners.delete(id);
      }
    }
    rmSync(this.readsFile(threadId), { force: true });
    rmSync(this.assetsDir(threadId), { recursive: true, force: true });
  }

  /** The chat whose cache holds this attachment, loading every chat's cache from disk once (after a restart). */
  attachmentOwner(attachmentId: string): string | null {
    const known = this.attachmentOwners.get(attachmentId);
    if (known) return known;
    if (this.scannedDisk) return null;
    this.scannedDisk = true;
    if (!existsSync(this.readsDir)) return null;
    for (const name of readdirSync(this.readsDir)) {
      if (!name.endsWith(".json")) continue;
      let threadId: string;
      try {
        threadId = decodeURIComponent(name.slice(0, -".json".length));
      } catch {
        continue;
      }
      if (!this.cache.has(threadId)) this.get(threadId);
    }
    return this.attachmentOwners.get(attachmentId) ?? null;
  }
}

/** Milliseconds left before `deadline`, at least 1. */
export function remainingMs(deadline: number): number {
  return Math.max(1, deadline - Date.now());
}

/**
 * GETs a file's bytes, giving up past `timeoutMs`. "over" when it is bigger than `maxBytes` (read no
 * further than that), null when the server does not answer 2xx.
 */
export async function fetchCapped(
  url: string,
  headers: Record<string, string>,
  maxBytes: number,
  timeoutMs: number,
): Promise<Uint8Array | "over" | null> {
  const res = await fetch(url, { headers, signal: AbortSignal.timeout(timeoutMs) });
  if (!res.ok || !res.body) {
    await res.body?.cancel();
    return null;
  }
  const declared = Number(res.headers.get("content-length"));
  if (Number.isFinite(declared) && declared > maxBytes) {
    await res.body.cancel();
    return "over";
  }
  const chunks: Uint8Array[] = [];
  let total = 0;
  const reader = res.body.getReader();
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (total > maxBytes) {
      await reader.cancel();
      return "over";
    }
    chunks.push(value);
  }
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bytes;
}
