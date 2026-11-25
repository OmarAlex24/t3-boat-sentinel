/**
 * Attachment uploads the Hub stages itself (before the chat's VM exists), following the T3 server's
 * contract (T3 Code 0.1.13-nightly.20261125.5245, `src/assets/AttachmentUpload.ts`):
 *
 * - `attachments.createUploadUrl` takes `{ type?: "image" | "file", name, mimeType, sizeBytes }` and
 *   answers `{ attachmentId, relativeUrl, expiresAt }` (`expiresAt` in epoch ms).
 * - The client then `POST`s the file's bytes to `relativeUrl` with `Content-Type: <mimeType>` and
 *   treats any 2xx as uploaded. T3 answers 204, 404 for an unknown or expired URL, 400 when the
 *   size differs from `sizeBytes`.
 */
import { randomUUID } from "node:crypto";
import { extname } from "node:path";

/** `ATTACHMENT_UPLOAD_URL_TTL_MS` in the T3 contracts. */
export const ATTACHMENT_UPLOAD_URL_TTL_MS = 600_000;
/** T3 sweeps pending (uploaded, never sent) attachments after a day. */
export const STAGED_ATTACHMENT_MAX_AGE_MS = 86_400_000;
export const ATTACHMENT_UPLOAD_ROUTE_PREFIX = "/api/attachments/upload/";

export type AttachmentType = "image" | "file";

export interface StagedAttachment {
  id: string;
  type: AttachmentType;
  name: string;
  mimeType: string;
  sizeBytes: number;
  bytes: Uint8Array | null;
  createdAt: number;
  /** The upload URL stops working after this (epoch ms). */
  expiresAt: number;
}

const GENERIC_MIME_TYPES = new Set([
  "application/octet-stream",
  "binary/octet-stream",
  "application/unknown",
  "video/mp2t",
  "video/vnd.dlna.mpeg-tts",
]);

/** T3's `isSvgAttachment`. */
export function isSvgAttachment(input: { name: string; mimeType: string }): boolean {
  const mimeType = input.mimeType.split(";", 1)[0]?.trim().toLowerCase() ?? "";
  return mimeType === "image/svg+xml" || ((mimeType === "" || GENERIC_MIME_TYPES.has(mimeType)) && /\.svg$/i.test(input.name));
}

/** T3's `attachmentFileExtension`: the stored file's extension, `.bin` when it has none usable. */
export function attachmentFileExtension(fileName: string): string {
  const extension = extname(fileName).toLowerCase();
  if (extension === ".part" || !/^\.[a-z0-9]{1,10}$/.test(extension)) return ".bin";
  return extension;
}

/**
 * Reads a `createUploadUrl` payload the way T3 does: `type` defaults to image, and a file that is
 * an SVG gets a `.svg` name. Null for a payload T3's schema rejects.
 */
export function parseUploadRequest(
  payload: unknown,
): { type: AttachmentType; name: string; mimeType: string; sizeBytes: number } | null {
  const p = (payload && typeof payload === "object" ? payload : {}) as Record<string, unknown>;
  const type: AttachmentType | null = p.type === undefined || p.type === "image" ? "image" : p.type === "file" ? "file" : null;
  const rawName = typeof p.name === "string" ? p.name.trim() : "";
  const mimeType = typeof p.mimeType === "string" ? p.mimeType.trim() : "";
  const sizeBytes = typeof p.sizeBytes === "number" && Number.isInteger(p.sizeBytes) ? p.sizeBytes : -1;
  if (!type || !rawName || rawName.length > 255 || !mimeType || mimeType.length > 100 || sizeBytes < 1) return null;
  const name = type === "file" && isSvgAttachment({ name: rawName, mimeType }) && !/\.svg$/i.test(rawName) ? `${rawName}.svg` : rawName;
  return { type, name, mimeType, sizeBytes };
}

/**
 * An id shaped like T3's pending upload ids (`pending-<uuid>`, plus `-<ext>` for a file), so the
 * client treats a Hub-staged upload like any other. It matches `ChatAttachmentId`.
 */
export function stagedAttachmentId(type: AttachmentType, name: string): string {
  const suffix = type === "file" ? `-${attachmentFileExtension(name).slice(1)}` : "";
  return `pending-${randomUUID()}${suffix}`;
}

/** The `createUploadUrl` payload that replays a staged upload into a VM. */
export function replayUploadRequest(staged: StagedAttachment): Record<string, unknown> {
  return {
    ...(staged.type === "file" ? { type: "file" } : {}),
    name: staged.name,
    mimeType: staged.mimeType,
    sizeBytes: staged.bytes?.byteLength ?? staged.sizeBytes,
  };
}

/** Headers for serving a staged upload's (or a sleeping chat's cached attachment's) bytes, as T3 serves uploaded bytes. */
export function stagedAssetHeaders(staged: { name: string; mimeType: string }): Record<string, string> {
  const headers: Record<string, string> = {
    "content-type": staged.mimeType,
    "cache-control": "private, max-age=3600",
    "x-content-type-options": "nosniff",
  };
  if (isSvgAttachment(staged)) headers["content-security-policy"] = "default-src 'none'; style-src 'unsafe-inline'; sandbox";
  return headers;
}
