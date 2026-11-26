import { afterEach, describe, expect, test } from "bun:test";
import { mkdtempSync, readdirSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { isHubStagedAttachmentId, stagedAttachmentId, type StagedAttachment } from "./hub-attachments.ts";
import { HubStagedAttachmentStore } from "./hub-staged-store.ts";

const dirs: string[] = [];
afterEach(() => {
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});
const stateDir = () => {
  const dir = mkdtempSync(join(tmpdir(), "t3-sentinel-staged-store-"));
  dirs.push(dir);
  return dir;
};
const staged = (size: number, bytes: Uint8Array | null = new Uint8Array(size).fill(7)): StagedAttachment => ({
  id: stagedAttachmentId("image", "image.png"),
  type: "image",
  name: "image.png",
  mimeType: "image/png",
  sizeBytes: size,
  bytes,
  createdAt: Date.now(),
  expiresAt: Date.now() + 600_000,
});

describe("HubStagedAttachmentStore (staged uploads kept for a Hub restart)", () => {
  test("keeps and gives back an upload: private files, same bytes", () => {
    const dir = stateDir();
    const store = new HubStagedAttachmentStore(dir);
    const one = staged(64);
    expect(store.save(one)).toBe(true);
    const root = join(dir, "hub-staged-attachments");
    expect(statSync(root).mode & 0o777).toBe(0o700);
    for (const name of readdirSync(root)) expect(statSync(join(root, name)).mode & 0o777).toBe(0o600);
    // A fresh store on the same state dir (a restarted Hub) reads it
    const loaded = new HubStagedAttachmentStore(dir).load(one.id)!;
    expect(loaded).toMatchObject({ id: one.id, name: "image.png", mimeType: "image/png", sizeBytes: 64 });
    expect(loaded.bytes).toEqual(one.bytes);
    expect(store.storedIds()).toEqual([one.id]);
    store.remove(one.id);
    expect(store.storedIds()).toEqual([]);
    expect(store.load(one.id)).toBeNull();
  });

  test("an unfinished upload or a full store is not kept; a copy cut short is never loaded", () => {
    const store = new HubStagedAttachmentStore(stateDir());
    expect(store.save(staged(10, null))).toBe(false);
    store.maxBytes = 100;
    expect(store.save(staged(80))).toBe(true);
    expect(store.save(staged(80))).toBe(false);
    expect(store.storedIds()).toHaveLength(1);

    const dir = stateDir();
    const other = new HubStagedAttachmentStore(dir);
    const one = staged(32);
    other.save(one);
    writeFileSync(join(dir, "hub-staged-attachments", `${encodeURIComponent(one.id)}.bin`), new Uint8Array(5));
    expect(other.load(one.id)).toBeNull();
  });

  test("isHubStagedAttachmentId matches only the Hub's own staged ids", () => {
    expect(isHubStagedAttachmentId(stagedAttachmentId("image", "a.png"))).toBe(true);
    expect(isHubStagedAttachmentId(stagedAttachmentId("file", "notes.txt"))).toBe(true);
    expect(isHubStagedAttachmentId("vm-vm1-att-1")).toBe(false);
    expect(isHubStagedAttachmentId("thread-1-0b8f0c1e-0a6e-4a51-9d9e-3c3f3b1f2a10")).toBe(false);
    expect(isHubStagedAttachmentId(undefined)).toBe(false);
  });
});
