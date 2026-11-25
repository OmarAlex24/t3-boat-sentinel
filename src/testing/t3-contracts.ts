/**
 * Test support: the T3 app's own RPC contracts, loaded from an installed T3 Code app bundle.
 *
 * Reads `app.asar` (read only) from `T3_APP_ASAR` or the default macOS Nightly path, copies the
 * server chunks (`apps/server/dist/*.mjs`) to a cache under the OS temp dir, and imports the chunks
 * that export the RPC group (`WsRpcGroup`) and the schema helpers (`decodeUnknownExit`, `toCodecJson`).
 * Values are decoded exactly as the app's RPC client decodes a JSON frame.
 *
 * When no bundle is installed (CI), `loadT3Contracts()` returns null and contract tests skip.
 */
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const DEFAULT_ASAR = "/Applications/T3 Code (Nightly).app/Contents/Resources/app.asar";
const SERVER_DIST = "apps/server/dist";

type AnyModule = Record<string, unknown>;
type AsarEntry = { size: number; offset?: string; files?: Record<string, AsarEntry>; unpacked?: boolean };

export interface DecodeResult {
  ok: boolean;
  /** The schema error, as the app's formatter prints it. */
  error?: string;
}

export interface T3Contracts {
  /** `version` from the bundle's package.json, e.g. 0.1.13-nightly.20261125.5245. */
  version: string;
  asarPath: string;
  rpcTags: string[];
  hasRpc(tag: string): boolean;
  isStream(tag: string): boolean;
  /** An Exit `Success` value of a unary RPC, or a Chunk value of a stream RPC. */
  decodeSuccess(tag: string, value: unknown): DecodeResult;
  /** A typed `Fail` error. */
  decodeError(tag: string, error: unknown): DecodeResult;
  decodePayload(tag: string, payload: unknown): DecodeResult;
  /** A named export of the contracts chunk (e.g. `AttachmentCreateUploadUrlResult`). */
  decodeWith(exportName: string, value: unknown): DecodeResult;
  /** A named export of the contracts chunk, as a raw value (e.g. `ATTACHMENT_UPLOAD_URL_TTL_MS`). */
  exported(name: string): unknown;
  /** A whole `Exit` (success, typed Fail, Die, Interrupt), with the exit schema the app's RPC client uses. */
  decodeExit(tag: string, exit: unknown): DecodeResult;
  /** A frame the Hub sent for a request with this tag: each Chunk value, or the Exit. */
  decodeFrame(tag: string, frame: Record<string, unknown>): DecodeResult[];
}

async function extractServerChunks(asarPath: string, cacheDir: string): Promise<string> {
  const file = Bun.file(asarPath);
  const prefix = Buffer.from(await file.slice(0, 16).arrayBuffer());
  const headerSize = prefix.readUInt32LE(12);
  const base = 8 + prefix.readUInt32LE(4);
  const header = JSON.parse(Buffer.from(await file.slice(16, 16 + headerSize).arrayBuffer()).toString("utf8")) as AsarEntry;
  const at = (path: string): AsarEntry | undefined =>
    path.split("/").reduce<AsarEntry | undefined>((node, part) => node?.files?.[part], header);

  const pkgEntry = at("package.json");
  let version = "unknown";
  if (pkgEntry?.offset !== undefined) {
    const off = base + Number(pkgEntry.offset);
    version = String((JSON.parse(await file.slice(off, off + pkgEntry.size).text()) as { version?: string }).version ?? "unknown");
  }
  const dist = at(SERVER_DIST);
  if (!dist?.files) throw new Error(`${asarPath} has no ${SERVER_DIST}`);
  mkdirSync(cacheDir, { recursive: true });
  for (const [name, entry] of Object.entries(dist.files)) {
    if (!name.endsWith(".mjs") || entry.offset === undefined) continue;
    const off = base + Number(entry.offset);
    writeFileSync(join(cacheDir, name), Buffer.from(await file.slice(off, off + entry.size).arrayBuffer()));
  }
  writeFileSync(join(cacheDir, ".version"), version);
  return version;
}

/** Export names of a bundled chunk, by their source name (`export { x as y }` -> x: y). */
function exportMap(source: string): Map<string, string> {
  const map = new Map<string, string>();
  const blocks = source.match(/export \{([^}]*)\}/g) ?? [];
  const last = blocks.at(-1);
  if (!last) return map;
  for (const part of last.slice(8, -1).split(",")) {
    const [local, exported] = part.trim().split(/\s+as\s+/);
    if (local) map.set(local, exported ?? local);
  }
  return map;
}

async function importBySourceName(dir: string, wanted: string[]): Promise<(name: string) => unknown> {
  for (const name of readdirSync(dir)) {
    if (!name.endsWith(".mjs")) continue;
    const source = readFileSync(join(dir, name), "latin1");
    if (!source.includes("export {")) continue;
    const map = exportMap(source);
    if (!wanted.every((w) => map.has(w))) continue;
    const mod = (await import(join(dir, name))) as AnyModule;
    return (key: string) => {
      const exported = map.get(key);
      return exported === undefined ? undefined : mod[exported];
    };
  }
  throw new Error(`no chunk in ${dir} exports ${wanted.join(", ")}`);
}

let cached: Promise<T3Contracts | null> | null = null;

export function loadT3Contracts(): Promise<T3Contracts | null> {
  // An installed app whose bundle this loader cannot read must not fail every test: warn and skip
  cached ??= load().catch((err) => {
    console.warn(`[contracts] could not load the T3 app's schemas; contract decodes skipped: ${err instanceof Error ? err.message : err}`);
    return null;
  });
  return cached;
}

async function load(): Promise<T3Contracts | null> {
  const asarPath = process.env.T3_APP_ASAR || DEFAULT_ASAR;
  if (!existsSync(asarPath)) return null;
  const st = statSync(asarPath);
  const key = createHash("sha256").update(`${asarPath}:${st.size}:${st.mtimeMs}`).digest("hex").slice(0, 16);
  const cacheDir = join(tmpdir(), "t3-app-contracts", key);
  const version = existsSync(join(cacheDir, ".version"))
    ? readFileSync(join(cacheDir, ".version"), "utf8")
    : await extractServerChunks(asarPath, cacheDir);

  const schema = await importBySourceName(cacheDir, ["decodeUnknownExit", "toCodecJson"]);
  const model = await importBySourceName(cacheDir, ["WsRpcGroup", "AttachmentCreateUploadUrlResult"]);
  const decodeUnknownExit = schema("decodeUnknownExit") as (s: unknown) => (v: unknown) => { _tag: string; toString(): string };
  const toCodecJson = schema("toCodecJson") as (s: unknown) => unknown;
  const ExitSchema = schema("Exit") as (success: unknown, failure: unknown, defect: unknown) => unknown;
  const UnionSchema = schema("Union") as (members: unknown[]) => unknown;
  const VoidSchema = schema("Void");
  type RpcDef = {
    successSchema: { success?: unknown; error?: unknown };
    errorSchema: unknown;
    payloadSchema: unknown;
    defectSchema: unknown;
    middlewares: Set<{ error: unknown }>;
  };
  const requests = (model("WsRpcGroup") as { requests: Map<string, RpcDef> }).requests;

  const decoders = new Map<unknown, (v: unknown) => { _tag: string; toString(): string }>();
  const run = (s: unknown, value: unknown): DecodeResult => {
    let d = decoders.get(s);
    if (!d) {
      d = decodeUnknownExit(toCodecJson(s));
      decoders.set(s, d);
    }
    const exit = d(value);
    return exit._tag === "Success" ? { ok: true } : { ok: false, error: String(exit) };
  };
  const rpc = (tag: string): RpcDef => {
    const def = requests.get(tag);
    if (!def) throw new Error(`the app has no RPC "${tag}"`);
    return def;
  };
  const isStream = (tag: string) => rpc(tag).successSchema.success !== undefined;
  const successSchema = (tag: string) => {
    const s = rpc(tag).successSchema;
    return s.success ?? s;
  };
  // Rebuilt like effect's `Rpc.exitSchema` (not exported by the bundle): streams end with a Void exit
  const exitSchemas = new Map<string, unknown>();
  const exitSchema = (tag: string) => {
    let s = exitSchemas.get(tag);
    if (!s) {
      const def = rpc(tag);
      const stream = def.successSchema.success !== undefined;
      const failures = new Set<unknown>([def.errorSchema]);
      if (stream) failures.add(def.successSchema.error);
      for (const mw of def.middlewares) failures.add(mw.error);
      s = ExitSchema(stream ? VoidSchema : def.successSchema, UnionSchema([...failures]), def.defectSchema);
      exitSchemas.set(tag, s);
    }
    return s;
  };

  const contracts: T3Contracts = {
    version,
    asarPath,
    rpcTags: [...requests.keys()],
    hasRpc: (tag) => requests.has(tag),
    isStream,
    decodeSuccess: (tag, value) => run(successSchema(tag), value),
    decodeError: (tag, error) => run(rpc(tag).errorSchema, error),
    decodePayload: (tag, payload) => run(rpc(tag).payloadSchema, payload),
    decodeWith: (name, value) => {
      const s = model(name);
      if (!s) throw new Error(`the contracts chunk has no export "${name}"`);
      return run(s, value);
    },
    exported: (name) => model(name),
    decodeExit: (tag, exit) => run(exitSchema(tag), exit),
    decodeFrame: (tag, frame) => {
      if (frame._tag === "Chunk") {
        return ((frame.values as unknown[]) ?? []).map((v) => contracts.decodeSuccess(tag, v));
      }
      return frame._tag === "Exit" ? [contracts.decodeExit(tag, frame.exit)] : [];
    },
  };
  return contracts;
}
