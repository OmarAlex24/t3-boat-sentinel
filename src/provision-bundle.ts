import { randomBytes } from "node:crypto";
import { readFileSync, statSync } from "node:fs";

/**
 * One file the provisioning bundle places inside the VM.
 *
 * The archive only carries opaque names (`f0`, `f1`, …) and the bytes; where each file goes and with
 * which mode lives in the place script, which never holds file contents.
 */
export interface BundleFile {
  /** Local file to read, or the bytes to send (boot secrets and other generated files). */
  source: string | Uint8Array;
  /** Remote path: absolute, `~/…`, or relative to the remote home (like `boat scp`). */
  dest: string;
  /**
   * Mode for a file the place step creates (octal string, e.g. "600"). An existing file keeps its mode
   * and only gets new contents, like an scp overwrite. Defaults to the local file's mode.
   */
  mode?: string;
  /** Mode applied even when the file already existed (today's `upload` + `chmod`). */
  forceMode?: string;
  /** Files of the same group are placed together; a failed `optional` group is removed and reported. */
  group?: string;
}

export interface BundleGroupSpec {
  /** Removed (`rm -rf`) when any file of the group cannot be placed. */
  cleanupDir?: string;
}

export interface ProvisionBundle {
  /** ustar archive bytes; entries are `f<n>` at 0600. */
  archive: Uint8Array;
  /** Script that extracts the archive (uploaded to `remoteArchive`) and places every file. Paths and modes only. */
  placeScript: string;
  /** Where the archive must be uploaded. */
  remoteArchive: string;
  /** Groups the place script reports as failed print `T3S_BUNDLE_FAILED <group>` on stdout. */
  optionalGroups: string[];
  /** Files held back in VM staging for a later `placeDeferredScript` (e.g. `.env` after the repo sync). */
  deferred: Array<{ staged: string; dest: string; mode: string; forceMode?: string }>;
}

export const BUNDLE_DIR = "~/.t3-sentinel";
const FAILED_MARKER = "T3S_BUNDLE_FAILED";

/**
 * Builds the single archive and place script that replace one `boat scp` per provisioning file.
 *
 * `deferred` files are extracted into VM staging (0600 under the 0700 `~/.t3-sentinel`) and placed by
 * `placeDeferredScript` later, for ordering constraints such as the `.env` landing after the repo sync.
 */
export function buildProvisionBundle(
  files: BundleFile[],
  opts: { deferred?: BundleFile[]; groups?: Record<string, BundleGroupSpec> } = {},
): ProvisionBundle {
  const id = randomBytes(6).toString("hex");
  const remoteArchive = `${BUNDLE_DIR}/bundle-${id}.tar`;
  const entries: Array<{ name: string; data: Uint8Array }> = [];
  const placed: Array<{ name: string; file: BundleFile; mode: string }> = [];
  const deferred: ProvisionBundle["deferred"] = [];

  const add = (file: BundleFile): { name: string; mode: string } => {
    const name = `f${entries.length}`;
    const data = typeof file.source === "string" ? readFileSync(file.source) : file.source;
    const mode = checkMode(file.mode ?? (typeof file.source === "string" ? localMode(file.source) : "600"));
    if (file.forceMode) checkMode(file.forceMode);
    entries.push({ name, data });
    return { name, mode };
  };

  for (const file of files) {
    const { name, mode } = add(file);
    placed.push({ name, file, mode });
  }
  for (const file of opts.deferred ?? []) {
    const { name, mode } = add(file);
    const staged = `${BUNDLE_DIR}/deferred-${id}-${name}`;
    // Extracted into staging only; placed by `placeDeferredScript`
    placed.push({ name, file: { source: "", dest: staged, mode: "600", group: "__deferred" }, mode: "600" });
    deferred.push({ staged, dest: file.dest, mode, ...(file.forceMode ? { forceMode: file.forceMode } : {}) });
  }

  const groups = opts.groups ?? {};
  const optionalGroups = Object.keys(groups);
  const lines: string[] = [
    "set -eu",
    `mkdir -p ${remotePath(BUNDLE_DIR)} && chmod 700 ${remotePath(BUNDLE_DIR)}`,
    `t3s_x="$(mktemp -d ${remotePath(`${BUNDLE_DIR}/bundle-${id}.XXXXXX`)})"`,
    `trap 'rm -rf "$t3s_x" ${remotePath(remoteArchive)}' EXIT`,
    `tar -xf ${remotePath(remoteArchive)} -C "$t3s_x"`,
    PLACE_FN,
  ];
  // Mandatory files first, in order; then each optional group as one all-or-nothing block
  const ungrouped = placed.filter((p) => !p.file.group || !(p.file.group in groups));
  for (const p of ungrouped) lines.push(placeLine(p));
  for (const group of optionalGroups) {
    const members = placed.filter((p) => p.file.group === group);
    if (members.length === 0) continue;
    const cleanup = groups[group]!.cleanupDir;
    lines.push(
      `if ! { ${members.map(placeLine).join(" && ")}; }; then`,
      ...(cleanup ? [`  rm -rf ${remotePath(cleanup)}`] : []),
      `  echo ${FAILED_MARKER} ${shellQuote(group)}`,
      "fi",
    );
  }

  return { archive: ustar(entries), placeScript: lines.join("\n"), remoteArchive, optionalGroups, deferred };
}

/** Places the bundle's deferred files (staged 0600 under `~/.t3-sentinel`) at their destinations. */
export function placeDeferredScript(deferred: ProvisionBundle["deferred"]): string {
  return [
    "set -eu",
    PLACE_FN,
    ...deferred.map(
      (d) =>
        `t3s_place ${remotePath(d.staged)} ${remotePath(d.dest)} ${d.mode} ${d.forceMode ?? "''"} && rm -f ${remotePath(d.staged)}`,
    ),
  ].join("\n");
}

/** Groups the place script reported as failed. */
export function parseFailedGroups(stdout: string): string[] {
  const out: string[] = [];
  for (const line of stdout.split(/\r?\n/)) {
    const m = line.trim().match(new RegExp(`^${FAILED_MARKER} (\\S+)$`));
    if (m) out.push(m[1]!);
  }
  return out;
}

/**
 * `t3s_place <src> <dest> <create mode> [<forced mode>]`: an existing file keeps its inode and mode and
 * gets the new contents (what an scp overwrite does); a new one is created with `<create mode>`.
 * Parent directories are created with the VM's default umask, as `mkdir -p` did before. Every step
 * returns on failure itself: `set -e` does not apply inside the `if ! …` of an optional group.
 */
const PLACE_FN = [
  "t3s_place() {",
  '  mkdir -p "$(dirname "$2")" || return 1',
  '  if [ -e "$2" ]; then cat "$1" > "$2" || return 1; else { cp "$1" "$2" && chmod "$3" "$2"; } || return 1; fi',
  '  if [ -n "${4:-}" ]; then chmod "$4" "$2" || return 1; fi',
  "}",
].join("\n");

function placeLine(p: { name: string; file: BundleFile; mode: string }): string {
  return `t3s_place "$t3s_x/${p.name}" ${remotePath(p.file.dest)} ${p.mode} ${p.file.forceMode ?? "''"}`;
}

/** Shell word for a remote path: `~/x` expands to the home, a relative path resolves against it (like scp). */
export function remotePath(path: string): string {
  if (path === "~") return '"$HOME"';
  if (path.startsWith("~/")) return `"$HOME"/${shellQuote(path.slice(2))}`;
  if (path.startsWith("/")) return shellQuote(path);
  return `"$HOME"/${shellQuote(path)}`;
}

function checkMode(mode: string): string {
  if (!/^[0-7]{3,4}$/.test(mode)) throw new Error(`invalid file mode "${mode}" (expected octal like 600)`);
  return mode;
}

function localMode(path: string): string {
  return (statSync(path).mode & 0o777).toString(8).padStart(3, "0");
}

function shellQuote(s: string): string {
  return `'${s.replace(/'/g, `'\\''`)}'`;
}

/** Minimal POSIX ustar writer: regular files, short names, mode 0600, owner 0. */
export function ustar(entries: Array<{ name: string; data: Uint8Array }>): Uint8Array {
  const blocks: Uint8Array[] = [];
  const mtime = Math.floor(Date.now() / 1000);
  for (const { name, data } of entries) {
    if (Buffer.byteLength(name) > 99) throw new Error(`tar entry name too long: ${name}`);
    const h = new Uint8Array(512);
    const put = (offset: number, len: number, value: string) => {
      const bytes = Buffer.from(value, "utf8");
      h.set(bytes.subarray(0, len), offset);
    };
    const octal = (offset: number, len: number, value: number) => put(offset, len, `${value.toString(8).padStart(len - 1, "0")}\0`);
    put(0, 100, name);
    octal(100, 8, 0o600);
    octal(108, 8, 0);
    octal(116, 8, 0);
    octal(124, 12, data.byteLength);
    octal(136, 12, mtime);
    h.fill(0x20, 148, 156); // checksum computed with its own field as spaces
    put(156, 1, "0");
    put(257, 6, "ustar\0");
    put(263, 2, "00");
    let sum = 0;
    for (const b of h) sum += b;
    put(148, 8, `${sum.toString(8).padStart(6, "0")}\0 `);
    blocks.push(h, data);
    const pad = (512 - (data.byteLength % 512)) % 512;
    if (pad) blocks.push(new Uint8Array(pad));
  }
  blocks.push(new Uint8Array(1024));
  const total = blocks.reduce((n, b) => n + b.byteLength, 0);
  const out = new Uint8Array(total);
  let off = 0;
  for (const b of blocks) {
    out.set(b, off);
    off += b.byteLength;
  }
  return out;
}
