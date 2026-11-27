import { afterEach, describe, expect, test } from "bun:test";
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const SCRIPT = readFileSync(join(import.meta.dir, "..", "scripts", "provision-base.sh"), "utf8");

const cleanups: Array<() => void> = [];
afterEach(() => {
  while (cleanups.length) cleanups.pop()!();
});

/** `set -Eeuo pipefail`, the ERR trap and the path variables: the script up to its first `mkdir`. */
const preamble = SCRIPT.slice(SCRIPT.indexOf("set -Eeuo pipefail"), SCRIPT.indexOf('mkdir -p "$sentinel_dir"'));
/** The T3 install/update step, from `restart_t3=0` up to `wait_for_t3`. */
const versionStep = SCRIPT.slice(SCRIPT.indexOf("restart_t3=0"), SCRIPT.indexOf("wait_for_t3() {"));

const UPDATE_ERROR = "[19:59:10.224] ERROR (#2): CliUpdateError: Could not list t3 releases.";

/** A fake `t3` in `~/.local/bin`: `update` fails with the production error; `--version` exits with `versionExit`. */
function fakeHome(opts: { versionExit?: number; updateOk?: boolean } = {}): string {
  const home = mkdtempSync(join(tmpdir(), "t3-provision-base-"));
  cleanups.push(() => rmSync(home, { recursive: true, force: true }));
  mkdirSync(join(home, ".local", "bin"), { recursive: true });
  const t3 = join(home, ".local", "bin", "t3");
  writeFileSync(
    t3,
    [
      "#!/bin/sh",
      'case "$1" in',
      `  --version) [ -f "$HOME/updated" ] && echo "t3 v0.1.15" || echo "t3 v0.1.14"; exit ${opts.versionExit ?? 0} ;;`,
      opts.updateOk
        ? '  update) touch "$HOME/updated"; echo "updated"; exit 0 ;;'
        : `  update) echo "resolving newest release"; echo "${UPDATE_ERROR}"; exit 1 ;;`,
      "esac",
      "exit 2",
      "",
    ].join("\n"),
  );
  chmodSync(t3, 0o755);
  return home;
}

/** Runs `body` after the real preamble, from a file so `$LINENO` counts lines of that file. */
function run(home: string, body: string, env: Record<string, string> = {}) {
  const file = join(home, "provision-snippet.sh");
  const source = `${preamble}mkdir -p "$sentinel_dir"\n${body}\n`;
  writeFileSync(file, source);
  const proc = Bun.spawnSync(["bash", file], { env: { ...process.env, HOME: home, ...env } });
  return { source, exitCode: proc.exitCode, stdout: proc.stdout.toString(), stderr: proc.stderr.toString() };
}

describe("provision-base.sh t3 update", () => {
  const step = `${versionStep}echo "restart=$restart_t3"`;

  test("a failed `t3 update` on a wake keeps the working t3 and shows the end of update.log", () => {
    const home = fakeHome();
    const r = run(home, step, { T3_VERSION: "latest", BAKE_CLEAN: "0" });
    expect(r.exitCode).toBe(0);
    expect(r.stdout.trim()).toBe("restart=0");
    expect(r.stderr).toContain("[provision] warning: t3 update failed; keeping t3 v0.1.14");
    expect(r.stderr).toContain(UPDATE_ERROR);
    expect(r.stderr).not.toContain("unexpected failure");
    expect(readFileSync(join(home, ".t3-sentinel", "update.log"), "utf8")).toContain(UPDATE_ERROR);
  });

  test("a bake fails loudly on a failed `t3 update`, with the end of update.log", () => {
    const home = fakeHome();
    const r = run(home, step, { T3_VERSION: "latest", BAKE_CLEAN: "1" });
    expect(r.exitCode).toBe(1);
    expect(r.stdout).toBe("");
    expect(r.stderr).toContain("[provision] t3 update failed; end of");
    expect(r.stderr).toContain(UPDATE_ERROR);
    expect(r.stderr).not.toContain("unexpected failure");
  });

  test("a wake fails when the update failed and the installed t3 no longer runs", () => {
    const home = fakeHome({ versionExit: 1 });
    const r = run(home, step, { T3_VERSION: "latest", BAKE_CLEAN: "0" });
    expect(r.exitCode).toBe(1);
    expect(r.stderr).toContain("[provision] t3 update failed; end of");
    expect(r.stderr).toContain(UPDATE_ERROR);
  });

  test("a successful update to a new version restarts T3 quietly", () => {
    const home = fakeHome({ updateOk: true });
    const r = run(home, step, { T3_VERSION: "latest", BAKE_CLEAN: "0" });
    expect(r.exitCode).toBe(0);
    expect(r.stdout.trim()).toBe("restart=1");
    expect(r.stderr).toBe("");
  });
});

describe("provision-base.sh ERR trap", () => {
  test("an unexpected failure prints its line, exit status and command to stderr", () => {
    const home = fakeHome();
    const r = run(home, 'cat "$HOME/no-such-file" >/dev/null 2>&1\necho unreachable');
    const line = r.source.split("\n").findIndex((l) => l.startsWith('cat "$HOME/no-such-file"')) + 1;
    expect(r.exitCode).toBe(1);
    expect(r.stdout).toBe("");
    // bash re-prints the command (`>/dev/null` becomes `> /dev/null`), so only its start is compared
    expect(r.stderr.trim()).toStartWith(`[provision] unexpected failure (exit 1) at line ${line}: cat "$HOME/no-such-file"`);
    expect(r.stderr.trim().split("\n")).toHaveLength(1);
  });

  test("handled failures and failing command substitutions stay quiet; a failure inside a function reports once", () => {
    const home = fakeHome();
    const r = run(
      home,
      [
        "false || true",
        "if false; then :; fi",
        'x="$(false || true)"',
        "f() { false; echo f-continued; }",
        "if f; then :; fi",
        "g() { local v; v=\"$(exit 3)\"; echo never; }",
        "g",
      ].join("\n"),
    );
    const line = r.source.split("\n").findIndex((l) => l.startsWith("g() {")) + 1;
    expect(r.exitCode).toBe(3);
    expect(r.stdout.trim()).toBe("f-continued");
    expect(r.stderr.trim().split("\n")).toEqual([`[provision] unexpected failure (exit 3) at line ${line}: v="$(exit 3)"`]);
  });

  test("a multi-line command is reported by its first line only", () => {
    const home = fakeHome();
    const r = run(home, "bash -c 'exit 4\n# second line of a long inline script'");
    expect(r.exitCode).toBe(4);
    expect(r.stderr.trim()).toMatch(/^\[provision\] unexpected failure \(exit 4\) at line \d+: bash -c 'exit 4$/);
  });

  test("the whole script still parses", () => {
    const proc = Bun.spawnSync(["bash", "-n", join(import.meta.dir, "..", "scripts", "provision-base.sh")]);
    expect(proc.stderr.toString()).toBe("");
    expect(proc.exitCode).toBe(0);
  });
});
