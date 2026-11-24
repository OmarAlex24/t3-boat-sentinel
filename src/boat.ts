import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

export interface BoatSandbox {
  id: string;
  name?: string;
  state: string;
  stoppedAt?: string | null;
}

export interface BoatExecResult {
  exitCode: number | null;
  stdout: string;
  stderr: string;
}

export type ResumeResult = "resumed" | "data_expired";

export interface BoatClient {
  create(name: string, size: string, fromSnapshot?: string): Promise<string>;
  info(id: string): Promise<BoatSandbox>;
  list(): Promise<BoatSandbox[]>;
  resume(id: string, size: string): Promise<ResumeResult>;
  stop(id: string): Promise<void>;
  delete(id: string): Promise<void>;
  saveSnapshot(id: string, name: string): Promise<void>;
  namedSnapshots(): Promise<string[]>;
  hostPublic(id: string, port: number): Promise<string>;
  exec(id: string, script: string, timeoutSecs?: number): Promise<BoatExecResult>;
  execOk(id: string, script: string, timeoutSecs?: number): Promise<string>;
  upload(id: string, localPath: string, remotePath: string): Promise<void>;
}

/**
 * Thin wrapper around the official `boat` CLI (`--json` mode, `BOAT_NO_UPDATE=1`).
 * Uses the user's existing Boat login (`boat onboard`); never stores Boat credentials.
 */
export class BoatCli implements BoatClient {
  readonly bin: string;

  constructor(explicitBin?: string) {
    this.bin = explicitBin ?? BoatCli.locate();
  }

  static locate(): string {
    const installed = join(homedir(), ".ascii", "bin", "boat");
    if (existsSync(installed)) return installed;
    const onPath = Bun.which("boat");
    if (onPath) return onPath;
    return "boat";
  }

  private async output(args: string[], timeoutMs: number): Promise<{ ok: boolean; stdout: string; stderr: string }> {
    const sub = args[0] ?? "";
    const proc = Bun.spawn([this.bin, "--json", ...args], {
      stdout: "pipe",
      stderr: "pipe",
      env: { ...process.env, BOAT_NO_UPDATE: "1" },
    });

    const timer = setTimeout(() => {
      try {
        proc.kill("SIGKILL");
      } catch {
        // ignore
      }
    }, timeoutMs);

    try {
      const [stdout, stderr, exitCode] = await Promise.all([
        new Response(proc.stdout).text(),
        new Response(proc.stderr).text(),
        proc.exited,
      ]);
      return { ok: exitCode === 0, stdout, stderr };
    } catch (err) {
      throw new Error(`boat ${sub} failed: ${String(err)}`);
    } finally {
      clearTimeout(timer);
    }
  }

  private async run(args: string[], timeoutMs: number): Promise<string> {
    const res = await this.output(args, timeoutMs);
    if (!res.ok) {
      const sub = args[0] ?? "";
      const detail = (res.stderr.trim() || res.stdout.trim()).slice(0, 600);
      throw new Error(`boat ${sub} failed: ${detail}`);
    }
    return res.stdout;
  }

  private static lastJson<T = Record<string, unknown>>(stdout: string): T {
    const lines = stdout.split(/\r?\n/);
    for (let i = lines.length - 1; i >= 0; i--) {
      const trimmed = lines[i].trimStart();
      if (trimmed.startsWith("{")) {
        return JSON.parse(trimmed) as T;
      }
    }
    throw new Error(`boat printed no JSON: ${stdout.slice(0, 400)}`);
  }

  async create(name: string, size: string, fromSnapshot?: string): Promise<string> {
    const args = ["new", "--name", name, "--type", size, "--no-auto-stop"];
    if (fromSnapshot) args.push("--from", fromSnapshot);
    const out = await this.run(args, 300_000);
    const v = BoatCli.lastJson<{ id?: string }>(out);
    if (!v.id) throw new Error("boat new returned no id");
    return v.id;
  }

  async info(id: string): Promise<BoatSandbox> {
    const out = await this.run(["info", id], 20_000);
    const v = BoatCli.lastJson<{ sandbox: BoatSandbox }>(out);
    return v.sandbox;
  }

  async list(): Promise<BoatSandbox[]> {
    const out = await this.run(["list", "--all"], 20_000);
    const v = BoatCli.lastJson<{ sandboxes?: BoatSandbox[] }>(out);
    return v.sandboxes ?? [];
  }

  async resume(id: string, size: string): Promise<ResumeResult> {
    const res = await this.output(["resume", id, "--no-auto-stop", "--type", size], 300_000);
    if (res.ok) return "resumed";
    const combined = `${res.stderr}\n${res.stdout}`;
    if (combined.includes("snapshot_data_expired")) return "data_expired";
    throw new Error(`boat resume failed: ${(res.stderr.trim() || res.stdout.trim()).slice(0, 600)}`);
  }

  async stop(id: string): Promise<void> {
    await this.run(["stop", id], 90_000);
  }

  async delete(id: string): Promise<void> {
    await this.run(["delete", id, "--yes"], 120_000);
  }

  async saveSnapshot(id: string, name: string): Promise<void> {
    await this.run(["snapshot", id, name], 10 * 60_000);
  }

  async namedSnapshots(): Promise<string[]> {
    const out = await this.run(["snapshots"], 20_000);
    const v = BoatCli.lastJson<{ named?: Array<{ name: string }> }>(out);
    return (v.named ?? []).map((n) => n.name);
  }

  async hostPublic(id: string, port: number): Promise<string> {
    const out = await this.run(["host", id, String(port), "--public", "--localhost"], 60_000);
    const v = BoatCli.lastJson<{ url?: string }>(out);
    if (!v.url) throw new Error("boat host returned no url");
    return v.url.replace(/\/+$/, "");
  }

  async exec(id: string, script: string, timeoutSecs = 120): Promise<BoatExecResult> {
    const res = await this.output(
      ["exec", id, "--timeout", String(timeoutSecs), "--", "bash", "-lc", script],
      (timeoutSecs + 60) * 1000,
    );
    try {
      const v = BoatCli.lastJson<{ exitCode?: number | null; stdout?: string; stderr?: string }>(res.stdout);
      return {
        exitCode: v.exitCode ?? null,
        stdout: v.stdout ?? "",
        stderr: v.stderr ?? "",
      };
    } catch {
      throw new Error(`boat exec failed: ${(res.stderr.trim() || res.stdout.trim()).slice(0, 600)}`);
    }
  }

  async execOk(id: string, script: string, timeoutSecs = 120): Promise<string> {
    const r = await this.exec(id, script, timeoutSecs);
    if (r.exitCode !== 0) {
      const detail = (r.stderr.trim() || r.stdout.trim()).slice(0, 600);
      throw new Error(`remote command failed (exit ${r.exitCode}): ${detail}`);
    }
    return r.stdout;
  }

  async upload(id: string, localPath: string, remotePath: string): Promise<void> {
    await this.run(["scp", localPath, `${id}:${remotePath}`], 1800_000);
  }
}
