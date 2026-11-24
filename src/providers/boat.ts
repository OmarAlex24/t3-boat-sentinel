import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import type {
  CreateSandboxOptions,
  ExecResult,
  ExposedEndpoint,
  LegacyBoatClient,
  ProviderCapability,
  ResumeResult,
  SandboxInfo,
  SandboxProvider,
} from "./types.ts";

export interface BoatProviderOptions {
  bin?: string;
  /** Default port visibility when exposing T3 server ("public" or "private" token/cookie-gated). */
  hostVisibility?: "public" | "private";
}

/**
 * Native Boat (`boat.dev`) provider using the official `boat` CLI (`--json` mode, `BOAT_NO_UPDATE=1`).
 * Supports full 2-tier immutable named snapshots, pause/resume, and private or public hosted ports.
 * Uses the host's existing Boat login (`boat onboard`); never stores Boat credentials in Sentinel.
 */
export class BoatProvider implements SandboxProvider, LegacyBoatClient {
  readonly name = "boat";
  readonly capabilities: ReadonlySet<ProviderCapability> = new Set<ProviderCapability>([
    "pause",
    "snapshots",
    "private-preview",
  ]);
  readonly bin: string;
  readonly hostVisibility: "public" | "private";

  constructor(explicitBinOrOpts?: string | BoatProviderOptions) {
    if (typeof explicitBinOrOpts === "string") {
      this.bin = explicitBinOrOpts;
      this.hostVisibility = "public";
    } else {
      this.bin = explicitBinOrOpts?.bin ?? BoatProvider.locate();
      this.hostVisibility = explicitBinOrOpts?.hostVisibility ?? "public";
    }
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

  async create(optsOrName: CreateSandboxOptions | string, legacySize?: string, legacyFrom?: string): Promise<string> {
    const opts: CreateSandboxOptions =
      typeof optsOrName === "string"
        ? { name: optsOrName, size: legacySize ?? "default", fromSnapshot: legacyFrom }
        : optsOrName;
    const args = ["new", "--name", opts.name, "--type", opts.size, "--no-auto-stop"];
    if (opts.fromSnapshot) args.push("--from", opts.fromSnapshot);
    const out = await this.run(args, 300_000);
    const v = BoatProvider.lastJson<{ id?: string }>(out);
    if (!v.id) throw new Error("boat new returned no id");
    return v.id;
  }

  async info(id: string): Promise<SandboxInfo> {
    const out = await this.run(["info", id], 20_000);
    const v = BoatProvider.lastJson<{ sandbox: SandboxInfo }>(out);
    return v.sandbox;
  }

  async list(): Promise<SandboxInfo[]> {
    const out = await this.run(["list", "--all"], 20_000);
    const v = BoatProvider.lastJson<{ sandboxes?: SandboxInfo[] }>(out);
    return v.sandboxes ?? [];
  }

  async resume(id: string, size: string): Promise<ResumeResult> {
    const res = await this.output(["resume", id, "--no-auto-stop", "--type", size], 300_000);
    if (res.ok) return "resumed";
    const combined = `${res.stderr}\n${res.stdout}`;
    if (combined.includes("snapshot_data_expired")) return "data_expired";
    throw new Error(`boat resume failed: ${(res.stderr.trim() || res.stdout.trim()).slice(0, 600)}`);
  }

  async pause(id: string): Promise<void> {
    await this.run(["stop", id], 90_000);
  }

  async stop(id: string): Promise<void> {
    return this.pause(id);
  }

  async delete(id: string): Promise<void> {
    await this.run(["delete", id, "--yes"], 120_000);
  }

  async saveSnapshot(id: string, name: string): Promise<void> {
    await this.run(["snapshot", id, name], 10 * 60_000);
  }

  async listSnapshots(): Promise<string[]> {
    const out = await this.run(["snapshots"], 20_000);
    const v = BoatProvider.lastJson<{ named?: Array<{ name: string }> }>(out);
    return (v.named ?? []).map((n) => n.name);
  }

  async namedSnapshots(): Promise<string[]> {
    return this.listSnapshots();
  }

  async hostPublic(id: string, port: number): Promise<string> {
    const ep = await this.exposePort(id, port, { visibility: "public" });
    return ep.url;
  }

  async exposePort(
    id: string,
    port: number,
    opts?: { visibility?: "private" | "public" },
  ): Promise<ExposedEndpoint> {
    const visibility = opts?.visibility ?? this.hostVisibility;
    const flag = visibility === "private" ? "--private" : "--public";
    const out = await this.run(["host", id, String(port), flag, "--localhost"], 60_000);
    const v = BoatProvider.lastJson<{
      url?: string;
      headers?: Record<string, string>;
      cookie?: string;
      token?: string;
    }>(out);
    if (!v.url) throw new Error("boat host returned no url");

    if (visibility === "public") {
      return { url: v.url.replace(/\/+$/, "") };
    }

    const headers: Record<string, string> = { ...(v.headers ?? {}) };
    if (v.cookie && !headers.cookie && !headers.Cookie) {
      headers.cookie = v.cookie;
    }

    // If Boat returned a token-gated redirect URL (`https://...?token=...`), exchange it once to capture the gate Cookie
    const parsedUrl = new URL(v.url);
    if (Object.keys(headers).length === 0 && parsedUrl.search) {
      try {
        const probe = await fetch(v.url, { method: "GET", redirect: "manual", signal: AbortSignal.timeout(15_000) });
        const setCookie = probe.headers.get("set-cookie");
        if (setCookie) {
          const cookiePair = setCookie
            .split(/,(?=[^;]+=[^;]+)/)
            .map((part) => part.split(";")[0]?.trim())
            .filter(Boolean)
            .join("; ");
          if (cookiePair) headers.cookie = cookiePair;
        }
      } catch (err) {
        console.warn(`[boat] private preview cookie exchange failed for ${id}, falling back to raw URL:`, err);
      }
    }

    const baseUrl = `${parsedUrl.origin}${parsedUrl.pathname}`.replace(/\/+$/, "");
    return Object.keys(headers).length > 0 ? { url: baseUrl, headers } : { url: v.url.replace(/\/+$/, "") };
  }

  async exec(id: string, script: string, timeoutSecs = 120): Promise<ExecResult> {
    const res = await this.output(
      ["exec", id, "--timeout", String(timeoutSecs), "--", "bash", "-lc", script],
      (timeoutSecs + 60) * 1000,
    );
    try {
      const v = BoatProvider.lastJson<{ exitCode?: number | null; stdout?: string; stderr?: string }>(res.stdout);
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
