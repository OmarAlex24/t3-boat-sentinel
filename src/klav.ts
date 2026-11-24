import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { BoatClient } from "./boat.ts";
import type { KlavGlobalConfig, ProjectKlavConfig } from "./config.ts";

export interface KlavProvisionResult {
  /** In proxy mode: Klav `sandbox_id`; in agent mode: ephemeral agent name (`boat-<sandboxId>`). */
  agentName: string;
  project: string;
}

export interface KlavClient {
  provisionForSandbox(
    boat: BoatClient,
    sandboxId: string,
    projectKlav: ProjectKlavConfig,
    remoteHome?: string,
  ): Promise<KlavProvisionResult>;
  revokeForSandbox(handle: string): Promise<void>;
}

/**
 * Integrates Klav (https://klav.dev) zero-knowledge secrets into disposable Boat VMs.
 *
 * Supports two modes:
 * 1. `mode: "proxy"` (RECOMMENDED — ADR 0036 §4, zero master password on sentinel!):
 *    - Runs on the same host as `klav-proxy serve` (default 127.0.0.1:8790).
 *    - Calls `klav sandbox start --label ... --profile ... --package-file <tmp> --json`.
 *    - Uploads the one-time `ksb1_...` package into `/run/klav-sandbox.pkg` (tmpfs) inside Boat,
 *      runs `klav-forward down --if-other-package && klav-forward up --daemon`, deletes `/run/klav-sandbox.pkg`,
 *      and writes forwarder URLs to `~/.klav-forward/env.sh`. Real API keys NEVER leave `klav-proxy`!
 *    - On archive/reset: calls `klav sandbox stop <sandbox_id> --json`.
 *
 * 2. `mode: "agent"` (fallback when not running `klav-proxy`):
 *    - Registers an ephemeral Ed25519+X25519 agent (`boat-<sandboxId>`), approves it scoped to one Klav
 *      project via `--password-stdin`, uploads `.json`+`.key` (`0600`), configures `klav mcp`, and
 *      revokes it via `klav agent revoke --yes --json` on destroy.
 */
export class KlavCliProvider implements KlavClient {
  private readonly bin: string;

  constructor(private readonly cfg: KlavGlobalConfig) {
    this.bin = cfg.bin ?? Bun.which("klav") ?? "/usr/local/bin/klav";
  }

  private async runKlav(
    args: string[],
    opts: { klavHome?: string; stdin?: string; timeoutMs?: number } = {},
  ): Promise<{ stdout: string; stderr: string }> {
    const env: Record<string, string | undefined> = {
      ...process.env,
      KLAV_NO_DAEMON: "1",
    };
    if (opts.klavHome) env.KLAV_HOME = opts.klavHome;

    const proc = Bun.spawn([this.bin, ...args], {
      stdin: opts.stdin !== undefined ? "pipe" : "ignore",
      stdout: "pipe",
      stderr: "pipe",
      env,
    });

    if (opts.stdin !== undefined && proc.stdin) {
      proc.stdin.write(opts.stdin);
      proc.stdin.end();
    }

    const timeoutMs = opts.timeoutMs ?? 60_000;
    const timer = setTimeout(() => {
      try {
        proc.kill("SIGKILL");
      } catch {
        // ignore
      }
    }, timeoutMs);

    try {
      const [stdout, stderr, code] = await Promise.all([
        new Response(proc.stdout).text(),
        new Response(proc.stderr).text(),
        proc.exited,
      ]);
      if (code !== 0) {
        const detail = (stderr.trim() || stdout.trim()).slice(0, 500);
        throw new Error(`klav ${args[0] ?? ""} failed (exit ${code}): ${detail}`);
      }
      return { stdout, stderr };
    } finally {
      clearTimeout(timer);
    }
  }

  async provisionForSandbox(
    boat: BoatClient,
    sandboxId: string,
    projectKlav: ProjectKlavConfig,
    remoteHome = "/home/user",
  ): Promise<KlavProvisionResult> {
    if (this.cfg.mode === "proxy") {
      return this.provisionProxySandbox(boat, sandboxId, projectKlav, remoteHome);
    }
    return this.provisionEphemeralAgent(boat, sandboxId, projectKlav, remoteHome);
  }

  async revokeForSandbox(handle: string): Promise<void> {
    if (this.cfg.mode === "proxy") {
      const args = ["sandbox", "stop", handle, "--proxy-port", String(this.cfg.proxyPort), "--json"];
      if (this.cfg.issuerAgent) args.push("--agent", this.cfg.issuerAgent);
      await this.runKlav(args, { klavHome: this.cfg.issuerHome });
      console.log(`[klav-proxy] stopped sandbox "${handle}"`);
      return;
    }

    await this.runKlav(
      ["--no-daemon", "--password-stdin", "agent", "revoke", handle, "--yes", "--json"],
      {
        klavHome: this.cfg.accountHome,
        stdin: `${this.cfg.masterPassword}\n`,
      },
    );
    console.log(`[klav] revoked ephemeral agent "${handle}"`);
  }

  private async provisionProxySandbox(
    boat: BoatClient,
    sandboxId: string,
    projectKlav: ProjectKlavConfig,
    remoteHome: string,
  ): Promise<KlavProvisionResult> {
    const profiles = projectKlav.profiles ?? [];
    if (profiles.length === 0) {
      throw new Error("Klav proxy mode requires at least one profile in `project.klav.profiles`");
    }
    const ttl = projectKlav.ttl ?? this.cfg.defaultTtl;
    const tempDir = mkdtempSync(join(tmpdir(), "t3-sentinel-klav-pkg-"));
    const pkgPath = join(tempDir, "sandbox.pkg"); // must NOT exist before `klav sandbox start --package-file`

    try {
      const args = [
        "sandbox",
        "start",
        "--label",
        `t3-${sandboxId}`,
        "--ttl",
        ttl,
        "--package-file",
        pkgPath,
        "--proxy-port",
        String(this.cfg.proxyPort),
        "--json",
      ];
      if (this.cfg.issuerAgent) args.push("--agent", this.cfg.issuerAgent);
      for (const p of profiles) {
        args.push("--profile", p);
      }

      const res = await this.runKlav(args, { klavHome: this.cfg.issuerHome });
      const parsed = JSON.parse(res.stdout.trim()) as { sandbox_id?: string; setup?: string[] };
      const klavSandboxId = parsed.sandbox_id;
      if (!klavSandboxId) {
        throw new Error(`klav sandbox start returned no sandbox_id: ${res.stdout.slice(0, 300)}`);
      }

      // Upload one-time package into tmpfs (/run/klav-sandbox.pkg), claim with klav-forward, and delete immediately!
      await boat.upload(sandboxId, pkgPath, "/run/klav-sandbox.pkg");
      const setupLines = parsed.setup ?? [
        "klav-forward down --if-other-package",
        "klav-forward up --daemon",
        'eval "$(klav-forward env)"',
      ];
      await boat.execOk(
        sandboxId,
        [
          `set -euo pipefail`,
          `chmod 600 /run/klav-sandbox.pkg`,
          `KLAV_SANDBOX="$(cat /run/klav-sandbox.pkg)"`,
          `rm -f /run/klav-sandbox.pkg`,
          `export KLAV_SANDBOX`,
          ...setupLines,
          `mkdir -p ${remoteHome}/.klav-forward`,
          `klav-forward env > ${remoteHome}/.klav-forward/env.sh`,
          `chmod 600 ${remoteHome}/.klav-forward/env.sh`,
          `grep -q 'klav-forward/env.sh' ${remoteHome}/.bashrc 2>/dev/null || echo '[ -f "$HOME/.klav-forward/env.sh" ] && source "$HOME/.klav-forward/env.sh"' >> ${remoteHome}/.bashrc`,
          `unset KLAV_SANDBOX`,
        ].join("\n"),
        60,
      );

      console.log(`[klav-proxy] started sandbox "${klavSandboxId}" (profiles=${profiles.join(",")}) for Boat ${sandboxId}`);
      return { agentName: klavSandboxId, project: profiles.join(",") };
    } finally {
      rmSync(tempDir, { recursive: true, force: true });
    }
  }

  private async provisionEphemeralAgent(
    boat: BoatClient,
    sandboxId: string,
    projectKlav: ProjectKlavConfig,
    remoteHome: string,
  ): Promise<KlavProvisionResult> {
    if (!projectKlav.project) {
      throw new Error("Klav agent mode requires `project.klav.project`");
    }
    const agentName = `boat-${sandboxId}`;
    const tempHome = mkdtempSync(join(tmpdir(), "t3-sentinel-klav-"));

    try {
      const reg = await this.runKlav(
        [
          "agent",
          "register",
          "--name",
          agentName,
          "--server",
          this.cfg.server!,
          "--email",
          this.cfg.email!,
          "--note",
          `t3-boat-sentinel sandbox ${sandboxId} (${projectKlav.project})`,
          "--key-store",
          "file",
        ],
        { klavHome: tempHome },
      );
      const words = reg.stdout.trim().replace(/\s+/g, " ");
      if (!words) throw new Error("klav agent register returned no pairing words");

      const allow = (projectKlav.allow ?? this.cfg.defaultAllow).join(",");
      const deny = (projectKlav.deny ?? this.cfg.defaultDeny).join(",");
      const expires = projectKlav.expires ?? this.cfg.defaultExpires;

      const approveArgs = [
        "--no-daemon",
        "--password-stdin",
        "agent",
        "approve",
        agentName,
        "--project",
        projectKlav.project,
        "--allow",
        allow,
        "--deny",
        deny,
        "--expires",
        expires,
        "--words",
        words,
      ];
      if (projectKlav.policyFile) {
        approveArgs.push("--policy-file", projectKlav.policyFile);
      }

      await this.runKlav(approveArgs, {
        klavHome: this.cfg.accountHome,
        stdin: `${this.cfg.masterPassword}\n`,
      });

      await this.runKlav(["agent", "status", "--json"], { klavHome: tempHome }).catch((err) => {
        console.warn(`[klav] non-fatal TOFU status pin failed for ${agentName}:`, err);
      });

      const localJsonPath = join(tempHome, "agents", `${agentName}.json`);
      const localKeyPath = join(tempHome, "agents", `${agentName}.key`);
      if (!existsSync(localJsonPath) || !existsSync(localKeyPath)) {
        throw new Error(`Expected Klav agent files missing in ${tempHome}/agents`);
      }

      const remoteAgentsDir = `${remoteHome}/.config/klav/agents`;
      const remoteJsonPath = `${remoteAgentsDir}/${agentName}.json`;
      const remoteKeyPath = `${remoteAgentsDir}/${agentName}.key`;

      const agentMeta = JSON.parse(readFileSync(localJsonPath, "utf8")) as Record<string, unknown>;
      if (agentMeta.key_store && typeof agentMeta.key_store === "object") {
        (agentMeta.key_store as Record<string, unknown>).path = remoteKeyPath;
      }
      writeFileSync(localJsonPath, JSON.stringify(agentMeta, null, 2), { mode: 0o600 });

      await boat.execOk(sandboxId, `mkdir -p ${remoteAgentsDir} && chmod 700 ${remoteHome}/.config/klav ${remoteAgentsDir}`, 30);
      await boat.upload(sandboxId, localJsonPath, remoteJsonPath);
      await boat.upload(sandboxId, localKeyPath, remoteKeyPath);
      await boat.execOk(
        sandboxId,
        [
          `chmod 600 ${remoteJsonPath} ${remoteKeyPath}`,
          `KLAV_AGENT=${agentName} node -e '`,
          `const fs = require("fs"), os = require("os"), path = require("path");`,
          `const agent = process.env.KLAV_AGENT;`,
          `const claudeFile = path.join(os.homedir(), ".claude.json");`,
          `const claude = fs.existsSync(claudeFile) ? JSON.parse(fs.readFileSync(claudeFile, "utf8")) : {};`,
          `claude.mcpServers = { ...(claude.mcpServers || {}), klav: { command: "klav", args: ["mcp"], env: { KLAV_AGENT: agent } } };`,
          `fs.writeFileSync(claudeFile, JSON.stringify(claude, null, 2), { mode: 0o600 });`,
          `'`,
        ].join("\n"),
        30,
      );

      console.log(`[klav] provisioned ephemeral agent "${agentName}" for project "${projectKlav.project}"`);
      return { agentName, project: projectKlav.project };
    } finally {
      rmSync(tempHome, { recursive: true, force: true });
    }
  }
}
