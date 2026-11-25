import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { SandboxProvider } from "./providers/types.ts";
import type { KlavGlobalConfig, ProjectKlavConfig } from "./config.ts";

export type KlavSandboxTransport = Pick<SandboxProvider, "upload" | "execOk">;

export interface KlavProvisionResult {
  /** In proxy mode: Klav `sandbox_id`; in agent mode: ephemeral agent name (`boat-<sandboxId>`). */
  agentName: string;
  project: string;
}

export interface KlavClient {
  provisionForSandbox(
    boat: KlavSandboxTransport,
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
    boat: KlavSandboxTransport,
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

  private async mintTailscaleSandboxKey(): Promise<string> {
    const file = this.cfg.tailscaleOauthFile!;
    const raw = readFileSync(file, "utf8");
    let clientId = "";
    let clientSecret = "";
    for (const line of raw.split(/\r?\n/)) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) continue;
      const eq = trimmed.indexOf("=");
      if (eq <= 0) continue;
      const k = trimmed.slice(0, eq).trim();
      const v = trimmed.slice(eq + 1).trim();
      if (k === "client_id") clientId = v;
      if (k === "client_secret") clientSecret = v;
    }
    if (!clientId || !clientSecret) {
      throw new Error(`Invalid Tailscale OAuth file at ${file}: expected client_id=... and client_secret=...`);
    }

    const tokenRes = await fetch("https://api.tailscale.com/api/v2/oauth/token", {
      method: "POST",
      headers: { "content-type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({ client_id: clientId, client_secret: clientSecret }).toString(),
      signal: AbortSignal.timeout(20_000),
    });
    if (!tokenRes.ok) {
      throw new Error(`Tailscale OAuth token exchange failed (HTTP ${tokenRes.status})`);
    }
    const tokenJson = (await tokenRes.json()) as { access_token?: string };
    if (!tokenJson.access_token) {
      throw new Error("Tailscale OAuth token response missing access_token");
    }

    const tailnet = encodeURIComponent(this.cfg.tailnetName || "-");
    const keyRes = await fetch(`https://api.tailscale.com/api/v2/tailnet/${tailnet}/keys`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${tokenJson.access_token}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({
        keyType: "auth",
        description: "t3-boat-sentinel klav sandbox",
        expirySeconds: 3600,
        capabilities: {
          devices: {
            create: {
              reusable: false,
              ephemeral: true,
              preauthorized: true,
              tags: ["tag:klav-sandbox"],
            },
          },
        },
      }),
      signal: AbortSignal.timeout(20_000),
    });
    if (!keyRes.ok) {
      throw new Error(`Tailscale auth key creation failed (HTTP ${keyRes.status})`);
    }
    const keyJson = (await keyRes.json()) as { key?: string };
    if (!keyJson.key || keyJson.key.startsWith("tskey-client-")) {
      throw new Error("Tailscale returned invalid auth key");
    }
    return keyJson.key;
  }

  private async provisionProxySandbox(
    boat: KlavSandboxTransport,
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
    let klavSandboxId: string | null = null;

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
      klavSandboxId = parsed.sandbox_id ?? null;
      if (!klavSandboxId) {
        throw new Error(`klav sandbox start returned no sandbox_id: ${res.stdout.slice(0, 300)}`);
      }

      const shortId = klavSandboxId.replace(/[^a-z0-9]/gi, "").slice(0, 8).toLowerCase() || "sandbox";
      const tailnetSteps: string[] = [];
      if (this.cfg.tailscaleOauthFile) {
        const tsKey = await this.mintTailscaleSandboxKey();
        const tsKeyPath = join(tempDir, "tailnet.key");
        writeFileSync(tsKeyPath, tsKey, { mode: 0o600 });
        await boat.upload(sandboxId, tsKeyPath, "/dev/shm/klav-tailnet.key");
        tailnetSteps.push(
          `chmod 600 /dev/shm/klav-tailnet.key`,
          `sudo -n /usr/local/sbin/klav-tailnet-up "klav-${shortId}" < /dev/shm/klav-tailnet.key`,
          `rm -f /dev/shm/klav-tailnet.key`,
        );
      }

      // Upload one-time package into tmpfs (/dev/shm), claim with klav-forward, and delete immediately!
      await boat.upload(sandboxId, pkgPath, "/dev/shm/klav-sandbox.pkg");
      const setupLines = parsed.setup ?? [
        "klav-forward down --if-other-package",
        "klav-forward up --daemon",
        'eval "$(klav-forward env)"',
      ];
      await boat.execOk(
        sandboxId,
        [
          `set -euo pipefail`,
          `trap 'rm -f /dev/shm/klav-sandbox.pkg /dev/shm/klav-tailnet.key' EXIT`,
          ...tailnetSteps,
          `chmod 600 /dev/shm/klav-sandbox.pkg`,
          `KLAV_SANDBOX="$(cat /dev/shm/klav-sandbox.pkg)"`,
          `rm -f /dev/shm/klav-sandbox.pkg`,
          `export KLAV_SANDBOX`,
          ...setupLines,
          `unset KLAV_SANDBOX`,
          `mkdir -p ${remoteHome}/.klav-forward`,
          `klav-forward env > ${remoteHome}/.klav-forward/env.sh`,
          `chmod 600 ${remoteHome}/.klav-forward/env.sh`,
          `sed -E "s/^export //; s/'//g" ${remoteHome}/.klav-forward/env.sh > ${remoteHome}/.klav-forward/env.systemd`,
          `chmod 600 ${remoteHome}/.klav-forward/env.systemd`,
          `grep -q 'klav-forward/env.sh' ${remoteHome}/.bashrc 2>/dev/null || echo '[ -f "$HOME/.klav-forward/env.sh" ] && source "$HOME/.klav-forward/env.sh"' >> ${remoteHome}/.bashrc`,
        ].join("\n"),
        90,
      );

      console.log(`[klav-proxy] started sandbox "${klavSandboxId}" (profiles=${profiles.join(",")}) for Boat ${sandboxId}`);
      return { agentName: klavSandboxId, project: profiles.join(",") };
    } catch (err) {
      if (klavSandboxId) {
        await this.revokeForSandbox(klavSandboxId).catch(() => {});
      }
      throw err;
    } finally {
      rmSync(tempDir, { recursive: true, force: true });
    }
  }

  private async provisionEphemeralAgent(
    boat: KlavSandboxTransport,
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

/**
 * Port `klav-forward` answers on inside the VM. Not `KlavGlobalConfig.proxyPort`: that is the port of
 * `klav-proxy serve` on the issuer host.
 */
export const KLAV_FORWARD_PORT = 8790;

/** What one probe of the VM found; collected by a script, judged here so the decision is testable. */
export interface ForwarderFacts {
  /** Exit code of `klav-forward down`; `null` when it was not run or `klav-forward` is missing. */
  downExit: number | null;
  /** Something listens on 127.0.0.1:{@link KLAV_FORWARD_PORT}. */
  listening: boolean;
  /** PID recorded in `~/.klav-forward/state.json`, when one is there. */
  statePid: number | null;
  /** `/proc/<statePid>/comm`, when that process exists. */
  comm: string | null;
  /** `statePid` holds the listening socket on 127.0.0.1:{@link KLAV_FORWARD_PORT}. */
  pidOwnsListener: boolean;
}

export type ForwarderVerdict =
  | { action: "none" }
  | { action: "stop"; pid: number }
  | { action: "refuse"; reason: string };

const FORWARD_PORT_HEX = KLAV_FORWARD_PORT.toString(16).toUpperCase().padStart(4, "0");

/** Shell lines that set `inodes` to the socket inodes listening on 127.0.0.1:8790 (no `ss` needed). */
const LISTENER_INODES = [
  `inodes="$(awk 'NR>1 && toupper($2)=="0100007F:${FORWARD_PORT_HEX}" && $4=="0A" {print $10}' /proc/net/tcp 2>/dev/null)"`,
];

/** Shell lines that set `owns=1` when `$pid` holds one of `$inodes`. */
const PID_OWNS_LISTENER = [
  `owns=0`,
  `for ino in $inodes; do`,
  `  if ls -l "/proc/$pid/fd" 2>/dev/null | grep -qF "socket:[$ino]"; then owns=1; fi`,
  `done`,
];

/**
 * Runs `klav-forward down` (bounded, output discarded: it never carries the package or the env), then
 * prints only facts as `key=value` lines. Never prints the state file, the package or `klav-forward env`.
 */
export function forwarderProbeScript(downTimeoutSecs = 20): string {
  return [
    `# t3s:klav-forward-probe`,
    `set +e`,
    `if command -v klav-forward >/dev/null 2>&1; then`,
    `  if command -v timeout >/dev/null 2>&1; then timeout ${downTimeoutSecs} klav-forward down >/dev/null 2>&1; else klav-forward down >/dev/null 2>&1; fi`,
    `  echo "down_exit=$?"`,
    `fi`,
    ...LISTENER_INODES,
    `if [ -n "$inodes" ]; then echo "listening=1"; else echo "listening=0"; fi`,
    // The forwarder's own PID is the top-level "pid" key (seen on production VMs)
    `pid="$(grep -oE '"pid"[[:space:]]*:[[:space:]]*[0-9]+' "$HOME/.klav-forward/state.json" 2>/dev/null | head -n1 | grep -oE '[0-9]+$')"`,
    `echo "state_pid=$pid"`,
    `if [ -n "$pid" ] && [ -d "/proc/$pid" ]; then`,
    `  echo "comm=$(cat "/proc/$pid/comm" 2>/dev/null)"`,
    ...PID_OWNS_LISTENER.map((l) => `  ${l}`),
    `  echo "owns_listener=$owns"`,
    `fi`,
    `exit 0`,
  ].join("\n");
}

export function parseForwarderProbe(stdout: string): ForwarderFacts {
  const kv = new Map<string, string>();
  for (const line of stdout.split(/\r?\n/)) {
    const m = line.trim().match(/^([a-z_]+)=(.*)$/);
    if (m) kv.set(m[1]!, m[2]!.trim());
  }
  const int = (v: string | undefined) => (v !== undefined && /^\d+$/.test(v) ? Number(v) : null);
  return {
    downExit: int(kv.get("down_exit")),
    listening: kv.get("listening") === "1",
    statePid: int(kv.get("state_pid")),
    comm: kv.get("comm") || null,
    pidOwnsListener: kv.get("owns_listener") === "1",
  };
}

/**
 * Stops a forwarder only when it is provably the one the Hub started: the PID from its state file is a
 * `klav-forward` process that itself listens on 127.0.0.1:8790. Anything else on the port is left alone.
 */
export function judgeForwarder(f: ForwarderFacts): ForwarderVerdict {
  if (!f.listening) return { action: "none" };
  if (f.statePid === null || f.statePid <= 1) {
    return { action: "refuse", reason: `port ${KLAV_FORWARD_PORT} is in use and ~/.klav-forward/state.json names no process` };
  }
  if (f.comm !== "klav-forward") {
    return { action: "refuse", reason: `port ${KLAV_FORWARD_PORT} is in use and the recorded PID is not a klav-forward process` };
  }
  if (!f.pidOwnsListener) {
    return { action: "refuse", reason: `port ${KLAV_FORWARD_PORT} is held by another process than the recorded klav-forward` };
  }
  return { action: "stop", pid: f.statePid };
}

/** Re-checks the PID right before signalling it (it may have exited or been reused), then TERM, then KILL. */
export function forwarderStopScript(pid: number, termTimeoutSecs = 10): string {
  if (!Number.isInteger(pid) || pid <= 1) throw new Error(`refusing to stop PID ${pid}`);
  const polls = Math.max(1, Math.round(termTimeoutSecs * 5));
  return [
    `# t3s:klav-forward-stop`,
    `set +e`,
    `pid=${pid}`,
    `if [ "$(cat "/proc/$pid/comm" 2>/dev/null)" != "klav-forward" ]; then echo "stop=mismatch"; exit 0; fi`,
    ...LISTENER_INODES,
    ...PID_OWNS_LISTENER,
    `if [ "$owns" != "1" ]; then echo "stop=mismatch"; exit 0; fi`,
    `kill -TERM "$pid" 2>/dev/null`,
    `for _ in $(seq 1 ${polls}); do [ -d "/proc/$pid" ] || { echo "stop=term"; exit 0; }; sleep 0.2; done`,
    `kill -KILL "$pid" 2>/dev/null`,
    `for _ in $(seq 1 25); do [ -d "/proc/$pid" ] || { echo "stop=kill"; exit 0; }; sleep 0.2; done`,
    `echo "stop=alive"`,
    `exit 0`,
  ].join("\n");
}

export type ForwarderReleaseOutcome = "clear" | "stopped";

/**
 * Leaves 127.0.0.1:8790 free in a VM: `klav-forward down`, and when a leftover forwarder still answers
 * because `down` cannot reach it (another version's control socket), stops it itself, but only after
 * {@link judgeForwarder} verified it. Throws, with a message free of secrets, when the port stays taken.
 */
export async function releaseVmForwarder(
  boat: Pick<SandboxProvider, "exec">,
  sandboxId: string,
  opts: { termTimeoutSecs?: number } = {},
): Promise<ForwarderReleaseOutcome> {
  const probe = await boat.exec(sandboxId, forwarderProbeScript(), 60);
  const facts = parseForwarderProbe(probe.stdout);
  const verdict = judgeForwarder(facts);
  if (verdict.action === "none") return "clear";
  if (verdict.action === "refuse") throw new Error(`klav-forward cleanup: ${verdict.reason}; nothing was stopped`);

  const termTimeoutSecs = opts.termTimeoutSecs ?? 10;
  const res = await boat.exec(sandboxId, forwarderStopScript(verdict.pid, termTimeoutSecs), termTimeoutSecs + 30);
  const outcome = res.stdout.match(/stop=(term|kill|mismatch|alive)/)?.[1] ?? "unknown";
  if (outcome === "term" || outcome === "kill") {
    console.log(
      `[klav-forward] stopped leftover forwarder (pid ${verdict.pid}, ${outcome === "term" ? "TERM" : "KILL"}) in ${sandboxId}; \`klav-forward down\` exited ${facts.downExit ?? "n/a"}`,
    );
    return "stopped";
  }
  throw new Error(`klav-forward cleanup: leftover forwarder pid ${verdict.pid} was not stopped (${outcome})`);
}
