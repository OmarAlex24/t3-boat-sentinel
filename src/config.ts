import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import YAML from "yaml";

export interface SlotConfig {
  name: string;
  label?: string;
  size?: string;
}

export interface ProjectKlavConfig {
  /** For `klav.mode: "proxy"` (ADR 0036 §4): proxy profiles granted to the sandbox (e.g. ["northflank", "github"]). */
  profiles?: string[];
  /** TTL duration for proxy sandboxes (e.g. "8h"). */
  ttl?: string;
  /** For `klav.mode: "agent"`: Klav project path (e.g. "Work/my-app"). */
  project?: string;
  allow?: string[];
  deny?: string[];
  expires?: string;
  policyFile?: string;
}

export interface KlavGlobalConfig {
  /**
   * - "proxy" (recommended, ADR 0036 §4): sentinel acts as an issuer agent talking to local `klav-proxy serve`.
   *   Zero master password on sentinel; API keys never leave `klav-proxy`!
   * - "agent": provisions ephemeral Klav agents via `klav agent register/approve/revoke`.
   */
  mode: "proxy" | "agent";
  bin?: string;
  /** Proxy mode: KLAV_HOME of the Touch-ID-approved issuer agent on the proxy host. */
  issuerHome?: string;
  /** Proxy mode: issuer agent name (optional if only one agent exists in issuerHome). */
  issuerAgent?: string;
  /** Proxy mode: loopback port of `klav-proxy serve` on the same host (default 8790). */
  proxyPort: number;
  /** Proxy mode: default sandbox TTL (e.g. "8h"). */
  defaultTtl: string;
  /** Agent mode only: central Klav server HTTPS URL. */
  server?: string;
  email?: string;
  accountHome?: string;
  masterPassword?: string;
  linuxBinaryPath?: string;
  skillPath?: string;
  defaultAllow: string[];
  defaultDeny: string[];
  defaultExpires: string;
}

export interface ProjectConfig {
  snapshot: string;
  size?: string;
  repo?: string;
  workdir: string;
  registerProject?: boolean;
  /** Default provider & model used when `sentinel_spawn` is called with `prompt` alone. */
  defaultProvider?: string;
  defaultModel?: string;
  bakeCmd?: string;
  bootCmd?: string;
  envFile?: string;
  envTarget?: string;
  klav?: ProjectKlavConfig;
  bakeHourUtc?: number;
}

export interface SentinelConfig {
  domain: string;
  controlHost: string;
  listenHost: string;
  listenPort: number;
  stateDir: string;
  controlSecret: string;
  notifyUrl?: string;
  t3: {
    /** Exact T3 semver to pin (recommended, e.g. "0.0.48") or "latest". */
    version: string;
    channel: "stable" | "nightly";
    /** Default false: update cleanly during bake rather than mutating a live chat VM mid-session. */
    autoUpdateOnWake: boolean;
  };
  boat: {
    bin?: string;
    defaultSize: string;
    baseSnapshot: string;
    baseBakeHourUtc?: number;
    sleepAfterSecs: number;
    archiveGraceSecs: number;
    /** Trigger cleanup when chats are `archived` or `archived-or-settled`. */
    deleteWhen: "archived" | "archived-or-settled";
    /** Action on archive: `delete` immediately (after grace) or `stop` (keeps disk up to 14d). */
    archiveAction: "delete" | "stop";
    reaperIntervalSecs: number;
    /** Safety cap in hours for sleeping sandboxes (0 = disabled, default 0 to prevent surprise data loss). */
    maxSleepingHours: number;
    sandboxPrefix: string;
  };
  klav?: KlavGlobalConfig;
  /**
   * Wake behavior for sleeping slots:
   * - "manual" (default): returns 503 while asleep (zero phantom wakes, zero dropped frames); wake via dashboard/MCP/CLI
   * - "on-connect": transparently wakes the VM on incoming WS/HTTP connection and proxies once ready
   */
  wakePolicy: "manual" | "on-connect";
  slots: SlotConfig[];
  projects: Record<string, ProjectConfig>;
}

const SLOT_NAME_RE = /^[a-z0-9](?:[a-z0-9-]{0,30}[a-z0-9])?$/;
const ENV_PLACEHOLDER_RE = /\$\{([A-Z0-9_]+)\}/g;

export function expandEnvPlaceholders(raw: string, env: Record<string, string | undefined> = process.env): string {
  return raw.replace(ENV_PLACEHOLDER_RE, (full, name: string) => {
    const val = env[name];
    return val !== undefined ? val : full;
  });
}

export function loadConfig(configPath = process.env.SENTINEL_CONFIG ?? "./sentinel.yaml"): SentinelConfig {
  const abs = resolve(configPath);
  const raw = expandEnvPlaceholders(readFileSync(abs, "utf8"));
  const parsed = YAML.parse(raw) as Partial<SentinelConfig> | null;
  if (!parsed || typeof parsed !== "object") {
    throw new Error(`Invalid YAML config at ${abs}`);
  }

  const domain = requireString(parsed.domain, "domain").replace(/^https?:\/\//, "").replace(/\/+$/, "");
  const controlHost = (parsed.controlHost ?? `ctrl.${domain}`).replace(/^https?:\/\//, "").replace(/\/+$/, "");
  const listenHost = parsed.listenHost ?? "127.0.0.1";
  const listenPort = Number(parsed.listenPort ?? 8080);
  const stateDir = resolve(parsed.stateDir ?? "./data");
  const controlSecret = requireSecret(parsed.controlSecret, "controlSecret");

  const t3 = {
    version: String(parsed.t3?.version ?? "latest"),
    channel: (parsed.t3?.channel === "nightly" ? "nightly" : "stable") as "stable" | "nightly",
    autoUpdateOnWake: parsed.t3?.autoUpdateOnWake ?? false,
  };

  const boat = {
    bin: parsed.boat?.bin,
    defaultSize: parsed.boat?.defaultSize ?? "default",
    baseSnapshot: parsed.boat?.baseSnapshot ?? "base-t3",
    baseBakeHourUtc: parsed.boat?.baseBakeHourUtc,
    sleepAfterSecs: Number(parsed.boat?.sleepAfterSecs ?? 600),
    archiveGraceSecs: Number(parsed.boat?.archiveGraceSecs ?? 20),
    deleteWhen: (parsed.boat?.deleteWhen === "archived-or-settled" ? "archived-or-settled" : "archived") as
      | "archived"
      | "archived-or-settled",
    archiveAction: (parsed.boat?.archiveAction === "stop" ? "stop" : "delete") as "delete" | "stop",
    reaperIntervalSecs: Number(parsed.boat?.reaperIntervalSecs ?? 300),
    maxSleepingHours: Number(parsed.boat?.maxSleepingHours ?? 0),
    sandboxPrefix: parsed.boat?.sandboxPrefix ?? "t3s-",
  };

  let klav: KlavGlobalConfig | undefined;
  if (parsed.klav) {
    const mode = parsed.klav.mode === "agent" ? "agent" : "proxy";
    if (mode === "proxy") {
      klav = {
        mode: "proxy",
        bin: parsed.klav.bin,
        issuerHome: parsed.klav.issuerHome ? resolve(parsed.klav.issuerHome) : undefined,
        issuerAgent: parsed.klav.issuerAgent,
        proxyPort: Number(parsed.klav.proxyPort ?? 8790),
        defaultTtl: parsed.klav.defaultTtl ?? "8h",
        linuxBinaryPath: parsed.klav.linuxBinaryPath ? resolve(parsed.klav.linuxBinaryPath) : undefined,
        skillPath: parsed.klav.skillPath ? resolve(parsed.klav.skillPath) : undefined,
        defaultAllow: parsed.klav.defaultAllow ?? ["list", "inject"],
        defaultDeny: parsed.klav.defaultDeny ?? ["reveal", "delete", "fill", "create", "update", "totp"],
        defaultExpires: parsed.klav.defaultExpires ?? "24h",
      };
    } else {
      klav = {
        mode: "agent",
        bin: parsed.klav.bin,
        proxyPort: 8790,
        defaultTtl: "8h",
        server: requireString(parsed.klav.server, "klav.server").replace(/\/+$/, ""),
        email: requireString(parsed.klav.email, "klav.email"),
        accountHome: resolve(requireString(parsed.klav.accountHome, "klav.accountHome")),
        masterPassword: requireSecret(parsed.klav.masterPassword, "klav.masterPassword"),
        linuxBinaryPath: parsed.klav.linuxBinaryPath ? resolve(parsed.klav.linuxBinaryPath) : undefined,
        skillPath: parsed.klav.skillPath ? resolve(parsed.klav.skillPath) : undefined,
        defaultAllow: parsed.klav.defaultAllow ?? ["list", "inject"],
        defaultDeny: parsed.klav.defaultDeny ?? ["reveal", "delete", "fill", "create", "update", "totp"],
        defaultExpires: parsed.klav.defaultExpires ?? "24h",
      };
    }
  }

  const wakePolicy = parsed.wakePolicy === "on-connect" ? "on-connect" : "manual";

  const slots = Array.isArray(parsed.slots) ? parsed.slots : [];
  if (slots.length === 0) {
    throw new Error("Config must define at least one slot in `slots`");
  }
  const seen = new Set<string>();
  for (const s of slots) {
    if (!s?.name || !SLOT_NAME_RE.test(s.name)) {
      throw new Error(`Invalid slot name "${s?.name}". Use lowercase alphanumeric + hyphens.`);
    }
    if (seen.has(s.name)) {
      throw new Error(`Duplicate slot name "${s.name}"`);
    }
    seen.add(s.name);
  }

  const projects = parsed.projects ?? {};
  for (const [key, proj] of Object.entries(projects)) {
    if (!proj?.snapshot) {
      throw new Error(`Project "${key}" is missing required field "snapshot"`);
    }
    if (!proj?.workdir) {
      throw new Error(`Project "${key}" is missing required field "workdir"`);
    }
    if (proj.klav && !klav) {
      throw new Error(`Project "${key}" configures \`klav\`, but top-level \`klav\` config is missing`);
    }
  }

  return {
    domain,
    controlHost,
    listenHost,
    listenPort,
    stateDir,
    controlSecret,
    notifyUrl: parsed.notifyUrl,
    t3,
    boat,
    klav,
    wakePolicy,
    slots,
    projects,
  };
}

function requireString(val: unknown, name: string): string {
  if (typeof val !== "string" || val.trim().length === 0) {
    throw new Error(`Config is missing required string "${name}"`);
  }
  return val.trim();
}

function requireSecret(val: unknown, name: string): string {
  const s = requireString(val, name);
  if (s.startsWith("${") && s.endsWith("}")) {
    throw new Error(`Config "${name}" references unset environment variable ${s}`);
  }
  return s;
}
