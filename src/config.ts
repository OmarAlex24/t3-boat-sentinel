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
  /** Proxy mode: path on sentinel host to Linux x86_64 `klav-forward` binary installed into `baseSnapshot` during bake. */
  forwardBinaryPath?: string;
  /** Proxy mode: path on sentinel host to `klav-tailnet-up` script installed to `/usr/local/sbin/klav-tailnet-up` during bake. */
  tailnetUpScriptPath?: string;
  /**
   * Proxy mode (ADR 0042 §4): optional 0600 file containing `client_id=...` and `client_secret=...`
   * for a Tailscale OAuth client scoped strictly to `auth_keys` + `tag:klav-sandbox`.
   * Sentinel mints a single-use, ephemeral, preauthorized auth key per sandbox and runs `klav-tailnet-up`.
   */
  tailscaleOauthFile?: string;
  /** Tailscale tailnet name for key creation (default "-" = the OAuth client's own tailnet). */
  tailnetName?: string;
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

export interface InjectFileConfig {
  /** Local file on sentinel host (never baked into snapshots). */
  from: string;
  /** Remote path inside each spawned Boat VM (e.g. "~/.claude/settings.json"). */
  to: string;
  /** Optional chmod octal mode (default "600"). */
  mode?: string;
}

export interface LlmProxyConfig {
  /** Hostname exposed via Cloudflare Tunnel wildcard (e.g. "boat-llm.example.com"). */
  host: string;
  /** Private LAN / Tailscale upstream LLM API URL reachable from sentinel (e.g. "http://192.168.1.248:8317"). */
  upstreamUrl: string;
  /** Real upstream API key/token — stays strictly on sentinel; VMs receive a per-sandbox ephemeral token! */
  upstreamApiKey: string;
}

export interface HubConfig {
  /** Public hostname for the single unified T3 Environment multiplexer (e.g. "boat.example.com"). */
  host: string;
  /** Display label shown in T3 Code's Environments list (default "Boat"). */
  label: string;
  /** Maximum concurrent live Boat VMs managed by the Hub (default 10). */
  maxSandboxes: number;
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
    /** Optional JSON file on sentinel merged into `~/.t3/userdata/settings.json` on every VM boot (inherits providers & preferences). */
    settingsFile?: string;
  };
  boat: {
    bin?: string;
    defaultSize: string;
    baseSnapshot: string;
    /** Optional starting Boat snapshot when baking `baseSnapshot` (e.g. a Klav template like `klav-sandbox-4ff6ca3a`). */
    baseFrom?: string;
    /** Optional shell command run during `bake base` (e.g. install dormant `tailscale`, `northflank` CLI, etc.). */
    baseBakeCmd?: string;
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
    /** Optional files on sentinel uploaded into every spawned VM (never frozen into golden snapshots). */
    injectFiles: InjectFileConfig[];
  };
  klav?: KlavGlobalConfig;
  llmProxy?: LlmProxyConfig;
  /** Optional unified single-environment Hub multiplexer (1 T3 Environment -> N parallel Boat VMs, 1 chat = 1 VM). */
  hub?: HubConfig;
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
    settingsFile: parsed.t3?.settingsFile ? resolve(parsed.t3.settingsFile) : undefined,
  };

  const boat = {
    bin: parsed.boat?.bin,
    defaultSize: parsed.boat?.defaultSize ?? "default",
    baseSnapshot: parsed.boat?.baseSnapshot ?? "base-t3",
    baseFrom: parsed.boat?.baseFrom ? String(parsed.boat.baseFrom).trim() : undefined,
    baseBakeCmd: parsed.boat?.baseBakeCmd ? String(parsed.boat.baseBakeCmd) : undefined,
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
    injectFiles: Array.isArray(parsed.boat?.injectFiles)
      ? parsed.boat.injectFiles.map((f) => ({
          from: resolve(requireString(f.from, "boat.injectFiles[].from")),
          to: requireString(f.to, "boat.injectFiles[].to"),
          mode: f.mode ?? "600",
        }))
      : [],
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
        forwardBinaryPath: parsed.klav.forwardBinaryPath ? resolve(parsed.klav.forwardBinaryPath) : undefined,
        tailnetUpScriptPath: parsed.klav.tailnetUpScriptPath ? resolve(parsed.klav.tailnetUpScriptPath) : undefined,
        tailscaleOauthFile: parsed.klav.tailscaleOauthFile ? resolve(parsed.klav.tailscaleOauthFile) : undefined,
        tailnetName: parsed.klav.tailnetName ? String(parsed.klav.tailnetName).trim() : "-",
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

  let llmProxy: LlmProxyConfig | undefined;
  if (parsed.llmProxy) {
    llmProxy = {
      host: requireString(parsed.llmProxy.host, "llmProxy.host").replace(/^https?:\/\//, "").replace(/\/+$/, "").toLowerCase(),
      upstreamUrl: requireString(parsed.llmProxy.upstreamUrl, "llmProxy.upstreamUrl").replace(/\/+$/, ""),
      upstreamApiKey: requireSecret(parsed.llmProxy.upstreamApiKey, "llmProxy.upstreamApiKey"),
    };
  }

  let hub: HubConfig | undefined;
  if (parsed.hub) {
    hub = {
      host: requireString(parsed.hub.host, "hub.host").replace(/^https?:\/\//, "").replace(/\/+$/, "").toLowerCase(),
      label: parsed.hub.label ? String(parsed.hub.label).trim() : "Boat",
      maxSandboxes: Math.max(1, Number(parsed.hub.maxSandboxes ?? 10)),
    };
  }

  const wakePolicy = parsed.wakePolicy === "on-connect" ? "on-connect" : "manual";

  const slots = Array.isArray(parsed.slots) ? parsed.slots : [];
  if (slots.length === 0 && !hub) {
    throw new Error("Config must define at least one slot in `slots` or configure `hub`");
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
    llmProxy,
    hub,
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
