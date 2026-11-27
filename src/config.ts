import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import YAML from "yaml";
import { PROVIDER_INSTANCE_ID_RE } from "./provider-sessions.ts";

/** Default hours to keep a parked (settled) VM stopped in Boat before retiring it to read-only (2 days). */
export const DEFAULT_PARKED_TTL_HOURS = 48;
/** Default cap on parked (settled) VMs kept stopped in Boat (0 disables the cap). */
export const DEFAULT_MAX_PARKED_SANDBOXES = 8;

export interface SlotConfig {
  name: string;
  label?: string;
  size?: string;
}

export interface AutoUpsizeConfig {
  /** Target machine size to scale up to (e.g. "xlarge"). */
  to: string;
  /** Trigger when MemAvailable falls below this % of MemTotal (default 15). */
  minAvailableMemPct?: number;
  /** Trigger when 1-min loadavg >= minLoadPerCpu * vCPUs while memory is low (default 2.0). */
  minLoadPerCpu?: number;
  /** Trigger when 1-min loadavg >= highLoadPerCpu * vCPUs regardless of available RAM (default max(3.0, minLoadPerCpu); 0 disables). */
  highLoadPerCpu?: number;
  /** Seconds of sustained pressure before triggering (default 180). */
  sustainSecs?: number;
  /** Immediately trigger when `/proc/vmstat` `oom_kill` increases (default true). */
  onOomKill?: boolean;
  /** Automatically send a continuation message to interrupted active top-level threads after resume (default true). */
  autoContinue?: boolean;
}

export interface ResolvedAutoUpsizeConfig {
  to: string;
  minAvailableMemPct: number;
  minLoadPerCpu: number;
  highLoadPerCpu: number;
  sustainSecs: number;
  onOomKill: boolean;
  autoContinue: boolean;
}

/**
 * The effective auto-upsize config of one project. A project object replaces the global object;
 * `autoUpsize: false` on the project disables the feature even when `boat.autoUpsize` is configured.
 * Returns null when disabled.
 */
export function resolveAutoUpsizeConfig(config: SentinelConfig, project?: ProjectConfig): ResolvedAutoUpsizeConfig | null {
  const global = config.boat.autoUpsize;
  const local = project?.autoUpsize;
  if (local === false) return null;
  const globalObject = typeof global === "object" ? global : undefined;
  const raw = local ?? globalObject;
  if (!raw) return null;
  const minLoadPerCpu = raw.minLoadPerCpu ?? 2.0;
  return {
    to: raw.to.trim(),
    minAvailableMemPct: raw.minAvailableMemPct ?? 15,
    minLoadPerCpu,
    highLoadPerCpu: raw.highLoadPerCpu ?? Math.max(3.0, minLoadPerCpu),
    sustainSecs: raw.sustainSecs ?? 180,
    onOomKill: raw.onOomKill ?? true,
    autoContinue: raw.autoContinue ?? true,
  };
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
  /** Per-repo automatic VM upsizing override; `false` disables it even when `boat.autoUpsize` is set. */
  autoUpsize?: AutoUpsizeConfig | false;
  bakeHourUtc?: number;
  /**
   * Hub chats (default true): a new chat VM moves `workdir` to the remote's default branch before
   * T3 starts, discarding what the snapshot's bake left. Resumed VMs are never touched.
   */
  syncToRemoteOnStart?: boolean;
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

export interface HubPreviewConfig {
  /** "desktop-proxy": native Electron <webview> via lazy port gateway. "server": VM JPEG stream (default when omitted for backwards compatibility). */
  mode: "desktop-proxy" | "server";
  /** Optional Cloudflare Edge Worker base host (single-level zone such as "example.com" or "preview-example.net", never a nested subdomain) */
  edgeWorkerHost?: string;
  /** Shared secret with workers/preview-edge (NEVER a Cloudflare API key). */
  edgeWorkerSecret?: string;
  /** Cap upstream server-browser screencast maxWidth (0 or omitted = no cap). */
  streamMaxWidth?: number;
  /** Cap upstream server-browser screencast maxHeight (0 or omitted = no cap). */
  streamMaxHeight?: number;
  /** Cap/inject upstream server-browser JPEG quality 1..100 (0 or omitted = T3 default). */
  streamQuality?: number;
  /** Enable hover mouse-move and wheel coalescing on server-browser streams. */
  coalesceInput?: boolean;
  /**
   * In `desktop-proxy` mode, project agent-opened `runtime: "server"` preview tabs to native
   * `runtime: "desktop"` Electron webview tabs pointing at the gateway URL (default `true`).
   * Inside the VM the tab remains `runtime: "server"` so agent automation keeps working.
   */
  projectAgentTabs?: boolean;
}

export interface HubConfig {
  /** Public hostname for the single unified T3 Environment multiplexer (e.g. "boat.example.com"). */
  host: string;
  /** Display label shown in T3 Code's Environments list (default "Boat"). */
  label: string;
  /** Maximum concurrent live Boat VMs managed by the Hub (default 10). */
  maxSandboxes: number;
  /** Maximum parked (settled) VMs kept stopped in Boat (default 8; 0 disables the count cap). Oldest parked VMs retire to read-only when exceeded. */
  maxParkedSandboxes?: number;
  /** Optional preview routing and server-browser stream tuning. */
  preview?: HubPreviewConfig;
}

export type SandboxProviderConfig =
  | { type: "boat" }
  | {
      type: "sandbox-sdk";
      /** Adapter name in `@opencoredev/sandbox-sdk/<adapter>` (e.g. daytona, e2b, docker, vercel, upstash, railway, freestyle, boxd). */
      adapter: string;
      options?: Record<string, unknown>;
      sizeMap?: Record<string, unknown>;
      sentinelLabel?: string;
    }
  | {
      /** Celesto (celesto.ai) cloud computers over the REST API. No snapshots: T3 installs at boot. */
      type: "celesto";
      /** Resolved API key (`provider.apiKey`, else env `CELESTO_API_KEY`); empty when neither is set. */
      apiKey: string;
      baseUrl?: string;
      /** Computer template (default "coding-agent"). */
      templateId?: string;
      /** User T3 and every exec run as (default "ohm"; never root, Claude Code refuses root). */
      user?: string;
      /** Extra Sentinel size name -> Celesto `size_id` mappings (defaults: default->standard, medium/large->large). */
      sizeMap?: Record<string, string>;
    };

export interface CloudflareAccessConfig {
  /** Your Cloudflare Zero Trust team domain (e.g. "your-team.cloudflareaccess.com"). */
  teamDomain: string;
  /** Application Audience (AUD) tag from Cloudflare Zero Trust -> Access -> Applications. */
  aud: string;
  /** Optional email allow-list checked in addition to Cloudflare Access policy. */
  allowedEmails?: string[];
}

export interface SecurityConfig {
  /** Optional cryptographic Cloudflare Access JWT verification on `controlHost` (except `/internal/watchdog` and `/health`). */
  cloudflareAccess?: CloudflareAccessConfig;
  /** Disable built-in auth failure rate limiting (default false). */
  disableRateLimit?: boolean;
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
    /** Optional ACP Registry agent IDs to pre-prepare into `baseSnapshot` during `bake base` (e.g. ["devin"]); every bake verifies them. */
    prepareAcpAgents?: string[];
    /** Optional managed provider instance IDs to pre-install into `baseSnapshot` during `bake base` (e.g. ["antigravity"]); every bake verifies them. */
    installProviders?: string[];
    /** Pre-install T3's pinned server browser into `baseSnapshot` during `bake base` (default false). */
    preinstallBrowser?: boolean;
    /**
     * Optional provider instance IDs whose sign-in is kept across chat VMs (e.g. ["antigravity", "acpRegistry_devin"]).
     * The credential file is captured from a signed-in VM into `<stateDir>/credentials/<instanceId>/`
     * (0600) and copied into every new VM; never into snapshots. Supported: driver antigravity, and
     * driver acpRegistry with agentId devin (see `DRIVER_CREDENTIALS`).
     */
    persistProviderSessions?: string[];
  };
  boat: {
    bin?: string;
    /** Exposure mode for Boat's hosted port 3773: "public" (default, protected by T3 Bearer token) or "private" (Boat token/cookie-gated). */
    hostVisibility?: "public" | "private";
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
    /**
     * Hub only, with `deleteWhen: archived-or-settled`: what happens to a chat VM whose chats are all
     * settled but not all archived or deleted. `park` (default) stops it like an idle sleep and keeps
     * the chat in the sidebar; it does not count toward `hub.maxSandboxes` and wakes only when the chat
     * is continued (kept up to `parkedTtlHours` and `hub.maxParkedSandboxes`, then deleted in Boat and
     * kept read only). `delete` deletes it.
     */
    settleAction: "park" | "delete";
    /**
     * Hub only: hours to keep a parked (settled) VM stopped before deleting it in Boat and marking its
     * chat read only (default 48 = 2 days; Boat itself drops stopped VM disks after 14 days / 336h).
     */
    parkedTtlHours?: number;
    reaperIntervalSecs: number;
    /** Safety cap in hours for sleeping sandboxes (0 = disabled, default 0 to prevent surprise data loss). */
    maxSleepingHours: number;
    sandboxPrefix: string;
    /** Optional files on sentinel uploaded into every spawned VM (never frozen into golden snapshots). */
    injectFiles: InjectFileConfig[];
    /** Global default automatic VM upsizing; undefined/false disables it unless a project overrides it. */
    autoUpsize?: AutoUpsizeConfig | false;
  };
  klav?: KlavGlobalConfig;
  llmProxy?: LlmProxyConfig;
  /** Optional unified single-environment Hub multiplexer (1 T3 Environment -> N parallel Boat VMs, 1 chat = 1 VM). */
  hub?: HubConfig;
  /** Optional modular sandbox provider backend (defaults to native Boat CLI when omitted). */
  provider?: SandboxProviderConfig;
  /** Optional link security hardening (Cloudflare Access JWT verification + rate limiting). */
  security?: SecurityConfig;
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
    prepareAcpAgents: Array.isArray(parsed.t3?.prepareAcpAgents) ? parsed.t3.prepareAcpAgents.map(String) : undefined,
    installProviders: Array.isArray(parsed.t3?.installProviders) ? parsed.t3.installProviders.map(String) : undefined,
    preinstallBrowser: parsed.t3?.preinstallBrowser === true,
    persistProviderSessions: Array.isArray(parsed.t3?.persistProviderSessions)
      ? parsed.t3.persistProviderSessions.map((id) => {
          const s = String(id).trim();
          if (!PROVIDER_INSTANCE_ID_RE.test(s)) {
            throw new Error(`Invalid provider instance ID "${s}" in t3.persistProviderSessions`);
          }
          return s;
        })
      : undefined,
  };

  validateAutoUpsizeConfig(parsed.boat?.autoUpsize, "boat.autoUpsize");
  const boat = {
    bin: parsed.boat?.bin,
    hostVisibility: (parsed.boat?.hostVisibility === "public" ? "public" : "private") as "public" | "private",
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
    settleAction: (parsed.boat?.settleAction === "delete" ? "delete" : "park") as "park" | "delete",
    parkedTtlHours: parsePositiveHours(parsed.boat?.parkedTtlHours, "boat.parkedTtlHours", DEFAULT_PARKED_TTL_HOURS),
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
    ...(parsed.boat?.autoUpsize !== undefined ? { autoUpsize: parsed.boat.autoUpsize } : {}),
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
    let preview: HubPreviewConfig | undefined;
    if (parsed.hub.preview !== undefined) {
      if (!parsed.hub.preview || typeof parsed.hub.preview !== "object") {
        throw new Error(`Config "hub.preview" must be an object`);
      }
      const raw = parsed.hub.preview as Partial<HubPreviewConfig>;
      if (raw.mode !== undefined && raw.mode !== "server" && raw.mode !== "desktop-proxy") {
        throw new Error(`Invalid hub.preview.mode ${JSON.stringify(raw.mode)}: expected "server" or "desktop-proxy"`);
      }

      const hostSet = raw.edgeWorkerHost !== undefined && raw.edgeWorkerHost !== null && String(raw.edgeWorkerHost).trim() !== "";
      const secretSet = raw.edgeWorkerSecret !== undefined && raw.edgeWorkerSecret !== null && String(raw.edgeWorkerSecret).trim() !== "";
      if (hostSet !== secretSet) {
        throw new Error(`Config hub.preview.edgeWorkerHost and hub.preview.edgeWorkerSecret must be set together`);
      }
      const edgeWorkerHost = hostSet
        ? requireString(raw.edgeWorkerHost, "hub.preview.edgeWorkerHost").replace(/^https?:\/\//i, "").replace(/\/+$/, "").toLowerCase()
        : undefined;
      const edgeWorkerSecret = secretSet ? requireSecret(raw.edgeWorkerSecret, "hub.preview.edgeWorkerSecret") : undefined;
      const streamMaxWidth = optionalPositiveInteger(raw.streamMaxWidth, "hub.preview.streamMaxWidth");
      const streamMaxHeight = optionalPositiveInteger(raw.streamMaxHeight, "hub.preview.streamMaxHeight");
      const streamQuality = optionalStreamQuality(raw.streamQuality);

      if (raw.projectAgentTabs !== undefined && typeof raw.projectAgentTabs !== "boolean") {
        throw new Error(`Config "hub.preview.projectAgentTabs" must be true or false`);
      }

      preview = {
        mode: raw.mode === "desktop-proxy" ? "desktop-proxy" : "server",
        ...(edgeWorkerHost ? { edgeWorkerHost } : {}),
        ...(edgeWorkerSecret ? { edgeWorkerSecret } : {}),
        ...(streamMaxWidth !== undefined ? { streamMaxWidth } : {}),
        ...(streamMaxHeight !== undefined ? { streamMaxHeight } : {}),
        ...(streamQuality !== undefined ? { streamQuality } : {}),
        ...(raw.coalesceInput === true ? { coalesceInput: true } : {}),
        ...(raw.projectAgentTabs !== undefined ? { projectAgentTabs: raw.projectAgentTabs } : {}),
      };
    }
    hub = {
      host: requireString(parsed.hub.host, "hub.host").replace(/^https?:\/\//, "").replace(/\/+$/, "").toLowerCase(),
      label: parsed.hub.label ? String(parsed.hub.label).trim() : "Boat",
      maxSandboxes: Math.max(1, Number(parsed.hub.maxSandboxes ?? 10)),
      maxParkedSandboxes: parseNonNegativeInteger(
        parsed.hub.maxParkedSandboxes,
        "hub.maxParkedSandboxes",
        DEFAULT_MAX_PARKED_SANDBOXES,
      ),
      ...(preview ? { preview } : {}),
    };
  }

  let provider: SandboxProviderConfig | undefined;
  if (parsed.provider) {
    if (parsed.provider.type === "sandbox-sdk") {
      provider = {
        type: "sandbox-sdk",
        adapter: requireString(parsed.provider.adapter, "provider.adapter"),
        options: parsed.provider.options,
        sizeMap: parsed.provider.sizeMap,
        sentinelLabel: parsed.provider.sentinelLabel,
      };
    } else if (parsed.provider.type === "celesto") {
      const raw = parsed.provider as Record<string, unknown>;
      const configured = typeof raw.apiKey === "string" ? raw.apiKey.trim() : "";
      // An unset `${VAR}` placeholder stays literal after expansion: treat it as missing
      // May stay empty: CLI commands that only call the daemon need no key. `serve` and the first
      // Celesto API call fail clearly without one.
      const apiKey = configured && !/^\$\{[A-Z0-9_]+\}$/.test(configured) ? configured : (process.env.CELESTO_API_KEY ?? "").trim();
      const sizeMap =
        raw.sizeMap && typeof raw.sizeMap === "object"
          ? Object.fromEntries(Object.entries(raw.sizeMap as Record<string, unknown>).map(([k, v]) => [k, String(v)]))
          : undefined;
      provider = {
        type: "celesto",
        apiKey,
        ...(typeof raw.baseUrl === "string" && raw.baseUrl.trim() ? { baseUrl: raw.baseUrl.trim() } : {}),
        ...(typeof raw.templateId === "string" && raw.templateId.trim() ? { templateId: raw.templateId.trim() } : {}),
        ...(typeof raw.user === "string" && raw.user.trim() ? { user: raw.user.trim() } : {}),
        ...(sizeMap ? { sizeMap } : {}),
      };
    } else {
      provider = { type: "boat" };
    }
  }

  let security: SecurityConfig | undefined;
  if (parsed.security) {
    const cfa = parsed.security.cloudflareAccess;
    security = {
      disableRateLimit: parsed.security.disableRateLimit === true,
      ...(cfa
        ? {
            cloudflareAccess: {
              teamDomain: requireString(cfa.teamDomain, "security.cloudflareAccess.teamDomain")
                .replace(/^https?:\/\//, "")
                .replace(/\/+$/, ""),
              aud: requireString(cfa.aud, "security.cloudflareAccess.aud"),
              ...(Array.isArray(cfa.allowedEmails) ? { allowedEmails: cfa.allowedEmails.map(String) } : {}),
            },
          }
        : {}),
    };
  }

  const wakePolicy = parsed.wakePolicy === "on-connect" ? "on-connect" : "manual";

  const slots = Array.isArray(parsed.slots) ? parsed.slots : [];
  if (slots.length === 0 && !hub) {
    throw new Error("Config must define at least one slot in `slots` or configure `hub`");
  }
  const seen = new Set<string>();
  const desktopProxyEnabled = hub?.preview?.mode === "desktop-proxy";
  for (const s of slots) {
    if (!s?.name || !SLOT_NAME_RE.test(s.name)) {
      throw new Error(`Invalid slot name "${s?.name}". Use lowercase alphanumeric + hyphens.`);
    }
    if (desktopProxyEnabled && /^p\d+-/.test(s.name)) {
      throw new Error(`Slot name "${s.name}" conflicts with hub.preview desktop-proxy hostnames (p<port>-<slug>).`);
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
    if (proj.syncToRemoteOnStart !== undefined && typeof proj.syncToRemoteOnStart !== "boolean") {
      throw new Error(`Project "${key}": \`syncToRemoteOnStart\` must be true or false`);
    }
    validateAutoUpsizeConfig(proj.autoUpsize, `projects.${key}.autoUpsize`);
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
    provider,
    security,
    wakePolicy,
    slots,
    projects,
  };
}

function validateAutoUpsizeConfig(raw: unknown, name: string): void {
  if (raw === undefined || raw === null || raw === false) return;
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    throw new Error(`Config "${name}" must be an object or false`);
  }
  const cfg = raw as Record<string, unknown>;
  requireString(cfg.to, `${name}.to`);

  const numberField = (key: "minAvailableMemPct" | "minLoadPerCpu" | "highLoadPerCpu" | "sustainSecs"): number | undefined => {
    const val = cfg[key];
    if (val === undefined || val === null) return undefined;
    if (typeof val !== "number" || !Number.isFinite(val)) {
      throw new Error(`Config "${name}.${key}" must be a finite number`);
    }
    return val;
  };
  const minAvailableMemPct = numberField("minAvailableMemPct");
  if (minAvailableMemPct !== undefined && (minAvailableMemPct <= 0 || minAvailableMemPct >= 100)) {
    throw new Error(`Config "${name}.minAvailableMemPct" must be greater than 0 and less than 100`);
  }
  const minLoadPerCpu = numberField("minLoadPerCpu");
  if (minLoadPerCpu !== undefined && minLoadPerCpu <= 0) {
    throw new Error(`Config "${name}.minLoadPerCpu" must be greater than 0`);
  }
  const highLoadPerCpu = numberField("highLoadPerCpu");
  if (highLoadPerCpu !== undefined && highLoadPerCpu < 0) {
    throw new Error(`Config "${name}.highLoadPerCpu" must be 0 or greater`);
  }
  const sustainSecs = numberField("sustainSecs");
  if (sustainSecs !== undefined && sustainSecs < 5) {
    throw new Error(`Config "${name}.sustainSecs" must be at least 5`);
  }
  for (const key of ["onOomKill", "autoContinue"] as const) {
    if (cfg[key] !== undefined && typeof cfg[key] !== "boolean") {
      throw new Error(`Config "${name}.${key}" must be true or false`);
    }
  }
}

function optionalPositiveInteger(val: unknown, name: string): number | undefined {
  if (val === undefined || val === null) return undefined;
  const n = typeof val === "number" ? val : typeof val === "string" && val.trim() !== "" ? Number(val) : NaN;
  if (!Number.isInteger(n) || n < 0) {
    throw new Error(`Config "${name}" must be a positive integer (0 disables the cap)`);
  }
  return n === 0 ? undefined : n;
}

function parseNonNegativeInteger(val: unknown, name: string, fallback: number): number {
  if (val === undefined || val === null) return fallback;
  const n = typeof val === "number" ? val : typeof val === "string" && val.trim() !== "" ? Number(val) : NaN;
  if (!Number.isInteger(n) || n < 0) {
    throw new Error(`Config "${name}" must be a non-negative integer (0 disables the cap)`);
  }
  return n;
}

function parsePositiveHours(val: unknown, name: string, fallback: number): number {
  if (val === undefined || val === null) return fallback;
  const n = typeof val === "number" ? val : typeof val === "string" && val.trim() !== "" ? Number(val) : NaN;
  if (!Number.isFinite(n) || n <= 0) {
    throw new Error(`Config "${name}" must be a positive number of hours`);
  }
  return n;
}

function optionalStreamQuality(val: unknown): number | undefined {
  if (val === undefined || val === null) return undefined;
  const n = typeof val === "number" ? val : typeof val === "string" && val.trim() !== "" ? Number(val) : NaN;
  if (!Number.isInteger(n) || n < 0 || n > 100) {
    throw new Error(`Config "hub.preview.streamQuality" must be an integer from 1 to 100 (0 uses the T3 default)`);
  }
  return n === 0 ? undefined : n;
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
