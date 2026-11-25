import { randomBytes } from "node:crypto";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  CELESTO_MISSING_KEY_MESSAGE,
  asSandboxProvider,
  createSandboxProvider,
  type LegacyBoatClient,
  type SandboxProvider,
} from "./providers/index.ts";
import { loadConfig, type SentinelConfig } from "./config.ts";
import { ControlPlane } from "./control.ts";
import { HubServer, type HubMediaWsData, type HubSocketData, type HubWsData } from "./hub.ts";
import { JobManager } from "./jobs.ts";
import { KlavCliProvider, type KlavClient } from "./klav.ts";
import { LifecycleManager } from "./lifecycle.ts";
import { LlmProxy } from "./llm-proxy.ts";
import { runStdioMcp } from "./mcp.ts";
import { SlotProxy, type WsData } from "./proxy.ts";
import { AuthRateLimiter, applySecurityHeaders } from "./security.ts";
import { StateStore } from "./state.ts";

export type ServerWsData = WsData | HubSocketData;

export interface SentinelServerHandle {
  server: Bun.Server<ServerWsData>;
  state: StateStore;
  lifecycle: LifecycleManager;
  proxy: SlotProxy;
  hub?: HubServer;
  control: ControlPlane;
  jobs: JobManager;
  stop: () => void;
}

export function createSentinelServer(
  config: SentinelConfig,
  providerOrLegacy?: SandboxProvider | LegacyBoatClient,
  opts: { startBackgroundLoops?: boolean; klav?: KlavClient } = {},
): SentinelServerHandle {
  const provider = providerOrLegacy ? asSandboxProvider(providerOrLegacy) : createSandboxProvider(config);
  const state = new StateStore(config.stateDir, config);
  const klav = opts.klav ?? (config.klav ? new KlavCliProvider(config.klav) : undefined);
  const rateLimiter = config.security?.disableRateLimit ? undefined : new AuthRateLimiter();
  const lifecycle = new LifecycleManager(config, state, provider, klav);
  const proxy = new SlotProxy(config, state, lifecycle, rateLimiter);
  const hub = config.hub ? new HubServer(config, state, provider, lifecycle, klav, rateLimiter) : undefined;
  const jobs = new JobManager();
  const control = new ControlPlane(config, state, lifecycle, proxy, jobs, hub, rateLimiter);
  const llmProxy = config.llmProxy ? new LlmProxy(config.llmProxy, state, rateLimiter) : null;

  if (opts.startBackgroundLoops !== false) {
    lifecycle.startBackgroundLoops();
  }

  const slotWs = proxy.wsHandlers();
  const server = Bun.serve<ServerWsData>({
    hostname: config.listenHost,
    port: config.listenPort,
    // Bun defaults idleTimeout to 10s, which resets long proxy/control requests. Max is 255s.
    idleTimeout: 255,
    fetch(req, srv) {
      const host = (req.headers.get("host") ?? "").split(":")[0].toLowerCase();
      if (llmProxy && config.llmProxy && host === config.llmProxy.host) {
        return llmProxy.handle(req);
      }
      if (hub && config.hub && host === config.hub.host) {
        return hub.handleHttp(req, srv as unknown as Bun.Server<HubSocketData>);
      }
      if (host === config.controlHost.toLowerCase() || host === "localhost" || host === "127.0.0.1") {
        return control.handle(req);
      }
      const slotName = proxy.resolveSlotFromHost(req.headers.get("host"));
      if (slotName) {
        return proxy.handleHttp(slotName, req, srv as unknown as Bun.Server<WsData>);
      }
      return applySecurityHeaders(new Response("Not found", { status: 404 }));
    },
    websocket: {
      open(ws) {
        if (hub && "kind" in ws.data && ws.data.kind === "hub") {
          hub.onWsOpen(ws as Bun.ServerWebSocket<HubWsData>);
        } else if (hub && "kind" in ws.data && ws.data.kind === "hub-media") {
          hub.onMediaOpen(ws as Bun.ServerWebSocket<HubMediaWsData>);
        } else {
          slotWs.open?.(ws as Bun.ServerWebSocket<WsData>);
        }
      },
      message(ws, msg) {
        if (hub && "kind" in ws.data && ws.data.kind === "hub") {
          hub.onWsMessage(ws as Bun.ServerWebSocket<HubWsData>, msg);
        } else if (hub && "kind" in ws.data && ws.data.kind === "hub-media") {
          hub.onMediaMessage(ws as Bun.ServerWebSocket<HubMediaWsData>, msg);
        } else {
          slotWs.message?.(ws as Bun.ServerWebSocket<WsData>, msg);
        }
      },
      close(ws, code, reason) {
        if (hub && "kind" in ws.data && ws.data.kind === "hub") {
          hub.onWsClose(ws as Bun.ServerWebSocket<HubWsData>, code, reason);
        } else if (hub && "kind" in ws.data && ws.data.kind === "hub-media") {
          hub.onMediaClose(ws as Bun.ServerWebSocket<HubMediaWsData>, code, reason);
        } else {
          slotWs.close?.(ws as Bun.ServerWebSocket<WsData>, code, reason);
        }
      },
    },
  });

  return {
    server,
    state,
    lifecycle,
    proxy,
    hub,
    control,
    jobs,
    stop: () => {
      hub?.stop();
      lifecycle.stopBackgroundLoops();
      server.stop(true);
    },
  };
}

function initConfigFile(targetPath = "./sentinel.yaml"): void {
  const abs = resolve(targetPath);
  if (existsSync(abs)) {
    throw new Error(`${abs} already exists. Remove it first or edit it directly.`);
  }
  const examplePath = join(dirname(fileURLToPath(import.meta.url)), "..", "sentinel.example.yaml");
  const controlSecret = randomBytes(32).toString("hex");
  const content = readFileSync(examplePath, "utf8").replace("${CONTROL_SECRET}", controlSecret);
  writeFileSync(abs, content, { mode: 0o600 });
  console.log(`Created ${abs} with generated controlSecret.`);
  console.log("Edit `domain`, `controlHost`, and `projects`, then run `bun run start`.");
}

async function callControlApi(
  baseUrl: string,
  secret: string,
  path: string,
  body?: Record<string, unknown>,
): Promise<Record<string, unknown>> {
  const res = await fetch(`${baseUrl.replace(/\/+$/, "")}${path}`, {
    method: body ? "POST" : "GET",
    headers: {
      authorization: `Bearer ${secret}`,
      ...(body ? { "content-type": "application/json" } : {}),
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  const data = (await res.json()) as Record<string, unknown>;
  if (!res.ok) {
    throw new Error(String(data.error ?? `HTTP ${res.status}`));
  }
  return data;
}

async function pollControlJob(baseUrl: string, secret: string, jobId: string): Promise<unknown> {
  while (true) {
    const job = (await callControlApi(baseUrl, secret, `/api/jobs/${encodeURIComponent(jobId)}`)) as {
      status: string;
      result?: unknown;
      error?: string;
    };
    if (job.status === "completed") return job.result;
    if (job.status === "failed") throw new Error(job.error ?? "Job failed");
    await Bun.sleep(1500);
  }
}

async function main(): Promise<void> {
  const [cmd = "serve", ...rest] = process.argv.slice(2);

  if (cmd === "init") {
    initConfigFile(rest[0]);
    return;
  }

  if (cmd === "mcp") {
    let baseUrl = process.env.SENTINEL_URL;
    let secret = process.env.CONTROL_SECRET;
    if (!baseUrl || !secret) {
      const cfg = loadConfig();
      baseUrl ??= `http://${cfg.listenHost}:${cfg.listenPort}`;
      secret ??= cfg.controlSecret;
    }
    await runStdioMcp({
      call: (path, body) => callControlApi(baseUrl!, secret!, path, body),
    });
    return;
  }

  const config = loadConfig();
  const localBase = process.env.SENTINEL_URL ?? `http://${config.listenHost}:${config.listenPort}`;

  switch (cmd) {
    case "serve": {
      // The daemon reaches Celesto on its own (reaper, chats), so it needs the key from the start
      if (config.provider?.type === "celesto" && !config.provider.apiKey) {
        throw new Error(CELESTO_MISSING_KEY_MESSAGE);
      }
      const handle = createSentinelServer(config);
      console.log(`[sentinel] listening on http://${handle.server.hostname}:${handle.server.port}`);
      console.log(`[sentinel] control panel: https://${config.controlHost}`);
      if (config.hub) {
        console.log(`[sentinel] hub (1-env multiplexer): https://${config.hub.host} (max ${config.hub.maxSandboxes} sandboxes)`);
      }
      if (config.slots.length > 0) {
        console.log(
          `[sentinel] slots: ${config.slots.map((s) => `https://${s.name}.${config.domain}`).join(", ")}`,
        );
      }

      const shutdown = (sig: string) => {
        console.log(`[sentinel] received ${sig}, shutting down cleanly...`);
        handle.stop();
        process.exit(0);
      };
      process.on("SIGINT", () => shutdown("SIGINT"));
      process.on("SIGTERM", () => shutdown("SIGTERM"));
      break;
    }

    // All CLI commands talk to the running daemon (single writer of state.json!)
    case "bake": {
      const target = rest[0] ?? "base";
      const started = await callControlApi(localBase, config.controlSecret, "/api/bake", { target, async: true });
      console.log(`Baking "${target}" (job ${started.jobId})...`);
      const res = await pollControlJob(localBase, config.controlSecret, String(started.jobId));
      console.log(JSON.stringify(res, null, 2));
      break;
    }

    case "pair": {
      const slotName = rest[0];
      if (!slotName) throw new Error("Usage: sentinel pair <slot-name>");
      const res = await callControlApi(localBase, config.controlSecret, "/api/pair", { slot: slotName });
      console.log(`\nPair T3 Mobile / Desktop to slot "${slotName}" (valid 15 min):\n`);
      console.log(`  ${res.url}\n`);
      break;
    }

    case "token": {
      const [slotName = "hub", ...labelParts] = rest;
      const label = labelParts.join(" ").trim() || "machine";
      const res = await callControlApi(localBase, config.controlSecret, "/api/token", {
        slot: slotName,
        label,
      });
      console.log(JSON.stringify(res, null, 2));
      break;
    }

    case "revoke": {
      const [slotName, clientId] = rest;
      if (!slotName) throw new Error("Usage: sentinel revoke <slot-name> [client-id]");
      const res = await callControlApi(localBase, config.controlSecret, "/api/revoke", {
        slot: slotName,
        ...(clientId ? { clientId } : {}),
      });
      console.log(JSON.stringify(res, null, 2));
      break;
    }

    case "forget-session": {
      const instanceId = rest[0];
      if (!instanceId) throw new Error("Usage: sentinel forget-session <instanceId>");
      const res = await callControlApi(localBase, config.controlSecret, "/api/forget-session", { instanceId });
      console.log(JSON.stringify(res, null, 2));
      break;
    }

    case "spawn": {
      const [project, slot] = rest;
      if (!project) throw new Error("Usage: sentinel spawn <project> [slot]");
      const started = await callControlApi(localBase, config.controlSecret, "/api/spawn", {
        project,
        ...(slot ? { slot } : {}),
        async: true,
      });
      const res = await pollControlJob(localBase, config.controlSecret, String(started.jobId));
      console.log(JSON.stringify(res, null, 2));
      break;
    }

    case "wake": {
      const slot = rest[0];
      if (!slot) throw new Error("Usage: sentinel wake <slot>");
      const started = await callControlApi(localBase, config.controlSecret, "/api/wake", { slot, async: true });
      const res = await pollControlJob(localBase, config.controlSecret, String(started.jobId));
      console.log(JSON.stringify(res, null, 2));
      break;
    }

    case "sleep": {
      const slot = rest[0];
      if (!slot) throw new Error("Usage: sentinel sleep <slot>");
      const res = await callControlApi(localBase, config.controlSecret, "/api/sleep", { slot });
      console.log(JSON.stringify(res, null, 2));
      break;
    }

    case "reset": {
      const slot = rest[0];
      if (!slot) throw new Error("Usage: sentinel reset <slot>");
      const res = await callControlApi(localBase, config.controlSecret, "/api/reset", { slot });
      console.log(JSON.stringify(res, null, 2));
      break;
    }

    case "status": {
      const res = await callControlApi(localBase, config.controlSecret, "/api/status");
      console.log(JSON.stringify(res, null, 2));
      break;
    }

    default:
      console.error(
        `Unknown command "${cmd}". Commands: init | serve | mcp | bake <target> | pair <slot> | token <hub> [label] | revoke <slot> [clientId] | forget-session <instanceId> | spawn <project> [slot] | wake <slot> | sleep <slot> | reset <slot> | status`,
      );
      process.exit(1);
  }
}

if (import.meta.main) {
  main().catch((err) => {
    console.error("[sentinel] fatal:", err instanceof Error ? err.message : err);
    process.exit(1);
  });
}
