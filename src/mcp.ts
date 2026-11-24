/**
 * Zero-dependency stdio MCP server (`sentinel mcp`) so coding agents & personal assistants
 * (e.g., Zumith, Claude Code, Codex, T3 Code) can manage disposable Boat sandboxes.
 *
 * Long operations (`sentinel_spawn`, `sentinel_wake`, `sentinel_bake`) run as async jobs by default
 * so MCP clients never hit 60s tool timeouts; poll progress with `sentinel_job`.
 */

interface JsonRpcRequest {
  jsonrpc: "2.0";
  id?: string | number | null;
  method: string;
  params?: Record<string, unknown>;
}

export interface McpApiClient {
  call(path: string, body?: Record<string, unknown>): Promise<Record<string, unknown>>;
}

const TOOLS = [
  {
    name: "sentinel_status",
    description:
      "List the Hub environment (live per-chat Boat sandboxes), any legacy slots, configured & dynamically added projects, and golden snapshot freshness.",
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true },
    inputSchema: { type: "object", properties: {} },
  },
  {
    name: "sentinel_add_project",
    description:
      "Register a GitHub repo (e.g. `owner/repo` or full git URL) dynamically without editing sentinel.yaml, push it live to T3 Code Desktop/Mobile, and optionally start an async job to bake its immutable `env-<name>` golden snapshot.",
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true },
    inputSchema: {
      type: "object",
      properties: {
        repo: { type: "string", description: "GitHub shorthand `owner/repo` or full git clone URL." },
        name: { type: "string", description: "Optional short project slug (defaults to repo name)." },
        size: { type: "string", description: "Optional Boat VM size override (e.g. default, medium, large)." },
        klavProfiles: {
          type: "array",
          items: { type: "string" },
          description: "Optional Klav proxy profiles to grant sandboxes for this repo.",
        },
        bake: { type: "boolean", description: "Whether to bake the immutable golden snapshot now (default true)." },
      },
      required: ["repo"],
    },
  },
  {
    name: "sentinel_job",
    description: "Check the status and result of an async spawn, wake, or bake job returned by sentinel.",
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true },
    inputSchema: {
      type: "object",
      properties: {
        jobId: { type: "string", description: "The jobId returned by sentinel_spawn, sentinel_wake, or sentinel_bake." },
      },
      required: ["jobId"],
    },
  },
  {
    name: "sentinel_spawn",
    description:
      "Start an async job to spawn a brand-new disposable Boat VM from an immutable project snapshot into a free slot, auto-register the project in T3, and optionally start an initial thread with `prompt`. Poll with `sentinel_job`.",
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true },
    inputSchema: {
      type: "object",
      properties: {
        project: { type: "string", description: "Configured project name (e.g. my-app, scratch)." },
        slot: { type: "string", description: "Optional explicit slot name (e.g. boat-1). Picks a free slot if omitted." },
        replace: { type: "boolean", description: "Must be true to destroy and replace an occupied slot. Default false." },
        idempotencyKey: { type: "string", description: "Optional dedupe key so retried tool calls do not spawn twice." },
        prompt: { type: "string", description: "Optional initial prompt to start a thread immediately inside the new Boat VM." },
        title: { type: "string", description: "Optional thread title when `prompt` is provided." },
        provider: { type: "string", description: "Optional T3 provider instance id (defaults to project.defaultProvider)." },
        model: { type: "string", description: "Optional model id (defaults to project.defaultModel)." },
        runtimeMode: {
          type: "string",
          enum: ["approval-required", "auto-accept-edits", "auto", "full-access"],
          description: "Runtime permission mode for the launched thread.",
        },
      },
      required: ["project"],
    },
  },
  {
    name: "sentinel_wake",
    description: "Start an async job to wake a sleeping Boat slot. Poll with `sentinel_job`.",
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true },
    inputSchema: {
      type: "object",
      properties: { slot: { type: "string" } },
      required: ["slot"],
    },
  },
  {
    name: "sentinel_sleep",
    description: "Pause a running Boat slot without modifying any saved snapshot.",
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true },
    inputSchema: {
      type: "object",
      properties: { slot: { type: "string" } },
      required: ["slot"],
    },
  },
  {
    name: "sentinel_reset",
    description:
      "DESTRUCTIVE: Permanently delete a disposable Boat VM, revoke its ephemeral Klav agent, and free the slot. Pass `slot: \"hub:<sandboxId>\"` for a Hub VM, `slot: \"hub\"` for all Hub VMs, or a legacy slot name (`boat-1`). Unpushed git work in the VM will be lost.",
    annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: true },
    inputSchema: {
      type: "object",
      properties: { slot: { type: "string" } },
      required: ["slot"],
    },
  },
  {
    name: "sentinel_bake",
    description:
      "Start an async job to rebuild an immutable golden snapshot (`base` or a configured project name) inside a throwaway VM. Poll with `sentinel_job`.",
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false },
    inputSchema: {
      type: "object",
      properties: {
        target: { type: "string", description: "`base` or a configured project name." },
        idempotencyKey: { type: "string" },
      },
      required: ["target"],
    },
  },
];

export async function handleMcpRequest(req: JsonRpcRequest, api: McpApiClient): Promise<Record<string, unknown> | null> {
  if (req.method === "notifications/initialized" || req.id === undefined) {
    return null;
  }

  try {
    switch (req.method) {
      case "ping":
        return { jsonrpc: "2.0", id: req.id, result: {} };

      case "initialize":
        return {
          jsonrpc: "2.0",
          id: req.id,
          result: {
            protocolVersion: "2024-11-05",
            capabilities: { tools: {} },
            serverInfo: { name: "t3-boat-sentinel", version: "0.1.0" },
          },
        };

      case "tools/list":
        return { jsonrpc: "2.0", id: req.id, result: { tools: TOOLS } };

      case "tools/call": {
        const params = req.params ?? {};
        const name = String(params.name ?? "");
        const args = (params.arguments ?? {}) as Record<string, unknown>;
        const data = await dispatchTool(name, args, api);
        return {
          jsonrpc: "2.0",
          id: req.id,
          result: {
            content: [{ type: "text", text: JSON.stringify(data, null, 2) }],
          },
        };
      }

      default:
        return {
          jsonrpc: "2.0",
          id: req.id,
          error: { code: -32601, message: `Method not found: ${req.method}` },
        };
    }
  } catch (err) {
    return {
      jsonrpc: "2.0",
      id: req.id,
      result: {
        isError: true,
        content: [{ type: "text", text: err instanceof Error ? err.message : String(err) }],
      },
    };
  }
}

async function dispatchTool(
  name: string,
  args: Record<string, unknown>,
  api: McpApiClient,
): Promise<Record<string, unknown>> {
  switch (name) {
    case "sentinel_status":
      return api.call("/api/status");
    case "sentinel_add_project":
      return api.call("/api/projects", {
        repo: String(args.repo),
        ...(args.name ? { name: String(args.name) } : {}),
        ...(args.size ? { size: String(args.size) } : {}),
        ...(Array.isArray(args.klavProfiles) ? { klavProfiles: args.klavProfiles.map(String) } : {}),
        ...(args.bake !== undefined ? { bake: Boolean(args.bake) } : {}),
      });
    case "sentinel_job":
      return api.call(`/api/jobs/${encodeURIComponent(String(args.jobId))}`);
    case "sentinel_spawn":
      return api.call("/api/spawn", {
        project: String(args.project),
        async: true,
        ...(args.slot ? { slot: String(args.slot) } : {}),
        ...(args.replace === true ? { replace: true } : {}),
        ...(args.idempotencyKey ? { idempotencyKey: String(args.idempotencyKey) } : {}),
        ...(args.prompt ? { prompt: String(args.prompt) } : {}),
        ...(args.title ? { title: String(args.title) } : {}),
        ...(args.provider ? { provider: String(args.provider) } : {}),
        ...(args.model ? { model: String(args.model) } : {}),
        ...(args.runtimeMode ? { runtimeMode: String(args.runtimeMode) } : {}),
      });
    case "sentinel_wake":
      return api.call("/api/wake", { slot: String(args.slot), async: true });
    case "sentinel_sleep":
      return api.call("/api/sleep", { slot: String(args.slot) });
    case "sentinel_reset":
      return api.call("/api/reset", { slot: String(args.slot) });
    case "sentinel_bake":
      return api.call("/api/bake", {
        target: String(args.target),
        async: true,
        ...(args.idempotencyKey ? { idempotencyKey: String(args.idempotencyKey) } : {}),
      });
    default:
      throw new Error(`Unknown tool "${name}"`);
  }
}

export async function runStdioMcp(api: McpApiClient): Promise<void> {
  const decoder = new TextDecoder();
  let buffer = "";

  for await (const chunk of Bun.stdin.stream()) {
    buffer += decoder.decode(chunk, { stream: true });
    let idx: number;
    while ((idx = buffer.indexOf("\n")) !== -1) {
      const line = buffer.slice(0, idx).trim();
      buffer = buffer.slice(idx + 1);
      if (!line) continue;
      let req: JsonRpcRequest;
      try {
        req = JSON.parse(line) as JsonRpcRequest;
      } catch {
        continue;
      }
      const res = await handleMcpRequest(req, api);
      if (res) {
        process.stdout.write(`${JSON.stringify(res)}\n`);
      }
    }
  }
}
