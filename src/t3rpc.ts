import { randomUUID } from "node:crypto";

export interface LaunchThreadOptions {
  prompt: string;
  title?: string;
  provider?: string;
  model?: string;
  runtimeMode?: "approval-required" | "auto-accept-edits" | "auto" | "full-access";
}

/**
 * Ensure `workspaceRoot` is registered as a project on a fresh `t3 serve` instance
 * using T3 Code's official `POST /api/projects/mutate` endpoint.
 * Returns the registered `projectId`.
 */
export async function ensureT3Project(
  upstreamUrl: string,
  upstreamToken: string,
  title: string,
  workspaceRoot: string,
): Promise<string> {
  const headers = {
    authorization: `Bearer ${upstreamToken}`,
    "x-t3-orchestration-protocol": "2",
  };

  // 1. Check if already present in shell snapshot
  const shellRes = await fetch(`${upstreamUrl}/api/orchestration/shell`, {
    headers,
    signal: AbortSignal.timeout(15_000),
  });
  if (shellRes.ok) {
    const shell = (await shellRes.json()) as {
      projects?: Array<{ id: string; workspaceRoot?: string }>;
    };
    const existing = shell.projects?.find((p) => p.workspaceRoot === workspaceRoot);
    if (existing?.id) return existing.id;
  }

  // 2. Register project cleanly via official HTTP mutation
  const projectId = randomUUID();
  const res = await fetch(`${upstreamUrl}/api/projects/mutate`, {
    method: "POST",
    headers: { ...headers, "content-type": "application/json" },
    body: JSON.stringify({
      type: "project.create",
      commandId: randomUUID(),
      projectId,
      title,
      workspaceRoot,
    }),
    signal: AbortSignal.timeout(15_000),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`T3 project.create failed (HTTP ${res.status}): ${body.slice(0, 300)}`);
  }
  return projectId;
}

/**
 * Optionally start the initial chat thread + first prompt directly inside the newly spawned
 * Boat VM using T3 Code's `orchestration.launchThread` RPC (ideal for personal assistants
 * like Zumith spawning a fresh chat over MCP).
 */
export async function launchT3Thread(
  upstreamUrl: string,
  upstreamToken: string,
  projectId: string,
  opts: LaunchThreadOptions,
): Promise<{ threadId: string }> {
  const wsBase = upstreamUrl.replace(/^https:\/\//, "wss://").replace(/^http:\/\//, "ws://");
  const threadId = randomUUID();
  const reqId = "1";

  const payload: Record<string, unknown> = {
    commandId: randomUUID(),
    threadId,
    projectId,
    title: opts.title ?? opts.prompt.slice(0, 72),
    runtimeMode: opts.runtimeMode ?? "full-access",
    interactionMode: "default",
    workspaceStrategy: { type: "root" },
    initialMessage: { text: opts.prompt, attachments: [] },
  };
  if (opts.provider && opts.model) {
    payload.modelSelection = { instanceId: opts.provider, model: opts.model };
  }

  await new Promise<void>((resolve, reject) => {
    const ws = new WebSocket(`${wsBase}/ws?orchestrationProtocol=2`, {
      headers: { authorization: `Bearer ${upstreamToken}` },
    } as unknown as string[]);

    const timer = setTimeout(() => {
      try {
        ws.close();
      } catch {
        // ignore
      }
      reject(new Error("orchestration.launchThread timed out"));
    }, 30_000);

    ws.addEventListener("open", () => {
      ws.send(
        JSON.stringify({
          _tag: "Request",
          id: reqId,
          tag: "orchestration.launchThread",
          payload,
          headers: [],
        }),
      );
    });

    ws.addEventListener("message", (ev) => {
      try {
        const parsed = JSON.parse(String(ev.data)) as unknown;
        const frames = Array.isArray(parsed) ? parsed : [parsed];
        for (const frame of frames) {
          if (!frame || typeof frame !== "object") continue;
          const obj = frame as Record<string, unknown>;
          if (obj._tag === "Exit" && String(obj.requestId) === reqId) {
            clearTimeout(timer);
            ws.close();
            const exit = obj.exit as Record<string, unknown> | undefined;
            if (exit?._tag === "Success") {
              resolve();
            } else {
              reject(new Error(`orchestration.launchThread failed: ${JSON.stringify(exit)}`));
            }
            return;
          }
        }
      } catch {
        // ignore non-JSON frames
      }
    });

    ws.addEventListener("error", () => {
      clearTimeout(timer);
      reject(new Error("WebSocket error during orchestration.launchThread"));
    });
  });

  return { threadId };
}
