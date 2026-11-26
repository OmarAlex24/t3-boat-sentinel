import { afterEach, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { BoatExecResult, BoatSandbox, ResumeResult } from "./boat.ts";
import type { SentinelConfig } from "./config.ts";
import { createSentinelServer, type SentinelServerHandle } from "./index.ts";
import { hashToken } from "./auth.ts";
import { HubMcpOAuthHandler, MCP_PEER_LINK_SOFTWARE_ID, requestOrigin } from "./hub-mcp-oauth.ts";
import { REPO_SYNC_MARKER } from "./repo-sync.ts";
import { loadT3Contracts } from "./testing/t3-contracts.ts";

const t3Contracts = await loadT3Contracts();

const cleanups: Array<() => void | Promise<void>> = [];
afterEach(async () => {
  while (cleanups.length > 0) {
    const fn = cleanups.pop()!;
    await fn();
  }
});

function pkceChallenge(verifier: string): string {
  return createHash("sha256").update(verifier).digest("base64url");
}

function makeTempDir(prefix = "t3-peer-link-"): string {
  const dir = mkdtempSync(join(tmpdir(), prefix));
  cleanups.push(() => rmSync(dir, { recursive: true, force: true }));
  return dir;
}

class MiniBoatClient {
  sandboxes = new Map<string, BoatSandbox>();
  upstreamBaseUrl: string | null = null;
  createGate: Promise<void> | null = null;
  createError: Error | null = null;
  private seq = 1;

  async create(name: string, _size: string, _fromSnapshot?: string): Promise<string> {
    if (this.createGate) await this.createGate;
    if (this.createError) throw this.createError;
    const id = `sb-${this.seq++}`;
    this.sandboxes.set(id, { id, name, state: "running", stoppedAt: null });
    return id;
  }

  async info(id: string): Promise<BoatSandbox> {
    const sb = this.sandboxes.get(id);
    if (!sb) throw new Error(`sandbox ${id} not found`);
    return sb;
  }

  async list(): Promise<BoatSandbox[]> {
    return Array.from(this.sandboxes.values());
  }

  async resume(id: string): Promise<ResumeResult> {
    const sb = this.sandboxes.get(id);
    if (!sb) return "data_expired";
    sb.state = "running";
    sb.stoppedAt = null;
    return "resumed";
  }

  async stop(id: string): Promise<void> {
    const sb = this.sandboxes.get(id);
    if (sb) {
      sb.state = "stopped";
      sb.stoppedAt = new Date().toISOString();
    }
  }

  async delete(id: string): Promise<void> {
    this.sandboxes.delete(id);
  }

  async saveSnapshot(): Promise<void> {}
  async namedSnapshots(): Promise<string[]> {
    return ["env-my-app"];
  }
  async hostPublic(id: string): Promise<string> {
    return this.upstreamBaseUrl ?? `https://${id}.boat.test`;
  }
  async exec(_id: string, script: string): Promise<BoatExecResult> {
    if (script.includes(REPO_SYNC_MARKER)) {
      return { exitCode: 0, stdout: JSON.stringify({ status: "skipped", reason: "not-a-repository" }), stderr: "" };
    }
    if (script.includes("provision-base.sh")) {
      return {
        exitCode: 0,
        stdout: JSON.stringify({
          environmentId: "vm-env-test",
          descriptor: {
            environmentId: "vm-env-test",
            label: "Boat VM",
            platform: { os: "linux", arch: "x64" },
            serverVersion: "0.1.14",
            orchestrationProtocolVersion: 2,
            capabilities: { repositoryIdentity: true },
          },
          authSession: {
            authenticated: true,
            auth: {
              policy: "remote-reachable",
              bootstrapMethods: ["one-time-token"],
              sessionMethods: ["bearer-access-token"],
              sessionCookieName: "t3_session",
            },
            scopes: ["orchestration:read", "orchestration:operate"],
            permissions: ["orchestration:read", "orchestration:operate"],
          },
          upstreamToken: "upstream-secret-token",
          t3Version: "t3 v0.1.14",
        }),
        stderr: "",
      };
    }
    return { exitCode: 0, stdout: "", stderr: "" };
  }
  async execOk(id: string, script: string): Promise<string> {
    const res = await this.exec(id, script);
    if (res.exitCode !== 0) throw new Error(res.stderr || `exit ${res.exitCode}`);
    return res.stdout;
  }
  async upload(): Promise<void> {}
}

interface FakeVmHandle {
  server: ReturnType<typeof Bun.serve>;
  baseUrl: string;
  vmProjectId: string;
  dispatches: Array<Record<string, unknown>>;
  threads: Map<string, Record<string, unknown>>;
  projections: Map<string, Record<string, unknown>>;
  pushShellUpdate: (thread: Record<string, unknown>) => void;
  setThreadCompleted: (threadId: string, assistantText: string, runId?: string) => void;
}

function startFakeVm(name: string): FakeVmHandle {
  const vmProjectId = `vm-proj-${name}`;
  const dispatches: Array<Record<string, unknown>> = [];
  const threads = new Map<string, Record<string, unknown>>();
  const projections = new Map<string, Record<string, unknown>>();
  const shellSubs = new Set<{ ws: Bun.ServerWebSocket<unknown>; reqId: string | number }>();
  let seq = 1;

  const projectRow = {
    id: vmProjectId,
    title: "my-app",
    workspaceRoot: "/home/user/my-app",
    defaultModelSelection: null,
    scripts: [],
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:00.000Z",
    deletedAt: null,
  };

  const pushShellUpdate = (thread: Record<string, unknown>) => {
    threads.set(String(thread.id), thread);
    seq += 1;
    for (const sub of shellSubs) {
      sub.ws.send(
        JSON.stringify({
          _tag: "Chunk",
          requestId: sub.reqId,
          values: [{ kind: "thread.updated", sequence: seq, location: "active", thread }],
        }),
      );
    }
  };

  const setThreadCompleted = (threadId: string, assistantText: string, runId = `run-${threadId}-1`) => {
    const existing = threads.get(threadId);
    const now = new Date().toISOString();
    const updated = {
      ...(existing ?? {
        id: threadId,
        projectId: vmProjectId,
        title: "Delegated task",
        modelSelection: { instanceId: "claudeAgent", model: "claude-sonnet-4-6" },
        runtimeMode: "auto",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdAt: now,
      }),
      updatedAt: now,
      latestRun: {
        id: runId,
        ordinal: 1,
        status: "completed",
        startedAt: now,
        completedAt: now,
        errorMessage: null,
      },
      latestTurn: null,
      activeRunId: null,
      pendingApprovals: [],
      pendingUserInputs: [],
      hasPendingApprovals: false,
      hasPendingUserInputs: false,
      activityStatus: "idle",
      status: "completed",
      settled: false,
      pinned: false,
      unread: false,
    };
    projections.set(threadId, {
      schemaVersion: 2,
      snapshotSequence: seq + 1,
      thread: updated,
      runs: [
        {
          id: runId,
          ordinal: 1,
          status: "completed",
          mode: "normal",
          requestedAt: now,
          startedAt: now,
          completedAt: now,
        },
      ],
      messages: [
        {
          id: `msg-user-${threadId}`,
          role: "user",
          text: "Run tests and report",
          createdBy: "agent",
          creationSource: "mcp",
          createdAt: now,
          updatedAt: now,
        },
        {
          id: `msg-assistant-${threadId}`,
          role: "assistant",
          text: assistantText,
          createdBy: "agent",
          creationSource: "provider",
          createdAt: now,
          updatedAt: now,
        },
      ],
      turnItems: [
        {
          id: `item-user-${threadId}`,
          runId,
          messageId: `msg-user-${threadId}`,
          type: "user_message",
          status: "completed",
          createdAt: now,
          updatedAt: now,
        },
        {
          id: `item-assistant-${threadId}`,
          runId,
          messageId: `msg-assistant-${threadId}`,
          type: "assistant_message",
          status: "completed",
          createdAt: now,
          updatedAt: now,
        },
      ],
      runtimeRequests: [],
      subagents: [],
    });
    pushShellUpdate(updated);
  };

  const server = Bun.serve({
    port: 0,
    fetch(req, srv) {
      const url = new URL(req.url);
      if (url.pathname === "/.well-known/t3/environment") {
        return Response.json({
          environmentId: `vm-env-${name}`,
          label: name,
          platform: { os: "linux", arch: "x64" },
          serverVersion: "0.1.14",
          orchestrationProtocolVersion: 2,
          capabilities: {
            repositoryIdentity: true,
            accessTokenRenewal: true,
            linkThreadStream: true,
            linkClientPairing: true,
            linkPairingForLink: true,
            mcpHostedApproval: true,
            mcpApprovalCodes: true,
          },
        });
      }
      if (url.pathname === "/api/auth/session") {
        return Response.json({
          authenticated: true,
          auth: {
            policy: "remote-reachable",
            bootstrapMethods: ["one-time-token"],
            sessionMethods: ["bearer-access-token"],
            sessionCookieName: "t3_session",
          },
          scopes: ["orchestration:read", "orchestration:operate"],
          permissions: ["orchestration:read", "orchestration:operate"],
          sessionMethod: "bearer-access-token",
          expiresAt: new Date(Date.now() + 86_400_000).toISOString(),
        });
      }
      if (url.pathname === "/api/auth/websocket-ticket" || url.pathname.startsWith("/api/auth/websocket-ticket/")) {
        return Response.json({ ticket: "vm-ticket", expiresAt: new Date(Date.now() + 60_000).toISOString() });
      }
      if (url.pathname === "/api/orchestration/shell") {
        return Response.json({
          schemaVersion: 2,
          snapshotSequence: seq,
          projects: [projectRow],
          threads: Array.from(threads.values()),
          archivedThreads: [],
        });
      }
      if (url.pathname === "/api/projects/mutate") {
        return Response.json({ ok: true, project: projectRow });
      }
      if (req.headers.get("upgrade")?.toLowerCase() === "websocket") {
        if (srv.upgrade(req)) return undefined;
      }
      return new Response("Not Found", { status: 404 });
    },
    websocket: {
      message(ws, raw) {
        const msg = JSON.parse(String(raw)) as { _tag?: string; id?: string | number; tag?: string; payload?: Record<string, unknown> };
        if (msg._tag !== "Request" || !msg.tag) return;
        const reqId = msg.id!;
        const payload = msg.payload ?? {};
        const ok = (value: unknown) => {
          ws.send(JSON.stringify({ _tag: "Exit", requestId: reqId, exit: { _tag: "Success", value } }));
        };

        if (msg.tag === "server.getConfig") {
          return ok({
            environment: { environmentId: `vm-env-${name}`, label: name },
            providers: [
              {
                instanceId: "claudeAgent",
                provider: "claudeAgent",
                driverKind: "claudeAgent",
                enabled: true,
                installed: true,
                status: "ready",
                auth: { status: "authenticated" },
                models: [
                  { slug: "claude-sonnet-4-6", name: "Claude Sonnet 4.6", isCustom: false, capabilities: null },
                  { slug: "claude-opus-4-6", name: "Claude Opus 4.6", isCustom: false, capabilities: null },
                ],
              },
            ],
            settings: {},
          });
        }
        if (msg.tag === "orchestration.subscribeShell") {
          shellSubs.add({ ws, reqId });
          ws.send(
            JSON.stringify({
              _tag: "Chunk",
              requestId: reqId,
              values: [
                {
                  kind: "snapshot",
                  snapshot: {
                    schemaVersion: 2,
                    snapshotSequence: seq,
                    projects: [projectRow],
                    threads: Array.from(threads.values()),
                    archivedThreads: [],
                  },
                },
                { kind: "synchronized" },
              ],
            }),
          );
          return;
        }
        if (msg.tag === "orchestration.launchThread") {
          dispatches.push({ rpc: "launchThread", ...payload });
          const tid = String(payload.threadId);
          const now = new Date().toISOString();
          const runId = `run-${tid}-1`;
          const row = {
            id: tid,
            projectId: vmProjectId,
            title: String(payload.title ?? "Untitled"),
            modelSelection: payload.modelSelection ?? { instanceId: "claudeAgent", model: "claude-sonnet-4-6" },
            runtimeMode: payload.runtimeMode ?? "auto",
            interactionMode: payload.interactionMode ?? "default",
            branch: null,
            worktreePath: null,
            createdAt: now,
            updatedAt: now,
            latestRun: { id: runId, ordinal: 1, status: "running", startedAt: now, completedAt: null, errorMessage: null },
            latestTurn: null,
            activeRunId: runId,
            pendingApprovals: [],
            pendingUserInputs: [],
            hasPendingApprovals: false,
            hasPendingUserInputs: false,
            activityStatus: "working",
            status: "running",
            settled: false,
            pinned: false,
            unread: false,
          };
          projections.set(tid, {
            schemaVersion: 2,
            snapshotSequence: seq + 1,
            thread: row,
            runs: [{ id: runId, ordinal: 1, status: "running", mode: "normal", requestedAt: now, startedAt: now, completedAt: null }],
            messages: [
              {
                id: `msg-user-${tid}`,
                role: "user",
                text: String((payload.message as { text?: string } | undefined)?.text ?? ""),
                createdBy: "agent",
                creationSource: "mcp",
                createdAt: now,
                updatedAt: now,
              },
            ],
            turnItems: [
              {
                id: `item-user-${tid}`,
                runId,
                messageId: `msg-user-${tid}`,
                type: "user_message",
                status: "completed",
                createdAt: now,
                updatedAt: now,
              },
            ],
            runtimeRequests: [],
            subagents: [],
          });
          pushShellUpdate(row);
          return ok({ sequence: ++seq, threadId: tid, runId, status: "running" });
        }
        if (msg.tag === "orchestration.getThreadProjection") {
          const tid = String(payload.threadId);
          const proj = projections.get(tid);
          if (!proj) {
            ws.send(
              JSON.stringify({
                _tag: "Exit",
                requestId: reqId,
                exit: { _tag: "Failure", cause: { _tag: "Fail", error: { _tag: "ThreadNotFoundError", message: "not found" } } },
              }),
            );
            return;
          }
          ok(proj);
          return;
        }
        if (msg.tag === "orchestration.dispatchCommand") {
          dispatches.push({ rpc: "dispatchCommand", ...payload });
          const tid = String(payload.threadId ?? "");
          const type = String(payload.type ?? "");
          seq += 1;
          if (type === "message.dispatch") {
            const runId = `run-${tid}-${seq}`;
            return ok({ sequence: seq, runId, status: payload.dispatchMode === "queue_after_active" ? "queued" : "running" });
          }
          if (type === "thread.stop" || type === "run.interrupt") {
            const existing = threads.get(tid);
            if (existing) {
              const now = new Date().toISOString();
              const updated = {
                ...existing,
                updatedAt: now,
                activeRunId: null,
                status: "interrupted",
                latestRun: {
                  ...((existing.latestRun as Record<string, unknown> | undefined) ?? { id: `run-${tid}-1`, ordinal: 1 }),
                  status: "interrupted",
                  completedAt: now,
                },
              };
              const p = projections.get(tid) as { thread?: Record<string, unknown>; runs?: unknown[] } | undefined;
              if (p) {
                p.thread = updated;
                p.runs = [{ id: `run-${tid}-1`, ordinal: 1, status: "interrupted", mode: "normal", requestedAt: now, startedAt: now, completedAt: now }];
              }
              pushShellUpdate(updated);
            }
            return ok({ sequence: seq, interrupted: true });
          }
          return ok({ sequence: seq });
        }
        if (msg.tag === "server.subscribeConfig" || msg.tag === "subscribeServerConfig") {
          return;
        }
        return ok({ sequence: ++seq });
      },
      close(ws) {
        for (const sub of shellSubs) {
          if (sub.ws === ws) shellSubs.delete(sub);
        }
      },
    },
  });

  return {
    server,
    baseUrl: `http://127.0.0.1:${server.port}`,
    vmProjectId,
    dispatches,
    threads,
    projections,
    pushShellUpdate,
    setThreadCompleted,
  };
}

function makeHubConfig(stateDir: string): SentinelConfig {
  return {
    domain: "boat.example.com",
    controlHost: "sentinel.boat.example.com",
    controlSecret: "test-secret-0123456789abcdef",
    listenHost: "127.0.0.1",
    listenPort: 0,
    stateDir,
    hub: { host: "boat.boat.example.com", label: "Boat Cloud", maxSandboxes: 4 },
    boat: {
      baseSnapshot: "base-t3",
      defaultSize: "2gb",
      sleepAfterSecs: 1800,
      archiveGraceSecs: 60,
      deleteWhen: "archived-or-settled",
      archiveAction: "delete",
      settleAction: "park",
      reaperIntervalSecs: 600,
      maxSleepingHours: 0,
      sandboxPrefix: "t3-",
      hostVisibility: "public",
      injectFiles: [],
    },
    t3: { version: "0.1.14", channel: "stable", autoUpdateOnWake: false },
    projects: {
      "my-app": {
        repo: "https://github.com/OmarAlex24/my-app.git",
        snapshot: "env-my-app",
        workdir: "/home/user/my-app",
        defaultProvider: "claudeAgent",
        defaultModel: "claude-sonnet-4-6",
        bakeCmd: "git pull",
        bootCmd: "git fetch origin",
      },
    },
    wakePolicy: "on-connect",
    slots: [],
  };
}

async function pairOwnerClient(base: string, cfg: SentinelConfig): Promise<string> {
  const pairRes = await fetch(`${base}/api/pair`, {
    method: "POST",
    headers: { host: cfg.controlHost, authorization: `Bearer ${cfg.controlSecret}`, "content-type": "application/json" },
    body: JSON.stringify({ slot: "hub" }),
  });
  expect(pairRes.status).toBe(200);
  const { code } = (await pairRes.json()) as { code: string };
  const tokRes = await fetch(`${base}/oauth/token`, {
    method: "POST",
    headers: { host: cfg.hub!.host, "content-type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ subject_token: code, client_label: "MacBook Pro" }),
  });
  expect(tokRes.status).toBe(200);
  return ((await tokRes.json()) as { access_token: string }).access_token;
}

async function linkPeerViaOAuth(
  base: string,
  hubHost: string,
  ownerToken: string,
  access: "read-only" | "approval-required" | "auto-accept-edits" | "auto" | "full-access",
  clientName = "Mac T3 Code",
): Promise<{ accessToken: string; clientId: string; codeId: string }> {
  // 1. Mint one-time peer-link pairing code as owner (same as T3 Desktop LinkRequestCard)
  const mintRes = await fetch(`${base}/api/auth/pairing-token`, {
    method: "POST",
    headers: { host: hubHost, authorization: `Bearer ${ownerToken}`, "content-type": "application/json" },
    body: JSON.stringify({ label: clientName, access, purpose: "peer-link" }),
  });
  expect(mintRes.status).toBe(200);
  const minted = (await mintRes.json()) as { id: string; credential: string; access: string; purpose: string; expiresAt: string };
  expect(minted.purpose).toBe("peer-link");
  expect(minted.access).toBe(access);

  // 2. Dynamic client registration (RFC 7591)
  const redirectUri = "http://127.0.0.1:41234/callback";
  const regRes = await fetch(`${base}/oauth/mcp/register`, {
    method: "POST",
    headers: { host: hubHost, "content-type": "application/json" },
    body: JSON.stringify({
      client_name: clientName,
      redirect_uris: [redirectUri],
      grant_types: ["authorization_code"],
      response_types: ["code"],
      token_endpoint_auth_method: "none",
      software_id: MCP_PEER_LINK_SOFTWARE_ID,
    }),
  });
  expect(regRes.status).toBe(201);
  const reg = (await regRes.json()) as { client_id: string };

  // 3. Approve decision with PKCE S256 + pairing code
  const verifier = "verifier-0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  const challenge = pkceChallenge(verifier);
  const resource = `https://${hubHost}/mcp`;
  const decisionRes = await fetch(`${base}/oauth/mcp/decision`, {
    method: "POST",
    headers: { host: hubHost, "content-type": "application/json" },
    body: JSON.stringify({
      decision: {
        _tag: "pairing-code",
        access,
        code: minted.credential,
      },
      authorization: {
        response_type: "code",
        client_id: reg.client_id,
        redirect_uri: redirectUri,
        code_challenge: challenge,
        code_challenge_method: "S256",
        state: "state-abc",
        resource,
      },
    }),
  });
  expect(decisionRes.status).toBe(200);
  const { redirectTo } = (await decisionRes.json()) as { redirectTo: string };
  const cbUrl = new URL(redirectTo);
  expect(cbUrl.searchParams.get("state")).toBe("state-abc");
  const authCode = cbUrl.searchParams.get("code")!;
  expect(authCode).toBeTruthy();

  // 4. Exchange authorization code + PKCE verifier for Bearer token
  const tokenRes = await fetch(`${base}/oauth/mcp/token`, {
    method: "POST",
    headers: { host: hubHost, "content-type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "authorization_code",
      code: authCode,
      redirect_uri: redirectUri,
      client_id: reg.client_id,
      code_verifier: verifier,
      resource,
    }),
  });
  expect(tokenRes.status).toBe(200);
  const tok = (await tokenRes.json()) as { access_token: string; token_type: string; expires_in: number; scope: string };
  expect(tok.token_type).toBe("Bearer");
  return { accessToken: tok.access_token, clientId: reg.client_id, codeId: minted.id };
}

function makeMcpCaller(base: string, hubHost: string, token: string, extraHeaders: Record<string, string> = {}) {
  let sessionId: string | null = null;
  let nextId = 1;
  return {
    async rpc(method: string, params: Record<string, unknown> = {}, headersOverride: Record<string, string> = {}) {
      const res = await fetch(`${base}/mcp`, {
        method: "POST",
        headers: {
          host: hubHost,
          authorization: `Bearer ${token}`,
          "content-type": "application/json",
          accept: "application/json, text/event-stream",
          "mcp-protocol-version": "2025-06-18",
          ...(sessionId ? { "mcp-session-id": sessionId } : {}),
          ...extraHeaders,
          ...headersOverride,
        },
        body: JSON.stringify({ jsonrpc: "2.0", id: nextId++, method, params }),
      });
      const sid = res.headers.get("mcp-session-id");
      if (sid) sessionId = sid;
      const body = res.status === 202 ? null : ((await res.json()) as Record<string, unknown>);
      return { res, body };
    },
    async callTool(name: string, args: Record<string, unknown> = {}, headersOverride: Record<string, string> = {}) {
      const { res, body } = await this.rpc("tools/call", { name, arguments: args }, headersOverride);
      const result = body?.result as
        | { isError?: boolean; structuredContent: Record<string, unknown>; content: Array<{ type: string; text: string }> }
        | undefined;
      return { res, result, parsedText: result?.content?.[0]?.text ? JSON.parse(result.content[0].text) : null };
    },
  };
}

describe("Hub Linked Environment (Native T3 Peer Link)", () => {
  test("requestOrigin respects proxy headers and configured public host", () => {
    expect(
      requestOrigin(new Request("http://127.0.0.1:8787/.well-known/oauth-authorization-server", {
        headers: { host: "boat.omaralex.dev" },
      }), "boat.omaralex.dev"),
    ).toBe("https://boat.omaralex.dev");

    expect(
      requestOrigin(new Request("http://127.0.0.1:8787/mcp", {
        headers: { "x-forwarded-host": "boat.omaralex.dev", "x-forwarded-proto": "https" },
      })),
    ).toBe("https://boat.omaralex.dev");

    expect(
      requestOrigin(new Request("http://127.0.0.1:8787/mcp", {
        headers: { host: "boat.omaralex.dev", "cf-visitor": '{"scheme":"https"}' },
      })),
    ).toBe("https://boat.omaralex.dev");

    expect(requestOrigin(new Request("http://127.0.0.1:8787/mcp"))).toBe("http://127.0.0.1:8787");
  });

  test("descriptor advertises peer-link capabilities and strips unimplemented VM flags", async () => {
    const dir = makeTempDir();
    const cfg = makeHubConfig(dir);
    const boat = new MiniBoatClient();
    const handle = createSentinelServer(cfg, boat, { startBackgroundLoops: false });
    cleanups.push(() => handle.stop());

    // Simulate a VM template that advertised capabilities the Hub does not implement
    handle.state.setTemplates(
      {
        environmentId: "vm-template",
        label: "VM",
        platform: { os: "linux", arch: "x64" },
        serverVersion: "0.1.14",
        orchestrationProtocolVersion: 2,
        capabilities: {
          repositoryIdentity: true,
          accessTokenRenewal: true,
          linkThreadStream: true,
          linkClientPairing: true,
          linkPairingForLink: true,
          mcpHostedApproval: true,
          mcpApprovalCodes: true,
        },
      },
      null,
    );

    const base = `http://127.0.0.1:${handle.server.port}`;
    const res = await fetch(`${base}/.well-known/t3/environment`, { headers: { host: cfg.hub!.host } });
    expect(res.status).toBe(200);
    const desc = (await res.json()) as { capabilities: Record<string, unknown> };
    expect(desc.capabilities.mcpModeLimitHeader).toBe(true);
    expect(desc.capabilities.linkFence).toBe(true);
    expect(desc.capabilities.peerLinkPairingCodes).toBe(true);
    expect(desc.capabilities.linkThreadStream).toBeUndefined();
    expect(desc.capabilities.linkClientPairing).toBeUndefined();
    expect(desc.capabilities.linkPairingForLink).toBeUndefined();
    expect(desc.capabilities.mcpHostedApproval).toBeUndefined();
    expect(desc.capabilities.mcpApprovalCodes).toBeUndefined();
    expect(desc.capabilities.accessTokenRenewal).toBeUndefined();

    if (t3Contracts) {
      expect(t3Contracts.decodeHttp("metadata", "descriptor", desc)).toEqual({ ok: true });
    }
  });

  test("full OAuth 2.1 + PKCE S256 + RFC 7591 + RFC 9728 + RFC 7009 flow, isolation, and restart resilience", async () => {
    const dir = makeTempDir();
    const cfg = makeHubConfig(dir);
    const boat = new MiniBoatClient();
    const handle = createSentinelServer(cfg, boat, { startBackgroundLoops: false });
    cleanups.push(() => handle.stop());
    const base = `http://127.0.0.1:${handle.server.port}`;
    const hubHost = cfg.hub!.host;
    const ownerToken = await pairOwnerClient(base, cfg);

    // 1. RFC 9728 protected resource metadata & RFC 8414 authorization server metadata
    const prRes = await fetch(`${base}/.well-known/oauth-protected-resource/mcp`, { headers: { host: hubHost } });
    expect(prRes.status).toBe(200);
    const pr = (await prRes.json()) as Record<string, unknown>;
    expect(pr.resource).toBe(`https://${hubHost}/mcp`);
    expect(pr.authorization_servers).toEqual([`https://${hubHost}`]);

    const asRes = await fetch(`${base}/.well-known/oauth-authorization-server`, { headers: { host: hubHost } });
    expect(asRes.status).toBe(200);
    const asMeta = (await asRes.json()) as Record<string, unknown>;
    expect(asMeta.issuer).toBe(`https://${hubHost}`);
    expect(asMeta.token_endpoint).toBe(`https://${hubHost}/oauth/mcp/token`);
    expect(asMeta.registration_endpoint).toBe(`https://${hubHost}/oauth/mcp/register`);
    expect(asMeta.revocation_endpoint).toBe(`https://${hubHost}/oauth/mcp/revoke`);

    // 2. Unauthenticated /mcp returns 401 with RFC 9728 WWW-Authenticate header
    const unauth = await fetch(`${base}/mcp`, {
      method: "POST",
      headers: { host: hubHost, "content-type": "application/json" },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "initialize", params: {} }),
    });
    expect(unauth.status).toBe(401);
    expect(unauth.headers.get("www-authenticate")).toContain(`resource_metadata="https://${hubHost}/.well-known/oauth-protected-resource/mcp"`);

    // 3. Mint peer-link code with access=auto: cannot be redeemed on /oauth/token (interactive)
    const mintRes = await fetch(`${base}/api/auth/pairing-token`, {
      method: "POST",
      headers: { host: hubHost, authorization: `Bearer ${ownerToken}`, "content-type": "application/json" },
      body: JSON.stringify({ label: "Peer Test", access: "auto", purpose: "peer-link" }),
    });
    expect(mintRes.status).toBe(200);
    const minted = (await mintRes.json()) as { id: string; credential: string };

    const interactiveAttempt = await fetch(`${base}/oauth/token`, {
      method: "POST",
      headers: { host: hubHost, "content-type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({ subject_token: minted.credential, client_label: "Sneaky" }),
    });
    expect(interactiveAttempt.status).toBe(400);

    // 4. Register client & try over-scoped access (full-access > auto) -> rejected WITHOUT burning code!
    const redirectUri = "http://127.0.0.1:45555/cb";
    const regRes = await fetch(`${base}/oauth/mcp/register`, {
      method: "POST",
      headers: { host: hubHost, "content-type": "application/json" },
      body: JSON.stringify({
        client_name: "Mac Peer",
        redirect_uris: [redirectUri],
        software_id: MCP_PEER_LINK_SOFTWARE_ID,
      }),
    });
    expect(regRes.status).toBe(201);
    const reg = (await regRes.json()) as { client_id: string };

    const verifier = "v0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    const challenge = pkceChallenge(verifier);
    const overScoped = await fetch(`${base}/oauth/mcp/decision`, {
      method: "POST",
      headers: { host: hubHost, "content-type": "application/json" },
      body: JSON.stringify({
        decision: {
          _tag: "pairing-code",
          access: "full-access",
          code: minted.credential,
        },
        authorization: {
          response_type: "code",
          client_id: reg.client_id,
          redirect_uri: redirectUri,
          code_challenge: challenge,
          code_challenge_method: "S256",
          resource: `https://${hubHost}/mcp`,
        },
      }),
    });
    expect(overScoped.status).toBe(400);

    // 5. Same code STILL works when requesting allowed access ("auto")
    const approved = await fetch(`${base}/oauth/mcp/decision`, {
      method: "POST",
      headers: { host: hubHost, "content-type": "application/json" },
      body: JSON.stringify({
        decision: {
          _tag: "pairing-code",
          access: "auto",
          code: minted.credential,
        },
        authorization: {
          response_type: "code",
          client_id: reg.client_id,
          redirect_uri: redirectUri,
          code_challenge: challenge,
          code_challenge_method: "S256",
          resource: `https://${hubHost}/mcp`,
        },
      }),
    });
    expect(approved.status).toBe(200);
    const authCode = new URL(((await approved.json()) as { redirectTo: string }).redirectTo).searchParams.get("code")!;

    // 6. Wrong PKCE verifier fails AND burns the auth code
    const badPkce = await fetch(`${base}/oauth/mcp/token`, {
      method: "POST",
      headers: { host: hubHost, "content-type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        grant_type: "authorization_code",
        code: authCode,
        redirect_uri: redirectUri,
        client_id: reg.client_id,
        code_verifier: "wrong-verifier-0123456789abcdefghijklmnopqrstuvwxyz",
        resource: `https://${hubHost}/mcp`,
      }),
    });
    expect(badPkce.status).toBe(400);

    // Replaying the burned auth code even with the right verifier fails
    const replayCode = await fetch(`${base}/oauth/mcp/token`, {
      method: "POST",
      headers: { host: hubHost, "content-type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        grant_type: "authorization_code",
        code: authCode,
        redirect_uri: redirectUri,
        client_id: reg.client_id,
        code_verifier: verifier,
        resource: `https://${hubHost}/mcp`,
      }),
    });
    expect(replayCode.status).toBe(400);

    // 7. Complete clean OAuth link and verify isolation: peer token is REJECTED on non-MCP endpoints
    const peer = await linkPeerViaOAuth(base, hubHost, ownerToken, "auto", "Mac Coordinator");
    for (const [method, path] of [
      ["GET", "/api/auth/session"],
      ["GET", "/api/orchestration/shell"],
      ["POST", "/api/auth/websocket-ticket"],
      ["POST", "/api/auth/pairing-token"],
      ["POST", "/api/auth/clients/revoke"],
    ] as const) {
      const r = await fetch(`${base}${path}`, {
        method,
        headers: { host: hubHost, authorization: `Bearer ${peer.accessToken}`, "content-type": "application/json" },
        ...(method === "POST" ? { body: "{}" } : {}),
      });
      expect(r.status).toBe(401);
    }

    // 8. Stateless signed client_id survives a fresh HubMcpOAuthHandler instance (Hub restart)
    const freshOAuth = new HubMcpOAuthHandler(cfg, handle.state, handle.hub!.getAuth());
    const mint2 = await fetch(`${base}/api/auth/pairing-token`, {
      method: "POST",
      headers: { host: hubHost, authorization: `Bearer ${ownerToken}`, "content-type": "application/json" },
      body: JSON.stringify({ label: "Post-restart", access: "auto", purpose: "peer-link" }),
    });
    const minted2 = (await mint2.json()) as { credential: string };
    const postRestartRes = await freshOAuth.handleHttp(
      new Request(`https://${hubHost}/oauth/mcp/decision`, {
        method: "POST",
        headers: { host: hubHost, "content-type": "application/json" },
        body: JSON.stringify({
          decision: {
            _tag: "pairing-code",
            access: "auto",
            code: minted2.credential,
          },
          authorization: {
            response_type: "code",
            client_id: peer.clientId,
            redirect_uri: "http://127.0.0.1:41234/callback",
            code_challenge: challenge,
            code_challenge_method: "S256",
            resource: `https://${hubHost}/mcp`,
          },
        }),
      }),
      new URL(`https://${hubHost}/oauth/mcp/decision`),
    );
    expect(postRestartRes?.status).toBe(200);

    // 9. Rolling link expiry header on /mcp
    const mcp = makeMcpCaller(base, hubHost, peer.accessToken);
    const init = await mcp.rpc("initialize", { protocolVersion: "2025-06-18", capabilities: {}, clientInfo: { name: "t3", version: "1" } });
    expect(init.res.status).toBe(200);
    expect(init.res.headers.get("t3-link-expires-at")).toBeTruthy();

    // 10. Revocation via /oauth/mcp/revoke invalidates the token immediately
    const revokeRes = await fetch(`${base}/oauth/mcp/revoke`, {
      method: "POST",
      headers: { host: hubHost, "content-type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({ token: peer.accessToken }),
    });
    expect(revokeRes.status).toBe(200);

    const afterRevoke = await mcp.rpc("tools/list");
    expect(afterRevoke.res.status).toBe(401);

    // 11. Owner cannot revoke their own session (403)
    const ownerSession = (await (await fetch(`${base}/api/auth/session`, {
      headers: { host: hubHost, authorization: `Bearer ${ownerToken}` },
    })).json()) as { sessionId: string };
    const selfRevoke = await fetch(`${base}/api/auth/clients/revoke`, {
      method: "POST",
      headers: { host: hubHost, authorization: `Bearer ${ownerToken}`, "content-type": "application/json" },
      body: JSON.stringify({ sessionId: ownerSession.sessionId }),
    });
    expect(selfRevoke.status).toBe(403);

    // 12. Validate HTTP responses against T3 Code's actual Effect contracts
    if (t3Contracts) {
      expect(t3Contracts.decodeHttp("mcpOAuth", "mcpProtectedResource", pr)).toEqual({ ok: true });
      expect(t3Contracts.decodeHttp("mcpOAuth", "authorizationServer", asMeta)).toEqual({ ok: true });
      expect(t3Contracts.decodeHttp("auth", "pairingCredential", minted)).toEqual({ ok: true });
    }
  });

  test("mode limits, read-only enforcement, and link fencing across peer links", async () => {
    const dir = makeTempDir();
    const cfg = makeHubConfig(dir);
    const vm = startFakeVm("fence");
    cleanups.push(() => vm.server.stop(true));
    const boat = new MiniBoatClient();
    boat.upstreamBaseUrl = vm.baseUrl;
    const handle = createSentinelServer(cfg, boat, { startBackgroundLoops: false });
    cleanups.push(() => handle.stop());
    const base = `http://127.0.0.1:${handle.server.port}`;
    const hubHost = cfg.hub!.host;
    const ownerToken = await pairOwnerClient(base, cfg);

    const readOnlyPeer = await linkPeerViaOAuth(base, hubHost, ownerToken, "read-only", "Reader");
    const autoPeerA = await linkPeerViaOAuth(base, hubHost, ownerToken, "auto", "Auto Peer A");
    const autoPeerB = await linkPeerViaOAuth(base, hubHost, ownerToken, "auto", "Auto Peer B");
    const fullPeer = await linkPeerViaOAuth(base, hubHost, ownerToken, "full-access", "Full Peer");

    const ro = makeMcpCaller(base, hubHost, readOnlyPeer.accessToken);
    const peerA = makeMcpCaller(base, hubHost, autoPeerA.accessToken);
    const peerB = makeMcpCaller(base, hubHost, autoPeerB.accessToken);
    const full = makeMcpCaller(base, hubHost, fullPeer.accessToken);

    // Malformed T3-Mode-Limit header returns 400
    const malformed = await peerA.rpc("tools/list", {}, { "t3-mode-limit": "not-json" });
    expect(malformed.res.status).toBe(400);

    // Read-only link can read capabilities & projects, cannot launch
    const caps = await ro.callTool("orchestrator_capabilities");
    expect(caps.result?.isError).toBeFalsy();
    const projs = await ro.callTool("t3_project_list");
    expect(projs.result?.isError).toBeFalsy();
    const hubProjectId = String((projs.parsedText.projects as Array<{ id: string }>)[0].id);

    const roLaunch = await ro.callTool("t3_thread_launch", { projectId: hubProjectId, title: "Blocked" });
    expect(roLaunch.result?.isError).toBe(true);
    expect(roLaunch.parsedText.code).toBe("capability_denied");

    // Mode escalation via T3-Mode-Limit is rejected
    const escRuntime = await peerA.callTool(
      "t3_thread_launch",
      { projectId: hubProjectId, title: "Escalate", runtimeMode: "full-access" },
      { "t3-mode-limit": "auto-accept-edits/default" },
    );
    expect(escRuntime.result?.isError).toBe(true);
    expect(escRuntime.parsedText.code).toBe("runtime_mode_escalation_denied");

    const escInteraction = await peerA.callTool(
      "t3_thread_launch",
      { projectId: hubProjectId, title: "Escalate plan", interactionMode: "default" },
      { "t3-mode-limit": "auto/plan" },
    );
    expect(escInteraction.result?.isError).toBe(true);
    expect(escInteraction.parsedText.code).toBe("interaction_mode_escalation_denied");

    // Fenced auto peer cannot call Hub-admin sentinel tools
    const fencedAdmin = await peerA.callTool("sentinel_add_project", { repo: "https://github.com/acme/x.git" });
    expect(fencedAdmin.result?.isError).toBe(true);
    expect(fencedAdmin.parsedText.code).toBe("capability_denied");

    // Peer A launches a thread
    const launched = await peerA.callTool("t3_thread_launch", {
      projectId: hubProjectId,
      title: "Peer A Thread",
      message: "Do work",
    });
    expect(launched.result?.isError).toBeFalsy();
    const threadId = String(launched.parsedText.threadId);
    expect(handle.state.getHub()?.threadLinkOrigins?.[threadId]).toBeDefined();

    // Wait for VM to finish booting so thread is live on VM
    for (let i = 0; i < 40 && !vm.threads.has(threadId); i++) await Bun.sleep(25);
    expect(vm.threads.has(threadId)).toBe(true);

    // Peer A CAN mutate its own thread
    const sendA = await peerA.callTool("t3_thread_send", { threadId, message: "Follow up" });
    expect(sendA.result?.isError).toBeFalsy();

    // Peer B (different fenced link) CANNOT mutate Peer A's thread
    const sendB = await peerB.callTool("t3_thread_send", { threadId, message: "Hijack" });
    expect(sendB.result?.isError).toBe(true);
    expect(sendB.parsedText.code).toBe("capability_denied");

    const intB = await peerB.callTool("t3_thread_interrupt", { threadId, stop: true });
    expect(intB.result?.isError).toBe(true);
    expect(intB.parsedText.code).toBe("capability_denied");

    // Full-access peer is NOT fenced and CAN mutate Peer A's thread
    const sendFull = await full.callTool("t3_thread_send", { threadId, message: "Admin override" });
    expect(sendFull.result?.isError).toBeFalsy();
  });

  test("full remote delegation lifecycle: capabilities, launch, wait (boot -> complete), read chunking, interrupt, boot failure, and T3 contract decoding", async () => {
    const dir = makeTempDir();
    const cfg = makeHubConfig(dir);
    const vm = startFakeVm("delegate");
    cleanups.push(() => vm.server.stop(true));
    const boat = new MiniBoatClient();
    boat.upstreamBaseUrl = vm.baseUrl;
    const handle = createSentinelServer(cfg, boat, { startBackgroundLoops: false });
    cleanups.push(() => handle.stop());
    const base = `http://127.0.0.1:${handle.server.port}`;
    const hubHost = cfg.hub!.host;
    const ownerToken = await pairOwnerClient(base, cfg);
    const peer = await linkPeerViaOAuth(base, hubHost, ownerToken, "full-access", "Mac Coordinator");

    const senderHeader = [
      encodeURIComponent("env-mac-local"),
      encodeURIComponent("thread-coordinator-1"),
      encodeURIComponent("Coordinator chat"),
    ].join("/");
    const mcp = makeMcpCaller(base, hubHost, peer.accessToken, { "t3-sender-thread": senderHeader });

    // 1. 0-VM orchestrator_capabilities + search
    const caps = await mcp.callTool("orchestrator_capabilities");
    expect(caps.result?.isError).toBeFalsy();
    expect(caps.parsedText.providers.length).toBeGreaterThan(0);

    // 2. t3_project_list + t3_project_read
    const projList = await mcp.callTool("t3_project_list");
    expect(projList.result?.isError).toBeFalsy();
    const hubProjectId = String(projList.parsedText.projects[0].id);

    const projRead = await mcp.callTool("t3_project_read", { projectId: hubProjectId });
    expect(projRead.result?.isError).toBeFalsy();
    expect(projRead.parsedText.id).toBe(hubProjectId);

    // 3. Pause VM boot -> t3_thread_launch returns immediately with status="preparing" & runId=null
    let releaseBoot!: () => void;
    boat.createGate = new Promise<void>((r) => {
      releaseBoot = r;
    });
    const launch = await mcp.callTool("t3_thread_launch", {
      projectId: hubProjectId,
      title: "Delegated Worker",
      message: "Run tests and report",
      runtimeMode: "auto",
      interactionMode: "default",
      delegatedFrom: {
        environmentId: "env-mac-local",
        threadId: "thread-coordinator-1",
        title: "Coordinator chat",
      },
    });
    expect(launch.result?.isError).toBeFalsy();
    expect(launch.parsedText.status).toBe("preparing");
    expect(launch.parsedText.runId).toBeNull();
    expect(launch.parsedText.modelSelection).toEqual({ instanceId: "claudeAgent", model: "claude-sonnet-4-6" });
    const tid = String(launch.parsedText.threadId);
    expect(handle.state.getHub()?.threadLinkOrigins?.[tid]?.delegatedFrom).toEqual({
      environmentId: "env-mac-local",
      threadId: "thread-coordinator-1",
      title: "Coordinator chat",
    });

    // 4. t3_thread_wait while VM is booting times out cleanly with status="preparing"
    const waitBoot = await mcp.callTool("t3_thread_wait", {
      threadId: tid,
      untilDelegatedWorkSettled: true,
      timeoutMs: 120,
    });
    expect(waitBoot.result?.isError).toBeFalsy();
    expect(waitBoot.parsedText.timedOut).toBe(true);
    expect(waitBoot.parsedText.status).toBe("preparing");

    // 5. Release VM boot and wait for reactive wake when VM completes the run
    const waitPromise = mcp.callTool("t3_thread_wait", {
      threadId: tid,
      untilDelegatedWorkSettled: true,
      timeoutMs: 10_000,
    });
    releaseBoot();
    for (let i = 0; i < 60 && !vm.threads.has(tid); i++) await Bun.sleep(25);
    expect(vm.threads.has(tid)).toBe(true);

    // Complete the run with a >20k text payload to also test itemId + textOffset chunking
    const longAnswer = "A".repeat(25_000) + "-END-MARKER";
    vm.setThreadCompleted(tid, longAnswer);

    const waitDone = await waitPromise;
    expect(waitDone.result?.isError).toBeFalsy();
    expect(waitDone.parsedText.timedOut).toBe(false);
    expect(waitDone.parsedText.status).toBe("completed");
    expect(waitDone.parsedText.resultRun?.status).toBe("completed");

    // 6. t3_thread_read (messages view + truncation + textOffset continuation)
    const read1 = await mcp.callTool("t3_thread_read", { threadId: tid, view: "messages" });
    expect(read1.result?.isError).toBeFalsy();
    const assistantItem = (read1.parsedText.items as Array<Record<string, unknown>>).find((i) => i.type === "assistant_message")!;
    expect(assistantItem.textTruncated).toBe(true);
    expect(typeof assistantItem.nextTextOffset).toBe("number");

    const readChunk = await mcp.callTool("t3_thread_read", {
      threadId: tid,
      view: "messages",
      itemId: assistantItem.itemId,
      textOffset: assistantItem.nextTextOffset,
    });
    expect(readChunk.result?.isError).toBeFalsy();
    const tail = (readChunk.parsedText.items as Array<Record<string, unknown>>)[0];
    expect(String(tail.text)).toContain("-END-MARKER");
    expect(tail.textTruncated).toBe(false);
    expect(tail.nextTextOffset).toBeNull();

    // 7. t3_thread_send dispatches with dispatchMode + linkedSender (idle thread starts immediately)
    const send = await mcp.callTool("t3_thread_send", {
      threadId: tid,
      message: "Check lint too",
      mode: "queue",
    });
    expect(send.result?.isError).toBeFalsy();
    expect(send.parsedText.delivery).toBe("started");
    const lastDispatch = vm.dispatches.at(-1)!;
    expect(lastDispatch.type).toBe("message.dispatch");
    expect(lastDispatch.dispatchMode).toEqual({ type: "start_immediately" });
    expect(lastDispatch.linkedSender).toEqual({
      environmentId: "env-mac-local",
      threadId: "thread-coordinator-1",
      title: "Coordinator chat",
    });

    // 8. t3_thread_list, t3_thread_search, t3_thread_organize, t3_pending_request, t3_thread_interrupt
    const list = await mcp.callTool("t3_thread_list", { allProjects: true, titleContains: "Delegated" });
    expect(list.result?.isError).toBeFalsy();
    expect(list.parsedText.threads).toHaveLength(1);

    const search = await mcp.callTool("t3_thread_search", { query: "Delegated" });
    expect(search.result?.isError).toBeFalsy();
    expect(search.parsedText.matches[0].source).toBe("title");

    const organize = await mcp.callTool("t3_thread_organize", { threadId: tid, action: "pin" });
    expect(organize.result?.isError).toBeFalsy();
    expect(typeof organize.parsedText.sequence).toBe("number");

    const pendingList = await mcp.callTool("t3_pending_request", { threadId: tid, action: "list" });
    expect(pendingList.result?.isError).toBeFalsy();

    const interrupt = await mcp.callTool("t3_thread_interrupt", { threadId: tid, stop: true, clientRequestId: "int-1" });
    expect(interrupt.result?.isError).toBeFalsy();
    expect(["interrupt_requested", "no_active_run"]).toContain(interrupt.parsedText.status);

    // 9. Boot failure surfaces cleanly to t3_thread_wait + t3_thread_read
    boat.createGate = null;
    boat.createError = new Error("Boat capacity exhausted");
    const failLaunch = await mcp.callTool("t3_thread_launch", {
      projectId: hubProjectId,
      title: "Doomed Task",
      message: "Will fail to boot",
    });
    expect(failLaunch.result?.isError).toBeFalsy();
    const failTid = String(failLaunch.parsedText.threadId);

    const waitFail = await mcp.callTool("t3_thread_wait", {
      threadId: failTid,
      untilDelegatedWorkSettled: true,
      timeoutMs: 5_000,
    });
    expect(waitFail.result?.isError).toBeFalsy();
    expect(waitFail.parsedText.timedOut).toBe(false);
    expect(waitFail.parsedText.status).toBe("failed");

    const readFail = await mcp.callTool("t3_thread_read", { threadId: failTid, view: "activity" });
    expect(readFail.result?.isError).toBeFalsy();
    expect(readFail.parsedText.recentRuns[0].status).toBe("failed");
    expect((readFail.parsedText.items as Array<{ type: string; text: string }>).some((i) => i.type === "error" && i.text.includes("Boat capacity exhausted"))).toBe(true);
    boat.createError = null;

    // 10. Validate ALL MCP tool results against T3 Code's actual Effect schemas when installed
    if (t3Contracts) {
      const checks: Array<[string, unknown]> = [
        ["OrchestratorMcpCapabilitiesResult", caps.parsedText],
        ["Project", projRead.parsedText],
        ["OrchestratorMcpThreadWaitResult", waitBoot.parsedText],
        ["OrchestratorMcpThreadWaitResult", waitDone.parsedText],
        ["OrchestratorMcpThreadWaitResult", waitFail.parsedText],
        ["OrchestratorMcpThreadReadResult", read1.parsedText],
        ["OrchestratorMcpThreadReadResult", readChunk.parsedText],
        ["OrchestratorMcpThreadReadResult", readFail.parsedText],
        ["OrchestratorMcpThreadSendResult", send.parsedText],
        ["OrchestratorMcpThreadListResult", list.parsedText],
        ["OrchestratorMcpThreadInterruptResult", interrupt.parsedText],
      ];
      const failures: string[] = [];
      let checked = 0;
      for (const [schemaName, payload] of checks) {
        if (!t3Contracts.exported(schemaName)) continue;
        checked += 1;
        const r = t3Contracts.decodeWith(schemaName, payload);
        if (!r.ok) failures.push(`${schemaName}: ${r.error}`);
      }
      console.log(`[peer-link-test] decoded ${checked} MCP responses against T3 Code v${t3Contracts.version}`);
      expect(checked).toBeGreaterThan(0);
      expect(failures).toEqual([]);
    }
  });
});
