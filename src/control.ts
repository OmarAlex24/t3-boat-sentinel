import { extractBearer, safeEqual } from "./auth.ts";
import type { SentinelConfig } from "./config.ts";
import type { JobManager } from "./jobs.ts";
import type { LifecycleManager, WatchdogEventPayload } from "./lifecycle.ts";
import type { SlotProxy } from "./proxy.ts";
import type { StateStore } from "./state.ts";

/**
 * Control plane served on `https://ctrl.<domain>`:
 * - `/internal/watchdog`: authenticated with per-sandbox `slot.watchdogSecret` (never shared across VMs!)
 * - `/api/*`: authenticated strictly via `Authorization: Bearer <controlSecret>` header (no query tokens)
 * - `/api/jobs/:id`: poll async spawn/bake/wake/reset jobs without hitting HTTP timeouts
 * - `/`: mobile-first dashboard
 */
export class ControlPlane {
  constructor(
    private readonly config: SentinelConfig,
    private readonly state: StateStore,
    private readonly lifecycle: LifecycleManager,
    private readonly proxy: SlotProxy,
    private readonly jobs: JobManager,
  ) {}

  async handle(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const path = url.pathname;

    if (req.method === "GET" && path === "/health") {
      return Response.json({ ok: true, slots: this.state.listSlots().length });
    }

    // Per-sandbox watchdog callback (validates against THAT slot's unique secret!)
    if (req.method === "POST" && path === "/internal/watchdog") {
      const token = extractBearer(req.headers);
      if (!token) return Response.json({ error: "unauthorized" }, { status: 401 });

      const payload = (await req.json()) as WatchdogEventPayload;
      const slot = this.state.getSlot(payload.slot);
      if (!slot || !slot.watchdogSecret || slot.sandboxId !== payload.sandboxId || !safeEqual(token, slot.watchdogSecret)) {
        return Response.json({ error: "unauthorized" }, { status: 401 });
      }

      void this.lifecycle.handleWatchdogEvent(payload).catch((err) => {
        console.error("[control] watchdog event failed:", err);
      });
      return Response.json({ accepted: true });
    }

    if (req.method === "GET" && path === "/") {
      return new Response(renderDashboardHtml(this.config), {
        headers: { "content-type": "text/html; charset=utf-8" },
      });
    }

    // Control API: strictly Bearer header auth (never `?token=` in query strings/logs!)
    if (path.startsWith("/api/")) {
      const token = extractBearer(req.headers);
      if (!token || !safeEqual(token, this.config.controlSecret)) {
        return Response.json({ error: "unauthorized" }, { status: 401 });
      }
      return this.handleApi(req, url, path);
    }

    return new Response("Not found", { status: 404 });
  }

  private async handleApi(req: Request, url: URL, path: string): Promise<Response> {
    try {
      if (req.method === "GET" && path === "/api/status") {
        const slots = this.state.listSlots().map((s) => ({
          name: s.name,
          label: s.label,
          url: `https://${s.name}.${this.config.domain}`,
          phase: s.phase,
          generation: s.generation,
          project: s.project,
          snapshot: s.snapshot,
          sandboxId: s.sandboxId,
          klavAgent: s.klavAgentName,
          pairedClients: s.clients.map((c) => ({ id: c.id, label: c.label, pairedAt: c.pairedAt })),
          createdAt: s.createdAt,
          lastHeartbeatAt: s.lastHeartbeatAt,
          sleepingSince: s.sleepingSince,
          lastError: s.lastError,
        }));
        const projects = Object.entries(this.config.projects).map(([name, p]) => {
          const bake = this.state.getBake(name);
          return {
            name,
            snapshot: p.snapshot,
            klavProject: p.klav?.project ?? null,
            lastBakeAt: bake?.bakedAt ?? null,
            t3Version: bake?.t3Version ?? null,
          };
        });
        const baseBake = this.state.getBake("base");
        return Response.json({
          t3: this.config.t3,
          wakePolicy: this.config.wakePolicy,
          baseSnapshot: {
            name: this.config.boat.baseSnapshot,
            lastBakeAt: baseBake?.bakedAt ?? null,
            t3Version: baseBake?.t3Version ?? null,
          },
          slots,
          projects,
          pendingDeletes: this.state.listPendingDeletes().length,
        });
      }

      if (req.method === "GET" && path === "/api/jobs") {
        return Response.json({ jobs: this.jobs.list() });
      }

      const jobMatch = /^\/api\/jobs\/([^/]+)$/.exec(path);
      if (req.method === "GET" && jobMatch) {
        const job = this.jobs.get(jobMatch[1]);
        if (!job) return Response.json({ error: "job_not_found" }, { status: 404 });
        return Response.json(job);
      }

      if (req.method === "POST" && path === "/api/spawn") {
        const body = (await req.json()) as {
          project: string;
          slot?: string;
          replace?: boolean;
          prompt?: string;
          title?: string;
          provider?: string;
          model?: string;
          runtimeMode?: "approval-required" | "auto-accept-edits" | "auto" | "full-access";
          idempotencyKey?: string;
          async?: boolean;
        };
        const runSpawn = async () => {
          const res = await this.lifecycle.spawn(body.project, {
            slot: body.slot,
            replace: body.replace,
            prompt: body.prompt,
            title: body.title,
            provider: body.provider,
            model: body.model,
            runtimeMode: body.runtimeMode,
          });
          return {
            ok: true,
            slot: res.slot.name,
            label: res.slot.label,
            url: `https://${res.slot.name}.${this.config.domain}`,
            sandboxId: res.slot.sandboxId,
            project: res.slot.project,
            klavAgent: res.slot.klavAgentName,
            projectId: res.projectId,
            threadId: res.threadId,
            launchError: res.launchError,
          };
        };

        const isAsync = body.async === true || url.searchParams.get("async") === "1";
        if (isAsync) {
          const job = this.jobs.start("spawn", runSpawn, body.idempotencyKey);
          return Response.json({ ok: true, jobId: job.id, status: job.status }, { status: 202 });
        }
        return Response.json(await runSpawn());
      }

      if (req.method === "POST" && path === "/api/bake") {
        const body = (await req.json()) as { target: string; async?: boolean; idempotencyKey?: string };
        const isAsync = body.async === true || url.searchParams.get("async") === "1";
        if (isAsync) {
          const job = this.jobs.start("bake", () => this.lifecycle.bake(body.target), body.idempotencyKey);
          return Response.json({ ok: true, jobId: job.id, status: job.status }, { status: 202 });
        }
        const result = await this.lifecycle.bake(body.target);
        return Response.json({ ok: true, target: body.target, ...result });
      }

      if (req.method === "POST" && path === "/api/wake") {
        const body = (await req.json()) as { slot: string; async?: boolean };
        if (body.async === true || url.searchParams.get("async") === "1") {
          const job = this.jobs.start("wake", async () => {
            const slot = await this.lifecycle.wake(body.slot);
            return { ok: true, slot: slot.name, phase: slot.phase };
          });
          return Response.json({ ok: true, jobId: job.id, status: job.status }, { status: 202 });
        }
        const slot = await this.lifecycle.wake(body.slot);
        return Response.json({ ok: true, slot: slot.name, phase: slot.phase });
      }

      if (req.method === "POST" && path === "/api/sleep") {
        const body = (await req.json()) as { slot: string };
        const slot = await this.lifecycle.sleep(body.slot);
        return Response.json({ ok: true, slot: slot.name, phase: slot.phase });
      }

      if (req.method === "POST" && path === "/api/reset") {
        const body = (await req.json()) as { slot: string };
        const slot = await this.lifecycle.destroy(body.slot, "api-reset");
        return Response.json({ ok: true, slot: slot.name, phase: slot.phase });
      }

      if (req.method === "POST" && path === "/api/pair") {
        const body = (await req.json()) as { slot: string };
        const pair = this.proxy.createPairingLink(body.slot);
        return Response.json({ ok: true, slot: body.slot, ...pair });
      }

      if (req.method === "POST" && path === "/api/revoke") {
        const body = (await req.json()) as { slot: string; clientId?: string };
        const slot = this.state.updateSlot(body.slot, (s) => {
          s.clients = body.clientId ? s.clients.filter((c) => c.id !== body.clientId) : [];
        });
        return Response.json({ ok: true, slot: slot.name, pairedClients: slot.clients.length });
      }

      return Response.json({ error: "not_found" }, { status: 404 });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      return Response.json({ ok: false, error: msg }, { status: 400 });
    }
  }
}

function renderDashboardHtml(config: SentinelConfig): string {
  const projects = Object.keys(config.projects);
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>t3-boat-sentinel</title>
  <style>
    :root { color-scheme: dark; --bg: #0c0d10; --card: #15171d; --border: #262a34; --fg: #eceff4; --muted: #8b93a6; --accent: #f59e0b; --green: #22c55e; --red: #ef4444; }
    * { box-sizing: border-box; }
    body { margin: 0; font-family: system-ui, -apple-system, sans-serif; background: var(--bg); color: var(--fg); padding: 16px; max-width: 720px; margin-inline: auto; }
    h1 { font-size: 1.25rem; margin: 0 0 4px; }
    .sub { color: var(--muted); font-size: 0.875rem; margin-bottom: 16px; }
    .card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 14px; margin-bottom: 12px; }
    .row { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; justify-content: space-between; }
    button, select, input { font: inherit; border-radius: 8px; border: 1px solid var(--border); background: #1d2028; color: var(--fg); padding: 8px 12px; }
    button { cursor: pointer; font-weight: 500; }
    button.primary { background: var(--accent); color: #111; border-color: var(--accent); }
    button.danger { border-color: #7f1d1d; color: #fca5a5; }
    .badge { font-size: 0.75rem; padding: 2px 8px; border-radius: 999px; border: 1px solid var(--border); }
    .badge.running { border-color: var(--green); color: var(--green); }
    .badge.sleeping { border-color: var(--accent); color: var(--accent); }
    .badge.empty { color: var(--muted); }
    .meta { color: var(--muted); font-size: 0.8rem; margin-top: 6px; word-break: break-all; }
    .toast { margin: 10px 0; padding: 10px 12px; border-radius: 8px; background: #1e293b; font-size: 0.85rem; white-space: pre-wrap; display: none; }
    .toast.show { display: block; }
  </style>
</head>
<body>
  <h1>t3-boat-sentinel</h1>
  <div class="sub">1 chat = 1 disposable Boat VM · immutable golden snapshots</div>

  <div class="card">
    <div class="row">
      <input id="token" type="password" placeholder="Control secret (stored locally in browser)" style="flex:1;min-width:180px" />
      <button onclick="saveToken()">Save</button>
    </div>
  </div>

  <div class="card">
    <div style="font-weight:600;margin-bottom:8px">Spawn fresh sandbox</div>
    <div class="row">
      <select id="project" style="flex:1">
        ${projects.map((p) => `<option value="${p}">${p}</option>`).join("")}
      </select>
      <button class="primary" onclick="spawn()">New clean chat</button>
    </div>
  </div>

  <div id="toast" class="toast"></div>
  <div id="slots"></div>

  <div class="card">
    <div style="font-weight:600;margin-bottom:8px">Immutable golden snapshots (Re-bake)</div>
    <div class="row">
      <button onclick="bake('base')">Bake base-t3</button>
      ${projects.map((p) => `<button onclick="bake('${p}')">Bake ${p}</button>`).join("")}
    </div>
  </div>

  <script>
    const tokenEl = document.getElementById('token');
    tokenEl.value = localStorage.getItem('t3_sentinel_token') || '';
    function saveToken() { localStorage.setItem('t3_sentinel_token', tokenEl.value.trim()); load(); }
    function toast(msg) { const el = document.getElementById('toast'); el.textContent = msg; el.classList.add('show'); }
    async function api(path, body) {
      const t = tokenEl.value.trim();
      const res = await fetch(path, {
        method: body ? 'POST' : 'GET',
        headers: { 'authorization': 'Bearer ' + t, ...(body ? {'content-type':'application/json'} : {}) },
        body: body ? JSON.stringify(body) : undefined,
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || ('HTTP ' + res.status));
      return data;
    }
    async function pollJob(jobId) {
      while (true) {
        await new Promise(r => setTimeout(r, 1500));
        const job = await api('/api/jobs/' + jobId);
        if (job.status === 'completed') return job.result;
        if (job.status === 'failed') throw new Error(job.error || 'Job failed');
      }
    }
    async function load() {
      if (!tokenEl.value.trim()) return;
      try {
        const data = await api('/api/status');
        document.getElementById('slots').innerHTML = data.slots.map(s => \`
          <div class="card">
            <div class="row">
              <strong>\${s.label} <span class="badge \${s.phase}">\${s.phase}</span></strong>
              <div class="row">
                \${s.phase === 'sleeping' ? \`<button onclick="act('/api/wake',{slot:'\${s.name}',async:true},true)">Wake</button>\` : ''}
                \${s.phase === 'running' ? \`<button onclick="act('/api/sleep',{slot:'\${s.name}'})">Sleep</button>\` : ''}
                \${s.sandboxId ? \`<button class="danger" onclick="act('/api/reset',{slot:'\${s.name}'})">Reset &amp; Delete</button>\` : ''}
                <button onclick="pair('\${s.name}')">Pair</button>
              </div>
            </div>
            <div class="meta">\${s.url} · project: \${s.project || 'free'} · paired devices: \${s.pairedClients.length}</div>
            \${s.lastError ? \`<div class="meta" style="color:#fca5a5">Error: \${s.lastError}</div>\` : ''}
          </div>
        \`).join('');
      } catch (e) { toast('Error: ' + e.message); }
    }
    async function spawn() {
      const project = document.getElementById('project').value;
      toast('Spawning clean sandbox for ' + project + '...');
      try {
        const started = await api('/api/spawn', { project, async: true });
        const r = await pollJob(started.jobId);
        toast('Ready on ' + r.label + ' (' + r.url + ') — open it in T3 Code / T3 Mobile.');
        load();
      } catch (e) { toast('Error: ' + e.message); }
    }
    async function act(ep, body, isJob) {
      toast('Running ' + ep + '...');
      try {
        const r = await api(ep, body);
        if (isJob && r.jobId) await pollJob(r.jobId);
        toast('Done.');
        load();
      } catch (e) { toast('Error: ' + e.message); }
    }
    async function pair(slot) {
      try {
        const r = await api('/api/pair', { slot });
        await navigator.clipboard?.writeText(r.url).catch(() => {});
        toast('Pairing link (copied, valid 15 min):\\n' + r.url);
      } catch (e) { toast('Error: ' + e.message); }
    }
    async function bake(target) {
      toast('Baking immutable snapshot for ' + target + ' (1-4 min)...');
      try {
        const started = await api('/api/bake', { target, async: true });
        const r = await pollJob(started.jobId);
        toast('Snapshot ' + r.snapshot + ' updated (' + r.t3Version + ').');
        load();
      } catch (e) { toast('Error: ' + e.message); }
    }
    load();
    setInterval(load, 10000);
  </script>
</body>
</html>`;
}
