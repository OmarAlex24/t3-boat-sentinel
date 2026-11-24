import { extractBearer, safeEqual } from "./auth.ts";
import type { SentinelConfig } from "./config.ts";
import type { HubServer } from "./hub.ts";
import type { JobManager } from "./jobs.ts";
import type { LifecycleManager, WatchdogEventPayload } from "./lifecycle.ts";
import type { SlotProxy } from "./proxy.ts";
import { buildAutoProjectConfig, type StateStore } from "./state.ts";

/**
 * Control plane served on `https://ctrl.<domain>`:
 * - `/internal/watchdog`: authenticated with per-sandbox `watchdogSecret` (never shared across VMs!)
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
    private readonly hub?: HubServer,
  ) {}

  async handle(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const path = url.pathname;

    if (req.method === "GET" && path === "/health") {
      return Response.json({ ok: true, slots: this.state.listSlots().length });
    }

    // Per-sandbox watchdog callback (validates against THAT slot or hub sandbox's unique secret!)
    if (req.method === "POST" && path === "/internal/watchdog") {
      const token = extractBearer(req.headers);
      if (!token) return Response.json({ error: "unauthorized" }, { status: 401 });

      const payload = (await req.json()) as WatchdogEventPayload;
      if (this.hub && payload.slot?.startsWith("hub:")) {
        const sb = this.state.getHubSandbox(payload.sandboxId);
        if (!sb || !sb.watchdogSecret || !safeEqual(token, sb.watchdogSecret)) {
          return Response.json({ error: "unauthorized" }, { status: 401 });
        }
        void this.hub.handleWatchdogEvent(payload).catch((err) => {
          console.error("[control] hub watchdog event failed:", err);
        });
        return Response.json({ accepted: true });
      }

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
        const allProjects = this.state.getAllProjects(this.config);
        const projects = Object.entries(allProjects).map(([name, p]) => {
          const bake = this.state.getBake(name);
          return {
            name,
            repo: p.repo ?? null,
            workdir: p.workdir,
            size: p.size ?? this.config.boat.defaultSize,
            snapshot: p.snapshot,
            klavProject: p.klav?.project ?? (p.klav?.profiles ? p.klav.profiles.join(",") : null),
            dynamic: !Object.prototype.hasOwnProperty.call(this.config.projects, name),
            lastBakeAt: bake?.bakedAt ?? null,
            t3Version: bake?.t3Version ?? null,
          };
        });
        const baseBake = this.state.getBake("base");
        const hubState = this.state.getHub();
        const hubInfo =
          this.config.hub && hubState
            ? {
                url: `https://${this.config.hub.host}`,
                label: hubState.label,
                environmentId: hubState.environmentId,
                maxSandboxes: this.config.hub.maxSandboxes,
                pairedClients: hubState.clients.map((c) => ({ id: c.id, label: c.label, pairedAt: c.pairedAt })),
                sandboxes: this.state.listHubSandboxes().map((sb) => ({
                  sandboxId: sb.sandboxId,
                  project: sb.project,
                  phase: sb.phase,
                  threadIds: sb.threadIds,
                  klavAgent: sb.klavAgentName,
                  createdAt: sb.createdAt,
                  lastHeartbeatAt: sb.lastHeartbeatAt,
                  sleepingSince: sb.sleepingSince,
                })),
              }
            : null;
        return Response.json({
          t3: this.config.t3,
          wakePolicy: this.config.wakePolicy,
          baseSnapshot: {
            name: this.config.boat.baseSnapshot,
            lastBakeAt: baseBake?.bakedAt ?? null,
            t3Version: baseBake?.t3Version ?? null,
          },
          ...(hubInfo ? { hub: hubInfo } : {}),
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
        if (this.hub && body.slot === "hub") {
          for (const sb of this.state.listHubSandboxes()) {
            await this.hub.destroyHubSandbox(sb.sandboxId, "api-reset-all");
          }
          return Response.json({ ok: true, slot: "hub", phase: "empty" });
        }
        if (this.hub && body.slot.startsWith("hub:")) {
          const sbId = body.slot.slice(4);
          await this.hub.destroyHubSandbox(sbId, "api-reset");
          return Response.json({ ok: true, slot: body.slot, phase: "empty" });
        }
        const slot = await this.lifecycle.destroy(body.slot, "api-reset");
        return Response.json({ ok: true, slot: slot.name, phase: slot.phase });
      }

      if (req.method === "POST" && path === "/api/pair") {
        const body = (await req.json()) as { slot: string };
        if (body.slot === "hub") {
          if (!this.hub) return Response.json({ ok: false, error: "Hub is not enabled in sentinel.yaml" }, { status: 400 });
          const pair = this.hub.createPairingLink();
          return Response.json({ ok: true, slot: "hub", ...pair });
        }
        const pair = this.proxy.createPairingLink(body.slot);
        return Response.json({ ok: true, slot: body.slot, ...pair });
      }

      if (req.method === "POST" && path === "/api/projects") {
        const body = (await req.json()) as {
          repo: string;
          name?: string;
          workdir?: string;
          size?: string;
          klavProfiles?: string[];
          klavTtl?: string;
          defaultProvider?: string;
          defaultModel?: string;
          bake?: boolean;
        };
        const { name, project } = buildAutoProjectConfig(body);
        this.state.addDynamicProject(name, project);
        this.hub?.notifyProjectAdded(name);
        if (body.bake !== false) {
          const job = this.jobs.start("bake", () => this.lifecycle.bake(name));
          return Response.json(
            { ok: true, name, project, jobId: job.id, status: job.status },
            { status: 202 },
          );
        }
        return Response.json({ ok: true, name, project });
      }

      const deleteProjMatch = /^\/api\/projects\/([^/]+)$/.exec(path);
      if (req.method === "DELETE" && deleteProjMatch) {
        const projName = decodeURIComponent(deleteProjMatch[1]);
        if (Object.prototype.hasOwnProperty.call(this.config.projects, projName)) {
          return Response.json(
            { ok: false, error: `Project "${projName}" is defined in sentinel.yaml and cannot be deleted dynamically.` },
            { status: 400 },
          );
        }
        const removedHubId = this.state.removeDynamicProject(projName);
        if (!removedHubId && !this.state.getAllProjects(this.config)[projName]) {
          return Response.json({ ok: false, error: `Dynamic project "${projName}" not found` }, { status: 404 });
        }
        if (removedHubId) {
          this.hub?.notifyProjectRemoved(removedHubId);
        }
        return Response.json({ ok: true, removed: projName });
      }

      if (req.method === "POST" && path === "/api/revoke") {
        const body = (await req.json()) as { slot: string; clientId?: string };
        if (body.slot === "hub") {
          const hub = this.state.updateHub((h) => {
            h.clients = body.clientId ? h.clients.filter((c) => c.id !== body.clientId) : [];
          });
          return Response.json({ ok: true, slot: "hub", pairedClients: hub.clients.length });
        }
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
  const defaultSize = config.boat.defaultSize;
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>t3-boat-sentinel</title>
  <style>
    :root { color-scheme: dark; --bg: #0c0d10; --card: #15171d; --border: #262a34; --fg: #eceff4; --muted: #8b93a6; --accent: #f59e0b; --green: #22c55e; --red: #ef4444; --blue: #38bdf8; }
    * { box-sizing: border-box; }
    body { margin: 0; font-family: system-ui, -apple-system, sans-serif; background: var(--bg); color: var(--fg); padding: 16px; max-width: 780px; margin-inline: auto; }
    h1 { font-size: 1.3rem; margin: 0 0 4px; }
    h2 { font-size: 0.95rem; margin: 0 0 10px; font-weight: 600; }
    .sub { color: var(--muted); font-size: 0.875rem; margin-bottom: 16px; }
    .card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 14px; margin-bottom: 12px; }
    .subcard { background: #101218; border: 1px solid var(--border); border-radius: 10px; padding: 10px 12px; margin-top: 8px; }
    .row { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; justify-content: space-between; }
    .row-left { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
    button, select, input { font: inherit; border-radius: 8px; border: 1px solid var(--border); background: #1d2028; color: var(--fg); padding: 7px 12px; font-size: 0.88rem; }
    button { cursor: pointer; font-weight: 500; }
    button.primary { background: var(--accent); color: #111; border-color: var(--accent); }
    button.danger { border-color: #7f1d1d; color: #fca5a5; }
    button.sm { padding: 5px 10px; font-size: 0.8rem; }
    .badge { font-size: 0.72rem; padding: 2px 8px; border-radius: 999px; border: 1px solid var(--border); }
    .badge.running { border-color: var(--green); color: var(--green); }
    .badge.sleeping { border-color: var(--accent); color: var(--accent); }
    .badge.provisioning { border-color: var(--blue); color: var(--blue); }
    .badge.empty { color: var(--muted); }
    .meta { color: var(--muted); font-size: 0.8rem; margin-top: 4px; word-break: break-all; }
    .toast { margin: 10px 0; padding: 10px 12px; border-radius: 8px; background: #1e293b; font-size: 0.85rem; white-space: pre-wrap; display: none; }
    .toast.show { display: block; }
    label.chk { display: inline-flex; align-items: center; gap: 6px; font-size: 0.82rem; color: var(--muted); cursor: pointer; }
  </style>
</head>
<body>
  <h1>t3-boat-sentinel</h1>
  <div class="sub">1 chat = 1 disposable Boat VM · immutable golden snapshots · zero-trust proxies</div>

  <div class="card">
    <div class="row">
      <input id="token" type="password" placeholder="Control secret (stored locally in browser)" style="flex:1;min-width:180px" />
      <button onclick="saveToken()">Save</button>
    </div>
  </div>

  <div id="toast" class="toast"></div>
  <div id="hub"></div>

  <div class="card">
    <h2>+ Add GitHub Repo (no YAML edits)</h2>
    <div class="row" style="margin-bottom:8px">
      <input id="repoInput" type="text" placeholder="owner/repo or https://github.com/owner/repo.git" style="flex:2;min-width:220px" />
      <select id="repoSize">
        <option value="">Size: ${defaultSize} (default)</option>
        <option value="default">default (2vCPU/4GB)</option>
        <option value="medium">medium</option>
        <option value="large">large (4vCPU/8GB)</option>
      </select>
      <button class="primary" onclick="addRepo()">Add &amp; Bake</button>
    </div>
    <label class="chk"><input id="repoBake" type="checkbox" checked /> Immediately bake immutable <code>env-&lt;repo&gt;</code> snapshot</label>
  </div>

  <div id="projects"></div>
  <div id="slots"></div>

  <script>
    const tokenEl = document.getElementById('token');
    tokenEl.value = localStorage.getItem('t3_sentinel_token') || '';
    function saveToken() { localStorage.setItem('t3_sentinel_token', tokenEl.value.trim()); load(); }
    function toast(msg) { const el = document.getElementById('toast'); el.textContent = msg; el.classList.add('show'); }
    function esc(s) { return String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
    async function api(path, body, method) {
      const t = tokenEl.value.trim();
      const res = await fetch(path, {
        method: method || (body ? 'POST' : 'GET'),
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
        // 1. Hub card
        if (data.hub) {
          const h = data.hub;
          const sbs = h.sandboxes || [];
          document.getElementById('hub').innerHTML = \`
            <div class="card">
              <div class="row">
                <div class="row-left">
                  <strong>Hub: \${esc(h.label)}</strong>
                  <span class="badge running">\${sbs.length} / \${h.maxSandboxes} VMs</span>
                  <span class="badge">\${h.pairedClients.length} paired device\${h.pairedClients.length === 1 ? '' : 's'}</span>
                </div>
                <div class="row-left">
                  <button class="primary sm" onclick="pair('hub')">Pair Device</button>
                  \${sbs.length > 0 ? \`<button class="danger sm" onclick="act('/api/reset',{slot:'hub'})">Destroy All</button>\` : ''}
                </div>
              </div>
              <div class="meta">\${esc(h.url)} · default size: \${esc(data.t3?.channel || '')} · pending deletes: \${data.pendingDeletes}</div>
              \${sbs.length === 0
                ? '<div class="meta" style="margin-top:10px">No active sandboxes ($0/hr). Start a chat in T3 Code Desktop or Mobile to auto-spawn a fresh Boat VM.</div>'
                : sbs.map(sb => \`
                  <div class="subcard">
                    <div class="row">
                      <div class="row-left">
                        <strong>\${esc(sb.project)}</strong>
                        <span class="badge \${esc(sb.phase)}">\${esc(sb.phase)}</span>
                        <span class="meta" style="margin:0">\${esc(sb.sandboxId)}</span>
                      </div>
                      <button class="danger sm" onclick="act('/api/reset',{slot:'hub:\${esc(sb.sandboxId)}'})">Destroy</button>
                    </div>
                    <div class="meta">threads: \${(sb.threadIds || []).length}\${sb.klavAgent ? ' · klav: ' + esc(sb.klavAgent) : ''} · created: \${sb.createdAt ? new Date(sb.createdAt).toLocaleTimeString() : '-'}</div>
                  </div>
                \`).join('')}
            </div>
          \`;
        } else {
          document.getElementById('hub').innerHTML = '';
        }

        // 2. Projects & Golden Snapshots
        const base = data.baseSnapshot || {};
        document.getElementById('projects').innerHTML = \`
          <div class="card">
            <div class="row" style="margin-bottom:8px">
              <h2 style="margin:0">Projects &amp; Immutable Golden Snapshots</h2>
              <button class="sm" onclick="bake('base')">Bake \${esc(base.name || 'base-t3')}</button>
            </div>
            <div class="meta">Base: \${esc(base.name || 'base-t3')} · \${esc(base.t3Version || 'not baked')} · \${base.lastBakeAt ? new Date(base.lastBakeAt).toLocaleString() : 'never'}</div>
            \${(data.projects || []).map(p => \`
              <div class="subcard">
                <div class="row">
                  <div class="row-left">
                    <strong>\${esc(p.name)}</strong>
                    <span class="badge">\${esc(p.snapshot)}</span>
                    <span class="badge">\${esc(p.size)}</span>
                    \${p.dynamic ? '<span class="badge">dynamic</span>' : ''}
                  </div>
                  <div class="row-left">
                    <button class="sm" onclick="bake('\${esc(p.name)}')">Bake</button>
                    \${p.dynamic ? \`<button class="danger sm" onclick="removeProject('\${esc(p.name)}')">Remove</button>\` : ''}
                  </div>
                </div>
                <div class="meta">\${p.repo ? esc(p.repo) + ' · ' : ''}last bake: \${p.lastBakeAt ? new Date(p.lastBakeAt).toLocaleString() : 'not baked yet'}\${p.klavProject ? ' · klav: ' + esc(p.klavProject) : ''}</div>
              </div>
            \`).join('')}
          </div>
        \`;

        // 3. Legacy slots (only if configured)
        if ((data.slots || []).length > 0) {
          document.getElementById('slots').innerHTML = \`
            <div class="card">
              <h2>Legacy Fixed Slots</h2>
              \${data.slots.map(s => \`
                <div class="subcard">
                  <div class="row">
                    <strong>\${esc(s.label)} <span class="badge \${esc(s.phase)}">\${esc(s.phase)}</span></strong>
                    <div class="row-left">
                      \${s.phase === 'sleeping' ? \`<button class="sm" onclick="act('/api/wake',{slot:'\${esc(s.name)}',async:true},true)">Wake</button>\` : ''}
                      \${s.phase === 'running' ? \`<button class="sm" onclick="act('/api/sleep',{slot:'\${esc(s.name)}'})">Sleep</button>\` : ''}
                      \${s.sandboxId ? \`<button class="danger sm" onclick="act('/api/reset',{slot:'\${esc(s.name)}'})">Reset</button>\` : ''}
                      <button class="sm" onclick="pair('\${esc(s.name)}')">Pair</button>
                    </div>
                  </div>
                  <div class="meta">\${esc(s.url)} · project: \${esc(s.project || 'free')} · paired: \${s.pairedClients.length}</div>
                  \${s.lastError ? \`<div class="meta" style="color:#fca5a5">Error: \${esc(s.lastError)}</div>\` : ''}
                </div>
              \`).join('')}
            </div>
          \`;
        } else {
          document.getElementById('slots').innerHTML = '';
        }
      } catch (e) { toast('Error: ' + e.message); }
    }
    async function addRepo() {
      const repo = document.getElementById('repoInput').value.trim();
      const size = document.getElementById('repoSize').value || undefined;
      const bakeNow = document.getElementById('repoBake').checked;
      if (!repo) return toast('Enter owner/repo or git URL first.');
      toast('Adding ' + repo + (bakeNow ? ' and baking immutable snapshot (1-3 min)...' : '...'));
      try {
        const res = await api('/api/projects', { repo, size, bake: bakeNow });
        document.getElementById('repoInput').value = '';
        load();
        if (res.jobId) {
          const r = await pollJob(res.jobId);
          toast('Project "' + res.name + '" added & snapshot ' + r.snapshot + ' ready!');
          load();
        } else {
          toast('Project "' + res.name + '" added (pushed live to T3 Code).');
        }
      } catch (e) { toast('Error: ' + e.message); }
    }
    async function removeProject(name) {
      if (!confirm('Remove dynamic project "' + name + '"?')) return;
      try {
        await api('/api/projects/' + encodeURIComponent(name), null, 'DELETE');
        toast('Removed project "' + name + '".');
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
    setInterval(load, 8000);
  </script>
</body>
</html>`;
}
