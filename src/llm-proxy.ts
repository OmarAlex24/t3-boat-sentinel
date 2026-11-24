import { extractBearer, safeEqual } from "./auth.ts";
import type { LlmProxyConfig } from "./config.ts";
import {
  ROBOTS_TXT,
  applySecurityHeaders,
  clientIpFromRequest,
  rateLimitedResponse,
  type AuthRateLimiter,
} from "./security.ts";
import type { StateStore } from "./state.ts";

const STRIP_REQ_HEADERS = new Set([
  "host",
  "connection",
  "keep-alive",
  "transfer-encoding",
  "upgrade",
  "authorization",
  "x-api-key",
]);

const STRIP_RES_HEADERS = new Set([
  "connection",
  "keep-alive",
  "transfer-encoding",
  "content-encoding",
  "content-length",
]);

/**
 * Zero-trust per-sandbox LLM API proxy (runs on sentinel host).
 *
 * Security properties:
 * - Boat VMs NEVER join your Tailnet or LAN, and NEVER see your real upstream API key.
 * - Each freshly spawned Boat VM receives a unique ephemeral `llmToken` (`t3llm_...`).
 * - Requests are ONLY forwarded while that slot's phase is `running`; sleeping or deleted
 *   sandboxes immediately receive `401 Unauthorized`.
 * - Streams SSE responses directly through without buffering.
 */
export class LlmProxy {
  constructor(
    private readonly cfg: LlmProxyConfig,
    private readonly state: StateStore,
    private readonly rateLimiter?: AuthRateLimiter,
  ) {}

  async handle(req: Request): Promise<Response> {
    const url = new URL(req.url);
    if (req.method === "GET" && url.pathname === "/robots.txt") {
      return applySecurityHeaders(new Response(ROBOTS_TXT, { headers: { "content-type": "text/plain; charset=utf-8" } }));
    }

    const ip = clientIpFromRequest(req);
    if (this.rateLimiter) {
      const limit = this.rateLimiter.check("llm-auth", ip, 30, 5 * 60_000);
      if (!limit.allowed) return rateLimitedResponse(limit.retryAfterSecs);
    }

    const presentedKey = req.headers.get("x-api-key")?.trim() || extractBearer(req.headers);
    if (!presentedKey) {
      this.rateLimiter?.recordFailure("llm-auth", ip, 5 * 60_000);
      return applySecurityHeaders(
        Response.json({ error: { type: "authentication_error", message: "Missing sandbox LLM token" } }, { status: 401 }),
      );
    }

    const activeSlot = this.state
      .listSlots()
      .find((s) => s.phase === "running" && s.llmToken !== null && safeEqual(s.llmToken, presentedKey));

    const activeHubSandbox = activeSlot
      ? null
      : this.state
          .listHubSandboxes()
          .find((s) => s.phase === "running" && s.llmToken !== null && safeEqual(s.llmToken, presentedKey));

    if (!activeSlot && !activeHubSandbox) {
      this.rateLimiter?.recordFailure("llm-auth", ip, 5 * 60_000);
      return applySecurityHeaders(
        Response.json(
          { error: { type: "authentication_error", message: "Sandbox token inactive or revoked" } },
          { status: 401 },
        ),
      );
    }
    this.rateLimiter?.recordSuccess("llm-auth", ip);
    const ownerLabel = activeSlot ? `slot ${activeSlot.name}` : `hub sandbox ${activeHubSandbox!.sandboxId}`;

    const target = `${this.cfg.upstreamUrl}${url.pathname}${url.search}`;

    const headers = new Headers();
    for (const [k, v] of req.headers) {
      if (!STRIP_REQ_HEADERS.has(k.toLowerCase())) {
        headers.set(k, v);
      }
    }
    // Inject real upstream credential (kept strictly on sentinel host)
    headers.set("authorization", `Bearer ${this.cfg.upstreamApiKey}`);
    headers.set("x-api-key", this.cfg.upstreamApiKey);

    try {
      const upstream = await fetch(target, {
        method: req.method,
        headers,
        body: req.method === "GET" || req.method === "HEAD" ? undefined : req.body,
        redirect: "manual",
      });

      const outHeaders = new Headers();
      for (const [k, v] of upstream.headers) {
        if (!STRIP_RES_HEADERS.has(k.toLowerCase())) {
          outHeaders.set(k, v);
        }
      }
      return new Response(upstream.body, {
        status: upstream.status,
        statusText: upstream.statusText,
        headers: outHeaders,
      });
    } catch (err) {
      console.error(`[llm-proxy] upstream error for ${ownerLabel}:`, err);
      return Response.json(
        { error: { type: "api_error", message: "Upstream LLM proxy unreachable" } },
        { status: 502 },
      );
    }
  }
}
