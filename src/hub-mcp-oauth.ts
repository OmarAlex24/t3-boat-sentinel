import { createHash, createHmac, randomUUID } from "node:crypto";
import {
  hashToken,
  isAuthMcpClientAccess,
  mcpClientScopes,
  PEER_LINK_TOKEN_TTL_SECS,
  randomToken,
  safeEqual,
  type AuthMcpClientAccess,
  type SlotAuth,
} from "./auth.ts";
import { clientIpFromRequest, rateLimitedResponse, type AuthRateLimiter } from "./security.ts";
import type { SentinelConfig } from "./config.ts";
import type { StateStore } from "./state.ts";

/** Exact `software_id` T3 Code sends in `/oauth/mcp/register` when linking another environment. */
export const MCP_PEER_LINK_SOFTWARE_ID = "t3code-peer-link";

const AUTHORIZATION_CODE_TTL_MS = 60_000;
const MAX_CLIENT_NAME_LENGTH = 100;
const MAX_REDIRECT_URIS = 10;
const MAX_REDIRECT_URI_LENGTH = 512;
const MAX_UNBOUNDED_CLIENT_ID_REDIRECT_URIS = 5;
const MAX_CLIENT_ID_LENGTH = 10_240;
const DEFAULT_CLIENT_NAME = "MCP client";
const LOOPBACK_HOSTNAMES = new Set(["localhost", "127.0.0.1", "[::1]"]);
const CODE_CHALLENGE_PATTERN = /^[A-Za-z0-9_-]{43}$/;
const CODE_VERIFIER_PATTERN = /^[A-Za-z0-9\-._~]{43,128}$/;
const MCP_OAUTH_SCOPES = ["orchestration:read", "orchestration:operate"] as const;

const NO_STORE_HEADERS: Record<string, string> = {
  "cache-control": "no-store",
  pragma: "no-cache",
};

interface SignedClientPayload {
  v: 1;
  n: string;
  r: string[];
  p?: 1;
}

interface ParsedMcpClient {
  clientId: string;
  name: string;
  redirectUris: string[];
  peerLink: boolean;
}

interface PendingAuthCode {
  clientId: string;
  clientName: string;
  peerLink: boolean;
  redirectUri: string;
  codeChallenge: string;
  resource: string;
  access: AuthMcpClientAccess;
  expiresAtMs: number;
}

interface AuthorizationDetails {
  client: ParsedMcpClient;
  redirectUri: string;
  codeChallenge: string;
  state?: string;
  resource: string;
  issuer: string;
}

/**
 * Derive the public origin clients reached this Hub at.
 * Behind Cloudflare Tunnel / reverse proxies `req.url` can be `http://...`, while the peer sent
 * `https://<host>/mcp` as `resource`; checking forwarded headers and `hub.host` keeps both aligned.
 */
export function requestOrigin(req: Request, configuredHost?: string): string {
  const parsed = new URL(req.url);
  const rawHost =
    req.headers.get("x-forwarded-host")?.split(",")[0]?.trim() ||
    req.headers.get("host")?.trim() ||
    parsed.host;
  let proto = req.headers.get("x-forwarded-proto")?.split(",")[0]?.trim().toLowerCase();
  if (!proto) {
    const cfVisitor = req.headers.get("cf-visitor");
    if (cfVisitor) {
      try {
        const scheme = (JSON.parse(cfVisitor) as { scheme?: unknown }).scheme;
        if (scheme === "https" || scheme === "http") proto = scheme;
      } catch {
        // ignore malformed cf-visitor
      }
    }
  }
  if (!proto) {
    const hostWithoutPort = rawHost.replace(/:\d+$/, "").toLowerCase();
    const isConfiguredPublicHost =
      Boolean(configuredHost) &&
      hostWithoutPort === configuredHost!.toLowerCase() &&
      !LOOPBACK_HOSTNAMES.has(hostWithoutPort);
    proto = isConfiguredPublicHost ? "https" : parsed.protocol.replace(/:$/, "");
  }
  if (proto !== "http" && proto !== "https") proto = "https";
  return `${proto}://${rawHost}`;
}

function parseRedirectUri(value: string): URL | null {
  if (value.length > MAX_REDIRECT_URI_LENGTH || value.includes("#")) return null;
  try {
    const url = new URL(value);
    const allowed =
      url.protocol === "https:" || (url.protocol === "http:" && LOOPBACK_HOSTNAMES.has(url.hostname));
    return allowed && url.username === "" && url.password === "" ? url : null;
  } catch {
    return null;
  }
}

/** Loopback redirects ignore port (RFC 8252 §7.3); HTTPS redirects match exactly. */
function redirectMatches(registered: string, presented: string): boolean {
  const left = parseRedirectUri(registered);
  const right = parseRedirectUri(presented);
  if (!left || !right) return false;
  if (left.protocol === "https:" || right.protocol === "https:") return left.href === right.href;
  return left.hostname === right.hostname && left.pathname === right.pathname && left.search === right.search;
}

function sameResource(expectedResource: string, presented: string | undefined): boolean {
  if (presented === undefined) return true;
  if (presented.includes("#")) return false;
  try {
    const url = new URL(presented);
    const normalized = `${url.origin}${url.pathname.replace(/\/+$/u, "")}`;
    return normalized === expectedResource;
  } catch {
    return false;
  }
}

function redirectWith(redirectUri: string, params: Record<string, string | undefined>): string {
  const url = new URL(redirectUri);
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined) url.searchParams.set(k, v);
  }
  return url.toString();
}

function verifyPkceS256(verifier: string, challenge: string): boolean {
  if (!CODE_VERIFIER_PATTERN.test(verifier)) return false;
  const computed = createHash("sha256").update(verifier).digest("base64url");
  return safeEqual(computed, challenge);
}

function jsonResponse(body: unknown, status = 200, extraHeaders?: Record<string, string>): Response {
  return Response.json(body, {
    status,
    headers: {
      ...NO_STORE_HEADERS,
      ...(extraHeaders ?? {}),
    },
  });
}

function registrationError(error: "invalid_client_metadata" | "invalid_redirect_uri", description: string): Response {
  return jsonResponse({ error, error_description: description }, 400);
}

function approvalError(message: string): Response {
  return jsonResponse({ _tag: "AuthMcpApprovalError", message }, 400);
}

function tokenError(
  error: "invalid_request" | "invalid_client" | "invalid_grant" | "unsupported_grant_type",
  description: string,
): Response {
  return jsonResponse({ error, error_description: description }, 400);
}

/**
 * Native T3 Code MCP OAuth server (`/.well-known/oauth-*` and `/oauth/mcp/*`) for Boat Sentinel Hub.
 * Allows T3 Code's Linked Environments (`PeerLinks`) to pair directly with the Hub using a one-time
 * pairing code (either minted via `POST /api/auth/pairing-token` or `sentinel pair`).
 */
export class HubMcpOAuthHandler {
  private readonly codes = new Map<string, PendingAuthCode>();

  constructor(
    private readonly config: SentinelConfig,
    private readonly state: StateStore,
    private readonly auth: SlotAuth,
    private readonly rateLimiter?: AuthRateLimiter,
    private readonly onClientsChanged?: () => void,
  ) {}

  private signingSecret(): string {
    const hub = this.state.ensureHub(
      this.config.hub?.label ?? "Boat",
      Object.keys(this.state.getAllProjects(this.config)),
    );
    if (hub.mcpOAuthSecret) return hub.mcpOAuthSecret;
    const secret = randomToken(32);
    this.state.updateHub((h) => {
      if (!h.mcpOAuthSecret) h.mcpOAuthSecret = secret;
    });
    return this.state.ensureHub().mcpOAuthSecret!;
  }

  private signPayload(domain: string, body: string): string {
    return createHmac("sha256", this.signingSecret()).update(`${domain}.${body}`).digest("base64url");
  }

  private signClientId(name: string, redirectUris: string[], peerLink: boolean): string {
    const payload: SignedClientPayload = { v: 1, n: name, r: redirectUris, ...(peerLink ? { p: 1 } : {}) };
    const body = Buffer.from(JSON.stringify(payload), "utf8").toString("base64url");
    const sig = this.signPayload("mcp-client-id", body);
    return `${body}.${sig}`;
  }

  private parseClientId(clientId: string): ParsedMcpClient | null {
    const parts = clientId.split(".");
    if (parts.length !== 2) return null;
    const [body, sig] = parts;
    if (!body || !sig) return null;
    const expectedSig = this.signPayload("mcp-client-id", body);
    if (!safeEqual(sig, expectedSig)) return null;
    try {
      const parsed = JSON.parse(Buffer.from(body, "base64url").toString("utf8")) as Partial<SignedClientPayload>;
      if (
        parsed.v !== 1 ||
        typeof parsed.n !== "string" ||
        !Array.isArray(parsed.r) ||
        !parsed.r.every((u): u is string => typeof u === "string")
      ) {
        return null;
      }
      return {
        clientId,
        name: parsed.n,
        redirectUris: parsed.r,
        peerLink: parsed.p === 1,
      };
    } catch {
      return null;
    }
  }

  private pruneCodes(now = Date.now()): void {
    for (const [code, entry] of this.codes) {
      if (entry.expiresAtMs <= now) this.codes.delete(code);
    }
  }

  /** Returns a Response if the request matches an MCP OAuth / discovery path, else null. */
  async handleHttp(req: Request, url: URL): Promise<Response | null> {
    const path = url.pathname;
    const origin = requestOrigin(req, this.config.hub?.host);
    const resource = `${origin}/mcp`;

    if (
      req.method === "GET" &&
      (path === "/.well-known/oauth-protected-resource" || path === "/.well-known/oauth-protected-resource/mcp")
    ) {
      const hub = this.state.ensureHub(
        this.config.hub?.label ?? "Boat",
        Object.keys(this.state.getAllProjects(this.config)),
      );
      return jsonResponse({
        resource,
        authorization_servers: [origin],
        scopes_supported: [...MCP_OAUTH_SCOPES],
        bearer_methods_supported: ["header"],
        resource_name: hub.label || "T3 Code",
      });
    }

    if (req.method === "GET" && path === "/.well-known/oauth-authorization-server") {
      return jsonResponse({
        issuer: origin,
        authorization_endpoint: `${origin}/oauth/mcp/authorize`,
        token_endpoint: `${origin}/oauth/mcp/token`,
        registration_endpoint: `${origin}/oauth/mcp/register`,
        revocation_endpoint: `${origin}/oauth/mcp/revoke`,
        response_types_supported: ["code"],
        grant_types_supported: ["authorization_code"],
        code_challenge_methods_supported: ["S256"],
        token_endpoint_auth_methods_supported: ["none"],
        scopes_supported: [...MCP_OAUTH_SCOPES],
        authorization_response_iss_parameter_supported: true,
      });
    }

    if (req.method === "POST" && path === "/oauth/mcp/register") {
      return this.handleRegister(req);
    }

    if (req.method === "POST" && path === "/oauth/mcp/decision") {
      return this.handleDecision(req, origin, resource);
    }

    if (req.method === "POST" && path === "/oauth/mcp/token") {
      return this.handleToken(req, resource);
    }

    if (req.method === "POST" && path === "/oauth/mcp/revoke") {
      return this.handleRevoke(req);
    }

    return null;
  }

  private async handleRegister(req: Request): Promise<Response> {
    let raw: unknown;
    try {
      raw = await req.json();
    } catch {
      return registrationError("invalid_client_metadata", "Invalid JSON body.");
    }
    if (!raw || typeof raw !== "object") {
      return registrationError("invalid_client_metadata", "Client registration metadata must be an object.");
    }
    const meta = raw as Record<string, unknown>;
    const rawName = typeof meta.client_name === "string" ? meta.client_name.trim() : "";
    const name = (rawName.length > 0 ? rawName : DEFAULT_CLIENT_NAME).slice(0, MAX_CLIENT_NAME_LENGTH);
    const redirectUris = Array.isArray(meta.redirect_uris)
      ? meta.redirect_uris.filter((u): u is string => typeof u === "string")
      : [];
    if (redirectUris.length === 0 || redirectUris.length > MAX_REDIRECT_URIS) {
      return registrationError(
        "invalid_redirect_uri",
        `Register between 1 and ${MAX_REDIRECT_URIS} redirect URIs.`,
      );
    }
    if (!redirectUris.every((u) => parseRedirectUri(u) !== null)) {
      return registrationError(
        "invalid_redirect_uri",
        "Redirect URIs must be https, or http on localhost, 127.0.0.1 or [::1].",
      );
    }
    const peerLink = meta.software_id === MCP_PEER_LINK_SOFTWARE_ID;
    const clientId = this.signClientId(name, redirectUris, peerLink);
    if (redirectUris.length > MAX_UNBOUNDED_CLIENT_ID_REDIRECT_URIS && clientId.length > MAX_CLIENT_ID_LENGTH) {
      return registrationError("invalid_redirect_uri", "Redirect URIs are too long to fit in an authorization request.");
    }
    return jsonResponse(
      {
        client_id: clientId,
        client_name: name,
        redirect_uris: redirectUris,
        grant_types: ["authorization_code"],
        response_types: ["code"],
        token_endpoint_auth_method: "none",
      },
      201,
    );
  }

  private validateAuthorization(
    authReq: Record<string, unknown>,
    issuer: string,
    resource: string,
  ):
    | { ok: true; details: AuthorizationDetails }
    | { ok: false; kind: "page"; message: string }
    | { ok: false; kind: "redirect"; redirectTo: string } {
    const clientId = typeof authReq.client_id === "string" ? authReq.client_id : "";
    const client = this.parseClientId(clientId);
    if (!client) {
      return {
        ok: false,
        kind: "page",
        message: "This sign-in link names an unknown app. Start the sign-in again from it.",
      };
    }
    const redirectUri = typeof authReq.redirect_uri === "string" ? authReq.redirect_uri : undefined;
    if (!redirectUri || !client.redirectUris.some((reg) => redirectMatches(reg, redirectUri))) {
      return {
        ok: false,
        kind: "page",
        message: "This sign-in link sends you to an address the app did not register.",
      };
    }
    const state = typeof authReq.state === "string" ? authReq.state : undefined;
    const failRedirect = (error: string, description: string) => ({
      ok: false as const,
      kind: "redirect" as const,
      redirectTo: redirectWith(redirectUri, {
        error,
        error_description: description,
        state,
        iss: issuer,
      }),
    });
    if (authReq.response_type !== "code") {
      return failRedirect("unsupported_response_type", "Only response_type=code is supported.");
    }
    const codeChallenge = typeof authReq.code_challenge === "string" ? authReq.code_challenge : undefined;
    if (
      authReq.code_challenge_method !== "S256" ||
      !codeChallenge ||
      !CODE_CHALLENGE_PATTERN.test(codeChallenge)
    ) {
      return failRedirect("invalid_request", "PKCE with code_challenge_method=S256 is required.");
    }
    const presentedResource = typeof authReq.resource === "string" ? authReq.resource : undefined;
    if (!sameResource(resource, presentedResource)) {
      return failRedirect("invalid_target", `This server only issues tokens for ${resource}.`);
    }
    return {
      ok: true,
      details: {
        client,
        redirectUri,
        codeChallenge,
        state,
        resource,
        issuer,
      },
    };
  }

  private async handleDecision(req: Request, issuer: string, resource: string): Promise<Response> {
    const ip = clientIpFromRequest(req);
    if (this.rateLimiter) {
      const limit = this.rateLimiter.check("hub-pair", ip, 10, 15 * 60_000);
      if (!limit.allowed) return rateLimitedResponse(limit.retryAfterSecs);
    }

    let raw: unknown;
    try {
      raw = await req.json();
    } catch {
      return approvalError("Invalid JSON request body.");
    }
    if (!raw || typeof raw !== "object") {
      return approvalError("Invalid decision request.");
    }
    const body = raw as { authorization?: unknown; decision?: unknown };
    if (!body.authorization || typeof body.authorization !== "object") {
      return approvalError("Missing authorization request.");
    }
    const validated = this.validateAuthorization(
      body.authorization as Record<string, unknown>,
      issuer,
      resource,
    );
    if (!validated.ok) {
      if (validated.kind === "redirect") return jsonResponse({ redirectTo: validated.redirectTo }, 200);
      return approvalError(validated.message);
    }
    const { details } = validated;

    const decision = body.decision as Record<string, unknown> | undefined;
    if (!decision || typeof decision !== "object" || typeof decision._tag !== "string") {
      return approvalError("Missing approval decision.");
    }

    if (decision._tag === "deny") {
      return jsonResponse({
        redirectTo: redirectWith(details.redirectUri, {
          error: "access_denied",
          state: details.state,
          iss: details.issuer,
        }),
      });
    }

    if (decision._tag !== "pairing-code") {
      return approvalError("Enter a pairing code from this environment to approve the link.");
    }

    if (!isAuthMcpClientAccess(decision.access)) {
      return approvalError("Invalid access level.");
    }
    const rawCode = typeof decision.code === "string" ? decision.code.trim() : "";
    if (!rawCode) {
      return approvalError("A pairing code is required.");
    }

    const redeemed = this.auth.redeemPairingCodeDetailed(rawCode, {
      forPeerLink: details.client.peerLink,
      requestedAccess: decision.access,
    });
    if (!redeemed.ok) {
      this.rateLimiter?.recordFailure("hub-pair", ip, 15 * 60_000);
      if (redeemed.reason === "insufficient_scope") {
        return approvalError(
          "That pairing code cannot grant this access. It has not been used, so choose an access level it grants, or create a code with broader access.",
        );
      }
      return approvalError("That pairing code is unknown, expired, or already used.");
    }
    this.rateLimiter?.recordSuccess("hub-pair", ip);

    const now = Date.now();
    this.pruneCodes(now);
    const code = randomToken(32);
    this.codes.set(code, {
      clientId: details.client.clientId,
      clientName: details.client.name,
      peerLink: details.client.peerLink,
      redirectUri: details.redirectUri,
      codeChallenge: details.codeChallenge,
      resource: details.resource,
      access: redeemed.access ?? decision.access,
      expiresAtMs: now + AUTHORIZATION_CODE_TTL_MS,
    });

    return jsonResponse({
      redirectTo: redirectWith(details.redirectUri, {
        code,
        state: details.state,
        iss: details.issuer,
      }),
    });
  }

  private async handleToken(req: Request, expectedResource: string): Promise<Response> {
    const form = new URLSearchParams(await req.text());
    const grantType = form.get("grant_type") ?? undefined;
    if (grantType !== "authorization_code") {
      return tokenError("unsupported_grant_type", "Only authorization_code is supported.");
    }
    const code = form.get("code") ?? "";
    const redirectUri = form.get("redirect_uri") ?? "";
    const clientId = form.get("client_id") ?? "";
    const verifier = form.get("code_verifier") ?? "";
    const presentedResource = form.get("resource") ?? undefined;

    if (!code || !redirectUri || !clientId || !verifier) {
      return tokenError("invalid_request", "code, redirect_uri, client_id and code_verifier are required.");
    }
    if (!this.parseClientId(clientId)) {
      return tokenError("invalid_client", "The client is unknown.");
    }

    // Consume authorization code once even if subsequent checks fail (RFC 6749 §4.1.2).
    const pending = this.codes.get(code);
    if (pending) this.codes.delete(code);

    const now = Date.now();
    if (
      !pending ||
      pending.expiresAtMs <= now ||
      pending.clientId !== clientId ||
      pending.redirectUri !== redirectUri ||
      pending.resource !== expectedResource ||
      !sameResource(expectedResource, presentedResource) ||
      !verifyPkceS256(verifier, pending.codeChallenge)
    ) {
      return tokenError("invalid_grant", "The authorization code is invalid or expired.");
    }

    const token = randomToken(32);
    const clientRecordId = randomUUID();
    const pairedAt = new Date(now).toISOString();
    const expiresAt = new Date(now + PEER_LINK_TOKEN_TTL_SECS * 1000).toISOString();

    this.state.updateHub((h) => {
      h.clients.push({
        id: clientRecordId,
        tokenHash: hashToken(token),
        label: pending.clientName,
        pairedAt,
        lastSyncedSandboxId: null,
        peerLink: pending.peerLink,
        mcpOnly: true,
        access: pending.access,
        expiresAt,
      });
    });
    this.onClientsChanged?.();

    console.log(
      `[hub] paired ${pending.peerLink ? "peer link" : "MCP OAuth client"} "${pending.clientName}" (access=${pending.access}, clientId=${clientRecordId})`,
    );

    return jsonResponse({
      access_token: token,
      token_type: "Bearer",
      expires_in: PEER_LINK_TOKEN_TTL_SECS,
      scope: mcpClientScopes(pending.access).join(" "),
    });
  }

  private async handleRevoke(req: Request): Promise<Response> {
    const form = new URLSearchParams(await req.text());
    const token = form.get("token")?.trim();
    if (token) {
      const digest = hashToken(token);
      let removed = false;
      this.state.updateHub((h) => {
        const before = h.clients.length;
        h.clients = h.clients.filter((c) => !(c.mcpOnly && safeEqual(c.tokenHash, digest)));
        removed = h.clients.length !== before;
      });
      if (removed) {
        this.onClientsChanged?.();
        console.log("[hub] revoked MCP OAuth / peer-link token via /oauth/mcp/revoke");
      }
    }
    // RFC 7009 §2.2: always 200 regardless of whether the token existed.
    return new Response(null, { status: 200, headers: NO_STORE_HEADERS });
  }
}
