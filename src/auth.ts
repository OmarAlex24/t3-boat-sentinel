import { createHash, randomBytes, randomUUID, timingSafeEqual } from "node:crypto";

export type TicketUse = "rpc" | "media";

export const AUTH_MCP_CLIENT_ACCESS_LEVELS = [
  "read-only",
  "approval-required",
  "auto-accept-edits",
  "auto",
  "full-access",
] as const;
export type AuthMcpClientAccess = (typeof AUTH_MCP_CLIENT_ACCESS_LEVELS)[number];

export const RUNTIME_MODE_LEVELS = [
  "approval-required",
  "auto-accept-edits",
  "auto",
  "full-access",
] as const;
export type RuntimeMode = (typeof RUNTIME_MODE_LEVELS)[number];

export const INTERACTION_MODE_LEVELS = ["plan", "default"] as const;
export type ProviderInteractionMode = (typeof INTERACTION_MODE_LEVELS)[number];

const ACCESS_RANK: Record<AuthMcpClientAccess, number> = {
  "read-only": 0,
  "approval-required": 1,
  "auto-accept-edits": 2,
  auto: 3,
  "full-access": 4,
};

export function isAuthMcpClientAccess(value: unknown): value is AuthMcpClientAccess {
  return typeof value === "string" && (AUTH_MCP_CLIENT_ACCESS_LEVELS as readonly string[]).includes(value);
}

export function isRuntimeMode(value: unknown): value is RuntimeMode {
  return typeof value === "string" && (RUNTIME_MODE_LEVELS as readonly string[]).includes(value);
}

export function isInteractionMode(value: unknown): value is ProviderInteractionMode {
  return typeof value === "string" && (INTERACTION_MODE_LEVELS as readonly string[]).includes(value);
}

export function accessRank(access: AuthMcpClientAccess): number {
  return ACCESS_RANK[access];
}

export function mcpClientScopes(access: AuthMcpClientAccess): string[] {
  if (access === "read-only") return ["orchestration:read"];
  if (access === "full-access") {
    return [
      "orchestration:read",
      "orchestration:operate",
      "settings:write",
      "source-control:write",
    ];
  }
  return ["orchestration:read", "orchestration:operate"];
}

const PAIRING_TTL_MS = 15 * 60_000;
const TICKET_TTL_MS = 5 * 60_000;
export const CLIENT_TOKEN_TTL_SECS = 10 * 365 * 24 * 3600;
/** Rolling TTL for peer-link & MCP OAuth sessions (30 days, renewed on use). */
export const PEER_LINK_TOKEN_TTL_SECS = 30 * 24 * 3600;
/** Renew stored expiry when more than 24h behind the rolling window. */
export const PEER_LINK_RENEWAL_STEP_MS = 24 * 3600_000;

export const DEFAULT_T3_SCOPES = [
  "orchestration:read",
  "orchestration:operate",
  "terminal:operate",
  "access:read",
  "access:write",
  "relay:read",
  "relay:write",
] as const;

export const DEFAULT_T3_PERMISSIONS = [
  "orchestration:read",
  "orchestration:operate",
  "settings:write",
  "providers:manage",
  "environment:maintain",
  "preview:operate",
  "diagnostics:read",
  "terminal:read",
  "terminal:operate",
  "source-control:write",
  "filesystem:read",
  "filesystem:write",
  "access:read",
  "access:write",
  "relay:read",
  "relay:write",
] as const;

export function randomToken(bytes = 32): string {
  return randomBytes(bytes).toString("base64url");
}

/** Hash bearer tokens before persisting to disk so state.json never stores raw client secrets. */
export function hashToken(token: string): string {
  return createHash("sha256").update(token).digest("hex");
}

export function safeEqual(a: string, b: string): boolean {
  const ab = Buffer.from(a);
  const bb = Buffer.from(b);
  if (ab.length !== bb.length) return false;
  return timingSafeEqual(ab, bb);
}

export interface PairingEntry {
  id: string;
  code: string;
  label?: string;
  purpose?: "peer-link";
  access?: AuthMcpClientAccess;
  scopes?: string[];
  createdAt: number;
  expiresAt: number;
}

export interface NewPairingCodeOptions {
  label?: string;
  purpose?: "peer-link";
  access?: AuthMcpClientAccess;
  scopes?: string[];
  ttlMs?: number;
}

export interface RedeemPairingCodeOptions {
  /** True when redeeming for a peer link (`/oauth/mcp/decision` with `software_id=t3code-peer-link`). */
  forPeerLink?: boolean;
  /** Access level requested during MCP OAuth approval. */
  requestedAccess?: AuthMcpClientAccess;
}

export type RedeemPairingResult =
  | { ok: true; access?: AuthMcpClientAccess; entry: PairingEntry }
  | { ok: false; reason: "unknown_or_used" | "insufficient_scope" };

interface TicketEntry {
  use: TicketUse;
  clientId: string;
  expiresAt: number;
}

export interface ClientCredentialRecord {
  id: string;
  tokenHash: string;
  label: string;
  pairedAt: string;
  /** Last sandboxId this client synced against; used to strip stale resume cursors after VM swap. */
  lastSyncedSandboxId: string | null;
  /** True when issued via MCP OAuth for another T3 environment's peer link. */
  peerLink?: boolean;
  /** True when restricted to `/mcp` (and `/oauth/mcp/revoke`), never `/ws` or client HTTP APIs. */
  mcpOnly?: boolean;
  /** Access ceiling granted at MCP OAuth approval; unset means full-access (legacy client/machine token). */
  access?: AuthMcpClientAccess;
  /** Rolling ISO expiry for peer-link / MCP OAuth sessions; unset means non-expiring client token. */
  expiresAt?: string;
}

/**
 * Per-slot T3-compatible auth manager.
 * Sentinel owns the client-facing pairing code, long-lived hashed bearer tokens, and WS tickets,
 * so destroying and recreating the Boat VM behind a slot never invalidates T3 Mobile pairing.
 */
export class SlotAuth {
  private pairings = new Map<string, PairingEntry>();
  private tickets = new Map<string, TicketEntry>();

  private prunePairings(now = Date.now()): void {
    for (const [k, entry] of this.pairings) {
      if (entry.expiresAt <= now) this.pairings.delete(k);
    }
  }

  newPairingCode(opts?: NewPairingCodeOptions): {
    id: string;
    code: string;
    label?: string;
    purpose?: "peer-link";
    access?: AuthMcpClientAccess;
    expiresAt: string;
  } {
    const now = Date.now();
    this.prunePairings(now);
    const id = randomUUID();
    const code = randomToken(32);
    const expiresAt = now + (opts?.ttlMs ?? PAIRING_TTL_MS);
    const entry: PairingEntry = {
      id,
      code,
      ...(opts?.label ? { label: opts.label } : {}),
      ...(opts?.purpose ? { purpose: opts.purpose } : {}),
      ...(opts?.access ? { access: opts.access } : {}),
      ...(opts?.scopes ? { scopes: opts.scopes } : {}),
      createdAt: now,
      expiresAt,
    };
    this.pairings.set(code, entry);
    return {
      id,
      code,
      ...(entry.label ? { label: entry.label } : {}),
      ...(entry.purpose ? { purpose: entry.purpose } : {}),
      ...(entry.access ? { access: entry.access } : {}),
      expiresAt: new Date(expiresAt).toISOString(),
    };
  }

  listPairingLinks(): PairingEntry[] {
    this.prunePairings();
    return Array.from(this.pairings.values());
  }

  revokePairingLink(id: string): boolean {
    this.prunePairings();
    for (const [code, entry] of this.pairings) {
      if (entry.id === id) {
        this.pairings.delete(code);
        return true;
      }
    }
    return false;
  }

  redeemPairingCodeDetailed(code: string, opts?: RedeemPairingCodeOptions): RedeemPairingResult {
    const trimmed = code.trim();
    if (!trimmed) return { ok: false, reason: "unknown_or_used" };
    const now = Date.now();
    for (const [stored, entry] of this.pairings) {
      if (entry.expiresAt <= now) {
        this.pairings.delete(stored);
        continue;
      }
      if (!safeEqual(stored, trimmed)) continue;
      // Peer-link codes minted with an access level cannot grant a broader access than minted.
      // Leave the code unconsumed on insufficient_scope, matching T3 Code's EnvironmentAuth.
      if (entry.access && opts?.requestedAccess && accessRank(opts.requestedAccess) > accessRank(entry.access)) {
        return { ok: false, reason: "insufficient_scope" };
      }
      this.pairings.delete(stored);
      return { ok: true, access: opts?.requestedAccess ?? entry.access, entry };
    }
    return { ok: false, reason: "unknown_or_used" };
  }

  redeemPairingCode(code: string): boolean {
    const trimmed = code.trim();
    if (!trimmed) return false;
    const now = Date.now();
    for (const [stored, entry] of this.pairings) {
      if (entry.expiresAt <= now) {
        this.pairings.delete(stored);
        continue;
      }
      if (safeEqual(stored, trimmed)) {
        // Peer-link-only codes cannot mint full interactive desktop/mobile client sessions on /oauth/token.
        if (entry.purpose === "peer-link") return false;
        this.pairings.delete(stored);
        return true;
      }
    }
    return false;
  }

  issueTicket(use: TicketUse, clientId: string): { ticket: string; expiresAt: string } {
    const now = Date.now();
    for (const [k, v] of this.tickets) {
      if (v.expiresAt <= now) this.tickets.delete(k);
    }
    const ticket = randomToken(32);
    const expiresAt = now + TICKET_TTL_MS;
    this.tickets.set(ticket, { use, clientId, expiresAt });
    return { ticket, expiresAt: new Date(expiresAt).toISOString() };
  }

  /** Returns the owning clientId if ticket is valid for `use`, else null. */
  checkTicket(ticket: string, use: TicketUse): string | null {
    const entry = this.tickets.get(ticket);
    if (!entry) return null;
    const ok = entry.use === use && entry.expiresAt > Date.now();
    if (!ok) return null;
    if (use === "rpc") {
      this.tickets.delete(ticket);
    }
    return entry.clientId;
  }
}

export function extractBearer(headers: Headers): string | null {
  const auth = headers.get("authorization");
  if (!auth) return null;
  const match = /^Bearer\s+(.+)$/i.exec(auth.trim());
  return match ? match[1] : null;
}

export interface AuthenticateClientOptions {
  /** Allow tokens marked `mcpOnly` / `peerLink` (true only on `/mcp`). Defaults to false. */
  allowMcpOnly?: boolean;
}

export function authenticateClient(
  clients: ClientCredentialRecord[],
  headers: Headers,
  opts?: AuthenticateClientOptions,
): ClientCredentialRecord | null {
  const token = extractBearer(headers);
  if (!token) return null;
  return authenticateToken(clients, token, opts);
}

export function authenticateToken(
  clients: ClientCredentialRecord[],
  token: string,
  opts?: AuthenticateClientOptions,
): ClientCredentialRecord | null {
  if (!token) return null;
  const digest = hashToken(token);
  const found = clients.find((c) => safeEqual(c.tokenHash, digest)) ?? null;
  if (!found) return null;
  if (!opts?.allowMcpOnly && (found.mcpOnly === true || found.peerLink === true)) {
    return null;
  }
  if (found.expiresAt) {
    const expMs = Date.parse(found.expiresAt);
    if (!Number.isFinite(expMs) || expMs <= Date.now()) return null;
  }
  return found;
}
