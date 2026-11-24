import { createHash, randomBytes, timingSafeEqual } from "node:crypto";

export type TicketUse = "rpc" | "media";

const PAIRING_TTL_MS = 15 * 60_000;
const TICKET_TTL_MS = 5 * 60_000;
export const CLIENT_TOKEN_TTL_SECS = 10 * 365 * 24 * 3600;

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

interface PairingEntry {
  code: string;
  expiresAt: number;
}

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
}

/**
 * Per-slot T3-compatible auth manager.
 * Sentinel owns the client-facing pairing code, long-lived hashed bearer tokens, and WS tickets,
 * so destroying and recreating the Boat VM behind a slot never invalidates T3 Mobile pairing.
 */
export class SlotAuth {
  private pairings = new Map<string, number>();
  private tickets = new Map<string, TicketEntry>();

  newPairingCode(): { code: string; expiresAt: string } {
    const now = Date.now();
    for (const [k, exp] of this.pairings) {
      if (exp <= now) this.pairings.delete(k);
    }
    const code = randomToken(32);
    const expiresAt = now + PAIRING_TTL_MS;
    this.pairings.set(code, expiresAt);
    return { code, expiresAt: new Date(expiresAt).toISOString() };
  }

  redeemPairingCode(code: string): boolean {
    if (!code) return false;
    const now = Date.now();
    for (const [stored, exp] of this.pairings) {
      if (exp <= now) {
        this.pairings.delete(stored);
        continue;
      }
      if (safeEqual(stored, code)) {
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

export function authenticateClient(
  clients: ClientCredentialRecord[],
  headers: Headers,
): ClientCredentialRecord | null {
  const token = extractBearer(headers);
  if (!token) return null;
  const digest = hashToken(token);
  return clients.find((c) => safeEqual(c.tokenHash, digest)) ?? null;
}
