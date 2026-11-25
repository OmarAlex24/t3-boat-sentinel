/**
 * Shared pieces of the Hub's sleep/wake cycle: the transition events other parts of the Hub hook into,
 * the wake backoff, and the redaction applied to every wake failure before it is logged, stored or sent.
 */

/** One event per wake transition of a sleeping chat VM. */
export type HubWakeEvent =
  | { type: "wake-start"; sandboxId: string; threadIds: string[] }
  | { type: "wake-success"; sandboxId: string; threadIds: string[] }
  | {
      type: "wake-failure";
      sandboxId: string;
      threadIds: string[];
      step: string;
      /** Redacted; safe to show a client. */
      reason: string;
      /** When the next wake may start; `null` when the VM is gone (its disk expired). */
      retryAfter: string | null;
      attempts: number;
    };

export type HubWakeListener = (event: HubWakeEvent) => void;

/** Error thrown for a wake refused during the cooldown after a failure; never logged. */
export class HubWakeCooldownError extends Error {
  constructor(
    readonly sandboxId: string,
    readonly step: string,
    readonly reason: string,
    readonly retryAfter: string,
  ) {
    super(`Waking chat VM ${sandboxId} failed at ${step} (${reason}); next try after ${retryAfter}`);
    this.name = "HubWakeCooldownError";
  }
}

/** Doubling backoff from `baseMs`, capped at `maxMs`. `attempts` counts failures so far (1 = first). */
export function wakeBackoffMs(attempts: number, baseMs: number, maxMs: number): number {
  const exp = Math.min(Math.max(attempts, 1) - 1, 20);
  return Math.min(baseMs * 2 ** exp, maxMs);
}

const SECRET_PREFIXES = /\b(?:ksb1_|t3llm_|t3s_|tskey-|sk-|xox[a-z]-|gh[pousr]_|glpat-)[A-Za-z0-9_\-.]+/g;

/**
 * One-line reason safe to log and show: drops URL paths and queries, known token shapes, `NAME=value`
 * assignments (what `klav-forward env` prints), credential-looking key/value pairs and long opaque
 * strings, then caps the length.
 */
export function redactForLog(err: unknown, maxLength = 240): string {
  let s = err instanceof Error ? err.message : String(err);
  s = s.replace(/\b(?:https?|wss?):\/\/[^\s"'<>`]+/gi, (raw) => {
    try {
      const u = new URL(raw);
      return `${u.protocol}//${u.host}`;
    } catch {
      return "[url]";
    }
  });
  s = s.replace(SECRET_PREFIXES, "[redacted]");
  s = s.replace(/\b([A-Z][A-Z0-9_]{1,})=("[^"]*"|'[^']*'|\S+)/g, "$1=[redacted]");
  s = s.replace(
    /\b(bearer|token|secret|password|passwd|authorization|api[_-]?key)("?\s*[:=]\s*|\s+)("[^"]*"|'[^']*'|\S+)/gi,
    "$1$2[redacted]",
  );
  s = s.replace(/[A-Za-z0-9+/_\-]{32,}={0,2}/g, "[redacted]");
  s = s.replace(/\s+/g, " ").trim();
  return s.length > maxLength ? `${s.slice(0, maxLength)}…` : s;
}
