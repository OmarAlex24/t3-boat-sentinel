import type { CloudflareAccessConfig } from "./config.ts";

export const ROBOTS_TXT = "User-agent: *\nDisallow: /\n";

/**
 * Extract the best-effort client IP for rate limiting.
 * Prefers `CF-Connecting-IP` (set by Cloudflare Tunnel / edge) over `X-Forwarded-For`.
 */
export function clientIpFromRequest(req: Request): string {
  const cf = req.headers.get("cf-connecting-ip")?.trim();
  if (cf) return cf;
  const xff = req.headers.get("x-forwarded-for")?.split(",")[0]?.trim();
  if (xff) return xff;
  return "direct";
}

interface BucketRecord {
  failures: number[];
}

/**
 * Lightweight in-memory sliding-window rate limiter for authentication endpoints.
 * Only failed auth attempts count against the budget so legitimate clients are never throttled.
 */
export class AuthRateLimiter {
  private readonly buckets = new Map<string, BucketRecord>();

  check(bucket: string, ip: string, maxFailures: number, windowMs: number): { allowed: boolean; retryAfterSecs: number } {
    const key = `${bucket}:${ip}`;
    const rec = this.buckets.get(key);
    if (!rec) return { allowed: true, retryAfterSecs: 0 };
    const cutoff = Date.now() - windowMs;
    rec.failures = rec.failures.filter((ts) => ts > cutoff);
    if (rec.failures.length === 0) {
      this.buckets.delete(key);
      return { allowed: true, retryAfterSecs: 0 };
    }
    if (rec.failures.length >= maxFailures) {
      const oldest = rec.failures[0];
      const retryAfterSecs = Math.max(1, Math.ceil((oldest + windowMs - Date.now()) / 1000));
      return { allowed: false, retryAfterSecs };
    }
    return { allowed: true, retryAfterSecs: 0 };
  }

  recordFailure(bucket: string, ip: string, windowMs: number): void {
    const key = `${bucket}:${ip}`;
    const now = Date.now();
    const cutoff = now - windowMs;
    let rec = this.buckets.get(key);
    if (!rec) {
      rec = { failures: [] };
      this.buckets.set(key, rec);
    }
    rec.failures = rec.failures.filter((ts) => ts > cutoff);
    rec.failures.push(now);
    // Bound memory if attacked from many spoofed IPs
    if (this.buckets.size > 10_000) {
      const firstKey = this.buckets.keys().next().value;
      if (firstKey) this.buckets.delete(firstKey);
    }
  }

  recordSuccess(bucket: string, ip: string): void {
    this.buckets.delete(`${bucket}:${ip}`);
  }
}

export function rateLimitedResponse(retryAfterSecs: number): Response {
  return applySecurityHeaders(
    Response.json(
      { error: "too_many_requests", retryAfterSecs },
      { status: 429, headers: { "retry-after": String(retryAfterSecs) } },
    ),
  );
}

/**
 * Apply defense-in-depth headers to every HTTP response (blocks search indexing, MIME sniffing,
 * referrer leakage, and framing of the Control Plane dashboard).
 */
export function applySecurityHeaders(res: Response, opts: { isHtml?: boolean } = {}): Response {
  const headers = new Headers(res.headers);
  headers.set("x-robots-tag", "noindex, nofollow, noarchive");
  headers.set("x-content-type-options", "nosniff");
  headers.set("referrer-policy", "no-referrer");
  if (opts.isHtml) {
    headers.set("x-frame-options", "DENY");
    headers.set(
      "content-security-policy",
      "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'",
    );
  }
  return new Response(res.body, {
    status: res.status,
    statusText: res.statusText,
    headers,
  });
}

interface JwkKey {
  kid?: string;
  kty: string;
  n?: string;
  e?: string;
  alg?: string;
}

/**
 * Zero-dependency RS256 Cloudflare Access JWT verifier (`Cf-Access-Jwt-Assertion` header).
 * Verifies cryptographic signature against `https://<teamDomain>/cdn-cgi/access/certs`,
 * checks `aud`, `exp`, and optional `allowedEmails` allow-list.
 */
export class CloudflareAccessVerifier {
  private cachedKeys = new Map<string, CryptoKey>();
  private fetchedAt = 0;

  constructor(private readonly cfg: CloudflareAccessConfig) {}

  async verifyRequest(req: Request): Promise<{ ok: true; email?: string } | { ok: false; reason: string }> {
    const token =
      req.headers.get("cf-access-jwt-assertion")?.trim() ||
      extractCookie(req.headers.get("cookie"), "CF_Authorization");
    if (!token) {
      return { ok: false, reason: "missing_cf_access_jwt" };
    }
    return this.verifyJwt(token);
  }

  async verifyJwt(jwt: string): Promise<{ ok: true; email?: string } | { ok: false; reason: string }> {
    const parts = jwt.split(".");
    if (parts.length !== 3) return { ok: false, reason: "malformed_jwt" };
    const [headerB64, payloadB64, sigB64] = parts;

    let header: { kid?: string; alg?: string };
    let payload: { aud?: string | string[]; exp?: number; nbf?: number; email?: string; iss?: string };
    try {
      header = JSON.parse(decodeBase64UrlToString(headerB64)) as { kid?: string; alg?: string };
      payload = JSON.parse(decodeBase64UrlToString(payloadB64)) as {
        aud?: string | string[];
        exp?: number;
        nbf?: number;
        email?: string;
        iss?: string;
      };
    } catch {
      return { ok: false, reason: "invalid_jwt_json" };
    }

    if (header.alg !== "RS256" || !header.kid) {
      return { ok: false, reason: "unsupported_jwt_alg" };
    }

    const nowSec = Math.floor(Date.now() / 1000);
    if (typeof payload.exp !== "number" || payload.exp <= nowSec) {
      return { ok: false, reason: "jwt_expired" };
    }
    if (typeof payload.nbf === "number" && payload.nbf > nowSec + 60) {
      return { ok: false, reason: "jwt_not_yet_valid" };
    }

    const audList = Array.isArray(payload.aud) ? payload.aud : typeof payload.aud === "string" ? [payload.aud] : [];
    if (!audList.includes(this.cfg.aud)) {
      return { ok: false, reason: "invalid_jwt_aud" };
    }

    if (this.cfg.allowedEmails && this.cfg.allowedEmails.length > 0) {
      const email = (payload.email ?? "").toLowerCase();
      if (!email || !this.cfg.allowedEmails.some((e) => e.toLowerCase() === email)) {
        return { ok: false, reason: "email_not_allowed" };
      }
    }

    const key = await this.getSigningKey(header.kid);
    if (!key) return { ok: false, reason: "unknown_signing_key" };

    const data = new TextEncoder().encode(`${headerB64}.${payloadB64}`);
    const sig = decodeBase64Url(sigB64);
    const valid = await crypto.subtle.verify("RSASSA-PKCS1-v1_5", key, sig, data);
    if (!valid) return { ok: false, reason: "invalid_jwt_signature" };

    return { ok: true, email: payload.email };
  }

  private async getSigningKey(kid: string): Promise<CryptoKey | null> {
    const stale = Date.now() - this.fetchedAt > 15 * 60_000;
    if (!stale && this.cachedKeys.has(kid)) {
      return this.cachedKeys.get(kid)!;
    }
    const team = this.cfg.teamDomain.replace(/^https?:\/\//, "").replace(/\/+$/, "");
    const certsUrl = `https://${team}/cdn-cgi/access/certs`;
    const res = await fetch(certsUrl, { signal: AbortSignal.timeout(10_000) });
    if (!res.ok) return this.cachedKeys.get(kid) ?? null;
    const body = (await res.json()) as { keys?: JwkKey[] };
    const next = new Map<string, CryptoKey>();
    for (const jwk of body.keys ?? []) {
      if (!jwk.kid || jwk.kty !== "RSA") continue;
      try {
        const key = await crypto.subtle.importKey(
          "jwk",
          jwk as JsonWebKey,
          { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
          false,
          ["verify"],
        );
        next.set(jwk.kid, key);
      } catch {
        // skip unsupported key
      }
    }
    this.cachedKeys = next;
    this.fetchedAt = Date.now();
    return this.cachedKeys.get(kid) ?? null;
  }
}

function decodeBase64Url(input: string): Uint8Array<ArrayBuffer> {
  const padded = input.replace(/-/g, "+").replace(/_/g, "/") + "=".repeat((4 - (input.length % 4)) % 4);
  const buf = Buffer.from(padded, "base64");
  const out = new Uint8Array(new ArrayBuffer(buf.length));
  out.set(buf);
  return out;
}

function decodeBase64UrlToString(input: string): string {
  return new TextDecoder().decode(decodeBase64Url(input));
}

function extractCookie(cookieHeader: string | null, name: string): string | null {
  if (!cookieHeader) return null;
  for (const part of cookieHeader.split(";")) {
    const eq = part.indexOf("=");
    if (eq <= 0) continue;
    if (part.slice(0, eq).trim() === name) return part.slice(eq + 1).trim();
  }
  return null;
}
