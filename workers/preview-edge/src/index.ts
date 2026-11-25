export interface SealedEdgePayload {
  /** Upstream base URL (for example, https://abc.boat.dev), with no trailing slash. */
  u: string;
  /** Optional headers required by the upstream preview gate. */
  h?: Record<string, string>;
  /** Sandbox id. */
  s: string;
  /** Dev-server port (1..65535, except Sentinel's reserved control port 3773). */
  p: number;
  /** Expected lowercase preview hostname. */
  host: string;
  /** Expiration timestamp in Unix milliseconds. */
  exp: number;
}

export interface PreviewEdgeEnv {
  PREVIEW_EDGE_SECRET?: string;
  ALLOW_HTTP_UPSTREAM?: string;
}

const BOOTSTRAP_PARAM = "__t3e";
const HUB_BOOTSTRAP_PARAM = "__t3p";
const EDGE_COOKIE = "__t3_edge";
const PREVIEW_HOST_RE = /^p\d+-[a-z0-9]+\./i;
const RESERVED_SET_COOKIES = new Set(["__t3_preview", "__t3_edge", "_port_auth"]);
const ROBOTS_TXT = "User-agent: *\nDisallow: /\n";
const ROBOTS_TAG = "noindex, nofollow, noarchive";
const HEADER_NAME_RE = /^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/;

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

function base64UrlEncode(bytes: Uint8Array): string {
  let binary = "";
  for (let offset = 0; offset < bytes.length; offset += 0x8000) {
    binary += String.fromCharCode(...bytes.subarray(offset, offset + 0x8000));
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function base64UrlDecode(value: string) {
  if (!/^[A-Za-z0-9_-]*$/.test(value)) throw new Error("invalid base64url");
  const base64 = value.replace(/-/g, "+").replace(/_/g, "/");
  if (base64.length % 4 === 1) throw new Error("invalid base64url length");
  const padded = base64 + "=".repeat((4 - (base64.length % 4)) % 4);
  const binary = atob(padded);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

async function edgeKey(secret: string): Promise<CryptoKey> {
  const digest = await crypto.subtle.digest("SHA-256", textEncoder.encode(secret));
  return crypto.subtle.importKey("raw", digest, { name: "AES-GCM" }, false, ["encrypt", "decrypt"]);
}

function validPayloadShape(payload: unknown): payload is SealedEdgePayload {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) return false;
  const value = payload as Partial<SealedEdgePayload>;
  if (typeof value.u !== "string" || value.u.length === 0) return false;
  if (typeof value.s !== "string" || value.s.length === 0) return false;
  if (!Number.isInteger(value.p) || (value.p as number) < 1 || (value.p as number) > 65535 || value.p === 3773) return false;
  if (typeof value.host !== "string" || value.host.length === 0) return false;
  if (typeof value.exp !== "number" || !Number.isFinite(value.exp)) return false;
  if (value.h !== undefined) {
    if (!value.h || typeof value.h !== "object" || Array.isArray(value.h)) return false;
    for (const [name, headerValue] of Object.entries(value.h)) {
      if (!HEADER_NAME_RE.test(name) || typeof headerValue !== "string" || /[\r\n]/.test(headerValue)) return false;
    }
  }
  return true;
}

export async function sealEdgeTarget(payload: SealedEdgePayload, secret: string): Promise<string> {
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const key = await edgeKey(secret);
  const plaintext = textEncoder.encode(JSON.stringify(payload));
  const ciphertext = new Uint8Array(await crypto.subtle.encrypt({ name: "AES-GCM", iv }, key, plaintext));
  return `v1.${base64UrlEncode(iv)}.${base64UrlEncode(ciphertext)}`;
}

export async function unsealEdgeTarget(token: string, secret: string, nowMs = Date.now()): Promise<SealedEdgePayload | null> {
  try {
    const parts = token.split(".");
    if (parts.length !== 3 || parts[0] !== "v1") return null;

    const iv = base64UrlDecode(parts[1]);
    if (iv.length !== 12) return null;
    const ciphertext = base64UrlDecode(parts[2]);
    if (ciphertext.length < 16) return null;

    const key = await edgeKey(secret);
    const plaintext = await crypto.subtle.decrypt({ name: "AES-GCM", iv }, key, ciphertext);
    const payload: unknown = JSON.parse(textDecoder.decode(plaintext));
    if (!validPayloadShape(payload)) return null;
    if (payload.exp <= nowMs) return null;
    return payload;
  } catch {
    return null;
  }
}

function withRobotsTag(response: Response): Response {
  response.headers.set("x-robots-tag", ROBOTS_TAG);
  return response;
}

function unauthorized(): Response {
  return withRobotsTag(
    new Response("Unauthorized", {
      status: 401,
      headers: {
        "content-type": "text/plain; charset=utf-8",
        "cache-control": "no-store",
      },
    }),
  );
}

function badGateway(): Response {
  return withRobotsTag(
    new Response("Bad gateway", {
      status: 502,
      headers: { "content-type": "text/plain; charset=utf-8" },
    }),
  );
}

function verifiedUpstream(payload: SealedEdgePayload, url: URL, env: PreviewEdgeEnv): URL | null {
  if (payload.exp <= Date.now()) return null;
  if (payload.host.toLowerCase() !== url.hostname.toLowerCase()) return null;

  let upstream: URL;
  try {
    upstream = new URL(payload.u);
  } catch {
    return null;
  }
  if (upstream.protocol === "https:") return upstream;
  if (upstream.protocol === "http:" && env.ALLOW_HTTP_UPSTREAM === "1") return upstream;
  return null;
}

async function unsealAndVerify(token: string, url: URL, env: PreviewEdgeEnv): Promise<{ payload: SealedEdgePayload; upstream: URL } | null> {
  if (!env.PREVIEW_EDGE_SECRET) return null;
  const payload = await unsealEdgeTarget(token, env.PREVIEW_EDGE_SECRET);
  if (!payload) return null;
  const upstream = verifiedUpstream(payload, url, env);
  return upstream ? { payload, upstream } : null;
}

interface CookieEntry {
  name: string;
  value: string;
  raw: string;
}

function parseCookies(header: string | null): CookieEntry[] {
  if (!header) return [];
  const entries: CookieEntry[] = [];
  for (const rawPart of header.split(";")) {
    const part = rawPart.trim();
    if (!part) continue;
    const equals = part.indexOf("=");
    if (equals < 0) {
      entries.push({ name: part, value: "", raw: part });
    } else {
      const name = part.slice(0, equals).trim();
      if (name) entries.push({ name, value: part.slice(equals + 1).trim(), raw: part });
    }
  }
  return entries;
}

function edgeCookieValue(cookieHeader: string | null): string | null {
  for (const entry of parseCookies(cookieHeader)) {
    if (entry.name === EDGE_COOKIE) return entry.value;
  }
  return null;
}

function gateCookies(headersRecord: Record<string, string> | undefined): string[] {
  if (!headersRecord) return [];
  return Object.entries(headersRecord)
    .filter(([name]) => name.toLowerCase() === "cookie")
    .map(([, value]) => value.trim())
    .filter(Boolean);
}

function mergeForwardedCookies(requestCookieHeader: string | null, headersRecord: Record<string, string> | undefined): string | null {
  const appCookies = parseCookies(requestCookieHeader)
    .filter((entry) => entry.name !== EDGE_COOKIE)
    .map((entry) => entry.raw);
  const merged = [...gateCookies(headersRecord), ...appCookies];
  return merged.length > 0 ? merged.join("; ") : null;
}

function stripGatewayParams(search: string): string {
  if (!search) return "";
  const kept: string[] = [];
  for (const part of search.slice(1).split("&")) {
    const equals = part.indexOf("=");
    const rawName = equals < 0 ? part : part.slice(0, equals);
    let name = rawName;
    try {
      name = decodeURIComponent(rawName.replace(/\+/g, "%20"));
    } catch {
    }
    if (name !== BOOTSTRAP_PARAM && name !== HUB_BOOTSTRAP_PARAM) kept.push(part);
  }
  return kept.length > 0 ? `?${kept.join("&")}` : "";
}

function safePathname(pathname: string): string {
  const collapsed = pathname.replace(/\\/g, "/").replace(/^\/+/, "");
  return `/${collapsed}`;
}

function upstreamRequestUrl(payload: SealedEdgePayload, incomingUrl: URL): string {
  const base = payload.u.endsWith("/") ? payload.u.slice(0, -1) : payload.u;
  return `${base}${incomingUrl.pathname}${stripGatewayParams(incomingUrl.search)}`;
}

function sanitizeSetCookie(raw: string): string | null {
  const parts = raw.split(";");
  const first = parts[0]?.trim() ?? "";
  const eq = first.indexOf("=");
  if (eq <= 0) return null;
  const name = first.slice(0, eq).trim().toLowerCase();
  if (RESERVED_SET_COOKIES.has(name)) return null;
  const filteredAttrs = parts
    .slice(1)
    .map((part) => part.trim())
    .filter((part) => part.length > 0 && !/^domain\s*=/i.test(part));
  return [first, ...filteredAttrs].join("; ");
}

function sanitizeResponseCookies(headers: Headers): void {
  const getSetCookie = (headers as Headers & { getSetCookie?: () => string[] }).getSetCookie;
  const rawCookies =
    typeof getSetCookie === "function"
      ? getSetCookie.call(headers)
      : headers.get("set-cookie")
        ? [headers.get("set-cookie")!]
        : [];
  if (rawCookies.length === 0) return;
  headers.delete("set-cookie");
  for (const raw of rawCookies) {
    const sanitized = sanitizeSetCookie(raw);
    if (sanitized) headers.append("set-cookie", sanitized);
  }
}

function numericPort(url: URL): string {
  if (url.port) return url.port;
  if (url.protocol === "https:") return "443";
  if (url.protocol === "http:") return "80";
  return "";
}

function rewrittenLocation(location: string, payload: SealedEdgePayload, upstream: URL): string | null {
  if (!/^[A-Za-z][A-Za-z0-9+.-]*:/.test(location) && !location.startsWith("//")) return null;

  let target: URL;
  try {
    target = new URL(location, upstream);
  } catch {
    return null;
  }

  const hostname = target.hostname.toLowerCase();
  const isLoopbackPort =
    (hostname === "localhost" || hostname === "127.0.0.1" || hostname === "0.0.0.0" || hostname === "::1" || hostname === "[::1]") &&
    numericPort(target) === String(payload.p);
  if (!isLoopbackPort && target.origin !== upstream.origin) return null;
  return `https://${payload.host}${target.pathname}${target.search}${target.hash}`;
}

function rewriteOriginHeader(headers: Headers, upstream: URL, incomingUrl: URL): void {
  const origin = headers.get("origin");
  if (!origin) return;
  try {
    const originUrl = new URL(origin);
    if (originUrl.host.toLowerCase() === incomingUrl.host.toLowerCase()) {
      headers.set("origin", upstream.origin);
    }
  } catch {
  }
}

function prepareUpstreamRequest(request: Request, payload: SealedEdgePayload, upstream: URL, incomingUrl: URL): Request {
  const upstreamRequest = new Request(upstreamRequestUrl(payload, incomingUrl), request);
  const headers = upstreamRequest.headers;

  headers.delete("host");
  for (const [name, value] of Object.entries(payload.h ?? {})) {
    if (name.toLowerCase() !== "cookie") headers.set(name, value);
  }

  const cookie = mergeForwardedCookies(request.headers.get("cookie"), payload.h);
  if (cookie) headers.set("cookie", cookie);
  else headers.delete("cookie");

  headers.set("x-forwarded-host", incomingUrl.host);
  headers.set("x-forwarded-proto", incomingUrl.protocol.replace(":", ""));
  rewriteOriginHeader(headers, upstream, incomingUrl);
  return upstreamRequest;
}

async function proxyRequest(request: Request, payload: SealedEdgePayload, upstream: URL, incomingUrl: URL): Promise<Response> {
  const upstreamRequest = prepareUpstreamRequest(request, payload, upstream, incomingUrl);
  let upstreamResponse: Response;
  try {
    upstreamResponse = await globalThis.fetch(upstreamRequest, { redirect: "manual" });
  } catch {
    return badGateway();
  }

  if (upstreamResponse.status === 101) return upstreamResponse;

  const headers = new Headers(upstreamResponse.headers);
  sanitizeResponseCookies(headers);
  const location = headers.get("location");
  if (location) {
    const rewritten = rewrittenLocation(location, payload, upstream);
    if (rewritten) headers.set("location", rewritten);
  }
  headers.set("x-robots-tag", ROBOTS_TAG);

  return new Response(upstreamResponse.body, {
    status: upstreamResponse.status,
    statusText: upstreamResponse.statusText,
    headers,
  });
}

async function handleRequest(request: Request, env: PreviewEdgeEnv): Promise<Response> {
  const url = new URL(request.url);

  // Safety net for single-domain `*.example.com/*` routes: non-preview hostnames (e.g. hub/control)
  // pass straight through to the origin instead of returning 401.
  if (!PREVIEW_HOST_RE.test(url.hostname)) {
    return globalThis.fetch(request);
  }

  if (request.method === "GET" && url.pathname === "/robots.txt") {
    return withRobotsTag(
      new Response(ROBOTS_TXT, {
        headers: { "content-type": "text/plain; charset=utf-8" },
      }),
    );
  }

  const bootstrapToken = url.searchParams.get(BOOTSTRAP_PARAM);
  if ((request.method === "GET" || request.method === "HEAD") && bootstrapToken !== null) {
    const cleanLocation = `https://${url.host}${safePathname(url.pathname)}${stripGatewayParams(url.search)}`;
    const verified = await unsealAndVerify(bootstrapToken, url, env);
    if (verified) {
      const maxAge = Math.max(1, Math.ceil((verified.payload.exp - Date.now()) / 1000));
      const headers = new Headers({
        location: cleanLocation,
        "set-cookie": `${EDGE_COOKIE}=${bootstrapToken}; Path=/; HttpOnly; Secure; SameSite=None; Max-Age=${maxAge}`,
      });
      return withRobotsTag(new Response(null, { status: 302, headers }));
    }
    // Stale/expired __t3e link: if the browser already holds a valid edge session cookie, strip the param and continue.
    const existingCookie = edgeCookieValue(request.headers.get("cookie"));
    if (existingCookie && (await unsealAndVerify(existingCookie, url, env))) {
      return withRobotsTag(new Response(null, { status: 302, headers: { location: cleanLocation } }));
    }
    return unauthorized();
  }

  const token = edgeCookieValue(request.headers.get("cookie"));
  if (!token) return unauthorized();
  const verified = await unsealAndVerify(token, url, env);
  if (!verified) return unauthorized();
  return proxyRequest(request, verified.payload, verified.upstream, url);
}

export const worker = {
  fetch: handleRequest,
};

export default worker;
