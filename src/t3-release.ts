/**
 * Resolves `t3.version: latest` + `t3.channel` to an exact T3 release on the Sentinel host.
 *
 * Providers without snapshots install T3 on every chat VM. Left to itself, the installer
 * (`https://t3.codes/install.sh`) finds nightly and preview releases through the unauthenticated
 * GitHub API, whose 60 requests/hour per IP are often spent on shared cloud egress IPs (Celesto).
 * With an exact `T3CODE_VERSION` the installer skips the API and downloads straight from
 * `github.com/.../releases/download`, so Sentinel does the lookup once (with `GITHUB_TOKEN` when
 * set), caches it, and hands the VM the exact version.
 */

export const T3_REPO = "pingdotgg/t3code";
const DEFAULT_TTL_MS = 15 * 60_000;
const DEFAULT_TIMEOUT_MS = 15_000;
/** Same page cap as the installer. */
const MAX_PAGES = 10;

type FetchLike = (input: string, init: RequestInit) => Promise<Response>;

export interface T3ReleaseResolverOptions {
  fetch?: FetchLike;
  now?: () => number;
  /** How long a resolved version is reused (default 15 min). */
  ttlMs?: number;
  /** Upper bound for one lookup, all pages included (default 15 s). */
  timeoutMs?: number;
  /** Read at lookup time; `GITHUB_TOKEN` authenticates the API calls. */
  env?: Record<string, string | undefined>;
  log?: (message: string) => void;
}

/**
 * The installer's tag pattern for a channel (install.sh: `v\([0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*\)`
 * for stable, `v\([0-9][^"]*-<channel>\.[0-9]*\.[0-9]*\)` for nightly and preview), anchored to the
 * whole tag. Group 1 is the version without the leading `v`.
 */
export function t3TagPattern(channel: string): RegExp {
  if (channel === "stable") return /^v([0-9]+\.[0-9]+\.[0-9]+)$/;
  if (channel === "nightly" || channel === "preview") {
    return new RegExp(`^v([0-9][^"]*-${channel}\\.[0-9]*\\.[0-9]*)$`);
  }
  throw new Error(`Unknown T3 channel "${channel}" (expected stable, nightly or preview)`);
}

export class T3ReleaseResolver {
  private readonly fetchImpl: FetchLike;
  private readonly now: () => number;
  private readonly ttlMs: number;
  private readonly timeoutMs: number;
  private readonly env: Record<string, string | undefined>;
  private readonly log: (message: string) => void;
  private readonly cache = new Map<string, { version: string; at: number }>();
  private readonly inFlight = new Map<string, Promise<string>>();

  constructor(opts: T3ReleaseResolverOptions = {}) {
    this.fetchImpl = opts.fetch ?? ((input, init) => fetch(input, init));
    this.now = opts.now ?? Date.now;
    this.ttlMs = opts.ttlMs ?? DEFAULT_TTL_MS;
    this.timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    this.env = opts.env ?? process.env;
    this.log = opts.log ?? ((m) => console.warn(m));
  }

  /**
   * Exact version for `version`/`channel`. Anything but `latest` passes through unchanged. A failed
   * lookup is logged and returns `latest`, so the VM falls back to the installer's own lookup.
   */
  async resolve(version: string, channel: string): Promise<string> {
    if (version !== "latest") return version;
    const cached = this.cache.get(channel);
    if (cached && this.now() - cached.at < this.ttlMs) return cached.version;
    let pending = this.inFlight.get(channel);
    if (!pending) {
      pending = this.lookup(channel)
        .then((exact) => {
          this.cache.set(channel, { version: exact, at: this.now() });
          return exact;
        })
        .catch((err: unknown) => {
          const reason = err instanceof Error ? err.message : String(err);
          this.log(`[t3-release] could not resolve the latest ${channel} T3 release (${reason}); VMs fall back to the installer's lookup`);
          return "latest";
        })
        .finally(() => {
          this.inFlight.delete(channel);
        });
      this.inFlight.set(channel, pending);
    }
    return pending;
  }

  private async lookup(channel: string): Promise<string> {
    const pattern = t3TagPattern(channel);
    const signal = AbortSignal.timeout(this.timeoutMs);
    if (channel === "stable") {
      // Same source as the installer: release.yml marks only plain stable releases latest, and this
      // github.com page is outside the API quota
      const data = await this.getJson(`https://github.com/${T3_REPO}/releases/latest`, { Accept: "application/json" }, signal);
      const tag = (data as { tag_name?: unknown } | null)?.tag_name;
      const match = typeof tag === "string" ? pattern.exec(tag) : null;
      if (!match) throw new Error(`latest release tag ${JSON.stringify(tag)} is not a stable version`);
      return match[1];
    }
    const headers: Record<string, string> = {
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
    };
    const token = this.env.GITHUB_TOKEN?.trim();
    if (token) headers.Authorization = `Bearer ${token}`;
    for (let page = 1; page <= MAX_PAGES; page++) {
      const data = await this.getJson(
        `https://api.github.com/repos/${T3_REPO}/releases?per_page=100&page=${page}`,
        headers,
        signal,
      );
      if (!Array.isArray(data)) throw new Error("GitHub returned an unexpected releases payload");
      if (data.length === 0) break;
      // First match in API order, like the installer. Drafts (visible with a token) have no assets.
      for (const release of data as Array<{ tag_name?: unknown; draft?: unknown }>) {
        if (release?.draft === true || typeof release?.tag_name !== "string") continue;
        const match = pattern.exec(release.tag_name);
        if (match) return match[1];
      }
    }
    throw new Error(`no ${channel} release found`);
  }

  private async getJson(url: string, headers: Record<string, string>, signal: AbortSignal): Promise<unknown> {
    let res: Response;
    try {
      res = await this.fetchImpl(url, { headers: { "User-Agent": "t3-boat-sentinel", ...headers }, signal });
    } catch (err) {
      const reason = err instanceof Error ? (err.name === "TimeoutError" ? "timed out" : err.message) : String(err);
      throw new Error(`GET ${url} failed: ${reason}`);
    }
    if (!res.ok) {
      const remaining = res.headers.get("x-ratelimit-remaining");
      throw new Error(`GET ${url} returned HTTP ${res.status}${remaining === "0" ? " (rate limited)" : ""}`);
    }
    return res.json();
  }
}
