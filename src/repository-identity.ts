/**
 * A project's `repositoryIdentity` (`RepositoryIdentity` in the T3 contracts), derived from its
 * configured repo URL the way a T3 server derives it from the checkout's git remote
 * (T3 Code 0.1.13-nightly.20261125.5245: `normalizeGitRemoteUrl`, `parseGitRemoteRepository`,
 * `repositoryPathFromRemoteUrl`, `withoutRemoteUrlCredentials` and `buildRepositoryIdentity` in
 * `RepositoryIdentityResolver`).
 *
 * The app offers "Run on <environment>" for a local project when another environment reports a
 * project whose `repositoryIdentity.canonicalKey` matches (compared lower case). The key is
 * `host/owner/name`, lower case, without `.git`, the same for an SSH and an HTTPS remote.
 *
 * Not ported: T3's host-specific key spellings for Azure DevOps SSH, Bitbucket's SSH host and
 * TanStack Source. Those remotes get the generic key.
 */

const stripGitSuffix = (path: string) => path.replace(/\.git$/i, "");

function canonicalPathSegment(segment: string): string {
  try {
    return encodeURIComponent(decodeURIComponent(segment).toLowerCase()).toLowerCase();
  } catch {
    return segment.toLowerCase();
  }
}

/** A repository path in one lower-case spelling (`a%20(b)`, `a%20%28b%29` and `a (b)` agree). */
export function canonicalRepositoryPath(path: string): string {
  return path.split("/").map(canonicalPathSegment).join("/");
}

function lowerCaseRemote(value: string, dropSuffix: (path: string) => string = stripGitSuffix): string {
  return dropSuffix(value.trim().replace(/\/+$/g, "")).toLowerCase();
}

/** The host and repository path a remote names (lower case, no port), or null for one that names no host. */
export function parseGitRemoteRepository(value: string, options?: { keepGitSuffix?: boolean }): { host: string; path: string } | null {
  const dropSuffix = options?.keepGitSuffix ? (path: string) => path : stripGitSuffix;
  const normalized = lowerCaseRemote(value, dropSuffix);
  if (/^(?:ssh|https?|git):\/\//i.test(normalized)) {
    try {
      const url = new URL(value.trim().toLowerCase());
      const segments = dropSuffix(url.pathname.replace(/\/+$/, ""))
        .split("/")
        .filter((segment) => segment.length > 0);
      if (url.hostname && segments.length > 1) return { host: url.hostname, path: canonicalRepositoryPath(segments.join("/")) };
    } catch {
      return null;
    }
  }
  const scp = /^[a-z]:/i.test(normalized)
    ? null
    : /^(?:[a-zA-Z0-9._-]+@)?(\[[0-9a-f:.]+\]|[^@:/\\\s[\]]+):\/?([^/\s]+(?:\/[^/\s]+)+)$/i.exec(normalized);
  const scpHost = scp?.[1];
  const scpPath = scp?.[2];
  if (!scpHost || !scpPath) return null;
  const path = canonicalRepositoryPath(scpPath);
  if (!scpHost.startsWith("[")) return { host: scpHost, path };
  try {
    return { host: new URL(`ssh://${scpHost}`).hostname, path };
  } catch {
    return null;
  }
}

/** `host/path` for a remote that names a host, otherwise the remote itself, lower case. */
export function normalizeGitRemoteUrl(value: string): string {
  const repository = parseGitRemoteRepository(value);
  if (repository !== null) return `${repository.host}/${repository.path}`;
  const remote = lowerCaseRemote(value);
  const url = /^([a-z][a-z0-9+.-]*):\/\/(?:([^/?#:@]*)(?::[^/?#]*)?@)?([^?#]*)/.exec(remote);
  if (url === null) return remote;
  const [, scheme = "", user = "", rest = ""] = url;
  return lowerCaseRemote(`${scheme}://${scheme === "ssh" && user ? `${user}@` : ""}${rest}`);
}

/** The repository path as the remote spells it (`Owner/Name`), without `.git`. */
export function repositoryPathFromRemoteUrl(url: string | null | undefined): string | null {
  const remote = url?.trim() ?? "";
  const trimmed = /^(?:ssh|https?|git):\/\//iu.test(remote) ? (remote.split(/[?#]/u, 1)[0] ?? "") : remote;
  if (trimmed.length === 0) return null;
  const path =
    /^(?:[^@/\s]+@[^:/\s]+:|[^@:/\\\s]{2,}:(?!:)|(?:ssh|https?|git):\/\/[^/]+\/)((?:[^/\s]+\/)+[^/\s]+?)(?:\.git)?\/?$/iu
      .exec(trimmed)?.[1]
      ?.trim() ?? "";
  return path.length > 0 ? path : null;
}

/** An HTTP(S) remote without userinfo or query (a token is not part of the address). SSH keeps its user. */
export function withoutRemoteUrlCredentials(url: string): string {
  if (!/^https?:\/\//i.test(url)) return url;
  return url.replace(/^(https?:\/\/)[^/?#]*@/i, "$1").replace(/\?[^#]*/, "");
}

function repositoryPathOf(canonicalKey: string, remoteUrl: string): string {
  const canonicalPath = canonicalKey.split("/").slice(1).join("/");
  const displayPath = repositoryPathFromRemoteUrl(remoteUrl);
  if (displayPath === null) return canonicalPath;
  return (
    [displayPath, `${displayPath}.git`].find((path) => canonicalRepositoryPath(path) === canonicalRepositoryPath(canonicalPath)) ??
    canonicalPath
  );
}

const PROVIDER_BY_HOST: Record<string, string> = {
  "github.com": "github",
  "gitlab.com": "gitlab",
  "bitbucket.org": "bitbucket",
  "dev.azure.com": "azure-devops",
  "ssh.dev.azure.com": "azure-devops",
};

/** T3's `recognizeRemote` for the public hosts; any other host reads as `unknown`, as in T3. */
function providerKindOf(remoteUrl: string): string | null {
  const host = parseGitRemoteRepository(remoteUrl)?.host;
  if (!host) return null;
  return PROVIDER_BY_HOST[host] ?? "unknown";
}

/**
 * The identity a T3 server reports for a checkout at `rootPath` whose `origin` is `repoUrl`.
 * Null for a repo that names no host (a local path).
 */
export function repositoryIdentityFor(repoUrl: string, rootPath: string, remoteName = "origin"): Record<string, unknown> | null {
  const remoteUrl = withoutRemoteUrlCredentials(repoUrl.trim());
  if (!remoteUrl || parseGitRemoteRepository(remoteUrl) === null) return null;
  const canonicalKey = normalizeGitRemoteUrl(remoteUrl);
  const repositoryPath = repositoryPathOf(canonicalKey, remoteUrl);
  const segments = repositoryPath.split("/").filter((segment) => segment.length > 0);
  const owner = segments[0];
  const name = segments.at(-1);
  const provider = providerKindOf(remoteUrl);
  return {
    canonicalKey,
    locator: { source: "git-remote", remoteName, remoteUrl },
    ...(rootPath ? { rootPath } : {}),
    ...(repositoryPath ? { displayName: repositoryPath } : {}),
    ...(provider ? { provider } : {}),
    ...(owner ? { owner } : {}),
    ...(name ? { name } : {}),
  };
}
