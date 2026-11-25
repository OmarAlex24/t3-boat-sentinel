/**
 * Moves a FRESH chat VM's project checkout to the remote's current default branch before T3 starts,
 * so a chat never works on code as old as its snapshot. Only for a VM just created from a snapshot,
 * which holds no user work: the bake may have left the tree dirty (lockfiles, generated files) or
 * on an older commit, and both are discarded. Ignored files (node_modules, build caches) are kept.
 *
 * The script prints one JSON line with SHAs, the branch name and a count, never a path, a remote
 * URL or git's own output (which can carry a token in a URL).
 */

export type RepoSyncSkipReason = "no-workdir" | "not-a-repository" | "not-repo-root" | "no-origin";
export type RepoSyncFailReason = "fetch-failed" | "no-default-branch" | "out-of-time" | "reset-failed" | "exec-failed" | "timeout" | "no-result";

export type RepoSyncResult =
  | { status: "synced"; from: string | null; to: string; branch: string; discarded: number }
  | { status: "skipped"; reason: RepoSyncSkipReason }
  | { status: "failed"; reason: RepoSyncFailReason };

const SKIP_REASONS = new Set<string>(["no-workdir", "not-a-repository", "not-repo-root", "no-origin"]);
const FAIL_REASONS = new Set<string>(["fetch-failed", "no-default-branch", "out-of-time", "reset-failed"]);
const SHA_RE = /^[0-9a-f]{7,64}$/;
const BRANCH_RE = /^[A-Za-z0-9._/-]{1,200}$/;

function shellQuote(s: string): string {
  return `'${s.replace(/'/g, `'\\''`)}'`;
}

/** Marker in the script, so tests (and logs of exec calls) can tell it apart. */
export const REPO_SYNC_MARKER = "t3-sentinel-repo-sync";

/**
 * @param workdir the project checkout (must be the repo root, or nothing happens)
 * @param budgetSecs no step that changes the tree starts after this many seconds: if the Hub gave up
 *   waiting, a late fetch must not reset files while the next boot step uses them
 * @param keepPaths absolute paths sentinel placed inside the workdir; `git clean` keeps them
 */
export function buildRepoSyncScript(workdir: string, budgetSecs: number, keepPaths: string[] = []): string {
  const budget = Math.max(1, Math.floor(budgetSecs));
  const excludes = keepPaths
    .filter((p) => p.startsWith(`${workdir}/`) && p.length > workdir.length + 1)
    .map((p) => `-e ${shellQuote(`/${p.slice(workdir.length + 1)}`)}`)
    .join(" ");
  return `# ${REPO_SYNC_MARKER}
set -u
# Never prompt (no TTY here); credentials come from the VM's own git config, as for T3's Pull
export GIT_TERMINAL_PROMPT=0 GCM_INTERACTIVE=never
BUDGET=${budget}
from="" to="" branch="" discarded=0
out() { printf '{"status":"%s","reason":"%s","from":"%s","to":"%s","branch":"%s","discarded":%s}\\n' "$1" "$2" "$from" "$to" "$branch" "$discarded"; exit 0; }
left() { r=$((BUDGET - SECONDS)); [ "$r" -lt 1 ] && r=1; echo "$r"; }
bounded() { s=$(left); if command -v timeout >/dev/null 2>&1; then timeout "$s" "$@"; else "$@"; fi; }
cd ${shellQuote(workdir)} 2>/dev/null || out skipped no-workdir
top=$(git rev-parse --show-toplevel 2>/dev/null) || out skipped not-a-repository
[ "$top" = "$(pwd -P)" ] || out skipped not-repo-root
git remote get-url origin >/dev/null 2>&1 || out skipped no-origin
from=$(git rev-parse -q --verify HEAD 2>/dev/null || true)
bounded git fetch --quiet --prune --no-tags origin >/dev/null 2>&1 || out failed fetch-failed
def=$(git symbolic-ref -q --short refs/remotes/origin/HEAD 2>/dev/null || true)
if [ -z "$def" ]; then
  bounded git remote set-head origin --auto >/dev/null 2>&1 || true
  def=$(git symbolic-ref -q --short refs/remotes/origin/HEAD 2>/dev/null || true)
fi
[ -n "$def" ] || out failed no-default-branch
branch=$(printf '%s' "\${def#origin/}" | tr -cd 'A-Za-z0-9._/-')
[ "$SECONDS" -lt "$BUDGET" ] || out failed out-of-time
changed=$(git status --porcelain --untracked-files=no 2>/dev/null | wc -l | tr -d ' ')
stray=$(git clean -n -d ${excludes} 2>/dev/null | wc -l | tr -d ' ')
discarded=$((changed + stray))
git checkout -q -f -B "$branch" "$def" >/dev/null 2>&1 || out failed reset-failed
git reset -q --hard "$def" >/dev/null 2>&1 || out failed reset-failed
git clean -q -f -d ${excludes} >/dev/null 2>&1 || out failed reset-failed
to=$(git rev-parse HEAD 2>/dev/null)
out synced ""
`;
}

/** Reads the script's last JSON line; anything unexpected is a failure, never a thrown error. */
export function parseRepoSyncOutput(stdout: string): RepoSyncResult {
  const line = stdout
    .split(/\r?\n/)
    .reverse()
    .find((l) => l.trimStart().startsWith("{"));
  if (!line) return { status: "failed", reason: "no-result" };
  let raw: Record<string, unknown>;
  try {
    raw = JSON.parse(line) as Record<string, unknown>;
  } catch {
    return { status: "failed", reason: "no-result" };
  }
  const reason = String(raw.reason ?? "");
  if (raw.status === "skipped" && SKIP_REASONS.has(reason)) return { status: "skipped", reason: reason as RepoSyncSkipReason };
  if (raw.status === "failed" && FAIL_REASONS.has(reason)) return { status: "failed", reason: reason as RepoSyncFailReason };
  if (raw.status === "synced" && typeof raw.to === "string" && SHA_RE.test(raw.to) && typeof raw.branch === "string" && BRANCH_RE.test(raw.branch)) {
    const from = typeof raw.from === "string" && SHA_RE.test(raw.from) ? raw.from : null;
    const discarded = Number(raw.discarded);
    return { status: "synced", from, to: raw.to, branch: raw.branch, discarded: Number.isFinite(discarded) && discarded >= 0 ? discarded : 0 };
  }
  return { status: "failed", reason: "no-result" };
}

/** One log line, safe to print: SHAs, branch, counts and fixed reasons only. */
export function describeRepoSync(project: string, sandboxId: string, r: RepoSyncResult): string {
  if (r.status === "synced") {
    const from = r.from ? r.from.slice(0, 7) : "none";
    const moved = from === r.to.slice(0, 7) ? `already at ${r.to.slice(0, 7)}` : `${from} -> ${r.to.slice(0, 7)}`;
    return `[sync] ${project} on ${sandboxId}: origin/${r.branch} ${moved}, ${r.discarded} file(s) from the snapshot discarded`;
  }
  if (r.status === "skipped") return `[sync] ${project} on ${sandboxId}: skipped (${r.reason})`;
  return `[sync] ${project} on ${sandboxId}: failed (${r.reason}); this chat starts on the snapshot's code`;
}
