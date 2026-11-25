/**
 * A Boat project's refs (`vcs.listRefs`, `VcsListRefsResult` in the T3 contracts) as the Hub keeps
 * them for the ref picker of a project with no running VM.
 */

type Ref = Record<string, unknown> & { name?: unknown; isDefault?: unknown; isRemote?: unknown };

/** Whether a stored value still has the shape the app's `vcs.listRefs` schema needs. */
export function isVcsRefsResult(value: unknown): value is { refs: Ref[] } {
  const v = value as Record<string, unknown> | null;
  return (
    !!v &&
    typeof v === "object" &&
    Array.isArray(v.refs) &&
    typeof v.isRepo === "boolean" &&
    typeof v.hasPrimaryRemote === "boolean" &&
    typeof v.totalCount === "number" &&
    "nextCursor" in v
  );
}

/** The default branch a `vcs.listRefs` result names (its local default ref), if any. */
export function defaultBranchOfRefs(value: unknown): string | null {
  const refs = (value as { refs?: unknown } | null)?.refs;
  if (!Array.isArray(refs)) return null;
  const local = (refs as Ref[]).find((r) => r && typeof r === "object" && r.isDefault === true && !r.isRemote);
  return typeof local?.name === "string" && local.name ? local.name : null;
}

/**
 * A VM's `vcs.listRefs` answer as a template for every chat of the project: the remote refs and
 * the local default branch, checked out nowhere. A chat's own branches, worktrees and current
 * branch exist only in that chat's VM.
 */
export function templateVcsRefs(value: unknown): Record<string, unknown> | null {
  if (!isVcsRefsResult(value)) return null;
  const refs = value.refs
    .filter((r) => r && typeof r === "object" && (r.isRemote === true || r.isDefault === true))
    .map((r) => ({ ...r, current: r.isDefault === true && !r.isRemote, worktreePath: null }));
  return { ...(value as Record<string, unknown>), refs, totalCount: refs.length, nextCursor: null };
}

/**
 * `vcs.listRefs` for a project no VM has listed yet: its default branch and that branch on
 * `origin` when the bake recorded it, else no refs (never a guessed `main`).
 */
export function fallbackVcsRefs(defaultBranch: string | null, hasRemote: boolean): Record<string, unknown> {
  const refs = defaultBranch
    ? [
        { name: defaultBranch, current: true, isDefault: true, isRemote: false, worktreePath: null },
        ...(hasRemote
          ? [{ name: `origin/${defaultBranch}`, current: false, isDefault: true, isRemote: true, remoteName: "origin", worktreePath: null }]
          : []),
      ]
    : [];
  return { isRepo: true, hasPrimaryRemote: hasRemote, nextCursor: null, totalCount: refs.length, refs };
}
