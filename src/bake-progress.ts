/**
 * Steps of a snapshot bake (`LifecycleManager.bake`) and how a T3 clone progress bar shows them.
 *
 * T3 decodes clone progress against git's stages (`ProjectCloneSnapshot.stage`), so each bake step maps to
 * the closest git stage, and the honest step name goes in `detail`. `percent` is weighted by how long each
 * step typically takes; within a step it creeps toward the step's ceiling as time passes, never reaching it,
 * so the bar keeps moving on a slow step without ever claiming the next one has started.
 */

export type BakeStep = "create" | "upload" | "install" | "verify" | "stop" | "save";

export type CloneStage = "connecting" | "counting" | "receiving" | "resolving" | "checkout";

interface BakeStepSpec {
  /** Shown to the user in T3 and written to the `[bake]` log. */
  label: string;
  stage: CloneStage;
  /** Percent when the step starts; the next step's `from` is this step's ceiling. */
  from: number;
  /** Typical duration in ms: how fast `percent` approaches the ceiling. */
  typicalMs: number;
}

export const BAKE_STEPS: Record<BakeStep, BakeStepSpec> = {
  create: { label: "Creating a temporary Boat VM", stage: "connecting", from: 0, typicalMs: 30_000 },
  upload: { label: "Uploading setup scripts", stage: "connecting", from: 7, typicalMs: 15_000 },
  install: { label: "Cloning and installing dependencies on a temporary Boat VM", stage: "receiving", from: 10, typicalMs: 150_000 },
  verify: { label: "Verifying T3 and providers", stage: "resolving", from: 42, typicalMs: 150_000 },
  stop: { label: "Stopping the temporary VM", stage: "checkout", from: 75, typicalMs: 30_000 },
  save: { label: "Saving the Boat snapshot", stage: "checkout", from: 81, typicalMs: 90_000 },
};

const STEP_ORDER: BakeStep[] = ["create", "upload", "install", "verify", "stop", "save"];
/** The last step's ceiling: 100 is reserved for "done". */
const LAST_CEILING = 99;

export const PROJECT_CLONE_DETAIL_MAX = 200;

function ceilingOf(step: BakeStep): number {
  const next = STEP_ORDER[STEP_ORDER.indexOf(step) + 1];
  return next ? BAKE_STEPS[next].from : LAST_CEILING;
}

/** "45s", "1m 20s", "1h 2m". */
export function formatElapsed(ms: number): string {
  const total = Math.max(0, Math.floor(ms / 1000));
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const s = total % 60;
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

/** Stage, integer percent and detail for a bake that has been on `step` for `stepElapsedMs`. */
export function bakeProgress(
  step: BakeStep,
  stepElapsedMs: number,
): { stage: CloneStage; percent: number; detail: string } {
  const spec = BAKE_STEPS[step];
  const ceiling = ceilingOf(step);
  const span = ceiling - spec.from;
  const fraction = 1 - Math.exp(-Math.max(0, stepElapsedMs) / spec.typicalMs);
  // Strictly below the ceiling: the next step starts at `ceiling`, so the bar never runs ahead of the bake
  const percent = Math.min(ceiling - 1, spec.from + Math.floor(span * fraction));
  const detail = `${spec.label} · ${formatElapsed(stepElapsedMs)}`.slice(0, PROJECT_CLONE_DETAIL_MAX);
  return { stage: spec.stage, percent: Math.max(spec.from, percent), detail };
}
