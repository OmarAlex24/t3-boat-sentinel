/**
 * Pure overload evaluator mirrored by `scripts/watchdog.mjs` (which runs inside VMs and cannot import
 * this file). Keeping the decision here testable while the VM-side script stays self-contained.
 */
export interface WatchdogOverloadConfig {
  minAvailableMemPct?: number;
  minLoadPerCpu?: number;
  sustainSecs?: number;
  onOomKill?: boolean;
}

export interface WatchdogOverloadInput {
  now: number;
  cpus: number;
  memTotalKb: number | null;
  memAvailableKb: number | null;
  load1: number | null;
  oomKill: number | null;
  baselineOomKill: number | null;
  shellFailingSince: number | null;
  overloadedSince: number | null;
}

export interface WatchdogOverloadEvaluation {
  baselineOomKill: number | null;
  overloadedSince: number | null;
  memAvailPct: number | null;
  memAvailMb: number | null;
  pressure: boolean;
  trigger: {
    reason: "oom-kill" | "shell-unresponsive-low-mem" | "sustained-pressure";
    detail: Record<string, unknown>;
  } | null;
}

const DEFAULT_MIN_AVAILABLE_MEM_PCT = 15;
const DEFAULT_MIN_LOAD_PER_CPU = 2.0;
const DEFAULT_SUSTAIN_SECS = 180;

export function evaluateWatchdogOverload(
  config: WatchdogOverloadConfig,
  input: WatchdogOverloadInput,
): WatchdogOverloadEvaluation {
  const minAvailableMemPct = config.minAvailableMemPct ?? DEFAULT_MIN_AVAILABLE_MEM_PCT;
  const minLoadPerCpu = config.minLoadPerCpu ?? DEFAULT_MIN_LOAD_PER_CPU;
  const sustainSecs = config.sustainSecs ?? DEFAULT_SUSTAIN_SECS;
  const onOomKill = config.onOomKill !== false;

  const memAvailPct =
    input.memTotalKb !== null && input.memTotalKb > 0 && input.memAvailableKb !== null
      ? (input.memAvailableKb / input.memTotalKb) * 100
      : null;
  const memAvailMb = input.memAvailableKb !== null ? input.memAvailableKb / 1024 : null;

  let baselineOomKill = input.baselineOomKill;
  if (baselineOomKill === null && input.oomKill !== null) {
    // The first readable value is the baseline: OOM kills that predate this watchdog never trigger.
    baselineOomKill = input.oomKill;
  }
  const oomKillsDelta = input.oomKill !== null && baselineOomKill !== null ? input.oomKill - baselineOomKill : 0;
  if (onOomKill && oomKillsDelta > 0) {
    const detail = {
      reason: "oom-kill",
      oomKillsDelta,
      memAvailPct,
      memAvailMb,
      load1: input.load1,
      cpus: input.cpus,
    };
    return {
      baselineOomKill,
      overloadedSince: input.overloadedSince,
      memAvailPct,
      memAvailMb,
      pressure: false,
      trigger: { reason: "oom-kill", detail },
    };
  }

  const lowMemory = memAvailPct !== null && memAvailPct < minAvailableMemPct;
  const saturatedLoad = input.load1 !== null && input.load1 >= minLoadPerCpu * input.cpus;
  const pressure = lowMemory && (saturatedLoad || input.shellFailingSince !== null);
  if (!pressure) {
    return { baselineOomKill, overloadedSince: null, memAvailPct, memAvailMb, pressure, trigger: null };
  }

  const overloadedSince = input.overloadedSince ?? input.now;
  const sustainedMs = input.now - overloadedSince;
  if (sustainedMs < sustainSecs * 1000) {
    return { baselineOomKill, overloadedSince, memAvailPct, memAvailMb, pressure, trigger: null };
  }

  const reason = input.shellFailingSince !== null ? "shell-unresponsive-low-mem" : "sustained-pressure";
  const detail = {
    reason,
    sustainedSecs: Math.floor(sustainedMs / 1000),
    memAvailPct,
    memAvailMb,
    load1: input.load1,
    cpus: input.cpus,
  };
  return {
    baselineOomKill,
    overloadedSince,
    memAvailPct,
    memAvailMb,
    pressure,
    trigger: { reason, detail },
  };
}
