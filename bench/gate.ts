/**
 * The committed-baseline gate (#120's own Interface/Constraints) - one pure function, so a test can
 * feed it a synthetic report with no real benchmark run, and `bench/overhead.ts`'s CLI can feed it a
 * real one. REGRESSION-ONLY by decision: the ticket states a tolerance, never a direction, and
 * failing a run because it got FASTER would fight the ticket's own opening complaint (a stale
 * number nobody trusts) - `checkGate` only ever flags a leg that got slower than the committed
 * `bench/baseline.json`.
 *
 * Gates ABSOLUTE ns/row only, never `.ratio` (post-planning finding): `.ratio` divides by
 * `floorNsPerRow`, and dividing two independently noisy measurements compounds their noise -
 * measured, `pipelineNsPerRow` held to a 5% spread across 5 real consecutive runs while the same
 * runs' `.ratio` spread 14%, past this gate's own 10% tolerance with no code change between runs.
 * `pnpm bench:overhead` failed 2 of those 5 runs before the ratio check was removed. `.ratio` still
 * prints in every report and every doc table - it answers "is dispatching worth it here", which
 * absolute ns/row alone does not - it is simply no longer a gated number.
 *
 * WHICH field is gated splits on whether a leg has a `local` row (#120 follow-up, post-L5): `Pipeline`
 * has none and gates its own `pipelineNsPerRow`, unchanged. `ConcurrentPipeline`/`HttpPipeline`/
 * `ClusterPipeline` gate `local.nsPerRow` instead - their own DISPATCHED `pipelineNsPerRow` crosses a
 * real network/IPC boundary (`HttpPipeline`'s loopback POST, `ClusterPipeline`'s worker IPC), and
 * that leg's own `.ratio` already measures "is dispatching worth it here," never the package's own
 * overhead - gating its absolute ns/row on real machine jitter gates the wrong thing. Found when
 * L5's real ~48% reduction on every `.local()` row narrowed the SAME 20% tolerance's absolute band on
 * these two legs' DISPATCHED rows against unchanged real jitter, raising their flake rate to roughly
 * 1-in-10 with no regression to show for it - `local.nsPerRow` is a pure in-process measurement with
 * none of that noise, and is what this ticket's own Interface actually cares about protecting.
 */

/** One class's own measured (or committed-baseline) row - `local` is present only for the three
 * dispatching classes (#120's own Interface JSON has no `local` field on `Pipeline`). */
export interface LegReport {
  pipelineNsPerRow: number;
  floorNsPerRow: number;
  ratio: number;
  local?: {
    nsPerRow: number;
    dispatchesWhilePinned?: number;
    requestsWhilePinned?: number;
    workerPidsWhilePinned?: number[];
    /** `EventEmitterPipeline` alone (#180): a manually-registered extra Worker on `stage:0`,
     * counted while a `.local()` chain runs - `stageWork()` never runs there, so the composed
     * function never registers either, and this extra Worker is the whole of `listenerCount`. */
    workersWhilePinned?: number;
  };
}

export type LegName =
  | "Pipeline"
  | "ConcurrentPipeline"
  | "HttpPipeline"
  | "ClusterPipeline"
  | "Branch"
  | "EventEmitterPipeline";

export type OverheadReport = Record<LegName, LegReport>;

/**
 * How far a leg's own gated field may drift before it is a regression, PER LEG (#179) - measured
 * across five consecutive `pnpm bench:overhead` runs on an unchanged tree, not chosen:
 *
 * ```text
 * Pipeline             pipelineNsPerRow   min 15.99  max 17.01  spread  6.4%
 * ConcurrentPipeline   local.nsPerRow     min 16.16  max 16.91  spread  4.6%
 * HttpPipeline         local.nsPerRow     min 17.61  max 19.29  spread  9.5%
 * ClusterPipeline      local.nsPerRow     min 18.04  max 21.45  spread 18.9%
 * Branch               pipelineNsPerRow   min 21.04  max 22.58  spread  7.4%   (#180)
 * EventEmitterPipeline local.nsPerRow     min 22.74  max 24.92  spread  9.6%   (#180)
 * ```
 *
 * One tolerance across all four is what the single `ABSOLUTE_TOLERANCE = 0.2` was, and those numbers
 * are why it had to go: `ClusterPipeline`'s own natural spread is 18.9%, so a 20% ceiling sat inside
 * the noise and that leg failed on run-to-run jitter with no regression to show - it is what made an
 * earlier baseline regeneration look like a 25% `ClusterPipeline` regression nothing in the diff
 * could reach. The same 20% is meanwhile three times looser than `ConcurrentPipeline` needs.
 *
 * Each value is roughly double its leg's measured spread, which leaves room for a slower machine
 * while still catching a real regression - the smallest this stack actually made was 41%.
 */
export const LEG_TOLERANCE: Record<LegName, number> = {
  Pipeline: 0.15,
  ConcurrentPipeline: 0.15,
  HttpPipeline: 0.2,
  ClusterPipeline: 0.4,
  Branch: 0.15,
  EventEmitterPipeline: 0.2,
};

/**
 * Builds one `LegReport`, `ratio` always `pipelineNsPerRow / floorNsPerRow` - the ONE place that
 * division happens (code-review finding: it was retyped by hand at 5 call sites - once per leg in
 * `bench/legs/*.ts`, and a 6th time recomputing it under `BENCH_SYNTHETIC_REGRESSION`, with no
 * type error to catch a future site drifting from this file's own stated invariant).
 *
 * `legReport(30, 10)` → `{ pipelineNsPerRow: 30, floorNsPerRow: 10, ratio: 3 }`.
 */
export function legReport(
  pipelineNsPerRow: number,
  floorNsPerRow: number,
  local?: LegReport["local"],
): LegReport {
  return { pipelineNsPerRow, floorNsPerRow, ratio: pipelineNsPerRow / floorNsPerRow, local };
}

export interface GateResult {
  ok: boolean;
  violations: string[];
}

/**
 * `checkGate(report, baseline)` - the gated field (`pipelineNsPerRow` on `Pipeline`, `local.nsPerRow`
 * on a dispatching class - this file's own header) past `baseline`'s own value by more than
 * that leg's own `LEG_TOLERANCE`, WORSE (slower) is a violation; a leg that got faster never is. `.ratio` is
 * read from the report but never gated. A leg the baseline has but `report` does NOT is a violation
 * too - a report that cannot even be compared has failed to prove no regression, the same as one
 * that measured a real one (`code.md`'s "fail loud", not a silent pass for a lookup that came up
 * empty); the same holds for a dispatching class's own `local` row.
 *
 * `checkGate({ Pipeline: { pipelineNsPerRow: 100, floorNsPerRow: 4.2, ratio: 23.8 } }, { Pipeline: {
 * pipelineNsPerRow: 50, floorNsPerRow: 4.2, ratio: 11.9 } })` → one violation: `100` is double `50`,
 * past the 20% absolute tolerance. `checkGate({ ConcurrentPipeline: { pipelineNsPerRow: 900, ...,
 * local: { nsPerRow: 280 } } }, { ConcurrentPipeline: { pipelineNsPerRow: 300, ...,
 * local: { nsPerRow: 280 } } })` → no violation: the DISPATCHED leg tripled, but `local.nsPerRow` -
 * the gated field for this leg - is unchanged.
 */
export function checkGate(
  report: Partial<OverheadReport>,
  baseline: Partial<OverheadReport>,
): GateResult {
  const violations: string[] = [];
  for (const leg of Object.keys(baseline) as LegName[]) {
    const current = report[leg];
    const base = baseline[leg];
    if (!base) continue; // nothing in the baseline to compare this leg against
    if (!current) {
      violations.push(`${leg}: missing from the report - baseline has it, nothing to compare`);
      continue;
    }
    // Which field gates: this file's own header. `base.local` present is what marks a dispatching
    // class here; `Pipeline` has none and keeps its dispatched-leg gate unchanged.
    if (!base.local) {
      pushIfOverCeiling(
        violations,
        leg,
        "pipelineNsPerRow",
        current.pipelineNsPerRow,
        base.pipelineNsPerRow,
      );
      continue;
    }
    if (!current.local) {
      violations.push(
        `${leg}: local is missing from the report - baseline has it, nothing to compare`,
      );
      continue;
    }
    pushIfOverCeiling(
      violations,
      leg,
      "local.nsPerRow",
      current.local.nsPerRow,
      base.local.nsPerRow,
    );
  }
  return { ok: violations.length === 0, violations };
}

/** The one `> ceiling` check both gated fields share (`pipelineNsPerRow` on `Pipeline`, `local.nsPerRow`
 * on every dispatching class) - failing loud on a non-finite reading rather than letting an unguarded
 * `>` silently pass a broken measurement (`median()`'s own docstring names this exact hazard). */
function pushIfOverCeiling(
  violations: string[],
  leg: LegName,
  field: string,
  currentValue: number,
  baseValue: number,
): void {
  if (!Number.isFinite(currentValue)) {
    violations.push(`${leg}: ${field} is ${currentValue} - not a finite measurement`);
    return;
  }
  const tolerance = LEG_TOLERANCE[leg];
  const ceiling = baseValue * (1 + tolerance);
  if (currentValue > ceiling) {
    violations.push(
      `${leg}: ${field} ${currentValue.toFixed(1)} exceeds baseline ${baseValue.toFixed(1)} by ` +
        `more than ${tolerance * 100}% (ceiling ${ceiling.toFixed(1)})`,
    );
  }
}

/** The four dispatching classes `checkLocalParity` compares against `Pipeline` (#180's own
 * Done-when 8, 9) - every class with a `.local()` row. */
export type DispatchingLegName = Exclude<LegName, "Pipeline" | "Branch">;

/**
 * How far a dispatching class's own `local.nsPerRow / Pipeline.pipelineNsPerRow` ratio may read
 * before it is a parity violation - a CEILING on the ratio itself, not a baseline-relative percent
 * like `LEG_TOLERANCE`, because `checkLocalParity` takes no baseline: a pinned region runs
 * identical in-process code on every class (`architecture.md`'s own `.local()` section), so the
 * ratio SHOULD read ~1.0 on every one of them, in the SAME report, with no external baseline
 * needed to say so.
 *
 * Measured across five consecutive `pnpm bench:overhead` runs on an unchanged tree, not chosen:
 *
 * ```text
 * ConcurrentPipeline    min 0.924  max 1.027
 * HttpPipeline          min 1.015  max 1.073
 * ClusterPipeline       min 0.992  max 1.161
 * EventEmitterPipeline  min 1.305  max 1.474
 * ```
 *
 * The residual spread is explained, not removed (#180's own Done-when 9): `Pipeline` is measured
 * FIRST in `bench/overhead.ts`'s own run order, in its most favorable JIT/cache state. A swap probe
 * (measuring `EventEmitterPipeline` FIRST instead of last, `Pipeline` second instead of first) held
 * `EventEmitterPipeline`'s own ABSOLUTE `local.nsPerRow` stable either way (~22 ns/row), while
 * `Pipeline.pipelineNsPerRow` itself moved from 16.89 to 27.09 depending on ITS OWN position - the
 * spread is a measurement-ORDER artefact of what ran immediately before `Pipeline`'s own reading,
 * not a real per-class execution cost `.local()` pays. Each ceiling below is the measured max plus
 * one more spread's worth of headroom (the same "roughly double" spirit `LEG_TOLERANCE`'s own
 * header uses), read directly off the real run order every `pnpm bench:overhead` invocation uses -
 * not adjusted for the artefact, since that IS the shape a real run always measures.
 */
export const LOCAL_PARITY_CEILING: Record<DispatchingLegName, number> = {
  ConcurrentPipeline: 1.15,
  HttpPipeline: 1.15,
  ClusterPipeline: 1.35,
  EventEmitterPipeline: 1.65,
};

/**
 * `checkLocalParity(report)` - every dispatching class's own `local.nsPerRow` against `Pipeline`'s
 * `pipelineNsPerRow` FROM THE SAME REPORT (#180's own Done-when 8), never a committed baseline:
 * `checkGate` already gates each leg's absolute drift against its own history, and this instead
 * asks whether the four classes still agree with EACH OTHER on any one run - `checkGate` alone
 * cannot see four `local.nsPerRow` rows drifting apart from each other while each stays inside its
 * own tolerance against its own baseline. Regression-only, the same reason `checkGate` is: a ratio
 * BELOW its ceiling (even below 1.0 - a pinned region running FASTER than a bare `Pipeline`) is
 * never a violation.
 *
 * `checkLocalParity({ Pipeline: { pipelineNsPerRow: 17 }, ConcurrentPipeline: { local: { nsPerRow:
 * 17.5 }, ... }, ... })` → `{ ok: true, violations: [] }` (`17.5 / 17 = 1.029`, under
 * `ConcurrentPipeline`'s own `1.15` ceiling).
 */
export function checkLocalParity(report: Partial<OverheadReport>): GateResult {
  const violations: string[] = [];
  const pipelineNsPerRow = report.Pipeline?.pipelineNsPerRow;
  if (pipelineNsPerRow === undefined || !Number.isFinite(pipelineNsPerRow)) {
    violations.push(
      "Pipeline: pipelineNsPerRow is missing or not finite - nothing to compare .local() rows against",
    );
    return { ok: false, violations };
  }

  for (const leg of Object.keys(LOCAL_PARITY_CEILING) as DispatchingLegName[]) {
    const local = report[leg]?.local;
    if (!local) {
      violations.push(`${leg}: local is missing from the report - nothing to compare for parity`);
      continue;
    }
    if (!Number.isFinite(local.nsPerRow)) {
      violations.push(`${leg}: local.nsPerRow is ${local.nsPerRow} - not a finite measurement`);
      continue;
    }
    const ratio = local.nsPerRow / pipelineNsPerRow;
    const ceiling = LOCAL_PARITY_CEILING[leg];
    if (ratio > ceiling) {
      violations.push(
        `${leg}: local.nsPerRow (${local.nsPerRow.toFixed(1)}) / Pipeline.pipelineNsPerRow ` +
          `(${pipelineNsPerRow.toFixed(1)}) is ${ratio.toFixed(3)}x, past the ${ceiling}x parity ceiling`,
      );
    }
  }

  return { ok: violations.length === 0, violations };
}
