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
  };
}

export type LegName = "Pipeline" | "ConcurrentPipeline" | "HttpPipeline" | "ClusterPipeline";

export type OverheadReport = Record<LegName, LegReport>;

export const ABSOLUTE_TOLERANCE = 0.2;

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
 * `ABSOLUTE_TOLERANCE`, WORSE (slower) is a violation; a leg that got faster never is. `.ratio` is
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
  const ceiling = baseValue * (1 + ABSOLUTE_TOLERANCE);
  if (currentValue > ceiling) {
    violations.push(
      `${leg}: ${field} ${currentValue.toFixed(1)} exceeds baseline ${baseValue.toFixed(1)} by ` +
        `more than ${ABSOLUTE_TOLERANCE * 100}% (ceiling ${ceiling.toFixed(1)})`,
    );
  }
}
