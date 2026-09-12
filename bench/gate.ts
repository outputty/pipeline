/**
 * The committed-baseline gate (#120's own Interface/Constraints) - one pure function, so a test can
 * feed it a synthetic report with no real benchmark run, and `bench/overhead.ts`'s CLI can feed it a
 * real one. REGRESSION-ONLY by decision: the ticket states a tolerance, never a direction, and
 * failing a run because it got FASTER would fight the ticket's own opening complaint (a stale
 * number nobody trusts) - `checkGate` only ever flags a leg that got slower than the committed
 * `bench/baseline.json`.
 *
 * Gates ABSOLUTE `pipelineNsPerRow` only, never `.ratio` (post-planning finding): `.ratio` divides by
 * `floorNsPerRow`, and dividing two
 * independently noisy measurements compounds their noise - measured, `pipelineNsPerRow` held to a 5%
 * spread across 5 real consecutive runs while the same runs' `.ratio` spread 14%, past this gate's
 * own 10% tolerance with no code change between runs. `pnpm bench:overhead` failed 2 of those 5 runs
 * before the ratio check was removed. `.ratio` still prints in every report and every doc table -
 * it answers "is dispatching worth it here", which absolute `pipelineNsPerRow` alone does not - it
 * is simply no longer a gated number.
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

export interface GateResult {
  ok: boolean;
  violations: string[];
}

/**
 * `checkGate(report, baseline)` - `report[leg].pipelineNsPerRow` past `baseline`'s own value by more
 * than `ABSOLUTE_TOLERANCE`, WORSE (slower ns/row) is a violation; a leg that got faster never is.
 * `.ratio` is read from the report but never gated (see this file's own header). A leg the baseline
 * has but `report` does NOT is a violation too - a report
 * that cannot even be compared has failed to prove no regression, the same as one that measured a
 * real one (`code.md`'s "fail loud", not a silent pass for a lookup that came up empty).
 *
 * `checkGate({ Pipeline: { pipelineNsPerRow: 100, floorNsPerRow: 4.2, ratio: 23.8 } }, { Pipeline: {
 * pipelineNsPerRow: 50, floorNsPerRow: 4.2, ratio: 11.9 } })` → one violation: `100` is double `50`,
 * past the 20% absolute tolerance.
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
    if (!Number.isFinite(current.pipelineNsPerRow)) {
      // Every comparison against NaN is false, so an unguarded `>` below would silently PASS a
      // broken measurement (the exact hazard `median()`'s own docstring names) - a non-finite
      // reading has failed to prove no regression, same as a missing leg above.
      violations.push(
        `${leg}: pipelineNsPerRow is ${current.pipelineNsPerRow} - not a finite measurement`,
      );
      continue;
    }
    const absoluteCeiling = base.pipelineNsPerRow * (1 + ABSOLUTE_TOLERANCE);
    if (current.pipelineNsPerRow > absoluteCeiling) {
      violations.push(
        `${leg}: pipelineNsPerRow ${current.pipelineNsPerRow.toFixed(1)} exceeds baseline ` +
          `${base.pipelineNsPerRow.toFixed(1)} by more than ${ABSOLUTE_TOLERANCE * 100}% ` +
          `(ceiling ${absoluteCeiling.toFixed(1)})`,
      );
    }
  }
  return { ok: violations.length === 0, violations };
}
