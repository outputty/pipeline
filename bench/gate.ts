/**
 * The committed-baseline gate (#120's own Interface/Constraints) - one pure function, so a test can
 * feed it a synthetic report with no real benchmark run, and `bench/overhead.ts`'s CLI can feed it a
 * real one. REGRESSION-ONLY by decision: the ticket states a tolerance, never a direction, and
 * failing a run because it got FASTER would fight the ticket's own opening complaint (a stale
 * number nobody trusts) - `checkGate` only ever flags a leg that got slower or a worse ratio than
 * the committed `bench/baseline.json`.
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
export const RATIO_TOLERANCE = 0.1;

export interface GateResult {
  ok: boolean;
  violations: string[];
}

/**
 * `checkGate(report, baseline)` - `report[leg].pipelineNsPerRow` past `baseline`'s own value by more
 * than `ABSOLUTE_TOLERANCE`, or `.ratio` past it by more than `RATIO_TOLERANCE`, either direction
 * WORSE (slower ns/row, or a wider ratio) is a violation; a leg that got faster or narrowed its
 * ratio never is. A leg the baseline has but `report` does NOT is a violation too - a report
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
    const absoluteCeiling = base.pipelineNsPerRow * (1 + ABSOLUTE_TOLERANCE);
    if (current.pipelineNsPerRow > absoluteCeiling) {
      violations.push(
        `${leg}: pipelineNsPerRow ${current.pipelineNsPerRow.toFixed(1)} exceeds baseline ` +
          `${base.pipelineNsPerRow.toFixed(1)} by more than ${ABSOLUTE_TOLERANCE * 100}% ` +
          `(ceiling ${absoluteCeiling.toFixed(1)})`,
      );
    }
    const ratioCeiling = base.ratio * (1 + RATIO_TOLERANCE);
    if (current.ratio > ratioCeiling) {
      violations.push(
        `${leg}: ratio ${current.ratio.toFixed(2)} exceeds baseline ${base.ratio.toFixed(2)} by ` +
          `more than ${RATIO_TOLERANCE * 100}% (ceiling ${ratioCeiling.toFixed(2)})`,
      );
    }
  }
  return { ok: violations.length === 0, violations };
}
