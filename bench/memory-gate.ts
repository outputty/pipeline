/**
 * The committed-baseline gate for `bench/memory.ts`, mirroring `bench/gate.ts`'s own shape (#179):
 * one pure function, so a test can feed it a synthetic report with no real run, and the CLI can feed
 * it a real one. REGRESSION-ONLY, for the same reason `checkGate` is - failing a run for getting
 * CHEAPER would fight the point of measuring at all.
 *
 * Unlike `bench/gate.ts` this gates SEVERAL axes, because they are not equally stable and one
 * tolerance across all of them would be either useless on the steady ones or a flake machine on the
 * noisy ones. Each tolerance below is a MEASURED spread, not a guess - see `MEMORY_TOLERANCE`.
 *
 * `retainedMb` is gated as an ABSOLUTE ceiling rather than a ratio: it reads 0.00-0.02 MB on a
 * healthy chain, and a percentage tolerance over a near-zero baseline flags noise as a leak while
 * letting a real 50 MB leak through if the baseline happened to read 0.02.
 *
 * ⚠ `nsPerRow` is REPORTED and never gated here, which is the opposite of `bench/gate.ts`, whose
 * whole job it is. Two reasons, the first measured: four runs of identical code read 37.3, 56.0,
 * 68.5 and 77.9 ns/row on the fastest case - a 2.1x spread - while the same runs' allocation read
 * 45.7, 45.7 and 45.9 MB. This suite measures one chain five times; `bench/overhead.ts` measures
 * rounds against a hand-rolled floor and is built for timing. Gating it twice would add a second
 * flake source for a number already gated on the harness that can hold it.
 */

/** One case's own measured (or committed-baseline) row. */
export interface MemorySample {
  promisesPerRow: number;
  gcCount: number;
  gcCostMs: number;
  allocatedMb: number;
  retainedMb: number;
  nsPerRow: number;
}

export type MemoryReport = Record<string, MemorySample>;

/**
 * How far each axis may drift before it is a regression, measured across repeated suite runs on an
 * unchanged tree rather than chosen.
 *
 * - `allocatedMb` and `promisesPerRow` are counters, not timings: on an IN-PROCESS case both held
 *   inside 1% across repeated runs, so 10% is already generous and still catches a real change - the
 *   smallest real allocation change this stack made was 41%. A case that crosses a real boundary
 *   overrides the allocation figure - see `CASE_ALLOCATION_TOLERANCE`.
 * - `gcCount` and `gcCostMs` are small integers on the in-process cases (1 collection, under 1 ms),
 *   where a ratio is meaningless - one extra scavenge is +100%. Both take a ratio AND an absolute
 *   floor, so a case may always drift by `gcCountSlack` events or `gcCostSlackMs` before any ratio
 *   applies.
 */
export const MEMORY_TOLERANCE = {
  allocatedMb: 0.1,
  promisesPerRow: 0.1,
  gcCount: 0.5,
  gcCountSlack: 2,
  gcCostMs: 0.5,
  gcCostSlackMs: 1,
  /** An absolute MB ceiling, not a ratio - see this file's own header. */
  retainedMbCeiling: 1,
} as const;

/**
 * Allocation tolerance for the cases that cross a real boundary, where the in-process 10% does not
 * hold. Measured over five runs each, on an unchanged tree:
 *
 * ```text
 * Http loopback     7.6  7.7  8.6  9.0  9.8 MB   spread 29%
 * Cluster workers   2.9  3.2  3.3  3.5  3.8 MB   spread 31%
 * Pipeline array         45.7  45.8 MB            spread  1%
 * Concurrent array       46.9  47.3 MB            spread  1%
 * ```
 *
 * A loopback socket's own buffers are not deterministic the way an in-process array is, and these
 * two cases allocate little enough that a few KB of socket churn is a large percentage. The generous
 * value is what stops a false alarm; the small absolute totals are why it still catches anything
 * real - the sabotage that doubled `Concurrent array` would read as +100% here too.
 */
export const CASE_ALLOCATION_TOLERANCE: Record<string, number> = {
  "Http loopback": 0.6,
  "Cluster workers": 0.6,
};

export interface MemoryGateResult {
  ok: boolean;
  violations: string[];
}

/**
 * `checkMemoryGate(report, baseline)` - any gated axis worse than `baseline` by more than its own
 * tolerance is a violation; an axis that improved never is. A case the baseline has and `report` does
 * NOT is a violation too: a report that cannot be compared has failed to prove no regression, the
 * same as one that measured a real one.
 *
 * `checkMemoryGate({ a: { allocatedMb: 200, … } }, { a: { allocatedMb: 100, … } })` → one violation,
 * allocation doubled. The same call with `allocatedMb: 50` → no violation, it halved.
 */
export function checkMemoryGate(
  report: Partial<MemoryReport>,
  baseline: Partial<MemoryReport>,
): MemoryGateResult {
  const violations: string[] = [];
  for (const label of Object.keys(baseline)) {
    const base = baseline[label];
    if (!base) continue;
    const current = report[label];
    if (!current) {
      violations.push(`${label}: missing from the report - baseline has it, nothing to compare`);
      continue;
    }
    checkCase(violations, label, current, base);
  }
  return { ok: violations.length === 0, violations };
}

/** One case's own axes, its own function so `checkMemoryGate` above stays one loop (`max-depth: 2`). */
function checkCase(
  violations: string[],
  label: string,
  current: MemorySample,
  base: MemorySample,
): void {
  ratioAxis(violations, label, "allocatedMb", current.allocatedMb, base.allocatedMb, {
    tolerance: CASE_ALLOCATION_TOLERANCE[label] ?? MEMORY_TOLERANCE.allocatedMb,
    slack: 0,
    unit: "MB",
  });
  ratioAxis(violations, label, "promisesPerRow", current.promisesPerRow, base.promisesPerRow, {
    tolerance: MEMORY_TOLERANCE.promisesPerRow,
    // A baseline of 0.000 promises per row - the whole synchronous engine - would otherwise have a
    // ceiling of 0 and flag a single promise anywhere in the run. One per 1000 rows is still
    // decisively "this chain does not allocate per row".
    slack: 0.001,
    unit: "/row",
  });
  // `nsPerRow` is deliberately absent - see this file's own header. `bench/gate.ts` gates speed.
  ratioAxis(violations, label, "gcCount", current.gcCount, base.gcCount, {
    tolerance: MEMORY_TOLERANCE.gcCount,
    slack: MEMORY_TOLERANCE.gcCountSlack,
    unit: "",
  });
  ratioAxis(violations, label, "gcCostMs", current.gcCostMs, base.gcCostMs, {
    tolerance: MEMORY_TOLERANCE.gcCostMs,
    slack: MEMORY_TOLERANCE.gcCostSlackMs,
    unit: "ms",
  });

  if (!Number.isFinite(current.retainedMb)) {
    violations.push(`${label}: retainedMb is ${current.retainedMb} - not a finite measurement`);
    return;
  }
  if (current.retainedMb > MEMORY_TOLERANCE.retainedMbCeiling) {
    violations.push(
      `${label}: retainedMb ${current.retainedMb.toFixed(2)} exceeds the absolute ceiling ` +
        `${MEMORY_TOLERANCE.retainedMbCeiling} MB - a run is holding memory after two forced collections`,
    );
  }
}

/** One axis's own `> ceiling` check, failing loud on a non-finite reading rather than letting an
 * unguarded `>` silently pass a broken measurement - `bench/gate.ts`'s `pushIfOverCeiling` guards
 * the same hazard the same way. */
function ratioAxis(
  violations: string[],
  label: string,
  axis: string,
  currentValue: number,
  baseValue: number,
  opts: { tolerance: number; slack: number; unit: string },
): void {
  if (!Number.isFinite(currentValue)) {
    violations.push(`${label}: ${axis} is ${currentValue} - not a finite measurement`);
    return;
  }
  const ceiling = baseValue * (1 + opts.tolerance) + opts.slack;
  if (currentValue > ceiling) {
    violations.push(
      `${label}: ${axis} ${currentValue.toFixed(3)}${opts.unit} exceeds baseline ` +
        `${baseValue.toFixed(3)}${opts.unit} (ceiling ${ceiling.toFixed(3)}${opts.unit})`,
    );
  }
}
