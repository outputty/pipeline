/**
 * memory-gate.e2e.test.ts — #179's own memory gate, tested the way `bench/gate.ts` is: with
 * SYNTHETIC reports and no benchmark run at all. A case asserting against real measured numbers
 * would fail on any machine faster or slower than the one that recorded them; `pnpm bench:memory`
 * pasted into the PR is what proves the real numbers.
 */
import { describe, it, expect } from "vitest";
import {
  checkMemoryGate,
  MEMORY_TOLERANCE,
  CASE_ALLOCATION_TOLERANCE,
  type MemorySample,
} from "../bench/memory-gate";

/** A healthy in-process case, close to what `Concurrent array` really reads. */
const healthy: MemorySample = {
  promisesPerRow: 0.012,
  gcCount: 1,
  gcCostMs: 0.7,
  allocatedMb: 47.9,
  retainedMb: 0.01,
  heldAtEndMB: 47.1,
  nsPerRow: 67.5,
};

const withAxis = (axis: keyof MemorySample, value: number): MemorySample => ({
  ...healthy,
  [axis]: value,
});

describe("#179 checkMemoryGate - a regression on any axis is a violation", () => {
  it("passes an unchanged report", () => {
    expect(checkMemoryGate({ a: healthy }, { a: healthy })).toEqual({ ok: true, violations: [] });
  });

  it("flags allocation growth past its tolerance", () => {
    const result = checkMemoryGate({ a: withAxis("allocatedMb", 95.8) }, { a: healthy });
    expect(result.ok).toBe(false);
    expect(result.violations[0]).toContain("allocatedMb");
  });

  it("never flags an improvement, on any axis", () => {
    const better: MemorySample = {
      promisesPerRow: 0,
      gcCount: 0,
      gcCostMs: 0,
      allocatedMb: 1,
      retainedMb: 0,
      heldAtEndMB: 0,
      nsPerRow: 1,
    };
    expect(checkMemoryGate({ a: better }, { a: healthy }).ok).toBe(true);
  });

  it("flags a case the baseline has and the report does not", () => {
    // A report that cannot be compared has failed to prove no regression, the same as one that
    // measured a real regression - never a silent pass.
    const result = checkMemoryGate({}, { a: healthy });
    expect(result.ok).toBe(false);
    expect(result.violations[0]).toContain("missing from the report");
  });

  it("ignores a case the report has and the baseline does not", () => {
    expect(checkMemoryGate({ a: healthy, b: healthy }, { a: healthy }).ok).toBe(true);
  });

  it("flags a non-finite reading rather than passing it", () => {
    // An unguarded `>` passes `NaN` silently, which is a broken measurement reported as a clean run.
    const result = checkMemoryGate({ a: withAxis("allocatedMb", NaN) }, { a: healthy });
    expect(result.ok).toBe(false);
    expect(result.violations[0]).toContain("not a finite measurement");
  });

  it("lets a single extra collection through, and flags a burst", () => {
    // `gcCount` is a small integer on an in-process case: one extra scavenge is +100%, which a ratio
    // alone would call a regression on every other run. The slack is what makes the axis usable.
    expect(checkMemoryGate({ a: withAxis("gcCount", 2) }, { a: healthy }).ok).toBe(true);
    expect(checkMemoryGate({ a: withAxis("gcCount", 3) }, { a: healthy }).ok).toBe(true);
    expect(checkMemoryGate({ a: withAxis("gcCount", 30) }, { a: healthy }).ok).toBe(false);
  });

  it("does not flag one promise per 1000 rows against a zero baseline", () => {
    // The whole synchronous engine reads 0.000 promises per row, and a pure ratio over a zero
    // baseline has a ceiling of zero - it would flag a single promise anywhere in a 500,000-row run.
    const sync: MemorySample = { ...healthy, promisesPerRow: 0 };
    expect(checkMemoryGate({ a: { ...sync, promisesPerRow: 0.001 } }, { a: sync }).ok).toBe(true);
    expect(checkMemoryGate({ a: { ...sync, promisesPerRow: 2 } }, { a: sync }).ok).toBe(false);
  });

  it("gates retention as an absolute ceiling, never a ratio", () => {
    // A healthy chain holds 0.00-0.02 MB, so a percentage over that flags noise as a leak and lets a
    // real leak through whenever the baseline happened to read a shade higher.
    expect(checkMemoryGate({ a: withAxis("retainedMb", 0.9) }, { a: healthy }).ok).toBe(true);
    const leak = checkMemoryGate({ a: withAxis("retainedMb", 50) }, { a: healthy });
    expect(leak.ok).toBe(false);
    expect(leak.violations[0]).toContain("holding memory after two forced collections");
  });

  it("reports every violating axis, not just the first", () => {
    const result = checkMemoryGate(
      { a: { ...healthy, allocatedMb: 500, gcCount: 40, promisesPerRow: 9 } },
      { a: healthy },
    );
    expect(result.violations).toHaveLength(3);
  });

  it("never gates nsPerRow, however far it moves", () => {
    // Measured: four runs of identical code read 37.3, 56.0, 68.5 and 77.9 ns/row on the fastest
    // case, where the same runs' allocation read 45.7, 45.7 and 45.9 MB. Speed is gated by
    // `bench/gate.ts`, on a harness built to hold it; gating it here too only adds a flake source.
    expect(checkMemoryGate({ a: withAxis("nsPerRow", 10_000) }, { a: healthy }).ok).toBe(true);
  });

  it("gives a boundary-crossing case its own allocation tolerance", () => {
    // A loopback socket's buffers are not deterministic the way an in-process array is: measured
    // over five runs each, `Http loopback` allocation spread 29% and `Cluster workers` 31%, where
    // `Pipeline array` and `Concurrent array` both held 1%. One tolerance across all of them is a
    // flake machine on the two dispatching cases.
    const small: MemorySample = { ...healthy, allocatedMb: 7.6 };
    const drifted = { ...small, allocatedMb: 9.8 };

    expect(checkMemoryGate({ "Http loopback": drifted }, { "Http loopback": small }).ok).toBe(true);
    expect(checkMemoryGate({ "Pipeline array": drifted }, { "Pipeline array": small }).ok).toBe(
      false,
    );
    // Still catches a real one: the sabotage that doubled a case's allocation reads far past this.
    const doubled = { ...small, allocatedMb: 15.2 };
    expect(checkMemoryGate({ "Http loopback": doubled }, { "Http loopback": small }).ok).toBe(
      false,
    );
  });

  it("flags heldAtEndMB growth past its ratio tolerance", () => {
    const result = checkMemoryGate({ a: withAxis("heldAtEndMB", 65) }, { a: healthy });
    expect(result.ok).toBe(false);
    expect(result.violations[0]).toContain("heldAtEndMB");
  });

  it("gives a near-zero heldAtEndMB baseline absolute slack, the same shape as gcCount", () => {
    // A streaming leg's own baseline can read a fraction of an MB (#178's own `Pipeline .forEach()`
    // measurement: 0.2-0.3 MB at 10,000,000 rows) - a pure ratio there would flag ordinary scavenge
    // noise as a regression on every run.
    const streaming = { ...healthy, heldAtEndMB: 0.3 };
    expect(checkMemoryGate({ a: { ...streaming, heldAtEndMB: 1.5 } }, { a: streaming }).ok).toBe(
      true,
    );
    expect(checkMemoryGate({ a: { ...streaming, heldAtEndMB: 10 } }, { a: streaming }).ok).toBe(
      false,
    );
  });

  it("fails rather than silently passing when the baseline is missing heldAtEndMB (a pre-#178 baseline)", () => {
    // `undefined * (1 + tolerance) + slack` is `NaN`, and an unguarded `currentValue > NaN` is
    // always `false` - `ratioAxis`'s own baseValue finiteness guard is what turns that into a loud
    // failure instead of a silent pass for an old, unregenerated baseline.
    const { heldAtEndMB: _omitted, ...staleBaseline } = healthy;
    const result = checkMemoryGate({ a: healthy }, { a: staleBaseline as MemorySample });
    expect(result.ok).toBe(false);
    expect(result.violations[0]).toContain("heldAtEndMB baseline is undefined");
  });

  it("keeps its tolerances where the measurements put them", () => {
    // These are measured spreads, not preferences: allocation and promise counts held inside 1%
    // across repeated runs. A future edit that loosens one silently is what this case surfaces.
    expect(MEMORY_TOLERANCE.allocatedMb).toBe(0.1);
    expect(MEMORY_TOLERANCE.promisesPerRow).toBe(0.1);
    expect(MEMORY_TOLERANCE.retainedMbCeiling).toBe(1);
    expect(MEMORY_TOLERANCE.heldAtEndMB).toBe(0.25);
    expect(MEMORY_TOLERANCE.heldAtEndMbSlackMb).toBe(2);
    expect("nsPerRow" in MEMORY_TOLERANCE).toBe(false);
    expect(CASE_ALLOCATION_TOLERANCE["Http loopback"]).toBe(0.6);
    expect(CASE_ALLOCATION_TOLERANCE["Cluster workers"]).toBe(0.6);
  });
});
