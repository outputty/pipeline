/**
 * The `.branch()` leg (#180) - never measured before this ticket. `.branch()` is a STAGE on any
 * Pipeline class, and its matching and join both always run on the orchestrator
 * (`architecture.md`'s own "Branching" section) - unlike the four dispatching-class legs, it never
 * crosses a network/IPC boundary, so a bare `Pipeline` already measures the whole thing and there is
 * no `.local()` row: the same reason `bench/legs/pipeline.ts` has none.
 */
import { Pipeline, BranchBuilder } from "../../src";
import { canonicalInput, timeRounds, ROWS } from "../canonical";
import { legReport, type LegReport } from "../gate";

/** The record `.branch()` and its hand-rolled floor both produce. */
export interface BranchRecord {
  evens: number[];
  odds: number[];
}

/** `.when("evens", ...).otherwise("odds")` - the canonical branch every leg here measures, matching
 * `Pipeline.branch()`'s own docstring example: a router with no arm `build`, so matched items pass
 * through unchanged and the record is exactly what the hand-rolled floor below also produces. */
export function canonicalBranch(b: BranchBuilder<number>) {
  return b.when("evens", (x: number) => x % 2 === 0).otherwise("odds");
}

/**
 * The REAL floor: one pass, no `Transformer`/`BranchBuilder` machinery, no intermediate collected
 * array beyond the two output buckets themselves - the quickest in-process code producing the
 * IDENTICAL record `.branch()` does, per `canonical.ts`'s own output-matched-not-mechanism-matched
 * rule. This is also the shape `runBranch`'s own collect-then-walk is measured against (Done-when
 * 4): a single walk that groups directly, with no separate `collectItems` pass first.
 *
 * `handRolledBranchFloor([0, 1, 2, 3])` → `{ evens: [0, 2], odds: [1, 3] }`.
 */
export function handRolledBranchFloor(items: number[]): BranchRecord {
  const evens: number[] = [];
  const odds: number[] = [];
  for (const item of items) {
    if (item % 2 === 0) evens.push(item);
    else odds.push(item);
  }
  return { evens, odds };
}

/** Real, timed `pipelineNsPerRow`/`ratio` at `ROWS.Branch` rows, `rounds` rounds. No `.local()` row -
 * see this file's own header. `floorNsPerRow` is measured ONCE by the caller
 * (`bench/overhead.ts`) and passed in, the same reason every other leg takes it as a parameter
 * (`bench/legs/pipeline.ts`'s own docstring). */
export async function measureBranch(floorNsPerRow: number, rounds?: number): Promise<LegReport> {
  const items = canonicalInput(ROWS.Branch);
  const pipeline = new Pipeline<number>().branch(canonicalBranch);
  const pipelineNsPerRow = await timeRounds(() => {
    pipeline(items);
    return items.length;
  }, rounds);
  return legReport(pipelineNsPerRow, floorNsPerRow);
}

/** `branchMatchesFloor(50)` → `true` - small-N equality (matching every other leg's own
 * `*MatchesFloor` convention), never the full `ROWS.Branch` size. */
export function branchMatchesFloor(n: number): boolean {
  const items = canonicalInput(n);
  const out = new Pipeline<number>().branch(canonicalBranch)(items) as BranchRecord;
  const floor = handRolledBranchFloor(items);
  return (
    out.evens.length === floor.evens.length &&
    out.evens.every((v, i) => v === floor.evens[i]) &&
    out.odds.length === floor.odds.length &&
    out.odds.every((v, i) => v === floor.odds[i])
  );
}
