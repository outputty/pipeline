/**
 * The `Pipeline` leg (#120) - the base class, one chunk at a time, in this process. No `.local()`
 * row: every stage on a base `Pipeline` already runs here, so there is nothing a region could pin
 * that the chain doesn't already do.
 */
import { Pipeline } from "../../src";
import { canonicalChain, canonicalInput, handRolledFloor, timeRounds, ROWS } from "../canonical";
import { legReport, type LegReport } from "../gate";

/** Real, timed `pipelineNsPerRow`/`ratio` at `ROWS.Pipeline` rows, `rounds` rounds (round 1
 * discarded as warm-up - see `canonical.ts`). `floorNsPerRow` is measured ONCE by the caller
 * (`bench/overhead.ts`) and passed in, not re-measured here - `handRolledFloor` is class-independent
 * (`canonical.ts`'s own `timeFloor` docstring), so every leg re-timing it separately was 4 redundant
 * 5-round, 1,000,000-row measurements per `pnpm bench:overhead` run for one conceptual number
 * (code-review finding). */
export async function measurePipeline(floorNsPerRow: number, rounds?: number): Promise<LegReport> {
  const items = canonicalInput(ROWS.Pipeline);
  const pipeline = new Pipeline<number>().transform(canonicalChain);
  const pipelineNsPerRow = await timeRounds(async () => {
    await pipeline(items).toArray();
    return items.length; // ns/row is normalized to rows IN, not rows kept by the filter
  }, rounds);
  return legReport(pipelineNsPerRow, floorNsPerRow);
}

/** `pipelineMatchesFloor(50)` → `true` - a small, fast run (never the full `ROWS.Pipeline` size)
 * proving the real `Pipeline` and the hand-rolled floor produce IDENTICAL output, for Done-when 2's
 * "asserted in a real test, not eyeballed". */
export async function pipelineMatchesFloor(n: number): Promise<boolean> {
  const items = canonicalInput(n);
  const out = await new Pipeline<number>().transform(canonicalChain)(items).toArray();
  const floor = handRolledFloor(items);
  return out.length === floor.length && out.every((v, i) => v === floor[i]);
}
