/**
 * The `ConcurrentPipeline` leg (#120) - several chunks in flight in this process, fanned out
 * through `stageWork()`. Its `.local()` row proves a pinned region never reaches `stageWork()` at
 * all (Done-when 3), via a subclass that counts calls into a MODULE-level-shaped closure object
 * rather than an instance field - `.transform()`/`.local()`/`.buffer()` all copy-on-write through
 * `this.constructor` (`architecture.md`'s own `createPipeline()`), so an instance field resets on
 * every one of those calls; a counter captured by the override closure survives them.
 */
import { ConcurrentPipeline, type InternalTransformer, type Transformer } from "../../src";
import {
  canonicalChain,
  canonicalInput,
  handRolledFloor,
  timeRounds,
  ROWS,
  BUFFER_SIZE,
  MAX_CONCURRENCY,
} from "../canonical";
import { legReport, type LegReport } from "../gate";

/** A counting `ConcurrentPipeline` subclass, plus the counter its `stageWork()` override
 * increments - `countingConcurrentPipeline()`'s own return shape, named so its callers don't widen
 * it to an anonymous object type. */
export interface CountingConcurrentPipeline {
  Pipeline: new (options?: { maxConcurrency: number }) => ConcurrentPipeline<number>;
  counter: { calls: number };
}

/** A counter surviving copy-on-write, plus the subclass whose `stageWork()` override increments it -
 * `dispatchesWhilePinned` (below) reports it after a `.local()`-only run, and a real test uses the
 * same class to prove the counter itself moves for a stage placed OUTSIDE `.local()` (negative
 * control - a counter that never increments proves nothing). */
export function countingConcurrentPipeline(): CountingConcurrentPipeline {
  const counter = { calls: 0 };
  class StageWorkCounter extends ConcurrentPipeline<number> {
    protected override stageWork<U>(
      transformer: Transformer<number, U, "sync" | "async">,
      stageIndex: number,
    ): InternalTransformer<number, U> {
      counter.calls++;
      return super.stageWork(transformer, stageIndex);
    }
  }
  return { Pipeline: StageWorkCounter, counter };
}

/** Real, timed `pipelineNsPerRow`/`ratio` at `ROWS.ConcurrentPipeline` rows, plus the `.local()`
 * row: same rows, same chain, wrapped in `.local(build)` instead of dispatched, timed the same way -
 * `dispatchesWhilePinned` is read once, after every timed round, since the override body never runs
 * while pinned (architecture.md's own guarantee) and so never adds cost to the timed path.
 * `floorNsPerRow` is measured ONCE by the caller and passed in - see `measurePipeline`'s own
 * docstring for why. */
export async function measureConcurrentPipeline(
  floorNsPerRow: number,
  rounds?: number,
): Promise<LegReport> {
  const items = canonicalInput(ROWS.ConcurrentPipeline);
  const pipeline = new ConcurrentPipeline<number>({ maxConcurrency: MAX_CONCURRENCY })
    .buffer(BUFFER_SIZE)
    .transform(canonicalChain);
  const pipelineNsPerRow = await timeRounds(async () => {
    await pipeline(items).toArray();
    return items.length;
  }, rounds);

  const { Pipeline: CountingPipeline, counter } = countingConcurrentPipeline();
  const localPipeline = new CountingPipeline({ maxConcurrency: MAX_CONCURRENCY })
    .buffer(BUFFER_SIZE)
    .local((p) => p.transform(canonicalChain));
  const localNsPerRow = await timeRounds(async () => {
    await localPipeline(items).toArray();
    return items.length;
  }, rounds);

  return legReport(pipelineNsPerRow, floorNsPerRow, {
    nsPerRow: localNsPerRow,
    dispatchesWhilePinned: counter.calls,
  });
}

/** `concurrentMatchesFloor(50)` → `true` - small-N equality (Done-when 2), never the full
 * `ROWS.ConcurrentPipeline` size. */
export async function concurrentMatchesFloor(n: number): Promise<boolean> {
  const items = canonicalInput(n);
  const out = await new ConcurrentPipeline<number>({ maxConcurrency: MAX_CONCURRENCY })
    .buffer(BUFFER_SIZE)
    .transform(canonicalChain)(items)
    .toArray();
  const floor = handRolledFloor(items);
  return out.length === floor.length && out.every((v, i) => v === floor[i]);
}
