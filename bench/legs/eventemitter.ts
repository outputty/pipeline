/**
 * The `EventEmitterPipeline` leg (#180) - the fourth dispatching class, skipped since #124: `rg -l
 * EventEmitterPipeline bench/` returned nothing before this file, so every #179 figure excluded it.
 * It overrides `stageWork()` the same way `HttpPipeline` does and inherits the fan-out unchanged
 * (`architecture.md`'s "EventEmitterPipeline" section), which predicts #179's six fixes reached it -
 * "should have" is exactly the phrasing that produced #179's own wrong diagnosis, so it is measured
 * here rather than assumed.
 *
 * Its `.local()` row proves a pinned region registers no extra Worker and dispatches nothing
 * (Done-when 10): a manually-registered Worker on `stage:0`, counted while the `.local()` chain
 * runs - `stageWork()` never runs there, so the chain's own composed function never registers
 * either, and this manual Worker is the WHOLE of `listenerCount("stage:0")`.
 */
import { EventEmitterPipeline } from "../../src";
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

/** Real, timed `pipelineNsPerRow`/`ratio` at `ROWS.EventEmitterPipeline` rows, plus the `.local()`
 * row and its `workersWhilePinned` correctness check. `floorNsPerRow` is measured ONCE by the
 * caller and passed in - see `measurePipeline`'s own docstring for why. Each timed pipeline is its
 * own fresh instance with its own default emitter (never one shared `options.emitter` across two
 * constructions) - `architecture.md`'s own warning that two independently-constructed
 * `EventEmitterPipeline`s sharing one emitter both register on `stage:0` and race.
 *
 * `measureEventEmitterPipeline(11.4, 5)` → `{ pipelineNsPerRow: 17.9, floorNsPerRow: 11.4, ratio:
 * 1.57, local: { nsPerRow: 16.5, workersWhilePinned: 0 } }`.
 */
export async function measureEventEmitterPipeline(
  floorNsPerRow: number,
  rounds?: number,
): Promise<LegReport> {
  const items = canonicalInput(ROWS.EventEmitterPipeline);

  const pipeline = new EventEmitterPipeline<number>({ maxConcurrency: MAX_CONCURRENCY })
    .buffer(BUFFER_SIZE)
    .transform(canonicalChain);
  const pipelineNsPerRow = await timeRounds(async () => {
    await pipeline(items).toArray();
    return items.length;
  }, rounds);

  const localPipeline = new EventEmitterPipeline<number>({ maxConcurrency: MAX_CONCURRENCY })
    .buffer(BUFFER_SIZE)
    .local((p) => p.transform(canonicalChain));
  const counter = { calls: 0 };
  localPipeline.emitter.on("stage:0", () => {
    counter.calls++;
  });
  const localNsPerRow = await timeRounds(async () => {
    await localPipeline(items).toArray();
    return items.length;
  }, rounds);

  return legReport(pipelineNsPerRow, floorNsPerRow, {
    nsPerRow: localNsPerRow,
    workersWhilePinned: counter.calls,
  });
}

/** `eventEmitterMatchesFloor(50)` → `true` - small-N equality (Done-when 2), never the full
 * `ROWS.EventEmitterPipeline` size. */
export async function eventEmitterMatchesFloor(n: number): Promise<boolean> {
  const items = canonicalInput(n);
  const out = await new EventEmitterPipeline<number>().transform(canonicalChain)(items).toArray();
  const floor = handRolledFloor(items);
  return out.length === floor.length && out.every((v, i) => v === floor[i]);
}
