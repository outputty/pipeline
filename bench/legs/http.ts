/**
 * The `HttpPipeline` leg (#120) - each chunk POSTed to another instance over a real loopback HTTP
 * server. Its `.local()` row proves a pinned region never crosses the wire at all (Done-when 3),
 * via `countingHandler` (`bench/utils/loopbackServer.ts`) wrapping the SAME worker `.fetch` every
 * other run in this file serves - `requestsWhilePinned` is that counter, read once after the timed
 * pinned run, since a request never reaches the handler at all while pinned (no cost added to the
 * timed path, the same reason `ConcurrentPipeline`'s own counting subclass adds none).
 */
import { HttpPipeline } from "../../src";
import {
  canonicalChain,
  canonicalInput,
  handRolledFloor,
  timeRounds,
  timeFloor,
  ROWS,
  BUFFER_SIZE,
} from "../canonical";
import type { LegReport } from "../gate";
import { withLoopbackServer, countingHandler } from "../utils/loopbackServer";

/** The "another instance" side: an empty-source `HttpPipeline` holding the SAME stage definitions,
 * so its `.fetch` can serve them - the same pattern `pipelines.e2e.test.ts`'s own `makeWorker`
 * helper uses. */
function worker(): HttpPipeline<number> {
  return new HttpPipeline<number>({ url: "" }).transform(canonicalChain);
}

/** Real, timed `pipelineNsPerRow`/`floorNsPerRow`/`ratio` at `ROWS.HttpPipeline` rows over a real
 * loopback server, plus the `.local()` row. The un-pinned (main) run serves the worker's `.fetch`
 * directly, uncounted - only the `.local()` run needs `countingHandler`, since that is the one run
 * whose request count the ticket's own Done-when 3 asserts. */
export async function measureHttpPipeline(rounds?: number): Promise<LegReport> {
  const items = canonicalInput(ROWS.HttpPipeline);

  const floorNsPerRow = await timeFloor(rounds);

  const pipelineNsPerRow = await withLoopbackServer(worker().fetch, async (url) => {
    const pipeline = new HttpPipeline<number>({ url })
      .buffer(BUFFER_SIZE)
      .transform(canonicalChain);
    return timeRounds(async () => {
      await pipeline(items).toArray();
      return items.length;
    }, rounds);
  });

  const { handler: countedHandler, counter } = countingHandler(worker().fetch);
  const localNsPerRow = await withLoopbackServer(countedHandler, async (url) => {
    const localPipeline = new HttpPipeline<number>({ url })
      .buffer(BUFFER_SIZE)
      .local((p) => p.transform(canonicalChain));
    return timeRounds(async () => {
      await localPipeline(items).toArray();
      return items.length;
    }, rounds);
  });

  return {
    pipelineNsPerRow,
    floorNsPerRow,
    ratio: pipelineNsPerRow / floorNsPerRow,
    local: { nsPerRow: localNsPerRow, requestsWhilePinned: counter.requests },
  };
}

/** `httpMatchesFloor(50)` → `true` - small-N equality (Done-when 2), never the full
 * `ROWS.HttpPipeline` size. */
export async function httpMatchesFloor(n: number): Promise<boolean> {
  const items = canonicalInput(n);
  return withLoopbackServer(worker().fetch, async (url) => {
    const out = await new HttpPipeline<number>({ url }).transform(canonicalChain)(items).toArray();
    const floor = handRolledFloor(items);
    return out.length === floor.length && out.every((v, i) => v === floor[i]);
  });
}
