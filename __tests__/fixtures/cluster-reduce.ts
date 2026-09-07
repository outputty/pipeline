/**
 * #45 Done-when 6 — case 1 over a real `ClusterPipeline`. Each fold step appends `process.pid` to
 * the accumulator, so the primary's own script can tell whether the fold actually ran inside a
 * forked worker: the primary process never nulls its own `_chunks` (only a worker does,
 * `ClusterPipeline`'s constructor), so a `.reduce()` that silently fell back to the base
 * `Pipeline`'s in-process fold would print the right sum with every pid the primary's own - this
 * catches that.
 *
 * #62: `.reduce()` now really partitions. With no `.buffer()` the source is one chunk, so there is
 * only ever one real partition (`share()`'s free-slot dealing - every other requested partition
 * sees no chunks and emits nothing), and `.toArray()` naturally resolves to that ONE partition's
 * own accumulator with no merge step needed.
 *
 * A WORKER's own run of this same script resolves `.toArray()` to `[]` (architecture.md's own
 * documented constraint) - it prints NOTHING (review: `cluster.fork()`'s shared stdout pipe gives
 * no cross-process write-ordering guarantee, so relying on "the worker's line always lands first"
 * the way `cluster-basic.ts` does would be flaky here; skipping the print entirely means only the
 * PRIMARY ever writes a line, so `lastJsonLine` has nothing to race against).
 */
import { ClusterPipeline } from "../../src";

interface Tagged {
  sum: number;
  pids: number[];
}

const primaryPid = process.pid;

const [folded] = await new ClusterPipeline([1, 2, 3, 4, 5])
  .reduce(
    (acc: Tagged, x: number): Tagged => ({ sum: acc.sum + x, pids: [...acc.pids, process.pid] }),
    { sum: 0, pids: [] },
  )
  .toArray();

if (folded) {
  console.log(
    JSON.stringify({
      sum: folded.sum,
      dispatchedToWorker: folded.pids.some((pid) => pid !== primaryPid),
    }),
  );
}
