/**
 * #45 Done-when 6 — case 1 over a real `ClusterPipeline`. The accumulator carries `process.pid`
 * alongside the sum so the primary's own script can tell whether the fold actually ran inside a
 * forked worker: the primary process never nulls its own `_chunks` (only a worker does,
 * `ClusterPipeline`'s constructor), so a `.reduce()` that silently fell back to the base `Pipeline`'s
 * in-process fold would print the right sum without ever dispatching - this catches that.
 *
 * A WORKER's own run of this same script resolves `.toArray()` to `[]` (architecture.md's own
 * documented constraint) - it prints NOTHING (review: `cluster.fork()`'s shared stdout pipe gives
 * no cross-process write-ordering guarantee, so relying on "the worker's line always lands first"
 * the way `cluster-basic.ts` does would be flaky here; skipping the print entirely means only the
 * PRIMARY ever writes a line, so `lastJsonLine` has nothing to race against).
 */
import { ClusterPipeline } from "../../src";

const primaryPid = process.pid;

const [folded] = await new ClusterPipeline([1, 2, 3, 4, 5])
  .reduce(
    (acc: { sum: number; pid: number }, x: number) => ({ sum: acc.sum + x, pid: process.pid }),
    { sum: 0, pid: primaryPid },
  )
  .toArray();

if (folded) {
  console.log(JSON.stringify({ sum: folded.sum, dispatchedToWorker: folded.pid !== primaryPid }));
}
