/**
 * #45 Done-when 6 — case 1 over a real `ClusterPipeline`. The accumulator carries `process.pid`
 * alongside the sum so the primary's own script can tell whether the fold actually ran inside a
 * forked worker: the primary process never nulls its own `_chunks` (only a worker does,
 * `ClusterPipeline`'s constructor), so a `.reduce()` that silently fell back to the base `Pipeline`'s
 * in-process fold would print the right sum without ever dispatching - this catches that.
 */
import { ClusterPipeline } from "../../src";

const primaryPid = process.pid;

const [folded] = await new ClusterPipeline([1, 2, 3, 4, 5])
  .reduce(
    (acc: { sum: number; pid: number }, x: number) => ({ sum: acc.sum + x, pid: process.pid }),
    { sum: 0, pid: primaryPid },
  )
  .toArray();

console.log(JSON.stringify({ sum: folded.sum, dispatchedToWorker: folded.pid !== primaryPid }));
