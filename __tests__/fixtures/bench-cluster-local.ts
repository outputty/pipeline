/**
 * #120 Done-when 3 - `ClusterPipeline`'s own `.local()` correctness: every item's stage runs on the
 * PRIMARY's own pid, never a worker's. Run as a subprocess (real `ClusterPipeline` construction
 * forks real workers - never inside a Vitest worker). Deliberately small (10 items, not
 * `ROWS.ClusterPipeline`'s 20,000) and standalone rather than routed through
 * `measureClusterPipeline()` - a correctness check has no reason to pay for the full timed run.
 */
import { ClusterPipeline } from "../../src";

const primaryPid = process.pid;
const pids = await new ClusterPipeline<number>({ workers: 2 })
  .buffer(1)
  .local((p) => p.transform((t) => t.map((_x: number) => process.pid)))(
    Array.from({ length: 10 }, (_, i) => i),
  )
  .toArray();

console.log(
  JSON.stringify({ workerPidsWhilePinned: [...new Set(pids)].filter((pid) => pid !== primaryPid) }),
);
