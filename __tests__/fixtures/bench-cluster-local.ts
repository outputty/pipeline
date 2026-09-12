/**
 * #120 Done-when 3 - `ClusterPipeline`'s own `.local()` correctness: every item's stage runs on the
 * PRIMARY's own pid, never a worker's. Run as a subprocess (real `ClusterPipeline` construction
 * forks real workers - never inside a Vitest worker). `localPidCheck()` (`bench/legs/cluster.ts`) is
 * deliberately small (10 items, not `ROWS.ClusterPipeline`'s 20,000) and standalone rather than
 * routed through `measureClusterPipeline()` - a correctness check has no reason to pay for the full
 * timed run.
 */
import { localPidCheck } from "../../bench/legs/cluster";

const result = await localPidCheck(10);
console.log(JSON.stringify(result));
