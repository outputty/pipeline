/**
 * #120 Done-when 3 - `ClusterHttpPipeline`'s own `.local()` correctness: every item's stage runs on the
 * PRIMARY's own pid, never a worker's. Run as a subprocess (real `ClusterHttpPipeline` construction
 * forks real workers - never inside a Vitest worker). `localPidCheck()` (`bench/legs/cluster.ts`) is
 * deliberately small (10 items, not `ROWS.ClusterPipeline`'s 20,000) and standalone rather than
 * routed through `measureClusterPipeline()` - a correctness check has no reason to pay for the full
 * timed run. `bench/legs/cluster.ts` still names every export after `ClusterPipeline` (unedited by
 * this ticket, #201's own Done-when 8) - it constructs `ClusterPipeline` by name, unaffected by
 * which class backs that name.
 */
import { localPidCheck } from "../../bench/legs/cluster";

const result = await localPidCheck(10);
console.log(JSON.stringify(result));
