/**
 * #201 Done-when 4 — `new ClusterPipeline<number>({ maxConcurrency: 2 }).buffer(2).reduce(...)`
 * over `[1, 2, 3, 4, 5]`, verbatim, over the redesigned WS reduce wire. Prints two numbers summing
 * to 15 (`product.md`'s own partitioned-reduce example, split timing-dependent - a real run there
 * shows `[7, 8]`, not a fixed assertion to match). Mirrors `cluster-partitioned-reduce.ts` (#62),
 * imports swapped.
 *
 * Also queries the workers' own connection count (`queryAllConnections`, shared with
 * `websocket-cluster-connections.ts`): the round-robin fix (L3, `resolveConnect()`) closed a race
 * where every partition of a concurrent reduce landed on whichever worker the LAST partition's
 * write picked - a bug this fixture's own sum assertion alone cannot detect, since a fold that
 * collapses onto one connection still sums correctly. `totalConnections` proves the partitions
 * actually spread across distinct workers, not just that their results add up.
 *
 * `workers: 2` is pinned explicitly (mirroring `websocket-cluster-connections.ts`'s own fixture),
 * not left to default to `os.availableParallelism()`: a CPU-constrained host reporting 1 would
 * round-robin both `maxConcurrency: 2` partitions onto the SAME single worker, reading
 * `totalConnections: 1` for a reason unrelated to the round-robin fix this fixture exists to prove
 * (code review).
 */
import { ClusterPipeline } from "../../src/websocket";
import { queryAllConnections } from "../helpers/cluster-connections";

const sum = await new ClusterPipeline<number>({ workers: 2, maxConcurrency: 2 })

  .buffer(2)
  .reduce(
    (acc: number, x: number) => acc + x,
    0,
  )([1, 2, 3, 4, 5])
  .toArray();

const { totalConnections } = await queryAllConnections();

if (sum.length > 0) {
  console.log(JSON.stringify({ sum, total: sum.reduce((a, b) => a + b, 0), totalConnections }));
}
