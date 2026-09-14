/**
 * #201 Done-when 4 — `new ClusterPipeline<number>({ maxConcurrency: 2 }).buffer(2).reduce(...)`
 * over `[1, 2, 3, 4, 5]`, verbatim, over the redesigned WS reduce wire. Prints two numbers summing
 * to 15 (`product.md`'s own partitioned-reduce example, split timing-dependent - a real run there
 * shows `[7, 8]`, not a fixed assertion to match). Mirrors `cluster-partitioned-reduce.ts` (#62),
 * imports swapped.
 */
import { ClusterPipeline } from "../../src";

const sum = await new ClusterPipeline<number>({ maxConcurrency: 2 })

  .buffer(2)
  .reduce(
    (acc: number, x: number) => acc + x,
    0,
  )([1, 2, 3, 4, 5])
  .toArray();

if (sum.length > 0) {
  console.log(JSON.stringify({ sum, total: sum.reduce((a, b) => a + b, 0) }));
}
