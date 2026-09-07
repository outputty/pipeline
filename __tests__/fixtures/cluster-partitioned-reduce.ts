/**
 * #62 — the ticket's own two canonical chains (sum, and the count case whose fold would give a
 * different total if reused as its own merge) over a real `ClusterPipeline`, dispatched to real
 * forked workers. No combine step: each partition's own result flows downstream as an ordinary
 * value, same as `emit()` already does on a non-partitioned reduce. `merged` demonstrates the
 * optional, hand-written second reduce a caller writes when they want ONE final value.
 */
import { ClusterPipeline } from "../../src";

const sum = await new ClusterPipeline([1, 2, 3, 4, 5], { maxConcurrency: 2 })
  .buffer(2)
  .reduce((acc: number, x: number) => acc + x, 0)
  .toArray();

const count = await new ClusterPipeline([1, 2, 3, 4, 5], { maxConcurrency: 2 })
  .buffer(2)
  .reduce((acc: number, _x: number) => acc + 1, 0)
  .toArray();

const merged = await new ClusterPipeline([1, 2, 3, 4, 5], { maxConcurrency: 2 })
  .buffer(2)
  .reduce((acc: number, x: number) => acc + x, 0)
  .local((p) => p.reduce((acc: number, v: number) => acc + v, 0))
  .toArray();

// The count case merged, too - proves .local() isn't just adding numbers back together, it runs
// the CALLER's own merge function, which here still sums (partial counts), not counts-of-counts.
const countMerged = await new ClusterPipeline([1, 2, 3, 4, 5], { maxConcurrency: 2 })
  .buffer(2)
  .reduce((acc: number, _x: number) => acc + 1, 0)
  .local((p) => p.reduce((acc: number, v: number) => acc + v, 0))
  .toArray();

if (sum.length > 0 || count.length > 0 || merged.length > 0 || countMerged.length > 0) {
  console.log(
    JSON.stringify({
      sumTotal: sum.reduce((a, b) => a + b, 0),
      sumPartitions: sum.length,
      countTotal: count.reduce((a, b) => a + b, 0),
      countPartitions: count.length,
      merged,
      countMerged,
    }),
  );
}
