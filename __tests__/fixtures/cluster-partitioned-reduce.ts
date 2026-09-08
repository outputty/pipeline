/**
 * #62 Done-when 3 — the ticket's own two canonical chains (sum, and the count case whose combine
 * differs from its fold) over a real `ClusterPipeline`, dispatched to real forked workers.
 */
import { ClusterPipeline } from "../../src";

const sum = await new ClusterPipeline([1, 2, 3, 4, 5], { maxConcurrency: 2 })
  .buffer(2)
  .reduce((acc: number, x: number) => acc + x, 0)
  .local((p) => p.reduce((acc: number, v: number) => acc + v, 0))
  .toArray();

const count = await new ClusterPipeline([1, 2, 3, 4, 5], { maxConcurrency: 2 })
  .buffer(2)
  .reduce((acc: number, _x: number) => acc + 1, 0)
  .local((p) => p.reduce((acc: number, v: number) => acc + v, 0))
  .toArray();

if (sum.length > 0 || count.length > 0) {
  console.log(JSON.stringify({ sum, count }));
}
