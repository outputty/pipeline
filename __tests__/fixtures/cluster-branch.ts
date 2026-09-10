/**
 * #90 L11 review, finding 1 - `.branch()` on a `ClusterPipeline`.
 *
 * A branch arm's own pipeline used to carry the parent's `pipelineIndex`, and every
 * `ClusterPipeline` constructor claims that registry slot. A worker re-runs the entry module, so
 * its own `.branch()` call built the arm clones at load and overwrote `registry.get(0)` with a
 * stage-less one - after which the primary's `/pipeline/0/transform/0` was served by the ARM's
 * stage table. Measured before the fix: `{"rest":["REST:undefined","REST:undefined"]}`.
 */
import { Pipeline, ClusterPipeline } from "../../src";

type Order = { id: number; total: number };

const withVat = new Pipeline<Order>().transform((t) =>
  t.map((o: Order) => ({ ...o, total: Math.round(o.total * 1.2) })),
);

const routed = new ClusterPipeline(withVat, { workers: 2 }).branch((b) =>
  b
    .when(
      "big",
      (o: Order) => o.total > 200,
      (q) => q.transform((t) => t.map((o: Order) => `BIG:${o.id}`)),
    )
    .otherwise("rest", (q) =>
      q.local((r) => r.transform((t) => t.map((o: Order) => `REST:${o.id}`))),
    ),
);

console.log(
  JSON.stringify(
    await routed([
      { id: 1, total: 50 },
      { id: 2, total: 300 },
      { id: 3, total: 900 },
    ]),
  ),
);
