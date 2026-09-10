/**
 * #113 finding 2 - two sibling chains built off ONE `ClusterPipeline` base.
 *
 * `createPipeline()` forwarded `pipelineIndex`, and every constructor claimed that key, so both
 * siblings registered at the base's index and the second overwrote the first - on the primary AND
 * on every worker re-running this module. Calling the first then dispatched
 * `/pipeline/0/transform/0`, the worker resolved `registry[0]` to the SECOND chain, and the run
 * returned that chain's output with no error. Measured before the fix: `{"doubled":[100,200]}`.
 */
import { ClusterPipeline } from "../../src";

const base = new ClusterPipeline<number>({ workers: 2 });
const doubled = base.transform((t) => t.map((x: number) => x * 2));
const hundredfold = base.transform((t) => t.map((x: number) => x * 100));

console.log(
  JSON.stringify({
    doubled: await doubled([1, 2]).toArray(),
    hundredfold: await hundredfold([1, 2]).toArray(),
  }),
);
