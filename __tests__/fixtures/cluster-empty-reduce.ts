/**
 * #241 Done-when 6 - a `ClusterPipeline` and a `ClusterHttpPipeline` reduce over `[]` and over a
 * five-item stream at `.buffer(2)` (three chunks) with `maxConcurrency: 4`, so one partition never
 * sees a chunk. Prints the four results as one JSON line: an empty stream owes one seed, and the
 * unused partition owes nothing.
 *
 * Both chains are built before the first `await`: each cluster class keeps a registry keyed by
 * construction order, and a worker re-executes this module to hold the same stages. `workers: 2` is
 * pinned so the result does not depend on `os.availableParallelism()`.
 */
import { ClusterPipeline, ClusterHttpPipeline } from "../../src";

const sum = (acc: number, x: number) => acc + x;

const ws = new ClusterPipeline<number>({ workers: 2, maxConcurrency: 4 }).buffer(2).reduce(sum, 0);
const http = new ClusterHttpPipeline<number>({ workers: 2, maxConcurrency: 4 })
  .buffer(2)
  .reduce(sum, 0);

const result = {
  wsEmpty: await ws([]).toArray(),
  wsFive: await ws([1, 2, 3, 4, 5]).toArray(),
  httpEmpty: await http([]).toArray(),
  httpFive: await http([1, 2, 3, 4, 5]).toArray(),
};

console.log(JSON.stringify(result));
