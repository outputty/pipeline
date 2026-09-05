/**
 * #17 Done-when 2 — distinct `process.pid` values serve stage 0, and the count matches `workers`.
 * `maxConcurrency` must be at least `workers` so enough chunks are ever in flight at once for the
 * primary to round-robin a NEW connection to every worker (node-parallelism skill, T3: a reused
 * keep-alive connection stays pinned to one worker for its life).
 */
import { ClusterPipeline } from "../../src";

const workers = 3;
const items = Array.from({ length: 30 }, (_, i) => i);

const pids = await new ClusterPipeline(items, { workers, maxConcurrency: workers, chunkSize: 1 })
  .transform((t) => t.map((_x: number) => process.pid))
  .toArray();

console.log(JSON.stringify({ distinctPids: [...new Set(pids)].length, workers }));
