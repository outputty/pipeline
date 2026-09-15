/**
 * #201 Done-when 3 — dispatches more chunks than workers (workers: 2, 10 chunks) then asks each
 * worker how many `WebSocketServer` `"connection"` events it saw, over `cluster.fork()`'s own IPC
 * channel (`"outputty-pipeline-query-connections"` / `"outputty-pipeline-connections"`, the design
 * L3 implements for real). Queried immediately after the run, before `IDLE_KILL_MS` can kill the
 * workers. A worker that never answers (the HTTP-backed class, before L3) reads `-1`, so a missing
 * reply fails the assertion instead of hanging past `FIXTURE_TIMEOUT`.
 */
import { ClusterPipeline } from "../../src";
import { queryAllConnections } from "../helpers/cluster-connections";

const workers = 2;
const items = Array.from({ length: 10 }, (_, i) => i);

await new ClusterPipeline<number>({ workers, maxConcurrency: workers })
  .buffer(1)
  .transform((t) => t.map((x: number) => x))(items)
  .toArray();

const { totalConnections } = await queryAllConnections();

console.log(JSON.stringify({ totalConnections, workers }));
