/**
 * #201 Done-when 3 — dispatches more chunks than workers (workers: 2, 10 chunks) then asks each
 * worker how many `WebSocketServer` `"connection"` events it saw, over `cluster.fork()`'s own IPC
 * channel (`"outputty-pipeline-query-connections"` / `"outputty-pipeline-connections"`, the design
 * L3 implements for real). Queried immediately after the run, before `IDLE_KILL_MS` can kill the
 * workers. A worker that never answers (the HTTP-backed class, before L3) reads `-1`, so a missing
 * reply fails the assertion instead of hanging past `FIXTURE_TIMEOUT`.
 */
import cluster from "node:cluster";
import { ClusterPipeline } from "../../src";

const workers = 2;
const items = Array.from({ length: 10 }, (_, i) => i);

await new ClusterPipeline<number>({ workers, maxConcurrency: workers })
  .buffer(1)
  .transform((t) => t.map((x: number) => x))(items)
  .toArray();

function queryConnections(worker: cluster.Worker): Promise<number> {
  return new Promise((resolve) => {
    const timer = setTimeout(() => resolve(-1), 2000);
    worker.once("message", (message: unknown) => {
      const isConnectionsReply =
        typeof message === "object" &&
        message !== null &&
        "type" in message &&
        message.type === "outputty-pipeline-connections" &&
        "count" in message &&
        typeof message.count === "number";
      if (!isConnectionsReply) return;
      clearTimeout(timer);
      resolve((message as { count: number }).count);
    });
    worker.send({ type: "outputty-pipeline-query-connections" });
  });
}

const liveWorkers = Object.values(cluster.workers ?? {}).filter((w) => w !== undefined);
const counts = await Promise.all(liveWorkers.map(queryConnections));
const totalConnections = counts.reduce((a, b) => a + b, 0);

console.log(JSON.stringify({ totalConnections, workers }));
