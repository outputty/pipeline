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
    const onMessage = (message: unknown): void => {
      const isConnectionsReply =
        typeof message === "object" &&
        message !== null &&
        "type" in message &&
        message.type === "outputty-pipeline-connections" &&
        "count" in message &&
        typeof message.count === "number";
      // A non-matching message (any other IPC traffic sharing this channel) is not this worker's
      // reply - stay registered rather than a `.once` that would detach on it and never see the
      // real reply that follows.
      if (!isConnectionsReply) return;
      clearTimeout(timer);
      worker.off("message", onMessage);
      resolve((message as { count: number }).count);
    };
    const timer = setTimeout(() => {
      worker.off("message", onMessage);
      resolve(-1);
    }, 2000);
    worker.on("message", onMessage);
    worker.send({ type: "outputty-pipeline-query-connections" });
  });
}

const liveWorkers = Object.values(cluster.workers ?? {}).filter((w) => w !== undefined);
const counts = await Promise.all(liveWorkers.map(queryConnections));
// Fail loud rather than sum through a -1 sentinel: a genuine reply from one worker (e.g. 3) can
// numerically cancel a missing reply from another (-1), landing on `workers` by accident and
// passing the caller's `totalConnections === workers` assertion for the wrong reason.
const unanswered = counts.filter((count) => count === -1).length;
if (unanswered > 0) {
  throw new Error(`${unanswered} of ${counts.length} workers never answered the connections query`);
}
const totalConnections = counts.reduce((a, b) => a + b, 0);

console.log(JSON.stringify({ totalConnections, workers }));
