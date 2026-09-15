/**
 * Queries every live `node:cluster` worker for its own `WebSocketServer` connection count, over
 * `cluster.fork()`'s own IPC channel (`"outputty-pipeline-query-connections"` in,
 * `"outputty-pipeline-connections"` out - `WsWorkerSet.startWorkerServer()`, `src/pipelines/
 * cluster.ts`). Shared by `websocket-cluster-connections.ts` (#201 Done-when 3) and
 * `websocket-cluster-reduce.ts` (#201's own round-robin fix, verified on the reduce path too - the
 * fix that made Done-when 3 pass raced on the SAME shared field the reduce path dispatches
 * through, and a probe scoped to `.transform()` alone never re-ran on `.reduce()`).
 *
 * `queryAllConnections()` → `{ totalConnections: 2, workers: 2 }` for two live workers each holding
 * one open socket.
 */
import cluster from "node:cluster";

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

/** Queries every live worker and sums the replies. Throws loud on any unanswered worker rather
 * than summing through a `-1` sentinel: a genuine reply from one worker (e.g. 3) can numerically
 * cancel a missing reply from another (-1), landing on the expected total by accident. */
export async function queryAllConnections(): Promise<{
  totalConnections: number;
  workers: number;
}> {
  const liveWorkers = Object.values(cluster.workers ?? {}).filter((w) => w !== undefined);
  const counts = await Promise.all(liveWorkers.map(queryConnections));
  const unanswered = counts.filter((count) => count === -1).length;
  if (unanswered > 0) {
    throw new Error(
      `${unanswered} of ${counts.length} workers never answered the connections query`,
    );
  }
  return {
    totalConnections: counts.reduce((a, b) => a + b, 0),
    workers: liveWorkers.length,
  };
}
