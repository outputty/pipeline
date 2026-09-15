/**
 * websocket-pipeline.e2e.test.ts — #201's Done-when cases. `ClusterHttpPipeline` (real, unchanged)
 * is the renamed HTTP-based class Done-when 2 proves stayed untouched; `ClusterPipeline` stays an
 * alias of it until L3 reparents the name onto the real `WebSocketPipeline` - so Done-when 1/3/4,
 * which all name the ws-based transport specifically, run as `it.fails` until then, discriminating
 * on `instanceof WebSocketPipeline` (`.claude/rules/code.md`: a probe shaped like a real proof, not
 * one that would pass today for the wrong reason - checking output alone would already pass, since
 * the HTTP-backed alias produces the identical `[6,8,10]`/`15`).
 *
 * Done-when 3 (workers: 2, 10 chunks, `WebSocketServer`'s own `"connection"` count reads 2) runs as
 * a subprocess fixture - a real `ClusterPipeline` forks real workers, and `cluster.fork()` re-execs
 * `process.argv[1]`, Vitest's own entry inside a worker (`pipelines.e2e.test.ts`'s own header).
 * Timeout-guarded (`queryConnections`, `__tests__/fixtures/websocket-cluster-connections.ts`) so a
 * missing IPC reply reads as a wrong number rather than a hang.
 *
 * Done-when 5-8 (`pnpm bench:overhead`, `package.json`'s `ws` dependency, the `dist` grep, the
 * file-scope constraint) are repo-wide gates, verified once at the end of the docs layer, not here.
 */
import { describe, it, expect } from "vitest";
import { ClusterHttpPipeline, ClusterPipeline, WebSocketPipeline } from "../src";
import { FIXTURE_TIMEOUT, runFixtureJson } from "./helpers/fixtures";

describe("#201 ClusterPipeline is backed by WebSocketPipeline (Done-when 1)", () => {
  it.fails("the Interface program's own pipeline is a WebSocketPipeline instance", () => {
    const pipeline = new ClusterPipeline<number>().transform((t) =>
      t.map((x: number) => x * 2).filter((x: number) => x > 4),
    );
    expect(pipeline).toBeInstanceOf(WebSocketPipeline);
  });
});

describe("#201 ClusterHttpPipeline stays the untouched HTTP path (Done-when 2)", () => {
  // `[6,8,10]`/"one HTTP request per chunk" is already the real, passing proof in
  // `pipelines.e2e.test.ts`'s "#17 ClusterHttpPipeline canonical program" (renamed, unchanged
  // behavior) and `cluster-pids.ts`'s own distinct-worker-pid case; this adds the one thing #201
  // itself introduces - that the renamed class never becomes WebSocket-backed.
  it("is never WebSocket-backed", () => {
    const pipeline = new ClusterHttpPipeline<number>().transform((t) =>
      t.map((x: number) => x * 2).filter((x: number) => x > 4),
    );
    expect(pipeline).not.toBeInstanceOf(WebSocketPipeline);
  });
});

describe("#201 one persistent WS connection per worker, not one per chunk (Done-when 3)", () => {
  it.fails(
    "workers: 2, 10 chunks dispatched, WebSocketServer's own connection count reads 2",
    async () => {
      const result = await runFixtureJson<{ totalConnections: number; workers: number }>(
        "__tests__/fixtures/websocket-cluster-connections.ts",
      );
      expect(result.totalConnections).toBe(result.workers);
    },
    FIXTURE_TIMEOUT,
  );
});

describe("#201 partitioned reduce over the redesigned WS reduce wire (Done-when 4)", () => {
  it.fails("ClusterPipeline's reduce stage is a WebSocketPipeline instance", () => {
    const pipeline = new ClusterPipeline<number>({ maxConcurrency: 2 })
      .buffer(2)
      .reduce((acc: number, x: number) => acc + x, 0);
    expect(pipeline).toBeInstanceOf(WebSocketPipeline);
  });
});
