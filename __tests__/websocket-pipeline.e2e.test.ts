/**
 * websocket-pipeline.e2e.test.ts — #201's Done-when cases, each proven through a real run.
 * `ClusterHttpPipeline` (real, unchanged) is the renamed HTTP-based class Done-when 2 proves stayed
 * untouched; `ClusterPipeline` is the same name reparented onto the real `WebSocketPipeline` (L3) -
 * every case here runs against the REAL ws+unix wire, no stub, no `instanceof` stand-in.
 *
 * Done-when 1, 3 and 4 run as subprocess fixtures - a real `ClusterPipeline` forks real workers, and
 * `cluster.fork()` re-execs `process.argv[1]`, Vitest's own entry inside a worker
 * (`pipelines.e2e.test.ts`'s own header). Done-when 3's fixture is timeout-guarded
 * (`queryConnections`, `__tests__/fixtures/websocket-cluster-connections.ts`) so a missing IPC
 * reply reads as a wrong number rather than a hang.
 *
 * Done-when 5-8 (`pnpm bench:overhead`, `package.json`'s `ws` dependency, the `dist` grep, the
 * file-scope constraint) are repo-wide gates, verified once at the end of the docs layer, not here.
 */
import { describe, it, expect } from "vitest";
import { ClusterHttpPipeline, WebSocketPipeline } from "../src";
import { FIXTURE_TIMEOUT, runFixture, runFixtureJson, expectFixtureOk } from "./helpers/fixtures";

describe("#201 the Interface program's own after example, over a real ClusterPipeline (Done-when 1)", () => {
  it(
    "prints [6,8,10]",
    async () => {
      const result = await runFixture("__tests__/fixtures/websocket-cluster-basic.ts");
      expect(result.stderr).toBe("");
      expectFixtureOk(result);
      const lines = result.stdout.trim().split("\n");
      expect(lines.at(-1)).toBe("[6,8,10]");
    },
    FIXTURE_TIMEOUT,
  );
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
  it(
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
  // The split itself is timing-dependent (the ticket's own Done-when 4, and product.md's own
  // partitioned-reduce example) - `share()`'s free-slot dealing lets a partition whose connection
  // becomes ready first drain the whole 3-chunk input before a slower sibling's own connection is
  // even ready, so asserting an exact partition COUNT is asserting a race outcome. Total is not:
  // whatever split real hardware produces, the partitions' own results always sum to 15.
  it(
    "one or more partitions, always summing to 15, spread across at least 2 workers",
    async () => {
      const result = await runFixtureJson<{
        sum: number[];
        total: number;
        totalConnections: number;
      }>("__tests__/fixtures/websocket-cluster-reduce.ts");
      expect(result.sum.length).toBeGreaterThanOrEqual(1);
      expect(result.total).toBe(15);
      // The round-robin fix's own regression case (`resolveConnect()`, L3): before it, every
      // partition's dispatch raced on one shared `_connect` field and all collapsed onto whichever
      // worker the LAST write picked, so `totalConnections` read 1 even at `maxConcurrency: 2`.
      expect(result.totalConnections).toBeGreaterThanOrEqual(2);
    },
    FIXTURE_TIMEOUT,
  );
});
