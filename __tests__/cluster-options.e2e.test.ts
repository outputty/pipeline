/**
 * cluster-options.e2e.test.ts — #208's Done-when cases: `ClusterPipeline` accepts `codec`
 * (`WebSocketPipeline`'s own knob) and `ClusterHttpPipeline` accepts `client` (`HttpPipeline`'s
 * own knob, #179), both with no cast. Done-when 2 and 3 run as subprocess fixtures - a real
 * `ClusterPipeline`/`ClusterHttpPipeline` forks real workers, and `cluster.fork()` re-execs
 * `process.argv[1]`, Vitest's own entry inside a worker (`pipelines.e2e.test.ts`'s own header).
 */
import { describe, it, expect } from "vitest";
import { ClusterHttpPipeline, JsonCodec, fetchClient } from "../src";
import { ClusterPipeline } from "../src/websocket";
import { FIXTURE_TIMEOUT, runFixtureJson } from "./helpers/fixtures";

describe("#208 ClusterPipeline/ClusterHttpPipeline accept codec/client with no cast (Done-when 1)", () => {
  it("constructs both classes exactly as the ticket's own Interface example", () => {
    const a = new ClusterPipeline<number>({ workers: 2, maxConcurrency: 2, codec: new JsonCodec() })
      .buffer(1)
      .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 4));
    const b = new ClusterHttpPipeline<number>({ workers: 2, client: fetchClient })
      .buffer(1)
      .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 4));

    // Neither pipeline is ever drained here - constructing one only claims a registry slot
    // (`cluster.ts`'s own header: workers fork lazily, on the first chunk actually dispatched).
    // A real Pipeline instance IS a callable function (Language's own Pipeline entry).
    expect(typeof a).toBe("function");
    expect(typeof b).toBe("function");
  });
});

describe("#208 a codec's own store, shared across worker processes via process.env (Done-when 2)", () => {
  it(
    "sends only a 36-byte key over the wire; each of 2 workers decodes only that key",
    async () => {
      const result = await runFixtureJson<{
        transform: number[];
        workerDecodeBytes: number[];
        workersThatDecoded: number;
      }>("__tests__/fixtures/cluster-file-codec.ts");

      expect(result.transform).toEqual([6, 8, 10, 12, 14, 16]);
      expect(result.workerDecodeBytes).toEqual([36]);
      expect(result.workersThatDecoded).toBe(2);
    },
    FIXTURE_TIMEOUT,
  );
});

describe("#208 ClusterHttpPipeline dispatches every chunk through the caller's own client (Done-when 3)", () => {
  it(
    "one client call per chunk under .buffer(1), all counted in the primary",
    async () => {
      const result = await runFixtureJson<{ output: number[]; clientCallsInPrimary: number }>(
        "__tests__/fixtures/cluster-http-counting-client.ts",
      );

      expect(result.output).toEqual([6, 8, 10]);
      expect(result.clientCallsInPrimary).toBe(5);
    },
    FIXTURE_TIMEOUT,
  );
});
