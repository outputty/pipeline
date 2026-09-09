/**
 * `Pipeline.prototype.merge` (#41) — the instance-method sibling of the static `Pipeline.merge()`:
 * concatenates other pipelines onto ONE the caller already holds, keeping its class, its knobs and
 * its `_chunkTransforms` numbering. `__tests__/merge.e2e.test.ts` (the static) stays untouched; this
 * file is the instance method's own suite.
 */
import { describe, it, expect } from "vitest";
import { Pipeline, ConcurrentPipeline, HttpPipeline } from "../src";
import { HTTP_TIMEOUT, withServer } from "./helpers/fixtures";
import { LoggingContext } from "./fixtures/context-managers";

describe("Pipeline.prototype.merge (#41)", () => {
  it(
    "stage numbering CONTINUES across the merge - no /stage/0 collision (Done-when 1, 4)",
    async () => {
      // The ticket's own planning spike (#41's `## Interface`): remote's stage 0 is `x + 1`,
      // the merged stage is `x * 100`. The worker mirrors the orchestrator's own two stages.
      const worker = new HttpPipeline({ url: "" })
        .from<number>([])
        .transform((t) => t.map((x: number) => x + 1))
        .transform((t) => t.map((x: number) => x * 100));

      const pathCounts: Record<string, number> = {};
      const countingHandler = async (request: Request): Promise<Response> => {
        const path = new URL(request.url).pathname;
        pathCounts[path] = (pathCounts[path] ?? 0) + 1;
        return worker.fetch(request);
      };

      await withServer(countingHandler, async (url) => {
        // Stage 0, dispatched over HTTP before the merge - 4 items, one chunk each.
        const remote = new HttpPipeline({ url })
          .from<number>([1, 2, 3, 4])
          .buffer(1)
          .transform((t) => t.map((x: number) => x + 1));

        // A ConcurrentPipeline's OWN items, produced through its OWN in-process fan-out - never
        // touching the wire (Done-when 4: "each contributed items its own class produced").
        const local = new ConcurrentPipeline()
          .from<number>([10, 20])
          .buffer(1)
          .transform((t) => t.map((x: number) => x + 5));

        const merged = remote.merge(local).transform((t) => t.map((x: number) => x * 100));

        expect(merged.constructor.name).toBe("HttpPipeline");
        const out = await merged.toArray();

        expect(out).toEqual([200, 300, 400, 500, 1500, 2500]);
        // Two distinct routes hit - the spiked static designs put every hit on /stage/0 and
        // silently lost a transform (#41's own ticket).
        expect(Object.keys(pathCounts).sort()).toEqual(["/stage/0", "/stage/1"]);
        expect(pathCounts).toEqual({ "/stage/0": 4, "/stage/1": 6 });
      });
    },
    HTTP_TIMEOUT,
  );

  it("the merged pipeline keeps its class, knobs and buffered chunk cut (Done-when 2)", async () => {
    const base = new ConcurrentPipeline({
      maxConcurrency: 8,
      ordered: false,
    })
      .from<number>([1, 2])
      .buffer(1);
    const other = new Pipeline().from<number>([3, 4]);

    const merged = base.merge(other);

    expect(merged.constructor.name).toBe("ConcurrentPipeline");
    expect((merged as ConcurrentPipeline<number>).maxConcurrency).toBe(8);
    expect((merged as ConcurrentPipeline<number>).ordered).toBe(false);
    // No stage applied after the merge, so the underlying chunk stream is directly observable:
    // base's own buffer(1) cut ([1],[2]) survives, concatenated with other's un-buffered,
    // default-sized single chunk ([3,4]).
    const chunks: number[][] = [];
    for await (const chunk of merged) {
      chunks.push(chunk);
    }
    expect(chunks).toEqual([[1], [2], [3, 4]]);
  });

  it("contexts merge, later winning on a shared key - the static's own semantics (Done-when 3)", async () => {
    const p1 = new Pipeline().from([1]).context({ k1: "a", shared: "A" });
    const p2 = new Pipeline().from([2]).context({ k2: "b", shared: "B" });

    const merged = p1.merge(p2);

    expect(merged.contextManager.toDict()).toEqual({ k1: "a", shared: "B", k2: "b" });
  });

  it("mutates THIS pipeline's own manager in place, same instance forward (.context()'s own contract)", async () => {
    const mine = new LoggingContext();
    const p1 = new Pipeline({ context: mine }).from([1]).context({ shared: "first" });
    const p2 = new Pipeline().from([2]).context({ shared: "second" });

    const merged = p1.merge(p2);

    expect(merged.contextManager).toBe(mine);
    expect(mine.toDict()).toEqual({ shared: "second" });
  });

  it("merge() with no arguments returns an equivalent pipeline of the same class (Done-when 5)", async () => {
    const p = new ConcurrentPipeline({ maxConcurrency: 2 }).from<number>([1, 2, 3]);

    const merged = p.merge();

    expect(merged.constructor.name).toBe("ConcurrentPipeline");
    expect(await merged.toArray()).toEqual([1, 2, 3]);
  });

  it("merging an empty pipeline changes nothing (Done-when 5)", async () => {
    const p = new Pipeline().from<number>([1, 2, 3]);

    const merged = p.merge(new Pipeline().from<number>([]));

    expect(await merged.toArray()).toEqual([1, 2, 3]);
  });
});
