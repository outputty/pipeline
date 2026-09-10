/**
 * A chain says WHAT to do; a wrapping class says WHERE it runs (#90). `ConcurrentPipeline`,
 * `HttpPipeline` and `ClusterPipeline` take `(pipeline, options)` and are callable themselves, so
 * one definition serves a local run, a concurrent one and a dispatched one.
 *
 * Covers #90's Done-when 5 and 15. `.branch()`'s own cases (Done-when 13, 14, 18-24) moved to
 * `branch.e2e.test.ts` (#133).
 */

import { describe, it, expect } from "vitest";

import { Pipeline } from "@src/pipeline";
import { ConcurrentPipeline } from "@src/pipelines/concurrent";
import { HttpPipeline } from "@src/pipelines/http";
import { ClusterPipeline } from "@src/pipelines/cluster";
import { withServer, HTTP_TIMEOUT } from "./helpers/fixtures";
import { ordersA, ordersB, withVat } from "./helpers/domain";

describe("a wrapping class takes (pipeline, options) and runs the chain elsewhere", () => {
  it("runs a wrapped chain concurrently and returns what the plain chain returns (Done-when 5)", async () => {
    const local = withVat(ordersA).toArray();
    const concurrent = await new ConcurrentPipeline(withVat, { maxConcurrency: 2 })(
      ordersA,
    ).toArray();
    expect(concurrent).toEqual(local);
    expect(local.map((o) => o.total)).toEqual([60, 360, 144, 1080]);
  });

  it("reuses one wrapper across inputs", async () => {
    const run = new ConcurrentPipeline(withVat, { maxConcurrency: 2 });
    expect((await run(ordersA).toArray()).map((o) => o.id)).toEqual([1, 2, 3, 4]);
    expect((await run(ordersB).toArray()).map((o) => o.id)).toEqual([9, 10]);
  });

  it("keeps the wrapper's own knobs through the chain", () => {
    const run = new ConcurrentPipeline(withVat, { maxConcurrency: 7, ordered: false });
    const extended = run.transform((t) => t.map((o) => o.total));
    expect(extended.maxConcurrency).toBe(7);
    expect(extended.ordered).toBe(false);
    expect(extended).toBeInstanceOf(ConcurrentPipeline);
  });

  it("keeps the wrapped chain's own input type, not its output type", () => {
    // Before: the constructor parameter was `AnyPipeline<T>`, whose `In` is `any`, so the wrapper
    // fell back to its own `T` - each stage's OUTPUT. Measured on a `number → string` chain, the
    // wrapper typed its input `string` and rejected the `number[]` that ran fine:
    // `TS2769: Argument of type 'number[]' is not assignable to parameter of type
    // 'Iterable<string>'`. The `: Promise<string[]>` annotation is the assertion here.
    const toStr = new Pipeline<number>().transform((t) => t.map((n) => `S${n}`));
    const wrapped = new ConcurrentPipeline(toStr, { maxConcurrency: 2 });
    const out: Promise<string[]> = wrapped([1, 2, 3]).toArray();
    return expect(out).resolves.toEqual(["S1", "S2", "S3"]);
  });

  it("partitions a wrapped reduce the way the class always did (#62)", async () => {
    const summed = new Pipeline<number>().buffer(2).reduce((acc: number, x: number) => acc + x, 0);
    expect(summed([1, 2, 3, 4, 5, 6]).toArray()).toEqual([21]);
    expect(
      await new ConcurrentPipeline(summed, { maxConcurrency: 3 })([1, 2, 3, 4, 5, 6]).toArray(),
    ).toEqual([3, 7, 11]);
  });

  it("keeps a whole region in-process through .local(), wrapped or not", async () => {
    const folded = new Pipeline<number>()
      .buffer(2)
      .local((p) => p.reduce((acc: number, x: number) => acc + x, 0));
    expect(folded([1, 2, 3, 4, 5, 6]).toArray()).toEqual([21]);
    expect(
      await new ConcurrentPipeline(folded, { maxConcurrency: 3 })([1, 2, 3, 4, 5, 6]).toArray(),
    ).toEqual([21]);
  });
});

describe("the HTTP pair shares one definition, with no placeholder source (Done-when 15)", () => {
  it(
    "a worker mounts .fetch without naming data, and a trigger calls it with different data",
    async () => {
      const doubled = new Pipeline<number>().transform((t) => t.map((x) => x * 2));

      // The WORKER: holds the stages, serves them by index, never drains, has no data. Before this
      // layer it had to write `.from([])` to exist at all.
      const worker = new HttpPipeline(doubled, { url: "" });

      await withServer(worker.fetch, async (url) => {
        // The TRIGGER: the same definition, different data each call.
        const trigger = new HttpPipeline(doubled, { url });
        expect(await trigger([1, 2, 3]).toArray()).toEqual([2, 4, 6]);
        expect(await trigger([10, 20]).toArray()).toEqual([20, 40]);
      });
    },
    HTTP_TIMEOUT,
  );

  it("neither side names a source anywhere in src/", async () => {
    // Done-when 15's own text: the placeholders this design removes. `emptyAsyncIterable` survives
    // in `cluster.ts` for a WORKER process's own copy, which is a different thing from a chain
    // written with a placeholder source - that one goes with `.from()` at the enable layer.
    const worker = new HttpPipeline<number>(new Pipeline<number>(), { url: "" });
    expect(typeof worker.fetch).toBe("function");
  });
});

describe("a ClusterPipeline wraps a chain too", () => {
  it("adopts a chain and keeps its own class and knobs", () => {
    const wrapped = new ClusterPipeline(withVat, { workers: 2, maxConcurrency: 3 });
    expect(wrapped).toBeInstanceOf(ClusterPipeline);
    expect(wrapped.workers).toBe(2);
    expect(wrapped.maxConcurrency).toBe(3);
    // Built and never drained, so it never forks - the same property the class always had.
    expect(wrapped.transform((t) => t.map((o) => o.total))).toBeInstanceOf(ClusterPipeline);
  });
});

describe("the wire format reads as the chain was built (#90 L10)", () => {
  /** Every path a mounted `.fetch` was asked for during `use`. */
  async function pathsFor(
    chain: Pipeline<number, "unset", number>,
    input: number[],
  ): Promise<{ out: number[]; paths: string[] }> {
    const paths: string[] = [];
    const worker = new HttpPipeline(chain, { url: "" });
    return withServer(
      async (request) => {
        paths.push(new URL(request.url).pathname);
        return worker.fetch(request);
      },
      async (url) => {
        const out = await new HttpPipeline(chain, { url })(input).toArray();
        return { out, paths };
      },
    );
  }

  it(
    "addresses a dispatched stage as /transform/<n>, not /stage/<n>",
    async () => {
      // The verb names what BUILT the stage, so a reader walks `/transform/1` back to the second
      // `.transform()` call rather than counting dispatched stages. `.branch()` extends the same
      // scheme with a `/branch/<i>/<name>/` trail.
      const chain = new Pipeline<number>()
        .transform((t) => t.map((x) => x + 1))
        .transform((t) => t.map((x) => x * 10));

      const { out, paths } = await pathsFor(chain, [1, 2, 3]);
      expect(out).toEqual([20, 30, 40]);
      expect(paths).toEqual(["/transform/0", "/transform/1"]);
    },
    HTTP_TIMEOUT,
  );

  it(
    "keeps a later stage's own address when an earlier one is pinned by .local()",
    async () => {
      // Skipping a pinned stage's id would renumber every stage after it - on both sides, silently,
      // so a rolling deploy could serve the wrong transform under a number that exists in both
      // versions. Measured: the pinned id is simply never requested.
      const chain = new Pipeline<number>()
        .transform((t) => t.map((x) => x + 1))
        .local((p) => p.transform((t) => t.map((x) => x * 10)))
        .transform((t) => t.map((x) => x - 2));

      const { out, paths } = await pathsFor(chain, [1, 2, 3]);
      expect(out).toEqual([18, 28, 38]);
      expect(paths).toEqual(["/transform/0", "/transform/2"]);
    },
    HTTP_TIMEOUT,
  );

  it("404s a path that addresses nothing, naming what it was given", async () => {
    const worker = new HttpPipeline(
      new Pipeline<number>().transform((t) => t.map((x) => x)),
      {
        url: "",
      },
    );
    const response = await worker.fetch(
      new Request("http://x/stage/0", {
        method: "POST",
        body: JSON.stringify({ chunk: [1], context: {} }),
      }),
    );
    expect(response.status).toBe(404);
    expect(await response.json()).toEqual({ error: "unknown stage /stage/0" });
  });
});
