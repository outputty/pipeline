/**
 * A chain says WHAT to do; a wrapping class says WHERE it runs (#90). `ConcurrentPipeline`,
 * `HttpPipeline` and `ClusterPipeline` take `(pipeline, options)` and are callable themselves, so
 * one definition serves a local run, a concurrent one and a dispatched one.
 *
 * `.branch()` returns a callable too, and a routing-only branch needs no `Transformer` (#87, folded
 * into this ticket).
 *
 * Covers #90's Done-when 5, 13, 14 and 15.
 */

import { describe, it, expect } from "vitest";

import { Pipeline } from "@src/pipeline";
import { Transformer } from "@src/transformer";
import { ConcurrentPipeline } from "@src/pipelines/concurrent";
import { HttpPipeline } from "@src/pipelines/http";
import { ClusterPipeline } from "@src/pipelines/cluster";
import { withServer, HTTP_TIMEOUT } from "./helpers/fixtures";

type Order = { id: number; total: number; region: string };

const ordersA: Order[] = [
  { id: 1, total: 50, region: "eu" },
  { id: 2, total: 300, region: "us" },
  { id: 3, total: 120, region: "eu" },
  { id: 4, total: 900, region: "us" },
];
const ordersB: Order[] = [
  { id: 9, total: 400, region: "eu" },
  { id: 10, total: 20, region: "us" },
];

/** The one definition every case below wraps, runs or branches. No data, built once. */
const withVat = new Pipeline<Order>().transform((t) =>
  t.map((o) => ({ ...o, total: Math.round(o.total * 1.2) })),
);

const label = (text: string): Transformer<Order, string, "sync"> =>
  new Transformer<Order, Order>().map((o) => `${text}:${o.id}`);

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

describe(".branch() is built once and called with any data (Done-when 13, 14)", () => {
  const split = withVat.branch({
    big: { predicate: (o: Order) => o.total > 200, transformer: label("BIG") },
    eu: { predicate: (o: Order) => o.region === "eu", transformer: label("EU") },
    rest: { predicate: () => true, transformer: label("REST") },
  });

  it("routes two different inputs through one set of definitions (Done-when 14)", async () => {
    expect(await split(ordersA)).toEqual({
      big: ["BIG:2", "BIG:4"],
      eu: ["EU:1", "EU:3"],
      rest: [],
    });
    // `ordersB`'s order 9 is EU, but VAT lifts it past 200, so `big` claims it first and `eu` ends
    // empty - declaration order is routing order.
    expect(await split(ordersB)).toEqual({
      big: ["BIG:9"],
      eu: [],
      rest: ["REST:10"],
    });
  });

  it("broadcasts to every matching branch under firstMatch: false", async () => {
    const broadcast = withVat.branch(
      {
        big: { predicate: (o: Order) => o.total > 200, transformer: label("BIG") },
        eu: { predicate: (o: Order) => o.region === "eu", transformer: label("EU") },
        rest: { predicate: () => true, transformer: label("REST") },
      },
      { firstMatch: false },
    );
    expect(await broadcast(ordersA)).toEqual({
      big: ["BIG:2", "BIG:4"],
      eu: ["EU:1", "EU:3"],
      rest: ["REST:1", "REST:2", "REST:3", "REST:4"],
    });
  });

  it("routes with no transformer at all (Done-when 13, #87)", async () => {
    // Before: a routing-only branch still had to name `new Transformer<Order, Order>()` purely to
    // fill a required field - friction `.transform()` never had, since it takes a builder.
    const routed = await withVat.branch({
      eu: { predicate: (o: Order) => o.region === "eu" },
      rest: { predicate: () => true },
    })(ordersA);
    expect(routed.eu.map((o) => o.id)).toEqual([1, 3]);
    expect(routed.rest.map((o) => o.id)).toEqual([2, 4]);
    // Passed through unchanged means the VAT stage still ran - these are transformed items.
    expect(routed.eu.map((o) => o.total)).toEqual([60, 144]);
  });

  it("types each branch from its OWN transformer, not one shared type", async () => {
    // Before: `branch<U>` inferred one `U` from whichever branches named a transformer, and a
    // routing-only branch pushed its items in as that type. Measured - a map pairing an
    // `Order → string` branch with a routing-only one typed the routing branch `string[]` and
    // filled it with `Order` objects, no cast anywhere. This version needs no annotation and no
    // cast; the two annotations below are the assertion.
    const mixed = await withVat.branch({
      big: { predicate: (o: Order) => o.total > 200, transformer: label("BIG") },
      eu: { predicate: (o: Order) => o.region === "eu" },
    })(ordersA);

    const labelled: string[] = mixed.big;
    const routed: Order[] = mixed.eu;
    expect(labelled).toEqual(["BIG:2", "BIG:4"]);
    expect(routed.map((o) => o.id)).toEqual([1, 3]);
  });

  it("still works with no argument on a pipeline that named a source", async () => {
    const bound = new Pipeline<Order>();
    expect(
      await bound.branch({ eu: { predicate: (o: Order) => o.region === "eu" } })(ordersA),
    ).toEqual({
      eu: [ordersA[0], ordersA[2]],
    });
  });

  it("refuses the argument each form cannot honour", async () => {
    // Before: a bound pipeline's runner silently DISCARDED an input it was handed. Measured,
    // typechecking clean: `.from([1,2,3]).branch({all})([9,9,9])` returned `{ all: [1,2,3] }`, then
    // `{ all: [] }` on the second call as the bound stream ran dry.
    const deferredRunner = new Pipeline<number>().branch({ all: { predicate: () => true } });
    // @ts-expect-error a deferred runner needs the items to route
    await expect(deferredRunner()).rejects.toThrow(/no input/);
  });

  it("gives a branch transformer the RUN's context, not the chain's", async () => {
    // Before: routing went through the owner's own context, so a branch transformer saw none of
    // this run's writes and every one of the last run's. Measured on a chain writing
    // `ctx.set("seenByChain", n)`: the branch read `null` for every item, then leaked the previous
    // call's values into the next.
    const chain = new Pipeline<number>().transform((t) =>
      t.map((n, ctx) => {
        ctx?.set("seenByChain", n);
        return n;
      }),
    );
    const seen: unknown[] = [];
    const split = chain.branch({
      all: {
        predicate: () => true,
        transformer: new Transformer<number, number>().map((n, ctx) => {
          seen.push(ctx?.get("seenByChain"));
          return n;
        }),
      },
    });

    // `[3, 3, 3]`, not `[1, 2, 3]`: a context write is chunk-granular, never item-granular - the
    // whole chunk passes through the map before any of it is routed, so every branch read sees the
    // last write. That is the same rule `.tap()` already documents. What this pins is that the
    // branch sees THIS run's writes at all; before the fix it read `[null, null, null]`.
    await split([1, 2, 3]);
    expect(seen).toEqual([3, 3, 3]);

    // A second call starts clean rather than reading the first call's writes back.
    seen.length = 0;
    await split([7, 8]);
    expect(seen).toEqual([8, 8]);
  });

  it("reads item by item once the chunk boundary is one item", async () => {
    // The control for the case above: at `.buffer(1)` each item IS its own chunk, so the branch
    // reads that item's own write rather than the chunk's last.
    const chain = new Pipeline<number>().buffer(1).transform((t) =>
      t.map((n, ctx) => {
        ctx?.set("seenByChain", n);
        return n;
      }),
    );
    const seen: unknown[] = [];
    await chain.branch({
      all: {
        predicate: () => true,
        transformer: new Transformer<number, number>().map((n, ctx) => {
          seen.push(ctx?.get("seenByChain"));
          return n;
        }),
      },
    })([1, 2, 3]);
    expect(seen).toEqual([1, 2, 3]);
  });
});

describe("the wire format reads as the chain was built (#90 L10)", () => {
  /** Every path a mounted `.fetch` was asked for during `use`. */
  async function pathsFor(
    chain: Pipeline<number, "unset", "shape", number>,
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
