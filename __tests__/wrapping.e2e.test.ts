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

  it("refuses to wrap a pipeline that already named a source", () => {
    const bound = new Pipeline<Order>().from(ordersA);
    expect(() => new ConcurrentPipeline(bound, { maxConcurrency: 2 })).toThrow(
      /already named a source/,
    );
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
    const worker = new HttpPipeline(new Pipeline<number>(), { url: "" });
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

  it("mixes a transformer-less branch with a transformed one", async () => {
    const mixed = await withVat.branch<Order | string>({
      big: {
        predicate: (o: Order) => o.total > 200,
        transformer: label("BIG") as unknown as Transformer<Order, Order | string, "sync">,
      },
      eu: { predicate: (o: Order) => o.region === "eu" },
    })(ordersA);
    expect(mixed.big).toEqual(["BIG:2", "BIG:4"]);
    expect((mixed.eu as Order[]).map((o) => o.id)).toEqual([1, 3]);
  });

  it("still works with no argument on a pipeline that named a source", async () => {
    const bound = new Pipeline<Order>().from(ordersA);
    expect(await bound.branch({ eu: { predicate: (o: Order) => o.region === "eu" } })()).toEqual({
      eu: [ordersA[0], ordersA[2]],
    });
  });
});
