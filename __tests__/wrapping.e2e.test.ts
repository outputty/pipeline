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
import { ConcurrentPipeline } from "@src/pipelines/concurrent";
import { HttpPipeline } from "@src/pipelines/http";
import { ClusterPipeline } from "@src/pipelines/cluster";
import {
  withServer,
  HTTP_TIMEOUT,
  FIXTURE_TIMEOUT,
  runFixture,
  expectFixtureOk,
  lastJsonLine,
} from "./helpers/fixtures";
import { createHook } from "node:async_hooks";

/** Every `Promise` created while `fn` runs - the same instrument the sync-mode suite uses. */
function countPromises(fn: () => unknown): number {
  let created = 0;
  const hook = createHook({
    init(_id, type) {
      if (type === "PROMISE") created++;
    },
  });
  hook.enable();
  try {
    fn();
  } finally {
    hook.disable();
  }
  return created;
}

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
  const split = withVat.branch((b) =>
    b
      .when(
        "big",
        (o) => o.total > 200,
        (q) => q.transform((t) => t.map((o) => `BIG:${o.id}`)),
      )
      .when(
        "eu",
        (o) => o.region === "eu",
        (q) => q.transform((t) => t.map((o) => `EU:${o.id}`)),
      )
      .otherwise("rest", (q) => q.transform((t) => t.map((o) => `REST:${o.id}`))),
  );

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
    const broadcast = withVat.branch((b) =>
      b
        .when(
          "big",
          (o) => o.total > 200,
          (q) => q.transform((t) => t.map((o) => `BIG:${o.id}`)),
        )
        .when(
          "eu",
          (o) => o.region === "eu",
          (q) => q.transform((t) => t.map((o) => `EU:${o.id}`)),
        )
        .otherwise("rest", (q) => q.transform((t) => t.map((o) => `REST:${o.id}`)))
        .broadcast(),
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
    const routed = await withVat.branch((b) =>
      b.when("eu", (o) => o.region === "eu").otherwise("rest"),
    )(ordersA);
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
    const mixed = await withVat.branch((b) =>
      b
        .when(
          "big",
          (o) => o.total > 200,
          (q) => q.transform((t) => t.map((o) => `BIG:${o.id}`)),
        )
        .when("eu", (o) => o.region === "eu"),
    )(ordersA);

    const labelled: string[] = mixed.big;
    const routed: Order[] = mixed.eu;
    expect(labelled).toEqual(["BIG:2", "BIG:4"]);
    expect(routed.map((o) => o.id)).toEqual([1, 3]);
  });

  it("still works with no argument on a pipeline that named a source", async () => {
    const bound = new Pipeline<Order>();
    expect(await bound.branch((b) => b.when("eu", (o) => o.region === "eu"))(ordersA)).toEqual({
      eu: [ordersA[0], ordersA[2]],
    });
  });

  it("refuses the argument each form cannot honour", async () => {
    // Before: a bound pipeline's runner silently DISCARDED an input it was handed. Measured,
    // typechecking clean: `.from([1,2,3]).branch({all})([9,9,9])` returned `{ all: [1,2,3] }`, then
    // `{ all: [] }` on the second call as the bound stream ran dry.
    const deferredRunner = new Pipeline<number>().branch((b) => b.otherwise("all"));
    // Thrown, not rejected (#90): every callback here is synchronous, so the chain creates no
    // Promise and there is nothing for a rejection to travel on.
    // @ts-expect-error a runner needs the items to route
    expect(() => deferredRunner()).toThrow(/no input/);
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
    const split = chain.branch((b) =>
      b.otherwise("all", (q) =>
        q.transform((t) =>
          t.map((n, ctx) => {
            seen.push(ctx?.get("seenByChain"));
            return n;
          }),
        ),
      ),
    );

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

  it("reads the chain's final context whatever the chunk boundary was", async () => {
    // The control for the case above, and it now reads the SAME either way. `.branch()` drains the
    // parent chain in full before any arm runs - that is what a record of arrays joined on the
    // orchestrator requires - so an arm sees the chain's last write, not its own item's. Per-chunk
    // granularity belonged to the per-item routing this layer replaced.
    const chain = new Pipeline<number>().buffer(1).transform((t) =>
      t.map((n, ctx) => {
        ctx?.set("seenByChain", n);
        return n;
      }),
    );
    const seen: unknown[] = [];
    await chain.branch((b) =>
      b.otherwise("all", (q) =>
        q.transform((t) =>
          t.map((n, ctx) => {
            seen.push(ctx?.get("seenByChain"));
            return n;
          }),
        ),
      ),
    )([1, 2, 3]);
    expect(seen).toEqual([3, 3, 3]);
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

describe(".branch() is a stage whose arms run where the chain runs (#90 L11)", () => {
  type Order = { id: number; total: number; region: string };

  const orders: Order[] = [
    { id: 1, total: 50, region: "eu" },
    { id: 2, total: 300, region: "us" },
    { id: 3, total: 120, region: "eu" },
    { id: 4, total: 900, region: "us" },
  ];

  const withVat = new Pipeline<Order>().transform((t) =>
    t.map((o) => ({ ...o, total: Math.round(o.total * 1.2) })),
  );

  /** Every path a mounted `.fetch` was asked for while `use` ran. */
  async function servedBy<R>(
    chain: Pipeline<any, any, any>,
    use: (url: string) => Promise<R>,
  ): Promise<{ value: R; paths: string[] }> {
    const paths: string[] = [];
    const worker = new HttpPipeline(chain as never, { url: "" });
    return withServer(
      async (request) => {
        paths.push(new URL(request.url).pathname);
        return worker.fetch(request);
      },
      async (url) => ({ value: await use(url), paths }),
    );
  }

  it(
    "dispatches an arm's own stages, where the Transformer form ran them here (Done-when 18)",
    async () => {
      // BOTH sides declare the branch, because both run the same entry module - that is what makes
      // `/branch/0/big` mean the same arm on each. Before this layer an arm carried a `Transformer`,
      // which has no class and therefore no WHERE, so every arm ran in the orchestrating process.
      const declare = (p: HttpPipeline<Order, Order>) =>
        p.branch((b) =>
          b
            .when(
              "big",
              (o) => o.total > 200,
              (q) => q.transform((t) => t.map((o) => `BIG:${o.id}`)),
            )
            .otherwise("rest"),
        );

      const paths: string[] = [];
      const worker = new HttpPipeline(withVat, { url: "" });
      declare(worker);

      const value = await withServer(
        async (request) => {
          paths.push(new URL(request.url).pathname);
          return worker.fetch(request);
        },
        async (url) => declare(new HttpPipeline(withVat, { url }))(orders),
      );

      expect(value).toEqual({
        big: ["BIG:2", "BIG:4"],
        rest: [
          { id: 1, total: 60, region: "eu" },
          { id: 3, total: 144, region: "eu" },
        ],
      });
      // The parent's own stage AND the arm's, each under its own path.
      expect(paths).toContain("/transform/0");
      expect(paths).toContain("/branch/0/big/transform/0");
    },
    HTTP_TIMEOUT,
  );

  it(
    "runs an arm in the orchestrating process when its builder pins it (Done-when 19)",
    async () => {
      const { value, paths } = await servedBy(withVat, async (url) => {
        const live = new HttpPipeline(withVat, { url });
        return live.branch((b) =>
          b.when(
            "eu",
            (o) => o.region === "eu",
            (q) => q.local((r) => r.transform((t) => t.map((o) => o.id))),
          ),
        )(orders);
      });

      expect(value).toEqual({ eu: [1, 3] });
      // Only the parent's stage crossed: the arm's own transform was pinned by `.local()`.
      expect(paths).toEqual(["/transform/0"]);
    },
    HTTP_TIMEOUT,
  );

  it("never dispatches the demux, so a predicate may close over local state (Done-when 20)", () => {
    // Matching decides WHICH arm an item enters, so dispatching it would send every item out twice
    // and would stop a predicate reading anything the caller holds.
    let threshold = 200;
    const split = withVat.branch((b) =>
      b.when("big", (o) => o.total > threshold).otherwise("rest"),
    );

    expect(split(orders).big.map((o) => o.id)).toEqual([2, 4]);
    threshold = 100;
    expect(split(orders).big.map((o) => o.id)).toEqual([2, 3, 4]);
  });

  it("returns one record of arrays, each key typed by its own arm (Done-when 21)", () => {
    const split = withVat.branch((b) =>
      b
        .when(
          "big",
          (o) => o.total > 200,
          (q) => q.transform((t) => t.map((o) => `BIG:${o.id}`)),
        )
        .when("eu", (o) => o.region === "eu")
        .otherwise("rest", (q) => q.transform((t) => t.map((o) => o.id))),
    );

    const out = split(orders);
    // The two annotations ARE the assertion: `big` is `string[]` from its own arm, `eu` is
    // `Order[]` because it names no pipeline, and `rest` is `number[]` from its own.
    const labelled: string[] = out.big;
    const routed: Order[] = out.eu;
    const ids: number[] = out.rest;
    expect(labelled).toEqual(["BIG:2", "BIG:4"]);
    expect(routed.map((o) => o.id)).toEqual([1, 3]);
    expect(ids).toEqual([]);
  });

  it("creates zero promises when every arm is synchronous (Done-when 22)", () => {
    const split = withVat.branch((b) =>
      b
        .when(
          "big",
          (o) => o.total > 200,
          (q) => q.transform((t) => t.map((o) => o.id)),
        )
        .otherwise("rest"),
    );

    const out = split(orders);
    expect(typeof (out as unknown as { then?: unknown }).then).toBe("undefined");
    expect(countPromises(() => split(orders))).toBe(0);
  });

  it("widens the whole record on one async arm, awaiting only that arm (Done-when 23)", async () => {
    const split = withVat.branch((b) =>
      b
        .when(
          "big",
          (o) => o.total > 200,
          (q) => q.transform((t) => t.map(async (o) => o.id)),
        )
        .otherwise("rest", (q) => q.transform((t) => t.map((o) => o.id))),
    );

    const pending = split(orders);
    expect(typeof (pending as unknown as { then?: unknown }).then).toBe("function");
    expect(await pending).toEqual({ big: [2, 4], rest: [1, 3] });

    // Only the async arm was ever a promise: its sibling returned an array before the join saw it.
    const asyncArm = new Pipeline<Order>()
      .transform((t) => t.map(async (o) => o.id))(orders)
      .toArray();
    const syncArm = new Pipeline<Order>()
      .transform((t) => t.map((o) => o.id))(orders)
      .toArray();
    expect(typeof (asyncArm as unknown as { then?: unknown }).then).toBe("function");
    expect(Array.isArray(syncArm)).toBe(true);
  });

  it("routes the catch-all last however it was written, and broadcasts on demand (Done-when 24)", () => {
    // `.otherwise()` written FIRST still routes last - where `predicate: () => true` declared first
    // used to swallow every arm below it.
    const catchAllFirst = withVat.branch((b) =>
      b.otherwise("rest").when("big", (o) => o.total > 200),
    );
    expect(catchAllFirst(orders).big.map((o) => o.id)).toEqual([2, 4]);
    expect(catchAllFirst(orders).rest.map((o) => o.id)).toEqual([1, 3]);

    const broadcast = withVat.branch((b) =>
      b
        .when("big", (o) => o.total > 200)
        .when("eu", (o) => o.region === "eu")
        .broadcast(),
    );
    const out = broadcast(orders);
    expect(out.big.map((o) => o.id)).toEqual([2, 4]);
    expect(out.eu.map((o) => o.id)).toEqual([1, 3]);
  });

  it("refuses one name twice in a single .branch() call", () => {
    expect(() => withVat.branch((b) => b.when("a", () => true).when("a", () => false))).toThrow(
      /already declared in this .branch\(\) call/,
    );
    expect(() => withVat.branch((b) => b.otherwise("x").otherwise("y"))).toThrow(
      /already declared as "x"/,
    );
  });
});

describe("L11 review findings, each reproduced before it was fixed", () => {
  type Order = { id: number; total: number; region: string };

  const orders: Order[] = [
    { id: 1, total: 50, region: "eu" },
    { id: 2, total: 300, region: "us" },
    { id: 3, total: 120, region: "eu" },
    { id: 4, total: 900, region: "us" },
  ];

  const withVat = new Pipeline<Order>().transform((t) =>
    t.map((o) => ({ ...o, total: Math.round(o.total * 1.2) })),
  );

  it(
    "gives two sibling chains off one base their own worker registry slots (#113)",
    async () => {
      // `createPipeline()` forwarded `pipelineIndex` and every constructor claimed that key, so
      // both siblings registered at the base's index and the second overwrote the first - on the
      // primary and on every worker re-running the entry module. Calling the FIRST then got the
      // second's stages back, with no error. Measured before the fix: `{"doubled":[100,200]}`.
      const fixture = await runFixture("__tests__/fixtures/cluster-sibling-chains.ts");
      expectFixtureOk(fixture);
      expect(lastJsonLine(fixture)).toEqual({ doubled: [2, 4], hundredfold: [100, 200] });
    },
    FIXTURE_TIMEOUT,
  );

  it(
    "branches on a ClusterPipeline without an arm clobbering its own parent",
    async () => {
      // An arm's pipeline carried the parent's `pipelineIndex`, and every ClusterPipeline
      // constructor claims that registry slot - so a worker's own `.branch()` call overwrote
      // `registry.get(0)` with a stage-less arm clone at module load, and the primary's
      // `/pipeline/0/transform/0` was then served the arm's stage table. Measured before the fix:
      // `{"rest":["REST:undefined","REST:undefined"]}`.
      const fixture = await runFixture("__tests__/fixtures/cluster-branch.ts");
      expectFixtureOk(fixture);
      expect(lastJsonLine(fixture)).toEqual({ big: ["BIG:2", "BIG:3"], rest: ["REST:1"] });
    },
    FIXTURE_TIMEOUT,
  );

  it(
    "resolves a .reduce() inside an arm against that arm's own registry",
    async () => {
      // `serveReduceRequest` read the PARENT's `_reduceStages`, ignoring the branch trail: a 404
      // when the parent has no reduce, and silently the parent's own fold when it does.
      const declare = (p: HttpPipeline<Order, Order>) =>
        p.branch((b) =>
          b.when(
            "big",
            (o) => o.total > 200,
            (q) => q.reduce((acc: number, o: Order) => acc + o.total, 0),
          ),
        );

      const worker = new HttpPipeline(withVat, { url: "" });
      declare(worker);

      const value = await withServer(worker.fetch, async (url) =>
        declare(new HttpPipeline(withVat, { url }))(orders),
      );
      // 360 + 1080, folded on the worker under the arm's own reduce route.
      expect(value).toEqual({ big: [1440] });
    },
    HTTP_TIMEOUT,
  );

  it("types the record on the ARMS' Mode, not the chain's alone", async () => {
    // The runtime widens the whole record when any arm is pending, which Done-when 23 asserts. The
    // type denied it, so `split(x).big` compiled clean and was `undefined` at runtime.
    const split = withVat.branch((b) =>
      b.when(
        "big",
        (o) => o.total > 200,
        (q) => q.transform((t) => t.map(async (o) => o.id)),
      ),
    );
    const out = split(orders);
    expect(typeof (out as unknown as { then?: unknown }).then).toBe("function");
    expect(await out).toEqual({ big: [2, 4] });
  });

  it("disarms a pending arm when a later arm throws synchronously", async () => {
    // Review finding: `joinArms` ran a bare `arms.map(...)`, which abandons the array on a
    // synchronous throw - so `evens`' already-pending `toArray()` never reached `settleMaybe` and
    // never got a rejection handler. Measured before the fix: the caller saw `odds arm failed`, and
    // the process then died on `evens arm failed` under Node's default unhandled-rejection policy.
    // `mapSettle` is the helper that owes those siblings a `.catch`, and its own docstring says so.
    const split = new Pipeline<number>().branch((b) =>
      b
        .when(
          "evens",
          (x) => x % 2 === 0,
          (q) =>
            q.transform((t) =>
              t.map((_x: number): Promise<number> => Promise.reject(new Error("evens arm failed"))),
            ),
        )
        .otherwise("odds", (q) =>
          q.transform((t) =>
            t.map((_x: number): number => {
              throw new Error("odds arm failed");
            }),
          ),
        ),
    );

    // Thrown, not rejected: the parent chain and the demux are synchronous here, so the failure
    // leaves the runner the way every other sync failure leaves a terminal (#90).
    expect(() => split([1, 2, 3, 4])).toThrow("odds arm failed");
    // A macrotask later: `evens`' abandoned rejection would have taken the process down by now.
    await new Promise((resolve) => setTimeout(resolve, 10));
  });

  it("refuses an arm name that could not survive a route", () => {
    // The name goes straight into `/branch/<i>/<name>/transform/<n>`, and `.fetch()` matches an
    // ENCODED pathname - so `.when("big orders", …)` dispatched `/branch/0/big%20orders/…` and 404'd.
    expect(() => withVat.branch((b) => b.when("big orders", () => true))).toThrow(
      /not usable in a route/,
    );
    expect(() => withVat.branch((b) => b.when("a/b", () => true))).toThrow(/not usable in a route/);
    expect(() => withVat.branch((b) => b.when("big-orders_2.v~1", () => true))).not.toThrow();

    // Review finding: `.` and `..` pass the character class and are RELATIVE path segments.
    // `new URL()` rewrites `/branch/0/./transform/0` to `/branch/0/transform/0`, which misses
    // `.fetch()`'s trail regex and serves the PARENT chain's stage 0 - wrong data, no error.
    expect(() => withVat.branch((b) => b.when(".", () => true))).toThrow(/not usable in a route/);
    expect(() => withVat.branch((b) => b.when("..", () => true))).toThrow(/not usable in a route/);
  });

  it("gives the catch-all every item under broadcast, not only the unclaimed ones", () => {
    // The docstring claimed "every item no earlier arm claimed", which holds only in router mode:
    // broadcast means every MATCHING arm, and a catch-all's predicate accepts all of them.
    const routerMode = withVat.branch((b) => b.when("big", (o) => o.total > 200).otherwise("rest"));
    expect(routerMode(orders).rest.map((o) => o.id)).toEqual([1, 3]);

    const broadcastMode = withVat.branch((b) =>
      b
        .when("big", (o) => o.total > 200)
        .otherwise("rest")
        .broadcast(),
    );
    expect(broadcastMode(orders).rest.map((o) => o.id)).toEqual([1, 2, 3, 4]);
  });
});
