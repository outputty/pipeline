/**
 * A `Pipeline` holds its input TYPE and no data, so it IS the function (#90). Calling one pairs the
 * chain with an input and returns a `PipelineResult`, which is where the terminal ops live.
 *
 * Covers #90's Done-when 1, 2, 3, 8, 9, 10, 11 and 12. The `tsc` refusals (9, 10, 11) land as
 * `@ts-expect-error` lines; `TS2578: Unused '@ts-expect-error' directive` is the failure signal if
 * a refusal ever stops firing.
 */

import { describe, it, expect } from "vitest";
import { createHook } from "node:async_hooks";

import { Pipeline } from "@src/pipeline";
import { PipelineResult } from "@src/result";
import { SimpleContextManager } from "@src/context/simple";
import { runFixture, expectFixtureOk, lastJsonLine, FIXTURE_TIMEOUT } from "./helpers/fixtures";

/** Every chunk a pipeline yields, for the cases that assert a chunk BOUNDARY rather than items. */
async function chunksOf(result: { chunks(): AsyncIterable<unknown> }): Promise<unknown[]> {
  const out: unknown[] = [];
  for await (const chunk of result.chunks()) out.push(chunk);
  return out;
}

/** Counts every `Promise` created while `fn` runs, via `node:async_hooks`'s `PROMISE` resource
 * type - the same instrument `sync-mode.e2e.test.ts` uses, and for the same reason: patching
 * `queueMicrotask`/`process.nextTick` reports `0` even for real async work, so it would pass
 * vacuously. */
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

/** The canonical chain: no data, built once, reused by every test below. */
const withVat = new Pipeline<Order>().transform((t) =>
  t.map((o) => ({ ...o, total: Math.round(o.total * 1.2) })),
);

async function* asStream<T>(items: readonly T[]): AsyncGenerator<T> {
  for (const item of items) yield item;
}

describe("a Pipeline is callable and carries no data", () => {
  it("runs the same chain over two different inputs (Done-when 1)", () => {
    expect(
      withVat(ordersA)
        .toArray()
        .map((o) => o.total),
    ).toEqual([60, 360, 144, 1080]);
    expect(
      withVat(ordersB)
        .toArray()
        .map((o) => o.total),
    ).toEqual([480, 24]);
  });

  it("returns a plain array for a sync input, with no await (Done-when 1)", () => {
    const doubled = new Pipeline<number>().transform((t) =>
      t.map((x) => x * 2).filter((x) => x > 4),
    );
    const out: number[] = doubled([1, 2, 3, 4, 5]).toArray();
    expect(Array.isArray(out)).toBe(true);
    expect(out).toEqual([6, 8, 10]);
  });

  it("widens to a Promise when one callback is async (Done-when 2)", async () => {
    const doubled = new Pipeline<number>().transform((t) =>
      t.map(async (x) => x * 2).filter((x) => x > 4),
    );
    const pending: Promise<number[]> = doubled([1, 2, 3, 4, 5]).toArray();
    expect(typeof pending.then).toBe("function");
    await expect(pending).resolves.toEqual([6, 8, 10]);
  });

  it("widens to a Promise when the INPUT is async, on the same object (Done-when 2)", async () => {
    const sync: number[] = withVat(ordersA)
      .toArray()
      .map((o) => o.total);
    const viaStream = await withVat(asStream(ordersA)).toArray();
    expect(sync).toEqual([60, 360, 144, 1080]);
    expect(viaStream.map((o) => o.total)).toEqual(sync);
  });

  it("creates zero Promises draining a sync chain (Done-when 3)", () => {
    const doubled = new Pipeline<number>().transform((t) =>
      t.map((x) => x * 2).filter((x) => x > 4),
    );
    expect(countPromises(() => doubled([1, 2, 3, 4, 5]).toArray())).toBe(0);
  });

  it("is a real function, not merely callable (Done-when 8)", () => {
    expect(withVat).toBeInstanceOf(Function);
    expect(typeof withVat.call).toBe("function");
    expect(typeof withVat.apply).toBe("function");
  });

  it("passes directly where a function is expected (Done-when 8)", () => {
    const runners = [ordersA, ordersB].map((rows) => withVat(rows));
    expect(runners.map((r) => r.toArray().length)).toEqual([4, 2]);
  });

  it("keeps its class and its methods through the chain", () => {
    expect(withVat).toBeInstanceOf(Pipeline);
    expect(withVat.constructor.name).toBe("Pipeline");
    expect(withVat(ordersA)).toBeInstanceOf(PipelineResult);
  });
});

describe("a PipelineResult carries the terminals", () => {
  it("re-drains on every terminal, over an array (Done-when 12)", () => {
    const r = withVat(ordersA);
    expect(r.first(2).map((o) => o.id)).toEqual([1, 2]);
    expect(r.toArray().map((o) => o.id)).toEqual([1, 2, 3, 4]);
    const seen: number[] = [];
    r.forEach((o) => void seen.push(o.id));
    expect(seen).toEqual([1, 2, 3, 4]);
    expect([...r].map((o) => o.id)).toEqual([1, 2, 3, 4]);
  });

  it("yields [] on a second drain of a spent generator, by decision (Done-when 12)", async () => {
    // Replayability cannot be detected: the `src[Symbol.iterator]() === src` test reports a
    // ReadableStream as replayable when it is not, and running the test locks the stream so the
    // FIRST drain throws. So no detection is attempted, and a spent source reads empty.
    const r = withVat(asStream(ordersA));
    expect((await r.toArray()).length).toBe(4);
    expect(await r.toArray()).toEqual([]);
  });

  it("runs consume() for side effects and collects nothing", () => {
    const seen: number[] = [];
    const tapped = new Pipeline<number>().transform((t) => t.map((x) => (seen.push(x), x)));
    const out: void = tapped([1, 2, 3]).consume();
    expect(out).toBeUndefined();
    expect(seen).toEqual([1, 2, 3]);
  });

  it("keeps forEach sync for a sync callback and async for an async one", async () => {
    const seen: number[] = [];
    const plain = new Pipeline<number>();
    const syncReturn: void = plain([1, 2, 3]).forEach((x) => void seen.push(x));
    expect(syncReturn).toBeUndefined();
    expect(seen).toEqual([1, 2, 3]);

    const asyncSeen: number[] = [];
    const pending: Promise<void> = plain([1, 2, 3]).forEach(async (x) => {
      await Promise.resolve();
      asyncSeen.push(x);
    });
    expect(typeof pending.then).toBe("function");
    await pending;
    expect(asyncSeen).toEqual([1, 2, 3]);
  });

  it("iterates a sync result synchronously and asynchronously", async () => {
    const doubled = new Pipeline<number>().transform((t) => t.map((x) => x * 2));
    expect([...doubled([1, 2, 3])]).toEqual([2, 4, 6]);

    const collected: number[] = [];
    for await (const x of doubled([1, 2, 3])) collected.push(x);
    expect(collected).toEqual([2, 4, 6]);
  });

  it("iterates an async result asynchronously, and refuses sync spreading at runtime", async () => {
    const collected: number[] = [];
    for await (const o of withVat(asStream(ordersA))) collected.push(o.total);
    expect(collected).toEqual([60, 360, 144, 1080]);

    const asyncResult = withVat(asStream(ordersA)) as unknown as Iterable<Order>;
    expect(() => [...asyncResult]).toThrow(/not a sync iterable/);
  });

  it("yields ITEMS, not chunks, from for-await", async () => {
    const collected: unknown[] = [];
    for await (const x of new Pipeline<number>().buffer(2)([1, 2, 3])) collected.push(x);
    expect(collected).toEqual([1, 2, 3]);
  });
});

describe("the compiler refuses what the split forbids", () => {
  it("refuses chaining back off a result (Done-when 9)", () => {
    const r = withVat(ordersA);
    // TS2339: Property 'transform' does not exist on type 'PipelineResult<Order, "sync">'.
    // @ts-expect-error a result is not a pipeline; compose the chain before the data arrives
    expect(() => r.transform((t: unknown) => t)).toThrow();
  });

  it("refuses spreading an async result (Done-when 11)", () => {
    // TS2488: Type 'PipelineResult<Order, "async">' must have a '[Symbol.iterator]()' method that
    // returns an iterator.
    // @ts-expect-error an async result is not a sync iterable
    expect(() => [...withVat(asStream(ordersA))]).toThrow();
  });
});

describe("L6 review findings, each reproduced before it was fixed", () => {
  it("keeps .buffer()'s position in a chain composed before the input", async () => {
    // Before: the source-less arm recorded only the SIZE, which `fromSource` applied to the source
    // cut - so a `.buffer()` written after a stage took effect before it. Measured: chunks came out
    // `[[1,1,2,2],[3,3,4,4]]` against `[[1,1],[2,2],[3,3],[4,4]]` for the same chain after
    // `.from()`.
    const cut = await chunksOf(
      new Pipeline<number>().transform((t) => t.flatMap((x) => [x, x])).buffer(2)([1, 2, 3, 4]),
    );
    expect(cut).toEqual([
      [1, 1],
      [2, 2],
      [3, 3],
      [4, 4],
    ]);
  });

  it("still cuts the source when .buffer() comes before every stage", async () => {
    const cut = await chunksOf(new Pipeline<number>().buffer(2)([1, 2, 3, 4, 5]));
    expect(cut).toEqual([[1, 2], [3, 4], [5]]);
  });

  it("gives each call its own context, and keeps a caller-supplied one", () => {
    // Before: one default manager was shared by every call of a reusable chain, so a counting map
    // reported `6` after two three-item calls instead of `3` per run.
    const counting = new Pipeline<number>().transform((t) =>
      t.map((x, ctx) => {
        ctx?.set("n", ((ctx.get("n") as number) ?? 0) + 1);
        return x;
      }),
    );
    counting([1, 2, 3]).toArray();
    counting([1, 2, 3]).toArray();
    expect(counting.contextManager.get("n")).toBeUndefined();

    // A manager the caller named is theirs, and still accumulates across calls by design (#31).
    const mine = new SimpleContextManager();
    const owned = new Pipeline<number>({ context: mine }).transform((t) =>
      t.map((x, ctx) => {
        ctx?.set("n", ((ctx.get("n") as number) ?? 0) + 1);
        return x;
      }),
    );
    owned([1, 2, 3]).toArray();
    owned([1, 2, 3]).toArray();
    expect(mine.get("n")).toBe(6);
  });

  it(
    "constructs where code generation from strings is banned",
    async () => {
      // Before: `class Pipeline extends Function` called `super()`, which runs
      // `CreateDynamicFunction`. Measured under `node --disallow-code-generation-from-strings`:
      // `EvalError: Code generation from strings disallowed for this context` on the FIRST
      // `new Pipeline<number>()`. That contradicted the package's own runtime-neutrality claim, so the
      // prototype is reparented onto `Function.prototype` once instead.
      //
      // The ban is a process-level flag, so this runs in a child process. It is the real assertion;
      // the in-process checks below only say what the reparenting buys.
      const fixture = await runFixture("__tests__/fixtures/no-codegen.ts", [
        "--disallow-code-generation-from-strings",
      ]);
      expectFixtureOk(fixture);
      expect(lastJsonLine(fixture)).toEqual({
        values: [2, 4, 6],
        isFunction: true,
        hasCall: true,
      });

      const p = new Pipeline<number>();
      expect(p).toBeInstanceOf(Function);
      expect(typeof p.call).toBe("function");
    },
    FIXTURE_TIMEOUT,
  );

  it("leaves no unhandled rejection when sync iteration refuses an async result", async () => {
    // Before: `[Symbol.iterator]` started the drain, then threw and abandoned its promise -
    // `UNHANDLED REJECTION: boom` killed the process under Node's default.
    const failing = new Pipeline<number>().transform((t) =>
      t.map((x) => {
        if (x === 1) throw new Error("boom");
        return x;
      }),
    );
    let unhandled: unknown;
    const record = (reason: unknown): void => void (unhandled = reason);
    process.on("unhandledRejection", record);
    try {
      expect(() => [...(failing(asStream([1, 2, 3])) as unknown as Iterable<number>)]).toThrow(
        /not a sync iterable/,
      );
      await new Promise((r) => setTimeout(r, 60));
    } finally {
      process.off("unhandledRejection", record);
    }
    expect(unhandled).toBeUndefined();
  });

  it("captures a reduce seed once, so a mutable seed accumulates across calls", () => {
    // Not a fix - a recorded constraint. `initial` is captured when the stage is composed, which is
    // invisible for an immutable seed and wrong for a mutable one. `.reduce()`'s own docstring says
    // to fold into a fresh value instead; this pins the behaviour so the docs cannot drift from it.
    const mutating = new Pipeline<number>().reduce(
      (acc: number[], x: number) => (acc.push(x), acc),
      [] as number[],
    );
    expect(mutating([1, 2, 3]).toArray()).toEqual([[1, 2, 3]]);
    expect(mutating([1, 2, 3]).toArray()).toEqual([[1, 2, 3, 1, 2, 3]]);

    const immutable = new Pipeline<number>().reduce(
      (acc: number[], x: number) => [...acc, x],
      [] as number[],
    );
    expect(immutable([1, 2, 3]).toArray()).toEqual([[1, 2, 3]]);
    expect(immutable([1, 2, 3]).toArray()).toEqual([[1, 2, 3]]);
  });

  it("shadows Function.prototype.apply, the one break in substitutability", () => {
    const doubled = new Pipeline<number>().transform((t) => t.map((x) => x * 2));
    // `.call` reaches the call signature and produces a result.
    const viaCall = (doubled as unknown as { call: (t: unknown, i: number[]) => unknown }).call(
      null,
      [1, 2, 3],
    );
    expect(viaCall).toBeInstanceOf(PipelineResult);
    // `.apply` reaches `Pipeline.apply()`, the stage method, and produces a pipeline.
    const viaApply = (doubled as unknown as { apply: (t: unknown, a: unknown[]) => unknown }).apply(
      null,
      [[1, 2, 3]],
    );
    expect(viaApply).toBeInstanceOf(Pipeline);
    // The wrapper the docstring recommends works everywhere.
    const wrapped = (input: number[]): number[] => doubled(input).toArray();
    expect(wrapped.apply(null, [[1, 2, 3]])).toEqual([2, 4, 6]);
  });
});

describe("L8 review findings, each reproduced before it was fixed", () => {
  it("keeps a trailing .buffer() from re-cutting the source", () => {
    // Before: the deferred arm set `chunkSize` unconditionally, and `fromSource` reads it for the
    // SOURCE cut - so a `.buffer()` written after a stage re-cut the source retroactively.
    // Measured: `.transform(t => t.reduce(sum, 0)).buffer(3)` over `[1..6]` gave `[6, 15]` where
    // the same chain without the trailing `.buffer(3)` gave `[21]`.
    const folded = new Pipeline<number>().transform((t) =>
      t.reduce((a: number, x: number) => a + x, 0),
    );
    expect(folded([1, 2, 3, 4, 5, 6]).toArray()).toEqual([21]);
    expect(folded.buffer(3)([1, 2, 3, 4, 5, 6]).toArray()).toEqual([21]);

    // A `.buffer()` ahead of every stage still cuts the source, which is the case that needs it.
    const cut = new Pipeline<number>()
      .buffer(3)
      .transform((t) => t.reduce((a: number, x: number) => a + x, 0));
    expect(cut([1, 2, 3, 4, 5, 6]).toArray()).toEqual([6, 15]);
  });

  it("gives .branch()'s runner its input on an async chain too", async () => {
    // Before: `BranchRunner` keyed on Mode while the runtime keyed on boundness. With `.from()`
    // gone every pipeline is deferred, so an async chain typed the runner `() => Promise<R>` -
    // `TS2554` on the call that works, and `no input:` thrown by the call that compiled.
    const asyncChain = new Pipeline<number>().transform((t) => t.map(async (x) => x * 2));
    const split = asyncChain.branch({ big: { predicate: (x: number) => x > 2 } });
    expect(await split([1, 2, 3])).toEqual({ big: [4, 6] });

    const runner = new Pipeline<number>().branch({ all: { predicate: () => true } });
    await expect((runner as unknown as () => Promise<unknown>)()).rejects.toThrow(/no input/);
  });

  it("runs a .local() region once per terminal, not twice", async () => {
    // Before: each async terminal bound the chain twice - once to test for a sync chunk stream,
    // once inside its own async arm - so a user's `build` callback ran twice per call.
    let builds = 0;
    const chain = new Pipeline<number>().local((p) => {
      builds++;
      return p.transform((t) => t.map(async (x) => x * 2));
    });
    await chain([1, 2, 3]).toArray();
    expect(builds).toBe(1);
  });

  it("closes a sync source that a terminal stopped reading early", () => {
    // Before: `drainSync`'s early exit abandoned the iterator, so a generator's `finally` never
    // ran - a file handle or cursor held by a sync source leaked on `.first()` alone.
    let closed = false;
    function* source(): Generator<number> {
      try {
        yield 1;
        yield 2;
        yield 3;
      } finally {
        closed = true;
      }
    }
    expect(new Pipeline<number>().buffer(1)(source()).first(1)).toEqual([1]);
    expect(closed).toBe(true);
  });

  it("shows the same chunks whichever engine folded them", async () => {
    // Before: the sync fold's deferred arms yielded unguarded, where the async engine guards on
    // length - `[[],[],[],[15]]` against `[[15]]` for the identical chain.
    const folded = new Pipeline<number>()
      .buffer(2)
      .transform((t) => t.map(async (x) => x))
      .reduce((a: number, x: number) => a + x, 0);

    const viaSync: number[][] = [];
    for await (const chunk of folded([1, 2, 3, 4, 5]).chunks()) viaSync.push(chunk);

    async function* stream(): AsyncGenerator<number> {
      for (const x of [1, 2, 3, 4, 5]) yield x;
    }
    const viaAsync: number[][] = [];
    for await (const chunk of folded(stream()).chunks()) viaAsync.push(chunk);

    expect(viaSync).toEqual(viaAsync);
    expect(viaSync).toEqual([[15]]);
  });
});
