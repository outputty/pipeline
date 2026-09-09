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
    expect(typeof withVat.bind).toBe("function");
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
  it("refuses draining a pipeline that was given no input (Done-when 10, runtime half)", async () => {
    // `.toArray()` still EXISTS on `Pipeline` at this layer - the terminals leave it with `.from()`
    // at the enable layer, which is where Done-when 10's compile-time half lands as a `TS2339`.
    // Until then the source guard is what refuses it, so this pins the runtime behaviour.
    await expect(withVat.toArray()).rejects.toThrow(/no source/);
  });

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
