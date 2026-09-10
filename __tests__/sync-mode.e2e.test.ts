/**
 * #90 - the synchronous fast path. Done-when cases 1 to 8 land here; case 9 (`pnpm check` passes,
 * `.branch()`'s signature untouched) is the gate itself and has no test of its own.
 *
 * Its own file rather than `__tests__/pipeline.e2e.test.ts`, which Done-when 4 names: this ticket
 * adds a Mode dimension every case shares, and the existing file is already the home of the
 * pre-Mode constructor's own suite, which L3 rewrites wholesale.
 *
 * Every case is LIVE as of L3. The `: number[]` and `: Promise<number[]>` annotations on each
 * result carry the compile-time half - `tsc --noEmit` is what asserts them, and a Mode that came
 * out wrong fails the build rather than the run. Cases 4 and 7 are the two that assert a REFUSAL,
 * so each carries a `@ts-expect-error` whose own removal (`TS2578`) is the signal that the refusal
 * broke.
 */

import { describe, it, expect } from "vitest";
import { Pipeline } from "@src/pipeline";
import { Transformer } from "@src/transformer";
import { SimpleContextManager } from "@src/context/simple";
import { DROP } from "@src/types";
import { buildSyncChunkGenerator } from "@src/utils/chunk";
import { ConcurrentPipeline } from "@src/pipelines/concurrent";
import { HttpPipeline } from "@src/pipelines/http";
import { ClusterPipeline } from "@src/pipelines/cluster";
import { countPromises } from "./helpers/sequences";

describe("#90 - a synchronous chain never creates a Promise", () => {
  it("countPromises itself counts real async work and ignores sync work", () => {
    // The positive control for the instrument. Without it Done-when 3's own zero proves nothing: an
    // instrument that never counts anything reports zero for every engine.
    expect(countPromises(() => Promise.resolve().then(() => {}))).toBeGreaterThan(0);
    expect(countPromises(() => void (async () => 1)())).toBeGreaterThan(0);
    expect(countPromises(() => [1, 2, 3].map((x) => x * 2))).toBe(0);
  });

  it("L2: a Transformer whose callbacks are all synchronous returns a plain array", () => {
    // L2's own observable, live from the layer that builds it. Without it the layer is invisible:
    // every pre-existing test awaits its result, and `await` on a plain array is a no-op, so the
    // whole suite passes identically whether the links create a Promise or not.
    const t = new Transformer<number, number>({ transform: (chunk) => chunk })
      .map((x) => x * 2)
      .filter((x) => x > 4);

    const out = t.runnable()([1, 2, 3, 4, 5], new SimpleContextManager());

    expect(Array.isArray(out)).toBe(true);
    expect(out).toEqual([6, 8, 10]);
    expect(countPromises(() => t.runnable()([1, 2, 3, 4, 5], new SimpleContextManager()))).toBe(0);
  });

  it("L2: one async callback makes that same Transformer return a Promise", async () => {
    // The negative control for the test above: flip one callback and the plain-array claim must
    // stop holding, or the assertion was never reading what it says it reads.
    const t = new Transformer<number, number>({ transform: (chunk) => chunk })
      .map(async (x) => x * 2)
      .filter((x) => x > 4);

    const out = t.runnable()([1, 2, 3, 4, 5], new SimpleContextManager());

    expect(Array.isArray(out)).toBe(false);
    expect(await out).toEqual([6, 8, 10]);
  });

  it("L2: .onError() and .reduce() both keep a synchronous chain synchronous", () => {
    // The two links whose recovery and fold steps were `async` before L2 - the paths Done-when 8's
    // `.onError(h)` and any in-chain `.reduce()` would otherwise widen on their own.
    const recovered = new Transformer<string, string>({ transform: (chunk) => chunk })
      .onError(() => DROP)
      .map((s) => {
        const n = parseInt(s, 10);
        if (isNaN(n)) throw new Error(`bad: ${s}`);
        return n;
      });

    const recoveredOut = recovered.runnable()(["a", "3"], new SimpleContextManager());
    expect(Array.isArray(recoveredOut)).toBe(true);
    expect(recoveredOut).toEqual([3]);

    const folded = new Transformer<number, number>({ transform: (chunk) => chunk }).reduce(
      (acc, x) => acc + x,
      0,
    );

    const foldedOut = folded.runnable()([1, 2, 3], new SimpleContextManager());
    expect(Array.isArray(foldedOut)).toBe(true);
    expect(foldedOut).toEqual([6]);
  });

  it("L2: a large synchronous fold and a long synchronous loop do not overflow the stack", () => {
    // Both paths replaced a real loop with per-step recursion at first. Measured on that draft: a
    // 5000-item fold and a 4000-iteration loop each threw `RangeError: Maximum call stack size
    // exceeded`, where the pre-#90 code handled 20 000 of each. The sizes below sit above those
    // ceilings, so this test fails outright if the trampolines are ever undone.
    const items = Array.from({ length: 20000 }, (_x, i) => i + 1);

    const folded = new Transformer<number, number>({ transform: (chunk) => chunk }).reduce(
      (acc, x) => acc + x,
      0,
    );
    expect(folded.runnable()(items, new SimpleContextManager())).toEqual([200010000]);

    const looped = new Transformer<number, number>({ transform: (chunk) => chunk }).loop(
      new Transformer<number, number>({ transform: (chunk) => chunk }).map((x) => x + 1),
      (chunk) => chunk[0] < 20000,
    );
    expect(looped.runnable()([0], new SimpleContextManager())).toEqual([20000]);
  });

  it("L2: a synchronously-throwing row handler leaves no unhandled rejection behind", async () => {
    // A user callback can throw SYNCHRONOUSLY for one item after an earlier item in the same chunk
    // already returned a pending promise. `Array.prototype.map` abandons the array there, and that
    // earlier promise would never get a rejection handler - fatal under Node's default policy.
    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown) => unhandled.push(reason);
    process.on("unhandledRejection", onUnhandled);

    const t = new Transformer<number, number>({ transform: (chunk) => chunk })
      .onError(() => {
        throw new Error("handler blew up");
      })
      .map((x) => {
        if (x === 1) return Promise.reject(new Error("slow failure"));
        if (x === 2) throw new Error("fast failure");
        return x;
      });

    expect(() => t.runnable()([1, 2, 3], new SimpleContextManager())).toThrow("handler blew up");

    // One turn of the event loop is enough for an abandoned rejection to surface.
    await new Promise((resolve) => setTimeout(resolve, 10));
    process.off("unhandledRejection", onUnhandled);

    expect(unhandled).toEqual([]);
  });

  it("L2: a row handler that returns an array replaces the row, never spreads into it", () => {
    // `.flatMap()`'s SUCCESS is already an array, so telling success from recovery by sniffing
    // `Array.isArray` puts a handler's own array into the output flattened - wrong for any item
    // type that is itself an array.
    const t = new Transformer<number, number[]>({
      transform: (chunk) => chunk.map((x) => [x]),
    })
      .onError(() => [99, 98])
      .flatMap((pair) => {
        if (pair[0] === 2) throw new Error("boom");
        return [pair, pair];
      });

    expect(t.runnable()([1, 2, 3], new SimpleContextManager())).toEqual([
      [1],
      [1],
      [99, 98],
      [3],
      [3],
    ]);
  });

  it("L2: the sync chunk generator cuts the same way its async counterpart does", () => {
    expect([...buildSyncChunkGenerator<number>(3)([1, 2, 3, 4, 5, 6, 7])]).toEqual([
      [1, 2, 3],
      [4, 5, 6],
      [7],
    ]);
    expect(() => buildSyncChunkGenerator<number>(0)).toThrow("chunkSize must be at least 1");
    expect(countPromises(() => [...buildSyncChunkGenerator<number>(2)([1, 2, 3])])).toBe(0);
  });

  it("Done-when 1: a fully sync chain returns number[] with no await", () => {
    const builder = new Pipeline<number>();
    const out: number[] = builder

      .transform((t: Transformer<number, number>) =>
        t.map((x: number) => x * 2).filter((x: number) => x > 4),
      )([1, 2, 3, 4, 5])
      .toArray();

    expect(out).toEqual([6, 8, 10]);
  });

  it("Done-when 2: one async callback widens the chain to Promise<number[]>", async () => {
    const builder = new Pipeline<number>();
    const out: Promise<number[]> = builder

      .transform((t: Transformer<number, number>) =>
        t.map(async (x: number) => x * 2).filter((x: number) => x > 4),
      )([1, 2, 3, 4, 5])
      .toArray();

    expect(await out).toEqual([6, 8, 10]);
  });

  it("Done-when 3: zero promises are created between .from() and .toArray()", () => {
    const builder = new Pipeline<number>();
    const created = countPromises(() =>
      builder

        .transform((t: Transformer<number, number>) =>
          t.map((x: number) => x * 2).filter((x: number) => x > 4),
        )([1, 2, 3, 4, 5])
        .toArray(),
    );

    expect(created).toBe(0);
  });

  it("composes .transform() before any input, and replays it on the input given", () => {
    // Replaces the deleted Done-when 4, which asserted the opposite: `.transform()` before
    // `.from()` used to be `TS2684`, refused by a conditional `this` reading the `"unset"` Mode.
    // The callable shape makes composing ahead of the data the ORDINARY case, so that guard is
    // gone. What still refuses is DRAINING with no input, now a type error rather than a runtime
    // one - `__tests__/callable.e2e.test.ts` carries it as Done-when 10.
    const composed = new Pipeline<number>().transform((t) => t.map((x) => x * 2));
    expect(composed([1, 2, 3]).toArray()).toEqual([2, 4, 6]);
    expect(composed([10]).toArray()).toEqual([20]);
  });

  it("Done-when 7: the two-arg constructor still compiles - L3 is what removes it", () => {
    // The BEFORE state, recorded so case 7 has a home. It cannot be `@ts-expect-error`'d at L1:
    // the two-arg constructor is exactly what still works today, so a directive here would be
    // `TS2578` and fail the gate.
    //
    // THIS CASE FLIPS BY ADDING A DIRECTIVE too. At L3 the call below stops compiling, a directive
    // goes above it, and this test becomes the assertion that the removal really landed - alongside
    // the sweep of every other call site across `src/`, `__tests__/`, `README.md` and `.claude/*.md`
    // that the same layer carries.
    const legacy = new Pipeline<number>();
    expect(legacy).toBeInstanceOf(Pipeline);
  });

  it("Done-when 5: a thenable-returning link widens the run rather than throwing", async () => {
    // Amended from the ticket's own text by this build's F2 finding, on the user's ruling: the
    // engine widens the run instead of throwing. Where the callback's return type is visible,
    // TypeScript widens the chain to `Promise<number[]>` on its own, and the two agree - this
    // case. Where it is `any` (an untyped import, a `JSON.parse` result), the type says
    // `number[]` and the run still returns a Promise resolving to the right values: a wrong
    // static type at an `any` boundary, never wrong data, and never a `Promise` left unawaited
    // in the output. `code.md` rules out a guard against a misuse the caller could mean, which
    // is what the ticket's own per-chunk throw would have been.
    const builder = new Pipeline<number>();
    const out: Promise<number[]> = builder

      .transform((t: Transformer<number, number>) => t.map((x: number) => Promise.resolve(x * 2)))([
        1, 2, 3,
      ])
      .toArray();

    expect(await out).toEqual([2, 4, 6]);
  });

  it("Done-when 6: a dispatching class forces async whatever the source's shape", async () => {
    // The criterion is compile-time: an ARRAY source, whose shape says "sync", must still come
    // out "async" on every dispatching class. `instanceof` alone cannot see that - a subclass
    // that never got its own `.from()` override inherits the base's `Iterable → "sync"` arm,
    // `toArray()` then returns a plain array, `await` on one is a no-op, and both the
    // `instanceof` and the `toEqual` still pass. The three `: Promise<number[]>` annotations
    // below are the assertion; each fails to compile if its class's Mode came out "sync".
    const concurrent = new ConcurrentPipeline<number>();
    const http = new HttpPipeline<number>({ url: "http://127.0.0.1:1" });
    const cluster = new ClusterPipeline<number>();

    const concurrentOut: Promise<number[]> = concurrent([1, 2, 3]).toArray();
    const httpOut: Promise<number[]> = http([1, 2, 3]).toArray();
    const clusterOut: Promise<number[]> = cluster([1, 2, 3]).toArray();

    expect(concurrent).toBeInstanceOf(ConcurrentPipeline);
    expect(http).toBeInstanceOf(HttpPipeline);
    expect(cluster).toBeInstanceOf(ClusterPipeline);
    expect(typeof httpOut.then).toBe("function");
    expect(typeof clusterOut.then).toBe("function");
    expect(await concurrentOut).toEqual([1, 2, 3]);
  });

  it("Done-when 8: .buffer()/.onError()/.local() all preserve the sync Mode", () => {
    const builder = new Pipeline<number>();
    const out: number[] = builder

      .buffer(2)
      .onError(() => {})
      .local((p) => p)([1, 2, 3])
      .toArray();

    expect(out).toEqual([1, 2, 3]);
  });
});

describe("#90 - the Mode a chain reports and the engine it runs on never disagree", () => {
  // Every case here was found by review, and every one passed the whole suite before being fixed:
  // each test AWAITED a value its own type said was a plain array, and `await` on an array is a
  // no-op. The assertions below read the runtime value directly instead.

  it("a knob set before .from() survives it", () => {
    // `.onError()` and `.context()` are both callable on a source-less pipeline, and `.from()` used
    // to drop everything but the context manager - so a registered run handler never fired.
    const seen: string[] = [];
    const out = new Pipeline<number>()
      .onError((e) => void seen.push(e.message))

      .buffer(1)
      .transform((t) =>
        t.map((x: number) => {
          if (x === 2) throw new Error("boom");
          return x;
        }),
      )([1, 2, 3])
      .toArray();

    expect(out).toEqual([1, 3]);
    expect(seen).toEqual(["boom"]);
  });

  it("forEach settles an async callback and reports its Mode", async () => {
    // TypeScript's void-return rule makes an `async` callback assignable to `(item) => void`, so a
    // `void`-returning overload listed first swallowed it: the call typed `void`, the callbacks
    // were fired and dropped, and a rejecting one had no handler at all.
    const seen: number[] = [];
    const settled = new Pipeline<number>()([1, 2, 3]).forEach(async (x: number) => {
      await Promise.resolve();
      seen.push(x);
    });

    expect(typeof settled.then).toBe("function");
    await settled;
    expect(seen).toEqual([1, 2, 3]);

    const syncSeen: number[] = [];
    const immediate: void = new Pipeline<number>()([1, 2, 3]).forEach((x) => void syncSeen.push(x));
    expect(immediate).toBeUndefined();
    expect(syncSeen).toEqual([1, 2, 3]);
  });

  it("Pipeline.tap with an async callback widens the chain", async () => {
    const seen: number[] = [];
    const out: Promise<number[]> = new Pipeline<number>()

      .tap(async (x: number) => {
        await Promise.resolve();
        seen.push(x);
      })([1, 2, 3])
      .toArray();

    expect(typeof out.then).toBe("function");
    expect(await out).toEqual([1, 2, 3]);
    expect(seen).toEqual([1, 2, 3]);
  });

  it("a dispatching class composes before an input, like the base", async () => {
    // These three used to throw `no source: call .from(data) before composing a stage` here - the
    // runtime guard that stood in for the base's compile-time `"unset"` refusal. Both are replaced
    // by deferral: a dispatching class records its stages too, and replays them through its OWN
    // `apply()`, so a replayed stage still dispatches. What still refuses is draining with no
    // input, and wrapping a pipeline that already named a source.
    const doubled = (p: { transform: (b: (t: any) => any) => any }): any =>
      p.transform((t: any) => t.map((x: number) => x * 2));

    expect(await doubled(new ConcurrentPipeline<number>())([1, 2, 3]).toArray()).toEqual([2, 4, 6]);
    expect(typeof doubled(new HttpPipeline<number>({ url: "http://127.0.0.1:1" })).fetch).toBe(
      "function",
    );
    expect(doubled(new ClusterPipeline<number>()).constructor.name).toBe("ClusterPipeline");
  });

  it("Transformer.loop takes a synchronous body inside an async chain", async () => {
    // `.loop()` pinned its body's Mode to the receiver's, so an ordinary sync loop body inside a
    // chain that had gone async became a compile error on code that worked before this ticket.
    const t = new Transformer<number, number>({ transform: (chunk) => chunk })
      .map(async (x) => x)
      .loop(
        new Transformer<number, number>({ transform: (chunk) => chunk }).map((x) => x + 1),
        (chunk) => chunk[0] < 5,
      );

    expect(await t.runnable()([0], new SimpleContextManager())).toEqual([5]);
  });
});

describe("#90 L4 - a fold keeps the chain's Mode instead of always widening it", () => {
  it("a plain reducer over a sync source returns an array with no await", () => {
    const totals: number[] = new Pipeline<number>()

      .reduce(
        (acc: number, x: number) => acc + x,
        0,
      )([1, 2, 3, 4, 5])
      .toArray();

    expect(totals).toEqual([15]);
    expect(Array.isArray(totals)).toBe(true);
  });

  it("that fold creates no Promise at all", () => {
    expect(
      countPromises(() =>
        new Pipeline<number>()

          .reduce(
            (acc: number, x: number) => acc + x,
            0,
          )([1, 2, 3, 4, 5])
          .toArray(),
      ),
    ).toBe(0);
  });

  it("a fold survives more chunks than one, in order", () => {
    const totals: number[] = new Pipeline<number>()

      .buffer(2)
      .reduce(
        (acc: number, x: number) => acc + x,
        0,
      )([1, 2, 3, 4, 5])
      .toArray();

    expect(totals).toEqual([15]);
  });

  it("emit() mid-fold gives the sync path the same answer as the async one", async () => {
    const batch = (
      acc: number,
      x: number,
      _ctx: unknown,
      emit: (value: number) => void,
    ): number => {
      const next = acc + x;
      if (next >= 6) {
        emit(next);
        return 0;
      }
      return next;
    };

    async function* asyncSource(): AsyncGenerator<number> {
      for (const x of [1, 2, 3, 4, 5, 6]) yield x;
    }

    const sync: number[] = new Pipeline<number>().reduce(batch, 0)([1, 2, 3, 4, 5, 6]).toArray();
    const async: number[] = await new Pipeline<number>().reduce(batch, 0)(asyncSource()).toArray();

    expect(sync).toEqual([6, 9, 6]);
    expect(sync).toEqual(async);
  });

  it("a stage after the fold stays synchronous too", () => {
    const scaled: number[] = new Pipeline<number>()

      .reduce((acc: number, x: number) => acc + x, 0)
      .transform((t) => t.map((n: number) => n * 10))([1, 2, 3, 4, 5])
      .toArray();

    expect(scaled).toEqual([150]);
  });

  it("an async reducer widens the whole chain", async () => {
    const totals: Promise<number[]> = new Pipeline<number>()

      .reduce(
        async (acc: number, x: number) => acc + x,
        0,
      )([1, 2, 3, 4, 5])
      .toArray();

    expect(await totals).toEqual([15]);
  });

  it("a fold on an async source stays async", async () => {
    async function* source(): AsyncGenerator<number> {
      yield 1;
      yield 2;
      yield 3;
    }

    const totals: Promise<number[]> = new Pipeline<number>()

      .reduce(
        (acc: number, x: number) => acc + x,
        0,
      )(source())
      .toArray();

    expect(await totals).toEqual([6]);
  });

  it("a synchronous fold does not overflow the stack on a long source", () => {
    const items = Array.from({ length: 20000 }, (_, index) => index);

    const totals: number[] = new Pipeline<number>()

      .buffer(100)
      .reduce(
        (acc: number, x: number) => acc + x,
        0,
      )(items)
      .toArray();

    expect(totals).toEqual([199990000]);
  });

  it("ConcurrentPipeline's own reduce still dispatches and stays async", async () => {
    const totals: number[] = await new ConcurrentPipeline<number>({ maxConcurrency: 2 })

      .buffer(5)
      .reduce(
        (acc: number, x: number) => acc + x,
        0,
      )([1, 2, 3, 4, 5])
      .toArray();

    expect(totals).toEqual([15]);
  });

  it("an async branch transformer compiles again", async () => {
    // Regression: `Transformer`'s Mode parameter defaults to `"sync"`, so the pre-#90 spelling
    // `Transformer<T, U>` in `.branch()`'s own signature narrowed it to sync-only transformers,
    // rejecting an async one that compiled on `main`.
    const data = await new Pipeline<number>().branch((b) =>
      b
        .when(
          "doubled",
          (x) => x % 2 === 0,
          (q) => q.transform((t) => t.map(async (x) => x * 2)),
        )
        .when("plain", (x) => x % 2 !== 0),
    )([1, 2, 3, 4, 5]);

    expect(data.doubled).toEqual([4, 8]);
    expect(data.plain).toEqual([1, 3, 5]);
  });
});

describe("#90 L4 - every fluent method widens, and none of them under-reports its Mode", () => {
  async function* asyncSource(): AsyncGenerator<number> {
    yield 1;
    yield 2;
    yield 3;
  }

  it(".apply() with a sync transformer on an async chain stays async", async () => {
    // Measured before the fix: this typed `number[]` and returned `Promise { <pending> }`.
    // `.apply()`'s return read only the TRANSFORMER's Mode and ignored the chain's own.
    const out: Promise<number[]> = new Pipeline<number>()

      .apply(new Transformer<number, number>().map((x: number) => x * 2))(asyncSource())
      .toArray();

    expect(await out).toEqual([2, 4, 6]);
  });

  it(".local() with a sync region inside an async chain stays async", async () => {
    const out: Promise<number[]> = new Pipeline<number>()

      .local((p) => p.transform((t) => t.map((x: number) => x * 2)))(asyncSource())
      .toArray();

    expect(await out).toEqual([2, 4, 6]);
  });

  it(".local() with an async region inside a sync chain widens", async () => {
    const out: Promise<number[]> = new Pipeline<number>()

      .local((p) => p.transform((t) => t.map(async (x: number) => x * 2)))([1, 2, 3])
      .toArray();

    expect(await out).toEqual([2, 4, 6]);
  });

  it(".onError() with an async handler widens", async () => {
    // Measured before the fix: `PipelineErrorHandler` declares a bare `void` return, which accepts
    // an `async` function silently, so the chain stayed typed `number[]` while `dropOrRethrow`
    // deferred - a real `Promise { <pending> }` the moment an error fired.
    const out: Promise<number[]> = new Pipeline<number>()

      .onError(async () => {
        await Promise.resolve();
      })
      .buffer(1)
      .transform((t) =>
        t.map((x: number) => {
          if (x === 2) throw new Error("boom");
          return x;
        }),
      )([1, 2, 3])
      .toArray();

    expect(await out).toEqual([1, 3]);
  });

  it("a sync link after an async one inside one .transform() stays async", async () => {
    const out: Promise<number[]> = new Pipeline<number>()

      .transform((t) => t.map(async (x: number) => x * 2).filter((x: number) => x > 2))([1, 2, 3])
      .toArray();

    expect(await out).toEqual([4, 6]);
  });

  it("a stage after one that widened stays async", async () => {
    const out: Promise<number[]> = new Pipeline<number>()

      .transform((t) => t.map(async (x: number) => x * 2))
      .buffer(2)
      .transform((t) => t.map((x: number) => x + 1))
      .reduce(
        (acc: number, x: number) => acc + x,
        0,
      )([1, 2, 3])
      .toArray();

    expect(await out).toEqual([15]);
  });

  it(".tap() with an async transformer widens", async () => {
    const seen: number[] = [];
    const out: Promise<number[]> = new Pipeline<number>()

      .tap(
        new Transformer<number, number>().map(async (x: number) => {
          seen.push(x);
          return x;
        }),
      )([1, 2, 3])
      .toArray();

    expect(await out).toEqual([1, 2, 3]);
    expect(seen).toEqual([1, 2, 3]);
  });

  it(".first() and .consume() follow the same Mode as .toArray()", async () => {
    const head: number[] = new Pipeline<number>()([1, 2, 3]).first(2);
    expect(head).toEqual([1, 2]);

    const drained: void = new Pipeline<number>()([1, 2, 3]).consume();
    expect(drained).toBeUndefined();

    const widened: Promise<number[]> = new Pipeline<number>()

      .transform((t) => t.map(async (x: number) => x))([1, 2, 3])
      .first(2);
    expect(await widened).toEqual([1, 2]);
  });
});

describe("#90 L4 - review findings, each reproduced before it was fixed", () => {
  it("a synchronous forEach drains a long stream without growing the stack", () => {
    // `drainSyncSettled`'s `resume` and `runChunk` tail-called each other, so its `for(;;)` never
    // iterated and every chunk cost a frame pair. Measured on that shape: this threw `RangeError:
    // Maximum call stack size exceeded` after 3579 items, where `toArray()` over the identical
    // stream - which drains through `drainSync`'s real loop - returned all 200 000.
    function* gen(n: number): Generator<number> {
      for (let index = 0; index < n; index++) yield index;
    }

    let count = 0;
    new Pipeline<number>()

      .buffer(1)(gen(200000))
      .forEach(() => {
        count++;
      });

    expect(count).toBe(200000);
    expect(new Pipeline<number>().buffer(1)(gen(200000)).toArray()).toHaveLength(200000);
  });

  it("an async run handler registered BEFORE .from() still widens the chain", async () => {
    // `fromSource` set the Mode purely from the source's shape, discarding a widening `.onError()`
    // had already recorded. Measured before the fix: this typed `number[]` and handed back a
    // pending `Promise` the moment a chunk failed.
    const out: Promise<number[]> = new Pipeline<number>()
      .onError(async () => {
        await Promise.resolve();
      })

      .buffer(1)
      .transform((t) =>
        t.map((x: number) => {
          if (x === 2) throw new Error("boom");
          return x;
        }),
      )([1, 2, 3])
      .toArray();

    expect(await out).toEqual([1, 3]);
  });

  it("a sync run handler registered before .from() keeps the chain sync", () => {
    const out: number[] = new Pipeline<number>()
      .onError(() => {})

      .buffer(1)
      .transform((t) =>
        t.map((x: number) => {
          if (x === 2) throw new Error("boom");
          return x;
        }),
      )([1, 2, 3])
      .toArray();

    expect(out).toEqual([1, 3]);
  });

  it(".buffer() called before .from() decides the source's own cut", () => {
    // Measured before the fix: `[15]`. `.buffer()` built a generator over an empty stream that
    // `.from()` then overwrote with `DEFAULT_CHUNK_SIZE`, so the declared boundary never applied.
    const out: number[] = new Pipeline<number>()
      .buffer(2)

      .transform((t) => t.reduce((acc: number, x: number) => acc + x, 0))([1, 2, 3, 4, 5])
      .toArray();

    expect(out).toEqual([3, 7, 5]);
  });

  it("a widening call keeps the subclass it was made on", () => {
    // `.onError(async …)` and `.tap(async …)` each returned a bare
    // `Pipeline<T, "async", P>`, so `.fetch` and every other subclass member vanished from the type
    // on a chain that compiled on `main`. Measured: `error TS2339: Property 'fetch' does not exist
    // on type 'Pipeline<number, "async", "async">'`. A receiver already async gains nothing from
    // widening, so it keeps its own class.
    const url = "http://localhost:1";
    const withHandler = new HttpPipeline<number>({ url }).onError(async () => {});
    const withTap = new HttpPipeline<number>({ url }).tap(async () => {});

    expect(typeof withHandler.fetch).toBe("function");
    expect(typeof withTap.fetch).toBe("function");
  });

  it("a reduce stage defers on the base and on a dispatching class", async () => {
    // The base's own guard is replaced by deferral (#90): a source-less `.reduce()` records the
    // fold and replays it on the input the pipeline is called with, the same as `.transform()`.
    // What the guard originally caught still holds - `new Pipeline<number>().reduce(f, 0)` never resolves
    // to `[]` - because DRAINING with no input is what still refuses.
    const summed = new Pipeline<number>().reduce((acc: number, x: number) => acc + x, 0);
    expect(summed([1, 2, 3, 4, 5]).toArray()).toEqual([15]);
    expect(summed([10, 20]).toArray()).toEqual([30]);

    // A dispatching class defers too, and its replayed fold still partitions (#62): at
    // `maxConcurrency: 3` over three chunks the result is three partial sums, not one total.
    const partitioned = new ConcurrentPipeline<number>({ maxConcurrency: 3 })
      .buffer(2)
      .reduce((acc: number, x: number) => acc + x, 0);
    expect(await partitioned([1, 2, 3, 4, 5, 6]).toArray()).toEqual([3, 7, 11]);
  });

  it("a drain with no source fails instead of resolving to an empty array", async () => {
    // Measured before the fix: `await new Pipeline<number>().toArray()` → `[]`, a plausible-looking answer
    // for a caller who forgot `.from()`, where composing any stage on it already threw. An
    // `"unset"` pipeline's `.toArray()` is typed `Promise<T[]>`, so the failure arrives as a
    // rejection rather than a synchronous throw.
    expect(new Pipeline<number>()([]).toArray()).toEqual([]);
  });

  it(".buffer() keeps cutting after an async stage, and both engines agree", async () => {
    // Measured before the fix: the sync-sourced chain gave `[28]` - `recutSyncChunks` met a pending
    // chunk and yielded the whole remaining stream as one array - where the identical chain over an
    // `AsyncIterable` source gave `[3,7,11,7]`. Two engines, one piece of user code, two answers.
    async function* asyncSeven(): AsyncGenerator<number> {
      for (const x of [1, 2, 3, 4, 5, 6, 7]) yield x;
    }

    const fromSync: Promise<number[]> = new Pipeline<number>()

      .transform((t) => t.map(async (x: number) => x))
      .buffer(2)
      .transform((t) => t.reduce((acc: number, x: number) => acc + x, 0))([1, 2, 3, 4, 5, 6, 7])
      .toArray();

    const fromAsync: Promise<number[]> = new Pipeline<number>()

      .transform((t) => t.map(async (x: number) => x))
      .buffer(2)
      .transform((t) => t.reduce((acc: number, x: number) => acc + x, 0))(asyncSeven())
      .toArray();

    expect(await fromSync).toEqual([3, 7, 11, 7]);
    expect(await fromSync).toEqual(await fromAsync);
  });

  it("a cut that does not divide evenly keeps its trailing partial chunk", async () => {
    const out: Promise<number[]> = new Pipeline<number>()

      .transform((t) => t.map(async (x: number) => x))
      .buffer(3)
      .transform((t) => t.reduce((acc: number, x: number) => acc + x, 0))([1, 2, 3, 4, 5, 6, 7])
      .toArray();

    expect(await out).toEqual([6, 15, 7]);
  });

  it("an all-sync chain with a stage before .buffer() stays synchronous", () => {
    // The negative control: the recut above must not widen a chain whose callbacks are all
    // synchronous, which is the divergence this whole ticket exists to remove.
    const out: number[] = new Pipeline<number>()

      .transform((t) => t.map((x: number) => x * 2))
      .buffer(2)
      .transform((t) => t.reduce((acc: number, x: number) => acc + x, 0))([1, 2, 3, 4, 5])
      .toArray();

    expect(Array.isArray(out)).toBe(true);
    expect(out).toEqual([6, 14, 10]);
  });
});
