/**
 * buffer.e2e.test.ts — ticket #39's own Done-when cases 1-5, pinned during L1 as expected-fail and
 * flipped live here in L2: chunking moves off `Transformer` entirely onto an explicit
 * `Pipeline.buffer(size)` call, the ONE place a cut ever happens, persisted through every later
 * stage until called again.
 */
import { describe, it, expect } from "vitest";
import { createHook } from "node:async_hooks";
import { Pipeline } from "@src/pipeline";
import { ConcurrentPipeline } from "@src/pipelines/concurrent";
import { Transformer } from "@src/transformer";
import type { IContextManager } from "@src/types";
import { DROP } from "@src/types";
import { closingSource, closingAsyncSource, chunksOf } from "./helpers/sequences";

/** Counts every `Promise` created across a whole ASYNC drain, not just `fn`'s own synchronous
 * call (`./helpers/sequences`' own `countPromises` disables its hook the instant `fn()` returns,
 * before an awaited drain's own later ticks run) - the one case here that needs a promise count
 * spanning several microtask turns, since the fast path vs. per-item fallback this ticket adds
 * differ only in HOW MANY promises the drain creates, never in its output.
 *
 * `await countPromisesAsync(() => Promise.resolve(1).then(() => Promise.resolve(2)))` → `2`. */
async function countPromisesAsync(fn: () => Promise<unknown>): Promise<number> {
  let created = 0;
  const hook = createHook({
    init(_id, type) {
      if (type === "PROMISE") created++;
    },
  });
  hook.enable();
  try {
    await fn();
  } finally {
    hook.disable();
  }
  return created;
}

/** Records each chunk `.apply()` hands to a stage, before that stage's own transform runs -
 * a chunk-level probe, not a per-item one (`.tap(fn)` runs per item and can't see boundaries). */
function boundaryProbe<T>(seen: T[][]): Transformer<T, T> {
  return new Transformer<T, T>({
    transform: (chunk) => {
      seen.push([...chunk]);
      return chunk;
    },
  });
}

describe("#39 buffer() is the one explicit chunk boundary (Done-when 1)", () => {
  it("prints [4,6,8,10,12,14,16,18] with the two stages' own input boundaries", async () => {
    const stage1Input: number[][] = [];
    const stage2Input: number[][] = [];

    const out = await new ConcurrentPipeline<number>({ maxConcurrency: 8 })

      .buffer(2)
      .apply(boundaryProbe(stage1Input))
      .transform((t) => t.map((x: number) => x + 1))
      .buffer(4)
      .apply(boundaryProbe(stage2Input))
      .transform((t) => t.map((x: number) => x * 2))([1, 2, 3, 4, 5, 6, 7, 8])
      .toArray();

    expect(out).toEqual([4, 6, 8, 10, 12, 14, 16, 18]);
    expect(stage1Input).toEqual([
      [1, 2],
      [3, 4],
      [5, 6],
      [7, 8],
    ]);
    expect(stage2Input).toEqual([
      [2, 3, 4, 5],
      [6, 7, 8, 9],
    ]);
  });
});

describe("#39 no .buffer() between two stages means no re-chunk (Done-when 2)", () => {
  // Already holds today, unlike the other cases here: neither stage changes item count and both
  // default to the same chunk size, so nothing re-chunks between them even before the seam moves -
  // a plain `it`, per this repo's own convention (pipelines.e2e.test.ts's docstring).
  it("both stages see the identical chunk boundary", async () => {
    const stage1Input: number[][] = [];
    const stage2Input: number[][] = [];

    await new Pipeline<number>()

      .apply(boundaryProbe(stage1Input))
      .transform((t) => t.map((x: number) => x * 2))
      .apply(boundaryProbe(stage2Input))
      .transform((t) => t.map((x: number) => x + 1))([1, 2, 3, 4])
      .toArray();

    expect(stage1Input).toEqual([[1, 2, 3, 4]]);
    expect(stage2Input).toEqual([[2, 4, 6, 8]]);
  });
});

describe("#90 review - .buffer() refuses an invalid size at the call, not at the drain", () => {
  it("throws from .buffer(0) on a source-less chain", () => {
    // A deferred `.buffer()` only RECORDS the call, so validation used to wait for the chunker an
    // input eventually reached: `new Pipeline<number>().buffer(0)` returned a pipeline, and the
    // drain then threw `chunkSize must be at least 1` - a message that never names `.buffer()`.
    // Every chain is source-less by default now, so that is the ordinary path.
    expect(() => new Pipeline<number>().buffer(0)).toThrow("buffer size must be");
    expect(() => new Pipeline<number>().transform((t) => t.map((x) => x)).buffer(-5)).toThrow(
      "buffer size must be",
    );
    // A fractional size passed the `< 1` guard and made the two cutting paths disagree: the source
    // cut at 3 (`length >= 2.5`) where the re-cut sliced at 2 (`index + 2.5`), over the same data.
    expect(() => new Pipeline<number>().buffer(2.5)).toThrow("whole number");
    expect(() => new Pipeline<number>().buffer(1)).not.toThrow();
  });
});

describe("#39 two .buffer() calls back to back collapse to the last one (Done-when 3)", () => {
  it("matches .buffer(4) alone over [1..9]", async () => {
    const chained = await chunksOf(
      new Pipeline<number>().buffer(2).buffer(3).buffer(4)([1, 2, 3, 4, 5, 6, 7, 8, 9]),
    );
    const direct = await chunksOf(new Pipeline<number>().buffer(4)([1, 2, 3, 4, 5, 6, 7, 8, 9]));

    expect(chained).toEqual([[1, 2, 3, 4], [5, 6, 7, 8], [9]]);
    expect(direct).toEqual(chained);
  });
});

describe("#90 review - an early exit closes the source on both engines", () => {
  it("closes a sync generator when .first(1) stops a chain that re-cuts after a stage", () => {
    // `recutSyncChunks` drives its source through a MANUAL iterator, so closing the recut
    // generator taught the source nothing. Measured before the fix: `closed` stayed `false` here
    // and `true` on the async source below - two engines disagreeing on user code that differed
    // only in its source.
    const state = { closed: false };
    const chain = new Pipeline<number>()
      .buffer(10)
      .transform((t) => t.map((x) => x * 2))
      .buffer(2);

    expect(chain(closingSource(state)).first(1)).toEqual([0]);
    expect(state.closed).toBe(true);
  });

  it("closes an async generator on the same chain", async () => {
    const state = { closed: false };
    const chain = new Pipeline<number>()
      .buffer(10)
      .transform((t) => t.map((x) => x * 2))
      .buffer(2);

    expect(await chain(closingAsyncSource(state)).first(1)).toEqual([0]);
    expect(state.closed).toBe(true);
  });

  it("closes the source when a FAILED run stops the drain, on both engines", async () => {
    // The async engine gets this from `for await`, which calls `.return()` when its body throws.
    // `drainSync`'s pending arm attached only a fulfillment handler, so a rejected chunk left the
    // manual iterator open - measured, `source finally ran - sync input: false | async: true`,
    // the same two-engines-disagree class the source-close case above closed for early EXIT.
    const syncState = { closed: false };
    const boom = new Pipeline<number>().buffer(1).transform((t) =>
      t.map(async (x: number) => {
        if (x === 2) throw new Error("boom");
        return x;
      }),
    );

    await expect(boom(closingSource(syncState)).toArray()).rejects.toThrow("boom");
    expect(syncState.closed).toBe(true);

    const asyncState = { closed: false };
    await expect(boom(closingAsyncSource(asyncState)).toArray()).rejects.toThrow("boom");
    expect(asyncState.closed).toBe(true);
  });

  it("closes the source when a for...of breaks early, without draining the whole chain", () => {
    // `[Symbol.iterator]` was `toArray()[Symbol.iterator]()`, so a `break` ran the entire chain
    // first and never closed the source - where `.first(n)` over the same chain stopped early and
    // did. The sibling `[Symbol.asyncIterator]` was lazy the whole time.
    const state = { closed: false };
    let mapped = 0;
    const chain = new Pipeline<number>().buffer(1).transform((t) =>
      t.map((x: number) => {
        mapped++;
        return x;
      }),
    );

    const seen: number[] = [];
    for (const item of chain(closingSource(state))) {
      seen.push(item);
      if (seen.length === 3) break;
    }

    expect(seen).toEqual([0, 1, 2]);
    expect(state.closed).toBe(true);
    // The whole point: 100 items in the source, only what the loop asked for ran.
    expect(mapped).toBeLessThan(10);
  });

  it("closes the source when the re-cut runs over a pending tail", async () => {
    // `recutPending` takes the iterator over the moment a chunk is a Promise, so it owns the close
    // from that point on; `recutSyncChunks` must not close one it no longer drives.
    const state = { closed: false };
    const chain = new Pipeline<number>()
      .buffer(10)
      .transform((t) => t.map(async (x) => x * 2))
      .buffer(2);

    expect(await chain(closingSource(state)).first(1)).toEqual([0]);
    expect(state.closed).toBe(true);
  });
});

describe("async-engine tax spike (#120 follow-up) - a sync source on a forced-async class", () => {
  it("still cuts the exact same chunks .buffer(size) always did, on ConcurrentPipeline", async () => {
    // `sourcePolicy()` forces ConcurrentPipeline's Mode to "async" regardless of input shape - the
    // fast path this covers (a sync pre-buffer view kept alive through that forced Mode) must
    // produce IDENTICAL output to the per-item path it replaces, not just run faster. `.toArray()`
    // alone can't see a chunk boundary (`boundaryProbe`'s own docstring, above) - flattening
    // hides a wrong cut (e.g. one 7-item chunk) behind an identical array, so this reads the
    // actual boundary `.apply()` hands the next stage.
    const seen: number[][] = [];

    const out = await new ConcurrentPipeline<number>()
      .buffer(3)
      .apply(boundaryProbe(seen))([1, 2, 3, 4, 5, 6, 7])
      .toArray();

    expect(out).toEqual([1, 2, 3, 4, 5, 6, 7]);
    expect(seen).toEqual([[1, 2, 3], [4, 5, 6], [7]]);
  });

  it("still drops an item via DROP and never emits an empty trailing chunk, on ConcurrentPipeline", async () => {
    // The `.buffer(fn)` overload's own engine (`bufferReduceFunction`), exercised through the same
    // fast path - #88's Done-when 3 case, restated on a dispatching class.
    const items = [
      { v: 1, invalid: false },
      { v: 2, invalid: true },
      { v: 3, invalid: false },
    ];
    const dropInvalid = (item: (typeof items)[number]) => (item.invalid ? DROP : item);

    const out = await new ConcurrentPipeline<(typeof items)[number]>()
      .buffer(dropInvalid)(items)
      .toArray();

    expect(out).toEqual([items[0], items[2]]);
  });

  it("closes a sync generator source when .first(1) stops it early, on ConcurrentPipeline", async () => {
    // `toAsyncIterable()`'s hand-rolled iterator (replacing an `async function*`) must still forward
    // an early stop into `.return()` on the underlying sync generator - a version without that
    // forwarding left this `false` where the `async function*` it replaces left it `true`.
    const state = { closed: false };

    expect(await new ConcurrentPipeline<number>().buffer(1)(closingSource(state)).first(1)).toEqual(
      [0],
    );
    expect(state.closed).toBe(true);
  });

  it("a second back-to-back .buffer() still takes the fast path, on ConcurrentPipeline", async () => {
    // #39's Done-when 3 (two `.buffer()` calls collapse to the last) already holds for OUTPUT
    // either way - what's at stake here is F3's fast path staying live for the SECOND call too:
    // if the sync item view were nulled after the first `.buffer()` (rather than kept alive, as
    // the `isSync()` branch above already does), the second call would silently fall through to
    // `buildBufferGenerator`'s per-item path with no wrong OUTPUT to catch it - only more promises
    // created, at 1000 items an easy regression no `.toEqual()` on the result would ever see.
    // Measured directly (spiked, both real): the fast path creates 79 promises for this 9-item
    // case (`ConcurrentPipeline`'s own construction/dispatch scaffolding is most of that, unrelated
    // to `.buffer()`); temporarily nulling the kept-alive view to force the per-item fallback for
    // BOTH calls creates 196. The threshold sits between the two, with margin either side.
    const items = [1, 2, 3, 4, 5, 6, 7, 8, 9];

    const created = await countPromisesAsync(() =>
      new ConcurrentPipeline<number>().buffer(2).buffer(3)(items).toArray(),
    );

    expect(created).toBeLessThan(140);
  });

  it("rejects, rather than throws synchronously, when the sync source itself throws mid-pull", async () => {
    // `toAsyncIterable()`'s hand-rolled `next()` wraps the underlying sync iterator's own
    // `.next()` in a try/catch so a synchronous throw there surfaces as a REJECTED Promise, per
    // the `AsyncIterator` protocol's own contract - defensive, since every consumer in this
    // package pulls through `for await` (which already normalizes this regardless, verified: this
    // exact case still passes with the try/catch removed), but real for any future direct
    // `.next()` caller.
    function* throwsOnThird(): Generator<number> {
      yield 1;
      yield 2;
      throw new Error("bad item");
    }

    await expect(new ConcurrentPipeline<number>()(throwsOnThird()).toArray()).rejects.toThrow(
      "bad item",
    );
  });
});

// #39's own Done-when 4 and 5 - lifecycle hooks firing identically across every consumption path,
// and async iteration reading the same persisted chunk stream a terminal op does - are covered by
// __tests__/transforms.e2e.test.ts's single tap observation case now that #72 deletes hooks in
// favor of .tap(): the mechanism .tap() replaced them with is what that case proves fires the
// same way on .toArray(), on async iteration and on a .local() stage.

// #88 - `.buffer()` accepts a `BufferFunction<T>` in place of a size, deciding the chunk boundary
// per item instead of by count - `sizeReduceFunction`/`bufferReduceFunction` fold both forms
// through the same `Reducer<T[], T>` engine (`src/utils/reduce.ts`). Done-when 5 (`ChunkerFunction`
// gone from the public export surface) and 6 (no file outside src/, __tests__/, .claude/ changed)
// are structural checks, run via `rg`/`git diff` rather than a runtime case here.

describe("#88 buffer(fn) cuts a custom chunk boundary (Done-when 1)", () => {
  it("groups events into five-minute windows by their own timestamp, the exact boundary a tap sees", async () => {
    // The ticket's own literal expression, `t.tap(createTransformer<Event[]>().tap((chunk) =>
    // seen.push(chunk)))`, does not typecheck against `Transformer.tap`'s real overloads - the
    // `transformer` form takes `Transformer<Out, unknown>` (`Out` = the OUTER transformer's per-
    // ITEM type, `Event` here) and runs it over the whole chunk AT ONCE, never wrapping the chunk
    // as one opaque item the way `createTransformer<Event[]>()` would need. `boundaryProbe` (top of
    // this file) is this file's own proven idiom for "push the exact chunk array to `seen`" -
    // ticket #39's own Done-when 1 above uses it for the identical assertion shape.
    type Event = { id: number; ts: number };
    const events: Event[] = [
      { id: 1, ts: 0 },
      { id: 2, ts: 60_000 },
      { id: 3, ts: 240_000 },
      { id: 4, ts: 300_000 },
      { id: 5, ts: 301_000 },
    ];

    let windowStart = 0;
    // Typed by its own params/return, never `BufferFunction<Event>` - that union type (covering a
    // Promise-returning `fn` too) matches neither of `.buffer()`'s two narrower overloads (#88,
    // code-review: they split on Promise so an async `fn` widens the chain's own Mode).
    const fiveMinuteWindow = (item: Event, _ctx: IContextManager, emit: () => void): Event => {
      if (item.ts - windowStart >= 300_000) {
        emit();
        windowStart = item.ts;
      }
      return item;
    };

    const seen: Event[][] = [];
    const out = await new Pipeline<Event>()

      .buffer(fiveMinuteWindow)
      .apply(boundaryProbe(seen))(events)
      .toArray();

    expect(out).toEqual(events);
    expect(seen).toEqual([
      [events[0], events[1], events[2]],
      [events[3], events[4]],
    ]);
  });
});

describe("#88 buffer(size) stays unaffected by the reimplementation (Done-when 2)", () => {
  it("still prints all five events unchanged, individually, in order", async () => {
    const events = [1, 2, 3, 4, 5];
    const out = await new Pipeline<number>().buffer(3)(events).toArray();
    expect(out).toEqual(events);
  });
});

describe("#88 buffer(fn) drops an item via DROP (Done-when 3)", () => {
  it("omits the invalid item from the output entirely", async () => {
    const items = [
      { v: 1, invalid: false },
      { v: 2, invalid: true },
      { v: 3, invalid: false },
    ];
    const dropInvalid = (item: (typeof items)[number]) => (item.invalid ? DROP : item);

    const out = await new Pipeline<(typeof items)[number]>().buffer(dropInvalid)(items).toArray();

    expect(out).toEqual([items[0], items[2]]);
  });

  it("never lets a flush-then-drop leave an empty pending array as its own chunk", async () => {
    // `Reducer.itemsSinceEmit` increments BEFORE `fn` runs and DROP never undoes it outside a row
    // handler (none is registered here, per this ticket's own Constraints) - a flush immediately
    // followed by an item that drops leaves `pending` empty, and `.buffer(fn)`'s engine must guard
    // every yield against it (`.toArray()` alone would hide the gap; `boundaryProbe` does not).
    const items = [
      { v: 1, invalid: false },
      { v: 2, invalid: false },
      { v: 3, invalid: true }, // flushes [1, 2], then drops - pending is [] going into item 4
      { v: 4, invalid: true }, // drops again - final() must not push a trailing []
    ];
    const flushThenDrop = (
      item: (typeof items)[number],
      _ctx: IContextManager,
      emit: () => void,
    ) => {
      if (item.v === 3) emit();
      return item.invalid ? DROP : item;
    };

    const seen: (typeof items)[number][][] = [];
    const out = await new Pipeline<(typeof items)[number]>()

      .buffer(flushThenDrop)
      .apply(boundaryProbe(seen))(items)
      .toArray();

    expect(out).toEqual([items[0], items[1]]);
    expect(seen).toEqual([[items[0], items[1]]]);
  });
});

describe("#88 ConcurrentPipeline items in flight follow buffer(fn)'s own window size (Done-when 4)", () => {
  it("holds window-size x maxConcurrency callbacks at once, for a fixed 3-item window", async () => {
    let count = 0;
    const windowOfThree = (item: number, _ctx: IContextManager, emit: () => void): number => {
      count++;
      if (count % 3 === 0) emit();
      return item;
    };

    let inFlight = 0;
    let peak = 0;
    const items = Array.from({ length: 12 }, (_, i) => i);

    await new ConcurrentPipeline<number>({ maxConcurrency: 2 })
      .buffer(windowOfThree)
      .transform((t) =>
        t.map(async (x: number) => {
          inFlight++;
          peak = Math.max(peak, inFlight);
          await new Promise((resolve) => setTimeout(resolve, 20));
          inFlight--;
          return x;
        }),
      )(items)
      .toArray();

    // The same relationship product.md's own "Items in flight" table documents for a fixed size:
    // buffer's own window size (3) times maxConcurrency (2).
    expect(peak).toBe(3 * 2);
  });
});

describe("#88 buffer(fn) runs with no Transformer in scope, matching Pipeline.reduce()'s own docs", () => {
  it("propagates a throwing fn - no row-handler recovery, staying synchronous over a sync source", () => {
    const boom = (item: number): number => {
      if (item === 2) throw new Error("boom");
      return item;
    };

    // A plain array is a "sync"-Mode source, so `.buffer(boom)` stays zero-promise (#90) and its
    // failure THROWS out of `.toArray()` directly, rather than rejecting.
    expect(() => new Pipeline<number>().buffer(boom)([1, 2, 3]).toArray()).toThrow("boom");
  });
});

describe("#88 buffer(fn) closes the source on early exit after a real stage already ran", () => {
  it("closes a sync generator when .first(1) stops a chain that re-cuts with a BufferFunction", () => {
    // The numeric sibling of this case (above, "#90 review - an early exit closes the source on
    // both engines") exercises `recutSyncChunks`'s own manual-iterator cleanup; `.buffer(fn)`'s
    // equivalent sub-path (`recutSyncChunksWith`, src/utils/reduce.ts) folds each existing chunk
    // SLOT through the same `driveFold` engine instead - a plain nested `for...of`, the same shape
    // `foldSyncChunkStream` already uses - relying on a generator's own `.return()` propagation
    // rather than a manual iterator, and this is what proves that propagation still closes the
    // source once folding replaces a plain re-slice.
    const state = { closed: false };
    let count = 0;
    const sizeTwo = (item: number, _ctx: IContextManager, emit: () => void): number => {
      count++;
      if (count % 2 === 0) emit();
      return item;
    };
    const chain = new Pipeline<number>()
      .buffer(10)
      .transform((t) => t.map((x) => x * 2))
      .buffer(sizeTwo);

    expect(chain(closingSource(state)).first(1)).toEqual([0]);
    expect(state.closed).toBe(true);
  });
});

describe("#88 buffer(fn)'s recut-from-chunks sub-path keeps every emit its own chunk (code-review)", () => {
  it("never merges an async stage's own single slot back into one oversized chunk", async () => {
    // `recutSyncChunksWith` used to fold one incoming SLOT (here, the map stage's own single
    // 10-item chunk) and `.flat()` every value it emitted into ONE downstream chunk - correct only
    // when a slot emits at most once. A window function folding a real, multi-item slot emits
    // several times per slot as the ordinary case, not an edge case: measured before the fix,
    // `.buffer(10).transform((t) => t.map(async (x) => x * 2)).buffer(sizeTwo)` over the doubled
    // `[0,2,4,...,18]` yielded ONE 9-item chunk instead of six. `sizeTwo` flushes on every EVEN
    // count, so its own first flush fires after the second item, leaving item 0 as its own
    // 1-item chunk - real output, not a hand-derived guess (this repo's own "run before stating
    // an example's output" rule).
    let count = 0;
    const sizeTwo = (item: number, _ctx: IContextManager, emit: () => void): number => {
      count++;
      if (count % 2 === 0) emit();
      return item;
    };
    const chunks = await chunksOf(
      new Pipeline<number>()
        .buffer(10)
        .transform((t) => t.map(async (x: number) => x * 2))
        .buffer(sizeTwo)([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
    );

    expect(chunks).toEqual([[0], [2, 4], [6, 8], [10, 12], [14, 16], [18]]);
  });

  it("also drains correctly through .toArray()'s own iterator-driven consumer, not just .chunks()", async () => {
    // `driveFold`'s `remaining` queue is written inside a yielded chunk's OWN `.then` and read back
    // synchronously at the next `for` pass - correct only if every consumer awaits a pending chunk
    // before calling `.next()` again. `chunksOf` above proves it through `.chunks()`'s `for await`;
    // `.toArray()` goes through a different path (`drainSync`, a manual iterator) and must agree.
    let count = 0;
    const sizeTwo = (item: number, _ctx: IContextManager, emit: () => void): number => {
      count++;
      if (count % 2 === 0) emit();
      return item;
    };
    const items = await new Pipeline<number>()
      .buffer(10)
      .transform((t) => t.map(async (x: number) => x * 2))
      .buffer(sizeTwo)([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
      .toArray();

    expect(items).toEqual([0, 2, 4, 6, 8, 10, 12, 14, 16, 18]);
  });
});

describe("#88 a flush-then-append on the LAST item is not dropped (found while verifying the fix above)", () => {
  it("keeps the item that caused the final flush as its own trailing chunk", async () => {
    // `Reducer.final()`'s own `itemsSinceEmit` gate reads `0` right after an `emit()` - correct for
    // `.reduce()`'s contract (`Reducer`'s own docstring), where a post-emit return value may be an
    // unrelated fresh seed, but wrong for `bufferReduceFunction`'s flush-THEN-append shape: the
    // item that triggers the flush also becomes the first (and here, only) item of the new pending
    // array. `Reducer.current()`/`trailingOf` (src/utils/reduce.ts) reads the real pending state
    // instead, so this item survives as its own trailing chunk rather than vanishing.
    let windowStart = 0;
    const fiveMinuteWindow = (item: { ts: number }, _ctx: IContextManager, emit: () => void) => {
      if (item.ts - windowStart >= 300_000) {
        emit();
        windowStart = item.ts;
      }
      return item;
    };
    const events = [{ ts: 0 }, { ts: 300_000 }];

    const chunks = await chunksOf(new Pipeline<{ ts: number }>().buffer(fiveMinuteWindow)(events));

    expect(chunks).toEqual([[events[0]], [events[1]]]);
  });
});

describe("#88 buffer(fn) widens Mode to async for a Promise-returning fn (code-review)", () => {
  it("returns a real array once awaited, matching what .toArray()'s own async type already promises", async () => {
    // Before this fix, `.buffer(fn)` always returned `this`, so an async `fn` on an otherwise-sync
    // chain typechecked as `number[]` while `.toArray()` actually handed back a `Promise` at
    // runtime - exactly the type/runtime divergence #90 exists to prevent. `.reduce()`'s own two
    // overloads are the precedent this follows (Promise-returning first, widening Mode).
    const doubleAsync = async (item: number): Promise<number> => item * 2;
    // Assigned to a `Promise<number[]>`-typed binding with no cast: this line itself fails
    // `tsc --noEmit` if `.buffer(fn)` ever stops widening Mode for a Promise-returning `fn`.
    const result: Promise<number[]> = new Pipeline<number>()
      .buffer(doubleAsync)([1, 2, 3])
      .toArray();

    expect(await result).toEqual([2, 4, 6]);
  });
});
