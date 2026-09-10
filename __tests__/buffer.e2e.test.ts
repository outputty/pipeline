/**
 * buffer.e2e.test.ts — ticket #39's own Done-when cases 1-5, pinned during L1 as expected-fail and
 * flipped live here in L2: chunking moves off `Transformer` entirely onto an explicit
 * `Pipeline.buffer(size)` call, the ONE place a cut ever happens, persisted through every later
 * stage until called again.
 */
import { describe, it, expect } from "vitest";
import { Pipeline } from "@src/pipeline";
import { ConcurrentPipeline } from "@src/pipelines/concurrent";
import { Transformer } from "@src/transformer";
import { closingSource, closingAsyncSource } from "./helpers/sequences";

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
    const chained: number[][] = [];
    for await (const chunk of new Pipeline<number>()
      .buffer(2)
      .buffer(3)
      .buffer(4)([1, 2, 3, 4, 5, 6, 7, 8, 9])
      .chunks()) {
      chained.push(chunk);
    }

    const direct: number[][] = [];
    for await (const chunk of new Pipeline<number>()
      .buffer(4)([1, 2, 3, 4, 5, 6, 7, 8, 9])
      .chunks()) {
      direct.push(chunk);
    }

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

// #39's own Done-when 4 and 5 - lifecycle hooks firing identically across every consumption path,
// and async iteration reading the same persisted chunk stream a terminal op does - are covered by
// __tests__/transforms.e2e.test.ts's single tap observation case now that #72 deletes hooks in
// favor of .tap(): the mechanism .tap() replaced them with is what that case proves fires the
// same way on .toArray(), on async iteration and on a .local() stage.
