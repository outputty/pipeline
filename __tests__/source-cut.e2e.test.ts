/**
 * source-cut.e2e.test.ts — #179's Done-when 4 and 5: an array forced onto the async engine is cut
 * synchronously, and `.buffer(size)` cuts by COUNT on all three arms instead of folding every item
 * through `Reducer<T[], T>`.
 *
 * Both changes leave every chunk boundary exactly where it was, so the cases here assert the
 * boundaries first and the per-row promise cost second - the cost is the only thing that moved.
 */
import { describe, it, expect } from "vitest";
import { Pipeline } from "@src/pipeline";
import { ConcurrentPipeline } from "@src/pipelines/concurrent";
import { DROP } from "@src/types";
import { chunksOf, countPromisesAsync, closingAsyncSource } from "./helpers/sequences";

async function* asyncRange(count: number): AsyncGenerator<number> {
  for (let i = 0; i < count; i++) yield i;
}

const N = 10_000;
const rows = Array.from({ length: N }, (_, i) => i);

describe("#179 fromSource() cuts an array with slice on the async engine too (Done-when 4)", () => {
  it("costs a dispatching class nothing per row over an array source", async () => {
    const build = (): ConcurrentPipeline<number, number> =>
      new ConcurrentPipeline<number>({ maxConcurrency: 4 });

    expect(await build()(rows).toArray()).toEqual(rows);

    // 2.005 promises per row before this layer - `toAsyncIterable`'s own `Promise.resolve` per pull,
    // then `buildChunkGenerator`'s `for await` on top, both charged for data already in memory.
    const perRow = (await countPromisesAsync(() => build()(rows).toArray())) / N;
    expect(perRow).toBeLessThan(0.5);
  });

  it("cuts an array and a Set into the same chunks on a dispatching class", async () => {
    const values = [1, 2, 3, 4, 5, 6, 7];
    const build = (): ConcurrentPipeline<number, number> =>
      new ConcurrentPipeline<number>({ maxConcurrency: 2 }).buffer(3);

    expect(await chunksOf(build()(values))).toEqual([[1, 2, 3], [4, 5, 6], [7]]);
    expect(await chunksOf(build()(new Set(values)))).toEqual([[1, 2, 3], [4, 5, 6], [7]]);
  });

  it("keeps a string source on the general path, cut by character", async () => {
    const out = await new ConcurrentPipeline<string>({ maxConcurrency: 2 })
      .buffer(2)("abcde")
      .toArray();
    expect(out).toEqual(["a", "b", "c", "d", "e"]);
  });

  it("still closes a genuinely async source early", async () => {
    const state = { closed: false };
    const got = await new ConcurrentPipeline<number>({ maxConcurrency: 2 })
      .buffer(3)(closingAsyncSource(state))
      .first(2);
    expect(got).toEqual([0, 1]);
    expect(state.closed).toBe(true);
  });
});

describe("#179 .buffer(size) cuts by count on all three arms (Done-when 5)", () => {
  it("reaches buildChunkGenerator's own floor on a genuinely async source", async () => {
    const build = (): Pipeline<number, "async", number> =>
      new Pipeline<number>().buffer(1000) as unknown as Pipeline<number, "async", number>;
    const bare = (): Pipeline<number, "async", number> =>
      new Pipeline<number>() as unknown as Pipeline<number, "async", number>;

    expect((await build()(asyncRange(N)).toArray()).length).toBe(N);

    // The floor is the SAME chain with no `.buffer()` call at all: the fold engine cost 7.006
    // promises per row against the cutter's own 4.005, and the gap was the per-item closure call
    // plus the array-mutating accumulator a count-based cut never needs.
    const buffered = (await countPromisesAsync(() => build()(asyncRange(N)).toArray())) / N;
    const unbuffered = (await countPromisesAsync(() => bare()(asyncRange(N)).toArray())) / N;
    expect(buffered).toBeLessThan(unbuffered + 0.5);
    expect(buffered).toBeLessThan(5);
  });

  it("cuts the sync arm identically, creating no Promise", async () => {
    const out = new Pipeline<number>().buffer(2)([1, 2, 3, 4, 5]);
    expect(await chunksOf(out)).toEqual([[1, 2], [3, 4], [5]]);

    const created = await countPromisesAsync(async () =>
      new Pipeline<number>().buffer(2)([1, 2, 3, 4, 5]).toArray(),
    );
    // The whole synchronous engine allocates nothing per row and must stay that way; the handful
    // here belong to the `async` probe wrapper itself, not to the chain.
    expect(created).toBeLessThan(10);
  });

  it("cuts the forced-async arm identically, one crossing per chunk", async () => {
    const out = new ConcurrentPipeline<number>({ maxConcurrency: 2 }).buffer(2)([1, 2, 3, 4, 5]);
    expect(await chunksOf(out)).toEqual([[1, 2], [3, 4], [5]]);
  });

  it("collapses back-to-back .buffer() calls to the last one, on every arm", async () => {
    const values = [1, 2, 3, 4, 5, 6, 7, 8, 9];
    const expected = [[1, 2, 3, 4], [5, 6, 7, 8], [9]];

    expect(await chunksOf(new Pipeline<number>().buffer(2).buffer(3).buffer(4)(values))).toEqual(
      expected,
    );
    expect(
      await chunksOf(
        new ConcurrentPipeline<number>({ maxConcurrency: 2 }).buffer(2).buffer(3).buffer(4)(values),
      ),
    ).toEqual(expected);
    expect(
      await chunksOf(
        new Pipeline<number>().buffer(2).buffer(3).buffer(4)(
          (function* () {
            yield* values;
          })(),
        ),
      ),
    ).toEqual(expected);
  });

  it("re-cuts after a real stage has run, where only the chunk view survives", async () => {
    const out = new Pipeline<number>()
      .buffer(10)
      .transform((t) => t.flatMap((x: number) => [x, x]))
      .buffer(3)([1, 2, 3]);
    expect(await chunksOf(out)).toEqual([
      [1, 1, 2],
      [2, 3, 3],
    ]);
  });

  it("leaves .buffer(fn) on the fold engine, DROP and all", async () => {
    // A per-item callback is what the `Reducer<T[], T>` engine is for, and it keeps it: this case
    // pins that the numeric split did not take the callback form with it.
    const out = new Pipeline<number>().buffer((item: number, _ctx, emit) => {
      if (item < 0) return DROP;
      if (item % 3 === 0) {
        emit();
        return item;
      }
      return item;
    })([1, 2, 3, -1, 4, 5, 6]);

    expect(await chunksOf(out)).toEqual([[1, 2], [3, 4, 5], [6]]);
  });

  it("validates the size on the BOUND path too, under .buffer()'s own name", () => {
    // A chain is already bound inside `.local(build)`, so `p.buffer(n)` there takes the bound
    // branch, not the deferred one. That branch used to validate through `sizeReduceFunction`, which
    // checked only `size < 1` and named the internal knob - measured before this layer,
    // `.local((p) => p.buffer(2.5))` did not throw at all and `.buffer(0)` threw
    // `chunkSize must be at least 1`. BREAKING, and its own changeset says so.
    expect(() =>
      new Pipeline<number>()
        .local((p) => p.buffer(2.5))([1, 2, 3, 4, 5, 6, 7])
        .toArray(),
    ).toThrow("buffer size must be a whole number of at least 1");
    expect(() =>
      new Pipeline<number>()
        .local((p) => p.buffer(0))([1, 2, 3])
        .toArray(),
    ).toThrow("buffer size must be a whole number of at least 1");
    expect(() =>
      new Pipeline<number>()
        .local((p) => p.buffer(3))([1, 2, 3, 4, 5, 6, 7])
        .toArray(),
    ).not.toThrow();
  });
});
