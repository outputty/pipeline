/**
 * #179 - the places this package spent work per ROW that only needed doing per CHUNK, proven against
 * the real classes: `buildSyncChunkGenerator`'s array fast path (`src/utils/cut.ts`) and
 * `settleRows`' single output array on its synchronous armed arm (`src/transformer.ts`).
 *
 * Every case here asserts a BEHAVIOUR that must survive the optimisation, never a wall-clock number.
 * The time this ticket buys is gated by `pnpm bench:overhead` against `bench/baseline.json`, which is
 * where a timing assertion belongs; a test racing a nanosecond budget on a shared machine would fail
 * for reasons unrelated to the code under test (`bench/gate.ts`'s own header records that finding).
 */

import { describe, it, expect } from "vitest";

import { Pipeline } from "@src/pipeline";
import { Transformer } from "@src/transformer";
import { DROP } from "@src/types";
import { SimpleContextManager } from "@src/context/simple";
import { buildSyncChunkGenerator } from "@src/utils/chunk";
import { canonicalInput } from "../bench/canonical";
import { closingSource } from "./helpers/sequences";

/** The same values `canonicalInput(n)` holds, handed over as a generator rather than an array - the
 * one input shape that separates `buildSyncChunkGenerator`'s two arms, since the array arm is chosen
 * by `Array.isArray` alone. */
function* canonicalGenerator(n: number): Generator<number> {
  for (let i = 0; i < n; i++) yield i;
}

describe("#179 buildSyncChunkGenerator - the array arm and the per-item arm agree", () => {
  it("cuts an array and a generator over the same values into the same chunks", () => {
    const cut = buildSyncChunkGenerator<number>(1000);

    const fromArray = [...cut(canonicalInput(2500))];
    const fromGenerator = [...cut(canonicalGenerator(2500))];

    expect(fromArray.map((chunk) => chunk.length)).toEqual([1000, 1000, 500]);
    expect(fromGenerator.map((chunk) => chunk.length)).toEqual([1000, 1000, 500]);
    expect(fromArray.flat()).toEqual(fromGenerator.flat());
    expect(fromArray.flat()).toEqual(canonicalInput(2500));
  });

  it("cuts a final short chunk the same way from either shape", () => {
    const cut = buildSyncChunkGenerator<number>(3);

    expect([...cut([1, 2, 3, 4, 5, 6, 7])]).toEqual([[1, 2, 3], [4, 5, 6], [7]]);
    expect([...cut(canonicalGenerator(7))]).toEqual([[0, 1, 2], [3, 4, 5], [6]]);
  });

  it("keeps a string on the per-item arm - `Array.isArray` is the test, never `length`", () => {
    // A string is iterable AND length-bearing, so a `length`-plus-`slice` fast path would hand back
    // STRINGS here rather than arrays - `"abcde".slice(0, 2)` is `"ab"`, not `["a", "b"]`. The
    // per-item arm rejects nothing; it cuts a string into its characters, which is the shipped
    // behaviour and is what this case pins.
    const cut = buildSyncChunkGenerator<string>(2);

    expect([...cut("abcde")]).toEqual([["a", "b"], ["c", "d"], ["e"]]);
  });

  it("keeps a fractional size off the array arm, so both arms still agree", () => {
    // `slice(i, i + 2.5)` TRUNCATES both bounds where the per-item arm cuts at `length >= 2.5`, so
    // an unguarded array arm cut `[[1,2],[3,4,5],[6,7]]` against a `Set`'s own `[[1,2,3],[4,5,6],
    // [7]]` - one knob, two chunkings, the engine disagreeing with itself (review-caught, #179 L3).
    // `Pipeline`'s constructor refuses a fractional `chunkSize` outright, but this function is also
    // reached from `recut.ts` and `.buffer()`, so the arm carries its own `Number.isInteger` guard.
    const cut = buildSyncChunkGenerator<number>(2.5);
    const rows = [1, 2, 3, 4, 5, 6, 7];

    const fromArray = [...cut(rows)];
    const fromSet = [...cut(new Set(rows))];

    expect(fromArray).toEqual([[1, 2, 3], [4, 5, 6], [7]]);
    expect(fromArray).toEqual(fromSet);
  });
});

describe("#179 the array arm does not break an early stop on a generator source", () => {
  it("runs a generator source's own `finally` when `.first(1)` stops the chain early", async () => {
    const state = { closed: false };

    const rows = await new Pipeline<number>().buffer(2)(closingSource(state, 100)).first(1);

    expect(rows).toEqual([0]);
    expect(state.closed).toBe(true);
  });

  it("stops an array source early too, yielding only the chunks it needed", async () => {
    // The array arm returns without ever touching the iterator protocol, so there is no `finally` to
    // run on this side - what must still hold is that the chain stops pulling chunks.
    const rows = await new Pipeline<number>().buffer(2)(canonicalInput(100)).first(1);

    expect(rows).toEqual([0]);
  });
});

describe("#179 settleRows - the armed recovery path, end to end", () => {
  /** A `.map()` that throws on every odd row, recovered by dropping it - the shape that makes
   * `settleRows` place values and sentinels in one chunk at once. */
  const dropOdd = new Transformer<number, number>()
    .onError(() => DROP)
    .map((x: number) => {
      if (x % 2 === 1) throw new Error(`odd: ${x}`);
      return x;
    });

  it("keeps exactly the even rows of canonicalInput(2500)", async () => {
    const rows = await new Pipeline<number>().apply(dropOdd)(canonicalInput(2500)).toArray();

    expect(rows.length).toBe(1250);
    expect(rows[0]).toBe(0);
    expect(rows[1249]).toBe(2498);
    expect(rows.every((x) => x % 2 === 0)).toBe(true);
  });

  it("keeps a recovered row at its own index, never at the end", async () => {
    const repair = new Transformer<number, number>()
      .onError(() => -1)
      .map((x: number) => {
        if (x === 2) throw new Error("boom");
        return x * 10;
      });

    expect(await new Pipeline<number>().apply(repair)([1, 2, 3]).toArray()).toEqual([10, -1, 30]);
  });

  it("keeps index order when an asynchronous row settles out of order", async () => {
    // The asynchronous arm is the one that must still settle-then-place: row 1 resolves last in
    // wall-clock order and still lands at index 1.
    const slowFirst = new Transformer<number, number>()
      .onError(() => -1)
      .map(async (x: number) => {
        if (x === 2) throw new Error("boom");
        await new Promise((resolve) => setTimeout(resolve, x === 1 ? 20 : 0));
        return x * 10;
      });

    expect(await new Pipeline<number>().apply(slowFirst)([1, 2, 3]).toArray()).toEqual([
      10, -1, 30,
    ]);
  });
});

describe("#179 settleRows - the armed sync arm allocates one output array per chunk", () => {
  /** Counts every `Array.prototype.filter` call made while `fn` runs. The second array this ticket
   * removes was allocated by exactly that call - `settleRows` used to hand `mapSettle`'s own result
   * to `.filter()` to strip the `DROP` sentinel back out, once per chunk - so the count IS the
   * observable. Measured: restoring the pre-#179 body reads one call per chunk (`expected 3 to be
   * +0` over the three chunks below); the post-#179 body never calls it at all.
   *
   * `fn` must be fully synchronous: the patch is global for its duration, so an `await` inside would
   * let unrelated code be counted. */
  function countFilterCalls(fn: () => unknown): number {
    const original = Array.prototype.filter;
    let calls = 0;
    // oxlint-disable-next-line no-extend-native -- restored in the `finally` below; the count is the
    // whole point of the case, and no non-global seam reports it.
    Array.prototype.filter = function (this: unknown[], ...args: Parameters<typeof original>) {
      calls++;
      return original.apply(this, args);
    } as typeof original;
    try {
      fn();
    } finally {
      Array.prototype.filter = original;
    }
    return calls;
  }

  it("calls Array.prototype.filter zero times over three armed chunks", () => {
    const armed = new Transformer<number, number>()
      .onError(() => DROP)
      .map((x: number) => {
        if (x % 2 === 1) throw new Error(`odd: ${x}`);
        return x;
      });
    const run = armed.runnable();
    const ctx = new SimpleContextManager();
    const chunks = [
      [0, 1, 2],
      [3, 4, 5],
      [6, 7, 8],
    ];

    const kept: number[][] = [];
    const calls = countFilterCalls(() => {
      for (const chunk of chunks) kept.push(run(chunk, ctx) as number[]);
    });

    // The output is unchanged by the allocation change - assert it in the same case, so a run that
    // computed something else cannot read as a cheap one.
    expect(kept).toEqual([[0, 2], [4], [6, 8]]);
    expect(calls).toBe(0);
  });
});
