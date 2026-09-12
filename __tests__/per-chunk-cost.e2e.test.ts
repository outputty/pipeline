/**
 * #179 - the places this package spent work per ROW that only needed doing per CHUNK, proven against
 * the real classes. L1 covers `buildSyncChunkGenerator`'s array fast path (`src/utils/cut.ts`).
 *
 * Every case here asserts a BEHAVIOUR that must survive the optimisation, never a wall-clock number.
 * The time this ticket buys is gated by `pnpm bench:overhead` against `bench/baseline.json`, which is
 * where a timing assertion belongs; a test racing a nanosecond budget on a shared machine would fail
 * for reasons unrelated to the code under test (`bench/gate.ts`'s own header records that finding).
 */

import { describe, it, expect } from "vitest";

import { Pipeline } from "@src/pipeline";
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
    // A string is iterable AND length-bearing, so a `length`-based fast path would cut it into
    // characters here. The per-item arm is what a `Pipeline<string>` over a string source must keep
    // reaching, unchanged by this ticket.
    const cut = buildSyncChunkGenerator<string>(2);

    expect([...cut("abcde")]).toEqual([["a", "b"], ["c", "d"], ["e"]]);
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
