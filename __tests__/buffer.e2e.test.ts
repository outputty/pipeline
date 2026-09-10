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

    const out = await new ConcurrentPipeline({ maxConcurrency: 8 })
      .from([1, 2, 3, 4, 5, 6, 7, 8])
      .buffer(2)
      .apply(boundaryProbe(stage1Input))
      .transform((t) => t.map((x: number) => x + 1))
      .buffer(4)
      .apply(boundaryProbe(stage2Input))
      .transform((t) => t.map((x: number) => x * 2))
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

    await new Pipeline()
      .from([1, 2, 3, 4])
      .apply(boundaryProbe(stage1Input))
      .transform((t) => t.map((x: number) => x * 2))
      .apply(boundaryProbe(stage2Input))
      .transform((t) => t.map((x: number) => x + 1))
      .toArray();

    expect(stage1Input).toEqual([[1, 2, 3, 4]]);
    expect(stage2Input).toEqual([[2, 4, 6, 8]]);
  });
});

describe("#39 two .buffer() calls back to back collapse to the last one (Done-when 3)", () => {
  it("matches .buffer(4) alone over [1..9]", async () => {
    const chained: number[][] = [];
    for await (const chunk of new Pipeline()
      .from([1, 2, 3, 4, 5, 6, 7, 8, 9])
      .buffer(2)
      .buffer(3)
      .buffer(4)) {
      chained.push(chunk);
    }

    const direct: number[][] = [];
    for await (const chunk of new Pipeline().from([1, 2, 3, 4, 5, 6, 7, 8, 9]).buffer(4)) {
      direct.push(chunk);
    }

    expect(chained).toEqual([[1, 2, 3, 4], [5, 6, 7, 8], [9]]);
    expect(direct).toEqual(chained);
  });
});

// #39's own Done-when 4 and 5 - lifecycle hooks firing identically across every consumption path,
// and async iteration reading the same persisted chunk stream a terminal op does - are covered by
// __tests__/transforms.e2e.test.ts's single tap observation case now that #72 deletes hooks in
// favor of .tap(): the mechanism .tap() replaced them with is what that case proves fires the
// same way on .toArray(), on async iteration and on a .local() stage.
