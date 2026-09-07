/**
 * buffer.e2e.test.ts — ticket #39's own Done-when cases 1-5, pinned before the seam exists.
 *
 * Chunking moves off `Transformer` entirely onto an explicit `Pipeline.buffer(size)` call: the
 * ONE place a cut ever happens, persisted through every later stage until called again. Every
 * case here is `it.fails` until L2 lands - flip to `it` there, per case - except Done-when 2
 * (already a plain `it`: it holds today, unlike the rest, so there is nothing to flip).
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
  it.fails("prints [4,6,8,10,12,14,16,18] with the two stages' own input boundaries", async () => {
    const stage1Input: number[][] = [];
    const stage2Input: number[][] = [];

    const out = await new ConcurrentPipeline([1, 2, 3, 4, 5, 6, 7, 8], { maxConcurrency: 8 })
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

    await new Pipeline([1, 2, 3, 4])
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
  it.fails("matches .buffer(4) alone over [1..9]", async () => {
    const chained: number[][] = [];
    for await (const chunk of new Pipeline([1, 2, 3, 4, 5, 6, 7, 8, 9])
      .buffer(2)
      .buffer(3)
      .buffer(4)) {
      chained.push(chunk);
    }

    const direct: number[][] = [];
    for await (const chunk of new Pipeline([1, 2, 3, 4, 5, 6, 7, 8, 9]).buffer(4)) {
      direct.push(chunk);
    }

    expect(chained).toEqual([[1, 2, 3, 4], [5, 6, 7, 8], [9]]);
    expect(direct).toEqual(chained);
  });
});

describe("#39 lifecycle hooks fire identically everywhere (Done-when 4)", () => {
  it.fails(
    "Pipeline, async iteration, and a ConcurrentPipeline local stage all see the same order and onError",
    async () => {
      async function orderFor(run: (hooked: Transformer<number, number>) => Promise<unknown>) {
        const order: string[] = [];
        const hooked = new Transformer<number, number>()
          .map((x: number) => x * 2)
          .withHooks({
            onStart: () => order.push("start"),
            onComplete: () => order.push("complete"),
          });
        await run(hooked);
        return order;
      }

      const viaToArray = await orderFor((hooked) => new Pipeline([1, 2]).apply(hooked).toArray());
      const viaAsyncIteration = await orderFor(async (hooked) => {
        for await (const _chunk of new Pipeline([1, 2]).apply(hooked)) {
          // drain
        }
      });
      const viaLocalStage = await orderFor((hooked) =>
        new ConcurrentPipeline([1, 2]).apply(hooked, { local: true }).toArray(),
      );

      expect(viaToArray).toEqual(["start", "complete"]);
      expect(viaAsyncIteration).toEqual(["start", "complete"]);
      expect(viaLocalStage).toEqual(["start", "complete"]);

      // Done-when 4 also names onError explicitly: a chunk-level throw must still notify it,
      // on every one of the same three paths, before the error propagates.
      async function errorSeenFor(run: (hooked: Transformer<number, number>) => Promise<unknown>) {
        let seen: Error | undefined;
        const hooked = new Transformer<number, number>()
          .map((x: number) => {
            if (x === 2) throw new Error("boom");
            return x;
          })
          .withHooks({
            onError: (e) => {
              seen = e;
            },
          });
        await expect(run(hooked)).rejects.toThrow("boom");
        return seen;
      }

      const errViaToArray = await errorSeenFor((hooked) =>
        new Pipeline([1, 2]).apply(hooked).toArray(),
      );
      const errViaAsyncIteration = await errorSeenFor(async (hooked) => {
        for await (const _chunk of new Pipeline([1, 2]).apply(hooked)) {
          // drain
        }
      });
      const errViaLocalStage = await errorSeenFor((hooked) =>
        new ConcurrentPipeline([1, 2]).apply(hooked, { local: true }).toArray(),
      );

      expect(errViaToArray?.message).toBe("boom");
      expect(errViaAsyncIteration?.message).toBe("boom");
      expect(errViaLocalStage?.message).toBe("boom");
    },
  );
});

describe("#39 async iteration reads the same persisted chunk stream a terminal op does (Done-when 5)", () => {
  it.fails("no inertKnobsOf/'not applied in source position' error throws any more", async () => {
    const hooked = new Transformer<number, number>()
      .map((x: number) => x * 2)
      .withHooks({
        onStart: () => {},
      });
    const pipeline = new Pipeline([1, 2, 3]).apply(hooked);

    const chunks: number[][] = [];
    for await (const chunk of pipeline) {
      chunks.push(chunk);
    }

    expect(chunks.flat()).toEqual([2, 4, 6]);
  });
});
