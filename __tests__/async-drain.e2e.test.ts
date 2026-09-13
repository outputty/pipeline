/**
 * async-drain.e2e.test.ts — #179's Done-when 3, 8 and 9: every async terminal on `PipelineResult`
 * drains the CHUNK view and walks each chunk in a synchronous inner loop, rather than a flattened
 * per-item stream that cost one microtask per row.
 *
 * The output of every terminal is unchanged, so the cases here assert two things output alone
 * cannot show: the promise count per row, and that an early stop still closes its source.
 */
import { describe, it, expect } from "vitest";
import { Pipeline } from "@src/pipeline";
import { ConcurrentPipeline } from "@src/pipelines/concurrent";
import { closingAsyncSource, countPromisesAsync, chunksOf } from "./helpers/sequences";

/** A genuinely async source of `0..count-1` - an async generator, so the chain runs the async
 * engine whatever the class. */
async function* asyncRange(count: number): AsyncGenerator<number> {
  for (let i = 0; i < count; i++) yield i;
}

const N = 10_000;

describe("#179 the async terminals drain chunks, not a flattened item stream (Done-when 3)", () => {
  it("costs .forEach() no more per row than .toArray() on the identical chain", async () => {
    const build = (): ConcurrentPipeline<number, number> =>
      new ConcurrentPipeline<number>({ maxConcurrency: 4 }).buffer(1000);

    const collected = await countPromisesAsync(() => build()(asyncRange(N)).toArray());
    const each = await countPromisesAsync(() => {
      let seen = 0;
      return build()(asyncRange(N)).forEach(() => {
        seen++;
      });
    });

    // Both terminals now pay the source's own per-row floor and nothing on top of it: an async
    // generator costs 4.000 promises per row before any package code runs, and `.buffer(1000)` a
    // further 3.001. Measured on this exact chain, output asserted identical: `.toArray()` fell
    // from 12.009 per row to 7.006 and `.forEach()` from 14.009 to 7.008.
    //
    // The ceiling is asserted as well as the gap. `each < collected + 1` alone passes if BOTH
    // regress together, which is precisely what a return to the flattened drain would do.
    expect(collected / N).toBeLessThan(8);
    expect(each / N).toBeLessThan(collected / N + 1);
  });

  it("runs a synchronous .forEach() callback in order, every item, one array", async () => {
    const seen: number[] = [];
    await new Pipeline<number>()
      .buffer(3)(asyncRange(7))
      .forEach((x) => {
        seen.push(x);
      });
    expect(seen).toEqual([0, 1, 2, 3, 4, 5, 6]);
  });

  it("settles an async .forEach() callback before the next item runs", async () => {
    const order: string[] = [];
    await new Pipeline<number>()
      .buffer(2)(asyncRange(4))
      .forEach(async (x) => {
        order.push(`start ${x}`);
        await Promise.resolve();
        order.push(`end ${x}`);
      });
    expect(order).toEqual([
      "start 0",
      "end 0",
      "start 1",
      "end 1",
      "start 2",
      "end 2",
      "start 3",
      "end 3",
    ]);
  });

  it("yields items, in order, from the async iteration protocol", async () => {
    const seen: number[] = [];
    for await (const item of new Pipeline<number>().buffer(2)(asyncRange(5))) {
      seen.push(item);
    }
    expect(seen).toEqual([0, 1, 2, 3, 4]);
  });

  it("runs every stage for .consume(), collecting nothing", async () => {
    const seen: number[] = [];
    const result = await new Pipeline<number>()
      .buffer(2)
      .tap((x: number) => {
        seen.push(x);
      })(asyncRange(5))
      .consume();
    expect(result).toBeUndefined();
    expect(seen).toEqual([0, 1, 2, 3, 4]);
  });
});

describe("#179 an early stop still closes its source (Done-when 8)", () => {
  it("stops .first(2) at .buffer(3), pulling exactly one chunk and running the source's finally", async () => {
    const state = { closed: false };
    const pulled: number[] = [];

    async function* counted(): AsyncGenerator<number> {
      try {
        for (let i = 0; ; i++) {
          pulled.push(i);
          yield i;
        }
      } finally {
        state.closed = true;
      }
    }

    const got = await new Pipeline<number>().buffer(3)(counted()).first(2);
    expect(got).toEqual([0, 1]);
    expect(pulled).toEqual([0, 1, 2]);
    expect(state.closed).toBe(true);
  });

  it("closes an async source when the async iteration protocol breaks early", async () => {
    const state = { closed: false };
    const seen: number[] = [];
    for await (const item of new Pipeline<number>().buffer(2)(closingAsyncSource(state))) {
      seen.push(item);
      if (seen.length === 3) break;
    }
    expect(seen).toEqual([0, 1, 2]);
    expect(state.closed).toBe(true);
  });
});

describe("#179 the empty-chunk and pending-slot shapes are unchanged (Done-when 9)", () => {
  const sum = (acc: number, x: number): number => acc + x;

  it("agrees across chunks(), toArray() and first(1) on a pending-slot reduce", async () => {
    const build = (): Pipeline<number, "async", number> =>
      new Pipeline<number>()
        .buffer(2)
        .transform((t) => t.map(async (x: number) => x))
        .reduce(sum, 0) as unknown as Pipeline<number, "async", number>;

    const input = [1, 2, 3, 4, 5];
    expect(await chunksOf(build()(input))).toEqual([[15]]);
    expect(await build()(input).toArray()).toEqual([15]);
    expect(await build()(input).first(1)).toEqual([15]);
  });

  it("drops the chunks an async filter emptied", async () => {
    const build = (): Pipeline<number, "async", number> =>
      new Pipeline<number>()
        .buffer(2)
        .transform((t) => t.filter(async (x: number) => x > 4)) as unknown as Pipeline<
        number,
        "async",
        number
      >;

    const input = [1, 2, 3, 4, 5, 6];
    expect(await build()(input).toArray()).toEqual([5, 6]);
    expect(await build()(input).first(1)).toEqual([5]);
  });
});
