/**
 * #90 L5 - a runner takes a built pipeline and executes it somewhere else.
 *
 * Every case here asserts the same thing in a different shape: a runner returns what its CLASS
 * counterpart returns for the same chain. That equivalence is the whole point of the layer - a
 * caller builds a pipeline once, tests it synchronously, and decides later where it runs.
 */

import { describe, it, expect } from "vitest";
import { Pipeline } from "@src/pipeline";
import { ConcurrentPipeline } from "@src/pipelines/concurrent";
import { ConcurrentRunner } from "@src/runners/concurrent";
import { SimpleContextManager } from "@src/context/simple";

describe("#90 L5 - ConcurrentRunner runs a pipeline it is handed", () => {
  it("returns what a plain drain returns, for a chain with no concurrency to exploit", async () => {
    const pipeline = new Pipeline()
      .from([1, 2, 3, 4, 5])
      .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 4));

    expect(pipeline.toArray()).toEqual([6, 8, 10]);
    expect(await new ConcurrentRunner({ maxConcurrency: 3 }).run(pipeline)).toEqual([6, 8, 10]);
  });

  it("returns what ConcurrentPipeline returns for the same partitioned reduce", async () => {
    // #62's contract: one accumulator per partition. The runner must reproduce it exactly, since it
    // re-drives the plan onto that very class rather than reimplementing the fan-out.
    const viaClass = await new ConcurrentPipeline<number>({ maxConcurrency: 3 })
      .from([1, 2, 3, 4, 5, 6])
      .buffer(2)
      .reduce((acc: number, x: number) => acc + x, 0)
      .toArray();

    const viaRunner = await new ConcurrentRunner({ maxConcurrency: 3 }).run(
      new Pipeline()
        .from([1, 2, 3, 4, 5, 6])
        .buffer(2)
        .reduce((acc: number, x: number) => acc + x, 0),
    );

    expect(viaClass).toEqual([3, 7, 11]);
    expect(viaRunner).toEqual(viaClass);
  });

  it("honours a .local() region, so a pinned reduce still folds the whole stream", async () => {
    const out = await new ConcurrentRunner({ maxConcurrency: 3 }).run(
      new Pipeline()
        .from([1, 2, 3, 4, 5, 6])
        .buffer(2)
        .local((p) => p.reduce((acc: number, x: number) => acc + x, 0)),
    );

    expect(out).toEqual([21]);
  });

  it("runs a chain of several stages in order", async () => {
    const pipeline = new Pipeline()
      .from([1, 2, 3])
      .transform((t) => t.map((x: number) => x * 2))
      .transform((t) => t.map((x: number) => x + 1))
      .transform((t) => t.map((x: number) => x * 10));

    expect(pipeline.toArray()).toEqual([30, 50, 70]);
    expect(await new ConcurrentRunner({ maxConcurrency: 2 }).run(pipeline)).toEqual([30, 50, 70]);
  });

  it("runs an async-sourced pipeline too", async () => {
    async function* source(): AsyncGenerator<number> {
      for (const x of [1, 2, 3]) yield x;
    }

    const out = await new ConcurrentRunner({ maxConcurrency: 2 }).run(
      new Pipeline().from(source()).transform((t) => t.map(async (x: number) => x * 2)),
    );

    expect(out).toEqual([2, 4, 6]);
  });

  it("carries the caller's own context manager through, as the same instance", async () => {
    const context = new SimpleContextManager();
    context.set("multiplier", 10);

    const out = await new ConcurrentRunner({ maxConcurrency: 2 }).run(
      new Pipeline({ context })
        .from([1, 2, 3])
        .transform((t) => t.map((x: number, ctx) => x * (ctx.get("multiplier") as number))),
    );

    expect(out).toEqual([10, 20, 30]);
  });

  it("carries a registered run handler, so a failing chunk still drops rather than throwing", async () => {
    const seen: string[] = [];

    const out = await new ConcurrentRunner({ maxConcurrency: 2 }).run(
      new Pipeline()
        .from([1, 2, 3])
        .buffer(1)
        .onError((error) => void seen.push(error.message))
        .transform((t) =>
          t.map((x: number) => {
            if (x === 2) throw new Error("boom");
            return x;
          }),
        ),
    );

    expect(out).toEqual([1, 3]);
    expect(seen).toEqual(["boom"]);
  });

  it("refuses a pipeline that has no source, at compile time and at runtime", async () => {
    // `RunnablePipeline<T>` is `Pipeline<T, "sync" | "async", SourcePolicy>`, so an `"unset"`
    // pipeline is not one. The runtime throw below only covers a caller who casts past that.
    // Declared, never called: the assertion is the compile, and running it would leave a rejected
    // promise with no handler.
    const refused = (): unknown =>
      // @ts-expect-error - TS2345: 'Pipeline<number, "unset", "shape">' is not a RunnablePipeline
      new ConcurrentRunner().run(new Pipeline<number>());
    expect(typeof refused).toBe("function");

    const sourceless = new Pipeline<number>() as unknown as Parameters<
      ConcurrentRunner["run"]
    >[0] as never;
    await expect(new ConcurrentRunner().run(sourceless)).rejects.toThrow(
      "no source: call .from(data) before handing a pipeline to a runner",
    );
  });

  it("runs the same pipeline object twice, giving the same answer both times", async () => {
    // A pipeline is a description, so running it is not consuming it.
    const pipeline = new Pipeline().from([1, 2, 3]).transform((t) => t.map((x: number) => x * 2));
    const runner = new ConcurrentRunner({ maxConcurrency: 2 });

    expect(await runner.run(pipeline)).toEqual([2, 4, 6]);
    expect(await runner.run(pipeline)).toEqual([2, 4, 6]);
  });
});
