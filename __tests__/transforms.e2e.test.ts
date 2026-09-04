/**
 * transforms.e2e.test.ts — every `Transformer` operation proven through an ENTIRE PIPELINE RUN
 * (`new Pipeline(input).apply(transformer).toArray()`), never by poking a strategy/util/context
 * function in isolation. A behavior is only "covered" here if it changes the output (or context) of
 * a full run — the same way a caller would observe it. Chunk-level ops (`reduce`/`loop`/`catch`/
 * `setChunker`) use `.apply()` with an explicit `chunkSize` so the run actually crosses chunk
 * boundaries; `Pipeline.transform()` alone would collapse everything into one default 1000-item
 * chunk and hide it.
 */
import { describe, it, expect } from "vitest";
import { Pipeline, Transformer, SimpleContextManager } from "../src";

/** Run `input` through a real pipeline built on `transformer`, returning [results, contextSnapshot].
 * `Pipeline.toArray()` itself carries no context slot (#744) - this LOCAL helper builds its own
 * tuple from `.contextManager` afterward, so every call site below keeps reading `[results, ctx]`. */
async function run<I, O>(
  input: I[],
  transformer: Transformer<I, O>,
  context?: SimpleContextManager,
): Promise<[O[], Record<string, unknown>]> {
  const pipeline = context ? new Pipeline(input, { context }) : new Pipeline(input);
  const applied = pipeline.apply(transformer);
  const results = await applied.toArray();
  return [results, applied.contextManager.toDict()];
}

const T = <I>() => new Transformer<I, I>();
/** A chunk-sized transformer, so a run crosses chunk boundaries (chunk-level ops need this). */
const chunked = <I>(chunkSize: number) => new Transformer<I, I>({ chunkSize });

describe("transforms e2e — element ops through a full pipeline run", () => {
  it("map transforms every item; the 2-arg form receives the run context", async () => {
    const [plain] = await run(
      [1, 2, 3],
      T<number>().map((x) => x * 2),
    );
    expect(plain).toEqual([2, 4, 6]);

    const [withCtx] = await run(
      [1, 2, 3],
      T<number>().map((x, ctx) => x * (ctx.getOrDefault("mult", 1) as number)),
      new SimpleContextManager({ mult: 10 }),
    );
    expect(withCtx).toEqual([10, 20, 30]);
  });

  it("filter keeps matching items; the 2-arg form receives the run context", async () => {
    const [plain] = await run(
      [1, 2, 3, 4],
      T<number>().filter((x) => x % 2 === 0),
    );
    expect(plain).toEqual([2, 4]);

    const [withCtx] = await run(
      [1, 2, 3, 4, 5],
      T<number>().filter((x, ctx) => x > (ctx.getOrDefault("min", 0) as number)),
      new SimpleContextManager({ min: 3 }),
    );
    expect(withCtx).toEqual([4, 5]);
  });

  it("flatten expands arrays and flatMap maps-then-flattens", async () => {
    const [flat] = await run(
      [
        [1, 2],
        [3, 4],
      ],
      new Transformer<number[], number[]>().flatten<number>(),
    );
    expect(flat).toEqual([1, 2, 3, 4]);

    const [fm] = await run(
      [1, 2, 3],
      T<number>().flatMap((x) => [x, x * 10]),
    );
    expect(fm).toEqual([1, 10, 2, 20, 3, 30]);
  });

  it("tap runs a side effect and passes items through unchanged (function + transformer forms)", async () => {
    const seen: number[] = [];
    const [passed] = await run(
      [1, 2, 3],
      T<number>().tap((x) => {
        seen.push(x as number);
      }),
    );
    expect(passed).toEqual([1, 2, 3]);
    expect(seen).toEqual([1, 2, 3]);

    // tap(transformer) form: the sub-transformer runs for its effect, the stream is untouched.
    const tapped: number[] = [];
    const [passed2] = await run(
      [1, 2, 3],
      T<number>().tap(
        new Transformer<number, unknown>().map((x) => {
          tapped.push(x * 100);
          return x;
        }),
      ),
    );
    expect(passed2).toEqual([1, 2, 3]);
    expect(tapped).toEqual([100, 200, 300]);
  });

  it("apply composes a sub-transformer into the chain", async () => {
    const [out] = await run(
      [1, 2, 3, 4],
      T<number>().apply((t) => t.map((x) => x + 1).filter((x) => x % 2 === 0)),
    );
    expect(out).toEqual([2, 4]);
  });

  it("a tap that mutates context accumulates across the whole run", async () => {
    const [, ctx] = await run(
      [1, 2, 3],
      T<number>().tap((_, c) => c.set("count", (c.getOrDefault("count", 0) as number) + 1)),
      new SimpleContextManager({ count: 0 }),
    );
    expect(ctx.count).toBe(3);
  });
});

describe("transforms e2e — chunk-level ops (run crosses chunk boundaries)", () => {
  it("reduce collapses each chunk; chunk size decides the grouping (observable in output)", async () => {
    const sum = (a: number, b: number) => a + b;
    expect((await run([1, 2, 3, 4, 5], chunked<number>(3).reduce(sum, 0)))[0]).toEqual([6, 9]);
    // A different chunk size ⇒ different per-chunk grouping ⇒ different output: proves chunking.
    expect((await run([1, 2, 3, 4, 5], chunked<number>(2).reduce(sum, 0)))[0]).toEqual([3, 7, 5]);
    // Initial value applied per chunk.
    expect((await run([1, 2, 3, 4], chunked<number>(2).reduce(sum, 100)))[0]).toEqual([103, 107]);
  });

  it("reduce is context-aware and composes with map before/after", async () => {
    const [ctxOut] = await run(
      [1, 2, 3],
      chunked<number>(3).reduce((acc, x, c) => acc + x * (c.getOrDefault("mult", 1) as number), 0),
      new SimpleContextManager({ mult: 2 }),
    );
    expect(ctxOut).toEqual([12]);

    const [chained] = await run(
      [1, 2, 3],
      chunked<number>(3)
        .map((x) => x * 2)
        .reduce((a: number, b: number) => a + b, 0)
        .map((sum: number) => `sum:${sum}`),
    );
    expect(chained).toEqual(["sum:12"]);
  });

  it("terminal reduce (perChunk:false) folds the entire dataset to one value", async () => {
    const terminal = chunked<number>(2).reduce((a: number, b: number) => a + b, 0, {
      perChunk: false,
    });
    const collect = async (it: AsyncIterable<number>) => {
      const out: number[] = [];
      for await (const v of it) out.push(v);
      return out;
    };
    expect(await collect(terminal([1, 2, 3, 4, 5]))).toEqual([15]);
    expect(await collect(terminal([]))).toEqual([0]); // empty ⇒ initial value
    // Reusable across invocations.
    expect(await collect(terminal([10, 20, 30]))).toEqual([60]);
  });

  it("loop re-applies a sub-transformer per chunk until the condition fails or maxIterations", async () => {
    const doubler = new Transformer<number, number>({ transform: (c) => c.map((x) => x * 2) });
    // [1,2,3] -> [2,4,6] (all <=10) -> [4,8,12] (12>10, stop)
    expect(
      (
        await run(
          [1, 2, 3],
          chunked<number>(5).loop(doubler, (c) => c.every((x) => x <= 10)),
        )
      )[0],
    ).toEqual([4, 8, 12]);

    const inc = new Transformer<number, number>({ transform: (c) => c.map((x) => x + 1) });
    // Always-true condition, capped at 3 iterations: [1,2] -> [2,3] -> [3,4] -> [4,5]
    expect(
      (
        await run(
          [1, 2],
          chunked<number>(5).loop(inc, () => true, 3),
        )
      )[0],
    ).toEqual([4, 5]);
    // Condition false up front: data passes through untouched.
    expect(
      (
        await run(
          [1, 2, 3],
          chunked<number>(5).loop(doubler, () => false),
        )
      )[0],
    ).toEqual([1, 2, 3]);
  });

  it("catch runs a sub-pipeline, swallowing a throwing chunk to empty and reporting it", async () => {
    // No error ⇒ sub-pipeline output.
    expect(
      (
        await run(
          [1, 2, 3],
          chunked<number>(5).catch((t) => t.map((x) => x * 2)),
        )
      )[0],
    ).toEqual([2, 4, 6]);

    // A chunk that throws yields nothing; independent chunks still succeed.
    const [independent] = await run(
      [1, 2, 3, 4],
      chunked<number>(2).catch((t) =>
        t.map((x) => {
          if (x === 2) throw new Error("boom");
          return x * 10;
        }),
      ),
    );
    expect(independent).toEqual([30, 40]); // chunk [1,2] failed, chunk [3,4] survived

    // onError callback sees the failing chunk + error.
    const errors: { chunk: number[]; message: string }[] = [];
    await run(
      [1, 2, 3],
      chunked<number>(5).catch(
        (t) =>
          t.map((x) => {
            if (x === 2) throw new Error("Test error");
            return x;
          }),
        (chunk, error) => errors.push({ chunk: [...chunk], message: error.message }),
      ),
    );
    expect(errors).toEqual([{ chunk: [1, 2, 3], message: "Test error" }]);
  });

  it("shortCircuit aborts the run when its (optionally context-driven) condition holds", async () => {
    // Condition false: passthrough.
    expect(
      (
        await run(
          [1, 2, 3],
          chunked<number>(5).shortCircuit(() => false),
        )
      )[0],
    ).toEqual([1, 2, 3]);

    // Condition true: the whole run rejects.
    await expect(
      run(
        [1, 2, 3],
        chunked<number>(5).shortCircuit(() => true),
      ),
    ).rejects.toThrow(new Error("Short-circuit condition met, stopping execution."));

    // Context-driven, per chunk of 1: stops after the 2nd item once count reaches 2.
    let processed = 0;
    await expect(
      run(
        [1, 2, 3],
        chunked<number>(1)
          .tap((_, c) => {
            processed++;
            c.set("count", processed);
          })
          .shortCircuit((c) => (c.getOrDefault("count", 0) as number) >= 2),
        new SimpleContextManager(),
      ),
    ).rejects.toThrow();
    expect(processed).toBe(2);
  });

  it("setChunker overrides chunk size; the custom boundaries are visible in the run output", async () => {
    // Pair items into chunks of 2 regardless of chunkSize.
    const pairs = async function* (data: AsyncIterable<number>) {
      const buf: number[] = [];
      for await (const item of data) {
        buf.push(item);
        if (buf.length === 2) {
          yield [...buf];
          buf.length = 0;
        }
      }
      if (buf.length) yield buf;
    };
    // The base transform maps each chunk to its size, so the output stream IS the chunk boundaries.
    const transformer = new Transformer<number, number>({
      chunkSize: 100, // deliberately large — setChunker must win
      transform: (chunk) => [chunk.length],
    }).setChunker(pairs);

    const [out] = await run([1, 2, 3, 4, 5], transformer);
    expect(out).toEqual([2, 2, 1]); // pairs: [1,2], [3,4], [5]
  });
});
