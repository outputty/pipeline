/**
 * transforms.e2e.test.ts — every `Transformer` operation proven through an ENTIRE PIPELINE RUN
 * (`new Pipeline<number>().from(input).apply(transformer).toArray()`), never by poking a strategy/util/context
 * function in isolation. A behavior is only "covered" here if it changes the output (or context) of
 * a full run — the same way a caller would observe it. Chunk-level ops (`reduce`/`loop`) pass
 * `run()` an explicit `bufferSize` so the run actually crosses chunk boundaries (#39: the
 * `Pipeline`'s own `.buffer()`, never a `Transformer` knob) - `Pipeline.transform()` alone would
 * collapse everything into one default 1000-item chunk and hide it.
 */
import { describe, it, expect } from "vitest";
import { Pipeline, ConcurrentPipeline, Transformer, SimpleContextManager, DROP } from "../src";

/** The ticket's own canonical example (#78): throws `Invalid: <s>` for anything that doesn't parse
 * as an int. */
const parseStrict = (s: string): number => {
  const n = parseInt(s);
  if (isNaN(n)) throw new Error(`Invalid: ${s}`);
  return n;
};

/** Run `input` through a real pipeline built on `transformer`, returning [results, contextSnapshot].
 * `Pipeline.toArray()` itself carries no context slot (#744) - this LOCAL helper builds its own
 * tuple from `.contextManager` afterward, so every call site below keeps reading `[results, ctx]`.
 * `bufferSize`, when given, calls `.buffer()` before `.apply()` (#39). */
async function run<I, O>(
  input: I[],
  transformer: Transformer<I, O>,
  context?: SimpleContextManager,
  bufferSize?: number,
): Promise<[O[], Record<string, unknown>]> {
  let pipeline: Pipeline<I, "sync"> = context
    ? new Pipeline<number>({ context })(input)
    : new Pipeline<number>()(input);
  if (bufferSize !== undefined) pipeline = pipeline.buffer(bufferSize);
  const applied = pipeline.apply(transformer);
  const results = await applied.toArray();
  return [results, applied.contextManager.toDict()];
}

const T = <I>() => new Transformer<I, I>({ transform: (chunk) => chunk });

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

  it("tap: both overloads pass items through unchanged, its context writes accumulate across the whole run, and it fires identically on .toArray(), async iteration and a .local() stage (#72)", async () => {
    // Both overloads pass data through unchanged.
    const seenByFn: number[] = [];
    const [passedByFn] = await run(
      [1, 2, 3],
      T<number>().tap((x) => {
        seenByFn.push(x as number);
      }),
    );
    expect(passedByFn).toEqual([1, 2, 3]);
    expect(seenByFn).toEqual([1, 2, 3]);

    // tap(transformer) form: the sub-transformer runs for its effect, the stream is untouched.
    // `Transformer<number, number>` here, not `<number, unknown>` - the exact Done-when 7 shape,
    // so this pins the invariance fix (`t.tap(someTransformer)` used to fail to typecheck when the
    // sub-transformer's own Out was concrete rather than already `unknown`).
    const seenByTransformer: number[] = [];
    const [passedByTransformer] = await run(
      [1, 2, 3],
      T<number>().tap(
        new Transformer<number, number>().map((x) => {
          seenByTransformer.push(x * 100);
          return x * 2;
        }),
      ),
    );
    expect(passedByTransformer).toEqual([1, 2, 3]);
    expect(seenByTransformer).toEqual([100, 200, 300]);

    // Fires identically, its context writes accumulating across the whole run, whichever of the
    // three consumption paths drains the SAME persisted chunk stream (#39): .toArray(), async
    // iteration, or a ConcurrentPipeline .local() stage - #72's own replacement for the deleted
    // lifecycle-hooks knob's old Done-when 4/5 (#39) coverage.
    async function countFor(
      drain: (tapped: Transformer<number, number>) => Promise<Record<string, unknown>>,
    ): Promise<number> {
      const tapped = T<number>().tap((_x, c) =>
        c.set("count", (c.getOrDefault("count", 0) as number) + 1),
      );
      const ctx = await drain(tapped);
      return ctx.count as number;
    }

    const viaToArray = await countFor(async (tapped) => {
      const pipeline = new Pipeline<number>().apply(tapped);
      await pipeline([1, 2, 3]).toArray();
      return pipeline.contextManager.toDict();
    });
    const viaAsyncIteration = await countFor(async (tapped) => {
      const pipeline = new Pipeline<number>().apply(tapped);
      for await (const _chunk of pipeline) {
        // drain
      }
      return pipeline.contextManager.toDict();
    });
    const viaLocalStage = await countFor(async (tapped) => {
      const pipeline = new ConcurrentPipeline<number>().local((p) => p.apply(tapped));
      await pipeline([1, 2, 3]).toArray();
      return pipeline.contextManager.toDict();
    });

    expect(viaToArray).toBe(3);
    expect(viaAsyncIteration).toBe(3);
    expect(viaLocalStage).toBe(3);
  });

  it("apply composes a sub-transformer into the chain", async () => {
    const [out] = await run(
      [1, 2, 3, 4],
      T<number>().apply((t) => t.map((x) => x + 1).filter((x) => x % 2 === 0)),
    );
    expect(out).toEqual([2, 4]);
  });
});

describe("transforms e2e — chunk-level ops (run crosses chunk boundaries)", () => {
  it("reduce collapses each chunk; the Pipeline's own .buffer() size decides the grouping (observable in output)", async () => {
    const sum = (a: number, b: number) => a + b;
    expect((await run([1, 2, 3, 4, 5], T<number>().reduce(sum, 0), undefined, 3))[0]).toEqual([
      6, 9,
    ]);
    // A different buffer size ⇒ different per-chunk grouping ⇒ different output: proves chunking.
    expect((await run([1, 2, 3, 4, 5], T<number>().reduce(sum, 0), undefined, 2))[0]).toEqual([
      3, 7, 5,
    ]);
    // Initial value applied per chunk.
    expect((await run([1, 2, 3, 4], T<number>().reduce(sum, 100), undefined, 2))[0]).toEqual([
      103, 107,
    ]);
  });

  it("reduce is context-aware and composes with map before/after", async () => {
    const [ctxOut] = await run(
      [1, 2, 3],
      T<number>().reduce((acc, x, c) => acc + x * (c.getOrDefault("mult", 1) as number), 0),
      new SimpleContextManager({ mult: 2 }),
      3,
    );
    expect(ctxOut).toEqual([12]);

    const [chained] = await run(
      [1, 2, 3],
      T<number>()
        .map((x) => x * 2)
        .reduce((a: number, b: number) => a + b, 0)
        .map((sum: number) => `sum:${sum}`),
      undefined,
      3,
    );
    expect(chained).toEqual(["sum:12"]);
  });

  it("loop re-applies a sub-transformer per chunk until the condition fails or maxIterations", async () => {
    const doubler = new Transformer<number, number>({ transform: (c) => c.map((x) => x * 2) });
    // [1,2,3] -> [2,4,6] (all <=10) -> [4,8,12] (12>10, stop)
    expect(
      (
        await run(
          [1, 2, 3],
          T<number>().loop(doubler, (c) => c.every((x) => x <= 10)),
          undefined,
          5,
        )
      )[0],
    ).toEqual([4, 8, 12]);

    const inc = new Transformer<number, number>({ transform: (c) => c.map((x) => x + 1) });
    // Always-true condition, capped at 3 iterations: [1,2] -> [2,3] -> [3,4] -> [4,5]
    expect(
      (
        await run(
          [1, 2],
          T<number>().loop(inc, () => true, 3),
          undefined,
          5,
        )
      )[0],
    ).toEqual([4, 5]);
    // Condition false up front: data passes through untouched.
    expect(
      (
        await run(
          [1, 2, 3],
          T<number>().loop(doubler, () => false),
          undefined,
          5,
        )
      )[0],
    ).toEqual([1, 2, 3]);
  });

  describe("onError — the ROW handler (#78; replaces #40's chunk-level notification and .catch(), both deleted)", () => {
    it("Done-when 1: t.onError(() => DROP).map(parseStrict) drops only the failing rows", async () => {
      const [out] = await run(
        ["a", "b", "3", "d", "5"],
        T<string>()
          .onError(() => DROP)
          .map(parseStrict),
      );
      expect(out).toEqual([3, 5]);
    });

    it("Done-when 2: onError is position-independent — after .map() prints the same [3,5]", async () => {
      const [out] = await run(
        ["a", "b", "3", "d", "5"],
        T<string>()
          .map(parseStrict)
          .onError(() => DROP),
      );
      expect(out).toEqual([3, 5]);
    });

    it("Done-when 3: a returned value replaces the row, keeping its place and the chunk's length", async () => {
      const [out] = await run(
        ["a", "b", "3", "d", "5"],
        T<string>()
          .onError(() => -1)
          .map(parseStrict),
      );
      expect(out).toEqual([-1, -1, 3, -1, 5]);
    });

    it("Done-when 4: reaches .filter() too — a throwing predicate drops via the row handler", async () => {
      const [out] = await run(
        ["a", "b", "3", "d", "5"],
        T<string>()
          .onError(() => DROP)
          .filter((s) => parseStrict(s) > 3),
      );
      expect(out).toEqual(["5"]);
    });

    it("Done-when 5: reaches Transformer.reduce()'s fold step — the accumulator skips a dropped row", async () => {
      const [out] = await run(
        ["a", "b", "3", "d", "5"],
        T<string>()
          .onError(() => DROP)
          .reduce((acc, s) => acc + parseStrict(s), 0),
        undefined,
        5,
      );
      expect(out).toEqual([8]);
    });

    it("Done-when 6: an async row handler is awaited in place; a dead-letter store receives every dropped row", async () => {
      const deadLetter: string[] = [];
      const [out] = await run(
        ["a", "b", "3", "d", "5"],
        T<string>()
          .onError(async (item) => {
            await Promise.resolve(); // proves the handler is genuinely awaited, not fire-and-forget
            deadLetter.push(item as string);
            return DROP;
          })
          .map(parseStrict),
      );
      expect(out).toEqual([3, 5]);
      expect(deadLetter).toEqual(["a", "b", "d"]);
    });

    it("reaches .flatMap() — a recovered value becomes that row's one output item", async () => {
      const [out] = await run(
        ["a", "1,2", "b"],
        T<string>()
          .onError(() => DROP)
          .flatMap((s) =>
            s
              .split(",")
              .map(Number)
              .map((n) => {
                if (isNaN(n)) throw new Error(`Invalid: ${s}`);
                return n;
              }),
          ),
      );
      expect(out).toEqual([1, 2]);
    });

    it("reaches .tap(fn) — DROP removes the row, any other return keeps it (data untouched)", async () => {
      const seen: number[] = [];
      const [out] = await run(
        [1, 2, 3],
        T<number>()
          .onError(() => DROP)
          .tap((x) => {
            if (x === 2) throw new Error("boom");
            seen.push(x);
          }),
      );
      expect(out).toEqual([1, 3]);
      expect(seen).toEqual([1, 3]);
    });

    it("a rethrowing row handler escalates past the row: process() rejects with the original error", async () => {
      await expect(
        run(
          ["a", "3"],
          T<string>()
            .onError((_item, error) => {
              throw error;
            })
            .map(parseStrict),
        ),
      ).rejects.toThrow("Invalid: a");
    });

    it(".onError(() => DROP).map(fn) is a real recovery, never a silent no-op on success", async () => {
      // Control: no row throws, so the handler never runs and every row survives unchanged.
      const [out] = await run(
        ["1", "2", "3"],
        T<string>()
          .onError(() => DROP)
          .map(parseStrict),
      );
      expect(out).toEqual([1, 2, 3]);
    });
  });

  it("shortCircuit aborts the run when its (optionally context-driven) condition holds", async () => {
    // Condition false: passthrough.
    expect(
      (
        await run(
          [1, 2, 3],
          T<number>().shortCircuit(() => false),
          undefined,
          5,
        )
      )[0],
    ).toEqual([1, 2, 3]);

    // Condition true: the whole run rejects.
    await expect(
      run(
        [1, 2, 3],
        T<number>().shortCircuit(() => true),
        undefined,
        5,
      ),
    ).rejects.toThrow(new Error("Short-circuit condition met, stopping execution."));

    // Context-driven, per chunk of 1: stops after the 2nd item once count reaches 2.
    let processed = 0;
    await expect(
      run(
        [1, 2, 3],
        T<number>()
          .tap((_, c) => {
            processed++;
            c.set("count", processed);
          })
          .shortCircuit((c) => (c.getOrDefault("count", 0) as number) >= 2),
        new SimpleContextManager(),
        1,
      ),
    ).rejects.toThrow();
    expect(processed).toBe(2);
  });
});
