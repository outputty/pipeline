import { describe, it, expect } from "vitest";
import { Pipeline } from "@src/pipeline";
import { Transformer } from "@src/transformer";
import { SimpleContextManager } from "@src/context/simple";
import { LoggingContext, SealedContext } from "./fixtures/context-managers";
import { parseStrict } from "./helpers/sequences";

describe("Pipeline", () => {
  describe("constructor", () => {
    it("creates pipeline from sync iterable", async () => {
      const data = [1, 2, 3];
      const pipeline = new Pipeline<number>();
      const results = await pipeline(data).toArray();

      expect(results).toEqual([1, 2, 3]);
    });

    it("creates pipeline from async iterable", async () => {
      async function* asyncData() {
        yield 1;
        yield 2;
        yield 3;
      }

      const pipeline = new Pipeline<number>();
      const results = await pipeline(asyncData()).toArray();

      expect(results).toEqual([1, 2, 3]);
    });

    it("accepts custom context", async () => {
      const context = new SimpleContextManager({ key: "value" });
      const pipeline = new Pipeline<number>({ context });
      await pipeline([1, 2, 3]).toArray();
      const ctx = pipeline.contextManager.toDict();

      expect(ctx).toEqual({ key: "value" });
    });
  });

  describe("context", () => {
    it("sets context values via fluent API", async () => {
      const pipeline = new Pipeline<number>().context({ multiplier: 3 });
      await pipeline([1, 2, 3]).toArray();
      const ctx = pipeline.contextManager.toDict();

      expect(ctx).toEqual({ multiplier: 3 });
    });

    it("returns pipeline for chaining", async () => {
      const pipeline = new Pipeline<number>()

        .context({ key1: "value1" })
        .context({ key2: "value2" });

      await pipeline([1, 2, 3]).toArray();
      const ctx = pipeline.contextManager.toDict();

      expect(ctx).toEqual({ key1: "value1", key2: "value2" });
    });

    it("can be used with transformers", async () => {
      const results = await new Pipeline<number>()

        .context({ multiplier: 10 })
        .transform((t) =>
          t.map((x, ctx) => {
            const mult = ctx.getOrDefault("multiplier", 1) as number;
            return x * mult;
          }),
        )([1, 2, 3])
        .toArray();

      expect(results).toEqual([10, 20, 30]);
    });

    it("allows access via contextManager getter", () => {
      const pipeline = new Pipeline<number>().context({ key: "value" });

      expect(pipeline.contextManager.get("key")).toBe("value");
    });

    it("survives .context() as the SAME instance and receives its writes (#31, Done-when 1)", async () => {
      const mine = new LoggingContext();
      const afterContext = new Pipeline<number>({ context: mine }).context({ multiplier: 10 });

      expect(afterContext.contextManager).toBe(mine);

      await afterContext
        .transform((t) => t.map((x: number, ctx) => (ctx.set("k", x), x)))([1, 2])
        .toArray();

      expect(mine.constructor.name).toBe("LoggingContext");
      expect(mine.writes).toEqual(["multiplier", "k", "k"]);
    });

    it("propagates a manager's own rejection instead of bypassing it (#31, Done-when 2)", () => {
      const sealed = new SealedContext({ known: 1 });

      expect(() => new Pipeline<number>({ context: sealed }).context({ unknown: 2 })).toThrow(
        "SealedContext: unknown key 'unknown'",
      );
    });
  });

  describe("apply", () => {
    it("applies transformer to data", async () => {
      const pipeline = new Pipeline<number>();
      const transformer = new Transformer<number, number>().map((x: number) => x * 2);

      const results = await pipeline.apply(transformer)([1, 2, 3]).toArray();

      expect(results).toEqual([2, 4, 6]);
    });

    it("chains multiple transformers", async () => {
      const pipeline = new Pipeline<number>();
      const double = new Transformer<number, number>().map((x: number) => x * 2);
      const addOne = new Transformer<number, number>().map((x: number) => x + 1);

      const results = await pipeline.apply(double).apply(addOne)([1, 2, 3]).toArray();

      expect(results).toEqual([3, 5, 7]);
    });
  });

  describe("transform", () => {
    it("applies transformer builder function", async () => {
      const pipeline = new Pipeline<number>();

      const results = await pipeline
        .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 2))([1, 2, 3])
        .toArray();

      expect(results).toEqual([4, 6]);
    });

    it("chains with other operations", async () => {
      const pipeline = new Pipeline<string>();

      const results = await pipeline
        .transform((t) => t.map((s: string) => s.toUpperCase()))
        .transform((t) => t.map((s: string) => s + "!"))(["hello", "world"])
        .toArray();

      expect(results).toEqual(["HELLO!", "WORLD!"]);
    });
  });

  describe("buffer", () => {
    it("cuts the chunk boundary while maintaining item order", async () => {
      const pipeline = new Pipeline<number>();
      const results = await pipeline.buffer(2)([1, 2, 3, 4, 5]).toArray();

      expect(results).toEqual([1, 2, 3, 4, 5]);
    });

    it("works with empty input", async () => {
      const pipeline = new Pipeline<number>();
      const results = await pipeline.buffer(2)([]).toArray();

      expect(results).toEqual([]);
    });
  });

  describe("toArray", () => {
    it("collects all items to array", async () => {
      const pipeline = new Pipeline<number>();
      const results = await pipeline([1, 2, 3]).toArray();

      expect(results).toEqual([1, 2, 3]);
    });

    it("returns empty array for empty input", async () => {
      const pipeline = new Pipeline<number>();
      const results = await pipeline([]).toArray();

      expect(results).toEqual([]);
    });

    it("does not carry a context snapshot; .contextManager still resolves it afterward", async () => {
      const context = new SimpleContextManager({ key: "value" });
      const pipeline = new Pipeline<number>({ context });
      await pipeline([1]).toArray();
      const ctx = pipeline.contextManager.toDict();

      expect(ctx).toEqual({ key: "value" });
    });
  });

  describe("first", () => {
    it("returns first N elements", async () => {
      const pipeline = new Pipeline<number>();
      const results = await pipeline([1, 2, 3, 4, 5]).first(3);

      expect(results).toEqual([1, 2, 3]);
    });

    it("returns single element by default", async () => {
      const pipeline = new Pipeline<number>();
      const results = await pipeline([1, 2, 3]).first();

      expect(results).toEqual([1]);
    });

    it("returns all elements if N > length", async () => {
      const pipeline = new Pipeline<number>();
      const results = await pipeline([1, 2]).first(5);

      expect(results).toEqual([1, 2]);
    });

    it("throws if N < 1", () => {
      const pipeline = new Pipeline<number>();

      // Synchronously, not as a rejection (#90): a `"sync"` chain creates no `Promise`, so there is
      // nothing for a rejection to travel on. The same call on an async chain still rejects.
      expect(() => pipeline([1, 2, 3]).first(0)).toThrow(new Error("n must be at least 1"));
    });

    it("does not carry a context snapshot; .contextManager still resolves it afterward", async () => {
      const context = new SimpleContextManager({ key: "value" });
      const pipeline = new Pipeline<number>({ context });
      await pipeline([1, 2, 3]).first(2);
      const ctx = pipeline.contextManager.toDict();

      expect(ctx).toEqual({ key: "value" });
    });
  });

  describe("consume", () => {
    it("processes all items without collecting", async () => {
      let processed = 0;
      async function* data() {
        for (let i = 0; i < 5; i++) {
          processed++;
          yield i;
        }
      }

      const pipeline = new Pipeline<number>();
      await pipeline(data()).consume();

      expect(processed).toBe(5);
    });

    it("resolves undefined; .contextManager still resolves the context afterward", async () => {
      const context = new SimpleContextManager({ key: "value" });
      const pipeline = new Pipeline<number>({ context });
      const result = await pipeline([1, 2, 3]).consume();
      const ctx = pipeline.contextManager.toDict();

      expect(result).toBeUndefined();
      expect(ctx).toEqual({ key: "value" });
    });
  });

  describe("forEach", () => {
    it("applies function to each item", async () => {
      const items: number[] = [];
      const pipeline = new Pipeline<number>();

      await pipeline([1, 2, 3]).forEach((item) => {
        items.push(item * 2);
      });

      expect(items).toEqual([2, 4, 6]);
    });

    it("supports async functions", async () => {
      const items: number[] = [];
      const pipeline = new Pipeline<number>();

      await pipeline([1, 2, 3]).forEach(async (item) => {
        await Promise.resolve();
        items.push(item);
      });

      expect(items).toEqual([1, 2, 3]);
    });

    it("resolves undefined; .contextManager still resolves the context afterward", async () => {
      const context = new SimpleContextManager({ key: "value" });
      const pipeline = new Pipeline<number>({ context });
      const result = await pipeline([1, 2, 3]).forEach(() => {});
      const ctx = pipeline.contextManager.toDict();

      expect(result).toBeUndefined();
      expect(ctx).toEqual({ key: "value" });
    });
  });

  describe("branch", () => {
    it("routes items to different branches", async () => {
      const pipeline = new Pipeline<number>();

      const results = await pipeline.branch((b) =>
        b
          .when(
            "even",
            (x) => x % 2 === 0,
            (q) => q.transform((t) => t.map((x) => x * 10)),
          )
          .when(
            "odd",
            (x) => x % 2 !== 0,
            (q) => q.transform((t) => t.map((x) => x * 100)),
          ),
      )([1, 2, 3, 4, 5]);

      expect(results.even).toEqual([20, 40]);
      expect(results.odd).toEqual([100, 300, 500]);
    });

    it("uses first matching branch only", async () => {
      const pipeline = new Pipeline<number>();

      const results = await pipeline.branch((b) =>
        b
          .when(
            "positive",
            (x) => x > 0,
            (q) => q.transform((t) => t.map(() => "positive")),
          )
          .otherwise("all", (q) => q.transform((t) => t.map(() => "all"))),
      )([1, 2, 3]);

      // All items match 'positive' first
      expect(results.positive).toEqual(["positive", "positive", "positive"]);
      expect(results.all).toEqual([]);
    });

    it("handles empty input", async () => {
      const pipeline = new Pipeline<number>();

      const results = await pipeline.branch((b) => b.when("even", (x) => x % 2 === 0))([]);

      expect(results.even).toEqual([]);
    });

    it("handles no matching branches", async () => {
      const pipeline = new Pipeline<number>();

      const results = await pipeline.branch((b) => b.when("negative", (x) => x < 0))([1, 2, 3]);

      expect(results.negative).toEqual([]);
    });

    it("supports async predicates", async () => {
      const pipeline = new Pipeline<number>();

      const results = await pipeline.branch((b) =>
        b.when(
          "async",
          (x) => x > 1,
          (q) => q.transform((t) => t.map((x) => x * 2)),
        ),
      )([1, 2, 3]);

      expect(results.async).toEqual([4, 6]);
    });

    it("does not carry a context snapshot; .contextManager still resolves it afterward", async () => {
      const context = new SimpleContextManager({ key: "value" });
      const pipeline = new Pipeline<number>({ context });

      await pipeline.branch((b) => b.otherwise("all"))([1, 2]);
      const ctx = pipeline.contextManager.toDict();

      expect(ctx).toEqual({ key: "value" });
    });

    it("supports broadcast mode (firstMatch: false)", async () => {
      const pipeline = new Pipeline<number>();

      const results = await pipeline.branch((b) =>
        b
          .when(
            "even",
            (x) => x % 2 === 0,
            (q) => q.transform((t) => t.map((x) => x * 10)),
          )
          .when(
            "smallerThan4",
            (x) => x < 4,
            (q) => q.transform((t) => t.map((x) => x * 100)),
          )
          .broadcast(),
      )([1, 2, 3, 4, 5]);

      // In broadcast mode, items go to ALL matching branches
      // 1: matches smallerThan4 only -> 100
      // 2: matches both even and smallerThan4 -> 20, 200
      // 3: matches smallerThan4 only -> 300
      // 4: matches even only -> 40
      // 5: matches neither -> nothing
      expect(results.even).toEqual([20, 40]);
      expect(results.smallerThan4).toEqual([100, 200, 300]);
    });

    it("router mode (firstMatch: true) routes to first match only", async () => {
      const pipeline = new Pipeline<number>();

      const results = await pipeline.branch((b) =>
        b
          .when(
            "even",
            (x) => x % 2 === 0,
            (q) => q.transform((t) => t.map((x) => x * 10)),
          )
          .when(
            "smallerThan4",
            (x) => x < 4,
            (q) => q.transform((t) => t.map((x) => x * 100)),
          ),
      )([1, 2, 3, 4, 5]);

      // In router mode, items go to FIRST matching branch only
      // 1: matches smallerThan4 first? No, even is first but doesn't match -> smallerThan4 -> 100
      // 2: matches even first -> 20 (doesn't go to smallerThan4)
      // 3: matches smallerThan4 only -> 300
      // 4: matches even only -> 40
      // 5: matches neither -> nothing
      expect(results.even).toEqual([20, 40]);
      expect(results.smallerThan4).toEqual([100, 300]);
    });
  });

  describe("integration", () => {
    it("complex pipeline with multiple operations", async () => {
      const results = await new Pipeline<number>()

        .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 4))([1, 2, 3, 4, 5])
        .toArray();

      expect(results).toEqual([6, 8, 10]);
    });

    it("pipeline with chained apply calls", async () => {
      const double = new Transformer<number, number>().map((x: number) => x * 2);
      const toString = new Transformer<number, number>().map((x: number) => `value: ${x}`);

      const results = await new Pipeline<number>()
        .apply(double)
        .apply(toString)([1, 2, 3])
        .toArray();

      expect(results).toEqual(["value: 2", "value: 4", "value: 6"]);
    });
  });

  describe("Pipeline.onError() — the RUN handler (#78)", () => {
    it("Done-when 7: a chunk that can't be repaired is dropped, the run continues", async () => {
      const logged: string[] = [];
      const out = await new Pipeline<string>()

        .buffer(1)
        .onError((e) => logged.push(e.message))
        .transform((t) => t.map(parseStrict))(["1", "x", "3", "4"])
        .toArray();

      expect(out).toEqual([1, 3, 4]);
      expect(logged).toEqual(["Invalid: x"]);
    });

    it("Done-when 8: a handler that rethrows stops the run, throwing what it threw", () => {
      // Synchronously on a `"sync"` chain (#90) - every callback here is synchronous, so the throw
      // escapes `.toArray()` directly rather than as a rejected `Promise`.
      expect(() =>
        new Pipeline<string>()

          .buffer(1)
          .onError((e) => {
            throw e;
          })
          .transform((t) => t.map(parseStrict))(["1", "x", "3", "4"])
          .toArray(),
      ).toThrow("Invalid: x");
    });

    it("Done-when 10: a rethrowing ROW handler escalates to the RUN handler, which drops the chunk", async () => {
      // Same chain as Done-when 7, but the row handler is what rethrows this time - it never
      // recovers "x" itself, so the failure still reaches Pipeline.onError() as a chunk failure.
      const out = await new Pipeline<string>()

        .buffer(1)
        .onError(() => {
          /* swallow: drop the chunk, keep going */
        })
        .transform((t) =>
          t
            .onError((_item, error) => {
              throw error;
            })
            .map(parseStrict),
        )(["1", "x", "3", "4"])
        .toArray();

      expect(out).toEqual([1, 3, 4]);
    });

    it("is position-dependent, unlike Transformer.onError() — only a stage applied AFTER it is covered", async () => {
      // .onError() here is registered on a FRESH pipeline built by .transform() below, applied to
      // an ALREADY-DISPATCHED stage - too late for THIS run to see it, so the chunk failure still
      // propagates uncaught. Contrast with Done-when 7, where .onError() precedes .transform().
      // Thrown, not rejected (#90): every callback here is synchronous, so the whole chain is.
      expect(() =>
        new Pipeline<string>()

          .buffer(1)
          .transform((t) => t.map(parseStrict))
          .onError(() => {
            /* registered too late to catch the stage above */
          })(["1", "x", "3", "4"])
          .toArray(),
      ).toThrow("Invalid: x");
    });

    it("the upstream source itself rejecting, before any chunk is pulled, has no chunk to drop — it still propagates", async () => {
      // The run handler catches a CHUNK failure; a source that rejects before any chunk is ever
      // produced never reaches that per-chunk try/catch at all (the ticket's own Constraints: "the
      // unit dropped is the CHUNK").
      const boom = new Error("boom from the source");
      async function* failingSource(): AsyncGenerator<number> {
        throw boom;
      }
      const seen: Error[] = [];

      await expect(
        new Pipeline<number>()

          .onError((e) => seen.push(e))
          .transform((t) => t.map((x: number) => x))(failingSource())
          .toArray(),
      ).rejects.toThrow(boom);
      expect(seen).toEqual([]);
    });
  });

  // Async iteration reading the same persisted chunk stream a terminal op does (#39) is covered by
  // __tests__/transforms.e2e.test.ts's single tap observation case now that #72 deletes hooks in
  // favor of .tap() - it proves .tap() fires identically on .toArray(), on async iteration and on a
  // .local() stage.
});

describe("#113 - a stored `undefined` is a value, not an absent key", () => {
  it("agrees with .get() about whether the key exists", () => {
    // `getOrDefault` tested `value !== undefined`, so a deliberately stored `undefined` read back
    // as the default while `.get()` returned `undefined` - the two accessors disagreeing about
    // presence. It is the same distinction `DROP` exists for on the row-handler side.
    const ctx = new SimpleContextManager();
    ctx.set("lastError", undefined);

    expect(ctx.get("lastError")).toBeUndefined();
    expect(ctx.getOrDefault("lastError", "never ran")).toBeUndefined();
    expect(ctx.getOrDefault("neverSet", "never ran")).toBe("never ran");
  });
});
