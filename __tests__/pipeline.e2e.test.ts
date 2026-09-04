import { describe, it, expect } from "vitest";
import { Pipeline } from "@src/pipeline";
import { Transformer } from "@src/transformer";
import { concurrent } from "@src/strategies/concurrent";
import { SimpleContextManager } from "@src/context/simple";

describe("Pipeline", () => {
  describe("constructor", () => {
    it("creates pipeline from sync iterable", async () => {
      const data = [1, 2, 3];
      const pipeline = new Pipeline(data);
      const results = await pipeline.toArray();

      expect(results).toEqual([1, 2, 3]);
    });

    it("creates pipeline from async iterable", async () => {
      async function* asyncData() {
        yield 1;
        yield 2;
        yield 3;
      }

      const pipeline = new Pipeline(asyncData());
      const results = await pipeline.toArray();

      expect(results).toEqual([1, 2, 3]);
    });

    it("accepts custom context", async () => {
      const context = new SimpleContextManager({ key: "value" });
      const pipeline = new Pipeline([1, 2, 3], { context });
      await pipeline.toArray();
      const ctx = pipeline.contextManager.toDict();

      expect(ctx).toEqual({ key: "value" });
    });
  });

  describe("context", () => {
    it("sets context values via fluent API", async () => {
      const pipeline = new Pipeline([1, 2, 3]).context({ multiplier: 3 });
      await pipeline.toArray();
      const ctx = pipeline.contextManager.toDict();

      expect(ctx).toEqual({ multiplier: 3 });
    });

    it("returns pipeline for chaining", async () => {
      const pipeline = new Pipeline([1, 2, 3])
        .context({ key1: "value1" })
        .context({ key2: "value2" });

      await pipeline.toArray();
      const ctx = pipeline.contextManager.toDict();

      expect(ctx).toEqual({ key1: "value1", key2: "value2" });
    });

    it("can be used with transformers", async () => {
      const results = await new Pipeline([1, 2, 3])
        .context({ multiplier: 10 })
        .transform((t) =>
          t.map((x, ctx) => {
            const mult = ctx.getOrDefault("multiplier", 1) as number;
            return x * mult;
          }),
        )
        .toArray();

      expect(results).toEqual([10, 20, 30]);
    });

    it("allows access via contextManager getter", () => {
      const pipeline = new Pipeline([1, 2, 3]).context({ key: "value" });

      expect(pipeline.contextManager.get("key")).toBe("value");
    });
  });

  describe("apply", () => {
    it("applies transformer to data", async () => {
      const pipeline = new Pipeline([1, 2, 3]);
      const transformer = new Transformer<number, number>().map((x: number) => x * 2);

      const results = await pipeline.apply(transformer).toArray();

      expect(results).toEqual([2, 4, 6]);
    });

    it("chains multiple transformers", async () => {
      const pipeline = new Pipeline([1, 2, 3]);
      const double = new Transformer<number, number>().map((x: number) => x * 2);
      const addOne = new Transformer<number, number>().map((x: number) => x + 1);

      const results = await pipeline.apply(double).apply(addOne).toArray();

      expect(results).toEqual([3, 5, 7]);
    });

    it("preserves context across transformers", async () => {
      const context = new SimpleContextManager({ count: 0 });
      const pipeline = new Pipeline([1, 2, 3], { context });

      const transformer = new Transformer<number, number>().tap((_, ctx) => {
        const count = ctx.getOrDefault("count", 0) as number;
        ctx.set("count", count + 1);
      });

      await pipeline.apply(transformer).toArray();
      const ctx = pipeline.contextManager.toDict();

      expect(ctx.count).toBe(3);
    });
  });

  describe("transform", () => {
    it("applies transformer builder function", async () => {
      const pipeline = new Pipeline([1, 2, 3]);

      const results = await pipeline
        .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 2))
        .toArray();

      expect(results).toEqual([4, 6]);
    });

    it("chains with other operations", async () => {
      const pipeline = new Pipeline(["hello", "world"]);

      const results = await pipeline
        .transform((t) => t.map((s: string) => s.toUpperCase()))
        .transform((t) => t.map((s: string) => s + "!"))
        .toArray();

      expect(results).toEqual(["HELLO!", "WORLD!"]);
    });
  });

  describe("buffer", () => {
    it("buffers data while maintaining order", async () => {
      const pipeline = new Pipeline([1, 2, 3, 4, 5]);
      const results = await pipeline.buffer(2, 2).toArray();

      expect(results).toEqual([1, 2, 3, 4, 5]);
    });

    it("works with empty input", async () => {
      const pipeline = new Pipeline<number>([]);
      const results = await pipeline.buffer(2).toArray();

      expect(results).toEqual([]);
    });
  });

  describe("toArray", () => {
    it("collects all items to array", async () => {
      const pipeline = new Pipeline([1, 2, 3]);
      const results = await pipeline.toArray();

      expect(results).toEqual([1, 2, 3]);
    });

    it("returns empty array for empty input", async () => {
      const pipeline = new Pipeline<number>([]);
      const results = await pipeline.toArray();

      expect(results).toEqual([]);
    });

    it("does not carry a context snapshot; .contextManager still resolves it afterward", async () => {
      const context = new SimpleContextManager({ key: "value" });
      const pipeline = new Pipeline([1], { context });
      await pipeline.toArray();
      const ctx = pipeline.contextManager.toDict();

      expect(ctx).toEqual({ key: "value" });
    });
  });

  describe("first", () => {
    it("returns first N elements", async () => {
      const pipeline = new Pipeline([1, 2, 3, 4, 5]);
      const results = await pipeline.first(3);

      expect(results).toEqual([1, 2, 3]);
    });

    it("returns single element by default", async () => {
      const pipeline = new Pipeline([1, 2, 3]);
      const results = await pipeline.first();

      expect(results).toEqual([1]);
    });

    it("returns all elements if N > length", async () => {
      const pipeline = new Pipeline([1, 2]);
      const results = await pipeline.first(5);

      expect(results).toEqual([1, 2]);
    });

    it("throws if N < 1", async () => {
      const pipeline = new Pipeline([1, 2, 3]);

      await expect(pipeline.first(0)).rejects.toThrow(new Error("n must be at least 1"));
    });

    it("does not carry a context snapshot; .contextManager still resolves it afterward", async () => {
      const context = new SimpleContextManager({ key: "value" });
      const pipeline = new Pipeline([1, 2, 3], { context });
      await pipeline.first(2);
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

      const pipeline = new Pipeline(data());
      await pipeline.consume();

      expect(processed).toBe(5);
    });

    it("resolves undefined; .contextManager still resolves the context afterward", async () => {
      const context = new SimpleContextManager({ key: "value" });
      const pipeline = new Pipeline([1, 2, 3], { context });
      const result = await pipeline.consume();
      const ctx = pipeline.contextManager.toDict();

      expect(result).toBeUndefined();
      expect(ctx).toEqual({ key: "value" });
    });
  });

  describe("forEach", () => {
    it("applies function to each item", async () => {
      const items: number[] = [];
      const pipeline = new Pipeline([1, 2, 3]);

      await pipeline.forEach((item) => {
        items.push(item * 2);
      });

      expect(items).toEqual([2, 4, 6]);
    });

    it("supports async functions", async () => {
      const items: number[] = [];
      const pipeline = new Pipeline([1, 2, 3]);

      await pipeline.forEach(async (item) => {
        await Promise.resolve();
        items.push(item);
      });

      expect(items).toEqual([1, 2, 3]);
    });

    it("resolves undefined; .contextManager still resolves the context afterward", async () => {
      const context = new SimpleContextManager({ key: "value" });
      const pipeline = new Pipeline([1, 2, 3], { context });
      const result = await pipeline.forEach(() => {});
      const ctx = pipeline.contextManager.toDict();

      expect(result).toBeUndefined();
      expect(ctx).toEqual({ key: "value" });
    });
  });

  describe("branch", () => {
    it("routes items to different branches", async () => {
      const pipeline = new Pipeline([1, 2, 3, 4, 5]);

      const results = await pipeline.branch({
        even: {
          predicate: (x: number) => x % 2 === 0,
          transformer: new Transformer<number, number>().map((x: number) => x * 10),
        },
        odd: {
          predicate: (x: number) => x % 2 !== 0,
          transformer: new Transformer<number, number>().map((x: number) => x * 100),
        },
      });

      expect(results.even).toEqual([20, 40]);
      expect(results.odd).toEqual([100, 300, 500]);
    });

    it("uses first matching branch only", async () => {
      const pipeline = new Pipeline([1, 2, 3]);

      const results = await pipeline.branch({
        positive: {
          predicate: (x) => x > 0,
          transformer: new Transformer<number, string>().map(() => "positive"),
        },
        all: {
          predicate: () => true,
          transformer: new Transformer<number, string>().map(() => "all"),
        },
      });

      // All items match 'positive' first
      expect(results.positive).toEqual(["positive", "positive", "positive"]);
      expect(results.all).toEqual([]);
    });

    it("handles empty input", async () => {
      const pipeline = new Pipeline<number>([]);

      const results = await pipeline.branch({
        even: {
          predicate: (x) => x % 2 === 0,
          transformer: new Transformer<number, number>(),
        },
      });

      expect(results.even).toEqual([]);
    });

    it("handles no matching branches", async () => {
      const pipeline = new Pipeline([1, 2, 3]);

      const results = await pipeline.branch({
        negative: {
          predicate: (x) => x < 0,
          transformer: new Transformer<number, number>(),
        },
      });

      expect(results.negative).toEqual([]);
    });

    it("supports async predicates", async () => {
      const pipeline = new Pipeline([1, 2, 3]);

      const results = await pipeline.branch({
        async: {
          predicate: async (x) => x > 1,
          transformer: new Transformer<number, number>().map((x: number) => x * 2),
        },
      });

      expect(results.async).toEqual([4, 6]);
    });

    it("does not carry a context snapshot; .contextManager still resolves it afterward", async () => {
      const context = new SimpleContextManager({ key: "value" });
      const pipeline = new Pipeline([1, 2], { context });

      await pipeline.branch({
        all: {
          predicate: () => true,
          transformer: new Transformer<number, number>(),
        },
      });
      const ctx = pipeline.contextManager.toDict();

      expect(ctx).toEqual({ key: "value" });
    });

    it("supports broadcast mode (firstMatch: false)", async () => {
      const pipeline = new Pipeline([1, 2, 3, 4, 5]);

      const results = await pipeline.branch(
        {
          even: {
            predicate: (x: number) => x % 2 === 0,
            transformer: new Transformer<number, number>().map((x: number) => x * 10),
          },
          smallerThan4: {
            predicate: (x: number) => x < 4,
            transformer: new Transformer<number, number>().map((x: number) => x * 100),
          },
        },
        { firstMatch: false }, // Broadcast mode
      );

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
      const pipeline = new Pipeline([1, 2, 3, 4, 5]);

      const results = await pipeline.branch(
        {
          even: {
            predicate: (x: number) => x % 2 === 0,
            transformer: new Transformer<number, number>().map((x: number) => x * 10),
          },
          smallerThan4: {
            predicate: (x: number) => x < 4,
            transformer: new Transformer<number, number>().map((x: number) => x * 100),
          },
        },
        { firstMatch: true }, // Router mode (default)
      );

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
      const context = new SimpleContextManager({ processedCount: 0 });

      const results = await new Pipeline([1, 2, 3, 4, 5], { context })
        .transform((t) =>
          t
            .map((x: number) => x * 2)
            .filter((x: number) => x > 4)
            .tap((_, c) => {
              const count = c.getOrDefault("processedCount", 0) as number;
              c.set("processedCount", count + 1);
            }),
        )
        .toArray();

      expect(results).toEqual([6, 8, 10]);
      expect(context.toDict().processedCount).toBe(3);
    });

    it("pipeline with chained apply calls", async () => {
      const double = new Transformer<number, number>().map((x: number) => x * 2);
      const toString = new Transformer<number, number>().map((x: number) => `value: ${x}`);

      const results = await new Pipeline([1, 2, 3]).apply(double).apply(toString).toArray();

      expect(results).toEqual(["value: 2", "value: 4", "value: 6"]);
    });
  });

  describe("onError wiring (Transformer.execute)", () => {
    it("a registered onError handler fires on a chunk failure, and the error still propagates", async () => {
      const seen: Error[] = [];
      const boom = new Error("boom");
      const transformer = new Transformer<number, number>()
        .map((x: number) => {
          if (x === 2) throw boom;
          return x;
        })
        .onError((_chunk, error) => {
          seen.push(error);
        });

      await expect(new Pipeline([1, 2, 3]).apply(transformer).toArray()).rejects.toThrow(boom);
      expect(seen).toHaveLength(1);
      expect(seen[0].message).toBe("boom");
    });

    it("onError() registered BEFORE map() survives pipe()'s copy — the handler still fires", async () => {
      // Regression for #442: `pipe()` (driving map/filter/…) used to always construct a fresh,
      // empty ErrorHandler, so `t.onError(fn).map(g)` silently dropped `fn` — only the OTHER
      // order (map-then-onError, the test above) used to work.
      const seen: Error[] = [];
      const boom = new Error("boom");
      const transformer = new Transformer<number, number>()
        .onError((_chunk, error) => {
          seen.push(error);
        })
        .map((x: number) => {
          if (x === 2) throw boom;
          return x;
        });

      await expect(new Pipeline([1, 2, 3]).apply(transformer).toArray()).rejects.toThrow(boom);
      expect(seen).toHaveLength(1);
      expect(seen[0].message).toBe("boom");
    });
  });

  describe("source-position knobs fail loud on async iteration", () => {
    it("a pipeline carrying withExecutor raises naming the knob when iterated directly (m.from position)", async () => {
      const transformer = new Transformer<number, number>()
        .map((x: number) => x * 2)
        .withExecutor(concurrent({ maxConcurrency: 8 }));
      const pipeline = new Pipeline([1, 2, 3]).apply(transformer);

      await expect(async () => {
        for await (const _chunk of pipeline) {
          // never reached
        }
      }).rejects.toThrow(/withExecutor.*not applied in source position/);
    });

    it("a plain pipeline (no inert knobs) iterates fine directly", async () => {
      const transformer = new Transformer<number, number>().map((x: number) => x * 2);
      const pipeline = new Pipeline([1, 2, 3]).apply(transformer);

      const chunks: number[][] = [];
      for await (const chunk of pipeline) chunks.push(chunk);
      expect(chunks.flat()).toEqual([2, 4, 6]);
    });

    it("withExecutor is NOT inert through a terminal op (.toArray() calls Transformer.execute)", async () => {
      const transformer = new Transformer<number, number>()
        .map((x: number) => x * 2)
        .withExecutor(concurrent({ maxConcurrency: 8 }));
      const results = await new Pipeline([1, 2, 3]).apply(transformer).toArray();
      expect(results.slice().sort((a, b) => a - b)).toEqual([2, 4, 6]);
    });

    it("a pipeline carrying setChunker raises naming the knob when iterated directly (m.from position)", async () => {
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
      const transformer = new Transformer<number, number>().setChunker(pairs);
      const pipeline = new Pipeline([1, 2, 3]).apply(transformer);

      await expect(async () => {
        for await (const _chunk of pipeline) {
          // never reached
        }
      }).rejects.toThrow(/setChunker.*not applied in source position/);
    });
  });
});
