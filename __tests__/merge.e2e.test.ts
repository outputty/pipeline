import { describe, it, expect } from "vitest";
import { Pipeline } from "@src/pipeline";
import { LoggingContext } from "./fixtures/context-managers";

describe("Pipeline.merge", () => {
  describe("basic merging", () => {
    it("should merge two pipelines in sequence", async () => {
      const pipeline1 = new Pipeline([1, 2, 3]);
      const pipeline2 = new Pipeline([4, 5, 6]);

      const merged = Pipeline.merge([pipeline1, pipeline2]);
      const results = await merged.toArray();

      expect(results).toEqual([1, 2, 3, 4, 5, 6]);
    });

    it("should merge three or more pipelines", async () => {
      const p1 = new Pipeline(["a", "b"]);
      const p2 = new Pipeline(["c", "d"]);
      const p3 = new Pipeline(["e", "f"]);

      const merged = Pipeline.merge([p1, p2, p3]);
      const results = await merged.toArray();

      expect(results).toEqual(["a", "b", "c", "d", "e", "f"]);
    });

    it("infers a union of the merged pipelines' literal item types (#31, Done-when 7)", async () => {
      // Explicit type args on the two `Pipeline` constructions - a bare `new Pipeline(["a", "b"])`
      // widens to `Pipeline<string>` on its own (Pipeline's own constructor inference, unrelated
      // to `merge()`); `Pipeline<T>` is invariant (architecture.md), so the mismatched-type
      // assignment below would fail to COMPILE, not just to assert, if merge() collapsed the union.
      // The ARRAY-form call (#31) still distributes ElementOf<Ps[number]> over the union exactly
      // as the old rest-param form did.
      const typed: Pipeline<"a" | "b" | "c" | "d"> = Pipeline.merge([
        new Pipeline<"a" | "b">(["a", "b"]),
        new Pipeline<"c" | "d">(["c", "d"]),
      ]);
      expect(await typed.toArray()).toEqual(["a", "b", "c", "d"]);
    });

    it("should handle single pipeline", async () => {
      const pipeline = new Pipeline([1, 2, 3]);

      const merged = Pipeline.merge([pipeline]);
      const results = await merged.toArray();

      expect(results).toEqual([1, 2, 3]);
    });

    it("should return empty pipeline when no pipelines provided (#31, Done-when 4)", async () => {
      const merged = Pipeline.merge([]);
      const results = await merged.toArray();

      expect(results).toEqual([]);
    });

    it("still honors options.context with zero pipelines (review regression)", () => {
      const mine = new LoggingContext();
      const merged = Pipeline.merge([], { context: mine });

      expect(merged.contextManager).toBe(mine);
    });
  });

  describe("empty pipeline handling", () => {
    it("should handle empty pipelines gracefully", async () => {
      const pipeline1 = new Pipeline([1, 2]);
      const emptyPipeline = new Pipeline<number>([]);
      const pipeline2 = new Pipeline([3, 4]);

      const merged = Pipeline.merge([pipeline1, emptyPipeline, pipeline2]);
      const results = await merged.toArray();

      expect(results).toEqual([1, 2, 3, 4]);
    });

    it("should handle all empty pipelines", async () => {
      const empty1 = new Pipeline<string>([]);
      const empty2 = new Pipeline<string>([]);

      const merged = Pipeline.merge([empty1, empty2]);
      const results = await merged.toArray();

      expect(results).toEqual([]);
    });
  });

  describe("context merging", () => {
    it("should merge contexts from all pipelines", async () => {
      const pipeline1 = new Pipeline([1]).context({ key1: "value1" });
      const pipeline2 = new Pipeline([2]).context({ key2: "value2" });

      const merged = Pipeline.merge([pipeline1, pipeline2]);
      await merged.toArray();
      const ctx = merged.contextManager.toDict();

      expect(ctx).toEqual({
        key1: "value1",
        key2: "value2",
      });
    });

    it("should give precedence to later pipelines for overlapping keys, no options (#31, Done-when 4)", async () => {
      const pipeline1 = new Pipeline([1]).context({ shared: "first", unique1: "a" });
      const pipeline2 = new Pipeline([2]).context({ shared: "second", unique2: "b" });

      const merged = Pipeline.merge([pipeline1, pipeline2]);

      expect(merged.contextManager.constructor.name).toBe("SimpleContextManager");
      expect(merged.contextManager.toDict()).toEqual({
        shared: "second",
        unique1: "a",
        unique2: "b",
      });
    });

    it("returns a pipeline whose .contextManager IS the given context, same precedence (#31, Done-when 3)", async () => {
      const mine = new LoggingContext();
      const pipeline1 = new Pipeline([1]).context({ shared: "first", only1: "a" });
      const pipeline2 = new Pipeline([2]).context({ shared: "second", only2: "b" });

      const merged = Pipeline.merge([pipeline1, pipeline2], { context: mine });

      expect(merged.contextManager).toBe(mine);
      expect(merged.contextManager.constructor.name).toBe("LoggingContext");
      expect(merged.contextManager.toDict()).toEqual({
        shared: "second",
        only1: "a",
        only2: "b",
      });
    });

    it("honors options.contextFactory too, not just options.context (review regression)", () => {
      const pipeline1 = new Pipeline([1]).context({ key: "value" });

      const merged = Pipeline.merge([pipeline1], { contextFactory: () => new LoggingContext() });

      expect(merged.contextManager.constructor.name).toBe("LoggingContext");
      expect(merged.contextManager.toDict()).toEqual({ key: "value" });
    });

    it("honors options.contextFactory with zero pipelines too (review regression)", () => {
      const merged = Pipeline.merge([], { contextFactory: () => new LoggingContext() });

      expect(merged.contextManager.constructor.name).toBe("LoggingContext");
    });
  });

  describe("async data sources", () => {
    it("should handle async iterables", async () => {
      async function* asyncGen1() {
        yield "a";
        yield "b";
      }
      async function* asyncGen2() {
        yield "c";
        yield "d";
      }

      const pipeline1 = new Pipeline(asyncGen1());
      const pipeline2 = new Pipeline(asyncGen2());

      const merged = Pipeline.merge([pipeline1, pipeline2]);
      const results = await merged.toArray();

      expect(results).toEqual(["a", "b", "c", "d"]);
    });
  });

  describe("chaining after merge", () => {
    it("should allow transformations after merge", async () => {
      const pipeline1 = new Pipeline([1, 2]);
      const pipeline2 = new Pipeline([3, 4]);

      const merged = Pipeline.merge([pipeline1, pipeline2]);
      const results = await merged.transform((t) => t.map((x) => x * 2)).toArray();

      expect(results).toEqual([2, 4, 6, 8]);
    });

    it("should allow filtering after merge", async () => {
      const pipeline1 = new Pipeline([1, 2, 3]);
      const pipeline2 = new Pipeline([4, 5, 6]);

      const merged = Pipeline.merge([pipeline1, pipeline2]);
      const results = await merged.transform((t) => t.filter((x) => x % 2 === 0)).toArray();

      expect(results).toEqual([2, 4, 6]);
    });
  });

  describe("fan-in pattern (diamond)", () => {
    it("should enable diamond pattern: split then merge", async () => {
      // Source data
      const _source = new Pipeline([1, 2, 3, 4, 5]);

      // Simulate fan-out by creating two pipelines from arrays
      // (In real usage, these would come from branch() results)
      const evens = new Pipeline([2, 4]);
      const odds = new Pipeline([1, 3, 5]);

      // Fan-in: merge the branches back
      const merged = Pipeline.merge([evens, odds]);
      const results = await merged.toArray();

      // All items present (order may vary based on branch order)
      expect(results).toEqual([2, 4, 1, 3, 5]);
      expect(results.sort()).toEqual([1, 2, 3, 4, 5]);
    });
  });
});
