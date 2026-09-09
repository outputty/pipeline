/**
 * execution.e2e.test.ts — chunking, async I/O work, streaming edge behaviors and the factory
 * helpers, each proven through an ENTIRE PIPELINE RUN rather than by calling a function in
 * isolation.
 *
 * Concurrency used to be a `Transformer`-level pluggable seam here, before #17 deleted it - a
 * caller now wraps the chain in `ConcurrentPipeline`/`HttpPipeline`/`ClusterPipeline`
 * (`__tests__/pipelines.e2e.test.ts`) instead of configuring the `Transformer` that drives it.
 */
import { describe, it, expect } from "vitest";
import { Pipeline, Transformer, createTransformer } from "../src";

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** Run `input` through a real pipeline built on `transformer`, returning the collected results.
 * `bufferSize`, when given, calls `.buffer()` before `.apply()` - chunking is a `Pipeline` decision
 * now (#39), not the `Transformer`'s own. */
async function run<I, O>(
  input: I[],
  transformer: Transformer<I, O, "sync" | "async">,
  bufferSize?: number,
): Promise<O[]> {
  const pipeline =
    bufferSize !== undefined ? new Pipeline<number>().buffer(bufferSize) : new Pipeline<number>();
  return pipeline.apply(transformer)(input)(input).toArray();
}

describe("execution e2e — chunking through a full run", () => {
  it("preserves input order", async () => {
    const out = await run(
      [1, 2, 3, 4, 5],
      new Transformer<number, number>().map((x) => x * 2),
    );
    expect(out).toEqual([2, 4, 6, 8, 10]);
  });

  it("chunking is correct across many chunks — every item survives regardless of chunk size", async () => {
    const input = Array;
    const out = await run(
      input,
      new Transformer<number, number>().map((x) => x * 2),
      7,
    );
    expect(out).toEqual(input.map((x) => x * 2));
  });
});

describe("execution e2e — async I/O work through a run", () => {
  // A stand-in for a real network call: an async fn with latency. No mock HTTP layer.
  const fetchUser = async (id: number) => {
    await delay(1);
    return { id, name: `user-${id}` };
  };

  it("sequential async work keeps order", async () => {
    const out = await run(
      [1, 2, 3],
      new Transformer<number, number>().map((id) => fetchUser(id)),
    );
    expect(out).toEqual([
      { id: 1, name: "user-1" },
      { id: 2, name: "user-2" },
      { id: 3, name: "user-3" },
    ]);
  });

  it("an error in async work is turned into a value by the mapping fn, not thrown", async () => {
    const out = await run(
      [1, 2],
      new Transformer<number, number>().map(async (id) => {
        await delay(1);
        return id === 2 ? { ok: false } : { ok: true };
      }),
    );
    expect(out).toEqual([{ ok: true }, { ok: false }]);
  });
});

describe("execution e2e — streaming edge behaviors", () => {
  it("passes null and undefined items through untouched", async () => {
    const out = await run(
      [1, null, 2, undefined, 3],
      new Transformer<number | null | undefined, number | null | undefined>().map((x) =>
        x === null ? "null" : x === undefined ? "undefined" : String(x),
      ),
    );
    expect(out).toEqual(["1", "null", "2", "undefined", "3"]);
  });

  it("yields the items processed before a later item throws, then rejects", async () => {
    const processed: number[] = [];
    const itemThreeFailed = new Error("item 3 failed");
    await expect(
      run(
        [1, 2, 3, 4],
        new Transformer<number, number>().map((x) => {
          if (x === 3) throw itemThreeFailed;
          processed.push(x);
          return x * 2;
        }),
        1,
      ),
    ).rejects.toThrow(itemThreeFailed);
    expect(processed).toEqual([1, 2]); // items 1 and 2 ran before the failure
  });
});

describe("execution e2e — factory helpers produce working pipelines", () => {
  it("createTransformer builds a working chain", async () => {
    const out = await run(
      [1, 2, 3],
      createTransformer<number>()
        .map((x) => x * 2)
        .filter((x) => x > 2),
    );
    expect(out).toEqual([4, 6]);
  });
});
