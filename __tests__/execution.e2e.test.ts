/**
 * execution.e2e.test.ts — lifecycle hooks, streaming edge behaviors and the factory helpers, each
 * proven through an ENTIRE PIPELINE RUN rather than by calling a function in isolation.
 *
 * Concurrency used to be a `Transformer`-level pluggable seam here, before #17 deleted it - a
 * caller now wraps the chain in `ConcurrentPipeline`/`HttpPipeline`/`ClusterPipeline`
 * (`__tests__/pipelines.e2e.test.ts`) instead of configuring the `Transformer` that drives it.
 */
import { describe, it, expect, vi } from "vitest";
import { Pipeline, Transformer, createTransformer } from "../src";

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** Run `input` through a real pipeline built on `transformer`, returning the collected results.
 * `bufferSize`, when given, calls `.buffer()` before `.apply()` - chunking is a `Pipeline` decision
 * now (#39), not the `Transformer`'s own. */
async function run<I, O>(
  input: I[],
  transformer: Transformer<I, O>,
  bufferSize?: number,
): Promise<O[]> {
  const pipeline =
    bufferSize !== undefined ? new Pipeline(input).buffer(bufferSize) : new Pipeline(input);
  return pipeline.apply(transformer).toArray();
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
    const input = Array.from({ length: 250 }, (_, i) => i);
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

describe("execution e2e — lifecycle hooks fire during a run", () => {
  it("a plain-value hook (onStart: () => order.push(...)) compiles against the bare void return type", async () => {
    const order: string[] = [];
    const out = await run(
      [1],
      new Transformer<number, number>()
        .map((x) => x * 2)
        .withHooks({
          onStart: () => order.push("start"),
          onComplete: () => order.push("complete"),
        }),
    );
    expect(out).toEqual([2]);
    expect(order).toEqual(["start", "complete"]);
  });

  it("an async hook is still assignable to the bare void return type, and still awaited", async () => {
    const order: string[] = [];
    const out = await run(
      [1, 2],
      new Transformer<number, number>()
        .map((x) => x * 2)
        .withHooks({
          onStart: async () => {
            await delay(1);
            order.push("start");
          },
          onComplete: async () => {
            await delay(1);
            order.push("complete");
          },
        }),
    );
    expect(out).toEqual([2, 4]);
    // Both async hooks were AWAITED, not fired-and-forgotten: "start" is in before onComplete ran,
    // and onComplete's own push landed before this assertion, proving execute() awaited it too.
    expect(order).toEqual(["start", "complete"]);
  });

  it("onStart fires once before work, onComplete once after with item count", async () => {
    const onStart = vi.fn();
    const onComplete = vi.fn();
    const out = await run(
      [1, 2, 3],
      new Transformer<number, number>().map((x) => x * 2).withHooks({ onStart, onComplete }),
    );
    expect(out).toEqual([2, 4, 6]);
    expect(onStart).toHaveBeenCalledTimes(1);
    expect(onComplete).toHaveBeenCalledTimes(1);
    expect(onComplete).toHaveBeenCalledWith(3, expect.any(Number));
  });

  it("per-item hooks fire for each item with input/output (total is -1 while streaming)", async () => {
    const onItemStart = vi.fn();
    const onItemComplete = vi.fn();
    await run(
      [10, 20],
      new Transformer<number, number>()
        .map((x) => x * 2)
        .withHooks({ onItemStart, onItemComplete }),
    );
    expect(onItemStart).toHaveBeenNthCalledWith(1, 10, 0, -1);
    expect(onItemStart).toHaveBeenNthCalledWith(2, 20, 1, -1);
    expect(onItemComplete).toHaveBeenNthCalledWith(1, 10, 20, expect.any(Number));
    expect(onItemComplete).toHaveBeenNthCalledWith(2, 20, 40, expect.any(Number));
  });

  it("onItemError / onError fire on a failing item and the run rejects", async () => {
    const onItemError = vi.fn();
    const onError = vi.fn();
    const err = new Error("boom");
    await expect(
      run(
        [1, 2, 3],
        new Transformer<number, number>()
          .map((x) => {
            if (x === 2) throw err;
            return x;
          })
          .withHooks({ onItemError, onError }),
      ),
    ).rejects.toThrow(err);
    expect(onItemError).toHaveBeenCalledWith(2, err);
    expect(onError).toHaveBeenCalledWith(err);
  });

  it("hooks fire in order: start → (itemStart → itemComplete)* → complete", async () => {
    const order: string[] = [];
    await run(
      [1, 2],
      new Transformer<number, number>()
        .map((x) => x * 2)
        .withHooks({
          onStart: () => order.push("start"),
          onItemStart: (item) => order.push(`itemStart:${item}`),
          onItemComplete: (_in, out) => order.push(`itemComplete:${out}`),
          onComplete: () => order.push("complete"),
        }),
    );
    expect(order).toEqual([
      "start",
      "itemStart:1",
      "itemComplete:2",
      "itemStart:2",
      "itemComplete:4",
      "complete",
    ]);
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
