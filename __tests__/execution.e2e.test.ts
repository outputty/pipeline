/**
 * execution.e2e.test.ts — execution strategy, concurrency, custom executors, lifecycle hooks and the
 * factory helpers, each proven through an ENTIRE PIPELINE RUN rather than by calling a strategy /
 * function in isolation. Concurrency and chunking are observed the only way a caller can: output
 * order, item presence, a live concurrency counter, and correct results across many chunks.
 */
import { describe, it, expect, vi } from "vitest";
import {
  Pipeline,
  Transformer,
  createTransformer,
  sequential,
  concurrent,
  type ExecutionStrategy,
} from "../src";

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** Run `input` through a real pipeline built on `transformer`, returning the collected results. */
async function run<I, O>(input: I[], transformer: Transformer<I, O>): Promise<O[]> {
  return new Pipeline(input).apply(transformer).toArray();
}

describe("execution e2e — sequential vs concurrent through a full run", () => {
  it("the default (sequential) executor preserves input order", async () => {
    const out = await run(
      [1, 2, 3, 4, 5],
      new Transformer<number, number>().map((x) => x * 2),
    );
    expect(out).toEqual([2, 4, 6, 8, 10]);
  });

  it("ordered concurrency preserves order even when later items finish first", async () => {
    const out = await run(
      [1, 2, 3, 4, 5],
      new Transformer<number, number>({ chunkSize: 1 })
        .withExecutor(concurrent({ maxConcurrency: 4, ordered: true }))
        .map(async (x) => {
          await delay(10 - x); // reverse delay: item 5 finishes first
          return x * 2;
        }),
    );
    expect(out).toEqual([2, 4, 6, 8, 10]);
  });

  it("unordered concurrency yields every result (order may vary)", async () => {
    const out = await run(
      [1, 2, 3, 4],
      new Transformer<number, number>({ chunkSize: 1 })
        .withExecutor(concurrent({ maxConcurrency: 4, ordered: false }))
        .map((x) => x * 2),
    );
    expect(out.slice().sort((a, b) => a - b)).toEqual([2, 4, 6, 8]);
  });

  it("never exceeds maxConcurrency in flight", async () => {
    let active = 0;
    let peak = 0;
    await run(
      [1, 2, 3, 4, 5, 6],
      new Transformer<number, number>({ chunkSize: 1 })
        .withExecutor(concurrent({ maxConcurrency: 2, ordered: true }))
        .map(async (x) => {
          active++;
          peak = Math.max(peak, active);
          await delay(10);
          active--;
          return x;
        }),
    );
    expect(peak).toBeLessThanOrEqual(2);
  });

  it("maxConcurrency=1 behaves sequentially (order preserved despite reverse delays)", async () => {
    const finished: number[] = [];
    await run(
      [1, 2, 3],
      new Transformer<number, number>({ chunkSize: 1 })
        .withExecutor(concurrent({ maxConcurrency: 1, ordered: true }))
        .map(async (x) => {
          await delay(10 - x);
          finished.push(x);
          return x;
        }),
    );
    expect(finished).toEqual([1, 2, 3]);
  });

  it("chunking is correct across many chunks — every item survives regardless of chunk size", async () => {
    const input = Array.from({ length: 250 }, (_, i) => i);
    const out = await run(
      input,
      new Transformer<number, number>({ chunkSize: 7 }).map((x) => x * 2),
    );
    expect(out).toEqual(input.map((x) => x * 2));
  });
});

describe("execution e2e — switching executors + custom strategies mid-run", () => {
  it("withExecutor switches strategy while preserving the transform chain", async () => {
    const concurrentOut = await run(
      [1, 2, 3, 4, 5],
      createTransformer<number>()
        .map((x) => x * 2)
        .filter((x) => x > 4)
        .withExecutor(concurrent({ maxConcurrency: 2 })),
    );
    expect(concurrentOut.slice().sort((a, b) => a - b)).toEqual([6, 8, 10]);

    // Multiple switches in one chain still compute correctly: (x+1)*2-1
    const switched = await run(
      [1, 2, 3],
      createTransformer<number>()
        .map((x) => x + 1)
        .withExecutor(concurrent())
        .map((x) => x * 2)
        .withExecutor(sequential)
        .map((x) => x - 1),
    );
    expect(switched).toEqual([3, 5, 7]);
  });

  it("withExecutor(sequential) runs, and withExecutor(concurrent(...)) uppercases every item", async () => {
    const seqOut = await run(
      ["a", "b", "c"],
      new Transformer<string, string>().withExecutor(sequential).map((s) => s.toUpperCase()),
    );
    expect(seqOut).toEqual(["A", "B", "C"]);

    const concurrentOut = await run(
      ["a", "b", "c"],
      new Transformer<string, string>()
        .withExecutor(concurrent({ maxConcurrency: 10 }))
        .map((s) => s.toUpperCase()),
    );
    expect(concurrentOut).toEqual(["A", "B", "C"]);
  });

  it("a user-supplied custom strategy runs the whole pipeline and sees every chunk", async () => {
    const processedChunks: number[][] = [];
    const custom: ExecutionStrategy<number, number> = async function* (logic, chunks, ctx) {
      for await (const chunk of chunks) {
        processedChunks.push([...chunk]);
        yield logic(chunk, ctx);
      }
    };
    const out = await run(
      [1, 2, 3],
      createTransformer<number>()
        .map((x) => x * 2)
        .withExecutor(custom),
    );
    expect(out).toEqual([2, 4, 6]);
    expect(processedChunks).toEqual([[1, 2, 3]]);
  });

  it("withExecutor accepts an inline async function* with un-annotated params (the Interface example, verbatim)", async () => {
    const out = await run(
      [1, 2, 3, 4, 5],
      new Transformer<number, number>()
        .withExecutor(async function* (logic, chunks, ctx) {
          for await (const chunk of chunks) yield logic(chunk, ctx);
        })
        .map((x) => x * 2),
    );
    expect(out).toEqual([2, 4, 6, 8, 10]);
  });

  it("chunk.map((n) => n * 2) typechecks inside a custom async function* strategy body", async () => {
    const out = await run(
      [1, 2, 3],
      new Transformer<number, number>().withExecutor(async function* (_logic, chunks, _ctx) {
        for await (const chunk of chunks) yield chunk.map((n) => n * 2);
      }),
    );
    expect(out).toEqual([2, 4, 6]);
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
      new Transformer<number, { id: number; name: string }>().map((id) => fetchUser(id)),
    );
    expect(out).toEqual([
      { id: 1, name: "user-1" },
      { id: 2, name: "user-2" },
      { id: 3, name: "user-3" },
    ]);
  });

  it("concurrent async work returns every result", async () => {
    const out = await run(
      [1, 2, 3, 4],
      new Transformer<number, number>({ chunkSize: 1 })
        .withExecutor(concurrent({ maxConcurrency: 4 }))
        .map((id) => fetchUser(id)),
    );
    expect(out.map((u) => u.id).sort((a, b) => a - b)).toEqual([1, 2, 3, 4]);
  });

  it("an error in async work is turned into a value by the mapping fn, not thrown", async () => {
    const out = await run(
      [1, 2],
      new Transformer<number, { ok: boolean }>().map(async (id) => {
        await delay(1);
        return id === 2 ? { ok: false } : { ok: true };
      }),
    );
    expect(out).toEqual([{ ok: true }, { ok: false }]);
  });
});

describe("execution e2e — lifecycle hooks fire during a run", () => {
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
      new Transformer<number | null | undefined, string>().map((x) =>
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
        new Transformer<number, number>({ chunkSize: 1 }).map((x) => {
          if (x === 3) throw itemThreeFailed;
          processed.push(x);
          return x * 2;
        }),
      ),
    ).rejects.toThrow(itemThreeFailed);
    expect(processed).toEqual([1, 2]); // items 1 and 2 ran before the failure
  });
});

describe("execution e2e — factory helpers produce working pipelines", () => {
  it("createTransformer builds a sequential run", async () => {
    const out = await run(
      [1, 2, 3],
      createTransformer<number>()
        .map((x) => x * 2)
        .filter((x) => x > 2),
    );
    expect(out).toEqual([4, 6]);
  });

  it("createTransformer + withExecutor(concurrent(...)) builds an ordered concurrent run honoring its options", async () => {
    const out = await run(
      [1, 2, 3],
      createTransformer<number>(1)
        .withExecutor(concurrent({ maxConcurrency: 8, ordered: true }))
        .map(async (x) => {
          await delay(10 - x);
          return x * 2;
        }),
    );
    expect(out).toEqual([2, 4, 6]);
  });

  it("concurrent() rejects a maxConcurrency below 1, accepts the default and a positive value", () => {
    expect(() => concurrent({ maxConcurrency: 0 })).toThrow("maxConcurrency must be at least 1");
    expect(() => concurrent({ maxConcurrency: -3 })).toThrow("maxConcurrency must be at least 1");
    expect(() => concurrent()).not.toThrow();
    expect(() => concurrent({ maxConcurrency: 8 })).not.toThrow();
  });
});
