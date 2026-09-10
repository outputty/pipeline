/**
 * eventemitter.e2e.test.ts — #124's Done-when cases, each proven through a real
 * `EventEmitterPipeline` run over a real `node:events` `EventEmitter`. No mocks: every "worker" is
 * a real function registered on a real emitter, every race is a real `setTimeout`.
 *
 * Done-when 6 (an async Worker throwing after its own `await` leaks no unhandled rejection) runs as
 * a subprocess fixture (`__tests__/fixtures/eventemitter-async-throw.ts`) for the same reason
 * `concurrent-unhandled.ts` (#17) does: Vitest installs its own `unhandledRejection` handler and
 * would report a leak as a test-runner error, never a value this file can observe directly.
 *
 * Done-when 12 (`pnpm check` passes, no file outside the ticket's own Where) is a repo-wide gate,
 * not a per-case assertion - checked once at the end of the build, not here.
 *
 * Every Done-when case ran as `it.fails` against L1's stub, then flipped live once L2's real
 * dispatch landed (`pipelines.e2e.test.ts`'s own convention, #17). The two "review round" describes
 * below are not Done-when cases - they are regression tests for review-found fixes, added after.
 */
import { describe, it, expect } from "vitest";
import { EventEmitterPipeline, type WorkEvent } from "../src";
import { FIXTURE_TIMEOUT, runFixture, expectFixtureOk } from "./helpers/fixtures";

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** The real contract a Worker registered directly on `stage:<n>` receives - `code-review xhigh`'s
 * own F14: a hand-duplicated local interface reported errors the real shape never has. */
type WorkerEvent = WorkEvent<number, number>;

describe("#124 the Interface program (Done-when 1)", () => {
  it("prints [2,4,6,8,10,12] with dispatched/done per chunk, then stage 0 ended, then run ended", async () => {
    const pipeline = new EventEmitterPipeline<number>({ maxConcurrency: 2 })
      .buffer(1)
      .transform((t) => t.map((x: number) => x * 2));

    const labels: string[] = [];
    pipeline.emitter.on("stage:0:dispatched", () => labels.push("dispatched"));
    pipeline.emitter.on("stage:0:done", () => labels.push("done"));
    pipeline.emitter.on("stage:0:end", () => labels.push("stage 0 ended"));
    pipeline.emitter.on("pipeline:end", () => labels.push("run ended"));

    const out = await pipeline([1, 2, 3, 4, 5, 6]).toArray();

    expect(out).toEqual([2, 4, 6, 8, 10, 12]);
    expect(labels.filter((l) => l === "dispatched")).toHaveLength(6);
    expect(labels.filter((l) => l === "done")).toHaveLength(6);
    expect(labels.at(-2)).toBe("stage 0 ended");
    expect(labels.at(-1)).toBe("run ended");
  });
});

describe("#124 the composed function alone is a complete Worker (Done-when 2)", () => {
  it("runs with zero external .on() calls", async () => {
    const out = await new EventEmitterPipeline<number>()
      .buffer(1)
      .transform((t) => t.map((x: number) => x * 2))([1, 2, 3])
      .toArray();
    expect(out).toEqual([2, 4, 6]);
  });
});

describe("#124 the composed function registers once per stage index, never once per run (Done-when 3, 7)", () => {
  it("emitter.listenerCount('stage:0') is unchanged across two separate calls", async () => {
    const pipeline = new EventEmitterPipeline<number>()
      .buffer(1)
      .transform((t) => t.map((x: number) => x * 2));

    const first = await pipeline([1, 2]).toArray();
    expect(first).toEqual([2, 4]);
    expect(pipeline.emitter.listenerCount("stage:0")).toBe(1);

    const second = await pipeline([3, 4]).toArray();
    expect(second).toEqual([6, 8]);
    expect(pipeline.emitter.listenerCount("stage:0")).toBe(1);
  });

  it("a stage with no worker registered rejects immediately, naming the stage index", async () => {
    const pipeline = new EventEmitterPipeline<number>()
      .buffer(1)
      .transform((t) => t.map((x: number) => x * 2));

    await pipeline([1]).toArray();
    const [registered] = pipeline.emitter.listeners("stage:0");
    pipeline.emitter.off("stage:0", registered as (...args: unknown[]) => void);
    expect(pipeline.emitter.listenerCount("stage:0")).toBe(0);

    // The dedup Set still thinks stage 0 is registered (Done-when 3's own mechanism), so the
    // composed function is NOT re-added here - the stage genuinely has no worker left.
    await expect(pipeline([2]).toArray()).rejects.toThrow(/stage 0/);
  });
});

describe("#124 an external Worker races the composed function (Done-when 4)", () => {
  it("a Worker rejecting at 5ms beats one resolving at 30ms, even though the resolving one registered first", async () => {
    const pipeline = new EventEmitterPipeline<number>().buffer(1).transform((t) =>
      t.map(async (x: number) => {
        await delay(30);
        return x * 2;
      }),
    );

    pipeline.emitter.on("stage:0", async ({ chunk, reject }: WorkerEvent) => {
      await delay(5);
      reject(new Error(`external-worker-rejected-${chunk.join(",")}`));
    });

    await expect(pipeline([1]).toArray()).rejects.toThrow("external-worker-rejected-1");
  });
});

describe("#124 Pipeline.onError() reaches a rejecting Worker for free (Done-when 5)", () => {
  it("drops the chunk that fails and the run continues, with no explicit call in the new code", async () => {
    const out = await new EventEmitterPipeline<number>()
      .buffer(1)
      .onError(() => undefined)
      .transform((t) =>
        t.map((x: number) => {
          if (x === 3) throw new Error("boom on 3");
          return x * 2;
        }),
      )([1, 2, 3, 4])
      .toArray();
    expect(out).toEqual([2, 4, 8]);
  });
});

describe("#124 an async Worker that throws after its own await never hangs or leaks (Done-when 6)", () => {
  it(
    "rejects the chunk exactly as an explicit reject() would, and leaves no unhandled rejection",
    async () => {
      const fixture = await runFixture("__tests__/fixtures/eventemitter-async-throw.ts");
      expectFixtureOk(fixture);
      const result = JSON.parse(fixture.stdout.trim()) as {
        rejection: string | null;
        unhandled: string[];
      };
      expect(result.rejection).toBe("worker-threw-after-await-1,2,3");
      expect(result.unhandled).toEqual([]);
    },
    FIXTURE_TIMEOUT,
  );
});

describe("#124 maxConcurrency/ordered behave exactly as ConcurrentPipeline's own (Done-when 8)", () => {
  it("ordered: true - chunk 0 made 12x slower still comes out first", async () => {
    const out = await new EventEmitterPipeline<number>({ maxConcurrency: 4, ordered: true })
      .buffer(1)
      .transform((t) =>
        t.map(async (x: number) => {
          await delay(x === 1 ? 120 : 10);
          return x;
        }),
      )([1, 2, 3, 4])
      .toArray();
    expect(out).toEqual([1, 2, 3, 4]);
  });

  it("ordered: false - dispatches before the whole source has been pulled", async () => {
    const pulled: number[] = [];
    async function* source() {
      for (const x of [1, 2, 3, 4, 5, 6]) {
        pulled.push(x);
        yield x;
      }
    }
    await new EventEmitterPipeline<number>({ maxConcurrency: 2, ordered: false })
      .buffer(1)
      .transform((t) =>
        t.map(async (x: number) => {
          if (x === 1) expect(pulled.length).toBeLessThan(6);
          return x;
        }),
      )(source())
      .toArray();
  });
});

describe("#124 lifecycle events fire on channels separate from the worker channel (Done-when 9)", () => {
  it("an observer on stage:0:done alone is never handed a chunk to process", async () => {
    const received: unknown[] = [];
    const pipeline = new EventEmitterPipeline<number>()
      .buffer(1)
      .transform((t) => t.map((x: number) => x * 2));
    pipeline.emitter.on("stage:0:done", (event: unknown) => received.push(event));

    const out = await pipeline([1, 2]).toArray();

    expect(out).toEqual([2, 4]);
    expect(received).toHaveLength(2);
    for (const event of received) {
      expect((event as { respond?: unknown }).respond).toBeUndefined();
      expect((event as { reject?: unknown }).reject).toBeUndefined();
    }
  });
});

describe("#124 stage:<n>:end and pipeline:end fire once per run (Done-when 10)", () => {
  it(".first() then .toArray() on the same result fires pipeline:end twice", async () => {
    let stageEnds = 0;
    let pipelineEnds = 0;
    const pipeline = new EventEmitterPipeline<number>()
      .buffer(1)
      .transform((t) => t.map((x: number) => x * 2));
    pipeline.emitter.on("stage:0:end", () => stageEnds++);
    pipeline.emitter.on("pipeline:end", () => pipelineEnds++);

    const result = pipeline([1, 2, 3]);
    await result.first();
    expect(pipelineEnds).toBe(1);
    expect(stageEnds).toBe(1);

    await result.toArray();
    expect(pipelineEnds).toBe(2);
    expect(stageEnds).toBe(2);
  });
});

describe("#124 .local() dispatches nothing, and the stage after it resumes at the correct index (Done-when 11)", () => {
  it("stage 1 (the local region) never touches the emitter; stage 2 does", async () => {
    const pipeline = new EventEmitterPipeline<number>()
      .transform((t) => t.map((x: number) => x * 2))
      .local((p) => p.transform((t) => t.filter((x: number) => x > 2)))
      .transform((t) => t.map((x: number) => x + 1));

    const out = await pipeline([1, 2, 3]).toArray();

    expect(out).toEqual([5, 7]); // [2,4,6] -> filter >2 -> [4,6] -> +1 -> [5,7]
    expect(pipeline.emitter.listenerCount("stage:0")).toBe(1);
    expect(pipeline.emitter.listenerCount("stage:1")).toBe(0);
    expect(pipeline.emitter.listenerCount("stage:2")).toBe(1);
  });
});

// Not Done-when cases - real correctness gaps the review found in this class's own documented
// "every registered Worker runs on every chunk, first to settle decides" contract. Committed per
// this repo's Tests rule: the spike that proved each fix is deleted, and the answer survives here.
describe("#124 review round 1 - a synchronously-throwing Worker never aborts the dispatch loop", () => {
  it("F2: settles the dispatch with the throw, but a Worker registered after it still runs", async () => {
    const pipeline = new EventEmitterPipeline<number>()
      .buffer(1)
      .transform((t) => t.map((x: number) => x * 2));

    pipeline.emitter.on("stage:0", () => {
      throw new Error("sync-throw-from-first-worker");
    });

    let laterWorkerRan = false;
    pipeline.emitter.on("stage:0", ({ chunk, respond }: WorkerEvent) => {
      laterWorkerRan = true;
      respond(chunk.map((x) => x * 2));
    });

    await expect(pipeline([1]).toArray()).rejects.toThrow("sync-throw-from-first-worker");
    expect(laterWorkerRan).toBe(true);
  });
});

describe("#124 review round 1/2 + code-review xhigh - a throwing lifecycle observer never absorbs, masks or blocks a real outcome", () => {
  it(
    ":dispatched, :done (sync and async), a sibling :done listener, and :end/pipeline:end each surface as their own separate failure, never the dispatch's own",
    async () => {
      const fixture = await runFixture("__tests__/fixtures/eventemitter-throwing-observers.ts");
      expectFixtureOk(fixture);
      const result = JSON.parse(fixture.stdout.trim()) as {
        dispatched: { out: number[] | null; rejection: string | null };
        done: { out: number[] | null; rejection: string | null };
        asyncDone: { out: number[] | null; rejection: string | null };
        sibling: { out: number[] | null; siblingRan: boolean };
        ended: { rejection: string | null };
        unhandled: string[];
        uncaught: string[];
      };

      // F3: a throwing :dispatched listener used to reject the whole dispatch as if it were a
      // Worker failure - .onError() silently absorbed it and `out` came back [].
      expect(result.dispatched.rejection).toBeNull();
      expect(result.dispatched.out).toEqual([2, 4, 6]);

      // F4: a throwing :done listener fired inside the .then() callback that settles the real
      // dispatch - a raw emitter.emit() there would have escaped as an unhandled rejection instead
      // of settling the chunk first.
      expect(result.done.rejection).toBeNull();
      expect(result.done.out).toEqual([2, 4, 6]);

      // code-review xhigh's own F4: an ASYNC :done listener throwing AFTER its own `await` used to
      // leak as a real unhandledRejection instead of surfacing through emitSafely at all.
      expect(result.asyncDone.rejection).toBeNull();
      expect(result.asyncDone.out).toEqual([2, 4, 6]);

      // code-review xhigh's own F5: a synchronously-throwing :done listener used to stop Node's own
      // EventEmitter.emit() from ever reaching a SIBLING listener on the same event.
      expect(result.sibling.out).toEqual([2, 4, 6]);
      expect(result.sibling.siblingRan).toBe(true);

      // F5 (build's own numbering): a throwing :end/pipeline:end listener used to REPLACE a real,
      // already-propagating chunk error with its own unrelated one - both apply()'s and
      // drainable()'s own wrap are covered.
      expect(result.ended.rejection).toBe("real-chunk-failure");

      // Every observer's own throw still surfaces - as its own separate uncaughtException, never
      // as a silent unhandledRejection.
      expect(result.unhandled).toEqual([]);
      expect(result.uncaught.length).toBeGreaterThan(0);
    },
    FIXTURE_TIMEOUT,
  );
});
