/**
 * eventemitter.e2e.test.ts — #221's Done-when cases, each proven through a real
 * `EventEmitterPipeline` run over a real `node:events` `EventEmitter`. No mocks: every "worker" is
 * a real function registered on a real emitter, every race is a real `setTimeout`.
 *
 * #124's own guarantees this ticket deletes (Done-when 9) - a route's own `listenerCount` counting
 * the composed function, `off()` taking the composed function's own stage over, and "no worker
 * registered" rejecting - have no test here any more; the composed function is never a listener,
 * so none of the three has anything left to assert.
 *
 * #124's own Done-when 6 (an async Worker throwing after its own `await` leaks no unhandled
 * rejection) and the throwing-lifecycle-observer regressions run as subprocess fixtures, the same reason
 * `concurrent-unhandled.ts` (#17) does - Vitest installs its own `unhandledRejection`/
 * `uncaughtException` handlers and would report a leak as a test-runner error, never a value this
 * file can observe directly.
 *
 * Done-when 8 (the repo-wide `rg` sweep for the old spellings) and Done-when 12 (`pnpm check`,
 * including the memory-benchmark baseline) are repo-wide gates, not per-case assertions - checked
 * once at the end of the build, not here.
 */
import { describe, it, expect } from "vitest";
import { EventEmitter } from "node:events";
import { EventEmitterPipeline, type PipelineEmitter, type WorkEvent } from "../src/eventemitter";
import { FIXTURE_TIMEOUT, runFixtureJson } from "./helpers/fixtures";

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** The real contract a Worker registered directly on a route receives - `code-review xhigh`'s own
 * F14: a hand-duplicated local interface reported errors the real shape never has. */
type WorkerEvent = WorkEvent<number, number>;

describe("#221 the Interface program - events read as routes, the composed function never registers (Done-when 1, 6)", () => {
  it("returns {evens:[60,80,100],odds:[]} and records exactly the eight after-event names", async () => {
    const emitter = new EventEmitter();
    const seen: string[] = [];
    const emit = emitter.emit.bind(emitter);
    emitter.emit = ((event: string, ...args: unknown[]) => {
      seen.push(String(event));
      return emit(event, ...args);
    }) as typeof emitter.emit;

    const split = new EventEmitterPipeline<number>({ emitter })
      .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 4))
      .branch((b) =>
        b
          .when(
            "evens",
            (x) => x % 2 === 0,
            (q) => q.transform((t) => t.map((x) => x * 10)),
          )
          .otherwise("odds"),
      );

    const out = await split([1, 2, 3, 4, 5]);

    expect(out).toEqual({ evens: [60, 80, 100], odds: [] });
    expect(seen).toEqual([
      "/transform/0:dispatched",
      "/transform/0:done",
      "/transform/0:end",
      ":end",
      "/branch/0/evens/transform/0:dispatched",
      "/branch/0/evens/transform/0:done",
      "/branch/0/evens/transform/0:end",
      "/branch/0/evens:end",
    ]);
    // The composed function is called directly, never registered - the emitter carries only what
    // this run's own lifecycle observer above added, and even that observer used `.on()` on an
    // event this class itself emits, never a Worker channel, so nothing is left listening either.
    expect(emitter.eventNames()).toEqual([]);
  });
});

describe("#221 two sibling arms with a parent stage no longer collide on the same route (Done-when 2)", () => {
  it("returns {big:[40,50,60,70],rest:[-3]}, the record a plain Pipeline returns", async () => {
    const out = await new EventEmitterPipeline<number>()
      .transform((t) => t.map((x: number) => x + 1))
      .branch((b) =>
        b
          .when(
            "big",
            (x) => x > 3,
            (q) => q.transform((t) => t.map((x) => x * 10)),
          )
          .otherwise("rest", (q) => q.transform((t) => t.map((x) => -x))),
      )([2, 3, 4, 5, 6]);

    expect(out).toEqual({ big: [40, 50, 60, 70], rest: [-3] });
  });
});

describe("#221 two forks of one base chain each answer with their own output (Done-when 3)", () => {
  it("returns [20,30] and [-2,-3], not both collapsing to the first fork's result", async () => {
    const base = new EventEmitterPipeline<number>().transform((t) => t.map((x: number) => x + 1));

    const a = await base
      .transform((t) => t.map((v) => v * 10))([1, 2])
      .toArray();
    const b = await base
      .transform((t) => t.map((v) => -v))([1, 2])
      .toArray();

    expect(a).toEqual([20, 30]);
    expect(b).toEqual([-2, -3]);
  });
});

describe("#221 two independently-constructed pipelines sharing one emitter each answer with their own output (Done-when 4)", () => {
  it("returns [10,20] and [-1,-2], not both collapsing to the first pipeline's", async () => {
    const emitter = new EventEmitter();
    const p1 = new EventEmitterPipeline<number>({ emitter }).transform((t) =>
      t.map((x: number) => x * 10),
    );
    const p2 = new EventEmitterPipeline<number>({ emitter }).transform((t) =>
      t.map((x: number) => -x),
    );

    const out1 = await p1([1, 2]).toArray();
    const out2 = await p2([1, 2]).toArray();

    expect(out1).toEqual([10, 20]);
    expect(out2).toEqual([-1, -2]);
  });
});

describe("#221 a Worker registered on an arm's own route name answers that arm (Done-when 5)", () => {
  it("returns {all:['W2','W3','W4']} from a Worker on /branch/0/all/transform/0", async () => {
    const emitter = new EventEmitter();
    emitter.on("/branch/0/all/transform/0", ({ chunk, respond }: WorkEvent<number, string>) =>
      respond(chunk.map((x: number) => "W" + x)),
    );

    const out = await new EventEmitterPipeline<number>({ emitter })
      .transform((t) => t.map((x: number) => x + 1))
      .branch((b) => b.otherwise("all", (q) => q.transform((t) => t.map((x) => -x))))([1, 2, 3]);

    expect(out).toEqual({ all: ["W2", "W3", "W4"] });
  });
});

describe("#221 .local() emits :dispatched only on the two dispatched stages (Done-when 7)", () => {
  it("dispatches on /transform/0 and /transform/2 only, skipping the pinned local stage", async () => {
    const emitter = new EventEmitter();
    const dispatched: string[] = [];
    emitter.on("/transform/0:dispatched", () => dispatched.push("/transform/0"));
    emitter.on("/transform/1:dispatched", () => dispatched.push("/transform/1"));
    emitter.on("/transform/2:dispatched", () => dispatched.push("/transform/2"));

    const out = await new EventEmitterPipeline<number>({ emitter })
      .transform((t) => t.map((x: number) => x * 2))
      .local((p) => p.transform((t) => t.filter((x: number) => x > 2)))
      .transform((t) => t.map((x: number) => x + 1))([1, 2, 3])
      .toArray();

    expect(out).toEqual([5, 7]);
    expect(dispatched).toEqual(["/transform/0", "/transform/2"]);
  });
});

describe("#221 options.emitter missing off() throws on both constructor forms (Done-when 10)", () => {
  // Missing exactly `off` - a trust-boundary value, so a real caller-supplied emitter this
  // incomplete is exactly what assertPipelineEmitter() exists to catch at construction.
  const withoutOff = {
    on() {},
    listeners() {
      return [];
    },
    listenerCount() {
      return 0;
    },
    emit() {},
  } as unknown as PipelineEmitter;

  it("throws on the options-only chain form", () => {
    expect(() => new EventEmitterPipeline<number>({ emitter: withoutOff })).toThrow(
      "options.emitter is missing 'off()' - it must satisfy PipelineEmitter",
    );
  });

  it("throws on the wrapping (chain, options) form", () => {
    const chain = new EventEmitterPipeline<number>().transform((t) => t.map((x: number) => x));
    expect(() => new EventEmitterPipeline<number>(chain, { emitter: withoutOff })).toThrow(
      "options.emitter is missing 'off()' - it must satisfy PipelineEmitter",
    );
  });
});

describe("#221 the composed function alone is a complete Worker", () => {
  it("runs with zero external .on() calls", async () => {
    const out = await new EventEmitterPipeline<number>()
      .buffer(1)
      .transform((t) => t.map((x: number) => x * 2))([1, 2, 3])
      .toArray();
    expect(out).toEqual([2, 4, 6]);
  });
});

describe("#221 an external Worker races the composed function", () => {
  it("a Worker rejecting at 5ms beats one resolving at 30ms, even though the resolving one registered first", async () => {
    const pipeline = new EventEmitterPipeline<number>().buffer(1).transform((t) =>
      t.map(async (x: number) => {
        await delay(30);
        return x * 2;
      }),
    );

    pipeline.emitter.on("/transform/0", async ({ chunk, reject }: WorkerEvent) => {
      await delay(5);
      reject(new Error(`external-worker-rejected-${chunk.join(",")}`));
    });

    await expect(pipeline([1]).toArray()).rejects.toThrow("external-worker-rejected-1");
  });
});

describe("#221 Pipeline.onError() reaches a rejecting Worker for free", () => {
  it("drops the chunk that fails and the run continues, with no explicit call in the class's own code", async () => {
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

describe("#221 an async Worker that throws after its own await never hangs or leaks", () => {
  it(
    "rejects the chunk exactly as an explicit reject() would, and leaves no unhandled rejection",
    async () => {
      const result = await runFixtureJson<{ rejection: string | null; unhandled: string[] }>(
        "__tests__/fixtures/eventemitter-async-throw.ts",
        [],
        true,
      );
      expect(result.rejection).toBe("worker-threw-after-await-1,2,3");
      expect(result.unhandled).toEqual([]);
    },
    FIXTURE_TIMEOUT,
  );
});

describe("#221 maxConcurrency/ordered behave exactly as ConcurrentPipeline's own", () => {
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

describe("#221 lifecycle events fire on channels separate from the worker channel", () => {
  it("an observer on /transform/0:done alone is never handed a chunk to process", async () => {
    const received: unknown[] = [];
    const pipeline = new EventEmitterPipeline<number>()
      .buffer(1)
      .transform((t) => t.map((x: number) => x * 2));
    pipeline.emitter.on("/transform/0:done", (event: unknown) => received.push(event));

    const out = await pipeline([1, 2]).toArray();

    expect(out).toEqual([2, 4]);
    expect(received).toHaveLength(2);
    for (const event of received) {
      expect((event as { respond?: unknown }).respond).toBeUndefined();
      expect((event as { reject?: unknown }).reject).toBeUndefined();
    }
  });
});

describe("#221 <route>:end and :end fire once per run", () => {
  it(".first() then .toArray() on the same result fires :end twice", async () => {
    let stageEnds = 0;
    let pipelineEnds = 0;
    const pipeline = new EventEmitterPipeline<number>()
      .buffer(1)
      .transform((t) => t.map((x: number) => x * 2));
    pipeline.emitter.on("/transform/0:end", () => stageEnds++);
    pipeline.emitter.on(":end", () => pipelineEnds++);

    const result = pipeline([1, 2, 3]);
    await result.first();
    expect(pipelineEnds).toBe(1);
    expect(stageEnds).toBe(1);

    await result.toArray();
    expect(pipelineEnds).toBe(2);
    expect(stageEnds).toBe(2);
  });
});

// Not Done-when cases - real correctness gaps #124's own review found in this class's own
// documented "every registered Worker runs on every chunk, first to settle decides" contract.
// Committed per this repo's Tests rule: the spike that proved each fix is deleted, and the answer
// survives here, under the routes this ticket renamed them to.
describe("#221 review round 1 - a synchronously-throwing Worker never aborts the dispatch loop", () => {
  it("F2: settles the dispatch with the throw, but a Worker registered after it still runs", async () => {
    const pipeline = new EventEmitterPipeline<number>()
      .buffer(1)
      .transform((t) => t.map((x: number) => x * 2));

    pipeline.emitter.on("/transform/0", () => {
      throw new Error("sync-throw-from-first-worker");
    });

    let laterWorkerRan = false;
    pipeline.emitter.on("/transform/0", ({ chunk, respond }: WorkerEvent) => {
      laterWorkerRan = true;
      respond(chunk.map((x) => x * 2));
    });

    await expect(pipeline([1]).toArray()).rejects.toThrow("sync-throw-from-first-worker");
    expect(laterWorkerRan).toBe(true);
  });
});

describe("#221 review round 1/2 - a throwing lifecycle observer never absorbs or masks a real outcome", () => {
  it(
    "F3 (:dispatched), F4 (:done) and F5 (:end) each surface as their own separate failure, never the dispatch's own",
    async () => {
      const result = await runFixtureJson<{
        dispatched: { out: number[] | null; rejection: string | null };
        done: { out: number[] | null; rejection: string | null };
        ended: { rejection: string | null };
        unhandled: string[];
        uncaught: string[];
      }>("__tests__/fixtures/eventemitter-throwing-observers.ts", [], true);

      // F3: a throwing :dispatched listener used to reject the whole dispatch as if it were a
      // Worker failure - .onError() silently absorbed it and `out` came back [].
      expect(result.dispatched.rejection).toBeNull();
      expect(result.dispatched.out).toEqual([2, 4, 6]);

      // F4: a throwing :done listener fired inside the .then() callback that settles the real
      // dispatch - a raw emitter.emit() there would have escaped as an unhandled rejection instead
      // of settling the chunk first.
      expect(result.done.rejection).toBeNull();
      expect(result.done.out).toEqual([2, 4, 6]);

      // F5: a throwing /transform/0:end/:end listener used to REPLACE a real, already-propagating
      // chunk error with its own unrelated one - both apply()'s and drainable()'s own wrap are
      // covered.
      expect(result.ended.rejection).toBe("real-chunk-failure");

      // Every observer's own throw still surfaces - as its own separate uncaughtException, never
      // as a silent unhandledRejection. code-review xhigh's own F4/F5 (an ASYNC :done listener
      // throwing after its own await, and a SYNC :done throw blocking a sibling listener on the
      // same event) are NOT covered here by decision - fixing both would mean bypassing the
      // caller's own emitter.emit() entirely, breaking .once() and a custom emitter's own .emit()
      // override for every event this class emits, not only the Worker channel that already
      // accepts that cost. Left as ordinary EventEmitter behavior a caller is expected to know.
      expect(result.unhandled).toEqual([]);
      expect(result.uncaught.length).toBeGreaterThan(0);
    },
    FIXTURE_TIMEOUT,
  );
});
