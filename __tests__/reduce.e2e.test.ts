/**
 * reduce.e2e.test.ts — #45's Done-when cases, each proven through a REAL run: a real HTTP server on
 * loopback, a real `node:cluster` worker (subprocess fixture), a real duplex `fetch()`. No mocks.
 * #62's own cases (a partitioned reduce, no combine step) live in their own "#62 ..." blocks below,
 * same file, same real-run standard - one reduce mechanism, one home.
 *
 * Every case #45 hasn't yet built is `test.fails`. Done-when 3 (`Transformer.reduce`, unchanged,
 * still per-chunk) already holds today and is a normal, passing `test`, kept here as a regression
 * guard once #45 lands alongside it. As each layer lands, its case flips from `test.fails` to
 * `test`. Done-when 7 (a repo-wide grep for the deleted terminal-reduce option) and 8 (`pnpm check`)
 * are shell gates run at the enable layer, not vitest cases - neither has an "expected failure" a
 * test runner can pin.
 */
import { describe, test, expect } from "vitest";
import type { IContextManager } from "../src";
import { Pipeline, ConcurrentPipeline, HttpPipeline } from "../src";
import { FIXTURE_TIMEOUT, HTTP_TIMEOUT, withServer, runFixtureJson } from "./helpers/fixtures";
// The SAME parser the client (HttpPipeline.reduceWork) and server (.fetch's /reduce/<n> handling)
// use - review found this test hand-rolling its own copy, missing the shared one's trailing-buffer
// flush (a final unterminated frame silently dropped), so a wire-format bug there would be
// invisible here. Reusing it also means a fix to the shared parser IS exercised by this test.
import { readNdjsonLines } from "../src/utils/ndjson";

/** The emit-at-6 reducer the ticket's own Done-when 2 and 4 both use: banks a running total once it
 * reaches 6, resetting to 0 - no trailing value when the last item already banked one. */
function emitAtSix(
  acc: number,
  x: number,
  _ctx: IContextManager,
  emit: (v: number) => void,
): number {
  acc += x;
  if (acc >= 6) {
    emit(acc);
    return 0;
  }
  return acc;
}

describe("#45 the whole dataset folds and the chain continues (Done-when 1)", () => {
  test("prints [150]", async () => {
    const data = await new Pipeline<number>()

      .reduce((acc: number, x: number) => acc + x, 0)
      .transform((t) => t.map((n: number) => n * 10))([1, 2, 3, 4, 5])
      .toArray();
    expect(data).toEqual([150]);
  });
});

describe("#45 emit() mid-fold, no trailing initial value (Done-when 2)", () => {
  test("prints [60,90]", async () => {
    const data = await new Pipeline<number>()

      .reduce(emitAtSix, 0)
      .transform((t) => t.map((n: number) => n * 10))([1, 2, 3, 4, 5])
      .toArray();
    expect(data).toEqual([60, 90]);
  });
});

describe("#45 Transformer.reduce still folds ONE chunk and still chains (Done-when 3)", () => {
  test("prints [30,70,50] at .buffer(2), re-spelled from chunkSize since #39 shipped", async () => {
    const data = await new Pipeline<number>()

      .buffer(2)
      .transform((t) => t.reduce((a: number, x: number) => a + x, 0).map((n: number) => n * 10))([
        1, 2, 3, 4, 5,
      ])
      .toArray();
    expect(data).toEqual([30, 70, 50]);
  });
});

describe("#45 a real duplex connection streams emits before the request body closes (Done-when 4)", () => {
  test(
    "emits [6,9], at least one arriving before the request body closes",
    async () => {
      const worker = new HttpPipeline<number>({ url: "" }).reduce(emitAtSix, 0);

      await withServer(worker.fetch, async (url) => {
        let bodyClosed = false;
        const encoder = new TextEncoder();
        const framesToSend: number[][] = [[1, 2], [3, 4], [5]];

        const requestBody = new ReadableStream<Uint8Array>({
          async start(controller) {
            controller.enqueue(encoder.encode(`${JSON.stringify({ context: {} })}\n`));
            for (const chunk of framesToSend) {
              await new Promise((resolve) => setTimeout(resolve, 60));
              controller.enqueue(encoder.encode(`${JSON.stringify({ chunk })}\n`));
            }
            controller.close();
            bodyClosed = true;
          },
        });

        const response = await fetch(`${url}/reduce/0`, {
          method: "POST",
          headers: { "content-type": "application/x-ndjson" },
          body: requestBody,
          duplex: "half",
        } as RequestInit);

        const receipts: { emit: number[]; bodyClosedAtReceipt: boolean }[] = [];
        for await (const line of readNdjsonLines(response.body!)) {
          const frame = JSON.parse(line) as { emit: number[] };
          receipts.push({ emit: frame.emit, bodyClosedAtReceipt: bodyClosed });
        }

        expect(receipts.map((r) => r.emit[0])).toEqual([6, 9]);
        expect(receipts.some((r) => !r.bodyClosedAtReceipt)).toBe(true);
      });
    },
    HTTP_TIMEOUT,
  );
});

describe("#45 toNodeHandler streams both directions (Done-when 5)", () => {
  // Independent of HttpPipeline/reduce (L5's own /reduce/<n> route doesn't exist yet at this
  // layer) - a raw duplex echo handler, so this pins toNodeHandler's OWN bridge behavior alone.
  // Real, before this layer: all replies arrived in ONE frame after the request body closed
  // (architecture.md's own measurement, `handleOverBridge` buffering both directions whole).
  const echoHandler = async (request: Request): Promise<Response> => {
    const { readable, writable } = new TransformStream<Uint8Array, Uint8Array>();
    const writer = writable.getWriter();
    const encoder = new TextEncoder();
    (async () => {
      for await (const line of readNdjsonLines(request.body!)) {
        await writer.write(encoder.encode(`${line}\n`));
      }
      await writer.close();
    })().catch(() => writer.abort());
    return new Response(readable, { headers: { "content-type": "application/x-ndjson" } });
  };

  test(
    "at least 2 of 3 response frames arrive before the request body closes",
    async () => {
      await withServer(echoHandler, async (url) => {
        let bodyClosed = false;
        const encoder = new TextEncoder();

        const requestBody = new ReadableStream<Uint8Array>({
          async start(controller) {
            for (const item of [1, 2, 3]) {
              await new Promise((resolve) => setTimeout(resolve, 60));
              controller.enqueue(encoder.encode(`${JSON.stringify({ item })}\n`));
            }
            controller.close();
            bodyClosed = true;
          },
        });

        const response = await fetch(url, {
          method: "POST",
          headers: { "content-type": "application/x-ndjson" },
          body: requestBody,
          duplex: "half",
        } as RequestInit);

        let totalFrames = 0;
        let framesBeforeClose = 0;
        for await (const _line of readNdjsonLines(response.body!)) {
          totalFrames++;
          if (!bodyClosed) framesBeforeClose++;
        }

        expect(totalFrames).toBe(3);
        expect(framesBeforeClose).toBeGreaterThanOrEqual(2);
      });
    },
    HTTP_TIMEOUT,
  );
});

describe("#45 case 1 returns [150] on every Pipeline class (Done-when 6)", () => {
  test("Pipeline", async () => {
    const data = await new Pipeline<number>()

      .reduce((acc: number, x: number) => acc + x, 0)
      .transform((t) => t.map((n: number) => n * 10))([1, 2, 3, 4, 5])
      .toArray();
    expect(data).toEqual([150]);
  });

  // #62: a dispatched reduce now really partitions - written so this conformance case runs
  // identically on all four classes, per the ticket's own Constraints ("#37's premise... now has a
  // real exception"). With no `.buffer()` the source is one chunk (`DEFAULT_CHUNK_SIZE`), so there
  // is only ever one real partition regardless of how many were requested - `share()`'s free-slot
  // dealing means every OTHER partition sees no chunks and emits nothing, so the merged output is
  // naturally the same single `[150]` with no extra fold step needed.
  test("ConcurrentPipeline", async () => {
    const data = await new ConcurrentPipeline<number>()

      .reduce((acc: number, x: number) => acc + x, 0)
      .transform((t) => t.map((n: number) => n * 10))([1, 2, 3, 4, 5])
      .toArray();
    expect(data).toEqual([150]);
  });

  // Tracks request PATHS, not just a raw count - the chained .transform() after .reduce() makes
  // its OWN real request regardless, so a bare count passes even when .reduce() itself silently
  // fell back to ConcurrentPipeline's in-process fold (real regression found running this test:
  // it passed at L3, before HttpPipeline.reduceWork() existed, on the transform's request alone).
  test(
    "HttpPipeline, the reduce stage itself dispatched over /reduce/<n>",
    async () => {
      const requestPaths: string[] = [];
      // The worker must register the IDENTICAL stage sequence the orchestrator below dispatches
      // against, so the worker's own `.transform()` lands at the SAME index the orchestrator's
      // does (architecture.md: "index N means the same transform on both sides").
      const worker = new HttpPipeline<number>({ url: "" })
        .reduce((acc: number, x: number) => acc + x, 0)
        .transform((t) => t.map((n: number) => n * 10));
      const trackingHandler = async (request: Request): Promise<Response> => {
        requestPaths.push(new URL(request.url).pathname);
        return worker.fetch(request);
      };
      await withServer(trackingHandler, async (url) => {
        const data = await new HttpPipeline<number>({ url })

          .reduce((acc: number, x: number) => acc + x, 0)
          .transform((t) => t.map((n: number) => n * 10))([1, 2, 3, 4, 5])
          .toArray();
        expect(data).toEqual([150]);
        expect(requestPaths.some((p) => p.includes("/reduce/"))).toBe(true);
      });
    },
    HTTP_TIMEOUT,
  );

  // Cluster-context.ts's own pid-tagging technique - real subprocess fixture, see its own docstring.
  test(
    "ClusterPipeline, dispatched to a real worker process",
    async () => {
      const result = await runFixtureJson<{ sum: number; dispatchedToWorker: boolean }>(
        "__tests__/fixtures/cluster-reduce.ts",
      );
      expect(result.sum * 10).toBe(150);
      expect(result.dispatchedToWorker).toBe(true);
    },
    FIXTURE_TIMEOUT,
  );
});

describe("#62 a dispatched reduce partitions - each partition's result flows through, no debt", () => {
  test("buffer(2), maxConcurrency 2: N values summing to 15, no throw", async () => {
    const data = await new ConcurrentPipeline<number>({ maxConcurrency: 2 })

      .buffer(2)
      .reduce(
        (acc: number, x: number) => acc + x,
        0,
      )([1, 2, 3, 4, 5])
      .toArray();
    expect(data.reduce((a, b) => a + b, 0)).toBe(15);
    expect(data.length).toBeGreaterThanOrEqual(1);
    expect(data.length).toBeLessThanOrEqual(2); // partition count is a ceiling, not a promise
  });

  // The count case: reusing the fold as its own merge would be wrong here (partials [3,2] folded
  // by the SAME fn counts the partials, giving 2, not 5) - this is exactly why there's no automatic
  // merge at all. Left as N raw values, the caller decides what to do with them.
  test("the count case: N partial counts summing to 5, no throw", async () => {
    const data = await new ConcurrentPipeline<number>({ maxConcurrency: 2 })

      .buffer(2)
      .reduce(
        (acc: number, _x: number) => acc + 1,
        0,
      )([1, 2, 3, 4, 5])
      .toArray();
    expect(data.reduce((a, b) => a + b, 0)).toBe(5);
  });

  test("the base Pipeline is untouched - never partitions, prints [15]", async () => {
    const data = await new Pipeline<number>()

      .buffer(2)
      .reduce(
        (acc: number, x: number) => acc + x,
        0,
      )([1, 2, 3, 4, 5])
      .toArray();
    expect(data).toEqual([15]);
  });

  test("maxConcurrency 1 yields exactly one partition, prints [15] directly", async () => {
    const data = await new ConcurrentPipeline<number>({ maxConcurrency: 1 })

      .buffer(2)
      .reduce(
        (acc: number, x: number) => acc + x,
        0,
      )([1, 2, 3, 4, 5])
      .toArray();
    expect(data).toEqual([15]);
  });

  test("no .buffer() is one chunk, one partition - prints [15] directly, same as the base Pipeline", async () => {
    const data = await new ConcurrentPipeline<number>({ maxConcurrency: 2 })

      .reduce(
        (acc: number, x: number) => acc + x,
        0,
      )([1, 2, 3, 4, 5])
      .toArray();
    expect(data).toEqual([15]);
  });

  // The one thing a caller who wants ONE final value writes by hand - an ordinary second reduce,
  // the same pattern as folding down any other multi-value reduce output. Nothing named "combine".
  test("a caller who wants one value writes an ordinary second reduce via .local()", async () => {
    const data = await new ConcurrentPipeline<number>({ maxConcurrency: 2 })

      .buffer(2)
      .reduce((acc: number, x: number) => acc + x, 0)
      .local((p) => p.reduce((acc: number, v: number) => acc + v, 0))([1, 2, 3, 4, 5])
      .toArray();
    expect(data).toEqual([15]);
  });
});

describe("#62 an input chunk's emits stay together as one output chunk", () => {
  test("each output chunk holds exactly one input chunk's emits", async () => {
    const observedChunks: number[][] = [];
    const partitioned = new ConcurrentPipeline<number>({ maxConcurrency: 2 })

      .buffer(2)
      .reduce((_acc: number, x: number, _ctx: IContextManager, emit: (v: number) => void) => {
        emit(x * 10);
        return 0;
      }, 0);
    for await (const chunk of partitioned([1, 2, 3, 4, 5]).chunks()) {
      observedChunks.push([...chunk]);
    }
    // buffer(2) over [1,2,3,4,5] -> input chunks [1,2],[3,4],[5]; each item emits its own value,
    // so one INPUT chunk's emits (folded together) must equal that chunk's own doubled-times-ten
    // sum - partition assignment is timing-dependent, but the per-input-chunk grouping is not.
    const foldedPerChunk = observedChunks
      .map((c) => c.reduce((a, v) => a + v, 0))
      .sort((a, b) => a - b);
    expect(foldedPerChunk).toEqual([30, 50, 70]);
    expect(observedChunks.flat().sort((a, b) => a - b)).toEqual([10, 20, 30, 40, 50]);
  });

  test(".toArray() itself returns the same raw partials directly, no throw", async () => {
    const data = await new ConcurrentPipeline<number>({ maxConcurrency: 2 })

      .buffer(2)
      .reduce((_acc: number, x: number, _ctx: IContextManager, emit: (v: number) => void) => {
        emit(x * 10);
        return 0;
      }, 0)([1, 2, 3, 4, 5])
      .toArray();
    expect(data.flat().sort((a, b) => a - b)).toEqual([10, 20, 30, 40, 50]);
  });
});

describe("#62 the same chains behave identically over HttpPipeline and ClusterPipeline", () => {
  test(
    "HttpPipeline - sum and count both flow through as N values, no throw",
    async () => {
      const sumWorker = new HttpPipeline<number>({ url: "" }).reduce(
        (acc: number, x: number) => acc + x,
        0,
      );
      await withServer(sumWorker.fetch, async (url) => {
        const sum = await new HttpPipeline<number>({ url, maxConcurrency: 2 })

          .buffer(2)
          .reduce(
            (acc: number, x: number) => acc + x,
            0,
          )([1, 2, 3, 4, 5])
          .toArray();
        expect(sum.reduce((a, b) => a + b, 0)).toBe(15);

        // Same chain, merged by hand via .local() - the mechanism a caller reaches for, not just a
        // JS-side sum of the raw partials above.
        const merged = await new HttpPipeline<number>({ url, maxConcurrency: 2 })

          .buffer(2)
          .reduce((acc: number, x: number) => acc + x, 0)
          .local((p) => p.reduce((acc: number, v: number) => acc + v, 0))([1, 2, 3, 4, 5])
          .toArray();
        expect(merged).toEqual([15]);
      });

      const countWorker = new HttpPipeline<number>({ url: "" }).reduce(
        (acc: number, _x: number) => acc + 1,
        0,
      );
      await withServer(countWorker.fetch, async (url) => {
        const count = await new HttpPipeline<number>({ url, maxConcurrency: 2 })

          .buffer(2)
          .reduce(
            (acc: number, _x: number) => acc + 1,
            0,
          )([1, 2, 3, 4, 5])
          .toArray();
        expect(count.reduce((a, b) => a + b, 0)).toBe(5);

        const countMerged = await new HttpPipeline<number>({
          url,
          maxConcurrency: 2,
        })

          .buffer(2)
          .reduce((acc: number, _x: number) => acc + 1, 0)
          .local((p) => p.reduce((acc: number, v: number) => acc + v, 0))([1, 2, 3, 4, 5])
          .toArray();
        expect(countMerged).toEqual([5]);
      });
    },
    HTTP_TIMEOUT,
  );

  test(
    "ClusterPipeline - sum and count both flow through as N values; .local() still merges to one",
    async () => {
      const result = await runFixtureJson<{
        sumTotal: number;
        countTotal: number;
        merged: number[];
        countMerged: number[];
      }>("__tests__/fixtures/cluster-partitioned-reduce.ts");
      expect(result.sumTotal).toBe(15);
      expect(result.countTotal).toBe(5);
      expect(result.merged).toEqual([15]);
      expect(result.countMerged).toEqual([5]);
    },
    FIXTURE_TIMEOUT,
  );
});
