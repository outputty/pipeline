/**
 * reduce.e2e.test.ts — #45's Done-when cases, each proven through a REAL run: a real HTTP server on
 * loopback, a real `node:cluster` worker (subprocess fixture), a real duplex `fetch()`. No mocks.
 *
 * Every case #45 hasn't yet built is `test.fails`. Done-when 3 (`Transformer.reduce`, unchanged,
 * still per-chunk) already holds today and is a normal, passing `test`, kept here as a regression
 * guard once #45 lands alongside it. As each layer lands, its case flips from `test.fails` to
 * `test`. Done-when 7 (a repo-wide `perChunk` grep) and 8 (`pnpm check`) are shell gates run at the
 * enable layer, not vitest cases - neither has an "expected failure" a test runner can pin.
 */
import { describe, test, expect } from "vitest";
import type { IContextManager } from "../src";
import { Pipeline, ConcurrentPipeline, HttpPipeline } from "../src";
import {
  FIXTURE_TIMEOUT,
  HTTP_TIMEOUT,
  runFixture,
  withServer,
  expectFixtureOk,
  lastJsonLine,
} from "./helpers/fixtures";

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
    const data = await new Pipeline([1, 2, 3, 4, 5])
      .reduce((acc: number, x: number) => acc + x, 0)
      .transform((t) => t.map((n: number) => n * 10))
      .toArray();
    expect(data).toEqual([150]);
  });
});

describe("#45 emit() mid-fold, no trailing initial value (Done-when 2)", () => {
  test("prints [60,90]", async () => {
    const data = await new Pipeline([1, 2, 3, 4, 5])
      .reduce(emitAtSix, 0)
      .transform((t) => t.map((n: number) => n * 10))
      .toArray();
    expect(data).toEqual([60, 90]);
  });
});

describe("#45 Transformer.reduce still folds ONE chunk and still chains (Done-when 3)", () => {
  test("prints [30,70,50] at .buffer(2), re-spelled from chunkSize since #39 shipped", async () => {
    const data = await new Pipeline([1, 2, 3, 4, 5])
      .buffer(2)
      .transform((t) => t.reduce((a: number, x: number) => a + x, 0).map((n: number) => n * 10))
      .toArray();
    expect(data).toEqual([30, 70, 50]);
  });
});

describe("#45 a real duplex connection streams emits before the request body closes (Done-when 4)", () => {
  test.fails(
    "emits [6,9], at least one arriving before the request body closes",
    async () => {
      const worker = new HttpPipeline<number>([], { url: "" }).reduce(emitAtSix, 0);

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
        const reader = response.body!.getReader();
        const decoder = new TextDecoder();
        let buffer = "";
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          let newlineIndex: number;
          while ((newlineIndex = buffer.indexOf("\n")) !== -1) {
            const line = buffer.slice(0, newlineIndex);
            buffer = buffer.slice(newlineIndex + 1);
            if (line.length === 0) continue;
            const frame = JSON.parse(line) as { emit: number[] };
            receipts.push({ emit: frame.emit, bodyClosedAtReceipt: bodyClosed });
          }
        }

        expect(receipts.map((r) => r.emit[0])).toEqual([6, 9]);
        expect(receipts.some((r) => !r.bodyClosedAtReceipt)).toBe(true);
      });
    },
    HTTP_TIMEOUT,
  );
});

describe("#45 toNodeHandler streams both directions (Done-when 5)", () => {
  test.fails(
    "at least 2 of 3 response frames arrive before the request body closes",
    async () => {
      const worker = new HttpPipeline<number>([], { url: "" }).reduce(
        (acc: number, x: number, _ctx: IContextManager, emit: (v: number) => void): number => {
          emit(x);
          return acc;
        },
        0,
      );

      await withServer(worker.fetch, async (url) => {
        let bodyClosed = false;
        const encoder = new TextEncoder();

        const requestBody = new ReadableStream<Uint8Array>({
          async start(controller) {
            controller.enqueue(encoder.encode(`${JSON.stringify({ context: {} })}\n`));
            for (const item of [1, 2, 3]) {
              await new Promise((resolve) => setTimeout(resolve, 60));
              controller.enqueue(encoder.encode(`${JSON.stringify({ chunk: [item] })}\n`));
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

        let totalFrames = 0;
        let framesBeforeClose = 0;
        const reader = response.body!.getReader();
        const decoder = new TextDecoder();
        let buffer = "";
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          let newlineIndex: number;
          while ((newlineIndex = buffer.indexOf("\n")) !== -1) {
            const line = buffer.slice(0, newlineIndex);
            buffer = buffer.slice(newlineIndex + 1);
            if (line.length === 0) continue;
            totalFrames++;
            if (!bodyClosed) framesBeforeClose++;
          }
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
    const data = await new Pipeline([1, 2, 3, 4, 5])
      .reduce((acc: number, x: number) => acc + x, 0)
      .transform((t) => t.map((n: number) => n * 10))
      .toArray();
    expect(data).toEqual([150]);
  });

  test("ConcurrentPipeline", async () => {
    const data = await new ConcurrentPipeline([1, 2, 3, 4, 5])
      .reduce((acc: number, x: number) => acc + x, 0)
      .transform((t) => t.map((n: number) => n * 10))
      .toArray();
    expect(data).toEqual([150]);
  });

  // Tracks request PATHS, not just a raw count - the chained .transform() after .reduce() makes
  // its OWN real request regardless, so a bare count passes even when .reduce() itself silently
  // fell back to ConcurrentPipeline's in-process fold (real regression found running this test:
  // it passed at L3, before HttpPipeline.reduceWork() existed, on the transform's request alone).
  test.fails(
    "HttpPipeline, the reduce stage itself dispatched over /reduce/<n>",
    async () => {
      const requestPaths: string[] = [];
      const worker = new HttpPipeline<number>([], { url: "" })
        .reduce((acc: number, x: number) => acc + x, 0)
        .transform((t) => t.map((n: number) => n * 10));
      const trackingHandler = async (request: Request): Promise<Response> => {
        requestPaths.push(new URL(request.url).pathname);
        return worker.fetch(request);
      };
      await withServer(trackingHandler, async (url) => {
        const data = await new HttpPipeline<number>([1, 2, 3, 4, 5], { url })
          .reduce((acc: number, x: number) => acc + x, 0)
          .transform((t) => t.map((n: number) => n * 10))
          .toArray();
        expect(data).toEqual([150]);
        expect(requestPaths.some((p) => p.includes("/reduce/"))).toBe(true);
      });
    },
    HTTP_TIMEOUT,
  );

  // Cluster-context.ts's own pid-tagging technique - real subprocess fixture, see its own docstring.
  test.fails(
    "ClusterPipeline, dispatched to a real worker process",
    async () => {
      const fixture = await runFixture("__tests__/fixtures/cluster-reduce.ts");
      expectFixtureOk(fixture);
      const result = lastJsonLine<{ sum: number; dispatchedToWorker: boolean }>(fixture);
      expect(result.sum * 10).toBe(150);
      expect(result.dispatchedToWorker).toBe(true);
    },
    FIXTURE_TIMEOUT,
  );
});
