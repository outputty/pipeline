/**
 * websocket-wire.e2e.test.ts — `WebSocketPipeline`'s own wire (#201 L2), proven directly: a real
 * `ws+unix:` server (`helpers/websocket.ts`'s `withWebSocketServer`), a real client dispatch, no
 * mocks. `ClusterPipeline` reparenting onto this class is L3's own layer, not exercised here.
 */
import { describe, it, expect } from "vitest";
import { WebSocketPipeline } from "../src/websocket";
import { withWebSocketServer } from "./helpers/websocket";

/** The "another instance" side of a `WebSocketPipeline` chain - an empty-source pipeline whose only
 * job is to `serve()` the SAME stage definitions `builder` describes, mirroring `pipelines.e2e.test.ts`'s
 * own `makeWorker` for `HttpPipeline`. */
function makeWorker<U>(
  builder: (t: WebSocketPipeline<number>) => WebSocketPipeline<U, any>,
): WebSocketPipeline<U, any> {
  return builder(new WebSocketPipeline<number>({ connect: "" }));
}

describe("#201 the canonical map/filter chain over a real ws+unix connection", () => {
  it("prints [6,8,10]", async () => {
    const worker = makeWorker((t) =>
      t.transform((tr) => tr.map((x: number) => x * 2).filter((x: number) => x > 4)),
    );
    await withWebSocketServer(worker, async (connect) => {
      const out = await new WebSocketPipeline<number>({ connect })
        .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 4))([1, 2, 3, 4, 5])
        .toArray();
      expect(out).toEqual([6, 8, 10]);
    });
  });

  it("two stages each open one round trip and both run", async () => {
    const worker = makeWorker((t) =>
      t
        .transform((tr) => tr.map((x: number) => x * 2))
        .transform((tr) => tr.filter((x: number) => x > 4)),
    );
    await withWebSocketServer(worker, async (connect) => {
      const out = await new WebSocketPipeline<number>({ connect })
        .transform((t) => t.map((x: number) => x * 2))
        .transform((t) => t.filter((x: number) => x > 4))([1, 2, 3, 4, 5])
        .toArray();
      expect(out).toEqual([6, 8, 10]);
    });
  });
});

describe("#201 context propagates forward over the wire", () => {
  it("multiplies by the caller's own context value", async () => {
    const worker = makeWorker((t) =>
      t.transform((tr) => tr.map((x: number, ctx) => x * (ctx.get("multiplier") as number))),
    );
    await withWebSocketServer(worker, async (connect) => {
      const out = await new WebSocketPipeline<number>({ connect })
        .context({ multiplier: 10 })
        .transform((t) => t.map((x: number, ctx) => x * (ctx.get("multiplier") as number)))([
          1, 2, 3, 4, 5,
        ])
        .toArray();
      expect(out).toEqual([10, 20, 30, 40, 50]);
    });
  });
});

describe("#201 a stage's own throw reaches the dispatching side as a rejection", () => {
  it("names the stage in the error message", async () => {
    const worker = makeWorker((t) =>
      t.transform((tr) =>
        tr.map((x: number) => {
          if (x === 3) throw new Error("boom");
          return x;
        }),
      ),
    );
    await withWebSocketServer(worker, async (connect) => {
      const pipeline = new WebSocketPipeline<number>({ connect }).transform((t) =>
        t.map((x: number) => {
          if (x === 3) throw new Error("boom");
          return x;
        }),
      );
      await expect(pipeline([1, 2, 3]).toArray()).rejects.toThrow(/stage 0.*boom/);
    });
  });

  it("an unknown stage index names the range this deployment serves", async () => {
    const worker = makeWorker((t) => t.transform((tr) => tr.map((x: number) => x)));
    await withWebSocketServer(worker, async (connect) => {
      // Two stages dispatched against a worker that only ever registered one (index 0).
      const pipeline = new WebSocketPipeline<number>({ connect })
        .transform((t) => t.map((x: number) => x))
        .transform((t) => t.map((x: number) => x * 2));
      await expect(pipeline([1, 2, 3]).toArray()).rejects.toThrow(/unknown stage 1/);
    });
  });
});

describe("#201 partitioned reduce over the wire (product.md's own canonical example)", () => {
  it("prints two numbers summing to 15", async () => {
    const worker = makeWorker(
      (t) =>
        t.reduce((acc: number, x: number) => acc + x, 0) as unknown as WebSocketPipeline<
          number,
          any
        >,
    );
    await withWebSocketServer(worker, async (connect) => {
      const out = await new WebSocketPipeline<number>({ connect, maxConcurrency: 2 })
        .buffer(2)
        .reduce(
          (acc: number, x: number) => acc + x,
          0,
        )([1, 2, 3, 4, 5])
        .toArray();
      expect(out.reduce((a, b) => a + b, 0)).toBe(15);
    });
  });

  it("a single partition folds the whole stream to one value", async () => {
    const worker = makeWorker(
      (t) =>
        t.reduce((acc: number, x: number) => acc + x, 0) as unknown as WebSocketPipeline<
          number,
          any
        >,
    );
    await withWebSocketServer(worker, async (connect) => {
      const out = await new WebSocketPipeline<number>({ connect, maxConcurrency: 1 })
        .reduce(
          (acc: number, x: number) => acc + x,
          0,
        )([1, 2, 3, 4, 5])
        .toArray();
      expect(out).toEqual([15]);
    });
  });

  it("a mid-fold emit arrives as its own downstream value", async () => {
    const reducer = (acc: number, x: number, _ctx: unknown, emit: (v: number) => void): number => {
      const next = acc + x;
      if (next >= 6) {
        emit(next);
        return 0;
      }
      return next;
    };
    const worker = makeWorker(
      (t) => t.reduce(reducer, 0) as unknown as WebSocketPipeline<number, any>,
    );
    await withWebSocketServer(worker, async (connect) => {
      const out = await new WebSocketPipeline<number>({ connect, maxConcurrency: 1 })
        .reduce(
          reducer,
          0,
        )([1, 2, 3, 4, 5])
        .toArray();
      expect(out).toEqual([6, 9]);
    });
  });
});
