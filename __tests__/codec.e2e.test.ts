/**
 * codec.e2e.test.ts - #209's Done-when cases, every one live and unconditional now that `enable`
 * has deleted the flag and the eager decode/re-encode path it used to guard. 7 is a compile-time
 * probe.
 *
 * Cases 2-6 dial a real `ws+unix:` connection in-process (`withWebSocketServer`,
 * `websocket-wire.e2e.test.ts`'s own two-instance shape: a client-side pipeline and a separate
 * server-side one, so a codec passed to only one side isolates "primary" from "server" counts with
 * no fork needed). Case 1 needs a real fork to tell a primary's own counts from a worker's (mirrors
 * `cluster-file-codec.ts`, #208).
 */
import { describe, it, expect } from "vitest";
import { WebSocketPipeline, JsonCodec, type Codec, type PipelineSocket } from "../src";
import { withWebSocketServer } from "./helpers/websocket";
import { FIXTURE_TIMEOUT, runFixtureJson } from "./helpers/fixtures";

/** The "another instance" side of a chain - an empty-source pipeline that only `serve()`s the SAME
 * stage definitions `builder` describes, mirroring `websocket-wire.e2e.test.ts`'s own `makeWorker`,
 * with an optional `codec` so a case can count the SERVER's own encode/decode calls separately from
 * the client's. */
function makeWorker<U>(
  builder: (t: WebSocketPipeline<number>) => WebSocketPipeline<U, any>,
  codec?: Codec,
): WebSocketPipeline<U, any> {
  return builder(new WebSocketPipeline<number>({ connect: "", codec }));
}

/** Wraps an inner `Codec`, counting every `encode`/`decode` call on THIS instance - a case passes
 * one to only the side (client or server) whose count it needs, per this file's own header. */
class CountingCodec implements Codec {
  encodes = 0;
  decodes = 0;
  constructor(private readonly inner: Codec) {}
  encode(value: unknown): Uint8Array | Promise<Uint8Array> {
    this.encodes++;
    return this.inner.encode(value);
  }
  decode(bytes: Uint8Array): unknown {
    this.decodes++;
    return this.inner.decode(bytes);
  }
}

describe("#209 the Interface program's own after example, over a real ClusterPipeline (Done-when 1)", () => {
  it(
    "prints [7,9,11,13,15,17], primary write 8 read 6 for toArray, write 8 read 0 for consume",
    async () => {
      const result = await runFixtureJson<{
        out: number[];
        toArrayOps: { write: number; read: number };
        consumeOps: { write: number; read: number };
      }>("__tests__/fixtures/cluster-codec-class.ts");
      expect(result.out).toEqual([7, 9, 11, 13, 15, 17]);
      expect(result.toArrayOps).toEqual({ write: 8, read: 6 });
      expect(result.consumeOps).toEqual({ write: 8, read: 0 });
    },
    FIXTURE_TIMEOUT,
  );
});

describe("#209 dispatched replies stay encoded between two stages (Done-when 2)", () => {
  it("primary decode 8 encode 8 against base's own 16/16", async () => {
    const worker = makeWorker((t) =>
      t
        .transform((tr) => tr.map((x: number) => x * 2))
        .transform((tr) => tr.map((x: number) => x + 1)),
    );
    await withWebSocketServer(worker, async (connect) => {
      const codec = new CountingCodec(new JsonCodec());
      const out = await new WebSocketPipeline<number>({ connect, codec })
        .buffer(1)
        .transform((t) => t.map((x: number) => x * 2))
        .transform((t) => t.map((x: number) => x + 1))([1, 2, 3, 4, 5, 6, 7, 8])
        .toArray();
      expect(out).toEqual([3, 5, 7, 9, 11, 13, 15, 17]);
      expect(codec.decodes).toBe(8);
      expect(codec.encodes).toBe(8);
    });
  });
});

describe("#209 an emptied chunk is never re-dispatched to the next stage (Done-when 3)", () => {
  it("output [], the server decodes only 8 items against base's own 16", async () => {
    const serverCodec = new CountingCodec(new JsonCodec());
    const worker = makeWorker(
      (t) =>
        t
          .transform((tr) => tr.filter((x: number) => x > 100))
          .transform((tr) => tr.map((x: number) => x + 1)),
      serverCodec,
    );
    await withWebSocketServer(worker, async (connect) => {
      const out = await new WebSocketPipeline<number>({ connect })
        .buffer(1)
        .transform((t) => t.filter((x: number) => x > 100))
        .transform((t) => t.map((x: number) => x + 1))([1, 2, 3, 4, 5, 6, 7, 8])
        .toArray();
      expect(out).toEqual([]);
      expect(serverCodec.decodes).toBe(8);
    });
  });
});

describe("#209 a dispatched reduce forwards encoded chunks too (Done-when 4)", () => {
  it("output [73], the primary decodes only for the terminal .toArray()", async () => {
    const worker = makeWorker((t) => {
      const reduced = t
        .transform((tr) => tr.map((x: number) => x * 2))
        .reduce((acc: number, x: number) => acc + x, 0);
      return reduced.transform((tr) => tr.map((x: number) => x + 1));
    });
    await withWebSocketServer(worker, async (connect) => {
      const codec = new CountingCodec(new JsonCodec());
      const chain = new WebSocketPipeline<number>({ connect, codec, maxConcurrency: 1 })
        .buffer(1)
        .transform((t) => t.map((x: number) => x * 2))
        .reduce((acc: number, x: number) => acc + x, 0);
      const out = await chain
        .transform((t) => t.map((x: number) => x + 1))([1, 2, 3, 4, 5, 6, 7, 8])
        .toArray();
      expect(out).toEqual([73]);
      expect(codec.decodes).toBe(1);
    });
  });
});

describe("#209 tap/local/buffer recut and branch keep base's own output (Done-when 5)", () => {
  it("routes and taps identically whether chunks travel encoded or not", async () => {
    // The worker's own `.tap()`/`.local()` calls are otherwise no-ops here, but they must still be
    // present: `.tap()` occupies a stage INDEX on the dispatching side (`architecture.md`'s own
    // "Observation" section), so the worker's registry needs the identical shape to keep index 2's
    // real map(+1) stage lined up with what the client actually dispatches to.
    const worker = makeWorker((t) =>
      t
        .transform((tr) => tr.map((x: number) => x * 2))
        .tap(() => {})
        .local((p) => p)
        .transform((tr) => tr.map((x: number) => x + 1)),
    );
    await withWebSocketServer(worker, async (connect) => {
      const seenTapped: number[] = [];
      const routed = await new WebSocketPipeline<number>({ connect })
        .buffer(1)
        .transform((t) => t.map((x: number) => x * 2))
        .tap((x: number) => seenTapped.push(x))
        .local((p) => p)
        .buffer(3)
        .transform((t) => t.map((x: number) => x + 1))
        .branch((b) => b.when("big", (x: number) => x > 10).otherwise("rest"))([
        1, 2, 3, 4, 5, 6, 7, 8,
      ]);
      expect(routed).toEqual({ big: [11, 13, 15, 17], rest: [3, 5, 7, 9] });
      expect(seenTapped).toEqual([2, 4, 6, 8, 10, 12, 14, 16]);
    });
  });
});

describe("#209 a pipeline with no codec still encodes JSON (Done-when 6)", () => {
  it("wire payload bytes equal a plain TextEncoder/JSON.stringify of the chunk", async () => {
    const inner = new JsonCodec();
    let captured: Uint8Array | undefined;
    const captureCodec: Codec = {
      encode: (value) => inner.encode(value),
      decode: (bytes) => {
        captured = bytes;
        return inner.decode(bytes);
      },
    };
    const worker = makeWorker((t) => t.transform((tr) => tr.map((x: number) => x)), captureCodec);
    await withWebSocketServer(worker, async (connect) => {
      // No `codec` option on the client - exercises the default.
      await new WebSocketPipeline<number>({ connect })
        .transform((t) => t.map((x: number) => x))([1, 2, 3])
        .toArray();
    });
    expect(captured).toEqual(new TextEncoder().encode(JSON.stringify([1, 2, 3])));
  });
});

describe("#209 a reply with no row count fails loud", () => {
  // A foreign or version-skewed server that replies with the pre-#209 wire shape (no `rows` on the
  // header) - `WebSocketPipeline.serve()` itself always sets `rows` now, so this hand-rolls the
  // exact framing the class's own docstring documents (a 4-byte big-endian header-length prefix,
  // the JSON header, then the payload) to reach the gap a real deployment could still hit.
  function encodeRawFrame(header: object, payload: Uint8Array): Uint8Array {
    const headerBytes = new TextEncoder().encode(JSON.stringify(header));
    const frame = new Uint8Array(4 + headerBytes.length + payload.length);
    new DataView(frame.buffer).setUint32(0, headerBytes.length, false);
    frame.set(headerBytes, 4);
    frame.set(payload, 4 + headerBytes.length);
    return frame;
  }

  it("rejects naming the stage, rather than treating the reply as empty", async () => {
    const codec = new JsonCodec();
    const rowsLessServer = {
      serve(socket: PipelineSocket) {
        socket.onMessage((data) => {
          if (typeof data === "string") return;
          const view = new DataView(
            (data as Uint8Array).buffer,
            (data as Uint8Array).byteOffset,
            (data as Uint8Array).byteLength,
          );
          const headerLength = view.getUint32(0, false);
          const header = JSON.parse(
            new TextDecoder().decode((data as Uint8Array).subarray(4, 4 + headerLength)),
          ) as { id: number };
          // No `rows` field - the pre-#209 reply shape.
          socket.send(encodeRawFrame({ id: header.id }, codec.encode([1, 2])));
        });
      },
    };
    await withWebSocketServer(rowsLessServer, async (connect) => {
      const pipeline = new WebSocketPipeline<number>({ connect, codec }).transform((t) =>
        t.map((x: number) => x),
      );
      await expect(pipeline([1, 2]).toArray()).rejects.toThrow(
        /stage 0.*reply carried no row count/,
      );
    });
  });
});

describe("#209 jsonCodec is deleted (Done-when 7)", () => {
  it("importing it fails tsc with TS2305 - compile error", () => {
    // Type-only: never executed. `tsc --noEmit` is the real assertion; `@ts-expect-error` itself
    // fails (TS2578) if the import ever stopped erroring - the negative-case pattern
    // `.claude/rules/typescript.md` calls for over trusting a "should fail" claim.
    function typeOnlyCheck() {
      // @ts-expect-error - jsonCodec is deleted (BREAKING, #209); tsc: TS2305 Module '"../src"' has no exported member 'jsonCodec'
      type _JsonCodecGone = typeof import("../src").jsonCodec;
    }
    expect(typeof typeOnlyCheck).toBe("function");
  });
});
