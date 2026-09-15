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
// Done-when 7's own compile-time probe (below): a real named import, not a `typeof import()`
// namespace access, so its own `@ts-expect-error` pins the ACTUAL diagnostic that shape produces.
// @ts-expect-error - jsonCodec is deleted (BREAKING, #209); tsc: TS2724 '"../src"' has no exported member named 'jsonCodec'. Did you mean 'JsonCodec'?
import { jsonCodec as _jsonCodecDeleted } from "../src";

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

describe("#209 review: an emptied chunk is never sent to a dispatched reduce either", () => {
  // Same shape as "an emptied chunk is never re-dispatched to the next stage" (Done-when 3), but for
  // the reduce pump specifically (`WebSocketPipeline.reduceWork()`'s own dispatch loop) - the
  // `isEncodedChunk(chunk) && chunk.rows === 0` skip this review added there, mirroring
  // `ConcurrentPipeline.apply()`'s existing one for a plain dispatched transform.
  it("output [], the server decodes only 8 items - the empty chunk never reaches the fold", async () => {
    const serverCodec = new CountingCodec(new JsonCodec());
    const worker = makeWorker(
      (t) =>
        t
          .transform((tr) => tr.filter((x: number) => x > 100))
          .reduce((acc: number, x: number) => acc + x, 0),
      serverCodec,
    );
    await withWebSocketServer(worker, async (connect) => {
      const out = await new WebSocketPipeline<number>({ connect })
        .buffer(1)
        .transform((t) => t.filter((x: number) => x > 100))
        .reduce(
          (acc: number, x: number) => acc + x,
          0,
        )([1, 2, 3, 4, 5, 6, 7, 8])
        .toArray();
      expect(out).toEqual([]);
      expect(serverCodec.decodes).toBe(8);
    });
  });
});

describe("#209 review: the empty-chunk skip survives past one hop", () => {
  // The skip's own return value used to be a fresh `[]`, losing the encoded-chunk tag - the SECOND
  // dispatched stage's identical check no longer recognized it and dispatched a chunk already known
  // to be empty. Three dispatched stages here, only the first ever seeing real rows.
  it("the server decodes only 8 items across THREE dispatched stages, not 24", async () => {
    const serverCodec = new CountingCodec(new JsonCodec());
    const worker = makeWorker(
      (t) =>
        t
          .transform((tr) => tr.filter((x: number) => x > 100))
          .transform((tr) => tr.map((x: number) => x + 1))
          .transform((tr) => tr.map((x: number) => x + 1)),
      serverCodec,
    );
    await withWebSocketServer(worker, async (connect) => {
      const out = await new WebSocketPipeline<number>({ connect })
        .buffer(1)
        .transform((t) => t.filter((x: number) => x > 100))
        .transform((t) => t.map((x: number) => x + 1))
        .transform((t) => t.map((x: number) => x + 1))([1, 2, 3, 4, 5, 6, 7, 8])
        .toArray();
      expect(out).toEqual([]);
      expect(serverCodec.decodes).toBe(8);
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

describe("#209 branch after one dispatched stage matches planning spike 2's own output (Done-when 5)", () => {
  // The tap/local/buffer test above pins its own chain's output as a regression check, but its
  // branch numbers ([11,13,15,17]/[3,5,7,9]) are downstream of a SECOND dispatched stage
  // (map x+1) the ticket's own spike never ran before branching. Done-when 5 cites planning spike
  // 2's real branch output as `{"big":[10,12,14,16],"rest":[2,4,6,8]}` - one dispatched stage
  // (map x*2), then `.branch()` directly, predicate `x > 8`. This pins that exact shape.
  it("routes {big:[10,12,14,16],rest:[2,4,6,8]}, matching the ticket's own cited spike output", async () => {
    const worker = makeWorker((t) => t.transform((tr) => tr.map((x: number) => x * 2)));
    await withWebSocketServer(worker, async (connect) => {
      const routed = await new WebSocketPipeline<number>({ connect })
        .buffer(1)
        .transform((t) => t.map((x: number) => x * 2))
        .branch((b) => b.when("big", (x: number) => x > 8).otherwise("rest"))([
        1, 2, 3, 4, 5, 6, 7, 8,
      ]);
      expect(routed).toEqual({ big: [10, 12, 14, 16], rest: [2, 4, 6, 8] });
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

describe("#209 review: a codec decode failure bypasses Pipeline.onError() (pinning, not a Done-when)", () => {
  // Pins current behavior, found in code review, not settled as the intended design: before #209,
  // `stageWork()`'s onFrame decoded at dispatch time and a throwing `codec.decode()` rejected
  // `runStageChunk`'s own promise, which `Pipeline.onError()`'s documented per-chunk drop-and-continue
  // contract (Language, CLAUDE.md) caught like any other row failure. Now decode is deferred to
  // `materialize()`, called outside `runStageChunk` (`drainable()`, `.local()`'s seed, `flattenChunks`),
  // so `.onError()` is never consulted - the whole terminal rejects instead of dropping the one chunk.
  it("a stage failure IS dropped by onError (control) - a decode failure is NOT", async () => {
    const stageWorker = makeWorker((t) =>
      t.transform((tr) =>
        tr.map((x: number) => {
          if (x === 6) throw new Error("stage boom");
          return x;
        }),
      ),
    );
    await withWebSocketServer(stageWorker, async (connect) => {
      const out = await new WebSocketPipeline<number>({ connect })
        .buffer(1)
        .onError(() => {})
        .transform((t) =>
          t.map((x: number) => {
            if (x === 6) throw new Error("stage boom");
            return x;
          }),
        )([1, 2, 3, 4, 5, 6, 7, 8])
        .toArray();
      expect(out).toEqual([1, 2, 3, 4, 5, 7, 8]);
    });
  });

  it("a decode failure rejects the whole drain instead of dropping the one chunk", async () => {
    const worker = makeWorker((t) => t.transform((tr) => tr.map((x: number) => x)));
    const inner = new JsonCodec();
    let decodes = 0;
    const flakyCodec: Codec = {
      encode: (v) => inner.encode(v),
      decode: (bytes) => {
        decodes++;
        if (decodes === 3) throw new Error("boom: malformed reply");
        return inner.decode(bytes);
      },
    };
    await withWebSocketServer(worker, async (connect) => {
      const chain = new WebSocketPipeline<number>({ connect, codec: flakyCodec })
        .buffer(1)
        .onError(() => {})
        .transform((t) => t.map((x: number) => x));
      await expect(chain([1, 2, 3, 4, 5, 6, 7, 8]).toArray()).rejects.toThrow(
        /boom: malformed reply/,
      );
    });
  });
});

describe("#209 a reply with no row count fails loud", () => {
  // A foreign or version-skewed server that replies with the pre-#209 wire shape (no `rows` on the
  // header) - `WebSocketPipeline.serve()` itself always sets `rows` now, so this hand-rolls the
  // exact framing the class's own docstring documents (a 4-byte big-endian header-length prefix,
  // the JSON header, then the payload) to reach the gap a real deployment could still hit.
  function encodeRawFrame(header: { id: number }, payload: Uint8Array): Uint8Array {
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
  it("importing it fails tsc - compile error", () => {
    // Type-only: never executed, and never in doubt as `[import(0,0)] importing it fails tsc`'s
    // own real assertion - `tsc --noEmit` is that assertion, on the real `@ts-expect-error` above
    // this file's own imports. `TS2578: Unused '@ts-expect-error' directive` is the failure signal
    // if the import ever stopped erroring, the negative-case pattern `.claude/rules/typescript.md`
    // calls for over trusting a "should fail" claim - verified live: `import { jsonCodec } from
    // "../src"` reports `TS2724`, not the `TS2305` a bare "no exported member" guess would suggest,
    // because `JsonCodec` (the class) is close enough in spelling for tsc's own "did you mean"
    // suggestion to upgrade the diagnostic.
    expect(typeof _jsonCodecDeleted).toBe("undefined");
  });
});
