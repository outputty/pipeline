/**
 * `WebSocketPipeline` sends each chunk of a stage over one multiplexed WebSocket connection per
 * target, instead of one HTTP request per chunk.
 *
 * The wire is one binary frame per message: a 4-byte big-endian header length, the JSON header,
 * then the codec-encoded payload. A failure is a text frame of fixed JSON.
 * ```text
 * -> { id: 0, route: "/transform/0", context: { multiplier: 10 } } + codec.encode([1, 2])
 * <- { id: 0, rows: 2 } + codec.encode([2, 4])
 * <- (text) { "id": 0, "error": "…" }
 * ```
 * A reduce stream keeps one `id` for all its frames. The client ends it with `inputDone: true`,
 * and the server closes it with `done: true` after the trailing value.
 *
 * ⚠ The reduce wire is not pull-driven. A fast partition can send chunks faster than a slow
 * socket accepts them.
 */

import type { ConcurrentPipelineOptions } from "@src/pipelines/concurrent";
import { ConcurrentPipeline, parseRoute } from "@src/pipelines/concurrent";
import { Pipeline } from "@src/pipeline";
import type { PipelineConstructorOptions, WrappablePipeline } from "@src/pipeline";
import type { Transformer } from "@src/transformer";
import type {
  IContextManager,
  InternalTransformer,
  PipelineMode,
  ReduceFunction,
  ReduceWork,
  StageRoute,
} from "@src/types";
import { Reducer, foldChunk } from "@src/utils/reduce";
import { WebSocket as WSWebSocket, WebSocketServer } from "ws";
import type { IncomingMessage } from "node:http";
import type { Duplex } from "node:stream";
import type { Codec } from "@src/codec";
import { JsonCodec } from "@src/codec";
import { encodedChunk, encodeOrForward, isEmptyEncodedChunk } from "@src/utils/encoded-chunk";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

/**
 * The socket a worker serves on and a dispatch sends through. A caller adapts a runtime's own
 * WebSocket to it; `toNodeWebSocketHandler` does this for Node's `ws`.
 *
 * `pipeline.serve(socket)` → the pipeline answers every frame that arrives on `socket`.
 */
export interface PipelineSocket {
  send(data: string | Uint8Array): void;
  onMessage(fn: (data: string | Uint8Array) => void): void;
  onClose(fn: (code: number, reason: string) => void): void;
  close(code?: number, reason?: string): void;
}

/** Construction-time knobs for `WebSocketPipeline`. */
export type WebSocketPipelineOptions = {
  /** Where to dial for a dispatched chunk: `"ws+unix:/tmp/worker.sock:/"` or `"ws://host:port"`.
   *
   * ⚠ Write `ws+unix:` with no `//`. `ws+unix:///path:/` dials the wrong socket path. */
  connect: string;
  /** How a chunk is encoded on the wire. Defaults to `new JsonCodec()`. */
  codec?: Codec;
} & ConcurrentPipelineOptions;

type WebSocketPipelineConstructorOptions = WebSocketPipelineOptions & PipelineConstructorOptions;

/** A frame's JSON header. `id` ties every frame to the request that opened it; every client frame
 * repeats `route`, so the server keeps no per-id route state. */
interface Frame {
  id: number;
  route?: string;
  // oxlint-disable-next-line anti-slop/no-unsafe-dictionary-type -- Context is a generic bag by design, unknown until a caller parses it at its own boundary, the same contract HttpPipeline's own StageRequestBody.context discloses
  context?: Record<string, unknown>;
  /** Client -> server: no more reduce chunks are coming for this `id`. */
  inputDone?: boolean;
  /** Server -> client: no more reduce emits are coming for this `id`. */
  done?: boolean;
  /** Server -> client, on a reply with a payload: its row count, so the client keeps the payload
   * encoded. ⚠ A reply without it fails; reading it as zero rows drops real data. */
  rows?: number;
}

function noRowCountError(label: string): Error {
  return new Error(`${label}: reply carried no row count`);
}

interface ErrorFrame {
  id: number;
  error: string;
}

function encodeFrame(header: Frame, payload: Uint8Array): Uint8Array {
  const headerBytes = textEncoder.encode(JSON.stringify(header));
  const frame = new Uint8Array(4 + headerBytes.length + payload.length);
  new DataView(frame.buffer).setUint32(0, headerBytes.length, false);
  frame.set(headerBytes, 4);
  frame.set(payload, 4 + headerBytes.length);
  return frame;
}

function decodeFrame(data: Uint8Array) {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const headerLength = view.getUint32(0, false);
  const headerBytes = data.subarray(4, 4 + headerLength);
  const payload = data.subarray(4 + headerLength);
  return { header: JSON.parse(textDecoder.decode(headerBytes)) as Frame, payload };
}

/** ⚠ Never through `codec`: a reader tells an error from a reply by the text opcode alone. */
function encodeErrorFrame(id: number, error: string): string {
  return JSON.stringify({ id, error } satisfies ErrorFrame);
}

/** Reads a frame's `id` and `route` without decoding its payload, so a shared worker server can
 * pick the pipeline that owns the frame.
 *
 * `peekFrame(frame)` → `{ id: 0, route: "/pipeline/1/transform/0" }`. */
export function peekFrame(data: Uint8Array) {
  const { header } = decodeFrame(data);
  return { id: header.id, route: header.route };
}

/** Answers a frame whose route names no registered pipeline with an error frame.
 *
 * `sendUnknownRouteError(socket, 0, "/pipeline/9/transform/0")` → request 0 fails with a message
 * ending `unknown pipeline route /pipeline/9/transform/0`. */
export function sendUnknownRouteError(
  socket: PipelineSocket,
  id: number,
  route: string | undefined,
): void {
  socket.send(encodeErrorFrame(id, `unknown pipeline route ${route ?? "(missing)"}`));
}

/** ⚠ `ws` hands text frames over as a `Buffer` too; only `isBinary` tells them apart. */
function wrapWebSocket(ws: WSWebSocket): PipelineSocket {
  return {
    send: (data) => ws.send(data),
    onMessage: (fn) => {
      ws.on("message", (data: Buffer, isBinary: boolean) => {
        fn(
          isBinary
            ? new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
            : data.toString("utf-8"),
        );
      });
    },
    onClose: (fn) => {
      ws.on("close", (code: number, reason: Buffer) => fn(code, reason.toString("utf-8")));
    },
    close: (code, reason) => ws.close(code, reason),
  };
}

interface PendingRequest {
  onFrame: (header: Frame, payload: Uint8Array) => void;
  onError: (message: string) => void;
}

interface ClientConnection {
  socket: PipelineSocket;
  /** ⚠ Every dispatch awaits this before its first send; sending earlier races the handshake. */
  ready: Promise<void>;
  nextId: number;
  pending: Map<number, PendingRequest>;
}

/** One connection per `connect` target per process, shared by every `WebSocketPipeline`. */
const connections = new Map<string, ClientConnection>();

/** ⚠ A connection that closes or errors fails its pending requests and evicts itself, so the
 * next dispatch dials fresh. In-flight chunks are not replayed. */
function getConnection(connect: string): ClientConnection {
  const existing = connections.get(connect);
  if (existing) return existing;

  const ws = new WSWebSocket(connect);
  const socket = wrapWebSocket(ws);
  const pending = new Map<number, PendingRequest>();
  const conn: ClientConnection = {
    socket,
    pending,
    nextId: 0,
    ready: new Promise<void>((resolve, reject) => {
      ws.once("open", () => resolve());
      ws.once("error", (error: Error) => reject(error));
    }),
  };
  conn.ready.catch(() => {
    // ⚠ Marks the rejection handled; every caller awaits `ready` and sees it there.
  });

  socket.onMessage((data) => {
    if (typeof data === "string") {
      const frame = JSON.parse(data) as ErrorFrame;
      pending.get(frame.id)?.onError(frame.error);
      return;
    }
    const { header, payload } = decodeFrame(data);
    pending.get(header.id)?.onFrame(header, payload);
  });

  const evictAndFail = (message: string): void => {
    // ⚠ Evicts only THIS connection. "error" and "close" both fire, and by the second a fresh
    // connection may hold the key.
    if (connections.get(connect) === conn) connections.delete(connect);
    for (const request of pending.values()) request.onError(message);
    pending.clear();
  };
  socket.onClose((code, reason) =>
    evictAndFail(`connection to ${connect} closed (${code} ${reason})`),
  );
  ws.on("error", (error: Error) => evictAndFail(error.message));

  connections.set(connect, conn);
  return conn;
}

/**
 * Runs each stage of a chain on another instance of the same code, over one shared WebSocket
 * connection per `connect` target. The worker serves through `toNodeWebSocketHandler` or `serve()`.
 *
 * ```ts
 * const doubled = new Pipeline<number>().transform((t) => t.map((x) => x * 2));
 * const handler = toNodeWebSocketHandler(new WebSocketPipeline(doubled, { connect: "" }));
 * createServer()
 *   .on("upgrade", (req, socket, head) => handler.upgrade(req, socket, head))
 *   .listen(path);
 * const caller = new WebSocketPipeline(doubled, { connect: `ws+unix:${path}:/` });
 * await caller([1, 2, 3]).toArray(); // [2, 4, 6]
 * ```
 */
export class WebSocketPipeline<T, In = T> extends ConcurrentPipeline<T, In> {
  protected _connect: string;
  protected _codec: Codec;
  /** Server side: one `Reducer` per reduce `id` being served, removed when its stream ends. */
  private readonly reduceSessions = new Map<number, Reducer<unknown, unknown>>();

  /** Wraps a chain built elsewhere. The worker and the caller share one chain definition. */
  constructor(pipeline: WrappablePipeline<T, In>, options: WebSocketPipelineOptions);
  constructor(options: WebSocketPipelineConstructorOptions);
  constructor(
    first: WrappablePipeline<T, In> | WebSocketPipelineConstructorOptions,
    second?: WebSocketPipelineOptions,
  ) {
    const options = Pipeline.wrapping<WebSocketPipelineConstructorOptions>(first, second);
    super(options);
    this._connect = options.connect;
    this._codec = options.codec ?? new JsonCodec();
  }

  /** The target this instance dials for a dispatched chunk. */
  get connect(): string {
    return this._connect;
  }

  /** Carries `connect` and `codec` into the next copy-on-write instance. */
  protected override carriedKnobs(): WebSocketPipelineOptions {
    return { ...super.carriedKnobs(), connect: this._connect, codec: this._codec };
  }

  /** `true`: a reply from this class's workers stays encoded until a site reads its items. */
  protected override mayCarryEncodedChunks(): boolean {
    return true;
  }

  override transform<U, M2 extends "sync" | "async">(
    builder: (t: Transformer<T, T, "async">) => Transformer<T, U, M2>,
  ): WebSocketPipeline<U, In> {
    return super.transform(builder) as unknown as WebSocketPipeline<U, In>;
  }

  override apply<U>(transformer: Transformer<T, U, "sync" | "async">): WebSocketPipeline<U, In> {
    return super.apply(transformer) as unknown as WebSocketPipeline<U, In>;
  }

  override reduce<U>(fn: ReduceFunction<U, T>, initial: U): WebSocketPipeline<U, In> {
    return super.reduce(fn, initial) as unknown as WebSocketPipeline<U, In>;
  }

  override local<U, M2 extends PipelineMode>(
    build: (p: Pipeline<T, "async", any>) => Pipeline<U, M2, any>,
  ): WebSocketPipeline<U, In> {
    return super.local(build) as unknown as WebSocketPipeline<U, In>;
  }

  override queue(capacity: number): WebSocketPipeline<T, In> {
    return super.queue(capacity) as unknown as WebSocketPipeline<T, In>;
  }

  /** ⚠ Frames are queued per correlation id, never handled concurrently. A chunk frame and its
   * `inputDone` frame otherwise settle out of order: `[1,2,3,4,5]` summed to `[]`, not `[15]`. */
  private readonly frameQueues = new Map<number, Promise<void>>();

  /**
   * The worker side: answers every binary frame that arrives on `socket` with this chain's stages.
   * Text frames are ignored.
   *
   * `worker.serve(socket)` → a `/transform/0` frame carrying `[1, 2]` gets back `[2, 4]` for a
   * `.map((x) => x * 2)` stage 0.
   */
  serve(socket: PipelineSocket): void {
    socket.onMessage((data) => {
      if (typeof data === "string") return;
      this.receiveFrame(socket, data);
    });
  }

  /**
   * Answers one binary frame and replies on `socket`. A server shared by several pipelines calls
   * it after `peekFrame()` picks the owner.
   *
   * `pipeline.receiveFrame(socket, frame)` → the reply goes out on `socket` under the frame's `id`.
   */
  receiveFrame(socket: PipelineSocket, data: Uint8Array): void {
    const { header, payload } = decodeFrame(data);
    const prior = this.frameQueues.get(header.id) ?? Promise.resolve();
    const next = prior.then(() => this.handleParsedFrame(socket, header, payload));
    // ⚠ The tail catches, or one failed frame rejects every later frame for this id.
    this.frameQueues.set(
      header.id,
      next.catch(() => {}),
    );
  }

  private async handleParsedFrame(
    socket: PipelineSocket,
    header: Frame,
    payload: Uint8Array,
  ): Promise<void> {
    if (header.route === undefined) {
      socket.send(encodeErrorFrame(header.id, "request frame is missing a 'route'"));
      this.frameQueues.delete(header.id);
      return;
    }
    const parsed = parseRoute(header.route);
    if (parsed === null) {
      socket.send(encodeErrorFrame(header.id, `unknown route ${header.route}`));
      this.frameQueues.delete(header.id);
      return;
    }
    if (parsed.verb === "reduce") {
      // ⚠ Cleared on failure too, not only on `inputDone`, or a failed chunk leaks its entry.
      const ended = await this.handleReduceFrame(socket, header, parsed, payload);
      if (ended) this.frameQueues.delete(header.id);
      return;
    }
    await this.handleTransformFrame(socket, header, parsed, payload);
    this.frameQueues.delete(header.id);
  }

  private async handleTransformFrame(
    socket: PipelineSocket,
    header: Frame,
    parsed: StageRoute,
    payload: Uint8Array,
  ): Promise<void> {
    const resolved = this.resolveRegistries(parsed.trail);
    if (resolved === null) {
      socket.send(encodeErrorFrame(header.id, `unknown branch route ${header.route}`));
      return;
    }
    const { chunkTransforms } = resolved;
    const maxIndex = chunkTransforms.length - 1;
    if (parsed.index > maxIndex) {
      socket.send(
        encodeErrorFrame(
          header.id,
          `unknown stage ${parsed.index}; this deployment serves 0..${maxIndex}`,
        ),
      );
      return;
    }
    try {
      const ctx = this.applyContext(header.context);
      const chunk = (await this._codec.decode(payload)) as unknown[];
      const result = await chunkTransforms[parsed.index](chunk, ctx);
      socket.send(
        encodeFrame({ id: header.id, rows: result.length }, await this._codec.encode(result)),
      );
    } catch (error) {
      socket.send(
        encodeErrorFrame(header.id, error instanceof Error ? error.message : String(error)),
      );
    }
  }

  /** Returns whether this id's session ended, by `inputDone` or by failure. */
  private async handleReduceFrame(
    socket: PipelineSocket,
    header: Frame,
    parsed: StageRoute,
    payload: Uint8Array,
  ): Promise<boolean> {
    const resolved = this.resolveRegistries(parsed.trail);
    if (resolved === null) {
      socket.send(encodeErrorFrame(header.id, `unknown branch route ${header.route}`));
      return true;
    }
    const stage = resolved.reduceStages.get(parsed.index);
    if (!stage) {
      const known = [...resolved.reduceStages.keys()].join(",") || "none";
      socket.send(
        encodeErrorFrame(
          header.id,
          `unknown reduce stage ${parsed.index}; this deployment serves ${known}`,
        ),
      );
      return true;
    }

    let reducer = this.reduceSessions.get(header.id);
    if (!reducer) {
      reducer = new Reducer(stage.fn, stage.initial);
      this.reduceSessions.set(header.id, reducer);
    }
    const ctx = this.applyContext(header.context);

    try {
      if (header.inputDone === true) {
        await this.flushReduceSession(socket, header.id, reducer);
        return true;
      }
      const chunk = (await this._codec.decode(payload)) as unknown[];
      const emitted = await foldChunk(reducer, chunk, ctx);
      if (emitted.length > 0) {
        socket.send(
          encodeFrame({ id: header.id, rows: emitted.length }, await this._codec.encode(emitted)),
        );
      }
      return false;
    } catch (error) {
      socket.send(
        encodeErrorFrame(header.id, error instanceof Error ? error.message : String(error)),
      );
      this.reduceSessions.delete(header.id);
      return true;
    }
  }

  /** ⚠ A session that received no chunk sends no value. The seed for an empty stream comes from
   * `ConcurrentPipeline.reduce`, once per stage; sending it here repeats it once per partition. */
  private async flushReduceSession(
    socket: PipelineSocket,
    id: number,
    reducer: Reducer<unknown, unknown>,
  ): Promise<void> {
    const trailing = reducer.final();
    if (trailing.length > 0) {
      socket.send(encodeFrame({ id, rows: trailing.length }, await this._codec.encode(trailing)));
    }
    socket.send(encodeFrame({ id, done: true }, new Uint8Array(0)));
    this.reduceSessions.delete(id);
  }

  /** ⚠ Reuses the constructor's manager, never a fresh one per frame, so a `contextFactory` runs
   * once per process. */
  private applyContext(
    // oxlint-disable-next-line anti-slop/no-unsafe-dictionary-type -- Context is a generic bag by design, unknown until a caller parses it at its own boundary, the same contract HttpPipeline's own StageRequestBody.context discloses
    context: Record<string, unknown> | undefined,
  ): IContextManager {
    const ctx = this._context;
    for (const [key, value] of Object.entries(context ?? {})) {
      ctx.set(key, value);
    }
    return ctx;
  }

  /**
   * The target one dispatch uses, plus a `release` to call once it settles. `ClusterPipeline`
   * overrides it to round-robin across its workers.
   *
   * ⚠ Returned per call, never written to a field: concurrent partitions would race on one field
   * and all dial the last worker picked.
   */
  protected resolveConnect(): ResolvedConnect | Promise<ResolvedConnect> {
    return { connect: this._connect, release: () => {} };
  }

  /** Sends each chunk to the worker's `/transform/<n>` and returns its reply, still encoded. The
   * worker runs its own copy of the stage, so `transformer` is unused. */
  protected override stageWork<U>(
    _transformer: Transformer<T, U, "sync" | "async">,
    stageIndex: number,
  ): InternalTransformer<T, U> {
    const route = this.routePath("transform", stageIndex);
    return (chunk, ctx) =>
      new Promise<U[]>((resolve, reject) => {
        const dispatch = async (): Promise<void> => {
          const { connect: connectTarget, release } = await this.resolveConnect();
          try {
            const conn = getConnection(connectTarget);
            await conn.ready;
            const id = conn.nextId++;
            const payload = await encodeOrForward(chunk, this._codec);
            // ⚠ `pending.set` must follow this eviction check with no await between. An entry
            // registered after eviction is never settled.
            if (connections.get(connectTarget) !== conn) {
              throw new Error(
                `stage ${stageIndex} at ${connectTarget} failed: connection closed before dispatch`,
              );
            }
            try {
              conn.pending.set(id, {
                onFrame: (header, responsePayload) => {
                  conn.pending.delete(id);
                  release();
                  // ⚠ No `rows` fails: an encoded chunk read as zero rows is skipped downstream.
                  if (header.rows === undefined) {
                    reject(noRowCountError(`stage ${stageIndex} at ${connectTarget}`));
                    return;
                  }
                  resolve(
                    encodedChunk(responsePayload, header.rows, this._codec) as unknown as U[],
                  );
                },
                onError: (message) => {
                  conn.pending.delete(id);
                  release();
                  reject(new Error(`stage ${stageIndex} at ${connectTarget} failed: ${message}`));
                },
              });
              conn.socket.send(encodeFrame({ id, route, context: ctx.toDict() }, payload));
            } catch (sendError) {
              // ⚠ A DOM `WebSocket.send()` throws on a closed socket. The entry is removed, since
              // no reply will ever settle it.
              conn.pending.delete(id);
              throw sendError;
            }
          } catch (error) {
            release();
            throw error;
          }
        };
        dispatch().catch(reject);
      });
  }

  /** Streams the whole reduce stage to the worker's `/reduce/<n>` under one `id`, yielding each
   * emit as it arrives. */
  protected override reduceWork<U>(
    _fn: ReduceFunction<U, T>,
    _initial: U,
    stageIndex: number,
  ): ReduceWork<T, U> {
    const route = this.routePath("reduce", stageIndex);
    const self = this;

    return async function* dispatchReduce(chunks, ctx) {
      const { connect: connectTarget, release } = await self.resolveConnect();
      let conn: ClientConnection;
      let id: number;
      try {
        conn = getConnection(connectTarget);
        await conn.ready;
        id = conn.nextId++;
        // ⚠ `pending.set` must follow this eviction check with no await between. A session opened
        // after eviction waits forever.
        if (connections.get(connectTarget) !== conn) {
          throw new Error(
            `reduce stage ${stageIndex} at ${connectTarget} failed: connection closed before dispatch`,
          );
        }
      } catch (error) {
        // ⚠ Must release here, or a cluster's in-flight count never reaches zero and its workers
        // are never killed.
        release();
        throw error;
      }

      const emitQueue: U[][] = [];
      let waiter: (() => void) | null = null;
      let streamDone = false;
      let streamError: Error | null = null;
      const wake = (): void => {
        const settle = waiter;
        waiter = null;
        settle?.();
      };
      const fail = (error: Error): void => {
        streamError = error;
        streamDone = true;
        wake();
      };

      conn.pending.set(id, {
        onFrame: (header, responsePayload) => {
          if (header.done === true) {
            streamDone = true;
            wake();
            return;
          }
          // ⚠ No `rows` fails: an encoded chunk read as zero rows is skipped downstream.
          if (header.rows === undefined) {
            fail(noRowCountError(`reduce stage ${stageIndex} at ${connectTarget}`));
            return;
          }
          emitQueue.push(encodedChunk(responsePayload, header.rows, self._codec) as unknown as U[]);
          wake();
        },
        onError: (message) =>
          fail(new Error(`reduce stage ${stageIndex} at ${connectTarget} failed: ${message}`)),
      });

      // ⚠ Fed apart from the yield loop, so a slow upstream never holds back a queued emit.
      const pump = (async (): Promise<void> => {
        for await (const chunk of chunks) {
          // ⚠ `break` releases this partition's view of the shared stream once the id is dead.
          if (streamDone) break;
          // An emptied encoded chunk has nothing to fold.
          if (isEmptyEncodedChunk(chunk)) continue;
          const payload = await encodeOrForward(chunk, self._codec);
          if (streamDone) break;
          conn.socket.send(encodeFrame({ id, route, context: ctx.toDict() }, payload));
        }
        if (!streamDone) {
          conn.socket.send(encodeFrame({ id, route, inputDone: true }, new Uint8Array(0)));
        }
        // oxlint-disable-next-line anti-slop/no-unknown-parameters -- a rejected pump can carry anything JS can throw, the same catch-boundary contract toNodeHandler's own bridge (http.ts) already discloses
      })().catch((error: unknown) =>
        fail(error instanceof Error ? error : new Error(String(error))),
      );

      const nextEmit = async (): Promise<U[] | null> => {
        while (emitQueue.length === 0 && !streamDone) {
          await new Promise<void>((resolve) => {
            waiter = resolve;
          });
        }
        return emitQueue.length > 0 ? emitQueue.shift()! : null;
      };

      try {
        for (let emitted = await nextEmit(); emitted !== null; emitted = await nextEmit()) {
          yield emitted;
        }
        if (streamError) throw streamError;
      } finally {
        conn.pending.delete(id);
        await pump;
        release();
      }
    };
  }
}

/** The target one dispatch dials, plus a `release` the dispatch calls once it settles.
 *
 * `resolveConnect()` → `{ connect: "ws+unix:/tmp/w.sock:/", release }`. */
export interface ResolvedConnect {
  connect: string;
  release: () => void;
}

/** Accepts a Node `"upgrade"` event's request, socket and head as a pipeline connection.
 *
 * ⚠ Typed with `node:` types, never `ws`'s: a `ws` type in the published `.d.ts` makes every
 * consumer install `@types/ws`.
 *
 * `createServer().on("upgrade", (req, socket, head) => handler.upgrade(req, socket, head))` → each
 * upgrade becomes a served connection. */
export interface NodeWebSocketHandler {
  upgrade(request: IncomingMessage, socket: Duplex, head: Buffer): void;
}

/**
 * Serves a `WebSocketPipeline` from a Node `http.Server`: each accepted upgrade is handed to
 * `pipeline.serve()`. The handler never listens itself; the caller's server does.
 *
 * `createServer().on("upgrade", (req, socket, head) => handler.upgrade(req, socket, head))`, with
 * `handler = toNodeWebSocketHandler(pipeline)` → a server whose WebSocket clients reach
 * `pipeline`'s stages.
 */
export function toNodeWebSocketHandler(pipeline: {
  serve(socket: PipelineSocket): void;
}): NodeWebSocketHandler {
  const wss = new WebSocketServer({ noServer: true });
  wss.on("connection", (ws: WSWebSocket) => {
    pipeline.serve(wrapWebSocket(ws));
  });
  return {
    upgrade(request, socket, head) {
      wss.handleUpgrade(request, socket, head, (ws, req) => {
        wss.emit("connection", ws, req);
      });
    },
  };
}
