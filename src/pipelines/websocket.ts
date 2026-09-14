/**
 * `WebSocketPipeline` (#201) — each chunk of a stage dispatched over a multiplexed WebSocket
 * connection instead of one HTTP request per chunk (`HttpPipeline`). `ClusterPipeline` reparents
 * onto this class, `ws+unix:<worker socket path>:/` its default `connect`, so a worker sharing this
 * process's own machine pays no HTTP request-line/header parsing per chunk (#180's own finding:
 * 60-75% of `HttpPipeline`'s dispatched cost).
 *
 * The wire, one BINARY frame per dispatch, this class owns the framing (a 4-byte big-endian
 * header-length prefix, the JSON header, then the codec-encoded payload):
 * ```text
 * -> { id: 0, route: "/transform/0", context: { multiplier: 10 } } + codec.encode([1, 2])
 * <- { id: 0 } + codec.encode([2, 4])
 * ```
 * A failure is a separate TEXT frame, always fixed JSON regardless of `codec` - the WS opcode
 * itself the discriminator: `{ "id": 0, "error": "…" }`.
 *
 * `route` carries what a URL path carried before - `/transform/<n>`, `/reduce/<n>`,
 * `/branch/<i>/<name>/transform/<n>` - unchanged trail, new home (`routePath()`, mirroring
 * `HttpPipeline`'s own).
 *
 * One connection per `connect` target, memoized per process (`getConnection()`, below) -
 * planning's own "Connection shape" spike found this beats a pool sized to `maxConcurrency` on
 * every run (fewer sockets costs less kernel-side bookkeeping); every `stageWork()`/`reduceWork()`
 * dispatch correlates its own request/response by `id` over that one shared socket.
 *
 * A reduce stage shares the connection like any other stage, correlated by the SAME `id` across
 * every frame of its own stream: each upstream chunk is its own outgoing frame (route + context
 * repeated - simpler than tracking "have I sent this id's context yet" server-side, and cheap next
 * to a chunk's own payload), an `inputDone: true` frame (empty payload) signals no more chunks are
 * coming, and the server's own `done: true` frame (empty payload) closes the id after its trailing
 * `Reducer.final()` value, if any, has already been sent - the same `Reducer`/`foldChunk` engine
 * `HttpPipeline`'s own `runReduceStage` folds through (`src/utils/reduce.ts`). Unpriced here, named
 * so it is not mistaken for load-bearing: this reduce wire is NOT pull-driven the way the HTTP
 * `ReadableStream` wire is - a partition's own chunk frames go out as fast as `chunks` yields them,
 * so the fastest of `ConcurrentPipeline.reduce()`'s `share()`d partitions could in principle race
 * ahead of a slow socket. Not a Done-when 4 blocker at five items.
 */

import type { ConcurrentPipelineOptions } from "@src/pipelines/concurrent";
import { ConcurrentPipeline } from "@src/pipelines/concurrent";
import { Pipeline } from "@src/pipeline";
import type { PipelineConstructorOptions, WrappablePipeline } from "@src/pipeline";
import type { Transformer } from "@src/transformer";
import type {
  IContextManager,
  InternalTransformer,
  PipelineMode,
  ReduceFunction,
  ReduceWork,
  RouteVerb,
  StageRoute,
} from "@src/types";
import { Reducer, foldChunk } from "@src/utils/reduce";
import { WebSocket as WSWebSocket, WebSocketServer } from "ws";

/**
 * The payload-encoding seam (#201, folded in from the `plan-codec-seam` session) - orthogonal to
 * transport. `WebSocketPipeline`'s own knob alone: `HttpPipeline`/`ClusterHttpPipeline` keep their
 * unchanged JSON wire.
 */
export interface Codec {
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- a codec encodes ANY chunk value, one handler for every item type a chain has ever carried; narrowing would break that contract, the same reason RowErrorHandler's `item` stays unknown (types.ts)
  encode(value: unknown): Uint8Array | Promise<Uint8Array>;
  // oxlint-disable-next-line anti-slop/no-unknown-returns -- the decoded value is unknown until a caller's own stage parses it at its own boundary, the same contract Context's IContextManager.get() already discloses
  decode(bytes: Uint8Array): unknown | Promise<unknown>;
  contentType?: string;
}

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

/** The shipped, unchanged default codec - the same JSON shape `HttpPipeline`'s own wire already
 * sends, just over `Uint8Array` bytes instead of a JSON-typed HTTP body. Both directions reuse one
 * module-level `TextEncoder`/`TextDecoder` (both stateless) rather than allocating a fresh instance
 * per call, since every dispatched chunk pays this on the hot path #180 measured. */
export const jsonCodec: Codec = {
  encode: (value) => textEncoder.encode(JSON.stringify(value)),
  decode: (bytes) => JSON.parse(textDecoder.decode(bytes)) as unknown,
  contentType: "application/json",
};

/**
 * The bring-your-own-socket seam every runtime adapter targets (#201) - mirrors `PipelineEmitter`'s
 * own validated-at-construction interface (`eventemitter.ts`). A DOM-shaped `WebSocket` (Deno's
 * `Deno.upgradeWebSocket()`, Cloudflare's `WebSocketPair`) satisfies this directly; `toNodeWebSocketHandler`
 * (below) bridges `ws`'s own `WebSocketServer` for Node.
 */
export interface PipelineSocket {
  send(data: string | Uint8Array): void;
  onMessage(fn: (data: string | Uint8Array) => void): void;
  onClose(fn: (code: number, reason: string) => void): void;
  close(code?: number, reason?: string): void;
}

/** Construction-time knobs for `WebSocketPipeline`. */
export type WebSocketPipelineOptions = {
  /** Where to dial for a dispatched chunk - `"ws+unix:/tmp/worker.sock:/"` or `"ws://host:port"`.
   * `ws`'s own `ws+unix:` scheme splits its path on the FIRST `:` - everything before it is the
   * socket path, everything after (defaulting to `/`) is the URL path (verified against `ws`
   * 8.21.3's own `initAsClient`, `lib/websocket.js`) - a caller who writes `ws+unix:///path:/`
   * (an extra leading `//`, the URL-with-authority shape every other scheme here uses) dials the
   * wrong socket path (`"/path"` prefixed with an empty authority segment `ws` does not strip). No
   * default: unlike `HttpPipeline`'s `url`, a `WebSocketPipeline` built standalone (not through
   * `ClusterPipeline`) always names its own target. */
  connect: string;
  /** How a chunk is encoded on the wire. Defaults to `jsonCodec`. */
  codec?: Codec;
} & ConcurrentPipelineOptions;

/** `WebSocketPipeline`'s real constructor parameter type - see `ConcurrentPipelineConstructorOptions`
 * (`pipelines/concurrent.ts`) for why the base `Pipeline` internals must be included here too. */
type WebSocketPipelineConstructorOptions = WebSocketPipelineOptions & PipelineConstructorOptions;

/** A frame's own JSON header, both directions - `id` correlates every frame (a transform's single
 * response, or one reduce stream's many) to the request that opened it. `route` is present on every
 * OUTGOING (client -> server) frame - the server re-resolves it per frame rather than tracking "have
 * I seen this id's route yet", which is what lets a reduce stream's later chunk frames omit no
 * state at all. `inputDone`/`done` are the reduce stream's own start/stop signals; a transform
 * response never sets either. */
interface Frame {
  id: number;
  route?: string;
  // oxlint-disable-next-line anti-slop/no-unsafe-dictionary-type -- Context is a generic bag by design, unknown until a caller parses it at its own boundary, the same contract HttpPipeline's own StageRequestBody.context discloses
  context?: Record<string, unknown>;
  /** Client -> server: no more reduce chunks are coming for this `id`. */
  inputDone?: boolean;
  /** Server -> client: no more reduce emits are coming for this `id`, sent after any trailing
   * `Reducer.final()` value. */
  done?: boolean;
}

/** The one TEXT-frame shape either side sends on failure - fixed JSON regardless of `codec`. */
interface ErrorFrame {
  id: number;
  error: string;
}

/** Encodes one binary frame: a 4-byte big-endian header-length prefix, the header's own JSON UTF-8
 * bytes, then `payload` verbatim - `decodeFrame` (below) is this function's exact inverse. */
function encodeFrame(header: Frame, payload: Uint8Array): Uint8Array {
  const headerBytes = textEncoder.encode(JSON.stringify(header));
  const frame = new Uint8Array(4 + headerBytes.length + payload.length);
  new DataView(frame.buffer).setUint32(0, headerBytes.length, false);
  frame.set(headerBytes, 4);
  frame.set(payload, 4 + headerBytes.length);
  return frame;
}

/** `decodeFrame`'s own return shape - a parsed `Frame` header plus whatever payload bytes follow
 * it, possibly empty for an `inputDone`/`done` signal frame. */
interface DecodedFrame {
  header: Frame;
  payload: Uint8Array;
}

/** The exact inverse of `encodeFrame` - reads the length prefix, slices the header JSON off the
 * front, and returns whatever bytes remain as the payload. */
function decodeFrame(data: Uint8Array): DecodedFrame {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const headerLength = view.getUint32(0, false);
  const headerBytes = data.subarray(4, 4 + headerLength);
  const payload = data.subarray(4 + headerLength);
  return { header: JSON.parse(textDecoder.decode(headerBytes)) as Frame, payload };
}

/** The one TEXT frame either side sends on failure - `JSON.stringify` directly, never through
 * `codec`: an error is always plain JSON, the WS opcode (text, not binary) is what a reader
 * discriminates on. */
function encodeErrorFrame(id: number, error: string): string {
  return JSON.stringify({ id, error } satisfies ErrorFrame);
}

/** `peekFrame()`'s own return shape - a frame's `id`/`route` alone, read without decoding its
 * payload. */
export interface FramePreview {
  id: number;
  route: string | undefined;
}

/** A frame's own `id`/`route`, read WITHOUT decoding its payload - `ClusterPipeline`'s own shared
 * worker server (`cluster.ts`, #201 L3) needs only these two fields to route a frame to the right
 * registered pipeline by its `/pipeline/<i>/` prefix, before that pipeline's own `receiveFrame()`
 * decodes the same bytes again in full. */
export function peekFrame(data: Uint8Array): FramePreview {
  const { header } = decodeFrame(data);
  return { id: header.id, route: header.route };
}

/** Sends the one error shape `peekFrame()`'s own caller needs when a frame's route names no
 * registered pipeline - the same TEXT-frame contract `encodeErrorFrame` (above) already uses. */
export function sendUnknownRouteError(
  socket: PipelineSocket,
  id: number,
  route: string | undefined,
): void {
  socket.send(encodeErrorFrame(id, `unknown pipeline route ${route ?? "(missing)"}`));
}

/**
 * Reads back the route grammar `WebSocketPipeline.routePath()` builds (#201, mirroring
 * `HttpPipeline`'s own `parseRoute` in `http.ts`, kept as its own copy since the two parse different
 * strings - a URL pathname there, a bare JSON field here - even though the grammar is identical):
 * `/transform/<n>`, `/reduce/<n>`, optionally prefixed by a `/branch/<i>/<name>` trail.
 *
 * `parseRoute("/branch/1/big/transform/2")` → `{ trail: "/branch/1/big", verb: "transform", index: 2 }`.
 */
function parseRoute(route: string): StageRoute | null {
  const match = /(\/branch\/\d+\/[^/]+)?\/(transform|reduce)\/(\d+)$/.exec(route);
  if (match === null) return null;
  return { trail: match[1] ?? null, verb: match[2] as RouteVerb, index: Number(match[3]) };
}

/** Wraps a real `ws` `WebSocket` (client-dialed or server-accepted, identical shape either way) as
 * a `PipelineSocket` - the one seam `getConnection()` (client) and `toNodeWebSocketHandler()`
 * (server) both wrap through. `ws` delivers EVERY frame's payload as a `Buffer` on `"message"`,
 * text or binary alike, with `isBinary` the only discriminator (verified against `ws` 8.21.3's own
 * docs) - decoded to a `string` here for a text (error) frame, left as the `Buffer`'s own
 * `Uint8Array` view for a binary one, matching `PipelineSocket.onMessage`'s own contract. */
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

/** One caller awaiting a frame (or a stream of them) tagged with `id` - `onFrame` for a transform's
 * single response or a reduce stream's `emit`/`done` frames, `onError` for the TEXT error frame
 * either shape can receive instead. */
interface PendingRequest {
  onFrame: (header: Frame, payload: Uint8Array) => void;
  onError: (message: string) => void;
}

/** One multiplexed connection to a `connect` target - `getConnection()` memoizes exactly one of
 * these per target per process (the spike's own finding: fewer sockets beats a pool). `ready`
 * resolves once the underlying `ws` handshake completes; every dispatch awaits it before its own
 * first send, so a chunk composed before the socket is open queues behind the same promise instead
 * of racing the handshake. */
interface ClientConnection {
  socket: PipelineSocket;
  ready: Promise<void>;
  nextId: number;
  pending: Map<number, PendingRequest>;
}

/** Every open client connection, keyed by its own `connect` string - module-level, so two
 * `WebSocketPipeline` instances dialing the SAME target (two stages of one chain, or two separate
 * chains) share the one socket rather than each opening their own. */
const connections = new Map<string, ClientConnection>();

/** Dials `connect` on first use and memoizes the result; a later call for the SAME target returns
 * the identical `ClientConnection`. A connection that closes or errors rejects every request still
 * pending on it and evicts itself, so the NEXT dispatch to that target dials fresh rather than
 * reusing a dead socket forever - reconnect-mid-run semantics beyond that (replaying an in-flight
 * chunk) are `#201`'s own Settle first, not built here. */
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
    // Handled-marker only - every real caller `await`s `conn.ready` itself and sees the same
    // rejection there; this stops an unawaited construction-time reference from surfacing an
    // unhandled rejection before any dispatch ever reads it.
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
    // Only evicts THIS connection, never whatever the cache currently holds - a socket typically
    // fires both "error" and "close" for the same failure, and by the time the second one runs, a
    // concurrent dispatch may already have registered a fresh, healthy connection under the same
    // key. Deleting unconditionally would drop that live connection out of the cache for no reason.
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
 * Each chunk of a stage dispatched over a multiplexed WebSocket connection to another instance
 * running the SAME code (#201). One connection per `connect` target, request-id correlated -
 * planning's own spike found this beats a connection pool sized to `maxConcurrency` on every run
 * (fewer sockets costs less kernel-side bookkeeping).
 *
 * `new WebSocketPipeline([1,2,3,4,5], { connect: "ws+unix:/tmp/w.sock:/" }).transform((t) =>
 * t.map((x) => x * 2)).toArray()` → `[2,4,6,8,10]`, across two real instances.
 */
export class WebSocketPipeline<T, In = T> extends ConcurrentPipeline<T, In> {
  protected _connect: string;
  protected _codec: Codec;
  /** One `Reducer` per in-flight reduce `id` this instance is SERVING - `serve()`'s own state,
   * never touched by the dispatching (client) side. Built on first chunk frame for an `id`, torn
   * down once that id's `inputDone` frame arrives. */
  private readonly reduceSessions = new Map<number, Reducer<unknown, unknown>>();

  /** Wraps a chain built elsewhere, dispatching its stages over a WebSocket connection (#90's own
   * wrapping-constructor pattern, `HttpPipeline`/`ClusterPipeline`/`EventEmitterPipeline` share it). */
  constructor(pipeline: WrappablePipeline<T, In>, options: WebSocketPipelineOptions);
  constructor(options: WebSocketPipelineConstructorOptions);
  constructor(
    first: WrappablePipeline<T, In> | WebSocketPipelineConstructorOptions,
    second?: WebSocketPipelineOptions,
  ) {
    const options = Pipeline.wrapping<WebSocketPipelineConstructorOptions>(first, second);
    super(options);
    this._connect = options.connect;
    this._codec = options.codec ?? jsonCodec;
  }

  /** The target this instance dials for a dispatched chunk. */
  get connect(): string {
    return this._connect;
  }

  /**
   * Carries `connect`/`codec` into the NEXT instance a copy-on-write call builds, on top of what
   * `ConcurrentPipeline.carriedKnobs()` already carries forward (#133's pattern) - same reason,
   * two more fields.
   */
  protected override carriedKnobs(): WebSocketPipelineOptions {
    return { ...super.carriedKnobs(), connect: this._connect, codec: this._codec };
  }

  /**
   * Re-declared ONLY to narrow the static return type back to `WebSocketPipeline<U>` - the
   * inherited `ConcurrentPipeline.transform()` logic (fan-out, the knob-violation check) runs
   * completely unchanged via `super`, the same shape `HttpPipeline`/`ClusterPipeline` use.
   */
  override transform<U, M2 extends "sync" | "async">(
    builder: (t: Transformer<T, T, "async">) => Transformer<T, U, M2>,
  ): WebSocketPipeline<U, In> {
    return super.transform(builder) as unknown as WebSocketPipeline<U, In>;
  }

  override apply<U>(transformer: Transformer<T, U, "sync" | "async">): WebSocketPipeline<U, In> {
    return super.apply(transformer) as unknown as WebSocketPipeline<U, In>;
  }

  /** Re-declared ONLY to narrow the static return type - same reason as `.transform()`/`.apply()`
   * above. `ConcurrentPipeline.reduce()`'s own logic runs unchanged via `super`. */
  override reduce<U>(fn: ReduceFunction<U, T>, initial: U): WebSocketPipeline<U, In> {
    return super.reduce(fn, initial) as unknown as WebSocketPipeline<U, In>;
  }

  /** Re-declared ONLY to narrow `Pipeline.local()`'s return type (#61,
   * `~/.claude/rules/typescript.md`) - the body is an unchanged `super()` call, the same reason
   * `HttpPipeline.local()` needs none of its own logic either. */
  override local<U, M2 extends PipelineMode>(
    build: (p: Pipeline<T, "async", any>) => Pipeline<U, M2, any>,
  ): WebSocketPipeline<U, In> {
    return super.local(build) as unknown as WebSocketPipeline<U, In>;
  }

  /** Re-declared ONLY to narrow `Pipeline.queue()`'s return type (#123,
   * `~/.claude/rules/typescript.md`) - same reason as `.local()` above. */
  override queue(capacity: number): WebSocketPipeline<T, In> {
    return super.queue(capacity) as unknown as WebSocketPipeline<T, In>;
  }

  /** Every `id`'s own tail promise - `serve()` chains each new frame for a given `id` onto the
   * PRIOR one instead of dispatching every incoming message concurrently, so a reduce stream's own
   * chunk and `inputDone` frames (which race in over the wire back to back) still fold in the order
   * they were SENT rather than the order their own async work happens to settle in. A transform
   * frame needs no such ordering (each `id` is used once), but costs nothing to route through the
   * same queue. */
  private readonly frameQueues = new Map<number, Promise<void>>();

  /**
   * Registers this chain's stages on an already-open socket - the SERVER side of the wire, the role
   * `HttpPipeline.fetch` plays for HTTP. Wires `receiveFrame()` (below) to every binary message; a
   * stray TEXT frame reaching the server (only ever sent client -> server as an error, never a
   * request) is ignored, since there is no `id` on the sending side left waiting for a reply to it.
   */
  serve(socket: PipelineSocket): void {
    socket.onMessage((data) => {
      if (typeof data === "string") return;
      this.receiveFrame(socket, data);
    });
  }

  /**
   * Handles one already-decoded-once binary frame for THIS pipeline - the body `serve()`'s own
   * `onMessage` calls directly, exposed separately so a shared multi-pipeline worker server
   * (`ClusterPipeline`'s own bootstrap, `cluster.ts` #201 L3) can peek a frame's `/pipeline/<i>/`
   * prefix with `peekFrame()` (below), look up the RIGHT registered instance by index, and hand it
   * the SAME raw bytes - one socket, many pipeline definitions, exactly the role `.fetch()` plays
   * for `ClusterHttpPipeline`'s own shared worker server.
   */
  receiveFrame(socket: PipelineSocket, data: Uint8Array): void {
    const { header, payload } = decodeFrame(data);
    const prior = this.frameQueues.get(header.id) ?? Promise.resolve();
    const next = prior.then(() => this.handleParsedFrame(socket, header, payload));
    // A frame that fails is still a settled promise - the NEXT frame for this id must still run,
    // so the queue's own tail catches here rather than leaving a rejected promise every later
    // `.then()` on this id would otherwise inherit.
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
      return;
    }
    const parsed = parseRoute(header.route);
    if (parsed === null) {
      socket.send(encodeErrorFrame(header.id, `unknown route ${header.route}`));
      return;
    }
    if (parsed.verb === "reduce") {
      await this.handleReduceFrame(socket, header, parsed, payload);
      if (header.inputDone === true) this.frameQueues.delete(header.id);
      return;
    }
    await this.handleTransformFrame(socket, header, parsed, payload);
    this.frameQueues.delete(header.id);
  }

  /** Serves one `/transform/<n>` frame: decode, run the stage's own registered `ChunkTransform`
   * (`_chunkTransforms[index]`, `transformer.runnable()`'s own row-recovery included, exactly as
   * `HttpPipeline.fetch()` runs it), encode, reply on the SAME `id`. */
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
      socket.send(encodeFrame({ id: header.id }, await this._codec.encode(result)));
    } catch (error) {
      socket.send(
        encodeErrorFrame(header.id, error instanceof Error ? error.message : String(error)),
      );
    }
  }

  /** Serves one `/reduce/<n>` frame: an ordinary chunk folds through this `id`'s own `Reducer`
   * (`HttpPipeline`'s own `Reducer`/`foldChunk` engine, `src/utils/reduce.ts`, unchanged), replying
   * with whatever it emitted; `inputDone` flushes the trailing accumulator (`Reducer.final()`,
   * same "only if items were folded since the last emit" contract every reducer in the package
   * shares) and closes the session with a `done: true` frame. */
  private async handleReduceFrame(
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
    const stage = resolved.reduceStages.get(parsed.index);
    if (!stage) {
      const known = [...resolved.reduceStages.keys()].join(",") || "none";
      socket.send(
        encodeErrorFrame(
          header.id,
          `unknown reduce stage ${parsed.index}; this deployment serves ${known}`,
        ),
      );
      return;
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
        return;
      }
      const chunk = (await this._codec.decode(payload)) as unknown[];
      const emitted = await foldChunk(reducer, chunk, ctx);
      if (emitted.length > 0) {
        socket.send(encodeFrame({ id: header.id }, await this._codec.encode(emitted)));
      }
    } catch (error) {
      socket.send(
        encodeErrorFrame(header.id, error instanceof Error ? error.message : String(error)),
      );
      this.reduceSessions.delete(header.id);
    }
  }

  /** `inputDone`'s own handling, split out of `handleReduceFrame` to keep that method's own
   * `try` within this repo's `max-depth: 2` (the same reason `http.ts`'s `runReduceStage` splits
   * `flushTrailing` out of its own try/for-await). Sends the trailing `Reducer.final()` value, if
   * any was owed, then the `done: true` frame that closes this `id`'s session on both sides. */
  private async flushReduceSession(
    socket: PipelineSocket,
    id: number,
    reducer: Reducer<unknown, unknown>,
  ): Promise<void> {
    const trailing = reducer.final();
    if (trailing.length > 0) {
      socket.send(encodeFrame({ id }, await this._codec.encode(trailing)));
    }
    socket.send(encodeFrame({ id, done: true }, new Uint8Array(0)));
    this.reduceSessions.delete(id);
  }

  /** Applies an incoming frame's own `context` values onto `this._context` (the SAME instance the
   * constructor built, never a fresh one per frame - `HttpPipeline.fetch()`'s own #31 pattern) and
   * returns it. */
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
   * POSTs (over the wire, sends) the chunk to this stage's own route and waits for the SAME-`id`
   * response frame - `ConcurrentPipeline`'s own `apply()` calls this for every stage; the fan-out
   * and the knob-violation check are otherwise unchanged, inherited as-is. `transformer` itself is
   * unused, the same reason `HttpPipeline.stageWork()`'s is: a dispatching class sends a chunk plus
   * an INDEX, never a function.
   */
  protected override stageWork<U>(
    _transformer: Transformer<T, U, "sync" | "async">,
    stageIndex: number,
  ): InternalTransformer<T, U> {
    const route = this.routePath("transform", stageIndex);
    return (chunk, ctx) =>
      new Promise<U[]>((resolve, reject) => {
        // Captured ONCE, here - never re-read as `this._connect` later in this closure.
        // `ClusterPipeline`'s own round-robin (#201 L3) mutates `this._connect` on the SAME shared
        // instance between concurrent dispatches (`maxConcurrency > 1`), so re-reading it after an
        // `await` could compare THIS dispatch's own live connection against a DIFFERENT, later
        // dispatch's own target and reject a perfectly healthy connection as "closed".
        const connectTarget = this._connect;
        const conn = getConnection(connectTarget);
        const dispatch = async (): Promise<void> => {
          await conn.ready;
          const id = conn.nextId++;
          const payload = await this._codec.encode(chunk);
          // The two awaits above are the window a close/error can race through: `getConnection()`'s
          // own `evictAndFail` rejects only requests already in `pending` at the moment it runs, so
          // an entry registered AFTER that moment would otherwise never settle. Checked here, with
          // no further await before `pending.set()` below, so nothing can race between this check
          // and the registration it guards.
          if (connections.get(connectTarget) !== conn) {
            reject(
              new Error(
                `stage ${stageIndex} at ${connectTarget} failed: connection closed before dispatch`,
              ),
            );
            return;
          }
          conn.pending.set(id, {
            onFrame: (_header, responsePayload) => {
              conn.pending.delete(id);
              Promise.resolve(this._codec.decode(responsePayload)).then(
                (value) => resolve(value as U[]),
                reject,
              );
            },
            onError: (message) => {
              conn.pending.delete(id);
              reject(new Error(`stage ${stageIndex} at ${connectTarget} failed: ${message}`));
            },
          });
          conn.socket.send(encodeFrame({ id, route, context: ctx.toDict() }, payload));
        };
        dispatch().catch(reject);
      });
  }

  /**
   * Opens (or reuses) this instance's own multiplexed connection and drives one reduce stream over
   * it, correlated by ONE `id` for the whole stream - `stageWork()`'s sibling, `ConcurrentPipeline`'s
   * own default (one method above the class hierarchy) folds in-process instead. Every upstream
   * chunk is its own outgoing frame; every `emit`/`done` frame the server sends back is queued and
   * yielded in arrival order.
   */
  protected override reduceWork<U>(
    _fn: ReduceFunction<U, T>,
    _initial: U,
    stageIndex: number,
  ): ReduceWork<T, U> {
    const route = this.routePath("reduce", stageIndex);
    const self = this;

    return async function* dispatchReduce(chunks, ctx) {
      // Captured ONCE, here - same reason `stageWork()`'s own `connectTarget` is: `ClusterPipeline`'s
      // round-robin (#201 L3) can reassign `self._connect` for a LATER, concurrent partition before
      // this one's own error message reads it.
      const connectTarget = self._connect;
      const conn = getConnection(connectTarget);
      await conn.ready;
      const id = conn.nextId++;

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
          Promise.resolve(self._codec.decode(responsePayload)).then((value) => {
            emitQueue.push(value as U[]);
            wake();
          }, fail);
        },
        onError: (message) =>
          fail(new Error(`reduce stage ${stageIndex} at ${connectTarget} failed: ${message}`)),
      });

      // Fed independently of the yield loop below, so an upstream that yields slowly never blocks
      // an already-queued emit from being read - the same "start feeding before awaiting the call
      // that consumes it" ordering `.claude/rules/code.md` names for a duplex probe.
      const pump = (async (): Promise<void> => {
        for await (const chunk of chunks) {
          // The stream already failed or closed (this id's own `onError`/`done` fired) - `break`
          // runs the async-iteration protocol's own `.return()` on `chunks`, releasing this
          // partition's `share()` view rather than continuing to pull chunks a dead id can no
          // longer use away from sibling partitions still folding for real.
          if (streamDone) break;
          const payload = await self._codec.encode(chunk);
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

      // One emit at a time, or `null` once the stream is done - split out of the drain loop below
      // so that loop's own body stays within this repo's `max-depth: 2` (the same reason
      // `handleReduceFrame`'s own `inputDone` branch is `flushReduceSession`, above).
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
      }
    };
  }
}

/** `toNodeWebSocketHandler()`'s own parameter derivation - `ws`'s own `WebSocketServer.handleUpgrade`
 * signature (`Parameters<...>`), rather than an explicit `node:http`/`node:stream` import: this file
 * carries no `node:` import of its own this way, so it needs none of `.oxlintrc.json`'s per-file
 * exceptions the other three dispatching files already have. */
type HandleUpgradeParams = Parameters<InstanceType<typeof WebSocketServer>["handleUpgrade"]>;

/** The shape `toNodeWebSocketHandler()` (below) returns - one method, taking the same raw
 * request/socket/head Node's own `"upgrade"` event hands a listener. */
export interface NodeWebSocketHandler {
  upgrade(
    request: HandleUpgradeParams[0],
    socket: HandleUpgradeParams[1],
    head: HandleUpgradeParams[2],
  ): void;
}

/**
 * The one Node-only bridge every `WebSocketPipeline` server needs (#201) - Node exposes no
 * upgrade-to-WebSocket handler natively, the same gap `toNodeHandler` (`http.ts`) bridges for
 * `.fetch()`. The returned `wss` is `noServer`-mode, so it never listens itself; the caller's own
 * `"upgrade"` listener on a real `http.Server` (or the unix-socket-bound one `ClusterPipeline`'s
 * own workers run, #201 L3) calls `.upgrade()` with the raw request/socket/head Node hands it.
 *
 * Every accepted connection is wrapped once and handed to `pipeline.serve()` - one call site for
 * the `"connection"` event `#201`'s own Done-when 3 counts.
 *
 * `createServer((req, res) => { ... }).on("upgrade", (req, socket, head) =>
 * toNodeWebSocketHandler(pipeline).upgrade(req, socket, head))`.
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
