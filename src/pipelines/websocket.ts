/**
 * `WebSocketPipeline` (#201) — STUB. Real signatures throughout: the constructor, `carriedKnobs()`,
 * and every pure type-narrowing override (`transform`/`apply`/`reduce`/`local`/`queue`, each an
 * unchanged `super.X(...)` delegation, the same shape `HttpPipeline`/`ClusterHttpPipeline` use) are
 * real from here, since none of them carries any dispatch logic of its own. Only `serve()`,
 * `stageWork()` and `reduceWork()` throw - later layers fill them in.
 *
 * Each chunk of a stage dispatched over a multiplexed WebSocket connection instead of one HTTP
 * request per chunk (`HttpPipeline`) - `ClusterPipeline` reparents onto this class,
 * `ws+unix://<worker socket>` its default `connect`, so a worker sharing this process's own machine
 * pays no HTTP request-line/header parsing per chunk (#180's own finding: 60-75% of `HttpPipeline`'s
 * dispatched cost).
 */

import type { ConcurrentPipelineOptions } from "@src/pipelines/concurrent";
import { ConcurrentPipeline } from "@src/pipelines/concurrent";
import { Pipeline } from "@src/pipeline";
import type { PipelineConstructorOptions, WrappablePipeline } from "@src/pipeline";
import type { Transformer } from "@src/transformer";
import type { InternalTransformer, PipelineMode, ReduceFunction, ReduceWork } from "@src/types";

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

/** The shipped, unchanged default codec - the same JSON shape `HttpPipeline`'s own wire already
 * sends, just over `Uint8Array` bytes instead of a JSON-typed HTTP body. */
export const jsonCodec: Codec = {
  encode: (value) => new TextEncoder().encode(JSON.stringify(value)),
  decode: (bytes) => JSON.parse(new TextDecoder().decode(bytes)) as unknown,
  contentType: "application/json",
};

/**
 * The bring-your-own-socket seam every runtime adapter targets (#201) - mirrors `PipelineEmitter`'s
 * own validated-at-construction interface (`eventemitter.ts`). A DOM-shaped `WebSocket` (Deno's
 * `Deno.upgradeWebSocket()`, Cloudflare's `WebSocketPair`) satisfies this directly; Node needs
 * `toNodeWebSocketHandler` to bridge `ws`'s own `WebSocketServer`.
 */
export interface PipelineSocket {
  send(data: string | Uint8Array): void;
  onMessage(fn: (data: string | Uint8Array) => void): void;
  onClose(fn: (code: number, reason: string) => void): void;
  close(code?: number, reason?: string): void;
}

/** Construction-time knobs for `WebSocketPipeline`. */
export type WebSocketPipelineOptions = {
  /** Where to dial for a dispatched chunk - `"ws+unix:///tmp/worker.sock:/"` or `"ws://host:port"`.
   * No default: unlike `HttpPipeline`'s `url`, a `WebSocketPipeline` built standalone (not through
   * `ClusterPipeline`) always names its own target. */
  connect: string;
  /** How a chunk is encoded on the wire. Defaults to `jsonCodec`. */
  codec?: Codec;
} & ConcurrentPipelineOptions;

/** `WebSocketPipeline`'s real constructor parameter type - see `ConcurrentPipelineConstructorOptions`
 * (`pipelines/concurrent.ts`) for why the base `Pipeline` internals must be included here too. */
type WebSocketPipelineConstructorOptions = WebSocketPipelineOptions & PipelineConstructorOptions;

/**
 * Each chunk of a stage dispatched over a multiplexed WebSocket connection to another instance
 * running the SAME code (#201). One connection per `connect` target, request-id correlated -
 * planning's own spike found this beats a connection pool sized to `maxConcurrency` on every run
 * (fewer sockets costs less kernel-side bookkeeping).
 *
 * `new WebSocketPipeline([1,2,3,4,5], { connect: "ws+unix:///tmp/w.sock:/" }).transform((t) =>
 * t.map((x) => x * 2)).toArray()` → `[2,4,6,8,10]`, across two real instances.
 */
export class WebSocketPipeline<T, In = T> extends ConcurrentPipeline<T, In> {
  protected _connect: string;
  protected _codec: Codec;

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

  /**
   * Registers this chain's stages on an already-open socket - the SERVER side of the wire, the role
   * `HttpPipeline.fetch` plays for HTTP. Not yet implemented.
   */
  serve(_socket: PipelineSocket): void {
    throw new Error("WebSocketPipeline.serve() is not implemented yet (#201)");
  }

  protected override stageWork<U>(
    _transformer: Transformer<T, U, "sync" | "async">,
    _stageIndex: number,
  ): InternalTransformer<T, U> {
    throw new Error("WebSocketPipeline dispatch is not implemented yet (#201)");
  }

  protected override reduceWork<U>(
    _fn: ReduceFunction<U, T>,
    _initial: U,
    _stageIndex: number,
  ): ReduceWork<T, U> {
    throw new Error("WebSocketPipeline reduce dispatch is not implemented yet (#201)");
  }
}
