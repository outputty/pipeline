/**
 * Core type definitions for @outputty/pipeline.
 */

import type { MaybeAsyncChunks } from "./utils/drain";

/**
 * Items per chunk when a chain never calls `.buffer(size)`.
 */
export const DEFAULT_CHUNK_SIZE = 1000;

/**
 * Whether a chain runs synchronously, and so whether its terminal ops return a value or a
 * `Promise`. `"unset"` is the ordinary state of a composed chain: an async callback or an async
 * input can still decide it.
 *
 * `new Pipeline<number>()` is `"unset"`; calling it with an array keeps that, an `AsyncIterable`
 * widens it, and a single `Promise`-returning callback anywhere in the chain makes it `"async"`.
 */
export type PipelineMode = "unset" | "sync" | "async";

/**
 * What a class does to an input's shape: `"shape"` keeps it (an array runs `"sync"`), `"async"`
 * forces async. A runtime value only, read by `sourcePolicy()`.
 */
export type SourcePolicy = "shape" | "async";

/**
 * The Mode a stage produces from the chain's Mode `M` and the stage's Mode `S`. One async half makes
 * everything after it async; an `"unset"` stage leaves a `"sync"` chain undecided.
 *
 * ⚠ `M` is tested first so a class pinning `M` to `"async"` reduces while `S` is still abstract.
 * Testing `S` first gives the same concrete answers but stays deferred in a generic scope. Do not
 * reorder.
 *
 * `JoinMode<"sync", "sync">` → `"sync"`. `JoinMode<"async", "sync">` → `"async"`.
 * `JoinMode<"unset", "sync">` → `"unset"`. `JoinMode<"unset", "async">` → `"async"`.
 * `JoinMode<"sync", "unset">` → `"unset"`. `JoinMode<"async", "unset">` → `"async"`.
 */
export type JoinMode<M extends PipelineMode, S extends PipelineMode> = M extends "async"
  ? "async"
  : S extends "async"
    ? "async"
    : M extends "sync"
      ? S extends "unset"
        ? "unset"
        : S
      : "unset";

/**
 * The Mode the seed `Transformer` inside `.transform()` starts at, for a chain in Mode `M`. An
 * undecided chain starts synchronous.
 *
 * ⚠ Not the intersection `M & ("sync" | "async")`: that is `never` for `"unset"` and types every
 * terminal `never`.
 *
 * `SeedMode<"unset">` → `"sync"`. `SeedMode<"sync">` → `"sync"`. `SeedMode<"async">` → `"async"`.
 */
export type SeedMode<M extends PipelineMode> = M extends "async" ? "async" : "sync";

/**
 * A pipeline callback: maps `item` to `T` (or `Promise<T>`), with an optional shared `ctx`.
 *
 * ⚠ One signature, not a union of `(item)` / `(item, ctx)` arms: a union makes an un-annotated
 * `.map((o) => …)` callback's `o` an implicit `any`.
 *
 * `(o) => o.total` and `(o, ctx) => ctx.get("k")` both satisfy `PipelineFunction<Order, number>`.
 */
export type PipelineFunction<Out, T> = (item: Out, ctx: IContextManager) => T | Promise<T>;

/**
 * A reduce callback: folds `item` into `acc`, with an optional shared `ctx`, and can push a value
 * downstream mid-fold through `emit`. `Transformer.reduce` folds one chunk; `Pipeline.reduce` folds
 * every chunk.
 *
 * ⚠ `fn` always receives all four arguments, so `(acc, x, emit) => …` gets `ctx` in `emit`'s slot
 * and throws "emit is not a function". Write `(acc, x, _ctx, emit)`.
 *
 * `(acc, x) => acc + x` and `(acc, x, ctx, emit) => { acc += x; if (acc >= 6) { emit(acc); return 0;
 * } return acc; }` both satisfy it.
 */
export type ReduceFunction<U, Out> = (
  acc: U,
  item: Out,
  ctx: IContextManager,
  emit: (value: U) => void,
) => U | Promise<U>;

/**
 * The value a `RowErrorHandler` returns to remove its row from the output. A symbol, so `undefined`
 * stays an ordinary value a handler may return.
 *
 * `.onError(() => DROP).map(parseStrict)` over `["a", "3"]` → `[3]`.
 */
export const DROP: unique symbol = Symbol("DROP");

/**
 * The row handler `Transformer.onError(fn)` registers; may be async. `item` is `unknown` because one
 * handler covers every element-wise link, each at its own item type.
 *
 * Returning a value puts it in the row's place, and returning `DROP` removes the row. Throwing fails
 * the whole chunk, which reaches `PipelineErrorHandler`.
 *
 * `(item, error, ctx) => (error.message.includes("Invalid") ? DROP : -1)` recovers a bad row to
 * `-1` and drops anything else that fails.
 */
// oxlint-disable-next-line anti-slop/no-unknown-parameters, anti-slop/no-unknown-returns -- one handler covers every element-wise link in a chain at a different item type each time (Transformer.pipe() carries it forward unaffected by Out -> U); narrowing would break that contract
export type RowErrorHandler = (item: unknown, error: Error, ctx: IContextManager) => unknown;

/**
 * The run handler `Pipeline.onError(fn)` registers. Returning drops the failing chunk and the run
 * continues; throwing stops the run with that error.
 *
 * ⚠ Position-dependent, unlike `RowErrorHandler`: it catches only stages added after it.
 *
 * `(error, ctx) => console.warn(error.message)` logs and continues; `(error) => { throw error; }`
 * makes every chunk failure fatal, same as no handler at all.
 */
export type PipelineErrorHandler = (error: Error, ctx: IContextManager) => void;

/**
 * The row handler a composed chain's links read while they run. `Transformer.runnable()` builds it.
 *
 * `{ rowHandler: () => DROP }` → every link of that run drops a failing row.
 */
export interface RunScope {
  rowHandler?: RowErrorHandler;
}

/**
 * One chunk-transform step: a chunk in, the next chunk out, plain or as a `Promise`. `run` carries
 * the row handler; a standalone caller omits it.
 *
 * `(chunk) => chunk.map((x) => x * 2)` over `[1, 2, 3]` → `[2, 4, 6]`.
 */
export type InternalTransformer<In, Out> = (
  chunk: In[],
  ctx: IContextManager,
  run?: RunScope,
) => Out[] | Promise<Out[]>;

/**
 * Breaks an async iterable into chunks.
 */
export type ChunkerFunction<T> = (data: AsyncIterable<T>) => AsyncGenerator<T[]>;

/**
 * `.buffer(fn)`'s callback: decides the chunk boundary per item. `emit()` closes the pending chunk.
 * Returning a value appends it to the pending chunk; returning `DROP` skips the item.
 *
 * ⚠ `.buffer(size)` cuts by count and does not run through this callback.
 *
 * `(item, ctx, emit) => (item.ts - windowStart >= FIVE_MINUTES ? (emit(), windowStart = item.ts, item)
 * : item)` closes a chunk each time an item's timestamp crosses a five-minute window.
 */
export type BufferFunction<T> = (
  item: T,
  ctx: IContextManager,
  emit: () => void,
) => T | typeof DROP | Promise<T | typeof DROP>;

/**
 * Context manager interface for sharing state across pipeline operations.
 */
export interface IContextManager {
  /**
   * Get a value by key. Returns undefined if not present.
   */
  // oxlint-disable-next-line anti-slop/no-unknown-returns -- a generic bag by design, unknown until a caller parses it at its own boundary (see .oxlintrc.json)
  get(key: string): unknown;

  /**
   * Set a value by key.
   */
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- a generic bag by design, unknown until a caller parses it at its own boundary (see .oxlintrc.json)
  set(key: string, value: unknown): void;

  /**
   * Get a value by key with a default fallback.
   */
  getOrDefault<T>(key: string, defaultValue: T): T;

  /**
   * Convert context to a plain object (snapshot).
   */
  // oxlint-disable-next-line anti-slop/no-unsafe-dictionary-type -- a generic bag by design, unknown until a caller parses it at its own boundary (see .oxlintrc.json)
  toDict(): Record<string, unknown>;
}

/**
 * Options for `new Transformer()`. A caller passes `transform`; `.onError(fn)` sets `rowHandler`.
 *
 * `{ transform: (chunk) => chunk.map((x) => x * 2) }` → a transformer that doubles each item.
 */
export interface TransformerOptions<In, Out> {
  /**
   * The chunk transform this transformer starts from.
   */
  transform?: InternalTransformer<In, Out>;
  /** The row handler `.onError(fn)` installs. */
  rowHandler?: RowErrorHandler;
}

/**
 * A chain's registered stages, by index: the per-chunk transforms and the reduce stages. A
 * dispatching class's serving side looks a stage up here.
 *
 * `{ chunkTransforms: [stage0], reduceStages: new Map() }` → the table for
 * `.transform((t) => t.map(f).filter(g))`, which is one stage.
 */
export interface StageRegistries {
  chunkTransforms: ChunkTransform[];
  reduceStages: Map<number, ReduceStage>;
}

/**
 * One registered per-chunk stage: a chunk in, the next chunk out, with the shared context.
 *
 * `(chunk, ctx) => chunk.map((x) => x * 2)` over `[1, 2, 3]` → `[2, 4, 6]`.
 */
export type ChunkTransform = (
  chunk: unknown[],
  ctx: IContextManager,
) => unknown[] | Promise<unknown[]>;

/** One registered reduce stage: its fold and its seed. Untyped because one registry holds reduce
 * stages of every item type the chain carries.
 *
 * `.reduce((acc, x) => acc + x, 0)` → `{ fn: (acc, x) => acc + x, initial: 0 }`. */
export interface ReduceStage<U = unknown, T = unknown> {
  fn: ReduceFunction<U, T>;
  initial: U;
}

/**
 * Where a dispatching class runs one partition of a reduce stage: a chunk stream in, the fold's
 * values out. `ConcurrentPipeline.reduce()` calls it once per partition.
 *
 * `(chunks, ctx) => …` over one partition's chunks → that partition's folded values.
 */
export type ReduceWork<T, U> = (
  chunks: AsyncIterable<T[]>,
  ctx: IContextManager,
) => AsyncGenerator<U[]>;

/** The verb a dispatched route names: `/transform/<n>` for a per-chunk stage, `/reduce/<n>` for a
 * fold.
 *
 * `"transform"` → the verb in `/transform/0`; `"reduce"` → the verb in `/reduce/0`. */
export type RouteVerb = "transform" | "reduce";

/** A parsed dispatch route. `routePath()` builds its string form.
 *
 * `routePath("transform", 0)` → `"/transform/0"`; parsing it back →
 * `{ trail: null, verb: "transform", index: 0 }`. */
export interface StageRoute {
  trail: string | null;
  verb: RouteVerb;
  index: number;
}

/** A served route's stage, or the message a transport answers an unknown one with.
 *
 * A route naming stage 9 of a two-stage chain →
 * `{ ok: false, error: "unknown stage 9; this deployment serves 0..1" }`. */
export type StageLookup<S> = { ok: true; stage: S } | { ok: false; error: string };

/** A value tagged with the id of the partition or source that produced it, so a `Promise.race`
 * over several can tell which one settled. `fanOutUnordered` and `mergeUnordered` use it.
 *
 * `{ id: 2, result: [4, 5, 6] }` → partition 2's chunk. */
export interface Tagged<R> {
  id: number;
  result: R;
}

/**
 * The views a terminal op or `.branch()` drains a bound chain through. `syncChunks` is `null` for
 * an async run; `chunks` builds the async stream on demand.
 *
 * `new Pipeline<number>().drainable([1, 2, 3])` → `syncChunks` yields `[[1, 2, 3]]`, and `context`
 * is this run's manager.
 */
export interface Drainable<T> {
  syncChunks: MaybeAsyncChunks<T> | null;
  chunks: () => AsyncIterable<T[]>;
  context: IContextManager;
}
