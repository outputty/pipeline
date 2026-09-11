import type { MaybeAsyncChunks } from "./utils/chunk";

/**
 * The chunk boundary `.buffer()` applies when it is never called.
 */
export const DEFAULT_CHUNK_SIZE = 1000;

/**
 * Whether a chain runs synchronously, and therefore whether its terminal ops return a value or a
 * `Promise`. `"unset"` is the ordinary state of a composed chain: nothing about it is async yet,
 * and either an async callback or an async input decides otherwise later. Composing ahead of the
 * data is the normal case, so nothing refuses an `"unset"` receiver.
 *
 * `new Pipeline<number>()` is `"unset"`; calling it with an array keeps that, an `AsyncIterable`
 * widens it, and a single `Promise`-returning callback anywhere in the chain makes it `"async"`.
 */
export type PipelineMode = "unset" | "sync" | "async";

/**
 * What a `Pipeline` class does to an input's own shape. `"shape"` keeps it, so an array stays
 * `"sync"`; `"async"` overrides it, which is every dispatching class - `ConcurrentPipeline`,
 * `HttpPipeline` and `ClusterPipeline` all exist for I/O-bound work and have no synchronous case.
 *
 * A runtime value only, read by `sourcePolicy()`.
 */
export type SourcePolicy = "shape" | "async";

/**
 * The Mode a stage produces from the chain's own Mode `M` and the stage's own Mode `S`: a stage
 * runs synchronously only when the chain reaching it already does, since one asynchronous half
 * defers everything after it.
 *
 * `M` is tested FIRST, deliberately: a dispatching class pins `M` to the literal `"async"`, so
 * `JoinMode<"async", M2>` reduces immediately even where `M2` is still abstract - testing `S`
 * first would leave the conditional deferred inside that generic scope instead.
 *
 * `"unset"` is the source-less chain, which the callable shape makes the ordinary case: a chain
 * is composed before its input exists, so a sync stage leaves the decision open for the input to
 * make, while one async stage decides it whatever the input turns out to be.
 *
 * `S` may itself be `"unset"`, for a `.local()` region composed before any input. The two arms
 * that produces are deliberately asymmetric: an undecided stage leaves a `"sync"` chain undecided,
 * since the input can still make it either; it leaves an `"async"` chain async, since one
 * asynchronous half has already deferred everything after it whatever the region turns out to be.
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
 * The Mode the seed `Transformer` inside `.transform()` starts at, for a chain whose own Mode is
 * `M`. A source-less chain is provisionally synchronous: nothing about it is async yet, and
 * either an async callback or an async input decides otherwise later.
 *
 * `SeedMode<"unset">` → `"sync"`. `SeedMode<"sync">` → `"sync"`. `SeedMode<"async">` → `"async"`.
 */
export type SeedMode<M extends PipelineMode> = M extends "async" ? "async" : "sync";

/**
 * A pipeline callback: maps `item` to `T` (or `Promise<T>`), with an optional shared `ctx`.
 *
 * ONE signature, not a union of `(item)` / `(item, ctx)` arms — TypeScript will not contextually
 * type a parameter against a union of function types, so a union arm makes an un-annotated
 * `.map((o) => …)` callback an implicit `any`. Declaring `ctx` in the single signature keeps a
 * 1-arg caller valid by arity flexibility (fewer params is assignable) while restoring inference of
 * `item`. `isContextAware` (`./utils/helpers.ts`) reads `fn.length` at runtime to decide whether to
 * pass `ctx`.
 *
 * `(o) => o.total` and `(o, ctx) => ctx.get("k")` both satisfy `PipelineFunction<Order, number>`.
 */
export type PipelineFunction<Out, T> = (item: Out, ctx: IContextManager) => T | Promise<T>;

/**
 * A pipeline reduce callback: folds `item` into `acc`, with an optional shared `ctx`, and can push
 * a value downstream mid-fold via `emit` - `emit` is FOURTH, so `ctx` keeps arity 3, matching
 * `PipelineFunction`'s own `ctx` slot. ONE signature for the same reason as `PipelineFunction`
 * above — a union of arities would block contextual inference and make `acc`/`item` implicit `any`
 * in an un-annotated `.reduce((acc, x) => …)`.
 *
 * `Transformer.reduce(fn, initial)` folds the ONE chunk it receives and keeps no state between
 * chunks; `Pipeline.reduce(fn, initial)` folds EVERY chunk the pipeline produces, the only place
 * cross-chunk state lives (`ConcurrentPipeline.reduce(fn, initial)` is the one override that always
 * dispatches it - `.local(build)` is what keeps it in-process instead, the base `Pipeline` never
 * gains that override). Both call `fn` with all four arguments regardless of its declared arity —
 * JS ignores the extras, so `(acc, x, emit) => …` silently receives `ctx` in `emit`'s slot and
 * throws "emit is not a function" on the first call; write `(acc, x, _ctx, emit)`.
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
 * The sentinel a `RowErrorHandler` returns to remove its row from the output entirely - a
 * `unique symbol`, never a string or `null`, so `undefined` stays an ordinary value a handler may
 * legitimately return (a map to `undefined` is not the same as dropping the row). Every site that
 * reads a handler's return tests it with `!== DROP`, never a truthiness check.
 */
export const DROP: unique symbol = Symbol("DROP");

/**
 * The ROW handler - one plain function, may be async, registered via `Transformer.onError(fn)`.
 * `item` is `unknown` because one handler covers every element-wise link in a chain regardless of
 * that link's own item type (`.map()`, `.filter()`, `.flatMap()`, `.tap(fn)`,
 * `Transformer.reduce()`'s fold step); its return is unchecked for the same reason - inherent to
 * the design, not a gap.
 *
 * Returning a value puts that value in the row's place; returning `DROP` removes the row; throwing
 * (or returning a rejected `Promise`) escalates past the row to the CHUNK, reaching
 * `PipelineErrorHandler` (below) instead.
 *
 * `(item, error, ctx) => (error.message.includes("Invalid") ? DROP : -1)` recovers a bad row to
 * `-1` and drops anything else that fails.
 */
// oxlint-disable-next-line anti-slop/no-unknown-parameters, anti-slop/no-unknown-returns -- one handler covers every element-wise link in a chain at a different item type each time (Transformer.pipe() carries it forward unaffected by Out -> U); narrowing would break that contract
export type RowErrorHandler = (item: unknown, error: Error, ctx: IContextManager) => unknown;

/**
 * The RUN handler, registered via `Pipeline.onError(fn)` - position-DEPENDENT, unlike
 * `RowErrorHandler`: it must be set before the `.transform()`/`.apply()` call whose chunk failures
 * it should catch, since it reaches a stage only through that stage's own dispatch (`Pipeline.apply()`
 * threads it into `Transformer.process()`, `ConcurrentPipeline.apply()` reads it directly off `this`).
 * Returning drops the failing CHUNK and the run continues to the next one; throwing stops the run,
 * rejecting with whatever it throws.
 *
 * `(error, ctx) => console.warn(error.message)` logs and continues; `(error) => { throw error; }`
 * makes every chunk failure fatal, same as no handler at all.
 */
export type PipelineErrorHandler = (error: Error, ctx: IContextManager) => void;

/**
 * Carries a chain's row handler down through a composed `InternalTransformer` call -
 * `Transformer.runnable()` builds it once, reading `this.rowHandler` off the FINAL transformer, and
 * `pipe()` forwards the same instance to every link underneath. A link with no `run.rowHandler` set
 * runs its ordinary code path unchanged - the seam costs nothing until a handler is registered.
 */
export interface RunScope {
  rowHandler?: RowErrorHandler;
}

/**
 * A chunk-transform function: processes one chunk of `In` items into `Out` items (or a `Promise`
 * of them), given the shared context.
 *
 * `run` is optional and forwarded by `pipe()` alone - a caller driving a `Transformer` standalone
 * via `.process()` never has to supply it; `Transformer.runnable()` is what builds it from
 * `this.rowHandler` before the top of the chain is ever called.
 */
export type InternalTransformer<In, Out> = (
  chunk: In[],
  ctx: IContextManager,
  run?: RunScope,
) => Out[] | Promise<Out[]>;

/**
 * Breaks an async iterable into chunks - the return type of `buildChunkGenerator()`, `.buffer()`'s
 * own cutting engine.
 */
export type ChunkerFunction<T> = (data: AsyncIterable<T>) => AsyncGenerator<T[]>;

/**
 * `.buffer(fn)`'s own callback - decides the chunk boundary per item, in place of
 * `ChunkerFunction`'s whole-stream cut. `item` is folded into a `T[]` pending array the framework
 * owns and never hands to `fn`; `emit()` takes no value because there is nothing to pass - it flushes
 * whatever is currently pending and resets it to `[]`. Returning a value appends it to the
 * (possibly just-reset) pending array; returning `DROP` skips the item entirely, the same sentinel
 * `RowErrorHandler` already uses to mean "no row here." `.buffer(size)` is this same mechanism
 * configured with an identity `fn` and a framework-side auto-flush at `pending.length >= size` - see
 * `src/utils/reduce.ts`'s `sizeReduceFunction`/`bufferReduceFunction`, the two adapters onto the
 * `Reducer<T[], T>` class `Pipeline.reduce()` already uses.
 *
 * `(item, ctx, emit) => (item.ts - windowStart >= FIVE_MINUTES ? (emit(), windowStart = item.ts, item)
 * : item)` cuts a chunk boundary every time an item's own timestamp crosses a five-minute window,
 * the real chunk boundary `.tap()` observes in `product.md`'s own example.
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
 * Options for constructing a `Transformer`. `rowHandler` is set internally, by `.onError(fn)`'s
 * own copy-on-write - a caller constructs a `Transformer` from `{ transform }` alone in practice.
 */
export interface TransformerOptions<In, Out> {
  /**
   * Initial transformer function.
   */
  transform?: InternalTransformer<In, Out>;
  /** The row handler `.onError(fn)` installs; see `RowErrorHandler` above. */
  rowHandler?: RowErrorHandler;
}

/**
 * A stage's registered-transform table: `chunkTransforms`/`reduceStages`, shared across
 * `pipeline.ts` and every `pipelines/*.ts` dispatch override.
 *
 * `{ chunkTransforms: [mapStage, filterStage], reduceStages: new Map() }` → the table a two-stage
 * `.transform((t) => t.map(f).filter(g))` chain carries between copy-on-write calls.
 */
export interface StageRegistries {
  chunkTransforms: ChunkTransform[];
  reduceStages: Map<number, ReduceStage>;
}

/**
 * A chunk-wise transform function: takes one chunk (array) and produces the next chunk (array),
 * optionally reading/writing the shared context.
 *
 * `(chunk, ctx) => chunk.map((x) => x * 2)` over `[1, 2, 3]` → `[2, 4, 6]`.
 */
export type ChunkTransform = (
  chunk: unknown[],
  ctx: IContextManager,
) => unknown[] | Promise<unknown[]>;

/** A registered reduce stage's own definition - `pushReduceStage()` (`pipeline.ts`) is the one
 * place that builds one, `HttpPipeline.fetch()` the one place that reads one back to serve
 * `/reduce/<n>`. Untyped on `U`/`T` (kept as `unknown`) since a `Pipeline`'s own map holds reduce
 * stages of every type a chain has ever registered, not just its current `T`.
 *
 * `{ fn: (acc, x) => acc + x, initial: 0 }` → the stage `HttpPipeline.fetch()` looks up to serve
 * `/reduce/<n>` for a chain built as `.reduce((acc, x) => acc + x, 0)`. */
export interface ReduceStage<U = unknown, T = unknown> {
  fn: ReduceFunction<U, T>;
  initial: U;
}

/**
 * A dispatching class's own dispatched-reduce shape - the per-class override of WHERE a reduce
 * stage's fold actually runs, called once and returning a closure `ConcurrentPipeline.reduce()`
 * calls `maxConcurrency` times, each its own partition.
 *
 * `(chunks, ctx) => foldEachPartition(chunks, ctx)` - the closure `reduceWork()` returns, called
 * once per partition, each folding its own `share()` view of the one shared chunk stream.
 */
export type ReduceWork<T, U> = (
  chunks: AsyncIterable<T[]>,
  ctx: IContextManager,
) => AsyncGenerator<U[]>;

/** The two verbs a dispatched stage's route names: `"transform"` for a per-chunk stage,
 * `"reduce"` for a fold.
 *
 * `"transform"` → the verb in `/transform/0`; `"reduce"` → the verb in `/reduce/0`. */
export type RouteVerb = "transform" | "reduce";

/** A parsed dispatch route - what `HttpPipeline.fetch()`'s own path-matching produces, and
 * `routePath()` builds the string form of.
 *
 * `routePath("transform", 0)` → `"/transform/0"`; parsing it back →
 * `{ trail: null, verb: "transform", index: 0 }`. */
export interface StageRoute {
  trail: string | null;
  verb: RouteVerb;
  index: number;
}

/** A value tagged with the id of the partition or source that produced it.
 *
 * `{ id: 2, result: [4, 5, 6] }` → partition 2's own chunk, tagged so `Promise.race` over every
 * in-flight partition can tell which one just settled. */
export interface Tagged<R> {
  id: number;
  result: R;
}

/**
 * The four views a terminal op or a `.branch()` arm drains a bound chain through - unifies
 * `Pipeline.drainable()`'s own return shape, `PipelineResult`'s equivalent fields, and
 * `BranchOwner`'s own structural subset. `items`/`chunks` are THUNKS, not the streams themselves -
 * each terminal calls `Pipeline.drainable()` exactly once and threads the thunk into its own
 * sync/async arm, so building the stream is deferred to whichever arm actually runs.
 *
 * `pipeline.drainable([1, 2, 3])` → `{ syncChunks: [[1, 2, 3]], items: () => …, chunks: () => …,
 * context: <this run's manager> }` for a synchronous chain over an array.
 */
export interface Drainable<T> {
  /** The chain's chunks, already available synchronously, or `null` when the chain is (or became) async. */
  syncChunks: MaybeAsyncChunks<T> | null;
  /** Builds the item-level async stream, lazily - call once per drain. */
  items: () => AsyncIterable<T>;
  /** Builds the chunk-level async stream, lazily - call once per drain. */
  chunks: () => AsyncIterable<T[]>;
  /** This run's context manager. */
  context: IContextManager;
}
