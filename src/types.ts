/**
 * Core type definitions for @outputty/pipeline
 *
 * Migrated from laygo-python with async-first design.
 */

import type { MaybeAsyncChunks } from "./utils/chunk";

/**
 * Default chunk size for processing.
 */
export const DEFAULT_CHUNK_SIZE = 1000;

/**
 * Whether a chain runs synchronously, and therefore whether its terminal ops return a value or a
 * `Promise` (#90). `"unset"` is the ORDINARY state of a composed chain: nothing about it is async
 * yet, and either an async callback or an async input decides otherwise later. Composing ahead of
 * the data is the normal case, so nothing refuses an `"unset"` receiver.
 *
 * `new Pipeline<number>()` is `"unset"`; calling it with an array keeps that, an `AsyncIterable`
 * widens it, and a single `Promise`-returning callback anywhere in the chain makes it `"async"`.
 */
export type PipelineMode = "unset" | "sync" | "async";

/**
 * What a `Pipeline` class does to an input's own shape (#90). `"shape"` keeps it, so an array is
 * `"sync"`; `"async"` overrides it, which is every dispatching class - `ConcurrentPipeline`,
 * `HttpPipeline` and `ClusterPipeline` all exist for I/O-bound work and have no synchronous case.
 *
 * A RUNTIME value only, read by `sourcePolicy()`. It was also a type parameter on `Pipeline`, with
 * an `AssignMode<P, S>` operator applying it on top of every stage's own `JoinMode` - deleted, and
 * the reason is worth recording because the parameter outlived it twice over. Its stated reason was
 * that a subclass's `.from()` override had to be a narrowing of the base's; `.from()` went with
 * this ticket. What it was ACTUALLY still doing was collapsing `JoinMode<M, M2>` to the literal
 * `"async"` on a dispatching class, because `JoinMode` tested `S` first and so never reduced while
 * `M2` was abstract - the subclass's own narrowing overrides then failed `TS2416`, nine of them.
 * `JoinMode` testing `M` first (below) short-circuits on the concrete `"async"` those classes pin,
 * which is the same collapse one level up, with no parameter to carry.
 */
export type SourcePolicy = "shape" | "async";

/**
 * The Mode a stage produces from the chain's own Mode `M` and the stage's own Mode `S` (#90): a
 * stage runs synchronously only when the chain reaching it already does, since one asynchronous
 * half defers everything after it.
 *
 * `M` is tested FIRST, and deliberately: a dispatching class pins `M` to the literal `"async"`, so
 * `JoinMode<"async", M2>` reduces immediately even where `M2` is still abstract. Testing `S` first
 * gives the identical answer at every concrete instantiation, but leaves the conditional deferred
 * inside a generic scope - which is what made the three dispatching classes need a whole extra type
 * parameter to state what this line now states.
 *
 * `"unset"` is the source-less chain, which the callable shape makes the ordinary case: a chain is
 * composed before its input exists, so a sync stage leaves the decision open for the input to make,
 * while ONE async stage decides it whatever the input turns out to be.
 *
 * `S` may itself be `"unset"`, for a `.local()` region composed before any input. The two arms that
 * produces are deliberately asymmetric: an undecided stage leaves a `"sync"` chain undecided, since
 * the input can still make it either; it leaves an `"async"` chain async, since one asynchronous
 * half has already deferred everything after it whatever the region turns out to be.
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
 * `M` (#90). A source-less chain is provisionally synchronous: nothing about it is async yet, and
 * either an async callback or an async input decides otherwise later.
 *
 * Written as a conditional rather than the intersection `M & ("sync" | "async")` that preceded it.
 * That intersection is `never` for `"unset"`, which made the seed's Mode `never`, the callback's
 * own `M2` infer as `never`, and every source-less `.transform()` return `Pipeline<U, never, …>` -
 * a chain that then typed every terminal `never` and accepted nothing.
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
 * A pipeline reduce callback: folds `item` into `acc`, with an optional shared `ctx`, and can push a
 * value downstream mid-fold via `emit` (#45) — `emit` is FOURTH, so `ctx` keeps arity 3, matching
 * `PipelineFunction`'s own `ctx` slot (the arity-sniffing `isContextAwareReduce` this comment used to
 * reference is gone: every caller now always passes all four arguments, so no branch on `fn.length`
 * is needed). ONE signature for the same reason as `PipelineFunction` above — a union of arities
 * would block contextual inference and make `acc`/`item` implicit `any` in an un-annotated
 * `.reduce((acc, x) => …)`.
 *
 * `Transformer.reduce(fn, initial)` folds the ONE chunk it receives and keeps no state between
 * chunks; `Pipeline.reduce(fn, initial)` folds EVERY chunk the pipeline produces, the only place
 * cross-chunk state lives (`ConcurrentPipeline.reduce(fn, initial)` is the one override that always
 * dispatches it - `.local(build)` (#61) is what keeps it in-process instead, the base `Pipeline`
 * never gains that override). Both call `fn` with all four arguments regardless of its declared
 * arity — JS ignores the extras, so `(acc, x, emit) => …` silently receives `ctx` in `emit`'s slot and
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
 * The sentinel a `RowErrorHandler` returns to remove its row from the output entirely (#78) - a
 * `unique symbol`, never a string or `null`, so `undefined` stays an ordinary value a handler may
 * legitimately return (a map to `undefined` is not the same as dropping the row). Every site that
 * reads a handler's return tests it with `!== DROP`, never a truthiness check.
 */
export const DROP: unique symbol = Symbol("DROP");

/**
 * The ROW handler (#78; replaces #40's chunk-level notification contract entirely) - one plain
 * function, may be async, registered via `Transformer.onError(fn)`. `item` is `unknown` because one
 * handler covers every element-wise link in a chain regardless of that link's own item type
 * (`.map()`, `.filter()`, `.flatMap()`, `.tap(fn)`, `Transformer.reduce()`'s fold step); its return
 * is unchecked for the same reason - inherent to the design, not a gap.
 *
 * Returning a value puts that value in the row's place; returning `DROP` removes the row; throwing
 * (or returning a rejected `Promise`) escalates past the row to the CHUNK, reaching
 * `PipelineErrorHandler` (below) instead. The return type is bare `unknown` (#133) - `unknown |
 * typeof DROP | Promise<unknown | typeof DROP>` is compiler-identical, since `unknown` already
 * absorbs every other arm of a union it appears in.
 *
 * `(item, error, ctx) => (error.message.includes("Invalid") ? DROP : -1)` recovers a bad row to
 * `-1` and drops anything else that fails.
 */
export type RowErrorHandler = (item: unknown, error: Error, ctx: IContextManager) => unknown;

/**
 * The RUN handler (#78), registered via `Pipeline.onError(fn)` - position-DEPENDENT, unlike
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
 * Carries a chain's row handler down through a composed `InternalTransformer` call (#78) -
 * `Transformer.runnable()` builds it once, reading `this.rowHandler` off the FINAL transformer, and
 * `pipe()` forwards the same instance to every link underneath. A link with no `run.rowHandler` set
 * runs its pre-#78 code path unchanged - the seam costs nothing until a handler is registered.
 */
export interface RunScope {
  rowHandler?: RowErrorHandler;
}

/**
 * Internal transformer function that processes chunks.
 *
 * Supports both synchronous and asynchronous transformers.
 * When used with execution strategies, Promise results are automatically awaited.
 *
 * `run` is optional and forwarded by `pipe()` alone (#78) - a caller driving a `Transformer`
 * standalone via `.process()` never has to supply it; `Transformer.runnable()` is what builds it
 * from `this.rowHandler` before the top of the chain is ever called.
 *
 * Python equivalent:
 * ```python
 * type InternalTransformer[In, Out] = Callable[[list[In], IContextManager], list[Out]]
 * ```
 */
export type InternalTransformer<In, Out> = (
  chunk: In[],
  ctx: IContextManager,
  run?: RunScope,
) => Out[] | Promise<Out[]>;

/**
 * Chunker function type - breaks an async iterable into chunks.
 *
 * Python equivalent:
 * ```python
 * Callable[[Iterable[T]], Iterator[list[T]]]
 * ```
 */
export type ChunkerFunction<T> = (data: AsyncIterable<T>) => AsyncGenerator<T[]>;

/**
 * Context manager interface for sharing state across pipeline operations.
 *
 * Python equivalent:
 * ```python
 * class IContextManager(Protocol):
 *   def __getitem__(self, key: str) -> Any: ...
 *   def __setitem__(self, key: str, value: Any) -> None: ...
 *   def get(self, key: str, default: Any = None) -> Any: ...
 *   def to_dict(self) -> dict[str, Any]: ...
 * ```
 */
export interface IContextManager {
  /**
   * Get a value by key. Returns undefined if not present.
   */
  get(key: string): unknown;

  /**
   * Set a value by key.
   */
  set(key: string, value: unknown): void;

  /**
   * Get a value by key with a default fallback.
   */
  getOrDefault<T>(key: string, defaultValue: T): T;

  /**
   * Convert context to a plain object (snapshot).
   */
  toDict(): Record<string, unknown>;
}

/**
 * Options for creating a Transformer (#133: absorbs what `transformer.ts`'s own, unexported
 * `TransformerConstructorOptions` used to wrap this in - the two never had a real boundary between
 * them, since a caller passing `TransformerOptions` to `new Transformer()` could already set
 * `rowHandler` structurally, wrapper or not). `rowHandler` is set internally, by `.onError(fn)`'s
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
 * A stage's registered-transform table (#133) - `chunkTransforms`/`reduceStages` spelled inline 6x
 * across `pipeline.ts`'s own copy-on-write call sites and `pipelines/http.ts`'s registry lookup
 * before this. `ChunkTransform`/`ReduceStage` live here rather than in `pipeline.ts` because this
 * type, like `ReduceWork` below, is shared across `pipeline.ts` and every `pipelines/*.ts` dispatch
 * override.
 */
export interface StageRegistries {
  chunkTransforms: ChunkTransform[];
  reduceStages: Map<number, ReduceStage>;
}

/**
 * A chunk-wise transform function: takes one chunk (array) and produces the next chunk (array),
 * optionally reading/writing the shared context (#17, relocated from `pipeline.ts` by #133 so
 * `StageRegistries` above can reference it with no import cycle).
 */
export type ChunkTransform = (
  chunk: unknown[],
  ctx: IContextManager,
) => unknown[] | Promise<unknown[]>;

/** A registered reduce stage's own definition (relocated from `pipeline.ts` by #133, same reason as
 * `ChunkTransform` above) - `pushReduceStage()` (`pipeline.ts`) is the one place that builds one,
 * `HttpPipeline.fetch()` (#45 L5) the one place that reads one back to serve `/reduce/<n>`. Untyped
 * on `U`/`T` (kept as `unknown`) since a `Pipeline`'s own map holds reduce stages of every type a
 * chain has ever registered, not just its current `T`.
 *
 * `{ fn: (acc, x) => acc + x, initial: 0 }` → the stage `HttpPipeline.fetch()` looks up to serve
 * `/reduce/<n>` for a chain built as `.reduce((acc, x) => acc + x, 0)`. */
export interface ReduceStage<U = unknown, T = unknown> {
  fn: ReduceFunction<U, T>;
  initial: U;
}

/**
 * A dispatching class's own dispatched-reduce shape (#133) - spelled inline 3x today
 * (`pipelines/concurrent.ts`, `http.ts`, `cluster.ts`): the per-class override of WHERE a reduce
 * stage's fold actually runs, called once and returning a closure `ConcurrentPipeline.reduce()`
 * calls `maxConcurrency` times, each its own partition.
 */
export type ReduceWork<T, U> = (
  chunks: AsyncIterable<T[]>,
  ctx: IContextManager,
) => AsyncGenerator<U[]>;

/** The two verbs a dispatched stage's route names (#133) - spelled inline 4x across `pipelines/
 * http.ts` and `cluster.ts` before this: `/transform/<n>` for a per-chunk stage, `/reduce/<n>` for a
 * fold. */
export type RouteVerb = "transform" | "reduce";

/** A parsed dispatch route - what `HttpPipeline.fetch()`'s own path-matching produces, and
 * `routePath()` builds the string form of (#133). */
export interface StageRoute {
  trail: string | null;
  verb: RouteVerb;
  index: number;
}

/** A value tagged with the id of the partition or source that produced it (#133) - unifies
 * `pipelines/concurrent.ts`'s own `TaggedResult<U>` (`= Tagged<U[]>`) with the inline
 * `{ id: number; result: IteratorResult<U[]> }` shape `share()`'s racer already matched. */
export interface Tagged<R> {
  id: number;
  result: R;
}

/**
 * The four views a terminal op or a `.branch()` arm drains a bound chain through (#133) - unifies
 * `Pipeline.drainable()`'s own return shape (`pipeline.ts`), `PipelineResult`'s private re-spelling
 * of the same three fields cast through it (`result.ts`), and `BranchOwner`'s own three-field
 * structural subset (`branch.ts`). `items`/`chunks` are THUNKS, not the streams themselves - each
 * terminal calls `Pipeline.drainable()` exactly once and threads the thunk into its own sync/async
 * arm, so building the stream is deferred to whichever arm actually runs.
 */
export interface Drainable<T> {
  syncChunks: MaybeAsyncChunks<T> | null;
  items: () => AsyncIterable<T>;
  chunks: () => AsyncIterable<T[]>;
  context: IContextManager;
}
