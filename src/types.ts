/**
 * Core type definitions for @outputty/pipeline
 *
 * Migrated from laygo-python with async-first design.
 */

/**
 * Default chunk size for processing.
 */
export const DEFAULT_CHUNK_SIZE = 1000;

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
 * Error handler for chunk processing errors.
 *
 * Python equivalent:
 * ```python
 * type ChunkErrorHandler[In, U] = Callable[[list[In], Exception, IContextManager], list[U]]
 * ```
 */
export type ChunkErrorHandler<In, U = void> = (
  chunk: In[],
  error: Error,
  ctx: IContextManager,
) => U[] | void;

/**
 * Internal transformer function that processes chunks.
 *
 * Supports both synchronous and asynchronous transformers.
 * When used with execution strategies, Promise results are automatically awaited.
 *
 * Python equivalent:
 * ```python
 * type InternalTransformer[In, Out] = Callable[[list[In], IContextManager], list[Out]]
 * ```
 */
export type InternalTransformer<In, Out> = (
  chunk: In[],
  ctx: IContextManager,
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
 * Branch definition for Pipeline.branch() routing.
 *
 * Python equivalent:
 * ```python
 * BranchDict = dict[str, tuple[Callable[[T], bool], Transformer[T, U]]]
 * ```
 *
 * Note: Uses generic type parameter as placeholder since Transformer is imported separately.
 * The actual Transformer class is defined in transformer.ts.
 */
export interface BranchDefinition<T, _U, TTransformer = unknown> {
  /**
   * Predicate function to determine if item goes to this branch.
   */
  predicate: (item: T) => boolean | Promise<boolean>;

  /**
   * Transformer to apply to items that match the predicate.
   */
  transformer: TTransformer;
}

/**
 * Options for creating a Transformer.
 */
export interface TransformerOptions<In, Out> {
  /**
   * Initial transformer function.
   */
  transform?: InternalTransformer<In, Out>;
}

/**
 * Options for the branch operation.
 *
 * Python equivalent:
 * ```python
 * def branch(
 *   self,
 *   branches: Mapping[str, tuple[Transformer, Callable[[T], bool]]],
 *   *,
 *   first_match: bool = True,
 * ) -> tuple[dict[str, list], dict[str, Any]]:
 * ```
 */
export interface BranchOptions {
  /**
   * When true (default), items are routed to the first matching branch only.
   * When false (broadcast mode), items are sent to ALL matching branches.
   *
   * Python equivalent: `first_match` parameter
   */
  firstMatch?: boolean;
}
