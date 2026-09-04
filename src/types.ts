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
 * A pipeline reduce callback: folds `item` into `acc`, with an optional shared `ctx`.
 *
 * ONE signature for the same reason as `PipelineFunction` above — a union of `(acc, item)` /
 * `(acc, item, ctx)` arms would block contextual inference and make `acc`/`item` implicit `any` in an
 * un-annotated `.reduce((acc, x) => …)`. `isContextAwareReduce` reads `fn.length` at runtime.
 *
 * `(acc, x) => acc + x` and `(acc, x, ctx) => acc + x * ctx.get("w")` both satisfy it.
 */
export type PipelineReduceFunction<U, Out> = (
  acc: U,
  item: Out,
  ctx: IContextManager,
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
 * Execution strategy interface for different processing modes.
 *
 * Python equivalent:
 * ```python
 * class ExecutionStrategy[In, Out](ABC):
 *   @abstractmethod
 *   def execute(
 *     self,
 *     transformer_logic: InternalTransformer[In, Out],
 *     chunks: Iterator[list[In]],
 *     context: IContextManager,
 *   ) -> Iterator[list[Out]]:
 *     ...
 * ```
 */
export interface ExecutionStrategy<In, Out> {
  /**
   * Execute the transformer logic on chunks.
   *
   * @param transformerLogic - Function that processes a single chunk
   * @param chunks - Async iterable of input chunks
   * @param context - Shared context manager
   * @returns Async generator of output chunks
   */
  execute(
    transformerLogic: InternalTransformer<In, Out>,
    chunks: AsyncIterable<In[]>,
    context: IContextManager,
  ): AsyncGenerator<Out[]>;

  /**
   * Capability flag, read GENERICALLY by `Pipeline.apply()` to decide whether a knob
   * (`withExecutor`/`withHooks`/`chunkSize`) is honored when the pipeline is iterated as a laygo
   * source (the async-iteration path bypasses `Transformer.execute`, so only a strategy that runs
   * chunk-by-chunk with no concurrency/ordering machinery of its own can be silently correct
   * there). `SequentialStrategy.appliesInSourcePosition === true`; `ConcurrentStrategy` and any
   * other custom strategy default to `false` — routing never inspects `instanceof`/`.name`.
   */
  readonly appliesInSourcePosition: boolean;
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
   * Execution strategy for processing chunks.
   */
  strategy?: ExecutionStrategy<In, Out>;

  /**
   * Number of items per chunk.
   */
  chunkSize?: number;

  /**
   * Initial transformer function.
   */
  transform?: InternalTransformer<In, Out>;
}

/**
 * Options for concurrent execution strategy.
 */
export interface ConcurrentStrategyOptions {
  /**
   * Maximum number of concurrent chunk operations.
   */
  maxConcurrency?: number;

  /**
   * Whether to maintain input order in output.
   */
  ordered?: boolean;
}

/**
 * Options for the reduce operation.
 *
 * Python equivalent:
 * ```python
 * def reduce(self, function, initial, *, per_chunk: bool = False):
 *   ...
 * ```
 */
export interface ReduceOptions {
  /**
   * When true (default), reduce is applied per-chunk and returns a chainable Transformer.
   * When false, reduce becomes a terminal operation that reduces the ENTIRE dataset.
   *
   * Python equivalent: `per_chunk` parameter (inverted logic - `per_chunk=True` == `perChunk: true`)
   */
  perChunk?: boolean;
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

// ===== Executor Registry Types (Proposal C) =====

/**
 * Built-in executor type identifiers.
 */
export type ExecutorType = "sequential" | "concurrent";

/**
 * Options for executor creation.
 */
export interface ExecutorOptions {
  /**
   * Maximum concurrency level (for concurrent executor).
   */
  maxConcurrency?: number;

  /**
   * Whether to maintain input order in output.
   */
  ordered?: boolean;
}

/**
 * Factory interface for creating execution strategies.
 */
export interface ExecutorFactory<In = unknown, Out = unknown> {
  /**
   * Create an execution strategy with the given options.
   */
  create(options?: ExecutorOptions): ExecutionStrategy<In, Out>;
}

/**
 * Custom executor specification for user-provided strategies.
 */
export interface CustomExecutor<In, Out> {
  custom: ExecutionStrategy<In, Out>;
}

/**
 * Executor specification - either a built-in type or custom executor.
 */
export type ExecutorSpec<In, Out> = ExecutorType | CustomExecutor<In, Out>;

/**
 * Lifecycle hooks for transformer execution.
 *
 * Enables event-driven monitoring of transformation progress without
 * embedding event logic in transformer implementations.
 *
 * Usage:
 * ```typescript
 * transformer.withHooks({
 *   onItemStart: (item, index, total) => console.log(`Processing ${index}/${total}`),
 *   onItemComplete: (item, durationMs) => console.log(`Done in ${durationMs}ms`),
 * })
 * ```
 */
export interface TransformerLifecycleHooks<In = unknown, Out = unknown> {
  /**
   * Called when transformer execution starts.
   */
  onStart?: () => void | Promise<void>;

  /**
   * Called before each item is processed.
   * @param item - The input item being processed
   * @param index - Zero-based index of the item
   * @param total - Total number of items (-1 if unknown/streaming)
   */
  onItemStart?: (item: In, index: number, total: number) => void | Promise<void>;

  /**
   * Called after each item is successfully processed.
   * @param input - The original input item
   * @param output - The transformed output item
   * @param durationMs - Processing time in milliseconds
   */
  onItemComplete?: (input: In, output: Out, durationMs: number) => void | Promise<void>;

  /**
   * Called when an item fails to process.
   * @param item - The input item that failed
   * @param error - The error that occurred
   */
  onItemError?: (item: In, error: Error) => void | Promise<void>;

  /**
   * Called when transformer execution completes successfully.
   * @param totalItems - Total number of items processed
   * @param totalDurationMs - Total execution time in milliseconds
   */
  onComplete?: (totalItems: number, totalDurationMs: number) => void | Promise<void>;

  /**
   * Called when transformer execution fails.
   * @param error - The error that caused the failure
   */
  onError?: (error: Error) => void | Promise<void>;
}
