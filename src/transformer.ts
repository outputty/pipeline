/**
 * Transformer class - chainable chunk transformation operations.
 *
 * Python equivalent:
 * ```python
 * class Transformer[In, Out](BaseTransformer[In, Out]):
 *   def __init__(
 *     self,
 *     strategy: ExecutionStrategy[In, Out] | None = None,
 *     chunk_size: int | None = DEFAULT_CHUNK_SIZE,
 *     transformer: InternalTransformer[In, Out] | None = None,
 *   ) -> None:
 *     ...
 *
 *   def __call__(self, data: Iterable[In], context: IContextManager | None = None) -> Iterator[Out]:
 *     ...
 * ```
 */

import type {
  ExecutionStrategy,
  InternalTransformer,
  IContextManager,
  TransformerOptions,
  PipelineFunction,
  PipelineReduceFunction,
  ReduceOptions,
  TransformerLifecycleHooks,
  ChunkerFunction,
} from "./types";
import { DEFAULT_CHUNK_SIZE } from "./types";
import { buildChunkGenerator } from "./utils/chunk";
import { SimpleContextManager } from "./context/simple";
import { ErrorHandler, type ChunkErrorHandler } from "./errors/handler";
import { sequential } from "./strategies/sequential";
import { isContextAware, isContextAwareReduce } from "./utils/helpers";

/**
 * Construction-time knobs shared by every `Transformer<In, Out>` constructor overload below —
 * named once rather than repeated per overload.
 */
type TransformerConstructorOptions<In, Out> = TransformerOptions<In, Out> & {
  hooks?: TransformerLifecycleHooks<In, Out>;
  errorHandler?: ErrorHandler<In>;
  chunker?: ChunkerFunction<In>;
};

/**
 * A reusable, composable stage: `In` items in, `Out` items out, applied chunk by chunk. Built by
 * chaining (`.map()`, `.filter()`, `.reduce()`, …), each call returning a NEW `Transformer` so one
 * can be shared across pipelines without aliasing. It carries its own chunk size, execution
 * strategy and error handling, which is what lets `pipeline.apply(t)` stay a one-liner. Every
 * configuration method (`.onError()`, `.setChunker()`, `.withHooks()`, `.withExecutor()`) is
 * copy-on-write like `.map()`/`.filter()`: it returns a NEW `Transformer` carrying every other
 * knob forward, never mutates `this` — the shape `@outputty/laygo`'s own `Model#named` follows.
 *
 * `new Transformer<number, number>().map((n) => n * 2)` → a transformer a pipeline can `.apply()`.
 */
export class Transformer<In, Out> {
  /** Number of items per chunk */
  readonly chunkSize: number;

  /** Execution strategy for processing chunks */
  readonly strategy: ExecutionStrategy<In, Out>;

  /** The internal transform function */
  readonly transform: InternalTransformer<In, Out>;

  /** Error handler chain for this transformer */
  readonly errorHandler: ErrorHandler<In>;

  /** Lifecycle hooks for monitoring execution progress */
  readonly hooks?: TransformerLifecycleHooks<In, Out>;

  /** Custom chunk generator, when `.setChunker()` was called — undefined means the default,
   * chunkSize-driven generator built below. Public so `inertKnobsOf` (`pipeline.ts`) can detect a
   * custom chunker the async-iteration path can't honor, the same way it reads `hooks`/`strategy`. */
  readonly chunker?: ChunkerFunction<In>;

  /** Function to break input into chunks — `chunker` if set, else built from `chunkSize` */
  private chunkGenerator: ChunkerFunction<In>;

  /** Default context to use when none provided */
  private defaultContext: IContextManager;

  /**
   * Overload 1 — a real `transform` in hand. Not conditional on `In extends Out`, so it resolves
   * (and is preferred) even from inside this class's OWN generic methods, where `In`/`Out` are
   * still abstract type parameters a deferred conditional can never distribute over. Every
   * copy-on-write rebuild site below (`.pipe()`, `.withHooks()`, `.onError()`, `.setChunker()`,
   * `.withExecutor()`) already carries a real `transform` forward, so all of them land here.
   */
  constructor(
    options: TransformerConstructorOptions<In, Out> & { transform: InternalTransformer<In, Out> },
  );
  /**
   * Overload 2 — no `transform` given. Only sound when `In` is assignable to `Out` (the identity
   * default below is a real conversion, not a lie): `new Transformer<number, { id: number }>()`
   * is a compile error here, `new Transformer<number, number>()` is not. Resolves only for a
   * CONCRETE `In`/`Out` at an external call site — the conditional is deferred (unresolved) inside
   * this class's own generic methods, which is why they route through overload 1 instead.
   */
  constructor(...args: In extends Out ? [options?: TransformerConstructorOptions<In, Out>] : never);
  constructor(options?: TransformerConstructorOptions<In, Out>) {
    this.chunkSize = options?.chunkSize ?? DEFAULT_CHUNK_SIZE;
    // Reachable only via overload 2's `In extends Out` branch — a real conversion there, not a lie.
    this.transform = options?.transform ?? ((chunk, _ctx) => chunk as unknown as Out[]);
    this.errorHandler = options?.errorHandler ?? new ErrorHandler<In>();
    this.chunker = options?.chunker;
    this.chunkGenerator = this.chunker ?? buildChunkGenerator(this.chunkSize);
    this.defaultContext = new SimpleContextManager();
    this.strategy = options?.strategy ?? sequential<In, Out>;
    this.hooks = options?.hooks;
  }

  /**
   * Attach lifecycle hooks to this transformer.
   *
   * Creates a new transformer with the specified hooks that will be called
   * during execution. This enables event-driven monitoring without embedding
   * event logic in transformer implementations.
   *
   * @param hooks - Lifecycle hooks for monitoring execution
   * @returns New Transformer with hooks attached
   *
   * @example
   * ```typescript
   * const transformer = new Transformer()
   *   .map(item => item.toUpperCase())
   *   .withHooks({
   *     onItemStart: (item, index) => console.log(`Processing ${index}`),
   *     onItemComplete: (input, output, ms) => console.log(`Done in ${ms}ms`),
   *   })
   * ```
   */
  withHooks(hooks: TransformerLifecycleHooks<In, Out>): Transformer<In, Out> {
    return new Transformer<In, Out>({
      strategy: this.strategy,
      chunkSize: this.chunkSize,
      transform: this.transform,
      errorHandler: this.errorHandler,
      chunker: this.chunker,
      hooks,
    });
  }

  /**
   * Execute the transformer on input data.
   *
   * If hooks are attached, they will be called at appropriate lifecycle points:
   * - onStart: Before processing begins
   * - onItemStart: Before each item is processed
   * - onItemComplete: After each item is successfully processed
   * - onItemError: When an item fails to process
   * - onComplete: After all items are processed
   * - onError: When the transformer fails
   *
   * Python equivalent:
   * ```python
   * def __call__(self, data: Iterable[In], context: IContextManager | None = None) -> Iterator[Out]:
   *   run_context = context if context is not None else self._default_context
   *   chunks = self._chunk_generator(data)
   *   transformed_chunks = self.strategy(self.transformer, chunks, run_context)
   *   for chunk in transformed_chunks:
   *     yield from chunk
   * ```
   *
   * On a strategy/chunk failure, `this.errorHandler.handle([], error, runContext)` fires (any
   * handler registered via `.onError()`) BEFORE the error re-throws — a notification, not a
   * recovery path, so the failure still propagates to the caller.
   *
   * @param data - Async iterable of input items
   * @param context - Optional context manager for sharing state
   * @returns Async generator of output items
   *
   * @example
   * ```typescript
   * const t = new Transformer<number, number>().map((x) => x * 2);
   * for await (const item of t.execute(source([1, 2, 3]))) console.log(item); // 2, 4, 6
   *
   * // onError wiring: a throwing transform still propagates, but the handler sees it first.
   * let seen: Error | undefined;
   * const failing = new Transformer<number, number>()
   *   .map(() => { throw new Error("boom"); })
   *   .onError((e) => { seen = e; });
   * await expect(failing.execute(source([1])).next()).rejects.toThrow("boom");
   * // seen.message === "boom"
   * ```
   */
  async *execute(data: AsyncIterable<In>, context?: IContextManager): AsyncGenerator<Out> {
    const runContext = context ?? this.defaultContext;
    const startTime = Date.now();
    const itemCounter = { index: 0 };

    try {
      await this.hooks?.onStart?.();

      const hasItemHooks =
        this.hooks?.onItemStart ?? this.hooks?.onItemComplete ?? this.hooks?.onItemError;
      const chunks = this.chunkGenerator(data);

      if (hasItemHooks) {
        const wrappedTransform = this.wrapTransformForItemHooks(itemCounter);
        const transformedChunks = this.strategy(wrappedTransform, chunks, runContext);
        yield* this.drainChunks(transformedChunks);
      } else {
        const transformedChunks = this.strategy(this.transform, chunks, runContext);
        yield* this.drainChunks(transformedChunks, () => itemCounter.index++);
      }

      const totalDurationMs = Date.now() - startTime;
      await this.hooks?.onComplete?.(itemCounter.index, totalDurationMs);
    } catch (error) {
      await this.hooks?.onError?.(error as Error);
      // Registered via `.onError()` (above) — fires on any chunk failure surfacing from
      // `strategy.execute`, THEN the error still propagates (`.onError()` is a notification
      // hook, not a recovery path; `.catch()` is the sub-pipeline that actually recovers). The
      // offending chunk is not tracked at this scope (a failure can originate inside the
      // strategy's own concurrent fan-out), so handlers see `[]` — same as every OTHER caller of
      // this `execute()` catch, letting a handler tell error-occurred from error-details.
      this.errorHandler.handle([], error as Error, runContext);
      throw error;
    }
  }

  /**
   * Build a transform that emits `onItemStart`/`onItemComplete`/`onItemError`
   * hooks around each item of a chunk, one item at a time.
   *
   * Runs only when `execute()` detects at least one item-level hook attached.
   * Produces the same chunk output as `this.transform` would, but drives the
   * hooks as a side effect and advances `counter.index` per processed item.
   *
   * @example
   * `wrapTransformForItemHooks({ index: 0 })([1, 2], ctx)` → `[2, 4]` (with
   * `onItemStart`/`onItemComplete` invoked for each of `1` and `2`)
   */
  private wrapTransformForItemHooks(counter: { index: number }): InternalTransformer<In, Out> {
    return async (chunk, ctx) => {
      const results: Out[] = [];
      for (const item of chunk) {
        const output = await this.processItemWithHooks(item, ctx, counter);
        results.push(...output);
      }
      return results;
    };
  }

  /**
   * Process a single item through `this.transform`, emitting the item-level
   * lifecycle hooks around it and advancing the shared item counter.
   *
   * Runs once per item from within `wrapTransformForItemHooks`'s loop.
   * Returns the item's transform output, or re-throws after notifying
   * `onItemError` if the transform fails.
   *
   * @example
   * `processItemWithHooks(3, ctx, { index: 0 })` → `[6]` (with `onItemStart`
   * called at index `0`, then `onItemComplete` for the `6` output)
   */
  private async processItemWithHooks(
    item: In,
    ctx: IContextManager,
    counter: { index: number },
  ): Promise<Out[]> {
    const itemStartTime = Date.now();
    await this.hooks?.onItemStart?.(item, counter.index, -1); // -1 = total unknown (streaming)

    try {
      const singleResult = await this.transform([item], ctx);
      const itemDurationMs = Date.now() - itemStartTime;

      for (const output of singleResult) {
        await this.hooks?.onItemComplete?.(item, output, itemDurationMs);
      }
      counter.index++;
      return singleResult;
    } catch (error) {
      await this.hooks?.onItemError?.(item, error as Error);
      throw error;
    }
  }

  /**
   * Flatten transformed chunks into a single item stream.
   *
   * Runs at the tail of `execute()` to drain whichever chunk stream the
   * strategy produced. When `onItem` is supplied, it is invoked once per
   * item just before that item is yielded (used to advance the item
   * counter on the no-hooks fast path).
   *
   * @example
   * `drainChunks([[1, 2], [3]])` yields `1`, `2`, `3` in order.
   */
  private async *drainChunks(
    chunks: AsyncIterable<Out[]>,
    onItem?: () => void,
  ): AsyncGenerator<Out> {
    for await (const chunk of chunks) {
      for (const item of chunk) {
        onItem?.();
        yield item;
      }
    }
  }

  /**
   * Chain a new operation onto this transformer.
   *
   * This is the core internal method for building transformation chains.
   * Each operation creates a NEW transformer that composes the current
   * transform with the new operation.
   *
   * Python equivalent:
   * ```python
   * def _pipe[U](self, operation: Callable[[list[Out], IContextManager], list[U]]) -> "Transformer[In, U]":
   *   current_transformer = self.transformer
   *
   *   def new_transformer(chunk: list[In], ctx: IContextManager) -> list[U]:
   *     intermediate = current_transformer(chunk, ctx)
   *     return operation(intermediate, ctx)
   *
   *   return Transformer[In, U](
   *     strategy=copy.copy(self.strategy),
   *     chunk_size=self.chunk_size,
   *     transformer=new_transformer,
   *   )
   * ```
   *
   * @param operation - Function that transforms the output of the current transform
   * @returns A new Transformer with the composed operation
   */
  protected pipe<U>(
    operation: (chunk: Out[], ctx: IContextManager) => U[] | Promise<U[]>,
  ): Transformer<In, U> {
    const currentTransform = this.transform;

    const newTransform: InternalTransformer<In, U> = async (chunk, ctx) => {
      const intermediate = await currentTransform(chunk, ctx);
      return operation(intermediate, ctx);
    };

    return new Transformer<In, U>({
      // Strategy type needs to be cast since we're changing Out -> U
      strategy: this.strategy as unknown as ExecutionStrategy<In, U>,
      chunkSize: this.chunkSize,
      transform: newTransform,
      // errorHandler/chunker are keyed on `In`, unaffected by the Out -> U change, so both carry
      // forward — this is what keeps `.onError(fn).map(g)` and `.setChunker(c).map(g)` from
      // silently dropping the configuration this pipe() call would otherwise discard.
      errorHandler: this.errorHandler,
      chunker: this.chunker,
      // Note: hooks are NOT preserved through pipe() since types change Out -> U
      // Use withHooks() at the end of the chain
    });
  }

  /**
   * Transform each element using a mapping function.
   *
   * Python equivalent:
   * ```python
   * def map[U](self, function: PipelineFunction[Out, U]) -> "Transformer[In, U]":
   *   if is_context_aware(function):
   *     context_aware_func: Callable[[Out, IContextManager], U] = function
   *     return self._pipe(lambda chunk, ctx: [context_aware_func(x, ctx) for x in chunk])
   *
   *   non_context_func: Callable[[Out], U] = function
   *   return self._pipe(lambda chunk, _ctx: [non_context_func(x) for x in chunk])
   * ```
   *
   * @param fn - Mapping function (can be context-aware)
   * @returns New Transformer with map operation applied
   */
  map<U>(fn: PipelineFunction<Out, U>): Transformer<In, U> {
    if (isContextAware(fn)) {
      return this.pipe((chunk, ctx) => chunk.map((x) => fn(x, ctx) as U));
    }
    return this.pipe((chunk, _ctx) => chunk.map((x) => (fn as (item: Out) => U)(x)));
  }

  /**
   * Filter elements using a predicate function.
   *
   * Python equivalent:
   * ```python
   * def filter(self, predicate: PipelineFunction[Out, bool]) -> "Transformer[In, Out]":
   *   if is_context_aware(predicate):
   *     context_aware_predicate: Callable[[Out, IContextManager], bool] = predicate
   *     return self._pipe(lambda chunk, ctx: [x for x in chunk if context_aware_predicate(x, ctx)])
   *
   *   non_context_predicate: Callable[[Out], bool] = predicate
   *   return self._pipe(lambda chunk, _ctx: [x for x in chunk if non_context_predicate(x)])
   * ```
   *
   * @param predicate - Filter function (can be context-aware)
   * @returns New Transformer with filter operation applied
   */
  filter(predicate: PipelineFunction<Out, boolean>): Transformer<In, Out> {
    if (isContextAware(predicate)) {
      return this.pipe((chunk, ctx) => chunk.filter((x) => predicate(x, ctx) as boolean));
    }
    return this.pipe((chunk, _ctx) =>
      chunk.filter((x) => (predicate as (item: Out) => boolean)(x)),
    );
  }

  /**
   * Flatten nested arrays in the output.
   *
   * Python equivalent:
   * ```python
   * def flatten[T](
   *   self: Union["Transformer[In, list[T]]", "Transformer[In, tuple[T, ...]]", "Transformer[In, set[T]]"],
   * ) -> "Transformer[In, T]":
   *   return self._pipe(lambda chunk, ctx: [item for sublist in chunk for item in sublist])
   * ```
   *
   * @returns New Transformer with flattened output
   */
  flatten<U>(this: Transformer<In, U[]>): Transformer<In, U> {
    return this.pipe((chunk, _ctx) => chunk.flat());
  }

  /**
   * Map each element and flatten the results.
   *
   * Equivalent to `.map(fn).flatten()` but handles async functions properly.
   * Useful when the mapping function returns an array and you want to flatten the results.
   *
   * @param fn - Mapping function that returns an array (can be async)
   * @returns New Transformer with flatMap operation applied
   */
  flatMap<U>(fn: PipelineFunction<Out, U[]>): Transformer<In, U> {
    if (isContextAware(fn)) {
      return this.pipe(async (chunk, ctx) => {
        const results = await Promise.all(chunk.map((x) => fn(x, ctx) as U[] | Promise<U[]>));
        return results.flat();
      });
    }
    return this.pipe(async (chunk, _ctx) => {
      const results = await Promise.all(
        chunk.map((x) => (fn as (item: Out) => U[] | Promise<U[]>)(x)),
      );
      return results.flat();
    });
  }

  /**
   * Execute side effects for each element without modifying the data.
   *
   * Can be called with either:
   * - A function that receives each element (and optionally context)
   * - A Transformer whose transform function will be executed for side effects
   *
   * Python equivalent:
   * ```python
   * def tap(self, arg: Union["Transformer[Out, Any]", PipelineFunction[Out, Any]]) -> "Transformer[In, Out]":
   *   match arg:
   *     case Transformer() as transformer:
   *       tapped_func = transformer.transformer
   *       return self._pipe(lambda chunk, ctx: chunk if tapped_func(chunk, ctx) or True else chunk)
   *     case function if callable(function):
   *       if is_context_aware(function):
   *         context_aware_func: Callable[[Out, IContextManager], Any] = function
   *         return self._pipe(lambda chunk, ctx: [x for x in chunk if context_aware_func(x, ctx) or True])
   *       non_context_func: Callable[[Out], Any] = function
   *       return self._pipe(lambda chunk, _ctx: [x for x in chunk if non_context_func(x) or True])
   * ```
   */

  // Overload signatures
  tap(fn: PipelineFunction<Out, unknown>): Transformer<In, Out>;
  tap(transformer: Transformer<Out, unknown>): Transformer<In, Out>;
  tap(arg: PipelineFunction<Out, unknown> | Transformer<Out, unknown>): Transformer<In, Out> {
    // Check if arg is a Transformer instance
    if (arg instanceof Transformer) {
      const tappedTransform = arg.transform;
      return this.pipe((chunk, ctx) => {
        // Execute the tapped transformer for side effects only
        tappedTransform(chunk, ctx);
        return chunk;
      });
    }

    // Handle function case
    const fn = arg;
    if (isContextAware(fn)) {
      return this.pipe((chunk, ctx) => {
        chunk.forEach((x) => fn(x, ctx));
        return chunk;
      });
    }

    const nonContextFn = fn as (item: Out) => unknown;
    return this.pipe((chunk, _ctx) => {
      chunk.forEach((x) => nonContextFn(x));
      return chunk;
    });
  }

  /**
   * Apply a transformation function to this transformer.
   *
   * This is a composition helper that allows applying a function that takes
   * this transformer and returns a new one. Useful for extracting reusable
   * transformation chains.
   *
   * Python equivalent:
   * ```python
   * def apply[T](self, t: Callable[[Self], "Transformer[In, T]"]) -> "Transformer[In, T]":
   *   return t(self)
   * ```
   *
   * @param fn - Function that receives this transformer and returns a new one
   * @returns Result of applying the function to this transformer
   */
  apply<U>(fn: (t: this) => Transformer<In, U>): Transformer<In, U> {
    return fn(this);
  }

  /**
   * Register an error handler, returning a NEW transformer that carries it forward.
   *
   * Error handlers are called when chunk processing fails. Multiple handlers
   * can be registered and are called in LIFO (last-in-first-out) order.
   *
   * Copy-on-write, like every other configuration method here: `this` is never mutated, and every
   * later `.map()`/`.filter()`/… (`pipe()`) carries the returned transformer's error handler
   * forward, so `t.onError(fn).map(g)` no longer silently drops `fn` — `pipe()`'s constructor call
   * used to always start a fresh, empty `ErrorHandler`, discarding whatever `onError()` had just set.
   *
   * Python equivalent:
   * ```python
   * def on_error(self, handler: ChunkErrorHandler[In, Out] | ErrorHandler) -> "Transformer[In, Out]":
   *   match handler:
   *     case ErrorHandler():
   *       new_handler = handler
   *     case _ if callable(handler):
   *       new_handler = self.error_handler.clone().on_error(handler)
   *   return Transformer(..., error_handler=new_handler)
   * ```
   *
   * @param handler - Error handler function or ErrorHandler instance (replaces the chain entirely)
   * @returns A new Transformer carrying the updated error handler
   */
  onError(handler: ChunkErrorHandler<In> | ErrorHandler<In>): Transformer<In, Out> {
    const errorHandler =
      handler instanceof ErrorHandler ? handler : this.errorHandler.clone().onError(handler);
    return new Transformer<In, Out>({
      strategy: this.strategy,
      chunkSize: this.chunkSize,
      transform: this.transform,
      hooks: this.hooks,
      chunker: this.chunker,
      errorHandler,
    });
  }

  /**
   * Set a custom chunk generator, returning a NEW transformer that carries it forward.
   *
   * The chunk generator controls how input data is batched for processing.
   * By default, uses the chunkSize to create fixed-size chunks.
   *
   * Copy-on-write, like `.onError()`. The declared `chunker` also makes this knob VISIBLE to
   * `Pipeline`'s `inertKnobsOf` (`pipeline.ts`) the same way `.withExecutor()`/`.withHooks()`/a
   * non-default `chunkSize` already are — a `Pipeline` carrying this transformer now throws
   * naming `"setChunker"` if iterated directly (`for await`) instead of through a terminal op,
   * rather than silently running with the default chunker.
   *
   * Python equivalent:
   * ```python
   * def set_chunker(self, chunker: ChunkGenerator[In]) -> "Transformer[In, Out]":
   *   return Transformer(..., chunker=chunker)
   * ```
   *
   * @param chunker - Function that takes an async iterable and yields chunks
   * @returns A new Transformer carrying the custom chunker
   */
  setChunker(chunker: ChunkerFunction<In>): Transformer<In, Out> {
    return new Transformer<In, Out>({
      strategy: this.strategy,
      chunkSize: this.chunkSize,
      transform: this.transform,
      hooks: this.hooks,
      errorHandler: this.errorHandler,
      chunker,
    });
  }

  /**
   * Repeatedly apply a transformer while a condition is true.
   *
   * The loop continues until the condition returns false or maxIterations is reached.
   * Useful for iterative refinement operations where data needs multiple passes.
   *
   * `condition` is ONE signature, `(chunk, ctx) => boolean` — the same reason `PipelineFunction`
   * (`types.ts`) is one signature rather than a union of arities: a union blocks contextual
   * inference, making an un-annotated `(c) => c.every(...)` an implicit-`any` `c`. A 1-arg caller
   * stays valid by arity flexibility; `condition.length` still tells `ctx`-aware apart at runtime.
   *
   * @param loopTransformer - Transformer to apply on each iteration
   * @param condition - Function that returns true to continue looping
   * @param maxIterations - Optional maximum number of iterations
   * @returns New transformer with loop operation applied
   */
  loop(
    loopTransformer: Transformer<Out, Out>,
    condition: (chunk: Out[], ctx: IContextManager) => boolean,
    maxIterations?: number,
  ): Transformer<In, Out> {
    const loopedTransform = loopTransformer.transform;
    const conditionIsContextAware = condition.length >= 2;

    return this.pipe(async (chunk, ctx) => {
      let currentChunk = chunk;
      let iterations = 0;

      while (true) {
        if (maxIterations !== undefined && iterations >= maxIterations) {
          break;
        }

        const shouldContinue = conditionIsContextAware
          ? condition(currentChunk, ctx)
          : (condition as (chunk: Out[]) => boolean)(currentChunk);

        if (!shouldContinue) {
          break;
        }

        currentChunk = await loopedTransform(currentChunk, ctx);
        iterations++;
      }

      return currentChunk;
    });
  }

  /**
   * Reduce data to a single accumulated value.
   *
   * With `perChunk: true` (default): Each chunk is reduced independently.
   * With `perChunk: false`: Terminal operation that reduces the ENTIRE dataset.
   *
   * Python equivalent:
   * ```python
   * @overload
   * def reduce[U](self, function, initial, *, per_chunk: Literal[True]) -> "Transformer[In, U]": ...
   *
   * @overload
   * def reduce[U](self, function, initial, *, per_chunk: Literal[False] = False)
   *   -> Callable[[Iterable[In], IContextManager | None], Iterator[U]]: ...
   * ```
   *
   * @param fn - Reduce function: (acc, item) => acc or (acc, item, ctx) => acc
   * @param initial - Initial accumulator value
   * @param options - Optional settings: perChunk (default true)
   * @returns Transformer (perChunk=true) or terminal function (perChunk=false)
   */

  // Overload: per-chunk reduce returns a chainable Transformer
  reduce<U>(fn: PipelineReduceFunction<U, Out>, initial: U): Transformer<In, U>;
  reduce<U>(
    fn: PipelineReduceFunction<U, Out>,
    initial: U,
    options: { perChunk: true },
  ): Transformer<In, U>;

  // Overload: terminal reduce returns a callable function
  reduce<U>(
    fn: PipelineReduceFunction<U, Out>,
    initial: U,
    options: { perChunk: false },
  ): (data: AsyncIterable<In> | Iterable<In>, context?: IContextManager) => AsyncGenerator<U>;

  // Implementation
  reduce<U>(
    fn: PipelineReduceFunction<U, Out>,
    initial: U,
    options?: ReduceOptions,
  ):
    | Transformer<In, U>
    | ((data: AsyncIterable<In> | Iterable<In>, context?: IContextManager) => AsyncGenerator<U>) {
    const perChunk = options?.perChunk !== false; // Default to true

    if (perChunk) {
      // Per-chunk reduce: chainable operation
      if (isContextAwareReduce(fn)) {
        return this.pipe((chunk, ctx) => {
          if (chunk.length === 0) return [];
          return [
            chunk.reduce(
              (acc, val) => (fn as (acc: U, item: Out, ctx: IContextManager) => U)(acc, val, ctx),
              initial,
            ),
          ];
        });
      }

      const simpleFn = fn as (acc: U, item: Out) => U;
      return this.pipe((chunk, _ctx) => {
        if (chunk.length === 0) return [];
        return [chunk.reduce(simpleFn, initial)];
      });
    }

    // Terminal reduce: returns a callable that processes entire dataset
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self = this;
    const isContextAware = isContextAwareReduce(fn);

    return async function* terminalReduce(
      data: AsyncIterable<In> | Iterable<In>,
      context?: IContextManager,
    ): AsyncGenerator<U> {
      const runContext = context ?? new SimpleContextManager();

      // Execute the transformer to get all processed items
      const asyncData = self.toAsyncIterable(data);
      const processedItems = self.execute(asyncData, runContext);

      // Reduce all items to a single value
      let accumulator = initial;
      for await (const item of processedItems) {
        if (isContextAware) {
          accumulator = (fn as (acc: U, item: Out, ctx: IContextManager) => U)(
            accumulator,
            item,
            runContext,
          );
        } else {
          accumulator = (fn as (acc: U, item: Out) => U)(accumulator, item);
        }
      }

      yield accumulator;
    };
  }

  /**
   * Convert sync iterable to async iterable.
   */
  private toAsyncIterable<T>(data: AsyncIterable<T> | Iterable<T>): AsyncIterable<T> {
    if (Symbol.asyncIterator in data) {
      return data as AsyncIterable<T>;
    }
    const syncData = data as Iterable<T>;
    return {
      [Symbol.asyncIterator]: async function* () {
        for (const item of syncData) {
          yield item;
        }
      },
    };
  }

  /**
   * Execute a sub-pipeline with error handling.
   *
   * If the sub-pipeline throws an error, the error handler is invoked and
   * an empty array is returned for that chunk. This allows graceful recovery
   * from errors during processing.
   *
   * Python equivalent:
   * ```python
   * def catch[U](
   *   self,
   *   sub_pipeline_builder: Callable[["Transformer[Out, Out]"], "Transformer[Out, U]"],
   *   on_error: ChunkErrorHandler[Out, None] | None = None,
   * ) -> "Transformer[In, U]":
   *   ...
   * ```
   *
   * @param subPipelineBuilder - Function that builds the sub-pipeline to execute
   * @param onError - Optional error handler called when an error occurs
   * @returns New transformer with error handling applied
   */
  catch<U>(
    subPipelineBuilder: (t: Transformer<Out, Out>) => Transformer<Out, U>,
    onError?: ChunkErrorHandler<Out>,
  ): Transformer<In, U> {
    const catchErrorHandler = new ErrorHandler<Out>();

    if (onError) {
      catchErrorHandler.onError(onError);
    }

    const tempTransformer = new Transformer<Out, Out>({
      chunkSize: this.chunkSize,
      transform: (chunk) => chunk,
    });
    const subPipeline = subPipelineBuilder(tempTransformer);
    const subTransform = subPipeline.transform;

    return this.pipe(async (chunk, ctx) => {
      try {
        return await subTransform(chunk, ctx);
      } catch (error) {
        catchErrorHandler.handle(chunk, error as Error, ctx);
        return [];
      }
    });
  }

  /**
   * Stop processing when a condition is met.
   *
   * When the condition function returns true, throws an error to halt
   * the pipeline execution. Useful for implementing early exit conditions.
   *
   * Python equivalent:
   * ```python
   * def short_circuit(self, function: Callable[[IContextManager], bool | None]) -> "Transformer[In, Out]":
   *   def operation(chunk: list[Out], ctx: IContextManager) -> list[Out]:
   *     if function(ctx):
   *       raise RuntimeError("Short-circuit condition met, stopping execution.")
   *     return chunk
   *   return self._pipe(operation)
   * ```
   *
   * @param fn - Function that returns true to stop execution
   * @returns New transformer with short-circuit condition applied
   */
  shortCircuit(fn: (ctx: IContextManager) => boolean): Transformer<In, Out> {
    return this.pipe((chunk, ctx) => {
      if (fn(ctx)) {
        throw new Error("Short-circuit condition met, stopping execution.");
      }
      return chunk;
    });
  }

  // ===== Executor Configuration =====

  /**
   * Create a new Transformer with a different execution strategy, preserving the current
   * transformation chain. `strategy` is a plain `ExecutionStrategy` function — a built-in
   * (`sequential`/`concurrent(options)`) or a caller's own — never a name or a spec object, so
   * there is nothing else to register or look up.
   *
   * Usage:
   * ```typescript
   * // Use concurrent execution with 8 workers
   * const transformer = createTransformer<number>()
   *   .map(x => x * 2)
   *   .withExecutor(concurrent({ maxConcurrency: 8 }))
   *
   * // Use a custom executor
   * const transformer = createTransformer<number>()
   *   .map(x => x * 2)
   *   .withExecutor(async function* (logic, chunks, ctx) {
   *     for await (const chunk of chunks) yield logic(chunk, ctx);
   *   })
   * ```
   *
   * @param strategy - The execution strategy to switch to
   * @returns New Transformer with the specified execution strategy
   */
  withExecutor(strategy: ExecutionStrategy<In, Out>): Transformer<In, Out> {
    return new Transformer<In, Out>({
      strategy,
      chunkSize: this.chunkSize,
      transform: this.transform,
      hooks: this.hooks,
      errorHandler: this.errorHandler,
      chunker: this.chunker,
    });
  }
}
