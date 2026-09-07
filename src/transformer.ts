/**
 * Transformer class - chainable chunk transformation operations.
 *
 * Python equivalent:
 * ```python
 * class Transformer[In, Out](BaseTransformer[In, Out]):
 *   def __init__(
 *     self,
 *     transformer: InternalTransformer[In, Out] | None = None,
 *   ) -> None:
 *     ...
 *
 *   def __call__(self, chunks: Iterable[list[In]], context: IContextManager | None = None) -> Iterator[list[Out]]:
 *     ...
 * ```
 */

import type {
  InternalTransformer,
  IContextManager,
  TransformerOptions,
  PipelineFunction,
  ReduceFunction,
  TransformerLifecycleHooks,
  ChunkErrorHandler,
} from "./types";
import { SimpleContextManager } from "./context/simple";
import { ErrorHandler } from "./errors/handler";
import { isContextAware } from "./utils/helpers";
import { Reducer, foldChunk } from "./utils/reduce";

/**
 * Construction-time knobs shared by every `Transformer<In, Out>` constructor overload below —
 * named once rather than repeated per overload.
 */
type TransformerConstructorOptions<In, Out> = TransformerOptions<In, Out> & {
  hooks?: TransformerLifecycleHooks<In, Out>;
  errorHandler?: ErrorHandler<In>;
};

/**
 * The one chunk-draining order a standalone `Transformer.process()` runs, one chunk at a time, in
 * order (#17: replaces the deleted `sequential` execution strategy, which had the identical body -
 * a `ConcurrentPipeline`/`HttpPipeline`/`ClusterPipeline` is the replacement for concurrency,
 * wrapping the chain rather than configuring the `Transformer` that drives it).
 *
 * `runSequentially(logic, chunks, ctx)` → each output chunk yielded in input order.
 */
async function* runSequentially<In, Out>(
  transformerLogic: InternalTransformer<In, Out>,
  chunks: AsyncIterable<In[]>,
  context: IContextManager,
): AsyncGenerator<Out[]> {
  for await (const chunk of chunks) {
    yield transformerLogic(chunk, context);
  }
}

/**
 * A reusable, composable stage: `In` chunks in, `Out` chunks out, applied chunk by chunk - it never
 * decides how its own input was cut (#39: chunking lives on `Pipeline`'s `.buffer()` alone, never
 * here). Built by chaining (`.map()`, `.filter()`, `.reduce()`, …), each call returning a NEW
 * `Transformer` so one can be shared across pipelines without aliasing. It carries its own error
 * handling, which is what lets `pipeline.apply(t)` stay a one-liner. Every configuration method
 * (`.onError()`, `.withHooks()`) is copy-on-write like `.map()`/`.filter()`: it returns a NEW
 * `Transformer` carrying every other knob forward, never mutates `this` — the shape
 * `@outputty/laygo`'s own `Model#named` follows.
 *
 * `new Transformer<number, number>().map((n) => n * 2)` → a transformer a pipeline can `.apply()`.
 */
export class Transformer<In, Out> {
  /** The internal transform function */
  readonly transform: InternalTransformer<In, Out>;

  /** Error handler chain for this transformer */
  readonly errorHandler: ErrorHandler<In>;

  /** Lifecycle hooks for monitoring execution progress */
  readonly hooks?: TransformerLifecycleHooks<In, Out>;

  /** Default context to use when none provided */
  private defaultContext: IContextManager;

  /**
   * Overload 1 — a real `transform` in hand. Not conditional on `In extends Out`, so it resolves
   * (and is preferred) even from inside this class's OWN generic methods, where `In`/`Out` are
   * still abstract type parameters a deferred conditional can never distribute over. Every
   * copy-on-write rebuild site below (`.pipe()`, `.withHooks()`, `.onError()`) already carries a
   * real `transform` forward, so all of them land here.
   */
  constructor(
    options: TransformerConstructorOptions<In, Out> & { transform: InternalTransformer<In, Out> },
  );
  /**
   * Overload 2 — no `transform` given. Only sound when `In` is assignable to `Out` (the identity
   * default below is a real conversion, not a lie): `new Transformer<number, { id: number }>()`
   * is a compile error here, `new Transformer<number, number>()` is not. Resolves only when `In`/
   * `Out` are CONCRETE types at the call site — a deferred conditional never distributes over an
   * abstract type parameter, so ANY generic scope still holding `In`/`Out` unresolved (this class's
   * own methods, but equally `createTransformer<T>()` or a caller's own generic helper) fails to
   * resolve it too and must route through overload 1 with a real `transform` instead, even where
   * `In`/`Out` happen to be the same type parameter (`T extends T` is never specially recognized).
   */
  constructor(...args: In extends Out ? [options?: TransformerConstructorOptions<In, Out>] : never);
  constructor(options?: TransformerConstructorOptions<In, Out>) {
    // Reachable only via overload 2's `In extends Out` branch — a real conversion there, not a lie.
    this.transform = options?.transform ?? ((chunk, _ctx) => chunk as unknown as Out[]);
    this.errorHandler = options?.errorHandler ?? new ErrorHandler<In>();
    this.defaultContext = new SimpleContextManager();
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
      transform: this.transform,
      errorHandler: this.errorHandler,
      hooks,
    });
  }

  /**
   * Run this transformer over chunks the CALLER already cut - chunk in, chunk out, no chunking
   * knowledge of its own (#39). The one seam every `Pipeline` class drives it through:
   * `Pipeline.apply()` hands it `this._chunks` directly, and a caller running a `Transformer`
   * standalone supplies its own already-cut `AsyncIterable<In[]>`.
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
   * def __call__(self, chunks: Iterable[list[In]], context: IContextManager | None = None) -> Iterator[list[Out]]:
   *   run_context = context if context is not None else self._default_context
   *   for chunk in chunks:
   *     yield self.transformer(chunk, run_context)
   * ```
   *
   * On a chunk failure, `this.errorHandler.handle([], error, runContext)` fires (any
   * handler registered via `.onError()`) BEFORE the error re-throws — a notification, not a
   * recovery path, so the failure still propagates to the caller.
   *
   * @param chunks - Async iterable of already-cut input chunks
   * @param context - Optional context manager for sharing state
   * @returns Async generator of output chunks
   *
   * @example
   * ```typescript
   * const t = new Transformer<number, number>().map((x) => x * 2);
   * for await (const chunk of t.process(chunksOf([1, 2, 3]))) console.log(chunk); // [2, 4, 6]
   *
   * // onError wiring: a throwing transform still propagates, but the handler sees it first.
   * let seen: Error | undefined;
   * const failing = new Transformer<number, number>()
   *   .map(() => { throw new Error("boom"); })
   *   .onError((e) => { seen = e; });
   * await expect(failing.process(chunksOf([1])).next()).rejects.toThrow("boom");
   * // seen.message === "boom"
   * ```
   */
  async *process(chunks: AsyncIterable<In[]>, context?: IContextManager): AsyncGenerator<Out[]> {
    const runContext = context ?? this.defaultContext;
    const startTime = Date.now();
    const itemCounter = { index: 0 };

    try {
      await this.hooks?.onStart?.();

      const hasItemHooks =
        this.hooks?.onItemStart ?? this.hooks?.onItemComplete ?? this.hooks?.onItemError;

      if (hasItemHooks) {
        const wrappedTransform = this.wrapTransformForItemHooks(itemCounter);
        yield* runSequentially(wrappedTransform, chunks, runContext);
      } else {
        yield* this.countOutputItems(
          runSequentially(this.transform, chunks, runContext),
          itemCounter,
        );
      }

      const totalDurationMs = Date.now() - startTime;
      await this.hooks?.onComplete?.(itemCounter.index, totalDurationMs);
    } catch (error) {
      await this.hooks?.onError?.(error as Error);
      // Registered via `.onError()` (above) — fires on any chunk failure, THEN the error still
      // propagates (`.onError()` is a notification hook, not a recovery path; `.catch()` is the
      // sub-pipeline that actually recovers). The offending chunk is not tracked at this scope, so
      // handlers see `[]` — same as every OTHER caller of this `process()` catch, letting a
      // handler tell error-occurred from error-details.
      this.errorHandler.handle([], error as Error, runContext);
      throw error;
    }
  }

  /**
   * Yields each chunk unchanged, advancing `counter.index` by its OUTPUT length first - the
   * no-item-hooks fast path's own item count for `onComplete` (a filter can shrink a chunk, so the
   * count is read from what is actually yielded, not the input). Its own method purely to keep
   * `process()` within this repo's own `max-depth: 2` rule.
   *
   * @example
   * `countOutputItems([[1, 2], [3]], counter)` yields `[1, 2]` then `[3]`, leaving
   * `counter.index` at `3`.
   */
  private async *countOutputItems(
    chunks: AsyncIterable<Out[]>,
    counter: { index: number },
  ): AsyncGenerator<Out[]> {
    for await (const chunk of chunks) {
      counter.index += chunk.length;
      yield chunk;
    }
  }

  /**
   * Build a transform that emits `onItemStart`/`onItemComplete`/`onItemError`
   * hooks around each item of a chunk, one item at a time.
   *
   * Runs only when `process()` detects at least one item-level hook attached.
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
      transform: newTransform,
      // errorHandler is keyed on `In`, unaffected by the Out -> U change, so it carries forward -
      // this is what keeps `.onError(fn).map(g)` from silently dropping the configuration this
      // pipe() call would otherwise discard.
      errorHandler: this.errorHandler,
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
      return this.pipe((chunk, ctx) => Promise.all(chunk.map((x) => fn(x, ctx))) as Promise<U[]>);
    }
    return this.pipe(
      (chunk, _ctx) =>
        Promise.all(chunk.map((x) => (fn as (item: Out) => U | Promise<U>)(x))) as Promise<U[]>,
    );
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
      return this.pipe(async (chunk, ctx) => {
        const keep = await Promise.all(chunk.map((x) => predicate(x, ctx)));
        return chunk.filter((_x, i) => keep[i]);
      });
    }
    return this.pipe(async (chunk, _ctx) => {
      const fn = predicate as (item: Out) => boolean | Promise<boolean>;
      const keep = await Promise.all(chunk.map((x) => fn(x)));
      return chunk.filter((_x, i) => keep[i]);
    });
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
      return this.pipe(async (chunk, ctx) => {
        // Execute the tapped transformer for side effects only, awaited before the chunk moves on
        await tappedTransform(chunk, ctx);
        return chunk;
      });
    }

    // Handle function case
    const fn = arg;
    if (isContextAware(fn)) {
      return this.pipe(async (chunk, ctx) => {
        await Promise.all(chunk.map((x) => fn(x, ctx)));
        return chunk;
      });
    }

    const nonContextFn = fn as (item: Out) => unknown;
    return this.pipe(async (chunk, _ctx) => {
      await Promise.all(chunk.map((x) => nonContextFn(x)));
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
   * The function arm is typed as a bare `void` return, never `ChunkErrorHandler<In>` (`void[] |
   * void` with its default `U`) — a union loses the void-return exemption TypeScript grants a
   * literal `void`, so an ordinary `(chunk, err) => arr.push(err)` would stop compiling
   * (`.claude/rules/typescript.md`, 2026-09-05). This handler's return is ignored regardless (see
   * `process()`'s own catch, above) - `.onError()` is a notification hook here, never `.catch()`'s
   * recovery path, so bare `void` also states that intent.
   *
   * Python equivalent:
   * ```python
   * def on_error(self, handler: ChunkErrorHandler[In, None] | ErrorHandler) -> "Transformer[In, Out]":
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
  onError(
    handler: ((chunk: In[], error: Error, ctx: IContextManager) => void) | ErrorHandler<In>,
  ): Transformer<In, Out> {
    const errorHandler =
      handler instanceof ErrorHandler ? handler : this.errorHandler.clone().onError(handler);
    return new Transformer<In, Out>({
      transform: this.transform,
      hooks: this.hooks,
      errorHandler,
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
   * Folds this ONE chunk into one or more values via `emit` - no state survives to the next chunk,
   * `Pipeline.reduce()`'s the only place cross-chunk state lives (#45; replaces the deleted
   * whole-dataset terminal form entirely - `Pipeline.reduce()`, run over an actual `Pipeline`, is
   * its replacement).
   *
   * @param fn - `(acc, item, ctx, emit) => acc` - called with all four arguments regardless of its
   *   own declared arity (JS ignores extras), so `(acc, item) => acc` and `(acc, item, ctx, emit) =>
   *   …` both work.
   * @param initial - Initial accumulator value, reset for every chunk.
   * @returns A new `Transformer` whose output is whatever `emit()` pushed plus the trailing
   *   accumulator (only if items were folded since the last emit).
   *
   * @example
   * `new Transformer<number, number>().reduce((acc, x) => acc + x, 0)` over chunks `[[1,2],[3]]` →
   * `[3]` then `[3]` (each chunk's own independent sum).
   */
  reduce<U>(fn: ReduceFunction<U, Out>, initial: U): Transformer<In, U> {
    return this.pipe(async (chunk, ctx) => {
      if (chunk.length === 0) return [];
      const reducer = new Reducer<U, Out>(fn, initial);
      const values = await foldChunk(reducer, chunk, ctx);
      values.push(...reducer.final());
      return values;
    });
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
   * If the sub-pipeline throws, `onError` is invoked with the failing chunk and the error. Its
   * return value REPLACES the chunk (an array) or DROPS it (`undefined`, or no `onError` given) -
   * `ErrorHandler.handle()` (`errors/handler.ts`) is what resolves that value when several
   * handlers are chained onto `onError` (#15).
   *
   * Python equivalent:
   * ```python
   * def catch[U](
   *   self,
   *   sub_pipeline_builder: Callable[["Transformer[Out, Out]"], "Transformer[Out, U]"],
   *   on_error: ChunkErrorHandler[Out, U] | None = None,
   * ) -> "Transformer[In, U]":
   *   ...
   * ```
   *
   * @param subPipelineBuilder - Function that builds the sub-pipeline to execute
   * @param onError - Optional error handler called when an error occurs; its returned array
   *   replaces the failing chunk, `undefined` drops it
   * @returns New transformer with error handling applied
   */
  catch<U>(
    subPipelineBuilder: (t: Transformer<Out, Out>) => Transformer<Out, U>,
    onError?: ChunkErrorHandler<Out, U>,
  ): Transformer<In, U> {
    const catchErrorHandler = new ErrorHandler<Out, U>();

    if (onError) {
      catchErrorHandler.onError(onError);
    }

    const tempTransformer = new Transformer<Out, Out>({
      transform: (chunk) => chunk,
    });
    const subPipeline = subPipelineBuilder(tempTransformer);
    const subTransform = subPipeline.transform;

    return this.pipe(async (chunk, ctx) => {
      try {
        return await subTransform(chunk, ctx);
      } catch (error) {
        // `handle()` (`errors/handler.ts`) returns the first registered handler's replacement
        // array, LIFO order; `undefined` (no handler, or every one passed) drops the chunk.
        const replacement = catchErrorHandler.handle(chunk, error as Error, ctx);
        return replacement ?? [];
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
}
