import type { IContextManager, PipelineFunction, PipelineErrorHandler } from "@src/types";

/**
 * Type guard that checks if a pipeline function is context-aware (takes 2+ parameters).
 *
 * Context-aware functions have the signature: `(item: T, ctx: IContextManager) => U`
 * Non-context-aware functions have the signature: `(item: T) => U`
 *
 * @param fn - The pipeline function to check
 * @returns True if the function takes a context parameter
 *
 * @example
 * ```ts
 * const simple = (x: number) => x * 2;
 * const withContext = (x: number, ctx: IContextManager) => x * ctx.getOrDefault('multiplier', 1);
 *
 * isContextAware(simple);      // → false (fn.length === 1)
 * isContextAware(withContext); // → true (fn.length === 2)
 * ```
 */
export function isContextAware<Out, T>(
  fn: PipelineFunction<Out, T>,
): fn is (item: Out, ctx: IContextManager) => T | Promise<T> {
  return fn.length >= 2;
}

/**
 * The RUN-handler decision every per-chunk catch site shares: `Transformer.process()`'s own
 * `runSequentially` loop (a local stage) and `ConcurrentPipeline.apply()`'s wrapped `work` (a
 * dispatched one) - so "call the handler, or propagate" lives once rather than twice. No handler
 * registered → rethrow (the run dies). A handler that itself throws (or a caller who writes `(e) =>
 * { throw e; }`) still propagates - it is not caught here, so it escalates past this call to
 * whatever awaits the caller. `runHandler` is declared as bare `void`, which still accepts an
 * `async` callback - this function is itself `async` and `await`s the call so a Promise-returning
 * handler's own rejection is caught HERE rather than becoming an unhandled rejection the caller
 * never sees: with no `await`, a handler that decides to rethrow only after an `await` of its own
 * would resolve this function normally (its caller then treats the chunk as dropped) before that
 * rejection ever surfaces.
 *
 * `await dropOrRethrow(undefined, err, ctx)` throws `err`. `await dropOrRethrow((e) => log(e), err,
 * ctx)` calls the handler and returns normally - the caller drops the chunk and continues.
 */
export function dropOrRethrow(
  runHandler: PipelineErrorHandler | undefined,
  error: Error,
  ctx: IContextManager,
): void | Promise<void> {
  if (!runHandler) throw error;
  // Dual-mode: a SYNCHRONOUS handler returns here without creating a `Promise`, so a
  // `"sync"`-Mode chain that drops a chunk still returns its array rather than silently widening to
  // a `Promise` its own compile-time type never promised. The async arm keeps the reason above: the
  // rejection is settled HERE, so a handler that decides to rethrow only after an `await` of its own
  // still reaches the caller instead of becoming an unhandled rejection.
  const result: unknown = runHandler(error, ctx);
  return isThenable(result) ? Promise.resolve(result).then(() => undefined) : undefined;
}

/**
 * True when `value` is a thenable - the one place a "did this stay synchronous?" decision is made.
 * Structural, not `instanceof Promise`: a caller's own thenable, a `PromiseLike` from another realm
 * and a native `Promise` all have to widen the chain the same way.
 *
 * `isThenable(1)` → `false`. `isThenable(Promise.resolve(1))` → `true`.
 */
export function isThenable<T>(value: T | PromiseLike<T>): value is PromiseLike<T> {
  return (
    value !== null &&
    (typeof value === "object" || typeof value === "function") &&
    typeof (value as PromiseLike<T>).then === "function"
  );
}

/**
 * Runs `next` on `value`, creating NO `Promise` when `value` is not already one - the replacement
 * for every `await` on a composition seam, so a chain whose callbacks all return plain values runs
 * start to finish without a microtask. When `value` IS a thenable the call defers through `.then`,
 * which is the chain widening to async exactly where the first async link sits.
 *
 * `chain(2, (x) => x * 2)` → `4`, no `Promise` created. `chain(Promise.resolve(2), (x) => x * 2)` →
 * a `Promise` of `4`.
 */
export function chain<A, B>(
  value: A | Promise<A>,
  next: (resolved: A) => B | Promise<B>,
): B | Promise<B> {
  // `Promise.resolve` on an already-native `Promise` returns that same instance, so the async arm
  // allocates nothing extra; it is here to normalize a caller's own non-native thenable.
  return isThenable(value) ? Promise.resolve(value).then(next) : next(value);
}

/**
 * Collects per-item results into one array, staying synchronous when NO item is pending -
 * `Promise.all`'s replacement wherever a chunk's items were mapped one at a time. `Promise.all`
 * always allocates and always defers, even over an array of plain values, which is what makes a
 * fully-synchronous `.map()` cost a microtask per chunk without this.
 *
 * `settleMaybe([1, 2])` → `[1, 2]`, no `Promise` created. `settleMaybe([1, Promise.resolve(2)])` →
 * a `Promise` of `[1, 2]`.
 */
export function settleMaybe<T>(values: (T | PromiseLike<T>)[]): T[] | Promise<T[]> {
  return values.some((value) => isThenable(value)) ? Promise.all(values) : (values as T[]);
}

/**
 * Runs `run` over every item of `chunk` and settles the results, staying synchronous when none is
 * pending - the ONE per-item map every element-wise link goes through, rather than a bare
 * `chunk.map(...)` at each site.
 *
 * The bare form is unsafe here: `run` is a caller's own callback, so it can throw SYNCHRONOUSLY for
 * item `i` after items `0..i-1` already returned pending promises. `Array.prototype.map` abandons
 * the array at that point, leaving those promises with no rejection handler ever attached - one of
 * them rejecting then crashes the process under Node's default unhandled-rejection policy. This loop
 * attaches a throwaway `.catch` to whatever was already created, then rethrows.
 *
 * `mapSettle([1, 2], (x) => x * 2)` → `[2, 4]`, no `Promise` created.
 */
export function mapSettle<T, R>(chunk: T[], run: (item: T) => R | Promise<R>): R[] | Promise<R[]> {
  const results: (R | Promise<R>)[] = [];
  try {
    for (const item of chunk) {
      results.push(run(item));
    }
  } catch (error) {
    disarm(results);
    throw error;
  }
  return settleMaybe(results);
}

/**
 * Attaches a throwaway rejection handler to every pending value in `created` - what `mapSettle`
 * above owes the siblings of an item whose callback threw synchronously, since nothing downstream
 * will ever await them. Its own function to keep `mapSettle`'s `catch` at this repo's `max-depth:
 * 2`.
 */
function disarm<R>(created: (R | Promise<R>)[]): void {
  for (const value of created) {
    if (isThenable(value)) void Promise.resolve(value).catch(() => {});
  }
}

/**
 * The try/catch-if-thenable/recover skeleton every ROW- or CHUNK-level recovery site shares: try
 * `attempt`; a synchronous throw OR a rejected `Promise` both route to `recover`. Neither path
 * builds a closure this function doesn't already need - `recover` is a plain function the CALLER
 * already holds, invoked directly at the failure site, never wrapped or hoisted here. Serves
 * `runStageChunk` below and `transformer.ts`'s own `attemptRow`.
 *
 * `Reducer.fold` (`utils/reduce.ts`) does NOT use this, by decision: its own docstring records a
 * measured perf note (372.5 ns/item for a hoisted commit/recover pair against 8.9 ns/item inlined)
 * that is specifically about its PER-ITEM fold path - the two sites this helper serves are each
 * called at most once per chunk's own row or once per chunk, never once per item inside a hot fold,
 * so the trade that note rejects for `Reducer.fold` does not apply here.
 *
 * `tryRecover(() => parseStrict("3"), () => -1)` → `3`, no `Promise` created, `recover` never
 * called.
 */
export function tryRecover<R>(
  attempt: () => R | Promise<R>,
  recover: (error: Error) => R | Promise<R>,
): R | Promise<R> {
  try {
    const result = attempt();
    return isThenable(result) ? Promise.resolve(result).catch(recover) : result;
  } catch (error) {
    return recover(error as Error);
  }
}

/**
 * Runs one chunk through a stage on the `"sync"` engine, applying `Pipeline.onError()`'s own RUN
 * handler exactly as `runSequentially` does for the async engine - the two engines must agree on
 * what a chunk failure means, and `dropOrRethrow` is where that decision already lives.
 *
 * A dropped chunk becomes `[]` rather than disappearing: the sync stream is a generator of chunks,
 * so an empty chunk is how "this one contributed nothing" is spelled. A synchronous handler keeps
 * the whole thing synchronous; an async one widens the run from this chunk on.
 *
 * `runStageChunk(doubler, [1, 2], ctx, undefined)` → `[2, 4]`, no `Promise` created.
 */
export function runStageChunk<In, Out>(
  runnable: (chunk: In[], ctx: IContextManager) => Out[] | Promise<Out[]>,
  chunk: In[],
  ctx: IContextManager,
  runHandler?: PipelineErrorHandler,
): Out[] | Promise<Out[]> {
  return tryRecover(
    () => runnable(chunk, ctx),
    (error) => dropChunk<Out>(runHandler, error, ctx),
  );
}

/** One chunk's failure answer: run the handler, then contribute nothing. Its own function so
 * `runStageChunk`'s happy path never builds a closure for it. */
function dropChunk<Out>(
  runHandler: PipelineErrorHandler | undefined,
  error: Error,
  ctx: IContextManager,
): Out[] | Promise<Out[]> {
  return chain(dropOrRethrow(runHandler, error, ctx), () => [] as Out[]);
}
