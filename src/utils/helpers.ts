/**
 * Helper utilities for checking function signatures.
 *
 * Python equivalent:
 * ```python
 * def is_context_aware(func: Callable) -> bool:
 *   sig = inspect.signature(func)
 *   params = list(sig.parameters.values())
 *   return len(params) >= 2
 * ```
 */

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
 * ```typescript
 * const simple = (x: number) => x * 2;
 * const withContext = (x: number, ctx: IContextManager) => x * ctx.getOrDefault('multiplier', 1);
 *
 * isContextAware(simple);      // false (fn.length === 1)
 * isContextAware(withContext); // true (fn.length === 2)
 * ```
 */
export function isContextAware<Out, T>(
  fn: PipelineFunction<Out, T>,
): fn is (item: Out, ctx: IContextManager) => T | Promise<T> {
  return fn.length >= 2;
}

/**
 * The RUN-handler decision every per-chunk catch site shares (#78): `Transformer.process()`'s own
 * `runSequentially` loop (a local stage) and `ConcurrentPipeline.apply()`'s wrapped `work` (a
 * dispatched one) - the two sites #40 built and #78 reuses, so "call the handler, or propagate"
 * lives once rather than twice. No handler registered → rethrow (today's behaviour, the run dies).
 * A handler that itself throws (or a caller who writes `(e) => { throw e; }`) still propagates - it
 * is not caught here, so it escalates past this call to whatever awaits the caller. `runHandler` is
 * declared as bare `void` (`.claude/rules/typescript.md`), which still accepts an `async` callback -
 * this function is itself `async` and `await`s the call so a Promise-returning handler's own
 * rejection is caught HERE rather than becoming an unhandled rejection the caller never sees: with
 * no `await`, a handler that decides to rethrow only after an `await` of its own would resolve this
 * function normally (its caller then treats the chunk as dropped) before that rejection ever
 * surfaces.
 *
 * `await dropOrRethrow(undefined, err, ctx)` throws `err`. `await dropOrRethrow((e) => log(e), err,
 * ctx)` calls the handler and returns normally - the caller drops the chunk and continues.
 */
export async function dropOrRethrow(
  runHandler: PipelineErrorHandler | undefined,
  error: Error,
  ctx: IContextManager,
): Promise<void> {
  if (!runHandler) throw error;
  await runHandler(error, ctx);
}

/**
 * True when `value` is a thenable - the one place a "did this stay synchronous?" decision is made
 * (#90). Structural, not `instanceof Promise`: a caller's own thenable, a `PromiseLike` from another
 * realm and a native `Promise` all have to widen the chain the same way.
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
 * Runs `next` on `value`, creating NO `Promise` when `value` is not already one (#90) - the
 * replacement for every `await` on a composition seam, so a chain whose callbacks all return plain
 * values runs start to finish without a microtask. When `value` IS a thenable the call defers
 * through `.then`, which is the chain widening to async exactly where the first async link sits.
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
 * Collects per-item results into one array, staying synchronous when NO item is pending (#90) -
 * `Promise.all`'s replacement wherever a chunk's items were mapped one at a time. `Promise.all`
 * always allocates and always defers, even over an array of plain values, which is what made a
 * fully-synchronous `.map()` cost a microtask per chunk before this.
 *
 * `settleMaybe([1, 2])` → `[1, 2]`, no `Promise` created. `settleMaybe([1, Promise.resolve(2)])` →
 * a `Promise` of `[1, 2]`.
 */
export function settleMaybe<T>(values: (T | PromiseLike<T>)[]): T[] | Promise<T[]> {
  return values.some((value) => isThenable(value)) ? Promise.all(values) : (values as T[]);
}
