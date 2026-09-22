/** Small helpers that keep synchronous chains synchronous, and recover from failures. */

import type { IContextManager, PipelineFunction, PipelineErrorHandler } from "@src/types";

/**
 * Whether a pipeline callback declares a second, context parameter.
 *
 * ⚠ Reads `fn.length`, so `(x, ctx = d) => …` and `(x, ...rest) => …` count as NOT context-aware.
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
 * Applies `Pipeline.onError()`'s handler to a failed chunk. Returning means "drop the chunk and
 * continue"; with no handler, or a handler that throws, the error propagates.
 *
 * `dropOrRethrow(undefined, err, ctx)` throws `err`. `dropOrRethrow((e) => log(e), err, ctx)` calls
 * the handler and returns `undefined`.
 */
export function dropOrRethrow(
  runHandler: PipelineErrorHandler | undefined,
  error: Error,
  ctx: IContextManager,
): void | Promise<void> {
  if (!runHandler) throw error;
  // ⚠ Return an async handler's promise, so a rethrow after its own `await` reaches the caller.
  const result: unknown = runHandler(error, ctx);
  return isThenable(result) ? Promise.resolve(result).then(() => undefined) : undefined;
}

/**
 * Whether `value` is a thenable, which is what turns a chain async.
 *
 * ⚠ A structural test, not `instanceof Promise`: a caller's own thenable must count too.
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
 * Applies `next` to `value`, waiting only if `value` is a thenable. Use it in place of `await` so
 * a synchronous chain creates no `Promise`.
 *
 * `chain(2, (x) => x * 2)` → `4`, no `Promise` created. `chain(Promise.resolve(2), (x) => x * 2)` →
 * a `Promise` of `4`.
 */
export function chain<A, B>(
  value: A | Promise<A>,
  next: (resolved: A) => B | Promise<B>,
): B | Promise<B> {
  return isThenable(value) ? Promise.resolve(value).then(next) : next(value);
}

/**
 * `Promise.all` that returns the array itself when no value is pending.
 *
 * `settleMaybe([1, 2])` → `[1, 2]`, no `Promise` created. `settleMaybe([1, Promise.resolve(2)])` →
 * a `Promise` of `[1, 2]`.
 */
export function settleMaybe<T>(values: (T | PromiseLike<T>)[]): T[] | Promise<T[]> {
  return values.some((value) => isThenable(value)) ? Promise.all(values) : (values as T[]);
}

/**
 * Maps `run` over a chunk and settles the results, returning a plain array when none is pending.
 *
 * ⚠ Not a bare `chunk.map(run)`: when `run` throws synchronously, the promises already created
 * must be marked handled, or one rejecting later crashes the process.
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
 * Marks every pending value in `created` as handled, for promises nothing will await after a
 * synchronous throw.
 *
 * `disarm([Promise.reject(new Error("x"))])` → `undefined`, and no unhandled rejection.
 */
export function disarm<R>(created: (R | Promise<R>)[]): void {
  for (const value of created) {
    if (isThenable(value)) void Promise.resolve(value).catch(() => {});
  }
}

/**
 * Runs `attempt`, and hands a synchronous throw or a rejection alike to `recover`. It returns a
 * plain value when `attempt` does.
 *
 * `tryRecover(() => JSON.parse("3"), () => -1)` → `3`; `tryRecover(() => JSON.parse("x"), () => -1)`
 * → `-1`. No `Promise` created.
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
 * Runs one chunk through a stage, applying `Pipeline.onError()`'s handler on failure. A dropped
 * chunk comes back as `[]`.
 *
 * `runStageChunk(doubler, [1, 2], ctx)` → `[2, 4]`, no `Promise` created. A failing stage with a
 * handler that returns → `[]`.
 */
export function runStageChunk<In, Out>(
  runnable: (chunk: In[], ctx: IContextManager) => Out[] | Promise<Out[]>,
  chunk: In[],
  ctx: IContextManager,
  runHandler?: PipelineErrorHandler,
): Out[] | Promise<Out[]> {
  return tryRecover(
    () => runnable(chunk, ctx),
    (error) => chain(dropOrRethrow(runHandler, error, ctx), () => [] as Out[]),
  );
}
