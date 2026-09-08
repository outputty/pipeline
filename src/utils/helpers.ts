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
