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

import type { IContextManager, PipelineFunction, PipelineReduceFunction } from "@src/types";

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
 * Type guard that checks if a reduce function is context-aware (takes 3+ parameters).
 *
 * Context-aware reduce functions have the signature: `(acc: U, item: T, ctx: IContextManager) => U`
 * Non-context-aware reduce functions have the signature: `(acc: U, item: T) => U`
 *
 * Python equivalent:
 * ```python
 * def is_context_aware_reduce(func: Callable) -> bool:
 *   sig = inspect.signature(func)
 *   params = list(sig.parameters.values())
 *   return len(params) >= 3
 * ```
 *
 * @param fn - The reduce function to check
 * @returns True if the function takes a context parameter
 *
 * @example
 * ```typescript
 * const simple = (acc: number, x: number) => acc + x;
 * const withContext = (acc: number, x: number, ctx: IContextManager) =>
 *   acc + x * ctx.getOrDefault('multiplier', 1);
 *
 * isContextAwareReduce(simple);      // false (fn.length === 2)
 * isContextAwareReduce(withContext); // true (fn.length === 3)
 * ```
 */
export function isContextAwareReduce<U, Out>(
  fn: PipelineReduceFunction<U, Out>,
): fn is (acc: U, item: Out, ctx: IContextManager) => U | Promise<U> {
  return fn.length >= 3;
}
