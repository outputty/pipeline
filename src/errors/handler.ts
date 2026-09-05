/**
 * Error handler for managing chunk processing errors.
 *
 * Python equivalent:
 * ```python
 * type Handler = Callable[[list, Exception, IContextManager], list | None]
 *
 * class ErrorHandler:
 *   def __init__(self) -> None:
 *     self._handlers: list[Handler] = []
 *
 *   def on_error(self, handler: Handler) -> "ErrorHandler":
 *     self._handlers.insert(0, handler)
 *     return self
 *
 *   def handle(self, chunk: list, error: Exception, context: IContextManager) -> list | None:
 *     winner = None
 *     for handler in self._handlers:
 *       result = handler(chunk, error, context)
 *       if winner is None:
 *         winner = result
 *     return winner
 * ```
 */

import type { IContextManager } from "@src/types";

/**
 * ErrorHandler manages a chain of error handlers for chunk processing errors.
 *
 * `In` is the type of chunk a registered handler receives; `U` is the type of array a handler may
 * return to replace that chunk. `types.ts` exports this same shape as its own public handler
 * type, for callers to annotate a standalone handler against - this class takes the signature
 * inline rather than re-declaring or re-importing that type under a second name here.
 *
 * Handlers are called in reverse order of registration (last registered = first called),
 * matching the Python behavior where handlers are inserted at the beginning.
 *
 * @example
 * ```typescript
 * const handler = new ErrorHandler<number>();
 *
 * handler
 *   .onError((chunk, error, ctx) => {
 *     console.log(`First handler: ${error.message}`);
 *   })
 *   .onError((chunk, error, ctx) => {
 *     console.log(`Second handler (called first): ${error.message}`);
 *   });
 *
 * handler.handle([1, 2, 3], new Error('test'), ctx);
 * // Output:
 * // Second handler (called first): test
 * // First handler: test
 * ```
 */
export class ErrorHandler<In, U = void> {
  private handlers: ((chunk: In[], error: Error, ctx: IContextManager) => U[] | void)[] = [];

  /**
   * Register an error handler.
   *
   * Handlers are called in reverse order of registration (LIFO).
   *
   * @param handler - Function to call when an error occurs
   * @returns This ErrorHandler for chaining
   */
  onError(handler: (chunk: In[], error: Error, ctx: IContextManager) => U[] | void): this {
    // Insert at beginning (matches Python behavior)
    this.handlers.unshift(handler);
    return this;
  }

  /**
   * Call every registered handler with the error context, LIFO order (last registered runs
   * first) — EVERY handler still runs, unconditionally, the same as before a handler's return
   * value meant anything (a chained `.onError().onError()` notification pair must both still
   * fire). The FIRST one to return an array is the winner: its return value is `handle()`'s own
   * return value. A handler returning `undefined` only means it did not win - it is not skipped,
   * and does not stop a later (earlier-registered) handler from running or winning in its place.
   *
   * `Transformer.catch()` (`transformer.ts`) is the one caller that uses this return value, to
   * replace or drop the failing chunk. `Transformer.execute()`'s own call, `handle([], error,
   * ctx)` on a strategy-level failure, is a notification only and ignores it — `.onError()` is a
   * hook, not a recovery path there.
   *
   * @param chunk - The chunk that caused the error
   * @param error - The error that occurred
   * @param ctx - The context manager
   * @returns The first (LIFO-order) handler's replacement array, or `undefined` if none replaced
   *   the chunk
   *
   * @example
   * `handle(["a"], err, ctx)` with handlers `[() => undefined, () => [999]]` (registration order)
   * runs the second handler first (LIFO), which wins with `[999]`; the first handler still runs
   * but its `undefined` never overrides an earlier win from a handler registered after it.
   */
  handle(chunk: In[], error: Error, ctx: IContextManager): U[] | void {
    let winner: U[] | void = undefined;
    for (const handler of this.handlers) {
      const result = handler(chunk, error, ctx);
      if (winner === undefined) {
        winner = result;
      }
    }
    return winner;
  }

  /**
   * Check if any handlers are registered.
   *
   * @returns True if at least one handler is registered
   */
  hasHandlers(): boolean {
    return this.handlers.length > 0;
  }

  /**
   * Create a copy of this ErrorHandler with all handlers.
   *
   * @returns A new ErrorHandler with the same handlers
   */
  clone(): ErrorHandler<In, U> {
    const copy = new ErrorHandler<In, U>();
    // Copy handlers in same order
    copy.handlers = [...this.handlers];
    return copy;
  }
}
