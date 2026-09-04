/**
 * Error handler for managing chunk processing errors.
 *
 * Python equivalent:
 * ```python
 * class ErrorHandler:
 *   def __init__(self) -> None:
 *     self._handlers: list[ChunkErrorHandler] = []
 *
 *   def on_error(self, handler: ChunkErrorHandler) -> "ErrorHandler":
 *     self._handlers.insert(0, handler)
 *     return self
 *
 *   def handle(self, chunk: list, error: Exception, context: IContextManager) -> None:
 *     [handler(chunk, error, context) for handler in self._handlers]
 * ```
 */

import type { IContextManager } from "@src/types";

/**
 * Type for chunk error handler functions.
 */
export type ChunkErrorHandler<In> = (chunk: In[], error: Error, ctx: IContextManager) => void;

/**
 * ErrorHandler manages a chain of error handlers for chunk processing errors.
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
export class ErrorHandler<In> {
  private handlers: ChunkErrorHandler<In>[] = [];

  /**
   * Register an error handler.
   *
   * Handlers are called in reverse order of registration (LIFO).
   *
   * @param handler - Function to call when an error occurs
   * @returns This ErrorHandler for chaining
   */
  onError(handler: ChunkErrorHandler<In>): this {
    // Insert at beginning (matches Python behavior)
    this.handlers.unshift(handler);
    return this;
  }

  /**
   * Call all registered handlers with the error context.
   *
   * Handlers are called in order (newest first, oldest last).
   *
   * @param chunk - The chunk that caused the error
   * @param error - The error that occurred
   * @param ctx - The context manager
   */
  handle(chunk: In[], error: Error, ctx: IContextManager): void {
    for (const handler of this.handlers) {
      handler(chunk, error, ctx);
    }
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
  clone(): ErrorHandler<In> {
    const copy = new ErrorHandler<In>();
    // Copy handlers in same order
    copy.handlers = [...this.handlers];
    return copy;
  }
}
