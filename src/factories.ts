/**
 * Top-level factory functions for building a `Transformer` directly, without the `new Transformer()`
 * constructor's options object. Their own module: `index.ts` is a barrel only — re-exports, zero
 * logic — so these live here rather than inline in it.
 */

import { Transformer } from "./transformer";

/**
 * Create a new sequential Transformer. Chunk-agnostic - it takes no chunk size; the caller's
 * `Pipeline` decides that via `.buffer(size)`.
 *
 * @returns A new Transformer with sequential execution
 */
export function createTransformer<T>(): Transformer<T, T> {
  return new Transformer<T, T>({ transform: (chunk) => chunk });
}
