/**
 * Top-level factory functions for building a `Transformer` directly, without the `new Transformer()`
 * constructor's options object. Their own module: `index.ts` is a barrel only — re-exports, zero
 * logic — so these live here rather than inline in it.
 */

import { Transformer } from "./transformer";
import { DEFAULT_CHUNK_SIZE } from "./types";

/**
 * Create a new sequential Transformer.
 *
 * Python equivalent:
 * ```python
 * def create_transformer[T](_type_hint: type[T], chunk_size: int = DEFAULT_CHUNK_SIZE) -> Transformer[T, T]:
 *   return Transformer[T, T](chunk_size=chunk_size)
 * ```
 *
 * @param chunkSize - Number of items per chunk (default: 1000)
 * @returns A new Transformer with sequential execution
 */
export function createTransformer<T>(chunkSize = DEFAULT_CHUNK_SIZE): Transformer<T, T> {
  return new Transformer<T, T>({ chunkSize });
}
