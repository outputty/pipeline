/**
 * Chunk utilities for breaking data into manageable pieces.
 *
 * Python equivalent:
 * ```python
 * def build_chunk_generator[T](chunk_size: int) -> Callable[[Iterable[T]], Iterator[list[T]]]:
 *   def chunk_generator(data: Iterable[T]) -> Iterator[list[T]]:
 *     data_iter = iter(data)
 *     while chunk := list(itertools.islice(data_iter, chunk_size)):
 *       yield chunk
 *   return chunk_generator
 * ```
 */

import type { ChunkerFunction } from "@src/types";

/**
 * Build a chunking function that breaks an async iterable into chunks of a specified size.
 *
 * @param chunkSize - Maximum number of items per chunk
 * @returns A function that takes an AsyncIterable and yields chunks
 *
 * @example
 * ```typescript
 * const chunker = buildChunkGenerator<number>(3);
 * const data = (async function* () { for (let i = 1; i <= 7; i++) yield i; })();
 *
 * for await (const chunk of chunker(data)) {
 *   console.log(chunk);
 * }
 * // Output: [1, 2, 3], [4, 5, 6], [7]
 * ```
 */
export function buildChunkGenerator<T>(chunkSize: number): ChunkerFunction<T> {
  if (chunkSize < 1) {
    throw new Error("chunkSize must be at least 1");
  }

  return async function* chunkGenerator(data: AsyncIterable<T>): AsyncGenerator<T[]> {
    let chunk: T[] = [];

    for await (const item of data) {
      chunk.push(item);

      if (chunk.length >= chunkSize) {
        yield chunk;
        chunk = [];
      }
    }

    // Yield any remaining items as the final chunk
    if (chunk.length > 0) {
      yield chunk;
    }
  };
}

/**
 * Normalize a mixed stream of single items and pre-chunked arrays into chunks.
 *
 * Runs whenever a source stream mixes loose items with already-chunked arrays
 * (e.g. an ingestion source that occasionally emits a batch). Non-array items
 * are buffered in arrival order; the buffer is flushed as a chunk whenever an
 * array item is encountered (the array itself passes through as its own chunk,
 * unwrapped) or when the stream ends. Order is always preserved.
 *
 * @param stream - Async iterable yielding either loose items or arrays of items
 * @returns An async generator of chunks (arrays), in stream order
 *
 * @example
 * ```typescript
 * async function* mixed() {
 *   yield { id: 1 };
 *   yield [{ id: 2 }, { id: 3 }];
 *   yield { id: 4 };
 * }
 * for await (const chunk of normalize(mixed())) {
 *   console.log(chunk);
 * }
 * // Output: [{id:1}], [{id:2},{id:3}], [{id:4}]
 * ```
 */
export async function* normalize<T>(stream: AsyncIterable<T | T[]>): AsyncGenerator<T[]> {
  let buffer: T[] = [];

  for await (const item of stream) {
    if (!Array.isArray(item)) {
      buffer.push(item as T);
      continue;
    }
    if (buffer.length > 0) {
      yield buffer;
      buffer = [];
    }
    yield item;
  }

  if (buffer.length > 0) {
    yield buffer;
  }
}
