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
import { isThenable } from "@src/utils/helpers";

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

/**
 * Flattens a chunk stream into its items, in order (#39) - the one place a chunk becomes items
 * again, shared by `Pipeline`'s own terminal ops and `.buffer()`'s re-cut fallback.
 *
 * @example
 * `[...flattenChunks([[1, 2], [3]])]` → `[1, 2, 3]`.
 */
export async function* flattenChunks<T>(chunks: AsyncIterable<T[]>): AsyncGenerator<T> {
  for await (const chunk of chunks) {
    yield* chunk;
  }
}

/**
 * `buildChunkGenerator`'s synchronous counterpart (#90) - identical cutting, over an `Iterable`
 * rather than an `AsyncIterable`, so an in-memory source never becomes an async iterator just to be
 * chunked. That per-item conversion, not the per-chunk `Promise.all`, is where most of the old
 * cost sat: a chain with ZERO transform stages still paid it.
 *
 * Not a shared implementation with the async one: a `for await` loop and a `for` loop are different
 * statements, and an `async function*` is async even when its input is not - there is no body both
 * can share that stays synchronous for this one.
 *
 * `[...buildSyncChunkGenerator<number>(3)([1, 2, 3, 4, 5, 6, 7])]` → `[[1, 2, 3], [4, 5, 6], [7]]`.
 */
export function buildSyncChunkGenerator<T>(
  chunkSize: number,
): (data: Iterable<T>) => Generator<T[]> {
  if (chunkSize < 1) {
    throw new Error("chunkSize must be at least 1");
  }

  return function* chunkGenerator(data: Iterable<T>): Generator<T[]> {
    let chunk: T[] = [];

    for (const item of data) {
      chunk.push(item);

      if (chunk.length >= chunkSize) {
        yield chunk;
        chunk = [];
      }
    }

    if (chunk.length > 0) {
      yield chunk;
    }
  };
}

/**
 * `flattenChunks`'s synchronous counterpart (#90) - the one place a sync chunk stream becomes its
 * items again, shared by the sync terminal ops and `.buffer()`'s own sync re-cut fallback.
 *
 * `[...flattenSyncChunks([[1, 2], [3]])]` → `[1, 2, 3]`.
 */
export function* flattenSyncChunks<T>(chunks: Iterable<T[]>): Generator<T> {
  for (const chunk of chunks) {
    yield* chunk;
  }
}

/**
 * A sync chunk stream whose individual chunks may still be pending (#90) - what a `"sync"`-Mode
 * `Pipeline` carries. A stage whose callbacks were all synchronous puts a plain array in; one that
 * returned a thenable puts a `Promise` in, and that is where the run widens to async.
 */
export type MaybeAsyncChunks<T> = Iterable<T[] | Promise<T[]>>;

/**
 * Drains a `MaybeAsyncChunks` stream item by item into `onItem`, staying synchronous until the first
 * pending chunk (#90) - the ONE drain every synchronous terminal op goes through (`toArray`,
 * `first`, `consume`, `forEach`), so the "did this stay synchronous?" decision and the early-exit
 * decision each live in one place rather than four.
 *
 * `onItem` returning `true` stops the drain, which is what `.first(n)` needs; returning anything
 * else continues. A pending chunk hands the rest of the stream to a `.then` continuation running on
 * its own microtask, so a long stream never grows the stack.
 *
 * `drainSync(chunksOf([[1, 2], [3]]), (x) => out.push(x) && false)` → `undefined`, no `Promise`
 * created, with `out` `[1, 2, 3]`.
 */
export function drainSync<T>(
  chunks: MaybeAsyncChunks<T>,
  onItem: (item: T) => boolean | void,
): void | Promise<void> {
  const iterator = chunks[Symbol.iterator]();

  // A plain loop for the synchronous case; `resume` re-enters only across an async boundary.
  const resume = (): void | Promise<void> => {
    for (;;) {
      const step = iterator.next();
      if (step.done === true) return;

      const chunk = step.value;
      if (isThenable(chunk)) {
        return Promise.resolve(chunk).then((settled) => (pushAll(settled, onItem) ? undefined : resume()));
      }
      if (pushAll(chunk, onItem)) return;
    }
  };

  return resume();
}

/**
 * Re-cuts an already-staged sync chunk stream at a new boundary (#90) - `.buffer()`'s own fallback
 * once a real stage has consumed the pre-buffer item view, so the re-cut runs over that stage's
 * OUTPUT rather than the original source.
 *
 * Settled chunks re-cut exactly, synchronously, at `size`. A PENDING chunk cannot: its items are not
 * known yet, and a sync generator has to decide it is done before that promise could resolve. From
 * the first pending chunk on, the remainder is therefore delivered as ONE pending chunk rather than
 * several of `size` - correct items, coarser boundary. A pending chunk only exists once a callback
 * returned a thenable, which is a chain the overloads already typed `"async"`, so no chain that
 * `.toArray()` types `T[]` ever reaches this arm.
 *
 * `[...recutSyncChunks([[1, 2], [3, 4, 5]], 2)]` → `[[1, 2], [3, 4], [5]]`.
 */
export function* recutSyncChunks<T>(
  chunks: MaybeAsyncChunks<T>,
  size: number,
): Generator<T[] | Promise<T[]>> {
  if (size < 1) {
    throw new Error("chunkSize must be at least 1");
  }

  let carry: T[] = [];
  const iterator = chunks[Symbol.iterator]();

  for (;;) {
    const step = iterator.next();
    if (step.done === true) break;

    const chunk = step.value;
    if (isThenable(chunk)) {
      yield collectRest(carry, chunk, iterator);
      return;
    }

    carry.push(...chunk);
    while (carry.length >= size) {
      yield carry.slice(0, size);
      carry = carry.slice(size);
    }
  }

  if (carry.length > 0) {
    yield carry;
  }
}

/** Everything left in a re-cut once a pending chunk is met: the carry, that chunk, and every chunk
 * after it, as one settled array. Its own function to keep `recutSyncChunks` at this repo's
 * `max-depth: 2`. */
async function collectRest<T>(
  carry: T[],
  pending: Promise<T[]>,
  iterator: Iterator<T[] | Promise<T[]>>,
): Promise<T[]> {
  const rest = [...carry, ...(await pending)];
  for (;;) {
    const step = iterator.next();
    if (step.done === true) return rest;
    rest.push(...(await step.value));
  }
}

/** Hands one settled chunk's items to `onItem`, reporting whether it asked to stop. Its own
 * function to keep `drainSync`'s loop at this repo's `max-depth: 2`. */
function pushAll<T>(chunk: T[], onItem: (item: T) => boolean | void): boolean {
  for (const item of chunk) {
    if (onItem(item) === true) return true;
  }
  return false;
}

/**
 * Wraps an existing iterator as an `AsyncIterable` that pulls from that SAME iterator on every
 * `.next()` call. Calling `share()` N times over one iterator and handing each result to its own
 * consumer is free-slot dealing (`ConcurrentPipeline.reduce()`, #62, fans one chunk stream out to
 * `maxConcurrency` independent partitions this way): whichever consumer calls `.next()` next gets
 * the next item, with no dealer, no per-consumer queue and no backpressure mechanism of its own - a
 * slow consumer simply calls `.next()` less often, so the other consumers pick up its slack.
 * Deliberately never delegates `.return()`/`.throw()`: one consumer stopping early (a `for await`
 * `break`) must not close the shared iterator out from under every other consumer still pulling
 * from it.
 *
 * @example
 * 3 consumers sharing one iterator over `[0..8]`, consumer 0 made 30x slower than the other two ->
 * per-consumer `[[0],[1,3,5,7],[2,4,6,8]]`, union 9 of 9 distinct, 0 duplicates.
 */
export function share<T>(iterator: AsyncIterator<T>): AsyncIterable<T> {
  return {
    [Symbol.asyncIterator]() {
      return { next: () => iterator.next() };
    },
  };
}
