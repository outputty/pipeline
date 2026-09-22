/** Cuts streams into chunks, flattens them back, and shares, prefetches or collects them. */

import type { ChunkerFunction } from "@src/types";
import { chain, isThenable } from "@src/utils/helpers";
import { drainSync, type MaybeAsyncChunks } from "@src/utils/drain";
import { isEncodedChunk, materialize } from "@src/utils/encoded-chunk";

/** Refuses a chunk size below 1, for the chunk cutters.
 *
 * `assertPositiveChunkSize(0)` throws `Error("chunkSize must be at least 1")`;
 * `assertPositiveChunkSize(3)` returns. */
export function assertPositiveChunkSize(size: number): void {
  if (size < 1) {
    throw new Error("chunkSize must be at least 1");
  }
}

/** Refuses a numeric knob that is not a whole number of at least 1, naming the knob in the error.
 *
 * `assertWholeNumberAtLeastOne("queue capacity", 0)` throws `Error("queue capacity must be a whole
 * number of at least 1")`; `assertWholeNumberAtLeastOne("queue capacity", 3)` returns. */
export function assertWholeNumberAtLeastOne(label: string, value: number): void {
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(`${label} must be a whole number of at least 1`);
  }
}

/**
 * Builds a function that cuts an async iterable into chunks of at most `chunkSize` items.
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
  assertPositiveChunkSize(chunkSize);

  return async function* chunkGenerator(data: AsyncIterable<T>): AsyncGenerator<T[]> {
    let chunk: T[] = [];

    for await (const item of data) {
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
 * Flattens a chunk stream into its items, in order, so `.buffer()` can re-cut it. Decodes any
 * encoded chunk on the way.
 *
 * A stream of `[1, 2]` then `[3]` → yields `1`, `2`, `3`.
 */
export async function* flattenChunks<T>(chunks: AsyncIterable<T[]>): AsyncGenerator<T> {
  for await (const chunk of chunks) {
    // Test before calling `materialize`: it is `async`, so calling it costs a Promise per chunk.
    yield* isEncodedChunk(chunk) ? await materialize(chunk) : chunk;
  }
}

/**
 * Turns a synchronously-cut chunk stream into an async one, for a chain on the async engine whose
 * data is in memory. A pending chunk is awaited.
 *
 * `asAsyncChunks([[1, 2], Promise.resolve([3])])` yields `[1, 2]`, then `[3]`.
 */
export async function* asAsyncChunks<T>(chunks: MaybeAsyncChunks<T>): AsyncGenerator<T[]> {
  for (const chunk of chunks) yield isThenable(chunk) ? await chunk : chunk;
}

/**
 * `buildChunkGenerator` over a synchronous `Iterable`, so an in-memory source is cut without
 * becoming async. An array is cut with `slice`.
 *
 * ⚠ Test `Array.isArray`, never `length`. A string has a `length` too, and `slice` would cut
 * `"abcd"` into `"ab"` rather than `["a", "b"]`.
 *
 * ⚠ Keep `Number.isInteger` on the `slice` arm. `slice` truncates a fractional size, so `2.5` would
 * cut an array differently from a `Set` holding the same items.
 *
 * `[...buildSyncChunkGenerator<number>(3)([1, 2, 3, 4, 5, 6, 7])]` → `[[1, 2, 3], [4, 5, 6], [7]]`.
 */
export function buildSyncChunkGenerator<T>(
  chunkSize: number,
): (data: Iterable<T>) => Generator<T[]> {
  assertPositiveChunkSize(chunkSize);

  return function* chunkGenerator(data: Iterable<T>): Generator<T[]> {
    if (Array.isArray(data) && Number.isInteger(chunkSize)) {
      for (let i = 0; i < data.length; i += chunkSize) {
        yield (data as T[]).slice(i, i + chunkSize);
      }
      return;
    }

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
 * Lets several consumers pull from one iterator: each item goes to whichever consumer asks next.
 * `ConcurrentPipeline.reduce()` deals its chunks to its partitions this way.
 *
 * ⚠ Never forwards `.return()`, so one consumer's `break` does not close the iterator for the rest.
 *
 * Two `share(it)` consumers over an iterator of `[0..8]` → each item reaches exactly one of them.
 */
export function share<T>(iterator: AsyncIterator<T>): AsyncIterable<T> {
  return {
    [Symbol.asyncIterator]() {
      return { next: () => iterator.next() };
    },
  };
}

/**
 * Fetches up to `capacity` chunks ahead of the consumer, so the source keeps working while the
 * consumer is busy. The chunks and their order are unchanged. `.queue(capacity)` uses it.
 *
 * ⚠ Keep it a generator: its body runs on the first pull, so nothing is fetched at construction.
 *
 * `prefetch(upstream, 3)` → the same chunks as `upstream`, with up to 3 already requested.
 */
export async function* prefetch<T>(
  upstream: AsyncIterable<T[]>,
  capacity: number,
): AsyncGenerator<T[]> {
  const iterator = upstream[Symbol.asyncIterator]();
  const pending: Promise<IteratorResult<T[]>>[] = [];

  const pull = (): void => {
    const next = iterator.next();
    // ⚠ Marks it handled: a chunk never reached after an earlier throw must not crash the process.
    next.catch(() => {});
    pending.push(next);
  };

  try {
    for (let i = 0; i < capacity; i++) pull();
    for (let step = await pending.shift()!; !step.done; step = await pending.shift()!) {
      pull();
      yield step.value;
    }
  } finally {
    await iterator.return?.();
  }
}

/**
 * Collects a pipeline result's items into an array, stopping once `limit` items are in hand. It
 * returns a plain array when the chain is synchronous. `.toArray()` and `.first(n)` use it.
 *
 * `syncChunks` is the synchronous chunk view, or `null` on the async engine, where `chunks` is read.
 *
 * `collectItems([[1, 2], [3]], unused)` → `[1, 2, 3]`; with `limit` 2 → `[1, 2]`. No `Promise`.
 */
export function collectItems<T>(
  syncChunks: MaybeAsyncChunks<T> | null,
  chunks: () => AsyncIterable<T[]>,
  limit?: number,
): T[] | Promise<T[]> {
  const results: T[] = [];
  if (syncChunks === null) return collectAsyncChunks(results, limit, chunks);
  return chain(
    drainSync(syncChunks, (item) => {
      results.push(item);
      return limit !== undefined && results.length >= limit;
    }),
    () => results,
  );
}

async function collectAsyncChunks<T>(
  results: T[],
  limit: number | undefined,
  chunks: () => AsyncIterable<T[]>,
): Promise<T[]> {
  for await (const chunk of chunks()) {
    const take =
      limit === undefined ? chunk.length : Math.min(chunk.length, limit - results.length);
    for (let i = 0; i < take; i++) results.push(chunk[i]);
    if (limit !== undefined && results.length >= limit) break;
  }
  return results;
}
