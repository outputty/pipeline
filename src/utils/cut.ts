import type { ChunkerFunction } from "@src/types";
import { chain } from "@src/utils/helpers";
import { drainSync, dispatchSync, type MaybeAsyncChunks } from "@src/utils/drain";

/** The `chunkSize`/`size` guard `buildChunkGenerator`, `buildSyncChunkGenerator` and
 * `recut.ts`'s own `recutSyncChunks` each need before doing any real work. The message stays
 * verbatim - `sync-mode.e2e.test.ts` asserts it.
 *
 * `assertPositiveChunkSize(0)` throws `Error("chunkSize must be at least 1")`;
 * `assertPositiveChunkSize(3)` returns, no error. */
export function assertPositiveChunkSize(size: number): void {
  if (size < 1) {
    throw new Error("chunkSize must be at least 1");
  }
}

/** The `capacity`/`size` guard a NUMERIC knob shares across two call sites - `.buffer(size)`'s own
 * deferred-branch check and `.queue(capacity)` - labelled so each throws under its own name rather
 * than a generic one. Kept apart from `assertPositiveChunkSize` above: that one's own message is
 * asserted verbatim by `sync-mode.e2e.test.ts` and is never the wording a caller-facing knob like
 * `.buffer(fn)`'s `size` overload or `.queue()` owes its user.
 *
 * `assertWholeNumberAtLeastOne("queue capacity", 0)` throws `Error("queue capacity must be a whole
 * number of at least 1")`; `assertWholeNumberAtLeastOne("queue capacity", 3)` returns. */
export function assertWholeNumberAtLeastOne(label: string, value: number): void {
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(`${label} must be a whole number of at least 1`);
  }
}

/**
 * Build a chunking function that breaks an async iterable into chunks of a specified size.
 *
 * @param chunkSize - Maximum number of items per chunk
 * @returns A function that takes an AsyncIterable and yields chunks
 *
 * @example
 * ```ts
 * const chunker = buildChunkGenerator<number>(3);
 * const data = (async function* () { for (let i = 1; i <= 7; i++) yield i; })();
 *
 * for await (const chunk of chunker(data)) {
 *   console.log(chunk);
 * }
 * // → [1, 2, 3], [4, 5, 6], [7]
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
 * ```ts
 * async function* mixed() {
 *   yield { id: 1 };
 *   yield [{ id: 2 }, { id: 3 }];
 *   yield { id: 4 };
 * }
 * for await (const chunk of normalize(mixed())) {
 *   console.log(chunk);
 * }
 * // → [{id:1}], [{id:2},{id:3}], [{id:4}]
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
 * Flattens a chunk stream into its items, in order - the one place a chunk becomes items again,
 * shared by `Pipeline`'s own terminal ops and `.buffer()`'s re-cut fallback.
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
 * `buildChunkGenerator`'s synchronous counterpart - identical cutting, over an `Iterable` rather
 * than an `AsyncIterable`, so an in-memory source never becomes an async iterator just to be
 * chunked. That per-item conversion, not the per-chunk `Promise.all`, is where most of the cost of
 * a fully synchronous chain would otherwise sit: a chain with ZERO transform stages still pays it
 * without this.
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
  assertPositiveChunkSize(chunkSize);

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
 * Wraps an existing iterator as an `AsyncIterable` that pulls from that SAME iterator on every
 * `.next()` call. Calling `share()` N times over one iterator and handing each result to its own
 * consumer is free-slot dealing (`ConcurrentPipeline.reduce()` fans one chunk stream out to
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

/**
 * Prefetches up to `capacity` chunks ahead of the consumer - an already-cut chunk stream in, the
 * same stream out, only WHEN each chunk is fetched changes. Written as a plain `async function*`
 * deliberately, mirroring `ConcurrentPipeline`'s own `fanOutOrdered` (`src/pipelines/concurrent.ts`)
 * rather than a hand-rolled `AsyncIterable` object: a generator's body does not run at all until its
 * OWN first `.next()` call, which is what makes "the pump starts on the first consumer pull, not at
 * construction" free rather than a flag this function has to track itself. The same guarantee makes
 * concurrent callers safe with NO manual locking - the language serializes concurrent `.next()` calls
 * on one generator instance into one resumption at a time, so `share()`-based fan-out
 * (`ConcurrentPipeline.reduce()`'s own partitioning) can wrap this generator's iterator exactly the
 * way it wraps `_chunks`' own, with the identical no-dealer fairness `share()` already documents.
 *
 * `pending` holds exactly `capacity` `upstream.next()` calls at every steady-state point: the first
 * `capacity` are issued before the first chunk is ever yielded, and each `.shift()` is followed by
 * one more `upstream.next()` call, keeping the window full until `upstream` reports done. Every
 * pushed promise gets a throwaway `.catch(() => {})` the instant it is created (the ORIGINAL
 * reference is what `pending` holds and what a later `await` re-throws for real) - `fanOutOrdered`'s
 * own comment explains why: without it, a chunk queued `capacity` deep but never reached because an
 * EARLIER one threw first is an unhandled rejection, not a caught one.
 *
 * No `Promise.race` anywhere: a single async generator source serializes its own internal work
 * regardless of how many `.next()` calls are already in flight, so racing them buys no overlap. The
 * overlap this function buys comes from PRODUCTION and CONSUMPTION running concurrently (the source
 * keeps working while the consumer processes an earlier chunk), never from concurrent production
 * itself.
 *
 * @example
 * `prefetch(upstream, 3)` over a 100ms/item source feeding a 30ms/item consumer, 5 items: the fully
 * serial baseline (no queue) runs ~671ms; queued, ~539ms - overlap, same 5 outputs, same order.
 */
export async function* prefetch<T>(
  upstream: AsyncIterable<T[]>,
  capacity: number,
): AsyncGenerator<T[]> {
  const iterator = upstream[Symbol.asyncIterator]();
  const pending: Promise<IteratorResult<T[]>>[] = [];

  const pull = (): void => {
    const next = iterator.next();
    next.catch(() => {});
    pending.push(next);
  };

  try {
    for (let i = 0; i < capacity; i++) pull();
    yield* drainPrefetched(pending, pull);
  } finally {
    await iterator.return?.();
  }
}

/** `prefetch()`'s own steady-state loop, its own function so the `try/finally` around it (which
 * must wrap the WHOLE pump, not just this loop, so an early `.return()` during the initial fill
 * still closes `iterator`) costs one nesting level, not two (this repo's own `max-depth: 2`). */
async function* drainPrefetched<T>(
  pending: Promise<IteratorResult<T[]>>[],
  pull: () => void,
): AsyncGenerator<T[]> {
  for (;;) {
    const { done, value } = await pending.shift()!;
    if (done) return;
    pull();
    yield value;
  }
}

/**
 * Collects a bound pipeline's items to an array, staying synchronous when the chain is, and
 * stopping early once `limit` items are in hand - the ONE collect every caller shares:
 * `PipelineResult.toArray()`, its `first(n)` (which IS `toArray` with a limit), and `.branch()`,
 * which collects the parent chain before routing.
 *
 * `syncChunks` is `Pipeline.drainable()`'s own sync view, `null` on the async engine, where `items`
 * is read instead.
 *
 * `collectItems(chunksOf([[1, 2], [3]]), noItems)` → `[1, 2, 3]`, no `Promise` created.
 */
export function collectItems<T>(
  syncChunks: MaybeAsyncChunks<T> | null,
  items: () => AsyncIterable<T>,
  limit?: number,
): T[] | Promise<T[]> {
  const results: T[] = [];
  return dispatchSync(
    syncChunks,
    (chunks) =>
      chain(
        drainSync(chunks, (item) => {
          results.push(item);
          return limit !== undefined && results.length >= limit;
        }),
        () => results,
      ),
    () => collectAsyncItems(results, limit, items),
  );
}

/** `collectItems`'s async arm, its own function so the caller above stays one expression per
 * engine. */
async function collectAsyncItems<T>(
  results: T[],
  limit: number | undefined,
  items: () => AsyncIterable<T>,
): Promise<T[]> {
  for await (const item of items()) {
    results.push(item);
    if (limit !== undefined && results.length >= limit) break;
  }
  return results;
}
