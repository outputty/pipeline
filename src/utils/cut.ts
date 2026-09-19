/**
 * Cutting a stream into chunks, flattening chunks back to items, sharing one iterator across several
 * consumers, collecting a drain to an array, and prefetching a shared iterator's own chunks ahead of
 * the consumer (`prefetch`, #123) - split out of `chunk.ts` along with `drain.ts` (draining a
 * `MaybeAsyncChunks` stream) and `recut.ts` (re-cutting an already-staged one), re-exported from
 * `chunk.ts` so nothing importing that barrel has to change. `prefetch` lands here rather than a
 * dedicated file: the ticket's own file scope named `share()` as its neighbor, and #133's own split
 * groups real seams, not one file per function - a prefetching helper over a shared iterator is the
 * same "sharing" family `share()` itself is.
 */

import type { ChunkerFunction } from "@src/types";
import { chain } from "@src/utils/helpers";
import { drainSync, dispatchSync, type MaybeAsyncChunks } from "@src/utils/drain";
import { isEncodedChunk, materialize } from "@src/utils/encoded-chunk";

/** The `chunkSize`/`size` guard `buildChunkGenerator`, `buildSyncChunkGenerator` and
 * `recut.ts`'s own `recutSyncChunks` each need before doing any real work (#133: was spelled
 * inline 3x, collapsed to this one call). The message stays verbatim -
 * `sync-mode.e2e.test.ts` asserts it.
 *
 * `assertPositiveChunkSize(0)` throws `Error("chunkSize must be at least 1")`;
 * `assertPositiveChunkSize(3)` returns, no error. */
export function assertPositiveChunkSize(size: number): void {
  if (size < 1) {
    throw new Error("chunkSize must be at least 1");
  }
}

/** The `capacity`/`size` guard every NUMERIC knob shares, labelled so each throws under its own name
 * rather than a generic one. Four call sites: `.buffer(size)`'s deferred branch and its bound branch
 * (#179 - the bound one used to validate through `sizeReduceFunction`, deleted with the fold engine's
 * numeric arm), `.queue(capacity)` (#123), and the constructor's own `chunkSize` (#179).
 *
 * Kept apart from `assertPositiveChunkSize` above: that one's own message is asserted verbatim by
 * `sync-mode.e2e.test.ts` and is never the wording a caller-facing knob owes its user.
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

    // Yield any remaining items as the final chunk
    if (chunk.length > 0) {
      yield chunk;
    }
  };
}

/**
 * Flattens a chunk stream into its items, in order (#39) - the one place a chunk becomes items
 * again for `.buffer()`'s re-cut fallback. Materializes each chunk first (#209): an encoded chunk
 * a dispatched stage left behind is decoded here, since a re-cut needs real items to slice.
 *
 * @example
 * `[...flattenChunks([[1, 2], [3]])]` → `[1, 2, 3]`.
 */
export async function* flattenChunks<T>(chunks: AsyncIterable<T[]>): AsyncGenerator<T> {
  for await (const chunk of chunks) {
    // `isEncodedChunk` checked synchronously first (#209) - `materialize` is `async`, so awaiting
    // it costs a Promise even on its own no-op fast path; skipping the call for a real chunk keeps
    // this generator's per-chunk cost at what it was before the encoded-chunk mechanism existed.
    yield* isEncodedChunk(chunk) ? await materialize(chunk) : chunk;
  }
}

/**
 * Hands a synchronously-cut chunk stream to a consumer that needs an `AsyncIterable` (#179) - the
 * seam between `buildSyncChunkGenerator` above and the two places a chain is on the async engine
 * while its DATA is not: `fromSource()`'s own forced-async branch over an array, and `.buffer(size)`
 * re-cutting a source a dispatching class pinned async.
 *
 * One `Promise` per CHUNK, where `buildChunkGenerator` over `toAsyncIterable(data)` pays one per
 * ROW twice over - `toAsyncIterable`'s own `Promise.resolve` per pull, then the cutter's `for await`
 * on top. Nothing here can be pending: `buildSyncChunkGenerator` yields real arrays, so no `await`
 * on the yielded value is needed and none is written.
 *
 * `[...] = await Array.fromAsync(asAsyncChunks([[1, 2], [3]]))` → `[[1, 2], [3]]`.
 */
export async function* asAsyncChunks<T>(chunks: Iterable<T[]>): AsyncGenerator<T[]> {
  yield* chunks;
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
 * An ARRAY source takes `slice` instead (#179): the per-item loop below pays the iterator protocol
 * once per row and regrows `chunk` from empty as it fills, where `slice(i, i + chunkSize)` produces
 * the identical chunk in one correctly-sized allocation. Measured over 1,000,000 rows into 1000-row
 * chunks, output asserted identical: 10.80, 10.62, 10.38 ns/row for the per-item loop against 0.37,
 * 0.42, 0.36 for `slice`. `fromSource()` (`src/pipeline.ts`) hands this function the caller's own
 * array unwrapped, so an ordinary `new Pipeline<number>()(items)` takes this arm.
 *
 * ⚠ `Array.isArray` is the test, never a `length` check. A string is iterable AND length-bearing,
 * and a `length`-plus-`slice` guard would hand back STRINGS where every other source yields arrays:
 * `slice` on a string returns a string, so a `Pipeline<string>` over `"abcd"` would produce `"ab"`
 * rather than `["a", "b"]`. The per-item arm rejects nothing - it cuts a string into its characters,
 * which is the shipped behaviour and stays so. Every other `Iterable` - a `Set`, a `Map`, a
 * generator, a caller's own iterable object - keeps the per-item arm, including its own early-stop
 * behaviour: the array arm never touches the iterator protocol at all, so a source's `finally` block
 * has nothing to run there and nothing to close.
 *
 * ⚠ `Number.isInteger` guards the arm too, and is not optional. `slice(i, i + chunkSize)` truncates
 * both bounds where the per-item arm cuts at `length >= chunkSize`, so a fractional size made the
 * two arms disagree on the SAME data: `chunkSize: 2.5` over `[1..7]` cut an array into
 * `[[1,2],[3,4,5],[6,7]]` against a `Set`'s own `[[1,2,3],[4,5,6],[7]]` - one knob, two chunkings,
 * the engine disagreeing with itself (review-caught). A fractional size is incoherent either way and
 * the constructor refuses it now (`Pipeline`'s own `chunkSize` guard, the same one `.buffer(size)`
 * has always had); this arm still checks, because this function is reached from `recut.ts` and from
 * `.buffer()` as well, and an arm that silently re-cuts differently is worse than a slower one.
 *
 * `[...buildSyncChunkGenerator<number>(3)([1, 2, 3, 4, 5, 6, 7])]` → `[[1, 2, 3], [4, 5, 6], [7]]`,
 * by either arm.
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

/**
 * Prefetches up to `capacity` chunks ahead of the consumer (#123) - an already-cut chunk stream in,
 * the same stream out, only WHEN each chunk is fetched changes. Written as a plain `async function*`
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
 * regardless of how many `.next()` calls are already in flight, so racing them buys no overlap -
 * proven during #123's own planning. The overlap this function buys comes from PRODUCTION and
 * CONSUMPTION running concurrently (the source keeps working while the consumer processes an
 * earlier chunk), never from concurrent production itself.
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
    for (let step = await pending.shift()!; !step.done; step = await pending.shift()!) {
      pull();
      yield step.value;
    }
  } finally {
    await iterator.return?.();
  }
}

/**
 * Collects a bound pipeline's items to an array, staying synchronous when the chain is (#90), and
 * stopping early once `limit` items are in hand - the ONE collect every caller shares:
 * `PipelineResult.toArray()`, its `first(n)` (which IS `toArray` with a limit), and `.branch()`,
 * which collects the parent chain before routing. Three copies of the same engine decision before.
 *
 * `syncChunks` is `Pipeline.drainable()`'s own sync view, `null` on the async engine, where `chunks`
 * is read instead.
 *
 * The async arm walks CHUNKS, never a flattened item stream (#179). `Pipeline.drainable()`'s own
 * chunk view already arrives in chunks, so flattening it first cost one `await` - and therefore one
 * microtask - per ROW, for data that was never per-row to begin with. Measured on
 * `ConcurrentPipeline` at N=10,000 over an async generator with `.buffer(1000)`, output asserted
 * identical: `.toArray()` fell from 12.009 promises per row to 7.006, which is the source's own
 * floor - an async generator costs 4.000 per row before any package code runs, and `.buffer(size)`
 * a further 3.001.
 *
 * `collectItems(chunksOf([[1, 2], [3]]), noChunks)` → `[1, 2, 3]`, no `Promise` created.
 */
export function collectItems<T>(
  syncChunks: MaybeAsyncChunks<T> | null,
  chunks: () => AsyncIterable<T[]>,
  limit?: number,
): T[] | Promise<T[]> {
  const results: T[] = [];
  return dispatchSync(
    syncChunks,
    (syncView) =>
      chain(
        drainSync(syncView, (item) => {
          results.push(item);
          return limit !== undefined && results.length >= limit;
        }),
        () => results,
      ),
    () => collectAsyncChunks(results, limit, chunks),
  );
}

/** `collectItems`'s async arm, its own function so the caller above stays one expression per
 * engine. The early exit is a `break` out of the `for await`, exactly as the flattened version's
 * was, so the chunk iterator's own `.return()` still runs and a generator source still reaches its
 * `finally`. */
async function collectAsyncChunks<T>(
  results: T[],
  limit: number | undefined,
  chunks: () => AsyncIterable<T[]>,
): Promise<T[]> {
  for await (const chunk of chunks()) {
    if (takeChunk(results, chunk, limit)) break;
  }
  return results;
}

/** Appends one chunk's items to `results`, reporting whether `limit` is now reached - its own
 * function so `collectAsyncChunks` above stays within this repo's own `max-depth: 2`. The
 * unlimited case skips the per-item check entirely, which is `.toArray()`'s own path; a spread
 * (`results.push(...chunk)`) is deliberately not used, since it passes a whole chunk as arguments
 * and a large enough one overflows the call stack. */
function takeChunk<T>(results: T[], chunk: T[], limit: number | undefined): boolean {
  if (limit === undefined) {
    for (let i = 0; i < chunk.length; i++) results.push(chunk[i]);
    return false;
  }
  for (let i = 0; i < chunk.length; i++) {
    results.push(chunk[i]);
    if (results.length >= limit) return true;
  }
  return false;
}
