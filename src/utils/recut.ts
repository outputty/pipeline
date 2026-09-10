/**
 * Re-cutting an already-staged sync chunk stream at a new boundary (#90, #133) - split out of
 * `chunk.ts` along with `cut.ts` (cutting/flattening/sharing) and `drain.ts` (draining a
 * `MaybeAsyncChunks` stream), re-exported from `chunk.ts` so nothing importing that barrel has to
 * change.
 */

import { isThenable } from "@src/utils/helpers";
import { close, type MaybeAsyncChunks } from "@src/utils/drain";
import { assertPositiveChunkSize } from "@src/utils/cut";

/** The pair every function in this file threads together - the source iterator and the boundary
 * it's being re-cut to (#133: was two separate positional parameters carried through
 * `recutFrom`/`recutPending`, one small object instead). `cutChunk` (below) needs only `size`, so
 * it keeps `size` as its own parameter rather than taking this whole state. */
interface RecutState<T> {
  iterator: Iterator<T[] | Promise<T[]>>;
  size: number;
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
  assertPositiveChunkSize(size);

  const state: RecutState<T> = { iterator: chunks[Symbol.iterator](), size };

  try {
    yield* recutFrom(state);
  } finally {
    // A consumer that stops early - `PipelineResult.first(n)` - closes THIS generator, and a MANUAL
    // iterator learns nothing from a `for…of` that never ran, so its source's own `finally` never
    // runs. Measured before this: a 100-item generator behind `.buffer(10).transform(f).buffer(2)`,
    // drained with `.first(1)`, left the source open where the identical chain over an
    // `AsyncIterable` closed it - two engines disagreeing on user code that differed only in its
    // source, which is exactly what #90 exists to remove. Closing an already-exhausted iterator is
    // a no-op, so this needs no "did it finish?" flag.
    close(state.iterator);
  }
}

/** `recutSyncChunks`'s cutting loop, its own function so the close above costs no nesting - this
 * repo caps blocks at `max-depth: 2`. */
function* recutFrom<T>(state: RecutState<T>): Generator<T[] | Promise<T[]>> {
  let carry: T[] = [];

  for (;;) {
    const step = state.iterator.next();
    if (step.done === true) break;

    const chunk = step.value;
    if (isThenable(chunk)) {
      yield* recutPending(carry, chunk, state);
      return;
    }

    carry = yield* cutChunk(carry, chunk, state.size);
  }

  if (carry.length > 0) {
    yield carry;
  }
}

/**
 * Yields every whole `size` cut `chunk` can serve given what `carry` already holds, and RETURNS the
 * sub-`size` remainder for the next chunk - read as `carry = yield* cutChunk(carry, chunk, size)`.
 *
 * Cutting by index out of `chunk` is what keeps the re-cut linear. Buffering each chunk into
 * `carry` and re-slicing it per cut re-copies the remainder every time, which is quadratic in the
 * chunk: measured, `.buffer(N).transform(f).buffer(2)` over 80 000 items ran 1418 ms where the same
 * chain with no re-cut ran 12 ms, and this shape returns the identical output in about 1 ms.
 *
 * `[...cutChunk([1], [2, 3, 4, 5], 2)]` → `[[1, 2], [3, 4]]`, returning `[5]`.
 */
function* cutChunk<T>(carry: T[], chunk: T[], size: number): Generator<T[], T[]> {
  let index = 0;

  if (carry.length > 0) {
    const need = size - carry.length;
    if (chunk.length < need) return [...carry, ...chunk];
    yield [...carry, ...chunk.slice(0, need)];
    index = need;
  }

  while (index + size <= chunk.length) {
    yield chunk.slice(index, index + size);
    index += size;
  }

  return index < chunk.length ? chunk.slice(index) : [];
}

/**
 * Keeps re-cutting at `size` once a pending chunk is met, yielding one promise per cut instead of
 * one promise for the whole remaining stream (#90).
 *
 * The earlier shape collapsed here: it returned the carry, the pending chunk and every chunk after
 * it as ONE settled array, so `.buffer(2)` after an async stage stopped cutting entirely. Measured,
 * a seven-item source summed per chunk gave `[28]` where the identical chain over an `AsyncIterable`
 * source gave `[3,7,11,7]` - two engines disagreeing on user code that differed only in its source.
 *
 * `MaybeAsyncChunks` is a SYNCHRONOUS iterable, so the number of cuts is not knowable up front once
 * the tail is pending. This works because every consumer of a chunk stream settles a chunk before
 * pulling the next one (`drainSync`, `drainSyncSettled`, `asyncItems` and this
 * function itself all await or `chain` on the pending chunk first), so `state` is already current
 * when the generator decides whether to yield again.
 */
function* recutPending<T>(
  carry: T[],
  pending: Promise<T[]>,
  state: RecutState<T>,
): Generator<Promise<T[]>> {
  // `carry` is never read again after this call, so the tail owns it rather than copying it.
  const buffered = { buffer: carry, exhausted: false };
  let first: Promise<T[]> | null = pending;

  const cut = async (): Promise<T[]> => {
    while (buffered.buffer.length < state.size && !buffered.exhausted) {
      if (first !== null) {
        buffered.buffer.push(...(await first));
        first = null;
        continue;
      }
      const step = state.iterator.next();
      if (step.done === true) {
        buffered.exhausted = true;
        break;
      }
      buffered.buffer.push(...(await step.value));
    }
    return buffered.buffer.splice(0, state.size);
  };

  // A buffer that drains exactly on a boundary costs one more `cut()`, which discovers exhaustion
  // and returns `[]` - `[...recutSyncChunks([Promise.resolve([1,2,3,4])], 2)]` yields
  // `[[1,2],[3,4],[]]` where the settled path yields `[[1,2],[3,4],[5]]`. Not knowable before the
  // cut settles, and a generator cannot un-yield; `PipelineResult.chunks()` drops the empties,
  // which is where a consumer can see them. Every other consumer pushes nothing for one.
  while (!buffered.exhausted || buffered.buffer.length > 0) {
    yield cut();
  }
}
