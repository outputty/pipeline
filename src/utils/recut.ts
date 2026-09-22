/** Re-cuts a stage's chunk stream at a new chunk size. */

import { isThenable } from "@src/utils/helpers";
import { close, type MaybeAsyncChunks } from "@src/utils/drain";
import { assertPositiveChunkSize } from "@src/utils/cut";

interface RecutState<T> {
  iterator: Iterator<T[] | Promise<T[]>>;
  size: number;
}

/**
 * Re-cuts a stage's output chunks into chunks of `size`, for a `.buffer(size)` after that stage.
 * Settled chunks re-cut synchronously; from the first pending chunk on, each cut is a `Promise`.
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
    close(state.iterator);
  }
}

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
 * `recutSyncChunks` over a stage's async chunk stream, slicing inside each chunk rather than
 * flattening it to one item per pull. The chunks it yields are the same.
 *
 * `recutChunks(chunks, 2)` over `[1, 2]` then `[3, 4, 5]` → yields `[1, 2]`, `[3, 4]`, `[5]`.
 */
export async function* recutChunks<T>(
  chunks: AsyncIterable<T[]>,
  size: number,
): AsyncGenerator<T[]> {
  assertPositiveChunkSize(size);
  let carry: T[] = [];
  for await (const chunk of chunks) carry = yield* cutChunk(carry, chunk, size);
  if (carry.length > 0) yield carry;
}

/**
 * ⚠ Slices by index out of `chunk`. Appending `chunk` to `carry` and re-slicing that per cut
 * re-copies the remainder every time, which is quadratic.
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
 * ⚠ Relies on every consumer settling a yielded cut before pulling the next one. Only then is
 * `buffered` current when the loop decides whether to yield again.
 */
function* recutPending<T>(
  carry: T[],
  pending: Promise<T[]>,
  state: RecutState<T>,
): Generator<T[] | Promise<T[]>> {
  const { size, iterator } = state;
  // Items not yet cut are `buffer[cursor..]`. A cut is a `slice`, so the buffer is never shifted.
  let buffer = carry;
  let cursor = 0;
  let exhausted = false;
  let first: T[] | Promise<T[]> | null = pending;

  const append = (items: T[]): void => {
    if (cursor === buffer.length) {
      // ⚠ Adopted, never written to: `take()` only slices it, and `append` copies before joining.
      buffer = items;
    } else {
      const joined = buffer.slice(cursor);
      for (let i = 0; i < items.length; i++) joined.push(items[i]);
      buffer = joined;
    }
    cursor = 0;
  };

  const take = (): T[] => {
    const out = buffer.slice(cursor, cursor + size);
    cursor += out.length;
    return out;
  };

  /** The next upstream chunk, or `null` once the source is spent. */
  const nextChunk = (): T[] | Promise<T[]> | null => {
    if (first !== null) {
      const pendingFirst = first;
      first = null;
      return pendingFirst;
    }
    const step = iterator.next();
    if (step.done === true) {
      exhausted = true;
      return null;
    }
    return step.value;
  };

  const fill = async (): Promise<T[]> => {
    while (buffer.length - cursor < size && !exhausted) {
      const next = nextChunk();
      if (next === null) break;
      append(isThenable(next) ? await next : next);
    }
    return take();
  };

  // A stream that ends exactly on a boundary yields one trailing `[]`: exhaustion is known only
  // once that cut settles. `PipelineResult.chunks()` drops it.
  while (!exhausted || buffer.length - cursor > 0) {
    // A cut the buffer already holds is yielded as it is, with no `Promise`.
    yield buffer.length - cursor >= size ? take() : fill();
  }
}
