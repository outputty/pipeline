/** Re-cuts a synchronous chunk stream at a new chunk size. */

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
): Generator<Promise<T[]>> {
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

  // A stream that ends exactly on a boundary yields one trailing `[]`: exhaustion is known only
  // once that cut settles. `PipelineResult.chunks()` drops it.
  while (!buffered.exhausted || buffered.buffer.length > 0) {
    yield cut();
  }
}
