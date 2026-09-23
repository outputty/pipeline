/** Drains a synchronous chunk stream for the terminal operations of a `"sync"` chain. */

import { chain, isThenable, tryRecover } from "@src/utils/helpers";

/**
 * A synchronous chunk stream whose chunks may still be pending: what a `"sync"` chain carries. A
 * stage with an async callback puts a `Promise` in, and the run turns async there.
 */
export type MaybeAsyncChunks<T> = Iterable<T[] | Promise<T[]>>;

/**
 * Hands every item of a chunk stream to `onItem`, in order. It stays synchronous until the first
 * pending chunk. `onItem` returning `true` stops the drain and closes the source.
 *
 * `drainSync([[1, 2], [3]], (x) => void out.push(x))` → `undefined`, no `Promise` created, with
 * `out` `[1, 2, 3]`.
 */
export function drainSync<T>(
  chunks: MaybeAsyncChunks<T>,
  onItem: (item: T) => boolean | void,
): void | Promise<void> {
  return drainSyncChunks(chunks, (chunk) => pushAll(chunk, onItem));
}

/**
 * `drainSync` a whole chunk at a time: `onChunk` returning `true` stops the drain and closes the
 * source.
 *
 * `drainSyncChunks([[1, 2], [3]], (c) => void out.push(c.length))` → `undefined`, no `Promise`
 * created, with `out` `[2, 1]`.
 */
export function drainSyncChunks<T>(
  chunks: MaybeAsyncChunks<T>,
  onChunk: (chunk: T[]) => boolean | void,
): void | Promise<void> {
  const iterator = chunks[Symbol.iterator]();

  const resume = (): void | Promise<void> => {
    for (;;) {
      const step = iterator.next();
      if (step.done === true) return;

      const chunk = step.value;
      if (isThenable(chunk)) {
        return Promise.resolve(chunk).then((settled) =>
          onChunk(settled) === true ? close(iterator) : resume(),
        );
      }
      if (onChunk(chunk) === true) return close(iterator);
    }
  };

  return closingOnFailure(iterator, resume);
}

function closingOnFailure<R>(iterator: Iterator<unknown>, drain: () => R): R {
  return tryRecover(drain, (error) => {
    close(iterator);
    throw error;
  }) as R;
}

/**
 * `drainSync` for an `onItem` that may be async, as `.forEach()` allows: each item's callback
 * settles before the next item starts. It stays synchronous while the chunks and callback do.
 *
 * `drainSyncSettled([[1, 2]], (x) => void out.push(x))` → `undefined`, no `Promise` created.
 */
export function drainSyncSettled<T>(
  chunks: MaybeAsyncChunks<T>,
  onItem: (item: T) => void | Promise<void>,
): void | Promise<void> {
  const iterator = chunks[Symbol.iterator]();

  // ⚠ `runChunk` must never call `resume` synchronously: that recursion overflows the stack on a
  // long synchronous stream. It may re-enter only after an async boundary.
  const runChunk = (chunk: T[], start: number): void | Promise<void> => {
    for (let index = start; index < chunk.length; index++) {
      const settled = onItem(chunk[index]);
      if (isThenable(settled)) {
        const resumeAt = index + 1;
        return Promise.resolve(settled).then(() =>
          chain(runChunk(chunk, resumeAt), () => resume()),
        );
      }
    }
    return undefined;
  };

  const resume = (): void | Promise<void> => {
    for (;;) {
      const step = iterator.next();
      if (step.done === true) return;

      const chunk = step.value;
      if (isThenable(chunk)) {
        return Promise.resolve(chunk).then((settled) =>
          chain(runChunk(settled, 0), () => resume()),
        );
      }
      const ran = runChunk(chunk, 0);
      if (isThenable(ran)) return ran;
    }
  };

  return closingOnFailure(iterator, resume);
}

/** Closes a source iterator a consumer stopped reading early, so a generator's `finally` runs and
 * releases what it holds.
 *
 * `close(generator)` → `undefined`, the generator's `finally` run. */
export function close(iterator: Iterator<unknown>): void {
  iterator.return?.();
}

function pushAll<T>(chunk: T[], onItem: (item: T) => boolean | void): boolean {
  for (const item of chunk) {
    if (onItem(item) === true) return true;
  }
  return false;
}
