/**
 * Draining a `MaybeAsyncChunks` stream (#90, #133) - the synchronous engine's own terminal-op
 * drivers, split out of `chunk.ts` along with `cut.ts` (cutting/flattening/sharing) and `recut.ts`
 * (re-cutting an already-staged stream), re-exported from `chunk.ts` so nothing importing that
 * barrel has to change.
 */

import { chain, isThenable } from "@src/utils/helpers";

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
        return Promise.resolve(chunk).then((settled) =>
          pushAll(settled, onItem) ? close(iterator) : resume(),
        );
      }
      if (pushAll(chunk, onItem)) return close(iterator);
    }
  };

  return closingOnFailure(iterator, resume);
}

/**
 * Runs `drain`, closing `iterator` on ANY failure - a synchronous throw or a rejection (#90).
 *
 * The async engine gets this for free: `runSequentially`'s `for await` calls `.return()` on its
 * source when the loop body throws. A MANUAL iterator has to do it itself, and without this the two
 * engines disagreed on a failed run - measured, a sync generator's own `finally` did not run where
 * the identical chain over an async source released it. Stays synchronous when `drain` does.
 */
function closingOnFailure<R>(iterator: Iterator<unknown>, drain: () => R): R {
  try {
    const result = drain();
    if (!isThenable(result)) return result;
    return Promise.resolve(result).catch((error: unknown) => {
      close(iterator);
      throw error;
    }) as R;
  } catch (error) {
    close(iterator);
    throw error;
  }
}

/**
 * `drainSync`'s sibling for a callback whose OWN return has to settle before the next item (#90) -
 * what `Pipeline.forEach` needs, since a `forEach` callback is allowed to be async and its failures
 * must still reach the caller.
 *
 * Stays synchronous while both the chunks and the callback do, and widens at the first thenable
 * either produces. Items are settled strictly in order, so an async `forEach` behaves like the
 * `for await` loop it replaces rather than a `Promise.all` fan-out.
 *
 * `drainSyncSettled(chunksOf([[1, 2]]), (x) => void out.push(x))` → `undefined`, no `Promise`
 * created.
 */
export function drainSyncSettled<T>(
  chunks: MaybeAsyncChunks<T>,
  onItem: (item: T) => void | Promise<void>,
): void | Promise<void> {
  const iterator = chunks[Symbol.iterator]();

  // `runChunk` finishes ONE chunk and returns; advancing to the next is `resume`'s own loop. It must
  // never tail-call `resume` itself: that made the two mutually recursive, so `resume`'s `for(;;)`
  // never iterated and every chunk cost a stack frame pair. Measured on that shape, a synchronous
  // `Pipeline.forEach` over `.buffer(1)` threw `RangeError: Maximum call stack size exceeded` after
  // 3579 items, where `toArray()` - which drains through `drainSync`'s real loop - returned all
  // 200 000. Re-entry across an ASYNC boundary is the one safe case, since that continuation runs on
  // a fresh stack.
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

/** Closes a source iterator that a consumer stopped reading early, so a generator's own `finally`
 * runs and whatever it holds - a file handle, a cursor - is released. `for await`/`break` does this
 * for the async engine; the sync drains have to do it themselves. Exported (#133) - `recut.ts`'s own
 * `recutSyncChunks` calls this too, rather than hand-inlining the identical `iterator.return?.()`. */
export function close(iterator: Iterator<unknown>): void {
  iterator.return?.();
}

/** Hands one settled chunk's items to `onItem`, reporting whether it asked to stop. Its own
 * function to keep `drainSync`'s loop at this repo's `max-depth: 2`. */
function pushAll<T>(chunk: T[], onItem: (item: T) => boolean | void): boolean {
  for (const item of chunk) {
    if (onItem(item) === true) return true;
  }
  return false;
}
