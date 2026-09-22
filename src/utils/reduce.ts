/** The folds behind `.reduce()` at every level, and behind `.buffer(fn)`. */

import type { IContextManager, ReduceFunction, RowErrorHandler, BufferFunction } from "@src/types";
import { DROP } from "@src/types";
import type { MaybeAsyncChunks } from "@src/utils/drain";
import { chain, isThenable } from "@src/utils/helpers";

/**
 * A fold over items, one at a time, that can also `emit()` values mid-fold. Call `.fold()` per
 * item and `.final()` once at the end, which returns the trailing accumulator if one is owed.
 *
 * `new Reducer((acc, x) => acc + x, 0)`, `.fold(1, ctx)`, `.fold(2, ctx)`, `.final()` → `[3]`.
 */
export class Reducer<U, T> {
  private acc: U;
  /** ⚠ Incremented BEFORE `fn` runs, so an `emit()` on the last item leaves it at `0` and `.final()`
   * owes nothing. Incrementing after would add a spurious trailing value. */
  private itemsSinceEmit = 0;
  /** Whether `fn` has run at all; `emit()` never resets it. */
  private folded = false;

  constructor(
    private readonly fn: ReduceFunction<U, T>,
    initial: U,
    /** Recovers an item whose `fn` call failed, as `Transformer.onError()` registers it. */
    private readonly rowHandler?: RowErrorHandler,
  ) {
    this.acc = initial;
  }

  /** Folds one item and returns what `emit()` pushed during it, in order. When `fn` throws, the
   * row handler's value replaces the accumulator, and `DROP` skips the item. With no handler, the
   * error propagates.
   *
   * `new Reducer((acc, x, _ctx, emit) => (x === 6 ? (emit(acc + x), 0) : acc + x), 0).fold(6, ctx)`
   * → `[6]`, the accumulator `emit()` just pushed.
   *
   * `new Reducer((_acc, s) => { const n = parseInt(s); if (isNaN(n)) throw new Error("bad"); return n; }, 0, () => DROP).fold("x", ctx)`
   * → `[]`, the accumulator left at its prior value.
   */
  fold(item: T, ctx: IContextManager): U[] | Promise<U[]> {
    const emitted: U[] = [];
    this.folded = true;
    this.itemsSinceEmit++;

    try {
      const next = this.fn(this.acc, item, ctx, (value) => {
        emitted.push(value);
        this.itemsSinceEmit = 0;
      });
      // ⚠ Commit inline, not through closures built before the call: this runs once per item, and
      // allocating them here makes the fold several times slower.
      if (!isThenable(next)) {
        this.acc = next;
        return emitted;
      }
      return Promise.resolve(next).then(
        (acc) => {
          this.acc = acc;
          return emitted;
        },
        (error: Error) => this.recover(item, ctx, emitted, error),
      );
    } catch (error) {
      return this.recover(item, ctx, emitted, error as Error);
    }
  }

  private recover(item: T, ctx: IContextManager, emitted: U[], error: Error): U[] | Promise<U[]> {
    if (!this.rowHandler) throw error;
    return chain(this.rowHandler(item, error, ctx), (recovered) => {
      // ⚠ `DROP` undoes `fold()`'s increment only above `0`: `fn` may `emit()` and then throw.
      if (recovered === DROP) {
        if (this.itemsSinceEmit > 0) this.itemsSinceEmit--;
      } else {
        this.acc = recovered as U;
      }
      return emitted;
    });
  }

  /** The final accumulator if items were folded since the last `emit()`, else `[]`.
   * `seedIfEmpty` returns the seed when nothing was folded at all.
   *
   * ⚠ A partition of a partitioned reduce never passes `seedIfEmpty`: its share of the stream can
   * be empty while the stage's is not.
   *
   * `new Reducer((acc, x) => acc + x, 0).final(true)` → `[0]`; `.final()` → `[]`. After one
   * `.fold(1, ctx)`, both → `[1]`. */
  final(seedIfEmpty = false): U[] {
    return this.itemsSinceEmit > 0 || (seedIfEmpty && !this.folded) ? [this.acc] : [];
  }

  /** The current accumulator, whether or not an `emit()` just ran.
   *
   * ⚠ `.buffer(fn)` reads this, not `.final()`, for its trailing chunk. An item that flushes and is
   * then appended leaves `.final()` at `[]`, which would drop that last item. */
  current(): U {
    return this.acc;
  }
}

/**
 * Folds one chunk's items through `reducer`, in order, and returns everything they emitted. It
 * stays synchronous while every fold does.
 *
 * `foldChunk(new Reducer((acc, x) => acc + x, 0), [1, 2, 3], ctx)` → `[]`: nothing emitted, the
 * sum waits for `.final()`.
 */
export function foldChunk<U, T>(
  reducer: Reducer<U, T>,
  chunk: readonly T[],
  ctx: IContextManager,
): U[] | Promise<U[]> {
  const emitted: U[] = [];

  // ⚠ `drain` re-enters itself only after an async fold. Recursing per item overflows the stack
  // on a large chunk with a synchronous reducer.
  const drain = (start: number): U[] | Promise<U[]> => {
    for (let index = start; index < chunk.length; index++) {
      const values = reducer.fold(chunk[index], ctx);
      if (isThenable(values)) {
        return Promise.resolve(values).then((settled) => {
          emitted.push(...settled);
          return drain(index + 1);
        });
      }
      emitted.push(...(values as U[]));
    }
    return emitted;
  };

  return drain(0);
}

/**
 * Folds an async chunk stream with one accumulator, in this process. It yields each input chunk's
 * emits as one output chunk, then the trailing accumulator. `seedIfEmpty` yields the seed when
 * nothing was folded.
 *
 * `foldChunkStream((acc, x) => acc + x, 0, chunks, ctx)` over `[1, 2]` then `[3]` → yields `[6]`.
 * Over no chunks with `seedIfEmpty` → yields `[0]`.
 */
export async function* foldChunkStream<U, T>(
  fn: ReduceFunction<U, T>,
  initial: U,
  chunks: AsyncIterable<T[]>,
  ctx: IContextManager,
  seedIfEmpty = false,
): AsyncGenerator<U[]> {
  const reducer = new Reducer<U, T>(fn, initial);
  for await (const chunk of chunks) {
    const folded = foldChunk(reducer, chunk, ctx);
    const out = isThenable(folded) ? await folded : folded;
    if (out.length > 0) yield out;
  }
  const trailing = reducer.final(seedIfEmpty);
  if (trailing.length > 0) yield trailing;
}

/**
 * `foldChunkStream` over a synchronous chunk stream, for `.reduce()` on a `"sync"` chain. It stays
 * synchronous until the first pending chunk, and yields the seed when nothing was folded.
 *
 * ⚠ The seed is decided in `final()`, never by whether a chunk was yielded: a pending chunk can
 * resolve empty.
 *
 * `foldSyncChunkStream((acc, x) => acc + x, 0, [[1, 2], [3]], ctx)` → yields `[6]` once, no
 * `Promise` created. Over `[]` → yields `[0]` once, no `Promise` created.
 */
export function* foldSyncChunkStream<U, T>(
  fn: ReduceFunction<U, T>,
  initial: U,
  chunks: MaybeAsyncChunks<T>,
  ctx: IContextManager,
): MaybeAsyncChunks<U> {
  const reducer = new Reducer<U, T>(fn, initial);
  yield* driveFold(
    chunks,
    (slot) =>
      chain(
        chain(slot, (items) => foldChunk(reducer, items, ctx)),
        (out) => [out],
      ),
    () => [reducer.final(true)],
  );
}

/** ⚠ Flush, then append: the item whose callback calls `emit()` opens the next chunk. */
function bufferReduceFunction<T>(fn: BufferFunction<T>): ReduceFunction<T[], T> {
  return (acc, item, ctx, emit) => {
    let pending = acc;
    const flush = () => {
      emit(pending);
      pending = [];
    };
    return chain(fn(item, ctx, flush), (value) => {
      if (value !== DROP) pending.push(value);
      return pending;
    });
  };
}

function* nonEmpty<T>(values: T[][]): Generator<T[]> {
  for (const value of values) {
    if (value.length > 0) yield value;
  }
}

function trailingOf<T>(reducer: Reducer<T[], T>): T[][] {
  const pending = reducer.current();
  return pending.length > 0 ? [pending] : [];
}

/**
 * Cuts an async item stream into chunks with a `.buffer(fn)` fold: every flush is its own chunk,
 * and the pending remainder is the last. Empty chunks are never yielded.
 *
 * `buildBufferGenerator(everySecondItem, ctx)` over items `1, 2, 3` → yields `[1, 2]` then `[3]`.
 */
export function buildBufferGenerator<T>(
  fn: BufferFunction<T>,
  ctx: IContextManager,
): (data: AsyncIterable<T>) => AsyncGenerator<T[]> {
  const reduceFn = bufferReduceFunction(fn);
  return async function* bufferGenerator(data: AsyncIterable<T>): AsyncGenerator<T[]> {
    const reducer = new Reducer<T[], T>(reduceFn, []);
    for await (const item of data) {
      const folded = reducer.fold(item, ctx);
      yield* nonEmpty(isThenable(folded) ? await folded : folded);
    }
    yield* nonEmpty(trailingOf(reducer));
  };
}

/**
 * ⚠ Every chunk one unit's `work` returns is yielded as its own slot, never merged: each is a real
 * chunk boundary. An async unit's extra chunks wait in `remaining` until the caller resumes.
 */
function* driveFold<T, Unit>(
  units: Iterable<Unit>,
  work: (unit: Unit) => T[][] | Promise<T[][]>,
  final: () => T[][],
): MaybeAsyncChunks<T> {
  let tail: Promise<T[][]> | null = null;
  let remaining: T[][] = [];

  for (const unit of units) {
    if (remaining.length > 0) {
      yield* nonEmpty(remaining);
      remaining = [];
    }

    const out: T[][] | Promise<T[][]> = tail === null ? work(unit) : tail.then(() => work(unit));

    if (isThenable(out)) {
      tail = out as Promise<T[][]>;
      yield tail.then((values) => {
        remaining = values.slice(1);
        return values[0] ?? [];
      });
      continue;
    }
    // ⚠ Guarded: most folds emit nothing, and `yield*` builds a generator per item even for `[]`.
    if ((out as T[][]).length > 0) yield* nonEmpty(out as T[][]);
  }

  yield* nonEmpty(remaining);
  if (tail !== null) {
    yield tail.then(() => final().flat());
    return;
  }
  yield* nonEmpty(final());
}

/**
 * `buildBufferGenerator` over a synchronous item stream. It stays synchronous until the first async
 * fold.
 *
 * `[...buildSyncBufferGenerator(everySecondItem, ctx)([1, 2, 3])]` → `[[1, 2], [3]]`, no `Promise`
 * created.
 */
export function buildSyncBufferGenerator<T>(
  fn: BufferFunction<T>,
  ctx: IContextManager,
): (data: Iterable<T>) => MaybeAsyncChunks<T> {
  const reduceFn = bufferReduceFunction(fn);
  return function* bufferGenerator(data: Iterable<T>): MaybeAsyncChunks<T> {
    const reducer = new Reducer<T[], T>(reduceFn, []);
    yield* driveFold(
      data,
      (item) => reducer.fold(item, ctx),
      () => trailingOf(reducer),
    );
  };
}

/**
 * Re-cuts a stage's output chunks with a `.buffer(fn)` fold, for a `.buffer(fn)` after that stage.
 *
 * ⚠ A slot can be pending even on a `"sync"` chain, so each slot is folded as it settles, never
 * flattened to items first.
 *
 * `[...recutSyncChunksWith([[1, 2], [3]], everySecondItem, ctx)]` → `[[1, 2], [3]]`, no `Promise`
 * created.
 */
export function recutSyncChunksWith<T>(
  chunks: MaybeAsyncChunks<T>,
  fn: BufferFunction<T>,
  ctx: IContextManager,
): MaybeAsyncChunks<T> {
  const reducer = new Reducer<T[], T>(bufferReduceFunction(fn), []);
  return driveFold(
    chunks,
    (slot) => chain(slot, (items) => foldChunk(reducer, items, ctx)),
    () => trailingOf(reducer),
  );
}
