/** The folds behind `.reduce()` at every level. */

import type { IContextManager, ReduceFunction, RowErrorHandler } from "@src/types";
import { DROP } from "@src/types";
import type { MaybeAsyncChunks } from "@src/utils/drain";
import { chain, isThenable } from "@src/utils/helpers";

const NOTHING_EMITTED: readonly never[] = Object.freeze([]);

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
    this.folded = true;
    this.itemsSinceEmit++;

    try {
      const next = this.fn(this.acc, item, ctx, this.emit);
      // ⚠ Commit inline, not through closures built before the call: this runs once per item, and
      // allocating them here makes the fold several times slower.
      if (!isThenable(next)) {
        this.acc = next;
        return this.takeEmitted();
      }
      return Promise.resolve(next).then(
        (acc) => {
          this.acc = acc;
          // ⚠ Read here, not before the call: an async `fn` can `emit()` after its first `await`.
          return this.takeEmitted();
        },
        (error: Error) => this.recover(item, ctx, error),
      );
    } catch (error) {
      return this.recover(item, ctx, error as Error);
    }
  }

  /** What `emit()` pushed during the current `fold()`, collected lazily. Folds on one `Reducer`
   * never overlap: every caller settles a fold before starting the next. */
  private emitted: U[] | null = null;

  private readonly emit = (value: U): void => {
    (this.emitted ??= []).push(value);
    this.itemsSinceEmit = 0;
  };

  /** Hands the current fold's emits to the caller and starts the next fold empty. The shared empty
   * result is frozen: a caller reads it, never writes it. */
  private takeEmitted(): U[] {
    const emitted = this.emitted;
    if (emitted === null) return NOTHING_EMITTED as unknown as U[];
    this.emitted = null;
    return emitted;
  }

  private recover(item: T, ctx: IContextManager, error: Error): U[] | Promise<U[]> {
    if (!this.rowHandler) {
      this.emitted = null;
      throw error;
    }
    return chain(this.rowHandler(item, error, ctx), (recovered) => {
      // ⚠ `DROP` undoes `fold()`'s increment only above `0`: `fn` may `emit()` and then throw.
      if (recovered === DROP) {
        if (this.itemsSinceEmit > 0) this.itemsSinceEmit--;
      } else {
        this.acc = recovered as U;
      }
      return this.takeEmitted();
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
}

function pushAll<U>(target: U[], values: readonly U[]): void {
  for (let i = 0; i < values.length; i++) target.push(values[i]);
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
          pushAll(emitted, settled);
          return drain(index + 1);
        });
      }
      pushAll(emitted, values as U[]);
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

function* nonEmpty<T>(values: T[][]): Generator<T[]> {
  for (const value of values) {
    if (value.length > 0) yield value;
  }
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
