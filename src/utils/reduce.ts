/**
 * The fold state machine shared by every reducer (#45): `Transformer.reduce` (one instance per
 * chunk, no cross-chunk state) and `Pipeline.reduce`/`ConcurrentPipeline.reduceWork` (one instance
 * for the whole stream). Both call `fn` with the full `(acc, item, ctx, emit)` signature regardless
 * of its declared arity - JS ignores extra arguments, so a 2-arg `(acc, x) => acc + x` and a 4-arg
 * `(acc, x, ctx, emit) => …` both just work.
 */

import type { IContextManager, ReduceFunction, RowErrorHandler } from "@src/types";
import { DROP } from "@src/types";
import type { MaybeAsyncChunks } from "@src/utils/chunk";
import { chain, isThenable } from "@src/utils/helpers";

/**
 * Folds items one at a time into `U`, buffering values `emit()` pushes and tracking whether the
 * trailing accumulator is still owed. `new Reducer(fn, 0).fold(x, ctx)` per item, `.final()` once -
 * `Transformer.reduce`'s per-chunk branch, `foldChunk`/`foldChunkStream` (below), and
 * `HttpPipeline`'s own server-side `runReduceStage` (`src/pipelines/http.ts`, one instance per
 * duplex connection) are its callers.
 */
export class Reducer<U, T> {
  private acc: U;
  /** Items folded since the last `emit()` - `0` right after an emit, so `.final()` knows whether a
   * trailing value is still owed. Incremented BEFORE `fn` runs, never after: an `emit()` firing
   * while folding the LAST item must leave this at `0`, not `1`, or `.final()` pushes a spurious
   * value nothing was folded into since that emit (the ticket's own Constraints: real, that bug
   * produced `[60,90,0]` where `[60,90]` was written). */
  private itemsSinceEmit = 0;

  constructor(
    private readonly fn: ReduceFunction<U, T>,
    initial: U,
    /** The row handler (#78), read once at construction and applied to every `.fold()` call -
     * `Transformer.reduce()` passes `run?.rowHandler` here; `foldChunkStream`'s own callers
     * (`Pipeline.reduce()`, `ConcurrentPipeline.reduceWork()`) never pass one, since they fold with
     * no `Transformer` in scope (the ticket's own Constraints). */
    private readonly rowHandler?: RowErrorHandler,
  ) {
    this.acc = initial;
  }

  /** Folds one item, returning whatever `emit()` pushed during this call, in emit order - `[]` when
   * `fn` didn't emit. A throwing `fn` (#78) hands the item to `this.rowHandler`, when registered: a
   * returned value REPLACES the accumulator directly (never re-runs `fn`, so a handler cannot cause
   * a second throw), `DROP` skips the item - the increment above is undone, so `.final()` doesn't
   * owe a trailing value for a row that never actually folded. No handler registered: the throw
   * propagates unchanged, same as before #78.
   *
   * `new Reducer((acc, x, _ctx, emit) => (x === 6 ? (emit(acc + x), 0) : acc + x), 0).fold(6, ctx)`
   * → `[6]`, the accumulator `emit()` just pushed.
   *
   * `new Reducer((_acc, s) => { const n = parseInt(s); if (isNaN(n)) throw new Error("bad"); return n; }, 0, () => DROP).fold("x", ctx)`
   * → `[]`, the accumulator left at its prior value.
   */
  fold(item: T, ctx: IContextManager): U[] | Promise<U[]> {
    const emitted: U[] = [];
    this.itemsSinceEmit++;

    try {
      const next = this.fn(this.acc, item, ctx, (value) => {
        emitted.push(value);
        this.itemsSinceEmit = 0;
      });
      // Not `await` (#90): a synchronous `fn` folds without creating a `Promise`, which is what
      // keeps a `.reduce()` stage inside an all-sync chain synchronous end to end.
      //
      // The commit is INLINE rather than a `commit`/`recover` pair built before the call. Hoisting
      // them read better and cost the fold almost everything it had: measured over 200 000 items,
      // building both per item ran at 372.5 ns/item against 8.9 ns/item for this shape - and the
      // async fold #90 replaced, which allocated a `Promise` per item, ran at 94.5 ns. Zero
      // promises and four times the CPU is not the trade this ticket exists to make.
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

  /** The recovery both failure arms share (#78): a synchronous throw from `fn` and a REJECTED
   * promise are the two ways it can fail, and they must behave identically. A rejection never
   * reaches `fold()`'s own `catch`, so the async arm passes this as `.then`'s rejection handler.
   *
   * `emitted` is threaded in rather than captured, so the failure path allocates the closure and
   * the happy path does not. */
  private recover(item: T, ctx: IContextManager, emitted: U[], error: Error): U[] | Promise<U[]> {
    if (!this.rowHandler) throw error;
    return chain(this.rowHandler(item, error, ctx), (recovered) => {
      this.applyRecovery(recovered as U | typeof DROP);
      return emitted;
    });
  }

  /** Applies a recovered row (#78): `DROP` undoes `fold()`'s own increment - guarded, since `fn`
   * can `emit()` (resetting `itemsSinceEmit` to `0`) and THEN throw, and `0 - 1` would leave
   * `.final()` owing a value nothing was folded into since that emit - any other value replaces the
   * accumulator directly. Split out of `fold()` to keep that method's own `try`/`catch` at this
   * repo's `max-depth: 2`. */
  private applyRecovery(recovered: U | typeof DROP): void {
    if (recovered === DROP) {
      if (this.itemsSinceEmit > 0) this.itemsSinceEmit--;
      return;
    }
    this.acc = recovered;
  }

  /** The final accumulator, only if items were folded since the last `emit()` - `[]` otherwise. */
  final(): U[] {
    return this.itemsSinceEmit > 0 ? [this.acc] : [];
  }
}

/**
 * Folds one chunk's items through an already-constructed `reducer`, collecting whatever `.fold()`
 * emits across every item into one array, in order - the one loop shape every reducer caller
 * shares (`foldChunkStream` below, `Transformer.reduce`'s per-chunk pipe callback, and
 * `HttpPipeline`'s server-side `foldChunkFrame`, `src/pipelines/http.ts`), pulled out so a fix to
 * the fold-accumulation loop itself (review: `itemsSinceEmit`'s own ordering subtlety) lands once
 * rather than in three copies that could drift apart.
 *
 * `foldChunk(new Reducer((acc, x) => acc + x, 0), [1, 2, 3], ctx)` → `[]` (nothing emitted
 * mid-fold; the accumulator itself only ever surfaces via `.final()`).
 */
export function foldChunk<U, T>(
  reducer: Reducer<U, T>,
  chunk: readonly T[],
  ctx: IContextManager,
): U[] | Promise<U[]> {
  const emitted: U[] = [];

  // Folds are ORDER-DEPENDENT - one accumulator, one item at a time - so item `i + 1` cannot start
  // until `i` has settled. `drain` re-enters itself ONLY across an async boundary, the same
  // recurse-across-async shape `Transformer.loop()`'s own `drain` uses and explains in full
  // (`transformer.ts`) - recursing per ITEM instead overflows the stack on a synchronous reducer:
  // measured here, a 5000-item chunk threw `RangeError: Maximum call stack size exceeded`, where
  // the pre-#90 loop returned its sum, and the ceiling moved with `.buffer()`.
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
 * Folds one chunk stream into emitted-value chunks, in-process and sequentially, ONE accumulator
 * for the WHOLE stream it is handed - the shared body behind base `Pipeline.reduce()`'s own fold
 * and `ConcurrentPipeline.reduceWork()`'s own default. A dispatched, partitioned reduce (#62) calls
 * this once PER PARTITION, each over its own `share()` view of the source, so "one accumulator" is
 * per-partition there, not per-stage - `maxConcurrency` now decides how many of these run at once,
 * never whether more than one does. Streams: yields whatever a given input chunk emitted as its own
 * output chunk, then the trailing accumulator once the stream ends.
 *
 * `foldChunkStream((acc, x) => acc + x, 0, chunksOf([[1,2],[3]]), ctx)` → yields `[6]` once, the
 * whole dataset's sum, nothing emitted mid-fold.
 */
export async function* foldChunkStream<U, T>(
  fn: ReduceFunction<U, T>,
  initial: U,
  chunks: AsyncIterable<T[]>,
  ctx: IContextManager,
): AsyncGenerator<U[]> {
  const reducer = new Reducer<U, T>(fn, initial);
  for await (const chunk of chunks) {
    const out = await foldChunk(reducer, chunk, ctx);
    if (out.length > 0) yield out;
  }
  // `foldChunkStream` stays an async generator because its INPUT is an `AsyncIterable`, not because
  // a fold must defer: `foldSyncChunkStream` (below) folds the same reducer over a sync chunk stream
  // and is what `Pipeline.reduce()` picks on a `"sync"` chain.
  const trailing = reducer.final();
  if (trailing.length > 0) yield trailing;
}

/**
 * `foldChunkStream`'s synchronous counterpart (#90): folds a `MaybeAsyncChunks` stream into
 * emitted-value chunks, ONE accumulator for the whole stream, staying synchronous until the first
 * pending chunk. `Pipeline.reduce()` picks this over `foldChunkStream` when its chain is `"sync"`.
 *
 * A fold is ORDER-DEPENDENT across chunks as well as within one, so chunk `n + 1` cannot fold until
 * `n` has settled: `tail` carries whatever the last chunk is still waiting on, and the first
 * thenable therefore defers every chunk after it too. That deferral runs on `.then`, so a long
 * stream never grows the stack.
 *
 * `foldSyncChunkStream((acc, x) => acc + x, 0, [[1, 2], [3]], ctx)` → yields `[6]` once, no
 * `Promise` created.
 */
export function* foldSyncChunkStream<U, T>(
  fn: ReduceFunction<U, T>,
  initial: U,
  chunks: MaybeAsyncChunks<T>,
  ctx: IContextManager,
): MaybeAsyncChunks<U> {
  const reducer = new Reducer<U, T>(fn, initial);
  let tail: Promise<U[]> | null = null;

  for (const chunk of chunks) {
    const fold = (): U[] | Promise<U[]> => chain(chunk, (items) => foldChunk(reducer, items, ctx));
    const out: U[] | Promise<U[]> = tail === null ? fold() : tail.then(fold);

    if (isThenable(out)) {
      tail = out as Promise<U[]>;
      // NOT guarded on length, unlike the settled arm below: a pending chunk's emptiness is not
      // knowable until it settles, and a generator cannot un-yield. `PipelineResult.chunks()`
      // drops the empties instead, which is where they are observable.
      yield out as Promise<U[]>;
      continue;
    }
    if ((out as U[]).length > 0) yield out as U[];
  }

  // The trailing accumulator owes the same ordering: once anything deferred, it is only known after
  // the last chunk settles.
  if (tail !== null) {
    yield tail.then(() => reducer.final());
    return;
  }
  const trailing = reducer.final();
  if (trailing.length > 0) yield trailing;
}
