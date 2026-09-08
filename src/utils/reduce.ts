/**
 * The fold state machine shared by every reducer (#45): `Transformer.reduce` (one instance per
 * chunk, no cross-chunk state) and `Pipeline.reduce`/`ConcurrentPipeline.reduceWork` (one instance
 * for the whole stream). Both call `fn` with the full `(acc, item, ctx, emit)` signature regardless
 * of its declared arity - JS ignores extra arguments, so a 2-arg `(acc, x) => acc + x` and a 4-arg
 * `(acc, x, ctx, emit) => …` both just work.
 */

import type { IContextManager, ReduceFunction } from "@src/types";

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
  ) {
    this.acc = initial;
  }

  /** Folds one item, returning whatever `emit()` pushed during this call, in emit order - `[]` when
   * `fn` didn't emit.
   *
   * `new Reducer((acc, x, _ctx, emit) => (x === 6 ? (emit(acc + x), 0) : acc + x), 0).fold(6, ctx)`
   * → `[6]`, the accumulator `emit()` just pushed. */
  async fold(item: T, ctx: IContextManager): Promise<U[]> {
    this.itemsSinceEmit++;
    const emitted: U[] = [];
    this.acc = await this.fn(this.acc, item, ctx, (value) => {
      emitted.push(value);
      this.itemsSinceEmit = 0;
    });
    return emitted;
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
export async function foldChunk<U, T>(
  reducer: Reducer<U, T>,
  chunk: Iterable<T>,
  ctx: IContextManager,
): Promise<U[]> {
  const emitted: U[] = [];
  for (const item of chunk) {
    emitted.push(...(await reducer.fold(item, ctx)));
  }
  return emitted;
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
  const trailing = reducer.final();
  if (trailing.length > 0) yield trailing;
}
