import type { IContextManager, ReduceFunction, RowErrorHandler, BufferFunction } from "@src/types";
import { DROP } from "@src/types";
import type { MaybeAsyncChunks } from "@src/utils/chunk";
import { chain, isThenable } from "@src/utils/helpers";
import { assertPositiveChunkSize } from "@src/utils/cut";

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
   * value nothing was folded into since that emit. */
  private itemsSinceEmit = 0;

  constructor(
    private readonly fn: ReduceFunction<U, T>,
    initial: U,
    /** The row handler, read once at construction and applied to every `.fold()` call -
     * `Transformer.reduce()` passes `run?.rowHandler` here; `foldChunkStream`'s own callers
     * (`Pipeline.reduce()`, `ConcurrentPipeline.reduceWork()`) never pass one, since they fold with
     * no `Transformer` in scope. */
    private readonly rowHandler?: RowErrorHandler,
  ) {
    this.acc = initial;
  }

  /** Folds one item, returning whatever `emit()` pushed during this call, in emit order - `[]` when
   * `fn` didn't emit. A throwing `fn` hands the item to `this.rowHandler`, when registered: a
   * returned value REPLACES the accumulator directly (never re-runs `fn`, so a handler cannot cause
   * a second throw), `DROP` skips the item - the increment above is undone, so `.final()` doesn't
   * owe a trailing value for a row that never actually folded. No handler registered: the throw
   * propagates unchanged.
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
      // Not `await`: a synchronous `fn` folds without creating a `Promise`, which is what keeps a
      // `.reduce()` stage inside an all-sync chain synchronous end to end.
      //
      // The commit is INLINE rather than a `commit`/`recover` pair built before the call. Hoisting
      // them read better and cost the fold almost everything it had: measured over 200 000 items,
      // building both per item ran at 372.5 ns/item against 8.9 ns/item for this shape - a
      // per-item `Promise` allocation is not the trade this file exists to make.
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

  /** The recovery both failure arms share: a synchronous throw from `fn` and a REJECTED promise are
   * the two ways it can fail, and they must behave identically. A rejection never reaches `fold()`'s
   * own `catch`, so the async arm passes this as `.then`'s rejection handler.
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

  /** Applies a recovered row: `DROP` undoes `fold()`'s own increment - guarded, since `fn` can
   * `emit()` (resetting `itemsSinceEmit` to `0`) and THEN throw, and `0 - 1` would leave `.final()`
   * owing a value nothing was folded into since that emit - any other value replaces the
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

  /** The RAW current accumulator, with no `itemsSinceEmit` gating - `.buffer()`'s own trailing-chunk
   * logic needs "is there a non-empty pending array right now", a different question than
   * `.final()`'s "was anything folded since the last `emit()`": a fold that flushes AND appends the
   * SAME item in one call (`bufferReduceFunction`'s own flush-then-append shape) resets
   * `itemsSinceEmit` to `0` by `.final()`'s own design - correct for `.reduce()`'s contract, where a
   * post-emit return value may be an unrelated fresh seed - but WRONG for `.buffer(fn)`, where the
   * returned array always IS the real pending state. Measured: without this, the very last item of a
   * stream that both flushed and appended (`.buffer(10).transform((t) => t.map(async (x) => x *
   * 2)).buffer(sizeTwo)` over `[0..9]`, `sizeTwo` flushing every 2 items) silently dropped its own
   * trailing chunk - `.final()` read `0` where `.current()` reads `1`. */
  current(): U {
    return this.acc;
  }
}

/**
 * Folds one chunk's items through an already-constructed `reducer`, collecting whatever `.fold()`
 * emits across every item into one array, in order - the one loop shape every reducer caller
 * shares (`foldChunkStream` below, `Transformer.reduce`'s per-chunk pipe callback, and
 * `HttpPipeline`'s server-side `foldChunkFrame`, `src/pipelines/http.ts`), pulled out so a fix to
 * the fold-accumulation loop itself lands once rather than in three copies that could drift apart.
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
  // measured, a 5000-item chunk threw `RangeError: Maximum call stack size exceeded` under a
  // per-item recursive form, and the ceiling moves with `.buffer()`.
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
 * and `ConcurrentPipeline.reduceWork()`'s own default. A dispatched, partitioned reduce calls this
 * once PER PARTITION, each over its own `share()` view of the source, so "one accumulator" is
 * per-partition there, not per-stage - `maxConcurrency` decides how many of these run at once,
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
 * `foldChunkStream`'s synchronous counterpart: folds a `MaybeAsyncChunks` stream into emitted-value
 * chunks, ONE accumulator for the whole stream, staying synchronous until the first pending chunk.
 * `Pipeline.reduce()` picks this over `foldChunkStream` when its chain is `"sync"`.
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

/**
 * Adapts `.buffer(size)`'s own numeric form onto the SAME `ReduceFunction<T[], T>` shape
 * `bufferReduceFunction` (below) builds from a caller's `BufferFunction` - "one engine, not two":
 * both feed `buildBufferGenerator`/`buildSyncBufferGenerator`. `acc` is MUTATED and returned by
 * reference, never copied per item - `Reducer.fold()` was itself measured at 8.9 ns/item against
 * 372.5 ns/item for a copying shape, and an extra per-item array copy here would spend that budget
 * straight back.
 *
 * `assertPositiveChunkSize` runs HERE, eagerly, at generator-construction time - the same moment
 * `buildChunkGenerator`/`buildSyncChunkGenerator` already validate, so `.buffer(0)` on an already-
 * bound `Pipeline` still throws before any item is pulled, not at the first drain.
 *
 * `sizeReduceFunction<number>(3)([], 1, ctx, () => {})` → `[1]`, the same array reference, until a
 * third item pushes it to `.length >= 3` and it is emitted and reset.
 */
export function sizeReduceFunction<T>(size: number): ReduceFunction<T[], T> {
  assertPositiveChunkSize(size);
  return (acc, item, _ctx, emit) => {
    acc.push(item);
    if (acc.length >= size) {
      emit(acc);
      return [];
    }
    return acc;
  };
}

/**
 * Adapts a caller's `BufferFunction<T>` onto `ReduceFunction<T[], T>` - `pending` is the SAME
 * mutable array `Reducer` folds as `acc`, never exposed to `fn` directly: `flush` (the zero-arg
 * `emit` a `BufferFunction` receives) pushes the current `pending` onto the underlying reducer's own
 * `emit` and rebinds `pending` to a fresh `[]`, so a value `fn` returns AFTER calling `flush` appends
 * to the just-reset array, never the one already handed off. `chain` (not `await`) is what keeps a
 * fully synchronous `BufferFunction` from creating a `Promise` per item, the same reason `Reducer.fold`
 * itself avoids one.
 *
 * `bufferReduceFunction<number>((item, _ctx, emit) => (item % 2 === 0 ? (emit(), item) : item))`
 * folding `[1, 2, 3]` from `[]`: item `1` → `[1]`; item `2` flushes `[1]` (emitted) then appends `2`
 * to the reset array → `[2]`; item `3` → `[2, 3]`.
 */
export function bufferReduceFunction<T>(fn: BufferFunction<T>): ReduceFunction<T[], T> {
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

/** Yields every non-empty `T[]` in `values`, in order - the guard every engine function below needs
 * on its settled yields, split out so a caller nesting it inside its own `for` loop stays at this
 * repo's `max-depth: 2` (`yield*` is not a block, so `for (const item of data) yield* nonEmpty(...)`
 * costs one level of nesting, not two).
 *
 * `[...nonEmpty([[1], [], [2, 3]])]` → `[[1], [2, 3]]`. */
function* nonEmpty<T>(values: T[][]): Generator<T[]> {
  for (const value of values) {
    if (value.length > 0) yield value;
  }
}

/** `.buffer()`'s own trailing-chunk check - `reducer.current()`, not `reducer.final()`: the pending
 * array `sizeReduceFunction`/`bufferReduceFunction` return always IS the real state to flush, where
 * `.final()`'s own `itemsSinceEmit` gate answers a DIFFERENT question (`Reducer`'s own docstring)
 * that reads `0` for a fold that both flushed and appended the SAME item, silently dropping it when
 * that item was also the stream's last.
 *
 * `trailingOf(reducer)` → `[[...pending]]` if `pending.length > 0`, else `[]`. */
function trailingOf<T>(reducer: Reducer<T[], T>): T[][] {
  const pending = reducer.current();
  return pending.length > 0 ? [pending] : [];
}

/**
 * `.buffer()`'s own item-level engine, async arm - folds `data` through a fresh `Reducer<T[], T>`
 * one item at a time and yields each emitted pending array as its OWN chunk, never grouping more
 * than one emit together the way `foldChunkStream`'s chunk-granular fold does (there, one INPUT
 * chunk's worth of emits collapses into one downstream value by design; here, each `emit()` -
 * whether `sizeReduceFunction`'s own auto-flush or a caller's explicit `flush()` - IS a chunk
 * boundary and must stay its own chunk). `reduceFn` is `sizeReduceFunction(size)` or
 * `bufferReduceFunction(fn)` - this generator itself never knows which. Every yield is guarded on
 * `length > 0`: `sizeReduceFunction` can never emit an empty pending array (a positive `size` only
 * flushes once `acc.length >= size`), but a caller's own `BufferFunction` can call `flush()` on an
 * already-empty pending array (two `DROP`s in a row after a flush) - `.toArray()` would hide it (an
 * empty chunk flattens to nothing), but `.apply()`/a `ConcurrentPipeline` dispatch would not, the
 * same reason `buildChunkGenerator`/`foldSyncChunkStream`'s own settled arm already guard theirs.
 *
 * `buildBufferGenerator(sizeReduceFunction(2), ctx)(asyncFrom([1, 2, 3]))` → yields `[1, 2]` then
 * `[3]`, the same output `buildChunkGenerator(2)` already produces for `.buffer(2)`.
 */
export function buildBufferGenerator<T>(
  reduceFn: ReduceFunction<T[], T>,
  ctx: IContextManager,
): (data: AsyncIterable<T>) => AsyncGenerator<T[]> {
  return async function* bufferGenerator(data: AsyncIterable<T>): AsyncGenerator<T[]> {
    const reducer = new Reducer<T[], T>(reduceFn, []);
    for await (const item of data) {
      yield* nonEmpty(await reducer.fold(item, ctx));
    }
    yield* nonEmpty(trailingOf(reducer));
  };
}

/**
 * The shared item-by-item / slot-by-slot fold-and-yield engine `buildSyncBufferGenerator` and
 * `recutSyncChunksWith` (below) both drive - the SAME tail-chaining `foldSyncChunkStream` uses,
 * generalised over "a unit of input" (one raw item, or one existing chunk's worth of items) instead
 * of assuming which. `work(unit)` folds ONE unit and may emit any number of chunks; every one of
 * them must reach the caller as its OWN separate `MaybeAsyncChunks` slot, never grouped, since each
 * `emit()` IS a chunk boundary - `.flat()`-ing them together silently merges a real re-cut's own
 * multiple windows into one oversized chunk whenever a single unit produces more than one.
 *
 * A `MaybeAsyncChunks` slot carries exactly one `T[]` (or a `Promise` of one), so once a unit's
 * fold goes async, only its FIRST emitted chunk can be the yielded promise's own resolved value;
 * `remaining` queues whatever else that unit produced, drained (still in order) the moment this
 * generator is resumed - which only happens after the caller has awaited that promise, by which
 * point `remaining` is already populated. The pending arm's OWN first-value yield stays UNGUARDED
 * against emptiness, like `foldSyncChunkStream`'s: not knowable until settled, and a generator
 * cannot un-yield.
 *
 * `[...driveFold([1, 2, 3, 4], (n) => (n % 2 === 0 ? [[n]] : [[]]), () => [])]` → `[[2], [4]]`. */
function* driveFold<T, Unit>(
  units: Iterable<Unit>,
  work: (unit: Unit) => T[][] | Promise<T[][]>,
  final: () => T[][],
): MaybeAsyncChunks<T> {
  let tail: Promise<T[][]> | null = null;
  let remaining: T[][] = [];

  for (const unit of units) {
    yield* nonEmpty(remaining);
    remaining = [];

    const out: T[][] | Promise<T[][]> = tail === null ? work(unit) : tail.then(() => work(unit));

    if (isThenable(out)) {
      tail = out as Promise<T[][]>;
      yield tail.then((values) => {
        remaining = values.slice(1).filter((value) => value.length > 0);
        return values[0] ?? [];
      });
      continue;
    }
    yield* nonEmpty(out as T[][]);
  }

  yield* nonEmpty(remaining);
  if (tail !== null) {
    yield tail.then(() => final().flat());
    return;
  }
  yield* nonEmpty(final());
}

/**
 * `buildBufferGenerator`'s synchronous counterpart - a plain `function*` over `Iterable<T>` that
 * stays synchronous, creating no `Promise`, for as long as every fold settles synchronously; the
 * first thenable widens `driveFold`'s own `tail` and every later item chains off it, matching
 * `.buffer(size)`'s own sync-to-async widening rule.
 *
 * `[...buildSyncBufferGenerator(sizeReduceFunction(2), ctx)([1, 2, 3])]` → `[[1, 2], [3]]`, no
 * `Promise` created.
 */
export function buildSyncBufferGenerator<T>(
  reduceFn: ReduceFunction<T[], T>,
  ctx: IContextManager,
): (data: Iterable<T>) => MaybeAsyncChunks<T> {
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
 * `.buffer(fn)`'s own "re-cut already-produced chunks" sub-path - the sibling `recutSyncChunks`
 * takes for the numeric case, once a real stage has run and only `_syncChunks` (not raw items)
 * survives. Folds each existing chunk SLOT through `foldChunk` via `driveFold`, never flattening to
 * items first - because a `MaybeAsyncChunks` slot can carry a genuinely pending `Promise<T[]>` even
 * while the CHAIN's own Mode still reads `"sync"`: a stage between two `.buffer()` calls widens
 * only THAT stage's own output, not the chain's Mode, so `isSync()` being `true` does not mean
 * every slot already settled - measured, `.buffer(10).transform((t) => t.map(async (x) => x *
 * 2)).buffer(fn)` carries a `Promise<T[]>` slot straight into this function, and that ONE slot's
 * own fold (over every item the map stage produced) can still emit several separate chunks -
 * exactly the multi-emit-per-unit case `driveFold` exists to keep separate.
 *
 * `[...recutSyncChunksWith([[1, 2], [3]], sizeReduceFunction(2), ctx)]` → `[[1, 2], [3]]`, no
 * `Promise` created.
 */
export function recutSyncChunksWith<T>(
  chunks: MaybeAsyncChunks<T>,
  reduceFn: ReduceFunction<T[], T>,
  ctx: IContextManager,
): MaybeAsyncChunks<T> {
  const reducer = new Reducer<T[], T>(reduceFn, []);
  return driveFold(
    chunks,
    (slot) => chain(slot, (items) => foldChunk(reducer, items, ctx)),
    () => trailingOf(reducer),
  );
}
