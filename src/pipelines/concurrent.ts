/**
 * `ConcurrentPipeline` (#17) — runs up to `maxConcurrency` chunks of one stage at once, in this
 * process. Replaces the deleted `concurrent()` execution strategy: where `concurrent()`
 * configured a `Transformer`, this configures a `Pipeline` - the chain is identical, only the
 * class differs.
 *
 * `apply()` does NOT call `Transformer.process()` the way the base class does - that bypass IS the
 * mechanism, since `process()` runs a chain sequentially, one chunk at a time. Instead it fans
 * `this._chunks` - the pipeline's OWN already-cut chunk stream, set by `.buffer()` (#39) - out
 * through `stageWork()`, the one method a subclass overrides to change WHERE a stage's work
 * actually happens (`HttpPipeline`, #17 L4, overrides it to POST).
 */

import type {
  IContextManager,
  InternalTransformer,
  ReduceFunction,
  SourcePolicy,
} from "@src/types";
import { Pipeline, type PipelineOptions, type PipelineSource } from "@src/pipeline";
import type { ChunkTransform } from "@src/pipeline";
import { Transformer } from "@src/transformer";
import { foldChunkStream } from "@src/utils/reduce";
import { share } from "@src/utils/chunk";
import { dropOrRethrow } from "@src/utils/helpers";

/** Construction-time knobs for `ConcurrentPipeline` and every class that extends it. */
export interface ConcurrentPipelineOptions {
  /** Chunks kept in flight at once. Default `4`. */
  maxConcurrency?: number;
  /** Restore input order in output. Default `true`. */
  ordered?: boolean;
}

/** Every `ConcurrentPipeline` constructor's real parameter type: its own knobs PLUS the base
 * `Pipeline` internals (`context`, `chunks`, …) that `createPipeline()` (below) must be able
 * to pass through on every copy-on-write call. Not exported - a caller only ever sees
 * `ConcurrentPipelineOptions`; the intersection is this file's own plumbing. */
type ConcurrentPipelineConstructorOptions = ConcurrentPipelineOptions & PipelineOptions;

/** One in-flight chunk's promise, tagged with an id so `fanOutUnordered` can tell which slot in
 * `inFlight` finished once `Promise.race` settles - `Promise.race` alone only returns the winning
 * VALUE, not which input promise produced it. */
interface TaggedResult<U> {
  id: number;
  result: U[];
}

/**
 * `ordered: true`'s fan-out: a sliding window of `maxConcurrency` chunks, yielded in ARRIVAL
 * order (never completion order) - a chunk finishing early still waits behind an earlier, slower
 * one. Streams: only `maxConcurrency` chunks are ever pulled ahead of what has been yielded.
 * Yields each dispatched chunk's own RESULT ARRAY, not flattened items (#39) - the fanned-out
 * output is itself a real `_chunks` boundary a later `.buffer()` can recut from.
 *
 * Every dispatched promise gets a throwaway `.catch(() => {})` the moment it is created - purely
 * to mark it "handled" for Node's unhandled-rejection detector. The ORIGINAL promise reference
 * (not the caught one) is what `inFlight` holds and what gets `await`ed in order below, so a real
 * failure still surfaces there, at the position it belongs to.
 *
 * @example
 * chunks `[[1],[2],[3],[4]]`, chunk `1` twelve times slower than the rest, `maxConcurrency: 4` →
 * yields `[1], [2], [3], [4]` in that order - `2`/`3`/`4` finish first but wait behind `1`.
 */
async function* fanOutOrdered<T, U>(
  chunks: AsyncIterable<T[]>,
  work: (chunk: T[], ctx: IContextManager) => U[] | Promise<U[]>,
  ctx: IContextManager,
  maxConcurrency: number,
): AsyncGenerator<U[]> {
  const inFlight: Promise<U[]>[] = [];

  for await (const chunk of chunks) {
    const p = Promise.resolve(work(chunk, ctx));
    p.catch(() => {
      // Handled-marker only; the `await` below (on this SAME promise) still throws for real.
    });
    inFlight.push(p);

    if (inFlight.length >= maxConcurrency) {
      yield await inFlight.shift()!;
    }
  }

  while (inFlight.length > 0) {
    yield await inFlight.shift()!;
  }
}

/**
 * `ordered: false`'s fan-out: a sliding window of `maxConcurrency` chunks, yielded in COMPLETION
 * order - never draining `chunks` before dispatching (the bug this replaces, #16/#17 Done-when 9).
 * `pullNext()` requests exactly one new chunk per completed slot, so at most `maxConcurrency`
 * chunks are ever in flight or buffered ahead of what has been yielded. Yields each dispatched
 * chunk's own RESULT ARRAY, not flattened items (#39) - same reason as `fanOutOrdered`, above.
 *
 * `Promise.race()` itself attaches a handler to EVERY promise passed to it, so no in-flight
 * promise is ever unhandled, win or lose the race - true even on rejection, and even for a
 * promise still pending when this generator stops after an earlier one throws.
 *
 * @example
 * chunks `[[1],[2],[3],[4]]`, `maxConcurrency: 2`, chunk `1` slower than `2` → yields `[2]` before
 * `[1]` (completion order), then `[3]` and `[4]` as their own slots free up.
 */
async function* fanOutUnordered<T, U>(
  chunks: AsyncIterable<T[]>,
  work: (chunk: T[], ctx: IContextManager) => U[] | Promise<U[]>,
  ctx: IContextManager,
  maxConcurrency: number,
): AsyncGenerator<U[]> {
  const iterator = chunks[Symbol.asyncIterator]();
  const inFlight = new Map<number, Promise<TaggedResult<U>>>();
  let nextId = 0;
  let exhausted = false;

  async function pullNext(): Promise<void> {
    if (exhausted) return;
    const next = await iterator.next();
    if (next.done) {
      exhausted = true;
      return;
    }
    const id = nextId++;
    const tagged = Promise.resolve(work(next.value, ctx)).then((result) => ({ id, result }));
    // Handled-marker only, the same reason as `fanOutOrdered`'s: `Promise.race()` (below) marks
    // every promise it is GIVEN as handled, but the ramp-up loop can throw (a failing
    // `iterator.next()`) before an earlier iteration's promise ever reaches a `race()` call -
    // this closes that gap regardless of when, or whether, `race()` gets to see it.
    tagged.catch(() => {});
    inFlight.set(id, tagged);
  }

  for (let i = 0; i < maxConcurrency && !exhausted; i++) {
    await pullNext();
  }

  while (inFlight.size > 0) {
    const { id, result } = await Promise.race(inFlight.values());
    inFlight.delete(id);
    yield result;
    await pullNext();
  }
}

/**
 * Merges N partitions' own reduceWork generators (`ConcurrentPipeline.reduce()`, #62) into one, in
 * COMPLETION order - the same pull-next-per-slot shape as `fanOutUnordered` above, but merging
 * whole generators rather than one promise per chunk: there is no order between partitions (a
 * partitioned reduce's own Constraints), so whichever partition's next output chunk is ready first
 * is yielded first. A partition dropping out (its own `share()` view of the shared source ran dry)
 * is simply removed from the race; the merge itself ends once every partition has.
 *
 * @example
 * two partitions, `[[8],[7]]` (fast) and `[[15]]` (slower) -> yields `[8]`, `[7]`, then `[15]` once
 * it arrives - completion order, never partition order.
 */
async function* mergeUnordered<U>(sources: AsyncGenerator<U[]>[]): AsyncGenerator<U[]> {
  const inFlight = new Map<number, Promise<{ id: number; result: IteratorResult<U[]> }>>();

  function pull(id: number): void {
    const tagged = sources[id]!.next().then((result) => ({ id, result }));
    // Handled-marker only, the same reason `fanOutUnordered`'s own tagged promises get one: a
    // partition that loses the race - or resolves after some OTHER partition already threw and
    // this generator stopped consuming - must never surface as an unhandled rejection.
    tagged.catch(() => {});
    inFlight.set(id, tagged);
  }

  for (let id = 0; id < sources.length; id++) pull(id);

  while (inFlight.size > 0) {
    const { id, result } = await Promise.race(inFlight.values());
    inFlight.delete(id);
    if (result.done) continue;
    yield result.value;
    pull(id);
  }
}

/**
 * Runs up to `maxConcurrency` chunks of one stage at once, in this process. Replaces the deleted
 * `concurrent()` execution strategy (#17): where `concurrent()` configured a `Transformer`, this
 * configures a `Pipeline` — the chain is identical, only the class differs.
 *
 * `new ConcurrentPipeline([1,2,3,4,5], { maxConcurrency: 4 }).transform((t) => t.map((x) => x *
 * 2)).toArray()` → `[2,4,6,8,10]`.
 */
// `In` (#90) is the type this pipeline is CALLED with, fixed when the chain is declared and carried
// unchanged through every stage - unlike `T`, which becomes each stage's own output. It defaults to
// `T` so an existing two-argument spelling keeps meaning what it did.
export class ConcurrentPipeline<T, M extends "async" = "async", In = T> extends Pipeline<
  T,
  "async",
  "async",
  In
> {
  /** Chunks of the current stage kept in flight at once. */
  readonly maxConcurrency: number;
  /** Whether output order is restored to match input order once a chunk finishes. */
  readonly ordered: boolean;

  constructor(options?: ConcurrentPipelineConstructorOptions) {
    super(options);
    this.maxConcurrency = options?.maxConcurrency ?? 4;
    // Validated eagerly, at construction - the deleted concurrent() strategy did the same (review
    // found this dropped: maxConcurrency <= 0 made fanOutUnordered's ramp-up loop never run at
    // all, silently returning [] without ever touching the source).
    if (this.maxConcurrency < 1) {
      throw new Error("maxConcurrency must be at least 1");
    }
    this.ordered = options?.ordered ?? true;
  }

  /**
   * Carries `maxConcurrency`/`ordered` into the NEXT instance a copy-on-write call
   * (`.context()`, `.buffer()`, `.transform()`, `.apply()`) builds, the same way
   * `HttpPipeline`/`ClusterPipeline` (#17 L4/L5) override this method again for their own extra
   * knobs (`url`, `workers`).
   *
   * @example
   * `new ConcurrentPipeline([1], { maxConcurrency: 8 }).context({ k: 1 }).maxConcurrency` → `8`,
   * not the constructor default `4` - without this override, `Pipeline.createPipeline()`'s base
   * implementation reconstructs via `this.constructor` but only forwards `PipelineOptions` fields,
   * which do not include `maxConcurrency`.
   */
  protected override createPipeline<U>(
    chunks: AsyncIterable<U[]>,
    options: PipelineOptions,
  ): ConcurrentPipeline<U, M, In> {
    const Ctor = this.constructor as new (
      options?: ConcurrentPipelineConstructorOptions,
    ) => ConcurrentPipeline<U, M, In>;
    return new Ctor({ ...options, ...this.concurrentOptions(), chunks });
  }

  /** This level's OWN knobs, for a subclass's `createPipeline()` override to spread alongside its
   * own extra ones (`HttpPipeline.url`, `ClusterPipeline.workers`) - the one place
   * `maxConcurrency`/`ordered` are listed, so a future knob added here needs no edit in
   * `HttpPipeline`/`ClusterPipeline` to keep surviving copy-on-write (review: three separate
   * hand-copied field lists is exactly the shape that drops a knob when one copy is missed). */
  protected concurrentOptions(): ConcurrentPipelineOptions {
    return {
      maxConcurrency: this.maxConcurrency,
      ordered: this.ordered,
    };
  }

  /**
   * Always dispatches - `.local(build)` (#61) is what keeps a stage in the orchestrating process
   * now, wrapping a whole region rather than flagging one call. Everything here goes through this
   * class's own `stageWork()` fan-out below.
   */
  override transform<U, M2 extends "sync" | "async">(
    // The same `"unset"` refusal the base carries (#90). Without it here, an override re-declares
    // `transform` WITHOUT the guard and a source-less dispatching chain compiles, then resolves to
    // `[]` at runtime - a chain composed with no engine decided, which is what the guard exists to
    // make impossible.
    builder: (t: Transformer<T, T, "async">) => Transformer<T, U, M2>,
  ): ConcurrentPipeline<U, M, In> {
    // A dispatching class is `"async"` whatever its callbacks return, so the seed is typed there
    // rather than at the caller's own Mode.
    //
    // The conditional `this` this override used to carry is gone with the base's own (#90): its
    // guard read `M extends "unset"`, and this class fixes `M` at `"async"`, so it never once
    // refused anything. Composing before an input is the ordinary case now regardless.
    const seed = new Transformer<T, T, "async">({ transform: (chunk) => chunk });
    return this.apply(builder(seed));
  }

  override apply<U>(
    transformer: Transformer<T, U, "sync" | "async">,
  ): ConcurrentPipeline<U, M, In> {
    // This body does not delegate to the base's `apply()`, so it needs the base's own source guard
    // (#90) - a dispatching class has no `"unset"` Mode for a conditional `this` to refuse.
    this.requireSource();
    const stageIndex = this._chunkTransforms.length;
    const rawWork = this.stageWork(transformer, stageIndex);
    // The run handler's own chunk-drop decision (#78) - `dropOrRethrow` (`utils/helpers.ts`) is the
    // same "call the handler, or propagate" `Transformer.process()`'s own `runSequentially` loop
    // makes for a local stage; a handler that returns (rather than throws) means "drop this chunk",
    // and `[]` is the empty-chunk answer the fan-out below needs for that. No handler registered:
    // `dropOrRethrow` rethrows, same as before #78.
    const work: InternalTransformer<T, U> = async (chunk, ctx) => {
      try {
        return await rawWork(chunk, ctx);
      } catch (error) {
        await dropOrRethrow(this._runHandler, error as Error, ctx);
        return [];
      }
    };
    const fanOut = this.ordered ? fanOutOrdered : fanOutUnordered;
    // `this._chunks` handed straight to the fan-out - no chunking call of this class's own (#39):
    // whatever boundary `.buffer()` (or the constructor's own default) already cut is what gets
    // dispatched, chunk for chunk.
    const newChunks = fanOut(this._chunks, work, this._context, this.maxConcurrency);

    return this.createPipeline<U>(newChunks, {
      // Spread first (#90): a dispatched stage that rebuilt its options field by field silently
      // dropped `mode`, so the pipeline reverted to `"unset"` after its first `.transform()` and
      // `.local()`'s own region then refused to compose a stage at all.
      ...this.carriedOptions(),
      context: this._context,
      chunkTransforms: [
        ...this._chunkTransforms,
        // `transformer.runnable()` (#78), not `transformer.transform` directly - the seam that
        // carries the transformer's own row handler in, so a WORKER's identical registry entry
        // (`HttpPipeline.fetch()`'s own `_chunkTransforms[requested]` lookup) gets row recovery too.
        transformer.runnable() as unknown as ChunkTransform,
      ],
      // Carried forward like every base `Pipeline` copy-on-write method already does (#45) - a
      // dropped `_reduceStages` here would silently lose a stage a prior `.reduce()` registered
      // the moment `ConcurrentPipeline.reduce()` (#45 L3) stops throwing and starts populating it.
      reduceStages: this._reduceStages,
      // A dispatched stage's own output IS a real chunk stream now (#39) - a later `.buffer()`
      // flattens it like any other stage's output, so no pre-buffer item view survives this call.
      preBufferItems: null,
      runHandler: this._runHandler,
    });
  }

  /**
   * Folds every chunk this pipeline produces (#45) by PARTITIONING it (#62): `maxConcurrency`
   * independent accumulators, each its own `reduceWork()` call over its own `share()` view of the
   * ONE underlying chunk stream (free-slot dealing, `src/utils/chunk.ts` - a slow partition simply
   * calls `.next()` less often, so the others pick up its slack), merged in completion order since
   * there is no order between partitions. Each partition's own result - an `emit()` mid-fold, or its
   * trailing accumulator once its share of the stream ends - flows downstream as an ordinary value,
   * exactly like a non-partitioned reduce's own `emit()` output already does: no debt, no throw, no
   * combine step. A caller who wants ONE final value writes an ordinary second reduce, the same way
   * they would fold down any other multi-value reduce output: `.local((p) => p.reduce(mergeFn,
   * initial))` runs it in-process, over the WHOLE stream, sequentially.
   *
   * `new ConcurrentPipeline([1,2,3,4,5],{maxConcurrency:2}).buffer(2).reduce((a,x)=>a+x,0)
   * .local((p)=>p.reduce((a,v)=>a+v,0)).toArray()` → `[15]`.
   */
  override reduce<U>(fn: ReduceFunction<U, T>, initial: U): ConcurrentPipeline<U, M, In> {
    // A dispatching class has no `"unset"` Mode for a compile-time guard to test, so the refusal is
    // this call - the same reason `apply()` above makes it (#90).
    this.requireSource();
    const { stageIndex, chunkTransforms, reduceStages } = this.pushReduceStage(fn, initial);
    const work = this.reduceWork(fn, initial, stageIndex);

    // ONE shared iterator over `this._chunks` - `maxConcurrency` partitions each get their own
    // `share()` view of it, never their own slice: the partition count is a CEILING, not a
    // promise, since a partition whose view never sees a chunk (fewer chunks than partitions)
    // simply yields nothing. Each partition's own result (an emit mid-fold, or its trailing
    // accumulator once its share of chunks ends) flows downstream as an ordinary value - no
    // debt, no throw. A caller who wants ONE final value writes an ordinary second reduce, same
    // as any other multi-value reduce output: `.local((p) => p.reduce(mergeFn, initial))`.
    const iterator = this._chunks[Symbol.asyncIterator]();
    const partitions = Array.from({ length: this.maxConcurrency }, () =>
      work(share(iterator), this._context),
    );
    const newChunks = mergeUnordered(partitions);

    return this.createPipeline<U>(newChunks, {
      ...this.carriedOptions(),
      context: this._context,
      chunkTransforms,
      reduceStages,
      preBufferItems: null,
    });
  }

  /**
   * Narrows `Pipeline.local()`'s return type only (#61, `~/.claude/rules/typescript.md`) - the body
   * is an unchanged `super()` call, since `local()`'s own base implementation already builds a
   * plain `Pipeline` for the region and carries the result back through THIS class's own
   * `createPipeline()` override, which is what keeps `maxConcurrency`/`ordered` alive for whatever
   * comes after the region.
   */
  /**
   * Forced `"async"` whatever the source's shape (#90) - ConcurrentPipeline exists for I/O-bound work and
   * has no synchronous case, so an array source runs on the async engine here exactly as an
   * `AsyncIterable` one does. `sourcePolicy()` below is the runtime half; the `"async"` third type
   * argument on the `extends` clause above is the compile-time half, and is what makes this
   * override a genuine narrowing of the base's own two arms rather than a conflict with them.
   *
   * `new ConcurrentPipeline().from([1, 2, 3])` → `ConcurrentPipeline<number, "async">`.
   */
  override from<U>(data: PipelineSource<U>): ConcurrentPipeline<U, M> {
    // `In` becomes `U` here, not the receiver's own: `.from()` BINDS an input, so whatever the
    // chain accepted before is spent. Every other override carries `In` through unchanged.
    return this.fromSource<U>(data, "async") as unknown as ConcurrentPipeline<U, M>;
  }

  protected override sourcePolicy(): SourcePolicy {
    return "async";
  }

  override local<U, M2 extends "sync" | "async">(
    build: (p: Pipeline<T, "async", "shape", any>) => Pipeline<U, M2, "shape", any>,
  ): ConcurrentPipeline<U, M, In> {
    return super.local(build) as unknown as ConcurrentPipeline<U, M, In>;
  }

  /**
   * The one method a subclass overrides to change WHERE a reducer runs (#45). `stageWork()`'s
   * sibling: a reducer streams in and out (it emits fewer or more values than it consumes), so this
   * returns a generator over OUTPUT CHUNKS rather than an `InternalTransformer`. `.reduce()` (#62)
   * calls this ONE closure `maxConcurrency` times, once per partition, each over its own `share()`
   * view of the shared chunk stream - this class's own implementation (below) folds one partition
   * in-process and sequentially, one accumulator PER PARTITION now, not one for the whole stage;
   * `HttpPipeline` overrides it to open one duplex POST per partition instead, so N partitions are N
   * concurrent POSTs to the SAME `/reduce/<n>`, each with its own accumulator server-side
   * (`runReduceStage` builds a fresh `Reducer` per request already, unchanged by #62).
   */
  protected reduceWork<U>(
    fn: ReduceFunction<U, T>,
    initial: U,
    _stageIndex: number,
  ): (chunks: AsyncIterable<T[]>, ctx: IContextManager) => AsyncGenerator<U[]> {
    return (chunks, ctx) => foldChunkStream(fn, initial, chunks, ctx);
  }

  /**
   * The one method a subclass overrides to change WHERE a stage runs. `ConcurrentPipeline`'s own
   * implementation runs the stage's work directly, in-process; `HttpPipeline` (#17 L4) overrides
   * it to POST the chunk to another instance instead - `apply()`'s own fan-out (above) is
   * identical either way, since it only ever calls `stageWork()`'s return value.
   *
   * `stageWork(transformer, 0)([1,2], ctx)` → `transformer.transform([1,2], ctx)`'s own result.
   */
  protected stageWork<U>(
    transformer: Transformer<T, U, "sync" | "async">,
    _stageIndex: number,
  ): InternalTransformer<T, U> {
    // `transformer.runnable()` (#78) wires the transformer's own row handler into every dispatch
    // this method's return value drives - the same seam `apply()` (above) uses to populate this
    // pipeline's own `_chunkTransforms` entry.
    return transformer.runnable();
  }
}
