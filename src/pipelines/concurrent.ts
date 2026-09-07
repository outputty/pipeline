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

import type { IContextManager, InternalTransformer, ReduceFunction } from "@src/types";
import { Pipeline, type PipelineOptions, type PipelineSource } from "@src/pipeline";
import type { ChunkTransform } from "@src/pipeline";
import { Transformer } from "@src/transformer";

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

/** Per-stage override, passed as `.transform()`/`.apply()`'s second argument. */
export interface StageOptions {
  /** Run this one stage in the orchestrating process instead of dispatching it. */
  local?: boolean;
}

/** One in-flight chunk's promise, tagged with an id so `fanOutUnordered` can tell which slot in
 * `inFlight` finished once `Promise.race` settles - `Promise.race` alone only returns the winning
 * VALUE, not which input promise produced it. */
interface TaggedResult<U> {
  id: number;
  result: U[];
}

/**
 * Which of a `Transformer`'s knobs are INERT on a DISPATCHED stage - `stageWork()` runs the stage
 * directly (its own `.transform` function, or a POST over the wire), never `Transformer.process()`,
 * so `.hooks`/`.onError()` never take effect there (architecture.md's own documented contract;
 * `.buffer()` reaches a dispatched stage exactly like a local one now, since `Pipeline` owns the
 * cut, so there is nothing left to check for chunking - #39 deleted the whole `chunkSize`/
 * `setChunker` half of this check along with `Transformer`'s own knobs).
 *
 * `dispatchKnobViolations(new Transformer().withHooks({}))` → `["withHooks"]`;
 * `dispatchKnobViolations(new Transformer().onError(() => {}))` → `["onError"]`;
 * `dispatchKnobViolations(new Transformer())` → `[]`.
 */
function dispatchKnobViolations<In, Out>(transformer: Transformer<In, Out>): string[] {
  const violations: string[] = [];
  if (transformer.hooks !== undefined) violations.push("withHooks");
  if (transformer.errorHandler.hasHandlers()) violations.push("onError");
  return violations;
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
 * Runs up to `maxConcurrency` chunks of one stage at once, in this process. Replaces the deleted
 * `concurrent()` execution strategy (#17): where `concurrent()` configured a `Transformer`, this
 * configures a `Pipeline` — the chain is identical, only the class differs.
 *
 * `new ConcurrentPipeline([1,2,3,4,5], { maxConcurrency: 4 }).transform((t) => t.map((x) => x *
 * 2)).toArray()` → `[2,4,6,8,10]`.
 */
export class ConcurrentPipeline<T> extends Pipeline<T> {
  /** Chunks of the current stage kept in flight at once. */
  readonly maxConcurrency: number;
  /** Whether output order is restored to match input order once a chunk finishes. */
  readonly ordered: boolean;

  constructor(source: PipelineSource<T>, options?: ConcurrentPipelineConstructorOptions) {
    super(source, options);
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
  ): ConcurrentPipeline<U> {
    const Ctor = this.constructor as new (
      data: PipelineSource<U>,
      options?: ConcurrentPipelineConstructorOptions,
    ) => ConcurrentPipeline<U>;
    return new Ctor([], { ...options, ...this.concurrentOptions(), chunks });
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
   * `options?.local: true` is `super.apply(transformer)` at every level (product.md) - the base
   * class's own `Transformer.process()` path, in-process, sequential, no fan-out. Everything else
   * goes through this class's own `stageWork()` fan-out below.
   */
  override transform<U>(
    builder: (t: Transformer<T, T>) => Transformer<T, U>,
    options?: StageOptions,
  ): ConcurrentPipeline<U> {
    return this.apply(builder(new Transformer<T, T>({ transform: (chunk) => chunk })), options);
  }

  override apply<U>(transformer: Transformer<T, U>, options?: StageOptions): ConcurrentPipeline<U> {
    if (options?.local) {
      return super.apply(transformer) as ConcurrentPipeline<U>;
    }

    // `withHooks`/`onError` only ever take effect through `Transformer.process()`, which
    // `stageWork()` (below) never calls on ANY consumption path - not just async-iteration, the
    // way the base class's own terminal-op path is fine but its old source-position path was not.
    // Fail loud immediately rather than silently never firing.
    const knobViolations = dispatchKnobViolations(transformer);
    if (knobViolations.length > 0) {
      throw new Error(
        `${this.constructor.name}: ${knobViolations.join("/")} never take effect on a dispatched ` +
          `stage (stageWork() runs the stage directly, never Transformer.process()). Drop the ` +
          `knob, or pass { local: true } to run this stage in-process instead.`,
      );
    }

    const stageIndex = this._chunkTransforms.length;
    const work = this.stageWork(transformer, stageIndex);
    const fanOut = this.ordered ? fanOutOrdered : fanOutUnordered;
    // `this._chunks` handed straight to the fan-out - no chunking call of this class's own (#39):
    // whatever boundary `.buffer()` (or the constructor's own default) already cut is what gets
    // dispatched, chunk for chunk.
    const newChunks = fanOut(this._chunks, work, this._context, this.maxConcurrency);

    return this.createPipeline<U>(newChunks, {
      context: this._context,
      chunkTransforms: [
        ...this._chunkTransforms,
        transformer.transform as unknown as ChunkTransform,
      ],
      // Carried forward like every base `Pipeline` copy-on-write method already does (#45) - a
      // dropped `_reduceStages` here would silently lose a stage a prior `.reduce()` registered
      // the moment `ConcurrentPipeline.reduce()` (#45 L3) stops throwing and starts populating it.
      reduceStages: this._reduceStages,
      // A dispatched stage's own output IS a real chunk stream now (#39) - a later `.buffer()`
      // flattens it like any other stage's output, so no pre-buffer item view survives this call.
      preBufferItems: null,
    });
  }

  /**
   * Fold every chunk this pipeline produces (#45) — STUB (L1): the real signature, body throwing.
   * Adds `StageOptions` on top of the base `Pipeline.reduce(fn, initial)` signature, mirroring
   * `apply()`/`transform()`'s own base-vs-`{ local: true }` split (L3 fills in the fold via
   * `reduceWork()`, this class's own override point, `stageWork()`'s sibling).
   */
  override reduce<U>(
    _fn: ReduceFunction<U, T>,
    _initial: U,
    _options?: StageOptions,
  ): ConcurrentPipeline<U> {
    throw new Error("ConcurrentPipeline.reduce: not implemented (#45 L3)");
  }

  /**
   * The one method a subclass overrides to change WHERE a reducer runs (#45) — STUB (L1): the real
   * signature, body throwing. `stageWork()`'s sibling: a reducer streams in and out (it emits fewer
   * or more values than it consumes), so this returns a generator over OUTPUT CHUNKS rather than an
   * `InternalTransformer`. `ConcurrentPipeline` (L3) folds in-process and sequentially -
   * `maxConcurrency` is inert on a reduce stage, one accumulator, one connection; `HttpPipeline` (L5)
   * overrides it to open one duplex POST instead.
   */
  protected reduceWork<U>(
    _fn: ReduceFunction<U, T>,
    _initial: U,
    _stageIndex: number,
  ): (chunks: AsyncIterable<T[]>, ctx: IContextManager) => AsyncGenerator<U[]> {
    throw new Error("ConcurrentPipeline.reduceWork: not implemented (#45 L3)");
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
    transformer: Transformer<T, U>,
    _stageIndex: number,
  ): InternalTransformer<T, U> {
    return (chunk, ctx) => transformer.transform(chunk, ctx);
  }
}
