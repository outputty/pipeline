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
  PipelineMode,
  ChunkTransform,
  Tagged,
  ReduceWork,
} from "@src/types";
import { Pipeline, type PipelineConstructorOptions, type WrappablePipeline } from "@src/pipeline";
import { Transformer } from "@src/transformer";
import { foldChunkStream } from "@src/utils/reduce";
import { share } from "@src/utils/chunk";
import { runStageChunk } from "@src/utils/helpers";

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
type ConcurrentPipelineConstructorOptions = ConcurrentPipelineOptions & PipelineConstructorOptions;

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
  // `Tagged<U[]>` (`@src/types`), not a local `TaggedResult` (#133) - each in-flight chunk's own
  // promise, tagged with an id so this function can tell which slot in `inFlight` finished once
  // `Promise.race` settles: `Promise.race` alone only returns the winning VALUE, not which input
  // promise produced it.
  const inFlight = new Map<number, Promise<Tagged<U[]>>>();
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

  // The source is closed however this generator ends - exhausted, thrown, or stopped early by a
  // consumer's `break`/`.first(n)` (#113). `fanOutOrdered` gets this from its own `for await`,
  // which calls `.return()` on exit; a MANUAL iterator has to do it, and without this the same
  // chain leaked its source under `ordered: false` and released it under `ordered: true` - one
  // boolean apart, two resource outcomes, invisible until the process runs out of handles.
  try {
    for (let i = 0; i < maxConcurrency && !exhausted; i++) {
      await pullNext();
    }

    while (inFlight.size > 0) {
      const { id, result } = await Promise.race(inFlight.values());
      inFlight.delete(id);
      yield result;
      await pullNext();
    }
  } finally {
    if (!exhausted) await iterator.return?.();
  }
}

/**
 * One partition's own accumulator seed, copied from the caller's `initial` (#113).
 *
 * `Pipeline.reduce(fn, initial)` takes a VALUE, and a partitioned reduce needs one accumulator per
 * partition - so handing the same object to all of them made them one accumulator wearing N names.
 * A primitive copies by assignment; anything else is `structuredClone`d.
 *
 * A seed `structuredClone` cannot copy - a function, a class instance, anything holding one - RAISES
 * here rather than silently reverting to the shared object that produced the defect. The caller's
 * own fix is `.local((p) => p.reduce(fn, initial))`, which runs one unpartitioned fold in this
 * process, so the seed is never copied at all.
 *
 * `seedFor(0)` → `0`. `seedFor([])` → a fresh `[]` each call.
 */
function seedFor<U>(initial: U): U {
  if (initial === null || typeof initial !== "object") return initial;

  let copy: U;
  try {
    copy = structuredClone(initial);
  } catch (error) {
    throw new Error(`${SEED_REFUSAL}: ${(error as Error).message}`);
  }

  // A CLASS INSTANCE does not throw - `structuredClone` copies its own properties and silently
  // drops the prototype, so the partition folds into a stripped object and fails later with
  // something like `acc.add is not a function`, pointing at the caller's own reducer rather than at
  // the copy. Comparing prototypes catches exactly that: `Map`, `Set`, `Date` and a plain object
  // or array all keep theirs, and anything carrying behaviour does not.
  if (Object.getPrototypeOf(copy) !== Object.getPrototypeOf(initial)) {
    throw new Error(`${SEED_REFUSAL}: a class instance loses its prototype when copied`);
  }
  return copy;
}

/** `seedFor`'s refusal, one string so its two throw sites cannot drift on the advice they give. */
const SEED_REFUSAL =
  "a partitioned reduce needs one accumulator per partition, and this seed cannot be copied. " +
  "Pass a seed structuredClone can copy, or wrap the fold in " +
  ".local((p) => p.reduce(fn, initial)) to run it unpartitioned in this process";

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
  const inFlight = new Map<number, Promise<Tagged<IteratorResult<U[]>>>>();

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
export class ConcurrentPipeline<T, In = T> extends Pipeline<T, "async", In> {
  /** Chunks of the current stage kept in flight at once. */
  readonly maxConcurrency: number;
  /** Whether output order is restored to match input order once a chunk finishes. */
  readonly ordered: boolean;

  /** Wraps a chain built elsewhere, running its stages concurrently (#90) - the chain says WHAT to
   * do, this class says WHERE. Only a source-less pipeline can be wrapped, since its stages are
   * still recorded calls to replay; one already bound through `.from()` is refused. */
  constructor(pipeline: WrappablePipeline<T, In>, options?: ConcurrentPipelineOptions);
  constructor(options?: ConcurrentPipelineConstructorOptions);
  constructor(
    first?: WrappablePipeline<T, In> | ConcurrentPipelineConstructorOptions,
    second?: ConcurrentPipelineOptions,
  ) {
    const options = Pipeline.wrapping<ConcurrentPipelineConstructorOptions>(first, second);
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
   * (`.context()`, `.buffer()`, `.transform()`, `.apply()`) builds, via `Pipeline.createPipeline()`'s
   * own `carriedKnobs()` seam (#133) - `HttpPipeline`/`ClusterPipeline`/`EventEmitterPipeline` each
   * override this method again, `{ ...super.carriedKnobs(), <their own field(s)> }`, for their own
   * extra knobs (`url`, `workers`, `emitter`).
   *
   * @example
   * `new ConcurrentPipeline({ maxConcurrency: 8 }).context({ k: 1 }).maxConcurrency` → `8`, not the
   * constructor default `4` - without this override, `Pipeline.createPipeline()`'s base
   * implementation reconstructs via `this.constructor` but only forwards `PipelineConstructorOptions`
   * fields, which do not include `maxConcurrency`.
   */
  protected override carriedKnobs(): ConcurrentPipelineOptions {
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
  ): ConcurrentPipeline<U, In> {
    // A dispatching class is `"async"` whatever its callbacks return, so the seed is typed there
    // rather than at the caller's own Mode.
    //
    // The conditional `this` this override used to carry is gone with the base's own (#90): its
    // guard read `M extends "unset"`, and this class fixes `M` at `"async"`, so it never once
    // refused anything. Composing before an input is the ordinary case now regardless.
    const seed = new Transformer<T, T, "async">({ transform: (chunk) => chunk });
    return this.apply(builder(seed));
  }

  override apply<U>(transformer: Transformer<T, U, "sync" | "async">): ConcurrentPipeline<U, In> {
    // This body does not delegate to the base's `apply()`, so it repeats the base's own deferral
    // (#90): with no input yet, the call is recorded and replayed later - against THIS class, so
    // the replayed stage still dispatches. Without it a dispatching pipeline inherited the call
    // signature and typed fine, then threw `no source: call .from(data) before composing a stage`
    // the moment a stage was composed.
    if (this.isDeferred()) {
      return this.defer<U, ConcurrentPipeline<U, In>>((p) =>
        p.apply(transformer as Transformer<unknown, U, "sync" | "async">),
      );
    }
    const stageIndex = this._chunkTransforms.length;
    const rawWork = this.stageWork(transformer, stageIndex);
    // `runStageChunk` (`utils/helpers.ts`) is the same "call the handler, or propagate" decision
    // the sync engine makes (#78/#90) - a handler that returns rather than throws means "drop this
    // chunk", and `[]` is the empty-chunk answer the fan-out below needs for that. Kept `async` on
    // purpose: it turns a synchronous throw from a LOCAL `rawWork` into a rejection, which
    // `fanOutOrdered` needs to fail at the chunk's own ordered position.
    const work: InternalTransformer<T, U> = async (chunk, ctx) =>
      runStageChunk(rawWork, chunk, ctx, this._runHandler);
    const fanOut = this.ordered ? fanOutOrdered : fanOutUnordered;
    // `this._chunks` handed straight to the fan-out - no chunking call of this class's own (#39):
    // whatever boundary `.buffer()` (or the constructor's own default) already cut is what gets
    // dispatched, chunk for chunk.
    const newChunks = fanOut(this._chunks, work, this._context, this.maxConcurrency);

    // The explicit 2nd type argument is `createPipeline()`'s own `R` (#133, `pipeline.ts`) - it
    // hands back `ConcurrentPipeline<U, In>` directly, no trailing `as X` cast of this method's own.
    return this.createPipeline<U, ConcurrentPipeline<U, In>>(newChunks, {
      // Spread first (#90): a dispatched stage that rebuilt its options field by field silently
      // dropped `mode`, so the pipeline reverted to `"unset"` after its first `.transform()` and
      // `.local()`'s own region then refused to compose a stage at all.
      ...this.carriedOptions(),
      chunkTransforms: [
        ...this._chunkTransforms,
        // `transformer.runnable()` (#78), not `transformer.transform` directly - the seam that
        // carries the transformer's own row handler in, so a WORKER's identical registry entry
        // (`HttpPipeline.fetch()`'s own `_chunkTransforms[requested]` lookup) gets row recovery too.
        transformer.runnable() as unknown as ChunkTransform,
      ],
      // A dispatched stage's own output IS a real chunk stream now (#39) - a later `.buffer()`
      // flattens it like any other stage's output, so no pre-buffer item view survives this call
      // (`freshPreBuffer()` also nulls `syncPreBufferItems`, a no-op here - this class is always
      // `"async"` and has no sync chunk stream of its own to reset).
      ...this.freshPreBuffer(),
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
  override reduce<U>(fn: ReduceFunction<U, T>, initial: U): ConcurrentPipeline<U, In> {
    // Defers with no input yet, the same as `apply()` above (#90). Replaying it through this same
    // method is what keeps the partitioning (#62) identical either way.
    if (this.isDeferred()) {
      return this.defer<U, ConcurrentPipeline<U, In>>((p) =>
        p.reduce(
          fn as (acc: U, item: unknown, ctx: IContextManager, emit: (v: U) => void) => U,
          initial,
        ),
      );
    }
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

    // See `apply()`'s own identical `createPipeline<U, R>()` call above.
    return this.createPipeline<U, ConcurrentPipeline<U, In>>(newChunks, {
      ...this.carriedOptions(),
      chunkTransforms,
      reduceStages,
      ...this.freshPreBuffer(),
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
   * Forced `"async"` whatever the source's shape (#90) - `ConcurrentPipeline`, `HttpPipeline` and
   * `ClusterPipeline` all exist for I/O-bound work and have no synchronous case, so an array source
   * runs on the async engine here exactly as an `AsyncIterable` one does. `HttpPipeline`/
   * `ClusterPipeline` inherit this override unchanged rather than re-declaring it (#133: both used
   * to redeclare an identical `return "async"`, and their own `bind()` overrides, which narrowed
   * `Pipeline.bind()`'s return type to their own class and nothing else, added no behavior at all -
   * `Pipeline.bind()` already dispatches through `this.sourcePolicy()` polymorphically, so the base
   * implementation alone is correct on every subclass).
   *
   * `new ConcurrentPipeline(chain)([1, 2, 3])` runs on the async engine whatever `chain` was; so does
   * `new HttpPipeline(chain, { url })` and `new ClusterPipeline(chain)`, both through this same
   * override.
   */
  protected override sourcePolicy(): SourcePolicy {
    return "async";
  }

  override local<U, M2 extends PipelineMode>(
    build: (p: Pipeline<T, "async", any>) => Pipeline<U, M2, any>,
  ): ConcurrentPipeline<U, In> {
    return super.local(build) as unknown as ConcurrentPipeline<U, In>;
  }

  /** Re-declared ONLY to narrow `Pipeline.queue()`'s return type (#123,
   * `~/.claude/rules/typescript.md`) - same reason as `.local()` above. The body is an unchanged
   * `super.queue()` call: `.queue()`'s own prefetch engine reads `this.chunkStream()`, which already
   * dispatches through this class's own chunk stream regardless of the class calling it. */
  override queue(capacity: number): ConcurrentPipeline<T, In> {
    return super.queue(capacity) as unknown as ConcurrentPipeline<T, In>;
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
  ): ReduceWork<T, U> {
    // A SEED PER PARTITION, not the caller's one value handed to all of them (#113). This closure
    // is called `maxConcurrency` times, so a mutable `initial` was one accumulator shared by every
    // partition: measured, `.buffer(1).reduce((acc, x) => (acc.push(x), acc), [])` over `[1,2,3,4]`
    // at `maxConcurrency: 2` returned `[[1,2,3,4],[1,2,3,4]]` - the SAME array twice, where two
    // partitions owe `[[1,3],[2,4]]` - and a downstream merge then double-counted every item.
    return (chunks, ctx) => foldChunkStream(fn, seedFor(initial), chunks, ctx);
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
