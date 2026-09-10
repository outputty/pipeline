/**
 * Pipeline class - high-level API for composing transformers with data sources.
 *
 * Python equivalent:
 * ```python
 * class Pipeline[T]:
 *   def __init__(self, *data: Iterable[T], context_manager: IContextManager | None = None):
 *     if len(data) == 0:
 *       raise ValueError("At least one data source must be provided to Pipeline.")
 *     self.data_source = itertools.chain.from_iterable(data) if len(data) > 1 else data[0]
 *     self.processed_data = iter(self.data_source)
 *     self.context_manager = context_manager or SimpleContextManager()
 *
 *   def apply(self, transformer) -> "Pipeline[U]": ...
 *   def transform(self, t) -> "Pipeline[U]": ...
 *   def buffer(self, size) -> "Pipeline[T]": ...
 *   def to_list() -> (list, context): ...
 *   def first(n) -> (list, context): ...
 *   def consume() -> context: ...
 *   def each(fn) -> context: ...
 *   def branch(branches) -> (dict, context): ...
 * ```
 */

import type {
  IContextManager,
  ReduceFunction,
  PipelineFunction,
  PipelineErrorHandler,
  PipelineMode,
  SourcePolicy,
  AssignMode,
  JoinMode,
  SeedMode,
} from "./types";
import { DEFAULT_CHUNK_SIZE } from "./types";
import { SimpleContextManager } from "./context/simple";
import { Transformer } from "./transformer";
import type { MaybeAsyncChunks } from "./utils/chunk";
import {
  buildChunkGenerator,
  buildSyncChunkGenerator,
  flattenChunks,
  recutSyncChunks,
  drainSync,
} from "./utils/chunk";
import { chain, isThenable, dropOrRethrow, settleMaybe } from "./utils/helpers";
import { PipelineResult } from "./result";
import { BranchBuilder, type ResultsOf, type ModeOfArms, type BranchArm } from "./branch";
import { foldChunkStream, foldSyncChunkStream } from "./utils/reduce";

/** The chunk stream a `Pipeline` that has no source yet carries - `.from()` is what replaces it.
 * Shared rather than rebuilt per instance: it is empty and stateless. */
const EMPTY_CHUNKS: AsyncIterable<never[]> = {
  // eslint-disable-next-line @typescript-eslint/require-await
  async *[Symbol.asyncIterator]() {},
};

/** Converts a sync iterable to an async one, for the `"async"` arm of `.from()` alone. A `"sync"`
 * source never goes through this: skipping it is most of what #90 recovers. */
function toAsyncIterable<U>(data: PipelineSource<U>): AsyncIterable<U> {
  if (Symbol.asyncIterator in Object(data)) {
    return data as AsyncIterable<U>;
  }
  const syncIterable = data as Iterable<U>;
  return {
    [Symbol.asyncIterator]: async function* () {
      for (const item of syncIterable) {
        yield item;
      }
    },
  };
}

/**
 * A chunk-wise transform function: takes one chunk (array) and produces the
 * next chunk (array), optionally reading/writing the shared context.
 *
 * Exported (#17) so a dispatching subclass (`ConcurrentPipeline`/`HttpPipeline`/`ClusterPipeline`,
 * `src/pipelines/`) can type its own `_chunkTransforms`-adjacent bookkeeping against the same shape
 * `Pipeline` itself uses, rather than re-declaring it.
 */
export type ChunkTransform = (
  chunk: unknown[],
  ctx: IContextManager,
) => unknown[] | Promise<unknown[]>;

/**
 * What a `Pipeline<T>` may be built from - a stream/collection of items. Terminal ops
 * (`toArray`/`first`/…) and async iteration both read the SAME persisted chunk stream (#39) - there
 * is no longer a separate pre-chunked-array reading, since the only consumer that ever cared about
 * an array-shaped source element (the deleted source-position replay) is gone.
 */
export type PipelineSource<T> = AsyncIterable<T> | Iterable<T>;

/**
 * A `Pipeline` whose Mode is not tracked (#90) - what the copy-on-write seam `createPipeline()` and
 * the stage seams `apply()`/`reduce()`/`local()` produce.
 *
 * None of them can know the Mode: `createPipeline()` reconstructs through `this.constructor`, and
 * `apply()` takes a transformer of either Mode. The PUBLIC method that returns one re-asserts it -
 * `.transform()` from its callback's own return, `.from()` from the source's shape - which is the
 * one place the Mode is actually decided. Keeping the erasure to this alias rather than spelling
 * `"sync" | "async"` at each site means a reader sees "the Mode is re-asserted above" rather than
 * "this really can be either".
 */
export type AnyPipeline<U> = Pipeline<U, PipelineMode, SourcePolicy, any>;

/**
 * A chain a wrapping class can adopt (#90): source-less, so its stages are still recorded calls to
 * replay, and carrying its own input type `In` so the wrapper can accept the same input.
 *
 * `In` is named rather than `any` because a wrapper's constructor INFERS it: with `any` there the
 * wrapper fell back to its own `T`, which is each stage's OUTPUT type, so
 * `new ConcurrentPipeline(numberToString)` typed its input `string` and rejected the `number[]`
 * that ran fine at runtime. `"unset"` is what makes wrapping a `.from()`-bound pipeline a compile
 * error rather than only a runtime throw.
 */
export type WrappablePipeline<T, In> = Pipeline<T, "unset", SourcePolicy, In>;

/**
 * One stage recorded on a source-less pipeline, replayed against the bound pipeline once an input
 * arrives (#90).
 *
 * A stage cannot run when it is composed, because there is nothing yet to run it over. Recording
 * the CALL rather than its result is what lets the replay go back through the real `.apply()`,
 * `.reduce()` and `.local()` - so a deferred chain and a `.from()` chain execute the same code, and
 * a stage kind added later needs no deferral logic of its own.
 */
export type PendingStage = (pipeline: AnyPipeline<any>) => AnyPipeline<any>;

/**
 * Groups one run's items by the arm each belongs to (#90) - `.branch()`'s demux.
 *
 * Runs in the ORCHESTRATING process by decision, never dispatched: a predicate decides WHICH arm an
 * item enters, so sending it out would cost every item two trips (one to be classified, one to be
 * worked on) and would stop a predicate closing over anything the caller holds.
 *
 * @example
 * `demux(orders, [big, rest], false)` → `Map { "big" => [order 2, order 4], "rest" => [order 1] }`.
 */
function demux<T>(items: T[], arms: readonly BranchArm<T>[], broadcast: boolean): Map<string, T[]> {
  const grouped = new Map<string, T[]>(arms.map((arm) => [arm.name, []]));
  for (const item of items) {
    claimItem(item, arms, grouped, broadcast);
  }
  return grouped;
}

/** One item's own routing pass, split out so `demux` stays within this repo's nesting limit. */
function claimItem<T>(
  item: T,
  arms: readonly BranchArm<T>[],
  grouped: Map<string, T[]>,
  broadcast: boolean,
): void {
  for (const arm of arms) {
    if (!arm.predicate(item)) continue;
    grouped.get(arm.name)!.push(item);
    if (!broadcast) return;
  }
}

/**
 * What `.branch()` returns (#90): the arms bound once, callable with any input.
 *
 * `.branch()` is a STAGE, not a terminal - it hands back a runner rather than the results, so the
 * definitions are written once and the caller picks what to do with each call's record. The Mode
 * follows the same rule every other terminal does: every arm synchronous returns the record plainly,
 * and one asynchronous arm widens the whole record to a single `Promise`.
 *
 * `split(orders)` → `{ big: ["BIG:2"], eu: [1, 3], rest: [] }`.
 */
export interface BranchRunner<In, R, M extends PipelineMode> {
  (input: AsyncIterable<In>): Promise<R>;
  // Keyed on `"async"`, not on `"sync"`: `"unset"` is the ordinary state of a composed chain and is
  // synchronous over a synchronous input, so testing for `"sync"` would type every undecided chain's
  // record a `Promise` while the runtime handed back the record plainly.
  (input: Iterable<In>): M extends "async" ? Promise<R> : R;
}

/** What `.branch()` produces: one record, keyed by arm name, joined on the orchestrator - the only
 * process that sees every arm, since arms can be remote. Typed loosely on the arms' own outputs,
 * because a builder's arms are collected at runtime rather than inferred from an object literal. */
export type BranchResults = Record<string, unknown[]>;

/** Construction-time knobs for a `Pipeline` — every field optional. */
export interface PipelineOptions {
  /**
   * An already-built context manager, for THIS process. Takes precedence over `contextFactory`
   * (#31) - a caller who already holds the instance they want (the orchestrating process,
   * typically) passes it here; a process that must build its OWN instance (a `ClusterPipeline`
   * worker, re-executing the same entry module with no way to receive an already-built instance
   * across the process boundary) uses `contextFactory` instead.
   */
  context?: IContextManager;
  /**
   * How to build a context manager, for any OTHER process than the one that already has `context`.
   * Invoked at most ONCE per process - in the constructor, only when `context` is absent - and the
   * built instance is then carried forward through every copy-on-write call
   * (`.context()`/`.transform()`/`.buffer()`) the same way an explicit `context` would be, so a
   * `ClusterPipeline` worker's own `.fetch()` (`src/pipelines/http.ts`) serves every request off
   * the SAME instance the constructor built, never a second one (#31). `context` and
   * `contextFactory` together is not an error: the instance serves this process, the factory
   * serves every other one.
   */
  contextFactory?: () => IContextManager;
  /**
   * Internal: an already-cut chunk stream to seed `_chunks` with directly, bypassing the
   * constructor's own default cut - the copy-on-write path every method below (`.apply()`,
   * `.buffer()`, `.context()`) uses via `createPipeline()`. Not intended for direct external use.
   */
  chunks?: AsyncIterable<unknown[]>;
  /**
   * Internal: the pre-buffer ITEM view `.buffer()` recuts from on a second, back-to-back call -
   * `null` once a real stage has consumed `chunks` (`.apply()` sets it), so a LATER `.buffer()`
   * falls back to flattening whatever that stage actually produced instead. Not intended for
   * direct external use.
   */
  preBufferItems?: AsyncIterable<unknown> | null;
  /**
   * Internal: the chain of chunk-wise transforms accumulated via `.apply()`/`.transform()` -
   * `HttpPipeline`'s own `.fetch()` looks a stage up by index here to serve a dispatched request.
   * Not intended for direct external use.
   */
  chunkTransforms?: ChunkTransform[];
  /**
   * Internal: every reduce stage registered via `.reduce()`, keyed by its index in the SAME shared
   * space `chunkTransforms` uses - `HttpPipeline`'s own `.fetch()` (#45 L5) looks a stage up here to
   * serve a `/reduce/<n>` request. Not intended for direct external use.
   */
  reduceStages?: Map<number, ReduceStage>;
  /**
   * The RUN handler (#78), registered via `.onError()` - position-DEPENDENT, unlike `Transformer`'s
   * own row handler: only a stage applied AFTER `.onError()` sees it, since it reaches a chunk
   * failure only through that stage's own dispatch (`Pipeline.apply()`/`ConcurrentPipeline.apply()`).
   * Carried forward by every copy-on-write call the same way `context`/`chunkTransforms` are.
   */
  runHandler?: PipelineErrorHandler;
  /**
   * Internal: the RUNTIME half of `PipelineMode` (#90) - which engine this pipeline's own terminal
   * ops read. Set by `.from()` from the source's shape and the class's own `SourcePolicy`, and
   * carried forward by every copy-on-write call. Not intended for direct external use.
   */
  mode?: PipelineMode;
  /**
   * Internal: the SYNC chunk stream a `"sync"`-Mode pipeline reads (#90), whose individual chunks
   * may still be pending once a stage's callback returned a thenable. `null` on an `"async"` chain,
   * where `chunks` carries the stream instead. Not intended for direct external use.
   */
  syncChunks?: MaybeAsyncChunks<unknown> | null;
  /**
   * Internal: `preBufferItems`' sync counterpart (#90) - the raw item view a back-to-back
   * `.buffer()` recuts from on a `"sync"` chain. Not intended for direct external use.
   */
  syncPreBufferItems?: Iterable<unknown> | null;
  /**
   * Internal: the chunk boundary `.buffer(size)` last declared, carried so a `.buffer()` called
   * BEFORE `.from()` still decides the source's own cut (#90). `.from()` read `DEFAULT_CHUNK_SIZE`
   * unconditionally before this existed, so that call was silently discarded. Not intended for
   * direct external use.
   */
  chunkSize?: number;
  /**
   * Internal: every stage composed while the pipeline had no source, in order, replayed by
   * `.from()` once an input arrives (#90). Not intended for direct external use.
   */
  pendingStages?: PendingStage[];
  /**
   * Internal: the route prefix an arm's own stages address themselves under, `/branch/<i>/<name>`
   * (#90). Empty on a chain's own stages. Without it an arm's stage 0 collided with the parent's
   * stage 0 on the worker, which served the parent's transform for both. Not intended for direct
   * external use.
   */
  routeTrail?: string;
  /**
   * Internal: every `.branch()` stage's own arms, keyed by the branch's index in the shared stage
   * space (#90) - the registry a serving side walks to resolve a `/branch/<i>/<name>/` trail. Not
   * intended for direct external use.
   */
  branchStages?: Map<number, BranchArm<unknown>[]>;
  /**
   * Internal: whether `context` was invented by a `Pipeline` rather than named by the caller (#90).
   * A default-built manager belongs to one run, so a reusable chain gets a fresh one per call; a
   * `context` or `contextFactory` the caller named is theirs and is kept. Carried explicitly
   * through copy-on-write, since every such call passes an already-resolved `context` and would
   * otherwise look caller-supplied. Not intended for direct external use.
   */
  contextIsDefault?: boolean;
  /**
   * Internal: whether an input has been bound to this chain (#90). A RUNTIME fact, kept apart from
   * `mode`, which is a TYPE fact about what the chain produces. `"unset"` used to answer both, and
   * the two are independent: a callable chain is `"unset"` for its whole life and becomes bound
   * only for the duration of one call. Not intended for direct external use.
   */
  bound?: boolean;
}

/** A registered reduce stage's own definition - `pushReduceStage()` (below) is the one place that
 * builds one, `HttpPipeline.fetch()` (#45 L5) the one place that reads one back to serve
 * `/reduce/<n>`. Untyped on `U`/`T` (kept as `unknown`) since a `Pipeline`'s own map holds reduce
 * stages of every type a chain has ever registered, not just its current `T`.
 *
 * `{ fn: (acc, x) => acc + x, initial: 0 }` → the stage `HttpPipeline.fetch()` (#45 L5) looks up to
 * serve `/reduce/<n>` for a chain built as `.reduce((acc, x) => acc + x, 0)`. */
export interface ReduceStage<U = unknown, T = unknown> {
  fn: ReduceFunction<U, T>;
  initial: U;
}

/** The `_chunkTransforms` slot a reduce stage occupies - a reduce stage isn't a per-chunk
 * `ChunkTransform` (it folds across chunks, not one chunk in for one chunk out), so its slot throws
 * if ever invoked as one. This IS the fail-loud guard #45's own ticket wanted from the killed
 * `sourcePositionViolations` mechanism (#39 deleted it entirely, and there is no separate replay
 * path left for a violations list to protect against - architecture.md's own text says so). */
function reduceStagePlaceholder(stageIndex: number): ChunkTransform {
  return () => {
    throw new Error(
      `stage ${stageIndex} is a reduce stage, not a plain per-chunk transform - it cannot serve ` +
        `/stage/${stageIndex}`,
    );
  };
}

/**
 * Runs one chunk through a stage on the `"sync"` engine (#90), applying `Pipeline.onError()`'s own
 * RUN handler exactly as `runSequentially` does for the async engine - the two engines must agree on
 * what a chunk failure means, and `dropOrRethrow` is where that decision already lives.
 *
 * A dropped chunk becomes `[]` rather than disappearing: the sync stream is a generator of chunks,
 * so an empty chunk is how "this one contributed nothing" is spelled. A synchronous handler keeps
 * the whole thing synchronous; an async one widens the run from this chunk on.
 *
 * `runStageChunk(doubler, [1, 2], ctx, undefined)` → `[2, 4]`, no `Promise` created.
 */
function runStageChunk<In, Out>(
  runnable: (chunk: In[], ctx: IContextManager) => Out[] | Promise<Out[]>,
  chunk: In[],
  ctx: IContextManager,
  runHandler?: PipelineErrorHandler,
): Out[] | Promise<Out[]> {
  const dropped = (error: Error): Out[] | Promise<Out[]> =>
    chain(dropOrRethrow(runHandler, error, ctx), () => [] as Out[]);
  try {
    const result = runnable(chunk, ctx);
    return isThenable(result) ? Promise.resolve(result).catch(dropped) : result;
  } catch (error) {
    return dropped(error as Error);
  }
}

/**
 * A lazy, chunked stream of `T`. Nothing runs until a terminal operation (`toArray`, `first`, async
 * iteration, …) pulls: `.apply()`/`.transform()` compose transformers, chunking is handled for
 * you, and a chunk's items flow to the next stage as a group. The whole chain shares one context
 * manager, so a context-aware transformer can read and write state across stages — `.context()`
 * itself still returns a NEW `Pipeline` (copy-on-write, like `.apply()`/`.transform()`/`.buffer()`),
 * carrying the SAME context manager forward, mutated in place (#31) — a caller's own
 * `IContextManager` class is never copied into a fresh `SimpleContextManager` and discarded.
 *
 * `await new Pipeline([1, 2, 3]).transform((t) => t.map((x) => x * 2)).toArray()` →
 * `[2, 4, 6]`. A terminal op's return carries no context snapshot (#744) - read `.contextManager`
 * for that.
 */
/**
 * The call signature every `Pipeline` carries (#90), declared as a merged interface because a class
 * body cannot hold one. Calling a pipeline pairs its chain with an input and returns a
 * `PipelineResult`; the chain itself is never bound to data, so the same pipeline serves every call.
 *
 * The two overloads mirror `.from()`'s own: a sync input keeps a sync chain sync, an async one
 * widens. `In` is the input type the pipeline was declared with, which is why the class carries it
 * separately from `T` - `T` moves to each stage's output type as the chain is composed, and by the
 * last stage no longer says what the chain accepts.
 */
export interface Pipeline<
  T,
  M extends PipelineMode = "unset",
  P extends SourcePolicy = "shape",
  In = T,
> {
  (input: AsyncIterable<In>): PipelineResult<T, "async">;
  (input: Iterable<In>): PipelineResult<T, M extends "async" ? "async" : AssignMode<P, "sync">>;
}

export class Pipeline<
  T,
  M extends PipelineMode = "unset",
  P extends SourcePolicy = "shape",
  In = T,
> {
  // Protected (#17), not private: a dispatching subclass's own overridden `createPipeline()`
  // (below) reads these to carry them into the next instance the same way this base
  // implementation does - `private` would put them out of reach from `src/pipelines/`.
  //
  // Each carries a definite-assignment `!` because the constructor assigns them onto the FUNCTION
  // it returns rather than onto `this` (#90, see the constructor). TypeScript checks initialization
  // against `this`, which that constructor never touches, so without the `!` every field reports
  // `TS2564` while the runtime assigns all of them.
  /** The persisted chunk stream every terminal op and async iteration reads (#39) - cut ONCE,
   * either by the constructor's own default or by `.buffer(size)`, and carried unchanged through
   * every later stage until another `.buffer()` call declares a new one. */
  protected _chunks!: AsyncIterable<T[]>;
  /** The pre-buffer ITEM view a back-to-back `.buffer()` call recuts from, or `null` once a real
   * stage (`.apply()`) has consumed `_chunks` - see `PipelineOptions.preBufferItems`. */
  protected _preBufferItems!: AsyncIterable<T> | null;
  protected _context!: IContextManager;
  protected _chunkTransforms!: ChunkTransform[];
  /** Every reduce stage registered via `.reduce()`, keyed by its index in the shared stage-index
   * space - see `PipelineOptions.reduceStages`. */
  protected _reduceStages!: Map<number, ReduceStage>;
  /** The RUN handler (#78), registered via `.onError()` - see `PipelineOptions.runHandler`. */
  protected _runHandler?: PipelineErrorHandler;
  /** Which engine this pipeline's terminal ops read (#90) - see `PipelineOptions.mode`. */
  protected _mode!: PipelineMode;
  /** The sync chunk stream, on a `"sync"`-Mode pipeline - see `PipelineOptions.syncChunks`. */
  protected _syncChunks!: MaybeAsyncChunks<T> | null;
  /** `_preBufferItems`' sync counterpart - see `PipelineOptions.syncPreBufferItems`. */
  protected _syncPreBufferItems!: Iterable<T> | null;
  /** The chunk boundary `.buffer(size)` last declared - see `PipelineOptions.chunkSize`. */
  protected _chunkSize!: number;
  /** Stages composed before a source existed - see `PipelineOptions.pendingStages`. */
  protected _pendingStages!: PendingStage[];
  /** Whether `_context` was invented here rather than named by the caller - see
   * `PipelineOptions.contextIsDefault`. */
  protected _contextIsDefault!: boolean;
  /** The route prefix this pipeline's stages sit under - see `PipelineOptions.routeTrail`. */
  protected _routeTrail!: string;
  /** Every `.branch()` stage's arms, by branch index - see `PipelineOptions.branchStages`. */
  protected _branchStages!: Map<number, BranchArm<unknown>[]>;
  /** `registriesFor`'s memo, by trail - built on first serve, the same way `registries()` memoises
   * its own. Never carried through copy-on-write; the next instance's arms are its own. */
  protected _armRegistries?: Map<
    string,
    { chunkTransforms: ChunkTransform[]; reduceStages: Map<number, ReduceStage> }
  >;
  /** Whether an input has been bound to this chain - see `PipelineOptions.bound`. */
  protected _bound!: boolean;
  /** `registries()`'s memo - built on first serve, never carried through copy-on-write, since the
   * next instance's own stage list is different. */
  protected _registries?: {
    chunkTransforms: ChunkTransform[];
    reduceStages: Map<number, ReduceStage>;
  };

  /**
   * Create a new Pipeline from a data source.
   *
   * @param data - Sync or async iterable data source
   * @param options - Optional pipeline configuration
   */
  constructor(options?: PipelineOptions) {
    // A constructor that RETURNS a function is what makes an instance callable (#90). Two halves,
    // both load-bearing:
    //
    // - Returning `self` makes the instance a function. A derived class's own `this` becomes
    //   whatever the base constructor returned, so `ConcurrentPipeline`/`HttpPipeline`/
    //   `ClusterPipeline` set their extra knobs onto this same object without changing a line.
    // - `setPrototypeOf(self, new.target.prototype)` restores the methods and `instanceof` that a
    //   bare function would not have, AND resolves `this.constructor` to the real subclass, which
    //   is what `createPipeline()`'s copy-on-write depends on.
    //
    // `Pipeline.prototype` is reparented onto `Function.prototype` ONCE, below this class, which is
    // what keeps every instance a real function - `instanceof Function`, `.bind`, `.call`. The
    // obvious `class Pipeline extends Function` does the same thing but calls `super()`, which runs
    // `CreateDynamicFunction`: banned wherever code generation from strings is, so `new Pipeline()`
    // threw `EvalError: Code generation from strings disallowed for this context` under
    // `node --disallow-code-generation-from-strings`, and would on a CSP page or a Cloudflare
    // Worker. Reparenting the prototype costs nothing and runs everywhere.
    //
    // ⚠ One exception to substitutability: `.apply` is a STAGE method here, so it shadows
    // `Function.prototype.apply`. Measured: `score.call(null, [1,2,3])` returns a `PipelineResult`,
    // `score.apply(null, [[1,2,3]])` returns a `Pipeline` - it reached `Pipeline.apply()`. `.bind`
    // and `.call` are unaffected. A consumer that invokes callbacks via `fn.apply(ctx, args)` needs
    // a wrapper: `(input) => score(input)`.
    const self = ((input: PipelineSource<unknown>) => {
      // A pipeline that already named a source through `.from()` has materialised its stages into
      // a chunk stream, and only stages recorded SINCE then can be replayed onto a new input - so
      // calling one silently dropped every earlier stage. Measured: `.from([1,2,3]).transform(x =>
      // x * 2)` drained in place to `[2,4,6]`, then the same object called with `[10,20]` returned
      // `[10,20]`. Refused rather than half-honoured; the state disappears entirely once `.from()`
      // does, at which point every pipeline is callable and none is bound.
      if (self._bound) {
        throw new Error(
          "cannot call a pipeline that already named a source with .from() - build the chain without .from() and call it with the input instead",
        );
      }
      return new PipelineResult<T, PipelineMode>(
        self as unknown as Pipeline<unknown, "sync" | "async", SourcePolicy, unknown>,
        input,
      );
    }) as unknown as Pipeline<T, M, P, In>;
    Object.setPrototypeOf(self, new.target.prototype);

    // `contextFactory` runs ONLY when `context` is absent, and only HERE - every copy-on-write
    // call below (`.context()`, `.apply()`, `.buffer()`) always passes an already-resolved
    // `context`, so a `ClusterPipeline` chain's later `.transform()` calls never re-invoke it
    // (#31, Done-when 6: once per process, not once per stage or per request).
    self._context = options?.context ?? options?.contextFactory?.() ?? new SimpleContextManager();
    // Whether the context above is one this pipeline INVENTED, rather than one the caller named.
    // A caller who passes `context` or `contextFactory` owns the instance and keeps it across every
    // call; a default-built one belongs to a single run, and is replaced per call by `fromSource`.
    // Without that, one reusable chain accumulated: a `ctx.set("n", n + 1)` map reported `n === 6`
    // after two three-item calls, where each run should have seen `3`.
    self._contextIsDefault =
      options?.contextIsDefault ??
      (options?.context === undefined && options?.contextFactory === undefined);
    self._chunkTransforms = options?.chunkTransforms ?? [];
    self._reduceStages = options?.reduceStages ?? new Map();
    self._runHandler = options?.runHandler;
    self._mode = options?.mode ?? "unset";
    self._syncChunks = (options?.syncChunks ?? null) as MaybeAsyncChunks<T> | null;
    self._chunks = (options?.chunks ?? EMPTY_CHUNKS) as AsyncIterable<T[]>;
    self._preBufferItems = (options?.preBufferItems ?? null) as AsyncIterable<T> | null;
    self._syncPreBufferItems = (options?.syncPreBufferItems ?? null) as Iterable<T> | null;
    self._chunkSize = options?.chunkSize ?? DEFAULT_CHUNK_SIZE;
    self._pendingStages = options?.pendingStages ?? [];
    self._bound = options?.bound ?? false;
    self._routeTrail = options?.routeTrail ?? "";
    self._branchStages = options?.branchStages ?? new Map();
    return self;
  }

  /**
   * Names the source, and with it the engine the whole chain runs on (#90) - an `Iterable` makes the
   * chain `"sync"`, an `AsyncIterable` makes it `"async"`, and a dispatching class overrides both to
   * `"async"` (`SourcePolicy`). Before this call the pipeline is `"unset"` and `.transform()` refuses
   * it, so a chain can never be composed without an engine decided.
   *
   * Replaces the constructor's own `data` parameter (#90, BREAKING, no deprecation period). A
   * constructor overload cannot vary its class's own generic return, so `new Pipeline(data)` could
   * never have inferred the Mode from the source's shape - verified directly with `tsc --strict`
   * during planning. An ordinary method can, which is the whole reason this exists.
   *
   * @example
   * `new Pipeline().from([1, 2, 3]).toArray()` → `[1, 2, 3]`, typed `number[]`, no `await`.
   * `new Pipeline().from(asyncSource).toArray()` → typed `Promise<number[]>`.
   */
  protected bind<U>(data: AsyncIterable<U>): Pipeline<U, "async", P, In>;
  // A receiver already widened to `"async"` stays async whatever the source's own shape (#90):
  // `.onError()` and `.context()` are both callable BEFORE `.from()`, so an async run handler
  // registered there had its widening discarded here - the chain typed `number[]` while
  // `dropOrRethrow` deferred on that handler the moment a chunk failed. `"unset"` is the ordinary
  // case and still takes the source's own shape, which is what keeps `.from([1,2,3])` synchronous.
  protected bind<U>(
    data: Iterable<U>,
  ): Pipeline<U, M extends "async" ? "async" : AssignMode<P, "sync">, P>;
  protected bind<U>(data: PipelineSource<U>): Pipeline<U, "sync" | "async", P> {
    return this.fromSource<U>(data, this.sourcePolicy()) as Pipeline<U, "sync" | "async", P>;
  }

  /**
   * The shared body behind every class's own `.from()` (#90). `policy` is what a dispatching
   * subclass overrides via `sourcePolicy()` below - it never re-implements this cutting and
   * bookkeeping, so a change to how a source becomes chunks lands once rather than four times.
   *
   * A `"sync"` source is cut by `buildSyncChunkGenerator` and never becomes an async iterator at
   * all: that per-item conversion, not the per-chunk `Promise.all`, is where most of the pre-#90
   * cost sat - a chain with ZERO transform stages still paid it.
   */
  protected fromSource<U>(data: PipelineSource<U>, policy: SourcePolicy): AnyPipeline<U> {
    const isAsyncSource = Symbol.asyncIterator in Object(data);
    // `this._mode` is read too, so a receiver already widened before `.from()` stays widened - see
    // `from`'s own Iterable overload for the case that made this necessary.
    const mode: "sync" | "async" =
      isAsyncSource || policy === "async" || this._mode === "async" ? "async" : "sync";

    if (mode === "sync") {
      const items = data as Iterable<U>;
      return this.replayPending(
        this.createPipeline<U>(EMPTY_CHUNKS as AsyncIterable<U[]>, {
          // Every knob set BEFORE `.from()` carries through it - `.onError()` and `.context()` are
          // both callable on a source-less pipeline, and dropping them here made a registered run
          // handler silently never fire.
          ...this.carriedOptions(),
          mode,
          pendingStages: [],
          context: this.contextForRun(),
          bound: true,
          syncChunks: buildSyncChunkGenerator<U>(this._chunkSize)(items),
          syncPreBufferItems: items,
          preBufferItems: null,
        }),
      );
    }

    const items = toAsyncIterable(data);
    return this.replayPending(
      this.createPipeline<U>(buildChunkGenerator<U>(this._chunkSize)(items), {
        ...this.carriedOptions(),
        mode,
        pendingStages: [],
        context: this.contextForRun(),
        bound: true,
        preBufferItems: items,
        syncChunks: null,
        syncPreBufferItems: null,
      }),
    );
  }

  /**
   * Runs every stage this pipeline recorded while it had no source, in order, against `bound` (#90).
   *
   * `bound` carries no pending stages of its own - `fromSource()` clears them as it binds - so each
   * replayed call takes the ordinary, immediate path through `.apply()`/`.reduce()`/`.local()`.
   * That is the whole point: a chain composed before its input and one composed after it execute
   * the identical code.
   *
   * @example
   * A pipeline recording one `.map((x) => x * 2)` stage, bound to `[1, 2, 3]`, replays that one
   * call and drains to `[2, 4, 6]`.
   */
  private replayPending<U>(bound: AnyPipeline<U>): AnyPipeline<U> {
    let current: AnyPipeline<any> = bound;
    for (const stage of this._pendingStages) current = stage(current);
    return current as AnyPipeline<U>;
  }

  /**
   * What THIS class does to a source's own shape - the runtime half of `SourcePolicy` (#90). The
   * base keeps the shape; every dispatching class overrides this to `"async"`, which is the ONE line
   * each of them changes rather than re-implementing `.from()`.
   */
  protected sourcePolicy(): SourcePolicy {
    return "shape";
  }

  /**
   * Builds the NEXT `Pipeline` in a copy-on-write chain, via `this.constructor` rather than a
   * hard-coded `new Pipeline<U>` (#17) — the ONE seam every copy-on-write method below
   * (`.apply()`, `.context()`, `.buffer()`) goes through, so a subclass built on `Pipeline`
   * survives its own `.transform()` chain instead of silently decaying to a plain `Pipeline`.
   *
   * `chunks` is an ALREADY-CUT stream (#39) - this method never cuts one of its own, it only
   * threads the caller's chunk stream (plus context/chunkTransforms/preBufferItems) into a fresh
   * instance of THIS pipeline's own class. `[]` is passed as the constructor's own positional
   * `data` and is never read: `options.chunks` being set routes the constructor past its
   * default-cutting branch entirely.
   *
   * A subclass whose constructor takes EXTRA knobs (`ConcurrentPipeline.maxConcurrency`,
   * `HttpPipeline.url`, …) overrides this method to carry them forward explicitly — `this.
   * constructor` alone only reproduces knobs `PipelineOptions` itself already carries. This base
   * implementation is correct for `Pipeline` itself and for any subclass whose constructor takes
   * nothing beyond `(source, options)`.
   *
   * @example
   * A `Sub extends Pipeline` with no extra constructor params: `new Sub([1]).transform(f)
   * .constructor.name` → `"Sub"`, because `apply()` (below) calls this method rather than `new
   * Pipeline(...)` directly.
   */
  protected createPipeline<U>(
    chunks: AsyncIterable<U[]>,
    options: PipelineOptions,
  ): AnyPipeline<U> {
    const Ctor = this.constructor as new (options?: PipelineOptions) => AnyPipeline<U>;
    return new Ctor({ ...options, chunks });
  }

  /** Every knob a copy-on-write call carries into the next instance (#90) - named once here rather
   * than repeated field by field at each of the eight call sites, so a knob added later reaches all
   * of them. `chunks` is passed separately, since each caller supplies its own. */
  protected carriedOptions(): PipelineOptions {
    return {
      context: this._context,
      chunkTransforms: this._chunkTransforms,
      reduceStages: this._reduceStages,
      preBufferItems: this._preBufferItems,
      syncPreBufferItems: this._syncPreBufferItems,
      chunkSize: this._chunkSize,
      runHandler: this._runHandler,
      mode: this._mode,
      syncChunks: this._syncChunks,
      pendingStages: this._pendingStages,
      contextIsDefault: this._contextIsDefault,
      bound: this._bound,
      routeTrail: this._routeTrail,
      branchStages: this._branchStages,
    };
  }

  /** The context manager one RUN gets. A caller who named a `context` or a `contextFactory` keeps
   * the instance they own across every call; a default-built manager is per-run, so two calls of
   * one reusable chain never see each other's writes (#90). */
  protected contextForRun(): IContextManager {
    if (!this._contextIsDefault || !this.isDeferred()) return this._context;
    // A fresh manager per run, SEEDED from the chain's own values - `.context({ multiplier: 10 })`
    // declares a default every run starts from, and an empty manager here dropped it, so a stage
    // reading `ctx.getOrDefault("multiplier", 1)` fell back to `1`. Seeded and separate is what
    // keeps `.context()` working while two calls still never see each other's writes.
    return new SimpleContextManager(this._context.toDict());
  }

  /**
   * Records a stage instead of running it, on a pipeline that has no source yet (#90).
   *
   * `run` is the stage's own call, replayed by `.from()` against the bound pipeline - so a deferred
   * chain and a `.from()` chain reach the identical `.apply()`/`.reduce()`/`.local()` body, and a
   * deferred stage's Mode, chunking and error handling need no separate implementation.
   *
   * @example
   * `new Pipeline<number>().transform((t) => t.map((x) => x * 2))` records one stage; calling that
   * pipeline with `[1, 2, 3]` replays it and yields `[2, 4, 6]`.
   */
  protected defer<U>(run: PendingStage, extra?: PipelineOptions): AnyPipeline<U> {
    return this.createPipeline<U>(EMPTY_CHUNKS as AsyncIterable<U[]>, {
      ...this.carriedOptions(),
      ...extra,
      pendingStages: [...this._pendingStages, run],
    });
  }

  /** Whether this pipeline records its stages rather than running them - true until an input
   * arrives, which is every pipeline built the callable way (#90). */
  protected isDeferred(): boolean {
    return !this._bound;
  }

  /**
   * This pipeline's stage registries - the two maps a SERVING side reads to answer `/stage/<n>` and
   * `/reduce/<n>` (#90).
   *
   * A deferred pipeline has recorded its stages but not run them, so both registries are empty
   * until something binds an input. A worker never binds one: it holds the chain to serve it, and
   * has no data by definition. So the stages are replayed here against an empty source, once and
   * memoised - which is exactly what the caller's own `.from([])` placeholder used to do, moved
   * inside where it belongs. The empty source produces no chunks, so nothing runs; only the
   * registries are the point.
   *
   * @example
   * A worker holding one `.transform()` serves stage index `0` after this, where before it reported
   * `unknown stage 0; this deployment serves 0..-1`.
   */
  /**
   * The registries a `/branch/<i>/<name>/` trail addresses (#90): the ARM's own, not this
   * pipeline's. The worker runs the same entry module, so its `.branch()` call built the same arms
   * in the same order - resolving the trail rebuilds the arm's pipeline and reads what it registered.
   *
   * @example
   * `registriesFor("/branch/0/big")` returns the stage table of the `big` arm of the first
   * `.branch()` call, so `/branch/0/big/transform/0` serves that arm's own first stage.
   */
  registriesFor(trail: string): {
    chunkTransforms: ChunkTransform[];
    reduceStages: Map<number, ReduceStage>;
  } | null {
    const match = /^\/branch\/(\d+)\/([^/]+)$/.exec(trail);
    if (!match) return null;
    const arms = this._branchStages.get(Number(match[1]));
    const arm = arms?.find((candidate) => candidate.name === match[2]);
    if (!arm?.build) return null;
    const memo = (this._armRegistries ??= new Map());
    const cached = memo.get(trail);
    if (cached !== undefined) return cached;
    const armPipeline = arm.build(
      this.emptyOfOwnClass<unknown>(this._context, trail) as AnyPipeline<unknown>,
    ) as AnyPipeline<unknown>;
    const built = armPipeline.registries();
    memo.set(trail, built);
    return built;
  }

  protected registries(): {
    chunkTransforms: ChunkTransform[];
    reduceStages: Map<number, ReduceStage>;
  } {
    if (!this.isDeferred()) {
      return { chunkTransforms: this._chunkTransforms, reduceStages: this._reduceStages };
    }
    this._registries ??= (() => {
      const materialised = this.bind([] as T[]) as unknown as AnyPipeline<T>;
      return {
        chunkTransforms: materialised._chunkTransforms,
        reduceStages: materialised._reduceStages,
      };
    })();
    return this._registries;
  }

  /**
   * The options that reproduce `pipeline`'s chain on ANOTHER class (#90) - what a wrapping class's
   * `(pipeline, options)` constructor spreads to adopt a chain built elsewhere.
   *
   * Only a source-less pipeline can be adopted: its stages are still recorded calls, so replaying
   * them against the adopting class makes each one run THAT class's way. A pipeline already bound
   * through `.from()` has materialised its stages into a chunk stream that belongs to the class
   * that built it, and there is nothing left to replay - so this refuses rather than adopting half
   * a chain.
   *
   * @example
   * `new HttpPipeline(scored, { url })` runs `scored`'s stages over HTTP, where `scored([1,2,3])`
   * runs the identical stages in this process.
   */
  protected static adopt(pipeline: AnyPipeline<any>): PipelineOptions {
    if (pipeline._bound) {
      throw new Error(
        "cannot wrap a pipeline that already named a source with .from() - build the chain without .from() and wrap that",
      );
    }
    return {
      context: pipeline._context,
      contextIsDefault: pipeline._contextIsDefault,
      chunkTransforms: [...pipeline._chunkTransforms],
      reduceStages: new Map(pipeline._reduceStages),
      runHandler: pipeline._runHandler,
      chunkSize: pipeline._chunkSize,
      pendingStages: [...pipeline._pendingStages],
    };
  }

  /**
   * Resolves a wrapping class's two constructor forms into the one `PipelineOptions` its `super()`
   * call takes (#90): `(pipeline, options)` adopts a chain built elsewhere, `(options)` builds an
   * empty one. Written once here so `ConcurrentPipeline`, `HttpPipeline` and `ClusterPipeline`
   * cannot drift on which of the two they accept, or on how a wrapped chain is carried in.
   *
   * @example
   * `Pipeline.wrapping(scored, { url })` → `scored`'s stages plus `{ url }`, ready for `super()`.
   */
  protected static wrapping<O extends PipelineOptions>(
    first: AnyPipeline<any> | O | undefined,
    second: O | undefined,
  ): O {
    if (first instanceof Pipeline) return { ...Pipeline.adopt(first), ...second } as O;
    return (first ?? second ?? {}) as O;
  }

  /** This pipeline's CHUNKS as an async stream, whichever engine it runs on (#90) - what a merge
   * reads, since a `"sync"` pipeline leaves `_chunks` empty and carries `_syncChunks` instead. */
  protected chunkStream(): AsyncIterable<T[]> {
    if (this._mode !== "sync" || this._syncChunks === null) return this._chunks;
    const syncChunks = this._syncChunks;
    return {
      [Symbol.asyncIterator]: async function* () {
        for (const chunk of syncChunks) yield await chunk;
      },
    };
  }

  /**
   * Refuses a DRAIN on a pipeline that was given no input (#90). `asyncItems()` is the only caller:
   * every terminal op reads through it, so one check covers them all.
   *
   * It used to refuse a STAGE too, from `apply()` and `ConcurrentPipeline.apply()`, because
   * composing before a source was the mistake. Deferral replaced that - composing ahead of the data
   * is now the ordinary case, and every class records its stages rather than refusing them. What
   * this still catches is the case it was written for: `new Pipeline().toArray()` resolving to `[]`,
   * a plausible-looking answer for a caller who simply has no data.
   */
  protected requireSource(): void {
    if (!this._bound) {
      throw new Error("no input: call the pipeline with the items to process");
    }
  }

  /** Whether this pipeline runs on the synchronous engine (#90) - what `reduce()` asks before
   * choosing between the sync and the async fold. */
  protected isSync(): boolean {
    return this._mode === "sync" && this._syncChunks !== null;
  }

  /** This pipeline's chunks as a SYNC stream, for the synchronous fold. A pipeline that is not
   * `"sync"` has none, so this raises rather than inventing one; `isSync()` above is the guard
   * every caller checks first. */
  protected syncChunkStream(): MaybeAsyncChunks<T> {
    if (this._mode !== "sync" || this._syncChunks === null) {
      throw new Error("no sync chunk stream: this pipeline runs on the asynchronous engine");
    }
    return this._syncChunks;
  }

  /** This pipeline's items, as one stream, whichever engine it runs on (#90) - the seam the async
   * terminal ops and `[Symbol.asyncIterator]` read, so neither has to branch on `_mode` itself. */
  protected asyncItems(): AsyncIterable<T> {
    // A drain refuses a source-less pipeline for the same reason a stage does (#90): without this,
    // `new Pipeline().toArray()` resolved to `[]`, a plausible-looking answer for a caller who
    // simply forgot `.from()`, where composing any stage on the same pipeline throws.
    this.requireSource();
    if (this._mode !== "sync" || this._syncChunks === null) {
      return flattenChunks(this._chunks);
    }
    const syncChunks = this._syncChunks;
    return {
      [Symbol.asyncIterator]: async function* () {
        for (const chunk of syncChunks) {
          yield* await chunk;
        }
      },
    };
  }

  // ===== Static Factory Methods =====

  /**
   * Get the current context manager (read-only access).
   */
  get contextManager(): IContextManager {
    return this._context;
  }

  /**
   * Merges values into the caller's OWN context manager, MUTATING it in place, then returns a NEW
   * `Pipeline` carrying that SAME instance forward - copy-on-write for the pipeline itself, but the
   * manager it carries is never replaced or copied. `this._context.set()` runs for every key in
   * `ctx`, so a manager that refuses an unknown key (a sealed manager, `#31`) throws HERE,
   * propagating to the caller, instead of being silently bypassed by a copy step that never called
   * its `.set()` at all. A caller still holding the pre-`.context()` reference sees every later
   * write too - both references alias the SAME manager (#31; this used to build a fresh
   * `SimpleContextManager` and copy values into it, which is why a caller's own class went deaf the
   * moment `.context()` ran). The same aliasing applies to TWO SIBLINGS built off one shared base:
   * `const a = base.context({m:"a"}); const b = base.context({m:"b"})` leaves `a`, `b` and `base`
   * all reading the ONE manager `base` started with, so `b`'s write is the value every one of them
   * sees - forking into independently-configured branches needs a caller-supplied manager per
   * branch, never two `.context()` calls off the same parent. Per-key, not transactional: `ctx`'s
   * entries are set one at a time in `Object.entries()` order, so a manager that throws partway
   * through (a sealed manager rejecting one key among several) leaves every EARLIER key's write
   * already applied - the same partial-application a caller looping `.set()` calls by hand would
   * get, never rolled back.
   *
   * Python equivalent:
   * ```python
   * def context(self, ctx: dict[str, Any]) -> "Pipeline[T]":
   *   for key, value in ctx.items():
   *     self.context_manager[key] = value
   *   return Pipeline(self.data_source, context=self.context_manager)
   * ```
   *
   * @param ctx - Dictionary of context values to merge in
   * @returns A new instance of THIS pipeline's own class, carrying the SAME (now-mutated) context
   *   manager forward. Typed `this` (#17), not `Pipeline<T>` - `T` never changes here, so a
   *   dispatching subclass's own `.local(build)` region still typechecks after a `.context()` call,
   *   the same as it would directly off the constructor.
   *
   * @example
   * A manager that records every key written (`__tests__/fixtures/context-managers.ts`'s own
   * `LoggingContext`), handed in at construction: `new Pipeline([1, 2], { context: mine }).context({
   * multiplier: 10 }).transform((t) => t.map((x, ctx) => (ctx.set("k", x), x))).toArray()` then
   * `mine.writes` → `["multiplier", "k", "k"]` - `mine` itself received every write (#31).
   */
  context(ctx: Record<string, unknown>): this {
    for (const [key, value] of Object.entries(ctx)) {
      this._context.set(key, value);
    }
    return this.createPipeline<T>(this._chunks, this.carriedOptions()) as this;
  }

  /**
   * Registers the RUN handler (#78) - position-DEPENDENT, unlike `Transformer.onError()`'s own row
   * handler: only a stage applied AFTER this call sees it, since `Pipeline.apply()`/
   * `ConcurrentPipeline.apply()` are what actually read `this._runHandler` at dispatch time. On a
   * chunk failure that reaches either of those (a chunk-wide throw, or a row handler that itself
   * rethrows and so escalates past the row), `handler` is called with the error and the context:
   * returning drops the failing chunk and the run continues to the next one; throwing stops the run,
   * rejecting with whatever it threw. Copy-on-write, like every other configuration method here.
   *
   * @param handler - The run handler, `(error, ctx) => void`.
   * @returns A new instance of THIS pipeline's own class, carrying the handler forward.
   *
   * @example
   * `new Pipeline(["1","x","3","4"]).buffer(1).onError((e) => console.warn(e.message))
   * .transform((t) => t.map(parseStrict)).toArray()` → `[1, 3, 4]` - the chunk holding `"x"` is
   * dropped, every other chunk survives.
   */
  onError(
    handler: (error: Error, ctx: IContextManager) => Promise<void>,
  ): M extends "async" ? this : Pipeline<T, "async", P, In>;
  onError(handler: (error: Error, ctx: IContextManager) => void): this;
  onError(handler: PipelineErrorHandler): this | Pipeline<T, "async", P, In> {
    // An ASYNC handler widens the chain (#90), the same rule `.tap()` follows. `PipelineErrorHandler`
    // declares a bare `void` return, which accepts an `async` function silently, so without the
    // overload above the chain kept its `"sync"` type while `dropOrRethrow` deferred on the handler's
    // own promise - a chain typed `number[]` handed back a pending `Promise` the moment an error
    // actually fired.
    //
    // ⚠ The widening is TYPE-ONLY: `_mode` still comes from the input's own shape, so a chain over
    // a sync input hands back a plain array while its type says `Promise<T[]>`. Measured:
    // `new Pipeline<number>().onError(async () => {}).transform(t => t.map(x => x * 2))` called
    // with `[1,2,3]` returns `[2,4,6]` with `typeof result.then === "undefined"`. `await` on that
    // array is a no-op, so the pessimism is safe there; `.then(…)` on it is a `TypeError`. The
    // handler cannot be inspected for asynchrony without guessing (a plain function returning a
    // promise is indistinguishable from a sync one), so the type stays pessimistic by decision.
    // Defers like a stage (#90), because it IS positional: `.onError()` covers only stages applied
    // AFTER it, and setting `runHandler` on the deferred pipeline instead made it cover the whole
    // chain - a handler written after `.transform()` silently started catching that transform.
    if (this.isDeferred()) {
      return this.defer<T>((p) => p.onError(handler)) as this;
    }
    return this.createPipeline<T>(this._chunks, {
      ...this.carriedOptions(),
      runHandler: handler,
    }) as this;
  }

  /**
   * Apply a transformer to the pipeline data - `transformer.process(this._chunks, ctx, runHandler)`
   * runs it directly over the pipeline's own persisted chunk stream, no cut here (#39): chunking is
   * never this method's decision, only `.buffer()`'s.
   *
   * `transformer.runnable()` (#78) is what carries the transformer's own row handler into
   * `_chunkTransforms` - the SAME function `process()` (below `runnable()`'s own call inside it)
   * drives, so a dispatched stage on `HttpPipeline`/`ClusterPipeline` gets row recovery too, off the
   * identical entry a worker's own copy of this chain built. `this._runHandler` (`.onError()`, #78)
   * is threaded through as `process()`'s own third argument, reaching `runSequentially`'s per-chunk
   * catch.
   *
   * Python equivalent:
   * ```python
   * def apply(self, transformer: Transformer[T, U]) -> "Pipeline[U]":
   *   self.chunks = transformer.process(self.chunks, self.context_manager)
   *   return self
   * ```
   *
   * `pipeline.apply(new Transformer<T, T>().map((x) => x * 2))` on a pipeline of `[1, 2, 3]` →
   * `.toArray()` resolves `[2, 4, 6]`.
   */
  apply<U, M2 extends "sync" | "async">(
    transformer: Transformer<T, U, M2>,
  ): Pipeline<U, AssignMode<P, JoinMode<M, M2>>, P, In> {
    // No source yet: record the call and replay it when one arrives (#90). This is the ordinary
    // case for a callable pipeline, where the chain is composed before any data exists.
    if (this.isDeferred()) {
      return this.defer<U>((p) => p.apply(transformer as Transformer<unknown, U, M2>)) as Pipeline<
        U,
        AssignMode<P, JoinMode<M, M2>>,
        P,
        In
      >;
    }
    const runnable = transformer.runnable();
    const carried = {
      ...this.carriedOptions(),
      chunkTransforms: [...this._chunkTransforms, runnable as unknown as ChunkTransform],
      // A real stage just consumed the chunk stream - nothing left to recut a back-to-back
      // `.buffer()` from except this stage's own output, so the pre-buffer item views reset.
      preBufferItems: null,
      syncPreBufferItems: null,
    };

    // A `"sync"` chain maps its chunks through the SAME `runnable` a `"sync"` one does, but through
    // a sync generator (#90): no `Promise` is created unless the transformer itself returns one,
    // and that chunk then travels the stream as a pending value the terminal op settles.
    if (this._mode === "sync" && this._syncChunks !== null) {
      const source = this._syncChunks;
      const ctx = this._context;
      const runHandler = this._runHandler;
      function* stageChunks(): Generator<U[] | Promise<U[]>> {
        for (const chunk of source) {
          yield chain(chunk, (settled) => runStageChunk(runnable, settled, ctx, runHandler)) as
            U[] | Promise<U[]>;
        }
      }
      return this.createPipeline<U>(EMPTY_CHUNKS as AsyncIterable<U[]>, {
        ...carried,
        syncChunks: stageChunks(),
      }) as Pipeline<U, AssignMode<P, JoinMode<M, M2>>, P, In>;
    }

    return this.createPipeline<U>(
      transformer.process(this._chunks, this._context, this._runHandler),
      {
        ...carried,
        syncChunks: null,
      },
    ) as Pipeline<U, AssignMode<P, JoinMode<M, M2>>, P, In>;
  }

  /**
   * Apply a transformer builder function.
   *
   * Composable before an input exists (#90): the conditional `this` that once refused an `"unset"`
   * receiver is gone, because composing a chain ahead of its data is now the ordinary case rather
   * than the mistake. A source-less `.transform()` records the stage; calling the pipeline replays
   * it. What still refuses is DRAINING without an input, which the `Pipeline`/`PipelineResult`
   * split makes a type error rather than a runtime one.
   *
   * Python equivalent:
   * ```python
   * def transform(self, t: Callable[[Transformer[T, T]], Transformer[T, U]]) -> "Pipeline[U]":
   *   transformer = t(Transformer[T, T]())
   *   return self.apply(transformer)
   * ```
   */
  transform<U, M2 extends "sync" | "async">(
    t: (transformer: Transformer<T, T, SeedMode<M>>) => Transformer<T, U, M2>,
  ): Pipeline<U, AssignMode<P, JoinMode<M, M2>>, P, In> {
    const transformer = t(new Transformer<T, T, SeedMode<M>>({ transform: (chunk) => chunk }));
    return this.apply(transformer) as unknown as Pipeline<U, AssignMode<P, JoinMode<M, M2>>, P, In>;
  }

  /**
   * The chunk boundary - explicit, opt-in (#39). Every later `.transform()`/`.apply()` sees these
   * chunks unchanged until another `.buffer()` call declares a new one.
   *
   * Recuts from `_preBufferItems` (the raw item stream) when it is still set - nothing has
   * consumed `_chunks` since the last cut, so a run of `.buffer()` calls with nothing between them
   * collapses to only the LAST one ever actually applied, never stacking a redundant
   * flatten-then-recut on top of an intermediate cut nobody asked to see. Once a real stage
   * (`.apply()`) has run, `_preBufferItems` is `null` and this flattens `_chunks` itself first -
   * a genuine re-chunk of that stage's own output.
   *
   * Typed `this` (#17) - `T` never changes here either, so this stays chainable on a dispatching
   * subclass without losing its own `.local(build)` overload.
   *
   * Python equivalent:
   * ```python
   * def buffer(self, size: int) -> "Pipeline[T]":
   *   items = self.pre_buffer_items if self.pre_buffer_items is not None else flatten(self.chunks)
   *   return Pipeline(build_chunk_generator(size)(items), pre_buffer_items=items)
   * ```
   *
   * @example
   * `new Pipeline([1, 2, 3, 4, 5, 6, 7, 8, 9]).buffer(2).buffer(3).buffer(4)` yields the same
   * chunks as `.buffer(4)` alone: `[[1, 2, 3, 4], [5, 6, 7, 8], [9]]` - no trace of an
   * intermediate 2- or 3-cut.
   */
  buffer(size: number): this {
    // Before an input there is no stream to cut, so the call is recorded and replayed in PLACE
    // (#90) - the same deferral `.apply()`/`.reduce()`/`.local()` use, for the same reason.
    //
    // Recording only the size, as an earlier form did, loses the call's position: `fromSource` then
    // applied it to the SOURCE cut, so a `.buffer()` written after a stage took effect before it.
    // Measured on `.transform(t => t.flatMap(x => [x, x])).buffer(2)` over `[1,2,3,4]` - chunks
    // came out `[[1,1,2,2],[3,3,4,4]]` where the same chain after `.from()` gives
    // `[[1,1],[2,2],[3,3],[4,4]]`. `chunkSize` is still carried alongside, because a `.buffer()`
    // written BEFORE any stage must also cut the source itself, which is what it now does by
    // replaying against a pipeline whose source is already cut at that size.
    if (this.isDeferred()) {
      const cutsTheSource = this._pendingStages.length === 0;
      return this.defer<T>((p) => p.buffer(size), cutsTheSource ? { chunkSize: size } : {}) as this;
    }

    // The `"sync"` arm recuts with the sync chunker (#90) - going through the async one here would
    // make `.buffer()` alone widen a chain whose every callback is synchronous, which is exactly
    // the Mode/runtime divergence this ticket exists to remove.
    if (this._mode === "sync" && this._syncChunks !== null) {
      const items = this._syncPreBufferItems;
      // Nothing has consumed the stream since the last cut: re-cut from the raw items, so a run of
      // back-to-back `.buffer()` calls collapses to the LAST one, exactly as the async arm does.
      if (items !== null) {
        return this.createPipeline<T>(EMPTY_CHUNKS as AsyncIterable<T[]>, {
          ...this.carriedOptions(),
          syncChunks: buildSyncChunkGenerator<T>(size)(items),
          syncPreBufferItems: items,
        }) as this;
      }
      return this.createPipeline<T>(EMPTY_CHUNKS as AsyncIterable<T[]>, {
        ...this.carriedOptions(),
        syncChunks: recutSyncChunks(this._syncChunks, size),
        syncPreBufferItems: null,
      }) as this;
    }

    const items = this._preBufferItems ?? flattenChunks(this._chunks);
    return this.createPipeline<T>(buildChunkGenerator<T>(size)(items), {
      ...this.carriedOptions(),
      preBufferItems: items,
    }) as this;
  }

  /**
   * Fold every chunk this pipeline produces into one or more values (#45), sequential and
   * in-process. `Transformer.reduce` folds ONE chunk and keeps nothing between chunks; this folds
   * EVERYTHING the pipeline produces, the only place cross-chunk state lives. `ConcurrentPipeline`
   * overrides this to dispatch it instead - `.local(build)` (#61) is what keeps a reduce stage
   * in-process on a dispatching class now.
   *
   * A fold is order-dependent but not inherently asynchronous (#90): over a `"sync"` chain a plain
   * reducer folds with no `Promise` created, so the stage keeps the chain's own Mode. A
   * `Promise`-returning reducer takes the first overload and widens the whole chain, the same rule
   * `.transform()` follows.
   *
   * ⚠ `initial` is captured ONCE, when the stage is composed, so a reusable chain hands every call
   * the same value. That is invisible for an immutable seed and wrong for a mutable one: measured,
   * `new Pipeline<number>().reduce((acc, x) => (acc.push(x), acc), [])` returns `[[1,2,3]]` on its
   * first call and `[[1,2,3,1,2,3]]` on its second. Fold into a fresh value (`[...acc, x]`), or
   * build the chain inside a function so each call gets its own seed.
   *
   * `new Pipeline().from([1,2,3,4,5]).reduce((acc, x) => acc + x, 0).transform((t) => t.map((n) => n *
   * 10)).toArray()` → `[1500]`, a `number[]` with no `await`.
   */
  reduce<U>(
    fn: (acc: U, item: T, ctx: IContextManager, emit: (value: U) => void) => Promise<U>,
    initial: U,
  ): Pipeline<U, "async", P, In>;
  reduce<U>(
    fn: (acc: U, item: T, ctx: IContextManager, emit: (value: U) => void) => U,
    initial: U,
  ): Pipeline<U, AssignMode<P, JoinMode<M, "sync">>, P, In>;
  reduce<U>(fn: ReduceFunction<U, T>, initial: U): AnyPipeline<U> {
    // A reduce stage defers like any other (#90) - see `apply()`. Replaying it through this same
    // method is what keeps its own registry (`_reduceStages`) and its Mode rule identical either
    // way, rather than a deferred fold needing a second implementation.
    if (this.isDeferred()) {
      // The cast picks `reduce`'s own SYNC overload for the replay. Its two overloads differ only
      // in the Mode they report to the caller, which the receiver above has already recorded; the
      // runtime body is one, and a union-typed `fn` matches neither overload on its own.
      return this.defer<U>((p) =>
        p.reduce(
          fn as (acc: U, item: unknown, ctx: IContextManager, emit: (v: U) => void) => U,
          initial,
        ),
      );
    }
    const { chunkTransforms, reduceStages } = this.pushReduceStage(fn, initial);
    // Spread, never field by field (#90): every hand-built option object here has eventually
    // dropped a knob nobody remembered to add - `mode` once, then `bound`, each silently. The
    // overrides below are the fields this stage genuinely changes.
    const carried = {
      ...this.carriedOptions(),
      chunkTransforms,
      reduceStages,
      preBufferItems: null,
      syncPreBufferItems: null,
    };

    // `foldSyncChunkStream` folds the same reducer over the sync chunk stream, deferring only at the
    // first thenable a chunk or the reducer itself produces. `foldChunkStream` is that fold over an
    // `AsyncIterable`, which is the only reason the second arm is always `"async"`.
    if (this.isSync()) {
      return this.createPipeline<U>(EMPTY_CHUNKS as AsyncIterable<U[]>, {
        ...carried,
        mode: this.sourcePolicy() === "async" ? "async" : "sync",
        syncChunks: foldSyncChunkStream(fn, initial, this.syncChunkStream(), this._context),
      }) as AnyPipeline<U>;
    }

    return this.createPipeline<U>(foldChunkStream(fn, initial, this.chunkStream(), this._context), {
      ...carried,
      mode: "async",
      syncChunks: null,
    }) as AnyPipeline<U>;
  }

  /**
   * Runs `build`'s whole region against a base `Pipeline` over this pipeline's own chunk stream and
   * context (#61) - a region, not a per-stage flag, so several consecutive stages that must stay in
   * the orchestrating process are written once instead of repeated on every one of them. Nothing
   * `build` does can dispatch: the pipeline it receives IS a base `Pipeline`, so "local" is a
   * property of the region's class rather than a knob checked per call. The built region's chunks,
   * context, chunk transforms and reduce stages carry back through `this.createPipeline()`, which a
   * dispatching subclass overrides to resume ITS OWN class for whatever comes after the region; on
   * the base class that carry-back is an identity, which is what lets one `.local(build)` call run
   * unchanged on every `Pipeline` subclass.
   *
   * @param build - Runs against a base `Pipeline` seeded from this pipeline's own state; its
   *   returned `Pipeline<U>` becomes the region's output.
   * @returns A new instance of THIS pipeline's own class (or `Pipeline<U>` on the base class
   *   itself), continuing to dispatch normally for whatever comes after the region.
   *
   * @example
   * `new ConcurrentPipeline([1, 2, 3, 4, 5], { maxConcurrency: 2 }).buffer(2).local((p) =>
   * p.transform((t) => t.map((x: number) => x * 2)).reduce((acc: number, x: number) => acc + x,
   * 0)).toArray()` → `[30]` - the map and the fold both run in-process, in one region, instead of
   * dispatching two separate stages.
   */
  local<U, M2 extends PipelineMode>(
    build: (p: Pipeline<T, M, "shape", any>) => Pipeline<U, M2, "shape", any>,
  ): Pipeline<U, AssignMode<P, JoinMode<M, M2>>, P, In> {
    // A region defers whole (#90) - see `apply()`. Deferring the `.local()` CALL rather than its
    // built region is what keeps `build`'s own stages recorded against the region's real pipeline,
    // which is where they pin themselves in the orchestrating process.
    if (this.isDeferred()) {
      return this.defer<U>((p) => (p as unknown as Pipeline<T, M, P, In>).local(build)) as Pipeline<
        U,
        AssignMode<P, JoinMode<M, M2>>,
        P,
        In
      >;
    }
    // Both option objects SPREAD their source pipeline's own carried knobs (#90) - see `reduce()`
    // for why: a hand-built list here dropped `bound`, so the region deferred instead of running
    // and `.local((p) => p.reduce(sum, 0))` over `[1..6]` returned the six items rather than `[21]`.
    const region = new Pipeline<T, "sync" | "async", SourcePolicy>({
      ...this.carriedOptions(),
      chunks: this._chunks,
      pendingStages: [],
    });
    const built = build(region as unknown as Pipeline<T, M, "shape">);
    return this.createPipeline<U>(built._chunks, {
      ...built.carriedOptions(),
    }) as Pipeline<U, AssignMode<P, JoinMode<M, M2>>, P, In>;
  }

  /**
   * An observation point that always runs in the orchestrating process, whatever class it is
   * called on (#72). Delegates to `Transformer.tap`, wrapped in `.local(build)` so the callback and
   * its context writes stay where the caller is even on `HttpPipeline`/`ClusterPipeline`, where a
   * dispatched stage either side of it still dispatches. Declared once here, returning `this` -
   * `Transformer.tap` keeps `Out` unchanged, so `T` never changes either and no subclass
   * re-declaration is needed, unlike `.local()` itself.
   *
   * `arg`'s two overloads dispatch to `Transformer.tap`'s own two overloads inside `build` - a
   * plain `t.tap(arg)` call with `arg` still typed as their union would not typecheck against either
   * overload individually, so the `instanceof` check narrows it first, mirroring
   * `Transformer.tap`'s own implementation.
   *
   * @example
   * `new Pipeline([1, 2, 3]).tap((x) => seen.push(x)).transform((t) => t.map((x) => x *
   * 2)).toArray()` → `[2, 4, 6]`, with `seen` `[1, 2, 3]`.
   */
  tap(
    fn: (item: T, ctx: IContextManager) => Promise<unknown>,
  ): M extends "async" ? this : Pipeline<T, "async", P, In>;
  tap(fn: (item: T, ctx: IContextManager) => unknown): this;
  tap(
    transformer: Transformer<T, unknown, "async">,
  ): M extends "async" ? this : Pipeline<T, "async", P, In>;
  tap(transformer: Transformer<T, unknown, "sync">): this;
  tap(
    arg: PipelineFunction<T, unknown> | Transformer<T, unknown, "sync" | "async">,
  ): this | Pipeline<T, "async", P, In> {
    // Both arms below call the exact same runtime expression, `t.tap(arg)` - this is NOT dead code:
    // `Transformer.tap` is itself overloaded, and a union-typed `arg` matches neither overload on
    // its own, so the instanceof check exists purely to narrow `arg`'s STATIC type per arm before
    // each (otherwise-identical) call, the same way `Transformer.tap`'s own implementation narrows
    // it internally. Collapsing this to one arm - `p.transform((t) => t.tap(arg))` - fails to
    // typecheck. Never edit one arm without the other; a real behavior change belongs in
    // `Transformer.tap` itself, which both arms delegate to unconditionally.
    // The region is typed at a CONCRETE Mode before `.transform()` is called on it: inside this
    // generic method `M` is still abstract, so `.transform()`'s own `M extends "unset" ? never`
    // guard cannot resolve and refuses the receiver. The guard is for a CALLER who has not called
    // `.from()` yet; `.tap()` is only reachable from a pipeline that already has a source, so the
    // narrowing is sound - `M` here is never `"unset"`.
    return this.local((p) => {
      const sourced = p as Pipeline<T, "sync" | "async", "shape"> as Pipeline<T, "sync", "shape">;
      // Both arms call the identical runtime expression; the narrowing exists purely to pick one
      // of `Transformer.tap`'s own overloads, which a union-typed `arg` matches neither of. The
      // Mode cast on the transformer arm is safe for the same reason: `tap` runs its argument for
      // side effects and returns the chunk unchanged, so the argument's own Mode never reaches the
      // value this region produces - only the CALLER's `tap` overload records it, above.
      const tapped = arg as Transformer<T, unknown, "sync">;
      return (arg instanceof Transformer
        ? sourced.transform((t) => t.tap(tapped))
        : sourced.transform((t) =>
            t.tap(arg as PipelineFunction<T, unknown>),
          )) as unknown as Pipeline<T, "sync", "shape">;
      // `as unknown` first: `.local()`'s return names a concrete `In`, which `this` need not share
      // (#90 added `In` as the class's fourth type parameter), so the two no longer overlap enough
      // for a direct cast. `.tap()` keeps `T` and every knob, so the receiver's own type is right.
    }) as unknown as this;
  }

  /**
   * Registers a new reduce stage at the next index in the shared stage-index space
   * `_chunkTransforms` already uses (#45) - `ConcurrentPipeline.reduce()`'s own override calls this
   * too, so both share one bookkeeping seam rather than two copies that could drift apart.
   *
   * @example
   * On a pipeline with one prior `.transform()` stage, `pushReduceStage(fn, 0)` → `{ stageIndex: 1,
   * … }`, with `chunkTransforms[1]` a placeholder that throws if ever run as a per-chunk transform.
   */
  protected pushReduceStage<U>(
    fn: ReduceFunction<U, T>,
    initial: U,
  ): {
    stageIndex: number;
    chunkTransforms: ChunkTransform[];
    reduceStages: Map<number, ReduceStage>;
  } {
    const stageIndex = this._chunkTransforms.length;
    const reduceStages = new Map(this._reduceStages);
    reduceStages.set(stageIndex, { fn, initial } as ReduceStage);
    return {
      stageIndex,
      chunkTransforms: [...this._chunkTransforms, reduceStagePlaceholder(stageIndex)],
      reduceStages,
    };
  }

  // ===== Terminal Operations =====

  /**
   * What a `PipelineResult` needs to drain this pipeline (#90) - the ONE seam the terminal ops read.
   *
   * `toArray`/`first`/`consume`/`forEach` used to live here, which meant a chain could be drained
   * with no input at all: `new Pipeline().toArray()` compiled and resolved to `[]`. They belong to
   * a result, and a result exists only once an input has been given. This exposes the two views
   * they need - the sync chunk stream where there is one, and the item stream otherwise - so
   * neither class has to reach into the other's fields.
   *
   * @example
   * A bound sync pipeline over `[1, 2, 3]` returns `{ syncChunks: <generator>, … }`; an async one
   * returns `{ syncChunks: null, … }` and the caller reads `items()` instead.
   */
  drainable(input: PipelineSource<In>): {
    syncChunks: MaybeAsyncChunks<T> | null;
    items: () => AsyncIterable<T>;
    chunks: () => AsyncIterable<T[]>;
    context: IContextManager;
  } {
    const bound = this.bind(input as Iterable<In>) as unknown as AnyPipeline<T>;
    return {
      syncChunks: bound.isSync() ? bound._syncChunks : null,
      items: () => bound.asyncItems(),
      chunks: () => bound.chunkStream(),
      // THIS run's manager, which is a fresh one per call unless the caller named their own (#90).
      // `.branch()` reads it so an arm's own pipeline sees the writes the parent chain just made,
      // rather than the chain's manager, which holds the previous call's.
      context: bound._context,
    };
  }

  /**
   * Route items to different branches based on predicates.
   *
   * With `firstMatch: true` (default): Items are routed to the first matching branch only.
   * With `firstMatch: false` (broadcast mode): Items are sent to ALL matching branches.
   *
   * Python equivalent:
   * ```python
   * def branch(
   *   self,
   *   branches: Mapping[str, tuple[Transformer[T, U], Callable[[T], bool]]],
   *   *,
   *   first_match: bool = True,
   * ) -> dict[str, list[U]]:
   *   if first_match:
   *     # Router mode - item goes to first matching branch
   *     ...
   *   else:
   *     # Broadcast mode - item goes to ALL matching branches
   *     ...
   * ```
   *
   * @param branches - Map of branch name to { predicate, transformer }
   * @param options - Optional settings: firstMatch (default true)
   * @returns A RUNNER, not the results (#90) - call it to route one input. On a source-less
   *   pipeline it takes the items; on one already bound through `.from()` it takes none, and each
   *   form refuses the other's argument rather than ignoring it. ⚠ BREAKING: `await p.branch({…})`
   *   becomes `await p.branch({…})()`; awaiting the runner alone yields the function.
   *   Read context via `.contextManager` afterward if needed (#744).
   */
  branch<B extends BranchBuilder<T, any, any>>(
    build: (builder: BranchBuilder<T>) => B,
  ): BranchRunner<In, ResultsOf<B>, JoinMode<M, ModeOfArms<B>>> {
    const builder = build(new BranchBuilder<T>());
    const arms = builder.arms();
    const broadcast = builder.isBroadcast();
    const owner = this;
    // The branch's own index in the shared stage space. Both sides walk the same entry module in
    // the same order, so an orchestrator and a worker agree on it without exchanging anything.
    const branchIndex = this._branchStages.size;
    this._branchStages.set(branchIndex, arms as BranchArm<unknown>[]);

    const run = (input?: PipelineSource<In>): BranchResults | Promise<BranchResults> => {
      if (input === undefined) {
        throw new Error(
          "no input: a pipeline holds no data, so .branch()'s runner needs one - call it with the items to route",
        );
      }

      // ONE bind for the whole branch: the parent chain runs, and the arms below share the
      // context that run created rather than the chain's own.
      const { syncChunks, items: itemsOf, context } = owner.drainable(input);
      const collected: T[] = [];
      const items = (
        syncChunks !== null
          ? chain(
              drainSync(syncChunks, (item) => void collected.push(item as T)),
              () => collected,
            )
          : (async () => {
              for await (const item of itemsOf()) collected.push(item);
              return collected;
            })()
      ) as T[] | Promise<T[]>;

      // `chain` defers only at a real thenable, so a synchronous parent stays synchronous here.
      return chain(items, (settled: T[]) => {
        const grouped = demux(settled, arms, broadcast);

        // ROUTER - each arm's own pipeline over its own items, of THIS pipeline's class, so an
        // arm's stages dispatch wherever the parent's do and `.local()` inside pins one. A
        // synchronous arm returns an array right here; only an asynchronous one hands back a
        // promise.
        const outputs = arms.map((arm) => {
          const armItems = grouped.get(arm.name)!;
          if (arm.build === undefined) return armItems as unknown[];
          const armPipeline = owner.emptyOfOwnClass<T>(
            context,
            `/branch/${branchIndex}/${arm.name}`,
          ) as unknown as Pipeline<T, "unset", "shape", T>;
          const builtArm = arm.build(armPipeline) as unknown as (i: T[]) => {
            toArray(): unknown[] | Promise<unknown[]>;
          };
          return builtArm(armItems).toArray();
        });

        // JOIN - a plain record unless at least one arm is pending, and then only those are
        // awaited. On the orchestrator by necessity: arms can be remote, so it is the only process
        // that sees all of them.
        return chain(settleMaybe(outputs), (armResults: unknown[][]) =>
          Object.fromEntries(arms.map((arm, i) => [arm.name, armResults[i]])),
        ) as BranchResults | Promise<BranchResults>;
      }) as BranchResults | Promise<BranchResults>;
    };

    return run as BranchRunner<In, ResultsOf<B>, JoinMode<M, ModeOfArms<B>>>;
  }

  /**
   * A stage-less pipeline of THIS pipeline's own class, for one `.branch()` arm to build on (#90).
   *
   * The class is the whole point: an arm built on `HttpPipeline` dispatches its stages the way the
   * parent's do, and `.local()` inside the arm's builder pins it here instead. The `Transformer`
   * this replaces had no class, so every arm ran where the caller was however the chain was built.
   *
   * @example
   * On an `HttpPipeline`, `emptyOfOwnClass()` is an `HttpPipeline` sharing the parent's url and
   * context, with no stages of its own yet.
   */
  protected emptyOfOwnClass<U>(context: IContextManager, routeTrail = ""): AnyPipeline<U> {
    return this.createPipeline<U>(EMPTY_CHUNKS as AsyncIterable<U[]>, {
      ...this.carriedOptions(),
      context,
      contextIsDefault: false,
      routeTrail,
      branchStages: new Map(),
      chunkTransforms: [],
      reduceStages: new Map(),
      pendingStages: [],
      preBufferItems: null,
      syncPreBufferItems: null,
      syncChunks: null,
      mode: "unset",
      bound: false,
    });
  }
}

// Every `Pipeline` instance IS a function (#90, see the constructor), so its prototype chain must
// reach `Function.prototype` - that is what makes `instanceof Function`, `.bind` and `.call` work
// on one. Done here, once, rather than via `class Pipeline extends Function`: `super()` on a
// `Function` subclass runs `CreateDynamicFunction`, which throws `EvalError: Code generation from
// strings disallowed for this context` wherever code generation is banned (a CSP page, a Cloudflare
// Worker, `node --disallow-code-generation-from-strings`). Every subclass inherits the reparenting
// through its own prototype chain, so this line covers `ConcurrentPipeline`, `HttpPipeline` and
// `ClusterPipeline` too.
Object.setPrototypeOf(Pipeline.prototype, Function.prototype);
