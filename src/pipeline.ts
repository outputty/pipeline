/**
 * `Pipeline` - a chain over an input TYPE, holding no data (#90).
 *
 * Composed once and RUN by calling it, so one definition serves every input. `.transform()`,
 * `.apply()`, `.buffer()`, `.reduce()`, `.local()`, `.tap()`, `.context()` and `.branch()` compose;
 * calling the result hands back a `PipelineResult` (`./result.ts`), which is where every terminal
 * op lives. Where a stage RUNS is chosen by constructing a class - `ConcurrentPipeline`,
 * `HttpPipeline`, `ClusterPipeline` (`./pipelines/`) - never by configuring the chain.
 *
 * `new Pipeline<number>().transform((t) => t.map((x) => x * 2))([1, 2, 3]).toArray()` → `[2, 4, 6]`,
 * with no `await` and no `Promise` created.
 */

import type {
  IContextManager,
  ReduceFunction,
  PipelineFunction,
  PipelineErrorHandler,
  PipelineMode,
  SourcePolicy,
  JoinMode,
  SeedMode,
  ChunkTransform,
  ReduceStage,
  StageRegistries,
  Drainable,
  BufferFunction,
} from "./types";
import { DEFAULT_CHUNK_SIZE, DROP } from "./types";
import { SimpleContextManager } from "./context/simple";
import { Transformer } from "./transformer";
import type { MaybeAsyncChunks } from "./utils/chunk";
import {
  buildChunkGenerator,
  buildSyncChunkGenerator,
  flattenChunks,
  recutSyncChunks,
  prefetch,
} from "./utils/chunk";
import { assertWholeNumberAtLeastOne } from "./utils/cut";
import { chain, runStageChunk } from "./utils/helpers";
import { PipelineResult } from "./result";
import { BranchBuilder, runBranch } from "./branch";
import type {
  ArmPipeline,
  BranchArm,
  BranchOwner,
  BranchRunner,
  ModeOfArms,
  ResultsOf,
} from "./branch";
import {
  foldChunkStream,
  foldSyncChunkStream,
  sizeReduceFunction,
  bufferReduceFunction,
  buildBufferGenerator,
  buildSyncBufferGenerator,
  recutSyncChunksWith,
} from "./utils/reduce";

/** Builds a plain async-iterable from an async generator function (#133) - the
 * `{ [Symbol.asyncIterator]: gen }` wrapper every hand-rolled adapter below repeats. */
function asyncIterableFrom<U>(gen: () => AsyncGenerator<U>): AsyncIterable<U> {
  return { [Symbol.asyncIterator]: gen };
}

/** The chunk stream a `Pipeline` that has no input yet carries, and the one a WORKER process's own
 * copy is reset to (`ClusterPipeline`'s constructor). Shared rather than rebuilt per instance: it
 * is empty, stateless and re-iterable, where a spent generator would read empty only once. */
export const EMPTY_CHUNKS: AsyncIterable<never[]> = asyncIterableFrom(
  // eslint-disable-next-line @typescript-eslint/require-await
  async function* () {},
);

/** `EMPTY_CHUNKS`, cast to the chunk type a call site actually needs (#133) - the same
 * `EMPTY_CHUNKS as AsyncIterable<U[]>` cast was spelled inline 8x across this file and
 * `pipelines/cluster.ts` before this; `EMPTY_CHUNKS` itself stays typed `never[]` so a caller
 * reading it directly (never a call site here) still sees "genuinely empty", not "empty of some
 * particular type".
 *
 * `emptyChunks<number>()` → `EMPTY_CHUNKS`, typed `AsyncIterable<number[]>`.
 */
export function emptyChunks<U>(): AsyncIterable<U[]> {
  return EMPTY_CHUNKS as AsyncIterable<U[]>;
}

/** Whether `data` is itself async-iterable, rather than merely iterable (#90) - the one test
 * `.from()`'s own Mode decision and `toAsyncIterable()` below both need (#133: was spelled inline
 * twice). */
function isAsyncSource(data: PipelineSource<unknown>): boolean {
  return Symbol.asyncIterator in Object(data);
}

/** Converts a sync iterable to an async one, for the `"async"` arm of `.from()` alone. A `"sync"`
 * source never goes through this: skipping it is most of what #90 recovers. */
function toAsyncIterable<U>(data: PipelineSource<U>): AsyncIterable<U> {
  if (isAsyncSource(data)) {
    return data as AsyncIterable<U>;
  }
  const syncIterable = data as Iterable<U>;
  return asyncIterableFrom(async function* () {
    for (const item of syncIterable) {
      yield item;
    }
  });
}

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
export type AnyPipeline<U> = Pipeline<U, PipelineMode, any>;

/**
 * A chain a wrapping class can adopt (#90): source-less, so its stages are still recorded calls to
 * replay, and carrying its own input type `In` so the wrapper can accept the same input.
 *
 * `In` is named rather than `any` because a wrapper's constructor INFERS it: with `any` there the
 * wrapper fell back to its own `T`, which is each stage's OUTPUT type, so
 * `new ConcurrentPipeline(numberToString)` typed its input `string` and rejected the `number[]`
 * that ran fine at runtime.
 *
 * Any Mode, deliberately. It was `"unset"`, to make wrapping a `.from()`-bound pipeline a compile
 * error rather than a runtime throw - but `.from()` went with this ticket, and the bound left
 * behind refused a chain holding ONE async callback: `new ConcurrentPipeline(asyncChain, {…})` was
 * `TS2345`, on the very class that forces `"async"` and therefore exists for exactly that chain.
 * Boundness is `adopt()`'s check, at runtime, where the runtime fact `_bound` actually lives.
 */
export type WrappablePipeline<T, In> = Pipeline<T, PipelineMode, In>;

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
 * The knobs a CALLER writes when constructing a `Pipeline` - both optional, and both about the
 * context manager, which is the only construction-time decision a caller actually makes.
 *
 * Everything else a pipeline carries is `PipelineState` below. The two were ONE exported interface,
 * and most of its fields repeated "Internal: … Not intended for direct external use" in their own
 * docstrings - a public surface saying over and over that it was not public.
 */
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
}

/**
 * Everything a copy-on-write call carries from one instance to the next (#90) - internal, and
 * declared apart from `PipelineOptions` so the exported surface is the two knobs above.
 *
 * `carriedOptions()` returns this whole shape rather than naming fields at each call site, which is
 * what stops a knob being dropped: a hand-built object here lost `mode` once and then `bound`, and
 * the second one made `.local((p) => p.reduce(sum, 0))` over `[1..6]` return the six items instead
 * of `[21]`. `adopt()` and `emptyOfOwnClass()` still name their fields, deliberately - one excludes
 * what a wrapped chain must not inherit, the other resets an arm to blank.
 */
export interface PipelineState {
  /**
   * an already-cut chunk stream to seed `_chunks` with directly, bypassing the
   * constructor's own default cut - the copy-on-write path every method below (`.apply()`,
   * `.buffer()`, `.context()`) uses via `createPipeline()`.
   */
  chunks?: AsyncIterable<unknown[]>;
  /**
   * the pre-buffer ITEM view `.buffer()` recuts from on a second, back-to-back call -
   * `null` once a real stage has consumed `chunks` (`.apply()` sets it), so a LATER `.buffer()`
   * falls back to flattening whatever that stage actually produced instead.
   */
  preBufferItems?: AsyncIterable<unknown> | null;
  /**
   * the chain of chunk-wise transforms accumulated via `.apply()`/`.transform()` -
   * `HttpPipeline`'s own `.fetch()` looks a stage up by index here to serve a dispatched request.
   *
   */
  chunkTransforms?: ChunkTransform[];
  /**
   * every reduce stage registered via `.reduce()`, keyed by its index in the SAME shared
   * space `chunkTransforms` uses - `HttpPipeline`'s own `.fetch()` (#45 L5) looks a stage up here to
   * serve a `/reduce/<n>` request.
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
   * the RUNTIME half of `PipelineMode` (#90) - which engine this pipeline's own terminal
   * ops read. Set by `.from()` from the source's shape and the class's own `SourcePolicy`, and
   * carried forward by every copy-on-write call.
   */
  mode?: PipelineMode;
  /**
   * the SYNC chunk stream a `"sync"`-Mode pipeline reads (#90), whose individual chunks
   * may still be pending once a stage's callback returned a thenable. `null` on an `"async"` chain,
   * where `chunks` carries the stream instead.
   */
  syncChunks?: MaybeAsyncChunks<unknown> | null;
  /**
   * `preBufferItems`' sync counterpart (#90) - the raw item view a back-to-back
   * `.buffer()` recuts from on a `"sync"` chain.
   */
  syncPreBufferItems?: Iterable<unknown> | null;
  /**
   * the chunk boundary `.buffer(size)` last declared, carried so a `.buffer()` called
   * BEFORE `.from()` still decides the source's own cut (#90). `.from()` read `DEFAULT_CHUNK_SIZE`
   * unconditionally before this existed, so that call was silently discarded.
   */
  chunkSize?: number;
  /**
   * every stage composed while the pipeline had no source, in order, replayed by
   * `.from()` once an input arrives (#90).
   */
  pendingStages?: PendingStage[];
  /**
   * the route prefix an arm's own stages address themselves under, `/branch/<i>/<name>`
   * (#90). Empty on a chain's own stages. Without it an arm's stage 0 collided with the parent's
   * stage 0 on the worker, which served the parent's transform for both.
   */
  routeTrail?: string;
  /**
   * every `.branch()` stage's own arms, keyed by the branch's index in the shared stage
   * space (#90) - the registry a serving side walks to resolve a `/branch/<i>/<name>/` trail. Not
   * intended for direct external use.
   */
  branchStages?: Map<number, BranchArm<unknown>[]>;
  /**
   * whether `context` was invented by a `Pipeline` rather than named by the caller (#90).
   * A default-built manager belongs to one run, so a reusable chain gets a fresh one per call; a
   * `context` or `contextFactory` the caller named is theirs and is kept. Carried explicitly
   * through copy-on-write, since every such call passes an already-resolved `context` and would
   * otherwise look caller-supplied.
   */
  contextIsDefault?: boolean;
  /**
   * whether an input has been bound to this chain (#90). A RUNTIME fact, kept apart from
   * `mode`, which is a TYPE fact about what the chain produces. `"unset"` used to answer both, and
   * the two are independent: a callable chain is `"unset"` for its whole life and becomes bound
   * only for the duration of one call.
   */
  bound?: boolean;
}

/** What the constructor and `createPipeline()` take: a caller's own knobs plus the carried state.
 * Every internal call site passes both, which is why they were one interface to begin with. */
export type PipelineConstructorOptions = PipelineOptions & PipelineState;

/** The `_chunkTransforms` slot a reduce stage occupies - a reduce stage isn't a per-chunk
 * `ChunkTransform` (it folds across chunks, not one chunk in for one chunk out), so its slot throws
 * if ever invoked as one. This IS the fail-loud guard #45's own ticket wanted from the killed
 * `sourcePositionViolations` mechanism (#39 deleted it entirely, and there is no separate replay
 * path left for a violations list to protect against - architecture.md's own text says so). */
function reduceStagePlaceholder(stageIndex: number): ChunkTransform {
  return () => {
    throw new Error(
      `stage ${stageIndex} is a reduce stage, not a plain per-chunk transform - it cannot serve ` +
        `/transform/${stageIndex}`,
    );
  };
}

/**
 * A chain over `T`, holding its input TYPE and no data (#90). Nothing runs until the pipeline is
 * CALLED and a terminal on the `PipelineResult` (`toArray`, `first`, iteration, …) pulls:
 * `.apply()`/`.transform()` compose transformers, chunking is handled for
 * you, and a chunk's items flow to the next stage as a group. The whole chain shares one context
 * manager, so a context-aware transformer can read and write state across stages — `.context()`
 * itself still returns a NEW `Pipeline` (copy-on-write, like `.apply()`/`.transform()`/`.buffer()`),
 * carrying the SAME context manager forward, mutated in place (#31) — a caller's own
 * `IContextManager` class is never copied into a fresh `SimpleContextManager` and discarded.
 *
 * `new Pipeline<number>().transform((t) => t.map((x) => x * 2))([1, 2, 3]).toArray()` → `[2, 4, 6]`,
 * with no `await`: every callback here is synchronous. A terminal's return carries no context
 * snapshot (#744), and a CALL seeds a fresh manager from the chain's own values - so a run's
 * `ctx.set()` reaches the caller only through a manager passed as `options.context`.
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
export interface Pipeline<T, M extends PipelineMode = "unset", In = T> {
  (input: AsyncIterable<In>): PipelineResult<T, "async">;
  (input: Iterable<In>): PipelineResult<T, M extends "async" ? "async" : "sync">;
}

export class Pipeline<T, M extends PipelineMode = "unset", In = T> {
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
  protected _armRegistries?: Map<string, StageRegistries>;
  /** Whether an input has been bound to this chain - see `PipelineOptions.bound`. */
  protected _bound!: boolean;
  /** `registries()`'s memo - built on first serve, never carried through copy-on-write, since the
   * next instance's own stage list is different. */
  protected _registries?: StageRegistries;

  /**
   * Builds a chain over `T`, with no data. `T` is the type it will be CALLED with.
   *
   * @param options - The caller's own context manager or factory (`PipelineOptions`), plus the
   *   carried state a copy-on-write call threads through (`PipelineState`), which no caller writes.
   */
  constructor(options?: PipelineConstructorOptions) {
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
    // ⚠ TWO exceptions to substitutability: `.apply` is a STAGE method here and `.bind` is
    // `.from()`'s protected survivor, so both shadow `Function.prototype`'s. `protected` is erased
    // at runtime, so a JS consumer reaches `.bind` too. Measured: `score.call(null, [1,2,3])`
    // returns a `PipelineResult`, `score.apply(null, [[1,2,3]])` returns a `Pipeline` - it reached
    // `Pipeline.apply()` - and `score.bind(null)` returns a `Pipeline` bound to `null` that throws
    // when called. Only `.call` is unaffected. A consumer that invokes callbacks via
    // `fn.apply(ctx, args)` or `fn.bind(ctx)` needs a wrapper: `(input) => score(input)`.
    const self = ((input: PipelineSource<unknown>) => {
      // A pipeline that already named a source through `.from()` has materialised its stages into
      // a chunk stream, and only stages recorded SINCE then can be replayed onto a new input - so
      // calling one silently dropped every earlier stage. Measured: `.from([1,2,3]).transform(x =>
      // x * 2)` drained in place to `[2,4,6]`, then the same object called with `[10,20]` returned
      // `[10,20]`. Refused rather than half-honoured; the state disappears entirely once `.from()`
      // does, at which point every pipeline is callable and none is bound.
      if (self._bound) {
        throw new Error(
          "cannot call a pipeline that is already bound to a source - build the chain, then call it with the input. Note that Pipeline.bind() is a stage method, not Function.prototype.bind: wrap the pipeline as `(input) => pipeline(input)` to bind a receiver",
        );
      }
      return new PipelineResult<T, PipelineMode>(
        self as unknown as Pipeline<unknown, "sync" | "async", unknown>,
        input,
      );
    }) as unknown as Pipeline<T, M, In>;
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
    self._chunks = (options?.chunks ?? emptyChunks<T>()) as AsyncIterable<T[]>;
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
  protected bind<U>(data: AsyncIterable<U>): Pipeline<U, "async", In>;
  // A receiver already widened to `"async"` stays async whatever the source's own shape (#90):
  // `.onError()` and `.context()` are both callable BEFORE `.from()`, so an async run handler
  // registered there had its widening discarded here - the chain typed `number[]` while
  // `dropOrRethrow` deferred on that handler the moment a chunk failed. `"unset"` is the ordinary
  // case and still takes the source's own shape, which is what keeps `.from([1,2,3])` synchronous.
  protected bind<U>(data: Iterable<U>): Pipeline<U, M extends "async" ? "async" : "sync">;
  protected bind<U>(data: PipelineSource<U>): Pipeline<U, "sync" | "async"> {
    return this.fromSource<U>(data, this.sourcePolicy()) as Pipeline<U, "sync" | "async">;
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
    // `this._mode` is read too, so a receiver already widened before `.from()` stays widened - see
    // `from`'s own Iterable overload for the case that made this necessary.
    const mode: "sync" | "async" =
      isAsyncSource(data) || policy === "async" || this._mode === "async" ? "async" : "sync";

    if (mode === "sync") {
      const items = data as Iterable<U>;
      return this.replayPending(
        this.createPipeline<U>(emptyChunks<U>(), {
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
   * `HttpPipeline.url`, …) overrides `carriedKnobs()` below to carry them forward, rather than this
   * method itself (#133: every dispatching class used to override `createPipeline()` wholesale,
   * repeating the identical `new Ctor({ ...options, ...knobs, chunks })` shape around its own one or
   * two extra fields) — `this.constructor` alone only reproduces knobs `PipelineOptions` itself
   * already carries. This base implementation is correct for `Pipeline` itself and for any subclass
   * whose constructor takes nothing beyond `(source, options)`.
   *
   * `R` is the caller's own return type (#133, the same seam `defer()` above uses and for the same
   * reason): a subclass method declaring a precise return type - `ConcurrentPipeline.apply()`
   * returning `ConcurrentPipeline<U, In>` - names it as `this.createPipeline<U,
   * ConcurrentPipeline<U, In>>(...)` and gets it back with no trailing `as X` cast of its own,
   * since this method's own default `AnyPipeline<U>` would otherwise be all a caller sees.
   *
   * @example
   * A `Sub extends Pipeline` with no extra constructor params: `new Sub([1]).transform(f)
   * .constructor.name` → `"Sub"`, because `apply()` (below) calls this method rather than `new
   * Pipeline(...)` directly.
   */
  protected createPipeline<U, R = AnyPipeline<U>>(
    chunks: AsyncIterable<U[]>,
    options: PipelineConstructorOptions,
  ): R {
    const Ctor = this.constructor as new (options?: PipelineConstructorOptions) => AnyPipeline<U>;
    return new Ctor({ ...options, ...this.carriedKnobs(), chunks }) as unknown as R;
  }

  /** A dispatching subclass's own EXTRA constructor knobs, beyond what `PipelineOptions` itself
   * already carries (#133) - `createPipeline()` above spreads this onto every copy-on-write
   * instance it builds, which is the one seam a subclass needs to override rather than
   * `createPipeline()` as a whole. The base has none. `ConcurrentPipeline` overrides it to return
   * `{ maxConcurrency, ordered }`; `HttpPipeline`/`ClusterPipeline`/`EventEmitterPipeline` each
   * extend their PARENT's own override with `{ ...super.carriedKnobs(), <their own field(s)> }`.
   *
   * Typed `object`, not `Record<string, unknown>`: a subclass's own named-field interface
   * (`ConcurrentPipelineOptions`, `HttpPipelineOptions`, …) has no index signature, so `tsc` refuses
   * it as an override of a `Record<string, unknown>`-returning method even though every field it
   * declares is itself spreadable into one. */
  protected carriedKnobs(): object {
    return {};
  }

  /** Every knob a copy-on-write call carries into the next instance (#90) - named once here rather
   * than repeated field by field at each of the eight call sites, so a knob added later reaches all
   * of them. `chunks` is passed separately, since each caller supplies its own. */
  protected carriedOptions(): PipelineConstructorOptions {
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
      // A COPY, not the map itself: `.branch()` writes into `_branchStages`, and every other
      // stage method returns a fresh instance rather than mutating one. Shared by reference, one
      // chain's `.branch()` landed in every ancestor's and sibling's registry, and `branchIndex`
      // (derived from `.size`) then depended on which sibling was declared first.
      branchStages: new Map(this._branchStages),
    };
  }

  /** The pre-buffer reset every REAL stage needs, once it has consumed the chunk stream (#133) -
   * nothing is left to recut a back-to-back `.buffer()` from except that stage's own output, so
   * both views null out. Named once here rather than repeated as a `{ preBufferItems: null,
   * syncPreBufferItems: null }` pair at `apply()`/`reduce()` (below) and
   * `ConcurrentPipeline.apply()`/`.reduce()`'s own dispatched equivalents - a dispatching
   * subclass has no sync chunk stream of its own, so nulling `syncPreBufferItems` there is a
   * no-op, not a behavior change. */
  protected freshPreBuffer(): Pick<
    PipelineConstructorOptions,
    "preBufferItems" | "syncPreBufferItems"
  > {
    return { preBufferItems: null, syncPreBufferItems: null };
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
   * `R` is the caller's own return type (#133) - `createPipeline()` only ever produces the loosely
   * typed `AnyPipeline<U>`, so every caller used to re-assert its own precise type in an `as X` at
   * the CALL site (7 of them, across this file and `ConcurrentPipeline`'s own overrides). Naming
   * `R` here moves that one assertion inside `defer()` itself - a caller writes
   * `this.defer<U, Pipeline<U, JoinMode<M, M2>, In>>(...)` and gets the precise type back with no
   * trailing cast of its own.
   *
   * @example
   * `new Pipeline<number>().transform((t) => t.map((x) => x * 2))` records one stage; calling that
   * pipeline with `[1, 2, 3]` replays it and yields `[2, 4, 6]`.
   */
  protected defer<U, R = AnyPipeline<U>>(run: PendingStage, extra?: PipelineState): R {
    return this.createPipeline<U>(emptyChunks<U>(), {
      ...this.carriedOptions(),
      ...extra,
      pendingStages: [...this._pendingStages, run],
    }) as unknown as R;
  }

  /** Whether this pipeline records its stages rather than running them - true until an input
   * arrives, which is every pipeline built the callable way (#90). */
  protected isDeferred(): boolean {
    return !this._bound;
  }

  /**
   * This pipeline's stage registries - the two maps a SERVING side reads to answer `/transform/<n>` and
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
  protected registriesFor(trail: string): StageRegistries | null {
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

  protected registries(): StageRegistries {
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
  protected static adopt(pipeline: AnyPipeline<any>): PipelineConstructorOptions {
    if (pipeline._bound) {
      throw new Error(
        "cannot wrap a pipeline that is already bound to a source - wrap the unbound chain instead",
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
  protected static wrapping<O extends PipelineConstructorOptions>(
    first: AnyPipeline<any> | O | undefined,
    second: O | undefined,
  ): O {
    if (first instanceof Pipeline) return { ...Pipeline.adopt(first), ...second } as O;
    return (first ?? second ?? {}) as O;
  }

  /** This pipeline's CHUNKS as an async stream, whichever engine it runs on (#90) - a `"sync"`
   * pipeline leaves `_chunks` empty and carries `_syncChunks` instead, so this is the one place
   * that difference is resolved. `asyncItems()` and `PipelineResult.chunks()` both read it. */
  protected chunkStream(): AsyncIterable<T[]> {
    if (!this.isSync()) return this._chunks;
    const syncChunks = this._syncChunks!;
    return asyncIterableFrom(async function* () {
      for (const chunk of syncChunks) yield await chunk;
    });
  }

  /** Whether this pipeline runs on the synchronous engine (#90) - what `reduce()`/`apply()`/
   * `buffer()`/`chunkStream()`/`syncChunkStream()` all ask before choosing between the sync and
   * the async engine, rather than each raw-spelling `_mode === "sync" && _syncChunks !== null`
   * (#133: was spelled inline 4x, its own negation included). */
  protected isSync(): boolean {
    return this._mode === "sync" && this._syncChunks !== null;
  }

  /** This pipeline's chunks as a SYNC stream, for the synchronous fold. A pipeline that is not
   * `"sync"` has none, so this raises rather than inventing one; `isSync()` above is the guard
   * every caller checks first. */
  protected syncChunkStream(): MaybeAsyncChunks<T> {
    if (!this.isSync()) {
      throw new Error("no sync chunk stream: this pipeline runs on the asynchronous engine");
    }
    return this._syncChunks!;
  }

  /** This pipeline's items, as one stream, whichever engine it runs on (#90) - the seam the async
   * terminal ops and `[Symbol.asyncIterator]` read, so neither has to branch on `_mode` itself. */
  protected asyncItems(): AsyncIterable<T> {
    return flattenChunks(this.chunkStream());
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
  ): M extends "async" ? this : Pipeline<T, "async", In>;
  onError(handler: (error: Error, ctx: IContextManager) => void): this;
  onError(handler: PipelineErrorHandler): this | Pipeline<T, "async", In> {
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
      return this.defer<T, this>((p) => p.onError(handler));
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
  ): Pipeline<U, JoinMode<M, M2>, In> {
    // No source yet: record the call and replay it when one arrives (#90). This is the ordinary
    // case for a callable pipeline, where the chain is composed before any data exists.
    if (this.isDeferred()) {
      return this.defer<U, Pipeline<U, JoinMode<M, M2>, In>>((p) =>
        p.apply(transformer as Transformer<unknown, U, M2>),
      );
    }
    const runnable = transformer.runnable();
    const carried = {
      ...this.carriedOptions(),
      chunkTransforms: [...this._chunkTransforms, runnable as unknown as ChunkTransform],
      // A real stage just consumed the chunk stream - nothing left to recut a back-to-back
      // `.buffer()` from except this stage's own output, so the pre-buffer item views reset.
      ...this.freshPreBuffer(),
    };

    // A `"sync"` chain maps its chunks through the SAME `runnable` a `"sync"` one does, but through
    // a sync generator (#90): no `Promise` is created unless the transformer itself returns one,
    // and that chunk then travels the stream as a pending value the terminal op settles.
    if (this.isSync()) {
      const source = this._syncChunks!;
      const ctx = this._context;
      const runHandler = this._runHandler;
      function* stageChunks(): Generator<U[] | Promise<U[]>> {
        for (const chunk of source) {
          yield chain(chunk, (settled) => runStageChunk(runnable, settled, ctx, runHandler)) as
            U[] | Promise<U[]>;
        }
      }
      return this.createPipeline<U>(emptyChunks<U>(), {
        ...carried,
        syncChunks: stageChunks(),
      }) as Pipeline<U, JoinMode<M, M2>, In>;
    }

    return this.createPipeline<U>(
      transformer.process(this._chunks, this._context, this._runHandler),
      {
        ...carried,
        syncChunks: null,
      },
    ) as Pipeline<U, JoinMode<M, M2>, In>;
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
  ): Pipeline<U, JoinMode<M, M2>, In> {
    const transformer = t(new Transformer<T, T, SeedMode<M>>({ transform: (chunk) => chunk }));
    return this.apply(transformer) as unknown as Pipeline<U, JoinMode<M, M2>, In>;
  }

  /**
   * The chunk boundary - explicit, opt-in (#39). Every later `.transform()`/`.apply()` sees these
   * chunks unchanged until another `.buffer()` call declares a new one. `size` cuts by count;
   * `fn: BufferFunction<T>` (#88) decides the boundary per item instead - a `T[]` pending array the
   * framework owns, folded through it item by item. `fn`'s own `emit()` takes no value: it flushes
   * whatever is pending and resets it to `[]`; returning a value appends it to the (possibly
   * just-reset) pending array, returning `DROP` skips the item entirely. `.buffer(size)` is this
   * same engine configured with an identity `fn` and a framework-side auto-flush at
   * `pending.length >= size` - one engine, not two.
   *
   * Recuts from `_preBufferItems` (the raw item stream) when it is still set - nothing has
   * consumed `_chunks` since the last cut, so a run of `.buffer()` calls with nothing between them
   * collapses to only the LAST one ever actually applied, never stacking a redundant
   * flatten-then-recut on top of an intermediate cut nobody asked to see. Once a real stage
   * (`.apply()`) has run, `_preBufferItems` is `null` and this flattens `_chunks` itself first -
   * a genuine re-chunk of that stage's own output.
   *
   * Typed `this` (#17) - `T` never changes here either, so this stays chainable on a dispatching
   * subclass without losing its own `.local(build)` overload. `.buffer(fn)` widens Mode to
   * `"async"` when `fn` returns a `Promise`, the same rule `.transform()`/`.reduce()` already
   * follow: two overloads, ordered Promise-first, mirroring `Pipeline.reduce()`'s own split
   * (`ReduceFunction<U, T>`'s two overloads) rather than `.tap()`'s conditional-collapse form,
   * since neither `.reduce()` nor `.buffer()` needs the "already async, stay `this`" special case
   * `.tap()`'s own `M extends "async" ? this : …` exists for.
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
   *
   * @example
   * `new Pipeline(events).buffer((item, ctx, emit) => (item.invalid ? DROP : item)).toArray()`
   * drops an invalid item entirely, from the chunk it would otherwise have joined.
   */
  buffer(size: number): this;
  buffer(
    fn: (item: T, ctx: IContextManager, emit: () => void) => Promise<T | typeof DROP>,
  ): Pipeline<T, "async", In>;
  buffer(fn: (item: T, ctx: IContextManager, emit: () => void) => T | typeof DROP): this;
  buffer(sizeOrFn: number | BufferFunction<T>): this | Pipeline<T, "async", In> {
    // Before an input there is no stream to cut, so the call is recorded and replayed in PLACE
    // (#90) - the same deferral `.apply()`/`.reduce()`/`.local()` use, for the same reason.
    //
    // Recording only the size, as an earlier form did, loses the call's position: `fromSource` then
    // applied it to the SOURCE cut, so a `.buffer()` written after a stage took effect before it.
    // Measured on `.transform(t => t.flatMap(x => [x, x])).buffer(2)` over `[1,2,3,4]` - chunks
    // came out `[[1,1,2,2],[3,3,4,4]]` where the same chain after `.from()` gives
    // `[[1,1],[2,2],[3,3],[4,4]]`. `chunkSize` is still carried alongside a NUMERIC `.buffer()`,
    // because a `.buffer()` written BEFORE any stage must also cut the source itself, which is what
    // it now does by replaying against a pipeline whose source is already cut at that size.
    // `BufferFunction` has no equivalent knob (`_chunkSize` is a plain `number`), so a deferred
    // `.buffer(fn)` leaves the SOURCE's own first cut at the default and relies entirely on its own
    // replay's re-cut from `_preBufferItems` - the same final chunking either way, since that
    // re-cut always runs regardless of what the first cut used.
    if (this.isDeferred()) {
      // Validated HERE as well as in `sizeReduceFunction` (below), because a deferred `.buffer()`
      // only records the call: `new Pipeline<number>().buffer(0)` used to return a pipeline and
      // throw `chunkSize must be at least 1` later, at the drain, in a message that never names
      // `.buffer()`. Every chain is source-less by default now, so that is the ordinary path.
      // Non-integer refused as well as `< 1`: the two cutting paths round it differently.
      // `.buffer(2.5)` accumulated until `length >= 2.5`, so the SOURCE cut at 3, while
      // `cutChunk`'s re-cut sliced `index + 2.5` and cut at 2 - one call, two boundaries over the
      // same data, no error. `BufferFunction` takes no such validation - any function is accepted.
      if (typeof sizeOrFn === "number") {
        assertWholeNumberAtLeastOne("buffer size", sizeOrFn);
      }
      const cutsTheSource = this._pendingStages.length === 0;
      if (typeof sizeOrFn === "number") {
        const size = sizeOrFn;
        return this.defer<T, this>((p) => p.buffer(size), cutsTheSource ? { chunkSize: size } : {});
      }
      const fn = sizeOrFn;
      return this.defer<T, this>((p) => p.buffer(fn));
    }

    // "One engine, not two" (#88): both overloads fold items through the SAME `Reducer<T[], T>`-
    // based engine (`src/utils/reduce.ts`) - `sizeReduceFunction` configures it with an identity fn
    // and a framework-side auto-flush at `pending.length >= size`, `bufferReduceFunction` adapts a
    // caller's own `BufferFunction`. Built once here so every branch below shares the identical
    // `reduceFn`; `sizeReduceFunction` validates `sizeOrFn` eagerly, the same moment
    // `buildChunkGenerator`/`buildSyncChunkGenerator` already did for an already-bound `.buffer(0)`.
    const reduceFn: ReduceFunction<T[], T> =
      typeof sizeOrFn === "number"
        ? sizeReduceFunction<T>(sizeOrFn)
        : bufferReduceFunction<T>(sizeOrFn);

    // The `"sync"` arm recuts with the sync chunker (#90) - going through the async one here would
    // make `.buffer()` alone widen a chain whose every callback is synchronous, which is exactly
    // the Mode/runtime divergence this ticket exists to remove.
    if (this.isSync()) {
      const items = this._syncPreBufferItems;
      // Nothing has consumed the stream since the last cut: re-cut from the raw items, so a run of
      // back-to-back `.buffer()` calls collapses to the LAST one, exactly as the async arm does.
      if (items !== null) {
        return this.createPipeline<T>(emptyChunks<T>(), {
          ...this.carriedOptions(),
          syncChunks: buildSyncBufferGenerator<T>(reduceFn, this._context)(items),
          syncPreBufferItems: items,
        }) as this;
      }
      // A real stage already ran, so only `_syncChunks` survives. The numeric case keeps
      // `recutSyncChunks`'s own index-based re-slice untouched here - no Done-when case exercises
      // this sub-path, and it predates this ticket. A `BufferFunction` folds each existing chunk
      // SLOT through the same engine via `recutSyncChunksWith` - never flattened to items first,
      // because a slot can still carry a genuinely pending `Promise<T[]>` even while `isSync()`
      // reads `true` (a stage between two `.buffer()` calls widens only THAT stage's own output,
      // not the chain's Mode).
      return this.createPipeline<T>(emptyChunks<T>(), {
        ...this.carriedOptions(),
        syncChunks:
          typeof sizeOrFn === "number"
            ? recutSyncChunks(this._syncChunks!, sizeOrFn)
            : recutSyncChunksWith<T>(this._syncChunks!, reduceFn, this._context),
        syncPreBufferItems: null,
      }) as this;
    }

    const items = this._preBufferItems ?? flattenChunks(this._chunks);
    return this.createPipeline<T>(buildBufferGenerator<T>(reduceFn, this._context)(items), {
      ...this.carriedOptions(),
      preBufferItems: items,
    }) as this;
  }

  /**
   * Prefetches up to `capacity` chunks ahead of the consumer, decoupling WHEN a chunk is pulled
   * from `.buffer()`'s own already-cut stream from WHEN a downstream terminal asks for it (#123).
   * `.buffer()` still owns the cut itself; `.queue()` only changes the timing of each fetch.
   * Unconditionally widens Mode to `"async"`, the same way a dispatching class's `.from()` override
   * already does - a queued chunk may not be ready yet even on an otherwise fully synchronous
   * chain, so there is no "stays sync" case the way `.buffer(fn)`'s non-Promise overload has.
   *
   * `prefetch()` (`src/utils/cut.ts`) is the engine: an array of exactly `capacity` pending
   * `upstream.next()` promises, refilled one-for-one the instant the consumer takes the front one -
   * order preserved, never a race, never `Promise.race`. Reads `this.chunkStream()`, never
   * `this._chunks` directly, so a genuinely synchronous chain (`_syncChunks`, not `_chunks`) still
   * widens correctly - `chunkStream()` is the one seam that resolves either engine to an
   * `AsyncIterable<T[]>`, the same seam `.reduce()`'s own async arm reads.
   *
   * Python equivalent:
   * ```python
   * def queue(self, capacity: int) -> "Pipeline[T]":
   *   return Pipeline(prefetch(self.chunk_stream(), capacity))
   * ```
   *
   * @example
   * `new Pipeline<number>().buffer(1).queue(3)([1, 2, 3, 4, 5]).toArray()` → `Promise<number[]>`
   * resolving to `[1, 2, 3, 4, 5]` - every item synchronous, the chain still widened to async.
   */
  queue(capacity: number): Pipeline<T, "async", In> {
    // Validated once, above `isDeferred()` (unlike `.buffer()`'s two-site check): `.queue()` has no
    // reduceFn-style lazy construction to also guard, so `new Pipeline<number>().queue(0)` - never
    // bound to a source - throws HERE rather than waiting for a drain that never happens.
    assertWholeNumberAtLeastOne("queue capacity", capacity);

    if (this.isDeferred()) {
      return this.defer<T, Pipeline<T, "async", In>>((p) => p.queue(capacity));
    }

    return this.createPipeline<T, Pipeline<T, "async", In>>(
      prefetch<T>(this.chunkStream(), capacity),
      {
        ...this.carriedOptions(),
        ...this.freshPreBuffer(),
        mode: "async",
        syncChunks: null,
      },
    );
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
  ): Pipeline<U, "async", In>;
  reduce<U>(
    fn: (acc: U, item: T, ctx: IContextManager, emit: (value: U) => void) => U,
    initial: U,
  ): Pipeline<U, JoinMode<M, "sync">, In>;
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
      ...this.freshPreBuffer(),
    };

    // `foldSyncChunkStream` folds the same reducer over the sync chunk stream, deferring only at the
    // first thenable a chunk or the reducer itself produces. `foldChunkStream` is that fold over an
    // `AsyncIterable`, which is the only reason the second arm is always `"async"`.
    if (this.isSync()) {
      return this.createPipeline<U>(emptyChunks<U>(), {
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
    build: (p: Pipeline<T, M, any>) => Pipeline<U, M2, any>,
  ): Pipeline<U, JoinMode<M, M2>, In> {
    // A region defers whole (#90) - see `apply()`. Deferring the `.local()` CALL rather than its
    // built region is what keeps `build`'s own stages recorded against the region's real pipeline,
    // which is where they pin themselves in the orchestrating process.
    if (this.isDeferred()) {
      return this.defer<U, Pipeline<U, JoinMode<M, M2>, In>>((p) =>
        (p as unknown as Pipeline<T, M, In>).local(build),
      );
    }
    // Both option objects SPREAD their source pipeline's own carried knobs (#90) - see `reduce()`
    // for why: a hand-built list here dropped `bound`, so the region deferred instead of running
    // and `.local((p) => p.reduce(sum, 0))` over `[1..6]` returned the six items rather than `[21]`.
    const region = new Pipeline<T, "sync" | "async">({
      ...this.carriedOptions(),
      chunks: this._chunks,
      pendingStages: [],
    });
    // `build`'s own declared parameter type is `Pipeline<T, M, any>` - cast straight to it rather
    // than through the stale `"shape"` literal a deleted `SourcePolicy` type parameter left behind
    // (#133): a `SourcePolicy` value never belonged in `In`'s own position, and nothing here reads
    // it as one.
    const built = build(region as unknown as Pipeline<T, M, any>);
    return this.createPipeline<U>(built._chunks, {
      ...built.carriedOptions(),
    }) as Pipeline<U, JoinMode<M, M2>, In>;
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
  ): M extends "async" ? this : Pipeline<T, "async", In>;
  tap(fn: (item: T, ctx: IContextManager) => unknown): this;
  tap(
    transformer: Transformer<T, unknown, "async">,
  ): M extends "async" ? this : Pipeline<T, "async", In>;
  tap(transformer: Transformer<T, unknown, "sync">): this;
  tap(
    arg: PipelineFunction<T, unknown> | Transformer<T, unknown, "sync" | "async">,
  ): this | Pipeline<T, "async", In> {
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
      // `unknown`, not the stale `"shape"` literal a deleted `SourcePolicy` type parameter left in
      // `In`'s own position (#133) - a region's own `In` is never read inside `.local()`'s builder,
      // so any placeholder does, and `unknown` names that honestly rather than borrowing a runtime
      // value's own literal type.
      const sourced = p as Pipeline<T, "sync" | "async", unknown> as Pipeline<T, "sync", unknown>;
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
          )) as unknown as Pipeline<T, "sync", unknown>;
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
  ): { stageIndex: number } & StageRegistries {
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
  drainable(input: PipelineSource<In>): Drainable<T> {
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
   * Routes items into named arms, each with its own pipeline of THIS class (#90).
   *
   * A stage, not a terminal: matching and joining always run here, on the orchestrator, while an
   * arm's own stages dispatch wherever this class's do - `/branch/<i>/<name>/transform/<n>` on
   * `HttpPipeline`/`ClusterPipeline`, and in this process under `.local()`. The demux has no route
   * of its own, so a predicate may close over a local variable.
   *
   * `build` configures a `BranchBuilder`: `.when(name, predicate, build?)` per arm, `.otherwise(
   * name, build?)` for the catch-all, which is routed last whatever order it was written in, and
   * `.broadcast()` to send an item to EVERY matching arm rather than only the first.
   *
   * @param build - Receives a fresh `BranchBuilder` and returns it with its arms declared.
   * @returns A RUNNER, not the results - call it with one input per run. It produces ONE record
   *   keyed by arm name, each key typed by its own arm, and widens to a single `Promise` the moment
   *   one arm is asynchronous; every arm synchronous creates no `Promise` at all.
   *
   * `new Pipeline<number>().branch((b) => b.when("evens", (x) => x % 2 === 0).otherwise("odds"))([
   * 1, 2, 3, 4])` → `{ evens: [2, 4], odds: [1, 3] }`.
   */
  branch<B extends BranchBuilder<T, any, any>>(
    build: (builder: BranchBuilder<T>) => B,
  ): BranchRunner<In, ResultsOf<B>, JoinMode<M, ModeOfArms<B>>> {
    const builder = build(new BranchBuilder<T>());
    const arms = builder.arms();
    // The branch's own index in the shared stage space. Both sides walk the same entry module in
    // the same order, so an orchestrator and a worker agree on it without exchanging anything.
    const branchIndex = this._branchStages.size;
    this._branchStages.set(branchIndex, arms as BranchArm<unknown>[]);

    // The run loop lives in `branch.ts`, beside the builder and the arms it collects. `makeArm` is
    // passed rather than reached for: `emptyOfOwnClass` is protected and only in scope here, which
    // is what keeps `branch.ts`'s edge to this file type-only.
    return runBranch<T, In>({
      owner: this as unknown as BranchOwner<T, In>,
      arms,
      broadcast: builder.isBroadcast(),
      branchIndex,
      makeArm: (context, routeTrail) =>
        this.emptyOfOwnClass<T>(context, routeTrail) as unknown as ArmPipeline<T>,
    }) as BranchRunner<In, ResultsOf<B>, JoinMode<M, ModeOfArms<B>>>;
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
    return this.createPipeline<U>(emptyChunks<U>(), {
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
