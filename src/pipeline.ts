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
  BranchDefinition,
  BranchOptions,
  ReduceFunction,
  PipelineFunction,
  PipelineErrorHandler,
  PipelineMode,
  SourcePolicy,
  AssignMode,
  JoinMode,
} from "./types";
import { DEFAULT_CHUNK_SIZE } from "./types";
import { SimpleContextManager } from "./context/simple";
import { Transformer } from "./transformer";
import type { MaybeAsyncChunks } from "./utils/chunk";
import {
  buildChunkGenerator,
  buildSyncChunkGenerator,
  flattenChunks,
  drainSync,
  drainSyncSettled,
  recutSyncChunks,
} from "./utils/chunk";
import { chain, isThenable, dropOrRethrow } from "./utils/helpers";
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
 * The item type a `Pipeline<T>` carries, extracted from `P` itself rather than referenced as
 * `Pipeline<U>[number]` inline. A conditional type distributes only over a NAKED type parameter —
 * `Ps[number] extends Pipeline<infer U> ? U : never` (`Ps` a rest-param array) is an indexed
 * access, not naked, so it compiles but silently evaluates to `never`; extracting it into its own
 * `P extends Pipeline<infer U> ? U : never` and applying that to `Ps[number]` at the call site
 * keeps `P` naked, so it distributes across a union of differently-typed `Pipeline`s correctly.
 */
type ElementOf<Q> = Q extends Pipeline<infer U, infer _M, infer _P> ? U : never;

/** A `Pipeline`'s own Mode, extracted the same way `ElementOf` extracts its item type (#90) - kept
 * as its own alias so the conditional distributes over a UNION of differently-moded pipelines, the
 * `.claude/rules/typescript.md` rule a naked type parameter needs. */
type ModeOf<Q> = Q extends Pipeline<infer _U, infer M2, infer _P> ? M2 : never;

/** The Mode a merge produces: `"sync"` only when EVERY input is sync, `"async"` the moment one is
 * not (#90) - a merged stream is only consumable synchronously if all of its sources are. */
type MergedMode<Ps extends readonly Pipeline<any, "sync" | "async", SourcePolicy>[]> =
  ModeOf<Ps[number]> extends "sync" ? "sync" : "async";

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
export type AnyPipeline<U> = Pipeline<U, "sync" | "async", SourcePolicy>;

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

/** Merges each `source` context's values onto `target`, later sources winning on a shared key -
 * the ONE place the static `Pipeline.merge()` and the instance `.merge()` (#41) both copy source
 * pipelines' contexts onto a receiving manager, so a future change to context-merge semantics has
 * one home, not two independent copies. */
function mergeContextsInto(target: IContextManager, sources: IContextManager[]): void {
  for (const source of sources) {
    for (const [key, value] of Object.entries(source.toDict())) {
      target.set(key, value);
    }
  }
}

/** Concatenates several chunk streams into one, in order - no new chunking decision (#39): each
 * stream's own boundary is kept as-is. The ONE place the static `Pipeline.merge()` and the
 * instance `.merge()` (#41) both build a merged chunk stream. */
async function* concatChunks<U>(streams: AsyncIterable<U[]>[]): AsyncGenerator<U[]> {
  for (const stream of streams) {
    yield* stream;
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
export class Pipeline<T, M extends PipelineMode = "unset", P extends SourcePolicy = "shape"> {
  // Protected (#17), not private: a dispatching subclass's own overridden `createPipeline()`
  // (below) reads these to carry them into the next instance the same way this base
  // implementation does - `private` would put them out of reach from `src/pipelines/`.
  /** The persisted chunk stream every terminal op and async iteration reads (#39) - cut ONCE,
   * either by the constructor's own default or by `.buffer(size)`, and carried unchanged through
   * every later stage until another `.buffer()` call declares a new one. */
  protected _chunks: AsyncIterable<T[]>;
  /** The pre-buffer ITEM view a back-to-back `.buffer()` call recuts from, or `null` once a real
   * stage (`.apply()`) has consumed `_chunks` - see `PipelineOptions.preBufferItems`. */
  protected _preBufferItems: AsyncIterable<T> | null;
  protected _context: IContextManager;
  protected _chunkTransforms: ChunkTransform[];
  /** Every reduce stage registered via `.reduce()`, keyed by its index in the shared stage-index
   * space - see `PipelineOptions.reduceStages`. */
  protected _reduceStages: Map<number, ReduceStage>;
  /** The RUN handler (#78), registered via `.onError()` - see `PipelineOptions.runHandler`. */
  protected _runHandler?: PipelineErrorHandler;
  /** Which engine this pipeline's terminal ops read (#90) - see `PipelineOptions.mode`. */
  protected _mode: PipelineMode;
  /** The sync chunk stream, on a `"sync"`-Mode pipeline - see `PipelineOptions.syncChunks`. */
  protected _syncChunks: MaybeAsyncChunks<T> | null;
  /** `_preBufferItems`' sync counterpart - see `PipelineOptions.syncPreBufferItems`. */
  protected _syncPreBufferItems: Iterable<T> | null;

  /**
   * Create a new Pipeline from a data source.
   *
   * @param data - Sync or async iterable data source
   * @param options - Optional pipeline configuration
   */
  constructor(options?: PipelineOptions) {
    // `contextFactory` runs ONLY when `context` is absent, and only HERE - every copy-on-write
    // call below (`.context()`, `.apply()`, `.buffer()`) always passes an already-resolved
    // `context`, so a `ClusterPipeline` chain's later `.transform()` calls never re-invoke it
    // (#31, Done-when 6: once per process, not once per stage or per request).
    this._context = options?.context ?? options?.contextFactory?.() ?? new SimpleContextManager();
    this._chunkTransforms = options?.chunkTransforms ?? [];
    this._reduceStages = options?.reduceStages ?? new Map();
    this._runHandler = options?.runHandler;
    this._mode = options?.mode ?? "unset";
    this._syncChunks = (options?.syncChunks ?? null) as MaybeAsyncChunks<T> | null;
    this._chunks = (options?.chunks ?? EMPTY_CHUNKS) as AsyncIterable<T[]>;
    this._preBufferItems = (options?.preBufferItems ?? null) as AsyncIterable<T> | null;
    this._syncPreBufferItems = (options?.syncPreBufferItems ?? null) as Iterable<T> | null;
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
  from<U>(data: AsyncIterable<U>): Pipeline<U, "async", P>;
  from<U>(data: Iterable<U>): Pipeline<U, AssignMode<P, "sync">, P>;
  from<U>(data: PipelineSource<U>): Pipeline<U, "sync" | "async", P> {
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
    const mode: "sync" | "async" = isAsyncSource || policy === "async" ? "async" : "sync";

    if (mode === "sync") {
      const items = data as Iterable<U>;
      return this.createPipeline<U>(EMPTY_CHUNKS as AsyncIterable<U[]>, {
        // Every knob set BEFORE `.from()` carries through it - `.onError()` and `.context()` are
        // both callable on a source-less pipeline, and dropping them here made a registered run
        // handler silently never fire.
        ...this.carriedOptions(),
        mode,
        syncChunks: buildSyncChunkGenerator<U>(DEFAULT_CHUNK_SIZE)(items),
        syncPreBufferItems: items,
        preBufferItems: null,
      });
    }

    const items = toAsyncIterable(data);
    return this.createPipeline<U>(buildChunkGenerator<U>(DEFAULT_CHUNK_SIZE)(items), {
      ...this.carriedOptions(),
      mode,
      preBufferItems: items,
      syncChunks: null,
      syncPreBufferItems: null,
    });
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
      runHandler: this._runHandler,
      mode: this._mode,
      syncChunks: this._syncChunks,
    };
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
   * Refuses a stage on a pipeline that has no source yet (#90) - called by every `apply()`,
   * including `ConcurrentPipeline`'s own, which has its own body rather than delegating here.
   *
   * The base's `.transform()` refuses an `"unset"` receiver at COMPILE time, but a dispatching class
   * cannot: its Mode is fixed at `"async"` (that is what makes its narrowing overrides compile), so
   * it has no `"unset"` state for a conditional `this` to test. Without this check a chain built
   * with no source compiles on those three AND silently resolves to `[]`.
   */
  protected requireSource(): void {
    if (this._mode === "unset") {
      throw new Error("no source: call .from(data) before composing a stage");
    }
  }

  /** Whether this pipeline runs on the synchronous engine (#90) - what a merge asks each input
   * before deciding which engine the merged pipeline itself runs on. */
  protected isSync(): boolean {
    return this._mode === "sync" && this._syncChunks !== null;
  }

  /** This pipeline's chunks as a SYNC stream, for a sync merge. A pipeline that is not `"sync"`
   * has none, so this raises rather than inventing one - the `.merge()` signature already makes
   * that combination a compile error, and a runtime that disagreed with it would be worse. */
  protected syncChunkStream(): MaybeAsyncChunks<T> {
    if (this._mode !== "sync" || this._syncChunks === null) {
      throw new Error(
        "cannot merge an async pipeline into a sync one - call .from() on an async source, or merge the other way round",
      );
    }
    return this._syncChunks;
  }

  /** This pipeline's items, as one stream, whichever engine it runs on (#90) - the seam the async
   * terminal ops and `[Symbol.asyncIterator]` read, so neither has to branch on `_mode` itself. */
  protected asyncItems(): AsyncIterable<T> {
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

  /**
   * Async-iterate the pipeline yielding TRANSFORMED CHUNKS - the exact same persisted `_chunks`
   * stream every terminal op reads (#39). Whichever transforms were accumulated via `.apply()`/
   * `.transform()` already ran when `_chunks` was built (each `.apply()` call runs
   * `Transformer.process()` immediately, lazily, over the prior `_chunks`); this loop simply
   * drains that result, so hooks/`.onError()` fire identically here as through `.toArray()` - no
   * separate replay, no knob this path can't honor.
   *
   * Runs whenever the pipeline is consumed with `for await...of` instead of a terminal operation
   * like `.toArray()` — this is also the path a bare `Pipeline` handed to laygo's `m.from()`
   * (`@outputty/laygo`) drains as a source.
   *
   * @example
   * ```typescript
   * const pipeline = new Pipeline(source).transform((t) => t.map((r) => r.id * 2));
   * for await (const chunk of pipeline) {
   *   console.log(chunk); // e.g. [2], [4, 6]
   * }
   * ```
   */
  async *[Symbol.asyncIterator](): AsyncGenerator<T[]> {
    yield* this.chunkStream();
  }

  // ===== Static Factory Methods =====

  /**
   * Merge multiple pipelines into a single pipeline (fan-in pattern), always a FRESH, plain
   * `Pipeline` - for a caller who holds no pipeline of its own to continue. See the instance
   * `.merge(...others)` (#41, below `.context()`) to merge onto a pipeline already held instead,
   * keeping its own class, knobs and stage numbering.
   *
   * All items from all input pipelines are yielded in sequence, each source pipeline's OWN
   * already-cut `_chunks` boundary preserved rather than re-derived - merging never re-chunks
   * (#39). `options.context`, when given, is the SAME instance returned as `.contextManager` on
   * the merged pipeline - mutated in place with every source pipeline's own context values, later
   * pipelines taking precedence on a shared key (#31; one of the TWO seams where values flow
   * BACKWARD across pipelines - the instance `.merge()` is the other - which is why both take or
   * already hold a manager explicitly rather than only ever receiving one at construction). With
   * no `options`, a fresh `SimpleContextManager` is built and populated the same way - today's
   * behaviour, unchanged.
   *
   * Python equivalent:
   * ```python
   * @classmethod
   * def merge(cls, pipelines: list["Pipeline[T]"], *, context: IContextManager | None = None) -> "Pipeline[T]":
   *   if not pipelines:
   *     return cls([])
   *   merged_context = context or SimpleContextManager()
   *   for p in pipelines:
   *     for key, value in p.context_manager.to_dict().items():
   *       merged_context[key] = value
   *   async def merged_chunks():
   *     for pipeline in pipelines:
   *       async for chunk in pipeline.chunks:
   *         yield chunk
   *   return cls([], chunks=merged_chunks(), context=merged_context)
   * ```
   *
   * @param pipelines - Pipelines to merge, as an array - not a rest param (#31, BREAKING): an array
   *   literal keeps `ElementOf<Ps[number]>` distributing over a UNION of differently-typed
   *   pipelines, exactly as the old rest-param form did, while leaving a second parameter free for
   *   `options`.
   * @param options - `{ context }` to carry a caller's own manager through the merge, or
   *   `{ contextFactory }` to build one (review: `options` is typed as the full `PipelineOptions` -
   *   the same type `contextFactory` lives on - so both are honored the same way the constructor
   *   does, `context ?? contextFactory() ?? a fresh SimpleContextManager`; omitted, or `{}`, keeps
   *   today's `SimpleContextManager` behaviour.
   * @returns A new pipeline that yields all items from all input pipelines. `Pipeline.merge([])` →
   *   an empty pipeline.
   *
   * @example
   * `Pipeline.merge([p1, p2], { context: mine })` then `.contextManager === mine` → `true` (#31,
   * Done-when 3) - verified live via `__tests__/fixtures/context-managers.ts`'s own `LoggingContext`.
   */
  static merge<Ps extends readonly Pipeline<any, "sync" | "async", SourcePolicy>[]>(
    pipelines: Ps,
    options?: PipelineOptions,
  ): Pipeline<ElementOf<Ps[number]>, MergedMode<Ps>> {
    type U = ElementOf<Ps[number]>;

    // `options?.context`/`contextFactory` both honored on the zero-pipeline path too (review: the
    // fast return used to drop context entirely, and contextFactory was never read at all - a
    // caller passing either got a fresh SimpleContextManager instead, breaking the same-instance
    // contract every other path here keeps).
    if (pipelines.length === 0) {
      // `ModeOf<never>` resolves `"sync"`, so an empty merge is a sync empty pipeline - not an
      // `"unset"` one whose `.toArray()` would return a `Promise` against a `T[]` annotation.
      return new Pipeline<U, MergedMode<Ps>>({
        context: options?.context ?? options?.contextFactory?.(),
        mode: "sync",
        syncChunks: [],
      });
    }

    // Merge contexts from all pipelines into the caller's own manager when given (#31) - never a
    // fresh copy that would discard it, the same reason .context() (above) mutates in place.
    const mergedContext =
      options?.context ?? options?.contextFactory?.() ?? new SimpleContextManager();
    mergeContextsInto(
      mergedContext,
      pipelines.map((pipeline) => pipeline._context),
    );

    // Concatenates each source pipeline's OWN chunk stream in sequence - no new chunking decision
    // at merge time (#39): a pipeline cut at 2 and one cut at 4 both keep their own boundary.
    // The runtime engine has to match the Mode the return type promises (#90). Typing the result
    // `"sync"` while leaving `_mode` at its `"unset"` default made `.toArray()` hand back a
    // `Promise` where the caller's own annotation said `number[]` - the exact divergence this
    // ticket removes, reintroduced at the one seam that builds a stranger rather than copying self.
    if (pipelines.every((pipeline) => pipeline.isSync())) {
      const streams = pipelines.map(
        (pipeline) => pipeline.syncChunkStream() as MaybeAsyncChunks<U>,
      );
      function* concatSync(): Generator<U[] | Promise<U[]>> {
        for (const stream of streams) yield* stream;
      }
      return new Pipeline<U, MergedMode<Ps>>({
        context: mergedContext,
        mode: "sync",
        syncChunks: concatSync(),
      });
    }

    return new Pipeline<U, MergedMode<Ps>>({
      context: mergedContext,
      mode: "async",
      chunks: concatChunks(
        pipelines.map((pipeline) => pipeline.chunkStream() as AsyncIterable<U[]>),
      ),
    });
  }

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
  onError(handler: (error: Error, ctx: IContextManager) => Promise<void>): Pipeline<T, "async", P>;
  onError(handler: (error: Error, ctx: IContextManager) => void): this;
  onError(handler: PipelineErrorHandler): this | Pipeline<T, "async", P> {
    // An ASYNC handler widens the chain (#90), the same rule `.tap()` follows. `PipelineErrorHandler`
    // declares a bare `void` return, which accepts an `async` function silently, so without the
    // overload above the chain kept its `"sync"` type while `dropOrRethrow` deferred on the handler's
    // own promise - a chain typed `number[]` handed back a pending `Promise` the moment an error
    // actually fired. A handler that never fires makes the widening pessimistic, never wrong: an
    // `await` on the array it still returns is a no-op.
    return this.createPipeline<T>(this._chunks, {
      ...this.carriedOptions(),
      runHandler: handler,
    }) as this;
  }

  /**
   * Concatenate OTHER pipelines' items onto THIS one - the instance-method sibling of the static
   * `Pipeline.merge()`, for a caller who already holds a pipeline to continue rather than a fresh
   * one to build. Copy-on-write through `createPipeline()` (#41), so the result carries THIS
   * pipeline's class, knobs, address AND `_chunkTransforms` - a stage applied after the merge runs
   * WHERE this pipeline runs, at the next index rather than restarting at 0. The static's own
   * `new Pipeline(...)` always builds a plain `Pipeline` and always starts stage numbering over,
   * which collides once a merged dispatching pipeline gains one more stage (#41's own ticket); this
   * method has no such problem, because there is no stranger - it continues an instance that
   * already has its own address and its own stage table.
   *
   * `others`' own chunk transforms are NOT carried forward - each has already run, producing that
   * pipeline's own `_chunks`, which is all this method reads from it. Only `_preBufferItems` resets
   * to `null` (`.apply()`'s own reason: several chunk streams are now concatenated, so there is no
   * single raw item view left for a later `.buffer()` to recut from).
   *
   * Each other pipeline's context merges into THIS one's manager, mutating it in place -
   * `.context()`'s own reason: a caller's own manager class is never silently swapped for a copy -
   * later pipelines winning on a shared key, through the same `mergeContextsInto()` the static
   * `Pipeline.merge()` (above) shares with this method rather than a second copy of the loop.
   *
   * Typed `this` (#17/#31's own reason on `.context()`): `T` never changes here, so `.fetch` and
   * `.local(build)` still typecheck off the result on a dispatching subclass.
   *
   * @param others - Pipelines to concatenate onto this one, in order. None: returns an equivalent
   *   pipeline of the same class with nothing appended.
   *
   * @example
   * The ticket's own planning spike (`#41`'s `## Interface`), reproduced live in
   * `__tests__/merge-instance.e2e.test.ts`: `new HttpPipeline([1,2,3,4], { url })
   * .transform((t) => t.map((x) => x + 1))` (stage 0, dispatched) `.merge(new
   * ConcurrentPipeline([10, 20]).transform((t) => t.map((x) => x + 5)))` (the
   * `ConcurrentPipeline`'s own items, through its own in-process fan-out) `.transform((t) =>
   * t.map((x) => x * 100))` (stage 1, continuing THIS pipeline's own index, never restarting at
   * 0) → `.toArray()` → `[200,300,400,500,1500,2500]`.
   */
  merge(
    ...others: Pipeline<T, M extends "async" ? "sync" | "async" : "sync", SourcePolicy>[]
  ): this;
  merge(...others: Pipeline<T, "sync" | "async", SourcePolicy>[]): Pipeline<T, "async", P>;
  merge(...others: Pipeline<T, "sync" | "async", SourcePolicy>[]): this | Pipeline<T, "async", P> {
    mergeContextsInto(
      this._context,
      others.map((other) => other._context),
    );

    // Every pipeline is read through `chunkStream()`, never `_chunks` directly: a `"sync"` one
    // carries its chunks in `_syncChunks` and leaves `_chunks` empty, so reading the field would
    // silently merge nothing (#90).
    //
    // ONE async pipeline anywhere in `others` sends the whole merge down the async arm (#90), which
    // is what makes merging widen rather than refuse - the same rule every other stage follows, and
    // the reason the second overload above exists. `chunkStream()` converts a sync pipeline's own
    // chunks on the way in, so that arm already handles the mixed case unchanged.
    if (this._mode === "sync" && this._syncChunks !== null && others.every((o) => o.isSync())) {
      const streams = [this._syncChunks, ...others.map((other) => other.syncChunkStream())];
      function* concatSync(): Generator<T[] | Promise<T[]>> {
        for (const stream of streams) yield* stream;
      }
      return this.createPipeline<T>(EMPTY_CHUNKS as AsyncIterable<T[]>, {
        ...this.carriedOptions(),
        syncChunks: concatSync(),
        preBufferItems: null,
        syncPreBufferItems: null,
      }) as this;
    }

    return this.createPipeline<T>(
      concatChunks([this.chunkStream(), ...others.map((other) => other.chunkStream())]),
      {
        ...this.carriedOptions(),
        // Several chunk streams are concatenated now, so there is no single raw item view left for
        // a later `.buffer()` to recut from, and the merged stream is async whatever the inputs
        // were - the signature already refuses merging async INTO sync.
        preBufferItems: null,
        syncPreBufferItems: null,
        mode: "async",
        syncChunks: null,
      },
    ) as this;
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
  ): Pipeline<U, AssignMode<P, JoinMode<M, M2>>, P> {
    this.requireSource();
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
      }) as Pipeline<U, AssignMode<P, JoinMode<M, M2>>, P>;
    }

    return this.createPipeline<U>(
      transformer.process(this._chunks, this._context, this._runHandler),
      {
        ...carried,
        syncChunks: null,
      },
    ) as Pipeline<U, AssignMode<P, JoinMode<M, M2>>, P>;
  }

  /**
   * Apply a transformer builder function.
   *
   * Python equivalent:
   * ```python
   * def transform(self, t: Callable[[Transformer[T, T]], Transformer[T, U]]) -> "Pipeline[U]":
   *   transformer = t(Transformer[T, T]())
   *   return self.apply(transformer)
   * ```
   */
  transform<U, M2 extends "sync" | "async">(
    this: M extends "unset" ? never : Pipeline<T, M, P>,
    t: (transformer: Transformer<T, T, M & ("sync" | "async")>) => Transformer<T, U, M2>,
  ): Pipeline<U, AssignMode<P, JoinMode<M, M2>>, P> {
    const transformer = t(
      new Transformer<T, T, M & ("sync" | "async")>({ transform: (chunk) => chunk }),
    );
    return this.apply(transformer) as unknown as Pipeline<U, AssignMode<P, JoinMode<M, M2>>, P>;
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
   * `new Pipeline().from([1,2,3,4,5]).reduce((acc, x) => acc + x, 0).transform((t) => t.map((n) => n *
   * 10)).toArray()` → `[1500]`, a `number[]` with no `await`.
   */
  reduce<U>(
    fn: (acc: U, item: T, ctx: IContextManager, emit: (value: U) => void) => Promise<U>,
    initial: U,
  ): Pipeline<U, "async", P>;
  reduce<U>(
    fn: (acc: U, item: T, ctx: IContextManager, emit: (value: U) => void) => U,
    initial: U,
  ): Pipeline<U, AssignMode<P, JoinMode<M, "sync">>, P>;
  reduce<U>(fn: ReduceFunction<U, T>, initial: U): AnyPipeline<U> {
    const { chunkTransforms, reduceStages } = this.pushReduceStage(fn, initial);
    const carried = {
      context: this._context,
      chunkTransforms,
      reduceStages,
      preBufferItems: null,
      syncPreBufferItems: null,
      runHandler: this._runHandler,
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
  local<U, M2 extends "sync" | "async">(
    build: (p: Pipeline<T, M, "shape">) => Pipeline<U, M2, "shape">,
  ): Pipeline<U, AssignMode<P, JoinMode<M, M2>>, P> {
    const region = new Pipeline<T, "sync" | "async", SourcePolicy>({
      context: this._context,
      chunkTransforms: this._chunkTransforms,
      reduceStages: this._reduceStages,
      chunks: this._chunks,
      preBufferItems: this._preBufferItems,
      syncPreBufferItems: this._syncPreBufferItems,
      runHandler: this._runHandler,
      mode: this._mode,
      syncChunks: this._syncChunks,
    });
    const built = build(region as unknown as Pipeline<T, M, "shape">);
    return this.createPipeline<U>(built._chunks, {
      context: built._context,
      chunkTransforms: built._chunkTransforms,
      reduceStages: built._reduceStages,
      preBufferItems: built._preBufferItems,
      syncPreBufferItems: built._syncPreBufferItems,
      runHandler: built._runHandler,
      mode: built._mode,
      syncChunks: built._syncChunks,
    }) as Pipeline<U, AssignMode<P, JoinMode<M, M2>>, P>;
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
  tap(fn: (item: T, ctx: IContextManager) => Promise<unknown>): Pipeline<T, "async", P>;
  tap(fn: (item: T, ctx: IContextManager) => unknown): this;
  tap(transformer: Transformer<T, unknown, "async">): Pipeline<T, "async", P>;
  tap(transformer: Transformer<T, unknown, "sync">): this;
  tap(
    arg: PipelineFunction<T, unknown> | Transformer<T, unknown, "sync" | "async">,
  ): this | Pipeline<T, "async", P> {
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
    }) as this;
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
   * Collect all results to an array. Read context via `.contextManager` afterward if needed - a
   * terminal op's return no longer carries a context snapshot (#744).
   *
   * Python equivalent:
   * ```python
   * def to_list(self) -> list[T]:
   *   return list(self.processed_data)
   * ```
   */
  toArray(): M extends "sync" ? T[] : Promise<T[]> {
    const results: T[] = [];
    if (this._mode === "sync" && this._syncChunks !== null) {
      return chain(
        drainSync(this._syncChunks, (item) => void results.push(item)),
        () => results,
      ) as M extends "sync" ? T[] : Promise<T[]>;
    }
    return this.collectAsync(results, undefined) as M extends "sync" ? T[] : Promise<T[]>;
  }

  /** The async engine's own collect loop, shared by `toArray` and `first` (#90) - `limit` is
   * `first`'s early exit, `undefined` for the whole stream. */
  private async collectAsync(results: T[], limit: number | undefined): Promise<T[]> {
    for await (const item of this.asyncItems()) {
      results.push(item);
      if (limit !== undefined && results.length >= limit) break;
    }
    return results;
  }

  /**
   * Get the first N elements. Read context via `.contextManager` afterward if needed (#744).
   *
   * Python equivalent:
   * ```python
   * def first(self, n: int = 1) -> list[T]:
   *   assert n >= 1, "n must be at least 1"
   *   return list(itertools.islice(self.processed_data, n))
   * ```
   */
  first(n = 1): M extends "sync" ? T[] : Promise<T[]> {
    if (n < 1) {
      throw new Error("n must be at least 1");
    }

    const results: T[] = [];
    if (this._mode === "sync" && this._syncChunks !== null) {
      return chain(
        drainSync(this._syncChunks, (item) => {
          results.push(item);
          return results.length >= n;
        }),
        () => results,
      ) as M extends "sync" ? T[] : Promise<T[]>;
    }
    return this.collectAsync(results, n) as M extends "sync" ? T[] : Promise<T[]>;
  }

  /**
   * Consume all items without collecting them. Read context via `.contextManager` afterward if
   * needed (#744).
   *
   * Python equivalent:
   * ```python
   * def consume(self) -> None:
   *   for _ in self.processed_data:
   *     pass
   * ```
   */
  consume(): M extends "sync" ? void : Promise<void> {
    if (this._mode === "sync" && this._syncChunks !== null) {
      return drainSync(this._syncChunks, () => {}) as M extends "sync" ? void : Promise<void>;
    }
    return this.consumeAsync() as M extends "sync" ? void : Promise<void>;
  }

  /** `consume`'s async arm, split out so the method above stays a single expression per engine. */
  private async consumeAsync(): Promise<void> {
    for await (const _ of this.asyncItems()) {
      // Just consume, don't collect
    }
  }

  /**
   * Apply a side-effect function to each item. Read context via `.contextManager` afterward if
   * needed (#744).
   *
   * Python equivalent:
   * ```python
   * def each(self, function: PipelineFunction[T]) -> None:
   *   for item in self.processed_data:
   *     function(item)
   * ```
   */
  forEach(fn: (item: T) => Promise<void>): Promise<void>;
  forEach(fn: (item: T) => void): M extends "sync" ? void : Promise<void>;
  forEach(fn: (item: T) => void | Promise<void>): void | Promise<void> {
    if (this.isSync() && this._syncChunks !== null) {
      // Each callback's own return is settled before the next item, so a `forEach` that turns out
      // to be async still runs strictly in order and still reports its own failures. The overload
      // above is why the ASYNC arm is declared first: TypeScript's void-return rule makes an
      // `async` callback assignable to `(item: T) => void`, so a `void`-returning arm listed first
      // would swallow it, type the call `void`, and leave every callback fired and dropped.
      return drainSyncSettled(this._syncChunks, fn);
    }
    return this.forEachAsync(fn);
  }

  /** `forEach`'s async arm, which awaits each callback in turn. */
  private async forEachAsync(fn: (item: T) => void | Promise<void>): Promise<void> {
    for await (const item of this.asyncItems()) {
      await fn(item);
    }
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
   * @returns Results by branch name. Read context via `.contextManager` afterward if needed (#744).
   */
  async branch<U>(
    // Either Mode (#90). `Transformer`'s Mode parameter DEFAULTS to `"sync"`, so the pre-#90
    // spelling `Transformer<T, U>` would have silently narrowed this to sync-only transformers and
    // rejected `new Transformer<T, U>().map(async (x) => …)`, which compiled before. `.branch()`
    // awaits every branch's own result regardless, so accepting both is the behaviour it always had.
    branches: Record<string, BranchDefinition<T, U, Transformer<T, U, "sync" | "async">>>,
    options?: BranchOptions,
  ): Promise<Record<string, U[]>> {
    const firstMatch = options?.firstMatch !== false; // Default to true (router mode)

    const results: Record<string, U[]> = {};
    for (const key of Object.keys(branches)) {
      results[key] = [];
    }

    for await (const item of this.asyncItems()) {
      await this.routeItemToBranches(item, branches, results, firstMatch);
    }

    return results;
  }

  /**
   * Route a single item to every matching branch (or just the first, under
   * router mode), pushing each branch's transformed output into `results`.
   *
   * Runs once per item from within `branch()`'s consumption loop.
   *
   * @example
   * `routeItemToBranches(4, { even: { predicate: (n) => n % 2 === 0,
   * transformer } }, results, true)` pushes `transformer`'s output for `4`
   * onto `results.even`.
   */
  private async routeItemToBranches<U>(
    item: T,
    branches: Record<string, BranchDefinition<T, U, Transformer<T, U, "sync" | "async">>>,
    results: Record<string, U[]>,
    firstMatch: boolean,
  ): Promise<void> {
    for (const [key, { predicate, transformer }] of Object.entries(branches)) {
      const matches = await predicate(item);
      if (!matches) continue;

      await this.pushBranchOutput(item, transformer, results, key);

      // In router mode, stop after first match; in broadcast mode, continue
      if (firstMatch) {
        break;
      }
    }
  }

  /**
   * Run a single item through a branch's transformer and collect its output.
   *
   * Runs once per matched (item, branch) pair from `routeItemToBranches`. The item is its own
   * one-item CHUNK (#39: `Transformer.process()` takes chunks, not items) - no default-chunking
   * question here, since a branch always sends exactly one item at a time.
   *
   * @example
   * `pushBranchOutput(4, doubler, { even: [] }, "even")` mutates
   * `results.even` to `[8]`.
   */
  private async pushBranchOutput<U>(
    item: T,
    transformer: Transformer<T, U, "sync" | "async">,
    results: Record<string, U[]>,
    key: string,
  ): Promise<void> {
    async function* oneItem() {
      yield item;
    }
    const singleItemChunk = buildChunkGenerator<T>(1)(oneItem());

    for await (const chunk of transformer.process(singleItemChunk, this._context)) {
      results[key].push(...chunk);
    }
  }
}
