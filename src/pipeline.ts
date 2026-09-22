/**
 * `Pipeline` - a chain over an input type, holding no data.
 *
 * You compose a chain once and run it by calling it with an input. The call returns a
 * `PipelineResult` (`./result.ts`), which holds every terminal. The class you construct decides
 * where stages run (`./pipelines/`).
 *
 * `new Pipeline<number>().transform((t) => t.map((x) => x * 2))([1, 2, 3]).toArray()` → `[2, 4, 6]`.
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
import type { MaybeAsyncChunks } from "./utils/drain";
import {
  asAsyncChunks,
  assertWholeNumberAtLeastOne,
  buildChunkGenerator,
  buildSyncChunkGenerator,
  flattenChunks,
  prefetch,
} from "./utils/cut";
import { recutChunks, recutSyncChunks } from "./utils/recut";
import {
  cutItemsWith,
  cutSyncItemsWith,
  recutChunksWith,
  recutSyncChunksWith,
} from "./utils/buffer-cut";
import { applyContextValues, chain, runStageChunk } from "./utils/helpers";
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
import { foldChunkStream, foldSyncChunkStream } from "./utils/reduce";

function asyncIterableFrom<U>(gen: () => AsyncGenerator<U>): AsyncIterable<U> {
  return { [Symbol.asyncIterator]: gen };
}

/** ⚠ One shared iterable, not a generator: a spent generator reads empty only once. */
const EMPTY_CHUNKS: AsyncIterable<never[]> = asyncIterableFrom(
  // eslint-disable-next-line @typescript-eslint/require-await
  async function* () {},
);

/** `EMPTY_CHUNKS`, typed for the chunk type a call site needs.
 *
 * `emptyChunks<number>()` → `EMPTY_CHUNKS`, typed `AsyncIterable<number[]>`.
 */
export function emptyChunks<U>(): AsyncIterable<U[]> {
  return EMPTY_CHUNKS as AsyncIterable<U[]>;
}

function isAsyncSource(data: PipelineSource<unknown>): boolean {
  return Symbol.asyncIterator in Object(data);
}

function toAsyncIterable<U>(data: PipelineSource<U>): AsyncIterable<U> {
  if (isAsyncSource(data)) {
    return data as AsyncIterable<U>;
  }
  const syncIterable = data as Iterable<U>;
  // ⚠ Keep `return()` forwarding: without it, an early `.first(n)` skips a source generator's
  // `finally`.
  return {
    [Symbol.asyncIterator]() {
      const iterator = syncIterable[Symbol.iterator]();
      return {
        next(): Promise<IteratorResult<U>> {
          // A sync throw from `.next()` becomes a rejection, as the async iterator protocol requires.
          try {
            return Promise.resolve(iterator.next());
          } catch (error) {
            return Promise.reject(error as Error);
          }
        },
        return(value?: U): Promise<IteratorResult<U>> {
          iterator.return?.(value as U);
          return Promise.resolve({ value: value as U, done: true });
        },
      };
    },
  };
}

/** What a `Pipeline` may be called with: any sync or async iterable of items. */
export type PipelineSource<T> = AsyncIterable<T> | Iterable<T>;

/**
 * A `Pipeline` whose Mode is not tracked. Internal seams return it; the public method that calls
 * them restores the precise Mode.
 */
export type AnyPipeline<U> = Pipeline<U, PipelineMode, any>;

/**
 * An unbound chain a wrapping class can adopt, with its own input type `In`.
 *
 * ⚠ `In` must stay named, not `any`: with `any`, a wrapper infers its input from the last stage's
 * output type and rejects valid input.
 */
export type WrappablePipeline<T, In> = Pipeline<T, PipelineMode, In>;

/** One stage call recorded on an unbound pipeline, replayed against it once an input is bound. */
export type PendingStage = (pipeline: AnyPipeline<any>) => AnyPipeline<any>;

/** The knobs a caller passes when constructing a `Pipeline`. Both choose its context manager. */
export interface PipelineOptions {
  /** A context manager for this process. It takes precedence over `contextFactory`. */
  context?: IContextManager;
  /**
   * Builds a context manager for a process that cannot receive `context`, such as a cluster
   * worker. It runs at most once per process, and only when `context` is absent.
   */
  contextFactory?: () => IContextManager;
}

/** Internal state each copy-on-write call carries to the next instance. */
export interface PipelineState {
  /** An already-cut chunk stream the new instance reads. */
  chunks?: AsyncIterable<unknown[]>;
  /** The raw items a back-to-back `.buffer()` recuts from; `null` once a stage has run. */
  preBufferItems?: AsyncIterable<unknown> | null;
  /** Every stage's chunk transform, by stage index. A serving side looks stages up here. */
  chunkTransforms?: ChunkTransform[];
  /** Every reduce stage, keyed by its index in the same space as `chunkTransforms`. */
  reduceStages?: Map<number, ReduceStage>;
  /** The run handler from `.onError()`. Only stages applied after it see it. */
  runHandler?: PipelineErrorHandler;
  /** Which engine the terminals read at runtime. */
  mode?: PipelineMode;
  /** The chunk stream of a `"sync"` chain; `null` on an `"async"` one, which uses `chunks`. */
  syncChunks?: MaybeAsyncChunks<unknown> | null;
  /** `preBufferItems` for a `"sync"` chain. */
  syncPreBufferItems?: Iterable<unknown> | null;
  /** The chunk size that cuts the input. A whole number of at least 1. */
  chunkSize?: number;
  /** Every stage composed before an input was bound, in order. */
  pendingStages?: PendingStage[];
  /** The route prefix a branch arm's stages sit under, `/branch/<i>/<name>`; empty on a chain. */
  routeTrail?: string;
  /** Every `.branch()` stage's arms, keyed by branch index. A serving side resolves trails here. */
  branchStages?: Map<number, BranchArm<unknown>[]>;
  /** Whether the pipeline built `context` itself. A built manager is replaced on every call. */
  contextIsDefault?: boolean;
  /** Whether an input is bound. `mode` is a separate, type-level fact. */
  bound?: boolean;
}

/** What the constructor and `createPipeline()` take: the caller's knobs plus the carried state. */
export type PipelineConstructorOptions = PipelineOptions & PipelineState;

function reduceStagePlaceholder(stageIndex: number): ChunkTransform {
  return () => {
    throw new Error(
      `stage ${stageIndex} is a reduce stage, not a plain per-chunk transform - it cannot serve ` +
        `/transform/${stageIndex}`,
    );
  };
}

/**
 * The call signature of every `Pipeline`. Calling one runs the chain over `input` and returns a
 * `PipelineResult`. A sync input keeps a sync chain sync; an async input makes the result async.
 */
export interface Pipeline<T, M extends PipelineMode = "unset", In = T> {
  (input: AsyncIterable<In>): PipelineResult<T, "async">;
  (input: Iterable<In>): PipelineResult<T, M extends "async" ? "async" : "sync">;
}

/**
 * A chain over input type `In`, holding no data. Stage methods return a new pipeline; calling one
 * runs it over an input and returns a `PipelineResult`.
 *
 * All stages share one context manager. A run's `ctx.set()` reaches the caller only through a
 * manager passed as `options.context`.
 *
 * `new Pipeline<number>().transform((t) => t.map((x) => x * 2))([1, 2, 3]).toArray()` → `[2, 4, 6]`.
 */
export class Pipeline<T, M extends PipelineMode = "unset", In = T> {
  protected _chunks!: AsyncIterable<T[]>;
  protected _preBufferItems!: AsyncIterable<T> | null;
  protected _context!: IContextManager;
  protected _chunkTransforms!: ChunkTransform[];
  protected _reduceStages!: Map<number, ReduceStage>;
  protected _runHandler?: PipelineErrorHandler;
  protected _mode!: PipelineMode;
  protected _syncChunks!: MaybeAsyncChunks<T> | null;
  protected _syncPreBufferItems!: Iterable<T> | null;
  protected _chunkSize!: number;
  protected _pendingStages!: PendingStage[];
  protected _contextIsDefault!: boolean;
  protected _routeTrail!: string;
  protected _branchStages!: Map<number, BranchArm<unknown>[]>;
  /** `registriesFor()`'s memo, by trail. Never carried through copy-on-write. */
  protected _armRegistries?: Map<string, StageRegistries>;
  protected _bound!: boolean;
  /** `registries()`'s memo. Never carried through copy-on-write. */
  protected _registries?: StageRegistries;

  /**
   * Builds an empty chain whose input type is `T`.
   *
   * @param options - The caller's context manager or factory. The `PipelineState` fields are for
   *   internal copy-on-write calls; a caller reaches them through `.buffer()`, `.context()` and
   *   `.onError()`.
   */
  constructor(options?: PipelineConstructorOptions) {
    // Returning a function makes the instance callable. `setPrototypeOf` gives it the class's
    // methods and makes `this.constructor` the real subclass.
    //
    // ⚠ `.apply` is a stage method and `.bind` binds an input, so both shadow `Function.prototype`'s.
    // Only `.call` is safe; wrap as `(input) => pipeline(input)` for a caller that uses the others.
    const self = ((input: PipelineSource<unknown>) => {
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

    self._context = options?.context ?? options?.contextFactory?.() ?? new SimpleContextManager();
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
    // ⚠ Validate here: a caller can pass `chunkSize` directly, and an unchecked `2.5` cuts an array
    // and a `Set` into different chunks.
    if (options?.chunkSize !== undefined) {
      assertWholeNumberAtLeastOne("chunkSize", options.chunkSize);
    }
    self._chunkSize = options?.chunkSize ?? DEFAULT_CHUNK_SIZE;
    self._pendingStages = options?.pendingStages ?? [];
    self._bound = options?.bound ?? false;
    self._routeTrail = options?.routeTrail ?? "";
    self._branchStages = options?.branchStages ?? new Map();
    return self;
  }

  /**
   * Binds an input to this chain and returns the bound pipeline that drains it. An `Iterable` runs
   * on the sync engine and an `AsyncIterable` on the async one, unless `sourcePolicy()` forces async.
   */
  protected bind<U>(data: AsyncIterable<U>): Pipeline<U, "async", In>;
  protected bind<U>(data: Iterable<U>): Pipeline<U, M extends "async" ? "async" : "sync">;
  protected bind<U>(data: PipelineSource<U>): Pipeline<U, "sync" | "async"> {
    return this.fromSource<U>(data, this.sourcePolicy()) as Pipeline<U, "sync" | "async">;
  }

  /** Cuts `data` into chunks and replays the recorded stages over them. `policy` is the class's
   * `sourcePolicy()`. */
  protected fromSource<U>(data: PipelineSource<U>, policy: SourcePolicy): AnyPipeline<U> {
    const mode: "sync" | "async" =
      isAsyncSource(data) || policy === "async" || this._mode === "async" ? "async" : "sync";
    const boundOptions: PipelineConstructorOptions = {
      ...this.carriedOptions(),
      mode,
      pendingStages: [],
      context: this.contextForRun(),
      bound: true,
    };

    if (mode === "sync") {
      const items = data as Iterable<U>;
      return this.replayPending(
        this.createPipeline<U>(emptyChunks<U>(), {
          ...boundOptions,
          syncChunks: buildSyncChunkGenerator<U>(this._chunkSize)(items),
          syncPreBufferItems: items,
          preBufferItems: null,
        }),
      );
    }

    const items = toAsyncIterable(data);
    // An array forced onto the async engine is cut synchronously, so it pays no per-item promise.
    const chunks = Array.isArray(data)
      ? asAsyncChunks<U>(buildSyncChunkGenerator<U>(this._chunkSize)(data as Iterable<U>))
      : buildChunkGenerator<U>(this._chunkSize)(items);
    return this.replayPending(
      this.createPipeline<U>(chunks, {
        ...boundOptions,
        preBufferItems: items,
        syncChunks: null,
        // A sync input forced async keeps its sync view, so `.buffer()` can recut it synchronously.
        syncPreBufferItems: isAsyncSource(data) ? null : (data as Iterable<U>),
      }),
    );
  }

  private replayPending<U>(bound: AnyPipeline<U>): AnyPipeline<U> {
    let current: AnyPipeline<any> = bound;
    for (const stage of this._pendingStages) current = stage(current);
    return current as AnyPipeline<U>;
  }

  /** Which engine a bound input runs on: `"shape"` follows the input, `"async"` forces async. A
   * dispatching class overrides it to `"async"`. */
  protected sourcePolicy(): SourcePolicy {
    return "shape";
  }

  /**
   * Builds the next instance of this pipeline's own class over an already-cut `chunks` stream. A
   * subclass with extra constructor knobs overrides `carriedKnobs()`, not this. `R` is the return
   * type the caller wants back.
   *
   * @example
   * `class Sub extends Pipeline<number> {}`: `new Sub().transform((t) => t.map((x) => x + 1))
   * .constructor.name` → `"Sub"`.
   */
  protected createPipeline<U, R = AnyPipeline<U>>(
    chunks: AsyncIterable<U[]>,
    options: PipelineConstructorOptions,
  ): R {
    const Ctor = this.constructor as new (options?: PipelineConstructorOptions) => AnyPipeline<U>;
    return new Ctor({ ...options, ...this.carriedKnobs(), chunks }) as unknown as R;
  }

  /** A subclass's extra constructor knobs, which `createPipeline()` carries to every new instance.
   * An override returns `{ ...super.carriedKnobs(), <own fields> }`. */
  protected carriedKnobs(): object {
    // oxlint-disable-next-line anti-slop/no-known-value-widening -- object is the widest safe common supertype every subclass override's own named-field interface satisfies; Record<string, unknown> refuses those overrides under tsc
    return {};
  }

  /** Every knob a copy-on-write call carries into the next instance, except `chunks`. Spread it
   * whole; a hand-built list drops knobs silently. */
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
      // ⚠ A copy: `.branch()` writes into this map, and a shared one leaks arms into siblings.
      branchStages: new Map(this._branchStages),
    };
  }

  /** The options a real stage passes once it has consumed the chunk stream, so a later `.buffer()`
   * recuts that stage's output. */
  protected freshPreBuffer(): Pick<
    PipelineConstructorOptions,
    "preBufferItems" | "syncPreBufferItems"
  > {
    return { preBufferItems: null, syncPreBufferItems: null };
  }

  /** The context manager one run gets. A caller-named manager is kept across calls; a default one
   * is replaced per run, so two calls never see each other's writes. */
  protected contextForRun(): IContextManager {
    if (!this._contextIsDefault || !this.isDeferred()) return this._context;
    // ⚠ Seed from the chain's values; an empty manager drops what `.context()` declared.
    return new SimpleContextManager(this._context.toDict());
  }

  /** Records a stage call on an unbound pipeline, to replay once an input is bound. `extra`
   * overrides carried state; `R` is the return type the caller wants back. */
  protected defer<U, R = AnyPipeline<U>>(run: PendingStage, extra?: PipelineState): R {
    return this.createPipeline<U>(emptyChunks<U>(), {
      ...this.carriedOptions(),
      ...extra,
      pendingStages: [...this._pendingStages, run],
    }) as unknown as R;
  }

  /** Whether stage calls are recorded rather than run: true until an input is bound. */
  protected isDeferred(): boolean {
    return !this._bound;
  }

  /**
   * The stage registries of the branch arm that `trail` (`/branch/<i>/<name>`) addresses, or `null`
   * when no such arm exists.
   *
   * `registriesFor("/branch/0/big")` → the stage table of the `big` arm of the first `.branch()`.
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

  /**
   * The stage registries a serving side reads to answer `/transform/<n>` and `/reduce/<n>`. An
   * unbound chain replays its stages over an empty input once to fill them.
   */
  protected registries(): StageRegistries {
    if (!this.isDeferred()) {
      return { chunkTransforms: this._chunkTransforms, reduceStages: this._reduceStages };
    }
    if (this._registries === undefined) {
      const materialised = this.bind([] as T[]) as unknown as AnyPipeline<T>;
      this._registries = {
        chunkTransforms: materialised._chunkTransforms,
        reduceStages: materialised._reduceStages,
      };
    }
    return this._registries;
  }

  /**
   * The options that reproduce an unbound `pipeline`'s chain on another class. It throws for a
   * bound pipeline, which has no recorded stages left to replay.
   *
   * @example
   * `new HttpPipeline(scored, { url })` runs `scored`'s stages over HTTP.
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
   * Resolves a wrapping class's two constructor forms into its `super()` options:
   * `(pipeline, options)` adopts a chain, `(options)` builds an empty one.
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

  /** This pipeline's chunks as an async stream, whichever engine it runs on. */
  protected chunkStream(): AsyncIterable<T[]> {
    if (!this.isSync()) return this._chunks;
    const syncChunks = this._syncChunks!;
    return asyncIterableFrom(() => asAsyncChunks(syncChunks));
  }

  /**
   * `chunks` as real items, for a site that reads them. A class whose stages can reply with encoded
   * chunks overrides it to decode them; every other class reads its chunks as they are.
   *
   * `readableChunks(chunks)` → the same `chunks` iterable, on this class.
   */
  protected readableChunks(chunks: AsyncIterable<T[]>): AsyncIterable<T[]> {
    return chunks;
  }

  /** Whether this pipeline runs on the synchronous engine. */
  protected isSync(): boolean {
    return this._mode === "sync" && this._syncChunks !== null;
  }

  /** The context manager this chain carries. */
  get contextManager(): IContextManager {
    return this._context;
  }

  /**
   * Writes `ctx` into this chain's context manager and returns a new pipeline carrying that same
   * manager. A manager that refuses a key throws here.
   *
   * ⚠ Siblings built off one base share one manager, so the last `.context()` call wins for all of
   * them. Use a separate manager per branch. The write is per key, not transactional.
   *
   * @example
   * `const mine = new SimpleContextManager()`, then `new Pipeline<number>({ context: mine })
   * .context({ multiplier: 10 }).transform((t) => t.map((x, ctx) => x * (ctx!.get("multiplier") as
   * number)))([1, 2]).toArray()` → `[10, 20]`, and `mine.toDict()` → `{ multiplier: 10 }`.
   */
  // oxlint-disable-next-line anti-slop/no-unsafe-dictionary-type -- Context is a generic bag by design, unknown until a caller parses it at its own boundary (see .oxlintrc.json)
  context(ctx: Record<string, unknown>): this {
    applyContextValues(this._context, ctx);
    return this.createPipeline<T>(this._chunks, this.carriedOptions()) as this;
  }

  /**
   * Registers the run handler for chunk failures in stages applied after this call. Returning drops
   * the failing chunk and continues; throwing stops the run with that error.
   *
   * ⚠ An async handler widens the type only. A sync input still returns a plain array, so `.then()`
   * on it is a `TypeError`.
   *
   * @example
   * `new Pipeline<string>().buffer(1).onError(() => {}).transform((t) => t.map(parseStrict))
   * (["1", "x", "3", "4"]).toArray()` → `[1, 3, 4]`.
   */
  onError(
    handler: (error: Error, ctx: IContextManager) => Promise<void>,
  ): M extends "async" ? this : Pipeline<T, "async", In>;
  onError(handler: (error: Error, ctx: IContextManager) => void): this;
  onError(handler: PipelineErrorHandler): this | Pipeline<T, "async", In> {
    // ⚠ Defer like a stage: setting `runHandler` directly would cover stages written before it.
    if (this.isDeferred()) {
      return this.defer<T, this>((p) => p.onError(handler));
    }
    return this.createPipeline<T>(this._chunks, {
      ...this.carriedOptions(),
      runHandler: handler,
    }) as this;
  }

  /**
   * Adds `transformer` as one stage, run over each chunk.
   *
   * `new Pipeline<number>().apply(new Transformer<number, number>().map((x) => x * 2))([1, 2, 3])
   * .toArray()` → `[2, 4, 6]`.
   */
  apply<U, M2 extends "sync" | "async">(
    transformer: Transformer<T, U, M2>,
  ): Pipeline<U, JoinMode<M, M2>, In> {
    if (this.isDeferred()) {
      return this.defer<U, Pipeline<U, JoinMode<M, M2>, In>>((p) =>
        p.apply(transformer as Transformer<unknown, U, M2>),
      );
    }
    const runnable = transformer.runnable();
    const carried = {
      ...this.carriedOptions(),
      chunkTransforms: [...this._chunkTransforms, runnable as unknown as ChunkTransform],
      ...this.freshPreBuffer(),
    };

    // A `"sync"` chain runs the same `runnable` an `"async"` one does, with no `Promise` unless the
    // transformer returns one.
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
   * Adds one stage, built by `t` from a fresh `Transformer`.
   *
   * `new Pipeline<number>().transform((t) => t.map((x) => x * 2))([1, 2, 3]).toArray()` → `[2, 4, 6]`.
   */
  transform<U, M2 extends "sync" | "async">(
    t: (transformer: Transformer<T, T, SeedMode<M>>) => Transformer<T, U, M2>,
  ): Pipeline<U, JoinMode<M, M2>, In> {
    const transformer = t(new Transformer<T, T, SeedMode<M>>({ transform: (chunk) => chunk }));
    return this.apply(transformer) as unknown as Pipeline<U, JoinMode<M, M2>, In>;
  }

  /**
   * Sets the chunk boundary every later stage sees. `size` cuts by count. `fn` decides per item:
   * return the item to keep it, `DROP` to skip it, and call `emit()` to close the current chunk.
   *
   * Back-to-back `.buffer()` calls collapse to the last one. A `Promise`-returning `fn` makes the
   * chain async.
   *
   * @example
   * `new Pipeline<number>().buffer(2).buffer(3).buffer(4)([1, 2, 3, 4, 5, 6, 7, 8, 9]).chunks()`
   * yields `[1, 2, 3, 4]`, `[5, 6, 7, 8]`, `[9]`.
   *
   * @example
   * `new Pipeline<{ id: number; invalid: boolean }>().buffer((item) => (item.invalid ? DROP : item))
   * ([{ id: 1, invalid: false }, { id: 2, invalid: true }, { id: 3, invalid: false }]).toArray()`
   * → `[{ id: 1, invalid: false }, { id: 3, invalid: false }]`.
   */
  buffer(size: number): this;
  buffer(
    fn: (item: T, ctx: IContextManager, emit: () => void) => Promise<T | typeof DROP>,
  ): Pipeline<T, "async", In>;
  buffer(fn: (item: T, ctx: IContextManager, emit: () => void) => T | typeof DROP): this;
  buffer(sizeOrFn: number | BufferFunction<T>): this | Pipeline<T, "async", In> {
    // ⚠ Refuse a fractional size too: the cutting paths round it differently.
    if (typeof sizeOrFn === "number") {
      assertWholeNumberAtLeastOne("buffer size", sizeOrFn);
    }

    // A numeric `.buffer()` before any stage also sets the size that cuts the input.
    if (this.isDeferred()) {
      if (typeof sizeOrFn === "number") {
        const size = sizeOrFn;
        const cutsTheSource = this._pendingStages.length === 0;
        return this.defer<T, this>((p) => p.buffer(size), cutsTheSource ? { chunkSize: size } : {});
      }
      const fn = sizeOrFn;
      return this.defer<T, this>((p) => p.buffer(fn));
    }

    if (typeof sizeOrFn === "number") {
      const size = sizeOrFn;
      return this.cutBy(
        buildSyncChunkGenerator<T>(size),
        (slots) => recutSyncChunks(slots, size),
        buildChunkGenerator<T>(size),
        (chunks) => recutChunks(chunks, size),
      );
    }
    const fn = sizeOrFn;
    const ctx = this._context;
    return this.cutBy(
      cutSyncItemsWith<T>(fn, ctx),
      (slots) => recutSyncChunksWith(slots, fn, ctx),
      cutItemsWith<T>(fn, ctx),
      recutChunksWith<T>(fn, ctx),
    );
  }

  private cutBy(
    cutSync: (items: Iterable<T>) => MaybeAsyncChunks<T>,
    recut: (slots: MaybeAsyncChunks<T>) => MaybeAsyncChunks<T>,
    cutAsync: (items: AsyncIterable<T>) => AsyncIterable<T[]>,
    recutAsync: (chunks: AsyncIterable<T[]>) => AsyncIterable<T[]>,
  ): this {
    // ⚠ `recut` re-slices slots without flattening: a slot can hold a pending `Promise<T[]>` even
    // on a sync chain.
    if (this.isSync()) {
      const items = this._syncPreBufferItems;
      return this.createPipeline<T>(emptyChunks<T>(), {
        ...this.carriedOptions(),
        syncChunks: items !== null ? cutSync(items) : recut(this._syncChunks!),
      }) as this;
    }

    if (this._syncPreBufferItems !== null) {
      const syncItems = this._syncPreBufferItems;
      return this.createPipeline<T>(asAsyncChunks<T>(cutSync(syncItems)), {
        ...this.carriedOptions(),
        preBufferItems: null,
        syncPreBufferItems: syncItems,
      }) as this;
    }

    if (this._preBufferItems !== null) {
      return this.createPipeline<T>(cutAsync(this._preBufferItems), this.carriedOptions()) as this;
    }
    const chunks = this.readableChunks(this._chunks);
    return this.createPipeline<T>(recutAsync(chunks), {
      ...this.carriedOptions(),
      // Drained only when a back-to-back `.buffer()` replaces this one, never alongside `recutAsync`.
      preBufferItems: flattenChunks(chunks),
    }) as this;
  }

  /**
   * Prefetches up to `capacity` chunks ahead of the consumer, in order. The chain always becomes
   * async.
   *
   * @example
   * `new Pipeline<number>().buffer(1).queue(3)([1, 2, 3, 4, 5]).toArray()` → a `Promise` resolving
   * to `[1, 2, 3, 4, 5]`.
   */
  queue(capacity: number): Pipeline<T, "async", In> {
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
   * Folds every item the chain produces into one or more values, starting from `initial`. `emit()`
   * pushes a value downstream mid-fold. A `Promise`-returning `fn` makes the chain async.
   *
   * ⚠ `initial` is captured once, so every call of a reusable chain shares it. Called twice with
   * `[1, 2, 3]`, a fold that pushes into an array seed returns `[[1,2,3]]`, then `[[1,2,3,1,2,3]]`.
   * Fold into a fresh value instead.
   *
   * `new Pipeline<number>().reduce((acc, x) => acc + x, 0).transform((t) => t.map((n) => n * 10))
   * ([1, 2, 3, 4, 5]).toArray()` → `[150]`.
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
    if (this.isDeferred()) {
      // The cast only picks an overload: a union-typed `fn` matches neither on its own.
      return this.defer<U>((p) =>
        p.reduce(
          fn as (acc: U, item: any, ctx: IContextManager, emit: (v: U) => void) => U,
          initial,
        ),
      );
    }
    const { chunkTransforms, reduceStages } = this.pushReduceStage(fn, initial);
    const carried = {
      ...this.carriedOptions(),
      chunkTransforms,
      reduceStages,
      ...this.freshPreBuffer(),
    };

    if (this.isSync()) {
      return this.createPipeline<U>(emptyChunks<U>(), {
        ...carried,
        syncChunks: foldSyncChunkStream(fn, initial, this._syncChunks!, this._context),
      }) as AnyPipeline<U>;
    }

    return this.createPipeline<U>(
      foldChunkStream(fn, initial, this.chunkStream(), this._context, true),
      {
        ...carried,
        mode: "async",
        syncChunks: null,
      },
    ) as AnyPipeline<U>;
  }

  /**
   * Runs the stages `build` adds in this process, whatever class this pipeline is. Stages after the
   * region run the way this class runs them.
   *
   * @param build - Receives a plain `Pipeline` over this chain's chunks; its result is the region.
   *
   * @example
   * `new ConcurrentPipeline<number>({ maxConcurrency: 2 }).buffer(2).local((p) => p.transform((t) =>
   * t.map((x: number) => x * 2)).reduce((acc: number, x: number) => acc + x, 0))([1, 2, 3, 4, 5])
   * .toArray()` → a `Promise` resolving to `[30]`.
   */
  local<U, M2 extends PipelineMode>(
    build: (p: Pipeline<T, M, any>) => Pipeline<U, M2, any>,
  ): Pipeline<U, JoinMode<M, M2>, In> {
    if (this.isDeferred()) {
      return this.defer<U, Pipeline<U, JoinMode<M, M2>, In>>((p) =>
        (p as unknown as Pipeline<T, M, In>).local(build),
      );
    }
    const region = new Pipeline<T, "sync" | "async">({
      ...this.carriedOptions(),
      chunks: this.readableChunks(this._chunks),
      pendingStages: [],
    });
    const built = build(region as unknown as Pipeline<T, M, any>);
    return this.createPipeline<U>(built._chunks, {
      ...built.carriedOptions(),
    }) as Pipeline<U, JoinMode<M, M2>, In>;
  }

  /**
   * Watches each item, or each chunk through a `Transformer`, without changing it. It always runs
   * in this process, whatever class this pipeline is, so its context writes stay here.
   *
   * @example
   * `new Pipeline<number>().tap((x) => { seen.push(x); }).transform((t) => t.map((x) => x * 2))
   * ([1, 2, 3]).toArray()` → `[2, 4, 6]`, with `seen` → `[1, 2, 3]`.
   */
  tap<R>(
    fn: (item: T, ctx: IContextManager) => Promise<R>,
  ): M extends "async" ? this : Pipeline<T, "async", In>;
  tap(fn: (item: T, ctx: IContextManager) => void): this;
  tap(
    transformer: Transformer<T, unknown, "async">,
  ): M extends "async" ? this : Pipeline<T, "async", In>;
  tap(transformer: Transformer<T, unknown, "sync">): this;
  tap(
    arg: PipelineFunction<T, unknown> | Transformer<T, unknown, "sync" | "async">,
  ): this | Pipeline<T, "async", In> {
    return this.local((p) => {
      const sourced = p as Pipeline<T, "sync" | "async", unknown> as Pipeline<T, "sync", unknown>;
      // `Transformer.tap` accepts a function or a `Transformer` at runtime; the cast only picks an
      // overload. The caller's `tap` overload records the Mode.
      const tapped = arg as Transformer<T, unknown, "sync">;
      return sourced.transform((t) => t.tap(tapped)) as unknown as Pipeline<T, "sync", unknown>;
    }) as unknown as this;
  }

  /** Registers a reduce stage at the next stage index and returns the updated registries. Its
   * `chunkTransforms` slot throws if run as a per-chunk transform. */
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

  /**
   * Binds `input` and returns what a `PipelineResult` drains. That is the sync chunk stream (or
   * `null`), the async chunk stream, and this run's context manager.
   *
   * `materialize: false` hands back the chunks without making them readable as items; `.consume()`
   * passes it because it reads no item.
   */
  drainable(input: PipelineSource<In>, materialize = true): Drainable<T> {
    const bound = this.bind(input as Iterable<In>) as unknown as AnyPipeline<T>;
    return {
      syncChunks: bound.isSync() ? bound._syncChunks : null,
      chunks: () => (materialize ? bound.readableChunks(bound.chunkStream()) : bound.chunkStream()),
      // ⚠ This run's manager, not the chain's: an arm must see the writes this run just made.
      context: bound._context,
    };
  }

  /**
   * Routes items into named arms, each with its own pipeline of this class. Predicates run in this
   * process, so they may read local state; arm stages run where this class runs stages.
   *
   * ⚠ The parent chain drains fully before any arm runs, so an arm reads the parent's final
   * context, not the value at its own item.
   *
   * @param build - Declares the arms on a `BranchBuilder`: `.when()`, `.otherwise()`, `.broadcast()`.
   * @returns A runner. Calling it returns one record keyed by arm name, or a `Promise` of one when
   *   any arm is async.
   *
   * `new Pipeline<number>().branch((b) => b.when("evens", (x) => x % 2 === 0).otherwise("odds"))
   * ([1, 2, 3, 4])` → `{ evens: [2, 4], odds: [1, 3] }`.
   */
  branch<B extends BranchBuilder<T, any, any>>(
    build: (builder: BranchBuilder<T>) => B,
  ): BranchRunner<In, ResultsOf<B>, JoinMode<M, ModeOfArms<B>>> {
    const builder = build(new BranchBuilder<T>());
    const arms = builder.arms();
    const branchIndex = this._branchStages.size;
    this._branchStages.set(branchIndex, arms as BranchArm<unknown>[]);

    // `makeArm` is passed in so `branch.ts` imports only types from this file.
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
   * An empty, unbound pipeline of this class with the given context and route trail, for a
   * `.branch()` arm to build on.
   *
   * @example
   * On an `HttpPipeline`, it returns an `HttpPipeline` with the parent's url and no stages.
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

// Makes every `Pipeline` instance, subclasses included, a real function (`instanceof Function`,
// `.call`). ⚠ Not `class Pipeline extends Function`: its `super()` throws `EvalError` wherever
// code generation from strings is banned (a CSP page, a Cloudflare Worker).
Object.setPrototypeOf(Pipeline.prototype, Function.prototype);
