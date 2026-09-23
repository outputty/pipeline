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
  InternalTransformer,
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
import { applyContextValues, isThenable, runStageChunk } from "./utils/helpers";
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

/** A stage call as a function over a pipeline. Kept for `PipelineState.pendingStages`, which the
 * constructor accepts and ignores. */
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

/**
 * Internal state each copy-on-write call carries to the next instance. The constructor reads
 * `chunkSize`, `runHandler`, `mode`, `routeTrail`, `branchStages`, `contextIsDefault` and `bound`;
 * it accepts the other fields and ignores them.
 */
export interface PipelineState {
  /** Ignored. */
  chunks?: AsyncIterable<unknown[]>;
  /** Ignored. */
  preBufferItems?: AsyncIterable<unknown> | null;
  /** Ignored; the stage tables come from the recorded stages. */
  chunkTransforms?: ChunkTransform[];
  /** Ignored; the stage tables come from the recorded stages. */
  reduceStages?: Map<number, ReduceStage>;
  /** The run handler every recorded stage starts with, until an `.onError()` replaces it. */
  runHandler?: PipelineErrorHandler;
  /** `"async"` runs every input on the async engine. */
  mode?: PipelineMode;
  /** Ignored. */
  syncChunks?: MaybeAsyncChunks<unknown> | null;
  /** Ignored. */
  syncPreBufferItems?: Iterable<unknown> | null;
  /** The chunk size that cuts the input. A whole number of at least 1. */
  chunkSize?: number;
  /** Ignored; the recorded stages travel with copy-on-write. */
  pendingStages?: PendingStage[];
  /** The route prefix a branch arm's stages sit under, `/branch/<i>/<name>`; empty on a chain. */
  routeTrail?: string;
  /** Every `.branch()` stage's arms, keyed by branch index. A serving side resolves trails here. */
  branchStages?: Map<number, BranchArm<unknown>[]>;
  /** Whether the pipeline built `context` itself. A built manager is replaced on every call. */
  contextIsDefault?: boolean;
  /** Whether this is a `.local()` region's pipeline, which cannot be called or wrapped. */
  bound?: boolean;
}

/** What the constructor and `createPipeline()` take: the caller's knobs plus the carried state. */
export type PipelineConstructorOptions = PipelineOptions & PipelineState;

/** One stage call, as a stage method records it. `origin` marks where a `.local()` region starts. */
export type StageDescriptor =
  | { readonly kind: "apply"; readonly transformer: Transformer<any, any, any> }
  | { readonly kind: "reduce"; readonly fn: ReduceFunction<any, any>; readonly initial: unknown }
  | { readonly kind: "buffer"; readonly size: number }
  | { readonly kind: "bufferFn"; readonly fn: BufferFunction<any> }
  | { readonly kind: "queue"; readonly capacity: number }
  | { readonly kind: "onError"; readonly handler: PipelineErrorHandler }
  | {
      readonly kind: "local";
      readonly build: (p: AnyPipeline<any>) => AnyPipeline<any>;
    }
  | { readonly kind: "origin" };

/** A recorded chain, newest stage first. Copy-on-write shares every earlier node. */
export interface StageNode {
  readonly prev: StageNode | null;
  readonly desc: StageDescriptor;
}

/**
 * One run's streams, which each stage replaces in turn. `syncChunks` is `null` on the async engine,
 * which reads `chunks`. `syncItems`/`asyncItems` are the raw input a `.buffer()` recuts, `null` once
 * a stage has consumed it.
 */
export interface RunFlow {
  ctx: IContextManager;
  syncChunks: MaybeAsyncChunks<unknown> | null;
  chunks: AsyncIterable<unknown[]>;
  syncItems: Iterable<unknown> | null;
  asyncItems: AsyncIterable<unknown> | null;
}

/** What compiling a stage needs from the stages before it: the run handler in force and the next
 * stage index. */
export interface PlanState {
  runHandler: PipelineErrorHandler | undefined;
  index: number;
}

/** A compiled stage: `run` replaces the flow's streams with this stage's output. */
export interface StageOp {
  run(flow: RunFlow): void;
  /** The in-process stages this op runs, which a neighbouring in-process op can fuse with. */
  readonly steps?: readonly InProcessStep[];
}

/** One stage run in this process, with the run handler in force where it was added. */
export interface InProcessStep {
  readonly transformer: Transformer<any, any, any>;
  readonly runnable: InternalTransformer<unknown, unknown>;
  readonly runHandler: PipelineErrorHandler | undefined;
}

/** A chain compiled once: its stages oldest first, and their ops when no `.local()` region makes
 * them depend on the call. */
export interface Plan {
  descs: readonly StageDescriptor[];
  ops: readonly StageOp[] | null;
  cutSync: (items: Iterable<unknown>) => MaybeAsyncChunks<unknown>;
  cutAsync: (items: AsyncIterable<unknown>) => AsyncIterable<unknown[]>;
  forcesAsync: boolean;
}

/** Carries the recorded stages through the constructor without widening its options type. */
const RECORDED: unique symbol = Symbol("recorded");

type RecordedOptions = PipelineConstructorOptions & { [RECORDED]?: StageNode | null };

const ORIGIN: StageDescriptor = { kind: "origin" };

function reduceStagePlaceholder(stageIndex: number): ChunkTransform {
  return () => {
    throw new Error(
      `stage ${stageIndex} is a reduce stage, not a plain per-chunk transform - it cannot serve ` +
        `/transform/${stageIndex}`,
    );
  };
}

/** The flow's chunks as an async stream, whichever engine it runs on. */
function streamOf(flow: RunFlow): AsyncIterable<unknown[]> {
  const syncChunks = flow.syncChunks;
  if (syncChunks === null) return flow.chunks;
  return asyncIterableFrom(() => asAsyncChunks(syncChunks));
}

/** Marks the raw input consumed, so a later `.buffer()` recuts the stage output instead. */
function consumed(flow: RunFlow): void {
  flow.syncItems = null;
  flow.asyncItems = null;
}

function runOps(ops: readonly StageOp[], flow: RunFlow): void {
  for (let i = 0; i < ops.length; i++) ops[i]!.run(flow);
}

/** The stages from `tail` back to (not including) `origin`, oldest first; `null` when `tail` does
 * not descend from `origin`. */
function descriptorsSince(
  tail: StageNode | null,
  origin: StageNode | null,
): StageDescriptor[] | null {
  const descs: StageDescriptor[] = [];
  let node = tail;
  while (node !== origin) {
    if (node === null) return null;
    descs.push(node.desc);
    node = node.prev;
  }
  return descs.reverse();
}

/**
 * Runs every step over each chunk in turn, one chunk at a time: the order a generator per step
 * gives, with one generator for all of them.
 */
function* stageChunks(
  source: MaybeAsyncChunks<unknown>,
  steps: readonly InProcessStep[],
  ctx: IContextManager,
): Generator<unknown[] | Promise<unknown[]>> {
  for (const chunk of source) {
    let slot = chunk;
    for (let i = 0; i < steps.length; i++) slot = runStep(steps[i]!, slot, ctx);
    yield slot;
  }
}

function runStep(
  step: InProcessStep,
  slot: unknown[] | Promise<unknown[]>,
  ctx: IContextManager,
): unknown[] | Promise<unknown[]> {
  if (!isThenable(slot)) return runStageChunk(step.runnable, slot, ctx, step.runHandler);
  return Promise.resolve(slot).then((settled) =>
    runStageChunk(step.runnable, settled, ctx, step.runHandler),
  );
}

/** The op for consecutive in-process stages. A sync run fuses them into one generator; an async
 * run keeps one `Transformer.process()` per stage. */
function inProcessOp(steps: readonly InProcessStep[]): StageOp {
  return {
    steps,
    run(flow) {
      if (flow.syncChunks !== null) {
        flow.syncChunks = stageChunks(flow.syncChunks, steps, flow.ctx);
      } else {
        for (const step of steps) {
          flow.chunks = step.transformer.process(flow.chunks, flow.ctx, step.runHandler);
        }
      }
      consumed(flow);
    },
  };
}

/** `ops` with every run of adjacent in-process ops merged into one. */
function fuseInProcess(ops: readonly StageOp[]): StageOp[] {
  const fused: StageOp[] = [];
  for (const op of ops) {
    const last = fused.at(-1);
    if (op.steps !== undefined && last?.steps !== undefined) {
      fused[fused.length - 1] = inProcessOp([...last.steps, ...op.steps]);
    } else {
      fused.push(op);
    }
  }
  return fused;
}

/** The op for one `.buffer()`: cut the raw input when no stage has consumed it yet, else recut the
 * stage output. `readable` decodes chunks before an async recut reads their items. */
function cutOp(
  cutSync: (items: Iterable<unknown>) => MaybeAsyncChunks<unknown>,
  recut: (slots: MaybeAsyncChunks<unknown>) => MaybeAsyncChunks<unknown>,
  cutAsync: (items: AsyncIterable<unknown>) => AsyncIterable<unknown[]>,
  recutAsync: (chunks: AsyncIterable<unknown[]>) => AsyncIterable<unknown[]>,
  flow: RunFlow,
  readable: (chunks: AsyncIterable<unknown[]>) => AsyncIterable<unknown[]>,
): void {
  // ⚠ `recut` re-slices slots without flattening: a slot can hold a pending `Promise<T[]>` even
  // on a sync chain.
  if (flow.syncChunks !== null) {
    flow.syncChunks = flow.syncItems !== null ? cutSync(flow.syncItems) : recut(flow.syncChunks);
    return;
  }
  if (flow.syncItems !== null) {
    flow.chunks = asAsyncChunks(cutSync(flow.syncItems));
    flow.asyncItems = null;
    return;
  }
  if (flow.asyncItems !== null) {
    flow.chunks = cutAsync(flow.asyncItems);
    return;
  }
  const chunks = readable(flow.chunks);
  flow.chunks = recutAsync(chunks);
  // Drained only when a back-to-back `.buffer()` replaces this one, never alongside `recutAsync`.
  flow.asyncItems = flattenChunks(chunks);
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
 * A pipeline records its stages and compiles them once, on its first call, into a plan that each
 * call runs with its own streams and context.
 *
 * All stages share one context manager. A run's `ctx.set()` reaches the caller only through a
 * manager passed as `options.context`.
 *
 * `new Pipeline<number>().transform((t) => t.map((x) => x * 2))([1, 2, 3]).toArray()` → `[2, 4, 6]`.
 */
export class Pipeline<T, M extends PipelineMode = "unset", In = T> {
  protected _context!: IContextManager;
  protected _contextIsDefault!: boolean;
  /** The run handler the first recorded stage starts with. */
  protected _runHandler?: PipelineErrorHandler;
  protected _mode!: PipelineMode;
  protected _chunkSize!: number;
  protected _routeTrail!: string;
  protected _branchStages!: Map<number, BranchArm<unknown>[]>;
  /** Whether this is a `.local()` region's pipeline, which cannot be called or wrapped. */
  protected _bound!: boolean;
  /** The recorded stages, newest first; `null` for none. */
  protected _tail!: StageNode | null;
  /** `plan()`'s memo. Never carried through copy-on-write. */
  protected _plan?: Plan;
  /** `registriesFor()`'s memo, by trail. Never carried through copy-on-write. */
  protected _armRegistries?: Map<string, StageRegistries>;
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
    self._runHandler = options?.runHandler;
    self._mode = options?.mode ?? "unset";
    // ⚠ Validate here: a caller can pass `chunkSize` directly, and an unchecked `2.5` cuts an array
    // and a `Set` into different chunks.
    if (options?.chunkSize !== undefined) {
      assertWholeNumberAtLeastOne("chunkSize", options.chunkSize);
    }
    self._chunkSize = options?.chunkSize ?? DEFAULT_CHUNK_SIZE;
    self._bound = options?.bound ?? false;
    self._routeTrail = options?.routeTrail ?? "";
    self._branchStages = options?.branchStages ?? new Map();
    self._tail = (options as RecordedOptions | undefined)?.[RECORDED] ?? null;
    return self;
  }

  /**
   * A copy of this chain that cannot be called or wrapped, like a `.local()` region's pipeline.
   * `data` is never read. It keeps `.bind` a `Pipeline` method, so it never reaches
   * `Function.prototype.bind`.
   *
   * `pipeline.bind([1, 2])([3])` → throws "cannot call a pipeline that is already bound…".
   */
  protected bind<U>(data: AsyncIterable<U>): Pipeline<U, "async", In>;
  protected bind<U>(data: Iterable<U>): Pipeline<U, M extends "async" ? "async" : "sync">;
  protected bind<U>(_data: PipelineSource<U>): Pipeline<U, "sync" | "async"> {
    return this.createPipeline({ ...this.carriedOptions(), bound: true }, this._tail);
  }

  /** Which engine a call runs on: `"shape"` follows the input, `"async"` forces async. A
   * dispatching class overrides it to `"async"`. */
  protected sourcePolicy(): SourcePolicy {
    return "shape";
  }

  /**
   * Builds the next instance of this pipeline's own class, recording `tail` as its stages. A
   * subclass with extra constructor knobs overrides `carriedKnobs()`, not this. `R` is the return
   * type the caller wants back.
   *
   * @example
   * `class Sub extends Pipeline<number> {}`: `new Sub().transform((t) => t.map((x) => x + 1))
   * .constructor.name` → `"Sub"`.
   */
  protected createPipeline<R>(options: PipelineConstructorOptions, tail: StageNode | null): R {
    const Ctor = this.constructor as new (options?: PipelineConstructorOptions) => R;
    return new Ctor({ ...options, ...this.carriedKnobs(), [RECORDED]: tail } as RecordedOptions);
  }

  /** A copy of this pipeline with `desc` recorded after its stages. `extra` overrides carried
   * state. */
  protected record<R>(desc: StageDescriptor, extra?: PipelineState): R {
    return this.createPipeline<R>(
      { ...this.carriedOptions(), ...extra },
      { prev: this._tail, desc },
    );
  }

  /** A subclass's extra constructor knobs, which `createPipeline()` carries to every new instance.
   * An override returns `{ ...super.carriedKnobs(), <own fields> }`. */
  protected carriedKnobs(): object {
    // oxlint-disable-next-line anti-slop/no-known-value-widening -- object is the widest safe common supertype every subclass override's own named-field interface satisfies; Record<string, unknown> refuses those overrides under tsc
    return {};
  }

  /** Every knob a copy-on-write call carries into the next instance. Spread it whole; a hand-built
   * list drops knobs silently. */
  protected carriedOptions(): PipelineConstructorOptions {
    return {
      context: this._context,
      contextIsDefault: this._contextIsDefault,
      chunkSize: this._chunkSize,
      runHandler: this._runHandler,
      mode: this._mode,
      bound: this._bound,
      routeTrail: this._routeTrail,
      // ⚠ A copy: `.branch()` writes into this map, and a shared one leaks arms into siblings.
      branchStages: new Map(this._branchStages),
    };
  }

  /** The context manager one run gets. A caller-named manager is kept across calls; a default one
   * is replaced per run, so two calls never see each other's writes. */
  protected contextForRun(): IContextManager {
    if (!this._contextIsDefault || this._bound) return this._context;
    // ⚠ Seed from the chain's values; an empty manager drops what `.context()` declared.
    return new SimpleContextManager(this._context.toDict());
  }

  /** This chain compiled once. Its ops are `null` when a `.local()` region makes them depend on
   * the call; each call then compiles them afresh. */
  protected plan(): Plan {
    if (this._plan !== undefined) return this._plan;
    // A bound pipeline cuts its input and runs none of its stages.
    const descs = this._bound ? [] : descriptorsSince(this._tail, null)!;
    const perCall = descs.some((desc) => desc.kind === "local");
    this._plan = {
      descs,
      ops: perCall ? null : this.compileStages(descs, { runHandler: this._runHandler, index: 0 }),
      cutSync: buildSyncChunkGenerator<unknown>(this._chunkSize),
      cutAsync: buildChunkGenerator<unknown>(this._chunkSize),
      forcesAsync: this.sourcePolicy() === "async" || this._mode === "async",
    };
    return this._plan;
  }

  /** Compiles stages with no `.local()` among them into ops, advancing `state` past them. */
  protected compileStages(descs: readonly StageDescriptor[], state: PlanState): StageOp[] {
    const ops: StageOp[] = [];
    const readable = (chunks: AsyncIterable<unknown[]>) => this.readableChunks(chunks as never);
    for (const desc of descs) {
      switch (desc.kind) {
        case "apply":
          ops.push(this.planApply(desc.transformer, state.index++, state.runHandler));
          break;
        case "reduce":
          ops.push(this.planReduce(desc.fn, desc.initial, state.index++));
          break;
        case "onError":
          state.runHandler = desc.handler;
          break;
        case "buffer":
          ops.push(bySizeOp(desc.size, readable));
          break;
        case "bufferFn":
          ops.push(byFnOp(desc.fn, readable));
          break;
        case "queue":
          ops.push(queueOp(desc.capacity));
          break;
        default:
          break;
      }
    }
    return fuseInProcess(ops);
  }

  /**
   * The op for one stage added by `.apply()`/`.transform()`. This class runs it in this process;
   * a dispatching class overrides it to run the stage elsewhere.
   *
   * `planApply(doubler, 0, undefined).run(flow)` → `flow`'s chunks, doubled.
   */
  protected planApply(
    transformer: Transformer<any, any, any>,
    _index: number,
    runHandler: PipelineErrorHandler | undefined,
  ): StageOp {
    const runnable = transformer.runnable() as InternalTransformer<unknown, unknown>;
    return inProcessOp([{ transformer, runnable, runHandler }]);
  }

  /**
   * The op for one `.reduce()` stage. This class folds every chunk in this process with one
   * accumulator; a dispatching class overrides it to partition the fold.
   *
   * `planReduce((a, x) => a + x, 0, 0).run(flow)` over `[1, 2]` then `[3]` → `flow` yields `[6]`.
   */
  protected planReduce<U>(fn: ReduceFunction<U, any>, initial: U, _index: number): StageOp {
    return {
      run(flow) {
        if (flow.syncChunks !== null) {
          flow.syncChunks = foldSyncChunkStream(fn, initial, flow.syncChunks, flow.ctx);
        } else {
          flow.chunks = foldChunkStream(fn, initial, flow.chunks, flow.ctx, true);
        }
        consumed(flow);
      },
    };
  }

  /** Runs `descs` over `flow`, compiling each `.local()` region as the call reaches it. Returns
   * `false` once a region's `build` returned a pipeline it did not derive from its argument; the
   * flow is then empty and the later stages never run. */
  protected runStages(descs: readonly StageDescriptor[], state: PlanState, flow: RunFlow): boolean {
    let start = 0;
    for (let i = 0; i < descs.length; i++) {
      const desc = descs[i]!;
      if (desc.kind !== "local") continue;
      runOps(this.compileStages(descs.slice(start, i), state), flow);
      if (!this.runRegion(desc.build, state, flow)) return false;
      start = i + 1;
    }
    runOps(this.compileStages(descs.slice(start), state), flow);
    return true;
  }

  /** A plain pipeline for a `.local()` region's `build`, over this run's context. Its stages are
   * recorded after an origin node, so the ones `build` added can be read back. */
  private regionOf(ctx: IContextManager, runHandler: PipelineErrorHandler | undefined) {
    const origin: StageNode = { prev: null, desc: ORIGIN };
    const region = new Pipeline<unknown, "sync" | "async">({
      context: ctx,
      contextIsDefault: this._contextIsDefault,
      chunkSize: this._chunkSize,
      runHandler,
      bound: true,
      routeTrail: this._routeTrail,
      branchStages: new Map(this._branchStages),
      [RECORDED]: origin,
    } as RecordedOptions);
    return { region, origin };
  }

  /** Runs one `.local()` region: `build` runs now, and its stages run in this process. */
  private runRegion(
    build: (p: AnyPipeline<any>) => AnyPipeline<any>,
    state: PlanState,
    flow: RunFlow,
  ): boolean {
    const { region, origin } = this.regionOf(flow.ctx, state.runHandler);
    if (flow.syncChunks === null) flow.chunks = this.readableChunks(flow.chunks as never);
    const built = build(region);
    const descs = descriptorsSince(built._tail, origin);
    if (descs === null) {
      flow.syncChunks = null;
      flow.chunks = EMPTY_CHUNKS;
      consumed(flow);
      return false;
    }
    return region.runStages(descs, state, flow);
  }

  /** Runs this chain over `input` with the run's context `ctx` and returns the final streams. */
  private runFlow(input: PipelineSource<unknown>, ctx: IContextManager): RunFlow {
    const plan = this.plan();
    const flow: RunFlow = {
      ctx,
      syncChunks: null,
      chunks: EMPTY_CHUNKS,
      syncItems: null,
      asyncItems: null,
    };
    if (!isAsyncSource(input) && !plan.forcesAsync) {
      const items = input as Iterable<unknown>;
      flow.syncChunks = plan.cutSync(items);
      flow.syncItems = items;
    } else {
      const items = toAsyncIterable(input);
      // An array forced onto the async engine is cut synchronously, so it pays no per-item promise.
      flow.chunks = Array.isArray(input)
        ? asAsyncChunks(plan.cutSync(input))
        : plan.cutAsync(items);
      flow.asyncItems = items;
      // A sync input forced async keeps its sync view, so `.buffer()` can recut it synchronously.
      flow.syncItems = isAsyncSource(input) ? null : (input as Iterable<unknown>);
    }
    if (plan.ops !== null) runOps(plan.ops, flow);
    else this.runStages(plan.descs, { runHandler: this._runHandler, index: 0 }, flow);
    return flow;
  }

  /** Fills `tables` with the stages of `descs`, running each `.local()` region's `build` once over
   * `ctx`. Returns `false` when a region's `build` returned an unrelated pipeline. */
  private registerStages(
    descs: readonly StageDescriptor[],
    state: PlanState,
    tables: StageRegistries,
    ctx: IContextManager,
  ): boolean {
    for (const desc of descs) {
      if (desc.kind === "local" && !this.registerRegion(desc.build, state, tables, ctx)) {
        return false;
      }
      if (desc.kind === "apply") {
        tables.chunkTransforms[state.index++] = desc.transformer.runnable() as ChunkTransform;
      } else if (desc.kind === "reduce") {
        const stageIndex = state.index++;
        tables.chunkTransforms[stageIndex] = reduceStagePlaceholder(stageIndex);
        tables.reduceStages.set(stageIndex, { fn: desc.fn, initial: desc.initial });
      }
    }
    return true;
  }

  private registerRegion(
    build: (p: AnyPipeline<any>) => AnyPipeline<any>,
    state: PlanState,
    tables: StageRegistries,
    ctx: IContextManager,
  ): boolean {
    const { region, origin } = this.regionOf(ctx, state.runHandler);
    const regionDescs = descriptorsSince(build(region)._tail, origin);
    return regionDescs !== null && region.registerStages(regionDescs, state, tables, ctx);
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
   * The stage registries a serving side reads to answer `/transform/<n>` and `/reduce/<n>`, read
   * from the recorded stages. A `.local()` region's `build` runs once to list its stages.
   */
  protected registries(): StageRegistries {
    if (this._registries === undefined) {
      const tables: StageRegistries = { chunkTransforms: [], reduceStages: new Map() };
      const complete = this.registerStages(
        this.plan().descs,
        { runHandler: this._runHandler, index: 0 },
        tables,
        this.contextForRun(),
      );
      this._registries = complete ? tables : { chunkTransforms: [], reduceStages: new Map() };
    }
    return this._registries;
  }

  /**
   * The options that reproduce `pipeline`'s chain on another class. It throws for a `.local()`
   * region's pipeline, which exists only while its region is built.
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
      runHandler: pipeline._runHandler,
      chunkSize: pipeline._chunkSize,
      [RECORDED]: pipeline._tail,
    } as RecordedOptions;
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

  /**
   * `chunks` as real items, for a site that reads them. A class whose stages can reply with encoded
   * chunks overrides it to decode them; every other class reads its chunks as they are.
   *
   * `readableChunks(chunks)` → the same `chunks` iterable, on this class.
   */
  protected readableChunks(chunks: AsyncIterable<T[]>): AsyncIterable<T[]> {
    return chunks;
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
    return this.createPipeline<this>(this.carriedOptions(), this._tail);
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
    return this.record<this>({ kind: "onError", handler });
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
    return this.record({ kind: "apply", transformer });
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
    if (typeof sizeOrFn !== "number") {
      return this.record<this>({ kind: "bufferFn", fn: sizeOrFn as BufferFunction<unknown> });
    }
    // ⚠ Refuse a fractional size too: the cutting paths round it differently.
    assertWholeNumberAtLeastOne("buffer size", sizeOrFn);
    // A numeric `.buffer()` before any stage also sets the size that cuts the input. Inside a
    // `.local()` region it never does: the input was cut before the region ran.
    const cutsTheSource = this._tail === null && !this._bound;
    return this.record<this>(
      { kind: "buffer", size: sizeOrFn },
      cutsTheSource ? { chunkSize: sizeOrFn } : {},
    );
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
    return this.record({ kind: "queue", capacity });
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
    return this.record({ kind: "reduce", fn, initial });
  }

  /**
   * Runs the stages `build` adds in this process, whatever class this pipeline is. Stages after the
   * region run the way this class runs them. `build` runs once per terminal call.
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
    return this.record({
      kind: "local",
      build: build as unknown as (p: AnyPipeline<any>) => AnyPipeline<any>,
    });
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

  /**
   * Runs this chain over `input` and returns what a `PipelineResult` drains. That is the sync chunk
   * stream (or `null`), the async chunk stream, and this run's context manager.
   *
   * `materialize: false` hands back the chunks without making them readable as items; `.consume()`
   * passes it because it reads no item.
   */
  drainable(input: PipelineSource<In>, materialize = true): Drainable<T> {
    const flow = this.runFlow(input as PipelineSource<unknown>, this.contextForRun());
    return {
      syncChunks: flow.syncChunks as MaybeAsyncChunks<T> | null,
      chunks: () => {
        const stream = streamOf(flow) as AsyncIterable<T[]>;
        return materialize ? this.readableChunks(stream) : stream;
      },
      // ⚠ This run's manager, not the chain's: an arm must see the writes this run just made.
      context: flow.ctx,
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
   * An empty pipeline of this class with the given context and route trail, for a `.branch()` arm
   * to build on.
   *
   * @example
   * On an `HttpPipeline`, it returns an `HttpPipeline` with the parent's url and no stages.
   */
  protected emptyOfOwnClass<U>(context: IContextManager, routeTrail = ""): AnyPipeline<U> {
    return this.createPipeline<AnyPipeline<U>>(
      {
        ...this.carriedOptions(),
        context,
        contextIsDefault: false,
        routeTrail,
        branchStages: new Map(),
        mode: "unset",
        bound: false,
      },
      null,
    );
  }
}

/** A numeric `.buffer(size)`: cuts and recuts by count. */
function bySizeOp(
  size: number,
  readable: (chunks: AsyncIterable<unknown[]>) => AsyncIterable<unknown[]>,
): StageOp {
  const cutSync = buildSyncChunkGenerator<unknown>(size);
  const recut = (slots: MaybeAsyncChunks<unknown>) => recutSyncChunks(slots, size);
  const cutAsync = buildChunkGenerator<unknown>(size);
  const recutAsync = (chunks: AsyncIterable<unknown[]>) => recutChunks(chunks, size);
  return {
    run(flow) {
      cutOp(cutSync, recut, cutAsync, recutAsync, flow, readable);
    },
  };
}

/** A `.buffer(fn)`: `fn` decides each boundary, with the run's context. */
function byFnOp(
  fn: BufferFunction<unknown>,
  readable: (chunks: AsyncIterable<unknown[]>) => AsyncIterable<unknown[]>,
): StageOp {
  return {
    run(flow) {
      const ctx = flow.ctx;
      cutOp(
        cutSyncItemsWith(fn, ctx),
        (slots) => recutSyncChunksWith(slots, fn, ctx),
        cutItemsWith(fn, ctx),
        recutChunksWith(fn, ctx),
        flow,
        readable,
      );
    },
  };
}

/** A `.queue(capacity)`: prefetches the stream, which makes the run async from here on. */
function queueOp(capacity: number): StageOp {
  return {
    run(flow) {
      flow.chunks = prefetch(streamOf(flow), capacity);
      flow.syncChunks = null;
      consumed(flow);
    },
  };
}

// Makes every `Pipeline` instance, subclasses included, a real function (`instanceof Function`,
// `.call`). ⚠ Not `class Pipeline extends Function`: its `super()` throws `EvalError` wherever
// code generation from strings is banned (a CSP page, a Cloudflare Worker).
Object.setPrototypeOf(Pipeline.prototype, Function.prototype);
