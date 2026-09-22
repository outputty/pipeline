/**
 * `ConcurrentPipeline`, and the fan-out every dispatching class inherits. A subclass overrides
 * `stageWork()`/`reduceWork()` to choose where a stage runs.
 */

import type {
  IContextManager,
  InternalTransformer,
  ReduceFunction,
  SourcePolicy,
  PipelineMode,
  ChunkTransform,
  RouteVerb,
  StageRoute,
  StageRegistries,
  Tagged,
  ReduceWork,
} from "@src/types";
import { Pipeline, type PipelineConstructorOptions, type WrappablePipeline } from "@src/pipeline";
import { Transformer } from "@src/transformer";
import { foldChunkStream } from "@src/utils/reduce";
import { share } from "@src/utils/cut";
import { runStageChunk } from "@src/utils/helpers";
import { isEmptyEncodedChunk } from "@src/utils/encoded-chunk";

/** Construction-time knobs for `ConcurrentPipeline` and every class that extends it. */
export interface ConcurrentPipelineOptions {
  /** Chunks kept in flight at once. Default `4`. */
  maxConcurrency?: number;
  /** Restore input order in output. Default `true`. */
  ordered?: boolean;
}

type ConcurrentPipelineConstructorOptions = ConcurrentPipelineOptions & PipelineConstructorOptions;

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
      // ⚠ Marks it handled for a chunk never awaited after an earlier throw; `await p` still throws.
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

async function* fanOutUnordered<T, U>(
  chunks: AsyncIterable<T[]>,
  work: (chunk: T[], ctx: IContextManager) => U[] | Promise<U[]>,
  ctx: IContextManager,
  maxConcurrency: number,
): AsyncGenerator<U[]> {
  const iterator = chunks[Symbol.asyncIterator]();
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
    // ⚠ Marks it handled: the ramp-up loop can throw before this promise ever reaches `race()`.
    tagged.catch(() => {});
    inFlight.set(id, tagged);
  }

  // ⚠ A manual iterator is not closed by `for await`, so `finally` closes the source on every exit.
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
 * A fresh copy of the seed for one partition, so partitions never share one mutable accumulator.
 * A seed that cannot be copied throws; `.local((p) => p.reduce(fn, initial))` avoids the copy.
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

  // ⚠ Refuse a class instance: `structuredClone` drops its prototype without throwing.
  if (Object.getPrototypeOf(copy) !== Object.getPrototypeOf(initial)) {
    throw new Error(`${SEED_REFUSAL}: a class instance loses its prototype when copied`);
  }
  return copy;
}

const SEED_REFUSAL =
  "a partitioned reduce needs one accumulator per partition, and this seed cannot be copied. " +
  "Pass a seed structuredClone can copy, or wrap the fold in " +
  ".local((p) => p.reduce(fn, initial)) to run it unpartitioned in this process";

/**
 * ⚠ The seed of an empty partitioned reduce comes from here, once for the stage. A partition
 * seeding itself would repeat it once per partition.
 */
async function* seedIfNoChunk<U>(source: AsyncGenerator<U[]>, seed: () => U): AsyncGenerator<U[]> {
  let yielded = false;
  for await (const chunk of source) {
    yielded = true;
    yield chunk;
  }
  if (!yielded) yield [seed()];
}

async function* mergeUnordered<U>(sources: AsyncGenerator<U[]>[]): AsyncGenerator<U[]> {
  const inFlight = new Map<number, Promise<Tagged<IteratorResult<U[]>>>>();

  function pull(id: number): void {
    const tagged = sources[id]!.next().then((result) => ({ id, result }));
    // ⚠ Marks it handled: a partition still pending after another one threw is never awaited.
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
 * Runs a chain with up to `maxConcurrency` chunks of each stage in flight at once, in this process.
 * The chain says what to do; this class says where.
 *
 * `In` is the type the pipeline is called with; `T` is the current stage's output.
 *
 * ```typescript
 * const doubled = new Pipeline<number>().transform((t) => t.map((x) => x * 2));
 * await new ConcurrentPipeline(doubled, { maxConcurrency: 4 })([1, 2, 3, 4, 5]).toArray();
 * // → [2, 4, 6, 8, 10]
 * ```
 */
export class ConcurrentPipeline<T, In = T> extends Pipeline<T, "async", In> {
  /** Chunks of the current stage kept in flight at once. */
  readonly maxConcurrency: number;
  /** Whether output order is restored to match input order once a chunk finishes. */
  readonly ordered: boolean;

  /** Wraps a chain built elsewhere, running its stages concurrently. A pipeline already bound to a
   * source is refused. */
  constructor(pipeline: WrappablePipeline<T, In>, options?: ConcurrentPipelineOptions);
  constructor(options?: ConcurrentPipelineConstructorOptions);
  constructor(
    first?: WrappablePipeline<T, In> | ConcurrentPipelineConstructorOptions,
    second?: ConcurrentPipelineOptions,
  ) {
    const options = Pipeline.wrapping<ConcurrentPipelineConstructorOptions>(first, second);
    super(options);
    this.maxConcurrency = options?.maxConcurrency ?? 4;
    if (this.maxConcurrency < 1) {
      throw new Error("maxConcurrency must be at least 1");
    }
    this.ordered = options?.ordered ?? true;
  }

  /**
   * Carries `maxConcurrency` and `ordered` into each instance a chained call builds. A subclass
   * with its own knobs extends this.
   *
   * `new ConcurrentPipeline({ maxConcurrency: 8 }).context({ k: 1 }).maxConcurrency` → `8`.
   */
  protected override carriedKnobs(): ConcurrentPipelineOptions {
    return {
      maxConcurrency: this.maxConcurrency,
      ordered: this.ordered,
    };
  }

  /**
   * Adds a stage that is dispatched through `stageWork()`. Wrap a region in `.local()` to run it
   * in this process instead.
   */
  override transform<U, M2 extends "sync" | "async">(
    builder: (t: Transformer<T, T, "async">) => Transformer<T, U, M2>,
  ): ConcurrentPipeline<U, In> {
    const seed = new Transformer<T, T, "async">({ transform: (chunk) => chunk });
    return this.apply(builder(seed));
  }

  override apply<U>(transformer: Transformer<T, U, "sync" | "async">): ConcurrentPipeline<U, In> {
    if (this.isDeferred()) {
      return this.defer<U, ConcurrentPipeline<U, In>>((p) =>
        p.apply(transformer as Transformer<unknown, U, "sync" | "async">),
      );
    }
    const stageIndex = this._chunkTransforms.length;
    const rawWork = this.stageWork(transformer, stageIndex);
    // ⚠ Kept `async`, so a synchronous throw fails at the chunk's own position in the ordered output.
    const work: InternalTransformer<T, U> = async (chunk, ctx) => {
      // ⚠ An emptied encoded chunk is not sent, and is returned as it is, not as `[]`, so the next
      // dispatched stage still skips it.
      if (isEmptyEncodedChunk(chunk)) return chunk as unknown as U[];
      return runStageChunk(rawWork, chunk, ctx, this._runHandler);
    };
    const fanOut = this.ordered ? fanOutOrdered : fanOutUnordered;
    const newChunks = fanOut(this._chunks, work, this._context, this.maxConcurrency);

    return this.createPipeline<U, ConcurrentPipeline<U, In>>(newChunks, {
      ...this.carriedOptions(),
      chunkTransforms: [
        ...this._chunkTransforms,
        // ⚠ `runnable()`, not `transformer.transform`: a worker serving this entry needs the row handler.
        transformer.runnable() as unknown as ChunkTransform,
      ],
      ...this.freshPreBuffer(),
    });
  }

  /**
   * Folds the stream in up to `maxConcurrency` partitions, each with its own accumulator. Each
   * partition's result flows downstream as its own value, in completion order. For one final value,
   * fold the partials again with `.local((p) => p.reduce(mergeFn, initial))`.
   *
   * ```typescript
   * const summed = new Pipeline<number>()
   *   .buffer(2)
   *   .reduce((a, x) => a + x, 0)
   *   .local((p) => p.reduce((a, v) => a + v, 0));
   * await new ConcurrentPipeline(summed, { maxConcurrency: 2 })([1, 2, 3, 4, 5]).toArray();
   * // → [15]
   * ```
   */
  override reduce<U>(fn: ReduceFunction<U, T>, initial: U): ConcurrentPipeline<U, In> {
    if (this.isDeferred()) {
      return this.defer<U, ConcurrentPipeline<U, In>>((p) =>
        p.reduce(
          fn as (acc: U, item: any, ctx: IContextManager, emit: (v: U) => void) => U,
          initial,
        ),
      );
    }
    const { stageIndex, chunkTransforms, reduceStages } = this.pushReduceStage(fn, initial);
    const work = this.reduceWork(fn, initial, stageIndex);

    const iterator = this._chunks[Symbol.asyncIterator]();
    const partitions = Array.from({ length: this.maxConcurrency }, () =>
      work(share(iterator), this._context),
    );
    const newChunks = seedIfNoChunk(mergeUnordered(partitions), () => seedFor(initial));

    return this.createPipeline<U, ConcurrentPipeline<U, In>>(newChunks, {
      ...this.carriedOptions(),
      chunkTransforms,
      reduceStages,
      ...this.freshPreBuffer(),
    });
  }

  /**
   * Runs every dispatching class on the async engine, whatever the input's shape.
   *
   * `new ConcurrentPipeline(chain)([1, 2, 3])` → an async result, even for an array input.
   */
  protected override sourcePolicy(): SourcePolicy {
    return "async";
  }

  override local<U, M2 extends PipelineMode>(
    build: (p: Pipeline<T, "async", any>) => Pipeline<U, M2, any>,
  ): ConcurrentPipeline<U, In> {
    return super.local(build) as unknown as ConcurrentPipeline<U, In>;
  }

  override queue(capacity: number): ConcurrentPipeline<T, In> {
    return super.queue(capacity) as unknown as ConcurrentPipeline<T, In>;
  }

  /**
   * Where one partition of a reduce stage runs; a subclass overrides it to fold elsewhere. The
   * returned function is called once per partition and yields that partition's output chunks.
   * This class folds in this process.
   *
   * `reduceWork((a, x) => a + x, 0, 0)(chunks, ctx)` over `[1, 2]` then `[3]` → yields `[6]`.
   */
  protected reduceWork<U>(
    fn: ReduceFunction<U, T>,
    initial: U,
    _stageIndex: number,
  ): ReduceWork<T, U> {
    return (chunks, ctx) => foldChunkStream(fn, seedFor(initial), chunks, ctx);
  }

  /**
   * Where one chunk of a stage runs; a subclass overrides it to run the chunk elsewhere. This class
   * runs it in this process.
   *
   * `stageWork(doubler, 0)([1, 2], ctx)` → `[2, 4]`.
   */
  protected stageWork<U>(
    transformer: Transformer<T, U, "sync" | "async">,
    _stageIndex: number,
  ): InternalTransformer<T, U> {
    return transformer.runnable();
  }

  /**
   * The route a dispatched stage is addressed by, following the path the chain was built along.
   *
   * `routePath("transform", 2)` → `"/transform/2"`; inside a `.branch()` arm named `big` →
   * `"/branch/0/big/transform/2"`.
   */
  protected routePath(verb: RouteVerb, index: number): string {
    return `${this._routeTrail}/${verb}/${index}`;
  }

  /** The stage registries a route's `trail` names: this pipeline's own for `null`, else an arm's.
   * `null` when no arm matches. */
  protected resolveRegistries(trail: string | null): StageRegistries | null {
    return trail === null ? this.registries() : this.registriesFor(trail);
  }
}

/**
 * Parses a route `routePath()` built: `/transform/<n>` or `/reduce/<n>`, optionally after a
 * `/branch/<i>/<name>` trail. Returns `null` for anything else.
 *
 * ⚠ Not anchored at the start: a cluster worker receives the route with its `/pipeline/<i>` prefix.
 *
 * `parseRoute("/pipeline/0/branch/1/big/transform/2")` →
 * `{ trail: "/branch/1/big", verb: "transform", index: 2 }`.
 */
export function parseRoute(route: string): StageRoute | null {
  const match = /(\/branch\/\d+\/[^/]+)?\/(transform|reduce)\/(\d+)$/.exec(route);
  if (match === null) return null;
  return { trail: match[1] ?? null, verb: match[2] as RouteVerb, index: Number(match[3]) };
}
