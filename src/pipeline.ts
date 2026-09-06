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

import type { IContextManager, BranchDefinition, BranchOptions } from "./types";
import { DEFAULT_CHUNK_SIZE } from "./types";
import { SimpleContextManager } from "./context/simple";
import { Transformer } from "./transformer";
import { normalize } from "./utils/chunk";

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
type ElementOf<P> = P extends Pipeline<infer U> ? U : never;

/**
 * Drain complete batches off the front of `buffer` while it holds at least
 * `size` batches, flattening each drained batch into individual items.
 *
 * Runs from within `Pipeline#buffer`'s streaming loop, once per incoming
 * item that fills a batch. Mutates `buffer` in place (shifting drained
 * batches off) and yields their items in order.
 *
 * @example
 * `[...drainReadyBatches([[1, 2], [3, 4]], 2)]` → `[1, 2, 3, 4]`, leaving
 * `buffer` empty.
 */
function* drainReadyBatches<T>(buffer: T[][], size: number): Generator<T> {
  while (buffer.length >= size) {
    const batch = buffer.shift()!;
    yield* batch;
  }
}

/**
 * What a `Pipeline<T>` may be built from — a stream/collection of **items**
 * (`T`) OR of pre-chunked **arrays** (`T[]`), in any mix. Async iteration
 * (`for await…of`, chunk-preserving) accepts both: an array element is its own
 * chunk boundary, loose items accumulate (via `normalize`). Terminal ops
 * (`toArray`/`first`/…) assume an **item** stream — feeding them a pre-chunked
 * source is undefined; use async iteration for that case.
 */
export type PipelineSource<T> = AsyncIterable<T | T[]> | Iterable<T | T[]>;

/** Construction-time knobs for a `Pipeline` — every field optional. */
export interface PipelineOptions {
  /**
   * Optional context manager for sharing state across operations.
   */
  context?: IContextManager;
  /**
   * Internal: the original, untransformed source used to derive chunk
   * boundaries for async iteration. Not intended for direct external use.
   */
  rootSource?: AsyncIterable<unknown>;
  /**
   * Internal: the chain of chunk-wise transforms accumulated via `.apply()`/
   * `.transform()`, replayed over each `normalize()` chunk during async
   * iteration. Not intended for direct external use.
   */
  chunkTransforms?: ChunkTransform[];
  /**
   * Internal: names of `Transformer` knobs (`withHooks`/a non-default `chunkSize`/`setChunker`, or
   * a dispatching `ConcurrentPipeline`/`HttpPipeline`/`ClusterPipeline` stage's own class name)
   * applied onto this pipeline that the ASYNC-ITERATION path (`[Symbol.asyncIterator]`,
   * `chunkTransforms` above) cannot honor — it replays each transform's plain function directly,
   * never `Transformer.execute()`, so hooks/chunk-size/a dispatched stage is silently inert on that
   * path. Accumulated (never cleared) across `.apply()` calls so iterating a pipeline built from
   * several applied transformers reports every inert knob. Not intended for direct external use.
   */
  sourcePositionViolations?: string[];
}

/**
 * Which of a `Transformer`'s knobs are INERT when replayed via `Pipeline`'s async-iteration path
 * (`chunkTransforms`, which calls the transform function directly — never `Transformer.execute()`,
 * so `.hooks`/`.onError()`/a non-default `.chunkSize`/a custom `.setChunker()` chunker never take
 * effect there).
 *
 * Runs once per `Pipeline#apply()` call, to grow `sourcePositionViolations` (this file's `apply()`).
 * The chunker check reads `transformer.chunker` (public, set by `.setChunker()`) — never a
 * chunk-generator identity comparison, which a rebuilt default generator would fail anyway. The
 * `errorHandler` check reads `.hasHandlers()` (`errors/handler.ts`) — an `ErrorHandler` always
 * exists on a `Transformer` (the constructor default), so its PRESENCE is never the signal, only
 * whether anything was ever registered via `.onError()`.
 *
 * `inertKnobsOf(new Transformer().withHooks({}))` → `["withHooks"]`;
 * `inertKnobsOf(new Transformer().onError(() => {}))` → `["onError"]`;
 * `inertKnobsOf(new Transformer().setChunker(custom))` → `["setChunker"]`;
 * `inertKnobsOf(new Transformer())` → `[]`.
 *
 * Exported (#17) so `ConcurrentPipeline.apply()` (`src/pipelines/concurrent.ts`) can report the
 * SAME transformer-level violations alongside its own ("this stage was dispatched, not applied
 * in-process") - the two lists have different sources but the same shape and the same consumer
 * (`sourcePositionViolations`). Review (#17) found `.onError()` missing here entirely - silently
 * inert on a dispatched stage (`stageWork()` never calls `execute()`, the only path that consults
 * it) with no fail-loud signal, unlike every other knob this function already covered.
 */
export function inertKnobsOf<In, Out>(transformer: Transformer<In, Out>): string[] {
  const violations: string[] = [];
  if (transformer.hooks !== undefined) violations.push("withHooks");
  if (transformer.errorHandler.hasHandlers()) violations.push("onError");
  if (transformer.chunkSize !== DEFAULT_CHUNK_SIZE) violations.push("chunkSize");
  if (transformer.chunker !== undefined) violations.push("setChunker");
  return violations;
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
export class Pipeline<T> {
  // Protected (#17), not private: a dispatching subclass's own overridden `createPipeline()`
  // (below) reads these to carry them into the next instance the same way this base
  // implementation does - `private` would put them out of reach from `src/pipelines/`.
  protected dataSource: AsyncIterable<T>;
  protected _context: IContextManager;
  protected _rootSource: AsyncIterable<unknown>;
  protected _chunkTransforms: ChunkTransform[];
  protected _sourcePositionViolations: string[];

  /**
   * Create a new Pipeline from a data source.
   *
   * @param data - Sync or async iterable data source
   * @param options - Optional pipeline configuration
   */
  constructor(data: PipelineSource<T>, options?: PipelineOptions) {
    // `dataSource` is the ITEM-view terminal ops (`toArray`/`apply`/…) consume;
    // a pre-chunked (`T[]`) source is only sound under async iteration, which
    // reads `_rootSource` through `normalize` (Array.isArray at runtime), so the
    // compile-time narrowing to `AsyncIterable<T>` here is safe for its callers.
    this.dataSource = this.toAsyncIterable(data) as AsyncIterable<T>;
    this._context = options?.context ?? new SimpleContextManager();
    this._rootSource = options?.rootSource ?? (this.dataSource as AsyncIterable<unknown>);
    this._chunkTransforms = options?.chunkTransforms ?? [];
    this._sourcePositionViolations = options?.sourcePositionViolations ?? [];
  }

  /**
   * Builds the NEXT `Pipeline` in a copy-on-write chain, via `this.constructor` rather than a
   * hard-coded `new Pipeline<U>` (#17) — the ONE seam every copy-on-write method below
   * (`.apply()`, `.context()`, `.buffer()`) goes through, so a subclass built on `Pipeline`
   * survives its own `.transform()` chain instead of silently decaying to a plain `Pipeline`.
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
  protected createPipeline<U>(data: AsyncIterable<U>, options: PipelineOptions): Pipeline<U> {
    const Ctor = this.constructor as new (
      data: AsyncIterable<U>,
      options?: PipelineOptions,
    ) => Pipeline<U>;
    return new Ctor(data, options);
  }

  /**
   * Async-iterate the pipeline yielding TRANSFORMED CHUNKS whose boundaries
   * match `normalize(rootSource)` — i.e. the original source's array/single-item
   * shape decides where one chunk ends and the next begins, not a fixed
   * chunk size. Each chunk is replayed through every chunk-wise transform
   * accumulated via `.apply()`/`.transform()`, in order.
   *
   * Runs whenever the pipeline is consumed with `for await...of` instead of
   * a terminal operation like `.toArray()` — this is also the path a bare
   * `Pipeline` handed to laygo's `m.from()` (`@outputty/laygo`) drains as a source.
   *
   * ```text
   * [Symbol.asyncIterator]()
   * ├─ any inert knob recorded (withHooks/chunkSize/a dispatched stage, `inertKnobsOf`)? ──yes──▶ throw
   * │        no
   * ▼
   * for chunk of normalize(rootSource) → replay each chunkTransform in order → yield
   * ```
   *
   * FAILS LOUD (does not silently drop the knob) when this pipeline carries a `Transformer` knob
   * the chunk-transform replay below cannot honor (`withHooks`/a non-default `chunkSize` —
   * `inertKnobsOf`, above `apply()` — or a dispatched `ConcurrentPipeline`/`HttpPipeline`/
   * `ClusterPipeline` stage, `pipelines/concurrent.ts`'s own `apply()`): those only take effect
   * through `Transformer.execute()` or the fan-out itself, which this loop never calls, so a
   * `ConcurrentPipeline` handed straight to `m.from()` would otherwise run silently sequential and
   * in-process instead of raising.
   *
   * @example
   * ```typescript
   * const pipeline = new Pipeline(source).transform((t) => t.map((r) => r.id * 2));
   * for await (const chunk of pipeline) {
   *   console.log(chunk); // e.g. [2], [4, 6]
   * }
   * // new Pipeline(source).apply(new Transformer().withHooks({ onStart: () => {} }))
   * // handed to a for-await loop throws naming 'withHooks'.
   * ```
   */
  async *[Symbol.asyncIterator](): AsyncGenerator<T[]> {
    if (this._sourcePositionViolations.length > 0) {
      throw new Error(
        `Pipeline: ${this._sourcePositionViolations.join("/")} not applied in source position ` +
          `(iterating a Pipeline directly — e.g. via m.from(pipeline) — replays each chunk ` +
          `transform's plain function, bypassing Transformer.execute()). Call a terminal op ` +
          `(.toArray()/.forEach()/.consume()/…) instead, or drop the knob.`,
      );
    }
    for await (const chunk of normalize(this._rootSource)) {
      let current: unknown[] = chunk;
      for (const transform of this._chunkTransforms) {
        current = await transform(current, this._context);
      }
      yield current as T[];
    }
  }

  // ===== Static Factory Methods =====

  /**
   * Merge multiple pipelines into a single pipeline (fan-in pattern).
   *
   * All items from all input pipelines are yielded in sequence.
   * Context is merged from all pipelines, with later pipelines taking precedence.
   *
   * Python equivalent:
   * ```python
   * @classmethod
   * def merge(cls, *pipelines: "Pipeline[T]") -> "Pipeline[T]":
   *   async def merged_generator():
   *     for pipeline in pipelines:
   *       async for item in pipeline.dataSource:
   *         yield item
   *   merged_context = {}
   *   for p in pipelines:
   *     merged_context.update(p.context_manager.to_dict())
   *   return cls(merged_generator(), context=merged_context)
   * ```
   *
   * @param pipelines - Pipelines to merge
   * @returns A new pipeline that yields all items from all input pipelines
   */
  static merge<Ps extends readonly Pipeline<any>[]>(
    ...pipelines: Ps
  ): Pipeline<ElementOf<Ps[number]>> {
    type U = ElementOf<Ps[number]>;

    if (pipelines.length === 0) {
      return new Pipeline<U>([]);
    }

    // Merge contexts from all pipelines
    const mergedContext = new SimpleContextManager();
    for (const pipeline of pipelines) {
      const ctx = pipeline._context.toDict();
      for (const [key, value] of Object.entries(ctx)) {
        mergedContext.set(key, value);
      }
    }

    // Create async generator that yields from all pipelines in sequence
    async function* mergedGenerator(): AsyncGenerator<U> {
      for (const pipeline of pipelines) {
        for await (const item of pipeline.dataSource) {
          yield item;
        }
      }
    }

    return new Pipeline<U>(mergedGenerator(), { context: mergedContext });
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
   * branch, never two `.context()` calls off the same parent.
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
   *   dispatching subclass's own `.transform(fn, { local: true })` still typechecks after a
   *   `.context()` call, the same as it would directly off the constructor.
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
    return this.createPipeline<T>(this.dataSource, {
      context: this._context,
      rootSource: this._rootSource,
      chunkTransforms: this._chunkTransforms,
      sourcePositionViolations: this._sourcePositionViolations,
    }) as this;
  }

  /**
   * Convert sync iterable to async iterable.
   */
  private toAsyncIterable<U>(data: AsyncIterable<U> | Iterable<U>): AsyncIterable<U> {
    // Check for async iterator
    if (Symbol.asyncIterator in Object(data)) {
      return data as AsyncIterable<U>;
    }

    // Convert sync iterable to async
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
   * Apply a transformer to the pipeline data.
   *
   * Python equivalent:
   * ```python
   * def apply(self, transformer: Transformer[T, U]) -> "Pipeline[U]":
   *   if isinstance(transformer, Transformer):
   *     self.processed_data = transformer(self.processed_data, self.context_manager)
   *   return self
   * ```
   *
   * Also records any of the transformer's knobs the async-iteration path (`[Symbol.asyncIterator]`,
   * above) cannot honor (`inertKnobsOf`) onto `sourcePositionViolations`, so a later source-position
   * consumption fails loud instead of silently ignoring them.
   *
   * `pipeline.apply(new Transformer<T, T>().map((x) => x * 2))` on a pipeline of `[1, 2, 3]` →
   * `.toArray()` resolves `[2, 4, 6]`. `pipeline.apply(new Transformer().withHooks({onStart:
   * ()=>{}}))` then iterated with `for await` (not a terminal op) → throws naming `'withHooks'`
   * (`sourcePositionViolations` picked it up here).
   */
  apply<U>(transformer: Transformer<T, U>): Pipeline<U> {
    const newData = transformer.execute(this.dataSource, this._context);
    return this.createPipeline<U>(newData, {
      context: this._context,
      rootSource: this._rootSource,
      chunkTransforms: [
        ...this._chunkTransforms,
        transformer.transform as unknown as ChunkTransform,
      ],
      sourcePositionViolations: [...this._sourcePositionViolations, ...inertKnobsOf(transformer)],
    });
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
  transform<U>(t: (transformer: Transformer<T, T>) => Transformer<T, U>): Pipeline<U> {
    const transformer = t(new Transformer<T, T>({ transform: (chunk) => chunk }));
    return this.apply(transformer);
  }

  /**
   * Create a buffered version of the pipeline for pre-fetching.
   *
   * Note: In TypeScript with async iterators, natural backpressure exists.
   * This method creates a simple batching buffer.
   *
   * Typed `this`, like `.context()` (#17) - `T` never changes here either, so this stays chainable
   * on a dispatching subclass without losing its own `.transform(fn, { local: true })` overload.
   *
   * Python equivalent:
   * ```python
   * def buffer(self, size: int, batch_size: int = 1000) -> "Pipeline[T]":
   *   # Uses Queue and ThreadPoolExecutor for pre-fetching
   *   ...
   * ```
   */
  buffer(size: number, batchSize = 1000): this {
    const source = this.dataSource;

    async function* bufferedStream(): AsyncGenerator<T> {
      const buffer: T[][] = [];
      let currentBatch: T[] = [];

      for await (const item of source) {
        currentBatch.push(item);

        if (currentBatch.length >= batchSize) {
          buffer.push(currentBatch);
          currentBatch = [];
          yield* drainReadyBatches(buffer, size);
        }
      }

      // Flush remaining items
      if (currentBatch.length > 0) {
        buffer.push(currentBatch);
      }
      for (const batch of buffer) {
        yield* batch;
      }
    }

    // Carries rootSource/chunkTransforms/sourcePositionViolations forward unchanged (#17) - only
    // the item stream itself is rebatched here. Dropping them (as this line used to) loses the
    // async-iteration replay's own stage history AND silently erases any "not applied in source
    // position" violation `.apply()` already recorded - review found `.buffer()` after a
    // `ConcurrentPipeline` dispatch made the fail-loud guarantee vanish, verified live.
    return this.createPipeline<T>(bufferedStream(), {
      context: this._context,
      rootSource: this._rootSource,
      chunkTransforms: this._chunkTransforms,
      sourcePositionViolations: this._sourcePositionViolations,
    }) as this;
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
  async toArray(): Promise<T[]> {
    const results: T[] = [];
    for await (const item of this.dataSource) {
      results.push(item);
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
  async first(n = 1): Promise<T[]> {
    if (n < 1) {
      throw new Error("n must be at least 1");
    }

    const results: T[] = [];
    for await (const item of this.dataSource) {
      results.push(item);
      if (results.length >= n) {
        break;
      }
    }
    return results;
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
  async consume(): Promise<void> {
    for await (const _ of this.dataSource) {
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
  async forEach(fn: (item: T) => void | Promise<void>): Promise<void> {
    for await (const item of this.dataSource) {
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
    branches: Record<string, BranchDefinition<T, U, Transformer<T, U>>>,
    options?: BranchOptions,
  ): Promise<Record<string, U[]>> {
    const firstMatch = options?.firstMatch !== false; // Default to true (router mode)

    const results: Record<string, U[]> = {};
    for (const key of Object.keys(branches)) {
      results[key] = [];
    }

    for await (const item of this.dataSource) {
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
    branches: Record<string, BranchDefinition<T, U, Transformer<T, U>>>,
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
   * Runs once per matched (item, branch) pair from `routeItemToBranches`.
   *
   * @example
   * `pushBranchOutput(4, doubler, { even: [] }, "even")` mutates
   * `results.even` to `[8]`.
   */
  private async pushBranchOutput<U>(
    item: T,
    transformer: Transformer<T, U>,
    results: Record<string, U[]>,
    key: string,
  ): Promise<void> {
    const singleItemIterable = {
      [Symbol.asyncIterator]: async function* () {
        yield item;
      },
    };

    for await (const output of transformer.execute(singleItemIterable, this._context)) {
      results[key].push(output);
    }
  }
}
