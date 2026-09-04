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
import { sequential } from "./strategies/sequential";
import { normalize } from "./utils/chunk";

/**
 * A chunk-wise transform function: takes one chunk (array) and produces the
 * next chunk (array), optionally reading/writing the shared context.
 */
type ChunkTransform = (chunk: unknown[], ctx: IContextManager) => unknown[] | Promise<unknown[]>;

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
   * Internal: names of `Transformer` knobs (`withExecutor`/`withHooks`/a non-default
   * `chunkSize`) applied onto this pipeline that the ASYNC-ITERATION path (`[Symbol.asyncIterator]`,
   * `chunkTransforms` above) cannot honor — it replays each transform's plain function directly,
   * never `Transformer.execute()`, so a strategy/hooks/chunk-size configured via `.withExecutor()`/
   * `.withHooks()`/a custom `chunkSize` is silently inert on that path. Accumulated (never cleared)
   * across `.apply()` calls so iterating a pipeline built from several applied transformers reports
   * every inert knob. Not intended for direct external use.
   */
  sourcePositionViolations?: string[];
}

/**
 * Which of a `Transformer`'s knobs are INERT when replayed via `Pipeline`'s async-iteration path
 * (`chunkTransforms`, which calls the transform function directly — never `Transformer.execute()`,
 * so `.strategy`/`.hooks`/a non-default `.chunkSize`/a custom `.setChunker()` chunker never take
 * effect there).
 *
 * Runs once per `Pipeline#apply()` call, to grow `sourcePositionViolations` (this file's `apply()`).
 *
 * Compares `transformer.strategy` against the built-in `sequential` BY REFERENCE — never
 * `instanceof`/`.name`, which a plain function has neither of. `.withExecutor(sequential)` sets
 * `strategy` to that exact same function reference the constructor already defaults to, so it
 * reads as safe, same as never calling `.withExecutor()` at all; any other function reference
 * (`concurrent(...)`, a caller's own) is flagged, whether or not it happens to behave like
 * `sequential` — a plain function carries no capability metadata of its own to ask instead, unlike
 * the class-based `SequentialStrategy.appliesInSourcePosition` this seam replaced, so ONLY the
 * built-in `sequential` export is ever recognized as source-position-safe. Matches the OLD design's
 * own default: a strategy there was flagged unsafe unless it explicitly declared otherwise, and no
 * custom strategy could inherit `sequential`'s safety without doing so itself either. The chunker
 * check reads `transformer.chunker` (public, set by `.setChunker()`) the same way — never a
 * chunk-generator identity comparison, which a rebuilt default generator would fail anyway.
 *
 * `inertKnobsOf(new Transformer().withExecutor(concurrent()))` → `["withExecutor"]`;
 * `inertKnobsOf(new Transformer().withExecutor(sequential))` → `[]`;
 * `inertKnobsOf(new Transformer().setChunker(custom))` → `["setChunker"]`;
 * `inertKnobsOf(new Transformer())` → `[]`.
 */
function inertKnobsOf<In, Out>(transformer: Transformer<In, Out>): string[] {
  const violations: string[] = [];
  if (transformer.strategy !== sequential) violations.push("withExecutor");
  if (transformer.hooks !== undefined) violations.push("withHooks");
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
 * carrying a context manager seeded from the current one forward.
 *
 * `await new Pipeline([1, 2, 3]).transform((t) => t.map((x) => x * 2)).toArray()` →
 * `[2, 4, 6]`. A terminal op's return carries no context snapshot (#744) - read `.contextManager`
 * for that.
 */
export class Pipeline<T> {
  private dataSource: AsyncIterable<T>;
  private _context: IContextManager;
  private _rootSource: AsyncIterable<unknown>;
  private _chunkTransforms: ChunkTransform[];
  private _sourcePositionViolations: string[];

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
   * ├─ any inert knob recorded (withExecutor/withHooks/chunkSize, `inertKnobsOf`)? ──yes──▶ throw
   * │        no
   * ▼
   * for chunk of normalize(rootSource) → replay each chunkTransform in order → yield
   * ```
   *
   * FAILS LOUD (does not silently drop the knob) when this pipeline carries a `Transformer` knob
   * the chunk-transform replay below cannot honor (`withExecutor`/`withHooks`/a non-default
   * `chunkSize` — `inertKnobsOf`, above `apply()`): those only take effect through
   * `Transformer.execute()`, which this loop never calls, so a `.withExecutor(concurrent())`
   * pipeline handed straight to `m.from()` would otherwise run — silently sequential, silently
   * un-hooked — instead of raising.
   *
   * @example
   * ```typescript
   * const pipeline = new Pipeline(source).transform((t) => t.map((r) => r.id * 2));
   * for await (const chunk of pipeline) {
   *   console.log(chunk); // e.g. [2], [4, 6]
   * }
   * // new Pipeline(source).apply(new Transformer().withExecutor(concurrent()))
   * // handed to a for-await loop throws naming 'withExecutor'.
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
   * Merge values into the pipeline's context, returning a NEW Pipeline that carries the merged
   * context forward — copy-on-write, like `.apply()`/`.transform()`/`.buffer()`; `this` is never
   * mutated. A later chained call (`.apply()`, `.transform()`, …) reads the RETURNED Pipeline's own
   * `_context`, so the merge is visible downstream exactly as before; only a caller still holding
   * the pre-`.context()` reference sees the old, un-merged context, the same as any other operation
   * here.
   *
   * Python equivalent:
   * ```python
   * def context(self, ctx: dict[str, Any]) -> "Pipeline[T]":
   *   merged = {**self.context_manager.to_dict(), **ctx}
   *   return Pipeline(self.data_source, context=merged)
   * ```
   *
   * @param ctx - Dictionary of context values to merge in
   * @returns A new Pipeline carrying the merged context
   */
  context(ctx: Record<string, unknown>): Pipeline<T> {
    const newContext = new SimpleContextManager();
    for (const [key, value] of Object.entries(this._context.toDict())) {
      newContext.set(key, value);
    }
    for (const [key, value] of Object.entries(ctx)) {
      newContext.set(key, value);
    }
    return new Pipeline<T>(this.dataSource, {
      context: newContext,
      rootSource: this._rootSource,
      chunkTransforms: this._chunkTransforms,
      sourcePositionViolations: this._sourcePositionViolations,
    });
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
   * `.toArray()` resolves `[2, 4, 6]`. `pipeline.apply(new Transformer().withExecutor
   * (concurrent()))` then iterated with `for await` (not a terminal op) → throws naming
   * `'withExecutor'` (`sourcePositionViolations` picked it up here).
   */
  apply<U>(transformer: Transformer<T, U>): Pipeline<U> {
    const newData = transformer.execute(this.dataSource, this._context);
    return new Pipeline<U>(newData, {
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
   * Python equivalent:
   * ```python
   * def buffer(self, size: int, batch_size: int = 1000) -> "Pipeline[T]":
   *   # Uses Queue and ThreadPoolExecutor for pre-fetching
   *   ...
   * ```
   */
  buffer(size: number, batchSize = 1000): Pipeline<T> {
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

    return new Pipeline<T>(bufferedStream(), { context: this._context });
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
