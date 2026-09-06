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
import { buildChunkGenerator, flattenChunks } from "./utils/chunk";

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
 * What a `Pipeline<T>` may be built from - a stream/collection of items. Terminal ops
 * (`toArray`/`first`/…) and async iteration both read the SAME persisted chunk stream (#39) - there
 * is no longer a separate pre-chunked-array reading, since the only consumer that ever cared about
 * an array-shaped source element (the deleted source-position replay) is gone.
 */
export type PipelineSource<T> = AsyncIterable<T> | Iterable<T>;

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
   * `HttpPipeline`'s own `.fetch()` (`src/pipelines/http.ts:170`) looks a stage up by index here
   * to serve a dispatched request. Not intended for direct external use.
   */
  chunkTransforms?: ChunkTransform[];
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
  /** The persisted chunk stream every terminal op and async iteration reads (#39) - cut ONCE,
   * either by the constructor's own default or by `.buffer(size)`, and carried unchanged through
   * every later stage until another `.buffer()` call declares a new one. */
  protected _chunks: AsyncIterable<T[]>;
  /** The pre-buffer ITEM view a back-to-back `.buffer()` call recuts from, or `null` once a real
   * stage (`.apply()`) has consumed `_chunks` - see `PipelineOptions.preBufferItems`. */
  protected _preBufferItems: AsyncIterable<T> | null;
  protected _context: IContextManager;
  protected _chunkTransforms: ChunkTransform[];

  /**
   * Create a new Pipeline from a data source.
   *
   * @param data - Sync or async iterable data source
   * @param options - Optional pipeline configuration
   */
  constructor(data: PipelineSource<T>, options?: PipelineOptions) {
    // `contextFactory` runs ONLY when `context` is absent, and only HERE - every copy-on-write
    // call below (`.context()`, `.apply()`, `.buffer()`) always passes an already-resolved
    // `context`, so a `ClusterPipeline` chain's later `.transform()` calls never re-invoke it
    // (#31, Done-when 6: once per process, not once per stage or per request).
    this._context = options?.context ?? options?.contextFactory?.() ?? new SimpleContextManager();
    this._chunkTransforms = options?.chunkTransforms ?? [];

    if (options?.chunks !== undefined) {
      // The copy-on-write path: `data` is inert (an internal caller passes `[]`) since the chunk
      // stream already exists - `createPipeline()` (below) is the one caller that takes this.
      this._chunks = options.chunks as AsyncIterable<T[]>;
      this._preBufferItems = (options.preBufferItems ?? null) as AsyncIterable<T> | null;
    } else {
      const items = this.toAsyncIterable(data);
      this._preBufferItems = items;
      this._chunks = buildChunkGenerator<T>(DEFAULT_CHUNK_SIZE)(items);
    }
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
  protected createPipeline<U>(chunks: AsyncIterable<U[]>, options: PipelineOptions): Pipeline<U> {
    const Ctor = this.constructor as new (
      data: PipelineSource<U>,
      options?: PipelineOptions,
    ) => Pipeline<U>;
    return new Ctor([], { ...options, chunks });
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
    yield* this._chunks;
  }

  // ===== Static Factory Methods =====

  /**
   * Merge multiple pipelines into a single pipeline (fan-in pattern).
   *
   * All items from all input pipelines are yielded in sequence, each source pipeline's OWN
   * already-cut `_chunks` boundary preserved rather than re-derived - merging never re-chunks
   * (#39). `options.context`, when given, is the SAME instance returned as `.contextManager` on
   * the merged pipeline - mutated in place with every source pipeline's own context values, later
   * pipelines taking precedence on a shared key (#31; the one place values flow BACKWARD across
   * pipelines, which is why this is the one seam that takes a manager explicitly rather than only
   * ever receiving one at construction). With no `options`, a fresh `SimpleContextManager` is
   * built and populated the same way - today's behaviour, unchanged.
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
  static merge<Ps extends readonly Pipeline<any>[]>(
    pipelines: Ps,
    options?: PipelineOptions,
  ): Pipeline<ElementOf<Ps[number]>> {
    type U = ElementOf<Ps[number]>;

    // `options?.context`/`contextFactory` both honored on the zero-pipeline path too (review: the
    // fast return used to drop context entirely, and contextFactory was never read at all - a
    // caller passing either got a fresh SimpleContextManager instead, breaking the same-instance
    // contract every other path here keeps).
    if (pipelines.length === 0) {
      return new Pipeline<U>([], { context: options?.context ?? options?.contextFactory?.() });
    }

    // Merge contexts from all pipelines into the caller's own manager when given (#31) - never a
    // fresh copy that would discard it, the same reason .context() (above) mutates in place.
    const mergedContext =
      options?.context ?? options?.contextFactory?.() ?? new SimpleContextManager();
    for (const pipeline of pipelines) {
      const ctx = pipeline._context.toDict();
      for (const [key, value] of Object.entries(ctx)) {
        mergedContext.set(key, value);
      }
    }

    // Concatenates each source pipeline's OWN chunk stream in sequence - no new chunking decision
    // at merge time (#39): a pipeline cut at 2 and one cut at 4 both keep their own boundary.
    async function* mergedChunks(): AsyncGenerator<U[]> {
      for (const pipeline of pipelines) {
        for await (const chunk of pipeline._chunks) {
          yield chunk as U[];
        }
      }
    }

    return new Pipeline<U>([], { context: mergedContext, chunks: mergedChunks() });
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
    return this.createPipeline<T>(this._chunks, {
      context: this._context,
      chunkTransforms: this._chunkTransforms,
      preBufferItems: this._preBufferItems,
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
   * Apply a transformer to the pipeline data - `transformer.process(this._chunks, ctx)` runs it
   * directly over the pipeline's own persisted chunk stream, no cut here (#39): chunking is never
   * this method's decision, only `.buffer()`'s.
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
  apply<U>(transformer: Transformer<T, U>): Pipeline<U> {
    const newChunks = transformer.process(this._chunks, this._context);
    return this.createPipeline<U>(newChunks, {
      context: this._context,
      chunkTransforms: [
        ...this._chunkTransforms,
        transformer.transform as unknown as ChunkTransform,
      ],
      // A real stage just consumed `_chunks` - nothing left to recut a back-to-back `.buffer()`
      // from except this stage's own output, so the pre-buffer item view resets to null.
      preBufferItems: null,
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
   * subclass without losing its own `.transform(fn, { local: true })` overload.
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
    const items = this._preBufferItems ?? flattenChunks(this._chunks);
    const chunks = buildChunkGenerator<T>(size)(items);
    return this.createPipeline<T>(chunks, {
      context: this._context,
      chunkTransforms: this._chunkTransforms,
      preBufferItems: items,
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
    for await (const item of flattenChunks(this._chunks)) {
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
    for await (const item of flattenChunks(this._chunks)) {
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
    for await (const _ of flattenChunks(this._chunks)) {
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
    for await (const item of flattenChunks(this._chunks)) {
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

    for await (const item of flattenChunks(this._chunks)) {
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
    transformer: Transformer<T, U>,
    results: Record<string, U[]>,
    key: string,
  ): Promise<void> {
    const singleItemChunk = {
      [Symbol.asyncIterator]: async function* () {
        yield [item];
      },
    };

    for await (const chunk of transformer.process(singleItemChunk, this._context)) {
      results[key].push(...chunk);
    }
  }
}
