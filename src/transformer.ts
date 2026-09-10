/**
 * Transformer class - chainable chunk transformation operations.
 *
 * Python equivalent:
 * ```python
 * class Transformer[In, Out](BaseTransformer[In, Out]):
 *   def __init__(
 *     self,
 *     transformer: InternalTransformer[In, Out] | None = None,
 *   ) -> None:
 *     ...
 *
 *   def __call__(self, chunks: Iterable[list[In]], context: IContextManager | None = None) -> Iterator[list[Out]]:
 *     ...
 * ```
 */

import type {
  InternalTransformer,
  IContextManager,
  TransformerOptions,
  PipelineFunction,
  ReduceFunction,
  RowErrorHandler,
  PipelineErrorHandler,
  RunScope,
} from "./types";
import { DROP } from "./types";
import { SimpleContextManager } from "./context/simple";
import { isContextAware, dropOrRethrow, chain, mapSettle, isThenable } from "./utils/helpers";
import { Reducer, foldChunk } from "./utils/reduce";

/**
 * Construction-time knobs shared by every `Transformer<In, Out>` constructor overload below —
 * named once rather than repeated per overload.
 */
type TransformerConstructorOptions<In, Out> = TransformerOptions<In, Out> & {
  rowHandler?: RowErrorHandler;
};

/**
 * The one chunk-draining order a standalone `Transformer.process()` runs, one chunk at a time, in
 * order (#17: replaces the deleted `sequential` execution strategy, which had the identical body -
 * a `ConcurrentPipeline`/`HttpPipeline`/`ClusterPipeline` is the replacement for concurrency,
 * wrapping the chain rather than configuring the `Transformer` that drives it).
 *
 * `runHandler` is `Pipeline.onError()`'s own RUN handler (#78), threaded in by `process()` below -
 * this loop, not `process()`'s own (deleted) outer catch, is where a chunk failure is still caught
 * while an async generator can still yield again afterward (an async generator that already threw
 * is finished, so the recovery has to live INSIDE the loop that produces chunks, not around it).
 * `dropOrRethrow` (`utils/helpers.ts`) is the same "call the handler, or propagate" decision
 * `ConcurrentPipeline.apply()`'s wrapped `work` makes for a dispatched stage - one home, not two.
 *
 * `runSequentially(logic, chunks, ctx, (e) => console.warn(e))` over a chunk stream where one chunk
 * throws → that chunk is skipped, every other chunk still yields.
 */
async function* runSequentially<In, Out>(
  transformerLogic: InternalTransformer<In, Out>,
  chunks: AsyncIterable<In[]>,
  context: IContextManager,
  runHandler?: PipelineErrorHandler,
): AsyncGenerator<Out[]> {
  for await (const chunk of chunks) {
    try {
      yield await transformerLogic(chunk, context);
    } catch (error) {
      await dropOrRethrow(runHandler, error as Error, context);
    }
  }
}

/**
 * The row-level recovery `.map()`/`.filter()`/`.tap(fn)` share (#78): try `attempt`, and on a throw
 * (or a rejected `Promise`) call `rowHandler` for a replacement value or `DROP`. Results keep their
 * ORIGINAL index regardless of completion order, and `.filter()` afterward removes only `DROP`s,
 * leaving every recovered or successful row at its own position.
 *
 * A `rowHandler` that itself throws (or rejects) propagates from here (#78 Done-when 10), by one of
 * two routes now (#90): it fails that item's promise, which `mapSettle`'s own `Promise.all` turns
 * into a rejected chunk, OR - when both `attempt` and the handler are synchronous - it throws
 * straight out of this call, which `mapSettle` catches long enough to disarm every sibling promise
 * already created before rethrowing. Either way the chunk fails and the pipeline's run handler sees
 * it.
 *
 * `settleRows(["a", "3"], (s) => { const n = parseInt(s); if (isNaN(n)) throw new Error("bad"); return n; }, () => DROP, ctx)`
 * → `[3]`.
 */
function settleRows<T, U>(
  chunk: T[],
  attempt: (item: T) => U | typeof DROP | Promise<U | typeof DROP>,
  rowHandler: RowErrorHandler,
  ctx: IContextManager,
): U[] | Promise<U[]> {
  const settled = mapSettle(chunk, (item) =>
    attemptRow(item, attempt, rowHandler, ctx, (recovered) => recovered as U | typeof DROP),
  );
  // `U` is an unconstrained type parameter here, so TS cannot itself prove a plain `!== DROP` check
  // narrows to `U` (it could, in principle, be instantiated to include the `DROP` symbol's own
  // type) - the cast is honest because `DROP` is a runtime-unique symbol no caller's `U` actually
  // overlaps with in practice, and every filtered entry really is one attempt's real result.
  return chain(settled, (rows) => rows.filter((v) => v !== DROP) as U[]);
}

/**
 * One row's try/recover step (#78), staying synchronous when `attempt` does (#90) - shared by
 * `settleRows` and `settleRowsFlat` below rather than written once per helper, since both need the
 * identical "run it, and on a throw OR a rejection hand the row to `rowHandler`" decision and only
 * differ in what they do with the SUCCESS value. A synchronous `attempt` that throws recovers
 * synchronously too; an async one recovers through `.catch`, which is where a REJECTION (as opposed
 * to a throw) is caught at all.
 *
 * `attemptRow("a", parseStrict, () => DROP, ctx)` → `DROP`, no `Promise` created.
 */
function attemptRow<T, R>(
  item: T,
  attempt: (item: T) => R | Promise<R>,
  rowHandler: RowErrorHandler,
  ctx: IContextManager,
  onRecovered: (recovered: unknown) => R,
): R | Promise<R> {
  // `onRecovered` is what keeps SUCCESS and RECOVERY on separate channels. Sniffing the value's own
  // shape instead cannot tell them apart: `.flatMap()`'s success is already an array, so a handler
  // that legitimately returns an array had its value spread across the output rather than placed in
  // the failing row's slot - wrong for any `U` that is itself an array type.
  const recover = (error: Error) => chain(rowHandler(item, error, ctx), onRecovered);
  try {
    const result = attempt(item);
    return isThenable(result) ? Promise.resolve(result).catch(recover) : result;
  } catch (error) {
    return recover(error as Error);
  }
}

/**
 * `.flatMap()`'s own row-level recovery (#78) - the same try/attempt/recover shape as `settleRows`
 * above, but each item's SUCCESS is already an array to flatten, so a recovered value (not itself
 * required to be an array) is wrapped as its own one-element array in the row's place; `DROP`
 * contributes nothing for that row. `.flat()` afterward is the same post-processing the no-handler
 * path already used.
 *
 * `settleRowsFlat([1, 2], (x) => (x === 2 ? Promise.reject(new Error("boom")) : [x, x]), () => -1, ctx)`
 * → `[1, 1, -1]`.
 */
function settleRowsFlat<T, U>(
  chunk: T[],
  attempt: (item: T) => U[] | Promise<U[]>,
  rowHandler: RowErrorHandler,
  ctx: IContextManager,
): U[] | Promise<U[]> {
  const perItem = mapSettle(chunk, (item) =>
    // A SUCCESS is already an array to flatten. A recovered value is not required to be one, so it
    // takes the failing row's place as its own single-element array - even when it IS an array -
    // and `DROP` contributes nothing. That decision belongs on the recovery channel alone, which is
    // why it is passed to `attemptRow` rather than applied to its return.
    attemptRow<T, U[]>(item, attempt, rowHandler, ctx, (recovered) =>
      recovered === DROP ? [] : [recovered as U],
    ),
  );
  return chain(perItem, (rows) => rows.flat());
}

/**
 * A reusable, composable stage: `In` chunks in, `Out` chunks out, applied chunk by chunk - it never
 * decides how its own input was cut (#39: chunking lives on `Pipeline`'s `.buffer()` alone, never
 * here). Built by chaining (`.map()`, `.filter()`, `.reduce()`, …), each call returning a NEW
 * `Transformer` so one can be shared across pipelines without aliasing. It carries its own error
 * handling, which is what lets `pipeline.apply(t)` stay a one-liner. Every configuration method
 * (`.onError()`) is copy-on-write like `.map()`/`.filter()`: it returns a NEW `Transformer`
 * carrying every other knob forward, never mutates `this` — the shape
 * `@outputty/laygo`'s own `Model#named` follows.
 *
 * `new Transformer<number, number>().map((n) => n * 2)` → a transformer a pipeline can `.apply()`.
 */
export class Transformer<In, Out, M extends "sync" | "async" = "sync"> {
  /**
   * Type-only (#90), never assigned and never read at runtime: `M` appears in no member's parameter
   * or return type on its own, so without this field TypeScript treats two `Transformer`s differing
   * only in `M` as the same type and the Mode never reaches `Pipeline.transform()`'s inference.
   */
  declare readonly __mode: M;

  /** The internal transform function */
  readonly transform: InternalTransformer<In, Out>;

  /**
   * The ROW handler (#78; replaces #40's chunk-level notification chain entirely) - one plain
   * function, position-independent: `pipe()` (below) carries it forward onto every new `Transformer`
   * a link returns, so `t.onError(h).map(f)` and `t.map(f).onError(h)` read it identically. `undefined`
   * means no row-level recovery is registered - the seam every element-wise link checks before
   * paying for a per-row try/catch.
   */
  readonly rowHandler?: RowErrorHandler;

  /** Default context to use when none provided */
  private defaultContext: IContextManager;

  /**
   * Overload 1 — a real `transform` in hand. Not conditional on `In extends Out`, so it resolves
   * (and is preferred) even from inside this class's OWN generic methods, where `In`/`Out` are
   * still abstract type parameters a deferred conditional can never distribute over. Every
   * copy-on-write rebuild site below (`.pipe()`, `.onError()`) already carries a real `transform`
   * forward, so all of them land here.
   */
  constructor(
    options: TransformerConstructorOptions<In, Out> & { transform: InternalTransformer<In, Out> },
  );
  /**
   * Overload 2 — no `transform` given. Only sound when `In` is assignable to `Out` (the identity
   * default below is a real conversion, not a lie): `new Transformer<number, { id: number }>()`
   * is a compile error here, `new Transformer<number, number>()` is not. Resolves only when `In`/
   * `Out` are CONCRETE types at the call site — a deferred conditional never distributes over an
   * abstract type parameter, so ANY generic scope still holding `In`/`Out` unresolved (this class's
   * own methods, but equally `createTransformer<T>()` or a caller's own generic helper) fails to
   * resolve it too and must route through overload 1 with a real `transform` instead, even where
   * `In`/`Out` happen to be the same type parameter (`T extends T` is never specially recognized).
   */
  constructor(...args: In extends Out ? [options?: TransformerConstructorOptions<In, Out>] : never);
  constructor(options?: TransformerConstructorOptions<In, Out>) {
    // Reachable only via overload 2's `In extends Out` branch — a real conversion there, not a lie.
    this.transform = options?.transform ?? ((chunk, _ctx) => chunk as unknown as Out[]);
    this.rowHandler = options?.rowHandler;
    this.defaultContext = new SimpleContextManager();
  }

  /**
   * The seam that carries the row handler into a runnable chunk-transform function (#78) - reads
   * `this.rowHandler` off the FINAL transformer (position-independent, since `pipe()` already
   * carried it forward from wherever `.onError()` was actually called) and builds the `RunScope`
   * every composed link underneath (`pipe()`'s own closure) reads to decide whether to try/catch
   * per row. Called wherever a `Transformer` becomes runnable: `process()` (below), `Pipeline.apply()`
   * and `ConcurrentPipeline.apply()` (both store the result into `_chunkTransforms`), and
   * `ConcurrentPipeline.stageWork()`'s own default - which is also what `HttpPipeline.fetch()`'s
   * stage registry lookup ends up invoking, since a worker's own `_chunkTransforms` entry was built
   * the same way when its own copy of the entry module constructed the same chain.
   *
   * `t.onError(() => DROP).map(parseStrict).runnable()(["a", "3"], ctx)` → `[3]`.
   */
  runnable(): InternalTransformer<In, Out> {
    const run: RunScope = { rowHandler: this.rowHandler };
    return (chunk, ctx) => this.transform(chunk, ctx, run);
  }

  /**
   * Run this transformer over chunks the CALLER already cut - chunk in, chunk out, no chunking
   * knowledge of its own (#39). The one seam every `Pipeline` class drives it through:
   * `Pipeline.apply()` hands it `this._chunks` directly, and a caller running a `Transformer`
   * standalone supplies its own already-cut `AsyncIterable<In[]>`.
   *
   * Python equivalent:
   * ```python
   * def __call__(self, chunks: Iterable[list[In]], context: IContextManager | None = None) -> Iterator[list[Out]]:
   *   run_context = context if context is not None else self._default_context
   *   for chunk in chunks:
   *     yield self.transformer(chunk, run_context)
   * ```
   *
   * `runHandler` is `Pipeline.onError()`'s own RUN handler (#78) - `Pipeline.apply()` passes its
   * `_runHandler` through here, and it reaches `runSequentially`'s per-chunk try/catch (above): no
   * handler means a chunk failure still propagates and ends the run, same as before #78; a handler
   * that returns drops the failing chunk and lets the run continue to the next one, and one that
   * throws stops the run with whatever it threw. Row-level recovery (`.onError()` on this
   * `Transformer`) is separate and always active regardless of `runHandler` - `this.runnable()`
   * (above) is what wires it into `transformerLogic` before the loop ever sees a chunk.
   *
   * @param chunks - Async iterable of already-cut input chunks
   * @param context - Optional context manager for sharing state
   * @param runHandler - `Pipeline.onError()`'s own run handler, forwarded by `Pipeline.apply()`
   * @returns Async generator of output chunks
   *
   * @example
   * ```typescript
   * const t = new Transformer<number, number>().map((x) => x * 2);
   * for await (const chunk of t.process(chunksOf([1, 2, 3]))) console.log(chunk); // [2, 4, 6]
   *
   * // row recovery: a throwing row is dropped, its siblings survive (#78).
   * const recovered = new Transformer<string, number>()
   *   .onError(() => DROP)
   *   .map((s) => { const n = parseInt(s); if (isNaN(n)) throw new Error(`bad: ${s}`); return n; });
   * for await (const chunk of recovered.process(chunksOf(["a", "3"]))) console.log(chunk); // [3]
   * ```
   */
  async *process(
    chunks: AsyncIterable<In[]>,
    context?: IContextManager,
    runHandler?: PipelineErrorHandler,
  ): AsyncGenerator<Out[]> {
    const runContext = context ?? this.defaultContext;
    yield* runSequentially(this.runnable(), chunks, runContext, runHandler);
  }

  /**
   * Chain a new operation onto this transformer.
   *
   * This is the core internal method for building transformation chains.
   * Each operation creates a NEW transformer that composes the current
   * transform with the new operation.
   *
   * Python equivalent:
   * ```python
   * def _pipe[U](self, operation: Callable[[list[Out], IContextManager], list[U]]) -> "Transformer[In, U]":
   *   current_transformer = self.transformer
   *
   *   def new_transformer(chunk: list[In], ctx: IContextManager) -> list[U]:
   *     intermediate = current_transformer(chunk, ctx)
   *     return operation(intermediate, ctx)
   *
   *   return Transformer[In, U](
   *     chunk_size=self.chunk_size,
   *     transformer=new_transformer,
   *   )
   * ```
   *
   * @param operation - Function that transforms the output of the current transform
   * @returns A new Transformer with the composed operation
   */
  protected pipe<U>(
    operation: (chunk: Out[], ctx: IContextManager, run?: RunScope) => U[] | Promise<U[]>,
  ): Transformer<In, U, M> {
    const currentTransform = this.transform;

    // `chain`, not `await` (#90): a link whose own operation returns a plain array composes with
    // the one before it WITHOUT creating a `Promise`, so a chain of purely synchronous callbacks
    // runs from source to terminal op with no microtask at all. The moment any link returns a
    // thenable, `chain` defers through `.then` and every link after it composes asynchronously -
    // that deferral IS the run widening to async, at exactly the link that made it async.
    const newTransform: InternalTransformer<In, U> = (chunk, ctx, run) =>
      chain(currentTransform(chunk, ctx, run), (intermediate) => operation(intermediate, ctx, run));

    return new Transformer<In, U, M>({
      transform: newTransform,
      // rowHandler is keyed on no type parameter at all (`unknown` item), unaffected by the
      // Out -> U change, so it carries forward - this is what makes `.onError()` position-
      // independent: `t.onError(fn).map(g)` and `t.map(g).onError(fn)` both read it the same way.
      rowHandler: this.rowHandler,
    });
  }

  /**
   * Transform each element using a mapping function.
   *
   * Python equivalent:
   * ```python
   * def map[U](self, function: PipelineFunction[Out, U]) -> "Transformer[In, U]":
   *   if is_context_aware(function):
   *     context_aware_func: Callable[[Out, IContextManager], U] = function
   *     return self._pipe(lambda chunk, ctx: [context_aware_func(x, ctx) for x in chunk])
   *
   *   non_context_func: Callable[[Out], U] = function
   *   return self._pipe(lambda chunk, _ctx: [non_context_func(x) for x in chunk])
   * ```
   *
   * @param fn - Mapping function (can be context-aware)
   * @returns New Transformer with map operation applied
   */
  map<U>(fn: (item: Out, ctx: IContextManager) => Promise<U>): Transformer<In, U, "async">;
  map<U>(
    fn: (item: Out, ctx: IContextManager) => U extends Promise<unknown> ? never : U,
  ): Transformer<In, U, M>;
  map<U>(fn: (item: Out, ctx: IContextManager) => U): Transformer<In, U, "async">;
  map<U>(fn: PipelineFunction<Out, U>): Transformer<In, U, "sync" | "async"> {
    if (isContextAware(fn)) {
      return this.pipe((chunk, ctx, run) => {
        // No handler registered: the plain path (#78 Done-when 11 - the seam costs nothing until
        // `.onError()` is actually called). `mapSettle`, not `Promise.all` (#90): a chunk whose
        // items all came back as plain values is returned as-is, creating no `Promise` at all.
        if (!run?.rowHandler) {
          return mapSettle(chunk, (x) => fn(x, ctx));
        }
        return settleRows(chunk, (x) => fn(x, ctx), run.rowHandler, ctx);
      });
    }
    return this.pipe((chunk, _ctx, run) => {
      const plain = fn as (item: Out) => U | Promise<U>;
      if (!run?.rowHandler) {
        return mapSettle(chunk, (x) => plain(x));
      }
      return settleRows(chunk, (x) => plain(x), run.rowHandler, _ctx);
    });
  }

  /**
   * Filter elements using a predicate function.
   *
   * Python equivalent:
   * ```python
   * def filter(self, predicate: PipelineFunction[Out, bool]) -> "Transformer[In, Out]":
   *   if is_context_aware(predicate):
   *     context_aware_predicate: Callable[[Out, IContextManager], bool] = predicate
   *     return self._pipe(lambda chunk, ctx: [x for x in chunk if context_aware_predicate(x, ctx)])
   *
   *   non_context_predicate: Callable[[Out], bool] = predicate
   *   return self._pipe(lambda chunk, _ctx: [x for x in chunk if non_context_predicate(x)])
   * ```
   *
   * @param predicate - Filter function (can be context-aware)
   * @returns New Transformer with filter operation applied
   */
  filter(
    predicate: (item: Out, ctx: IContextManager) => Promise<boolean>,
  ): Transformer<In, Out, "async">;
  filter(predicate: (item: Out, ctx: IContextManager) => boolean): Transformer<In, Out, M>;
  filter(predicate: PipelineFunction<Out, boolean>): Transformer<In, Out, "sync" | "async"> {
    if (isContextAware(predicate)) {
      return this.pipe((chunk, ctx, run) => {
        if (!run?.rowHandler) {
          return chain(
            mapSettle(chunk, (x) => predicate(x, ctx)),
            (keep) => chunk.filter((_x, i) => keep[i]),
          );
        }
        return settleRows(
          chunk,
          (x) => chain(predicate(x, ctx), (keep) => (keep ? x : DROP)),
          run.rowHandler,
          ctx,
        );
      });
    }
    return this.pipe((chunk, _ctx, run) => {
      const fn = predicate as (item: Out) => boolean | Promise<boolean>;
      if (!run?.rowHandler) {
        return chain(
          mapSettle(chunk, (x) => fn(x)),
          (keep) => chunk.filter((_x, i) => keep[i]),
        );
      }
      return settleRows(
        chunk,
        (x) => chain(fn(x), (keep) => (keep ? x : DROP)),
        run.rowHandler,
        _ctx,
      );
    });
  }

  /**
   * Flatten nested arrays in the output.
   *
   * Python equivalent:
   * ```python
   * def flatten[T](
   *   self: Union["Transformer[In, list[T]]", "Transformer[In, tuple[T, ...]]", "Transformer[In, set[T]]"],
   * ) -> "Transformer[In, T]":
   *   return self._pipe(lambda chunk, ctx: [item for sublist in chunk for item in sublist])
   * ```
   *
   * @returns New Transformer with flattened output
   */
  flatten<U>(this: Transformer<In, U[], M>): Transformer<In, U, M> {
    return this.pipe((chunk, _ctx) => chunk.flat());
  }

  /**
   * Map each element and flatten the results.
   *
   * Equivalent to `.map(fn).flatten()` but handles async functions properly.
   * Useful when the mapping function returns an array and you want to flatten the results.
   *
   * @param fn - Mapping function that returns an array (can be async)
   * @returns New Transformer with flatMap operation applied
   */
  flatMap<U>(fn: (item: Out, ctx: IContextManager) => Promise<U[]>): Transformer<In, U, "async">;
  flatMap<U>(fn: (item: Out, ctx: IContextManager) => U[]): Transformer<In, U, M>;
  flatMap<U>(fn: PipelineFunction<Out, U[]>): Transformer<In, U, "sync" | "async"> {
    if (isContextAware(fn)) {
      return this.pipe((chunk, ctx, run) => {
        if (!run?.rowHandler) {
          const results = mapSettle(chunk, (x) => fn(x, ctx) as U[] | Promise<U[]>);
          return chain(results, (rows) => rows.flat());
        }
        return settleRowsFlat(chunk, (x) => fn(x, ctx) as U[] | Promise<U[]>, run.rowHandler, ctx);
      });
    }
    return this.pipe((chunk, _ctx, run) => {
      const plain = fn as (item: Out) => U[] | Promise<U[]>;
      if (!run?.rowHandler) {
        const results = mapSettle(chunk, (x) => plain(x));
        return chain(results, (rows) => rows.flat());
      }
      return settleRowsFlat(chunk, (x) => plain(x), run.rowHandler, _ctx);
    });
  }

  /**
   * Execute side effects for each element without modifying the data.
   *
   * Can be called with either:
   * - A function that receives each element (and optionally context)
   * - A Transformer whose transform function will be executed for side effects
   *
   * Python equivalent:
   * ```python
   * def tap(self, arg: Union["Transformer[Out, Any]", PipelineFunction[Out, Any]]) -> "Transformer[In, Out]":
   *   match arg:
   *     case Transformer() as transformer:
   *       tapped_func = transformer.transformer
   *       return self._pipe(lambda chunk, ctx: chunk if tapped_func(chunk, ctx) or True else chunk)
   *     case function if callable(function):
   *       if is_context_aware(function):
   *         context_aware_func: Callable[[Out, IContextManager], Any] = function
   *         return self._pipe(lambda chunk, ctx: [x for x in chunk if context_aware_func(x, ctx) or True])
   *       non_context_func: Callable[[Out], Any] = function
   *       return self._pipe(lambda chunk, _ctx: [x for x in chunk if non_context_func(x) or True])
   * ```
   */

  // Overload signatures
  tap(fn: (item: Out, ctx: IContextManager) => Promise<unknown>): Transformer<In, Out, "async">;
  tap(fn: (item: Out, ctx: IContextManager) => unknown): Transformer<In, Out, M>;
  tap(transformer: Transformer<Out, unknown, "async">): Transformer<In, Out, "async">;
  tap(transformer: Transformer<Out, unknown, "sync">): Transformer<In, Out, M>;
  tap(
    arg: PipelineFunction<Out, unknown> | Transformer<Out, unknown, "sync" | "async">,
  ): Transformer<In, Out, "sync" | "async"> {
    // Check if arg is a Transformer instance - chunk-aware, keeps chunk semantics (row handling
    // does not reach it, per the ticket's own Constraints: a chunk-aware link cannot take per-row
    // semantics).
    if (arg instanceof Transformer) {
      const tappedTransform = arg.transform;
      return this.pipe((chunk, ctx) =>
        // The tapped transformer runs for side effects only, and settles before the chunk moves on.
        // A tapped chain that is itself synchronous settles without a `Promise` (#90).
        chain(tappedTransform(chunk, ctx), () => chunk),
      );
    }

    // Handle function case
    const fn = arg;
    if (isContextAware(fn)) {
      return this.pipe((chunk, ctx, run) => {
        if (!run?.rowHandler) {
          return chain(
            mapSettle(chunk, (x) => fn(x, ctx)),
            () => chunk,
          );
        }
        return settleRows(chunk, (x) => chain(fn(x, ctx), () => x), run.rowHandler, ctx);
      });
    }

    const nonContextFn = fn as (item: Out) => unknown;
    return this.pipe((chunk, _ctx, run) => {
      if (!run?.rowHandler) {
        return chain(
          mapSettle(chunk, (x) => nonContextFn(x)),
          () => chunk,
        );
      }
      return settleRows(chunk, (x) => chain(nonContextFn(x), () => x), run.rowHandler, _ctx);
    });
  }

  /**
   * Apply a transformation function to this transformer.
   *
   * This is a composition helper that allows applying a function that takes
   * this transformer and returns a new one. Useful for extracting reusable
   * transformation chains.
   *
   * Python equivalent:
   * ```python
   * def apply[T](self, t: Callable[[Self], "Transformer[In, T]"]) -> "Transformer[In, T]":
   *   return t(self)
   * ```
   *
   * @param fn - Function that receives this transformer and returns a new one
   * @returns Result of applying the function to this transformer
   */
  apply<U, M2 extends "sync" | "async">(
    fn: (t: this) => Transformer<In, U, M2>,
  ): Transformer<In, U, M2> {
    return fn(this);
  }

  /**
   * Register the ROW handler, returning a NEW transformer that carries it forward (#78; reshaped,
   * BREAKING - replaces #40's chunk-level notification contract entirely, and `.catch()` is
   * deleted alongside it).
   *
   * Copy-on-write, like every other configuration method here: `this` is never mutated, and
   * `pipe()` carries the returned transformer's `rowHandler` forward onto every later
   * `.map()`/`.filter()`/…, which is what makes `.onError()` POSITION-INDEPENDENT -
   * `t.onError(fn).map(g)` and `t.map(g).onError(fn)` behave identically. One plain function, not a
   * chain: a second `.onError()` call replaces the first, the same as every other configuration
   * method here (`.buffer()`, `.context()`).
   *
   * `handler` receives the failing row, the `Error`, and the context. Returning a value puts that
   * value in the row's place; returning the exported `DROP` sentinel removes the row; throwing (or
   * returning a rejected `Promise`) escalates past the row to the CHUNK, reaching
   * `Pipeline.onError()` instead. Reaches every element-wise call - `.map()`, `.filter()`,
   * `.flatMap()`, `.tap(fn)` - plus `Transformer.reduce()`'s fold step; never `.tap(transformer)` or
   * `.loop()`, which stay chunk-aware (the ticket's own Constraints: a chunk-aware link cannot take
   * per-row semantics).
   *
   * @param handler - The row handler; may be async.
   * @returns A new Transformer carrying the handler forward.
   *
   * @example
   * `t.onError(() => DROP).map(parseStrict)` over `["a","3"]` → `[3]`.
   */
  onError(handler: RowErrorHandler): Transformer<In, Out, M> {
    return new Transformer<In, Out, M>({
      transform: this.transform,
      rowHandler: handler,
    });
  }

  /**
   * Repeatedly apply a transformer while a condition is true.
   *
   * The loop continues until the condition returns false or maxIterations is reached.
   * Useful for iterative refinement operations where data needs multiple passes.
   *
   * `condition` is ONE signature, `(chunk, ctx) => boolean` — the same reason `PipelineFunction`
   * (`types.ts`) is one signature rather than a union of arities: a union blocks contextual
   * inference, making an un-annotated `(c) => c.every(...)` an implicit-`any` `c`. A 1-arg caller
   * stays valid by arity flexibility; `condition.length` still tells `ctx`-aware apart at runtime.
   *
   * @param loopTransformer - Transformer to apply on each iteration
   * @param condition - Function that returns true to continue looping
   * @param maxIterations - Optional maximum number of iterations
   * @returns New transformer with loop operation applied
   */
  loop(
    loopTransformer: Transformer<Out, Out, "async">,
    condition: (chunk: Out[], ctx: IContextManager) => boolean,
    maxIterations?: number,
  ): Transformer<In, Out, "async">;
  loop(
    loopTransformer: Transformer<Out, Out, "sync">,
    condition: (chunk: Out[], ctx: IContextManager) => boolean,
    maxIterations?: number,
  ): Transformer<In, Out, M>;
  loop(
    loopTransformer: Transformer<Out, Out, "sync" | "async">,
    condition: (chunk: Out[], ctx: IContextManager) => boolean,
    maxIterations?: number,
  ): Transformer<In, Out, "sync" | "async"> {
    const loopedTransform = loopTransformer.transform;
    const conditionIsContextAware = condition.length >= 2;

    // A real `while` loop carries the synchronous case (#90), and `drain` re-enters itself ONLY
    // across an async boundary, where the continuation runs on a fresh stack in its own microtask.
    // Recursing per ITERATION instead overflows the stack on a synchronous looped transformer:
    // measured, 4000 iterations over a one-link body threw `RangeError: Maximum call stack size
    // exceeded`, where the pre-#90 loop completed 20 000, and the ceiling fell further as the
    // looped body gained links.
    const drain = (
      startChunk: Out[],
      ctx: IContextManager,
      startIteration: number,
    ): Out[] | Promise<Out[]> => {
      let currentChunk = startChunk;
      let iterations = startIteration;

      while (true) {
        if (maxIterations !== undefined && iterations >= maxIterations) return currentChunk;

        const shouldContinue = conditionIsContextAware
          ? condition(currentChunk, ctx)
          : (condition as (chunk: Out[]) => boolean)(currentChunk);
        if (!shouldContinue) return currentChunk;

        const next = loopedTransform(currentChunk, ctx);
        if (isThenable(next)) {
          const resumeAt = iterations + 1;
          return Promise.resolve(next).then((settled) => drain(settled, ctx, resumeAt));
        }
        currentChunk = next;
        iterations++;
      }
    };

    return this.pipe((chunk, ctx) => drain(chunk, ctx, 0));
  }

  /**
   * Folds this ONE chunk into one or more values via `emit` - no state survives to the next chunk,
   * `Pipeline.reduce()`'s the only place cross-chunk state lives (#45; replaces the deleted
   * whole-dataset terminal form entirely - `Pipeline.reduce()`, run over an actual `Pipeline`, is
   * its replacement).
   *
   * The registered row handler (`.onError()`) reaches this fold's own per-item step too (#78) - a
   * `fn` that throws for one item hands that item to the handler; a returned value REPLACES the
   * accumulator directly (never re-runs `fn`, so a handler cannot cause a second throw), `DROP`
   * skips the item entirely (the accumulator is unchanged, and it does not count toward whether
   * `.final()` still owes a trailing value). `Pipeline.reduce()` never reaches this - it folds with
   * no `Transformer` in scope at all (the ticket's own Constraints).
   *
   * @param fn - `(acc, item, ctx, emit) => acc` - called with all four arguments regardless of its
   *   own declared arity (JS ignores extras), so `(acc, item) => acc` and `(acc, item, ctx, emit) =>
   *   …` both work.
   * @param initial - Initial accumulator value, reset for every chunk.
   * @returns A new `Transformer` whose output is whatever `emit()` pushed plus the trailing
   *   accumulator (only if items were folded since the last emit).
   *
   * @example
   * `new Transformer<number, number>().reduce((acc, x) => acc + x, 0)` over chunks `[[1,2],[3]]` →
   * `[3]` then `[3]` (each chunk's own independent sum).
   */
  reduce<U>(
    fn: (acc: U, item: Out, ctx: IContextManager, emit: (value: U) => void) => Promise<U>,
    initial: U,
  ): Transformer<In, U, "async">;
  reduce<U>(
    fn: (acc: U, item: Out, ctx: IContextManager, emit: (value: U) => void) => U,
    initial: U,
  ): Transformer<In, U, M>;
  reduce<U>(fn: ReduceFunction<U, Out>, initial: U): Transformer<In, U, "sync" | "async"> {
    return this.pipe((chunk, ctx, run) => {
      if (chunk.length === 0) return [];
      const reducer = new Reducer<U, Out>(fn, initial, run?.rowHandler);
      return chain(foldChunk(reducer, chunk, ctx), (values) => {
        values.push(...reducer.final());
        return values;
      });
    });
  }

  /**
   * Convert sync iterable to async iterable.
   */
  private toAsyncIterable<T>(data: AsyncIterable<T> | Iterable<T>): AsyncIterable<T> {
    if (Symbol.asyncIterator in data) {
      return data as AsyncIterable<T>;
    }
    const syncData = data as Iterable<T>;
    return {
      [Symbol.asyncIterator]: async function* () {
        for (const item of syncData) {
          yield item;
        }
      },
    };
  }

  /**
   * Stop processing when a condition is met.
   *
   * When the condition function returns true, throws an error to halt
   * the pipeline execution. Useful for implementing early exit conditions.
   *
   * Python equivalent:
   * ```python
   * def short_circuit(self, function: Callable[[IContextManager], bool | None]) -> "Transformer[In, Out]":
   *   def operation(chunk: list[Out], ctx: IContextManager) -> list[Out]:
   *     if function(ctx):
   *       raise RuntimeError("Short-circuit condition met, stopping execution.")
   *     return chunk
   *   return self._pipe(operation)
   * ```
   *
   * @param fn - Function that returns true to stop execution
   * @returns New transformer with short-circuit condition applied
   */
  shortCircuit(fn: (ctx: IContextManager) => boolean): Transformer<In, Out, M> {
    return this.pipe((chunk, ctx) => {
      if (fn(ctx)) {
        throw new Error("Short-circuit condition met, stopping execution.");
      }
      return chunk;
    });
  }
}
