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
import {
  isContextAware,
  dropOrRethrow,
  chain,
  mapSettle,
  isThenable,
  tryRecover,
} from "./utils/helpers";
import { Reducer, foldChunk } from "./utils/reduce";

/**
 * The one chunk-draining order a standalone `Transformer.process()` runs, one chunk at a time, in
 * order - a `ConcurrentPipeline`/`HttpPipeline`/`ClusterPipeline` is what adds concurrency,
 * wrapping the chain rather than configuring the `Transformer` that drives it.
 *
 * `runHandler` is `Pipeline.onError()`'s own RUN handler, threaded in by `process()` below - this
 * loop, not a wrapping try/catch outside it, is where a chunk failure is still caught while an
 * async generator can still yield again afterward (an async generator that already threw is
 * finished, so the recovery has to live INSIDE the loop that produces chunks, not around it).
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
 * The row-level recovery `.map()`/`.filter()`/`.tap(fn)` share: try `attempt`, and on a throw (or a
 * rejected `Promise`) call `rowHandler` for a replacement value or `DROP`. Results keep their
 * ORIGINAL index regardless of completion order, and `.filter()` afterward removes only `DROP`s,
 * leaving every recovered or successful row at its own position.
 *
 * A `rowHandler` that itself throws (or rejects) propagates from here, by one of two routes: it
 * fails that item's promise, which `mapSettle`'s own `Promise.all` turns into a rejected chunk, OR
 * - when both `attempt` and the handler are synchronous - it throws straight out of this call,
 * which `mapSettle` catches long enough to disarm every sibling promise already created before
 * rethrowing. Either way the chunk fails and the pipeline's run handler sees it.
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
 * One row's try/recover step, staying synchronous when `attempt` does - shared by `settleRows` and
 * `settleRowsFlat` below rather than written once per helper, since both need the identical "run
 * it, and on a throw OR a rejection hand the row to `rowHandler`" decision and only differ in what
 * they do with the SUCCESS value. A synchronous `attempt` that throws recovers synchronously too;
 * an async one recovers through `.catch`, which is where a REJECTION (as opposed to a throw) is
 * caught at all.
 *
 * `attemptRow("a", parseStrict, () => DROP, ctx)` → `DROP`, no `Promise` created.
 */
function attemptRow<T, R>(
  item: T,
  attempt: (item: T) => R | Promise<R>,
  rowHandler: RowErrorHandler,
  ctx: IContextManager,
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- recovered is RowErrorHandler's own unknown return value, by the same design (see types.ts)
  onRecovered: (recovered: unknown) => R,
): R | Promise<R> {
  // `onRecovered` is what keeps SUCCESS and RECOVERY on separate channels. Sniffing the value's own
  // shape instead cannot tell them apart: `.flatMap()`'s success is already an array, so a handler
  // that legitimately returns an array had its value spread across the output rather than placed in
  // the failing row's slot - wrong for any `U` that is itself an array type.
  return tryRecover(
    () => attempt(item),
    (error) => chain(rowHandler(item, error, ctx), onRecovered),
  );
}

/**
 * `.flatMap()`'s own row-level recovery - the same try/attempt/recover shape as `settleRows`
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
 * decides how its own input was cut (chunking lives on `Pipeline`'s `.buffer()` alone, never here).
 * Built by chaining (`.map()`, `.filter()`, `.reduce()`, …), each call returning a NEW `Transformer`
 * so one can be shared across pipelines without aliasing. It carries its own error handling, which
 * is what lets `pipeline.apply(t)` stay a one-liner. Every configuration method (`.onError()`) is
 * copy-on-write like `.map()`/`.filter()`: it returns a NEW `Transformer` carrying every other knob
 * forward, never mutates `this`.
 *
 * `new Transformer<number, number>().map((n) => n * 2)` → a transformer a pipeline can `.apply()`.
 */
export class Transformer<In, Out, M extends "sync" | "async" = "sync"> {
  declare readonly __mode: M;

  /** The internal transform function */
  readonly transform: InternalTransformer<In, Out>;

  /**
   * The ROW handler - one plain function, position-independent: `pipe()` (below) carries it forward
   * onto every new `Transformer` a link returns, so `t.onError(h).map(f)` and `t.map(f).onError(h)`
   * read it identically. `undefined` means no row-level recovery is registered - the seam every
   * element-wise link checks before paying for a per-row try/catch.
   */
  readonly rowHandler?: RowErrorHandler;

  private defaultContext: IContextManager;

  /**
   * Overload 1 — a real `transform` in hand. Not conditional on `In extends Out`, so it resolves
   * (and is preferred) even from inside this class's OWN generic methods, where `In`/`Out` are
   * still abstract type parameters a deferred conditional can never distribute over. Every
   * copy-on-write rebuild site below (`.pipe()`, `.onError()`) already carries a real `transform`
   * forward, so all of them land here.
   */
  constructor(options: TransformerOptions<In, Out> & { transform: InternalTransformer<In, Out> });
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
  constructor(...args: In extends Out ? [options?: TransformerOptions<In, Out>] : never);
  constructor(options?: TransformerOptions<In, Out>) {
    // Reachable only via overload 2's `In extends Out` branch — a real conversion there, not a lie.
    this.transform = options?.transform ?? ((chunk, _ctx) => chunk as unknown as Out[]);
    this.rowHandler = options?.rowHandler;
    this.defaultContext = new SimpleContextManager();
  }

  /**
   * The seam that carries the row handler into a runnable chunk-transform function - reads
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
   * knowledge of its own. The one seam every `Pipeline` class drives it through: `Pipeline.apply()`
   * hands it `this._chunks` directly, and a caller running a `Transformer` standalone supplies its
   * own already-cut `AsyncIterable<In[]>`.
   *
   * `runHandler` is `Pipeline.onError()`'s own RUN handler - `Pipeline.apply()` passes its
   * `_runHandler` through here, and it reaches `runSequentially`'s per-chunk try/catch (above): no
   * handler means a chunk failure still propagates and ends the run; a handler that returns drops
   * the failing chunk and lets the run continue to the next one, and one that throws stops the run
   * with whatever it threw. Row-level recovery (`.onError()` on this `Transformer`) is separate and
   * always active regardless of `runHandler` - `this.runnable()` (above) is what wires it into
   * `transformerLogic` before the loop ever sees a chunk.
   *
   * @param chunks - Async iterable of already-cut input chunks
   * @param context - Optional context manager for sharing state
   * @param runHandler - `Pipeline.onError()`'s own run handler, forwarded by `Pipeline.apply()`
   * @returns Async generator of output chunks
   *
   * @example
   * ```ts
   * const t = new Transformer<number, number>().map((x) => x * 2);
   * for await (const chunk of t.process(chunksOf([1, 2, 3]))) console.log(chunk); // → [2, 4, 6]
   *
   * const recovered = new Transformer<string, number>()
   *   .onError(() => DROP)
   *   .map((s) => { const n = parseInt(s); if (isNaN(n)) throw new Error(`bad: ${s}`); return n; });
   * for await (const chunk of recovered.process(chunksOf(["a", "3"]))) console.log(chunk); // → [3]
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
   * Chains a new chunk-wise operation onto this transformer's own output, returning a NEW
   * `Transformer` that runs the current transform first and feeds its result into `operation` - the
   * one seam every configuration method (`.map()`, `.filter()`, `.onError()`, …) below composes
   * through.
   */
  protected pipe<U>(
    operation: (chunk: Out[], ctx: IContextManager, run?: RunScope) => U[] | Promise<U[]>,
  ): Transformer<In, U, M> {
    const currentTransform = this.transform;

    // `chain`, not `await`: a link whose own operation returns a plain array composes with the one
    // before it WITHOUT creating a `Promise`, so a chain of purely synchronous callbacks runs from
    // source to terminal op with no microtask at all. The moment any link returns a thenable,
    // `chain` defers through `.then` and every link after it composes asynchronously - that
    // deferral IS the run widening to async, at exactly the link that made it async.
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
   * Maps each element through `fn`, an optional per-row error handler applied per item.
   *
   * @param fn - Mapping function (context-aware if it declares a second parameter).
   * @returns New Transformer with map operation applied
   *
   * @example
   * `new Transformer<number, number>().map((x) => x * 2)` over `[1, 2, 3]` → `[2, 4, 6]`.
   */
  map<U>(fn: (item: Out, ctx: IContextManager) => Promise<U>): Transformer<In, U, "async">;
  map<U>(
    fn: (item: Out, ctx: IContextManager) => U extends Promise<unknown> ? never : U,
  ): Transformer<In, U, M>;
  map<U>(fn: (item: Out, ctx: IContextManager) => U): Transformer<In, U, "async">;
  map<U>(fn: PipelineFunction<Out, U>): Transformer<In, U, "sync" | "async"> {
    // ONE `call` closure picked once, context-aware or not, rather than the whole `pipe()` body
    // written twice per arm - `filter`/`flatMap`/`tap(fn)` below share this exact shape.
    const call = isContextAware(fn)
      ? (x: Out, ctx: IContextManager) => fn(x, ctx)
      : (x: Out, _ctx: IContextManager) => (fn as (item: Out) => U | Promise<U>)(x);
    return this.pipe((chunk, ctx, run) => {
      // No handler registered: the plain path - the seam costs nothing until `.onError()` is
      // actually called. `mapSettle`, not `Promise.all`: a chunk whose items all came back as
      // plain values is returned as-is, creating no `Promise` at all.
      if (!run?.rowHandler) {
        return mapSettle(chunk, (x) => call(x, ctx));
      }
      return settleRows(chunk, (x) => call(x, ctx), run.rowHandler, ctx);
    });
  }

  /**
   * Keeps only the elements `predicate` accepts.
   *
   * @param predicate - Filter function (context-aware if it declares a second parameter).
   * @returns New Transformer with filter operation applied
   *
   * @example
   * `new Transformer<number, number>().filter((x) => x > 1)` over `[1, 2, 3]` → `[2, 3]`.
   */
  filter(
    predicate: (item: Out, ctx: IContextManager) => Promise<boolean>,
  ): Transformer<In, Out, "async">;
  filter(predicate: (item: Out, ctx: IContextManager) => boolean): Transformer<In, Out, M>;
  filter(predicate: PipelineFunction<Out, boolean>): Transformer<In, Out, "sync" | "async"> {
    const call = isContextAware(predicate)
      ? (x: Out, ctx: IContextManager) => predicate(x, ctx)
      : (x: Out, _ctx: IContextManager) =>
          (predicate as (item: Out) => boolean | Promise<boolean>)(x);
    return this.pipe((chunk, ctx, run) => {
      if (!run?.rowHandler) {
        return chain(
          mapSettle(chunk, (x) => call(x, ctx)),
          (keep) => chunk.filter((_x, i) => keep[i]),
        );
      }
      return settleRows(
        chunk,
        (x) => chain(call(x, ctx), (keep) => (keep ? x : DROP)),
        run.rowHandler,
        ctx,
      );
    });
  }

  /**
   * Flattens one level of nested array/tuple/set output - `.map(fn).flatten()`'s own second half,
   * split out so a caller who already has array-shaped output need not repeat the map.
   *
   * @returns New Transformer with flattened output
   *
   * @example
   * `new Transformer<number, number[]>().map((x) => [x, x]).flatten()` over `[1, 2]` →
   * `[1, 1, 2, 2]`.
   */
  flatten<U>(this: Transformer<In, U[], M>): Transformer<In, U, M> {
    return this.pipe((chunk, _ctx) => chunk.flat());
  }

  /**
   * Maps each element through `fn`, then flattens the result by one level - equivalent to
   * `.map(fn).flatten()` in one link, so a row-level error handler applies to the whole map+flatten
   * step rather than the map alone.
   *
   * @param fn - Mapping function that returns an array (can be async)
   * @returns New Transformer with flatMap operation applied
   *
   * @example
   * `new Transformer<number, number>().flatMap((x) => [x, x])` over `[1, 2]` → `[1, 1, 2, 2]`.
   */
  flatMap<U>(fn: (item: Out, ctx: IContextManager) => Promise<U[]>): Transformer<In, U, "async">;
  flatMap<U>(fn: (item: Out, ctx: IContextManager) => U[]): Transformer<In, U, M>;
  flatMap<U>(fn: PipelineFunction<Out, U[]>): Transformer<In, U, "sync" | "async"> {
    const call = isContextAware(fn)
      ? (x: Out, ctx: IContextManager) => fn(x, ctx) as U[] | Promise<U[]>
      : (x: Out, _ctx: IContextManager) => (fn as (item: Out) => U[] | Promise<U[]>)(x);
    return this.pipe((chunk, ctx, run) => {
      if (!run?.rowHandler) {
        const results = mapSettle(chunk, (x) => call(x, ctx));
        return chain(results, (rows) => rows.flat());
      }
      return settleRowsFlat(chunk, (x) => call(x, ctx), run.rowHandler, ctx);
    });
  }

  /**
   * Observes each element (or, given a `Transformer`, the whole chunk) without changing the data -
   * the chain continues with the SAME values that went in.
   *
   * A `Transformer` argument runs chunk-aware, for side effects only, and stays chunk-aware
   * regardless of any row handler registered - a chunk-aware link cannot take per-row semantics. A
   * plain function argument runs per item, and DOES take the registered row handler like `.map()`.
   *
   * @example
   * `new Transformer<number, number>().tap((x) => console.log(x))` over `[1, 2]` passes `[1, 2]`
   * through unchanged, having logged both.
   */
  tap<R>(fn: (item: Out, ctx: IContextManager) => Promise<R>): Transformer<In, Out, "async">;
  tap(fn: (item: Out, ctx: IContextManager) => void): Transformer<In, Out, M>;
  tap(transformer: Transformer<Out, unknown, "async">): Transformer<In, Out, "async">;
  tap(transformer: Transformer<Out, unknown, "sync">): Transformer<In, Out, M>;
  tap(
    arg: PipelineFunction<Out, unknown> | Transformer<Out, unknown, "sync" | "async">,
  ): Transformer<In, Out, "sync" | "async"> {
    if (arg instanceof Transformer) {
      const tappedTransform = arg.transform;
      return this.pipe((chunk, ctx) =>
        // The tapped transformer runs for side effects only, and settles before the chunk moves on.
        // A tapped chain that is itself synchronous settles without a `Promise`.
        chain(tappedTransform(chunk, ctx), () => chunk),
      );
    }

    const fn = arg;
    const call = isContextAware(fn)
      ? (x: Out, ctx: IContextManager) => fn(x, ctx)
      : (x: Out, _ctx: IContextManager) => (fn as (item: Out) => void)(x);
    return this.pipe((chunk, ctx, run) => {
      if (!run?.rowHandler) {
        return chain(
          mapSettle(chunk, (x) => call(x, ctx)),
          () => chunk,
        );
      }
      return settleRows(chunk, (x) => chain(call(x, ctx), () => x), run.rowHandler, ctx);
    });
  }

  /**
   * Applies `fn` to this transformer and returns its result - a composition helper for extracting a
   * reusable chain of calls into a named function rather than inlining it.
   *
   * @param fn - Function that receives this transformer and returns a new one
   * @returns Result of applying the function to this transformer
   *
   * @example
   * `const double = (t) => t.map((x: number) => x * 2); new Transformer<number, number>().apply(double)`
   * behaves exactly like writing `.map((x) => x * 2)` directly.
   */
  apply<U, M2 extends "sync" | "async">(
    fn: (t: this) => Transformer<In, U, M2>,
  ): Transformer<In, U, M2> {
    return fn(this);
  }

  /**
   * Registers the ROW handler, returning a NEW transformer that carries it forward.
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
   * `.loop()`, which stay chunk-aware.
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
   * Repeatedly applies `loopTransformer` to this transformer's own output while `condition` is
   * true, stopping early once `maxIterations` is reached.
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
    // Reuses the same arity check every element-wise link already does, rather than re-inlining
    // `condition.length >= 2` - `condition` operates on a whole chunk, not one item, but
    // `isContextAware`'s own arity test is generic over the callback's first parameter's type.
    const conditionIsContextAware = isContextAware<Out[], boolean>(condition);

    // A real `while` loop carries the synchronous case, and `drain` re-enters itself ONLY across an
    // async boundary, where the continuation runs on a fresh stack in its own microtask. Recursing
    // per ITERATION instead overflows the stack on a synchronous looped transformer: measured, 4000
    // iterations over a one-link body threw `RangeError: Maximum call stack size exceeded` under a
    // per-iteration recursive form, and the ceiling falls further as the looped body gains links.
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
   * Folds this ONE chunk into one or more values via `emit` - no state survives to the next chunk;
   * `Pipeline.reduce()` is the only place cross-chunk state lives.
   *
   * The registered row handler (`.onError()`) reaches this fold's own per-item step too - a `fn`
   * that throws for one item hands that item to the handler; a returned value REPLACES the
   * accumulator directly (never re-runs `fn`, so a handler cannot cause a second throw), `DROP`
   * skips the item entirely (the accumulator is unchanged, and it does not count toward whether
   * `.final()` still owes a trailing value). `Pipeline.reduce()` never reaches this - it folds with
   * no `Transformer` in scope at all.
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
   * Throws once `fn` returns true, halting the run rather than producing a chunk - the earliest
   * point a chain can stop itself mid-transform based on the shared context.
   *
   * @param fn - Function that returns true to stop execution
   * @returns New transformer with short-circuit condition applied
   *
   * @example
   * `new Transformer<number, number>().shortCircuit((ctx) => ctx.get("stop") === true)` throws the
   * moment a chunk arrives with `stop` set in the context, instead of processing it.
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
