/**
 * Transformer class - chainable chunk transformation operations.
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
import {
  withContext,
  dropOrRethrow,
  chain,
  mapSettle,
  settleMaybe,
  isThenable,
  tryRecover,
  disarm,
} from "./utils/helpers";
import { Reducer, foldChunk } from "./utils/reduce";

/**
 * ⚠ The catch sits inside the loop: an async generator that has thrown is finished, so a catch
 * around it could not continue past a failed chunk.
 */
async function* runSequentially<In, Out>(
  transformerLogic: InternalTransformer<In, Out>,
  chunks: AsyncIterable<In[]>,
  context: IContextManager,
  runHandler?: PipelineErrorHandler,
): AsyncGenerator<Out[]> {
  for await (const chunk of chunks) {
    try {
      const out = transformerLogic(chunk, context);
      yield isThenable(out) ? await out : out;
    } catch (error) {
      await dropOrRethrow(runHandler, error as Error, context);
    }
  }
}

function filterStep<T>(
  item: T,
  predicate: (item: T) => boolean | Promise<boolean>,
  kept: T[],
  tail: (boolean | Promise<boolean>)[] | undefined,
): (boolean | Promise<boolean>)[] | undefined {
  const result = predicate(item);
  if (tail) {
    tail.push(result);
    return tail;
  }
  if (isThenable(result)) return [result];
  if (result) kept.push(item);
  return undefined;
}

function filterSettle<T>(
  chunk: T[],
  predicate: (item: T) => boolean | Promise<boolean>,
): T[] | Promise<T[]> {
  const kept: T[] = [];
  let tail: (boolean | Promise<boolean>)[] | undefined;
  // ⚠ Recorded where tail starts, not derived as chunk.length - tail.length: that is wrong the
  // moment filterStep pushes anything but one entry per item.
  let tailStart = -1;
  try {
    for (let i = 0; i < chunk.length; i++) {
      const before = tail;
      tail = filterStep(chunk[i], predicate, kept, tail);
      tailStart = !before && tail ? i : tailStart;
    }
  } catch (error) {
    if (tail) disarm(tail);
    throw error;
  }
  if (!tail) return kept;
  return chain(settleMaybe(tail), (keep) => {
    for (let i = 0; i < keep.length; i++) {
      if (keep[i]) kept.push(chunk[tailStart + i]);
    }
    return kept;
  });
}

function settleRowStep<T, U>(
  item: T,
  attempt: (item: T) => U | typeof DROP | Promise<U | typeof DROP>,
  rowHandler: RowErrorHandler,
  ctx: IContextManager,
  kept: U[],
  tail: (U | typeof DROP | Promise<U | typeof DROP>)[] | undefined,
): (U | typeof DROP | Promise<U | typeof DROP>)[] | undefined {
  // A throw or a rejection hands the row to `rowHandler`, whose return is the row's result.
  const result = tryRecover(
    () => attempt(item),
    (error) => rowHandler(item, error, ctx) as U | typeof DROP | Promise<U | typeof DROP>,
  );
  if (tail) {
    tail.push(result);
    return tail;
  }
  if (isThenable(result)) return [result];
  if (result !== DROP) kept.push(result as U);
  return undefined;
}

/**
 * ⚠ The async arm settles every pending row before placing any: output order follows chunk index,
 * never settle order. A throwing `rowHandler` fails the whole chunk.
 */
function settleRows<T, U>(
  chunk: T[],
  attempt: (item: T) => U | typeof DROP | Promise<U | typeof DROP>,
  rowHandler: RowErrorHandler,
  ctx: IContextManager,
): U[] | Promise<U[]> {
  const kept: U[] = [];
  let tail: (U | typeof DROP | Promise<U | typeof DROP>)[] | undefined;
  try {
    for (let i = 0; i < chunk.length; i++) {
      tail = settleRowStep(chunk[i], attempt, rowHandler, ctx, kept, tail);
    }
  } catch (error) {
    if (tail) disarm(tail);
    throw error;
  }
  if (!tail) return kept;
  return chain(settleMaybe(tail), (rows) => {
    for (let i = 0; i < rows.length; i++) {
      if (rows[i] !== DROP) kept.push(rows[i] as U);
    }
    return kept;
  });
}

/**
 * A reusable chunk stage: `In` chunks in, `Out` chunks out. Every method returns a new
 * `Transformer`, so one can be shared across pipelines. How the input is cut is the `Pipeline`'s
 * decision.
 *
 * `new Transformer<number, number>().map((n) => n * 2)` → a transformer a pipeline can `.apply()`.
 */
export class Transformer<In, Out, M extends "sync" | "async" = "sync"> {
  /**
   * ⚠ Type-only, never assigned: without it two `Transformer`s differing only in `M` are the same
   * type, and the Mode never reaches `Pipeline.transform()`.
   */
  declare readonly __mode: M;

  /** The composed chunk transform. */
  readonly transform: InternalTransformer<In, Out>;

  /**
   * The row handler `.onError()` registered, or `undefined` for none.
   */
  readonly rowHandler?: RowErrorHandler;

  /** The context `.process()` uses when the caller passes none. */
  private defaultContext?: IContextManager;

  /**
   * Build from a real `transform`.
   *
   * ⚠ Not conditional on `In extends Out`, so it resolves inside a generic scope where overload 2
   * cannot.
   */
  constructor(options: TransformerOptions<In, Out> & { transform: InternalTransformer<In, Out> });
  /**
   * Build the identity transformer, allowed only when `In` is assignable to `Out`.
   * `new Transformer<number, number>()` compiles; `new Transformer<number, { id: number }>()` does
   * not.
   *
   * ⚠ Resolves only at concrete types; a generic scope must pass a `transform` through overload 1.
   */
  constructor(...args: In extends Out ? [options?: TransformerOptions<In, Out>] : never);
  constructor(options?: TransformerOptions<In, Out>) {
    this.transform = options?.transform ?? ((chunk, _ctx) => chunk as unknown as Out[]);
    this.rowHandler = options?.rowHandler;
  }

  /**
   * This transformer as one chunk-transform function, with its row handler applied.
   *
   * `t.onError(() => DROP).map(parseStrict).runnable()(["a", "3"], ctx)` → `[3]`.
   */
  runnable(): InternalTransformer<In, Out> {
    const run: RunScope = { rowHandler: this.rowHandler };
    return (chunk, ctx) => this.transform(chunk, ctx, run);
  }

  /**
   * Run this transformer over chunks the caller already cut, one chunk at a time, in order.
   * `runHandler` decides a failed chunk: return to drop it, throw to stop.
   *
   * @example
   * ```typescript
   * async function* chunksOf<T>(...chunks: T[][]) { yield* chunks; }
   * const t = new Transformer<number, number>().map((x) => x * 2);
   * for await (const chunk of t.process(chunksOf([1, 2, 3]))) console.log(chunk); // [2, 4, 6]
   * ```
   */
  async *process(
    chunks: AsyncIterable<In[]>,
    context?: IContextManager,
    runHandler?: PipelineErrorHandler,
  ): AsyncGenerator<Out[]> {
    const runContext = context ?? (this.defaultContext ??= new SimpleContextManager());
    yield* runSequentially(this.runnable(), chunks, runContext, runHandler);
  }

  /**
   * A new transformer that runs `operation` on this one's output, keeping the row handler.
   */
  protected pipe<U>(
    operation: (chunk: Out[], ctx: IContextManager, run?: RunScope) => U[] | Promise<U[]>,
  ): Transformer<In, U, M> {
    const currentTransform = this.transform;

    // ⚠ `chain`, not `await`: `await` creates a `Promise` per link, so a fully synchronous chain
    // would stop being synchronous.
    const newTransform: InternalTransformer<In, U> = (chunk, ctx, run) =>
      chain(currentTransform(chunk, ctx, run), (intermediate) => operation(intermediate, ctx, run));

    return new Transformer<In, U, M>({
      transform: newTransform,
      // Carried forward, so `t.onError(fn).map(g)` and `t.map(g).onError(fn)` behave the same.
      rowHandler: this.rowHandler,
    });
  }

  /**
   * Map each item through `fn`, which may read the context and may be async.
   *
   * `.map((x) => x * 2)` over `[1, 2, 3]` → `[2, 4, 6]`.
   */
  map<U>(fn: (item: Out, ctx: IContextManager) => Promise<U>): Transformer<In, U, "async">;
  map<U>(
    fn: (item: Out, ctx: IContextManager) => U extends Promise<unknown> ? never : U,
  ): Transformer<In, U, M>;
  map<U>(fn: (item: Out, ctx: IContextManager) => U): Transformer<In, U, "async">;
  map<U>(fn: PipelineFunction<Out, U>): Transformer<In, U, "sync" | "async"> {
    const call = withContext(fn);
    return this.pipe((chunk, ctx, run) => {
      // ⚠ `mapSettle`, not `Promise.all`: `Promise.all` makes a fully synchronous chunk async.
      if (!run?.rowHandler) {
        return mapSettle(chunk, (x) => call(x, ctx));
      }
      return settleRows(chunk, (x) => call(x, ctx), run.rowHandler, ctx);
    });
  }

  /**
   * Keep the items `predicate` accepts. The predicate may read the context and may be async.
   *
   * `.filter((x) => x > 1)` over `[1, 2, 3]` → `[2, 3]`.
   */
  filter(
    predicate: (item: Out, ctx: IContextManager) => Promise<boolean>,
  ): Transformer<In, Out, "async">;
  filter(predicate: (item: Out, ctx: IContextManager) => boolean): Transformer<In, Out, M>;
  filter(predicate: PipelineFunction<Out, boolean>): Transformer<In, Out, "sync" | "async"> {
    const call = withContext(predicate);
    return this.pipe((chunk, ctx, run) => {
      if (!run?.rowHandler) {
        return filterSettle(chunk, (x) => call(x, ctx));
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
   * Flatten one level of nested arrays.
   *
   * `.flatten()` over `[[1, 2], [3]]` → `[1, 2, 3]`.
   */
  flatten<U>(this: Transformer<In, U[], M>): Transformer<In, U, M> {
    return this.pipe((chunk, _ctx) => chunk.flat());
  }

  /**
   * Map each item to an array and flatten the results. `fn` may be async.
   *
   * `.flatMap((x) => [x, x])` over `[1, 2]` → `[1, 1, 2, 2]`.
   */
  flatMap<U>(fn: (item: Out, ctx: IContextManager) => Promise<U[]>): Transformer<In, U, "async">;
  flatMap<U>(fn: (item: Out, ctx: IContextManager) => U[]): Transformer<In, U, M>;
  flatMap<U>(fn: PipelineFunction<Out, U[]>): Transformer<In, U, "sync" | "async"> {
    const call = withContext(fn);
    return this.pipe((chunk, ctx, run) => {
      if (!run?.rowHandler) {
        const results = mapSettle(chunk, (x) => call(x, ctx));
        return chain(results, (rows) => rows.flat());
      }
      // ⚠ A recovered value is wrapped as one row, even when it is an array; wrapping a success
      // instead would nest it.
      const { rowHandler } = run;
      const wrapped: RowErrorHandler = (item, error, rowCtx) =>
        chain(rowHandler(item, error, rowCtx), (recovered) =>
          recovered === DROP ? DROP : [recovered],
        );
      return chain(
        settleRows(chunk, (x) => call(x, ctx), wrapped, ctx),
        (rows) => rows.flat(),
      );
    });
  }

  /**
   * Run a side effect without changing the data: `fn` per item, or a `Transformer` per chunk.
   *
   * `.tap((x) => console.log(x))` over `[1, 2]` → `[1, 2]`, having logged each item.
   */
  tap<R>(fn: (item: Out, ctx: IContextManager) => Promise<R>): Transformer<In, Out, "async">;
  tap(fn: (item: Out, ctx: IContextManager) => void): Transformer<In, Out, M>;
  tap(transformer: Transformer<Out, unknown, "async">): Transformer<In, Out, "async">;
  tap(transformer: Transformer<Out, unknown, "sync">): Transformer<In, Out, M>;
  tap(
    arg: PipelineFunction<Out, unknown> | Transformer<Out, unknown, "sync" | "async">,
  ): Transformer<In, Out, "sync" | "async"> {
    // A tapped `Transformer` sees whole chunks, so the row handler does not reach it.
    if (arg instanceof Transformer) {
      const tappedTransform = arg.transform;
      return this.pipe((chunk, ctx) =>
        // The tapped transformer settles before the chunk moves on.
        chain(tappedTransform(chunk, ctx), () => chunk),
      );
    }

    const call = withContext(arg);
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
   * Pass this transformer to `fn` and return what it builds, so a reusable chain reads inline.
   *
   * `t.apply((x) => x.map(double))` → the same as `t.map(double)`.
   */
  apply<U, M2 extends "sync" | "async">(
    fn: (t: this) => Transformer<In, U, M2>,
  ): Transformer<In, U, M2> {
    return fn(this);
  }

  /**
   * Recover a failing row: `handler` returns a replacement value, returns `DROP` to remove it, or
   * throws to fail the chunk. Covers `.map()`, `.filter()`, `.flatMap()`, `.tap(fn)`, `.reduce()`.
   *
   * Position does not matter: `t.onError(h).map(f)` equals `t.map(f).onError(h)`. A second call
   * replaces the first.
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
   * Re-run `loopTransformer` on each chunk while `condition` holds, up to `maxIterations` times.
   *
   * `.loop(new Transformer<number, number>().map((x) => x * 2), (c) => c[0] < 10)` over `[1]` →
   * `[16]`.
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
    const shouldLoop = withContext(condition);

    // ⚠ A `while` loop, recursing only across an async boundary: recursing per iteration overflows
    // the stack on a synchronous looped transformer.
    const drain = (
      startChunk: Out[],
      ctx: IContextManager,
      startIteration: number,
    ): Out[] | Promise<Out[]> => {
      let currentChunk = startChunk;
      let iterations = startIteration;

      while (true) {
        if (maxIterations !== undefined && iterations >= maxIterations) return currentChunk;

        if (!shouldLoop(currentChunk, ctx)) return currentChunk;

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
   * Fold each chunk on its own into the values `fn` emits, plus the final accumulator. No state
   * survives to the next chunk; `Pipeline.reduce()` folds across chunks.
   *
   * An empty chunk emits `initial`. A row handler's return replaces the accumulator, and `DROP`
   * skips the item.
   *
   * @example
   * `new Transformer<number, number>().reduce((acc, x) => acc + x, 0)` over chunks `[[1,2],[3]]` →
   * `[3]` then `[3]` (each chunk's own independent sum). Over `[[], [1,2]]` → `[0]` then `[3]`.
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
      const reducer = new Reducer<U, Out>(fn, initial, run?.rowHandler);
      return chain(foldChunk(reducer, chunk, ctx), (values) => {
        values.push(...reducer.final(true));
        return values;
      });
    });
  }

  /**
   * Fail the chunk once `fn` returns true for the context, which stops the run unless a run handler
   * drops it.
   *
   * `.shortCircuit((ctx) => ctx.get("stop") === true)` → throws "Short-circuit condition met…" once
   * `stop` is set.
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
