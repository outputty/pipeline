
import type { Drainable, PipelineMode } from "./types";
import type { Pipeline, PipelineSource } from "./pipeline";
import type { MaybeAsyncChunks } from "./utils/chunk";
import { isThenable } from "./utils/helpers";
import { collectItems, drainSyncSettled } from "./utils/chunk";
import { dispatchSync } from "./utils/drain";

/** The pipeline shape a `PipelineResult` drains, with the Mode and dispatch policy erased to a
 * plain `"sync" | "async"` union: a result is handed its pipeline by `Pipeline`'s own call
 * signature, which has already fixed both, so nothing downstream needs to track which one it got. */
type BoundPipeline<T> = Pipeline<T, "sync" | "async", unknown>;

/**
 * One call of a `Pipeline` over one input.
 *
 * Every terminal RE-DRAINS: it re-binds the input to the chain and runs it again. Replayability
 * cannot be detected at runtime - the `src[Symbol.iterator]() === src` test agrees with reality on
 * arrays, `Set`s, strings, custom iterables, generators and `Map.values()`, then reports a
 * `ReadableStream` as replayable when a second drain yields `[]`, and merely running the test locks
 * the stream so the FIRST drain throws. So no detection is attempted: an array or a `Set` re-drains
 * correctly, and a spent generator or stream yields `[]`.
 *
 * @example
 * `const r = score([1, 2, 3]); r.first(1)` → `[2]`, then `r.toArray()` → `[2, 4, 6]`.
 */
export class PipelineResult<T, M extends PipelineMode> {
  private readonly _pipeline: BoundPipeline<unknown>;
  private readonly _input: PipelineSource<unknown>;

  /** Pairs a chain with the one input it will run over for this call. */
  constructor(pipeline: BoundPipeline<unknown>, input: PipelineSource<unknown>) {
    this._pipeline = pipeline;
    this._input = input;
  }

  /**
   * Binds this result's input to its pipeline and returns the drain view a terminal reads from.
   * Called exactly once per terminal call, so each terminal re-drains independently without
   * running a user's `.local(build)` callback twice for the same call.
   *
   * The returned `context` field goes unread here; only `branch.ts`'s own `runBranch` needs it.
   */
  private drainable(): Drainable<T> {
    return this._pipeline.drainable(this._input) as Drainable<T>;
  }

  /**
   * Iterate the CHUNKS this run produces, rather than its items - the boundary `.buffer(size)`
   * declared, as the chain actually cut it.
   *
   * A chunk view is the deliberate ask, never what a plain `for await` hands you by accident; the
   * item-wise loop (`[Symbol.asyncIterator]`, below) stays the default.
   *
   * @example
   * `for await (const chunk of pipeline.buffer(2)([1, 2, 3]).chunks())` yields `[1, 2]`, then `[3]`.
   */
  async *chunks(): AsyncGenerator<T[]> {
    // Empty chunks are dropped, so the two engines agree on what a consumer sees. A sync fold
    // cannot guard its own pending yields, since emptiness is not knowable before a chunk settles.
    for await (const chunk of this.drainable().chunks()) {
      if (chunk.length > 0) yield chunk;
    }
  }

  /**
   * Collect every item to an array. Synchronous when the chain and the input both are.
   *
   * @example
   * `score([1, 2, 3]).toArray()` → `[2, 4, 6]`, typed `number[]`, no `await`.
   */
  toArray(): M extends "sync" ? T[] : Promise<T[]> {
    return this.collect(undefined);
  }

  /**
   * Collect the first `n` items, stopping the drain once it has them.
   *
   * @example
   * `score([1, 2, 3]).first(2)` → `[2, 4]`.
   */
  first(n = 1): M extends "sync" ? T[] : Promise<T[]> {
    if (n < 1) {
      throw new Error("n must be at least 1");
    }
    return this.collect(n);
  }

  /** Collects up to `limit` items, `undefined` for the whole stream - `first` IS `toArray` with an
   * early exit, so the two engines' collect decision is made once here rather than twice per
   * method. Calls `drainable()` exactly once, like every other terminal. */
  private collect(limit: number | undefined): M extends "sync" ? T[] : Promise<T[]> {
    const { syncChunks, items } = this.drainable();
    return collectItems(syncChunks, items, limit) as M extends "sync" ? T[] : Promise<T[]>;
  }

  /**
   * Run the chain for its side effects, collecting nothing.
   *
   * `forEach` with a no-op callback IS this, on both engines: it drains the same stream and settles
   * nothing, since a no-op returns no thenable.
   *
   * @example
   * `score([1, 2, 3]).consume()` → `undefined`, every stage having run.
   */
  consume(): M extends "sync" ? void : Promise<void> {
    return this.forEach(() => {});
  }

  /**
   * Call `fn` for each item, in order.
   *
   * The `Promise<void>` arm is declared FIRST because the void-return rule makes an `async` callback
   * assignable to `(item: T) => void`: a `void` arm listed first would swallow it, type the call
   * `void`, and leave every callback fired and undrained.
   *
   * @example
   * `score([1, 2, 3]).forEach(write)` → `undefined`, `write` called with `2`, `4`, `6`.
   */
  forEach(fn: (item: T) => Promise<void>): Promise<void>;
  forEach(fn: (item: T) => void): M extends "sync" ? void : Promise<void>;
  forEach(fn: (item: T) => void | Promise<void>): void | Promise<void> {
    const { syncChunks, items } = this.drainable();
    // Each callback's own return is settled before the next item, so a `forEach` that turns out to
    // be async still runs strictly in order and still reports its own failures.
    return dispatchSync(
      syncChunks,
      (chunks) => drainSyncSettled(chunks, fn),
      () => this.forEachAsync(fn, items),
    );
  }

  /** `forEach`'s async arm, which awaits each callback in turn. */
  private async forEachAsync(
    fn: (item: T) => void | Promise<void>,
    items: () => AsyncIterable<T>,
  ): Promise<void> {
    for await (const item of items()) {
      await fn(item);
    }
  }

  /**
   * Iterate the items synchronously. Present only on a `"sync"` result: `[...score(stream)]` is
   * `TS2488`, since an async chain has nothing to hand back item by item without awaiting.
   *
   * @example
   * `[...score([1, 2, 3])]` → `[2, 4, 6]`.
   */
  [Symbol.iterator](): M extends "sync" ? Iterator<T> : never {
    const { syncChunks } = this.drainable();
    return dispatchSync(
      syncChunks,
      (chunks) => syncItems(chunks),
      () => {
        throw new TypeError(
          "an async pipeline result is not a sync iterable - use `for await`, or await .toArray()",
        );
      },
    ) as unknown as M extends "sync" ? Iterator<T> : never;
  }

  /**
   * Iterate the ITEMS asynchronously. Present on every result, sync ones included, so one loop
   * shape reads any chain.
   *
   * ⚠ This yields items where this SAME result's own `.chunks()` yields CHUNKS. The divergence is
   * deliberate: `[...r]` yields items and `forEach` receives items, so a `for await` handing back
   * an array would be the one loop out of three that reads differently.
   *
   * @example
   * `for await (const x of score([1, 2, 3]))` yields `2`, `4`, `6`.
   */
  async *[Symbol.asyncIterator](): AsyncGenerator<T> {
    yield* this.drainable().items();
  }
}

/**
 * A `"sync"` result's items, yielded lazily - what `[Symbol.iterator]` hands back.
 *
 * Lazy, not `toArray()[Symbol.iterator]()`: a generator's own `return()` runs its `finally`, which
 * closes the chunk iterator exactly as an early `.first(n)` does, so both of a result's iteration
 * protocols agree on when the source closes.
 *
 * A pending chunk throws rather than blocking: the type says `"sync"`, so reaching one means an
 * `any` boundary let an async callback through, and there is nothing to hand back item by item.
 */
function* syncItems<T>(chunks: MaybeAsyncChunks<T>): Generator<T> {
  for (const chunk of chunks) {
    if (isThenable(chunk)) {
      // Nothing else will await it, and an abandoned rejection is fatal under Node's default.
      void Promise.resolve(chunk).catch(() => {});
      throw new TypeError(
        "an async pipeline result is not a sync iterable - use `for await`, or await .toArray()",
      );
    }
    yield* chunk;
  }
}
