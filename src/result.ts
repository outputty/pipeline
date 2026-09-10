/**
 * What calling a `Pipeline` produces (#90): one call's output, over one input.
 *
 * A `Pipeline` holds its input TYPE and no data, so it carries no terminal ops - it cannot be
 * drained without being given something to drain. Calling it pairs the chain with an input and
 * hands back this, which is where `toArray`/`first`/`consume`/`forEach` and both iteration
 * protocols live. The split is what keeps a chain reusable: `score(a)` and `score(b)` are two
 * results over one chain, not two chains.
 *
 * A result is not chainable. `score(rows).transform(...)` is `TS2339` - a chain is composed before
 * the data arrives, never after.
 */

import type { PipelineMode } from "./types";
import type { Pipeline, PipelineSource } from "./pipeline";
import type { MaybeAsyncChunks } from "./utils/chunk";
import { isThenable } from "./utils/helpers";
import { collectItems, drainSyncSettled } from "./utils/chunk";

/** The pipeline shape a result drains, with the Mode and policy erased - a result is handed its
 * pipeline by `Pipeline`'s own call signature, which has already fixed both. */
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
  /** The chain, still source-less - re-bound to `_input` once per terminal. */
  private readonly _pipeline: BoundPipeline<unknown>;
  /** The input this result was called with, kept rather than drained, so a terminal can re-run. */
  private readonly _input: PipelineSource<unknown>;

  constructor(pipeline: BoundPipeline<unknown>, input: PipelineSource<unknown>) {
    this._pipeline = pipeline;
    this._input = input;
  }

  /** Binds the input to the chain and returns the views a terminal drains through. Runs ONCE per
   * terminal call - which is what makes every terminal re-drain, and equally what stops one from
   * re-draining twice: each terminal destructures both halves here and threads `items` into its own
   * async arm. Calling it again there ran a user's `.local(build)` callback twice per call. */
  private drainable(): {
    syncChunks: MaybeAsyncChunks<T> | null;
    items: () => AsyncIterable<T>;
    chunks: () => AsyncIterable<T[]>;
  } {
    return this._pipeline.drainable(this._input) as {
      syncChunks: MaybeAsyncChunks<T> | null;
      items: () => AsyncIterable<T>;
      chunks: () => AsyncIterable<T[]>;
    };
  }

  /**
   * Iterate the CHUNKS this run produces, rather than its items - the boundary `.buffer(size)`
   * declared, as the chain actually cut it.
   *
   * `Pipeline` used to carry this as its own `[Symbol.asyncIterator]`. With no input on a chain
   * there is nothing to iterate, so it moved here with the rest of the drains, and the item-wise
   * `for await` above stays the default: a chunk view is the deliberate ask, never what a plain
   * loop hands you by accident.
   *
   * @example
   * `for await (const chunk of pipeline.buffer(2).chunks([1, 2, 3]))` yields `[1, 2]`, then `[3]`.
   */
  async *chunks(): AsyncGenerator<T[]> {
    // Empty chunks are dropped, so the two engines agree on what a consumer sees. A sync fold
    // cannot guard its own pending yields - emptiness is not knowable before a chunk settles -
    // so `.buffer(2).transform(t => t.map(async x => x)).reduce(sum, 0)` over `[1..5]` produced
    // `[[],[],[],[15]]` on a sync source against `[[15]]` on an async one.
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

  /** Collects up to `limit` items, `undefined` for the whole stream (#90) - `first` IS `toArray`
   * with an early exit, so the two engines' collect decision is made once here rather than twice
   * per method. Calls `drainable()` exactly once, like every other terminal. */
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
    if (syncChunks !== null) {
      // Each callback's own return is settled before the next item, so a `forEach` that turns out
      // to be async still runs strictly in order and still reports its own failures.
      return drainSyncSettled(syncChunks, fn);
    }
    return this.forEachAsync(fn, items);
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
    const drained = this.toArray();
    if (isThenable(drained)) {
      // The drain has already STARTED, so abandoning its promise here leaves a rejection nobody
      // handles - fatal under Node's default. Measured on an async chain whose map throws: the
      // `TypeError` below printed, then `UNHANDLED REJECTION: boom` killed the process.
      drained.catch(() => {});
      throw new TypeError(
        "an async pipeline result is not a sync iterable - use `for await`, or await .toArray()",
      );
    }
    return (drained as T[])[Symbol.iterator]() as unknown as M extends "sync" ? Iterator<T> : never;
  }

  /**
   * Iterate the ITEMS asynchronously. Present on every result, sync ones included, so one loop
   * shape reads any chain.
   *
   * ⚠ This yields items where `Pipeline`'s own `[Symbol.asyncIterator]` yields CHUNKS. The
   * divergence is deliberate: on a result, `[...r]` yields items and `forEach` receives items, so a
   * `for await` handing back an array would be the one loop out of three that reads differently.
   *
   * @example
   * `for await (const x of score([1, 2, 3]))` yields `2`, `4`, `6`.
   */
  async *[Symbol.asyncIterator](): AsyncGenerator<T> {
    yield* this.drainable().items();
  }
}
