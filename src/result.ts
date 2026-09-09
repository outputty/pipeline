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

import type { PipelineMode, SourcePolicy } from "./types";
import type { Pipeline, PipelineSource } from "./pipeline";
import type { MaybeAsyncChunks } from "./utils/chunk";
import { chain, isThenable } from "./utils/helpers";
import { drainSync, drainSyncSettled } from "./utils/chunk";

/** The pipeline shape a result drains, with the Mode and policy erased - a result is handed its
 * pipeline by `Pipeline`'s own call signature, which has already fixed both. */
type BoundPipeline<T> = Pipeline<T, "sync" | "async", SourcePolicy, unknown>;

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

  /** Binds the input to the chain and returns the two views a terminal drains through. Runs once
   * per terminal call, which is what makes every terminal re-drain. */
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
    yield* this.drainable().chunks();
  }

  /**
   * Collect every item to an array. Synchronous when the chain and the input both are.
   *
   * @example
   * `score([1, 2, 3]).toArray()` → `[2, 4, 6]`, typed `number[]`, no `await`.
   */
  toArray(): M extends "sync" ? T[] : Promise<T[]> {
    const results: T[] = [];
    const { syncChunks } = this.drainable();
    if (syncChunks !== null) {
      return chain(
        drainSync(syncChunks, (item) => void results.push(item)),
        () => results,
      ) as M extends "sync" ? T[] : Promise<T[]>;
    }
    return this.collectAsync(results, undefined) as M extends "sync" ? T[] : Promise<T[]>;
  }

  /** The async engine's own collect loop, shared by `toArray` and `first` (#90) - `limit` is
   * `first`'s early exit, `undefined` for the whole stream. */
  private async collectAsync(results: T[], limit: number | undefined): Promise<T[]> {
    for await (const item of this.drainable().items()) {
      results.push(item);
      if (limit !== undefined && results.length >= limit) break;
    }
    return results;
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

    const results: T[] = [];
    const { syncChunks } = this.drainable();
    if (syncChunks !== null) {
      return chain(
        drainSync(syncChunks, (item) => {
          results.push(item);
          return results.length >= n;
        }),
        () => results,
      ) as M extends "sync" ? T[] : Promise<T[]>;
    }
    return this.collectAsync(results, n) as M extends "sync" ? T[] : Promise<T[]>;
  }

  /**
   * Run the chain for its side effects, collecting nothing.
   *
   * @example
   * `score([1, 2, 3]).consume()` → `undefined`, every stage having run.
   */
  consume(): M extends "sync" ? void : Promise<void> {
    const { syncChunks } = this.drainable();
    if (syncChunks !== null) {
      return drainSync(syncChunks, () => {}) as M extends "sync" ? void : Promise<void>;
    }
    return this.consumeAsync() as M extends "sync" ? void : Promise<void>;
  }

  /** `consume`'s async arm, split out so the method above stays a single expression per engine. */
  private async consumeAsync(): Promise<void> {
    for await (const _ of this.drainable().items()) {
      // Just consume, don't collect
    }
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
    const { syncChunks } = this.drainable();
    if (syncChunks !== null) {
      // Each callback's own return is settled before the next item, so a `forEach` that turns out
      // to be async still runs strictly in order and still reports its own failures.
      return drainSyncSettled(syncChunks, fn);
    }
    return this.forEachAsync(fn);
  }

  /** `forEach`'s async arm, which awaits each callback in turn. */
  private async forEachAsync(fn: (item: T) => void | Promise<void>): Promise<void> {
    for await (const item of this.drainable().items()) {
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
