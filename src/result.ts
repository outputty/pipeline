/**
 * What calling a `Pipeline` produces: one call's output, over one input. The terminal ops live
 * here, so one chain serves many inputs: `score(a)` and `score(b)` are two results over one chain.
 *
 * A result is not chainable: `score(rows).transform(...)` is `TS2339`.
 */

import type { Drainable, PipelineMode } from "./types";
import type { Pipeline, PipelineSource } from "./pipeline";
import type { MaybeAsyncChunks } from "./utils/chunk";
import { isThenable } from "./utils/helpers";
import { collectItems, drainSyncSettled } from "./utils/chunk";

/** The pipeline shape a result drains, with the Mode erased. */
type BoundPipeline<T> = Pipeline<T, "sync" | "async", unknown>;

/**
 * One call of a `Pipeline` over one input.
 *
 * ⚠ Every terminal re-drains the input; replayability is not detectable, so none is attempted. An
 * array re-drains correctly, and a spent generator or stream yields `[]`.
 *
 * @example
 * `const r = score([1, 2, 3]); r.first(1)` → `[2]`, then `r.toArray()` → `[2, 4, 6]`.
 */
export class PipelineResult<T, M extends PipelineMode> {
  /** The chain, re-bound to `_input` once per terminal. */
  private readonly _pipeline: BoundPipeline<unknown>;
  /** The input this result was called with, kept so a terminal can re-run it. */
  private readonly _input: PipelineSource<unknown>;

  constructor(pipeline: BoundPipeline<unknown>, input: PipelineSource<unknown>) {
    this._pipeline = pipeline;
    this._input = input;
  }

  /** ⚠ Call once per terminal and thread both views through: a second call runs a `.local(build)`
   * callback twice. */
  private drainable(materialize = true): Drainable<T> {
    return this._pipeline.drainable(this._input, materialize) as Drainable<T>;
  }

  /**
   * Iterate the chunks this run produces, as `.buffer()` cut them, rather than its items.
   *
   * @example
   * `for await (const chunk of new Pipeline<number>().buffer(2)([1, 2, 3]).chunks())` yields
   * `[1, 2]`, then `[3]`.
   */
  async *chunks(): AsyncGenerator<T[]> {
    // ⚠ Empty chunks are dropped, or a sync source yields `[[],[],[],[15]]` where an async one
    // yields `[[15]]` for the same fold.
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

  private collect(limit: number | undefined): M extends "sync" ? T[] : Promise<T[]> {
    const { syncChunks, chunks } = this.drainable();
    return collectItems(syncChunks, chunks, limit) as M extends "sync" ? T[] : Promise<T[]>;
  }

  /**
   * Run the chain for its side effects, collecting nothing.
   *
   * @example
   * `score([1, 2, 3]).consume()` → `undefined`, every stage having run.
   */
  consume(): M extends "sync" ? void : Promise<void> {
    const { syncChunks, chunks } = this.drainable(false);
    return (
      syncChunks !== null ? drainSyncSettled(syncChunks, () => {}) : this.consumeAsync(chunks)
    ) as M extends "sync" ? void : Promise<void>;
  }

  private async consumeAsync(chunks: () => AsyncIterable<T[]>): Promise<void> {
    for await (const _chunk of chunks()) {
      // Every stage already ran to produce this chunk; there is nothing left to do with it.
    }
  }

  /**
   * Call `fn` for each item, in order.
   *
   * ⚠ The `Promise<void>` overload comes first: listed second, a `void` overload swallows an async
   * callback and types the call `void`, leaving it undrained.
   *
   * @example
   * `score([1, 2, 3]).forEach(write)` → `undefined`, `write` called with `2`, `4`, `6`.
   */
  forEach(fn: (item: T) => Promise<void>): Promise<void>;
  forEach(fn: (item: T) => void): M extends "sync" ? void : Promise<void>;
  forEach(fn: (item: T) => void | Promise<void>): void | Promise<void> {
    const { syncChunks, chunks } = this.drainable();
    // Each callback settles before the next item, so an async `forEach` runs in order and reports
    // its failures.
    return syncChunks !== null ? drainSyncSettled(syncChunks, fn) : this.forEachAsync(fn, chunks);
  }

  private async forEachAsync(
    fn: (item: T) => void | Promise<void>,
    chunks: () => AsyncIterable<T[]>,
  ): Promise<void> {
    for await (const chunk of chunks()) {
      await settleChunk(chunk, fn);
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
    if (syncChunks === null) {
      throw new TypeError(
        "an async pipeline result is not a sync iterable - use `for await`, or await .toArray()",
      );
    }
    return syncItems(syncChunks) as unknown as M extends "sync" ? Iterator<T> : never;
  }

  /**
   * Iterate the items asynchronously. Present on every result, sync ones included, so one loop
   * shape reads any chain.
   *
   * @example
   * `for await (const x of score([1, 2, 3]))` yields `2`, `4`, `6`.
   */
  async *[Symbol.asyncIterator](): AsyncGenerator<T> {
    for await (const chunk of this.drainable().chunks()) {
      yield* chunk;
    }
  }
}

/** ⚠ `isThenable`, not a bare `await`: awaiting a plain value allocates a `Promise` per row for a
 * synchronous callback. */
async function settleChunk<T>(chunk: T[], fn: (item: T) => void | Promise<void>): Promise<void> {
  for (let i = 0; i < chunk.length; i++) {
    const settled = fn(chunk[i]);
    if (isThenable(settled)) await settled;
  }
}

/**
 * ⚠ Lazy, not `toArray()[Symbol.iterator]()`: the eager form runs the whole chain before a `break`
 * and leaves the source open. A pending chunk throws, since there is no item to hand back.
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
