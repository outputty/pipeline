/** The cutters behind `.buffer(fn)`: each runs `fn` over every item, one pending chunk per run. */

import type { BufferFunction, IContextManager } from "@src/types";
import { DROP } from "@src/types";
import { isThenable } from "@src/utils/helpers";
import type { MaybeAsyncChunks } from "@src/utils/drain";

/** One run's pending chunk and the `emit()` that closes it. */
interface Cut<T> {
  /** Calls `fn` on `item` with this run's `emit()`. */
  call(item: T): T | typeof DROP | Promise<T | typeof DROP>;
  /** Appends `verdict` unless it is `DROP`, and returns the chunk `emit()` closed since the last
   * settle, or `null`. */
  settle(verdict: T | typeof DROP): T[] | null;
  /** The items not yet closed into a chunk. */
  pending(): T[];
  /** Whether an `emit()` found nothing to close since the last call, which it resets. */
  flushedEmpty(): boolean;
}

function openCut<T>(fn: BufferFunction<T>, ctx: IContextManager): Cut<T> {
  let pending: T[] = [];
  let closed: T[] | null = null;
  let flushedEmpty = false;
  // ⚠ Flush, then append: `settle` pushes the item after `emit()` swapped `pending` out, so the
  // item whose callback calls `emit()` opens the next chunk.
  const emit = (): void => {
    if (pending.length === 0) {
      flushedEmpty = true;
      return;
    }
    closed = pending;
    pending = [];
  };
  return {
    call: (item) => fn(item, ctx, emit),
    settle: (verdict) => {
      if (verdict !== DROP) pending.push(verdict);
      const out = closed;
      closed = null;
      return out;
    },
    pending: () => pending,
    flushedEmpty: () => {
      const flushed = flushedEmpty;
      flushedEmpty = false;
      return flushed;
    },
  };
}

/**
 * Cuts a synchronous item stream with `fn`. It stays synchronous until `fn` first returns a
 * thenable.
 *
 * ⚠ From that item on, every item yields exactly one slot, `[]` when it closed nothing, and the
 * pending chunk is always yielded last. A downstream stage sees each slot, so collapsing the empty
 * ones changes what a per-chunk `Transformer.reduce` returns.
 *
 * `[...cutSyncItemsWith(everySecondItem, ctx)([1, 2, 3])]` → `[[1, 2], [3]]`, no `Promise` created.
 */
export function cutSyncItemsWith<T>(
  fn: BufferFunction<T>,
  ctx: IContextManager,
): (data: Iterable<T>) => MaybeAsyncChunks<T> {
  return function* bufferGenerator(data: Iterable<T>): MaybeAsyncChunks<T> {
    const cut = openCut(fn, ctx);
    let slotPerItem = false;
    const settleSlot = (settled: T | typeof DROP): T[] => cut.settle(settled) ?? [];
    // What one item yields: a closed chunk, a slot once any verdict was a thenable, else nothing.
    const step = (item: T): T[] | Promise<T[]> | null => {
      const verdict = cut.call(item);
      if (isThenable(verdict)) {
        slotPerItem = true;
        return Promise.resolve(verdict).then(settleSlot);
      }
      return cut.settle(verdict) ?? (slotPerItem ? [] : null);
    };

    // ⚠ Index an array: `for...of` over one is slower. Any other iterable keeps `for...of`, which
    // closes it when the consumer stops early.
    yield* Array.isArray(data) ? stepIndexed(data as T[], step) : stepEach(data, step);

    const pending = cut.pending();
    if (slotPerItem || pending.length > 0) yield pending;
  };
}

function* stepIndexed<T>(
  items: T[],
  step: (item: T) => T[] | Promise<T[]> | null,
): Generator<T[] | Promise<T[]>> {
  for (let i = 0; i < items.length; i++) {
    const out = step(items[i]);
    if (out !== null) yield out;
  }
}

function* stepEach<T>(
  items: Iterable<T>,
  step: (item: T) => T[] | Promise<T[]> | null,
): Generator<T[] | Promise<T[]>> {
  for (const item of items) {
    const out = step(item);
    if (out !== null) yield out;
  }
}

/**
 * Cuts an async item stream with `fn`: every flush is its own chunk, and the pending remainder is
 * the last. Empty chunks are never yielded.
 *
 * `cutItemsWith(everySecondItem, ctx)` over items `1, 2, 3` → yields `[1, 2]` then `[3]`.
 */
export function cutItemsWith<T>(
  fn: BufferFunction<T>,
  ctx: IContextManager,
): (data: AsyncIterable<T>) => AsyncGenerator<T[]> {
  return async function* bufferGenerator(data: AsyncIterable<T>): AsyncGenerator<T[]> {
    const cut = openCut(fn, ctx);
    for await (const item of data) {
      const verdict = cut.call(item);
      const out = cut.settle(isThenable(verdict) ? await verdict : verdict);
      if (out !== null) yield out;
    }
    const pending = cut.pending();
    if (pending.length > 0) yield pending;
  };
}

/**
 * `cutItemsWith` over a stage's async chunk stream, looping inside each chunk rather than
 * flattening it to one item per pull. The chunks it yields are the same.
 *
 * `recutChunksWith(everySecondItem, ctx)` over chunks `[1]` then `[2, 3]` → yields `[1, 2]` then
 * `[3]`.
 */
export function recutChunksWith<T>(
  fn: BufferFunction<T>,
  ctx: IContextManager,
): (chunks: AsyncIterable<T[]>) => AsyncGenerator<T[]> {
  return async function* bufferGenerator(chunks: AsyncIterable<T[]>): AsyncGenerator<T[]> {
    const cut = openCut(fn, ctx);
    for await (const chunk of chunks) yield* closedIn(cut, chunk);
    const pending = cut.pending();
    if (pending.length > 0) yield pending;
  };
}

/** The chunks `cut` closes over `items`, in order, awaiting only a verdict that is a thenable. */
async function* closedIn<T>(cut: Cut<T>, items: T[]): AsyncGenerator<T[]> {
  for (let i = 0; i < items.length; i++) {
    const verdict = cut.call(items[i]);
    const out = cut.settle(isThenable(verdict) ? await verdict : verdict);
    if (out !== null) yield out;
  }
}

/** One incoming slot's chunks: `head` is what its first `emit()` closed (`[]` when that closed
 * nothing, `null` when none ran), `rest` every later chunk it closed. */
interface SlotCut<T> {
  head: T[] | null;
  rest: T[][] | null;
}

/**
 * Re-cuts a stage's output chunks with `fn`, for a `.buffer(fn)` after that stage. Each incoming
 * slot runs whole before any chunk it closed is yielded. It stays synchronous until a slot is
 * pending or `fn` returns a thenable.
 *
 * ⚠ From that slot on, every slot yields one leading slot: the first chunk an `emit()` closed in
 * it, or `[]` when that `emit()` found nothing to close or none ran. Its other chunks follow, and
 * the pending chunk is always yielded last. A downstream stage sees each of these slots.
 *
 * `[...recutSyncChunksWith([[1, 2], [3]], everySecondItem, ctx)]` → `[[1, 2], [3]]`, no `Promise`
 * created.
 */
export function* recutSyncChunksWith<T>(
  chunks: MaybeAsyncChunks<T>,
  fn: BufferFunction<T>,
  ctx: IContextManager,
): MaybeAsyncChunks<T> {
  const cut = openCut(fn, ctx);

  const record = (result: SlotCut<T>, out: T[] | null): void => {
    const flushedEmpty = cut.flushedEmpty();
    if (result.head === null) {
      if (out !== null) result.head = out;
      else if (flushedEmpty) result.head = [];
    } else if (out !== null) {
      (result.rest ??= []).push(out);
    }
  };

  // ⚠ Re-enters itself only after an async verdict. Recursing per item overflows the stack on a
  // large chunk.
  const run = (result: SlotCut<T>, items: T[], start: number): void | Promise<void> => {
    for (let i = start; i < items.length; i++) {
      const verdict = cut.call(items[i]);
      if (isThenable(verdict)) {
        return Promise.resolve(verdict).then((settled) => {
          record(result, cut.settle(settled));
          return run(result, items, i + 1);
        });
      }
      record(result, cut.settle(verdict));
    }
    return undefined;
  };

  let slotPerSlot = false;
  // A pending slot's result. ⚠ Its `rest` is read at the next pull, so it relies on the caller
  // settling a yielded slot before pulling the next one, as `recutPending` does.
  let settling: SlotCut<T> | null = null;

  for (const slot of chunks) {
    if (settling?.rest) yield* settling.rest;
    settling = null;
    const result: SlotCut<T> = { head: null, rest: null };
    cut.flushedEmpty();

    const ran = isThenable(slot)
      ? Promise.resolve(slot).then((items) => run(result, items, 0))
      : run(result, slot, 0);
    if (isThenable(ran)) {
      slotPerSlot = true;
      settling = result;
      yield Promise.resolve(ran).then(() => result.head ?? []);
      continue;
    }
    if (slotPerSlot) yield result.head ?? [];
    else if (result.head !== null && result.head.length > 0) yield result.head;
    if (result.rest !== null) yield* result.rest;
  }

  if (settling?.rest) yield* settling.rest;
  const pending = cut.pending();
  if (slotPerSlot || pending.length > 0) yield pending;
}
