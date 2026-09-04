/**
 * Concurrent execution strategy - processes chunks in parallel with p-limit.
 *
 * Python equivalent:
 * ```python
 * class ThreadedStrategy[In, Out](ExecutionStrategy[In, Out]):
 *   def __init__(self, max_workers: int = 4, ordered: bool = True):
 *     self.max_workers = max_workers
 *     self.ordered = ordered
 *
 *   def execute(self, transformer_logic, chunks, context):
 *     with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
 *       gen_func = self._ordered_generator if self.ordered else self._unordered_generator
 *       yield from gen_func(chunks, transformer_logic, executor, context)
 * ```
 *
 * In TypeScript, we use p-limit for concurrency control since Node.js is single-threaded.
 */

import pLimit, { type LimitFunction } from "p-limit";
import type {
  ExecutionStrategy,
  InternalTransformer,
  IContextManager,
  ConcurrentStrategyOptions,
} from "@src/types";

/**
 * Runs up to `options.maxConcurrency` chunks through the transformer at once, while still emitting
 * them in their original order by default — a slow chunk holds back only the results behind it, not
 * the workers. Use it when the per-chunk work is I/O-bound; `sequential` is the default otherwise.
 *
 * Validates `maxConcurrency` eagerly, at call time — `concurrent({ maxConcurrency: 0 })` throws
 * before `.withExecutor()` ever runs the strategy, matching the class constructor this replaces.
 * `pLimit` is created fresh inside the returned generator, so each run of the same `concurrent(...)`
 * result starts with an empty limiter rather than sharing one across runs.
 *
 * `concurrent({ maxConcurrency: 10 })` → an `ExecutionStrategy` a `.withExecutor()` call passes
 * straight through.
 */
export function concurrent<In, Out>(
  options?: ConcurrentStrategyOptions,
): ExecutionStrategy<In, Out> {
  const maxConcurrency = options?.maxConcurrency ?? 4;
  if (maxConcurrency < 1) {
    throw new Error("maxConcurrency must be at least 1");
  }
  const ordered = options?.ordered ?? true;

  return async function* (transformerLogic, chunks, context) {
    const limit = pLimit(maxConcurrency);
    if (ordered) {
      yield* orderedExecution(transformerLogic, chunks, context, limit, maxConcurrency);
    } else {
      yield* unorderedExecution(transformerLogic, chunks, context, limit);
    }
  };
}

/**
 * Ordered execution - maintains input order in output.
 *
 * Python equivalent:
 * ```python
 * def _ordered_generator(self, chunks, logic, executor, ctx):
 *   futures = []
 *   for chunk in chunks:
 *     futures.append(executor.submit(logic, chunk, ctx))
 *     while len(futures) >= self.max_workers:
 *       yield futures.pop(0).result()
 *   for future in futures:
 *     yield future.result()
 * ```
 */
async function* orderedExecution<In, Out>(
  transformerLogic: InternalTransformer<In, Out>,
  chunks: AsyncIterable<In[]>,
  context: IContextManager,
  limit: LimitFunction,
  maxConcurrency: number,
): AsyncGenerator<Out[]> {
  const promises: Promise<Out[]>[] = [];

  for await (const chunk of chunks) {
    promises.push(limit(() => Promise.resolve(transformerLogic(chunk, context))));

    // Yield completed results while maintaining order
    while (promises.length >= maxConcurrency) {
      yield await promises.shift()!;
    }
  }

  // Yield remaining results in order
  for (const promise of promises) {
    yield await promise;
  }
}

/**
 * Unordered execution - yields results as they complete.
 *
 * Python equivalent:
 * ```python
 * def _unordered_generator(self, chunks, logic, executor, ctx):
 *   futures = set()
 *   for chunk in chunks:
 *     futures.add(executor.submit(logic, chunk, ctx))
 *     if len(futures) >= self.max_workers:
 *       done, futures = wait(futures, return_when=FIRST_COMPLETED)
 *       for future in done:
 *         yield future.result()
 *   for future in as_completed(futures):
 *     yield future.result()
 * ```
 */
async function* unorderedExecution<In, Out>(
  transformerLogic: InternalTransformer<In, Out>,
  chunks: AsyncIterable<In[]>,
  context: IContextManager,
  limit: LimitFunction,
): AsyncGenerator<Out[]> {
  // Collect all chunks and process concurrently
  const allChunks: In[][] = [];
  for await (const chunk of chunks) {
    allChunks.push(chunk);
  }

  if (allChunks.length === 0) {
    return;
  }

  // Create all promises with concurrency limit
  const promisesWithIndex = allChunks.map((chunk, index) => ({
    promise: limit(() => Promise.resolve(transformerLogic(chunk, context))),
    index,
  }));

  // Yield as they complete (unordered)
  const remaining = new Set(promisesWithIndex);

  while (remaining.size > 0) {
    // Race all remaining promises
    const { result, item } = await Promise.race(
      Array.from(remaining).map(async (item) => ({
        result: await item.promise,
        item,
      })),
    );
    remaining.delete(item);
    yield result;
  }
}
