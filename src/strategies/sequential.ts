/**
 * Sequential execution strategy - processes chunks one at a time.
 *
 * Python equivalent:
 * ```python
 * class SequentialStrategy[In, Out](ExecutionStrategy[In, Out]):
 *   def execute(self, transformer_logic, chunks, context):
 *     for chunk in chunks:
 *       yield transformer_logic(chunk, context)
 * ```
 */

import type { ExecutionStrategy, InternalTransformer, IContextManager } from "@src/types";

/**
 * The DEFAULT execution strategy: one chunk at a time, in order, each fully processed before the
 * next starts. No buffering and no reordering, which is what makes it safe to bypass entirely when
 * a transformer sits in source position.
 */
export class SequentialStrategy<In, Out> implements ExecutionStrategy<In, Out> {
  /** In-order, one-chunk-at-a-time — no concurrency/ordering machinery a source-position
   * async-iteration bypass could silently drop, so it's safe to run there. */
  readonly appliesInSourcePosition = true;

  async *execute(
    transformerLogic: InternalTransformer<In, Out>,
    chunks: AsyncIterable<In[]>,
    context: IContextManager,
  ): AsyncGenerator<Out[]> {
    for await (const chunk of chunks) {
      yield transformerLogic(chunk, context);
    }
  }
}
