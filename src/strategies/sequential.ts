import type { InternalTransformer, IContextManager } from "@src/types";

/**
 * The DEFAULT execution strategy: one chunk at a time, in order, each fully processed before the
 * next starts. No buffering and no reordering — the same reason `Transformer`'s default runs safely
 * when consumed directly as an async source rather than through `.execute()` (`pipeline.ts`'s
 * `inertKnobsOf`, which tracks whether `.withExecutor()` was ever called, not what strategy it set).
 *
 * `sequential(logic, chunks, ctx)` → each output chunk yielded in input order.
 */
export async function* sequential<In, Out>(
  transformerLogic: InternalTransformer<In, Out>,
  chunks: AsyncIterable<In[]>,
  context: IContextManager,
): AsyncGenerator<Out[]> {
  for await (const chunk of chunks) {
    yield transformerLogic(chunk, context);
  }
}
