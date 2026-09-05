/**
 * `ClusterPipeline` — STUB (#17 L1). Real signature, throwing body. L5 fills in the worker
 * bootstrap: one shared port and worker set per process, lazily brought up on first drain.
 */

import type { ConcurrentPipelineOptions, StageOptions } from "@src/pipelines/concurrent";
import { HttpPipeline } from "@src/pipelines/http";
import type { PipelineSource } from "@src/pipeline";
import type { Transformer } from "@src/transformer";
import type { InternalTransformer } from "@src/types";

/** Construction-time knobs for `ClusterPipeline`. */
export type ClusterPipelineOptions = { workers?: number } & ConcurrentPipelineOptions;

/**
 * Each chunk of a stage dispatched to another process on the SAME machine (#17). Brings up its
 * own `node:cluster` workers on first run; every later `ClusterPipeline` in the process reuses
 * them. Fully opaque: no server, no listen, no fork, no url in caller code.
 *
 * `new ClusterPipeline([1,2,3,4,5]).transform((t) => t.map((x) => x * 2)).toArray()` →
 * `[2,4,6,8,10]`, served by real worker processes (L5).
 */
export class ClusterPipeline<T> extends HttpPipeline<T> {
  readonly workers: number;

  constructor(source: PipelineSource<T>, options?: ClusterPipelineOptions) {
    // The real url is only known once L5's bootstrap picks a port; "" is inert here — never
    // dialled, since every method below still throws.
    super(source, { ...options, url: "" });
    this.workers = options?.workers ?? 0;
  }

  override transform<U>(
    builder: (t: Transformer<T, T>) => Transformer<T, U>,
    options?: StageOptions,
  ): ClusterPipeline<U> {
    throw new Error("ClusterPipeline.transform: not implemented (#17 L5)");
  }

  override apply<U>(transformer: Transformer<T, U>, options?: StageOptions): ClusterPipeline<U> {
    throw new Error("ClusterPipeline.apply: not implemented (#17 L5)");
  }

  protected override stageWork<U>(
    transformer: Transformer<T, U>,
    stageIndex: number,
  ): InternalTransformer<T, U> {
    throw new Error("ClusterPipeline.stageWork: not implemented (#17 L5)");
  }
}
