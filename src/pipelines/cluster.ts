/**
 * `ClusterPipeline` — STUB (#17 L1), copy-on-write wired (#17 L2). L5 fills in the worker
 * bootstrap: one shared port and worker set per process, lazily brought up on first drain.
 */

import type { ConcurrentPipelineOptions, StageOptions } from "@src/pipelines/concurrent";
import { HttpPipeline } from "@src/pipelines/http";
import type { PipelineOptions, PipelineSource } from "@src/pipeline";
import type { Transformer } from "@src/transformer";
import type { InternalTransformer } from "@src/types";

/** Construction-time knobs for `ClusterPipeline`. */
export type ClusterPipelineOptions = { workers?: number } & ConcurrentPipelineOptions;

/** `ClusterPipeline`'s real constructor parameter type - see `ConcurrentPipelineConstructorOptions`
 * (`pipelines/concurrent.ts`) for why the base `Pipeline` internals must be included here too. */
type ClusterPipelineConstructorOptions = ClusterPipelineOptions & PipelineOptions;

/**
 * Each chunk of a stage dispatched to another process on the SAME machine (#17). Brings up its
 * own `node:cluster` workers on first run; every later `ClusterPipeline` in the process reuses
 * them. Fully opaque: no server, no listen, no fork, no url in caller code.
 *
 * `new ClusterPipeline([1,2,3,4,5]).transform((t) => t.map((x) => x * 2)).toArray()` →
 * `[2,4,6,8,10]`, served by real worker processes (L5).
 */
export class ClusterPipeline<T> extends HttpPipeline<T> {
  /** Worker processes to bring up on first drain. `0` here is the L1/L2 stub default; L5 picks
   * the real one (`os.availableParallelism()`, per the ticket's own Constraints). */
  readonly workers: number;

  constructor(source: PipelineSource<T>, options?: ClusterPipelineConstructorOptions) {
    // The real url is only known once L5's bootstrap picks a port; "" is inert here — never
    // dialled, since every method below still throws.
    super(source, { ...options, url: "" });
    this.workers = options?.workers ?? 0;
  }

  /**
   * Carries `workers` into the NEXT instance a copy-on-write call builds, on top of what
   * `HttpPipeline.createPipeline()` already carries forward - same reason, one more field.
   */
  protected override createPipeline<U>(
    data: AsyncIterable<U>,
    options: PipelineOptions,
  ): ClusterPipeline<U> {
    const Ctor = this.constructor as new (
      data: AsyncIterable<U>,
      options?: ClusterPipelineConstructorOptions,
    ) => ClusterPipeline<U>;
    return new Ctor(data, {
      ...options,
      workers: this.workers,
      maxConcurrency: this.maxConcurrency,
      ordered: this.ordered,
      chunkSize: this.chunkSize,
    });
  }

  override transform<U>(
    _builder: (t: Transformer<T, T>) => Transformer<T, U>,
    _options?: StageOptions,
  ): ClusterPipeline<U> {
    throw new Error("ClusterPipeline.transform: not implemented (#17 L5)");
  }

  override apply<U>(_transformer: Transformer<T, U>, _options?: StageOptions): ClusterPipeline<U> {
    throw new Error("ClusterPipeline.apply: not implemented (#17 L5)");
  }

  protected override stageWork<U>(
    _transformer: Transformer<T, U>,
    _stageIndex: number,
  ): InternalTransformer<T, U> {
    throw new Error("ClusterPipeline.stageWork: not implemented (#17 L5)");
  }
}
