/**
 * `ConcurrentPipeline` — STUB (#17 L1). Every signature below is the real, settled shape; every
 * body throws. L3 fills in the fan-out; this file exists so the whole `pipelines/` family
 * typechecks and every Done-when case can be written as a real (failing) test against it.
 */

import type { InternalTransformer } from "@src/types";
import { Pipeline, type PipelineSource } from "@src/pipeline";
import type { Transformer } from "@src/transformer";

/** Construction-time knobs for `ConcurrentPipeline` and every class that extends it. */
export interface ConcurrentPipelineOptions {
  /** Chunks kept in flight at once. Default 4 (L3). */
  maxConcurrency?: number;
  /** Restore input order in output. Default `true` (L3). */
  ordered?: boolean;
  chunkSize?: number;
}

/** Per-stage override, passed as `.transform()`/`.apply()`'s second argument. */
export interface StageOptions {
  /** Run this one stage in the orchestrating process instead of dispatching it. */
  local?: boolean;
}

/**
 * Runs up to `maxConcurrency` chunks of one stage at once, in this process. Replaces the deleted
 * `concurrent()` execution strategy (#17): where `concurrent()` configured a `Transformer`, this
 * configures a `Pipeline` — the chain is identical, only the class differs.
 *
 * `new ConcurrentPipeline([1,2,3,4,5], { maxConcurrency: 4 }).transform((t) => t.map((x) => x *
 * 2)).toArray()` → `[2,4,6,8,10]` (L3).
 */
export class ConcurrentPipeline<T> extends Pipeline<T> {
  readonly maxConcurrency: number;
  readonly ordered: boolean;

  constructor(source: PipelineSource<T>, options?: ConcurrentPipelineOptions) {
    super(source);
    this.maxConcurrency = options?.maxConcurrency ?? 4;
    this.ordered = options?.ordered ?? true;
  }

  override transform<U>(
    _builder: (t: Transformer<T, T>) => Transformer<T, U>,
    _options?: StageOptions,
  ): ConcurrentPipeline<U> {
    throw new Error("ConcurrentPipeline.transform: not implemented (#17 L3)");
  }

  override apply<U>(
    _transformer: Transformer<T, U>,
    _options?: StageOptions,
  ): ConcurrentPipeline<U> {
    throw new Error("ConcurrentPipeline.apply: not implemented (#17 L3)");
  }

  /**
   * The one method a subclass overrides to change WHERE a stage runs. `ConcurrentPipeline`'s own
   * implementation runs the stage in-process; `HttpPipeline` (#17 L4) overrides it to POST
   * instead.
   */
  protected stageWork<U>(
    _transformer: Transformer<T, U>,
    _stageIndex: number,
  ): InternalTransformer<T, U> {
    throw new Error("ConcurrentPipeline.stageWork: not implemented (#17 L3)");
  }
}
