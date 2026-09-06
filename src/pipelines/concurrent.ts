/**
 * `ConcurrentPipeline` — STUB (#17 L1), copy-on-write wired (#17 L2). `.transform()`/`.apply()`/
 * `stageWork()` still throw - L3 fills in the fan-out. `.context()`/`.buffer()`, inherited from
 * `Pipeline` and NOT stubbed, are real and live today, which is why this class already needs a
 * real `createPipeline()` override: without one, either call silently resets `maxConcurrency`/
 * `ordered` to their constructor defaults on the returned instance (`Pipeline.createPipeline()`'s
 * own docstring names this exact failure mode).
 */

import type { InternalTransformer } from "@src/types";
import { Pipeline, type PipelineOptions, type PipelineSource } from "@src/pipeline";
import type { Transformer } from "@src/transformer";

/** Construction-time knobs for `ConcurrentPipeline` and every class that extends it. */
export interface ConcurrentPipelineOptions {
  /** Chunks kept in flight at once. Default 4 (L3). */
  maxConcurrency?: number;
  /** Restore input order in output. Default `true` (L3). */
  ordered?: boolean;
  /** Chunk size for the stage's own internal `Transformer` (L3 reads this; stored from L2 on so
   * `.context()`/`.buffer()` copy-on-write never silently drops it). */
  chunkSize?: number;
}

/** Every `ConcurrentPipeline` constructor's real parameter type: its own knobs PLUS the base
 * `Pipeline` internals (`context`, `rootSource`, …) that `createPipeline()` (below) must be able
 * to pass through on every copy-on-write call. Not exported - a caller only ever sees
 * `ConcurrentPipelineOptions`; the intersection is this file's own plumbing. */
type ConcurrentPipelineConstructorOptions = ConcurrentPipelineOptions & PipelineOptions;

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
  /** Chunks of the current stage kept in flight at once. */
  readonly maxConcurrency: number;
  /** Whether output order is restored to match input order once a chunk finishes. */
  readonly ordered: boolean;
  /** Chunk size for a stage's own internal `Transformer` - unused until L3, stored here so a
   * copy-on-write call (`.context()`, `.buffer()`) never silently drops the caller's value. */
  readonly chunkSize?: number;

  constructor(source: PipelineSource<T>, options?: ConcurrentPipelineConstructorOptions) {
    super(source, options);
    this.maxConcurrency = options?.maxConcurrency ?? 4;
    this.ordered = options?.ordered ?? true;
    this.chunkSize = options?.chunkSize;
  }

  /**
   * Carries `maxConcurrency`/`ordered`/`chunkSize` into the NEXT instance a copy-on-write call
   * (`.context()`, `.buffer()`, and - via `Pipeline.apply()` - `.transform()`/`.apply()` once L3
   * stops overriding them) builds, the same way `HttpPipeline`/`ClusterPipeline` (#17 L4/L5)
   * override this method again for their own extra knobs (`url`, `workers`).
   *
   * @example
   * `new ConcurrentPipeline([1], { maxConcurrency: 8 }).context({ k: 1 }).maxConcurrency` → `8`,
   * not the constructor default `4` - without this override, `Pipeline.createPipeline()`'s base
   * implementation reconstructs via `this.constructor` but only forwards `PipelineOptions` fields,
   * which do not include `maxConcurrency`.
   */
  protected override createPipeline<U>(
    data: AsyncIterable<U>,
    options: PipelineOptions,
  ): ConcurrentPipeline<U> {
    const Ctor = this.constructor as new (
      data: AsyncIterable<U>,
      options?: ConcurrentPipelineConstructorOptions,
    ) => ConcurrentPipeline<U>;
    return new Ctor(data, {
      ...options,
      maxConcurrency: this.maxConcurrency,
      ordered: this.ordered,
      chunkSize: this.chunkSize,
    });
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
