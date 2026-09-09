/**
 * Runners: WHERE a pipeline runs, separated from WHAT it does (#90).
 *
 * A `Pipeline` describes a chain - a source, stages in order, and the regions `.local(build)`
 * pinned. A runner takes that description and executes it somewhere else. The same object drains
 * here with `.toArray()` or runs there with `runner.run(pipeline)`, and both give the same values.
 *
 * This layer is ADDITIVE: `ConcurrentPipeline` and its siblings still exist and still work. A runner
 * re-drives a plan onto the class that already implements that dispatch, so the two paths are equal
 * by construction rather than by a second implementation kept in step by hand.
 */

import { Pipeline, type PipelinePlan } from "@src/pipeline";
import { ConcurrentPipeline, type ConcurrentPipelineOptions } from "@src/pipelines/concurrent";
import { Transformer } from "@src/transformer";
import type { IContextManager, InternalTransformer, SourcePolicy } from "@src/types";

/** Any pipeline a runner accepts: one that already has a source, whatever engine built it. */
export type RunnablePipeline<T> = Pipeline<T, "sync" | "async", SourcePolicy>;

/**
 * Reads a pipeline's plan and refuses one with no source (#90) - the guard every runner shares,
 * written once rather than four times.
 *
 * `RunnablePipeline<T>` already excludes an `"unset"` pipeline at COMPILE time, so this only fires
 * for a caller who cast past that. It is still a throw rather than a silent empty result, matching
 * `requireSource()`'s own contract on the stage path.
 */
export function planOf<T>(
  pipeline: RunnablePipeline<T>,
): PipelinePlan<T> & { source: NonNullable<PipelinePlan<T>["source"]> } {
  const plan = pipeline.plan();
  if (plan.source === null) {
    throw new Error("no source: call .from(data) before handing a pipeline to a runner");
  }
  return plan as PipelinePlan<T> & { source: NonNullable<PipelinePlan<T>["source"]> };
}

/**
 * Re-drives one plan onto `target`, stage by stage, in index order (#90) - the one place a plan
 * becomes execution, shared by every runner rather than copied per class.
 *
 * A pinned index runs through `.local(build)`, which is what keeps a `.local()` region in the
 * orchestrating process exactly as it is today. A reduce stage is replayed as `.reduce()` rather
 * than `.apply()`, because its `_chunkTransforms` slot is a placeholder that throws if invoked as a
 * per-chunk transform.
 */
export function driveOnto<T>(
  target: RunnablePipeline<T>,
  plan: PipelinePlan<T>,
): RunnablePipeline<T> {
  let current = target;

  for (let index = 0; index < plan.stages.length; index++) {
    const reduceStage = plan.reduceStages.get(index);
    if (reduceStage !== undefined) {
      // Cast to the ASYNC overload: a `ReduceFunction` returns `U | Promise<U>`, which matches
      // neither arm exactly, and a runner's own result is a `Promise` regardless. The type argument
      // is explicit because the cast alone leaves `U` to infer as `never`.
      const fn = reduceStage.fn as (
        acc: T,
        item: T,
        ctx: IContextManager,
        emit: (value: T) => void,
      ) => Promise<T>;
      current = current.reduce<T>(fn, reduceStage.initial as T) as RunnablePipeline<T>;
      continue;
    }

    const stage = plan.stages[index];
    const transformer = new Transformer<T, T>({
      transform: stage as unknown as InternalTransformer<T, T>,
    });

    current = plan.pinned.has(index)
      ? (current.local((p) => p.apply(transformer)) as RunnablePipeline<T>)
      : (current.apply(transformer) as RunnablePipeline<T>);
  }

  return current;
}

/**
 * Runs a pipeline's stages with `maxConcurrency` chunks in flight, in this process (#90) - the
 * runner form of `ConcurrentPipeline`, for a caller who wants to build a chain once and decide
 * later where it runs.
 *
 * `new ConcurrentRunner({ maxConcurrency: 3 }).run(new Pipeline().from([1,2,3,4,5,6]).buffer(2)
 * .reduce((acc, x) => acc + x, 0))` → `[3,7,11]`, one accumulator per partition, the same answer
 * `ConcurrentPipeline` gives for the same chain (#62).
 */
export class ConcurrentRunner {
  /** Chunks in flight at once. */
  readonly maxConcurrency: number;
  /** Whether results are re-ordered back into source order. */
  readonly ordered: boolean;

  constructor(options?: ConcurrentPipelineOptions) {
    this.maxConcurrency = options?.maxConcurrency ?? 4;
    this.ordered = options?.ordered ?? true;
  }

  /**
   * Runs `pipeline` here, with this runner's own concurrency. Always a `Promise`: a runner exists to
   * put work somewhere the caller is not, so there is no synchronous case for it to preserve.
   *
   * @param pipeline - A pipeline that already has a source. Its own class is ignored; only its plan
   *   is read, so a plain `Pipeline` and a `ConcurrentPipeline` describing the same chain run
   *   identically here.
   */
  async run<T>(pipeline: RunnablePipeline<T>): Promise<T[]> {
    const plan = planOf(pipeline);

    const target = new ConcurrentPipeline<T>({
      maxConcurrency: this.maxConcurrency,
      ordered: this.ordered,
      context: plan.context,
      chunkSize: plan.chunkSize,
      runHandler: plan.runHandler,
    }).from(plan.source) as unknown as RunnablePipeline<T>;

    return (await driveOnto(target, plan).toArray()) as T[];
  }
}
