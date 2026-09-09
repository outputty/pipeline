/**
 * `HttpRunner` and `ClusterRunner` (#90) - the dispatching runners, beside `ConcurrentRunner`.
 *
 * Both re-drive a pipeline's plan onto the class that already implements their dispatch, so a
 * runner and its class counterpart agree by construction rather than by a second implementation
 * kept in step by hand.
 */

import { HttpPipeline } from "@src/pipelines/http";
import { ClusterPipeline } from "@src/pipelines/cluster";
import { driveOnto, planOf, type RunnablePipeline } from "@src/runners/concurrent";

/** What a caller passes a dispatching runner, beyond the pipeline itself. */
export interface DispatchRunnerOptions {
  /** Chunks in flight at once. */
  maxConcurrency?: number;
  /** Whether results are re-ordered back into source order. */
  ordered?: boolean;
}

/**
 * Dispatches each chunk of each stage over HTTP to another instance running the same code (#90) -
 * the runner form of `HttpPipeline`.
 *
 * `pipeline` is bound at CONSTRUCTION, not only at `.run()`, because a serving process mounts
 * `.fetch` and never orchestrates: it has to hold the same stages at the same indices with no drain
 * of its own. That is the one asymmetry among the runners, and it exists because this is the one
 * whose server the caller mounts themselves.
 *
 * `const runner = new HttpRunner({ url }, p); app.mount("/pipeline", runner.fetch); await
 * runner.run(p)` → the same values `new HttpPipeline({ url })` gives for the chain `p` describes.
 */
export class HttpRunner<S = unknown> {
  /** Where this instance is mounted, as the caller's own code mounted it. */
  readonly url: string;
  /** The server surface: hand it to a router. Ready before anything runs. */
  readonly fetch: (request: Request) => Promise<Response>;

  private readonly options: DispatchRunnerOptions;
  private readonly served: HttpPipeline<S>;

  constructor(options: DispatchRunnerOptions & { url: string }, pipeline: RunnablePipeline<S>) {
    this.url = options.url;
    this.options = options;

    const plan = planOf(pipeline);
    this.served = driveOnto(
      new HttpPipeline<S>({
        url: options.url,
        maxConcurrency: options.maxConcurrency,
        ordered: options.ordered,
        context: plan.context,
        chunkSize: plan.chunkSize,
        runHandler: plan.runHandler,
      }).from(plan.source) as unknown as RunnablePipeline<S>,
      plan,
    ) as unknown as HttpPipeline<S>;

    this.fetch = (request: Request): Promise<Response> => this.served.fetch(request);
  }

  /**
   * Runs `pipeline` by dispatching every unpinned stage to `url`. The pipeline bound at
   * construction is what `.fetch` serves; this one is what the orchestrator drains, and in the
   * ordinary case they are the same object.
   */
  async run<T>(pipeline: RunnablePipeline<T>): Promise<T[]> {
    const plan = planOf(pipeline);

    const target = new HttpPipeline<T>({
      url: this.url,
      maxConcurrency: this.options.maxConcurrency,
      ordered: this.options.ordered,
      context: plan.context,
      chunkSize: plan.chunkSize,
      runHandler: plan.runHandler,
    }).from(plan.source) as unknown as RunnablePipeline<T>;

    return (await driveOnto(target, plan).toArray()) as T[];
  }
}

/**
 * Dispatches each chunk of each stage to another process on the same machine (#90) - the runner
 * form of `ClusterPipeline`. Fully opaque, as that class is: no server, no listen, no fork and no
 * url in caller code, so unlike `HttpRunner` nothing is bound at construction - a worker's own
 * serving side is internal.
 *
 * `new ClusterRunner({ workers: 4 }).run(new Pipeline().from([1,2,3]).transform((t) => t.map((x) =>
 * x * 2)))` → `[2,4,6]`, served by real worker processes.
 */
export class ClusterRunner {
  /** Worker processes to bring up on first dispatch. Defaults to the machine's parallelism. */
  readonly workers?: number;

  private readonly options: DispatchRunnerOptions;

  constructor(options?: DispatchRunnerOptions & { workers?: number }) {
    this.workers = options?.workers;
    this.options = options ?? {};
  }

  async run<T>(pipeline: RunnablePipeline<T>): Promise<T[]> {
    const plan = planOf(pipeline);

    const target = new ClusterPipeline<T>({
      workers: this.workers,
      maxConcurrency: this.options.maxConcurrency,
      ordered: this.options.ordered,
      context: plan.context,
      chunkSize: plan.chunkSize,
      runHandler: plan.runHandler,
    }).from(plan.source) as unknown as RunnablePipeline<T>;

    return (await driveOnto(target, plan).toArray()) as T[];
  }
}
