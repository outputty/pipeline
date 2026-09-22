/**
 * `ClusterHttpPipeline` runs each stage in forked `node:cluster` workers on this machine, over
 * HTTP. Each worker serves every pipeline at `/pipeline/<i>/transform/<n>` from one shared port.
 * Workers start on the first chunk dispatched, so a pipeline never drained never forks.
 *
 * ⚠ This module starts the worker HTTP server at load, in every worker. `websocket-cluster.ts`
 * must never import it.
 */

import cluster from "node:cluster";
import { createServer } from "node:http";
import { availableParallelism } from "node:os";
import type { AddressInfo } from "node:net";
import type { ConcurrentPipelineOptions } from "@src/pipelines/concurrent";
import { HttpPipeline, toNodeHandler, errorResponse } from "@src/pipelines/http";
import type { HttpPipelineOptions } from "@src/pipelines/http";
import type { PipelineClient } from "@src/pipelines/client";
import { Pipeline } from "@src/pipeline";
import type { WrappablePipeline } from "@src/pipeline";
import type { Transformer } from "@src/transformer";
import { onWorkerDrainNothing, pipelineRoute, WorkerSet } from "@src/pipelines/worker-set";
import type { SlotOptions } from "@src/pipelines/worker-set";
import type {
  InternalTransformer,
  ReduceFunction,
  PipelineMode,
  ReduceWork,
  RouteVerb,
} from "@src/types";

/** Construction-time knobs for `ClusterHttpPipeline`. */
export type ClusterHttpPipelineOptions = {
  /** Worker processes to fork. Defaults to `os.availableParallelism()`. */
  workers?: number;
  /** How the primary reaches a worker; see `HttpPipelineOptions.client`. */
  client?: PipelineClient;
} & ConcurrentPipelineOptions;

type ClusterHttpPipelineConstructorOptions = ClusterHttpPipelineOptions & SlotOptions;

/** One per process, on the primary and on every worker. `listen(0)` under `cluster` gives every
 * worker the same port, so the first address reported is the one every dispatch uses. */
const workerSet = new WorkerSet<ClusterHttpPipeline<unknown>, number>({
  readyType: "outputty-pipeline-ready",
  addressField: "port",
  addressKind: "number",
  failure: "a ClusterHttpPipeline worker failed before reporting its port",
});

/** The HTTP server every worker runs. It routes `/pipeline/<i>/…` to pipeline `i`'s `.fetch()`,
 * which reads the trailing `/transform/<n>`. */
function startWorkerServer(): void {
  const routeToRegisteredPipeline = async (request: Request): Promise<Response> => {
    const { pathname } = new URL(request.url);
    const pipeline = workerSet.lookupRoute(pathname);
    if (!pipeline) {
      return errorResponse(404, `unknown pipeline route ${pathname}`);
    }
    return pipeline.fetch(request);
  };

  const server = createServer(toNodeHandler(routeToRegisteredPipeline));
  server.listen(0, () => {
    const { port } = server.address() as AddressInfo;
    process.send?.({ type: "outputty-pipeline-ready", port });
  });
}

if (cluster.isWorker) {
  startWorkerServer();
}

/**
 * Runs each stage of a chain in forked worker processes on this machine, over HTTP. The caller
 * writes no server, port or fork; every `ClusterHttpPipeline` in the process shares the workers.
 *
 * ⚠ Each worker re-runs the entry module, so build the pipeline at the top level and drain it on
 * the primary only.
 *
 * ```ts
 * const chain = new Pipeline<number>().transform((t) => t.map((x) => x * 2));
 * const doubled = new ClusterHttpPipeline(chain);
 * if (cluster.isPrimary) console.log(await doubled([1, 2, 3]).toArray()); // [2, 4, 6]
 * ```
 */
export class ClusterHttpPipeline<T, In = T> extends HttpPipeline<T, In> {
  /** Worker processes to bring up on first drain. Default `os.availableParallelism()`. */
  readonly workers: number;
  /** This pipeline's position among the process's `ClusterHttpPipeline`s. It survives
   * copy-on-write, so the primary and every worker route the same pipeline to the same index. */
  readonly pipelineIndex: number;

  /** Wraps a chain built elsewhere, dispatching its stages to forked worker processes. */
  constructor(pipeline: WrappablePipeline<T, In>, options?: ClusterHttpPipelineOptions);
  constructor(options?: ClusterHttpPipelineConstructorOptions);
  constructor(
    first?: WrappablePipeline<T, In> | ClusterHttpPipelineConstructorOptions,
    second?: ClusterHttpPipelineOptions,
  ) {
    const options = Pipeline.wrapping<ClusterHttpPipelineConstructorOptions>(first, second);
    // The url is set once the workers pick a port, on the first dispatch.
    super(onWorkerDrainNothing({ ...options, url: "" }));
    this.workers = options?.workers ?? availableParallelism();
    this.pipelineIndex = workerSet.claimSlot(this as ClusterHttpPipeline<unknown>, options);
  }

  /** Carries `workers` and `pipelineIndex` into the next copy-on-write instance. */
  protected override carriedKnobs(): HttpPipelineOptions & {
    workers: number;
    pipelineIndex: number;
  } {
    return {
      ...super.carriedKnobs(),
      workers: this.workers,
      pipelineIndex: this.pipelineIndex,
    };
  }

  override transform<U, M2 extends "sync" | "async">(
    builder: (t: Transformer<T, T, "async">) => Transformer<T, U, M2>,
  ): ClusterHttpPipeline<U, In> {
    return super.transform(builder) as unknown as ClusterHttpPipeline<U, In>;
  }

  override apply<U>(transformer: Transformer<T, U, "sync" | "async">): ClusterHttpPipeline<U, In> {
    return super.apply(transformer) as unknown as ClusterHttpPipeline<U, In>;
  }

  override reduce<U>(fn: ReduceFunction<U, T>, initial: U): ClusterHttpPipeline<U, In> {
    return super.reduce(fn, initial) as unknown as ClusterHttpPipeline<U, In>;
  }

  override local<U, M2 extends PipelineMode>(
    build: (p: Pipeline<T, "async", any>) => Pipeline<U, M2, any>,
  ): ClusterHttpPipeline<U, In> {
    return super.local(build) as unknown as ClusterHttpPipeline<U, In>;
  }

  override queue(capacity: number): ClusterHttpPipeline<T, In> {
    return super.queue(capacity) as unknown as ClusterHttpPipeline<T, In>;
  }

  /** Prefixes every route with `/pipeline/<pipelineIndex>`, so several pipelines share one worker
   * server.
   *
   * `routePath("transform", 0)` → `/pipeline/2/transform/0` for `pipelineIndex` 2. */
  protected override routePath(verb: RouteVerb, index: number): string {
    return pipelineRoute(this.pipelineIndex, super.routePath(verb, index));
  }

  /** Starts the workers if needed, points `_url` at them and returns the dispatch's `release`. */
  protected async bootstrapAndSetUrl(): Promise<() => void> {
    const { addresses, release } = await workerSet.enter(this.workers);
    this._url = `http://localhost:${addresses[0]}`;
    return release;
  }

  protected override stageWork<U>(
    transformer: Transformer<T, U, "sync" | "async">,
    stageIndex: number,
  ): InternalTransformer<T, U> {
    const dispatch = super.stageWork(transformer, stageIndex);
    return async (chunk, ctx) => {
      const release = await this.bootstrapAndSetUrl();
      try {
        return await dispatch(chunk, ctx);
      } finally {
        release();
      }
    };
  }

  /** Holds one worker dispatch open for the whole reduce stream, not per chunk. */
  protected override reduceWork<U>(
    fn: ReduceFunction<U, T>,
    initial: U,
    stageIndex: number,
  ): ReduceWork<T, U> {
    const dispatch = super.reduceWork(fn, initial, stageIndex);
    const self = this;
    return async function* dispatchOnWorker(chunks, ctx) {
      const release = await self.bootstrapAndSetUrl();
      try {
        yield* dispatch(chunks, ctx);
      } finally {
        release();
      }
    };
  }
}
