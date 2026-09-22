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
import { emptyChunks, Pipeline } from "@src/pipeline";
import type { PipelineConstructorOptions, WrappablePipeline } from "@src/pipeline";
import type { Transformer } from "@src/transformer";
import { IDLE_KILL_MS } from "@src/types";
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

type ClusterHttpPipelineConstructorOptions = ClusterHttpPipelineOptions &
  PipelineConstructorOptions & { pipelineIndex?: number };

interface BootstrapResult {
  port: number;
}

function isReadyMessage(
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- this IS the I/O boundary parser the rule's own message asks for; message is genuinely unparsed until this function runs
  message: unknown,
): message is { type: "outputty-pipeline-ready"; port: number } {
  return (
    typeof message === "object" &&
    message !== null &&
    "type" in message &&
    message.type === "outputty-pipeline-ready" &&
    "port" in message &&
    typeof message.port === "number"
  );
}

class WorkerSet {
  private nextPipelineIndex = 0;
  private readonly registry = new Map<number, ClusterHttpPipeline<unknown>>();
  private bootstrapPromise: Promise<BootstrapResult> | undefined;
  private inFlight = 0;
  private idleTimer: ReturnType<typeof setTimeout> | undefined;
  /** ⚠ Kill only these ids: `cluster.workers` also holds every other worker set's workers. */
  private readonly ownWorkerIds = new Set<number>();

  register(pipeline: ClusterHttpPipeline<unknown>): number {
    const index = this.nextPipelineIndex++;
    this.registry.set(index, pipeline);
    return index;
  }

  claimIndex(): number {
    return this.nextPipelineIndex++;
  }

  lookup(index: number): ClusterHttpPipeline<unknown> | undefined {
    return this.registry.get(index);
  }

  /** Forks the workers once per process and resolves with the port they share. `listen(0)` under
   * `cluster` gives every worker the same port. */
  bootstrap(workerCount: number): Promise<BootstrapResult> {
    this.bootstrapPromise ??= new Promise((resolve, reject) => {
      const count = workerCount > 0 ? workerCount : availableParallelism();
      let sharedPort: number | undefined;
      let readyCount = 0;
      let settled = false;
      for (let i = 0; i < count; i++) {
        const worker = cluster.fork();
        this.ownWorkerIds.add(worker.id);
        worker.on("message", (message) => {
          if (!isReadyMessage(message)) return;
          sharedPort ??= message.port;
          readyCount++;
          if (readyCount === count && !settled) {
            settled = true;
            resolve({ port: sharedPort! });
          }
        });
        // ⚠ A worker that dies before reporting must reject, or every dispatch hangs forever.
        const fail = (detail: string): void => {
          if (settled) return;
          settled = true;
          reject(
            new Error(`a ClusterHttpPipeline worker failed before reporting its port: ${detail}`),
          );
        };
        worker.on("error", (error: Error) => fail(error.message));
        worker.on("exit", (code, signal) => fail(`exited with code ${code}, signal ${signal}`));
      }
    });
    return this.bootstrapPromise;
  }

  /** Marks one dispatch in flight and returns the port plus its `release`. A second `release`
   * call does nothing. */
  async enter(workerCount: number): Promise<{ port: number; release: () => void }> {
    const { port } = await this.bootstrap(workerCount);
    this.inFlight++;
    let released = false;
    const release = (): void => {
      if (released) return;
      released = true;
      this.inFlight--;
      this.scheduleIdleCheck();
    };
    return { port, release };
  }

  /** ⚠ The timer is `unref()`'d; the workers keep the process alive until it kills them. */
  private scheduleIdleCheck(): void {
    if (this.idleTimer) clearTimeout(this.idleTimer);
    this.idleTimer = setTimeout(() => {
      if (this.inFlight > 0) {
        this.scheduleIdleCheck();
        return;
      }
      this.kill();
    }, IDLE_KILL_MS);
    this.idleTimer.unref();
  }

  /** ⚠ Kills, never `.unref()`s: a forked worker holds the event loop open through a handle no
   * public API releases. */
  kill(): void {
    const workers = cluster.workers ?? {};
    for (const id of this.ownWorkerIds) {
      workers[id]?.kill();
    }
    this.ownWorkerIds.clear();
    this.bootstrapPromise = undefined; // a later dispatch bootstraps a fresh set
  }

  /** The HTTP server every worker runs. It routes `/pipeline/<i>/…` to pipeline `i`'s `.fetch()`,
   * which reads the trailing `/transform/<n>`. */
  startWorkerServer(): void {
    const routeToRegisteredPipeline = async (request: Request): Promise<Response> => {
      const { pathname } = new URL(request.url);
      const match = /^\/pipeline\/(\d+)\//.exec(pathname);
      const pipeline = match ? this.lookup(Number(match[1])) : undefined;
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
}

/** One per process, on the primary and on every worker. */
const workerSet = new WorkerSet();

if (cluster.isWorker) {
  workerSet.startWorkerServer();
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
    super({ ...options, url: "" });
    this.workers = options?.workers ?? availableParallelism();

    // Which instances claim a pipeline index:
    // - A composed, unbound instance with no trail claims a fresh one. ⚠ Inheriting the base's lets
    //   two sibling chains share an index, and the second silently serves both.
    // - ⚠ A bound instance never claims. Workers bind lazily, so a claim there shifts their
    //   indexes away from the primary's.
    // - ⚠ A `.branch()` arm never registers. It is reached through its parent's route, and
    //   registering it overwrites the parent.
    const claimsOwnSlot = options?.bound !== true && (options?.routeTrail ?? "") === "";
    this.pipelineIndex = claimsOwnSlot
      ? workerSet.register(this as ClusterHttpPipeline<unknown>)
      : (options?.pipelineIndex ?? workerSet.claimIndex());

    // ⚠ On a worker every terminal op resolves empty: a worker holds the stages and never
    // orchestrates a drain.
    if (cluster.isWorker) {
      this._chunks = emptyChunks<T>();
      this._preBufferItems = null;
    }
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
    return `/pipeline/${this.pipelineIndex}${super.routePath(verb, index)}`;
  }

  /** Starts the workers if needed, points `_url` at them and returns the dispatch's `release`. */
  protected async bootstrapAndSetUrl(): Promise<() => void> {
    const { port, release } = await workerSet.enter(this.workers);
    this._url = `http://localhost:${port}`;
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
