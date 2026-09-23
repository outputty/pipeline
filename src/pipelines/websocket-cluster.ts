/**
 * `ClusterPipeline` runs each stage in forked `node:cluster` workers on this machine, over
 * `WebSocketPipeline`'s `ws+unix:` wire. Workers start on the first chunk dispatched.
 *
 * ⚠ Each worker binds its own socket path, and dispatch round-robins across them. A connection is
 * persistent, so a shared target would leave every worker but one idle.
 *
 * ⚠ Never import `cluster.ts`: it starts the HTTP worker server at load, in every worker.
 */

import cluster from "node:cluster";
import { createServer } from "node:http";
import { availableParallelism, tmpdir } from "node:os";
import { join } from "node:path";
import type { ConcurrentPipelineOptions } from "@src/pipelines/concurrent";
import {
  WebSocketPipeline,
  toNodeWebSocketHandler,
  peekFrame,
  sendUnknownRouteError,
} from "@src/pipelines/websocket";
import type {
  WebSocketPipelineOptions,
  PipelineSocket,
  ResolvedConnect,
} from "@src/pipelines/websocket";
import type { Codec } from "@src/codec";
import { Pipeline } from "@src/pipeline";
import type { PipelineSource, WrappablePipeline } from "@src/pipeline";
import type { Transformer } from "@src/transformer";
import { drainsNothingOnWorker, pipelineRoute, WorkerSet } from "@src/pipelines/worker-set";
import type { SlotOptions } from "@src/pipelines/worker-set";
import type { Drainable, ReduceFunction, PipelineMode, RouteVerb } from "@src/types";

/** One per process, on the primary and on every worker. */
const wsWorkerSet = new WorkerSet<ClusterPipeline<unknown>, string>({
  readyType: "outputty-pipeline-ws-ready",
  addressField: "socketPath",
  addressKind: "string",
  failure: "a ClusterPipeline worker failed before reporting its socket",
});

let nextWorkerIndex = 0;

/** Starts the workers if needed and returns the next worker's target, round-robin, plus the
 * dispatch's `release`. */
async function enterNextWorker(workerCount: number): Promise<ResolvedConnect> {
  const { addresses, release } = await wsWorkerSet.enter(workerCount);
  const path = addresses[nextWorkerIndex % addresses.length]!;
  nextWorkerIndex++;
  return { connect: `ws+unix:${path}:/`, release };
}

/** The WebSocket server every worker runs on its own socket path. It routes `/pipeline/<i>/…`
 * frames to pipeline `i`'s `receiveFrame()`. */
function startWorkerServer(): void {
  // A live Set, answered over IPC for tests; not a lifetime counter.
  const openConnections = new Set<PipelineSocket>();
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- this IS the I/O boundary parser the rule's own message asks for; a worker's own "message" event is genuinely unparsed until this function runs
  process.on("message", (message: unknown) => {
    const isQuery =
      typeof message === "object" &&
      message !== null &&
      "type" in message &&
      message.type === "outputty-pipeline-query-connections";
    if (!isQuery) return;
    process.send?.({ type: "outputty-pipeline-connections", count: openConnections.size });
  });

  const routeToRegisteredPipeline = (socket: PipelineSocket, data: Uint8Array): void => {
    const { id, route } = peekFrame(data);
    const pipeline = wsWorkerSet.lookupRoute(route);
    if (!pipeline) {
      sendUnknownRouteError(socket, id, route);
      return;
    }
    pipeline.receiveFrame(socket, data);
  };

  const handler = toNodeWebSocketHandler({
    serve(socket) {
      openConnections.add(socket);
      socket.onClose(() => openConnections.delete(socket));
      socket.onMessage((data) => {
        if (typeof data === "string") return;
        routeToRegisteredPipeline(socket, data);
      });
    },
  });

  const socketPath = join(tmpdir(), `outputty-pipeline-ws-${process.pid}.sock`);
  const server = createServer();
  server.on("upgrade", (request, socket, head) => handler.upgrade(request, socket, head));
  server.listen(socketPath, () => {
    process.send?.({ type: "outputty-pipeline-ws-ready", socketPath });
  });
}

if (cluster.isWorker) {
  startWorkerServer();
}

/**
 * Runs each stage of a chain in forked worker processes on this machine, over WebSocket. The
 * caller writes no server, socket or fork; every `ClusterPipeline` in the process shares the
 * workers.
 *
 * ⚠ Each worker re-runs the entry module, so build the pipeline at the top level and drain it on
 * the primary only.
 *
 * ```ts
 * const chain = new Pipeline<number>().transform((t) => t.map((x) => x * 2));
 * const doubled = new ClusterPipeline(chain);
 * if (cluster.isPrimary) console.log(await doubled([1, 2, 3]).toArray()); // [2, 4, 6]
 * ```
 */
export class ClusterPipeline<T, In = T> extends WebSocketPipeline<T, In> {
  /** Worker processes to bring up on first drain. Default `os.availableParallelism()`. */
  readonly workers: number;
  /** This pipeline's position among the process's `ClusterPipeline`s. It survives copy-on-write, so
   * the primary and every worker route the same pipeline to the same index. */
  readonly pipelineIndex: number;

  /** Wraps a chain built elsewhere, dispatching its stages to forked worker processes. */
  constructor(pipeline: WrappablePipeline<T, In>, options?: ClusterPipelineOptions);
  constructor(options?: ClusterPipelineConstructorOptions);
  constructor(
    first?: WrappablePipeline<T, In> | ClusterPipelineConstructorOptions,
    second?: ClusterPipelineOptions,
  ) {
    const options = Pipeline.wrapping<ClusterPipelineConstructorOptions>(first, second);
    // Dispatch never reads `connect`; `resolveConnect()` supplies the target per call.
    super({ ...options, connect: "" });
    this.workers = options?.workers ?? availableParallelism();
    this.pipelineIndex = wsWorkerSet.claimSlot(this as ClusterPipeline<unknown>, options);
  }

  /** Carries `workers` and `pipelineIndex` into the next copy-on-write instance. */
  protected override carriedKnobs(): WebSocketPipelineOptions & {
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
  ): ClusterPipeline<U, In> {
    return super.transform(builder) as unknown as ClusterPipeline<U, In>;
  }

  override apply<U>(transformer: Transformer<T, U, "sync" | "async">): ClusterPipeline<U, In> {
    return super.apply(transformer) as unknown as ClusterPipeline<U, In>;
  }

  override reduce<U>(fn: ReduceFunction<U, T>, initial: U): ClusterPipeline<U, In> {
    return super.reduce(fn, initial) as unknown as ClusterPipeline<U, In>;
  }

  override local<U, M2 extends PipelineMode>(
    build: (p: Pipeline<T, "async", any>) => Pipeline<U, M2, any>,
  ): ClusterPipeline<U, In> {
    return super.local(build) as unknown as ClusterPipeline<U, In>;
  }

  override queue(capacity: number): ClusterPipeline<T, In> {
    return super.queue(capacity) as unknown as ClusterPipeline<T, In>;
  }

  /** Drains no chunks on a worker: a worker serves the stages and never orchestrates a run. */
  override drainable(input: PipelineSource<In>, materialize = true): Drainable<T> {
    return drainsNothingOnWorker(super.drainable(input, materialize));
  }

  /** Prefixes every route with `/pipeline/<pipelineIndex>`, so several pipelines share one worker
   * server.
   *
   * `routePath("transform", 0)` → `/pipeline/2/transform/0` for `pipelineIndex` 2. */
  protected override routePath(verb: RouteVerb, index: number): string {
    return pipelineRoute(this.pipelineIndex, super.routePath(verb, index));
  }

  /** Starts the workers if needed and picks the next one per dispatch; see
   * `WebSocketPipeline.resolveConnect()` for why it is never stored. */
  protected override resolveConnect(): Promise<ResolvedConnect> {
    return enterNextWorker(this.workers);
  }
}

/** Construction-time knobs for `ClusterPipeline`. */
export type ClusterPipelineOptions = {
  /** Worker processes to fork. Defaults to `os.availableParallelism()`. */
  workers?: number;
  /** How a chunk is encoded on the wire. Each worker builds its own by re-running the entry
   * module, so a store the codec writes to must be reachable from every worker. */
  codec?: Codec;
} & ConcurrentPipelineOptions;

type ClusterPipelineConstructorOptions = ClusterPipelineOptions & SlotOptions;
