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
import { emptyChunks, Pipeline } from "@src/pipeline";
import type { PipelineConstructorOptions, WrappablePipeline } from "@src/pipeline";
import type { Transformer } from "@src/transformer";
import { IDLE_KILL_MS } from "@src/types";
import type { ReduceFunction, PipelineMode, RouteVerb } from "@src/types";

function isWsReadyMessage(
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- this IS the I/O boundary parser the rule's own message asks for; message is genuinely unparsed until this function runs
  message: unknown,
): message is { type: "outputty-pipeline-ws-ready"; socketPath: string } {
  return (
    typeof message === "object" &&
    message !== null &&
    "type" in message &&
    message.type === "outputty-pipeline-ws-ready" &&
    "socketPath" in message &&
    typeof message.socketPath === "string"
  );
}

class WsWorkerSet {
  private nextPipelineIndex = 0;
  private readonly registry = new Map<number, ClusterPipeline<unknown>>();
  private bootstrapPromise: Promise<string[]> | undefined;
  private inFlight = 0;
  private idleTimer: ReturnType<typeof setTimeout> | undefined;
  private nextWorkerIndex = 0;
  /** ⚠ Kill only these ids: `cluster.workers` also holds every other worker set's workers. */
  private readonly ownWorkerIds = new Set<number>();

  register(pipeline: ClusterPipeline<unknown>): number {
    const index = this.nextPipelineIndex++;
    this.registry.set(index, pipeline);
    return index;
  }

  claimIndex(): number {
    return this.nextPipelineIndex++;
  }

  lookup(index: number): ClusterPipeline<unknown> | undefined {
    return this.registry.get(index);
  }

  /** Forks the workers once per process and resolves with each worker's socket path. */
  bootstrap(workerCount: number): Promise<string[]> {
    this.bootstrapPromise ??= new Promise((resolve, reject) => {
      const count = workerCount > 0 ? workerCount : availableParallelism();
      const paths: string[] = [];
      let settled = false;
      for (let i = 0; i < count; i++) {
        const worker = cluster.fork();
        this.ownWorkerIds.add(worker.id);
        worker.on("message", (message) => {
          if (!isWsReadyMessage(message)) return;
          paths.push(message.socketPath);
          if (paths.length === count && !settled) {
            settled = true;
            resolve(paths);
          }
        });
        // ⚠ A worker that dies before reporting must reject, or every dispatch hangs forever.
        const fail = (detail: string): void => {
          if (settled) return;
          settled = true;
          reject(
            new Error(`a ClusterPipeline worker failed before reporting its socket: ${detail}`),
          );
        };
        worker.on("error", (error: Error) => fail(error.message));
        worker.on("exit", (code, signal) => fail(`exited with code ${code}, signal ${signal}`));
      }
    });
    return this.bootstrapPromise;
  }

  /** Marks one dispatch in flight and returns the next worker's target, round-robin, plus its
   * `release`. */
  async enter(workerCount: number): Promise<{ connect: string; release: () => void }> {
    const paths = await this.bootstrap(workerCount);
    const path = paths[this.nextWorkerIndex % paths.length]!;
    this.nextWorkerIndex++;
    this.inFlight++;
    let released = false;
    const release = (): void => {
      if (released) return;
      released = true;
      this.inFlight--;
      if (this.inFlight === 0) this.scheduleIdleCheck();
    };
    return { connect: `ws+unix:${path}:/`, release };
  }

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

  kill(): void {
    const workers = cluster.workers ?? {};
    for (const id of this.ownWorkerIds) {
      workers[id]?.kill();
    }
    this.ownWorkerIds.clear();
    this.bootstrapPromise = undefined;
  }

  /** The WebSocket server every worker runs on its own socket path. It routes `/pipeline/<i>/…`
   * frames to pipeline `i`'s `receiveFrame()`. */
  startWorkerServer(): void {
    // A live Set, answered over IPC for tests; not a lifetime counter.
    const openConnections = new Set<PipelineSocket>();
    // oxlint-disable-next-line anti-slop/no-unknown-parameters -- this IS the I/O boundary parser the rule's own message asks for; a worker's own "message" event is genuinely unparsed until this function runs, same reason isWsReadyMessage (above) narrows to unknown first
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
      const match = route !== undefined ? /^\/pipeline\/(\d+)\//.exec(route) : null;
      const pipeline = match ? this.lookup(Number(match[1])) : undefined;
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
}

/** One per process, on the primary and on every worker. */
const wsWorkerSet = new WsWorkerSet();

if (cluster.isWorker) {
  wsWorkerSet.startWorkerServer();
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
    const own: ClusterPipelineConstructorOptions & WebSocketPipelineOptions = {
      ...options,
      connect: "",
    };
    // ⚠ On a worker every terminal op resolves empty.
    if (cluster.isWorker) {
      own.chunks = emptyChunks<T>();
      own.preBufferItems = null;
    }
    super(own);
    this.workers = options?.workers ?? availableParallelism();

    // ⚠ Same index rules as `ClusterHttpPipeline`'s constructor: bound instances and branch arms
    // never register.
    const claimsOwnSlot = options?.bound !== true && (options?.routeTrail ?? "") === "";
    this.pipelineIndex = claimsOwnSlot
      ? wsWorkerSet.register(this as ClusterPipeline<unknown>)
      : (options?.pipelineIndex ?? wsWorkerSet.claimIndex());
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

  /** Prefixes every route with `/pipeline/<pipelineIndex>`, so several pipelines share one worker
   * server.
   *
   * `routePath("transform", 0)` → `/pipeline/2/transform/0` for `pipelineIndex` 2. */
  protected override routePath(verb: RouteVerb, index: number): string {
    return `/pipeline/${this.pipelineIndex}${super.routePath(verb, index)}`;
  }

  /** Starts the workers if needed and picks the next one per dispatch; see
   * `WebSocketPipeline.resolveConnect()` for why it is never stored. */
  protected override resolveConnect(): Promise<ResolvedConnect> {
    return wsWorkerSet.enter(this.workers);
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

type ClusterPipelineConstructorOptions = ClusterPipelineOptions &
  PipelineConstructorOptions & { pipelineIndex?: number };
