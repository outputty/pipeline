/**
 * `ClusterPipeline` (#201) — `ClusterHttpPipeline`'s "each chunk dispatched to another process"
 * shape (`cluster.ts`), over `WebSocketPipeline`'s own multiplexed `ws+unix:` wire instead: each
 * worker binds its own UNIQUE unix socket path (never a shared port - a WebSocket connection is
 * persistent, so #201's own Done-when 3 needs one distinct target per worker to count one
 * connection each), and dispatch round-robins across the bootstrapped set (`WsWorkerSet.enter()`).
 * Mirrors `WorkerSet`'s own shape throughout (`cluster.ts`) - `register()`/`claimIndex()`/`lookup()`/
 * `bootstrap()`/`enter()`/`kill()` all carry the identical role, one seam (a port vs. a set of
 * socket paths) apart. Workers come up lazily, on the first chunk actually dispatched.
 *
 * Split out of `cluster.ts` (#239) so the root entry never loads `ws`: this file ships on the
 * `@outputty/pipeline/websocket` entry only. It must not import `cluster.ts`, whose module scope
 * starts the HTTP worker server in every worker - a `/websocket`-only worker starts only this file's
 * server.
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

/** Validates a worker's own IPC "ready" message before `WsWorkerSet.bootstrap()` trusts its
 * `socketPath` - the WS-wire counterpart of `isReadyMessage` (`cluster.ts`), same reason.
 *
 * `isWsReadyMessage({ type: "outputty-pipeline-ws-ready", socketPath: "/tmp/w.sock" })` → `true`.
 */
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

/**
 * The per-PROCESS state every `ClusterPipeline` instance shares (#201) - `WorkerSet`'s own shape,
 * over N distinct socket paths instead of one shared port: a WebSocket connection is persistent, so
 * sharing one target across every worker would mean one worker ever gets dialed. `enter()`
 * round-robins across the bootstrapped set instead of handing back the single shared value
 * `WorkerSet.enter()` does.
 */
class WsWorkerSet {
  private nextPipelineIndex = 0;
  private readonly registry = new Map<number, ClusterPipeline<unknown>>();
  private bootstrapPromise: Promise<string[]> | undefined;
  private inFlight = 0;
  private idleTimer: ReturnType<typeof setTimeout> | undefined;
  private nextWorkerIndex = 0;
  /** `cluster.Worker.id` of every worker THIS set forked - `kill()` (below) uses it to kill only
   * its own workers out of `cluster.workers`, a registry `node:cluster` shares process-wide with
   * every other `WorkerSet`/`WsWorkerSet` in the process - same reason `WorkerSet.ownWorkerIds`
   * exists (#201 review). */
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

  /** Forks `workerCount` workers, waits for every one to report its OWN unique socket path over
   * `.fork()`'s own IPC channel - each worker computes its path from its own `process.pid`
   * (`startWorkerServer()`, below), guaranteed unique with no coordination needed. Collected by
   * arrival, not fork order: any stable set of `count` distinct paths round-robins identically.
   * Memoized exactly like `WorkerSet.bootstrap()` - every `ClusterPipeline` in this process shares
   * the same in-flight or already-resolved bootstrap. */
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
        // Same reasoning as `WorkerSet.bootstrap()`'s identical guard: a worker that dies before
        // reporting its socket must REJECT, or every later dispatch awaits a promise that never
        // settles.
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

  /** Bootstraps if needed, round-robins to the NEXT worker's own `connect` target, marks one
   * dispatch in flight, and returns its release - `stageWork()`/`reduceWork()` each call this once,
   * mirroring `WorkerSet.enter()`. */
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
      this.scheduleIdleCheck();
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

  /** Kills only the workers THIS set forked (`ownWorkerIds`, above), by id against
   * `cluster.workers` - same reason `WorkerSet.kill()` does (#201 review: a process using both
   * classes had one set's idle timer killing the other's still-in-flight workers, since both
   * iterated the same global map unconditionally). */
  kill(): void {
    const workers = cluster.workers ?? {};
    for (const id of this.ownWorkerIds) {
      workers[id]?.kill();
    }
    this.ownWorkerIds.clear();
    this.bootstrapPromise = undefined;
  }

  /** The one WS server every worker runs, routing a frame's own `/pipeline/<i>/` prefix
   * (`peekFrame()`, `websocket.ts`) to pipeline `i`'s own `receiveFrame()` - the WS-wire
   * counterpart of `WorkerSet.startWorkerServer()`. Each worker's own socket path is its own
   * `process.pid` (guaranteed unique, no coordination needed to avoid a collision), reported back
   * over IPC once listening.
   *
   * `openConnections` counts `WebSocketServer`'s own live `"connection"` count - a `Set` of the
   * connected sockets, not a lifetime total (#201 docs review: a plain incrementing counter read a
   * transient reconnect on one worker as two, which a real ordered-frame reconnect can legitimately
   * cause; a `Set` sized by `.size` only ever reports what's connected NOW). `toNodeWebSocketHandler()`
   * calls `pipeline.serve()` from inside its own `wss.on("connection", ...)` listener, once per
   * accepted connection, so tracking membership here IS counting that event - `#201`'s own Done-when
   * 3. Answered over `cluster.fork()`'s own IPC channel (`"outputty-pipeline-query-connections"` in,
   * `"outputty-pipeline-connections"` out) rather than exposed as a public API, since no caller-facing
   * knob observes it. */
  startWorkerServer(): void {
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

/** The one per-process `WsWorkerSet` every `ClusterPipeline` in this process shares - constructed
 * once, on both the primary and every worker, mirroring `workerSet` (`cluster.ts`). */
const wsWorkerSet = new WsWorkerSet();

if (cluster.isWorker) {
  wsWorkerSet.startWorkerServer();
}

/**
 * Each chunk of a stage dispatched to another process on the SAME machine, over a persistent
 * multiplexed WebSocket connection (#201). Brings up its own `node:cluster` workers on first run,
 * each its own `ws+unix:` socket; every later `ClusterPipeline` in the process reuses them. Fully
 * opaque: no server, no listen, no fork, no socket path in caller code.
 *
 * `new ClusterPipeline([1,2,3,4,5]).transform((t) => t.map((x) => x * 2)).toArray()` →
 * `[2,4,6,8,10]`, served by real worker processes over `ws+unix:`.
 */
export class ClusterPipeline<T, In = T> extends WebSocketPipeline<T, In> {
  /** Worker processes to bring up on first drain. Default `os.availableParallelism()`. */
  readonly workers: number;
  /** This pipeline's stable position among every `ClusterPipeline` constructed in this process -
   * see `ClusterHttpPipeline.pipelineIndex`, identical role. */
  readonly pipelineIndex: number;

  /** Wraps a chain built elsewhere, dispatching its stages to forked worker processes (#90) - see
   * `ClusterHttpPipeline`'s own constructor, identical shape and reasoning. */
  constructor(pipeline: WrappablePipeline<T, In>, options?: ClusterPipelineOptions);
  constructor(options?: ClusterPipelineConstructorOptions);
  constructor(
    first?: WrappablePipeline<T, In> | ClusterPipelineConstructorOptions,
    second?: ClusterPipelineOptions,
  ) {
    const options = Pipeline.wrapping<ClusterPipelineConstructorOptions>(first, second);
    // The real connect target is only known once wsWorkerSet.bootstrap() (below) picks a socket
    // path; "" is inert until the first actual dispatch sets it, inside stageWork()'s own returned
    // closure - same reason ClusterHttpPipeline's constructor passes url: "".
    super({ ...options, connect: "" });
    this.workers = options?.workers ?? availableParallelism();

    // Identical three-case slot-claiming logic to ClusterHttpPipeline's own constructor - see its
    // comment for the defect this replaces (#113).
    const claimsOwnSlot = options?.bound !== true && (options?.routeTrail ?? "") === "";
    this.pipelineIndex = claimsOwnSlot
      ? wsWorkerSet.register(this as ClusterPipeline<unknown>)
      : (options?.pipelineIndex ?? wsWorkerSet.claimIndex());

    // Same architecture.md constraint ClusterHttpPipeline's own constructor honors: a WORKER
    // process's terminal op must resolve immediately with an EMPTY result.
    if (cluster.isWorker) {
      this._chunks = emptyChunks<T>();
      this._preBufferItems = null;
    }
  }

  /** Carries `workers`/`pipelineIndex` into the NEXT instance a copy-on-write call builds - see
   * `ClusterHttpPipeline.carriedKnobs()`, identical role. */
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

  /** Routes this pipeline's stages through `/pipeline/<pipelineIndex>/<verb>/<n>` instead of plain
   * `WebSocketPipeline`'s `/<verb>/<n>` - see `ClusterHttpPipeline.routePath()`, identical role,
   * now overriding the SAME base (`ConcurrentPipeline.routePath()`, #201 review) either class
   * ultimately shares. */
  protected override routePath(verb: RouteVerb, index: number): string {
    return `/pipeline/${this.pipelineIndex}${super.routePath(verb, index)}`;
  }

  /**
   * Bootstraps the shared WS worker set and round-robins to the NEXT worker's own socket - overrides
   * `WebSocketPipeline.resolveConnect()` (#201 review) rather than wrapping `stageWork()`/
   * `reduceWork()` to mutate `this._connect` before calling `super`'s: that shared, mutable field
   * raced under concurrent dispatch (`ConcurrentPipeline.reduce()` launches every partition in one
   * synchronous burst, so every partition's own round-robin write landed on the SAME field before
   * any of them read it back) and every partition ended up on whichever worker the LAST write
   * picked - found live, a `maxConcurrency: 2` reduce read ONE connection, not two. `resolveConnect()`
   * is called fresh, once, by EACH dispatch (`WebSocketPipeline`'s own `stageWork()`/`reduceWork()`),
   * so there is nothing shared left to race on; `wsWorkerSet.enter()`'s own `{ connect, release }`
   * shape already matches `ResolvedConnect` exactly, no adapting needed.
   */
  protected override resolveConnect(): Promise<ResolvedConnect> {
    return wsWorkerSet.enter(this.workers);
  }
}

/** Construction-time knobs for `ClusterPipeline`. `codec` is `WebSocketPipeline`'s own knob,
 * forwarded to `super` unchanged - it never crosses the process boundary itself, so each worker
 * builds its own by re-running the entry module, and a store it writes to must be reachable from
 * every worker (`process.env`, which `cluster.fork()` inherits). */
export type ClusterPipelineOptions = {
  workers?: number;
  codec?: Codec;
} & ConcurrentPipelineOptions;

/** `ClusterPipeline`'s real constructor parameter type - see `ClusterHttpPipelineConstructorOptions`
 * for why the base `Pipeline` internals must be included here too. `pipelineIndex` is internal
 * plumbing, never set by a caller. */
type ClusterPipelineConstructorOptions = ClusterPipelineOptions &
  PipelineConstructorOptions & { pipelineIndex?: number };
