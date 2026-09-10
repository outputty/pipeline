/**
 * `ClusterPipeline` (#17) — each chunk of a stage dispatched to another PROCESS on the same
 * machine, via `node:cluster`. Reuses `HttpPipeline`'s own dispatch and `.fetch()` wholesale -
 * a cluster worker is just another `HttpPipeline` instance, reached at
 * `http://localhost:<bootstrapped port>`; `routePath()` changes to route several pipeline
 * definitions through the ONE server every worker runs (`/pipeline/<i>/<verb>/<n>`), and
 * `reduceWork()` (#45) wraps the SAME bootstrap/`inFlight` bracket `stageWork()` uses, but around
 * the whole reduce connection rather than one chunk.
 *
 * Brings its own workers up lazily, on the first chunk actually dispatched - `stageWork()` itself
 * still runs at BUILD time (`ConcurrentPipeline.apply()` calls it synchronously), but the bootstrap
 * lives inside the closure it RETURNS, which only ever runs when a terminal op drains the pipeline.
 * A `ClusterPipeline` built and never drained (case 7's own `.constructor.name` check) never forks.
 */

import cluster from "node:cluster";
import { createServer } from "node:http";
import { availableParallelism } from "node:os";
import type { AddressInfo } from "node:net";
import type { ConcurrentPipelineOptions } from "@src/pipelines/concurrent";
import { HttpPipeline, toNodeHandler } from "@src/pipelines/http";
import { EMPTY_CHUNKS, Pipeline } from "@src/pipeline";
import type { PipelineOptions, PipelineSource, WrappablePipeline } from "@src/pipeline";
import type { Transformer } from "@src/transformer";
import type {
  IContextManager,
  InternalTransformer,
  ReduceFunction,
  SourcePolicy,
  PipelineMode,
} from "@src/types";

/** Construction-time knobs for `ClusterPipeline`. */
export type ClusterPipelineOptions = { workers?: number } & ConcurrentPipelineOptions;

/** `ClusterPipeline`'s real constructor parameter type - see `ConcurrentPipelineConstructorOptions`
 * (`pipelines/concurrent.ts`) for why the base `Pipeline` internals must be included here too.
 * `pipelineIndex` is internal plumbing (below), never set by a caller. */
type ClusterPipelineConstructorOptions = ClusterPipelineOptions &
  PipelineOptions & { pipelineIndex?: number };

// ---- module-level, per-PROCESS state - one shared bootstrap and one shared registry for every
// ClusterPipeline instance, on both the primary and every worker (`cluster.fork()` re-execs the
// entry module, so this file, and everything in it, runs once per worker too). ----

let nextPipelineIndex = 0;
/** Every `ClusterPipeline` ever constructed in THIS process, keyed by its `pipelineIndex` - a
 * worker's own copy of this registry ends up identical to the primary's, because both run the
 * exact same entry module, constructing pipelines in the exact same order (product.md's own
 * "index N means the same transform on both sides", one level up). */
const registry = new Map<number, ClusterPipeline<unknown>>();

interface BootstrapResult {
  port: number;
}
let bootstrapPromise: Promise<BootstrapResult> | undefined;
let inFlight = 0;
let idleTimer: ReturnType<typeof setTimeout> | undefined;
/** How long with zero in-flight dispatches before workers are killed and the process can exit on
 * its own (Done-when 3). Not a caller-facing option - the ticket names the mechanism (an unref'd
 * idle timer), not a tuned value; a re-fork after a real idle gap costs ~50-60ms (architecture.md's
 * own measurement), which this window is comfortably larger than for back-to-back dispatches. */
const IDLE_KILL_MS = 500;

/** `worker.kill()` (not `.unref()`) - forked workers hold the event loop open through cluster's
 * shared `TCPServerWrap`, which no public API exposes to release (architecture.md's own
 * constraint), so an idle process only exits once every worker is actually killed. */
function killWorkers(): void {
  for (const worker of Object.values(cluster.workers ?? {})) {
    worker?.kill();
  }
  bootstrapPromise = undefined; // a later dispatch bootstraps a fresh set
}

/** Reschedules the idle-kill check, `unref()`'d so the timer itself never keeps the process alive -
 * only the (deliberately NOT unref'd) worker processes do that, until this fires. */
function scheduleIdleCheck(): void {
  if (idleTimer) clearTimeout(idleTimer);
  idleTimer = setTimeout(() => {
    if (inFlight > 0) {
      scheduleIdleCheck();
      return;
    }
    killWorkers();
  }, IDLE_KILL_MS);
  idleTimer.unref();
}

/** Forks `workerCount` workers (default `os.availableParallelism()`, per the ticket's own
 * Constraints), waits for every one to report the port it ended up listening on via `.fork()`'s own
 * IPC channel - `listen(0)` inside `cluster` yields every worker the SAME port (architecture.md's
 * own probe), so the first one to report it IS the shared port. Memoized: every `ClusterPipeline`
 * in this process shares the same in-flight or already-resolved bootstrap (Done-when 4). */
function bootstrapCluster(workerCount: number): Promise<BootstrapResult> {
  bootstrapPromise ??= new Promise((resolve) => {
    const count = workerCount > 0 ? workerCount : availableParallelism();
    let sharedPort: number | undefined;
    let readyCount = 0;
    for (let i = 0; i < count; i++) {
      const worker = cluster.fork();
      worker.on("message", (message: unknown) => {
        const { type, port } = (message ?? {}) as { type?: string; port?: number };
        if (type !== "outputty-pipeline-ready" || typeof port !== "number") return;
        sharedPort ??= port;
        readyCount++;
        if (readyCount === count) resolve({ port: sharedPort! });
      });
    }
  });
  return bootstrapPromise;
}

/** The one HTTP server every worker runs, routing `/pipeline/<i>/transform/<n>` to pipeline `i`'s own
 * `.fetch()` - which then parses `/stage/<n>` itself, prefix-agnostic, exactly as `HttpPipeline`
 * already does for two plain instances. `registry` is read at REQUEST time, always after the
 * worker's own copy of the entry module has finished its synchronous top-level construction (Node
 * runs a script's synchronous code to completion before any I/O callback, an incoming request
 * included) - architecture.md's own "the module must complete so every apply() call registers its
 * stage" falls out of that ordering, not anything this function does itself. */
function startWorkerServer(): void {
  const routeToRegisteredPipeline = async (request: Request): Promise<Response> => {
    const { pathname } = new URL(request.url);
    const match = /^\/pipeline\/(\d+)\//.exec(pathname);
    const pipeline = match ? registry.get(Number(match[1])) : undefined;
    if (!pipeline) {
      return Response.json({ error: `unknown pipeline route ${pathname}` }, { status: 404 });
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
 * Each chunk of a stage dispatched to another process on the SAME machine (#17). Brings up its
 * own `node:cluster` workers on first run; every later `ClusterPipeline` in the process reuses
 * them. Fully opaque: no server, no listen, no fork, no url in caller code.
 *
 * `new ClusterPipeline([1,2,3,4,5]).transform((t) => t.map((x) => x * 2)).toArray()` →
 * `[2,4,6,8,10]`, served by real worker processes.
 */
export class ClusterPipeline<T, In = T> extends HttpPipeline<T, In> {
  /** Worker processes to bring up on first drain. Default `os.availableParallelism()`. */
  readonly workers: number;
  /** This pipeline's stable position among every `ClusterPipeline` constructed in this process -
   * carried forward through copy-on-write (never reassigned by `.transform()`/`.context()`/…), so
   * the SAME logical pipeline keeps the SAME route on both the primary and every worker. */
  readonly pipelineIndex: number;

  /** Wraps a chain built elsewhere, dispatching its stages to forked worker processes (#90). The
   * CALLER no longer writes a placeholder source, because a wrapped chain has none by construction.
   * `EMPTY_CHUNKS` below is a different thing and still runs: it empties a WORKER process's own
   * already-bound copy, so a worker never orchestrates a drain of its own. */
  constructor(pipeline: WrappablePipeline<T, In>, options?: ClusterPipelineOptions);
  constructor(options?: ClusterPipelineConstructorOptions);
  constructor(
    first?: WrappablePipeline<T, In> | ClusterPipelineConstructorOptions,
    second?: ClusterPipelineOptions,
  ) {
    const options = Pipeline.wrapping<ClusterPipelineConstructorOptions>(first, second);
    // The real url is only known once bootstrapCluster() (below) picks a port; "" is inert until
    // the first actual dispatch sets it, inside stageWork()'s own returned closure.
    super({ ...options, url: "" });
    this.workers = options?.workers ?? availableParallelism();
    this.pipelineIndex = options?.pipelineIndex ?? nextPipelineIndex++;
    // Only a chain's OWN pipeline claims a registry slot. A `.branch()` arm carries a route trail
    // and is reached THROUGH its parent's route, so registering it would overwrite the parent at
    // the same `pipelineIndex` - measured, the primary's `/pipeline/0/transform/0` was then served
    // by the arm's stage table, and the run returned `REST:undefined` rather than failing.
    if (this._routeTrail === "") {
      registry.set(this.pipelineIndex, this as ClusterPipeline<unknown>);
    }

    // architecture.md's own constraint: a WORKER process's terminal op must resolve immediately
    // with an EMPTY result - the worker exists to hold the transforms (registered by the
    // .transform() calls below THIS constructor call, in the entry module the worker re-executes),
    // never to orchestrate. Emptying `_chunks` here, once, propagates through every later
    // copy-on-write step automatically: a fan-out built over an empty source yields nothing, so
    // the NEXT instance's own `_chunks` (that fan-out's generator) is empty too.
    if (cluster.isWorker) {
      this._chunks = EMPTY_CHUNKS as AsyncIterable<T[]>;
      this._preBufferItems = null;
    }
  }

  /**
   * Carries `workers`/`pipelineIndex` into the NEXT instance a copy-on-write call builds, alongside
   * `maxConcurrency`/`ordered` (`concurrentOptions()`, inherited) and `url` (kept correct once a
   * real dispatch has set it, so a `.context()` call after the pipeline is already live does not
   * reset it back to "").
   */
  protected override createPipeline<U>(
    chunks: AsyncIterable<U[]>,
    options: PipelineOptions,
  ): ClusterPipeline<U, In> {
    const Ctor = this.constructor as new (
      options?: ClusterPipelineConstructorOptions & { url: string },
    ) => ClusterPipeline<U, In>;
    const merged = {
      ...options,
      ...this.concurrentOptions(),
      workers: this.workers,
      pipelineIndex: this.pipelineIndex,
      url: this._url,
      chunks,
    };
    return new Ctor(merged);
  }

  override transform<U, M2 extends "sync" | "async">(
    // The conditional `this` is deleted with the base's own - see `HttpPipeline.transform()` for
    // the `TS2684` a second type-changing `.transform()` hit while it was still here (#90).
    builder: (t: Transformer<T, T, "async">) => Transformer<T, U, M2>,
  ): ClusterPipeline<U, In> {
    return super.transform(builder) as unknown as ClusterPipeline<U, In>;
  }

  override apply<U>(transformer: Transformer<T, U, "sync" | "async">): ClusterPipeline<U, In> {
    return super.apply(transformer) as unknown as ClusterPipeline<U, In>;
  }

  /** Re-declared ONLY to narrow the static return type back to `ClusterPipeline<U>` - same reason
   * as `.transform()`/`.apply()` above. `HttpPipeline.reduce()`'s own logic runs unchanged. */
  override reduce<U>(fn: ReduceFunction<U, T>, initial: U): ClusterPipeline<U, In> {
    return super.reduce(fn, initial) as unknown as ClusterPipeline<U, In>;
  }

  /**
   * Re-declared ONLY to narrow `Pipeline.local()`'s return type (#61,
   * `~/.claude/rules/typescript.md`) - same reason as `.transform()`/`.apply()`/`.reduce()` above.
   * `HttpPipeline.local()`'s own logic runs unchanged via `super`.
   */
  /**
   * Forced `"async"` whatever the source's shape (#90) - ClusterPipeline exists for I/O-bound work and
   * has no synchronous case, so an array source runs on the async engine here exactly as an
   * `AsyncIterable` one does. `sourcePolicy()` below is the runtime half; the `"async"` third type
   * argument on the `extends` clause above is the compile-time half, and is what makes this
   * override a genuine narrowing of the base's own two arms rather than a conflict with them.
   *
   * `new ClusterPipeline(chain)([1, 2, 3])` runs on the async engine whatever `chain` was.
   */
  protected override bind<U>(data: PipelineSource<U>): ClusterPipeline<U> {
    // `In` becomes `U` here - see `ConcurrentPipeline.bind()`.
    return this.fromSource<U>(data, this.sourcePolicy()) as unknown as ClusterPipeline<U>;
  }

  protected override sourcePolicy(): SourcePolicy {
    return "async";
  }

  override local<U, M2 extends PipelineMode>(
    build: (p: Pipeline<T, "async", any>) => Pipeline<U, M2, any>,
  ): ClusterPipeline<U, In> {
    return super.local(build) as unknown as ClusterPipeline<U, In>;
  }

  /** Routes this pipeline's stages through `/pipeline/<pipelineIndex>/<verb>/<n>` instead of plain
   * `HttpPipeline`'s `/<verb>/<n>` - the one hook `routePath()` (`http.ts`) exists for, so several
   * `ClusterPipeline`s can share one worker server without colliding on stage 0. */
  protected override routePath(verb: "transform" | "reduce", index: number): string {
    return `/pipeline/${this.pipelineIndex}${super.routePath(verb, index)}`;
  }

  /** Bootstraps the shared worker set (memoized, `bootstrapCluster()`) and points `this._url` at
   * it - the one bit `stageWork()` (once per chunk) and `reduceWork()` (once per whole stream)
   * share, rather than each inlining the same two lines. */
  protected async bootstrapAndSetUrl(): Promise<void> {
    const { port } = await bootstrapCluster(this.workers);
    this._url = `http://localhost:${port}`;
  }

  protected override stageWork<U>(
    transformer: Transformer<T, U, "sync" | "async">,
    stageIndex: number,
  ): InternalTransformer<T, U> {
    const dispatch = super.stageWork(transformer, stageIndex);
    return async (chunk, ctx) => {
      await this.bootstrapAndSetUrl();
      inFlight++;
      try {
        return await dispatch(chunk, ctx);
      } finally {
        inFlight--;
        scheduleIdleCheck();
      }
    };
  }

  /**
   * `stageWork()`'s own bootstrap/`inFlight` wrap, but for the WHOLE stream rather than once per
   * chunk - a reduce stage is one long-lived connection, so the bootstrap and `inFlight` bracket
   * the entire generator's life, not each chunk dispatched through it.
   */
  protected override reduceWork<U>(
    fn: ReduceFunction<U, T>,
    initial: U,
    stageIndex: number,
  ): (chunks: AsyncIterable<T[]>, ctx: IContextManager) => AsyncGenerator<U[]> {
    const dispatch = super.reduceWork(fn, initial, stageIndex);
    const self = this;
    return async function* dispatchOnWorker(chunks, ctx) {
      await self.bootstrapAndSetUrl();
      inFlight++;
      try {
        yield* dispatch(chunks, ctx);
      } finally {
        inFlight--;
        scheduleIdleCheck();
      }
    };
  }
}
