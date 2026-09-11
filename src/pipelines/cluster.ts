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
import { HttpPipeline, toNodeHandler, errorResponse } from "@src/pipelines/http";
import type { HttpPipelineOptions } from "@src/pipelines/http";
import { emptyChunks, Pipeline } from "@src/pipeline";
import type { PipelineConstructorOptions, WrappablePipeline } from "@src/pipeline";
import type { Transformer } from "@src/transformer";
import type {
  InternalTransformer,
  ReduceFunction,
  PipelineMode,
  ReduceWork,
  RouteVerb,
} from "@src/types";

/** Construction-time knobs for `ClusterPipeline`. */
export type ClusterPipelineOptions = { workers?: number } & ConcurrentPipelineOptions;

/** `ClusterPipeline`'s real constructor parameter type - see `ConcurrentPipelineConstructorOptions`
 * (`pipelines/concurrent.ts`) for why the base `Pipeline` internals must be included here too.
 * `pipelineIndex` is internal plumbing (below), never set by a caller. */
type ClusterPipelineConstructorOptions = ClusterPipelineOptions &
  PipelineConstructorOptions & { pipelineIndex?: number };

interface BootstrapResult {
  port: number;
}

/** How long with zero in-flight dispatches before workers are killed and the process can exit on
 * its own (Done-when 3). Not a caller-facing option - the ticket names the mechanism (an unref'd
 * idle timer), not a tuned value; a re-fork after a real idle gap costs ~50-60ms (architecture.md's
 * own measurement), which this window is comfortably larger than for back-to-back dispatches. */
const IDLE_KILL_MS = 500;

/**
 * The per-PROCESS state every `ClusterPipeline` instance shares, on both the primary and every
 * worker (`cluster.fork()` re-execs the entry module, so this class is instantiated once per
 * worker too) - one object instead of 5 module-level mutable bindings and 4 free functions closing
 * over them (#133). `workers` (below the class) is the ONE instance this file ever constructs.
 *
 * `register()`/`lookup()` are the pipeline registry: every `ClusterPipeline` ever constructed in
 * this process, keyed by its own `pipelineIndex` - a worker's own copy ends up identical to the
 * primary's, because both run the exact same entry module, constructing pipelines in the exact
 * same order (product.md's own "index N means the same transform on both sides", one level up).
 * `enter()` is `bootstrap()` plus the `inFlight`/idle-kill bracket `stageWork()` (once per chunk)
 * and `reduceWork()` (once per whole stream) both need - a caller `await`s it, does its dispatch,
 * then calls the release it returns; `stageWork()`'s own `finally { inFlight--; scheduleIdleCheck();
 * }` and `reduceWork()`'s identical copy collapse to that one call.
 */
class WorkerSet {
  private nextPipelineIndex = 0;
  private readonly registry = new Map<number, ClusterPipeline<unknown>>();
  private bootstrapPromise: Promise<BootstrapResult> | undefined;
  private inFlight = 0;
  private idleTimer: ReturnType<typeof setTimeout> | undefined;

  /** Claims the next pipeline index, registering `pipeline` at it so a routed request can find it
   * later - see `claimIndex()` for the unregistered, index-only case (`registries()`'s own replay,
   * a `.branch()` arm). */
  register(pipeline: ClusterPipeline<unknown>): number {
    const index = this.nextPipelineIndex++;
    this.registry.set(index, pipeline);
    return index;
  }

  /** Claims the next pipeline index with no registry entry - the case that must NOT be routable:
   * a bound replay or a `.branch()` arm, per `ClusterPipeline`'s own constructor comment. */
  claimIndex(): number {
    return this.nextPipelineIndex++;
  }

  lookup(index: number): ClusterPipeline<unknown> | undefined {
    return this.registry.get(index);
  }

  /** Forks `workerCount` workers (default `os.availableParallelism()`, per the ticket's own
   * Constraints), waits for every one to report the port it ended up listening on via `.fork()`'s
   * own IPC channel - `listen(0)` inside `cluster` yields every worker the SAME port
   * (architecture.md's own probe), so the first one to report it IS the shared port. Memoized:
   * every `ClusterPipeline` in this process shares the same in-flight or already-resolved bootstrap
   * (Done-when 4). */
  bootstrap(workerCount: number): Promise<BootstrapResult> {
    this.bootstrapPromise ??= new Promise((resolve, reject) => {
      const count = workerCount > 0 ? workerCount : availableParallelism();
      let sharedPort: number | undefined;
      let readyCount = 0;
      let settled = false;
      for (let i = 0; i < count; i++) {
        const worker = cluster.fork();
        worker.on("message", (message: unknown) => {
          const { type, port } = (message ?? {}) as { type?: string; port?: number };
          if (type !== "outputty-pipeline-ready" || typeof port !== "number") return;
          sharedPort ??= port;
          readyCount++;
          if (readyCount === count && !settled) {
            settled = true;
            resolve({ port: sharedPort! });
          }
        });
        // A worker that dies before reporting its port must REJECT (#113). With a resolve-only
        // promise, `readyCount` simply stalled below `count` and the bootstrap stayed pending
        // forever: a bad import in the entry module, a port-permission failure or an OOM kill left
        // every later `stageWork`/`reduceWork` awaiting a promise that never settles, so the
        // terminal op neither returned nor threw. A silent hang is the one outcome with no
        // diagnosis in it.
        const fail = (detail: string): void => {
          if (settled) return;
          settled = true;
          reject(new Error(`a ClusterPipeline worker failed before reporting its port: ${detail}`));
        };
        worker.on("error", (error: Error) => fail(error.message));
        worker.on("exit", (code, signal) => fail(`exited with code ${code}, signal ${signal}`));
      }
    });
    return this.bootstrapPromise;
  }

  /** Bootstraps if needed, marks one dispatch in flight, and returns the port plus its release -
   * `stageWork()` and `reduceWork()` each `await workerSet.enter(this.workers)`, read `port` for
   * `this._url`, dispatch, then call `release` exactly once (its own `released` guard makes a
   * second call a no-op, so a caller's own `finally` never double-decrements). `port` is returned
   * here rather than re-read via a second `bootstrap()` call (#133 review: `bootstrap()` is
   * memoized so a second call is not a race, but it is a needless microtask hop on every
   * dispatch for a value this method already has). */
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

  /** Reschedules the idle-kill check, `unref()`'d so the timer itself never keeps the process
   * alive - only the (deliberately NOT unref'd) worker processes do that, until this fires. */
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

  /** `worker.kill()` (not `.unref()`) - forked workers hold the event loop open through cluster's
   * shared `TCPServerWrap`, which no public API exposes to release (architecture.md's own
   * constraint), so an idle process only exits once every worker is actually killed. */
  kill(): void {
    for (const worker of Object.values(cluster.workers ?? {})) {
      worker?.kill();
    }
    this.bootstrapPromise = undefined; // a later dispatch bootstraps a fresh set
  }

  /** The one HTTP server every worker runs, routing `/pipeline/<i>/transform/<n>` to pipeline `i`'s
   * own `.fetch()` - which then parses `/stage/<n>` itself, prefix-agnostic, exactly as
   * `HttpPipeline` already does for two plain instances. The registry is read at REQUEST time,
   * always after the worker's own copy of the entry module has finished its synchronous top-level
   * construction (Node runs a script's synchronous code to completion before any I/O callback, an
   * incoming request included) - architecture.md's own "the module must complete so every apply()
   * call registers its stage" falls out of that ordering, not anything this method does itself. */
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

/** The one per-process `WorkerSet` every `ClusterPipeline` in this process shares - constructed
 * once, on both the primary and every worker (`cluster.fork()` re-execs this module). */
const workerSet = new WorkerSet();

if (cluster.isWorker) {
  workerSet.startWorkerServer();
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
   * `emptyChunks()` below is a different thing and still runs: it empties a WORKER process's own
   * already-bound copy, so a worker never orchestrates a drain of its own. */
  constructor(pipeline: WrappablePipeline<T, In>, options?: ClusterPipelineOptions);
  constructor(options?: ClusterPipelineConstructorOptions);
  constructor(
    first?: WrappablePipeline<T, In> | ClusterPipelineConstructorOptions,
    second?: ClusterPipelineOptions,
  ) {
    const options = Pipeline.wrapping<ClusterPipelineConstructorOptions>(first, second);
    // The real url is only known once workerSet.bootstrap() (below) picks a port; "" is inert
    // until the first actual dispatch sets it, inside stageWork()'s own returned closure.
    super({ ...options, url: "" });
    this.workers = options?.workers ?? availableParallelism();

    // A COMPOSED, trail-less instance is its own logical pipeline and claims its own slot;
    // everything else carries the slot it was built from (#113). Three cases, and the middle one
    // is the defect this replaces:
    //
    // - `new ClusterPipeline(...)` and every `.transform()`/`.context()` off one - composed,
    //   unbound, no trail. Each claims a FRESH index. Before this they all inherited the base's,
    //   so two sibling chains off one base both registered at it and the second overwrote the
    //   first: measured, `base.transform(x*2)` and `base.transform(x*100)` were all
    //   `pipelineIndex 0`, and calling the first returned `[100,200]` for `[1,2]` - the second
    //   chain's stages, no error.
    // - A BOUND instance - `registries()`'s own `bind([])` replay, or a real call's `bind(input)`.
    //   It must NOT claim, and not only to avoid a spare slot: the serving side replays LAZILY, on
    //   first request, so a claim there would advance this process's counter at a moment the
    //   orchestrator never reaches. Both sides agree on an index only while every claim happens
    //   during composition, which the entry module runs identically in both.
    // - A `.branch()` arm, which carries a route trail and is reached THROUGH its parent's route.
    //   Registering it overwrote the parent at the shared index, and the run returned
    //   `REST:undefined` rather than failing.
    const claimsOwnSlot = options?.bound !== true && (options?.routeTrail ?? "") === "";
    this.pipelineIndex = claimsOwnSlot
      ? workerSet.register(this as ClusterPipeline<unknown>)
      : (options?.pipelineIndex ?? workerSet.claimIndex());

    // architecture.md's own constraint: a WORKER process's terminal op must resolve immediately
    // with an EMPTY result - the worker exists to hold the transforms (registered by the
    // .transform() calls below THIS constructor call, in the entry module the worker re-executes),
    // never to orchestrate. Emptying `_chunks` here, once, propagates through every later
    // copy-on-write step automatically: a fan-out built over an empty source yields nothing, so
    // the NEXT instance's own `_chunks` (that fan-out's generator) is empty too.
    if (cluster.isWorker) {
      this._chunks = emptyChunks<T>();
      this._preBufferItems = null;
    }
  }

  /**
   * Carries `workers`/`pipelineIndex` into the NEXT instance a copy-on-write call builds, on top of
   * `HttpPipeline.carriedKnobs()`'s own `maxConcurrency`/`ordered`/`url` (#133 - `url` needs no
   * re-spelling here: `super.carriedKnobs()` already reads `this._url`, and `this` is this
   * instance's own `_url`, kept correct once a real dispatch has set it, so a `.context()` call
   * after the pipeline is already live does not reset it back to "").
   */
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
   * `HttpPipeline.local()`'s own logic runs unchanged via `super`. `bind()`/`sourcePolicy()` need no
   * such re-declaration (#133) - see `ConcurrentPipeline.sourcePolicy()`'s own docstring for why
   * this class has neither any more.
   */
  override local<U, M2 extends PipelineMode>(
    build: (p: Pipeline<T, "async", any>) => Pipeline<U, M2, any>,
  ): ClusterPipeline<U, In> {
    return super.local(build) as unknown as ClusterPipeline<U, In>;
  }

  /** Routes this pipeline's stages through `/pipeline/<pipelineIndex>/<verb>/<n>` instead of plain
   * `HttpPipeline`'s `/<verb>/<n>` - the one hook `routePath()` (`http.ts`) exists for, so several
   * `ClusterPipeline`s can share one worker server without colliding on stage 0. */
  protected override routePath(verb: RouteVerb, index: number): string {
    return `/pipeline/${this.pipelineIndex}${super.routePath(verb, index)}`;
  }

  /** Bootstraps the shared worker set (memoized, `WorkerSet.bootstrap()`), points `this._url` at
   * it, marks one dispatch in flight, and returns its release - `stageWork()` (once per chunk) and
   * `reduceWork()` (once per whole stream) each call this once instead of inlining the identical
   * bootstrap/`inFlight` bracket. */
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

  /**
   * `stageWork()`'s own bootstrap/`inFlight` wrap, but for the WHOLE stream rather than once per
   * chunk - a reduce stage is one long-lived connection, so the bootstrap and `inFlight` bracket
   * the entire generator's life, not each chunk dispatched through it.
   */
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
