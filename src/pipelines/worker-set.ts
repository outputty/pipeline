/**
 * The forked `node:cluster` worker set behind `ClusterHttpPipeline` and `ClusterPipeline`: pipeline
 * slots, the fork-and-wait bootstrap, the in-flight count and the idle kill. Each cluster file owns
 * one instance, its worker server and its wire; this module starts nothing at load.
 */

import cluster from "node:cluster";
import { availableParallelism } from "node:os";
import { emptyChunks } from "@src/pipeline";
import type { PipelineConstructorOptions } from "@src/pipeline";
import type { Drainable } from "@src/types";

/** Idle time with no dispatch in flight before a worker set is killed so the process can exit. */
export const IDLE_KILL_MS = 500;

/** What a cluster pipeline's constructor takes: the carried state plus its own slot. */
export type SlotOptions = PipelineConstructorOptions & { pipelineIndex?: number };

/** How a worker announces it is serving, and what the primary reports when one dies first. */
export type WorkerSetMessages = {
  /** The IPC `type` of a worker's ready message. */
  readyType: string;
  /** The ready message's field holding the worker's address. */
  addressField: string;
  /** The `typeof` that field's value must have. */
  addressKind: "number" | "string";
  /** The rejection text a worker that dies before reporting produces, ahead of `: <detail>`. */
  failure: string;
};

/** A dispatch in flight: every worker's address, in the order they reported, plus its `release`. */
export type WorkerSetEntry<V> = { addresses: V[]; release: () => void };

const PIPELINE_ROUTE = /^\/pipeline\/(\d+)\//;

/**
 * Prefixes a route with its pipeline's slot, so several pipelines share one worker server.
 *
 * `pipelineRoute(2, "/transform/0")` → `/pipeline/2/transform/0`.
 */
export function pipelineRoute(pipelineIndex: number, route: string): string {
  return `/pipeline/${pipelineIndex}${route}`;
}

/**
 * What a cluster pipeline's terminal drains: `drainable` itself on the primary, and no chunks on a
 * worker, because a worker holds the stages and never orchestrates a drain.
 *
 * `drainsNothingOnWorker(drainable)` on a worker → `{ syncChunks: null, chunks: () => <empty> }`
 * with `drainable`'s context.
 */
export function drainsNothingOnWorker<T>(drainable: Drainable<T>): Drainable<T> {
  if (!cluster.isWorker) return drainable;
  return { syncChunks: null, chunks: () => emptyChunks<T>(), context: drainable.context };
}

/**
 * One process's forked workers and the pipelines they serve, keyed by slot. `P` is the pipeline
 * class a worker server routes to; `V` is the address each worker reports when it is ready.
 */
export class WorkerSet<P, V extends number | string> {
  private nextPipelineIndex = 0;
  private readonly registry = new Map<number, P>();
  private bootstrapPromise: Promise<V[]> | undefined;
  private inFlight = 0;
  private idleTimer: ReturnType<typeof setTimeout> | undefined;
  /** ⚠ Kill only these ids: `cluster.workers` also holds every other worker set's workers. */
  private readonly ownWorkerIds = new Set<number>();

  constructor(private readonly messages: WorkerSetMessages) {}

  /**
   * Returns a new pipeline instance's slot, registering it when it is the one a worker should
   * route to.
   * - A composed, unbound instance with no trail claims a fresh slot. ⚠ Inheriting the base's lets
   *   two sibling chains share a slot, and the second silently serves both.
   * - ⚠ A bound instance never claims. `bind()` builds one outside composition, so a claim there
   *   shifts every later slot away from the other process's.
   * - ⚠ A `.branch()` arm never registers. It is reached through its parent's route, and
   *   registering it overwrites the parent.
   */
  claimSlot(pipeline: P, options: SlotOptions | undefined): number {
    if (options?.bound === true || (options?.routeTrail ?? "") !== "") {
      return options?.pipelineIndex ?? this.nextPipelineIndex++;
    }
    const index = this.nextPipelineIndex++;
    this.registry.set(index, pipeline);
    return index;
  }

  /** Returns the registered pipeline a `/pipeline/<i>/…` route names, if any. */
  lookupRoute(route: string | undefined): P | undefined {
    const match = route !== undefined ? PIPELINE_ROUTE.exec(route) : null;
    return match ? this.registry.get(Number(match[1])) : undefined;
  }

  private bootstrap(workerCount: number): Promise<V[]> {
    this.bootstrapPromise ??= new Promise((resolve, reject) => {
      const count = workerCount > 0 ? workerCount : availableParallelism();
      const addresses: V[] = [];
      let settled = false;
      for (let i = 0; i < count; i++) {
        const worker = cluster.fork();
        this.ownWorkerIds.add(worker.id);
        worker.on("message", (message) => {
          const address = this.readAddress(message);
          if (address === undefined) return;
          addresses.push(address);
          if (addresses.length === count && !settled) {
            settled = true;
            resolve(addresses);
          }
        });
        // ⚠ A worker that dies before reporting must reject, or every dispatch hangs forever.
        const fail = (detail: string): void => {
          if (settled) return;
          settled = true;
          reject(new Error(`${this.messages.failure}: ${detail}`));
        };
        worker.on("error", (error: Error) => fail(error.message));
        worker.on("exit", (code, signal) => fail(`exited with code ${code}, signal ${signal}`));
      }
    });
    return this.bootstrapPromise;
  }

  private readAddress(
    // oxlint-disable-next-line anti-slop/no-unknown-parameters -- this IS the I/O boundary parser the rule's own message asks for; message is genuinely unparsed until this function runs
    message: unknown,
  ): V | undefined {
    const { readyType, addressField, addressKind } = this.messages;
    if (typeof message !== "object" || message === null) return undefined;
    if (!("type" in message) || message.type !== readyType) return undefined;
    if (!(addressField in message)) return undefined;
    const address = (message as Record<string, V>)[addressField];
    return typeof address === addressKind ? address : undefined;
  }

  /** Starts the workers once per process, marks one dispatch in flight and returns every worker's
   * address plus the dispatch's `release`. A second `release` call does nothing. */
  async enter(workerCount: number): Promise<WorkerSetEntry<V>> {
    const addresses = await this.bootstrap(workerCount);
    this.inFlight++;
    let released = false;
    const release = (): void => {
      if (released) return;
      released = true;
      this.inFlight--;
      if (this.inFlight === 0) this.scheduleIdleCheck();
    };
    return { addresses, release };
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
  private kill(): void {
    const workers = cluster.workers ?? {};
    for (const id of this.ownWorkerIds) {
      workers[id]?.kill();
    }
    this.ownWorkerIds.clear();
    this.bootstrapPromise = undefined; // a later dispatch bootstraps a fresh set
  }
}
