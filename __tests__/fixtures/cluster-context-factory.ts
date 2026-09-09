/**
 * #31 Done-when 5 — a `ClusterPipeline` worker builds the CALLER'S OWN `IContextManager` class in
 * its own process, via `contextFactory`, and the value a stage's own `.context()` call sets
 * (`multiplier`) still crosses the wire onto that same worker-built instance. The orchestrator
 * process, given an explicit `context` instance instead, never invokes `contextFactory` at all -
 * `context` and `contextFactory` together is not an error (Constraints): each serves a different
 * process.
 */
import cluster from "node:cluster";
import { ClusterPipeline } from "../../src";
import type { IContextManager } from "../../src";

let factoryCalls = 0;

/** `builtPid` is a plain field, not a context KEY - the wire forwards the orchestrator's own
 * context values onto whichever manager serves a request (`.fetch()`'s own `.set()` loop,
 * `src/pipelines/http.ts`), so a value stored through `.set()`/`.get()` would be overwritten by
 * the orchestrator's OWN `builtPid` the moment a request lands, rather than surviving as "which
 * pid built THIS instance". */
class PoolContext implements IContextManager {
  readonly builtPid = process.pid;
  private data: Record<string, unknown> = {};
  get(key: string): unknown {
    return this.data[key];
  }
  set(key: string, value: unknown): void {
    this.data[key] = value;
  }
  getOrDefault<T>(key: string, defaultValue: T): T {
    const value = this.data[key];
    return value !== undefined ? (value as T) : defaultValue;
  }
  toDict(): Record<string, unknown> {
    return { ...this.data };
  }
}

// Only the PRIMARY gets an already-built instance handed in - a worker re-executing this same
// module has no way to receive it across the process boundary, so it relies on `contextFactory`.
const orchestratorInstance = cluster.isPrimary ? new PoolContext() : undefined;

const workers = 3;
// More items than workers, matching cluster-pids.ts's own convention (node-parallelism skill, T3):
// a reused keep-alive connection stays pinned to one worker for its life, so enough chunks must be
// in flight at once (maxConcurrency === workers) for the primary to round-robin a NEW connection
// to every worker at all.
const items = Array.from({ length: 30 }, (_, i) => i);

const pipeline = new ClusterPipeline<number>({
  workers,
  maxConcurrency: workers,
  context: orchestratorInstance,
  contextFactory: () => {
    factoryCalls++;
    return new PoolContext();
  },
});

const out = await pipeline
  .context({ multiplier: 10 })
  .buffer(1)
  .transform((t) =>
    t.map((x: number, ctx) => ({
      value: x * (ctx.get("multiplier") as number),
      pid: process.pid,
      builtPid: (ctx as PoolContext).builtPid,
      ctxClass: ctx.constructor.name,
    })),
  )(items)
  .toArray();

console.log(
  JSON.stringify({
    orchestratorCtxClass: pipeline.contextManager.constructor.name,
    ctxBuiltInOrchestrator: factoryCalls > 0,
    workerCtxClasses: [...new Set(out.map((r) => r.ctxClass))],
    distinctWorkerCtxPids: new Set(out.map((r) => r.builtPid)).size,
    ctxBuiltInSamePidAsServer: out.every((r) => r.pid === r.builtPid),
    multiplierCrossedWire: out.map((r) => r.value),
  }),
);
