/**
 * #31 Done-when 6 — `contextFactory` is invoked ONCE per process (counted as invocations, not
 * distinct served instances). The design this replaces let `.fetch()` build its own manager
 * alongside the constructor's, doubling the count per worker with one instance never used to
 * serve - `factoryCalls` here is a per-process counter, read live at serve time, so a rebuild
 * anywhere in that process would show up as a HIGHER max than 1 for that worker's pid.
 */
import { ClusterPipeline } from "../../src";
import { SimpleContextManager } from "../../src";

let factoryCalls = 0;

/** No real difference of its own from `SimpleContextManager` (#133) - kept as a distinct class
 * purely so `contextFactory`'s own `factoryCalls++` closure counts invocations that build one. */
class CountingContext extends SimpleContextManager {}

const workers = 2;
const chunkCount = 20;
const items = Array.from({ length: chunkCount }, (_, i) => i);

const pipeline = new ClusterPipeline<number>({
  workers,
  maxConcurrency: workers,
  contextFactory: () => {
    factoryCalls++;
    return new CountingContext();
  },
});

// Read right after construction, before any dispatch - the orchestrator's OWN process count,
// never touched again by a later copy-on-write call (`.buffer()`/`.transform()` always pass the
// already-built `context` forward, `src/pipeline.ts`'s own constructor).
const primaryBuilt = factoryCalls;

const out = await pipeline
  .buffer(1)
  .transform((t) =>
    t.map((_x: number, _ctx) => ({ pid: process.pid, factoryCallsSoFar: factoryCalls })),
  )(items)
  .toArray();

const maxCallsByPid = new Map<number, number>();
for (const { pid, factoryCallsSoFar } of out) {
  maxCallsByPid.set(pid, Math.max(maxCallsByPid.get(pid) ?? 0, factoryCallsSoFar));
}

console.log(
  JSON.stringify({
    primaryBuilt,
    chunks: chunkCount,
    maxBuiltPerWorkerPid: [...maxCallsByPid.values()],
    workerPids: maxCallsByPid.size,
  }),
);
