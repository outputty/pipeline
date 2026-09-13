/**
 * The memory and scheduling cost of one run, on the instruments that actually measure it (#179).
 * `bench/overhead.ts` answers "how fast"; this answers "at what allocation, how many collections,
 * and how many microtasks" - the three numbers every build owes before its first edit and again
 * before its docs layer.
 *
 * `pnpm bench:memory` prints every case; `pnpm bench:memory "<case>"` prints one. ONE case per
 * process is the supported way to compare two commits: a `heapUsed` reading moves with whatever ran
 * before it, and even the counters below share a warm heap.
 *
 * ## Why these instruments and not the obvious ones
 *
 * Each was chosen by measuring the candidates against one another on a real chain, not by
 * reputation. The rejected ones are recorded here because both read as correct until compared.
 *
 * - ALLOCATION is `v8.getHeapStatistics().total_allocated_bytes`, a cumulative counter, so it cannot
 *   be moved by when the collector happens to run. Measured within 0.1% across five runs.
 *   ⚠ REJECTED: a `process.memoryUsage().heapUsed` delta. It measures what the collector had not yet
 *   reached at the sample point, so a change that allocates LESS but collects less often reads as a
 *   regression. Real: one chain read 13.9 MB before a change and 37.3 after by that instrument,
 *   while `total_allocated_bytes` showed allocation had HALVED, 973 MB to 454 - the same change cut
 *   the collection count from 30 to 13, so more of a smaller total was still uncollected when
 *   sampled. That false alarm cost a full round to chase.
 *
 * - GC is `v8.GCProfiler`, which reports every event with its `gcType` and its `cost` in
 *   MICROseconds. ⚠ REJECTED: `PerformanceObserver` on `gc`. It reported ZERO events for a run
 *   `GCProfiler` counts 13 of and `--trace-gc` confirms - its entries arrive asynchronously, so a
 *   window that disconnects when the work finishes drops them.
 *
 * - RETENTION is `heapUsed` after two forced collections, which is what `heapUsed` is good for: only
 *   a leak moves it. Needs `--expose-gc`, which the `bench:memory` script passes.
 *
 * - PROMISES is `node:async_hooks`' `PROMISE` resource type, unchanged. It was never the unreliable
 *   one: a fully synchronous chain reads exactly 0, and a wall-clock or microtask-queue proxy reads
 *   0 for real async work too, passing vacuously.
 */

import { createHook } from "node:async_hooks";
import { getHeapStatistics, GCProfiler } from "node:v8";
import { Pipeline } from "../src";
import { ConcurrentPipeline } from "../src";
import { canonicalChain, canonicalInput, handRolledFloor } from "./canonical";

const MB = 1024 * 1024;
/** Large enough that a collection genuinely happens inside a run - at 50,000 rows no case collected
 * at all, so the GC axis measured nothing. */
const ROWS = 500_000;
const RUNS = 5;

/** What one case costs, each field on its own instrument. */
export interface MemorySample {
  promisesPerRow: number;
  gcCount: number;
  gcCostMs: number;
  allocatedMb: number;
  retainedMb: number;
  nsPerRow: number;
}

function forceGc(): void {
  const gc = (globalThis as { gc?: () => void }).gc;
  if (gc === undefined) {
    throw new Error("bench:memory needs --expose-gc; run it through `pnpm bench:memory`");
  }
  gc();
}

/** Two forced collections a tick apart, so a retention reading is the SETTLED one - a single `gc()`
 * leaves the previous run's garbage partly uncollected, which then reads as this run's. */
async function settle(): Promise<void> {
  forceGc();
  await new Promise((resolve) => setTimeout(resolve, 60));
  forceGc();
  await new Promise((resolve) => setTimeout(resolve, 60));
}

const median = (xs: number[]): number => [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)];

/**
 * Runs `run` `RUNS` times and reports the median of each axis. `expected` is asserted EVERY run, so
 * a number is never reported for a run that produced different output.
 *
 * `measureMemory(() => chain(rows).toArray(), 499_997)` → one `MemorySample`.
 */
export async function measureMemory(
  run: () => Promise<unknown>,
  expected: number,
): Promise<MemorySample> {
  // Warmed twice, so a first-run compile is never reported as an allocation or time regression.
  await run();
  await run();

  const allocated: number[] = [];
  const retained: number[] = [];
  const times: number[] = [];
  const promises: number[] = [];
  const gcCounts: number[] = [];
  const gcCosts: number[] = [];

  for (let i = 0; i < RUNS; i++) {
    await settle();
    const heapBefore = process.memoryUsage().heapUsed;

    const profiler = new GCProfiler();
    profiler.start();
    const allocBefore = getHeapStatistics().total_allocated_bytes;

    let created = 0;
    const hook = createHook({
      init(_id, type) {
        if (type === "PROMISE") created++;
      },
    });

    const started = process.hrtime.bigint();
    hook.enable();
    const out = (await run()) as unknown[];
    hook.disable();
    const elapsedNs = Number(process.hrtime.bigint() - started);
    const allocAfter = getHeapStatistics().total_allocated_bytes;
    const gc = profiler.stop() as unknown as { statistics?: { gcType: string; cost: number }[] };

    if (out.length !== expected) {
      throw new Error(`output changed: got ${out.length} rows, expected ${expected}`);
    }

    const stats = gc.statistics ?? [];
    allocated.push((allocAfter - allocBefore) / MB);
    times.push(elapsedNs / ROWS);
    promises.push(created / ROWS);
    gcCounts.push(stats.length);
    gcCosts.push(stats.reduce((sum, stat) => sum + stat.cost, 0) / 1000);

    await settle();
    retained.push((process.memoryUsage().heapUsed - heapBefore) / MB);
  }

  return {
    promisesPerRow: median(promises),
    gcCount: median(gcCounts),
    gcCostMs: median(gcCosts),
    allocatedMb: median(allocated),
    retainedMb: median(retained),
    nsPerRow: median(times),
  };
}

/** Every case this bench measures, sharing `canonical.ts`'s own chain and input with
 * `bench/overhead.ts` so the two benches never drift apart on what they run. */
function cases(): [string, () => Promise<unknown>, number][] {
  const rows = canonicalInput(ROWS);
  const kept = handRolledFloor(rows).length;

  async function* asyncRows(): AsyncGenerator<number> {
    for (let i = 0; i < ROWS; i++) yield i;
  }

  return [
    [
      "Pipeline array",
      () => Promise.resolve(new Pipeline<number>().transform(canonicalChain)(rows).toArray()),
      kept,
    ],
    [
      "Pipeline async source",
      () => new Pipeline<number>().transform(canonicalChain)(asyncRows()).toArray(),
      kept,
    ],
    [
      "Pipeline .buffer(1000) async",
      () => new Pipeline<number>().buffer(1000).transform(canonicalChain)(asyncRows()).toArray(),
      kept,
    ],
    [
      "Concurrent array",
      () =>
        new ConcurrentPipeline<number>({ maxConcurrency: 4 })
          .transform(canonicalChain)(rows)
          .toArray(),
      kept,
    ],
    [
      "Concurrent .local() region",
      () =>
        new ConcurrentPipeline<number>({ maxConcurrency: 4 })
          .buffer(1000)
          .local((p) => p.transform(canonicalChain))(rows)
          .toArray(),
      kept,
    ],
    [
      "Concurrent .forEach()",
      async () => {
        const seen: number[] = [];
        await new ConcurrentPipeline<number>({ maxConcurrency: 4 })
          .buffer(1000)
          .transform(canonicalChain)(rows)
          .forEach((x) => {
            seen.push(x);
          });
        return seen;
      },
      kept,
    ],
  ];
}

const HEADER =
  "case                           promises/row   GC   GC ms   alloc MB  held MB    ns/row";

function printRow(label: string, s: MemorySample): void {
  console.log(
    label.padEnd(30) +
      s.promisesPerRow.toFixed(3).padStart(13) +
      String(s.gcCount).padStart(5) +
      s.gcCostMs.toFixed(1).padStart(9) +
      s.allocatedMb.toFixed(1).padStart(11) +
      s.retainedMb.toFixed(2).padStart(9) +
      s.nsPerRow.toFixed(1).padStart(10),
  );
}

async function main(): Promise<void> {
  const all = cases();
  const wanted = process.argv[2];
  const selected = wanted ? all.filter(([label]) => label === wanted) : all;
  if (selected.length === 0) {
    throw new Error(`unknown case ${wanted}; known: ${all.map(([label]) => label).join(" | ")}`);
  }

  console.log(HEADER);
  for (const [label, run, expected] of selected) {
    printRow(label, await measureMemory(run, expected));
  }
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
