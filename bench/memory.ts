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

import cluster from "node:cluster";
import { createHook } from "node:async_hooks";
import { getHeapStatistics, GCProfiler } from "node:v8";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { Readable } from "node:stream";
import { Pipeline, ConcurrentPipeline, HttpPipeline, ClusterPipeline } from "../src";
import {
  canonicalChain,
  canonicalInput,
  handRolledFloor,
  BUFFER_SIZE,
  MAX_CONCURRENCY,
  CLUSTER_WORKERS,
} from "./canonical";
import { withLoopbackServer } from "./utils/loopbackServer";
import { checkMemoryGate, type MemoryReport, type MemorySample } from "./memory-gate";

const MB = 1024 * 1024;
/** Rows per case. The in-process classes run 500,000 - large enough that a collection genuinely
 * happens inside a run, where at 50,000 no case collected at all and the GC axis measured nothing.
 * The two DISPATCHING classes run `bench/canonical.ts`'s own 20,000, for its reason: each of their
 * chunks crosses a real boundary, so a larger N measures the loopback socket rather than this
 * package. Every axis below is per-row or per-run, so the two sizes stay comparable. */
const IN_PROCESS_ROWS = 500_000;
const DISPATCH_ROWS = 20_000;
const RUNS = 5;

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

/** One `v8.GCProfiler` collection event, the shape this file reads off `profiler.stop().statistics`
 * (undocumented in `@types/node`, hence the local shape rather than an import). */
interface GcStatEntry {
  gcType: string;
  cost: number;
  beforeGC: { heapStatistics: { usedHeapSize: number } };
  afterGC: { heapStatistics: { usedHeapSize: number } };
}

/**
 * Runs `run` `RUNS` times and reports the median of each axis. `expected` is asserted EVERY run, so
 * a report is never printed for a run that produced different output.
 *
 * `measureMemory(() => chain(rows).toArray(), identityOf(handRolledFloor(rows)), rows.length)` → one
 * `MemorySample`.
 */
export async function measureMemory(
  run: () => Promise<RunResult>,
  expected: MemoryIdentity,
  rows: number,
): Promise<MemorySample> {
  // Warmed twice, so a first-run compile is never reported as an allocation or time regression.
  await run();
  await run();

  const allocated: number[] = [];
  const retained: number[] = [];
  const held: number[] = [];
  const times: number[] = [];
  const promises: number[] = [];
  const gcCounts: number[] = [];
  const gcCosts: number[] = [];

  for (let i = 0; i < RUNS; i++) {
    await settle();
    const heapBefore = process.memoryUsage().heapUsed;
    const usedBefore = getHeapStatistics().used_heap_size;

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
    const out = await run();
    hook.disable();
    const elapsedNs = Number(process.hrtime.bigint() - started);
    const allocAfter = getHeapStatistics().total_allocated_bytes;
    // Sampled before `profiler.stop()` so a case with zero collections still has an immediate
    // fallback reading - see this function's own `heldAtEndMB` comment below.
    const usedRightAfter = getHeapStatistics().used_heap_size;
    const gc = profiler.stop() as unknown as { statistics?: GcStatEntry[] };

    const identity = identityOf(out);
    if (identity.count !== expected.count || identity.checksum !== expected.checksum) {
      throw new Error(
        `output changed: got {count: ${identity.count}, checksum: ${identity.checksum}}, ` +
          `expected {count: ${expected.count}, checksum: ${expected.checksum}}`,
      );
    }

    const stats = gc.statistics ?? [];
    allocated.push((allocAfter - allocBefore) / MB);
    times.push(elapsedNs / rows);
    promises.push(created / rows);
    gcCounts.push(stats.length);
    gcCosts.push(stats.reduce((sum, stat) => sum + stat.cost, 0) / 1000);
    // `heldAtEndMB`: peak LIVE heap the run reached, read off the collector's own events rather than
    // forced ones (`bench/memory-gate.ts`'s own header explains why this is not `retainedMb`). A
    // case with zero collections never triggered one large enough to sample, so it falls back to an
    // immediate read with no forced GC - the same instrument the ticket originally specified, safe
    // here only because it is a FALLBACK for a small, already-near-baseline case, never the primary
    // reading for a case that actually collects.
    const peakAfterGc = stats.length
      ? Math.max(...stats.map((stat) => stat.afterGC.heapStatistics.usedHeapSize))
      : usedRightAfter;
    held.push((peakAfterGc - usedBefore) / MB);

    await settle();
    retained.push((process.memoryUsage().heapUsed - heapBefore) / MB);
  }

  return {
    promisesPerRow: median(promises),
    gcCount: median(gcCounts),
    gcCostMs: median(gcCosts),
    allocatedMb: median(allocated),
    retainedMb: median(retained),
    heldAtEndMB: median(held),
    nsPerRow: median(times),
  };
}

/** A leg's own proof of correctness (#178): a count plus a running sum, so a STREAMING leg (a
 * `.forEach()` counter) can prove it produced the right rows without retaining them to compare
 * `.length` - retaining them would materialize the very output the leg exists to avoid holding.
 * STRICTLY STRONGER than the check it replaces, not weaker: the pre-#178 identity was `out.length
 * !== expected`, a bare count with no sum at all - a checksum still cannot catch a swap that
 * preserves both count and sum, but that residual gap is smaller than the one it closed, never
 * bigger. Full per-element equality is unavailable BY DEFINITION for a leg that never keeps its
 * elements. */
export interface MemoryIdentity {
  count: number;
  checksum: number;
}

/** What a case's `run()` may hand back: the existing eight legs still return their `.toArray()`
 * result unchanged, and `identityOf` below derives a `MemoryIdentity` from it OUTSIDE the timed and
 * profiled region - a streaming leg returns a `MemoryIdentity` it already computed as it went. */
type RunResult = number[] | MemoryIdentity;

/** `identityOf([6, 8])` → `{ count: 2, checksum: 14 }`. `identityOf({ count: 2, checksum: 14 })` →
 * the same object, unchanged - a leg that already tracked its own identity while streaming never
 * pays for a second pass over data it deliberately never kept. */
function identityOf(result: RunResult): MemoryIdentity {
  if (!Array.isArray(result)) return result;
  let checksum = 0;
  for (const value of result) checksum += value;
  return { count: result.length, checksum };
}

/** One case: its label, the run to measure, the identity its output must match, and the row count
 * its per-row axes divide by. */
type Case = [label: string, run: () => Promise<RunResult>, expected: MemoryIdentity, rows: number];

/** The "another instance" side of the `HttpPipeline` case - an empty-source pipeline holding the
 * SAME stage definitions, so its `.fetch` can serve them. `bench/legs/http.ts`'s own `worker()`. */
function httpWorker(): HttpPipeline<number> {
  return new HttpPipeline<number>({ url: "" }).transform(canonicalChain);
}

/** `.when("evens", ...).when("big", ...).otherwise("rest")` - the three-arm shape "Branch router"
 * and "Branch broadcast" (below) both share, `.broadcast()` the only difference between them
 * (#180's own Done-when 2, 3). No arm `build`, so a matched item passes through unchanged. Built on
 * `ConcurrentPipeline({ maxConcurrency: MAX_CONCURRENCY })`, per the ticket's own Done-when 3 -
 * `.branch()`'s matching and join never dispatch regardless of class (`architecture.md`'s
 * "Branching" section), so this measures the memory cost of classifying+joining on a class that
 * COULD dispatch, not a dispatch itself. */
function branchThreeWay(
  rows: number[],
  broadcast: boolean,
): Promise<{ evens: number[]; big: number[]; rest: number[] }> {
  const half = rows.length / 2;
  const runner = new ConcurrentPipeline<number>({ maxConcurrency: MAX_CONCURRENCY })
    .buffer(BUFFER_SIZE)
    .branch((b) => {
      const withArms = b
        .when("evens", (x: number) => x % 2 === 0)
        .when("big", (x: number) => x > half)
        .otherwise("rest");
      return broadcast ? withArms.broadcast() : withArms;
    });
  return Promise.resolve(runner(rows)) as Promise<{
    evens: number[];
    big: number[];
    rest: number[];
  }>;
}

/** The identity a real `branchThreeWay(rows, true)` run produces, computed independently rather
 * than run twice: `evens`/`big` overlap on a row that is both, and the catch-all takes EVERY row
 * under broadcast (`branch.ts`'s own "under broadcast the catch-all takes every item" rule) - the
 * same reason `comparisonCases` precomputes its own `expected` rather than deriving it from a
 * second, unmeasured run. */
function branchBroadcastExpected(rows: number[]): MemoryIdentity {
  const half = rows.length / 2;
  let count = 0;
  let checksum = 0;
  for (const x of rows) {
    if (x % 2 === 0) {
      count++;
      checksum += x;
    }
    if (x > half) {
      count++;
      checksum += x;
    }
    // "rest" - the catch-all, taking every row under broadcast regardless of the other two arms.
    count++;
    checksum += x;
  }
  return { count, checksum };
}

/** Every case this bench measures, sharing `canonical.ts`'s own chain and input with
 * `bench/overhead.ts` so the two benches never drift apart on what they run. */
/** The two scales `#178`'s own competitive comparison runs at: large enough that the array-chain
 * leg's own materialization genuinely costs it cache pressure and major collections, per the
 * ticket's own finding that the crossover between "this package is slower" and "this package is
 * faster" is real between 1,000,000 and 10,000,000 rows. */
const COMPARISON_SCALES = [1_000_000, 10_000_000] as const;

/**
 * The six legs `#178`'s own memory claim compares against, at ONE scale - `cases()` calls this once
 * per `COMPARISON_SCALES` entry. Every leg shares ONE pre-built input array (the ticket's own
 * Constraint: materialization sits outside every timed region) and ONE identity, derived once from
 * `handRolledFloor`, so a leg that computes something else is a failed run, not a fast one.
 *
 * `comparisonCases(5)[0]` times a real `Pipeline .forEach()` over `[0,1,2,3,4]`, asserting `{count:
 * 2, checksum: 14}` - `0,1,2` double to `0,2,4`, none `> 4`, dropped; `3,4` double to `6,8`, both
 * kept - matching `handRolledFloor([0,1,2,3,4])`'s own `[6, 8]`.
 */
function comparisonCases(scaleRows: number): Case[] {
  const rows = canonicalInput(scaleRows);
  const expected = identityOf(handRolledFloor(rows));
  const scale = scaleRows >= 1_000_000 ? `${scaleRows / 1_000_000}M` : `${scaleRows}`;

  async function* handRolledAsyncGenerator(): AsyncGenerator<number> {
    for (const item of rows) {
      const doubled = item * 2;
      if (doubled > 4) yield doubled;
    }
  }

  return [
    [
      `Pipeline .forEach() @${scale}`,
      async () => {
        let count = 0;
        let checksum = 0;
        await new Pipeline<number>()
          .transform(canonicalChain)(rows)
          .forEach((x: number) => {
            count++;
            checksum += x;
          });
        return { count, checksum };
      },
      expected,
      scaleRows,
    ],
    [
      // Materializes by definition - the ticket's own Constraint requires showing BOTH sides, since
      // `.toArray()` retaining every output row is not this package's failure to stream, it is the
      // terminal the caller chose.
      `Pipeline .toArray() @${scale}`,
      () => Promise.resolve(new Pipeline<number>().transform(canonicalChain)(rows).toArray()),
      expected,
      scaleRows,
    ],
    [
      `Array.prototype .map().filter().forEach() @${scale}`,
      async () => {
        let count = 0;
        let checksum = 0;
        rows
          .map((x) => x * 2)
          .filter((x) => x > 4)
          .forEach((x) => {
            count++;
            checksum += x;
          });
        return { count, checksum };
      },
      expected,
      scaleRows,
    ],
    [
      // The real floor (#120's own planning, reused here for the same reason): no intermediate
      // array, no `Transformer` machinery, the quickest in-process code producing the same rows in
      // the same order. Deliberately NOT `handRolledFloor` - that function materializes an array to
      // serve as this suite's own identity oracle, where this leg's whole point is holding nothing.
      `fused for loop @${scale}`,
      async () => {
        let count = 0;
        let checksum = 0;
        for (const item of rows) {
          const doubled = item * 2;
          if (doubled > 4) {
            count++;
            checksum += doubled;
          }
        }
        return { count, checksum };
      },
      expected,
      scaleRows,
    ],
    [
      `async function* by hand @${scale}`,
      async () => {
        let count = 0;
        let checksum = 0;
        for await (const value of handRolledAsyncGenerator()) {
          count++;
          checksum += value;
        }
        return { count, checksum };
      },
      expected,
      scaleRows,
    ],
    [
      // `Readable.prototype.map`/`.filter`/`.forEach` are all present on Node 26.5.0, no flag -
      // verified before writing this leg. `{ concurrency: 1 }` keeps it a straight sequential drain,
      // the same shape every other leg here runs.
      `node:stream Readable.map().filter() @${scale}`,
      async () => {
        let count = 0;
        let checksum = 0;
        await Readable.from(rows)
          .map((x: number) => x * 2, { concurrency: 1 })
          .filter((x: number) => x > 4, { concurrency: 1 })
          .forEach((x: number) => {
            count++;
            checksum += x;
          });
        return { count, checksum };
      },
      expected,
      scaleRows,
    ],
  ];
}

function cases(): Case[] {
  const rows = canonicalInput(IN_PROCESS_ROWS);
  const kept = identityOf(handRolledFloor(rows));
  const rowsIdentity = identityOf(rows);
  const dispatchRows = canonicalInput(DISPATCH_ROWS);
  const dispatchKept = identityOf(handRolledFloor(dispatchRows));
  const clusterPipeline = new ClusterPipeline<number>({
    workers: CLUSTER_WORKERS,
    maxConcurrency: MAX_CONCURRENCY,
  })
    .buffer(BUFFER_SIZE)
    .transform(canonicalChain);

  async function* asyncRows(): AsyncGenerator<number> {
    for (let i = 0; i < IN_PROCESS_ROWS; i++) yield i;
  }

  return [
    [
      "Pipeline array",
      () => Promise.resolve(new Pipeline<number>().transform(canonicalChain)(rows).toArray()),
      kept,
      IN_PROCESS_ROWS,
    ],
    [
      "Pipeline async source",
      () => new Pipeline<number>().transform(canonicalChain)(asyncRows()).toArray(),
      kept,
      IN_PROCESS_ROWS,
    ],
    [
      "Pipeline .buffer(1000) async",
      () => new Pipeline<number>().buffer(1000).transform(canonicalChain)(asyncRows()).toArray(),
      kept,
      IN_PROCESS_ROWS,
    ],
    [
      "Concurrent array",
      () =>
        new ConcurrentPipeline<number>({ maxConcurrency: MAX_CONCURRENCY })
          .transform(canonicalChain)(rows)
          .toArray(),
      kept,
      IN_PROCESS_ROWS,
    ],
    [
      "Concurrent .local() region",
      () =>
        new ConcurrentPipeline<number>({ maxConcurrency: MAX_CONCURRENCY })
          .buffer(BUFFER_SIZE)
          .local((p) => p.transform(canonicalChain))(rows)
          .toArray(),
      kept,
      IN_PROCESS_ROWS,
    ],
    [
      "Concurrent .forEach()",
      async () => {
        const seen: number[] = [];
        await new ConcurrentPipeline<number>({ maxConcurrency: MAX_CONCURRENCY })
          .buffer(BUFFER_SIZE)
          .transform(canonicalChain)(rows)
          .forEach((x) => {
            seen.push(x);
          });
        return seen;
      },
      kept,
      IN_PROCESS_ROWS,
    ],
    [
      // Router mode (the default): every item goes to exactly ONE of three mutually exclusive,
      // exhaustive arms - no arm `build`, so matched items pass through unchanged and the union of
      // every arm's own output is exactly `rows`, unreordered. `maxConcurrency: 4` on the PARENT
      // class (#180's own Done-when 3) - `.branch()`'s own matching and join never dispatch
      // regardless of class (architecture.md's "Branching" section), so this measures the memory
      // cost of classifying+joining on a class that COULD dispatch, not a dispatch itself.
      "Branch router",
      // Returns the combined arm output as a plain array, same as every other case here - the
      // checksum/count tally happens in `identityOf`, AFTER this run is timed and profiled
      // (code-review finding: an earlier revision tallied inside the timed closure via a separate
      // `sumArms` helper, inflating this case's own ns/row and promises/row past what its sibling
      // cases pay for the identical kind of work).
      async () => {
        const record = await branchThreeWay(rows, false);
        return [...record.evens, ...record.big, ...record.rest];
      },
      // router covers every row exactly once (no arm `build` - matched items pass through
      // unchanged), so the union's own count/checksum is identical to `rows` itself.
      rowsIdentity,
      IN_PROCESS_ROWS,
    ],
    [
      // `.broadcast()`: the SAME three-arm shape, but every matching arm takes the item rather than
      // only the first - `evens` and `big` overlap on every row that is both, and the catch-all
      // takes EVERY row under broadcast (branch.ts's own "under broadcast the catch-all takes every
      // item" rule). `branchBroadcastExpected` computes the identical sum a real broadcast run
      // produces, the same way `comparisonCases` precomputes its own `expected`.
      "Branch broadcast",
      async () => {
        const record = await branchThreeWay(rows, true);
        return [...record.evens, ...record.big, ...record.rest];
      },
      branchBroadcastExpected(rows),
      IN_PROCESS_ROWS,
    ],
    [
      // The server is stood up INSIDE the measured run, deliberately: a dispatching class's own cost
      // includes what its wire makes the process allocate, and hoisting the server out would measure
      // a chain whose counterparty nobody pays for. It is the same shape every run, so the constant
      // it adds cancels in a before/after comparison.
      "Http loopback",
      () =>
        withLoopbackServer(httpWorker().fetch, (url) =>
          new HttpPipeline<number>({ url, maxConcurrency: MAX_CONCURRENCY })
            .buffer(BUFFER_SIZE)
            .transform(canonicalChain)(dispatchRows)
            .toArray(),
        ),
      dispatchKept,
      DISPATCH_ROWS,
    ],
    [
      // ⚠ Constructed ONCE, outside the run, and that is load-bearing rather than tidy. A
      // `ClusterPipeline`'s route carries the index of the pipeline DEFINITION it belongs to, and a
      // forked worker re-executes this file to rebuild the same definitions in the same order. Built
      // inside the run, the primary constructed one per measured round and asked for
      // `/pipeline/5/transform/0` from a worker that had only built `/pipeline/0/...`:
      // `stage 0 at http://localhost:64829 failed: unknown pipeline route /pipeline/5/transform/0`.
      // `bench/overhead.ts` hoists its own for the same reason.
      "Cluster workers",
      () => clusterPipeline(dispatchRows).toArray(),
      dispatchKept,
      DISPATCH_ROWS,
    ],
    ...COMPARISON_SCALES.flatMap((scaleRows) => comparisonCases(scaleRows)),
  ];
}

/** Widest label this file's own `cases()` produces, computed rather than hand-counted - the longest
 * comparison-leg label (#178) is 46 characters, well past the eight original legs' own widest
 * ("Concurrent .local() region", 27), and a fixed `padEnd(30)` misaligns every row past it. */
const LABEL_WIDTH = 46;

const HEADER =
  "case".padEnd(LABEL_WIDTH + 2) +
  "promises/row".padStart(13) +
  "GC".padStart(5) +
  "GC ms".padStart(9) +
  "alloc MB".padStart(11) +
  "held MB".padStart(9) +
  "leak MB".padStart(9) +
  "ns/row".padStart(10);

function formatRow(label: string, s: MemorySample): string {
  return (
    label.padEnd(LABEL_WIDTH + 2) +
    s.promisesPerRow.toFixed(3).padStart(13) +
    String(s.gcCount).padStart(5) +
    s.gcCostMs.toFixed(1).padStart(9) +
    s.allocatedMb.toFixed(1).padStart(11) +
    s.heldAtEndMB.toFixed(1).padStart(9) +
    s.retainedMb.toFixed(2).padStart(9) +
    s.nsPerRow.toFixed(1).padStart(10)
  );
}

/** Every case, measured. Exported so `bench/compare.ts` can run the suite inside a checked-out ref
 * without re-declaring what the suite IS. */
export async function runMemorySuite(only?: string): Promise<MemoryReport> {
  const all = cases();
  const selected = only ? all.filter(([label]) => label === only) : all;
  if (selected.length === 0) {
    throw new Error(`unknown case ${only}; known: ${all.map(([label]) => label).join(" | ")}`);
  }

  const report: MemoryReport = {};
  for (const [label, run, expected, rows] of selected) {
    report[label] = await measureMemory(run, expected, rows);
  }
  return report;
}

export const MEMORY_BASELINE_PATH = new URL("./memory-baseline.json", import.meta.url);

function printReport(report: MemoryReport): void {
  console.log(HEADER);
  for (const [label, sample] of Object.entries(report)) console.log(formatRow(label, sample));
}

/**
 * `pnpm bench:memory` runs every case and gates it against `bench/memory-baseline.json`, exiting
 * non-zero on a regression. `--record` rewrites that baseline instead. `--json` prints the report as
 * JSON and gates nothing, which is what `bench/compare.ts` reads. `--case "<name>"` narrows to one
 * case, which is the supported way to compare two commits: a `heapUsed` reading moves with whatever
 * ran before it in the same process.
 */
async function main(): Promise<void> {
  // A forked `ClusterPipeline` worker re-executes THIS file (`cluster.fork()` re-execs
  // `process.argv[1]`), so the measure-and-print path is gated on being the primary - exactly as
  // `bench/overhead.ts` gates its own. The worker still constructs the same pipeline, because its
  // stage registry has to line up with what the primary dispatches; it measures nothing.
  if (!cluster.isPrimary) {
    // `cases()` itself is what builds the `ClusterPipeline` definition, so calling it is the whole
    // job here: the worker needs the same definitions in the same order as the primary, and must
    // measure nothing.
    cases();
    return;
  }

  const argv = process.argv.slice(2);
  const only = argv.includes("--case") ? argv[argv.indexOf("--case") + 1] : undefined;
  const report = await runMemorySuite(only);

  if (argv.includes("--json")) {
    console.log(JSON.stringify(report, null, 2));
    return;
  }

  printReport(report);

  if (argv.includes("--record")) {
    writeFileSync(MEMORY_BASELINE_PATH, `${JSON.stringify(report, null, 2)}\n`);
    console.log(`\nrecorded ${Object.keys(report).length} cases to bench/memory-baseline.json`);
    return;
  }

  if (!existsSync(MEMORY_BASELINE_PATH)) {
    console.log(
      "\nno bench/memory-baseline.json yet - run `pnpm bench:memory:record` to create it",
    );
    return;
  }

  const baseline = JSON.parse(readFileSync(MEMORY_BASELINE_PATH, "utf8")) as MemoryReport;
  // Gated against the cases actually measured, so `--case` narrows the gate with it rather than
  // failing every case it did not run.
  const scoped: Partial<MemoryReport> = only ? { [only]: baseline[only] } : baseline;
  const result = checkMemoryGate(report, scoped);

  if (result.ok) {
    console.log("\nno regression against bench/memory-baseline.json");
    return;
  }
  console.error(`\n${result.violations.length} regression(s) against bench/memory-baseline.json:`);
  for (const violation of result.violations) console.error(`  - ${violation}`);
  process.exitCode = 1;
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
