/**
 * `pnpm bench:overhead` (#120) - the CLI: measures all four `Pipeline` runner classes against their
 * hand-rolled floors (`bench/canonical.ts`), prints the report, and gates it against the committed
 * `bench/baseline.json` (20% absolute tolerance, 10% ratio tolerance, regression-only -
 * `bench/gate.ts`'s own docstring). Exits 1 on a gate failure, 0 otherwise.
 *
 * `BENCH_ROUNDS=<n>` overrides the default round count (`bench/canonical.ts`'s `ROUNDS`) - useful
 * for a fast CI-style smoke run; the gate's own correctness does not depend on how many rounds fed
 * it, though fewer rounds is a noisier measurement, closer to (or past) the committed tolerance by
 * chance alone. `BENCH_SYNTHETIC_REGRESSION=1` widens the `Pipeline` leg's own reported cost 100x
 * before the gate runs (#120 Done-when 4) - a test-only escape hatch proving the gate actually gates
 * a real CLI run, never a knob a real invocation sets; the multiplier dominates any real machine
 * noise, so the resulting failure is deterministic regardless of round count. `BENCH_SKIP_GATE=1`
 * skips the baseline comparison outright (report and exit 0 unconditionally) - test-only, for
 * proving the harness runs end to end without depending on the CURRENT machine's own noise staying
 * inside tolerance of a baseline committed on a different one.
 *
 * A forked `ClusterPipeline` worker re-executes this file (`cluster.fork()` re-execs
 * `process.argv[1]`) - `cluster.isPrimary` gates the "measure everything and print" path so a worker
 * only reaches `measureClusterPipeline()`'s own construction calls (which its registry needs, to
 * serve the primary's dispatched stage), never the other three legs' unrelated, expensive
 * measurements.
 */
import cluster from "node:cluster";
import { existsSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { measurePipeline } from "./legs/pipeline";
import { measureConcurrentPipeline } from "./legs/concurrent";
import { measureHttpPipeline } from "./legs/http";
import { measureClusterPipeline } from "./legs/cluster";
import { checkGate, type OverheadReport } from "./gate";

const BASELINE_PATH = fileURLToPath(new URL("./baseline.json", import.meta.url));

function rounds(): number | undefined {
  const raw = process.env.BENCH_ROUNDS;
  return raw ? Number(raw) : undefined;
}

async function main(): Promise<void> {
  if (!cluster.isPrimary) {
    await measureClusterPipeline(rounds());
    return;
  }

  const report: OverheadReport = {
    Pipeline: await measurePipeline(rounds()),
    ConcurrentPipeline: await measureConcurrentPipeline(rounds()),
    HttpPipeline: await measureHttpPipeline(rounds()),
    ClusterPipeline: await measureClusterPipeline(rounds()),
  };

  if (process.env.BENCH_SYNTHETIC_REGRESSION === "1") {
    report.Pipeline = {
      ...report.Pipeline,
      pipelineNsPerRow: report.Pipeline.pipelineNsPerRow * 100,
    };
  }

  // Single-line, not pretty-printed: `runFixtureJson`'s own convention (every cluster-backed
  // fixture in this repo follows it) reads the LAST stdout line as the real result, and a
  // pretty-printed object's own last line is just a closing brace.
  console.log(JSON.stringify(report));

  if (process.env.BENCH_SKIP_GATE === "1") return;

  if (!existsSync(BASELINE_PATH)) {
    console.error(
      "No bench/baseline.json yet - printed only, not gated. Commit this run's own numbers as the baseline.",
    );
    return;
  }

  const baseline = JSON.parse(readFileSync(BASELINE_PATH, "utf-8")) as OverheadReport;
  const gate = checkGate(report, baseline);
  if (!gate.ok) {
    console.error("bench:overhead GATE FAILED:");
    for (const violation of gate.violations) console.error(`  - ${violation}`);
    process.exitCode = 1;
  }
}

await main();
