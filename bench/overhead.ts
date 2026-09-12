/**
 * `pnpm bench:overhead` (#120) - the CLI: measures all four `Pipeline` runner classes against their
 * hand-rolled floors (`bench/canonical.ts`), prints the report, and gates it against the committed
 * `bench/baseline.json` (20% absolute tolerance, regression-only, on `pipelineNsPerRow` or
 * `local.nsPerRow` depending on the leg - `bench/gate.ts`'s own docstring). Exits 1 on a gate
 * failure, 0 otherwise.
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
 * inside tolerance of a baseline committed on a different one. A MISSING `bench/baseline.json`
 * (never expected once this ticket's own bootstrap run is committed) exits 1 too, same as a real
 * gate failure - a gate that cannot run is not a passing one; regenerate it deliberately with
 * `BENCH_SKIP_GATE=1 pnpm bench:overhead > bench/baseline.json` (then re-format the file) rather
 * than relying on this path's own silence.
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
import { checkGate, legReport, type OverheadReport } from "./gate";
import { timeFloor } from "./canonical";

const BASELINE_PATH = fileURLToPath(new URL("./baseline.json", import.meta.url));

/** Parses `BENCH_ROUNDS`, raising on a non-finite value rather than letting it become `NaN` or
 * `Infinity` - unguarded, `timeRounds`'s own `rounds < 2` check is false for BOTH (every `NaN`
 * comparison is false, and `Infinity` is never less than 2), so a non-numeric value would silently
 * run the round loop zero times (surfacing as `median() of an empty array has no defined value`,
 * naming neither `BENCH_ROUNDS` nor the bad value) while `Infinity` itself would hang the loop
 * forever instead of failing at all - caught by `/code-review`, `BENCH_ROUNDS=Infinity` reaching
 * `Number("Infinity")` unrejected by a bare `Number.isNaN` check. */
function rounds(): number | undefined {
  const raw = process.env.BENCH_ROUNDS;
  if (!raw) return undefined;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) {
    throw new Error(`BENCH_ROUNDS must be a finite number, got ${JSON.stringify(raw)}`);
  }
  return parsed;
}

async function main(): Promise<void> {
  if (!cluster.isPrimary) {
    // A placeholder floor (0), never the real handRolledFloor timing: this call's whole return
    // value is discarded below, kept only for its construction side effect (registry alignment,
    // this file's own header) - re-timing an unrelated 1,000,000-row loop for a result nobody
    // reads would only waste CPU on every forked worker (code-review finding).
    await measureClusterPipeline(0, rounds());
    return;
  }

  // Measured ONCE and shared by every leg: handRolledFloor is class-independent
  // (bench/canonical.ts's own timeFloor docstring), so timing it separately per leg was 4 redundant
  // 5-round, 1,000,000-row measurements of the identical conceptual number (code-review finding).
  const floorNsPerRow = await timeFloor(rounds());
  const report: OverheadReport = {
    Pipeline: await measurePipeline(floorNsPerRow, rounds()),
    ConcurrentPipeline: await measureConcurrentPipeline(floorNsPerRow, rounds()),
    HttpPipeline: await measureHttpPipeline(floorNsPerRow, rounds()),
    ClusterPipeline: await measureClusterPipeline(floorNsPerRow, rounds()),
  };

  if (process.env.BENCH_SYNTHETIC_REGRESSION === "1") {
    // legReport() recomputes ratio (gate.ts's own docstring: ALWAYS pipelineNsPerRow /
    // floorNsPerRow) so the printed report stays internally consistent under the synthetic
    // multiplier instead of showing a ratio that no longer matches its own two inputs.
    report.Pipeline = legReport(
      report.Pipeline.pipelineNsPerRow * 100,
      report.Pipeline.floorNsPerRow,
    );
  }

  // Single-line, not pretty-printed: `runFixtureJson`'s own convention (every cluster-backed
  // fixture in this repo follows it) reads the LAST stdout line as the real result, and a
  // pretty-printed object's own last line is just a closing brace.
  console.log(JSON.stringify(report));

  if (process.env.BENCH_SKIP_GATE === "1") return;

  if (!existsSync(BASELINE_PATH)) {
    console.error(
      "bench:overhead GATE FAILED: bench/baseline.json is missing - nothing to gate against. " +
        "Regenerate it deliberately (see this file's own header) rather than treating this as a pass.",
    );
    process.exitCode = 1;
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
