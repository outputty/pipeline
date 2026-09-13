/**
 * `pnpm bench:compare <ref>` - the same memory suite measured against another commit's `src/` and
 * against the working tree, printed side by side (#179). This is the command a build runs to satisfy
 * `~/.claude/rules/code.md`'s "measure before the first edit and again before the docs layer": one
 * invocation, both numbers, no stash-and-restore dance to get the BEFORE reading back.
 *
 * ```text
 * pnpm bench:compare d6cc951
 * ```
 *
 * How it gets the other commit's numbers: `git checkout <ref> -- src/`, run the suite in a fresh
 * process, then `git checkout HEAD -- src/`. The suite itself is never checked out - it runs from the
 * WORKING TREE at both ends, so the two readings come from one identical harness measuring two
 * different implementations, which is the only way the numbers mean the same thing.
 *
 * ⚠ It refuses to run against a dirty `src/`, because restoring afterwards would discard real work.
 * Commit or stash first; the message says so.
 *
 * ONE case per process, always. A `heapUsed` reading moves with whatever ran before it, so a suite
 * run in a single process reports numbers that depend on case order - measured, the same case read
 * 13.9 MB as a table row and 19.8 MB alone.
 */

import { execFileSync } from "node:child_process";
import { checkMemoryGate, type MemoryReport, type MemorySample } from "./memory-gate";

const CASES = [
  "Pipeline array",
  "Pipeline async source",
  "Pipeline .buffer(1000) async",
  "Concurrent array",
  "Concurrent .local() region",
  "Concurrent .forEach()",
] as const;

function git(...args: string[]): string {
  return execFileSync("git", args, { encoding: "utf8" }).trim();
}

/** One case, in its own process, as JSON. Its own process is the point - see this file's header. */
function measureCase(label: string): MemorySample {
  const out = execFileSync(
    process.execPath,
    ["--expose-gc", "--import", "tsx", "bench/memory.ts", "--case", label, "--json"],
    { encoding: "utf8", maxBuffer: 32 * 1024 * 1024 },
  );
  const report = JSON.parse(out) as MemoryReport;
  const sample = report[label];
  if (!sample) throw new Error(`case ${label} produced no sample`);
  return sample;
}

function runSuite(): MemoryReport {
  const report: MemoryReport = {};
  for (const label of CASES) {
    process.stderr.write(`  measuring ${label}\n`);
    report[label] = measureCase(label);
  }
  return report;
}

/** `+12.3%`, or `-41.0%` where the number fell. `baseValue` of 0 has no ratio, so it reports the
 * absolute move instead of dividing by zero. */
function delta(current: number, base: number): string {
  if (base === 0) return current === 0 ? "   same" : `  +${current.toFixed(3)}`;
  const pct = ((current - base) / base) * 100;
  const sign = pct > 0 ? "+" : "";
  return `${sign}${pct.toFixed(1)}%`.padStart(7);
}

function printAxis(
  label: string,
  axis: keyof MemorySample,
  before: MemoryReport,
  after: MemoryReport,
): void {
  console.log(`\n${axis}`);
  for (const name of CASES) {
    const b = before[name];
    const a = after[name];
    if (!b || !a) continue;
    console.log(
      `  ${name.padEnd(30)}${b[axis].toFixed(3).padStart(11)} → ${a[axis].toFixed(3).padStart(11)}   ${delta(a[axis], b[axis])}`,
    );
  }
}

function main(): void {
  const ref = process.argv[2];
  if (!ref) {
    throw new Error("usage: pnpm bench:compare <git-ref>");
  }

  const dirty = git("status", "--porcelain", "--", "src");
  if (dirty !== "") {
    throw new Error(
      `src/ has uncommitted changes, and this command restores src/ from git when it finishes - ` +
        `commit or stash first, or the restore would discard them:\n${dirty}`,
    );
  }

  process.stderr.write(`measuring the working tree\n`);
  const after = runSuite();

  process.stderr.write(`measuring ${ref}\n`);
  git("checkout", ref, "--", "src");
  let before: MemoryReport;
  try {
    before = runSuite();
  } finally {
    // Always restored, including when a case throws - leaving another commit's `src/` in the working
    // tree is a far worse failure than the one that caused it.
    git("checkout", "HEAD", "--", "src");
  }

  console.log(`\n${ref} → working tree\n`);
  printAxis("", "allocatedMb", before, after);
  printAxis("", "promisesPerRow", before, after);
  printAxis("", "gcCount", before, after);
  printAxis("", "gcCostMs", before, after);
  printAxis("", "retainedMb", before, after);
  printAxis("", "nsPerRow", before, after);

  // The same gate `pnpm bench:memory` applies, with `<ref>` standing in for the committed baseline -
  // so "did this branch regress anything" is answered by the tool rather than by reading the table.
  const result = checkMemoryGate(after, before);
  if (result.ok) {
    console.log(`\nno regression against ${ref}`);
    return;
  }
  console.error(`\n${result.violations.length} regression(s) against ${ref}:`);
  for (const violation of result.violations) console.error(`  - ${violation}`);
  process.exitCode = 1;
}

main();
