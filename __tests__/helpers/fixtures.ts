/**
 * Shared e2e fixture helpers - a real loopback HTTP server (`withServer`) and a real subprocess
 * runner (`runFixture`/`expectFixtureOk`/`lastJsonLine`) for a `node:cluster`/`execFile` fixture
 * script. Extracted from `pipelines.e2e.test.ts` (#17) so `reduce.e2e.test.ts` (#45) reuses the same
 * seam instead of a second copy.
 */
import { execFile } from "node:child_process";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { expect } from "vitest";
import { toNodeHandler } from "../../src";

/** A subprocess fixture (`node:cluster`, `execFile`) pays real process/fork startup cost. */
export const FIXTURE_TIMEOUT = 20000;
/** An in-process, loopback-only HTTP case - no fork, no subprocess - fails fast on a real hang. */
export const HTTP_TIMEOUT = 5000;

export interface FixtureResult {
  stdout: string;
  stderr: string;
  code: number;
  /** Set when `execFile`'s own timeout killed the process - a killed process reports `err.code`
   * as `null` (Node has no exit code for it), not a real assertion failure's code. Surfaced here
   * instead of silently folding into a generic non-zero `code`. */
  timedOut: boolean;
}

/** Spawns a fixture script under `tsx` (needed for `src`'s extensionless imports and the `@src/*`
 * alias - Node's own ESM resolver has neither) and collects its stdout/stderr/exit code. Every
 * assertion a fixture's OWN Done-when cases need reads one shared run - never re-spawn the same
 * script per assertion.
 *
 * `nodeFlags` go ahead of `--import tsx`, for a fixture whose whole point is a process-level flag -
 * `no-codegen.ts` runs under `--disallow-code-generation-from-strings` (#90). */
export function runFixture(relativePath: string, nodeFlags: string[] = []): Promise<FixtureResult> {
  return new Promise((resolve) => {
    execFile(
      process.execPath,
      [...nodeFlags, "--import", "tsx", relativePath],
      { timeout: FIXTURE_TIMEOUT },
      (err, stdout, stderr) => {
        const errno = err as
          (NodeJS.ErrnoException & { code?: number; signal?: string; killed?: boolean }) | null;
        const timedOut = errno?.killed === true && errno?.signal != null;
        const code = errno ? (typeof errno.code === "number" ? errno.code : 1) : 0;
        resolve({ stdout, stderr, code, timedOut });
      },
    );
  });
}

/** Binds `handler` to a real loopback HTTP server, runs `use` against its `http://localhost:<port>`
 * url, and always closes the server after - the one seam every HTTP-backed case goes through, so a
 * leaked listening socket on a failed assertion can't happen. */
export async function withServer<T>(
  handler: (request: Request) => Promise<Response>,
  use: (url: string) => Promise<T>,
): Promise<T> {
  const server = createServer(toNodeHandler(handler));
  await new Promise<void>((resolve) => server.listen(0, resolve));
  const port = (server.address() as AddressInfo).port;
  try {
    return await use(`http://localhost:${port}`);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

/** Asserts a fixture exited 0 - and, when it didn't, says whether that's because
 * `FIXTURE_TIMEOUT`/`HTTP_TIMEOUT` killed it (a hang) rather than a real non-zero exit. */
export function expectFixtureOk(result: FixtureResult): void {
  expect(result.timedOut, `fixture timed out; stderr:\n${result.stderr}`).toBe(false);
  expect(result.code, `fixture exited ${result.code}; stderr:\n${result.stderr}`).toBe(0);
}

/** Parses a fixture's REAL result off its stdout's LAST line - a `ClusterPipeline` fixture's own
 * worker re-executes the entry module and prints its own empty placeholder first
 * (architecture.md's own documented constraint), so only the final line is the primary's own. */
export function lastJsonLine<T>(fixture: FixtureResult): T {
  const lines = fixture.stdout.trim().split("\n");
  return JSON.parse(lines.at(-1) ?? "") as T;
}

/** Runs a fixture, asserts it exited cleanly, and parses its real result - the
 * `runFixture` → `expectFixtureOk` → `lastJsonLine` triplet nearly every fixture-backed case
 * repeated (#133: was spelled inline 11+ times). A case that also needs the raw `FixtureResult`
 * (its `stderr`, say) still calls the three separately; this is for the ordinary case that only
 * wants the parsed JSON.
 *
 * `await runFixtureJson<{ distinctPids: number }>("__tests__/fixtures/cluster-pids.ts")` →
 * `{ distinctPids: 3 }`, having already asserted the fixture exited 0. */
export async function runFixtureJson<T>(
  relativePath: string,
  nodeFlags: string[] = [],
): Promise<T> {
  const fixture = await runFixture(relativePath, nodeFlags);
  expectFixtureOk(fixture);
  return lastJsonLine<T>(fixture);
}
