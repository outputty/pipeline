/**
 * #17 Done-when 8, new-behavior half — a chunk failure at `maxConcurrency: 4` must leave
 * `UNHANDLED []` on `ConcurrentPipeline`, at both `ordered: true` and `ordered: false`. The
 * shipped-`concurrent()` control lives in its own script
 * (`concurrent-unhandled-control.ts`) so that live assertion never depends on this file's build
 * state.
 *
 * Runs as its own process (`process.on("unhandledRejection", …)`) because Vitest installs its own
 * handler and would report a leak as a test-runner error, not a value this script can observe.
 */
import { ConcurrentPipeline } from "../../src";

const unhandled: string[] = [];
process.on("unhandledRejection", (reason) => {
  unhandled.push(String((reason as Error)?.message ?? reason));
});

async function settle(): Promise<void> {
  await new Promise((r) => setImmediate(r));
  await new Promise((r) => setTimeout(r, 20));
}

function failFrom2(x: number): number {
  if (x >= 2) throw new Error(`chunk-${x}-failed`);
  return x;
}

/** Only a `chunk-N-failed` rejection is the expected shape; anything else (e.g. a stub's "not
 * implemented" throw) is a real fault this fixture must surface, never swallow. */
function expectChunkFailure(error: unknown): void {
  if (!(error instanceof Error) || !/^chunk-\d+-failed$/.test(error.message)) {
    throw error;
  }
}

async function runConcurrentPipeline(ordered: boolean): Promise<string[]> {
  const cp = new ConcurrentPipeline<number>([1, 2, 3, 4, 5], { maxConcurrency: 4, ordered });
  try {
    await cp.transform((t) => t.map(failFrom2)).toArray();
  } catch (error) {
    expectChunkFailure(error);
  }
  await settle();
  const seen = [...unhandled];
  unhandled.length = 0;
  return seen;
}

async function main(): Promise<void> {
  const ordered = await runConcurrentPipeline(true);
  const unorderedResult = await runConcurrentPipeline(false);
  console.log(JSON.stringify({ ordered, unordered: unorderedResult }));
}

main().catch((error: unknown) => {
  // A real fault (not the expected chunk failure) - print it plainly instead of leaving stdout
  // empty, and exit non-zero so the calling test's own exit-code check catches it FIRST, before
  // JSON.parse on a "FATAL ..." line obscures the real message with a generic SyntaxError.
  console.log(`FATAL ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
});
