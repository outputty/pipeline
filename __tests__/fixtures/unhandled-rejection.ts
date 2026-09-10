/**
 * Captures every `unhandledRejection` this process sees, for a subprocess fixture proving a chunk
 * failure never leaks one - `concurrent-unhandled.ts` (#17) and `eventemitter-async-throw.ts` (#124)
 * both need it, extracted here rather than duplicated a second time. Never imports Vitest: a
 * fixture script runs as its OWN process specifically to escape Vitest's own `unhandledRejection`
 * handler, so this module must stay free of that import too.
 */

const unhandled: string[] = [];
process.on("unhandledRejection", (reason) => {
  unhandled.push(String((reason as Error)?.message ?? reason));
});

/**
 * Waits `settleMs` for pending microtasks and timers to fire, then hands back every rejection seen
 * since the last call (or since the process started) and clears the buffer.
 *
 * `await drainUnhandledRejections()` after a run that is expected to leak nothing → `[]`.
 */
export async function drainUnhandledRejections(settleMs = 20): Promise<string[]> {
  await new Promise((resolve) => setImmediate(resolve));
  await new Promise((resolve) => setTimeout(resolve, settleMs));
  const seen = [...unhandled];
  unhandled.length = 0;
  return seen;
}
