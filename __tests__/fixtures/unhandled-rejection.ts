/**
 * Captures every `unhandledRejection` this process sees, for a subprocess fixture proving a chunk
 * failure never leaks one - `concurrent-unhandled.ts` (#17) and `eventemitter-async-throw.ts`/
 * `eventemitter-throwing-observers.ts` (#124) all need it, extracted here rather than duplicated a
 * second time. Never imports Vitest: a fixture script runs as its OWN process specifically to escape
 * Vitest's own handlers, so this module must stay free of that import too.
 */

/** `error instanceof Error ? error.message : String(error)` - every fixture script that captures a
 * caught error's own message needs this. */
export function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/** Copies `buffer`'s current contents out and clears it in place - the one settle-then-copy-then-clear
 * shape both `drainUnhandledRejections()` and `drainUncaughtExceptions()` share. */
async function drain(buffer: string[], settleMs: number): Promise<string[]> {
  await new Promise((resolve) => setImmediate(resolve));
  await new Promise((resolve) => setTimeout(resolve, settleMs));
  const seen = [...buffer];
  buffer.length = 0;
  return seen;
}

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
export function drainUnhandledRejections(settleMs = 20): Promise<string[]> {
  return drain(unhandled, settleMs);
}

/** `null` until `captureUncaughtExceptions()` opts a fixture in - importing this MODULE must not
 * install the handler as a side effect (code-review xhigh, F6): `concurrent-unhandled.ts` (#17)
 * imports `drainUnhandledRejections`/`runFixtureMain` from here without ever expecting an uncaught
 * exception, and relies on Node's own default crash-on-uncaught-exception behavior to surface a real
 * fault as a non-zero exit. Installing the handler unconditionally at import time silently absorbed
 * that fault into an array nobody reads instead, masking it as a clean, zero-exit run. */
let uncaughtExceptions: string[] | null = null;

/** Opts a fixture INTO `uncaughtException` capture - an explicit call, never a side effect of
 * importing this module (see `uncaughtExceptions`'s own docstring above). Call it before anything
 * that could throw uncaught: `eventemitter-throwing-observers.ts` (#124) needs it for a throwing
 * lifecycle observer's own `emitSafely()`-scheduled rethrow, which surfaces as an uncaught exception,
 * not a rejection. Idempotent - a second call is a no-op. */
export function captureUncaughtExceptions(): void {
  if (uncaughtExceptions !== null) return;
  const buffer: string[] = [];
  uncaughtExceptions = buffer;
  process.on("uncaughtException", (error) => {
    buffer.push(errorMessage(error));
  });
}

/** The `uncaughtException` sibling of `drainUnhandledRejections()` - reads `[]` for a fixture that
 * never called `captureUncaughtExceptions()`, since nothing was ever captured to drain. */
export function drainUncaughtExceptions(settleMs = 20): Promise<string[]> {
  return uncaughtExceptions === null ? Promise.resolve([]) : drain(uncaughtExceptions, settleMs);
}

/** Runs a fixture script's own `main()`, reporting an uncaught rejection from it as `FATAL <message>`
 * on stdout with a non-zero exit code - every subprocess fixture under `__tests__/fixtures/` ends
 * with this same call. Printing `FATAL ...` (rather than leaving stdout empty) means the calling
 * test's own exit-code check catches a real fault FIRST, before `JSON.parse` on that line obscures
 * the real message with a generic `SyntaxError`. */
export function runFixtureMain(main: () => Promise<void>): void {
  main().catch((error: unknown) => {
    console.log(`FATAL ${errorMessage(error)}`);
    process.exitCode = 1;
  });
}
