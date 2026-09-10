/**
 * #124's own review rounds 1-2 - regression coverage for the throwing-observer fixes, per this
 * repo's Tests rule ("what survives is the answer, written into a real test in its proper home").
 * Runs as its own process for the same reason `eventemitter-async-throw.ts` does - `emitSafely()`'s
 * own `queueMicrotask(() => { throw error })` surfaces a throwing observer as a real
 * `uncaughtException`, which Vitest's own process-wide handler would report as a test-runner error
 * rather than a value this script can observe.
 */
import { EventEmitterPipeline } from "../../src";
import { drainUnhandledRejections } from "./unhandled-rejection";

const uncaught: string[] = [];
process.on("uncaughtException", (error) => {
  uncaught.push(error instanceof Error ? error.message : String(error));
});

/** A throwing `stage:0:dispatched` listener used to synchronously reject the whole dispatch as if
 * it were a Worker failure - `.onError()` silently absorbed it and `out` came back `[]`. */
async function dispatchedListenerThrows(): Promise<{
  out: number[] | null;
  rejection: string | null;
}> {
  const pipeline = new EventEmitterPipeline<number>()
    .buffer(1)
    .transform((t) => t.map((x: number) => x * 2));
  pipeline.emitter.on("stage:0:dispatched", () => {
    throw new Error("observer-boom-on-dispatched");
  });

  let out: number[] | null = null;
  let rejection: string | null = null;
  try {
    out = await pipeline([1, 2, 3]).toArray();
  } catch (error) {
    rejection = error instanceof Error ? error.message : String(error);
  }
  return { out, rejection };
}

/** A throwing `stage:0:end` listener used to REPLACE a real, already-propagating chunk error with
 * its own unrelated one (JS's finally-overrides-exception semantics), since the throw happened
 * inside `withEndSignal`'s own `finally` block. */
async function endListenerThrowsOverRealFailure(): Promise<{ rejection: string | null }> {
  const pipeline = new EventEmitterPipeline<number>().buffer(1).transform((t) =>
    t.map((x: number) => {
      if (x === 2) throw new Error("real-chunk-failure");
      return x;
    }),
  );
  pipeline.emitter.on("stage:0:end", () => {
    throw new Error("observer-boom-on-end");
  });

  let rejection: string | null = null;
  try {
    await pipeline([1, 2]).toArray();
  } catch (error) {
    rejection = error instanceof Error ? error.message : String(error);
  }
  return { rejection };
}

async function main(): Promise<void> {
  const dispatched = await dispatchedListenerThrows();
  const ended = await endListenerThrowsOverRealFailure();
  const unhandled = await drainUnhandledRejections(60);
  console.log(JSON.stringify({ dispatched, ended, unhandled, uncaught }));
}

main().catch((error: unknown) => {
  console.log(`FATAL ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
});
