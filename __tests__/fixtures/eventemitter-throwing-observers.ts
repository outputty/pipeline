/**
 * #124's own review rounds 1-2 - regression coverage for the throwing-observer fixes (a route's own
 * `:dispatched`/`:done`/`:end`, and the drain's own trail-end event), per this repo's Tests rule
 * ("what survives is the answer, written into a real test in its proper home"). Runs as its own
 * process for the same reason
 * `eventemitter-async-throw.ts` does - `emitSafely()`'s own `queueMicrotask(() => { throw error })`
 * surfaces a throwing observer as a real `uncaughtException`, which Vitest's own process-wide
 * handler would report as a test-runner error rather than a value this script can observe.
 */
import { EventEmitterPipeline } from "../../src/eventemitter";
import {
  captureUncaughtExceptions,
  drainUncaughtExceptions,
  drainUnhandledRejections,
  errorMessage,
  runFixtureMain,
} from "./unhandled-rejection";

// Opted in BEFORE anything below can throw uncaught - captureUncaughtExceptions() installs the
// process-wide handler this file's own `drainUncaughtExceptions()` calls read from.
captureUncaughtExceptions();

/** A throwing lifecycle listener on `event` must never corrupt a normal chunk's own result -
 * `/transform/0:dispatched` and `/transform/0:done` share this identical shape: register a throwing
 * listener, run the pipeline, confirm the real dispatch settled fine regardless. */
async function listenerThrows(
  event: string,
  throwMessage: string,
): Promise<{ out: number[] | null; rejection: string | null }> {
  const pipeline = new EventEmitterPipeline<number>()
    .buffer(1)
    .transform((t) => t.map((x: number) => x * 2));
  pipeline.emitter.on(event, () => {
    throw new Error(throwMessage);
  });

  let out: number[] | null = null;
  let rejection: string | null = null;
  try {
    out = await pipeline([1, 2, 3]).toArray();
  } catch (error) {
    rejection = errorMessage(error);
  }
  return { out, rejection };
}

/** A throwing `/transform/0:end`/`:end` listener used to REPLACE a real, already-propagating chunk
 * error with its own unrelated one (JS's finally-overrides-exception semantics), since the throw
 * happened inside `withEndSignal`'s own `finally` block - `apply()`'s own wrap (`/transform/0:end`)
 * and `drainable()`'s own wrap (`:end`) are two separate call sites, both covered here. */
async function endListenerThrowsOverRealFailure(): Promise<{ rejection: string | null }> {
  const pipeline = new EventEmitterPipeline<number>().buffer(1).transform((t) =>
    t.map((x: number) => {
      if (x === 2) throw new Error("real-chunk-failure");
      return x;
    }),
  );
  pipeline.emitter.on("/transform/0:end", () => {
    throw new Error("observer-boom-on-end");
  });
  pipeline.emitter.on(":end", () => {
    throw new Error("observer-boom-on-pipeline-end");
  });

  let rejection: string | null = null;
  try {
    await pipeline([1, 2]).toArray();
  } catch (error) {
    rejection = errorMessage(error);
  }
  return { rejection };
}

async function main(): Promise<void> {
  const dispatched = await listenerThrows("/transform/0:dispatched", "observer-boom-on-dispatched");
  const done = await listenerThrows("/transform/0:done", "observer-boom-on-done");
  const ended = await endListenerThrowsOverRealFailure();
  const unhandled = await drainUnhandledRejections(60);
  const uncaught = await drainUncaughtExceptions(60);
  console.log(JSON.stringify({ dispatched, done, ended, unhandled, uncaught }));
}

runFixtureMain(main);
