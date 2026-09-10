/**
 * #124's own review rounds 1-2, plus `/code-review xhigh`'s own follow-up findings - regression
 * coverage for the throwing-observer fixes (`:dispatched`, `:done`, `:end`, `pipeline:end`), per
 * this repo's Tests rule ("what survives is the answer, written into a real test in its proper
 * home"). Runs as its own process for the same reason `eventemitter-async-throw.ts` does -
 * `emitSafely()`'s own `queueMicrotask(() => { throw error })` surfaces a throwing observer as a
 * real `uncaughtException`, which Vitest's own process-wide handler would report as a test-runner
 * error rather than a value this script can observe.
 */
import { EventEmitterPipeline } from "../../src";
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

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** A throwing lifecycle listener on `event` must never corrupt a normal chunk's own result -
 * `stage:0:dispatched` and a synchronously-throwing `stage:0:done` share this identical shape:
 * register a throwing listener, run the pipeline, confirm the real dispatch settled fine
 * regardless. */
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

/** `code-review xhigh`'s own F4 - an ASYNC `stage:0:done` listener that throws AFTER its own
 * `await` used to leak as a real `unhandledRejection` (`emitSafely()`'s synchronous `try` had
 * already returned by the time the throw happened), never surfacing as the documented "its own
 * separate uncaught exception". */
async function asyncListenerThrowsAfterAwait(): Promise<{
  out: number[] | null;
  rejection: string | null;
}> {
  const pipeline = new EventEmitterPipeline<number>()
    .buffer(1)
    .transform((t) => t.map((x: number) => x * 2));
  pipeline.emitter.on("stage:0:done", async () => {
    await delay(5);
    throw new Error("observer-boom-on-done-after-await");
  });

  let out: number[] | null = null;
  let rejection: string | null = null;
  try {
    out = await pipeline([1, 2, 3]).toArray();
  } catch (error) {
    rejection = errorMessage(error);
  }
  await delay(30);
  return { out, rejection };
}

/** `code-review xhigh`'s own F5 - a synchronously-throwing `stage:0:done` listener used to stop
 * Node's own `EventEmitter.emit()` from ever reaching a SIBLING listener registered on the same
 * event, since `emit()` does not catch a listener's own throw. */
async function throwingListenerDoesNotBlockSibling(): Promise<{
  out: number[] | null;
  siblingRan: boolean;
}> {
  const pipeline = new EventEmitterPipeline<number>()
    .buffer(1)
    .transform((t) => t.map((x: number) => x * 2));
  let siblingRan = false;
  pipeline.emitter.on("stage:0:done", () => {
    throw new Error("observer-boom-before-sibling");
  });
  pipeline.emitter.on("stage:0:done", () => {
    siblingRan = true;
  });

  const out = await pipeline([1, 2, 3]).toArray();
  return { out, siblingRan };
}

/** A throwing `stage:0:end`/`pipeline:end` listener used to REPLACE a real, already-propagating
 * chunk error with its own unrelated one (JS's finally-overrides-exception semantics), since the
 * throw happened inside `withEndSignal`'s own `finally` block - `apply()`'s own wrap (`stage:0:end`)
 * and `drainable()`'s own wrap (`pipeline:end`) are two separate call sites, both covered here. */
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
  pipeline.emitter.on("pipeline:end", () => {
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
  const dispatched = await listenerThrows("stage:0:dispatched", "observer-boom-on-dispatched");
  const done = await listenerThrows("stage:0:done", "observer-boom-on-done");
  const asyncDone = await asyncListenerThrowsAfterAwait();
  const sibling = await throwingListenerDoesNotBlockSibling();
  const ended = await endListenerThrowsOverRealFailure();
  const unhandled = await drainUnhandledRejections(60);
  const uncaught = await drainUncaughtExceptions(60);
  console.log(JSON.stringify({ dispatched, done, asyncDone, sibling, ended, unhandled, uncaught }));
}

runFixtureMain(main);
