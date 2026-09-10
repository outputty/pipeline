/**
 * #124 Done-when 6 - an async Worker that throws AFTER its own `await` (never calling `reject()`
 * explicitly) must reject the chunk's dispatch the same as a synchronous throw or an explicit
 * `reject()` call: no hang, no unhandled rejection. Runs as its own process
 * (`unhandled-rejection.ts`'s own `process.on("unhandledRejection", …)`), the same reason
 * `concurrent-unhandled.ts` (#17) does - Vitest installs its own handler and would report a leak as
 * a test-runner error, never a value this script can observe.
 *
 * The composed function (the chain's own `.transform()`) is deliberately SLOWER (30ms) than the
 * external Worker's throw-after-await (5ms), so the external Worker's rejection is what decides the
 * chunk - proving BOTH halves of #124's own Done-when 4 (first to settle wins) and Done-when 6 (a
 * throw after `await`, not an explicit `reject()`, still settles it) in one real run, and proving
 * the SLOWER, losing composed-function promise never leaks as unhandled once it resolves later.
 */
import { EventEmitterPipeline } from "../../src";
import { drainUnhandledRejections, errorMessage, runFixtureMain } from "./unhandled-rejection";

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main(): Promise<void> {
  const pipeline = new EventEmitterPipeline<number>().transform((t) =>
    t.map(async (x: number) => {
      await delay(30);
      return x * 2;
    }),
  );

  pipeline.emitter.on("stage:0", async ({ chunk }: { chunk: number[] }) => {
    await delay(5);
    // No explicit reject() call - the throw itself is the whole point of this fixture.
    throw new Error(`worker-threw-after-await-${chunk.join(",")}`);
  });

  let rejection: string | null = null;
  try {
    await pipeline([1, 2, 3]).toArray();
  } catch (error) {
    rejection = errorMessage(error);
  }

  const unhandled = await drainUnhandledRejections(60);
  console.log(JSON.stringify({ rejection, unhandled }));
}

runFixtureMain(main);
