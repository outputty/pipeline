/**
 * #17 Done-when 8, control half — the shipped `concurrent()` strategy (#16, folded into #17)
 * leaks a real unhandled rejection on this chain. Deleted along with `src/strategies/` in the
 * enable layer, per the ticket's own Done-when 8: "after removal only the [] assertion remains".
 * Kept in its own script (not sharing one with `ConcurrentPipeline`) so this half stays a live,
 * passing assertion independent of #17's own build state.
 */
import { concurrent } from "../../src/strategies/concurrent";
import { Transformer, Pipeline } from "../../src";

const unhandled: string[] = [];
process.on("unhandledRejection", (reason) => {
  unhandled.push(String((reason as Error)?.message ?? reason));
});

function failFrom2(x: number): number {
  if (x >= 2) throw new Error(`chunk-${x}-failed`);
  return x;
}

async function main(): Promise<void> {
  const t = new Transformer<number, number>({ chunkSize: 1 })
    .withExecutor(concurrent({ maxConcurrency: 4, ordered: true }))
    .map(failFrom2);
  try {
    await new Pipeline([1, 2, 3, 4, 5]).apply(t).toArray();
  } catch {
    // expected: the run itself still rejects on the first failing chunk
  }
  await new Promise((r) => setImmediate(r));
  await new Promise((r) => setTimeout(r, 20));
  console.log(JSON.stringify({ control: unhandled }));
}

main();
