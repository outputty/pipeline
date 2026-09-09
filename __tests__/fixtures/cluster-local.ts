/**
 * #61 Done-when 2 — the ticket's own canonical program over a real `ClusterPipeline`: the reduce
 * folds inside `.local(build)`, so it must run in the PRIMARY process, never a forked worker - the
 * accumulator carries `process.pid` alongside the sum, the same technique `cluster-reduce.ts` uses,
 * so the primary's own script can tell a region that silently dispatched from one that stayed put.
 */
import { ClusterPipeline } from "../../src";

const primaryPid = process.pid;

const [folded] = await new ClusterPipeline({ maxConcurrency: 2 })
  .from([1, 2, 3, 4, 5])
  .buffer(2)
  .local((p) =>
    p.reduce(
      (acc: { sum: number; pid: number }, x: number) => ({ sum: acc.sum + x, pid: process.pid }),
      { sum: 0, pid: primaryPid },
    ),
  )
  .toArray();

if (folded) {
  console.log(JSON.stringify({ sum: folded.sum, stayedInPrimary: folded.pid === primaryPid }));
}
