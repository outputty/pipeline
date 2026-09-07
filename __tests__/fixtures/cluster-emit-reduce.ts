/**
 * #62 — an emit-declaring reduce (`fn.length === 4`) owes no combine at all: `.toArray()` over a
 * real `ClusterPipeline`, dispatched to real forked workers, returns the raw per-partition emits
 * with no throw and no `.combine()` call, exactly like `ConcurrentPipeline`'s own case.
 */
import { ClusterPipeline } from "../../src";
import type { IContextManager } from "../../src";

const data = await new ClusterPipeline([1, 2, 3, 4, 5], { maxConcurrency: 2 })
  .buffer(2)
  .reduce((_acc: number, x: number, _ctx: IContextManager, emit: (v: number) => void) => {
    emit(x * 10);
    return 0;
  }, 0)
  .toArray();

if (data.length > 0) {
  console.log(JSON.stringify({ values: data.flat() }));
}
