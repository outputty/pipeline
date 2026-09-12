/**
 * The `ClusterPipeline` leg (#120) - STUB, landing L3 (real forked workers, floor equality, and the
 * `.local()` row's `workerPidsWhilePinned` correctness, plus the CLI that wires all four legs
 * together). `__tests__/bench.e2e.test.ts`'s own ClusterPipeline cases stay `it.fails` against this
 * stub until L3 replaces it.
 */
import type { LegReport } from "../gate";

export async function measureClusterPipeline(_rounds?: number): Promise<LegReport> {
  throw new Error("ClusterPipeline leg not implemented yet (#120 L3)");
}

export async function clusterMatchesFloor(_n: number): Promise<boolean> {
  throw new Error("ClusterPipeline leg not implemented yet (#120 L3)");
}
