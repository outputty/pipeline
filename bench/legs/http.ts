/**
 * The `HttpPipeline` leg (#120) - STUB, landing L2 (a real loopback server, floor equality, and
 * the `.local()` row's `requestsWhilePinned` correctness). `__tests__/bench.e2e.test.ts`'s own
 * HttpPipeline cases stay `it.fails` against this stub until L2 replaces it.
 */
import type { LegReport } from "../gate";

export async function measureHttpPipeline(_rounds?: number): Promise<LegReport> {
  throw new Error("HttpPipeline leg not implemented yet (#120 L2)");
}

export async function httpMatchesFloor(_n: number): Promise<boolean> {
  throw new Error("HttpPipeline leg not implemented yet (#120 L2)");
}
