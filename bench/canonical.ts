/**
 * The one canonical program every leg of `bench/overhead.ts` measures, and the one hand-rolled
 * floor every leg compares against (#120) - declared once so a `ConcurrentPipeline`/`HttpPipeline`/
 * `ClusterPipeline` leg and its own equality test read the identical chain, input and row count a
 * `Pipeline` leg does, per `code.md`'s "two callers disagree, read the declaration they share" rule.
 *
 * The floor is OUTPUT-matched, not mechanism-matched (the ticket's own Constraints): for the three
 * dispatching classes it is the exact same in-process loop, deliberately skipping the network/IPC
 * boundary each of them would otherwise cross - their ratio measures "is dispatching worth it here",
 * never "is this package's own per-chunk loop efficient", which only the `Pipeline` leg's ratio
 * measures cleanly.
 */

import type { Transformer } from "../src";

/** `.map((x) => x * 2).filter((x) => x > 4)` - every leg's own `.transform()` call passes this
 * builder, so the composed chain is textually identical everywhere it appears. */
export function canonicalChain<M extends "sync" | "async">(
  t: Transformer<number, number, M>,
): Transformer<number, number, M> {
  return t.map((x: number) => x * 2).filter((x: number) => x > 4);
}

/** `canonicalInput(5)` → `[0, 1, 2, 3, 4]` - deterministic, no allocation beyond the one array. */
export function canonicalInput(n: number): number[] {
  return Array.from({ length: n }, (_, i) => i);
}

/**
 * The REAL floor (#120's own planning): a single fused loop producing the identical output to
 * `canonicalChain`, with no intermediate array and no `Transformer` machinery at all - the
 * quickest in-process code producing the SAME rows in the SAME order.
 *
 * `handRolledFloor([0, 1, 2, 3])` → `[6]` (`0*2=0` dropped, `1*2=2` dropped, `2*2=4` dropped
 * (`4 > 4` is false), `3*2=6` kept).
 */
export function handRolledFloor(items: number[]): number[] {
  const out: number[] = [];
  for (const item of items) {
    const doubled = item * 2;
    if (doubled > 4) out.push(doubled);
  }
  return out;
}

/** Rows measured per class (#120's own Interface) - fixed so every run, and the committed baseline
 * it gates against, measure the same shape. */
export const ROWS = {
  Pipeline: 1_000_000,
  ConcurrentPipeline: 200_000,
  HttpPipeline: 20_000,
  ClusterPipeline: 20_000,
} as const;

/** `ConcurrentPipeline`/`HttpPipeline`/`ClusterPipeline` all measure at this buffer and
 * `maxConcurrency` (the package's own default - #120's Interface names it only for
 * `ConcurrentPipeline`, and leaves the other two at that same default rather than overriding it). */
export const BUFFER_SIZE = 1000;
export const MAX_CONCURRENCY = 4;
export const CLUSTER_WORKERS = 2;

/** Timed rounds per leg (chosen for this harness, not stated by the ticket): round 1 is discarded
 * as JIT/connection warm-up, matching the ticket's own "first round of N discarded" wording and its
 * "HTTP's first round ran 2.9x its median" evidence - `median` is the ticket's own aggregate. */
export const ROUNDS = 5;

/** The middle value of `values` sorted ascending - `median([3, 1, 2])` → `2`. An even count
 * averages its two middle values, `median([1, 2, 3, 4])` → `2.5`. Raises on an empty array rather
 * than returning `NaN`: a lookup with nothing to look up has no valid answer to hand back
 * (`code.md`'s "fail loud" rule) - a `NaN` here would silently pass `checkGate`'s own `>` comparison
 * every time, reporting a broken measurement as a clean one. */
export function median(values: number[]): number {
  if (values.length === 0) throw new Error("median() of an empty array has no defined value");
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
}

/** Times `run` over `rounds` rounds, discarding round 1 as warm-up, and returns the ns/row median of
 * the rest (#120's own gate tolerance section) - shared by every leg so "how a leg is timed" is one
 * function, not one copy per class. `run` returns the row count it actually produced, since a
 * hand-rolled floor and a `Pipeline` leg both already know it without a second pass to count it.
 * Raises for `rounds < 2`: round 1 is always discarded as warm-up, so at least one round must
 * remain for `median` to have anything to measure. */
export async function timeRounds(
  run: () => Promise<number> | number,
  rounds: number = ROUNDS,
): Promise<number> {
  if (rounds < 2) {
    throw new Error(`timeRounds needs at least 2 rounds (1 warm-up + 1 measured), got ${rounds}`);
  }
  const nsPerRowByRound: number[] = [];
  for (let round = 0; round < rounds; round++) {
    const start = performance.now();
    const rows = await run();
    const elapsedMs = performance.now() - start;
    if (round === 0) continue; // warm-up
    nsPerRowByRound.push((elapsedMs * 1_000_000) / rows);
  }
  return median(nsPerRowByRound);
}
