/**
 * The one chain and one gate assertion every leg in `benchmarks/` runs (#178) - declared once so no
 * leg drifts from what the other nine measure. Table 1's own chain: `map(x => x * 2)` then
 * `filter(x => x > 4)` then `toArray()`, no grouping step (`Iterator.prototype.groupBy` is undefined
 * on every one of the six pinned runtimes, and `Transformer` has no grouping op - the ticket's own
 * Done-when 9).
 *
 * `canonicalSource(5)` → `[1, 2, 3, 4, 5]`. `GATE_EXPECTED` is what every leg's `sequential()` must
 * produce from it: `[6, 8, 10]` (`1*2=2, 2*2=4` both `<= 4`, dropped; `3*2=6, 4*2=8, 5*2=10` all `>
 * 4`, kept) - the same canonical example `.claude/examples.md` and Done-when 6/7 use.
 */

/** `canonicalSource(5)` → `[1, 2, 3, 4, 5]`. Starts at 1, not 0, so the gate's own small case has no
 * zero in it - `0 * 2 = 0`, which is never `> 4` regardless of a leg's own off-by-one, so a source
 * starting at 0 could hide a boundary bug the gate exists to catch. */
export function canonicalSource(n) {
  return Array.from({ length: n }, (_, i) => i + 1);
}

/** The chain every leg's `sequential()`/`concurrent()` must compute, expressed once as a plain
 * function so a leg with no fluent `.map()/.filter()` (a hand-rolled loop) still shares the exact
 * same rule as one that does. */
export function chainOne(x) {
  const doubled = x * 2;
  return doubled > 4 ? doubled : undefined;
}

/** `expectedOutput([1,2,3,4,5])` → `[6, 8, 10]` - the oracle every leg's `gate()` call compares
 * against, computed once so ten legs never each hand-roll their own copy of the same rule. */
export function expectedOutput(source) {
  const out = [];
  for (const x of source) {
    const y = chainOne(x);
    if (y !== undefined) out.push(y);
  }
  return out;
}

export const GATE_SOURCE = canonicalSource(5);
export const GATE_EXPECTED = expectedOutput(GATE_SOURCE);
