/**
 * The one harness every leg's row goes through (#178) - `gate` before any timing, `timeFixed` for
 * table 1, `timeAtInFlight` for table 2. No timing dependency: `performance.now()` is the whole
 * instrument, per the ticket's own Constraint.
 */

import { GATE_SOURCE, GATE_EXPECTED } from "./canonical.mjs";

const median = (sorted) => {
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
};

/**
 * Asserts `leg.sequential(source, { chunkSize: source.length })` produces `expected`, BEFORE any
 * timing - a leg that fails this never gets a number recorded (Done-when 7). Raises with the leg's
 * own name and both arrays on a mismatch, rather than returning a boolean a caller could ignore.
 *
 * `gate(pipelineLeg)` against the default `GATE_SOURCE`/`GATE_EXPECTED` asserts `[6, 8, 10]` from
 * `[1, 2, 3, 4, 5]` - the canonical example every table 1 row shares.
 */
export async function gate(leg, source = GATE_SOURCE, expected = GATE_EXPECTED) {
  const result = await leg.sequential(source, { chunkSize: source.length });
  const actual = Array.isArray(result) ? result : Array.from(result);
  const matches =
    actual.length === expected.length && actual.every((value, i) => value === expected[i]);
  if (!matches) {
    throw new Error(
      `${leg.name}: gate failed - got ${JSON.stringify(actual)}, expected ${JSON.stringify(expected)}`,
    );
  }
}

/**
 * Runs `fn` `repeats` times (default 5, the ticket's own fixed count - no warm-up discarded, every
 * repeat counted) and returns `{ median, min, max }` in milliseconds. Raises for `repeats < 1`: a
 * median of nothing has no defined value.
 *
 * `timeFixed(() => leg.sequential(source, { chunkSize: 1000 }))` → one `{ median, min, max }`.
 */
export async function timeFixed(fn, { repeats = 5 } = {}) {
  if (repeats < 1) {
    throw new Error(`timeFixed needs at least 1 repeat, got ${repeats}`);
  }
  const times = [];
  for (let i = 0; i < repeats; i++) {
    const started = performance.now();
    await fn();
    times.push(performance.now() - started);
  }
  const sorted = [...times].sort((a, b) => a - b);
  return { median: median(sorted), min: sorted[0], max: sorted[sorted.length - 1] };
}

/** Wraps `work` to count simultaneous in-flight calls, returning the wrapped function and a getter
 * for the peak it saw - the one instrument `timeAtInFlight` needs, shared so it is measured the same
 * way whether the caller is `leg.concurrent`'s own internal scheduler or the timed run around it. */
function trackPeakInFlight(work) {
  let current = 0;
  let peak = 0;
  const wrapped = async (item) => {
    current++;
    peak = Math.max(peak, current);
    try {
      return await work(item);
    } finally {
      current--;
    }
  };
  return { wrapped, peak: () => peak };
}

/**
 * Measures `leg.concurrent(source, { inFlight: target, work })`'s REAL simultaneous in-flight count
 * with one untimed dry run, throws if it does not equal `target` (Done-when 10), and only then times
 * `repeats` clean runs with the caller's own unwrapped `work` (the wrapped, counting version never
 * enters the timed region - the per-call increment/decrement would be measured as this package's own
 * cost otherwise). Returns `null` for a leg with no bounded concurrency (`leg.concurrent` absent) -
 * table 2 omits that leg by naming it, never by silently producing no row (Done-when 11).
 *
 * `timeAtInFlight(streamingIterablesLeg, { source, target: 1000, work: setTimeoutZero })` → `{
 * measuredPeak: 1000, median, min, max }`, or throws if the real peak was ever anything else.
 */
export async function timeAtInFlight(leg, { source, target, work, repeats = 5 }) {
  if (typeof leg.concurrent !== "function") return null;

  const { wrapped, peak } = trackPeakInFlight(work);
  await leg.concurrent(source, { inFlight: target, work: wrapped });
  const measuredPeak = peak();
  if (measuredPeak !== target) {
    throw new Error(`${leg.name}: measured peak in-flight ${measuredPeak} !== target ${target}`);
  }

  const timing = await timeFixed(() => leg.concurrent(source, { inFlight: target, work }), {
    repeats,
  });
  return { measuredPeak, ...timing };
}
