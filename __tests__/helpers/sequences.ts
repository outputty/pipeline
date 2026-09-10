/**
 * Small, in-process test-only utilities with no shared theme beyond that (#133) - each was its own
 * repeated copy before landing here, not a family: `countPromises` (4 byte-identical copies:
 * `sync-mode`/`wrapping`/`callable`, moved here, plus `sync-mode`'s own missed one folded in later),
 * `chunksOf` (`callable.e2e.test.ts`'s own definition, used wherever a case wants the whole chunk
 * stream rather than items), `parseStrict` (3 byte-identical copies: `pipeline`/`transforms`/
 * `pipelines`), `closingSource`/`closingAsyncSource` (5 near-duplicate inline generators across
 * `buffer`/`pipelines`/`callable`, each proving an early exit ran a source's own `finally`).
 */

import { createHook } from "node:async_hooks";

/** Counts every `Promise` created while `fn` runs, via `node:async_hooks`'s `PROMISE` resource
 * type - patching `queueMicrotask`/`process.nextTick` would report `0` even for real async work,
 * passing vacuously.
 *
 * `countPromises(() => 1 + 1)` → `0`. `countPromises(() => Promise.resolve(1))` → `1`. */
export function countPromises(fn: () => unknown): number {
  let created = 0;
  const hook = createHook({
    init(_id, type) {
      if (type === "PROMISE") created++;
    },
  });
  hook.enable();
  try {
    fn();
  } finally {
    hook.disable();
  }
  return created;
}

/** Every chunk a pipeline result yields, for a case that asserts a chunk BOUNDARY rather than
 * items.
 *
 * `chunksOf(new Pipeline<number>().buffer(2)([1, 2, 3]))` → `[[1, 2], [3]]`. */
export async function chunksOf(result: { chunks(): AsyncIterable<unknown> }): Promise<unknown[]> {
  const out: unknown[] = [];
  for await (const chunk of result.chunks()) out.push(chunk);
  return out;
}

/** Parses `s`, throwing on anything that is not a real integer - the row-recovery cases' own
 * "a callback that can genuinely fail" fixture.
 *
 * `parseStrict("3")` → `3`. `parseStrict("x")` throws `Error("Invalid: x")`. */
export function parseStrict(s: string): number {
  const n = parseInt(s);
  if (isNaN(n)) throw new Error(`Invalid: ${s}`);
  return n;
}

/** A sync generator over `0..count-1` that flips `state.closed` when its own `finally` runs -
 * proves an early exit (`.first(n)`, a `break`, a rejected chunk) closed the REAL source, not
 * just stopped reading it.
 *
 * `const state = { closed: false }; pipeline(closingSource(state, 3)).first(1)` → `[0]`,
 * `state.closed` → `true`. */
export function closingSource(state: { closed: boolean }, count = 100): Generator<number> {
  return (function* () {
    try {
      for (let i = 0; i < count; i++) yield i;
    } finally {
      state.closed = true;
    }
  })();
}

/** The async twin of `closingSource` - same contract, over `for await`'s own `.return()` path.
 *
 * `const state = { closed: false }; await pipeline(closingAsyncSource(state, 3)).first(1)` →
 * `[0]`, `state.closed` → `true`. */
export function closingAsyncSource(
  state: { closed: boolean },
  count = 100,
): AsyncGenerator<number> {
  return (async function* () {
    try {
      for (let i = 0; i < count; i++) yield i;
    } finally {
      state.closed = true;
    }
  })();
}
