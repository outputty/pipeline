/**
 * Small, repeated test-only utilities that touch neither HTTP nor subprocesses (#133) -
 * `countPromises` (3 byte-identical copies before this: `sync-mode`/`wrapping`/`callable`),
 * `chunksOf` (`callable.e2e.test.ts`'s own definition, used wherever a case wants the whole chunk
 * stream rather than items), `parseStrict` (3 byte-identical copies: `pipeline`/`transforms`/
 * `pipelines`).
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
