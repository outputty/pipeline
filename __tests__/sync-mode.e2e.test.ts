/**
 * #90 - the synchronous fast path. Done-when cases 1 to 8 land here; case 9 (`pnpm check` passes,
 * `.branch()`'s signature untouched) is the gate itself and has no test of its own.
 *
 * Its own file rather than `__tests__/pipeline.e2e.test.ts`, which Done-when 4 names: this ticket
 * adds a Mode dimension every case shares, and the existing file is already the home of the
 * pre-Mode constructor's own suite, which L3 rewrites wholesale.
 *
 * L1 of the stack: every case is expected-to-fail. A runtime case is marked with vitest's own
 * `it.fails`; the not-yet-built API it calls is marked with `@ts-expect-error`, since `tsc --noEmit`
 * covers `__tests__/` and an un-suppressed error breaks `pnpm check` outright. For most cases the
 * flip is REMOVING a directive, and `TS2578: Unused '@ts-expect-error' directive` is the signal that
 * one started passing early. Cases 4 and 7 flip the other way - L3 ADDS a directive to each, on the
 * `.transform()` call and on the two-arg constructor - so each carries its own note below saying so.
 *
 * Under a suppressed line the expression degrades to `any`, which is why each callback below spells
 * its own parameter types out. The CALLBACK PARAMETER annotations are the redundant ones L3 deletes;
 * the `: number[]` and `: Promise<number[]>` annotations on each result stay, and are what carry
 * Done-when 1, 2, 5, 6 and 8's compile-time half once the directives above them are gone.
 */

import { describe, it, expect } from "vitest";
import { createHook } from "node:async_hooks";
import { Pipeline } from "@src/pipeline";
import { Transformer } from "@src/transformer";
import { ConcurrentPipeline } from "@src/pipelines/concurrent";
import { HttpPipeline } from "@src/pipelines/http";
import { ClusterPipeline } from "@src/pipelines/cluster";

/**
 * Counts every `Promise` created while `fn` runs, via `node:async_hooks`'s `PROMISE` resource type.
 *
 * Done-when 3 suggested instrumenting `queueMicrotask`/`process.nextTick` instead. Measured: that
 * patch reports `0` for `Promise.resolve().then(() => {})` and for an `async` function call - V8
 * schedules a promise job without routing it through either global, so the assertion would pass
 * vacuously whatever the engine did. This hook reports `2` and `1` for those same two controls and
 * `0` for plain synchronous array work, which is what the first test below pins.
 *
 * `countPromises(() => [1, 2].map((x) => x * 2))` → `0`.
 */
function countPromises(fn: () => unknown): number {
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

describe("#90 - a synchronous chain never creates a Promise", () => {
  it("countPromises itself counts real async work and ignores sync work", () => {
    // The positive control for the instrument. Without it Done-when 3's own zero proves nothing: an
    // instrument that never counts anything reports zero for every engine.
    expect(countPromises(() => Promise.resolve().then(() => {}))).toBeGreaterThan(0);
    expect(countPromises(() => void (async () => 1)())).toBeGreaterThan(0);
    expect(countPromises(() => [1, 2, 3].map((x) => x * 2))).toBe(0);
  });

  it.fails("Done-when 1: a fully sync chain returns number[] with no await", () => {
    // @ts-expect-error - L1: the no-source constructor does not exist yet (#90)
    const builder = new Pipeline<number>({});
    const out: number[] = builder
      // @ts-expect-error - L1: .from() does not exist yet (#90)
      .from([1, 2, 3, 4, 5])
      .transform((t: Transformer<number, number>) =>
        t.map((x: number) => x * 2).filter((x: number) => x > 4),
      )
      .toArray();

    expect(out).toEqual([6, 8, 10]);
  });

  it.fails("Done-when 2: one async callback widens the chain to Promise<number[]>", async () => {
    // @ts-expect-error - L1: the no-source constructor does not exist yet (#90)
    const builder = new Pipeline<number>({});
    const out: Promise<number[]> = builder
      // @ts-expect-error - L1: .from() does not exist yet (#90)
      .from([1, 2, 3, 4, 5])
      .transform((t: Transformer<number, number>) =>
        t.map(async (x: number) => x * 2).filter((x: number) => x > 4),
      )
      .toArray();

    expect(await out).toEqual([6, 8, 10]);
  });

  it.fails("Done-when 3: zero promises are created between .from() and .toArray()", () => {
    // @ts-expect-error - L1: the no-source constructor does not exist yet (#90)
    const builder = new Pipeline<number>({});
    const created = countPromises(() =>
      builder
        // @ts-expect-error - L1: .from() does not exist yet (#90)
        .from([1, 2, 3, 4, 5])
        .transform((t: Transformer<number, number>) =>
          t.map((x: number) => x * 2).filter((x: number) => x > 4),
        )
        .toArray(),
    );

    expect(created).toBe(0);
  });

  it("Done-when 4: .transform() before .from() is a compile error", () => {
    // Type-only: never executed. `tsc --noEmit` is the real assertion, matching the convention in
    // `__tests__/pipelines.e2e.test.ts`.
    //
    // THIS CASE FLIPS BY ADDING A DIRECTIVE, not by removing one, and nothing in the gate forces
    // that move on its own. At L3 the constructor becomes valid, so the directive below goes
    // `TS2578` and must be deleted; the `.transform()` line then errors, and the case is live only
    // once a directive sits THERE reading the "unset" Mode refusal's own diagnostic. L3 confirms
    // that diagnostic is `TS2684` specifically (the conditional `this` parameter), not a `TS2345`
    // argument mismatch, by deleting the new directive once and reading what tsc prints.
    function typeOnlyCheck() {
      // @ts-expect-error - L1: the no-source constructor does not exist yet (#90)
      const builder = new Pipeline<number>({});
      builder.transform((t) => t.map((x) => x));
    }
    expect(typeof typeOnlyCheck).toBe("function");
  });

  it("Done-when 7: the two-arg constructor still compiles - L3 is what removes it", () => {
    // The BEFORE state, recorded so case 7 has a home. It cannot be `@ts-expect-error`'d at L1:
    // the two-arg constructor is exactly what still works today, so a directive here would be
    // `TS2578` and fail the gate.
    //
    // THIS CASE FLIPS BY ADDING A DIRECTIVE too. At L3 the call below stops compiling, a directive
    // goes above it, and this test becomes the assertion that the removal really landed - alongside
    // the sweep of every other call site across `src/`, `__tests__/`, `README.md` and `.claude/*.md`
    // that the same layer carries.
    const legacy = new Pipeline<number>([1, 2, 3], {});
    expect(legacy).toBeInstanceOf(Pipeline);
  });

  it.fails(
    "Done-when 5: a thenable-returning link widens the run rather than throwing",
    async () => {
      // Amended from the ticket's own text by this build's F2 finding, on the user's ruling: the
      // engine widens the run instead of throwing. Where the callback's return type is visible,
      // TypeScript widens the chain to `Promise<number[]>` on its own, and the two agree - this
      // case. Where it is `any` (an untyped import, a `JSON.parse` result), the type says
      // `number[]` and the run still returns a Promise resolving to the right values: a wrong
      // static type at an `any` boundary, never wrong data, and never a `Promise` left unawaited
      // in the output. `code.md` rules out a guard against a misuse the caller could mean, which
      // is what the ticket's own per-chunk throw would have been.
      // @ts-expect-error - L1: the no-source constructor does not exist yet (#90)
      const builder = new Pipeline<number>({});
      const out: Promise<number[]> = builder
        // @ts-expect-error - L1: .from() does not exist yet (#90)
        .from([1, 2, 3])
        .transform((t: Transformer<number, number>) => t.map((x: number) => Promise.resolve(x * 2)))
        .toArray();

      expect(await out).toEqual([2, 4, 6]);
    },
  );

  it.fails(
    "Done-when 6: a dispatching class forces async whatever the source's shape",
    async () => {
      // The criterion is compile-time: an ARRAY source, whose shape says "sync", must still come
      // out "async" on every dispatching class. `instanceof` alone cannot see that - a subclass
      // that never got its own `.from()` override inherits the base's `Iterable → "sync"` arm,
      // `toArray()` then returns a plain array, `await` on one is a no-op, and both the
      // `instanceof` and the `toEqual` still pass. The three `: Promise<number[]>` annotations
      // below are the assertion; each fails to compile if its class's Mode came out "sync".
      // @ts-expect-error - L1: the no-source constructor does not exist yet (#90)
      const concurrent = new ConcurrentPipeline<number>({}).from([1, 2, 3]);
      // @ts-expect-error - L1: the no-source constructor does not exist yet (#90)
      const http = new HttpPipeline<number>({ url: "http://127.0.0.1:1" }).from([1, 2, 3]);
      // @ts-expect-error - L1: the no-source constructor does not exist yet (#90)
      const cluster = new ClusterPipeline<number>({}).from([1, 2, 3]);

      const concurrentOut: Promise<number[]> = concurrent.toArray();
      const httpOut: Promise<number[]> = http.toArray();
      const clusterOut: Promise<number[]> = cluster.toArray();

      expect(concurrent).toBeInstanceOf(ConcurrentPipeline);
      expect(http).toBeInstanceOf(HttpPipeline);
      expect(cluster).toBeInstanceOf(ClusterPipeline);
      expect(typeof httpOut.then).toBe("function");
      expect(typeof clusterOut.then).toBe("function");
      expect(await concurrentOut).toEqual([1, 2, 3]);
    },
  );

  it.fails("Done-when 8: .buffer()/.onError()/.local() all preserve the sync Mode", () => {
    // @ts-expect-error - L1: the no-source constructor does not exist yet (#90)
    const builder = new Pipeline<number>({});
    const out: number[] = builder
      // @ts-expect-error - L1: .from() does not exist yet (#90)
      .from([1, 2, 3])
      .buffer(2)
      .onError(() => {})
      .local((p: Pipeline<number>) => p)
      .toArray();

    expect(out).toEqual([1, 2, 3]);
  });
});
