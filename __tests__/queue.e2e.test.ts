/**
 * queue.e2e.test.ts — ticket #123's own Done-when cases, pinned during L1 as expected-fail:
 * `.queue(capacity)` does not exist on `main` yet, so every call below carries `@ts-expect-error`
 * and the whole chain throws a real `TypeError` at the `.queue()` call itself, which is what makes
 * each case fail today - flipped live, `@ts-expect-error`-free, in L2. Done-when 8 (no file outside
 * `src/pipeline.ts`, `src/utils/cut.ts`, `src/utils/chunk.ts` (barrel export only), `src/types.ts`,
 * and their tests changed) is a structural check, run via `git diff --stat` in the L2/docs PR bodies
 * rather than a runtime case here - see the ticket's own plan comment for the file-scope resolution.
 */
import { describe, it, expect } from "vitest";
import { Pipeline } from "@src/pipeline";
import { ConcurrentPipeline } from "@src/pipelines/concurrent";
import type { Transformer } from "@src/transformer";
import { closingSource, closingAsyncSource } from "./helpers/sequences";

describe("#123 queue(n) prefetches ahead of the consumer (Done-when 1)", () => {
  it.fails("prints [6, 8, 10], unchanged from buffer(2).transform(...) alone", async () => {
    const out = await new Pipeline<number>()
      .buffer(2)
      // @ts-expect-error - .queue() doesn't exist on main yet (#123, flips live in L2)
      .queue(3)
      .transform((t: Transformer<number, number>) =>
        t.map((x: number) => x * 2).filter((x: number) => x > 4),
      )([1, 2, 3, 4, 5])
      .toArray();

    expect(out).toEqual([6, 8, 10]);
  });
});

describe("#123 queue(n) overlaps production and consumption (Done-when 2)", () => {
  it.fails("runs measurably below the fully-serial baseline over the same delays", async () => {
    // `.buffer(1)` before the stage under test (`.claude/rules/typescript.md`): at the default chunk
    // size, 5 items are ONE chunk, so a queue of CHUNKS would have nothing to prefetch and the
    // measurement would show no gap for the wrong reason. Real reference numbers from the ticket's
    // own planning spike, same delays: ~671ms serial, ~539ms queued (19.7% faster).
    const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
    async function* slowSource() {
      for (let i = 1; i <= 5; i++) {
        await delay(100);
        yield i;
      }
    }

    // The queued run first, so an expected-fail L1 throws immediately at `.queue()` and never pays
    // either delay; only L2, once `.queue()` is real, spends the wall-clock this case needs.
    const queuedStart = Date.now();
    await new Pipeline<number>()
      .buffer(1)
      // @ts-expect-error - .queue() doesn't exist on main yet (#123, flips live in L2)
      .queue(3)
      .transform((t: Transformer<number, number>) =>
        t.map(async (x: number) => {
          await delay(30);
          return x;
        }),
      )(slowSource())
      .toArray();
    const queuedMs = Date.now() - queuedStart;

    const serialStart = Date.now();
    await new Pipeline<number>()
      .buffer(1)
      .transform((t) =>
        t.map(async (x: number) => {
          await delay(30);
          return x;
        }),
      )(slowSource())
      .toArray();
    const serialMs = Date.now() - serialStart;

    expect(queuedMs).toBeLessThan(serialMs * 0.9);
  });
});

describe("#123 queue(n) starts pulling only on the first consumer pull (Done-when 3)", () => {
  it.fails(
    "pulls zero items with no terminal call, then exactly capacity on the first pull",
    async () => {
      let pulls = 0;
      async function* countingSource() {
        for (let i = 1; i <= 10; i++) {
          pulls++;
          yield i;
        }
      }

      const chain = new Pipeline<number>()
        .buffer(1)
        // @ts-expect-error - .queue() doesn't exist on main yet (#123, flips live in L2)
        .queue(3);
      const result = chain(countingSource());
      expect(pulls).toBe(0);

      const iterator = result.chunks()[Symbol.asyncIterator]();
      await iterator.next();
      // The array fills to `capacity` on this FIRST pull, not at construction (the ticket's own
      // Constraint) - so exactly 3 items have been pulled from the source, not 1.
      expect(pulls).toBe(3);
    },
  );
});

describe("#123 queue(n) closes the source on an early stop, on both engines (Done-when 4)", () => {
  it.fails("closes a sync generator when .first(1) stops a chain ending in .queue()", async () => {
    const state = { closed: false };
    const chain = new Pipeline<number>()
      .buffer(1)
      // @ts-expect-error - .queue() doesn't exist on main yet (#123, flips live in L2)
      .queue(3);

    expect(await chain(closingSource(state)).first(1)).toEqual([0]);
    expect(state.closed).toBe(true);
  });

  it.fails("closes an async generator on the same chain", async () => {
    const state = { closed: false };
    const chain = new Pipeline<number>()
      .buffer(1)
      // @ts-expect-error - .queue() doesn't exist on main yet (#123, flips live in L2)
      .queue(3);

    expect(await chain(closingAsyncSource(state)).first(1)).toEqual([0]);
    expect(state.closed).toBe(true);
  });
});

describe("#123 queue(n) surfaces a mid-stream throw to the consumer's next pull (Done-when 5)", () => {
  it.fails(
    "rejects the terminal call with the source's own error, consuming every chunk",
    async () => {
      async function* throwingSource() {
        yield 1;
        yield 2;
        throw new Error("boom");
      }
      const chain = new Pipeline<number>()
        .buffer(1)
        // @ts-expect-error - .queue() doesn't exist on main yet (#123, flips live in L2)
        .queue(3);

      await expect(chain(throwingSource()).toArray()).rejects.toThrow("boom");
    },
  );

  it.fails(
    "leaves zero unhandledRejection events when a consumer stops before the throw is pulled",
    async () => {
      // Capacity 3 over a 2-item-then-throw source: the first pull fills the array with 3 pending
      // `upstream.next()` calls, the third one being the throw - `.first(1)` never asks for it, so
      // that rejection is the one case that fires `unhandledRejection` if nothing catches it at push
      // time. The "consume everything" case above doesn't discriminate this from a caught rejection.
      const seen: string[] = [];
      const onUnhandled = (reason: unknown): void => {
        seen.push(reason instanceof Error ? reason.message : String(reason));
      };
      process.on("unhandledRejection", onUnhandled);
      try {
        async function* throwingSource() {
          yield 1;
          yield 2;
          throw new Error("boom");
        }
        const chain = new Pipeline<number>()
          .buffer(1)
          // @ts-expect-error - .queue() doesn't exist on main yet (#123, flips live in L2)
          .queue(3);

        expect(await chain(throwingSource()).first(1)).toEqual([1]);
        await new Promise((resolve) => setImmediate(resolve));
        await new Promise((resolve) => setTimeout(resolve, 20));
      } finally {
        process.removeListener("unhandledRejection", onUnhandled);
      }
      expect(seen).toEqual([]);
    },
  );
});

describe("#123 queue(n) composes with ConcurrentPipeline.reduce()'s own partitioning (Done-when 6)", () => {
  it.fails("returns two correct partition sums, no deadlock or starvation", async () => {
    const sums = await new ConcurrentPipeline<number>({ maxConcurrency: 2 })
      .buffer(2)
      // @ts-expect-error - .queue() doesn't exist on main yet (#123, flips live in L2)
      .queue(3)
      .reduce(
        (acc: number, x: number) => acc + x,
        0,
      )([1, 2, 3, 4, 5, 6, 7, 8])
      .toArray();

    expect(sums).toHaveLength(2);
    expect(sums.reduce((a: number, b: number) => a + b, 0)).toBe(36);
  });
});

describe("#123 queue(n) refuses an invalid capacity, matching buffer()'s own wording (Done-when 7)", () => {
  it.fails("throws on 0, a non-integer, and a negative capacity", () => {
    expect(() =>
      // @ts-expect-error - .queue() doesn't exist on main yet (#123, flips live in L2)
      new Pipeline<number>().queue(0),
    ).toThrow("must be a whole number of at least 1");
    expect(() =>
      // @ts-expect-error - .queue() doesn't exist on main yet (#123, flips live in L2)
      new Pipeline<number>().queue(2.5),
    ).toThrow("must be a whole number of at least 1");
    expect(() =>
      // @ts-expect-error - .queue() doesn't exist on main yet (#123, flips live in L2)
      new Pipeline<number>().queue(-3),
    ).toThrow("must be a whole number of at least 1");
  });
});
