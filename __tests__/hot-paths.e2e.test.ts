/**
 * hot-paths.e2e.test.ts - the behaviours the per-row fast paths must keep: a fold that emits after
 * its first `await`, `.flatMap()`'s exact `.flat()` semantics, `.tap(fn)`'s unhandled-rejection
 * guard, and `.buffer(size)` re-cutting across a pending chunk.
 */
import { describe, it, expect } from "vitest";
import { Pipeline, Transformer, SimpleContextManager, DROP } from "../src";

const T = <I>() => new Transformer<I, I>({ transform: (chunk) => chunk });

async function chunksOf<T>(result: { chunks(): AsyncGenerator<T[]> }): Promise<T[][]> {
  const out: T[][] = [];
  for await (const chunk of result.chunks()) out.push(chunk);
  return out;
}

/** Runs `body` and returns every unhandled rejection it left behind, one event-loop turn later. */
async function unhandledDuring(body: () => void): Promise<unknown[]> {
  const unhandled: unknown[] = [];
  const onUnhandled = (reason: unknown) => unhandled.push(reason);
  process.on("unhandledRejection", onUnhandled);
  try {
    body();
    await new Promise((resolve) => setTimeout(resolve, 10));
  } finally {
    process.off("unhandledRejection", onUnhandled);
  }
  return unhandled;
}

describe("reduce: an async fold that emits after its first await", () => {
  const emitOnEven = async (
    acc: number,
    x: number,
    _ctx: unknown,
    emit: (value: number) => void,
  ): Promise<number> => {
    await Promise.resolve();
    if (x % 2 === 0) {
      emit(acc + x);
      return 0;
    }
    return acc + x;
  };

  it("Pipeline.reduce keeps every late emit, in order, plus the trailing accumulator", async () => {
    const out = await new Pipeline<number>()
      .buffer(2)
      .reduce(
        emitOnEven,
        0,
      )([1, 2, 3, 4, 5])
      .toArray();
    expect(out).toEqual([3, 7, 5]);
  });

  it("Transformer.reduce keeps a late emit made just before the fold throws into a DROP handler", async () => {
    const t = T<number>()
      .onError(() => DROP)
      .reduce(async (acc: number, x: number, _ctx, emit) => {
        await Promise.resolve();
        if (x === 2) {
          emit(99);
          throw new Error("after emit");
        }
        return acc + x;
      }, 0);
    const out = await new Pipeline<number>().apply(t)([1, 2, 3]).toArray();
    expect(out).toEqual([99, 4]);
  });

  it("a late emit never leaks into the next item's fold", async () => {
    const out = await new Pipeline<number>()
      .reduce(async (acc: number, x: number, _ctx, emit) => {
        await Promise.resolve();
        emit(x * 10);
        return acc + x;
      }, 0)([1, 2, 3])
      .toArray();
    expect(out).toEqual([10, 20, 30]);
  });
});

describe("flatMap matches .flat() exactly", () => {
  /** `[1, <hole>, 2]`, built by index so the hole is real. */
  const withHole = (): number[] => {
    const arr = [1];
    arr[2] = 2;
    return arr;
  };
  const holey = (x: number): number[] =>
    x === 1
      ? withHole()
      : x === 2
        ? (7 as unknown as number[])
        : x === 3
          ? []
          : [x, [x] as unknown as number];

  it("skips holes and pushes a non-array result as one item, synchronously", () => {
    const out = new Pipeline<number>()
      .transform((t) => t.flatMap(holey))([1, 2, 3, 4])
      .toArray();
    expect(out).toEqual([1, 2, 7, 4, [4]]);
    expect(out.length).toBe(5);
  });

  it("does the same when some results are pending, keeping chunk order", async () => {
    const out = await new Pipeline<number>()
      .transform((t) =>
        t.flatMap(
          (x: number) => (x % 2 === 0 ? Promise.resolve(holey(x)) : holey(x)) as Promise<number[]>,
        ),
      )([1, 2, 3, 4])
      .toArray();
    expect(out).toEqual([1, 2, 7, 4, [4]]);
  });
});

describe("tap(fn)", () => {
  it("a sync throw after an earlier pending promise leaves no unhandled rejection behind", async () => {
    const throwing = ((x: number) => {
      if (x === 1) return Promise.reject(new Error("slow failure"));
      if (x === 2) throw new Error("fast failure");
      return undefined;
    }) as (item: number) => void;
    const t = T<number>().tap(throwing);
    const unhandled = await unhandledDuring(() => {
      expect(() => t.runnable()([1, 2, 3], new SimpleContextManager())).toThrow("fast failure");
    });
    expect(unhandled).toEqual([]);
  });

  it("waits for every pending callback, and rejects with the one that rejects", async () => {
    const seen: number[] = [];
    const t = T<number>().tap((x: number) =>
      x === 2
        ? new Promise<void>((resolve) => setTimeout(() => (seen.push(x), resolve()), 5))
        : void seen.push(x),
    );
    const chunk = [1, 2, 3];
    const out = await t.runnable()(chunk, new SimpleContextManager());
    expect(out).toBe(chunk);
    expect(seen).toEqual([1, 3, 2]);

    const failing = T<number>().tap(async (x: number) => {
      if (x === 3) throw new Error("tap failed");
    });
    await expect(failing.runnable()([1, 2, 3], new SimpleContextManager())).rejects.toThrow(
      "tap failed",
    );
  });
});

describe("buffer(size) after a pending chunk", () => {
  const slowAfter3 = (x: number) => (x > 3 ? Promise.resolve(x) : x) as Promise<number>;

  it("carries a settled remainder across the first pending chunk", async () => {
    const p = new Pipeline<number>()
      .buffer(3)
      .transform((t) => t.map(slowAfter3))
      .buffer(2);
    const input = [1, 2, 3, 4, 5, 6, 7];
    expect(await chunksOf(p(input))).toEqual([[1, 2], [3, 4], [5, 6], [7]]);
    expect(await p(input).toArray()).toEqual(input);
  });

  it("ends exactly on a boundary with no extra chunk", async () => {
    const p = new Pipeline<number>()
      .buffer(4)
      .transform((t) => t.map(async (x: number) => x))
      .buffer(2);
    expect(await chunksOf(p([1, 2, 3, 4, 5, 6, 7, 8]))).toEqual([
      [1, 2],
      [3, 4],
      [5, 6],
      [7, 8],
    ]);
  });

  it("joins remainders across several small pending chunks", async () => {
    const p = new Pipeline<number>()
      .buffer(1)
      .transform((t) => t.map(async (x: number) => x))
      .buffer(3);
    expect(await chunksOf(p([1, 2, 3, 4, 5, 6, 7]))).toEqual([[1, 2, 3], [4, 5, 6], [7]]);
  });
});
