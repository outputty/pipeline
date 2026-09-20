/**
 * reduce-empty.e2e.test.ts - #241's Done-when cases: a reduce that received no data emits its seed,
 * once per `.reduce()` stage, on every class. Each case is a real run of the real class; the HTTP
 * case goes through a real loopback server and the cluster case through a real subprocess fixture.
 *
 * The seed is the fold's identity, so `[]` folds to `0` for a sum exactly as `[].reduce(f, 0)` does.
 * A partition of a partitioned reduce that sees no chunk stays silent: the partition count is a
 * ceiling, so a five-item stream cut into three chunks still returns three values at
 * `maxConcurrency: 4`, never four.
 */
import { describe, test, expect } from "vitest";
import type { IContextManager } from "../src";
import {
  Pipeline,
  ConcurrentPipeline,
  EventEmitterPipeline,
  HttpPipeline,
  Transformer,
} from "../src";
import { FIXTURE_TIMEOUT, HTTP_TIMEOUT, withServer, runFixtureJson } from "./helpers/fixtures";
import { countPromises } from "./helpers/sequences";

const sum = (acc: number, x: number) => acc + x;

/** The bank-at-6 reducer `reduce.e2e.test.ts` uses: emits a running total once it reaches 6. */
function emitAtSix(
  acc: number,
  x: number,
  _ctx: IContextManager,
  emit: (v: number) => void,
): number {
  acc += x;
  if (acc >= 6) {
    emit(acc);
    return 0;
  }
  return acc;
}

async function* nothing(): AsyncGenerator<number> {}

describe("#241 Pipeline.reduce over no data emits its seed (Done-when 1-3)", () => {
  test("[] on the synchronous engine prints [0], a plain array, with no promise created", () => {
    let data: unknown;
    const promises = countPromises(() => {
      data = new Pipeline<number>().reduce(sum, 0)([]).toArray();
    });
    expect(promises).toBe(0);
    expect(Array.isArray(data)).toBe(true);
    expect(data).toEqual([0]);
  });

  test("an empty async iterable prints [0]", async () => {
    expect(await new Pipeline<number>().reduce(sum, 0)(nothing()).toArray()).toEqual([0]);
  });

  test("[1,2,3] after a filter that empties it prints [0]", async () => {
    const data = await new Pipeline<number>()
      .transform((t) => t.filter((x: number) => x > 9))
      .reduce(
        sum,
        0,
      )([1, 2, 3])
      .toArray();
    expect(data).toEqual([0]);
  });

  test("the control [1,2,3] still prints [6]", async () => {
    expect(await new Pipeline<number>().reduce(sum, 0)([1, 2, 3]).toArray()).toEqual([6]);
  });

  test("an async stage that empties every chunk, then a reduce, prints [0]", async () => {
    const data = await new Pipeline<number>()
      .transform((t) => t.map(async (x: number) => x * 2).filter((x: number) => x > 99))
      .reduce(
        sum,
        0,
      )([1, 2, 3])
      .toArray();
    expect(data).toEqual([0]);
  });
});

describe("#241 a partitioned reduce emits the seed once, never per partition (Done-when 4-6)", () => {
  test("ConcurrentPipeline at maxConcurrency 4 over [] prints [0]", async () => {
    const data = await new ConcurrentPipeline<number>({ maxConcurrency: 4 })
      .reduce(
        sum,
        0,
      )([])
      .toArray();
    expect(data).toEqual([0]);
  });

  test("three chunks at maxConcurrency 4 print three values summing to 15, never a fourth", async () => {
    const data = await new ConcurrentPipeline<number>({ maxConcurrency: 4 })
      .buffer(2)
      .reduce(
        sum,
        0,
      )([1, 2, 3, 4, 5])
      .toArray();
    expect(data).toHaveLength(3);
    expect(data.reduce((a, b) => a + b, 0)).toBe(15);
  });

  test("a seed of 100 over [] prints [100], not one 100 per partition", async () => {
    const data = await new ConcurrentPipeline<number>({ maxConcurrency: 4 })
      .reduce(
        sum,
        100,
      )([])
      .toArray();
    expect(data).toEqual([100]);
  });

  test("a mutable seed over [] is a copy each call", async () => {
    const collect = (acc: number[], x: number) => (acc.push(x), acc);
    const seed: number[] = [];
    const pipeline = new ConcurrentPipeline<number>({ maxConcurrency: 4 }).reduce(collect, seed);
    const first = await pipeline([]).toArray();
    const second = await pipeline([]).toArray();
    expect(first).toEqual([[]]);
    expect(first[0]).not.toBe(seed);
    expect(first[0]).not.toBe(second[0]);
  });

  test(
    "HttpPipeline over a real loopback server: [] prints [0], five items print three values",
    async () => {
      const worker = new HttpPipeline<number>({ url: "" }).reduce(sum, 0);
      const { empty, five } = await withServer(
        (request) => worker.fetch(request),
        async (url) => {
          const pipeline = new HttpPipeline<number>({ url, maxConcurrency: 4 })
            .buffer(2)
            .reduce(sum, 0);
          return {
            empty: await pipeline([]).toArray(),
            five: await pipeline([1, 2, 3, 4, 5]).toArray(),
          };
        },
      );
      expect(empty).toEqual([0]);
      expect(five).toHaveLength(3);
      expect(five.reduce((a, b) => a + b, 0)).toBe(15);
    },
    HTTP_TIMEOUT,
  );

  test(
    "ClusterPipeline and ClusterHttpPipeline (subprocess): [] prints [0], five items print three values",
    async () => {
      const result = await runFixtureJson<{
        wsEmpty: number[];
        wsFive: number[];
        httpEmpty: number[];
        httpFive: number[];
      }>("__tests__/fixtures/cluster-empty-reduce.ts");
      expect(result.wsEmpty).toEqual([0]);
      expect(result.httpEmpty).toEqual([0]);
      for (const five of [result.wsFive, result.httpFive]) {
        expect(five).toHaveLength(3);
        expect(five.reduce((a, b) => a + b, 0)).toBe(15);
      }
    },
    FIXTURE_TIMEOUT,
  );

  test("EventEmitterPipeline over [] prints [0]", async () => {
    expect(await new EventEmitterPipeline<number>().reduce(sum, 0)([]).toArray()).toEqual([0]);
  });
});

describe("#241 what the seed does around a reduce (Done-when 7-9, 12)", () => {
  test("a bank-at-6 reducer prints [0] over [] and [6] over [1,2,3]", async () => {
    const pipeline = new Pipeline<number>().reduce(emitAtSix, 0);
    expect(await pipeline([]).toArray()).toEqual([0]);
    expect(await pipeline([1, 2, 3]).toArray()).toEqual([6]);
  });

  test("a stage after the reduce runs once over the empty stream", async () => {
    const data = await new Pipeline<number>()
      .reduce(sum, 0)
      .transform((t) => t.map((n: number) => n * 10))([])
      .toArray();
    expect(data).toEqual([0]);
  });

  test("a branch arm with a reduce and no routed item returns its seed", async () => {
    const data = new Pipeline<number>().branch((b) =>
      b
        .when(
          "big",
          (x) => x > 9,
          (q) => q.reduce(sum, 0),
        )
        .otherwise("rest"),
    )([1, 2, 3]);
    expect(data).toEqual({ big: [0], rest: [1, 2, 3] });
  });

  test(".buffer(fn) over [] yields no chunk and never calls its callback", async () => {
    let calls = 0;
    const chunks: number[][] = [];
    for await (const chunk of new Pipeline<number>()
      .buffer((x: number, _ctx, emit) => {
        calls++;
        emit();
        return x;
      })([])
      .chunks()) {
      chunks.push(chunk);
    }
    expect(chunks).toEqual([]);
    expect(calls).toBe(0);
  });
});

describe("#241 Transformer.reduce emits its seed for a chunk that arrives empty (Done-when 10-11)", () => {
  test("a filter that empties the one chunk, then a reduce, prints [0]", async () => {
    const data = await new Pipeline<number>()
      .transform((t) => t.filter((x: number) => x > 9).reduce(sum, 0))([1, 2, 3])
      .toArray();
    expect(data).toEqual([0]);
  });

  test("at .buffer(1) each emptied chunk adds one seed: [0,2,3]", async () => {
    const data = await new Pipeline<number>()
      .buffer(1)
      .transform((t) => t.filter((x: number) => x > 1).reduce(sum, 0))([1, 2, 3])
      .toArray();
    expect(data).toEqual([0, 2, 3]);
  });

  test("process([[], [1,2]]) yields [0] then [3]", async () => {
    const out: number[][] = [];
    const chunks = (async function* () {
      yield [] as number[];
      yield [1, 2];
    })();
    for await (const chunk of new Transformer<number, number>().reduce(sum, 0).process(chunks)) {
      out.push(chunk);
    }
    expect(out).toEqual([[0], [3]]);
  });

  test("a transform over no chunk at all still prints [], the transformer never runs", async () => {
    const data = await new Pipeline<number>()
      .transform((t) => t.reduce(sum, 0))([])
      .toArray();
    expect(data).toEqual([]);
  });
});
