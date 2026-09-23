/**
 * A pipeline records its stages and compiles them into a plan; a call runs the plan over its own
 * streams (#253). These cases pin what the recorder must keep from the replaying chain it
 * replaced: `.local()` regions, stage numbering, cluster slots and fused in-process stages.
 */

import { describe, it, expect } from "vitest";

import { Pipeline } from "@src/pipeline";
import { ConcurrentPipeline } from "@src/pipelines/concurrent";
import { HttpPipeline } from "@src/pipelines/http";
import { ClusterHttpPipeline } from "@src/pipelines/cluster";
import { SimpleContextManager } from "@src/context/simple";

/** An `HttpPipeline` over `chain` whose client calls a worker's `.fetch` in this process, plus the
 * paths it requested. */
function inProcess(chain: Pipeline<number, any, number>) {
  const worker = new HttpPipeline(chain, { url: "" });
  const paths: string[] = [];
  const caller = new HttpPipeline(chain, {
    url: "http://worker",
    maxConcurrency: 1,
    client: (url, init) => {
      paths.push(new URL(url).pathname);
      return worker.fetch(new Request(url, init));
    },
  });
  return { worker, caller, paths };
}

function post(worker: HttpPipeline<number, number>, path: string): Promise<Response> {
  return worker.fetch(
    new Request(`http://x${path}`, {
      method: "POST",
      body: JSON.stringify({ chunk: [1, 2], context: {} }),
    }),
  );
}

describe(".local() regions", () => {
  it("numbers a stage after a region past every stage the region added", async () => {
    const chain = new Pipeline<number>()
      .buffer(2)
      .transform((t) => t.map((x) => x + 1))
      .local((p) =>
        p.transform((t) => t.map((x: number) => x * 10)).reduce((a: number, x: number) => a + x, 0),
      )
      .transform((t) => t.map((x) => x - 1));

    const { caller, paths } = inProcess(chain);
    expect(await caller([1, 2, 3, 4]).toArray()).toEqual([139]);
    expect([...new Set(paths)]).toEqual(["/transform/0", "/transform/3"]);
  });

  it("runs a region's build once for the worker's stage table, not once per request", async () => {
    let builds = 0;
    const chain = new Pipeline<number>().local((p) => {
      builds++;
      return p.transform((t) => t.map((x: number) => x * 2));
    });
    const worker = new HttpPipeline(chain, { url: "" });
    expect(builds).toBe(0);

    expect(await (await post(worker, "/transform/0")).json()).toEqual({ chunk: [2, 4] });
    expect(await (await post(worker, "/transform/0")).json()).toEqual({ chunk: [2, 4] });
    expect(builds).toBe(1);
  });

  it("refuses to call or wrap the pipeline a region's build receives", () => {
    const calling = new Pipeline<number>().local((p) => {
      (p as unknown as (input: number[]) => unknown)([1]);
      return p;
    });
    expect(() => calling([1]).toArray()).toThrow(
      "cannot call a pipeline that is already bound to a source",
    );

    const wrapping = new Pipeline<number>().local((p) => {
      new ConcurrentPipeline(p);
      return p;
    });
    expect(() => wrapping([1]).toArray()).toThrow(
      "cannot wrap a pipeline that is already bound to a source - wrap the unbound chain instead",
    );
  });

  it("keeps a run handler registered inside a region for the stages after it", () => {
    const dropped: string[] = [];
    const chain = new Pipeline<number>()
      .local((p) => p.onError((error) => void dropped.push(error.message)))
      .buffer(1)
      .transform((t) =>
        t.map((x) => {
          if (x === 3) throw new Error("three");
          return x;
        }),
      );

    expect(chain([1, 2, 3, 4]).toArray()).toEqual([1, 2, 4]);
    expect(dropped).toEqual(["three"]);
  });

  it("writes a region's .context() into the run's manager, which a later stage reads", () => {
    const supplied = new SimpleContextManager();
    const build = (p: Pipeline<number, any, any>) =>
      p
        .local((q) => {
          q.context({ offset: 3 });
          return q;
        })
        .transform((t) => t.map((x: number, ctx) => x + (ctx.get("offset") as number)));

    expect(build(new Pipeline<number>({ context: supplied }))([1, 2]).toArray()).toEqual([4, 5]);
    expect(supplied.toDict()).toEqual({ offset: 3 });

    const byDefault = build(new Pipeline<number>());
    expect(byDefault([1, 2]).toArray()).toEqual([4, 5]);
    expect(byDefault.contextManager.toDict()).toEqual({});
  });

  it("yields no items and serves no stage when build returns a pipeline it did not derive", async () => {
    const chain = new Pipeline<number>()
      .transform((t) => t.map((x) => x + 1))
      .local(() => new Pipeline<number>().transform((t) => t.map((x) => x * 7)))
      .transform((t) => t.map((x) => x - 1));

    const out = chain([1, 2, 3]).toArray();
    expect(out).toBeInstanceOf(Promise);
    expect(await out).toEqual([]);

    const response = await post(new HttpPipeline(chain, { url: "" }), "/transform/0");
    expect(response.status).toBe(404);
    expect(await response.json()).toEqual({
      error: "unknown stage 0; this deployment serves 0..-1",
    });
  });
});

describe("cluster slots", () => {
  it("claims one slot per stage call on the primary, and none for a call or a stage table", () => {
    const first = new ClusterHttpPipeline(new Pipeline<number>());
    const steps = [
      first.transform((t) => t.map((x: number) => x)),
      first.tap(() => {}),
      first.context({ k: 1 }),
      first.buffer(2),
      first.onError(() => {}),
      first.reduce((a: number, x: number) => a + x, 0),
      first.queue(2),
      first.local((p) => p),
    ];
    expect(steps.map((step) => step.pipelineIndex - first.pipelineIndex)).toEqual([
      1, 2, 3, 4, 5, 6, 7, 8,
    ]);

    const last = steps.at(-1)!;
    void last([1, 2, 3]);
    (last as unknown as { registries(): unknown }).registries();
    expect(new ClusterHttpPipeline(new Pipeline<number>()).pipelineIndex).toBe(
      last.pipelineIndex + 1,
    );
  });
});

describe("adjacent in-process stages", () => {
  it("close a sync source that a terminal stopped reading early", () => {
    let closed = false;
    let pulled = 0;
    function* source(): Generator<number> {
      try {
        for (let i = 1; i <= 50; i++) {
          pulled++;
          yield i;
        }
      } finally {
        closed = true;
      }
    }
    const out = new Pipeline<number>()
      .buffer(1)
      .transform((t) => t.map((x) => x + 1))
      .transform((t) => t.filter((x) => x % 2 === 0))
      .transform((t) => t.map((x) => x * 10))(source())
      .first(1);

    expect(out).toEqual([20]);
    expect(pulled).toBe(1);
    expect(closed).toBe(true);
  });

  it("each keep the run handler that was in force where they were added", () => {
    const failing = (at: number) => (x: number) => {
      if (x === at) throw new Error(`fail ${at}`);
      return x;
    };
    const dropped: string[] = [];
    const chain = new Pipeline<number>()
      .buffer(1)
      .transform((t) => t.map(failing(9)))
      .onError((error) => void dropped.push(error.message))
      .transform((t) => t.map(failing(2)));

    expect(chain([1, 2, 3]).toArray()).toEqual([1, 3]);
    expect(dropped).toEqual(["fail 2"]);
    expect(() => chain([1, 9, 3]).toArray()).toThrow("fail 9");
  });
});
