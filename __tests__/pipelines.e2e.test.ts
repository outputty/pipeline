/**
 * pipelines.e2e.test.ts — #17's Done-when cases, each proven through a REAL run: a real HTTP
 * server on loopback, a real `node:cluster` worker process, a real `execFile`'d script. No mocks.
 *
 * Cluster cases (1-4, 14-cluster) run as SUBPROCESS FIXTURES (`__tests__/fixtures/*.ts`), spawned
 * via `execFile(process.execPath, ["--import", "tsx", path])` - `cluster.fork()` re-execs
 * `process.argv[1]`, which inside a Vitest worker is Vitest's OWN entry, so a `ClusterPipeline`
 * built in-process here would fork Vitest itself. HTTP and in-process cases run directly.
 *
 * Every case that names a class or behavior #17 has not yet built is `it.fails` - a case that
 * already holds today (the shipped `concurrent()` control) is a normal, passing `it`. As each
 * layer lands, its cases flip from `it.fails` to `it`.
 */
import { describe, it, expect } from "vitest";
import { Pipeline, Transformer, ConcurrentPipeline, HttpPipeline, ClusterPipeline } from "../src";
import {
  FIXTURE_TIMEOUT,
  HTTP_TIMEOUT,
  runFixture,
  withServer,
  expectFixtureOk,
  lastJsonLine,
} from "./helpers/fixtures";

/** The "another instance" side of an `HttpPipeline` chain: an empty-source pipeline whose only
 * job is to hold the SAME stage definitions `builder` describes, so its `.fetch` can serve them. */
function makeWorker<U>(builder: (t: HttpPipeline<number>) => HttpPipeline<U>): HttpPipeline<U> {
  return builder(new HttpPipeline<number>([], { url: "" }));
}

describe("#17 ClusterPipeline canonical program (Done-when 1, 3)", () => {
  it(
    "prints [6,8,10] with no server/listen/fork/url in caller code, and exits on its own",
    async () => {
      const result = await runFixture("__tests__/fixtures/cluster-basic.ts");
      expect(result.stderr).toBe("");
      expectFixtureOk(result); // Done-when 3: exits cleanly on its own, no explicit teardown
      // Done-when 1: the canonical result, the LAST line - every worker also re-executes the
      // entry module and prints ITS OWN empty placeholder first (architecture.md's own documented
      // constraint: "a violation shows as duplicated output, never an error").
      const lines = result.stdout.trim().split("\n");
      expect(lines.at(-1)).toBe("[6,8,10]");
    },
    FIXTURE_TIMEOUT,
  );
});

describe("#17 ClusterPipeline dispatches to real worker processes (Done-when 2)", () => {
  it(
    "distinct process.pid values serve stage 0, count matches workers",
    async () => {
      const fixture = await runFixture("__tests__/fixtures/cluster-pids.ts");
      expectFixtureOk(fixture);
      const result = lastJsonLine<{ distinctPids: number; workers: number }>(fixture);
      expect(result.distinctPids).toBe(result.workers);
    },
    FIXTURE_TIMEOUT,
  );
});

describe("#17 three ClusterPipelines share one port and worker set (Done-when 4)", () => {
  it(
    "every pipeline's url is identical",
    async () => {
      const fixture = await runFixture("__tests__/fixtures/cluster-shared-port.ts");
      expectFixtureOk(fixture);
      const result = lastJsonLine<{ ports: string[]; results: number[][] }>(fixture);
      expect(new Set(result.ports).size).toBe(1);
      expect(result.results).toEqual([[1], [2], [3]]);
    },
    FIXTURE_TIMEOUT,
  );
});

describe("#17 HttpPipeline across two real instances (Done-when 5)", () => {
  it(
    "dispatches over a real loopback HTTP request and prints [6,8,10]",
    async () => {
      const worker = makeWorker((t) =>
        t.transform((tr) => tr.map((x: number) => x * 2).filter((x: number) => x > 4)),
      );
      await withServer(worker.fetch, async (url) => {
        const out = await new HttpPipeline<number>([1, 2, 3, 4, 5], { url })
          .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 4))
          .toArray();
        expect(out).toEqual([6, 8, 10]);
      });
    },
    HTTP_TIMEOUT,
  );
});

describe("#61 .local(build) prints the ticket's own canonical program on every class (Done-when 1-3)", () => {
  it("ConcurrentPipeline.buffer(2).local((p) => p.reduce(...)).toArray() prints [15]", async () => {
    const out = await new ConcurrentPipeline([1, 2, 3, 4, 5], { maxConcurrency: 2 })
      .buffer(2)
      .local((p) => p.reduce((a: number, x: number) => a + x, 0))
      .toArray();
    expect(out).toEqual([15]);
  });

  it("the same chain over a base Pipeline prints [15]", async () => {
    const out = await new Pipeline([1, 2, 3, 4, 5])
      .buffer(2)
      .local((p) => p.reduce((a: number, x: number) => a + x, 0))
      .toArray();
    expect(out).toEqual([15]);
  });

  it("the same chain over an HttpPipeline prints [15], with no HTTP request ever sent", async () => {
    let requests = 0;
    const worker = makeWorker((t) => t);
    const countingHandler = async (request: Request): Promise<Response> => {
      requests++;
      return worker.fetch(request);
    };
    await withServer(countingHandler, async (url) => {
      const out = await new HttpPipeline<number>([1, 2, 3, 4, 5], { url, maxConcurrency: 2 })
        .buffer(2)
        .local((p) => p.reduce((a: number, x: number) => a + x, 0))
        .toArray();
      expect(out).toEqual([15]);
      expect(requests).toBe(0); // a region with no dispatched stage never crosses the wire
    });
  });

  it(
    "the same chain over a real ClusterPipeline prints [15], folded in the primary process",
    async () => {
      const fixture = await runFixture("__tests__/fixtures/cluster-local.ts");
      expectFixtureOk(fixture);
      const result = lastJsonLine<{ sum: number; stayedInPrimary: boolean }>(fixture);
      expect(result.sum).toBe(15);
      expect(result.stayedInPrimary).toBe(true);
    },
    FIXTURE_TIMEOUT,
  );

  it("a multi-stage region runs entirely in the orchestrating process and prints [30]", async () => {
    const out = await new ConcurrentPipeline([1, 2, 3, 4, 5], { maxConcurrency: 2 })
      .local((p) =>
        p
          .transform((t) => t.map((x: number) => x * 2))
          .reduce((acc: number, x: number) => acc + x, 0),
      )
      .toArray();
    expect(out).toEqual([30]);
  });
});

describe("#61 .local(build) keeps a whole region in-process, replacing { local: true }", () => {
  it(
    "makes ZERO HTTP requests for the local region",
    async () => {
      let requests = 0;
      const worker = makeWorker((t) => t.transform((tr) => tr.map((x: number) => x * 2)));
      const countingHandler = async (request: Request): Promise<Response> => {
        requests++;
        return worker.fetch(request);
      };
      await withServer(countingHandler, async (url) => {
        const out = await new HttpPipeline<number>([1, 2, 3], { url })
          .transform((t) => t.map((x: number) => x * 2))
          .local((p) => p.transform((t) => t.filter((x: number) => x > 2)))
          .toArray();
        expect(out).toEqual([4, 6]);
        expect(requests).toBe(1); // only the first (dispatched) stage crossed the wire
      });
    },
    HTTP_TIMEOUT,
  );

  it("{ local: true } no longer typechecks as a second argument to .transform() on any class - compile error", () => {
    // Type-only: never executed. `tsc --noEmit` is the real assertion; `@ts-expect-error` itself
    // fails (TS2578) if the call ever stopped erroring - the negative-case pattern
    // `.claude/rules/typescript.md` calls for over trusting a "should fail" claim.
    function typeOnlyCheck() {
      const plain = new Pipeline<number>([1]);
      // @ts-expect-error - .transform() takes no second argument on any Pipeline class now (#61)
      plain.transform((t) => t, { local: true });
      const concurrent = new ConcurrentPipeline<number>([1]);
      // @ts-expect-error - the dispatching classes lost the same second argument (#61, BREAKING)
      concurrent.transform((t) => t, { local: true });
    }
    expect(typeof typeOnlyCheck).toBe("function");
  });
});

describe("#17 .constructor.name is the leaf class after two .transform() calls (Done-when 7)", () => {
  it("a plain Pipeline stays Pipeline", () => {
    const p = new Pipeline([1])
      .transform((t) => t.map((x: number) => x))
      .transform((t) => t.map((x: number) => x));
    expect(p.constructor.name).toBe("Pipeline");
  });

  it("a ConcurrentPipeline stays ConcurrentPipeline", () => {
    const p = new ConcurrentPipeline([1])
      .transform((t) => t.map((x: number) => x))
      .transform((t) => t.map((x: number) => x));
    expect(p.constructor.name).toBe("ConcurrentPipeline");
  });

  it("an HttpPipeline stays HttpPipeline", () => {
    const p = new HttpPipeline([1], { url: "http://localhost:1" })
      .transform((t) => t.map((x: number) => x))
      .transform((t) => t.map((x: number) => x));
    expect(p.constructor.name).toBe("HttpPipeline");
  });

  it("a ClusterPipeline stays ClusterPipeline (constructed only - never drained, never forks)", () => {
    const p = new ClusterPipeline([1])
      .transform((t) => t.map((x: number) => x))
      .transform((t) => t.map((x: number) => x));
    expect(p.constructor.name).toBe("ClusterPipeline");
  });
});

describe("#17 .context()/.buffer() carry a subclass's own knobs forward (createPipeline)", () => {
  // Regression: review found ConcurrentPipeline/HttpPipeline dropping maxConcurrency/url on
  // .context() - Pipeline.createPipeline()'s base implementation only forwards PipelineOptions
  // fields, so a subclass with EXTRA constructor knobs must override it, which these two now do.
  it("ConcurrentPipeline keeps maxConcurrency/ordered through .context()", () => {
    const p = new ConcurrentPipeline([1], { maxConcurrency: 8, ordered: false }).context({ k: 1 });
    expect(p.constructor.name).toBe("ConcurrentPipeline");
    expect(p.maxConcurrency).toBe(8);
    expect(p.ordered).toBe(false);
    expect(p.contextManager.toDict()).toEqual({ k: 1 });
  });

  it("HttpPipeline keeps url through .context()", () => {
    const p = new HttpPipeline([1], { url: "http://example.test" }).context({ k: 1 });
    expect(p.constructor.name).toBe("HttpPipeline");
    expect(p.url).toBe("http://example.test");
  });

  it("ClusterPipeline keeps workers through .context()", () => {
    const p = new ClusterPipeline([1], { workers: 3 }).context({ k: 1 });
    expect(p.constructor.name).toBe("ClusterPipeline");
    expect(p.workers).toBe(3);
  });

  it("a dispatched stage's own output survives .buffer(), async-iterable like any other chunk stream (#39)", async () => {
    // A dispatched stage's own output IS a real `_chunks` boundary now (#39) - there is no
    // separate "source position" mechanism left to lose track of it after `.buffer()` recuts.
    const p = new ConcurrentPipeline([1, 2, 3])
      .transform((t) => t.map((x: number) => x * 2))
      .buffer(10);
    const chunks: number[][] = [];
    for await (const chunk of p) {
      chunks.push(chunk);
    }
    expect(chunks.flat()).toEqual([2, 4, 6]);
  });
});

describe("#17 a knob that only takes effect via Transformer.process() fails loud, not silent", () => {
  // Regression: stageWork() never calls process() on ANY consumption path (not just async
  // iteration), so .withHooks() on a dispatched stage used to run with the hook silently never
  // firing - no error, no warning. ConcurrentPipeline.apply() now throws instead. .onError() left
  // this refusal in #40 - it reports a dispatched stage's failing chunk directly (below).
  it("rejects .withHooks() on a non-local stage", () => {
    const hooked = new Transformer<number, number>()
      .map((x: number) => x * 2)
      .withHooks({ onStart: () => {} });
    expect(() => new ConcurrentPipeline([1, 2, 3]).apply(hooked)).toThrow(
      /withHooks never take effect on a dispatched stage/,
    );
  });

  it(".local(build) still runs a hooked stage in-process, hooks intact", async () => {
    const order: string[] = [];
    const hooked = new Transformer<number, number>()
      .map((x: number) => x * 2)
      .withHooks({ onStart: () => order.push("start"), onComplete: () => order.push("complete") });
    const out = await new ConcurrentPipeline([1, 2, 3]).local((p) => p.apply(hooked)).toArray();
    expect(out).toEqual([2, 4, 6]);
    expect(order).toEqual(["start", "complete"]);
  });
});

describe("#40 .onError() receives the chunk that actually failed", () => {
  // Real run from planning: chunkSize 2 over [1,2,3,4], throwing on 3 - the failing chunk is
  // [3,4], never the whole source and never [] (Done-when 1, 2).
  const throwOn3 = (t: Transformer<number, number>) =>
    t.map((x: number) => {
      if (x === 3) throw new Error("boom on 3");
      return x;
    });

  it("Pipeline: handler sees chunk [3,4], the run still rejects with the original error", async () => {
    const seen: number[][] = [];
    const transformer = throwOn3(new Transformer<number, number>()).onError((chunk) => {
      seen.push(chunk);
    });
    await expect(new Pipeline([1, 2, 3, 4]).buffer(2).apply(transformer).toArray()).rejects.toThrow(
      "boom on 3",
    );
    expect(seen).toEqual([[3, 4]]);
  });

  it("ConcurrentPipeline: handler sees chunk [3,4], the run still rejects with the original error", async () => {
    const seen: number[][] = [];
    const transformer = throwOn3(new Transformer<number, number>()).onError((chunk) => {
      seen.push(chunk);
    });
    await expect(
      new ConcurrentPipeline([1, 2, 3, 4]).buffer(2).apply(transformer).toArray(),
    ).rejects.toThrow("boom on 3");
    expect(seen).toEqual([[3, 4]]);
  });

  it(
    "HttpPipeline: handler sees chunk [3,4]; the rejection wraps the original error with the stage and url",
    async () => {
      const worker = makeWorker((t) => t.transform((tr) => throwOn3(tr)));
      const seen: number[][] = [];
      const transformer = throwOn3(new Transformer<number, number>()).onError((chunk) => {
        seen.push(chunk);
      });
      await withServer(worker.fetch, async (url) => {
        const orchestrator = new HttpPipeline<number>([1, 2, 3, 4], { url })
          .buffer(2)
          .apply(transformer);
        await expect(orchestrator.toArray()).rejects.toThrow(/stage 0.*failed: boom on 3/);
      });
      expect(seen).toEqual([[3, 4]]);
    },
    HTTP_TIMEOUT,
  );

  it('several handlers still run LIFO on Pipeline: ["second","first"]', async () => {
    const calls: string[] = [];
    const transformer = throwOn3(new Transformer<number, number>())
      .onError(() => calls.push("first"))
      .onError(() => calls.push("second"));
    await expect(
      new Pipeline([1, 2, 3, 4]).buffer(2).apply(transformer).toArray(),
    ).rejects.toThrow();
    expect(calls).toEqual(["second", "first"]);
  });

  it('several handlers still run LIFO on ConcurrentPipeline: ["second","first"]', async () => {
    const calls: string[] = [];
    const transformer = throwOn3(new Transformer<number, number>())
      .onError(() => calls.push("first"))
      .onError(() => calls.push("second"));
    await expect(
      new ConcurrentPipeline([1, 2, 3, 4]).buffer(2).apply(transformer).toArray(),
    ).rejects.toThrow();
    expect(calls).toEqual(["second", "first"]);
  });

  it("ConcurrentPipeline.apply() no longer refuses a stage carrying .onError()", () => {
    const withHandler = new Transformer<number, number>()
      .map((x: number) => x * 2)
      .onError(() => {});
    expect(() => new ConcurrentPipeline([1, 2, 3]).apply(withHandler)).not.toThrow();
  });
});

describe("#17 ConcurrentPipeline validates maxConcurrency eagerly", () => {
  // Regression: the deleted concurrent() strategy threw "maxConcurrency must be at least 1"
  // eagerly; ConcurrentPipeline's own constructor dropped that check, so maxConcurrency <= 0 made
  // fanOutUnordered's ramp-up loop never run at all - silently [] instead of an error.
  it("throws on a non-positive maxConcurrency", () => {
    expect(() => new ConcurrentPipeline([1], { maxConcurrency: 0 })).toThrow(
      "maxConcurrency must be at least 1",
    );
    expect(() => new ConcurrentPipeline([1], { maxConcurrency: -3 })).toThrow(
      "maxConcurrency must be at least 1",
    );
  });

  it("accepts the default and a positive value", () => {
    expect(() => new ConcurrentPipeline([1])).not.toThrow();
    expect(() => new ConcurrentPipeline([1], { maxConcurrency: 8 })).not.toThrow();
  });
});

describe("#17 a chunk failure never leaks an unhandled rejection (Done-when 8)", () => {
  // The shipped-concurrent() control (proving the OLD strategy leaked, on the identical chain)
  // lived here through L3 - deleted now that concurrent() itself is gone with the seam, per the
  // ticket's own Done-when 8: "after removal only the [] assertion remains".
  it(
    "ConcurrentPipeline leaves UNHANDLED [] at both ordered: true and ordered: false",
    async () => {
      const fixture = await runFixture("__tests__/fixtures/concurrent-unhandled.ts");
      expectFixtureOk(fixture);
      const result = JSON.parse(fixture.stdout.trim()) as {
        ordered: string[];
        unordered: string[];
      };
      expect(result.ordered).toEqual([]);
      expect(result.unordered).toEqual([]);
    },
    FIXTURE_TIMEOUT,
  );
});

describe("#17 ordered: false streams instead of draining the source first (Done-when 9)", () => {
  it("dispatches before the whole source has been pulled", async () => {
    const pulled: number[] = [];
    async function* source() {
      for (const x of [1, 2, 3, 4, 5, 6]) {
        pulled.push(x);
        yield x;
      }
    }
    const cp = new ConcurrentPipeline<number>(source(), {
      maxConcurrency: 2,
      ordered: false,
    });
    await cp
      .buffer(1) // one item per chunk, so "not drained first" is actually observable
      .transform((t) =>
        t.map(async (x: number) => {
          // A real, if crude, snapshot: once dispatch for x=1 begins, the source must not have
          // already been pulled to the end - that is what "not drained first" means here.
          if (x === 1) expect(pulled.length).toBeLessThan(6);
          return x;
        }),
      )
      .toArray();
  });
});

describe("#17 ordered: true restores source order under a slow first chunk (Done-when 10)", () => {
  it("chunk 0 made 12x slower still comes out first", async () => {
    const delay = (ms: number) => new Promise((r) => setTimeout(r, ms));
    const cp = new ConcurrentPipeline<number>([1, 2, 3, 4], {
      maxConcurrency: 4,
      ordered: true,
    });
    const out = await cp
      .buffer(1)
      .transform((t) =>
        t.map(async (x: number) => {
          await delay(x === 1 ? 120 : 10);
          return x;
        }),
      )
      .toArray();
    expect(out).toEqual([1, 2, 3, 4]);
  });
});

describe("#17 async map/filter results are awaited (Done-when 11)", () => {
  it("prints [4,6], not []", async () => {
    const out = await new Pipeline([1, 2, 3])
      .transform((t) => t.map(async (x: number) => x * 2).filter((x) => x > 2))
      .toArray();
    expect(out).toEqual([4, 6]);
  });
});

describe("#17 the same async chain over HttpPipeline (Done-when 12)", () => {
  it(
    "prints [4,6] over the wire, never [{},{},{}]",
    async () => {
      const worker = makeWorker((t) =>
        t.transform((tr) => tr.map(async (x: number) => x * 2).filter((x) => x > 2)),
      );
      await withServer(worker.fetch, async (url) => {
        const out = await new HttpPipeline<number>([1, 2, 3], { url })
          .transform((t) => t.map(async (x: number) => x * 2).filter((x) => x > 2))
          .toArray();
        expect(out).toEqual([4, 6]);
      });
    },
    HTTP_TIMEOUT,
  );
});

describe("#17 an unknown stage index 404s (Done-when 13)", () => {
  it(
    'returns 404 {"error":"unknown stage 99; this deployment serves 0..1"}',
    async () => {
      const worker = makeWorker((t) =>
        t.transform((tr) => tr.map((x: number) => x)).transform((tr) => tr.map((x: number) => x)),
      );
      const res = await worker.fetch(
        new Request("http://x/stage/99", {
          method: "POST",
          body: JSON.stringify({ chunk: [1], context: {} }),
        }),
      );
      expect(res.status).toBe(404);
      expect(await res.json()).toEqual({ error: "unknown stage 99; this deployment serves 0..1" });
    },
    HTTP_TIMEOUT,
  );
});

describe("#17 .fetch() fails loud on a malformed request, never hangs the client", () => {
  // Regression: review found request.json() called outside any try/catch, inside toNodeHandler's
  // un-caught async IIFE - a bodyless or malformed POST rejected uncaught, leaving the client
  // hanging with no response until its own timeout, rather than a fast, real 400.
  const worker = makeWorker((t) => t.transform((tr) => tr.map((x: number) => x * 2)));

  it("400s on a missing body", async () => {
    const res = await worker.fetch(new Request("http://x/stage/0", { method: "POST" }));
    expect(res.status).toBe(400);
    expect(await res.json()).toEqual({ error: "request body is not valid JSON" });
  });

  it("400s on malformed JSON", async () => {
    const res = await worker.fetch(
      new Request("http://x/stage/0", { method: "POST", body: "not json" }),
    );
    expect(res.status).toBe(400);
    expect(await res.json()).toEqual({ error: "request body is not valid JSON" });
  });

  it("400s when the 'context' field is missing", async () => {
    const res = await worker.fetch(
      new Request("http://x/stage/0", { method: "POST", body: JSON.stringify({ chunk: [1] }) }),
    );
    expect(res.status).toBe(400);
    expect(await res.json()).toEqual({ error: "request body is missing a 'context' object" });
  });

  it("400s when 'context' is an array, not an object", async () => {
    // Regression: review found `typeof [] === "object"` let an array slip past the "is it an
    // object" check, silently becoming a string-indexed context ({"0":1,"1":2}) instead of
    // failing loud, inconsistent with the stricter Array.isArray check already used for 'chunk'.
    const res = await worker.fetch(
      new Request("http://x/stage/0", {
        method: "POST",
        body: JSON.stringify({ chunk: [1], context: [1, 2, 3] }),
      }),
    );
    expect(res.status).toBe(400);
    expect(await res.json()).toEqual({ error: "request body is missing a 'context' object" });
  });

  it("toNodeHandler's bridge itself never hangs, even for a handler that throws", async () => {
    const throwingHandler = (_request: Request): Promise<Response> => {
      throw new Error("handler blew up");
    };
    await withServer(throwingHandler, async (url) => {
      const res = await fetch(url, { method: "POST" });
      expect(res.status).toBe(500);
    });
  });
});

describe("#17 .context() propagates through the wire (Done-when 14)", () => {
  it(
    "prints [10,20,30,40,50] through HttpPipeline",
    async () => {
      const worker = makeWorker((t) =>
        t.transform((tr) => tr.map((x: number, ctx) => x * (ctx.get("multiplier") as number))),
      );
      await withServer(worker.fetch, async (url) => {
        const out = await new HttpPipeline<number>([1, 2, 3, 4, 5], { url })
          .context({ multiplier: 10 })
          .transform((t) => t.map((x: number, ctx) => x * (ctx.get("multiplier") as number)))
          .toArray();
        expect(out).toEqual([10, 20, 30, 40, 50]);
      });
    },
    HTTP_TIMEOUT,
  );

  it(
    "prints [10,20,30,40,50] through ClusterPipeline, still a ClusterPipeline after .context()",
    async () => {
      const fixture = await runFixture("__tests__/fixtures/cluster-context.ts");
      expectFixtureOk(fixture);
      const result = lastJsonLine<{ out: number[]; ctorNameAfterContext: string }>(fixture);
      expect(result.out).toEqual([10, 20, 30, 40, 50]);
      expect(result.ctorNameAfterContext).toBe("ClusterPipeline");
    },
    FIXTURE_TIMEOUT,
  );
});

describe("#17 a stage's HTTP 500 throws from the terminal op, never reaches .catch() (Done-when 15)", () => {
  it(
    "rejects naming the stage index and url, and the .catch() handler never runs",
    async () => {
      const worker = makeWorker((t) =>
        t.transform((tr) =>
          tr.map((_x: number) => {
            throw new Error("boom");
          }),
        ),
      );
      const onError = () => {
        throw new Error("must not be reached");
      };
      await withServer(worker.fetch, async (url) => {
        const orchestrator = new HttpPipeline<number>([1, 2, 3], { url }).transform((t) =>
          t.catch(
            (sub) => sub.map((x: number) => x),
            () => {
              onError();
              return undefined;
            },
          ),
        );
        const escapedUrl = url.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
        await expect(orchestrator.toArray()).rejects.toThrow(new RegExp(`stage 0.*${escapedUrl}`));
      });
    },
    HTTP_TIMEOUT,
  );
});

describe("#31 a ClusterPipeline worker builds the caller's own class via contextFactory (Done-when 5)", () => {
  it(
    "each worker builds its own PoolContext once, and forward context still crosses the wire",
    async () => {
      const fixture = await runFixture("__tests__/fixtures/cluster-context-factory.ts");
      expectFixtureOk(fixture);
      const result = lastJsonLine<{
        orchestratorCtxClass: string;
        ctxBuiltInOrchestrator: boolean;
        workerCtxClasses: string[];
        distinctWorkerCtxPids: number;
        ctxBuiltInSamePidAsServer: boolean;
        multiplierCrossedWire: number[];
      }>(fixture);
      // The orchestrator was given an already-built instance (`context`), so `contextFactory`
      // never ran there - only every OTHER process (each worker) needed to build its own.
      expect(result.orchestratorCtxClass).toBe("PoolContext");
      expect(result.ctxBuiltInOrchestrator).toBe(false);
      expect(result.workerCtxClasses).toEqual(["PoolContext"]);
      expect(result.distinctWorkerCtxPids).toBe(3);
      expect(result.ctxBuiltInSamePidAsServer).toBe(true);
      expect(result.multiplierCrossedWire).toEqual(Array.from({ length: 30 }, (_, i) => i * 10));
    },
    FIXTURE_TIMEOUT,
  );
});

describe("#31 contextFactory is invoked once per process, not per request (Done-when 6)", () => {
  it(
    "primary builds once for its own _context; each worker builds once and reuses it to serve",
    async () => {
      const fixture = await runFixture("__tests__/fixtures/cluster-context-factory-invocations.ts");
      expectFixtureOk(fixture);
      const result = lastJsonLine<{
        primaryBuilt: number;
        chunks: number;
        maxBuiltPerWorkerPid: number[];
        workerPids: number;
      }>(fixture);
      expect(result).toEqual({
        primaryBuilt: 1,
        chunks: 20,
        maxBuiltPerWorkerPid: [1, 1],
        workerPids: 2,
      });
    },
    FIXTURE_TIMEOUT,
  );
});
