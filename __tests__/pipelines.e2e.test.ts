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
 * already holds today (the shipped `concurrent()` control, the `{local:true}` type error on a
 * plain `Pipeline`) is a normal, passing `it`. As each layer lands, its cases flip from `it.fails`
 * to `it`.
 */
import { describe, it, expect } from "vitest";
import { execFile } from "node:child_process";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { Pipeline, ConcurrentPipeline, HttpPipeline, ClusterPipeline, toNodeHandler } from "../src";

/** A subprocess fixture (`node:cluster`, `execFile`) pays real process/fork startup cost. */
const FIXTURE_TIMEOUT = 20000;
/** An in-process, loopback-only HTTP case - no fork, no subprocess - fails fast on a real hang. */
const HTTP_TIMEOUT = 5000;

interface FixtureResult {
  stdout: string;
  stderr: string;
  code: number;
  /** Set when `execFile`'s own timeout killed the process - a killed process reports `err.code`
   * as `null` (Node has no exit code for it), not a real assertion failure's code. Surfaced here
   * instead of silently folding into a generic non-zero `code`. */
  timedOut: boolean;
}

/** Spawns a fixture script under `tsx` (needed for `src`'s extensionless imports and the `@src/*`
 * alias - Node's own ESM resolver has neither) and collects its stdout/stderr/exit code. Every
 * assertion a fixture's OWN Done-when cases need reads one shared run - never re-spawn the same
 * script per assertion. */
function runFixture(relativePath: string): Promise<FixtureResult> {
  return new Promise((resolve) => {
    execFile(
      process.execPath,
      ["--import", "tsx", relativePath],
      { timeout: FIXTURE_TIMEOUT },
      (err, stdout, stderr) => {
        const errno = err as
          (NodeJS.ErrnoException & { code?: number; signal?: string; killed?: boolean }) | null;
        const timedOut = errno?.killed === true && errno?.signal != null;
        const code = errno ? (typeof errno.code === "number" ? errno.code : 1) : 0;
        resolve({ stdout, stderr, code, timedOut });
      },
    );
  });
}

/** Binds `handler` to a real loopback HTTP server, runs `use` against its `http://localhost:<port>`
 * url, and always closes the server after - the one seam every HTTP-backed case below goes
 * through, so a leaked listening socket on a failed assertion can't happen. */
async function withServer<T>(
  handler: (request: Request) => Promise<Response>,
  use: (url: string) => Promise<T>,
): Promise<T> {
  const server = createServer(toNodeHandler(handler));
  await new Promise<void>((resolve) => server.listen(0, resolve));
  const port = (server.address() as AddressInfo).port;
  try {
    return await use(`http://localhost:${port}`);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

/** The "another instance" side of an `HttpPipeline` chain: an empty-source pipeline whose only
 * job is to hold the SAME stage definitions `builder` describes, so its `.fetch` can serve them. */
function makeWorker<U>(builder: (t: HttpPipeline<number>) => HttpPipeline<U>): HttpPipeline<U> {
  return builder(new HttpPipeline<number>([], { url: "" }));
}

/** Asserts a fixture exited 0 - and, when it didn't, says whether that's because
 * `FIXTURE_TIMEOUT`/`HTTP_TIMEOUT` killed it (a hang) rather than a real non-zero exit. */
function expectFixtureOk(result: FixtureResult): void {
  expect(result.timedOut, `fixture timed out; stderr:\n${result.stderr}`).toBe(false);
  expect(result.code, `fixture exited ${result.code}; stderr:\n${result.stderr}`).toBe(0);
}

describe("#17 ClusterPipeline canonical program (Done-when 1, 3)", () => {
  it.fails(
    "prints [6,8,10] with no server/listen/fork/url in caller code, and exits on its own",
    async () => {
      const result = await runFixture("__tests__/fixtures/cluster-basic.ts");
      expect(result.stderr).toBe("");
      expectFixtureOk(result); // Done-when 3: exits cleanly on its own, no explicit teardown
      expect(result.stdout.trim()).toBe("[6,8,10]"); // Done-when 1: the canonical result
    },
    FIXTURE_TIMEOUT,
  );
});

describe("#17 ClusterPipeline dispatches to real worker processes (Done-when 2)", () => {
  it.fails(
    "distinct process.pid values serve stage 0, count matches workers",
    async () => {
      const fixture = await runFixture("__tests__/fixtures/cluster-pids.ts");
      expectFixtureOk(fixture);
      const result = JSON.parse(fixture.stdout.trim()) as { distinctPids: number; workers: number };
      expect(result.distinctPids).toBe(result.workers);
    },
    FIXTURE_TIMEOUT,
  );
});

describe("#17 three ClusterPipelines share one port and worker set (Done-when 4)", () => {
  it.fails(
    "every pipeline's url is identical",
    async () => {
      const fixture = await runFixture("__tests__/fixtures/cluster-shared-port.ts");
      expectFixtureOk(fixture);
      const result = JSON.parse(fixture.stdout.trim()) as { ports: string[]; results: number[][] };
      expect(new Set(result.ports).size).toBe(1);
      expect(result.results).toEqual([[1], [2], [3]]);
    },
    FIXTURE_TIMEOUT,
  );
});

describe("#17 HttpPipeline across two real instances (Done-when 5)", () => {
  it.fails(
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

describe("#17 { local: true } keeps a stage in-process (Done-when 6)", () => {
  it.fails(
    "makes ZERO HTTP requests for the local stage",
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
          .transform((t) => t.filter((x: number) => x > 2), { local: true })
          .toArray();
        expect(out).toEqual([4, 6]);
        expect(requests).toBe(1); // only the first (non-local) stage crossed the wire
      });
    },
    HTTP_TIMEOUT,
  );

  it("a plain Pipeline has no { local: true } second argument to .transform() - compile error", () => {
    // Type-only: never executed. `tsc --noEmit` is the real assertion; `@ts-expect-error` itself
    // fails (TS2578) if the call ever stopped erroring - the negative-case pattern
    // `.claude/rules/typescript.md` calls for over trusting a "should fail" claim.
    function typeOnlyCheck() {
      const plain = new Pipeline<number>([1]);
      // @ts-expect-error - Pipeline.transform() takes no StageOptions second argument
      plain.transform((t) => t, { local: true });
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

  it.fails("a ConcurrentPipeline stays ConcurrentPipeline", () => {
    const p = new ConcurrentPipeline([1])
      .transform((t) => t.map((x: number) => x))
      .transform((t) => t.map((x: number) => x));
    expect(p.constructor.name).toBe("ConcurrentPipeline");
  });

  it.fails("an HttpPipeline stays HttpPipeline", () => {
    const p = new HttpPipeline([1], { url: "http://localhost:1" })
      .transform((t) => t.map((x: number) => x))
      .transform((t) => t.map((x: number) => x));
    expect(p.constructor.name).toBe("HttpPipeline");
  });

  it.fails(
    "a ClusterPipeline stays ClusterPipeline (constructed only - never drained, never forks)",
    () => {
      const p = new ClusterPipeline([1])
        .transform((t) => t.map((x: number) => x))
        .transform((t) => t.map((x: number) => x));
      expect(p.constructor.name).toBe("ClusterPipeline");
    },
  );
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
});

describe("#17 a chunk failure never leaks an unhandled rejection (Done-when 8)", () => {
  it(
    "control: the shipped concurrent() strategy DOES leak, on this chain",
    async () => {
      const fixture = await runFixture("__tests__/fixtures/concurrent-unhandled-control.ts");
      expectFixtureOk(fixture);
      const result = JSON.parse(fixture.stdout.trim()) as { control: string[] };
      expect(result.control.length).toBeGreaterThan(0);
    },
    FIXTURE_TIMEOUT,
  );

  it.fails(
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
  it.fails("dispatches before the whole source has been pulled", async () => {
    const pulled: number[] = [];
    async function* source() {
      for (const x of [1, 2, 3, 4, 5, 6]) {
        pulled.push(x);
        yield x;
      }
    }
    const cp = new ConcurrentPipeline<number>(source(), { maxConcurrency: 2, ordered: false });
    await cp
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
  it.fails("chunk 0 made 12x slower still comes out first", async () => {
    const delay = (ms: number) => new Promise((r) => setTimeout(r, ms));
    const cp = new ConcurrentPipeline<number>([1, 2, 3, 4], {
      maxConcurrency: 4,
      ordered: true,
      chunkSize: 1,
    });
    const out = await cp
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
  it.fails(
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
  it.fails(
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

describe("#17 .context() propagates through the wire (Done-when 14)", () => {
  it.fails(
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

  it.fails(
    "prints [10,20,30,40,50] through ClusterPipeline, still a ClusterPipeline after .context()",
    async () => {
      const fixture = await runFixture("__tests__/fixtures/cluster-context.ts");
      expectFixtureOk(fixture);
      const result = JSON.parse(fixture.stdout.trim()) as {
        out: number[];
        ctorNameAfterContext: string;
      };
      expect(result.out).toEqual([10, 20, 30, 40, 50]);
      expect(result.ctorNameAfterContext).toBe("ClusterPipeline");
    },
    FIXTURE_TIMEOUT,
  );
});

describe("#17 a stage's HTTP 500 throws from the terminal op, never reaches .catch() (Done-when 15)", () => {
  it.fails(
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
