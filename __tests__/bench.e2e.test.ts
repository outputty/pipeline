/**
 * #120's own Done-when cases, proven against the real `bench/` harness - no mocks, real `Pipeline`
 * family instances, a real loopback server (HttpPipeline) and a real forked worker (ClusterPipeline,
 * L3, run as a subprocess fixture - `pipelines.e2e.test.ts`'s own header explains why a
 * `ClusterPipeline` is never constructed directly inside a Vitest worker).
 *
 * A case naming a class or a CLI behavior this layer hasn't built yet is `it.fails`, flipping to
 * `it` as each layer lands (`pipelines.e2e.test.ts`'s own established convention).
 *
 * No case here asserts against `bench/baseline.json`'s own numbers or wall-clock timing at all -
 * that would fail on any machine slower or faster than the one the baseline was measured on. The
 * gate's OWN logic is tested with synthetic reports (the "checkGate" describe block below); a real
 * `pnpm bench:overhead` run, pasted into the PR, is Done-when 1's actual proof.
 */
import { describe, it, expect } from "vitest";
import { canonicalChain, canonicalInput, median, timeRounds } from "../bench/canonical";
import { checkGate, type OverheadReport } from "../bench/gate";
import { pipelineMatchesFloor } from "../bench/legs/pipeline";
import { concurrentMatchesFloor, countingConcurrentPipeline } from "../bench/legs/concurrent";
import { httpMatchesFloor, measureHttpPipeline } from "../bench/legs/http";
import { withLoopbackServer, countingHandler } from "../bench/utils/loopbackServer";
import { HttpPipeline } from "../src";
import { FIXTURE_TIMEOUT, HTTP_TIMEOUT, runFixtureJson } from "./helpers/fixtures";

describe("#120 Done-when 2 - each class's hand-rolled floor matches its Pipeline-based leg", () => {
  it("Pipeline: identical output at a small N", async () => {
    expect(await pipelineMatchesFloor(50)).toBe(true);
  });

  it("ConcurrentPipeline: identical output at a small N", async () => {
    expect(await concurrentMatchesFloor(50)).toBe(true);
  });

  it(
    "HttpPipeline: identical output at a small N",
    async () => {
      expect(await httpMatchesFloor(50)).toBe(true);
    },
    HTTP_TIMEOUT,
  );

  it.fails(
    "ClusterPipeline: identical output at a small N (#120 L3, subprocess fixture)",
    async () => {
      const result = await runFixtureJson<{ matches: boolean }>(
        "__tests__/fixtures/bench-cluster-floor.ts",
      );
      expect(result.matches).toBe(true);
    },
    FIXTURE_TIMEOUT,
  );
});

describe("#120 Done-when 3 - .local() correctness per dispatching class", () => {
  it("ConcurrentPipeline: a stage OUTSIDE .local() DOES reach stageWork() - negative control", async () => {
    const { Pipeline: CountingPipeline, counter } = countingConcurrentPipeline();
    await new CountingPipeline({ maxConcurrency: 2 })
      .transform(canonicalChain)(canonicalInput(10))
      .toArray();
    expect(counter.calls).toBeGreaterThan(0);
  });

  it("ConcurrentPipeline: a stage INSIDE .local() never reaches stageWork() - 0 dispatches", async () => {
    const { Pipeline: CountingPipeline, counter } = countingConcurrentPipeline();
    await new CountingPipeline({ maxConcurrency: 2 })
      .local((p) => p.transform(canonicalChain))(canonicalInput(10))
      .toArray();
    expect(counter.calls).toBe(0);
  });

  it(
    "HttpPipeline: a stage OUTSIDE .local() DOES reach the worker - negative control",
    async () => {
      const { handler, counter } = countingHandler(
        new HttpPipeline<number>({ url: "" }).transform(canonicalChain).fetch,
      );
      await withLoopbackServer(handler, async (url) => {
        await new HttpPipeline<number>({ url })
          .transform(canonicalChain)(canonicalInput(10))
          .toArray();
      });
      expect(counter.requests).toBeGreaterThan(0);
    },
    HTTP_TIMEOUT,
  );

  it(
    "HttpPipeline: 0 requests served while pinned",
    async () => {
      const report = await measureHttpPipeline(2);
      expect(report.local?.requestsWhilePinned).toBe(0);
    },
    HTTP_TIMEOUT,
  );

  it.fails(
    "ClusterPipeline: every item's stage runs on the primary's own pid (#120 L3, subprocess fixture)",
    async () => {
      const result = await runFixtureJson<{ workerPidsWhilePinned: number[] }>(
        "__tests__/fixtures/bench-cluster-local.ts",
      );
      expect(result.workerPidsWhilePinned).toEqual([]);
    },
    FIXTURE_TIMEOUT,
  );
});

describe("#120 Done-when 1 - pnpm bench:overhead prints all four legs' rows", () => {
  it.fails(
    "bench/overhead.ts exists and reports all four classes plus their .local() rows (#120 L3)",
    async () => {
      const result = await runFixtureJson<Record<string, unknown>>("bench/overhead.ts");
      expect(Object.keys(result)).toEqual([
        "Pipeline",
        "ConcurrentPipeline",
        "HttpPipeline",
        "ClusterPipeline",
      ]);
    },
    FIXTURE_TIMEOUT,
  );
});

describe("#120 Done-when 4 - a synthetic regression makes the gate fail", () => {
  it.fails(
    "pnpm bench:overhead exits 0 clean, and 1 under BENCH_SYNTHETIC_REGRESSION=1 (#120 L3)",
    async () => {
      // BENCH_ROUNDS=2 (1 warm-up + 1 measured) keeps this test's two full CLI runs inside
      // FIXTURE_TIMEOUT - the exit code the gate produces does not depend on the round count.
      const { runFixture, expectFixtureOk } = await import("./helpers/fixtures");
      process.env.BENCH_ROUNDS = "2";
      try {
        const clean = await runFixture("bench/overhead.ts");
        expectFixtureOk(clean);
        process.env.BENCH_SYNTHETIC_REGRESSION = "1";
        const regressed = await runFixture("bench/overhead.ts");
        expect(regressed.code).toBe(1);
      } finally {
        delete process.env.BENCH_ROUNDS;
        delete process.env.BENCH_SYNTHETIC_REGRESSION;
      }
    },
    FIXTURE_TIMEOUT,
  );
});

describe("#120 checkGate - regression-only, synthetic reports (no real timing)", () => {
  const baseline: OverheadReport = {
    Pipeline: { pipelineNsPerRow: 50, floorNsPerRow: 4.2, ratio: 11.9 },
    ConcurrentPipeline: {
      pipelineNsPerRow: 350,
      floorNsPerRow: 19,
      ratio: 18.4,
      local: { nsPerRow: 300, dispatchesWhilePinned: 0 },
    },
    HttpPipeline: { pipelineNsPerRow: 1000, floorNsPerRow: 22, ratio: 45.5 },
    ClusterPipeline: { pipelineNsPerRow: 1200, floorNsPerRow: 21, ratio: 57.1 },
  };

  it("passes when the report matches the baseline exactly", () => {
    expect(checkGate(baseline, baseline)).toEqual({ ok: true, violations: [] });
  });

  it("passes when a leg got FASTER, however much (regression-only)", () => {
    const report = {
      ...baseline,
      Pipeline: { pipelineNsPerRow: 5, floorNsPerRow: 4.2, ratio: 1.2 },
    };
    expect(checkGate(report, baseline).ok).toBe(true);
  });

  it("fails when a leg's absolute ns/row widens past 20%", () => {
    const report = {
      ...baseline,
      Pipeline: { pipelineNsPerRow: 61, floorNsPerRow: 4.2, ratio: 14.5 },
    };
    const result = checkGate(report, baseline);
    expect(result.ok).toBe(false);
    expect(result.violations[0]).toMatch(/Pipeline: pipelineNsPerRow/);
  });

  it("fails when a leg's ratio widens past 10% even with absolute ns/row unchanged", () => {
    const report = {
      ...baseline,
      Pipeline: { pipelineNsPerRow: 50, floorNsPerRow: 3.5, ratio: 14.3 },
    };
    const result = checkGate(report, baseline);
    expect(result.ok).toBe(false);
    expect(result.violations[0]).toMatch(/Pipeline: ratio/);
  });

  it("stays within tolerance at exactly the boundary", () => {
    const report = {
      ...baseline,
      Pipeline: { pipelineNsPerRow: 60, floorNsPerRow: 4.2, ratio: 13.09 },
    };
    expect(checkGate(report, baseline).ok).toBe(true);
  });
});

describe("#120 median/timeRounds - fail loud rather than a silent NaN", () => {
  it("median([]) raises instead of returning NaN", () => {
    expect(() => median([])).toThrow(/empty array/);
  });

  it("median([3, 1, 2]) is the middle value; median([1, 2, 3, 4]) averages the middle two", () => {
    expect(median([3, 1, 2])).toBe(2);
    expect(median([1, 2, 3, 4])).toBe(2.5);
  });

  it("timeRounds(fn, 1) raises rather than returning a NaN a gate could silently treat as passing", async () => {
    await expect(timeRounds(() => 1, 1)).rejects.toThrow(/at least 2 rounds/);
  });
});
