/**
 * #120's own Done-when cases, proven against the real `bench/` harness - no mocks, real `Pipeline`
 * family instances, a real loopback server (HttpPipeline) and a real forked worker (ClusterPipeline,
 * run as a subprocess fixture - `pipelines.e2e.test.ts`'s own header explains why a
 * `ClusterPipeline` is never constructed directly inside a Vitest worker).
 *
 * No case here asserts against `bench/baseline.json`'s own numbers or wall-clock timing at all -
 * that would fail on any machine slower or faster than the one the baseline was measured on: the
 * gate's OWN logic is tested with synthetic reports (the "checkGate" describe block below), and
 * every CLI-level case that must reference the real gate sets `BENCH_SKIP_GATE=1` or leans on
 * `BENCH_SYNTHETIC_REGRESSION`'s 100x multiplier to stay deterministic. A real, full-round `pnpm
 * bench:overhead` run, pasted into the PR, is Done-when 1's actual proof.
 */
import { describe, it, expect } from "vitest";
import { canonicalChain, canonicalInput, median, timeRounds } from "../bench/canonical";
import { checkGate, type OverheadReport } from "../bench/gate";
import { pipelineMatchesFloor } from "../bench/legs/pipeline";
import { concurrentMatchesFloor, countingConcurrentPipeline } from "../bench/legs/concurrent";
import { httpMatchesFloor, measureHttpPipeline } from "../bench/legs/http";
import { withLoopbackServer, countingHandler } from "../bench/utils/loopbackServer";
import { HttpPipeline } from "../src";
import {
  FIXTURE_TIMEOUT,
  HTTP_TIMEOUT,
  runFixtureJson,
  runFixture,
  expectFixtureOk,
  lastJsonLine,
} from "./helpers/fixtures";

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

  it(
    "ClusterPipeline: identical output at a small N (subprocess fixture)",
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
      // floorNsPerRow (first arg) is irrelevant to this assertion - any finite placeholder works,
      // since measureHttpPipeline no longer measures it itself (bench/overhead.ts does, once).
      const report = await measureHttpPipeline(1, 2);
      expect(report.local?.requestsWhilePinned).toBe(0);
    },
    HTTP_TIMEOUT,
  );

  it(
    "ClusterPipeline: every item's stage runs on the primary's own pid (subprocess fixture)",
    async () => {
      const result = await runFixtureJson<{
        processedCount: number;
        workerPidsWhilePinned: number[];
      }>("__tests__/fixtures/bench-cluster-local.ts");
      // processedCount proves the region actually ran all 10 items - workerPidsWhilePinned being
      // empty means nothing on its own if the region silently produced no output at all.
      expect(result.processedCount).toBe(10);
      expect(result.workerPidsWhilePinned).toEqual([]);
    },
    FIXTURE_TIMEOUT,
  );
});

describe("#120 Done-when 1 - pnpm bench:overhead prints all four legs' rows", () => {
  it(
    "bench/overhead.ts reports all four classes plus their .local() rows",
    async () => {
      // BENCH_ROUNDS=2 (1 warm-up + 1 measured) keeps this a quick smoke check - the SHAPE of the
      // report does not depend on the round count. BENCH_SKIP_GATE=1 keeps it deterministic
      // regardless of how this machine's own noise compares to a baseline committed on another one
      // - a real, full-round, gated run is what gets pasted into the PR as Done-when 1's own proof.
      process.env.BENCH_ROUNDS = "2";
      process.env.BENCH_SKIP_GATE = "1";
      try {
        const result =
          await runFixtureJson<Record<string, { local?: unknown }>>("bench/overhead.ts");
        expect(Object.keys(result)).toEqual([
          "Pipeline",
          "ConcurrentPipeline",
          "HttpPipeline",
          "ClusterPipeline",
        ]);
        expect(result.ConcurrentPipeline.local).toBeDefined();
        expect(result.HttpPipeline.local).toBeDefined();
        expect(result.ClusterPipeline.local).toBeDefined();
      } finally {
        delete process.env.BENCH_ROUNDS;
        delete process.env.BENCH_SKIP_GATE;
      }
    },
    FIXTURE_TIMEOUT,
  );
});

describe("#120 Done-when 4 - a synthetic regression makes the gate fail", () => {
  it(
    "pnpm bench:overhead exits 0 clean, and 1 under BENCH_SYNTHETIC_REGRESSION=1",
    async () => {
      // BENCH_ROUNDS=2 keeps this test's two full CLI runs inside FIXTURE_TIMEOUT. The "clean" run
      // sets BENCH_SKIP_GATE=1 - deterministic exit 0 regardless of how this machine's own noise
      // compares to a baseline committed on another one, proving only that the harness runs end to
      // end. The regressed run gates for real: its 100x multiplier dominates any real machine noise,
      // so the resulting exit 1 is deterministic too.
      process.env.BENCH_ROUNDS = "2";
      try {
        process.env.BENCH_SKIP_GATE = "1";
        const clean = await runFixture("bench/overhead.ts");
        expectFixtureOk(clean);
        delete process.env.BENCH_SKIP_GATE;
        process.env.BENCH_SYNTHETIC_REGRESSION = "1";
        const regressed = await runFixture("bench/overhead.ts");
        expect(regressed.code).toBe(1);
        // The exit code alone proves the GATE fired, not that the multiplier reached the right
        // field: read the printed report back and confirm ratio still agrees with its own two
        // inputs (catches the multiplier updating pipelineNsPerRow but leaving ratio stale).
        const report = lastJsonLine<{
          Pipeline: { pipelineNsPerRow: number; floorNsPerRow: number; ratio: number };
        }>(regressed);
        expect(report.Pipeline.ratio).toBeCloseTo(
          report.Pipeline.pipelineNsPerRow / report.Pipeline.floorNsPerRow,
          6,
        );
      } finally {
        delete process.env.BENCH_ROUNDS;
        delete process.env.BENCH_SKIP_GATE;
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
    HttpPipeline: {
      pipelineNsPerRow: 1000,
      floorNsPerRow: 22,
      ratio: 45.5,
      local: { nsPerRow: 260, requestsWhilePinned: 0 },
    },
    ClusterPipeline: {
      pipelineNsPerRow: 1200,
      floorNsPerRow: 21,
      ratio: 57.1,
      local: { nsPerRow: 265, workerPidsWhilePinned: [] },
    },
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

  it("passes when a leg's ratio widens a lot with absolute ns/row unchanged - ratio is never gated", () => {
    // .ratio divides by floorNsPerRow, and dividing two independently noisy measurements compounds
    // their noise (bench/gate.ts's own header, the post-planning finding): a smaller floorNsPerRow
    // alone can double the ratio with pipelineNsPerRow untouched, which is exactly what this report
    // simulates. `Pipeline` has no `local` row, so pipelineNsPerRow is the gated field here - a
    // dispatching class instead gates local.nsPerRow (bench/gate.ts's own header).
    const report = {
      ...baseline,
      Pipeline: { pipelineNsPerRow: 50, floorNsPerRow: 1.5, ratio: 33.3 },
    };
    expect(checkGate(report, baseline).ok).toBe(true);
  });

  it("stays within tolerance at exactly the absolute boundary", () => {
    const report = {
      ...baseline,
      Pipeline: { pipelineNsPerRow: 60, floorNsPerRow: 4.2, ratio: 14.3 },
    };
    expect(checkGate(report, baseline).ok).toBe(true);
  });

  it("fails when a leg the baseline has is missing from the report, rather than silently skipping it", () => {
    const { Pipeline: _omitted, ...report } = baseline;
    const result = checkGate(report, baseline);
    expect(result.ok).toBe(false);
    expect(result.violations[0]).toMatch(/Pipeline: missing from the report/);
  });

  it("fails when a leg's pipelineNsPerRow is NaN, rather than silently passing (code-review finding)", () => {
    // Every comparison against NaN is false, so an unguarded `current.pipelineNsPerRow > ceiling`
    // would report `{ ok: true }` for a broken measurement - the same hazard median()'s own
    // docstring names, now for checkGate's own comparison.
    const report = {
      ...baseline,
      Pipeline: { pipelineNsPerRow: NaN, floorNsPerRow: 4.2, ratio: NaN },
    };
    const result = checkGate(report, baseline);
    expect(result.ok).toBe(false);
    expect(result.violations[0]).toMatch(/Pipeline: pipelineNsPerRow is NaN/);
  });

  it("passes when a dispatching class's DISPATCHED ns/row triples, local.nsPerRow unchanged (#120 follow-up)", () => {
    // A dispatching class's own DISPATCHED leg crosses a real network/IPC boundary (real jitter,
    // not this package's own overhead) - `local` present marks it as one, and it is `local.nsPerRow`
    // that gates now, never the dispatched `pipelineNsPerRow` itself.
    const report = {
      ...baseline,
      ConcurrentPipeline: { ...baseline.ConcurrentPipeline, pipelineNsPerRow: 1050 },
    };
    expect(checkGate(report, baseline).ok).toBe(true);
  });

  it("fails when a dispatching class's own local.nsPerRow widens past 20% (#120 follow-up)", () => {
    const report = {
      ...baseline,
      ConcurrentPipeline: {
        ...baseline.ConcurrentPipeline,
        local: { ...baseline.ConcurrentPipeline.local!, nsPerRow: 400 },
      },
    };
    const result = checkGate(report, baseline);
    expect(result.ok).toBe(false);
    expect(result.violations[0]).toMatch(/ConcurrentPipeline: local\.nsPerRow/);
  });

  it("fails when a dispatching class's local row is missing from the report (#120 follow-up)", () => {
    const { local: _omitted, ...concurrentWithoutLocal } = baseline.ConcurrentPipeline;
    const report = { ...baseline, ConcurrentPipeline: concurrentWithoutLocal };
    const result = checkGate(report, baseline);
    expect(result.ok).toBe(false);
    expect(result.violations[0]).toMatch(/ConcurrentPipeline: local is missing/);
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

describe("#120 bench/overhead.ts - BENCH_ROUNDS validation", () => {
  it("a non-numeric BENCH_ROUNDS fails loud, naming the bad value, rather than silently becoming NaN", async () => {
    process.env.BENCH_ROUNDS = "5x";
    try {
      const result = await runFixture("bench/overhead.ts");
      expect(result.code).not.toBe(0);
      expect(result.stderr).toMatch(/BENCH_ROUNDS must be a finite number, got "5x"/);
    } finally {
      delete process.env.BENCH_ROUNDS;
    }
  });

  it(
    "BENCH_ROUNDS=Infinity fails loud rather than hanging the round loop forever (code-review finding)",
    async () => {
      // Number("Infinity") is Infinity, not NaN - a bare Number.isNaN guard lets it through, and
      // timeRounds's own `rounds < 2` check is false for Infinity too, so the round loop never
      // terminates. `timedOut: false` is the discriminator: an unfixed rounds() hangs until
      // FIXTURE_TIMEOUT kills the process (timedOut: true, code 1 from the kill, not from a real
      // exit) - this asserts the CLI itself exits, and names the bad value, well before that.
      process.env.BENCH_ROUNDS = "Infinity";
      try {
        const result = await runFixture("bench/overhead.ts");
        expect(result.timedOut).toBe(false);
        expect(result.code).not.toBe(0);
        expect(result.stderr).toMatch(/BENCH_ROUNDS must be a finite number, got "Infinity"/);
      } finally {
        delete process.env.BENCH_ROUNDS;
      }
    },
    FIXTURE_TIMEOUT,
  );
});
