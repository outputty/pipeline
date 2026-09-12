/**
 * The `ClusterPipeline` leg (#120) - each chunk dispatched to another PROCESS on the same machine.
 * Its `.local()` row proves every item's stage ran on the PRIMARY's own pid, never a worker's
 * (Done-when 3) - a small, SEPARATE run (never the timed row itself, which would then be paying for
 * a `process.pid`-capturing link it doesn't otherwise need) maps `process.pid` inside `.local()` and
 * keeps only the pids that are NOT the primary's own; a correct region reports none.
 *
 * Every exported function here constructs a real `ClusterPipeline`, so it is called from a
 * SUBPROCESS ONLY - never inside a Vitest worker (`pipelines.e2e.test.ts`'s own header explains why:
 * `cluster.fork()` re-execs `process.argv[1]`, which inside a Vitest worker is Vitest's own entry).
 * `bench/overhead.ts` (the CLI) and `__tests__/fixtures/bench-cluster-*.ts` (this file's own test
 * subprocesses) are the only two callers.
 */
import { ClusterPipeline } from "../../src";
import {
  canonicalChain,
  canonicalInput,
  handRolledFloor,
  timeRounds,
  ROWS,
  BUFFER_SIZE,
  MAX_CONCURRENCY,
  CLUSTER_WORKERS,
} from "../canonical";
import type { LegReport } from "../gate";

/** Real, timed `pipelineNsPerRow`/`floorNsPerRow`/`ratio` at `ROWS.ClusterPipeline` rows over real
 * forked workers, plus the `.local()` row and its `workerPidsWhilePinned` correctness check. */
export async function measureClusterPipeline(rounds?: number): Promise<LegReport> {
  const items = canonicalInput(ROWS.ClusterPipeline);

  const floorNsPerRow = await timeRounds(() => {
    handRolledFloor(items);
    return items.length;
  }, rounds);

  const pipeline = new ClusterPipeline<number>({
    workers: CLUSTER_WORKERS,
    maxConcurrency: MAX_CONCURRENCY,
  })
    .buffer(BUFFER_SIZE)
    .transform(canonicalChain);
  const pipelineNsPerRow = await timeRounds(async () => {
    await pipeline(items).toArray();
    return items.length;
  }, rounds);

  const localPipeline = new ClusterPipeline<number>({
    workers: CLUSTER_WORKERS,
    maxConcurrency: MAX_CONCURRENCY,
  })
    .buffer(BUFFER_SIZE)
    .local((p) => p.transform(canonicalChain));
  const localNsPerRow = await timeRounds(async () => {
    await localPipeline(items).toArray();
    return items.length;
  }, rounds);

  // Small, separate correctness run (never folded into the timed row above): a link that captures
  // `process.pid` is real per-item cost `localNsPerRow` must not pay for.
  const primaryPid = process.pid;
  const pids = await new ClusterPipeline<number>({ workers: CLUSTER_WORKERS })
    .buffer(1)
    .local((p) => p.transform((t) => t.map((_x: number) => process.pid)))(canonicalInput(10))
    .toArray();
  const workerPidsWhilePinned = [...new Set(pids)].filter((pid) => pid !== primaryPid);

  return {
    pipelineNsPerRow,
    floorNsPerRow,
    ratio: pipelineNsPerRow / floorNsPerRow,
    local: { nsPerRow: localNsPerRow, workerPidsWhilePinned },
  };
}

/** `clusterMatchesFloor(50)` → `true` - small-N equality (Done-when 2), never the full
 * `ROWS.ClusterPipeline` size. Called from a subprocess fixture only (see this file's own header). */
export async function clusterMatchesFloor(n: number): Promise<boolean> {
  const items = canonicalInput(n);
  const out = await new ClusterPipeline<number>({ workers: CLUSTER_WORKERS })
    .transform(canonicalChain)(items)
    .toArray();
  const floor = handRolledFloor(items);
  return out.length === floor.length && out.every((v, i) => v === floor[i]);
}
