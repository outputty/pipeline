/**
 * client-fallback.e2e.test.ts — #179's Done-when 7: the default client falls back to the global
 * `fetch` when `node:http` does not import, and a dispatching chain works unchanged on it.
 *
 * Its OWN file, deliberately. `defaultClient()` resolves once per process and caches the result, so
 * a case that forces the fallback cannot share a process with one that wants the `node:http`
 * default - vitest isolates each test file, which is what keeps the two apart.
 *
 * The env var is the mechanism rather than a module mock: `.oxlintrc.json` sets
 * `anti-slop/no-module-mocking` to `error`, and a mock would prove the resolver's own branch rather
 * than a real chain running end to end on the fallback client.
 */
import { describe, it, expect, beforeAll } from "vitest";

beforeAll(() => {
  process.env.OUTPUTTY_PIPELINE_FORCE_FETCH = "1";
});

describe("#179 the default client falls back to the global fetch (Done-when 7)", () => {
  it("still matches the hand-rolled floor with node:http forced out", async () => {
    const { httpMatchesFloor } = await import("../bench/legs/http");
    expect(await httpMatchesFloor(50)).toBe(true);
  });

  it("resolves to fetchClient itself, not merely to something that works", async () => {
    const { defaultClient, fetchClient } = await import("@src/pipelines/client");
    expect(await defaultClient()).toBe(fetchClient);
  });

  it("keeps /reduce/<n> duplex on the fallback client too", async () => {
    // The global `fetch` streams both directions on Node, so the fallback is a correct client for
    // the duplex wire and not merely a slower one - this pins that rather than assuming it.
    const { HttpPipeline } = await import("@src/pipelines/http");
    const { withServer } = await import("./helpers/fixtures");
    const events: string[] = [];
    const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

    const worker = new HttpPipeline<number>({ url: "", maxConcurrency: 1 })
      .buffer(1)
      .reduce<number>((acc, item, _ctx, emit) => {
        emit(item);
        return acc;
      }, 0);

    async function* slowSource(): AsyncGenerator<number> {
      for (let i = 0; i < 5; i++) {
        await sleep(20);
        yield i;
      }
      events.push("source done");
    }

    const got = await withServer(worker.fetch, async (url) => {
      const trigger = new HttpPipeline<number>(worker, { url, maxConcurrency: 1 });
      const seen: number[] = [];
      for await (const item of trigger(slowSource())) {
        events.push(`got ${item}`);
        seen.push(item);
      }
      return seen;
    });

    expect(got).toEqual([0, 1, 2, 3, 4]);
    const firstGot = events.findIndex((e) => e.startsWith("got "));
    expect(firstGot).toBeGreaterThanOrEqual(0);
    expect(firstGot).toBeLessThan(events.indexOf("source done"));
  });
});
