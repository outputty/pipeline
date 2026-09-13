/**
 * client-seam.e2e.test.ts — #179's Done-when 6: `HttpPipeline` accepts `options.client`, carries it
 * through copy-on-write like every other knob, and dispatches BOTH `stageWork()` and `reduceWork()`
 * through it.
 *
 * The fallback half of Done-when 7 lives in its own file: the default client resolves ONCE per
 * process, so a case that forces it past `node:http` cannot share a process with these.
 */
import { describe, it, expect } from "vitest";
import { HttpPipeline } from "@src/pipelines/http";
import { defaultClient, fetchClient, type PipelineClient } from "@src/pipelines/client";
import { withServer } from "./helpers/fixtures";

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/** Wraps `inner`, recording the path of every request it dispatches - the one seam these cases
 * observe at, since a client is the thing under test rather than the thing under stub. */
function recording(inner: PipelineClient): { client: PipelineClient; paths: string[] } {
  const paths: string[] = [];
  return {
    paths,
    client: (url, init) => {
      paths.push(new URL(url).pathname);
      return inner(url, init);
    },
  };
}

describe("#179 HttpPipeline dispatches through options.client (Done-when 6)", () => {
  it("sends a transform stage through the caller's own client", async () => {
    const worker = new HttpPipeline<number>({ url: "" }).transform((t) =>
      t.map((x: number) => x * 2),
    );

    const seen = recording(fetchClient);
    const out = await withServer(worker.fetch, async (url) =>
      new HttpPipeline<number>(worker, { url, client: seen.client })([1, 2, 3]).toArray(),
    );

    expect(out).toEqual([2, 4, 6]);
    expect(seen.paths).toEqual(["/transform/0"]);
  });

  it("sends a reduce stage through the caller's own client too", async () => {
    const worker = new HttpPipeline<number>({ url: "", maxConcurrency: 1 }).reduce<number>(
      (acc, item) => acc + item,
      0,
    );

    const seen = recording(fetchClient);
    const out = await withServer(worker.fetch, async (url) =>
      new HttpPipeline<number>(worker, {
        url,
        maxConcurrency: 1,
        client: seen.client,
      })([1, 2, 3, 4]).toArray(),
    );

    expect(out).toEqual([10]);
    expect(seen.paths).toEqual(["/reduce/0"]);
  });

  it("carries the client through a copy-on-write call", async () => {
    // `.buffer()` and `.transform()` both build a NEW instance; a knob dropped there would silently
    // revert this chain to the runtime default, which is exactly what `carriedKnobs()` exists to
    // stop. Two stages, so the carry has to survive more than the first copy.
    const worker = new HttpPipeline<number>({ url: "" })
      .transform((t) => t.map((x: number) => x + 1))
      .transform((t) => t.map((x: number) => x * 10));

    const seen = recording(fetchClient);
    const out = await withServer(worker.fetch, async (url) => {
      const trigger = new HttpPipeline<number>(worker, { url, client: seen.client }).buffer(2);
      return trigger([1, 2, 3]).toArray();
    });

    expect(out).toEqual([20, 30, 40]);
    // Both stages still went through the caller's client after the copy. A dropped knob would leave
    // this EMPTY and the output identical - the chain would simply have reverted to the runtime
    // default, which is exactly the silent failure `carriedKnobs()` exists to prevent.
    expect(seen.paths).toEqual(["/transform/0", "/transform/1"]);
  });

  it("defaults to a client that streams both ways, so /reduce/<n> stays duplex", async () => {
    // The decisive observable, and the reason the default client cannot buffer: an emit must reach
    // the caller while the request body is still being written.
    //
    // Verified by sabotage rather than assumed. Rebuilding the default client to collect the
    // response into a `Buffer` before resolving - the shape the `node-http-runtime` skill records as
    // the trap - left every other case in this file passing and failed this one alone, with
    // `expected 7 to be less than 6`: every emit arrived after the source had finished.
    const events: string[] = [];

    const worker = new HttpPipeline<number>({ url: "", maxConcurrency: 1 })
      .buffer(1)
      .reduce<number>((acc, item, _ctx, emit) => {
        emit(item);
        return acc;
      }, 0);

    async function* slowSource(): AsyncGenerator<number> {
      for (let i = 0; i < 6; i++) {
        await sleep(20);
        events.push(`sent ${i}`);
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

    expect(got).toEqual([0, 1, 2, 3, 4, 5]);
    const firstGot = events.findIndex((e) => e.startsWith("got "));
    const sourceDone = events.indexOf("source done");
    expect(firstGot).toBeGreaterThanOrEqual(0);
    expect(firstGot).toBeLessThan(sourceDone);
  });

  it("resolves the default client once per process, never per chunk", async () => {
    const first = await defaultClient();
    const second = await defaultClient();
    expect(second).toBe(first);
  });
});
