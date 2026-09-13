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

describe("#179 review - the default client's own failure modes", () => {
  it("rejects a failing reduce body instead of killing the process", async () => {
    // `/reduce/<n>`'s request body pulls from the upstream chain, so a failing stage errors that
    // stream while the request is open. Probed before the fix: `Readable.fromWeb` emitted `error`
    // with no listener - `Unhandled 'error' event`, process exit 1 - where the global `fetch` this
    // client replaced rejected the request and let the failure propagate normally.
    const worker = new HttpPipeline<number>({ url: "", maxConcurrency: 1 })
      .buffer(1)
      .reduce<number>((acc, item) => acc + item, 0);

    async function* failingSource(): AsyncGenerator<number> {
      yield 1;
      yield 2;
      throw new Error("source blew up mid-stream");
    }

    await withServer(worker.fetch, async (url) => {
      const trigger = new HttpPipeline<number>(worker, { url, maxConcurrency: 1 });
      await expect(trigger(failingSource()).toArray()).rejects.toThrow("source blew up mid-stream");
    });
  });

  it("sends a non-http url through fetch rather than cleartext port 80", async () => {
    // `node:http` speaks cleartext only and reads an empty `port` as 80, so an `https:` url
    // dispatched here left the chunk JSON going in the clear to whatever answered on port 80.
    // Probed before the fix: `status 404 remote 104.20.23.154 80` for `https://example.com`.
    const calls: string[] = [];
    const client: PipelineClient = async (url) => {
      calls.push(url);
      return new Response(JSON.stringify({ chunk: [] }), {
        headers: { "content-type": "application/json" },
      });
    };

    // The caller's own client is dispatched to verbatim, whatever the protocol - the guard being
    // pinned here lives in the DEFAULT client, so this case pins the seam's contract and the case
    // below pins the guard itself.
    const worker = new HttpPipeline<number>({ url: "" }).transform((t) => t.map((x: number) => x));
    await new HttpPipeline<number>(worker, { url: "https://example.invalid", client })([
      1,
    ]).toArray();
    expect(calls).toEqual(["https://example.invalid/transform/0"]);
  });

  it("never speaks cleartext to an https url, inside the default client itself", async () => {
    // Asserted against a REAL cleartext listener, not an unroutable host: an unreachable name fails
    // on either path and proves nothing. Here the port genuinely answers plain HTTP, so an unguarded
    // `node:http` client reaches it and the server records a hit - which is precisely the leak, the
    // chunk JSON and any auth header going out in the clear. The guard must leave it at ZERO.
    const client = await defaultClient();
    const hits: string[] = [];

    await withServer(
      async (request) => {
        hits.push(new URL(request.url).pathname);
        return new Response(JSON.stringify({ chunk: [] }), {
          headers: { "content-type": "application/json" },
        });
      },
      async (url) => {
        const asHttps = url.replace("http://", "https://");
        await expect(client(`${asHttps}/transform/0`, { method: "POST" })).rejects.toThrow();
      },
    );

    expect(hits).toEqual([]);
  });

  it("dispatches to an IPv6 loopback url, brackets and all", async () => {
    // `URL.hostname` keeps the brackets on an IPv6 literal; `node:http` hands that to DNS verbatim
    // and fails `getaddrinfo ENOTFOUND [::1]`, where undici strips them.
    const worker = new HttpPipeline<number>({ url: "" }).transform((t) =>
      t.map((x: number) => x * 3),
    );
    const out = await withServer(worker.fetch, async (url) => {
      const port = new URL(url).port;
      return new HttpPipeline<number>(worker, { url: `http://[::1]:${port}` })([1, 2]).toArray();
    });
    expect(out).toEqual([3, 6]);
  });
});
