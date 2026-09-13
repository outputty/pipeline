/**
 * How a dispatched chunk reaches another instance (#179) - the one seam `HttpPipeline.stageWork()`
 * and `HttpPipeline.reduceWork()` both send through, and the knob `options.client` replaces.
 *
 * Two implementations, which is what makes this a seam rather than a constant: `fetchClient` below
 * is the global `fetch`, correct on every runtime; `nodeClient` is `node:http` with one shared
 * keep-alive `Agent`, and is the default wherever `node:http` imports.
 *
 * Measured on a real loopback server, 200 chunks of 1000 rows, output asserted identical between the
 * two clients:
 *
 * ```text
 * /transform/<n>  one request PER CHUNK   fetch 1749-1828 ns/row   node:http 306-349 ns/row
 * /reduce/<n>     ONE request per stream  fetch  117-135 ns/row    node:http 112-134 ns/row
 * ```
 *
 * The whole win is per-REQUEST, so it lands on `/transform/<n>` and vanishes on `/reduce/<n>`, where
 * one request carries the entire stream and the saving divides across every chunk in it.
 * `reduceWork()` moves to this seam anyway, so ONE client serves the whole class: a caller who
 * supplies `options.client` must not find their reduce stages still on a different one.
 */

/**
 * How a dispatched chunk reaches another instance. Takes the global `fetch` signature, so the
 * default is a drop-in and so is a caller's own.
 *
 * Named `client`, never `fetch`: `HttpPipeline.fetch` is already that class's own SERVER handler,
 * and the two would collide on one object.
 *
 * `client("http://worker/transform/0", { method: "POST", body })` → the worker's `Response`.
 */
export type PipelineClient = (url: string, init: RequestInit) => Promise<Response>;

/** The runtime-neutral client: the global `fetch`, unchanged. `duplex: "half"` is set
 * unconditionally because Node's undici `Request` requires it whenever a streaming body is passed
 * and ignores it otherwise - the same reason `nodeRequestToFetchRequest` (`http.ts`) sets it on the
 * server side without branching. */
export const fetchClient: PipelineClient = (url, init) =>
  fetch(url, { ...init, duplex: "half" } as RequestInit);

/** Set to `1` to force `defaultClient()` past `node:http` onto the global `fetch`, whatever the
 * runtime offers. It exists so a case can prove the fallback really is reachable; a module mock
 * cannot, since `.oxlintrc.json` sets `anti-slop/no-module-mocking` to `error`. */
const FORCE_FETCH = "OUTPUTTY_PIPELINE_FORCE_FETCH";

/** Resolved ONCE per process, never per chunk - the promise itself is the cache, so a concurrent
 * second caller awaits the same resolution rather than starting a second one. */
let resolved: Promise<PipelineClient> | null = null;

/**
 * The client a `HttpPipeline` uses when the caller named none: `node:http` with a shared keep-alive
 * agent where `node:http` imports, the global `fetch` everywhere else.
 *
 * Returns a `Promise` because the probe is a dynamic `import()`. A STATIC import would decide the
 * question at module load and take the whole package down on a runtime without `node:http`
 * (Cloudflare Workers), which is the opposite of the neutrality this seam exists to keep. The
 * promise resolves once and every later call awaits the settled one, so a dispatch pays a microtask,
 * never a module resolution.
 *
 * `await defaultClient()` → `nodeClient` on Node, `fetchClient` on a runtime without `node:http`.
 */
export function defaultClient(): Promise<PipelineClient> {
  resolved ??= resolveClient();
  return resolved;
}

/** `defaultClient()`'s own one-time probe, its own function so the cache above stays one line. */
async function resolveClient(): Promise<PipelineClient> {
  const env = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process
    ?.env;
  if (env?.[FORCE_FETCH] === "1") {
    return fetchClient;
  }
  try {
    // BOTH imports are dynamic, and both are inside this `try`. A static `node:stream` import beside
    // a dynamic `node:http` one would defeat the whole probe: the module would fail to LOAD on a
    // runtime that has neither, taking the package down rather than falling back.
    const http = await import("node:http");
    const { Readable } = await import("node:stream");
    return buildNodeClient(new http.Agent({ keepAlive: true }), http.request, Readable);
  } catch {
    // A runtime with no `node:http` at all. Nothing is logged: the global `fetch` is a correct
    // client, not a degraded one, and a warning on every Workers deployment would be noise.
    return fetchClient;
  }
}

/** The `node:http` client, over an agent and a request function handed in rather than imported here
 * - `resolveClient()` above already holds the module it probed, and passing them keeps this function
 * free of its own import.
 *
 * ⚠ Streams BOTH directions, and that is a correctness requirement rather than a speed one. The
 * `Response` resolves on the `response` event - headers - so the caller reads frames while the
 * request body is still being written, which is what `/reduce/<n>`'s duplex NDJSON wire needs. An
 * adapter that collected `init.body` to a string and the response into a `Buffer` would lose no data
 * and pass every `/transform/<n>` case, then silently break duplex: the `node-http-runtime` skill
 * records that exact trap, measured as every reply arriving in one frame after the request body
 * closed. */
function buildNodeClient(
  agent: import("node:http").Agent,
  request: typeof import("node:http").request,
  Readable: typeof import("node:stream").Readable,
): PipelineClient {
  return (url, init) =>
    new Promise<Response>((resolve, reject) => {
      const target = new URL(url);
      const req = request(
        {
          agent,
          hostname: target.hostname,
          port: target.port,
          path: `${target.pathname}${target.search}`,
          method: init.method ?? "GET",
          headers: init.headers as Record<string, string>,
        },
        (res) => resolve(toFetchResponse(res, Readable)),
      );
      req.on("error", reject);
      writeBody(req, init.body, Readable);
    });
}

/** One `IncomingMessage` as a real `Response`, its body streaming rather than collected - its own
 * function to keep `buildNodeClient`'s own callback within this repo's `max-depth: 2`. */
function toFetchResponse(
  res: import("node:http").IncomingMessage,
  Readable: typeof import("node:stream").Readable,
): Response {
  const headers = new Headers();
  for (const [key, value] of Object.entries(res.headers)) {
    if (value === undefined) continue;
    for (const v of Array.isArray(value) ? value : [value]) headers.append(key, v);
  }
  // `204`/`304` forbid a body on the `Response` constructor; neither is a status this package's own
  // `.fetch()` ever answers with, but a proxy in front of a worker can, and constructing one would
  // throw a `TypeError` the caller could not act on.
  const bodyless = res.statusCode === 204 || res.statusCode === 304;
  return new Response(bodyless ? null : (Readable.toWeb(res) as ReadableStream<Uint8Array>), {
    status: res.statusCode,
    headers,
  });
}

/** Writes whatever `init.body` holds into `req`, streaming a `ReadableStream` rather than collecting
 * it - the duplex half of the adapter. A string closes the request immediately, which is what a
 * one-shot `/transform/<n>` POST wants; a stream keeps it open, which is what `/reduce/<n>` needs. */
function writeBody(
  req: import("node:http").ClientRequest,
  body: RequestInit["body"],
  Readable: typeof import("node:stream").Readable,
): void {
  if (body === undefined || body === null) {
    req.end();
    return;
  }
  if (typeof body === "string") {
    req.end(body);
    return;
  }
  Readable.fromWeb(body as ReadableStream<Uint8Array>).pipe(req);
}
