/**
 * The HTTP client an `HttpPipeline` sends each dispatched chunk and reduce stream through.
 * `defaultClient()` picks `node:http` where it exists, and the global `fetch` elsewhere.
 */

/**
 * How a dispatched chunk reaches another instance. It has the global `fetch` signature, so `fetch`
 * itself fits. It is named `client` because `HttpPipeline.fetch` is the server handler.
 *
 * ⚠ A custom client must stream both directions: `/reduce/<n>` needs the `Response` on its headers
 * while the request body is still being sent. A buffering client passes `/transform/<n>` and
 * breaks reduce.
 *
 * `client("http://worker/transform/0", { method: "POST", body })` → the worker's `Response`.
 */
export type PipelineClient = (url: string, init: RequestInit) => Promise<Response>;

/** The global `fetch` as a `PipelineClient`, for any runtime.
 *
 * ⚠ Keep `duplex: "half"`: Node's `fetch` refuses a streamed body without it.
 *
 * `fetchClient(url, { method: "POST", body: stream })` → the `Response`, the body streamed. */
export const fetchClient: PipelineClient = (url, init) =>
  fetch(url, { ...init, duplex: "half" } as RequestInit);

/** Set to `1` to make `defaultClient()` return `fetchClient`, so a test can reach the fallback. */
const FORCE_FETCH = "OUTPUTTY_PIPELINE_FORCE_FETCH";

let resolved: Promise<PipelineClient> | null = null;
/** `resolved`'s value once it has settled. */
let settled: PipelineClient | null = null;

/**
 * The client an `HttpPipeline` uses when the caller names none: `node:http` with a keep-alive agent
 * where it exists, `fetchClient` elsewhere. It is resolved once per process.
 *
 * `await defaultClient()` → the `node:http` client on Node, `fetchClient` on a runtime without
 * `node:http`.
 */
export function defaultClient(): Promise<PipelineClient> {
  resolved ??= resolveClient().then((client) => (settled = client));
  return resolved;
}

/** `defaultClient()`, returned as a plain value once it has resolved, so a dispatch after the first
 * awaits nothing.
 *
 * `defaultClientNow()` → a `Promise` on the first call; the client itself once that settles. */
export function defaultClientNow(): PipelineClient | Promise<PipelineClient> {
  return settled ?? defaultClient();
}

async function resolveClient(): Promise<PipelineClient> {
  const env = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process
    ?.env;
  if (env?.[FORCE_FETCH] === "1") {
    return fetchClient;
  }
  try {
    // ⚠ Both imports stay dynamic, inside this `try`: a static one fails the module on a runtime
    // without it.
    const http = await import("node:http");
    const { Readable } = await import("node:stream");
    return buildNodeClient(new http.Agent({ keepAlive: true }), http.request, Readable);
  } catch {
    return fetchClient;
  }
}

/** ⚠ Must stream both directions: resolve on the response headers and stream `init.body`, never
 * collect either. Buffering passes `/transform/<n>` and breaks `/reduce/<n>`. */
function buildNodeClient(
  agent: import("node:http").Agent,
  request: typeof import("node:http").request,
  Readable: typeof import("node:stream").Readable,
): PipelineClient {
  return (url, init) => {
    const target = new URL(url);
    // ⚠ `node:http` is cleartext only: an `https:` url here would send the chunk unencrypted to
    // port 80. Anything but `http:` goes to `fetch`.
    if (target.protocol !== "http:") {
      return fetchClient(url, init);
    }
    return new Promise<Response>((resolve, reject) => {
      const req = request(
        {
          agent,
          // ⚠ Strip IPv6 brackets: `node:http` looks up `"[::1]"` verbatim and fails.
          hostname: target.hostname.replace(/^\[|\]$/g, ""),
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
  };
}

function toFetchResponse(
  res: import("node:http").IncomingMessage,
  Readable: typeof import("node:stream").Readable,
): Response {
  const headers = headersFromNode(res.headers);
  // ⚠ `new Response(body)` throws for `204`, `205` and `304`, and a proxy can send them.
  const bodyless = res.statusCode === 204 || res.statusCode === 205 || res.statusCode === 304;
  if (bodyless) {
    // ⚠ Drain it: an unread reply never returns its socket to the keep-alive pool.
    res.resume();
    return new Response(null, { status: res.statusCode, headers });
  }
  return new Response(Readable.toWeb(res) as ReadableStream<Uint8Array>, {
    status: res.statusCode,
    headers,
  });
}

export function headersFromNode(
  nodeHeaders: Record<string, string | string[] | undefined>,
): Headers {
  const headers = new Headers();
  for (const [key, value] of Object.entries(nodeHeaders)) {
    if (value === undefined) continue;
    for (const v of Array.isArray(value) ? value : [value]) headers.append(key, v);
  }
  return headers;
}

/** ⚠ Links a streamed body's failure to `req` both ways, which a bare `.pipe()` does not. A
 * failing source must reject the request, not crash the process. A failing request must cancel
 * the source. */
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
  const source = Readable.fromWeb(body as ReadableStream<Uint8Array>);
  source.on("error", (error: Error) => req.destroy(error));
  req.on("close", () => source.destroy());
  source.pipe(req);
}
