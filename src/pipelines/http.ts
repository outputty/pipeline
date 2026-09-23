/**
 * `HttpPipeline` sends each chunk of a stage over HTTP to another instance of the same code.
 * `toNodeHandler` mounts its `.fetch` on a Node `http.Server`.
 *
 * Wire format, one route per stage index:
 * ```text
 * POST <mount>/transform/0   { "chunk": [1, 2], "context": { "multiplier": 10 } }
 *                     -> { "chunk": [2, 4] }
 * POST <mount>/reduce/1      NDJSON in: {"context":{…}} then {"chunk":[…]} per chunk
 *                     -> NDJSON out: {"emit":[…]} as values are emitted, {"error":"…"} on failure
 * ```
 */

import type { ConcurrentPipelineOptions } from "@src/pipelines/concurrent";
import { ConcurrentPipeline, parseRoute } from "@src/pipelines/concurrent";
import { Pipeline } from "@src/pipeline";
import type { PipelineConstructorOptions, WrappablePipeline } from "@src/pipeline";
import type { Transformer } from "@src/transformer";
import type {
  IContextManager,
  InternalTransformer,
  ReduceFunction,
  PipelineMode,
  ReduceStage,
  ReduceWork,
  StageRoute,
} from "@src/types";
import { Reducer, foldChunk } from "@src/utils/reduce";
import { defaultClientNow, headersFromNode, type PipelineClient } from "@src/pipelines/client";
import { ndjsonFrame, readNdjsonLines } from "@src/utils/ndjson";
import { applyContextValues, errorMessage, isPlainRecord, toError } from "@src/utils/helpers";
import type { IncomingMessage, ServerResponse } from "node:http";
import { Readable } from "node:stream";

/** Construction-time knobs for `HttpPipeline` and every class that extends it. */
export type HttpPipelineOptions = {
  /** Where the other instance mounts its `.fetch`. */
  url: string;
  /**
   * How a dispatched chunk reaches the other instance. Defaults to the runtime's fastest client.
   *
   * ⚠ Must stream both directions: `/reduce/<n>` is duplex NDJSON, so the `Response` has to resolve
   * on headers.
   */
  client?: PipelineClient;
} & ConcurrentPipelineOptions;

type HttpPipelineConstructorOptions = HttpPipelineOptions & PipelineConstructorOptions;

interface StageRequestBody {
  chunk: unknown[];
  // oxlint-disable-next-line anti-slop/no-unsafe-dictionary-type -- Context is a generic bag by design, unknown until a caller parses it at its own boundary (see .oxlintrc.json)
  context: Record<string, unknown>;
}

interface StageResponseBody<U> {
  chunk: U[];
}

/** The JSON error response every route in the package answers a failure with.
 *
 * `errorResponse(404, "unknown stage 3")` → a 404 with body `{"error":"unknown stage 3"}`. */
export function errorResponse(status: number, message: string): Response {
  return Response.json({ error: message }, { status });
}

/** ⚠ Returns a failure rather than throwing, so `.fetch()` answers a malformed body with a 400
 * instead of a rejected promise. */
async function parseStageRequest(
  request: Request,
): Promise<{ ok: true; value: StageRequestBody } | { ok: false; error: string }> {
  let parsed: unknown;
  try {
    parsed = await request.json();
  } catch {
    return { ok: false, error: "request body is not valid JSON" };
  }

  if (typeof parsed !== "object" || parsed === null) {
    return { ok: false, error: "request body must be a JSON object" };
  }
  const { chunk, context } = parsed as Partial<StageRequestBody>;
  if (!Array.isArray(chunk)) {
    return { ok: false, error: "request body is missing a 'chunk' array" };
  }
  if (!isPlainRecord(context)) {
    return { ok: false, error: "request body is missing a 'context' object" };
  }
  return { ok: true, value: { chunk, context } };
}

async function runReduceStage(
  stage: ReduceStage,
  request: Request,
  ctx: IContextManager,
  writer: WritableStreamDefaultWriter<Uint8Array>,
): Promise<void> {
  try {
    const reducer = new Reducer(stage.fn, stage.initial);
    const lines = readNdjsonLines(request.body!);
    applyContextFrame(await lines.next(), ctx);
    for await (const line of lines) {
      await foldChunkFrame(line, reducer, ctx, writer);
    }
    await flushTrailing(reducer, writer);
  } catch (error) {
    await writer.write(ndjsonFrame({ error: errorMessage(error) }));
  } finally {
    await writer.close();
  }
}

function applyContextFrame(first: IteratorResult<string>, ctx: IContextManager): void {
  // A missing or malformed context frame throws, and the caller gets an `{"error":…}` frame.
  if (first.done) {
    throw new Error("reduce stream ended before a context frame was sent");
  }
  const frame = JSON.parse(first.value) as { context?: unknown };
  if (!isPlainRecord(frame.context)) {
    throw new Error("first reduce frame is missing a 'context' object");
  }
  applyContextValues(ctx, frame.context);
}

/** ⚠ Checks `Array.isArray`, not truthiness: a string `chunk` would fold character by character. */
async function foldChunkFrame(
  line: string,
  reducer: Reducer<unknown, unknown>,
  ctx: IContextManager,
  writer: WritableStreamDefaultWriter<Uint8Array>,
): Promise<void> {
  const frame = JSON.parse(line) as { chunk?: unknown };
  if (!Array.isArray(frame.chunk)) {
    throw new Error("reduce frame is missing a 'chunk' array");
  }
  const emitted = await foldChunk(reducer, frame.chunk, ctx);
  if (emitted.length > 0) {
    await writer.write(ndjsonFrame({ emit: emitted }));
  }
}

/** ⚠ A request that received no chunk sends nothing. The seed for an empty stream comes from
 * `ConcurrentPipeline.reduce`, once per stage; sending it here repeats it once per partition. */
async function flushTrailing(
  reducer: Reducer<unknown, unknown>,
  writer: WritableStreamDefaultWriter<Uint8Array>,
): Promise<void> {
  const trailing = reducer.final();
  if (trailing.length > 0) {
    await writer.write(ndjsonFrame({ emit: trailing }));
  }
}

/**
 * Runs each stage of a chain on another instance of the same code, one HTTP request per chunk.
 * The worker mounts `.fetch`; the caller passes the `url` where it is mounted.
 * `.local(build)` keeps a region in this process.
 *
 * ```ts
 * const doubled = new Pipeline<number>().transform((t) => t.map((x) => x * 2));
 * const worker = new HttpPipeline(doubled, { url: "" });
 * createServer(toNodeHandler(worker.fetch)).listen(port);
 * const caller = new HttpPipeline(doubled, { url: `http://127.0.0.1:${port}` });
 * await caller([1, 2, 3]).toArray(); // [2, 4, 6]
 * ```
 */
export class HttpPipeline<T, In = T> extends ConcurrentPipeline<T, In> {
  protected _url: string;
  /** ⚠ The caller's own value, `undefined` for the default. Resolving it here would bake the
   * default into every copy. */
  protected _client?: PipelineClient;

  /** Wraps a chain built elsewhere. The worker and the caller share one chain definition: the
   * worker mounts `.fetch`, the caller calls the instance with data. */
  constructor(pipeline: WrappablePipeline<T, In>, options: HttpPipelineOptions);
  constructor(options: HttpPipelineConstructorOptions);
  constructor(
    first: WrappablePipeline<T, In> | HttpPipelineConstructorOptions,
    second?: HttpPipelineOptions,
  ) {
    const options = Pipeline.wrapping<HttpPipelineConstructorOptions>(first, second);
    super(options);
    this._url = options.url;
    this._client = options.client;
  }

  /** Tested with `instanceof Promise` for the same reason as `resolveUrl()`. */
  private clientFor(): PipelineClient | Promise<PipelineClient> {
    return this._client ?? defaultClientNow();
  }

  /** Where the worker's `.fetch` is mounted. */
  get url(): string {
    return this._url;
  }

  override transform<U, M2 extends "sync" | "async">(
    builder: (t: Transformer<T, T, "async">) => Transformer<T, U, M2>,
  ): HttpPipeline<U, In> {
    return super.transform(builder) as unknown as HttpPipeline<U, In>;
  }

  override apply<U>(transformer: Transformer<T, U, "sync" | "async">): HttpPipeline<U, In> {
    return super.apply(transformer) as unknown as HttpPipeline<U, In>;
  }

  override reduce<U>(fn: ReduceFunction<U, T>, initial: U): HttpPipeline<U, In> {
    return super.reduce(fn, initial) as unknown as HttpPipeline<U, In>;
  }

  override local<U, M2 extends PipelineMode>(
    build: (p: Pipeline<T, "async", any>) => Pipeline<U, M2, any>,
  ): HttpPipeline<U, In> {
    return super.local(build) as unknown as HttpPipeline<U, In>;
  }

  override queue(capacity: number): HttpPipeline<T, In> {
    return super.queue(capacity) as unknown as HttpPipeline<T, In>;
  }

  /** Carries `url` and `client` into the next copy-on-write instance. */
  protected override carriedKnobs(): HttpPipelineOptions {
    return { ...super.carriedKnobs(), url: this._url, client: this._client };
  }

  /**
   * The worker side: serves this chain's stages at `/transform/<n>` and `/reduce/<n>`. It reads
   * only the trailing route, so a framework may mount it under any prefix. An unknown stage answers
   * 404, a malformed body 400, and a stage that throws 500 with its message.
   *
   * `fetch(new Request("http://x/transform/0", { method: "POST", body: JSON.stringify({ chunk:
   * [1, 2], context: {} }) }))` → `{"chunk":[2,4]}` for a `.map((x) => x * 2)` stage 0.
   */
  readonly fetch = async (request: Request): Promise<Response> => {
    const { pathname } = new URL(request.url);
    const route = parseRoute(pathname);

    if (route?.verb === "reduce") {
      return this.serveReduceRequest(route, request);
    }

    // ⚠ Answered before `resolveRegistries()`, which runs each `.local()` region's `build` and can
    // throw. A throw there turns this 404 into a rejected promise.
    if (route === null) {
      return errorResponse(404, `unknown stage ${pathname}`);
    }
    const lookup = this.lookupTransformStage(route, pathname);
    if (!lookup.ok) {
      return errorResponse(404, lookup.error);
    }

    const body = await parseStageRequest(request);
    if (!body.ok) {
      return errorResponse(400, body.error);
    }

    try {
      const ctx = this.applyContext(body.value.context);
      const result = await lookup.stage(body.value.chunk, ctx);
      return Response.json({ chunk: result } satisfies StageResponseBody<unknown>);
    } catch (error) {
      return errorResponse(500, errorMessage(error));
    }
  };

  private async serveReduceRequest(route: StageRoute, request: Request): Promise<Response> {
    // ⚠ A branch trail resolves to the arm's own registry. The parent's registry would serve the
    // parent's fold instead.
    const lookup = this.lookupReduceStage(route, route.trail);
    if (!lookup.ok) {
      return errorResponse(404, lookup.error);
    }
    const stage = lookup.stage;
    if (!request.body) {
      return errorResponse(400, "request body is missing");
    }

    const { readable, writable } = new TransformStream<Uint8Array, Uint8Array>();
    const writer = writable.getWriter();
    // ⚠ Not awaited: awaiting holds every emit back until the whole fold finishes.
    runReduceStage(stage, request, this._context, writer).catch(() => writer.abort());

    return new Response(readable, { headers: { "content-type": "application/x-ndjson" } });
  }

  /**
   * The url one dispatch uses, plus a `release` to call once it settles. `ClusterHttpPipeline`
   * overrides it to start its workers.
   *
   * ⚠ Returned per call, never written to a field: concurrent dispatches would race on one field.
   * Callers test it with `instanceof Promise`, never `isThenable`: that helper runs on every chunk,
   * and handing it this object shape slows every chunk.
   */
  protected resolveUrl(): ResolvedUrl | Promise<ResolvedUrl> {
    return { url: this._url, release: noRelease };
  }

  /** Sends each chunk to the worker's `/transform/<n>` and returns the worker's result. The worker
   * runs its own copy of the stage, so `transformer` is unused. */
  protected override stageWork<U>(
    _transformer: Transformer<T, U, "sync" | "async">,
    stageIndex: number,
  ): InternalTransformer<T, U> {
    return async (chunk, ctx) => {
      const resolved = this.resolveUrl();
      const { url, release } = resolved instanceof Promise ? await resolved : resolved;
      try {
        const clientOrPending = this.clientFor();
        const client = clientOrPending instanceof Promise ? await clientOrPending : clientOrPending;
        const response = await client(`${url}${this.routePath("transform", stageIndex)}`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ chunk, context: ctx.toDict() } satisfies StageRequestBody),
        });

        if (!response.ok) {
          const detail = await errorDetailOf(response);
          throw new Error(`stage ${stageIndex} at ${url} failed: ${detail}`);
        }

        const body = (await response.json()) as StageResponseBody<U>;
        return body.chunk;
      } finally {
        release();
      }
    };
  }

  /**
   * Streams the whole reduce stage over one duplex request to the worker's `/reduce/<n>`, yielding
   * each emit as it arrives. An `{"error":…}` frame arrives after the 200, so earlier emits have
   * already gone downstream.
   */
  protected override reduceWork<U>(
    _fn: ReduceFunction<U, T>,
    _initial: U,
    stageIndex: number,
  ): ReduceWork<T, U> {
    const self = this;

    return async function* dispatchReduce(chunks, ctx) {
      const resolved = self.resolveUrl();
      // One dispatch holds the whole reduce stream, released once the stream ends or is abandoned.
      const { url, release } = resolved instanceof Promise ? await resolved : resolved;
      try {
        const path = self.routePath("reduce", stageIndex);
        const label = `reduce stage ${stageIndex} at ${url}`;

        // ⚠ Built before the client call is awaited, or the request finishes before the response
        // starts.
        const requestBody = buildReduceRequestBody(chunks, ctx);

        // One client serves both routes.
        const clientOrPending = self.clientFor();
        const client = clientOrPending instanceof Promise ? await clientOrPending : clientOrPending;
        const response = await client(`${url}${path}`, {
          method: "POST",
          headers: { "content-type": "application/x-ndjson" },
          body: requestBody,
          // undici requires `duplex: "half"` on a streaming body.
          duplex: "half",
        } as RequestInit);

        if (!response.ok || !response.body) {
          const detail = await errorDetailOf(response);
          throw new Error(`${label} failed: ${detail}`);
        }

        yield* parseReduceFrames<U>(response.body, label);
      } finally {
        release();
      }
    };
  }
}

/** The url one dispatch posts to, plus a `release` the dispatch calls once it settles.
 *
 * `resolveUrl()` → `{ url: "http://localhost:41234", release }`. */
export interface ResolvedUrl {
  url: string;
  release: () => void;
}

const noRelease = (): void => {};

/** ⚠ Pull-driven. Draining `chunks` in `start` lets every partition of a shared stream pull the
 * whole source into memory before the server folds anything. */
function buildReduceRequestBody<T>(
  chunks: AsyncIterable<T[]>,
  ctx: IContextManager,
): ReadableStream<Uint8Array> {
  const upstream = chunks[Symbol.asyncIterator]();
  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(ndjsonFrame({ context: ctx.toDict() }));
    },
    async pull(controller) {
      const next = await upstream.next();
      if (next.done === true) {
        controller.close();
        return;
      }
      controller.enqueue(ndjsonFrame({ chunk: next.value }));
    },
    async cancel() {
      // Releases this partition's view of the shared stream.
      await upstream.return?.();
    },
  });
}

async function* parseReduceFrames<U>(
  body: ReadableStream<Uint8Array>,
  label: string,
): AsyncGenerator<U[]> {
  for await (const line of readNdjsonLines(body)) {
    const frame = JSON.parse(line) as { emit?: U[]; error?: string };
    if (frame.error !== undefined) {
      throw new Error(`${label} failed: ${frame.error}`);
    }
    if (frame.emit !== undefined && frame.emit.length > 0) {
      yield frame.emit;
    }
  }
}

async function errorDetailOf(response: Response): Promise<string> {
  try {
    const body = (await response.json()) as { error?: string };
    return body.error ?? `HTTP ${response.status}`;
  } catch {
    return `HTTP ${response.status}`;
  }
}

/**
 * Serves a `.fetch` handler from a Node `http.Server`, streaming both bodies. Node has no native
 * fetch-handler server; Bun, Deno and Cloudflare need no bridge.
 *
 * `createServer(toNodeHandler(pipeline.fetch)).listen(0)` → a server answering every route
 * `pipeline.fetch` serves.
 */
export function toNodeHandler(
  handler: (request: Request) => Promise<Response>,
): (req: IncomingMessage, res: ServerResponse) => void {
  return (req, res) => {
    // oxlint-disable-next-line anti-slop/no-unknown-parameters -- a rejection can carry anything JS can throw; genuinely unknown, not a gap
    handleOverBridge(req, res, handler).catch((error: unknown) => {
      // ⚠ A handler that rejects would otherwise leave the client waiting forever.
      if (!res.headersSent) {
        res.statusCode = 500;
        res.end(errorMessage(error));
      }
    });
  };
}

/** ⚠ Waits for `'drain'` when the socket backs up. Writing regardless piles a fast reduce stage's
 * emits into memory against a slow client. */
async function writeStreamedBody(res: ServerResponse, bodyStream: Readable): Promise<void> {
  for await (const chunk of bodyStream) {
    if (res.destroyed) break;
    const ok = res.write(chunk as Buffer);
    if (!ok && !res.destroyed) {
      // ⚠ Waits on 'close' too: a client gone while backed up never fires 'drain'. Both listeners
      // come off when either fires; `once` alone leaks one per wait and trips
      // `MaxListenersExceededWarning`.
      await new Promise<void>((resolve) => {
        const settle = (): void => {
          res.removeListener("drain", settle);
          res.removeListener("close", settle);
          resolve();
        };
        res.once("drain", settle);
        res.once("close", settle);
      });
    }
  }
  res.end();
}

/** ⚠ Streams the body in, so a reduce stage folds early chunks before later ones arrive. Whether
 * a body exists is decided by method, since GET/HEAD with a body makes `Request` throw. */
function nodeRequestToFetchRequest(req: IncomingMessage): Request {
  const hasBody = req.method !== "GET" && req.method !== "HEAD";
  return new Request(new URL(req.url ?? "/", `http://${req.headers.host ?? "localhost"}`), {
    method: req.method,
    headers: headersFromNode(req.headers),
    body: hasBody ? (Readable.toWeb(req) as ReadableStream<Uint8Array>) : undefined,
    // undici requires `duplex: "half"` whenever a body is passed.
    duplex: "half",
  });
}

async function handleOverBridge(
  req: IncomingMessage,
  res: ServerResponse,
  handler: (request: Request) => Promise<Response>,
): Promise<void> {
  const response = await handler(nodeRequestToFetchRequest(req));
  res.statusCode = response.status;
  response.headers.forEach((value, key) => res.setHeader(key, value));

  if (!response.body) {
    res.end();
    return;
  }

  // ⚠ Streamed, never buffered: a buffered `/reduce/<n>` response holds every emit until the fold
  // ends.
  const bodyStream = Readable.fromWeb(response.body as ReadableStream<Uint8Array>);
  // ⚠ A client gone mid-stream stops the handler; otherwise it writes to a dead socket forever.
  res.once("close", () => bodyStream.destroy());

  try {
    await writeStreamedBody(res, bodyStream);
  } catch (error) {
    // ⚠ Once headers are sent, a failure destroys the connection. An error response is no longer
    // possible, and leaving it hangs the client.
    if (res.headersSent) {
      res.destroy(toError(error));
    } else {
      res.statusCode = 500;
      res.end(errorMessage(error));
    }
  }
}
