import type { ConcurrentPipelineOptions } from "@src/pipelines/concurrent";
import { ConcurrentPipeline } from "@src/pipelines/concurrent";
import { Pipeline } from "@src/pipeline";
import type { PipelineConstructorOptions, WrappablePipeline } from "@src/pipeline";
import type { Transformer } from "@src/transformer";
import type {
  IContextManager,
  InternalTransformer,
  ReduceFunction,
  PipelineMode,
  ChunkTransform,
  ReduceStage,
  ReduceWork,
  RouteVerb,
  StageRoute,
} from "@src/types";
import { Reducer, foldChunk } from "@src/utils/reduce";
import { ndjsonFrame, readNdjsonLines } from "@src/utils/ndjson";
import type { IncomingMessage, ServerResponse } from "node:http";
import { Readable } from "node:stream";

/** Construction-time knobs for `HttpPipeline` and every class that extends it: the url where this
 * instance's `.fetch` handler will be mounted, plus every knob `ConcurrentPipeline` itself accepts. */
export type HttpPipelineOptions = { url: string } & ConcurrentPipelineOptions;

/** The full constructor argument shape used internally: `HttpPipelineOptions` plus `Pipeline`'s own
 * internal construction fields, since a subclass constructor must accept everything the base
 * constructor does too. */
type HttpPipelineConstructorOptions = HttpPipelineOptions & PipelineConstructorOptions;

/** The body `stageWork()` POSTs, and `.fetch()` (below) expects on the way in. */
interface StageRequestBody {
  /** The chunk's own items, not yet validated against the stage's real input type. */
  chunk: unknown[];
  /** The dispatching side's own context values, applied onto the receiving instance's context
   * manager via `.set()`. */
  // oxlint-disable-next-line anti-slop/no-unsafe-dictionary-type -- Context is a generic bag by design, unknown until a caller parses it at its own boundary (see .oxlintrc.json)
  context: Record<string, unknown>;
}

/** The body `.fetch()` returns on success, and `stageWork()` expects on the way back. */
interface StageResponseBody<U> {
  /** The stage's own output items. */
  chunk: U[];
}

/**
 * Builds the one JSON error-body shape every failure response in this file uses.
 *
 * Exported so `ClusterPipeline`'s own worker server, which routes to a `.fetch()` this file
 * builds, can answer an unknown route with the identical shape rather than its own spelling.
 */
export function errorResponse(status: number, message: string): Response {
  return Response.json({ error: message }, { status });
}

/** The 404 both `.fetch()`'s `stage` verb and `serveReduceRequest()` answer with when
 * `resolveRegistries(trail)` finds no arm this deployment holds. */
function unknownBranchRoute(trail: string | null): Response {
  return errorResponse(404, `unknown branch route ${trail}`);
}

/**
 * Parses and validates a stage POST body, so malformed JSON or a missing `chunk`/`context` field
 * fails here rather than reaching the stage's own transform with a half-formed value.
 *
 * Returns a discriminated result rather than throwing, so `.fetch()` (above) can turn a failure
 * into an ordinary 400 response instead of an unhandled rejection that would hang the client.
 */
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
  if (typeof context !== "object" || context === null || Array.isArray(context)) {
    return { ok: false, error: "request body is missing a 'context' object" };
  }
  return { ok: true, value: { chunk, context } };
}

/**
 * Runs one reduce stage's whole stream against `request.body`'s NDJSON frames, writing `{"emit":…}`
 * frames to `writer` as they happen and `{"error":…}` on a mid-stream failure.
 *
 * `serveReduceRequest` (below) is the one caller, kicking this off unawaited so the `Response` it
 * returns can start streaming immediately rather than after the whole fold finishes.
 */
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
    const message = error instanceof Error ? error.message : String(error);
    await writer.write(ndjsonFrame({ error: message }));
  } finally {
    await writer.close();
  }
}

/** Applies the wire's own first frame, `{"context":{…}}`, sent exactly once, onto `ctx` via
 * `.set()` - the same context merge a `/transform/<n>` request performs. */
function applyContextFrame(first: IteratorResult<string>, ctx: IContextManager): void {
  // Fails loud rather than silently dropping a malformed or absent first frame - thrown here,
  // inside `runReduceStage`'s own try, so it reaches the caller as a normal `{"error":…}` frame.
  if (first.done) {
    throw new Error("reduce stream ended before a context frame was sent");
  }
  const frame = JSON.parse(first.value) as { context?: unknown };
  if (typeof frame.context !== "object" || frame.context === null || Array.isArray(frame.context)) {
    throw new Error("first reduce frame is missing a 'context' object");
  }
  // oxlint-disable-next-line anti-slop/no-unsafe-dictionary-type -- Context is a generic bag by design, unknown until a caller parses it at its own boundary (see .oxlintrc.json)
  for (const [key, value] of Object.entries(frame.context as Record<string, unknown>)) {
    ctx.set(key, value);
  }
}

/**
 * Folds one `{"chunk":[…]}` frame: every value `reducer.fold()` emits while folding this chunk is
 * batched into a single `{"emit":…}` frame, sent once the whole chunk has folded.
 *
 * A missing or non-array `chunk` fails loud rather than being iterated as if it were one.
 */
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

/** Sends the final accumulator as its own `{"emit":…}` frame, only if items were folded since the
 * last emit. */
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
 * Reads back the route grammar `HttpPipeline.routePath()` builds: `/transform/<n>`, `/reduce/<n>`,
 * optionally prefixed by a `/branch/<i>/<name>` trail, with `ClusterPipeline`'s own `/pipeline/<i>`
 * ahead of all of it. Written and parsed in one file so the two cannot drift.
 *
 * Matches only the trailing segment rather than anchoring at the start, since a mounting framework
 * (or `ClusterPipeline`'s own prefix) may hand this function a longer pathname than it produced.
 *
 * `parseRoute("/pipeline/0/branch/1/big/transform/2")` →
 * `{ trail: "/branch/1/big", verb: "transform", index: 2 }`.
 */
function parseRoute(pathname: string): StageRoute | null {
  const match = /(\/branch\/\d+\/[^/]+)?\/(transform|reduce)\/(\d+)$/.exec(pathname);
  if (match === null) return null;
  return {
    trail: match[1] ?? null,
    verb: match[2] as RouteVerb,
    index: Number(match[3]),
  };
}

/**
 * Each chunk of a stage dispatched over HTTP to another instance running the same code, so one
 * chain definition serves as both the triggering caller and the worker that executes a stage.
 *
 * Mounts one route per stage index via its `fetch` handler; the caller supplies the url where that
 * handler is reachable. `.local(build)` keeps a whole region of the chain in this process instead.
 *
 * `new HttpPipeline([1,2,3,4,5], { url }).transform((t) => t.map((x) => x * 2)).toArray()` →
 * `[2,4,6,8,10]`, across two real instances.
 */
export class HttpPipeline<T, In = T> extends ConcurrentPipeline<T, In> {
  protected _url: string;

  /** Wraps a chain built elsewhere so its stages dispatch over HTTP instead of running here - the
   * shared shape that lets one definition serve as both the WORKER, which mounts `.fetch` with no
   * data of its own, and the TRIGGER, which calls the same wrapper with real data. */
  constructor(pipeline: WrappablePipeline<T, In>, options: HttpPipelineOptions);
  constructor(options: HttpPipelineConstructorOptions);
  constructor(
    first: WrappablePipeline<T, In> | HttpPipelineConstructorOptions,
    second?: HttpPipelineOptions,
  ) {
    const options = Pipeline.wrapping<HttpPipelineConstructorOptions>(first, second);
    super(options);
    this._url = options.url;
  }

  /** Where this instance's `.fetch` is mounted - the url another `HttpPipeline`/`ClusterPipeline`
   * POSTs a stage's chunk to.
   *
   * Set at construction; `ClusterPipeline` overwrites it once its own worker set picks a real
   * port. */
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

  /** Carries `url` into the next instance a copy-on-write call builds, on top of whatever
   * `ConcurrentPipeline.carriedKnobs()` already carries forward. */
  protected override carriedKnobs(): HttpPipelineOptions {
    return { ...super.carriedKnobs(), url: this._url };
  }

  /**
   * Serves one stage's chunk of work over HTTP: an unknown stage index 404s naming the range this
   * deployment actually serves, and a stage that throws 500s with the error message.
   *
   * Reads only the trailing `/transform/<n>` segment of the path, never anchoring at the start, so
   * a framework's own `.mount()` prefix rewrite never breaks routing.
   *
   * `fetch(new Request("http://x/transform/0", { method: "POST", body: JSON.stringify({ chunk: [1,2],
   * context: {} }) }))` → `{ chunk: [2,4] }` (a `.map((x) => x*2)` stage 0).
   */
  readonly fetch = async (request: Request): Promise<Response> => {
    const { pathname } = new URL(request.url);
    const route = parseRoute(pathname);

    if (route?.verb === "reduce") {
      return this.serveReduceRequest(route.index, request, route.trail);
    }

    // Checked before `registries()`, which can replay a whole deferred chain - an unrelated
    // request should not pay that cost, or turn a would-be 404 into a rejected promise.
    if (route === null) {
      return errorResponse(404, `unknown stage ${pathname}`);
    }
    const requested = route.index;

    // `registries()`, not `_chunkTransforms` directly: a worker holds a chain and never binds an
    // input, so its stages are still recorded calls until something replays them.
    const resolved = this.resolveRegistries(route.trail);
    if (resolved === null) {
      return unknownBranchRoute(pathname);
    }
    const { chunkTransforms } = resolved;
    const maxIndex = chunkTransforms.length - 1;
    if (requested > maxIndex) {
      return errorResponse(
        404,
        `unknown stage ${requested}; this deployment serves 0..${maxIndex}`,
      );
    }

    const body = await parseStageRequest(request);
    if (!body.ok) {
      return errorResponse(400, body.error);
    }

    try {
      // Reuses `this._context` - the same instance the constructor built - instead of building a
      // fresh manager from the wire every request; a rejecting manager's throw reaches the same
      // catch below as a stage's own transform error.
      const ctx = this._context;
      for (const [key, value] of Object.entries(body.value.context)) {
        ctx.set(key, value);
      }
      const result = await chunkTransforms[requested](body.value.chunk, ctx);
      return Response.json({ chunk: result } satisfies StageResponseBody<unknown>);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return errorResponse(500, message);
    }
  };

  /**
   * Serves one reduce stage's whole stream over one duplex connection - an unknown index means the
   * stack was never given a reducer at that position, never a stage that simply isn't one (that
   * case 404s in `.fetch()`'s own range check).
   *
   * Streams the response (`TransformStream`) so an emit reaches the caller as it happens, rather
   * than only after the whole fold completes.
   */
  private async serveReduceRequest(
    index: number,
    request: Request,
    trail: string | null,
  ): Promise<Response> {
    // `registries()`, same reason `.fetch()` uses it above: a worker's reduce stages are recorded
    // calls until something replays them. A `/branch/<i>/<name>/` trail resolves into the arm's
    // own registry rather than the parent's.
    const resolved = this.resolveRegistries(trail);
    if (resolved === null) {
      return unknownBranchRoute(trail);
    }
    const { reduceStages } = resolved;
    const stage = reduceStages.get(index);
    if (!stage) {
      const known = [...reduceStages.keys()].join(",") || "none";
      return errorResponse(404, `unknown reduce stage ${index}; this deployment serves ${known}`);
    }
    // A registered stage with no body is a different problem than an unknown one, reported as its
    // own 400 - the same shape `/transform/<n>`'s own `parseStageRequest()` uses for a missing body.
    if (!request.body) {
      return errorResponse(400, "request body is missing");
    }

    const { readable, writable } = new TransformStream<Uint8Array, Uint8Array>();
    const writer = writable.getWriter();
    // Not awaited: the Response below must return immediately so the client starts receiving
    // frames as `runReduceStage` writes them, rather than after the whole fold finishes.
    runReduceStage(stage, request, this._context, writer).catch(() => writer.abort());

    return new Response(readable, { headers: { "content-type": "application/x-ndjson" } });
  }

  /**
   * The outgoing path for `verb`/`index` - and, via `.fetch()`'s own trailing-segment match, the
   * incoming one too - one shared stage-index space for both `/transform/<n>` and `/reduce/<n>`.
   *
   * `ClusterPipeline` overrides this alone to route several pipeline definitions through one
   * shared worker server (`/pipeline/<i>/<verb>/<n>`), leaving dispatch and parsing untouched.
   */
  protected routePath(verb: RouteVerb, index: number): string {
    return `${this._routeTrail}/${verb}/${index}`;
  }

  /** This pipeline's own stage registries, or an arm's when `trail` names one - `null` when
   * `trail` names no arm this deployment holds. The one place `.fetch()` and
   * `serveReduceRequest()` both resolve a trail, instead of each spelling the same lookup. */
  private resolveRegistries(
    trail: string | null,
  ): { chunkTransforms: ChunkTransform[]; reduceStages: Map<number, ReduceStage> } | null {
    return trail === null ? this.registries() : this.registriesFor(trail);
  }

  /**
   * POSTs the chunk to `${url}${routePath("transform", stageIndex)}` instead of running it
   * in-process; `ConcurrentPipeline.apply()` calls this for every stage, with the fan-out
   * otherwise unchanged.
   *
   * `transformer` itself goes unused - a dispatching class sends a chunk plus an index, never a
   * function, and the receiving instance's own `.fetch()` is what actually runs the stage.
   */
  protected override stageWork<U>(
    _transformer: Transformer<T, U, "sync" | "async">,
    stageIndex: number,
  ): InternalTransformer<T, U> {
    return async (chunk, ctx) => {
      const response = await fetch(`${this._url}${this.routePath("transform", stageIndex)}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ chunk, context: ctx.toDict() } satisfies StageRequestBody),
      });

      if (!response.ok) {
        const detail = await errorDetailOf(response);
        throw new Error(`stage ${stageIndex} at ${this._url} failed: ${detail}`);
      }

      const body = (await response.json()) as StageResponseBody<U>;
      return body.chunk;
    };
  }

  /**
   * Opens one duplex POST to `${url}${routePath("reduce", stageIndex)}` for the whole stream,
   * rather than folding in-process: the accumulator lives in the connection for its life, so this
   * is the one override that changes WHERE a reducer runs, `stageWork()`'s own sibling for reduce.
   *
   * `{"emit":[…]}` frames arrive as they happen, and `{"error":"…"}` on a mid-stream failure
   * arrives after the response's own 200 - values already emitted have already reached downstream
   * stages.
   */
  protected override reduceWork<U>(
    _fn: ReduceFunction<U, T>,
    _initial: U,
    stageIndex: number,
  ): ReduceWork<T, U> {
    const self = this;

    return async function* dispatchReduce(chunks, ctx) {
      // `self._url`/`routePath()` are read HERE, at dispatch time, not captured before this
      // function returns: `ClusterPipeline` sets `self._url` to its bootstrapped port after this
      // method returns but before this generator actually runs.
      const url = self._url;
      const path = self.routePath("reduce", stageIndex);

      // Built (not yet consumed) before the `fetch()` call below is awaited, so the connection is
      // genuinely duplex rather than the request finishing before the response starts.
      const requestBody = buildReduceRequestBody(chunks, ctx);

      const response = await fetch(`${url}${path}`, {
        method: "POST",
        headers: { "content-type": "application/x-ndjson" },
        body: requestBody,
        // Required by Node's undici Request whenever a streaming body is passed - same reason
        // `handleOverBridge` (`toNodeHandler`) sets it unconditionally on the server side.
        duplex: "half",
      } as RequestInit);

      if (!response.ok || !response.body) {
        const detail = await errorDetailOf(response);
        throw new Error(`reduce stage ${stageIndex} at ${url} failed: ${detail}`);
      }

      yield* parseReduceFrames<U>(response.body, stageIndex, url);
    };
  }
}

/**
 * The reduce wire's own outgoing half: one `{"context":…}` frame, then one `{"chunk":…}` NDJSON
 * frame per upstream chunk.
 *
 * Driven by `pull`, never draining the whole stream inside `start`: `ConcurrentPipeline.reduce()`
 * calls this closure `maxConcurrency` times over one shared iterator, so eagerly draining it would
 * let each partition race to pull the whole source into its own queue before the server folds
 * anything, defeating `share()`'s own free-slot dealing between partitions.
 */
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
      // The consumer stopped reading - a failed request, an aborted response - so this partition's
      // view of the shared iterator is released rather than left open.
      await upstream.return?.();
    },
  });
}

/** The reduce wire's own incoming half: `{"emit":…}` frames as a remote fold produces them, and
 * `{"error":…}` on a mid-stream failure - arriving after the response's own 200, so values already
 * emitted have already entered downstream stages. */
async function* parseReduceFrames<U>(
  body: ReadableStream<Uint8Array>,
  stageIndex: number,
  url: string,
): AsyncGenerator<U[]> {
  for await (const line of readNdjsonLines(body)) {
    const frame = JSON.parse(line) as { emit?: U[]; error?: string };
    if (frame.error !== undefined) {
      throw new Error(`reduce stage ${stageIndex} at ${url} failed: ${frame.error}`);
    }
    if (frame.emit !== undefined && frame.emit.length > 0) {
      yield frame.emit;
    }
  }
}

/** The error message a stage's own `.fetch()` sent back on a non-2xx response, or the plain HTTP
 * status when the body was not the `{ error }` JSON shape `.fetch()` sends - a proxy's own error
 * page, say. */
async function errorDetailOf(response: Response): Promise<string> {
  try {
    const body = (await response.json()) as { error?: string };
    return body.error ?? `HTTP ${response.status}`;
  } catch {
    return `HTTP ${response.status}`;
  }
}

/**
 * Bridges a `.fetch` handler to `http.Server`'s `(req, res)` callback - Node exposes
 * `Request`/`Response`/`fetch` but serves no fetch handler natively, so nothing else in this
 * package's runtime-neutral surface can reach a plain `node:http` server on its own.
 *
 * Bun, Deno and Cloudflare need nothing; this bridge exists for Node only.
 *
 * `createServer(toNodeHandler(pipeline.fetch)).listen(0)`.
 */
export function toNodeHandler(
  handler: (request: Request) => Promise<Response>,
): (req: IncomingMessage, res: ServerResponse) => void {
  return (req, res) => {
    // oxlint-disable-next-line anti-slop/no-unknown-parameters -- a rejection can carry anything JS can throw; genuinely unknown, not a gap
    handleOverBridge(req, res, handler).catch((error: unknown) => {
      // A last-resort net: `handler` itself is expected to catch its own errors into a Response
      // (`HttpPipeline.fetch` does), but nothing upstream of this bridge can assume that of every
      // caller's own handler. Leaving this uncaught would turn a Node bridge into a hung client
      // connection and an unhandled rejection.
      if (!res.headersSent) {
        res.statusCode = 500;
        res.end(error instanceof Error ? error.message : String(error));
      }
    });
  };
}

/**
 * Writes `bodyStream` to `res` chunk by chunk, stopping early once the client has gone
 * (`res.destroyed`) instead of pulling from `bodyStream` forever against a dead socket.
 *
 * Waits for `'drain'` whenever `res.write()` reports the socket is backed up, rather than pulling
 * the next chunk immediately regardless - otherwise a fast producer piles up unboundedly in Node's
 * own internal write buffer against a slow client.
 */
async function writeStreamedBody(res: ServerResponse, bodyStream: Readable): Promise<void> {
  for await (const chunk of bodyStream) {
    if (res.destroyed) break;
    const ok = res.write(chunk as Buffer);
    if (!ok && !res.destroyed) {
      // Races 'close' alongside 'drain' - a client that disconnects while backed up never fires
      // 'drain' on a destroyed socket, which would otherwise hang this wait forever.
      //
      // Both listeners come off when either fires: `once` alone removes only the one that fired,
      // so the loser would stay attached to a response that outlives this wait, piling up one
      // retained listener per backpressure stall on a long-lived connection.
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

/**
 * Converts a Node `IncomingMessage` into a real `Request`.
 *
 * Streams the request body in as it arrives, rather than buffering it whole first, so `handler` (a
 * reduce stage's `.fetch()`, say) can start folding an early chunk before a later one has even
 * arrived. `GET`/`HEAD` carry no body - the Fetch spec's own `Request` constructor throws
 * otherwise - so gating on method, never on whether anything was ever read, is what streaming
 * needs.
 */
function nodeRequestToFetchRequest(req: IncomingMessage): Request {
  const headers = new Headers();
  for (const [key, value] of Object.entries(req.headers)) {
    if (value === undefined) continue;
    for (const v of Array.isArray(value) ? value : [value]) headers.append(key, v);
  }

  const hasBody = req.method !== "GET" && req.method !== "HEAD";
  return new Request(new URL(req.url ?? "/", `http://${req.headers.host ?? "localhost"}`), {
    method: req.method,
    headers,
    body: hasBody ? (Readable.toWeb(req) as ReadableStream<Uint8Array>) : undefined,
    // Required by Node's undici Request whenever a body is passed - harmless when body is
    // undefined, so set unconditionally rather than branching on it.
    duplex: "half",
  });
}

/**
 * Runs one Node request through `handler` and writes the resulting `Response` back to `res`,
 * streaming the response body out as it arrives rather than buffering it whole first.
 *
 * A response-body failure after bytes are already flushed destroys the connection instead of
 * hanging the client, since headers already sent rules out a fresh error response.
 */
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

  // Streams the response body out as it arrives, instead of buffering it whole first - a one-shot
  // handler (`/transform/<n>`) works identically either way, since its body is one chunk
  // regardless; a reduce stage's duplex response (`/reduce/<n>`) is what this actually unblocks.
  const bodyStream = Readable.fromWeb(response.body as ReadableStream<Uint8Array>);
  // A client that disconnects mid-stream must stop this loop pulling from `handler`'s own
  // generator, not run it to completion against a dead socket.
  res.once("close", () => bodyStream.destroy());

  try {
    await writeStreamedBody(res, bodyStream);
  } catch (error) {
    // A failure reading `response.body` after at least one chunk was already written can't become
    // a fresh error response - the client already has a 200 - so the connection is destroyed
    // instead.
    if (res.headersSent) {
      res.destroy(error instanceof Error ? error : new Error(String(error)));
    } else {
      res.statusCode = 500;
      res.end(error instanceof Error ? error.message : String(error));
    }
  }
}
