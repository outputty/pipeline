/**
 * `HttpPipeline` and `toNodeHandler` (#17) — each chunk of a stage dispatched over HTTP to another
 * instance running the SAME code. Overrides `stageWork()` alone, per `ConcurrentPipeline`'s own
 * contract: `.transform()`/`.apply()` are inherited unchanged, so the fan-out, the local-stage
 * check and the knob-violation check all keep working exactly as `ConcurrentPipeline` built them.
 *
 * Wire format, one route per stage index:
 * ```text
 * POST <mount>/stage/0   { "chunk": [1, 2], "context": { "multiplier": 10 } }
 *                     -> { "chunk": [2, 4] }
 * ```
 */

import type { ConcurrentPipelineOptions, StageOptions } from "@src/pipelines/concurrent";
import { ConcurrentPipeline } from "@src/pipelines/concurrent";
import type { PipelineOptions, PipelineSource } from "@src/pipeline";
import type { Transformer } from "@src/transformer";
import type { InternalTransformer, ReduceFunction } from "@src/types";
import type { IncomingMessage, ServerResponse } from "node:http";

/** `HttpPipeline`'s real constructor parameter type - see `ConcurrentPipelineConstructorOptions`
 * (`pipelines/concurrent.ts`) for why the base `Pipeline` internals must be included here too. */
type HttpPipelineConstructorOptions = { url: string } & ConcurrentPipelineOptions & PipelineOptions;

/** The body `stageWork()` POSTs, and `.fetch()` (below) expects on the way in. */
interface StageRequestBody {
  chunk: unknown[];
  context: Record<string, unknown>;
}

/** The body `.fetch()` returns on success, and `stageWork()` expects on the way back. */
interface StageResponseBody<U> {
  chunk: U[];
}

/**
 * Parses and validates a stage POST body - malformed JSON, a missing `chunk` array, or a missing
 * `context` object all fail here rather than reaching the worker's own context manager/the stage's
 * own transform with a half-formed value (this repo's own Fail Loud rule: "External data missing an
 * expected field fails at the parse"). A discriminated result, not a throw - `.fetch()` (above)
 * turns a failure into a 400 response; a raw JSON parse exception left uncaught here previously
 * became an unhandled rejection inside `toNodeHandler`'s bridge, hanging the calling client
 * (review, verified live: a bodyless POST to `/stage/0` never got a response).
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
 * Each chunk of a stage dispatched over HTTP to another instance running the SAME code (#17).
 * Mounts one route per stage index (`readonly fetch`); the caller gives it the url where that
 * `.fetch` is mounted. `{ local: true }` on `.transform()`/`.apply()` keeps one stage here instead.
 *
 * `new HttpPipeline([1,2,3,4,5], { url }).transform((t) => t.map((x) => x * 2)).toArray()` →
 * `[2,4,6,8,10]`, across two real instances.
 */
export class HttpPipeline<T> extends ConcurrentPipeline<T> {
  protected _url: string;

  constructor(source: PipelineSource<T>, options: HttpPipelineConstructorOptions) {
    super(source, options);
    this._url = options.url;
  }

  /** Where this instance's `.fetch` is mounted - the url another `HttpPipeline`/`ClusterPipeline`
   * POSTs a stage's chunk to. Set at construction; `ClusterPipeline` rewrites it once its
   * lazily-bootstrapped worker set picks a real port. */
  get url(): string {
    return this._url;
  }

  /**
   * Re-declared ONLY to narrow the static return type back to `HttpPipeline<U>` - the inherited
   * `ConcurrentPipeline.transform()`/`.apply()` logic (fan-out, `{ local: true }`, the
   * knob-violation check) runs completely unchanged via `super`. Without this, product.md's own
   * canonical example - two chained `.transform()` calls, then `.fetch` - would not typecheck:
   * `ConcurrentPipeline<U>` (the un-narrowed inherited return type) has no `.fetch`. The cast is
   * honest because `createPipeline()` (above) already makes the RUNTIME value an `HttpPipeline`.
   */
  override transform<U>(
    builder: (t: Transformer<T, T>) => Transformer<T, U>,
    options?: StageOptions,
  ): HttpPipeline<U> {
    return super.transform(builder, options) as HttpPipeline<U>;
  }

  override apply<U>(transformer: Transformer<T, U>, options?: StageOptions): HttpPipeline<U> {
    return super.apply(transformer, options) as HttpPipeline<U>;
  }

  /**
   * Re-declared ONLY to narrow the static return type back to `HttpPipeline<U>` - same reason as
   * `.transform()`/`.apply()` above. `ConcurrentPipeline.reduce()`'s own logic runs unchanged via
   * `super`.
   */
  override reduce<U>(
    fn: ReduceFunction<U, T>,
    initial: U,
    options?: StageOptions,
  ): HttpPipeline<U> {
    return super.reduce(fn, initial, options) as HttpPipeline<U>;
  }

  /**
   * Carries `url` into the NEXT instance a copy-on-write call builds, on top of what
   * `ConcurrentPipeline.createPipeline()` already carries forward - same reason, one more field.
   */
  protected override createPipeline<U>(
    chunks: AsyncIterable<U[]>,
    options: PipelineOptions,
  ): HttpPipeline<U> {
    const Ctor = this.constructor as new (
      data: PipelineSource<U>,
      options: HttpPipelineConstructorOptions,
    ) => HttpPipeline<U>;
    return new Ctor([], { ...options, ...this.concurrentOptions(), url: this._url, chunks });
  }

  /**
   * Serves one stage's chunk of work over HTTP. Prefix-agnostic (`node-http-runtime` skill): reads
   * only the trailing `/stage/<n>` segment, so a framework `.mount()` that rewrites the path ahead
   * of it (Hono's own default) never breaks routing.
   *
   * An unknown stage index 404s naming the range this deployment actually serves. A stage that
   * throws (its own transform chain, `.catch()` included, already ran and did NOT recover) 500s
   * with the error message - `stageWork()` (below) turns that into a thrown error on the
   * DISPATCHING side, which never reaches that side's own unrelated `.catch()` calls, since the
   * HTTP round trip happens entirely outside any `Transformer` chain.
   *
   * `fetch(new Request("http://x/stage/0", { method: "POST", body: JSON.stringify({ chunk: [1,2],
   * context: {} }) }))` → `{ chunk: [2,4] }` (a `.map((x) => x*2)` stage 0).
   */
  readonly fetch = async (request: Request): Promise<Response> => {
    const { pathname } = new URL(request.url);
    const match = /\/stage\/(\d+)$/.exec(pathname);
    const maxIndex = this._chunkTransforms.length - 1;
    const requested = match ? Number(match[1]) : NaN;

    if (!match || requested > maxIndex) {
      return Response.json(
        {
          error: `unknown stage ${match ? requested : pathname}; this deployment serves 0..${maxIndex}`,
        },
        { status: 404 },
      );
    }

    const body = await parseStageRequest(request);
    if (!body.ok) {
      return Response.json({ error: body.error }, { status: 400 });
    }

    try {
      // Reuses `this._context` - the SAME instance the constructor built (from `context` or
      // `contextFactory`, `src/pipeline.ts`) - instead of building a fresh `SimpleContextManager`
      // from the wire every request. A `contextFactory` is invoked once per process this way
      // (#31, Done-when 6); the wire's own forward-looking values still land, via `.set()`, onto
      // THIS manager, so a rejecting manager's throw reaches the same catch below as a stage's own
      // transform error, rather than escaping uncaught to `toNodeHandler`'s bridge.
      const ctx = this._context;
      for (const [key, value] of Object.entries(body.value.context)) {
        ctx.set(key, value);
      }
      const result = await this._chunkTransforms[requested](body.value.chunk, ctx);
      return Response.json({ chunk: result } satisfies StageResponseBody<unknown>);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return Response.json({ error: message }, { status: 500 });
    }
  };

  /** The outgoing path for `stageIndex`, and (via `.fetch()`'s prefix-agnostic trailing-segment
   * match) the incoming one too. `ClusterPipeline` overrides this alone to route several pipeline
   * definitions through one shared worker server (`/pipeline/<i>/stage/<n>`) without touching
   * `stageWork()`'s dispatch logic or `.fetch()`'s parsing at all. */
  protected stagePath(stageIndex: number): string {
    return `/stage/${stageIndex}`;
  }

  /**
   * POSTs the chunk to `${url}${stagePath(stageIndex)}` instead of running it in-process -
   * `ConcurrentPipeline`'s own `apply()` calls this for every non-local stage; the fan-out, the
   * `{ local: true }` check and the knob-violation check are otherwise unchanged, inherited as-is.
   *
   * `transformer` itself is unused - a dispatching class sends a chunk plus an INDEX, never a
   * function (product.md); the receiving instance's own `_chunkTransforms[stageIndex]` (its
   * `.fetch()`, above) is what actually runs the stage.
   */
  protected override stageWork<U>(
    _transformer: Transformer<T, U>,
    stageIndex: number,
  ): InternalTransformer<T, U> {
    return async (chunk, ctx) => {
      const response = await fetch(`${this._url}${this.stagePath(stageIndex)}`, {
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
}

/** The error message a stage's own `.fetch()` sent back on a non-2xx response, or the plain HTTP
 * status when the body wasn't the `{ error }` JSON shape `.fetch()` sends (a proxy's own error
 * page, say). Its own function, not inlined into `stageWork()`'s `if (!response.ok)` branch, to
 * keep that branch within `.oxlintrc.json`'s `max-depth: 2` (review found this comment citing the
 * wrong rule file and a nonexistent try/catch - corrected). */
async function errorDetailOf(response: Response): Promise<string> {
  try {
    const body = (await response.json()) as { error?: string };
    return body.error ?? `HTTP ${response.status}`;
  } catch {
    return `HTTP ${response.status}`;
  }
}

/**
 * Bridges a `.fetch` handler to `http.Server`'s `(req, res)` callback — Node exposes
 * `Request`/`Response`/`fetch` but serves no fetch handler natively (`node-http-runtime` skill:
 * `createServer(async (req) => new Response("hi"))` hangs, the returned `Response` is ignored).
 * Bun, Deno and Cloudflare need nothing; this exists for Node only.
 *
 * `createServer(toNodeHandler(pipeline.fetch)).listen(0)`.
 */
export function toNodeHandler(
  handler: (request: Request) => Promise<Response>,
): (req: IncomingMessage, res: ServerResponse) => void {
  return (req, res) => {
    handleOverBridge(req, res, handler).catch((error: unknown) => {
      // A LAST-RESORT net: `handler` itself is expected to catch its own errors into a Response
      // (`HttpPipeline.fetch` does), but nothing upstream of this bridge can assume that of every
      // caller's own handler. Leaving this uncaught turns a Node bridge into a hung client
      // connection and an unhandled rejection - the exact failure mode review found in `.fetch()`
      // itself before its own parsing was hardened.
      if (!res.headersSent) {
        res.statusCode = 500;
        res.end(error instanceof Error ? error.message : String(error));
      }
    });
  };
}

/** Reads the Node request into a real `Request`, runs `handler`, writes the resulting `Response`
 * back - `toNodeHandler()`'s own body, pulled out so that function stays a one-line dispatch to
 * this plus its safety net (above). */
async function handleOverBridge(
  req: IncomingMessage,
  res: ServerResponse,
  handler: (request: Request) => Promise<Response>,
): Promise<void> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    chunks.push(chunk as Buffer);
  }
  const body = chunks.length > 0 ? Buffer.concat(chunks) : undefined;

  const headers = new Headers();
  for (const [key, value] of Object.entries(req.headers)) {
    if (value === undefined) continue;
    for (const v of Array.isArray(value) ? value : [value]) headers.append(key, v);
  }

  const request = new Request(
    new URL(req.url ?? "/", `http://${req.headers.host ?? "localhost"}`),
    {
      method: req.method,
      headers,
      body,
      // Required by Node's undici Request whenever a body is passed - harmless when body is
      // undefined, so set unconditionally rather than branching on it.
      duplex: "half",
    },
  );

  const response = await handler(request);
  res.statusCode = response.status;
  response.headers.forEach((value, key) => res.setHeader(key, value));
  const responseBody = Buffer.from(await response.arrayBuffer());
  res.end(responseBody);
}
