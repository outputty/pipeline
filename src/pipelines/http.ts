/**
 * `HttpPipeline` and `toNodeHandler` (#17) — each chunk of a stage dispatched over HTTP to another
 * instance running the SAME code. Overrides `stageWork()` alone, per `ConcurrentPipeline`'s own
 * contract: `.transform()`/`.apply()` are inherited unchanged, so the fan-out and the
 * knob-violation check keep working exactly as `ConcurrentPipeline` built them; `.local(build)`
 * (#61) is a separate, always-inherited method that keeps a whole region in-process instead.
 *
 * Wire format, one route per stage index:
 * ```text
 * POST <mount>/transform/0   { "chunk": [1, 2], "context": { "multiplier": 10 } }
 *                     -> { "chunk": [2, 4] }
 * ```
 */

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

/** Construction-time knobs for `HttpPipeline` and every class that extends it - the same pattern
 * `ClusterPipelineOptions` (`pipelines/cluster.ts`) already uses (#133: was spelled inline 3x here
 * as `{ url: string } & ConcurrentPipelineOptions`). */
export type HttpPipelineOptions = { url: string } & ConcurrentPipelineOptions;

/** `HttpPipeline`'s real constructor parameter type - see `ConcurrentPipelineConstructorOptions`
 * (`pipelines/concurrent.ts`) for why the base `Pipeline` internals must be included here too. */
type HttpPipelineConstructorOptions = HttpPipelineOptions & PipelineConstructorOptions;

/** The body `stageWork()` POSTs, and `.fetch()` (below) expects on the way in. */
interface StageRequestBody {
  chunk: unknown[];
  context: Record<string, unknown>;
}

/** The body `.fetch()` returns on success, and `stageWork()` expects on the way back. */
interface StageResponseBody<U> {
  chunk: U[];
}

/** The one JSON error-body shape every failure response in this file uses (#133: was spelled
 * `Response.json({ error: … }, { status: … })` inline 6x across `.fetch()`/`serveReduceRequest()`).
 * Exported so `cluster.ts`'s own `WorkerSet.startWorkerServer()` - routing to a `.fetch()` this
 * same file builds - answers an unknown route with the identical shape rather than a 7th inline
 * spelling. */
export function errorResponse(status: number, message: string): Response {
  return Response.json({ error: message }, { status });
}

/** The 404 both `.fetch()`'s own `stage` verb and `serveReduceRequest()` answer with when
 * `resolveRegistries(trail)` finds no arm this deployment holds (#133: identical
 * `Response.json({ error: \`unknown branch route ${trail}\` }, { status: 404 })` spelled at both
 * call sites, differing only in the pathname/trail variable name). */
function unknownBranchRoute(trail: string | null): Response {
  return errorResponse(404, `unknown branch route ${trail}`);
}

/**
 * Parses and validates a stage POST body - malformed JSON, a missing `chunk` array, or a missing
 * `context` object all fail here rather than reaching the worker's own context manager/the stage's
 * own transform with a half-formed value (this repo's own Fail Loud rule: "External data missing an
 * expected field fails at the parse"). A discriminated result, not a throw - `.fetch()` (above)
 * turns a failure into a 400 response; a raw JSON parse exception left uncaught here previously
 * became an unhandled rejection inside `toNodeHandler`'s bridge, hanging the calling client
 * (review, verified live: a bodyless POST to `/transform/0` never got a response).
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
 * frames to `writer` as they happen and `{"error":…}` on a mid-stream failure - `serveReduceRequest`
 * (below) is the one caller, kicking this off unawaited so the `Response` it returns starts
 * streaming immediately. Split into the small helpers below purely to stay within this repo's own
 * `max-depth: 2` rule - a `try` around a `for await` around a conditional write is already 3 deep.
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

/** The wire's own FIRST frame, `{"context":{…}}`, sent exactly once - applies its values onto `ctx`
 * via `.set()`, same as a `/transform/<n>` request's own context merge. */
function applyContextFrame(first: IteratorResult<string>, ctx: IContextManager): void {
  // Fails loud (this repo's own rule: "External data missing an expected field fails at the
  // parse") rather than silently dropping the frame - review found the OLD version no-opping
  // here, discarding a malformed or absent first frame with no diagnostic. Thrown here, inside
  // `runReduceStage`'s own try, so it reaches the caller as a normal `{"error":…}` frame.
  if (first.done) {
    throw new Error("reduce stream ended before a context frame was sent");
  }
  const frame = JSON.parse(first.value) as { context?: unknown };
  if (typeof frame.context !== "object" || frame.context === null || Array.isArray(frame.context)) {
    throw new Error("first reduce frame is missing a 'context' object");
  }
  for (const [key, value] of Object.entries(frame.context as Record<string, unknown>)) {
    ctx.set(key, value);
  }
}

/** One `{"chunk":[…]}` frame's worth of folding - every value `reducer.fold()` emits while folding
 * this chunk is batched into ONE `{"emit":…}` frame, sent once the whole chunk is folded. Fails
 * loud on a missing/non-array `chunk` (this repo's own rule: "External data missing an expected
 * field fails at the parse") - review found this previously accepting any truthy `chunk`, so a
 * truthy non-array (e.g. a string) was silently iterated character-by-character instead of
 * rejected, the same bug class `applyContextFrame` (above) was already hardened against. */
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

/** The final accumulator, sent as its own `{"emit":…}` frame only if items were folded since the
 * last emit - `Reducer.final()`'s own contract, same as every other reducer in the package. */
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
 * Reads back the route grammar `HttpPipeline.routePath()` builds (#90): `/transform/<n>`,
 * `/reduce/<n>`, and either prefixed by a `/branch/<i>/<name>` trail, with `ClusterPipeline`'s own
 * `/pipeline/<i>` ahead of all of it. Written and parsed in one file so the two cannot drift.
 *
 * Deliberately NOT anchored at the start: `ClusterPipeline`'s shared worker server hands the whole
 * pathname through after looking the pipeline up by index, so the prefix it added is still on it.
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
 * Each chunk of a stage dispatched over HTTP to another instance running the SAME code (#17).
 * Mounts one route per stage index (`readonly fetch`); the caller gives it the url where that
 * `.fetch` is mounted. `.local(build)` (#61) keeps a whole region here instead.
 *
 * `new HttpPipeline([1,2,3,4,5], { url }).transform((t) => t.map((x) => x * 2)).toArray()` →
 * `[2,4,6,8,10]`, across two real instances.
 */
export class HttpPipeline<T, In = T> extends ConcurrentPipeline<T, In> {
  protected _url: string;

  /** Wraps a chain built elsewhere, dispatching its stages over HTTP (#90). This is what lets the
   * WORKER and the TRIGGER share one definition: the worker constructs the wrapper and mounts
   * `.fetch` without ever naming data, and the trigger constructs it and calls it with different
   * data each time. Neither writes the `.from([])` placeholder the worker used to need. */
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
   * POSTs a stage's chunk to. Set at construction; `ClusterPipeline` rewrites it once its
   * lazily-bootstrapped worker set picks a real port. */
  get url(): string {
    return this._url;
  }

  /**
   * Re-declared ONLY to narrow the static return type back to `HttpPipeline<U>` - the inherited
   * `ConcurrentPipeline.transform()`/`.apply()` logic (fan-out, the knob-violation check) runs
   * completely unchanged via `super`. Without this, product.md's own canonical example - two
   * chained `.transform()` calls, then `.fetch` - would not typecheck: `ConcurrentPipeline<U>` (the
   * un-narrowed inherited return type) has no `.fetch`. The cast is honest because
   * `createPipeline()` (above) already makes the RUNTIME value an `HttpPipeline`.
   */
  override transform<U, M2 extends "sync" | "async">(
    // The conditional `this` this override used to carry is deleted with the base's own (#90). It
    // read `M extends "unset"`, and this class fixes `M` at `"async"`, so it never once refused
    // anything - and once `In` existed it actively broke a SECOND type-changing `.transform()`,
    // because the guard's own type pins `In` to `T` while the two diverge at the first stage that
    // changes the item type: `TS2684: The 'this' context of type 'HttpPipeline<string,
    // number>' is not assignable to method's 'this' of type 'Pipeline<string, "async",
    // string>'`.
    builder: (t: Transformer<T, T, "async">) => Transformer<T, U, M2>,
  ): HttpPipeline<U, In> {
    return super.transform(builder) as unknown as HttpPipeline<U, In>;
  }

  override apply<U>(transformer: Transformer<T, U, "sync" | "async">): HttpPipeline<U, In> {
    return super.apply(transformer) as unknown as HttpPipeline<U, In>;
  }

  /**
   * Re-declared ONLY to narrow the static return type back to `HttpPipeline<U>` - same reason as
   * `.transform()`/`.apply()` above. `ConcurrentPipeline.reduce()`'s own logic runs unchanged via
   * `super`.
   */
  override reduce<U>(fn: ReduceFunction<U, T>, initial: U): HttpPipeline<U, In> {
    return super.reduce(fn, initial) as unknown as HttpPipeline<U, In>;
  }

  /**
   * Re-declared ONLY to narrow `Pipeline.local()`'s return type (#61,
   * `~/.claude/rules/typescript.md`) - the body is an unchanged `super()` call, since `local()`'s
   * own base implementation already builds a plain `Pipeline` for the region and carries the
   * result back through THIS class's own `createPipeline()`, which is what keeps `url` alive for
   * whatever comes after the region. `bind()`/`sourcePolicy()` need no such re-declaration here
   * (#133) - `ConcurrentPipeline.sourcePolicy()`'s own `"async"` override is inherited unchanged,
   * and nothing reads `bind()`'s own narrowed return type, so this class has neither any more.
   */
  override local<U, M2 extends PipelineMode>(
    build: (p: Pipeline<T, "async", any>) => Pipeline<U, M2, any>,
  ): HttpPipeline<U, In> {
    return super.local(build) as unknown as HttpPipeline<U, In>;
  }

  /**
   * Carries `url` into the NEXT instance a copy-on-write call builds, on top of what
   * `ConcurrentPipeline.carriedKnobs()` already carries forward (#133) - same reason, one more
   * field.
   */
  protected override carriedKnobs(): HttpPipelineOptions {
    return { ...super.carriedKnobs(), url: this._url };
  }

  /**
   * Serves one stage's chunk of work over HTTP. Prefix-agnostic (`node-http-runtime` skill): reads
   * only the trailing `/transform/<n>` segment, so a framework `.mount()` that rewrites the path ahead
   * of it (Hono's own default) never breaks routing.
   *
   * An unknown stage index 404s naming the range this deployment actually serves. A stage that
   * throws (its own transform chain, row recovery included, already ran and did NOT recover) 500s
   * with the error message - `stageWork()` (below) turns that into a thrown error on the
   * DISPATCHING side, which never reaches that side's own unrelated `Pipeline.onError()` run
   * handler until `ConcurrentPipeline.apply()`'s wrapped `work` catches it there (#78), since the
   * HTTP round trip happens entirely outside any `Transformer` chain.
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

    // A path that is not a stage route is answered before anything else runs. `registries()` below
    // can replay a whole deferred chain, and it sat ahead of this guard - so an unrelated request
    // paid that replay for a `maxIndex` it then ignored, and a replay that threw turned a 404 into
    // a rejected `fetch` promise instead of an error response.
    if (route === null) {
      return errorResponse(404, `unknown stage ${pathname}`);
    }
    const requested = route.index;

    // `registries()`, not `_chunkTransforms` directly (#90): a worker holds a chain and never
    // binds an input, so its stages are still recorded calls until something replays them. Reading
    // the raw field reported `unknown stage 0; this deployment serves 0..-1` for a chain that had
    // one stage.
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
      const result = await chunkTransforms[requested](body.value.chunk, ctx);
      return Response.json({ chunk: result } satisfies StageResponseBody<unknown>);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return errorResponse(500, message);
    }
  };

  /**
   * Serves one reduce stage's WHOLE stream over one duplex connection - `_reduceStages` (#45,
   * `src/pipeline.ts`) is the registry `.reduce()` populated at the SAME index `_chunkTransforms`'s
   * own placeholder occupies, so an unknown index here means the stack was never given one, never a
   * stage that just isn't a reducer (that case 404s above, on the `stage` verb's own range check).
   * The response streams (`TransformStream`) so an emit reaches the caller as it happens - the
   * whole point of `toNodeHandler` actually delivering bytes before the handler returns.
   */
  private async serveReduceRequest(
    index: number,
    request: Request,
    trail: string | null,
  ): Promise<Response> {
    // `registries()` for the same reason `fetch` above uses it (#90): a worker's reduce stages are
    // recorded calls until something replays them. A `/branch/<i>/<name>/` trail resolves into the
    // ARM's own registry - reading the parent's instead 404s when the parent has no reduce, and
    // silently serves the parent's own fold when it does.
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
    // A registered stage with no body is a DIFFERENT problem than an unknown one (review: the OLD
    // message said "unknown reduce stage N" even when N was real) - reported as its own 400, the
    // same shape /transform/<n>'s own parseStageRequest() uses for a missing body.
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

  /** The outgoing path for `verb`/`index`, and (via `.fetch()`'s prefix-agnostic trailing-segment
   * match) the incoming one too - one shared stage-index space for both `/transform/<n>` and
   * `/reduce/<n>` (#45).
   *
   * The verb is `transform`, not `stage` (#90): a route now reads as the chain was BUILT rather
   * than as a flat counter, so a reader can walk `/transform/1` back to the second `.transform()`
   * call without counting dispatched stages. `.branch()`'s own arms extend the same scheme with a
   * `/branch/<i>/<name>/` trail. `ClusterPipeline`
   * overrides this alone to route several pipeline definitions through one shared worker server
   * (`/pipeline/<i>/<verb>/<n>`) without touching `stageWork()`/`reduceWork()`'s own dispatch logic
   * or `.fetch()`'s parsing at all. */
  protected routePath(verb: RouteVerb, index: number): string {
    return `${this._routeTrail}/${verb}/${index}`;
  }

  /** This pipeline's own stage registries, or an ARM's when `trail` names one - `null` for a trail
   * naming no arm this deployment holds. The one place `fetch()` and `serveReduceRequest()` both
   * resolve, rather than each spelling the same ternary. */
  private resolveRegistries(
    trail: string | null,
  ): { chunkTransforms: ChunkTransform[]; reduceStages: Map<number, ReduceStage> } | null {
    return trail === null ? this.registries() : this.registriesFor(trail);
  }

  /**
   * POSTs the chunk to `${url}${routePath("transform", stageIndex)}` instead of running it in-process -
   * `ConcurrentPipeline`'s own `apply()` calls this for every stage; the fan-out and the
   * knob-violation check are otherwise unchanged, inherited as-is.
   *
   * `transformer` itself is unused - a dispatching class sends a chunk plus an INDEX, never a
   * function (product.md); the receiving instance's own `_chunkTransforms[stageIndex]` (its
   * `.fetch()`, above) is what actually runs the stage.
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
   * Opens ONE duplex POST to `${url}${routePath("reduce", stageIndex)}` for the WHOLE stream -
   * `stageWork()`'s sibling, `ConcurrentPipeline`'s own default (one method above the class
   * hierarchy) folds in-process instead. The accumulator lives in the connection for its life:
   * `{"context":{…}}` once, then `{"chunk":[…]}` per upstream chunk going out; `{"emit":[…]}`
   * frames come back as they happen, `{"error":"…"}` on a mid-stream failure - arriving AFTER the
   * 200, so values already emitted have already entered downstream stages (the ticket's own
   * Constraints, the same trade the killed pull topology was rejected for, accepted here
   * deliberately: emits arrive as they happen rather than after the whole fold completes).
   */
  protected override reduceWork<U>(
    _fn: ReduceFunction<U, T>,
    _initial: U,
    stageIndex: number,
  ): ReduceWork<T, U> {
    const self = this;

    return async function* dispatchReduce(chunks, ctx) {
      // `self._url`/`routePath()` are read HERE, at dispatch time, not captured before this
      // function returns - `ClusterPipeline.reduceWork()`'s own wrap (below) sets `self._url` to
      // the bootstrapped port AFTER this method returns but BEFORE this generator actually runs
      // (same reason `HttpPipeline.stageWork()`'s own returned closure reads `this._url` fresh
      // each call, never captured at construction time).
      const url = self._url;
      const path = self.routePath("reduce", stageIndex);

      // Built (not yet consumed) before the `fetch()` call below is awaited (`.claude/rules/code.md`:
      // a streaming/duplex probe's input starts before the call that consumes it), so the connection
      // is genuinely duplex rather than the request finishing before the response starts.
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

/** The reduce wire's own OUTGOING half: one `{"context":…}` frame, then one `{"chunk":…}` NDJSON
 * frame per upstream chunk (#133: split out of `reduceWork()`'s own `dispatchReduce` generator, kept
 * as ONE function since `start`/`pull`/`cancel` all close over the SAME `upstream` iterator).
 *
 * `pull`-driven, never draining the whole stream in `start` (#113): `start` ran its own `for await`
 * to completion and enqueued every frame without consulting `desiredSize`, so nothing throttled it -
 * and `ConcurrentPipeline.reduce()` calls the caller's own closure `maxConcurrency` times over ONE
 * `share()`d iterator, so N partitions each raced to pull the entire source into N in-memory queues
 * before the server had folded anything. That also destroyed the free-slot dealing `share()` exists
 * to provide: a slow partition stops pulling less than a fast one once neither is throttled. `pull`
 * is called only as the stream drains, so the shared iterator now advances at the rate the socket
 * accepts.
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

/** The reduce wire's own INCOMING half: `{"emit":…}` frames as a remote fold produces them,
 * `{"error":…}` on a mid-stream failure - arriving AFTER the response's own 200, so values already
 * emitted have already entered downstream stages (#133: split out of `reduceWork()`'s own
 * `dispatchReduce` generator). */
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
/** Writes `bodyStream` to `res` chunk by chunk, stopping early once the client has gone
 * (`res.destroyed`) instead of pulling from `bodyStream` forever against a dead socket. Waits for
 * `'drain'` whenever `res.write()` reports the socket is backed up (review: an unchecked write let
 * a fast producer - a reduce stage's own emits - pile up unboundedly in Node's internal write
 * buffer against a slow client) rather than pulling the next chunk immediately regardless. Its own
 * function, not inlined into `handleOverBridge`'s try block, to keep that block within this
 * repo's own `max-depth: 2` rule (the same reason `errorDetailOf`, above, is its own function). */
async function writeStreamedBody(res: ServerResponse, bodyStream: Readable): Promise<void> {
  for await (const chunk of bodyStream) {
    if (res.destroyed) break;
    const ok = res.write(chunk as Buffer);
    if (!ok && !res.destroyed) {
      // Races 'close' alongside 'drain' - a client that disconnects while backed up never fires
      // 'drain' on a destroyed socket, which would otherwise hang this wait forever.
      //
      // BOTH listeners come off when either fires (#113). `once` removes only the one that fired,
      // so the loser stayed attached to a response that outlives this wait: a reduce stage emitting
      // faster than a slow client drains backs up repeatedly on ONE response, and after eleven such
      // waits Node printed `MaxListenersExceededWarning: 11 close listeners added to
      // [ServerResponse]` - each retained closure holding its own `resolve` alive for the life of
      // the connection.
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

/** Converts a Node `IncomingMessage` into a real `Request` (#133: split out of `handleOverBridge`'s
 * own body, which mixed this conversion with writing the RESPONSE back).
 *
 * Streams the request body in as it arrives (#45) - `handler` (a reduce stage's `.fetch()`, for
 * one) can start folding an early chunk before a later one has even been sent. GET/HEAD forbid a
 * body entirely (the Fetch spec throws on the `Request` constructor otherwise) - gating on method,
 * not on whether anything was ever read, is what streaming needs: buffering used to decide this
 * from the collected length, which streaming has no equivalent of upfront.
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

  // Streams the response body out as it arrives, instead of buffering it whole first (#45) - a
  // one-shot handler (`/transform/<n>`) still works identically, since its body is one chunk either
  // way; a reduce stage's duplex response (`/reduce/<n>`, L5) is what this actually unblocks.
  const bodyStream = Readable.fromWeb(response.body as ReadableStream<Uint8Array>);
  // A client that disconnects mid-stream must stop this loop pulling from `handler`'s own
  // generator, not run it to completion against a dead socket (review, verified live: without
  // this, the loop kept writing to a destroyed response every tick for the rest of the process).
  res.once("close", () => bodyStream.destroy());

  try {
    await writeStreamedBody(res, bodyStream);
  } catch (error) {
    // A failure reading `response.body` AFTER at least one chunk was already written can't become
    // a fresh error response - the client already has a 200 - so the connection is destroyed
    // instead (review, verified live: leaving this uncaught hung the client forever, since
    // `toNodeHandler`'s own outer catch only acts `if (!res.headersSent)`).
    if (res.headersSent) {
      res.destroy(error instanceof Error ? error : new Error(String(error)));
    } else {
      res.statusCode = 500;
      res.end(error instanceof Error ? error.message : String(error));
    }
  }
}
