/**
 * `HttpPipeline` and `toNodeHandler` — STUBS (#17 L1), copy-on-write wired (#17 L2). L4 fills the
 * throwing bodies in: one route per stage index, the wire format, the 404/500 paths.
 */

import type { ConcurrentPipelineOptions, StageOptions } from "@src/pipelines/concurrent";
import { ConcurrentPipeline } from "@src/pipelines/concurrent";
import type { PipelineOptions, PipelineSource } from "@src/pipeline";
import type { Transformer } from "@src/transformer";
import type { InternalTransformer } from "@src/types";
import type { IncomingMessage, ServerResponse } from "node:http";

/** `HttpPipeline`'s real constructor parameter type - see `ConcurrentPipelineConstructorOptions`
 * (`pipelines/concurrent.ts`) for why the base `Pipeline` internals must be included here too. */
type HttpPipelineConstructorOptions = { url: string } & ConcurrentPipelineOptions & PipelineOptions;

/**
 * Each chunk of a stage dispatched over HTTP to another instance running the SAME code (#17).
 * Mounts one route per stage index (`readonly fetch`); the caller gives it the url where that
 * `.fetch` is mounted. `{ local: true }` on `.transform()`/`.apply()` keeps one stage here instead.
 *
 * `new HttpPipeline([1,2,3,4,5], { url }).transform((t) => t.map((x) => x * 2)).toArray()` →
 * `[2,4,6,8,10]`, across two real instances (L4).
 */
export class HttpPipeline<T> extends ConcurrentPipeline<T> {
  protected _url: string;

  constructor(source: PipelineSource<T>, options: HttpPipelineConstructorOptions) {
    super(source, options);
    this._url = options.url;
  }

  /** Where this instance's `.fetch` is mounted - the url another `HttpPipeline`/`ClusterPipeline`
   * POSTs a stage's chunk to. Set at construction; `ClusterPipeline` (#17 L5) rewrites it once its
   * lazily-bootstrapped worker set picks a real port. */
  get url(): string {
    return this._url;
  }

  /**
   * Carries `url` into the NEXT instance a copy-on-write call builds, on top of what
   * `ConcurrentPipeline.createPipeline()` already carries forward - same reason, one more field.
   */
  protected override createPipeline<U>(
    data: AsyncIterable<U>,
    options: PipelineOptions,
  ): HttpPipeline<U> {
    const Ctor = this.constructor as new (
      data: AsyncIterable<U>,
      options: HttpPipelineConstructorOptions,
    ) => HttpPipeline<U>;
    return new Ctor(data, {
      ...options,
      url: this._url,
      maxConcurrency: this.maxConcurrency,
      ordered: this.ordered,
      chunkSize: this.chunkSize,
    });
  }

  /** Prefix-agnostic: a framework mount may rewrite the path before this sees it. */
  readonly fetch = (_request: Request): Promise<Response> => {
    throw new Error("HttpPipeline.fetch: not implemented (#17 L4)");
  };

  override transform<U>(
    _builder: (t: Transformer<T, T>) => Transformer<T, U>,
    _options?: StageOptions,
  ): HttpPipeline<U> {
    throw new Error("HttpPipeline.transform: not implemented (#17 L4)");
  }

  override apply<U>(_transformer: Transformer<T, U>, _options?: StageOptions): HttpPipeline<U> {
    throw new Error("HttpPipeline.apply: not implemented (#17 L4)");
  }

  protected override stageWork<U>(
    _transformer: Transformer<T, U>,
    _stageIndex: number,
  ): InternalTransformer<T, U> {
    throw new Error("HttpPipeline.stageWork: not implemented (#17 L4)");
  }
}

/**
 * Bridges a `.fetch` handler to `http.Server`'s `(req, res)` callback — Node exposes
 * `Request`/`Response`/`fetch` but serves no fetch handler natively (`node-http-runtime` skill).
 * Bun, Deno and Cloudflare need nothing; this exists for Node only.
 *
 * `createServer(toNodeHandler(pipeline.fetch)).listen(0)` (L4).
 */
export function toNodeHandler(
  _handler: (request: Request) => Promise<Response>,
): (req: IncomingMessage, res: ServerResponse) => void {
  throw new Error("toNodeHandler: not implemented (#17 L4)");
}
