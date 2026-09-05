/**
 * `HttpPipeline` and `toNodeHandler` — STUBS (#17 L1). Real signatures, throwing bodies. L4 fills
 * these in: one route per stage index, the wire format, the 404/500 paths.
 */

import type { ConcurrentPipelineOptions, StageOptions } from "@src/pipelines/concurrent";
import { ConcurrentPipeline } from "@src/pipelines/concurrent";
import type { PipelineSource } from "@src/pipeline";
import type { Transformer } from "@src/transformer";
import type { InternalTransformer } from "@src/types";
import type { IncomingMessage, ServerResponse } from "node:http";

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

  constructor(source: PipelineSource<T>, options: { url: string } & ConcurrentPipelineOptions) {
    super(source, options);
    this._url = options.url;
  }

  get url(): string {
    return this._url;
  }

  /** Prefix-agnostic: a framework mount may rewrite the path before this sees it. */
  readonly fetch = (
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    request: Request,
  ): Promise<Response> => {
    throw new Error("HttpPipeline.fetch: not implemented (#17 L4)");
  };

  override transform<U>(
    builder: (t: Transformer<T, T>) => Transformer<T, U>,
    options?: StageOptions,
  ): HttpPipeline<U> {
    throw new Error("HttpPipeline.transform: not implemented (#17 L4)");
  }

  override apply<U>(transformer: Transformer<T, U>, options?: StageOptions): HttpPipeline<U> {
    throw new Error("HttpPipeline.apply: not implemented (#17 L4)");
  }

  protected override stageWork<U>(
    transformer: Transformer<T, U>,
    stageIndex: number,
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
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  handler: (request: Request) => Promise<Response>,
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
): (req: IncomingMessage, res: ServerResponse) => void {
  throw new Error("toNodeHandler: not implemented (#17 L4)");
}
