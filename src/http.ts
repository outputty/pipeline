/**
 * `@outputty/pipeline/http` (#249) - `HttpPipeline`, its dispatch-client seam, and the Node-only
 * bridge to a plain `http.Server` handler. Kept off the root entry so `@outputty/pipeline` carries
 * no `node:stream`/`node:http` import a browser bundler must resolve; a caller of this entry runs on
 * Node (or a runtime with `node:http`/`node:stream`).
 */

export { HttpPipeline, type HttpPipelineOptions, toNodeHandler } from "./pipelines/http";
export { type PipelineClient, fetchClient, defaultClient } from "./pipelines/client";
