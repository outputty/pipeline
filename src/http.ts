/**
 * `@outputty/pipeline/http` - `HttpPipeline`, its dispatch client, and the bridge to a plain
 * `http.Server` handler. Kept off the root entry so the root loads no node:* / ws import.
 */

export { HttpPipeline, type HttpPipelineOptions, toNodeHandler } from "./pipelines/http";
export { type PipelineClient, fetchClient, defaultClient } from "./pipelines/client";
