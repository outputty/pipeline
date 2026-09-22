/**
 * `@outputty/pipeline/cluster` (#249) - `ClusterHttpPipeline`, `node:cluster`'s own HTTP worker-pool
 * bootstrap. Kept off the root entry so `@outputty/pipeline` carries no `node:cluster`/`node:http`/
 * `node:os` import a browser bundler must resolve. Extends `HttpPipeline`, so this entry's own module
 * graph pulls `./http.ts`'s in too - both are Node-only already.
 */

export { ClusterHttpPipeline, type ClusterHttpPipelineOptions } from "./pipelines/cluster";
