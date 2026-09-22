/**
 * `@outputty/pipeline/cluster` - `ClusterHttpPipeline`, an `HttpPipeline` over a `node:cluster`
 * worker pool. Kept off the root entry so the root loads no node:* / ws import.
 */

export { ClusterHttpPipeline, type ClusterHttpPipelineOptions } from "./pipelines/cluster";
