/**
 * `@outputty/pipeline/websocket` - `WebSocketPipeline`, `ClusterPipeline` and their Node bridge.
 * Kept off the root entry so the root loads no node:* / ws import. A caller of this entry installs
 * `ws`, an optional peer dependency.
 */

export {
  WebSocketPipeline,
  type WebSocketPipelineOptions,
  type PipelineSocket,
  toNodeWebSocketHandler,
  type NodeWebSocketHandler,
} from "./pipelines/websocket";
export { ClusterPipeline, type ClusterPipelineOptions } from "./pipelines/websocket-cluster";
