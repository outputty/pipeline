/**
 * `@outputty/pipeline/websocket` (#239) - the two runners that need the `ws` package,
 * `WebSocketPipeline` and `ClusterPipeline`, and their bridge. Kept off the root entry so
 * `@outputty/pipeline` loads with no package installed; a caller of this entry installs `ws`
 * themselves (an optional peer dependency).
 */

export {
  WebSocketPipeline,
  type WebSocketPipelineOptions,
  type PipelineSocket,
  toNodeWebSocketHandler,
  type NodeWebSocketHandler,
} from "./pipelines/websocket";
export { ClusterPipeline, type ClusterPipelineOptions } from "./pipelines/websocket-cluster";
