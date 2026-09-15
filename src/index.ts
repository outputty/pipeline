/**
 * @outputty/pipeline
 *
 * Async streaming data processing pipelines with chunking and concurrency control.
 *
 * Migrated from laygo-python: https://github.com/ringoldsdev/laygo-python
 */

// Types
export type {
  PipelineFunction,
  ReduceFunction,
  InternalTransformer,
  BufferFunction,
  IContextManager,
  TransformerOptions,
  RowErrorHandler,
  PipelineErrorHandler,
  RunScope,
} from "./types";
export { DEFAULT_CHUNK_SIZE, DROP } from "./types";

// Context
export { SimpleContextManager } from "./context/simple";

// Utils - `normalize` is not exported here (#133, BREAKING): zero production callers post-#39,
// still defined internally in `./utils/chunk` for the one module that still uses it.
export { buildChunkGenerator, isContextAware } from "./utils";

// Transformer
export { Transformer } from "./transformer";

// Branching (#90) - the builder `.branch()` configures, and the arms it collects
export { BranchBuilder, type BranchArm, type ResultsOf } from "./branch";

// Pipeline
export { Pipeline, type PipelineOptions, type PipelineSource } from "./pipeline";

// What calling a Pipeline produces (#90) — the terminal ops live here, not on the chain
export { PipelineResult } from "./result";

// Pipeline family (#17) — where a chain's chunks are processed
export { ConcurrentPipeline, type ConcurrentPipelineOptions } from "./pipelines/concurrent";
export { HttpPipeline, type HttpPipelineOptions, toNodeHandler } from "./pipelines/http";
// How a dispatched chunk reaches another instance (#179) - `HttpPipelineOptions.client`'s own type,
// exported so a caller can annotate their own client, plus the two shipped implementations.
export { type PipelineClient, fetchClient, defaultClient } from "./pipelines/client";
export { ClusterHttpPipeline, type ClusterHttpPipelineOptions } from "./pipelines/cluster";
// `ClusterPipeline` (#201) - reparented onto `WebSocketPipeline` at L3. Until then this name is a
// plain alias of the untouched HTTP class, so `main` keeps its happy path at every merge.
export {
  ClusterHttpPipeline as ClusterPipeline,
  type ClusterHttpPipelineOptions as ClusterPipelineOptions,
} from "./pipelines/cluster";
export {
  EventEmitterPipeline,
  type EventEmitterPipelineOptions,
  type PipelineEmitter,
  type WorkEvent,
} from "./pipelines/eventemitter";
export {
  WebSocketPipeline,
  type WebSocketPipelineOptions,
  type PipelineSocket,
  type Codec,
  jsonCodec,
  toNodeWebSocketHandler,
  type NodeWebSocketHandler,
} from "./pipelines/websocket";

// Factory functions
export { createTransformer } from "./factories";
