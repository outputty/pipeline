/**
 * @outputty/pipeline
 *
 * Async streaming data processing pipelines with chunking and concurrency control.
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

// Utils
export { buildChunkGenerator } from "./utils/cut";
export { isContextAware } from "./utils/helpers";

// Transformer
export { Transformer } from "./transformer";

// Branching - the builder `.branch()` configures, and the arms it collects
export { BranchBuilder, type BranchArm, type ResultsOf } from "./branch";

// Pipeline
export { Pipeline, type PipelineOptions, type PipelineSource } from "./pipeline";

// What calling a Pipeline produces - the terminal ops live here, not on the chain
export { PipelineResult } from "./result";

// Pipeline family - where a chain's chunks are processed
export { ConcurrentPipeline, type ConcurrentPipelineOptions } from "./pipelines/concurrent";
// `HttpPipeline`, `ClusterHttpPipeline` and `EventEmitterPipeline` live on
// `@outputty/pipeline/http`, `/cluster` and `/eventemitter`; `WebSocketPipeline` and
// `ClusterPipeline` on `/websocket`.
// Kept off the root entry so the root loads no node:* / ws import.
// How a chunk is encoded on the wire
export { type Codec, JsonCodec } from "./codec";

// Factory functions
export { createTransformer } from "./factories";
