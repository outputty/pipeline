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
  PipelineReduceFunction,
  ChunkErrorHandler,
  InternalTransformer,
  ChunkerFunction,
  IContextManager,
  BranchDefinition,
  BranchOptions,
  TransformerOptions,
  ReduceOptions,
  TransformerLifecycleHooks,
} from "./types";
export { DEFAULT_CHUNK_SIZE } from "./types";

// Context
export { SimpleContextManager } from "./context/simple";

// Errors
export { ErrorHandler } from "./errors/handler";

// Utils
export { buildChunkGenerator, normalize, isContextAware, isContextAwareReduce } from "./utils";

// Transformer
export { Transformer } from "./transformer";

// Pipeline
export { Pipeline, type PipelineOptions, type PipelineSource } from "./pipeline";

// Pipeline family (#17) — where a chain's chunks are processed
export {
  ConcurrentPipeline,
  type ConcurrentPipelineOptions,
  type StageOptions,
} from "./pipelines/concurrent";
export { HttpPipeline, toNodeHandler } from "./pipelines/http";
export { ClusterPipeline, type ClusterPipelineOptions } from "./pipelines/cluster";

// Factory functions
export { createTransformer } from "./factories";
