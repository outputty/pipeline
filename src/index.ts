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
  ChunkerFunction,
  IContextManager,
  BranchDefinition,
  BranchOptions,
  TransformerOptions,
  RowErrorHandler,
  PipelineErrorHandler,
  RunScope,
} from "./types";
export { DEFAULT_CHUNK_SIZE, DROP } from "./types";

// Context
export { SimpleContextManager } from "./context/simple";

// Utils
export { buildChunkGenerator, normalize, isContextAware } from "./utils";

// Transformer
export { Transformer } from "./transformer";

// Pipeline
export { Pipeline, type PipelineOptions, type PipelineSource, type PipelinePlan } from "./pipeline";

// Pipeline family (#17) — where a chain's chunks are processed
export { ConcurrentPipeline, type ConcurrentPipelineOptions } from "./pipelines/concurrent";
export { HttpPipeline, toNodeHandler } from "./pipelines/http";
export { ClusterPipeline, type ClusterPipelineOptions } from "./pipelines/cluster";

// Runners (#90) - WHERE a pipeline runs, separated from what it does
export { ConcurrentRunner, type RunnablePipeline } from "./runners/concurrent";
export { HttpRunner, ClusterRunner, type DispatchRunnerOptions } from "./runners/http";

// Factory functions
export { createTransformer } from "./factories";
