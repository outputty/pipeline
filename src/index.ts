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
  ExecutionStrategy,
  BranchDefinition,
  BranchOptions,
  TransformerOptions,
  ConcurrentStrategyOptions,
  ReduceOptions,
  ExecutorType,
  ExecutorOptions,
  ExecutorFactory,
  CustomExecutor,
  ExecutorSpec,
  TransformerLifecycleHooks,
} from "./types";
export { DEFAULT_CHUNK_SIZE } from "./types";

// Context
export { SimpleContextManager } from "./context/simple";

// Errors
export { ErrorHandler } from "./errors/handler";

// Utils
export { buildChunkGenerator, normalize, isContextAware, isContextAwareReduce } from "./utils";

// Strategies
export { SequentialStrategy } from "./strategies/sequential";
export { ConcurrentStrategy } from "./strategies/concurrent";
export { registerExecutor, createStrategy } from "./strategies/registry";

// Transformer
export { Transformer } from "./transformer";

// Pipeline
export { Pipeline, type PipelineOptions, type PipelineSource } from "./pipeline";

// Factory functions
export { createTransformer, createConcurrentTransformer } from "./factories";
