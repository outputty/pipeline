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
export { HttpPipeline, toNodeHandler } from "./pipelines/http";
export { ClusterPipeline, type ClusterPipelineOptions } from "./pipelines/cluster";
export {
  EventEmitterPipeline,
  type EventEmitterPipelineOptions,
  type PipelineEmitter,
  type WorkEvent,
} from "./pipelines/eventemitter";

// Factory functions
export { createTransformer } from "./factories";
