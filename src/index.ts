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

// Utils
export { buildChunkGenerator } from "./utils/chunk";
export { isContextAware } from "./utils/helpers";

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
// `HttpPipeline`, `ClusterHttpPipeline` and `EventEmitterPipeline` are on their own entries
// (`@outputty/pipeline/http`, `/cluster`, `/eventemitter`, #249, BREAKING) - each needs a Node
// builtin (`node:stream`/`node:cluster`+`node:http`+`node:os`/`node:events`) a browser bundler
// cannot resolve. `WebSocketPipeline` and `ClusterPipeline` are on `@outputty/pipeline/websocket`
// (#239, BREAKING), the one entry that loads `ws` - this root loads no package and no node:* import.
// How a chunk is encoded on the wire (#209) - outside `pipelines/websocket.ts` so this barrel
// doesn't need to load `ws` just to reach `Codec`/`JsonCodec`.
export { type Codec, JsonCodec } from "./codec";

// Factory functions
export { createTransformer } from "./factories";
