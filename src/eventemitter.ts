/**
 * `@outputty/pipeline/eventemitter` - `EventEmitterPipeline`, dispatch over a `node:events`
 * `EventEmitter`. Kept off the root entry so the root loads no node:* / ws import.
 */

export {
  EventEmitterPipeline,
  type EventEmitterPipelineOptions,
  type PipelineEmitter,
  type WorkEvent,
} from "./pipelines/eventemitter";
