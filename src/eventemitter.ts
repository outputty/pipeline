/**
 * `@outputty/pipeline/eventemitter` (#249) - `EventEmitterPipeline`, dispatch over a `node:events`
 * `EventEmitter`. Kept off the root entry so `@outputty/pipeline` carries no `node:events` import a
 * browser bundler must resolve. An independent leaf: no cross-import with `http.ts`/`cluster.ts`.
 */

export {
  EventEmitterPipeline,
  type EventEmitterPipelineOptions,
  type PipelineEmitter,
  type WorkEvent,
} from "./pipelines/eventemitter";
