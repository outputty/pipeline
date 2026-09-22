/** `EventEmitterPipeline`: dispatches each stage's chunks to Workers registered on an emitter. */

import type { ConcurrentPipelineOptions } from "@src/pipelines/concurrent";
import { ConcurrentPipeline } from "@src/pipelines/concurrent";
import { Pipeline } from "@src/pipeline";
import type { PipelineConstructorOptions, PipelineSource, WrappablePipeline } from "@src/pipeline";
import type { Transformer } from "@src/transformer";
import type {
  Drainable,
  IContextManager,
  InternalTransformer,
  PipelineMode,
  ReduceFunction,
} from "@src/types";
import { toError } from "@src/utils/helpers";
import { EventEmitter } from "node:events";

/**
 * The emitter methods `EventEmitterPipeline` needs. A `node:events` `EventEmitter` satisfies it,
 * and so can a caller's own emitter.
 */
export interface PipelineEmitter {
  on(event: string, listener: (...args: any[]) => void): void;
  off(event: string, listener: (...args: any[]) => void): void;
  listeners(event: string): Array<(...args: any[]) => void>;
  /** How many Workers are registered on a route. */
  listenerCount(event: string): number;
  emit(event: string, ...args: unknown[]): void;
}

function assertPipelineEmitter(candidate: PipelineEmitter): void {
  const required = ["on", "off", "listeners", "listenerCount", "emit"] as const;
  for (const method of required) {
    if (typeof candidate[method] !== "function") {
      throw new Error(`options.emitter is missing '${method}()' - it must satisfy PipelineEmitter`);
    }
  }
}

/** `ConcurrentPipelineOptions` plus the emitter to dispatch through. Without one, a new
 * `node:events` `EventEmitter` is built. */
export type EventEmitterPipelineOptions = { emitter?: PipelineEmitter } & ConcurrentPipelineOptions;

/**
 * Runs each stage's chunks through the chain's own function and through every Worker registered on
 * `emitter` under the stage's route. The first to settle answers the chunk.
 *
 * Routes follow how the chain was built: `/transform/<n>`, or `/branch/<i>/<name>/transform/<n>`
 * inside an arm. Lifecycle events go to `<route>:dispatched`, `:done`, `:error` and `:end`.
 *
 * ```typescript
 * const doubled = new Pipeline<number>().transform((t) => t.map((x) => x * 2));
 * await new EventEmitterPipeline(doubled)([1, 2, 3]).toArray(); // → [2, 4, 6]
 * ```
 */
export class EventEmitterPipeline<T, In = T> extends ConcurrentPipeline<T, In> {
  readonly emitter: PipelineEmitter;

  /** Wraps a chain built elsewhere, dispatching its stages through the emitter. */
  constructor(pipeline: WrappablePipeline<T, In>, options?: EventEmitterPipelineOptions);
  constructor(options?: EventEmitterPipelineOptions & PipelineConstructorOptions);
  constructor(
    first?: WrappablePipeline<T, In> | (EventEmitterPipelineOptions & PipelineConstructorOptions),
    second?: EventEmitterPipelineOptions,
  ) {
    const options = Pipeline.wrapping<EventEmitterPipelineOptions & PipelineConstructorOptions>(
      first,
      second,
    );
    super(options);
    this.emitter = options?.emitter ?? new EventEmitter();
    assertPipelineEmitter(this.emitter);
  }

  /**
   * Carries the same `emitter` object into each instance a chained call builds, and into every
   * `.branch()` arm. A Worker registered on it can then answer any of their routes.
   */
  protected override carriedKnobs(): ConcurrentPipelineOptions & { emitter: PipelineEmitter } {
    return {
      ...super.carriedKnobs(),
      emitter: this.emitter,
    };
  }

  override transform<U, M2 extends "sync" | "async">(
    builder: (t: Transformer<T, T, "async">) => Transformer<T, U, M2>,
  ): EventEmitterPipeline<U, In> {
    return super.transform(builder) as unknown as EventEmitterPipeline<U, In>;
  }

  /**
   * Also emits `<route>:end` after the stage's last chunk.
   *
   * ⚠ Under `maxConcurrency > 1`, an early `.first(n)` can fire `:end` before some in-flight
   * `:done`/`:error` events. `:end` means no more chunks, not that every Worker finished.
   */
  override apply<U>(transformer: Transformer<T, U, "sync" | "async">): EventEmitterPipeline<U, In> {
    const dispatched = super.apply(transformer) as EventEmitterPipeline<U, In>;
    if (dispatched.isDeferred()) return dispatched;

    const emitter = this.emitter;
    const route = dispatched.routePath("transform", dispatched._chunkTransforms.length - 1);
    const source = dispatched._chunks;
    dispatched._chunks = withEndSignal(source, () => emitSafely(emitter, `${route}:end`));
    return dispatched;
  }

  override local<U, M2 extends PipelineMode>(
    build: (p: Pipeline<T, "async", any>) => Pipeline<U, M2, any>,
  ): EventEmitterPipeline<U, In> {
    return super.local(build) as unknown as EventEmitterPipeline<U, In>;
  }

  override queue(capacity: number): EventEmitterPipeline<T, In> {
    return super.queue(capacity) as unknown as EventEmitterPipeline<T, In>;
  }

  /** Folds in-process; the emitter is not involved. */
  override reduce<U>(fn: ReduceFunction<U, T>, initial: U): EventEmitterPipeline<U, In> {
    return super.reduce(fn, initial) as unknown as EventEmitterPipeline<U, In>;
  }

  /**
   * Runs a chunk through the chain's own function and every Worker on the stage's route; the first
   * to settle answers it. Emits `:dispatched`, then `:done` or `:error`.
   *
   * ⚠ Calls the chain's function directly, never as a listener, so forks, sibling arms and pipelines
   * sharing one emitter each get their own output.
   *
   * ⚠ Calls each listener directly, not through `emit()`, so a `.once()` Worker fires on every chunk.
   *
   * `stageWork(doubler, 0)([1, 2], ctx)` → `[2, 4]`, after emitting `/transform/0:dispatched`.
   */
  protected override stageWork<U>(
    transformer: Transformer<T, U, "sync" | "async">,
    stageIndex: number,
  ): InternalTransformer<T, U> {
    const runnable = transformer.runnable();
    const emitter = this.emitter;
    const route = this.routePath("transform", stageIndex);
    const dispatchedEvent = `${route}:dispatched`;
    const doneEvent = `${route}:done`;
    const errorEvent = `${route}:error`;
    return (chunk, ctx) =>
      new Promise<U[]>((resolve, reject) => {
        // ⚠ Every lifecycle emit goes through `emitSafely`, so a throwing observer never counts as
        // a Worker failure.
        emitSafely(emitter, dispatchedEvent, { chunk, ctx });

        // ⚠ Settle before emitting `:done`/`:error`, so a throwing observer cannot leave it pending.
        let settled = false;
        const settle = (
          outcome: { ok: true; value: U[] } | { ok: false; error: unknown },
        ): void => {
          if (settled) return;
          settled = true;
          if (outcome.ok) {
            resolve(outcome.value);
            emitSafely(emitter, doneEvent, { chunk: outcome.value, ctx });
          } else {
            reject(toError(outcome.error));
            emitSafely(emitter, errorEvent, { error: outcome.error, ctx });
          }
        };
        const respond = (value: U[]): void => settle({ ok: true, value });
        // oxlint-disable-next-line anti-slop/no-unknown-parameters -- a Worker may reject with anything, the same as a Promise; genuinely unknown, not a gap
        const doReject = (error: unknown): void => settle({ ok: false, error });

        try {
          Promise.resolve(runnable(chunk, ctx)).then(respond, doReject);
        } catch (error) {
          doReject(error);
        }

        const event: WorkEvent<T, U> = { chunk, ctx, respond, reject: doReject };
        for (const fn of emitter.listeners(route)) {
          try {
            Promise.resolve((fn as (event: WorkEvent<T, U>) => void)(event)).catch(doReject);
          } catch (error) {
            // ⚠ A synchronous throw must not end the loop before later Workers run.
            doReject(error);
          }
        }
      });
  }

  /**
   * Also emits `<trail>:end` once each terminal call has drained the result: `:end` for a chain,
   * `/branch/<i>/<name>:end` for an arm. `.first()` then `.toArray()` on one result emits it twice.
   */
  override drainable(input: PipelineSource<In>, materialize = true): Drainable<T> {
    const base = super.drainable(input, materialize);
    const emitter = this.emitter;
    const trailEnd = `${this._routeTrail}:end`;
    let fired = false;
    const fireOnce = (): void => {
      if (fired) return;
      fired = true;
      emitSafely(emitter, trailEnd);
    };
    return {
      ...base,
      chunks: () => withEndSignal(base.chunks(), fireOnce),
    };
  }
}

/** What a registered Worker receives for one chunk: the chunk, the context, and the two ways to
 * answer it. */
export interface WorkEvent<In, Out> {
  chunk: In[];
  ctx: IContextManager;
  respond: (value: Out[]) => void;
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- a Worker may reject with anything, the same as a Promise; genuinely unknown, not a gap
  reject: (error: unknown) => void;
}

async function* withEndSignal<V>(source: AsyncIterable<V>, onEnd: () => void): AsyncGenerator<V> {
  try {
    yield* source;
  } finally {
    onEnd();
  }
}

/**
 * ⚠ Rethrows a listener's throw on a later microtask, as its own uncaught exception. Letting it
 * escape here would replace a real stream error, or leave a dispatch pending.
 */
function emitSafely<P>(emitter: PipelineEmitter, event: string, payload?: P): void {
  try {
    emitter.emit(event, payload);
  } catch (error) {
    queueMicrotask(() => {
      throw error;
    });
  }
}
