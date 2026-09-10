/**
 * `EventEmitterPipeline` (#124) — STUB. Real signatures. `createPipeline` (the emitter/registered-
 * stages knobs) and every pure type-narrowing override (`transform`/`apply`/`local`, each an
 * unchanged `super.X(...)` delegation, the same shape `HttpPipeline`/`ClusterPipeline` use) are real
 * from here, since none of them carries any dispatch logic of their own. Only `stageWork` (the
 * emitter-based dispatch) and `drainable` (`pipeline:end`) throw - L2 fills those two in, and
 * extends `apply`'s own delegation with the `stage:<n>:end` wrap.
 */

import type { ConcurrentPipelineOptions } from "@src/pipelines/concurrent";
import { ConcurrentPipeline } from "@src/pipelines/concurrent";
import type { Pipeline, PipelineConstructorOptions, PipelineSource } from "@src/pipeline";
import type { Transformer } from "@src/transformer";
import type { IContextManager, InternalTransformer, PipelineMode } from "@src/types";
import type { MaybeAsyncChunks } from "@src/utils/chunk";
import { EventEmitter } from "node:events";

/**
 * The minimal listener surface `EventEmitterPipeline` dispatches through - `node:events`'s own
 * `EventEmitter` satisfies it, and so does a caller's own compatible emitter (a namespaced one, a
 * test double).
 */
export interface PipelineEmitter {
  on(event: string, listener: (...args: any[]) => void): unknown;
  off(event: string, listener: (...args: any[]) => void): unknown;
  listeners(event: string): Array<(...args: any[]) => void>;
  /** Done-when 3's own check (`emitter.listenerCount("stage:0")`) reads this directly - `node:events`
   * ships it natively, so declaring it here costs nothing for the shipped emitter. */
  listenerCount(event: string): number;
  emit(event: string, ...args: unknown[]): unknown;
}

/** Construction-time knobs for `EventEmitterPipeline` - `ConcurrentPipelineOptions` (inherited
 * unchanged) plus the emitter a caller may already hold; omitted, a fresh `node:events`
 * `EventEmitter` is built. */
export type EventEmitterPipelineOptions = { emitter?: PipelineEmitter } & ConcurrentPipelineOptions;

/** `EventEmitterPipeline`'s real constructor parameter type, plus the internal registered-stage
 * bookkeeping `createPipeline()` (below) carries forward BY REFERENCE - not part of the public
 * `EventEmitterPipelineOptions`, since a caller never sets it directly (#124 Done-when 3). */
type EventEmitterPipelineConstructorOptions = EventEmitterPipelineOptions &
  PipelineConstructorOptions & { registeredStages?: Set<string> };

/**
 * Each chunk of a stage handed to whichever Worker functions are registered on `pipeline.emitter`,
 * a `node:events`-shaped `EventEmitter` (#124). The chain's own composed function auto-registers as
 * a stage's first Worker, once per stage index; any number of extra Workers may register afterward
 * from anywhere in the process, and every one of them runs on every chunk - whichever settles
 * first, `respond()` or `reject()`, decides the chunk.
 *
 * `new EventEmitterPipeline<number>().transform((t) => t.map((x) => x * 2))([1, 2, 3]).toArray()` →
 * `[2, 4, 6]` (L2), the composed function alone a complete worker.
 */
export class EventEmitterPipeline<T, In = T> extends ConcurrentPipeline<T, In> {
  readonly emitter: PipelineEmitter;
  /** Event names already carrying the composed function's own registration - keyed by name, not by
   * `emitter.listeners().length`, so a caller removing that listener between two calls does not
   * cause a silent re-registration (#124 Done-when 3, 7). Carried forward BY REFERENCE through
   * `createPipeline()` (below), never copied - the same object the ORIGINAL, unbound pipeline holds. */
  protected _registeredStages: Set<string>;

  constructor(options?: EventEmitterPipelineConstructorOptions) {
    super(options);
    this.emitter = options?.emitter ?? new EventEmitter();
    this._registeredStages = options?.registeredStages ?? new Set();
  }

  /**
   * Carries `emitter`/`_registeredStages` into the NEXT instance a copy-on-write call builds, on
   * top of what `ConcurrentPipeline.createPipeline()` already carries forward - same reason,
   * two more fields. Both BY REFERENCE, never copied: the Set's own dedup (Done-when 3) and the
   * `emitter`'s own identity (a caller-supplied one, or the one built above) must be the SAME object
   * across every instance a `.transform()`/`.buffer()`/`.context()` call derives.
   */
  protected override createPipeline<U>(
    chunks: AsyncIterable<U[]>,
    options: PipelineConstructorOptions,
  ): EventEmitterPipeline<U, In> {
    const Ctor = this.constructor as new (
      options: EventEmitterPipelineConstructorOptions,
    ) => EventEmitterPipeline<U, In>;
    return new Ctor({
      ...options,
      ...this.concurrentOptions(),
      emitter: this.emitter,
      registeredStages: this._registeredStages,
      chunks,
    });
  }

  /**
   * Re-declared ONLY to narrow the static return type back to `EventEmitterPipeline<U, In>` - the
   * inherited `ConcurrentPipeline.transform()` logic runs unchanged via `super`, the same shape
   * `HttpPipeline`/`ClusterPipeline` use for the identical reason.
   */
  override transform<U, M2 extends "sync" | "async">(
    builder: (t: Transformer<T, T, "async">) => Transformer<T, U, M2>,
  ): EventEmitterPipeline<U, In> {
    return super.transform(builder) as unknown as EventEmitterPipeline<U, In>;
  }

  /**
   * Re-declared ONLY to narrow the static return type - `ConcurrentPipeline.apply()`'s own fan-out
   * runs unchanged via `super`, which in turn calls `this.stageWork()` polymorphically. L2 extends
   * this same delegation with the `stage:<n>:end` wrap; until `stageWork()` is filled in, calling
   * this on a BOUND pipeline throws from there, and composing ahead of an input (the ordinary case)
   * works today, exactly as it will once L2 lands.
   */
  override apply<U>(transformer: Transformer<T, U, "sync" | "async">): EventEmitterPipeline<U, In> {
    return super.apply(transformer) as unknown as EventEmitterPipeline<U, In>;
  }

  /**
   * Re-declared ONLY to narrow `Pipeline.local()`'s return type (#61,
   * `~/.claude/rules/typescript.md`) - the body is an unchanged `super()` call: a `.local()` region
   * never dispatches on ANY class, so nothing about the emitter-based dispatch this ticket adds
   * changes this method at all.
   */
  override local<U, M2 extends PipelineMode>(
    build: (p: Pipeline<T, "async", any>) => Pipeline<U, M2, any>,
  ): EventEmitterPipeline<U, In> {
    return super.local(build) as unknown as EventEmitterPipeline<U, In>;
  }

  protected override stageWork<U>(
    _transformer: Transformer<T, U, "sync" | "async">,
    _stageIndex: number,
  ): InternalTransformer<T, U> {
    throw new Error("EventEmitterPipeline.stageWork: not implemented (#124 L2)");
  }

  override drainable(_input: PipelineSource<In>): {
    syncChunks: MaybeAsyncChunks<T> | null;
    items: () => AsyncIterable<T>;
    chunks: () => AsyncIterable<T[]>;
    context: IContextManager;
  } {
    throw new Error("EventEmitterPipeline.drainable: not implemented (#124 L2)");
  }
}
