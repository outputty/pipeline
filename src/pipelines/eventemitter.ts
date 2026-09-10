/**
 * `EventEmitterPipeline` (#124) — a fourth `Pipeline` dispatch mode: each chunk of a stage handed to
 * whichever Worker functions are registered on `pipeline.emitter`, a `node:events`-shaped
 * `EventEmitter`, instead of POSTed over HTTP or sent to another process. `stageWork()` is the only
 * dispatch override - `ConcurrentPipeline.apply()`'s own fan-out (`fanOutOrdered`/`fanOutUnordered`,
 * `maxConcurrency`, `ordered`) is inherited UNCHANGED. `apply()` and `drainable()` are each
 * overridden a second time, on top of that, purely to emit `stage:<n>:end`/`pipeline:end`.
 *
 * `new EventEmitterPipeline<number>().transform((t) => t.map((x) => x * 2))([1, 2, 3]).toArray()` →
 * `[2, 4, 6]`, the composed function alone a complete Worker.
 */

import type { ConcurrentPipelineOptions } from "@src/pipelines/concurrent";
import { ConcurrentPipeline } from "@src/pipelines/concurrent";
import { Pipeline } from "@src/pipeline";
import type { PipelineConstructorOptions, PipelineSource, WrappablePipeline } from "@src/pipeline";
import type { Transformer } from "@src/transformer";
import type {
  IContextManager,
  InternalTransformer,
  PipelineMode,
  ReduceFunction,
} from "@src/types";
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

/** `options.emitter` is a trust-boundary value - a caller's own compatible emitter, not necessarily
 * `node:events`' own - so a missing method fails HERE, at construction, naming what is missing,
 * rather than surfacing later as a generic `TypeError` deep inside `stageWork()`'s dispatch closure
 * (this repo's own Fail Loud rule: "External data missing an expected field fails at the parse"). */
function assertPipelineEmitter(candidate: PipelineEmitter): void {
  const required = ["on", "off", "listeners", "listenerCount", "emit"] as const;
  for (const method of required) {
    if (typeof candidate[method] !== "function") {
      throw new Error(`options.emitter is missing '${method}()' - it must satisfy PipelineEmitter`);
    }
  }
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

export class EventEmitterPipeline<T, In = T> extends ConcurrentPipeline<T, In> {
  readonly emitter: PipelineEmitter;
  /** Event names already carrying the composed function's own registration - keyed by name, not by
   * `emitter.listeners().length`, so a caller removing that listener between two calls does not
   * cause a silent re-registration (#124 Done-when 3, 7). Carried forward BY REFERENCE through
   * `createPipeline()` (below), never copied - the same object the ORIGINAL, unbound pipeline holds. */
  protected _registeredStages: Set<string>;

  /** Wraps a chain built elsewhere, dispatching its stages through the emitter (#90's own
   * wrapping-constructor pattern, `HttpPipeline`/`ClusterPipeline` share it) - the WORKER and the
   * TRIGGER can then share one definition. */
  constructor(pipeline: WrappablePipeline<T, In>, options?: EventEmitterPipelineOptions);
  constructor(options?: EventEmitterPipelineConstructorOptions);
  constructor(
    first?: WrappablePipeline<T, In> | EventEmitterPipelineConstructorOptions,
    second?: EventEmitterPipelineOptions,
  ) {
    const options = Pipeline.wrapping<EventEmitterPipelineConstructorOptions>(first, second);
    super(options);
    this.emitter = options?.emitter ?? new EventEmitter();
    assertPipelineEmitter(this.emitter);
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
   * Narrows the static return type, same as `transform()` above, AND wraps the dispatched stage's
   * own output chunk stream so `stage:<n>:end` fires once, after every chunk that stage's fan-out
   * produced has been yielded (Done-when 9, 10). `stageIndex` is computed identically to how
   * `ConcurrentPipeline.apply()` computes it internally (`this._chunkTransforms.length`, read
   * before `super.apply()` runs), so the event name here always matches the one `stageWork()`
   * dispatches under.
   *
   * A still-deferred result (no source bound yet) is returned unwrapped: `super.apply()` itself
   * only RECORDED this call, to replay later against a bound instance - where this method runs
   * again, and wraps for real.
   */
  override apply<U>(transformer: Transformer<T, U, "sync" | "async">): EventEmitterPipeline<U, In> {
    const stageIndex = this._chunkTransforms.length;
    const dispatched = super.apply(transformer) as EventEmitterPipeline<U, In>;
    if (dispatched.isDeferred()) return dispatched;

    const emitter = this.emitter;
    const eventName = `stage:${stageIndex}`;
    const source = dispatched._chunks;
    dispatched._chunks = withEndSignal(source, () => emitter.emit(`${eventName}:end`));
    return dispatched;
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

  /**
   * Re-declared ONLY to narrow the static return type, the same reason as `transform()`/`local()`
   * above - `.reduce()` dispatch is INHERITED UNCHANGED from `ConcurrentPipeline` (#124's own Settle
   * first: it folds in-process, with no emitter involvement, silently different from every
   * `.transform()` stage - left that way here by decision, not an oversight).
   */
  override reduce<U>(fn: ReduceFunction<U, T>, initial: U): EventEmitterPipeline<U, In> {
    return super.reduce(fn, initial) as unknown as EventEmitterPipeline<U, In>;
  }

  /**
   * Registers the composed function as `stage:<n>`'s own first Worker, once per stage index -
   * `_registeredStages` (a `Set`, carried BY REFERENCE through `createPipeline()`) is what makes
   * this a once-EVER registration rather than once per bound call, since `stageWork()` itself
   * replays on every call (Done-when 3). Dispatch reads `emitter.listeners(eventName)` itself and
   * calls each directly, wrapped in `Promise.resolve(...).catch(...)`, never `emitter.emit()` -
   * `emit()` cannot catch a Worker's throw after its own `await` (Done-when 6). Every registered
   * Worker runs on every chunk; the first to SETTLE, `respond()` or `reject()`, decides it
   * (Done-when 4) - a native `Promise`'s own idempotence makes every later settle on the same
   * dispatch a no-op, guarded again here (`settled`) so the LIFECYCLE events stay exactly-once too.
   * No listener at all rejects immediately, naming the stage (Done-when 7).
   *
   * `stageWork(transformer, 0)` returns a function that, called with `([1,2], ctx)`, emits
   * `stage:0:dispatched`, runs every registered Worker, and settles with whichever responds or
   * rejects first.
   */
  protected override stageWork<U>(
    transformer: Transformer<T, U, "sync" | "async">,
    stageIndex: number,
  ): InternalTransformer<T, U> {
    const eventName = `stage:${stageIndex}`;
    if (!this._registeredStages.has(eventName)) {
      const runnable = transformer.runnable();
      this.emitter.on(eventName, (event: WorkEvent<T, U>) => {
        try {
          Promise.resolve(runnable(event.chunk, event.ctx)).then(event.respond, event.reject);
        } catch (error) {
          // A row's own SYNCHRONOUS throw (no Transformer.onError() row handler registered)
          // reaches here before Promise.resolve ever wraps it - converted to a normal reject()
          // call so it still emits stage:<n>:error and goes through the same settle-once guard as
          // every other failure, rather than only the Promise executor's own automatic catch.
          event.reject(error);
        }
      });
      this._registeredStages.add(eventName);
    }

    const emitter = this.emitter;
    return (chunk, ctx) =>
      new Promise<U[]>((resolve, reject) => {
        emitter.emit(`${eventName}:dispatched`, { chunk, ctx });

        let settled = false;
        const respond = (value: U[]): void => {
          if (settled) return;
          settled = true;
          emitter.emit(`${eventName}:done`, { chunk: value, ctx });
          resolve(value);
        };
        const doReject = (error: unknown): void => {
          if (settled) return;
          settled = true;
          emitter.emit(`${eventName}:error`, { error, ctx });
          reject(error instanceof Error ? error : new Error(String(error)));
        };

        const listeners = emitter.listeners(eventName);
        if (listeners.length === 0) {
          doReject(new Error(`no worker registered on stage ${stageIndex} (${eventName})`));
          return;
        }
        for (const fn of listeners) {
          Promise.resolve(
            (fn as (event: WorkEvent<T, U>) => unknown)({ chunk, ctx, respond, reject: doReject }),
          ).catch(doReject);
        }
      });
  }

  /**
   * Wraps `Pipeline.drainable()`'s own `items`/`chunks` thunks so `pipeline:end` fires once the
   * wrapped stream is exhausted - once per TERMINAL CALL, matching `PipelineResult`'s own "every
   * terminal re-drains" contract: calling `.first()` then `.toArray()` on the same result fires it
   * twice (Done-when 10), since each terminal calls `drainable()` fresh. `syncChunks` is always
   * `null` here - a dispatching class forces `"async"` Mode, so there is no sync stream to wrap.
   */
  override drainable(input: PipelineSource<In>): {
    syncChunks: MaybeAsyncChunks<T> | null;
    items: () => AsyncIterable<T>;
    chunks: () => AsyncIterable<T[]>;
    context: IContextManager;
  } {
    const base = super.drainable(input);
    const emitter = this.emitter;
    let fired = false;
    const fireOnce = (): void => {
      if (fired) return;
      fired = true;
      emitter.emit("pipeline:end");
    };
    return {
      ...base,
      items: () => withEndSignal(base.items(), fireOnce),
      chunks: () => withEndSignal(base.chunks(), fireOnce),
    };
  }
}

/** The shape a registered Worker receives - a plain event object, never the raw
 * `InternalTransformer` signature, so a Worker is a function of ONE argument regardless of what the
 * composed transform's own arity looks like. */
interface WorkEvent<In, Out> {
  chunk: In[];
  ctx: IContextManager;
  respond: (value: Out[]) => void;
  reject: (error: unknown) => void;
}

/** Wraps `source`, calling `onEnd` exactly once after the LAST value is yielded - whether the
 * consumer drains it to natural exhaustion or stops early (`.first(n)`'s own early return, which
 * calls the generator's `.return()` per the iterator protocol and runs this `finally`). Shared by
 * `apply()` (`stage:<n>:end`) and `drainable()` (`pipeline:end`) - the identical "fire once, on
 * exit either way" shape, over two different streams. */
async function* withEndSignal<V>(source: AsyncIterable<V>, onEnd: () => void): AsyncGenerator<V> {
  try {
    yield* source;
  } finally {
    onEnd();
  }
}
