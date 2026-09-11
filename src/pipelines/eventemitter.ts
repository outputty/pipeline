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
import { EventEmitter } from "node:events";

/**
 * The minimal listener surface `EventEmitterPipeline` dispatches through - `node:events`'s own
 * `EventEmitter` satisfies it, and so does a caller's own compatible emitter (a namespaced one, a
 * test double).
 */
export interface PipelineEmitter {
  /** Registers `listener` on `event`. */
  on(event: string, listener: (...args: any[]) => void): void;
  /** Removes `listener` from `event`. */
  off(event: string, listener: (...args: any[]) => void): void;
  /** Every function currently registered on `event`, in registration order. */
  listeners(event: string): Array<(...args: any[]) => void>;
  /** How many listeners `event` currently carries - `node:events` ships this natively, so declaring
   * it here costs nothing for the shipped emitter. */
  listenerCount(event: string): number;
  /** Calls every listener on `event` with `args`, synchronously, the same as `node:events`. */
  emit(event: string, ...args: unknown[]): void;
}

/** `options.emitter` is a trust-boundary value - a caller's own compatible emitter, not necessarily
 * `node:events`' own - so a missing method fails HERE, at construction, naming what is missing,
 * rather than surfacing later as a generic `TypeError` deep inside `stageWork()`'s dispatch closure. */
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
 * `EventEmitterPipelineOptions`, since a caller never sets it directly. */
type EventEmitterPipelineConstructorOptions = EventEmitterPipelineOptions &
  PipelineConstructorOptions & { registeredStages?: Set<string> };

/**
 * Dispatches each chunk of a stage to whichever Worker functions are registered on
 * `pipeline.emitter`, a `node:events`-shaped `EventEmitter`, instead of POSTing it over HTTP or
 * sending it to another process. `stageWork()` is the only dispatch override -
 * `ConcurrentPipeline.apply()`'s own fan-out (`fanOutOrdered`/`fanOutUnordered`, `maxConcurrency`,
 * `ordered`) is inherited unchanged. The chain's own composed function auto-registers as a stage's
 * first Worker, so a plain chain with no extra registration is already a complete Worker on its
 * own; any number of further Workers may register on the same stage afterward, from anywhere in
 * the process, and every one of them runs on every chunk - the first to settle decides it.
 *
 * `new EventEmitterPipeline<number>().transform((t) => t.map((x) => x * 2))([1, 2, 3]).toArray()` →
 * `[2, 4, 6]`, the composed function alone a complete Worker.
 */
export class EventEmitterPipeline<T, In = T> extends ConcurrentPipeline<T, In> {
  readonly emitter: PipelineEmitter;
  protected _registeredStages: Set<string>;

  /** Wraps a chain built elsewhere, dispatching its stages through the emitter - the same
   * wrapping-constructor pattern `HttpPipeline`/`ClusterPipeline` share - so the WORKER and the
   * TRIGGER can share one definition. */
  constructor(pipeline: WrappablePipeline<T, In>, options?: EventEmitterPipelineOptions);
  constructor(options?: EventEmitterPipelineConstructorOptions);
  constructor(
    first?: WrappablePipeline<T, In> | EventEmitterPipelineConstructorOptions,
    second?: EventEmitterPipelineOptions,
  ) {
    const options = Pipeline.wrapping<EventEmitterPipelineConstructorOptions>(first, second);
    super(options);
    this.emitter = options?.emitter ?? new EventEmitter();
    // `registeredStages` is only ever set by `createPipeline()` (below), never by a caller - its
    // presence is what tells apart a DERIVED instance (already validated once, at the ORIGINAL
    // caller-facing construction this chain started from) from that original construction itself,
    // so a long chain re-validates the identical, unchanged `emitter` object exactly once rather
    // than once per `.transform()`/`.buffer()`/`.context()` call.
    if (!options?.registeredStages) {
      assertPipelineEmitter(this.emitter);
    }
    this._registeredStages = options?.registeredStages ?? new Set();
  }

  /**
   * Carries `emitter`/`_registeredStages` into the NEXT instance a copy-on-write call builds, on
   * top of what `ConcurrentPipeline.carriedKnobs()` already carries forward. Both BY REFERENCE,
   * never copied: the Set's own dedup and the `emitter`'s own identity (a caller-supplied one, or
   * the one built above) must be the SAME object across every instance a
   * `.transform()`/`.buffer()`/`.context()` call derives.
   */
  protected override carriedKnobs(): ConcurrentPipelineOptions & {
    emitter: PipelineEmitter;
    registeredStages: Set<string>;
  } {
    return {
      ...super.carriedKnobs(),
      emitter: this.emitter,
      registeredStages: this._registeredStages,
    };
  }

  override transform<U, M2 extends "sync" | "async">(
    builder: (t: Transformer<T, T, "async">) => Transformer<T, U, M2>,
  ): EventEmitterPipeline<U, In> {
    return super.transform(builder) as unknown as EventEmitterPipeline<U, In>;
  }

  /**
   * Narrows the static return type, same as `transform()`, AND wraps the dispatched stage's own
   * output chunk stream so `stage:<n>:end` fires once, after every chunk that stage's fan-out
   * produced has been yielded. `stageIndex` is read OFF THE RESULT
   * (`dispatched._chunkTransforms.length - 1`, the slot `super.apply()` just appended), never
   * re-derived by independently repeating `ConcurrentPipeline.apply()`'s own internal computation -
   * so this stays correct even if that computation ever changes, with nothing to keep in sync by
   * hand.
   *
   * A still-deferred result (no source bound yet) is returned unwrapped: `super.apply()` itself
   * only RECORDED this call, to replay later against a bound instance - where this method runs
   * again, and wraps for real.
   */
  override apply<U>(transformer: Transformer<T, U, "sync" | "async">): EventEmitterPipeline<U, In> {
    const dispatched = super.apply(transformer) as EventEmitterPipeline<U, In>;
    if (dispatched.isDeferred()) return dispatched;

    const emitter = this.emitter;
    const eventName = `stage:${dispatched._chunkTransforms.length - 1}`;
    const source = dispatched._chunks;
    dispatched._chunks = withEndSignal(source, () => emitSafely(emitter, `${eventName}:end`));
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

  override reduce<U>(fn: ReduceFunction<U, T>, initial: U): EventEmitterPipeline<U, In> {
    return super.reduce(fn, initial) as unknown as EventEmitterPipeline<U, In>;
  }

  /**
   * Registers the composed function as `stage:<n>`'s own first Worker, once per stage index -
   * `_registeredStages` (a `Set`, carried BY REFERENCE through `createPipeline()`) is what makes
   * this a once-EVER registration rather than once per bound call, since `stageWork()` itself
   * replays on every call. Dispatch reads `emitter.listeners(eventName)` itself and calls each
   * directly INSIDE a `try`, wrapped in `Promise.resolve(...).catch(...)`, never `emitter.emit()` -
   * `emit()` cannot catch a Worker's throw after its own `await`, and the `try` is what stops a
   * Worker's SYNCHRONOUS throw aborting the loop before every later Worker has had its turn. Every
   * registered Worker runs on every chunk; the first to SETTLE, `respond()` or `reject()`, decides
   * it - a native `Promise`'s own idempotence makes every later settle on the same dispatch a
   * no-op, guarded again here (`settled`) so the LIFECYCLE events stay exactly-once too. No
   * listener at all rejects immediately, naming the stage.
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
      // A synchronous throw here (no Transformer.onError() row handler registered) is caught by
      // the dispatch loop's own try/catch below, the same as any external Worker's - this listener
      // needs no guard of its own.
      this.emitter.on(eventName, (event: WorkEvent<T, U>) => {
        Promise.resolve(runnable(event.chunk, event.ctx)).then(event.respond, event.reject);
      });
      this._registeredStages.add(eventName);
    }

    const emitter = this.emitter;
    // Computed once per stage (not once per chunk) - stageWork() itself already replays on every
    // bound call, but the string never varies across a single such call's own dispatches.
    const dispatchedEvent = `${eventName}:dispatched`;
    const doneEvent = `${eventName}:done`;
    const errorEvent = `${eventName}:error`;
    return (chunk, ctx) =>
      new Promise<U[]>((resolve, reject) => {
        // Every lifecycle emit, `:dispatched` included, goes through `emitSafely` - a throwing
        // observer on ANY of them surfaces as its own separate uncaught exception, never silently
        // absorbed as if it were a Worker's own failure and never masking a real one.
        emitSafely(emitter, dispatchedEvent, { chunk, ctx });

        // ONE settle path for both outcomes - settles the REAL dispatch first, unconditionally,
        // THEN emits the matching lifecycle event, so a throwing `:done`/`:error` listener can
        // never leave this Promise hanging, only ever surface as its own separate, later failure.
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
            reject(
              outcome.error instanceof Error ? outcome.error : new Error(String(outcome.error)),
            );
            emitSafely(emitter, errorEvent, { error: outcome.error, ctx });
          }
        };
        const respond = (value: U[]): void => settle({ ok: true, value });
        // oxlint-disable-next-line anti-slop/no-unknown-parameters -- a Worker may reject with anything, the same as a Promise; genuinely unknown, not a gap
        const doReject = (error: unknown): void => settle({ ok: false, error });

        const listeners = emitter.listeners(eventName);
        if (listeners.length === 0) {
          doReject(new Error(`no worker registered on stage ${stageIndex} (${eventName})`));
          return;
        }
        // One event object for every listener on this dispatch - none of its fields vary per
        // listener, so building it once outside the loop saves a redundant allocation per Worker.
        const event: WorkEvent<T, U> = { chunk, ctx, respond, reject: doReject };
        for (const fn of listeners) {
          try {
            Promise.resolve((fn as (event: WorkEvent<T, U>) => void)(event)).catch(doReject);
          } catch (error) {
            // The Worker threw SYNCHRONOUSLY, before Promise.resolve ever wrapped it - caught here
            // so it settles like any other failure instead of aborting the loop and skipping every
            // Worker registered after this one.
            doReject(error);
          }
        }
      });
  }

  /**
   * Wraps `Pipeline.drainable()`'s own `items`/`chunks` thunks so `pipeline:end` fires once the
   * wrapped stream is exhausted - once per TERMINAL CALL, matching `PipelineResult`'s own "every
   * terminal re-drains" contract: calling `.first()` then `.toArray()` on the same result fires it
   * twice, since each terminal calls `drainable()` fresh. `syncChunks` is always `null` here - a
   * dispatching class forces `"async"` Mode, so there is no sync stream to wrap.
   */
  override drainable(input: PipelineSource<In>): Drainable<T> {
    const base = super.drainable(input);
    const emitter = this.emitter;
    let fired = false;
    const fireOnce = (): void => {
      if (fired) return;
      fired = true;
      emitSafely(emitter, "pipeline:end");
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
export interface WorkEvent<In, Out> {
  /** The chunk this dispatch is for. */
  chunk: In[];
  /** The run's shared context. */
  ctx: IContextManager;
  /** Settles the dispatch successfully with this stage's own output chunk. */
  respond: (value: Out[]) => void;
  /** Settles the dispatch as a failure. */
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- a Worker may reject with anything, the same as a Promise; genuinely unknown, not a gap
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

/**
 * Emits a lifecycle event through the ORDINARY `emitter.emit()` call - the simplest shape available,
 * deliberately: a caller-supplied emitter's own `.emit()` override and Node's own `.once()` unwrap
 * machinery both keep working exactly as documented. A SYNCHRONOUS listener throw is caught here and
 * surfaced as its own separate uncaught exception on a later microtask, rather than escaping the
 * `.then()` callback `respond()`/`doReject()` run inside (`stageWork()`, above), which has no
 * downstream `.catch()` of its own. Two limitations this simplicity accepts, by decision, not fixed:
 * a synchronous throw still stops `.emit()`'s own internal loop before a listener registered AFTER
 * the throwing one on the SAME event ever runs - ordinary `EventEmitter` behavior, not a guarantee
 * this class makes about listener isolation; an ASYNC listener throwing AFTER its own `await` leaks
 * as a real `unhandledRejection` instead, since `.emit()` never awaits a listener's return value.
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
