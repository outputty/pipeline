/**
 * `EventEmitterPipeline` (#124, events renamed to routes #221) — a fourth `Pipeline` dispatch mode:
 * each chunk of a stage handed directly to the chain's own composed function, and to whichever extra
 * Worker functions a caller registered on `pipeline.emitter`, instead of POSTed over HTTP or sent to
 * another process. `stageWork()` is the only dispatch override - `ConcurrentPipeline.apply()`'s own
 * fan-out (`fanOutOrdered`/`fanOutUnordered`, `maxConcurrency`, `ordered`) is inherited UNCHANGED.
 * `apply()` and `drainable()` are each overridden a second time, on top of that, purely to emit
 * `<route>:end`/`<trail>:end`.
 *
 * Every event is named after the route the chain was built along - `routePath("transform", n)`,
 * the same `/transform/<n>`/`/branch/<i>/<name>/transform/<n>` grammar `HttpPipeline` dispatches to -
 * and the composed function is called directly, never registered as a listener: two forks of one
 * chain, two sibling `.branch()` arms, and two independently-constructed pipelines sharing one
 * `emitter` each call their OWN composed function with nothing shared to race on, whatever their
 * route happens to name (#221). `emitter.listeners(route)` only ever holds Workers a caller added.
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
  on(event: string, listener: (...args: any[]) => void): void;
  off(event: string, listener: (...args: any[]) => void): void;
  listeners(event: string): Array<(...args: any[]) => void>;
  /** A caller's own way to inspect how many Workers are registered on a route - `node:events` ships
   * this natively, so declaring it here costs nothing for the shipped emitter. */
  listenerCount(event: string): number;
  emit(event: string, ...args: unknown[]): void;
}

/** `options.emitter` is a trust-boundary value - a caller's own compatible emitter, not necessarily
 * `node:events`' own - so a missing method fails HERE, at construction, naming what is missing,
 * rather than surfacing later as a generic `TypeError` deep inside `stageWork()`'s dispatch closure
 * (this repo's own Fail Loud rule: "External data missing an expected field fails at the parse").
 * Called on EVERY construction (#221) - a long chain's `.transform()`/`.buffer()`/`.context()` calls
 * each re-validate the identical, unchanged `emitter` object, which costs nothing for a real one and
 * is what makes a caller-supplied emitter's own `off()` gap fail loud at every layer, not only the
 * first. */
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

export class EventEmitterPipeline<T, In = T> extends ConcurrentPipeline<T, In> {
  readonly emitter: PipelineEmitter;

  /** Wraps a chain built elsewhere, dispatching its stages through the emitter (#90's own
   * wrapping-constructor pattern, `HttpPipeline`/`ClusterPipeline` share it) - the WORKER and the
   * TRIGGER can then share one definition. */
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
   * Carries `emitter` into the NEXT instance a copy-on-write call builds, on top of what
   * `ConcurrentPipeline.carriedKnobs()` already carries forward (#133) - same reason, one more
   * field. BY REFERENCE, never copied: the `emitter`'s own identity (a caller-supplied one, or the
   * one built above) must be the SAME object across every instance a
   * `.transform()`/`.buffer()`/`.context()` call derives, and across every `.branch()` arm
   * (`Pipeline.emptyOfOwnClass()`), which is what lets a Worker registered on the shared emitter
   * answer an arm's own route.
   */
  protected override carriedKnobs(): ConcurrentPipelineOptions & { emitter: PipelineEmitter } {
    return {
      ...super.carriedKnobs(),
      emitter: this.emitter,
    };
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
   * own output chunk stream so `<route>:end` fires once, after every chunk that stage's fan-out
   * produced has been yielded (Done-when 1). `stageIndex` is read OFF THE RESULT
   * (`dispatched._chunkTransforms.length - 1`, the slot `super.apply()` just appended), never
   * re-derived by independently repeating `ConcurrentPipeline.apply()`'s own internal computation -
   * so this stays correct even if that computation ever changes, with nothing to keep in sync by
   * hand. `dispatched.routePath(...)` (not `this.routePath(...)`) is what makes the route carry the
   * `.branch()` arm's own trail when `dispatched` is an arm's pipeline, not the parent's.
   *
   * A still-deferred result (no source bound yet) is returned unwrapped: `super.apply()` itself
   * only RECORDED this call, to replay later against a bound instance - where this method runs
   * again, and wraps for real.
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

  /** Re-declared ONLY to narrow `Pipeline.queue()`'s return type (#123,
   * `~/.claude/rules/typescript.md`) - the body is an unchanged `super()` call: a queued chunk still
   * dispatches through whatever `stageWork()` override this class already runs. */
  override queue(capacity: number): EventEmitterPipeline<T, In> {
    return super.queue(capacity) as unknown as EventEmitterPipeline<T, In>;
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
   * Calls the chain's own composed function DIRECTLY - never registered as a listener (#221) - and
   * every Worker a caller separately registered on `emitter`, addressed by the SAME route this
   * stage dispatches under (`this.routePath("transform", stageIndex)`, `HttpPipeline`'s own
   * grammar). Calling the composed function directly rather than through the emitter is what makes
   * a fork, a sibling `.branch()` arm and a shared `emitter` each answer with their OWN output
   * (#221 Done-when 2, 3, 4) - nothing about them is shared to race on any more, since neither ever
   * touches the emitter at all.
   *
   * Dispatch reads `emitter.listeners(route)` itself and calls each directly INSIDE a `try`, wrapped
   * in `Promise.resolve(...).catch(...)`, never `emitter.emit()` - `emit()` cannot catch a Worker's
   * throw after its own `await` (Done-when 6 in #124, unchanged), and the `try` is what stops a
   * Worker's SYNCHRONOUS throw aborting the loop before every later Worker has had its turn. Every
   * Worker AND the composed function run on every chunk; the first to SETTLE, `respond()`/`reject()`
   * or the composed function's own resolve/reject, decides it - a native `Promise`'s own idempotence
   * makes every later settle on the same dispatch a no-op, guarded again here (`settled`) so the
   * LIFECYCLE events stay exactly-once too. The composed function always answers, so there is no
   * "no worker registered" case left to reject (#124's own rejection for it is deleted, #221
   * Done-when 9).
   *
   * `stageWork(transformer, 0)` returns a function that, called with `([1,2], ctx)`, emits
   * `/transform/0:dispatched`, runs the composed function and every registered Worker, and settles
   * with whichever responds or rejects first.
   */
  protected override stageWork<U>(
    transformer: Transformer<T, U, "sync" | "async">,
    stageIndex: number,
  ): InternalTransformer<T, U> {
    const runnable = transformer.runnable();
    const emitter = this.emitter;
    const route = this.routePath("transform", stageIndex);
    // Computed once per stage (not once per chunk) - stageWork() itself already replays on every
    // bound call, but the string never varies across a single such call's own dispatches.
    const dispatchedEvent = `${route}:dispatched`;
    const doneEvent = `${route}:done`;
    const errorEvent = `${route}:error`;
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

        // The chain's own composed function - called directly, never through the emitter, so it
        // never appears in `emitter.listeners(route)` or `emitter.eventNames()` (Done-when 6).
        try {
          Promise.resolve(runnable(chunk, ctx)).then(respond, doReject);
        } catch (error) {
          doReject(error);
        }

        // One event object for every Worker on this dispatch - none of its fields vary per Worker,
        // so building it once outside the loop saves a redundant allocation per one.
        const event: WorkEvent<T, U> = { chunk, ctx, respond, reject: doReject };
        for (const fn of emitter.listeners(route)) {
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
   * Wraps `Pipeline.drainable()`'s own `chunks` thunk so `<trail>:end` fires once the wrapped
   * stream is exhausted - once per TERMINAL CALL, matching `PipelineResult`'s own "every terminal
   * re-drains" contract: calling `.first()` then `.toArray()` on the same result fires it twice
   * (#124 Done-when 10), since each terminal calls `drainable()` fresh. `syncChunks` is always
   * `null` here - a dispatching class forces `"async"` Mode, so there is no sync stream to wrap.
   * `this._routeTrail` is `""` for a chain, so the event reads `:end`; for a `.branch()` arm it
   * reads `/branch/<i>/<name>:end` (#221) - the arm's own drain, distinct from any stage's own
   * `<route>:end`.
   *
   * ONE thunk covers every terminal since #179: `toArray`/`first`/`forEach`/`consume` and both
   * iteration protocols all read the chunk view now, where a flattened item view used to need its
   * own identical wrap beside this one.
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

/** The shape a registered Worker receives - a plain event object, never the raw
 * `InternalTransformer` signature, so a Worker is a function of ONE argument regardless of what the
 * composed transform's own arity looks like. */
export interface WorkEvent<In, Out> {
  chunk: In[];
  ctx: IContextManager;
  respond: (value: Out[]) => void;
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- a Worker may reject with anything, the same as a Promise; genuinely unknown, not a gap
  reject: (error: unknown) => void;
}

/** Wraps `source`, calling `onEnd` exactly once after the LAST value is yielded - whether the
 * consumer drains it to natural exhaustion or stops early (`.first(n)`'s own early return, which
 * calls the generator's `.return()` per the iterator protocol and runs this `finally`). Shared by
 * `apply()` (`<route>:end`) and `drainable()` (`<trail>:end`) - the identical "fire once, on
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
