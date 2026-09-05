/**
 * `ClusterPipeline` — STUB (#17 L1-L4): `.transform()`/`.apply()` are real (inherited from
 * `HttpPipeline`/`ConcurrentPipeline` unchanged), so building a chain never throws and never
 * touches a network socket.
 *
 * `stageWork()` itself runs at BUILD time, synchronously - `ConcurrentPipeline.apply()` calls it
 * to get the function it fans out, before any chunk exists (review, #17 L3→L4: an earlier draft of
 * this comment claimed `stageWork()` "only ever runs when a chunk is dispatched", which is false -
 * verified live, `new ClusterPipeline([1]).transform(f)` throws immediately). The RETURNED
 * closure is what stays lazy, and that is where this stub's throw lives - a chain still builds
 * without ever draining it (case 7, `.constructor.name` after two `.transform()` calls, checks
 * exactly that). L5's real worker bootstrap belongs inside that same closure, on its first call,
 * for the identical reason: a `ClusterPipeline` built in-process during a test must never fork on
 * construction, only on an actual drain.
 */

import type { ConcurrentPipelineOptions, StageOptions } from "@src/pipelines/concurrent";
import { HttpPipeline } from "@src/pipelines/http";
import type { PipelineOptions, PipelineSource } from "@src/pipeline";
import type { Transformer } from "@src/transformer";
import type { InternalTransformer } from "@src/types";

/** Construction-time knobs for `ClusterPipeline`. */
export type ClusterPipelineOptions = { workers?: number } & ConcurrentPipelineOptions;

/** `ClusterPipeline`'s real constructor parameter type - see `ConcurrentPipelineConstructorOptions`
 * (`pipelines/concurrent.ts`) for why the base `Pipeline` internals must be included here too. */
type ClusterPipelineConstructorOptions = ClusterPipelineOptions & PipelineOptions;

/**
 * Each chunk of a stage dispatched to another process on the SAME machine (#17). Brings up its
 * own `node:cluster` workers on first run; every later `ClusterPipeline` in the process reuses
 * them. Fully opaque: no server, no listen, no fork, no url in caller code.
 *
 * `new ClusterPipeline([1,2,3,4,5]).transform((t) => t.map((x) => x * 2)).toArray()` →
 * `[2,4,6,8,10]`, served by real worker processes (L5).
 */
export class ClusterPipeline<T> extends HttpPipeline<T> {
  /** Worker processes to bring up on first drain. `0` here is the L1/L2 stub default; L5 picks
   * the real one (`os.availableParallelism()`, per the ticket's own Constraints). */
  readonly workers: number;

  constructor(source: PipelineSource<T>, options?: ClusterPipelineConstructorOptions) {
    // The real url is only known once L5's bootstrap picks a port; "" is inert here — never
    // dialled, since every method below still throws.
    super(source, { ...options, url: "" });
    this.workers = options?.workers ?? 0;
  }

  /**
   * Carries `workers` into the NEXT instance a copy-on-write call builds, alongside
   * `maxConcurrency`/`ordered`/`chunkSize` (`concurrentOptions()`, inherited) and `url` (currently
   * inert here - the constructor below forces `url: ""` regardless of what it is given, until L5's
   * bootstrap gives it a real one to forward).
   */
  protected override createPipeline<U>(
    data: AsyncIterable<U>,
    options: PipelineOptions,
  ): ClusterPipeline<U> {
    const Ctor = this.constructor as new (
      data: AsyncIterable<U>,
      options?: ClusterPipelineConstructorOptions & { url: string },
    ) => ClusterPipeline<U>;
    const merged = {
      ...options,
      ...this.concurrentOptions(),
      workers: this.workers,
      url: this._url,
    };
    return new Ctor(data, merged);
  }

  override transform<U>(
    builder: (t: Transformer<T, T>) => Transformer<T, U>,
    options?: StageOptions,
  ): ClusterPipeline<U> {
    return super.transform(builder, options) as ClusterPipeline<U>;
  }

  override apply<U>(transformer: Transformer<T, U>, options?: StageOptions): ClusterPipeline<U> {
    return super.apply(transformer, options) as ClusterPipeline<U>;
  }

  protected override stageWork<U>(
    _transformer: Transformer<T, U>,
    _stageIndex: number,
  ): InternalTransformer<T, U> {
    // The throw lives INSIDE the returned closure, not here - stageWork() itself must succeed at
    // build time (see the class docstring), so a chain builds without ever draining it.
    return () => {
      throw new Error("ClusterPipeline.stageWork: not implemented (#17 L5)");
    };
  }
}
