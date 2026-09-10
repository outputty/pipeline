/**
 * `.branch()`'s builder and the arms it collects (#90).
 *
 * A branch arm pairs a predicate with an optional PIPELINE of the parent's own class. That class is
 * what decides where the arm's work runs - so an arm dispatches by default on
 * `ConcurrentPipeline`/`HttpPipeline`/`ClusterPipeline`, and `.local()` inside the arm's own builder
 * pins it in the orchestrating process. The `Transformer` this replaces had no class, so every
 * branch ran where the caller was however the chain was built.
 *
 * The builder exists so the arms are written as calls rather than as one object literal: declaration
 * order IS routing order, and a fluent chain makes that literal instead of a property of key
 * iteration.
 */

import type { AnyPipeline } from "./pipeline";

/** The record a builder's arms produce, read off the builder the caller's callback returned. */
export type ResultsOf<B> = B extends BranchBuilder<any, infer R> ? { [K in keyof R]: R[K] } : never;

/** One arm, as the builder collects it. `build` absent means the arm routes only and its items pass
 * through unchanged - the friction `.transform()` never had, since it takes a builder (#87). */
export interface BranchArm<T> {
  name: string;
  predicate: (item: T) => boolean;
  build?: (pipeline: AnyPipeline<T>) => AnyPipeline<unknown>;
  /** Set by `.otherwise()`. A catch-all is always last, whatever order it was written in. */
  isCatchAll?: boolean;
}

/**
 * Collects the arms of one `.branch()` call.
 *
 * @example
 * `b.when("big", (o) => o.total > 200, (p) => p.transform(...)).otherwise("rest")` gives two arms,
 * routed in that order, with `rest` taking whatever `big` did not.
 */
export class BranchBuilder<T, R = Record<never, never>> {
  private readonly _arms: BranchArm<T>[] = [];
  private _broadcast = false;

  /**
   * Routes every item this predicate accepts to `name`.
   *
   * With no `build`, matching items pass through unchanged. With one, the arm gets its own pipeline
   * of the parent's class, so its stages dispatch wherever the parent's do.
   *
   * @example
   * `.when("big", (o) => o.total > 200, (p) => p.transform((t) => t.map((o) => o.id)))` sends the
   * big orders through their own stage and yields their ids.
   */
  when<K extends string, U = T>(
    name: K,
    predicate: (item: T) => boolean,
    build?: (pipeline: AnyPipeline<T>) => AnyPipeline<U>,
  ): BranchBuilder<T, R & Record<K, U[]>> {
    this.claim(name);
    this._arms.push({ name, predicate, build: build as BranchArm<T>["build"] });
    return this as unknown as BranchBuilder<T, R & Record<K, U[]>>;
  }

  /**
   * The catch-all: every item no earlier arm claimed. Always routed last, whatever order it was
   * written in, so a catch-all written first cannot silently swallow the arms below it - which is
   * exactly what `predicate: () => true` declared first used to do.
   *
   * @example
   * `.otherwise("rest", (p) => p.transform((t) => t.map((o) => o.id)))` collects the unmatched
   * orders and yields their ids.
   */
  otherwise<K extends string, U = T>(
    name: K,
    build?: (pipeline: AnyPipeline<T>) => AnyPipeline<U>,
  ): BranchBuilder<T, R & Record<K, U[]>> {
    this.claim(name);
    if (this._arms.some((arm) => arm.isCatchAll)) {
      throw new Error(`.otherwise() is already declared as "${this.catchAllName()}"`);
    }
    this._arms.push({
      name,
      predicate: () => true,
      build: build as BranchArm<T>["build"],
      isCatchAll: true,
    });
    return this as unknown as BranchBuilder<T, R & Record<K, U[]>>;
  }

  /**
   * Sends every item to EVERY arm whose predicate accepts it, rather than only the first. Router
   * mode is the default, so most callers write neither.
   *
   * @example
   * `.when("eu", isEu).when("big", isBig).broadcast()` puts a big EU order in both arms.
   */
  broadcast(): BranchBuilder<T, R> {
    this._broadcast = true;
    return this;
  }

  /** The record this builder's arms produce - each key typed by its OWN arm, accumulated as
   * `.when()`/`.otherwise()` are called. Never inhabited; `.branch()` reads it with `infer`. */
  declare readonly results: R;

  /** The arms in routing order - declaration order, with the catch-all moved last. Read once by
   * `.branch()` after the caller's builder returns. */
  arms(): BranchArm<T>[] {
    const ordered = this._arms.filter((arm) => !arm.isCatchAll);
    const catchAll = this._arms.find((arm) => arm.isCatchAll);
    return catchAll ? [...ordered, catchAll] : ordered;
  }

  /** Whether every matching arm takes an item, rather than only the first. */
  isBroadcast(): boolean {
    return this._broadcast;
  }

  /** Refuses a name twice in ONE branch. Two separate `.branch()` calls may each declare a `rest`,
   * because their routes differ by the branch stage's own index - this only catches a collision
   * inside one call, where the second arm would otherwise overwrite the first's results key. */
  private claim(name: string): void {
    if (this._arms.some((arm) => arm.name === name)) {
      throw new Error(`branch "${name}" is already declared in this .branch() call`);
    }
  }

  private catchAllName(): string {
    return this._arms.find((arm) => arm.isCatchAll)!.name;
  }
}
