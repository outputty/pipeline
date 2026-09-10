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
 *
 * `Pipeline.branch()` is a thin method over `runBranch` below: it claims the branch's index in the
 * shared stage space and hands the arms here. The whole feature - the demux, the router and the
 * join - lives in this file, and its edge to `pipeline.ts` is type-only.
 */

import type { AnyPipeline, Pipeline, PipelineSource } from "./pipeline";
import type { IContextManager, JoinMode, PipelineMode } from "./types";
import type { MaybeAsyncChunks } from "./utils/chunk";
import { collectItems } from "./utils/chunk";
import { chain, mapSettle } from "./utils/helpers";

/** An arm's own pipeline, before its builder composes anything onto it. */
export type ArmPipeline<T> = Pipeline<T, "unset", T>;

/** What `runBranch` needs of the pipeline it belongs to: the one drain seam, nothing else. Declared
 * structurally so this file never imports `Pipeline` at runtime. */
export interface BranchOwner<T, In> {
  drainable(input: PipelineSource<In>): {
    syncChunks: MaybeAsyncChunks<T> | null;
    items: () => AsyncIterable<T>;
    context: IContextManager;
  };
}

/** The record a builder's arms produce, read off the builder the caller's callback returned. */
export type ResultsOf<B> =
  B extends BranchBuilder<any, infer R, any> ? { [K in keyof R]: R[K] } : never;

/** The Mode a builder's arms join to: `"async"` the moment ONE arm is, because the runtime widens
 * the whole record rather than one key. Read off the builder alongside `ResultsOf`. */
export type ModeOfArms<B> = B extends BranchBuilder<any, any, infer AM> ? AM : never;

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
export class BranchBuilder<T, R = Record<never, never>, AM extends PipelineMode = "unset"> {
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
  when<K extends string, U = T, M2 extends PipelineMode = "unset">(
    name: K,
    predicate: (item: T) => boolean,
    build?: (pipeline: Pipeline<T, "unset", T>) => Pipeline<U, M2, any>,
  ): BranchBuilder<T, R & Record<K, U[]>, JoinMode<AM, M2>> {
    this.claim(name);
    this._arms.push({ name, predicate, build: build as BranchArm<T>["build"] });
    return this as unknown as BranchBuilder<T, R & Record<K, U[]>, JoinMode<AM, M2>>;
  }

  /**
   * The catch-all, routed last whatever order it was written in - so one written first cannot
   * silently swallow the arms below it, which is exactly what `predicate: () => true` declared
   * first used to do.
   *
   * Under router mode (the default) it takes every item no earlier arm claimed. Under
   * `.broadcast()` it takes EVERY item, because broadcast means every matching arm and its
   * predicate accepts all of them.
   *
   * @example
   * `.otherwise("rest", (p) => p.transform((t) => t.map((o) => o.id)))` collects the unmatched
   * orders and yields their ids.
   */
  otherwise<K extends string, U = T, M2 extends PipelineMode = "unset">(
    name: K,
    build?: (pipeline: Pipeline<T, "unset", T>) => Pipeline<U, M2, any>,
  ): BranchBuilder<T, R & Record<K, U[]>, JoinMode<AM, M2>> {
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
    return this as unknown as BranchBuilder<T, R & Record<K, U[]>, JoinMode<AM, M2>>;
  }

  /**
   * Sends every item to EVERY arm whose predicate accepts it, rather than only the first. Router
   * mode is the default, so most callers write neither.
   *
   * @example
   * `.when("eu", isEu).when("big", isBig).broadcast()` puts a big EU order in both arms.
   */
  broadcast(): BranchBuilder<T, R, AM> {
    this._broadcast = true;
    return this;
  }

  /** The record this builder's arms produce - each key typed by its OWN arm, accumulated as
   * `.when()`/`.otherwise()` are called. Never inhabited; `.branch()` reads it with `infer`. */
  declare readonly results: R;

  /** The joined Mode of every arm. Never inhabited; `.branch()` reads it with `infer`. */
  declare readonly armMode: AM;

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
    // The name goes straight into a route - `/branch/<i>/<name>/transform/<n>` - and `.fetch()`
    // matches against an ENCODED pathname, so anything needing encoding never resolves. Measured:
    // `.when("big orders", …)` dispatched `/branch/0/big%20orders/transform/0` and 404'd.
    // `.` and `..` pass the character class but are RELATIVE path segments: `new URL()` rewrites
    // `/branch/0/./transform/0` to `/branch/0/transform/0`, which misses `.fetch()`'s trail regex
    // and serves the PARENT chain's stage 0 - wrong data, no error. `..` normalises to
    // `/branch/transform/0` and 404s.
    if (!/^[A-Za-z0-9_.~-]+$/.test(name) || name === "." || name === "..") {
      throw new Error(
        `branch "${name}" is not usable in a route - use letters, digits, and any of _ . ~ -, and not "." or ".." alone`,
      );
    }
  }

  private catchAllName(): string {
    return this._arms.find((arm) => arm.isCatchAll)!.name;
  }
}

/**
 * Groups one run's items by the arm each belongs to (#90) - `.branch()`'s demux.
 *
 * Runs in the ORCHESTRATING process by decision, never dispatched: a predicate decides WHICH arm an
 * item enters, so sending it out would cost every item two trips (one to be classified, one to be
 * worked on) and would stop a predicate closing over anything the caller holds.
 *
 * @example
 * `demux(orders, [big, rest], false)` → `Map { "big" => [order 2, order 4], "rest" => [order 1] }`.
 */
function demux<T>(items: T[], arms: readonly BranchArm<T>[], broadcast: boolean): Map<string, T[]> {
  const grouped = new Map<string, T[]>(arms.map((arm) => [arm.name, []]));
  for (const item of items) {
    claimItem(item, arms, grouped, broadcast);
  }
  return grouped;
}

/** One item's own routing pass, split out so `demux` stays within this repo's nesting limit. */
function claimItem<T>(
  item: T,
  arms: readonly BranchArm<T>[],
  grouped: Map<string, T[]>,
  broadcast: boolean,
): void {
  for (const arm of arms) {
    if (!arm.predicate(item)) continue;
    grouped.get(arm.name)!.push(item);
    if (!broadcast) return;
  }
}

/**
 * What `.branch()` returns (#90): the arms bound once, callable with any input.
 *
 * `.branch()` is a STAGE, not a terminal - it hands back a runner rather than the results, so the
 * definitions are written once and the caller picks what to do with each call's record. The Mode
 * follows the same rule every other terminal does: every arm synchronous returns the record plainly,
 * and one asynchronous arm widens the whole record to a single `Promise`.
 *
 * `split(orders)` → `{ big: ["BIG:2"], eu: [1, 3], rest: [] }`.
 */
export interface BranchRunner<In, R, M extends PipelineMode> {
  (input: AsyncIterable<In>): Promise<R>;
  // Keyed on `"async"`, not on `"sync"`: `"unset"` is the ordinary state of a composed chain and is
  // synchronous over a synchronous input, so testing for `"sync"` would type every undecided chain's
  // record a `Promise` while the runtime handed back the record plainly.
  (input: Iterable<In>): M extends "async" ? Promise<R> : R;
}

/** What `.branch()` produces: one record, keyed by arm name, joined on the orchestrator - the only
 * process that sees every arm, since arms can be remote. Typed loosely on the arms' own outputs,
 * because a builder's arms are collected at runtime rather than inferred from an object literal. */
export type BranchResults = Record<string, unknown[]>;

/**
 * One `.branch()` call's runner (#90): binds the parent chain to an input, groups the items by arm,
 * runs each arm's own pipeline over its own group, and joins the results into one record.
 *
 * Every step after the demux runs where the caller is, by necessity rather than by choice: arms can
 * be remote, so the orchestrator is the only process that sees all of them.
 *
 * Mode follows the same rule every terminal does - every arm synchronous returns the record plainly,
 * and one asynchronous arm widens the whole record to a single `Promise`, with its synchronous
 * siblings never wrapped.
 *
 * `runBranch({ owner, arms: [big, rest], broadcast: false, branchIndex: 0, makeArm })(orders)` →
 * `{ big: ["BIG:2"], rest: [order 1] }`.
 */
export function runBranch<T, In>(config: {
  owner: BranchOwner<T, In>;
  arms: readonly BranchArm<T>[];
  broadcast: boolean;
  branchIndex: number;
  makeArm: (context: IContextManager, routeTrail: string) => ArmPipeline<T>;
}): (input?: PipelineSource<In>) => BranchResults | Promise<BranchResults> {
  const { owner, arms, broadcast, branchIndex, makeArm } = config;

  return (input?: PipelineSource<In>) => {
    if (input === undefined) {
      throw new Error(
        "no input: a pipeline holds no data, so .branch()'s runner needs one - call it with the items to route",
      );
    }

    // ONE bind for the whole branch: the parent chain runs, and the arms below share the context
    // that run created rather than the chain's own.
    const { syncChunks, items: itemsOf, context } = owner.drainable(input);
    const items = collectItems(syncChunks, itemsOf) as T[] | Promise<T[]>;

    // `chain` defers only at a real thenable, so a synchronous parent stays synchronous here.
    return chain(items, (settled: T[]) =>
      joinArms(demux(settled, arms, broadcast), arms, branchIndex, makeArm, context),
    ) as BranchResults | Promise<BranchResults>;
  };
}

/**
 * The router and the join (#90): each arm's own pipeline over its own items, then one record.
 *
 * An arm's pipeline is of the PARENT's class, so its stages dispatch wherever the parent's do and
 * `.local()` inside the arm's builder pins it. A synchronous arm returns its array right here; only
 * an asynchronous one hands back a promise, and only those are awaited.
 *
 * Its own function so `runBranch` above stays within this repo's nesting limit.
 */
function joinArms<T>(
  grouped: Map<string, T[]>,
  arms: readonly BranchArm<T>[],
  branchIndex: number,
  makeArm: (context: IContextManager, routeTrail: string) => ArmPipeline<T>,
  context: IContextManager,
): BranchResults | Promise<BranchResults> {
  // `mapSettle`, never a bare `arms.map(...)`: an arm's own callbacks can throw SYNCHRONOUSLY after
  // an earlier arm already returned a pending `toArray()`. `Array.prototype.map` abandons the array
  // there, so that promise never reaches `settleMaybe` and never gets a rejection handler - measured
  // before this, a branch whose `evens` arm failed asynchronously and whose `odds` arm threw
  // synchronously reported `odds arm failed` to the caller and then killed the process on `evens`.
  // `mapSettle` disarms what was already created before rethrowing, and settles the rest.
  const outputs = mapSettle(arms as BranchArm<T>[], (arm) => {
    const armItems = grouped.get(arm.name)!;
    if (arm.build === undefined) return armItems as unknown[];
    const built = arm.build(makeArm(context, `/branch/${branchIndex}/${arm.name}`)) as unknown as (
      input: T[],
    ) => { toArray(): unknown[] | Promise<unknown[]> };
    return built(armItems).toArray();
  });

  return chain(outputs, (armResults: unknown[][]) =>
    Object.fromEntries(arms.map((arm, index) => [arm.name, armResults[index]])),
  ) as BranchResults | Promise<BranchResults>;
}
