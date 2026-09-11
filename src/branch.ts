import type { AnyPipeline, Pipeline, PipelineSource } from "./pipeline";
import type { Drainable, IContextManager, JoinMode, PipelineMode } from "./types";
import { collectItems } from "./utils/chunk";
import { chain, mapSettle } from "./utils/helpers";

/** An arm's own pipeline, before its builder composes anything onto it. */
export type ArmPipeline<T> = Pipeline<T, "unset", T>;

/**
 * The one drain seam `runBranch` needs from the pipeline it belongs to, declared structurally so
 * this file never imports `Pipeline` at runtime.
 *
 * `Drainable<T>` carries a fourth `chunks` field this interface leaves out: `runBranch` only ever
 * needs the sync view, the item stream and the run's own context, so `Pick` names just those three
 * instead of re-spelling their types.
 */
export interface BranchOwner<T, In> {
  /** Binds `input` and returns the sync-chunk, item and context views `runBranch` reads from it. */
  drainable(input: PipelineSource<In>): Pick<Drainable<T>, "syncChunks" | "items" | "context">;
}

/** The record a builder's arms produce, read off the builder the caller's callback returned. */
export type ResultsOf<B> =
  B extends BranchBuilder<any, infer R, any> ? { [K in keyof R]: R[K] } : never;

/** The Mode a builder's arms join to: `"async"` the moment ONE arm is, because the runtime widens
 * the whole record rather than one key. Read off the builder alongside `ResultsOf`. */
export type ModeOfArms<B> = B extends BranchBuilder<any, any, infer AM> ? AM : never;

/** One arm, as the builder collects it. */
export interface BranchArm<T> {
  /** The arm's name - also its key in the results record and its route path segment. */
  name: string;
  /** Decides whether an item enters this arm. */
  predicate: (item: T) => boolean;
  /** Builds this arm's own pipeline. Absent, the arm only routes and its items pass through
   * unchanged. */
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
    return this.pushArm<K, U, M2>(name, predicate, build);
  }

  /**
   * The catch-all, routed last whatever order it was written in, so an arm written first can never
   * silently swallow the arms below it.
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
    const existing = this.findCatchAll();
    if (existing !== undefined) {
      throw new Error(`.otherwise() is already declared as "${existing.name}"`);
    }
    return this.pushArm<K, U, M2>(name, () => true, build, true);
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

  declare readonly results: R;

  declare readonly armMode: AM;

  /** The arms in routing order - declaration order, with the catch-all moved last. Read once by
   * `.branch()` after the caller's builder returns. */
  arms(): BranchArm<T>[] {
    const ordered = this._arms.filter((arm) => !arm.isCatchAll);
    const catchAll = this.findCatchAll();
    return catchAll ? [...ordered, catchAll] : ordered;
  }

  /** Whether every matching arm takes an item, rather than only the first. */
  isBroadcast(): boolean {
    return this._broadcast;
  }

  /**
   * Builds the arm, pushes it, and recasts `this` to the builder's own next generic instantiation.
   *
   * Called after each caller has already `claim()`ed the arm's own name (and, for `.otherwise()`,
   * checked for an existing catch-all) - those checks are specific enough to each caller that
   * folding them in here would either run the catch-all check for `.when()` too or skip it for
   * `.otherwise()`.
   */
  private pushArm<K extends string, U, M2 extends PipelineMode>(
    name: string,
    predicate: (item: T) => boolean,
    build: ((pipeline: Pipeline<T, "unset", T>) => Pipeline<U, M2, any>) | undefined,
    isCatchAll?: boolean,
  ): BranchBuilder<T, R & Record<K, U[]>, JoinMode<AM, M2>> {
    this._arms.push({ name, predicate, build: build as BranchArm<T>["build"], isCatchAll });
    return this as unknown as BranchBuilder<T, R & Record<K, U[]>, JoinMode<AM, M2>>;
  }

  /** Refuses a name twice in ONE branch. Two separate `.branch()` calls may each declare a `rest`,
   * because their routes differ by the branch stage's own index - this only catches a collision
   * inside one call, where the second arm would otherwise overwrite the first's results key. */
  private claim(name: string): void {
    if (this._arms.some((arm) => arm.name === name)) {
      throw new Error(`branch "${name}" is already declared in this .branch() call`);
    }
    // The name becomes a route path segment - `/branch/<i>/<name>/transform/<n>` - so only
    // characters that resolve as written are accepted.
    if (!/^[A-Za-z0-9_.~-]+$/.test(name) || name === "." || name === "..") {
      throw new Error(
        `branch "${name}" is not usable in a route - use letters, digits, and any of _ . ~ -, and not "." or ".." alone`,
      );
    }
  }

  /** The catch-all arm, if one has been declared - `arms()` and `.otherwise()`'s own conflict check
   * both read this instead of each spelling `find((arm) => arm.isCatchAll)`. */
  private findCatchAll(): BranchArm<T> | undefined {
    return this._arms.find((arm) => arm.isCatchAll);
  }
}

/**
 * Groups one run's items by the arm each belongs to - `.branch()`'s demux.
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
 * What `.branch()` returns: the arms bound once, callable with any input.
 *
 * `.branch()` is a STAGE, not a terminal - it hands back a runner rather than the results, so the
 * definitions are written once and the caller picks what to do with each call's record.
 *
 * `split(orders)` → `{ big: ["BIG:2"], eu: [1, 3], rest: [] }`.
 */
export interface BranchRunner<In, R, M extends PipelineMode> {
  /** Runs the arms over an async source, returning one record. */
  (input: AsyncIterable<In>): Promise<R>;
  // Keyed on `"async"`, not on `"sync"`: `"unset"` is the ordinary state of a composed chain and is
  // synchronous over a synchronous input, so testing for `"sync"` would type every undecided chain's
  // record a `Promise` while the runtime handed back the record plainly.
  /** Runs the arms over a sync source; the record stays plain unless an arm widens Mode. */
  (input: Iterable<In>): M extends "async" ? Promise<R> : R;
}

/** What `.branch()` produces: one record, keyed by arm name, joined on the orchestrator - the only
 * process that sees every arm, since arms can be remote. Typed loosely on the arms' own outputs,
 * because a builder's arms are collected at runtime rather than inferred from an object literal. */
export type BranchResults = Record<string, unknown[]>;

/**
 * The fields `joinArms` needs from `runBranch`'s own config, threaded as one object.
 *
 * `context` stays its own parameter on `joinArms` rather than living here: it comes from the drain,
 * once per RUN, never from this config, which `.branch()` builds once when the arms are declared.
 */
interface ArmDispatch<T> {
  arms: readonly BranchArm<T>[];
  branchIndex: number;
  makeArm: (context: IContextManager, routeTrail: string) => ArmPipeline<T>;
}

/**
 * One `.branch()` call's runner: binds the parent chain to an input, groups the items by arm, runs
 * each arm's own pipeline over its own group, and joins the results into one record.
 *
 * `runBranch({ owner, arms: [big, rest], broadcast: false, branchIndex: 0, makeArm })(orders)` →
 * `{ big: ["BIG:2"], rest: [order 1] }`.
 */
export function runBranch<T, In>(
  config: { owner: BranchOwner<T, In>; broadcast: boolean } & ArmDispatch<T>,
): (input?: PipelineSource<In>) => BranchResults | Promise<BranchResults> {
  const { owner, arms, broadcast } = config;

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
      joinArms(demux(settled, arms, broadcast), config, context),
    ) as BranchResults | Promise<BranchResults>;
  };
}

/**
 * The router and the join: each arm's own pipeline over its own items, then one record.
 *
 * Its own function so `runBranch` above stays within this repo's nesting limit.
 */
function joinArms<T>(
  grouped: Map<string, T[]>,
  dispatch: ArmDispatch<T>,
  context: IContextManager,
): BranchResults | Promise<BranchResults> {
  const { arms, branchIndex, makeArm } = dispatch;
  // `mapSettle`, never a bare `arms.map(...)`: a later arm's own callback can throw SYNCHRONOUSLY
  // after an earlier arm already returned a pending `toArray()`, and `Array.prototype.map` would
  // abandon that pending promise with no rejection handler ever attached. `mapSettle` disarms what
  // was already created before rethrowing, and settles the rest.
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
