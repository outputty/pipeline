/**
 * `.branch()`'s builder, the arms it collects, and the runner that routes items into them.
 *
 * An arm pairs a predicate with an optional pipeline of the parent's class, so an arm runs wherever
 * the parent's stages run. `.local()` inside an arm pins it to this process.
 */

import type { AnyPipeline, Pipeline, PipelineSource } from "./pipeline";
import type { Drainable, IContextManager, JoinMode, PipelineMode } from "./types";
import { drainSyncChunks, type MaybeAsyncChunks } from "./utils/drain";
import { chain, mapSettle } from "./utils/helpers";

/** An arm's own pipeline, before its builder composes anything onto it. */
export type ArmPipeline<T> = Pipeline<T, "unset", T>;

/** What `runBranch` needs of the pipeline it belongs to: its drain. Structural, so this file never
 * imports `Pipeline` at runtime.
 *
 * `owner.drainable(orders)` → the chunks and context `runBranch` routes. */
export interface BranchOwner<T, In> {
  drainable(input: PipelineSource<In>): Pick<Drainable<T>, "syncChunks" | "chunks" | "context">;
}

/** The record a builder's arms produce, read off the builder the caller's callback returned. */
export type ResultsOf<B> =
  B extends BranchBuilder<any, infer R, any> ? { [K in keyof R]: R[K] } : never;

/** The Mode a builder's arms join to: `"async"` the moment ONE arm is, because the runtime widens
 * the whole record rather than one key. Read off the builder alongside `ResultsOf`. */
export type ModeOfArms<B> = B extends BranchBuilder<any, any, infer AM> ? AM : never;

/** One arm, as the builder collects it. With no `build`, the arm's items pass through unchanged.
 *
 * `{ name: "big", predicate: (o) => o.total > 200 }` → an arm that collects the big orders as-is. */
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
    return this.pushArm<K, U, M2>(name, predicate, build);
  }

  /**
   * The catch-all, routed last whatever order it was written in, so it never swallows a later arm.
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

  /** The record this builder's arms produce - each key typed by its OWN arm, accumulated as
   * `.when()`/`.otherwise()` are called. Never inhabited; `.branch()` reads it with `infer`. */
  declare readonly results: R;

  /** The joined Mode of every arm. Never inhabited; `.branch()` reads it with `infer`. */
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
    // ⚠ The name becomes a route segment. A name needing URL encoding 404s, and `.` silently serves
    // the parent chain's stage instead.
    if (!/^[A-Za-z0-9_.~-]+$/.test(name) || name === "." || name === "..") {
      throw new Error(
        `branch "${name}" is not usable in a route - use letters, digits, and any of _ . ~ -, and not "." or ".." alone`,
      );
    }
  }

  private findCatchAll(): BranchArm<T> | undefined {
    return this._arms.find((arm) => arm.isCatchAll);
  }
}

/**
 * ⚠ Runs in this process, never dispatched: a dispatched predicate costs every item two trips and
 * cannot read the caller's local state.
 */
function classifyItems<T>(
  syncChunks: MaybeAsyncChunks<T> | null,
  chunks: () => AsyncIterable<T[]>,
  arms: readonly BranchArm<T>[],
  broadcast: boolean,
): Map<string, T[]> | Promise<Map<string, T[]>> {
  // One bucket per arm, by position, so routing an item needs no lookup by name.
  const buckets: T[][] = arms.map(() => []);
  const grouped = new Map<string, T[]>(arms.map((arm, index) => [arm.name, buckets[index]!]));
  const claimChunk = (chunk: T[]): void => {
    for (let i = 0; i < chunk.length; i++) claimItem(chunk[i], arms, buckets, broadcast);
  };
  if (syncChunks === null) return classifyAsyncChunks(grouped, chunks, claimChunk);
  return chain(drainSyncChunks(syncChunks, claimChunk), () => grouped);
}

async function classifyAsyncChunks<T>(
  grouped: Map<string, T[]>,
  chunks: () => AsyncIterable<T[]>,
  claimChunk: (chunk: T[]) => void,
): Promise<Map<string, T[]>> {
  for await (const chunk of chunks()) claimChunk(chunk);
  return grouped;
}

function claimItem<T>(
  item: T,
  arms: readonly BranchArm<T>[],
  buckets: T[][],
  broadcast: boolean,
): void {
  for (let a = 0; a < arms.length; a++) {
    if (!arms[a]!.predicate(item)) continue;
    buckets[a]!.push(item);
    if (!broadcast) return;
  }
}

/**
 * What `.branch()` returns: the arms bound once, callable with any input. All-synchronous arms
 * return the record plainly; one async arm makes the whole record a `Promise`.
 *
 * `split([1, 2, 3, 4])` with arms `big` (x > 2, mapped to `BIG:x`), `odd` and `.otherwise("rest")`
 * → `{ big: ["BIG:3", "BIG:4"], odd: [1], rest: [2] }`.
 */
export interface BranchRunner<In, R, M extends PipelineMode> {
  (input: AsyncIterable<In>): Promise<R>;
  // ⚠ Keyed on `"async"`, not `"sync"`: testing `"sync"` types an `"unset"` chain's record a
  // `Promise` while the runtime returns it plainly.
  (input: Iterable<In>): M extends "async" ? Promise<R> : R;
}

/** What `.branch()` produces at runtime: one record of arrays, keyed by arm name. `BranchRunner`
 * carries the precise type.
 *
 * `{ big: ["BIG:3", "BIG:4"], rest: [2] }`. */
export type BranchResults = Record<string, unknown[]>;

interface ArmDispatch<T> {
  arms: readonly BranchArm<T>[];
  branchIndex: number;
  makeArm: (context: IContextManager, routeTrail: string) => ArmPipeline<T>;
}

/**
 * One `.branch()` call's runner: runs the parent chain over an input, routes each item to its arm,
 * runs each arm, and joins the results into one record in this process.
 *
 * `runBranch({ owner, arms: [big, rest], broadcast: false, branchIndex: 0, makeArm })([1, 2, 3, 4])`
 * → `{ big: [3, 4], rest: [1, 2] }` for routing-only arms.
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

    // One bind for the whole branch: the arms share the context this run created.
    const { syncChunks, chunks: chunksOf, context } = owner.drainable(input);
    const grouped = classifyItems(syncChunks, chunksOf, arms, broadcast);

    // `chain`, so a synchronous parent stays synchronous.
    return chain(grouped, (g: Map<string, T[]>) => joinArms(g, config, context)) as
      BranchResults | Promise<BranchResults>;
  };
}

function joinArms<T>(
  grouped: Map<string, T[]>,
  dispatch: ArmDispatch<T>,
  context: IContextManager,
): BranchResults | Promise<BranchResults> {
  const { arms, branchIndex, makeArm } = dispatch;
  // ⚠ `mapSettle`, not `arms.map`: an arm throwing synchronously after an earlier arm returned a
  // pending promise leaves that promise unhandled, and its rejection kills the process.
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
