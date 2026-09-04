/**
 * Executor registry — maps a named executor spec (`"sequential"`/`"concurrent"`/a custom
 * registered name) to the `ExecutionStrategy` it builds.
 *
 * Module-scope state, not a singleton class: no other `@outputty/*` package wraps a plain
 * name→factory map in a class exposing only a static accessor to one shared instance, and the
 * pattern bought nothing here either — `registerExecutor`/`createStrategy` are the whole surface a
 * caller needs, mirroring how an engine's own capability factories (`writeFactories`,
 * `@outputty/laygo`) are a plain object rather than a class instance.
 */

import type { ExecutionStrategy, ExecutorOptions, ExecutorFactory, ExecutorSpec } from "@src/types";
import { SequentialStrategy } from "./sequential";
import { ConcurrentStrategy } from "./concurrent";

/** A registered factory function: builds an `ExecutionStrategy` from optional creation options.
 * `factories.concurrent({ maxConcurrency: 8 })` → a fresh `ConcurrentStrategy`. */
type StrategyFactory<In = unknown, Out = unknown> = (
  options?: ExecutorOptions,
) => ExecutionStrategy<In, Out>;

const factories: Record<string, StrategyFactory> = {
  sequential: () => new SequentialStrategy(),
  concurrent: (options) =>
    new ConcurrentStrategy({
      maxConcurrency: options?.maxConcurrency ?? 4,
      ordered: options?.ordered ?? true,
    }),
};

/**
 * Register a custom executor factory, keyed by name, into the module's plain `Record` (never a
 * `Map` — no ordering/iteration-protocol need here, so the simpler literal wins). Makes the name
 * usable later as `transformer.withExecutor(name)`.
 *
 * `registerExecutor("custom", { create: () => new SequentialStrategy() })` then
 * `createStrategy("custom")` → that `SequentialStrategy` instance.
 */
export function registerExecutor(name: string, factory: ExecutorFactory): void {
  factories[name] = (options) => factory.create(options);
}

/**
 * Build an `ExecutionStrategy` from a spec: a registered name (built-in or via `registerExecutor`),
 * or `{ custom }` for a one-off strategy instance handed straight through.
 *
 * `createStrategy("sequential")` → a new `SequentialStrategy` instance.
 * `createStrategy("nope")` → throws `"Unknown executor type: nope. Available: sequential, concurrent"`.
 */
export function createStrategy<In, Out>(
  spec: ExecutorSpec<In, Out>,
  options?: ExecutorOptions,
): ExecutionStrategy<In, Out> {
  if (typeof spec === "object" && "custom" in spec) {
    return spec.custom;
  }

  const factory = factories[spec];
  if (!factory) {
    throw new Error(
      `Unknown executor type: ${spec}. Available: ${Object.keys(factories).join(", ")}`,
    );
  }

  return factory(options) as ExecutionStrategy<In, Out>;
}
