/**
 * The in-memory context manager.
 */

import type { IContextManager } from "@src/types";

/**
 * An in-memory context for sharing state across a pipeline run. It is not shared across processes.
 *
 * @example
 * ```typescript
 * const ctx = new SimpleContextManager({ count: 0 });
 *
 * ctx.set('count', 1);
 * ctx.get('count');           // 1
 * ctx.getOrDefault('missing', 0); // 0
 * ctx.toDict();               // { count: 1 }
 * ```
 */
export class SimpleContextManager implements IContextManager {
  // oxlint-disable-next-line anti-slop/no-unsafe-dictionary-type -- Context is a generic bag by design, unknown until a caller parses it at its own boundary (see .oxlintrc.json)
  private data: Record<string, unknown>;

  // oxlint-disable-next-line anti-slop/no-unsafe-dictionary-type -- Context is a generic bag by design, unknown until a caller parses it at its own boundary (see .oxlintrc.json)
  constructor(initial?: Record<string, unknown>) {
    this.data = initial ? { ...initial } : {};
  }

  // oxlint-disable-next-line anti-slop/no-unknown-returns -- Context is a generic bag by design, unknown until a caller parses it at its own boundary (see .oxlintrc.json)
  get(key: string): unknown {
    return this.data[key];
  }

  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- Context is a generic bag by design, unknown until a caller parses it at its own boundary (see .oxlintrc.json)
  set(key: string, value: unknown): void {
    this.data[key] = value;
  }

  /**
   * The value stored under `key`, or `defaultValue` when the key is absent.
   *
   * ⚠ Tests key presence, not `value !== undefined`, so a stored `undefined` is returned. Testing
   * the value makes this disagree with `.get()`.
   *
   * `ctx.set("k", undefined); ctx.getOrDefault("k", 0)` → `undefined`.
   */
  getOrDefault<T>(key: string, defaultValue: T): T {
    return Object.hasOwn(this.data, key) ? (this.data[key] as T) : defaultValue;
  }

  // oxlint-disable-next-line anti-slop/no-unsafe-dictionary-type -- Context is a generic bag by design, unknown until a caller parses it at its own boundary (see .oxlintrc.json)
  toDict(): Record<string, unknown> {
    // oxlint-disable-next-line anti-slop/no-known-value-widening -- Context is a generic bag by design, unknown until a caller parses it at its own boundary (see .oxlintrc.json)
    return { ...this.data };
  }
}
