/**
 * Simple context manager implementation.
 *
 * Python equivalent:
 * ```python
 * class SimpleContextManager:
 *   def __init__(self, initial: dict[str, Any] | None = None):
 *     self._data = initial.copy() if initial else {}
 *
 *   def __getitem__(self, key: str) -> Any:
 *     return self._data[key]
 *
 *   def __setitem__(self, key: str, value: Any) -> None:
 *     self._data[key] = value
 *
 *   def get(self, key: str, default: Any = None) -> Any:
 *     return self._data.get(key, default)
 *
 *   def to_dict(self) -> dict[str, Any]:
 *     return self._data.copy()
 * ```
 */

import type { IContextManager } from "@src/types";

/**
 * Simple in-memory context manager for sharing state across pipeline operations.
 *
 * Thread-safe in JavaScript (single-threaded event loop), but not process-safe.
 * For multi-process scenarios, use a more sophisticated implementation.
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

  /**
   * Create a new context manager.
   *
   * @param initial - Optional initial data to populate the context
   */
  // oxlint-disable-next-line anti-slop/no-unsafe-dictionary-type -- Context is a generic bag by design, unknown until a caller parses it at its own boundary (see .oxlintrc.json)
  constructor(initial?: Record<string, unknown>) {
    this.data = initial ? { ...initial } : {};
  }

  /**
   * Get a value by key.
   *
   * @param key - The key to look up
   * @returns The value, or undefined if not present
   */
  // oxlint-disable-next-line anti-slop/no-unknown-returns -- Context is a generic bag by design, unknown until a caller parses it at its own boundary (see .oxlintrc.json)
  get(key: string): unknown {
    return this.data[key];
  }

  /**
   * Set a value by key.
   *
   * @param key - The key to set
   * @param value - The value to store
   */
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- Context is a generic bag by design, unknown until a caller parses it at its own boundary (see .oxlintrc.json)
  set(key: string, value: unknown): void {
    this.data[key] = value;
  }

  /**
   * Get a value by key with a default fallback.
   *
   * Tests key PRESENCE, not `value !== undefined` (#113): a deliberately stored `undefined` is a
   * real value here, the same distinction `DROP` exists for on the row-handler side, where
   * `types.ts` records that "`undefined` stays an ordinary value a handler may legitimately
   * return". Comparing the value instead made `.set("k", undefined)` read back as the default while
   * `.get("k")` returned `undefined` - two accessors disagreeing about whether the key exists.
   *
   * @param key - The key to look up
   * @param defaultValue - Value to return if the key is absent
   * @returns The stored value if the key is present, otherwise the default
   */
  getOrDefault<T>(key: string, defaultValue: T): T {
    return Object.hasOwn(this.data, key) ? (this.data[key] as T) : defaultValue;
  }

  /**
   * Convert context to a plain object (snapshot).
   *
   * @returns A shallow copy of the internal data
   */
  // oxlint-disable-next-line anti-slop/no-unsafe-dictionary-type -- Context is a generic bag by design, unknown until a caller parses it at its own boundary (see .oxlintrc.json)
  toDict(): Record<string, unknown> {
    // oxlint-disable-next-line anti-slop/no-known-value-widening -- Context is a generic bag by design, unknown until a caller parses it at its own boundary (see .oxlintrc.json)
    return { ...this.data };
  }
}
