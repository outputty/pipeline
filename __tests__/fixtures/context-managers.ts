/**
 * #31 - two `IContextManager` test doubles a caller might reasonably bring their own class for.
 * Used by `pipeline.e2e.test.ts`; `merge.e2e.test.ts` imports the same file once its own L2 layer
 * needs a manager-identity check, rather than declaring a second copy.
 */
import type { IContextManager } from "@src/types";

/** Records every key written, in order - proves a manager handed to `Pipeline`/`Pipeline.merge`
 * survives as the SAME instance and keeps receiving writes, rather than being copied into a fresh
 * `SimpleContextManager` and discarded. */
export class LoggingContext implements IContextManager {
  private data: Record<string, unknown> = {};
  readonly writes: string[] = [];

  get(key: string): unknown {
    return this.data[key];
  }

  set(key: string, value: unknown): void {
    this.writes.push(key);
    this.data[key] = value;
  }

  getOrDefault<T>(key: string, defaultValue: T): T {
    const value = this.data[key];
    return value !== undefined ? (value as T) : defaultValue;
  }

  toDict(): Record<string, unknown> {
    return { ...this.data };
  }
}

/** Throws on `.set()` for a key outside its initial shape - proves a manager that REFUSES a write
 * propagates the error instead of being silently bypassed by a copy-into-`SimpleContextManager`
 * step that never calls the caller's own `.set()` at all. */
export class SealedContext implements IContextManager {
  private data: Record<string, unknown>;

  constructor(initial: Record<string, unknown>) {
    this.data = { ...initial };
  }

  get(key: string): unknown {
    return this.data[key];
  }

  set(key: string, value: unknown): void {
    if (!(key in this.data)) {
      throw new Error(`SealedContext: unknown key '${key}'`);
    }
    this.data[key] = value;
  }

  getOrDefault<T>(key: string, defaultValue: T): T {
    const value = this.data[key];
    return value !== undefined ? (value as T) : defaultValue;
  }

  toDict(): Record<string, unknown> {
    return { ...this.data };
  }
}
