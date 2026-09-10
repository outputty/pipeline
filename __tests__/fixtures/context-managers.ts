/**
 * #31 - two `IContextManager` test doubles a caller might reasonably bring their own class for,
 * shared by `pipeline.e2e.test.ts` rather than declared twice.
 *
 * Both extend `SimpleContextManager` (#133) rather than hand-rolling `get`/`set`/`getOrDefault`/
 * `toDict` - the "obviously incorrect" finding settled in planning: all 4 hand-rolled doubles in
 * this repo (this file's own two, plus `cluster-context-factory.ts`/
 * `cluster-context-factory-invocations.ts`) reimplemented `getOrDefault` with the PRE-#113
 * `value !== undefined` check, where `SimpleContextManager`'s own `Object.hasOwn` fix already
 * distinguishes a stored `undefined` from an absent key. Extending it means each double overrides
 * only its own real difference and inherits the current, correct behavior for everything else.
 */
import { SimpleContextManager } from "@src/context/simple";

/** Records every key written, in order - proves a manager handed to `Pipeline`/`Pipeline.merge`
 * survives as the SAME instance and keeps receiving writes, rather than being copied into a fresh
 * `SimpleContextManager` and discarded. */
export class LoggingContext extends SimpleContextManager {
  readonly writes: string[] = [];

  override set(key: string, value: unknown): void {
    this.writes.push(key);
    super.set(key, value);
  }
}

/** Throws on `.set()` for a key outside its initial shape - proves a manager that REFUSES a write
 * propagates the error instead of being silently bypassed by a copy-into-`SimpleContextManager`
 * step that never calls the caller's own `.set()` at all. */
export class SealedContext extends SimpleContextManager {
  private readonly allowedKeys: ReadonlySet<string>;

  constructor(initial: Record<string, unknown>) {
    super(initial);
    this.allowedKeys = new Set(Object.keys(initial));
  }

  override set(key: string, value: unknown): void {
    if (!this.allowedKeys.has(key)) {
      throw new Error(`SealedContext: unknown key '${key}'`);
    }
    super.set(key, value);
  }
}
