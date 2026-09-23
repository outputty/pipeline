/**
 * Factory functions for building a `Transformer` without the constructor's options object.
 */

import { Transformer } from "./transformer";

/**
 * Create an identity transformer to chain onto.
 *
 * @returns An identity `Transformer<T, T>`
 *
 * `createTransformer<number>().map((x) => x * 2)` over `[1, 2]` → `[2, 4]`.
 */
export function createTransformer<T>(): Transformer<T, T> {
  return new Transformer<T, T>({ transform: (chunk) => chunk });
}
