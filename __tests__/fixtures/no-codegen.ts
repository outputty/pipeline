/**
 * #90 - the package must construct where code generation from strings is banned: a CSP page, a
 * Cloudflare Worker, or this fixture's own `node --disallow-code-generation-from-strings`.
 *
 * Run as a child process by `__tests__/callable.e2e.test.ts`, because the ban is a process-level
 * flag. `class Pipeline extends Function` failed here with `EvalError: Code generation from strings
 * disallowed for this context`, thrown by `super()` on the first `new Pipeline()`.
 */
import { Pipeline } from "../../src";

const doubled = new Pipeline<number>().transform((t) => t.map((x) => x * 2));

console.log(
  JSON.stringify({
    values: doubled([1, 2, 3]).toArray(),
    isFunction: doubled instanceof Function,
    hasBind: typeof doubled.bind === "function",
  }),
);
