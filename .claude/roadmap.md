# @outputty/pipeline - Roadmap

Why each open ticket is worth building, and now. Status lives on GitHub Issues, not here: every ticket
is a GitHub issue, labelled `ready` (buildable) or `needs-planning` (grill it with `/plan` first), and
`gh` derives what is unblocked. This file is the durable index and the killed-idea dedup surface.

**Read the whole file** before evaluating an idea or closing work. A new idea is often a row that
already exists (Building / Later), or one already tried (Killed) - point the new one at that row.

## Building - open tickets, detail in each issue

- **The execution-strategy seam, and a typechecked test suite** (#5) - `ExecutionStrategy` becomes a
  function type, so a caller's strategy is an `async function*` with nothing to implement and nothing
  to register. Now, because `tsconfig.json` excludes `__tests__`: `pnpm check` never typechecks the
  suite, and it hides 12 errors from 5 causes. Two of those are the seam itself, and the other three
  are consumer-facing defects nobody could see - hooks returning `void | Promise<void>`, `loop`'s
  union-of-arities condition, and `new Transformer<In, Out>()` claiming a conversion it never makes.
  The gate ships in the same ticket, so the suite cannot drift again.

The next two candidates, not yet filed:

- An executable docs harness mirroring `outputty/laygo`'s `docs-examples.test.ts`: every `<!-- compiles
  -->`/`<!-- illustrative -->` fence in `product.md`/`architecture.md`/`README.md` is hand-verified
  today, not machine-checked. Until it exists, a docs pass is checked against the real test suite by
  hand, per `.claude/rules/docs.md`'s standing rule.
- A `Pipeline`-against-real-`Layer` integration proof, once both packages publish to npm
  (`outputty/laygo` #746) and a consumer can actually install both.

## Built

- **Split from `outputty/laygo`** (`outputty/laygo` #743, #744, #745) - `@outputty/pipeline` moves from
  `packages/pipeline` inside the laygo monorepo to its own repository. #743 dropped the terminal ops'
  context-tuple return in favor of reading `.contextManager` directly off the `Pipeline` instance after
  a terminal op resolves; #744 swept every caller and test onto the new shape; #745 deleted the package
  from `outputty/laygo` and flattened laygo itself to a single-package repo.
- **Core chunked-transform engine** - `Pipeline`, `Transformer`, chunking, the sequential and
  concurrent execution strategies, `SimpleContextManager`, `.catch()` chunk-level error handling, `.branch()` /
  `Pipeline.merge()`, lifecycle hooks. Migrated from
  [laygo-python](https://github.com/ringoldsdev/laygo-python), async-first, before this repo's own
  tracker existed - no ticket number.

## Killed

- **An open `ExecutorType`, by any mechanism** (#5) - three ways to let a registered executor name
  typecheck were priced: a declaration-merged `ExecutorRegistry` interface, the `(string & {})`
  widening, and leaving the cast in place. All three died with the named registry itself. Passing the
  strategy function directly removes the name, so there is nothing left to open. The `(string & {})`
  form was independently disqualified: a spike proved a typo such as `"btched"` compiles under it and
  fails only at runtime.
- **`registerExecutor` and a named executor registry** (#5) - a second way to inject a strategy that
  passing the function already covers, backed by process-wide mutable state with no per-test reset.
- **`appliesInSourcePosition` on `ExecutionStrategy`** (#5) - a flag with exactly one true
  implementation, which made every other strategy declare a line whose only correct value was `false`.
  `inertKnobsOf` asks the `Transformer` whether `.withExecutor()` was called instead.
- **`createConcurrentTransformer`** (#5) - `createTransformer(chunkSize).withExecutor(concurrent(...))`
  says the same thing, and the helper was the last duplicate of the `maxConcurrency: 4 / ordered: true`
  defaults that `concurrent()` owns.
