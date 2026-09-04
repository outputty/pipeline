# @outputty/pipeline - Roadmap

Why each open ticket is worth building, and now. Status lives on GitHub Issues, not here: every ticket
is a GitHub issue, labelled `ready` (buildable) or `needs-planning` (grill it with `/plan` first), and
`gh` derives what is unblocked. This file is the durable index and the killed-idea dedup surface.

**Read the whole file** before evaluating an idea or closing work. A new idea is often a row that
already exists (Building / Later), or one already tried (Killed) - point the new one at that row.

## Building - open tickets, detail in each issue

- **Cross-runtime benchmarks** (#11, blocked by #5) - the package ships no numbers, so nothing
  compares it against `ix`, `streaming-iterables`, `effect`, `rxjs` or the runtime's own stream
  helpers, and a hot-path change has no baseline to regress against. Now, because #5 rewrites the
  per-chunk hot path and any number taken before it lands is invalidated by it. Six pinned runtimes
  in Docker, two tables, results committed as JSON.

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
- **Core chunked-transform engine** - `Pipeline`, `Transformer`, chunking, `SequentialStrategy` /
  `ConcurrentStrategy`, `SimpleContextManager`, `.catch()` chunk-level error handling, `.branch()` /
  `Pipeline.merge()`, lifecycle hooks. Migrated from
  [laygo-python](https://github.com/ringoldsdev/laygo-python), async-first, before this repo's own
  tracker existed - no ticket number.

## Killed

None yet.
