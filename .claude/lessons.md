# Lessons

The mistakes made building this product, recorded so they are not repeated. `retro` appends entries at
the end of every planning session and inside every build's docs layer.

- A lesson remembers; the rule, skill or doc change it produced enforces. Every entry links that change.
- An entry is one paragraph; the incident's detail stays in the session.
- Newest first. Development context lives here and in the tracker, never in `product.md`.

## 2026-09-05 Four spikes argued for designs the user had not asked for

Planning #17, the user asked for "everything implemented as a Pipeline subclass? One for http, one for
cluster?". The reply built a standalone `HttpPipeline` holding a caller-owned stage map - not a subclass
at all - and let a fork's "a WorkerPoolPipeline class is not coherent" verdict, which was about the
worker's far side, kill the second class without ever addressing the subclass question. Corrected, the
next re-pitch proposed `.apply(stages.double)` recognised by reference, which still kept the stage map.
Both fork briefs had been written from a paraphrase rather than the user's own words, so both spikes
returned real output arguing for the wrong shape. `~/.claude/rules/code.md` now says to restate the
user's words as code before writing a spike brief.

## 2026-09-05 A ratio of two total runtimes was reported as overhead

Planning #17 compared `node:cluster` against a worker-thread pool on one CPU-bound job - sequential
77ms, piscina 24ms, cluster 33ms - and reported "roughly 30-40% more overhead" from 33 divided by 24.
Both numbers were mostly the CPU work; the dispatch cost was never isolated. Challenged with "how did
you measure 40%", a proper measurement (identity transform, work removed, paired interleaved rounds
across payload sizes) showed the ranking FLIPS at roughly 7-8 KB per chunk, so the original claim was
wrong in method and in direction. The same session then divided a per-request cost measured at one
payload size by 1000 rows to state a per-row figure, which the measurement does not support.
`~/.claude/rules/code.md`'s existing measure-before-normalising rule gained both sharpenings.

## 2026-09-05 Two counts were repeated from prose instead of being run

Planning #17 carried "94 tests" through several rounds; it came from PR #4's body and the real count is
100, confirmed by `npx vitest run`. The same session claimed `inertKnobsOf` "exists only to catch a
strategy the async-iteration path cannot honor" after reading one line of it - it checks four knobs
(`strategy`, `hooks`, `chunkSize`, `chunker`), so removing the execution seam shrinks it rather than
deleting it. `~/.claude/rules/code.md`'s "a docstring's claim is not evidence" line now covers counts
read from a PR body and scope claims made from one line of a function.

## 2026-09-05 Four rounds were spent on options the reader could not judge

Planning #17 put decisions to the user as priced options without first showing the mechanism they
rested on, and drew "I dont understand your conclusions", "why are we specifically talking about this?"
and two more. The round that landed instead wrote the causal chain first - the user asked for opacity,
so the library calls `cluster.fork()`, so Node re-runs the entry module, so a `db.migrate()` in that
file runs once per worker - and the answer came immediately. `~/.claude/rules/docs.md` now requires the
problem as currently seen, then the causal chain, then the options, each with its own end-to-end
example.

## 2026-09-04 A deferred conditional type never resolves inside its own abstract scope

`Transformer`'s constructor tried gating a mismatched `In`/`Out` with no `transform` via a single
conditional-tuple overload (`In extends Out ? [options?] : never`), verified in isolated probes against
concrete types. It failed at every internal call site still holding `In`/`Out` abstract - the class's own
methods, and even `.catch()`'s `tempTransformer` where both sides of `extends` were the literally same
parameter (`Out extends Out`). TypeScript never distributes a deferred conditional over an unresolved
type parameter. The fix, from a second `advisor` consult: a plain, non-conditional overload for "a real
transform already in hand," ahead of the conditional one, so internal generic sites resolve against the
first. `.claude/rules/typescript.md` now carries the rule.

## 2026-09-04 An array literal widens inside a wrapping generic call, not just at its own site

Ticket #5's `Pipeline.merge` Done-when example assumed `new Pipeline(["a","b"])` (no annotation) would
carry `"a"|"b"` through `Pipeline.merge(...)`'s inferred return type. A real probe
(`tmp/merge-literal-probe.ts`) showed the literal widens to `string` inside `merge`'s own arguments
regardless of the outer assignment's target type - only an explicit annotation at the `Pipeline`
construction site (`new Pipeline<"a" | "b">([...])`) preserves it. `merge`'s own `ElementOf<Ps[number]>`
inference is correct once given explicitly-typed pipelines; reported the discrepancy on the ticket rather
than silently rewriting its example. `.claude/rules/typescript.md` now carries the rule.

## 2026-09-04 A ticket's "spiked and verified" claim missed the one signature that mattered most

Ticket #5's Constraints claimed every Interface signature - the execution-strategy seam, `merge`'s
`ElementOf` inference, the hooks/loop fixes, and `Transformer`'s constructor - "was spiked against real
`src/` during planning, each spike asserting its failure cases with `@ts-expect-error`, and each
returned clean." Re-spiking the constructor's own conditional-tuple design during build (per
`~/.claude/rules/typescript.md`'s existing "extract a distributing conditional" rule) surfaced a real
gap: a conditional type gating a constructor's OWN parameter never resolves inside any generic scope
still holding its type parameters abstract, even where both sides of `extends` are the literally same
parameter (`Out extends Out` inside `.catch()`'s own `tempTransformer` construction still failed) - a
case the ticket's blanket "spiked" claim did not actually cover. The fix (a plain, non-conditional
overload for "a real transform already in hand," ahead of the conditional one) came from a second
`advisor` consult, not the ticket text. `~/.claude/rules/issues.md` now says a ticket's own
verification claim is re-run as a probe during build, never trusted at face value.

## 2026-09-04 A spike's rejected rows were the design space, not settled negatives

Planning #5, a spike enumerated five ways to author an execution strategy and asserted two of them
errors: an unconstructed class, and a bare `async function*`. Both were reported as costs of the
recommended shape, and a round closed on that shape. The next message reversed it and chose the bare
`async function*` - the row the spike had drawn and dismissed. A probe that enumerates forms has
enumerated the design space, and its rejected rows are where a preference most often sits. Produced
`~/.claude/rules/code.md`'s "Present every authoring form a probe rejected as a design option before
pricing it as a cost" (2026-09-04).

## 2026-09-04 The ticket was filed before the repo was swept

Planning #5 swept `src/`, `.claude/` and `__tests__/` for every name the seam change renames, filed
the ticket, then found five more stale `.withExecutor` call sites in `README.md` while editing the
docs afterwards. The ticket needed an edit it should never have needed. The same pass also found
`ts-pattern` declared as a runtime dependency, credited in `architecture.md` with "strategy-spec
matching", and imported nowhere in `src/`. Sharpened `~/.claude/rules/issues.md`'s existing
planning-time-file-list line to say the sweep runs BEFORE filing and covers README and consumer docs,
and produced `~/.claude/rules/code.md`'s "Grep for a dependency's import before repeating a doc's
claim about the role it plays" (2026-09-04).

## 2026-09-04 The package outgrew a callback-arity split before it outgrew a monorepo

`outputty/laygo` carried this package as `packages/pipeline` inside its own pnpm workspace from the
start, coupled by nothing but co-location - no import edge either direction (laygo's `Source` reaches
any `AsyncIterable` structurally). The coupling that actually needed fixing first was internal:
terminal ops (`toArray`/`first`/`consume`/`forEach`/`branch`) returned a `[data, context]` tuple every
caller had to destructure, which made a context read a positional-return contract rather than a normal
property read. `outputty/laygo` #743 dropped the tuple in favor of `.contextManager` read directly off
the resolved `Pipeline` instance; #744 sourced every caller and test onto the new shape. Only once that
internal shape was settled did #745 split the package out into this repository and flatten laygo itself
down to one package - splitting a coupling that was co-location-only, before the API shape underneath
it had settled, would have meant redoing the split's own boilerplate (package.json, CI, `.claude/`
docs) a second time. See `outputty/laygo`'s own `.claude/lessons.md` (2026-09-04) for the pnpm
workspace-boundary-marker mistake made during the flatten half of that same ticket.
