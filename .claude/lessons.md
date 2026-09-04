# Lessons

The mistakes made building this product, recorded so they are not repeated. `retro` appends entries at
the end of every planning session and inside every build's docs layer.

- A lesson remembers; the rule, skill or doc change it produced enforces. Every entry links that change.
- An entry is one paragraph; the incident's detail stays in the session.
- Newest first. Development context lives here and in the tracker, never in `product.md`.

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
