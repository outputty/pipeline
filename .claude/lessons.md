# Lessons

The mistakes made building this product, recorded so they are not repeated. `retro` appends entries at
the end of every planning session and inside every build's docs layer.

- A lesson remembers; the rule, skill or doc change it produced enforces. Every entry links that change.
- An entry is one paragraph; the incident's detail stays in the session.
- Newest first. Development context lives here and in the tracker, never in `product.md`.

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
