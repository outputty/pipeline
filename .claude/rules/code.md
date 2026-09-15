# Code - @outputty/pipeline

Each line is one rule: the moment, then the action. Rules that hold in any repo live in
`~/.claude/rules/`; this file is for what names this codebase's own seams and probes.

## Reuse, before writing

- Before writing a new import-boundary or structural prose rule, check whether an `.oxlintrc.json`
  override already covers it or could. (2026-09-11)
  - `.oxlintrc.json`'s own `overrides` array is a `files`/`excludeFiles`-scoped rule, checked on every
    `bunx oxlint src/` run - a `CLAUDE.md`/`architecture.md` sentence describing the same boundary is
    checked only when a reader happens to compare a new import against it by hand (#117).

## Shape

- Queue an incoming message on a multiplexed connection PER CORRELATION-ID, never dispatch every
  message concurrently. (2026-09-14)
  - Two messages sharing one `id` (a reduce stream's own chunk frame and its `inputDone` frame,
    `src/pipelines/websocket.ts`) can settle out of the order they were SENT once each takes a
    different async path to its own effect - `inputDone`'s path to `Reducer.final()` was shorter
    than a chunk's own path to `foldChunk()`, so the trailing flush ran before the chunk it was
    meant to flush had folded, and `[1,2,3,4,5]` summed to `[]` instead of `[15]`. A per-id
    `Map<id, Promise<void>>` tail-chain (`frameQueues`) fixed it: same-id messages await the prior
    one's own handling before their own runs; different ids still dispatch concurrently.

## Prove it

- Start feeding a streaming or duplex probe's input BEFORE awaiting the call that consumes it.
  (2026-09-06)
  - Awaiting first leaves the body stream unfed, and the resulting timeout mimics the negative
    result under test: `RESPONSE HEADERS AT +90011ms status 408` read as "the response is withheld
    until the request body is sent", where the reordered probe returned the opposite.

## Names and pointers

- Before moving a call a ticket says to move rather than duplicate, grep every path that reached
  its OLD location - the ticket's own worked example only proves the one path it exercises.
  (2026-09-08)
- Before writing that a subclass "does not override" a method, re-read its literal declaration - a
  return-type-narrowing override that delegates unchanged to `super` is still an override. (2026-09-08)
