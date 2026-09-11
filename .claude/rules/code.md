# Code - @outputty/pipeline

Each line is one rule: the moment, then the action. Rules that hold in any repo live in
`~/.claude/rules/`; this file is for what names this codebase's own seams and probes.

## Reuse, before writing

- Before writing a new import-boundary or structural prose rule, check whether an `.oxlintrc.json`
  override already covers it or could. (2026-09-11)
  - `.oxlintrc.json`'s own `overrides` array is a `files`/`excludeFiles`-scoped rule, checked on every
    `bunx oxlint src/` run - a `CLAUDE.md`/`architecture.md` sentence describing the same boundary is
    checked only when a reader happens to compare a new import against it by hand (#117).

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
