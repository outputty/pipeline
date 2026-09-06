# Code - @outputty/pipeline

Each line is one rule: the moment, then the action. Rules that hold in any repo live in
`~/.claude/rules/`; this file is for what names this codebase's own seams and probes.

## Prove it

- Start feeding a streaming or duplex probe's input BEFORE awaiting the call that consumes it.
  (2026-09-06)
  - Awaiting first leaves the body stream unfed, and the resulting timeout mimics the negative
    result under test: `RESPONSE HEADERS AT +90011ms status 408` read as "the response is withheld
    until the request body is sent", where the reordered probe returned the opposite.
