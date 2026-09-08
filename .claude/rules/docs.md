# Docs - @outputty/pipeline

Each line is one rule: the moment, then the action. Rules that hold in any repo live in
`~/.claude/rules/`; this file is for what names this codebase's own docs and question rounds.

## Asking

- An option's `preview` holds the literal code its label names, never a near neighbour of it.
  (2026-09-08)
  - A label promising `.exhaustive()` over a preview showing `.otherwise()` is two different
    mechanisms with two different guarantees; the reader picks on the label and gets the preview.
