---
"@outputty/pipeline": major
---

BREAKING: `new Pipeline({ chunkSize })` now refuses a value that is not a whole number of at least 1,
throwing `chunkSize must be a whole number of at least 1`. `.buffer(size)` has refused the same
values since #88; the constructor took `chunkSize` raw, so the one knob had two ways in and only one
of them checked.

An unchecked fractional size made the engine disagree with itself rather than merely cut oddly. At
`chunkSize: 2.5` over `[1, 2, 3, 4, 5, 6, 7]`, an array source cut into `[[1,2],[3,4,5],[6,7]]` while
a `Set` over the identical values cut into `[[1,2,3],[4,5,6],[7]]` - a `slice`-based cut truncates
both of its bounds, where an accumulating cut breaks at `length >= size`.

`chunkSize` belongs to `PipelineState`, the carried state a copy-on-write call threads through, and
`.buffer(size)` is how a caller is meant to set it. The constructor's declared parameter is the
intersection of that with `PipelineOptions`, so passing it directly typechecks and therefore reaches
real callers - which is why it is validated rather than trusted.

A caller passing a whole number, or passing no `chunkSize` at all, is unaffected.
