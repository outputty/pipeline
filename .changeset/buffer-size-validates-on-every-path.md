---
"@outputty/pipeline": major
---

BREAKING: `.buffer(size)` on an ALREADY-BOUND pipeline now refuses a size that is not a whole number
of at least 1, and reports it under its own name. A chain is already bound inside `.local(build)` and
inside a `.branch()` arm, so `p.buffer(n)` there took a different validation path from the ordinary
`new Pipeline<number>().buffer(n)`, and the two disagreed.

Measured on `new Pipeline<number>().local((p) => p.buffer(n))([1, 2, 3, 4, 5, 6, 7])`:

```text
                 before                             after
.buffer(2.5)     no throw, cut at length >= 2.5     buffer size must be a whole number of at least 1
.buffer(0)       chunkSize must be at least 1       buffer size must be a whole number of at least 1
.buffer(3)       no throw                           no throw
```

The bound path validated through `sizeReduceFunction`, the numeric adapter onto the `Reducer<T[], T>`
fold engine, which checked only `size < 1` and named the internal knob. `.buffer(size)` cuts by count
now rather than folding, so that adapter is deleted and both paths share the guard the deferred one
has used since #88.

A caller passing a whole number is unaffected. A caller relying on `.buffer(2.5)` inside a `.local()`
region was getting a chunking that no other source shape agreed with.
