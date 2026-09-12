---
"@outputty/pipeline": patch
---

`Transformer.filter()`'s synchronous, no-row-handler branch is now one pass over the chunk instead
of three (#120's own bench harness found the gap: build a keep-flag array, scan that whole array
for a thenable, then filter the chunk a third time reading the flags back). A synchronously-true
item is now kept the moment its own predicate call returns; only once a predicate call returns a
Promise does the rest of the chunk fall back to the settle-then-filter shape, and everything before
that point is never re-evaluated. Output is unchanged - measured on this package's own committed
`bench/overhead.ts` harness, the `Pipeline` leg's ns/row dropped from ~50 to ~30 on the same
canonical `.map().filter()` chain.
