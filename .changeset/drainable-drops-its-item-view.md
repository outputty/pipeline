---
"@outputty/pipeline": minor
---

BREAKING: `Drainable<T>` no longer carries `items`, so `pipeline.drainable(input).items()` is gone.
`Pipeline.drainable(input)` is public, so a consumer calling it directly gets `items is not a
function` rather than a type error. Read `chunks()` instead and flatten it if items are what you
want - `for await (const chunk of d.chunks()) for (const item of chunk) …` produces exactly what
`items()` produced, in the same order.

Nothing inside the package reads the item view any more. Every async terminal on `PipelineResult` -
`toArray()`, `first(n)`, `forEach(fn)`, `consume()` and `[Symbol.asyncIterator]` - now walks
`chunks()` with a synchronous inner loop, and `.branch()` collects through the same view. Flattening
first cost one `await`, and therefore one microtask, per ROW to re-derive items the chunk view
already held: measured on `ConcurrentPipeline` at N=10,000 over an async generator with
`.buffer(1000)`, output asserted identical, `.toArray()` fell from 12.009 promises per row to 7.006
and `.forEach()` from 14.009 to 7.008.

7.006 is the floor rather than a target: an async generator source costs 4.000 promises per row
before any package code runs, and `.buffer(size)` a further 3.001.
