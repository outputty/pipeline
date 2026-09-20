---
"@outputty/pipeline": patch
---

A reduce that received no data emits its seed once, where it used to emit nothing. A count over no
matching row is `0`, as `[].reduce(fn, seed)` returns it.

```ts
// before
new Pipeline<number>()
  .reduce(
    (acc, x) => acc + x,
    0,
  )([])
  .toArray(); // []

// after
new Pipeline<number>()
  .reduce(
    (acc, x) => acc + x,
    0,
  )([])
  .toArray(); // [0]
```

The rule holds for an empty input and for a stream a `filter` emptied, on every class. A partitioned
reduce (`ConcurrentPipeline`, `HttpPipeline`, `WebSocketPipeline`, both cluster classes) emits the seed
once for the stage; a partition that receives no chunk while its siblings fold stays silent, so three
chunks at `maxConcurrency: 4` still return three values. `Transformer.reduce` emits its seed for every
chunk that arrives empty, one a `filter` emptied included: `.buffer(1)` over `[1, 2, 3]` with
`.filter((x) => x > 1).reduce(sum, 0)` now returns `[0, 2, 3]`, where it returned `[2, 3]`. A reducer
that banks with `emit()` also gets its seed over no data. `.buffer(fn)` is unchanged.
