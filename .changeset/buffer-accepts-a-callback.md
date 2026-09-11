---
"@outputty/pipeline": minor
---

`Pipeline.buffer()` accepts a callback in place of a size, deciding the chunk boundary per item
instead of by count.

```ts
let windowStart = 0;
const fiveMinuteWindow = (item, ctx, emit) => {
  if (item.ts - windowStart >= 300_000) {
    emit();
    windowStart = item.ts;
  }
  return item;
};

const data = await new Pipeline(events).buffer(fiveMinuteWindow).toArray();
```

`emit()` takes no value: it flushes whatever is currently pending and resets it to `[]`. Returning a
value appends it to the (possibly just-reset) pending array; returning the exported `DROP` sentinel
skips the item entirely. A `Promise`-returning callback widens the pipeline's Mode to `"async"`,
matching `.reduce()`'s own two-overload split. `.buffer(size)`'s own behavior is unchanged.

**Breaking:** `ChunkerFunction` is no longer exported - dead since #39 (`Transformer.setChunker()`
was removed then), zero consumers. `BufferFunction` is the new exported type, for typing a
`.buffer(fn)` callback.

Migration: a caller importing `ChunkerFunction` has no replacement to import - the type described a
mechanism `.buffer()` has not used since #39, and nothing in this package's own source referenced it
either.
