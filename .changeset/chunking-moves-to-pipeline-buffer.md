---
"@outputty/pipeline": minor
---

**Breaking:** chunking is now the `Pipeline`'s own explicit decision, never the `Transformer`'s.
`Transformer.chunkSize`, `.setChunker()`, `.execute()` and `createTransformer(chunkSize)`'s argument
are removed; `ConcurrentPipelineOptions.chunkSize` is removed with them.

- `Pipeline.buffer(size)` replaces every one of those - the ONE place a cut happens, carried
  unchanged through every later stage until called again.
- `Transformer.process(chunks, context?)` replaces `.execute(items, context?)` - it now takes an
  already-cut `AsyncIterable<In[]>` and yields `Out[]`, chunk in, chunk out, deciding nothing about
  chunk size itself.
- `createTransformer()` takes no argument - a `Transformer` never carries a chunk size to begin with.

Migration:

```diff
-import { Transformer, createTransformer } from "@outputty/pipeline";
+import { Pipeline, createTransformer } from "@outputty/pipeline";

-const t = createTransformer<number>(2);
-for await (const chunk of t.execute(items)) { ... }
+const t = createTransformer<number>();
+const out = await new Pipeline(items).buffer(2).apply(t).toArray();
```
