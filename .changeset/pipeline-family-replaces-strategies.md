---
"@outputty/pipeline": minor
---

**Breaking:** where a chain's chunks run is now a class you construct, not an
`ExecutionStrategy` you configure on a `Transformer`. `ExecutionStrategy`, `.withExecutor()`,
`sequential`, `concurrent(options?)` and `ConcurrentStrategyOptions` are removed entirely.

- `ConcurrentPipeline` replaces `.withExecutor(concurrent(options))` - runs up to `maxConcurrency`
  chunks of a stage at once, in this process.
- `HttpPipeline` dispatches a stage's chunk to another instance over HTTP, given its url.
- `ClusterPipeline` dispatches to worker processes on the same machine, brought up automatically -
  no server, listen, fork or url in caller code.
- `{ local: true }` on `.transform()`/`.apply()` keeps one stage in the orchestrating process on
  any of the three.
- A plain `Pipeline` (the default, one chunk at a time) is unchanged - `Transformer.execute()`
  itself also runs sequentially now, since the pluggable strategy it dispatched through is gone.

Migration:

```diff
-import { Transformer, concurrent } from "@outputty/pipeline";
+import { ConcurrentPipeline } from "@outputty/pipeline";

-const data = await new Pipeline(rows)
-  .transform((t) => t.withExecutor(concurrent({ maxConcurrency: 8 })).map((x) => x * 2))
+const data = await new ConcurrentPipeline(rows, { maxConcurrency: 8 })
+  .transform((t) => t.map((x) => x * 2))
   .toArray();
```

Also fixes two consumer-facing defects the deleted strategy seam had hidden: `.map()`/`.filter()`
now await an async callback's result instead of casting a `Promise` straight into the output array
(`new Pipeline([1,2,3]).transform((t) => t.map(async (x) => x*2).filter((x) => x>2)).toArray()`
used to print `[]`, now `[4,6]`), and `Pipeline.buffer()` no longer silently drops the pipeline's
own chunk-transform history and source-position violations on copy-on-write.
