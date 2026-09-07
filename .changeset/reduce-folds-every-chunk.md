---
"@outputty/pipeline": minor
---

**Breaking:** `Pipeline.reduce(fn, initial)` folds every chunk the pipeline produces, not one.
`ReduceOptions`, `PipelineReduceFunction` and `Transformer.reduce`'s old per-chunk-toggle overload
are removed; `ReduceFunction<U, Out> = (acc, item, ctx, emit) => U | Promise<U>` is the one
signature, `emit` fourth so `ctx` keeps arity 3.

- `Transformer.reduce(fn, initial)` still folds the ONE chunk it receives and keeps no state
  between chunks.
- `Pipeline.reduce(fn, initial)` folds EVERY chunk the pipeline produces, in-process and
  sequential - the only place cross-chunk state lives. `ConcurrentPipeline.reduce(fn, initial)` is
  the one override that always dispatches it; wrap it in `.local(build)` to keep it in-process.
- `emit(value)` pushes one value downstream mid-fold and resets the accumulator; the trailing
  accumulator is only emitted if items were folded since the last `emit()`.
- A reduce stage dispatches like any other stage, over one duplex POST to `/reduce/<n>` whose
  accumulator lives for the connection's life, so `maxConcurrency` is inert on it.

Migration:

```diff
-import { Transformer } from "@outputty/pipeline";
+import { Pipeline } from "@outputty/pipeline";

-const total = await new Transformer<number, number>()
-  .reduce((acc, x, { perChunk: false }) => acc + x, 0)
-  .process(chunks);
+const total = await new Pipeline([1, 2, 3, 4, 5])
+  .reduce((acc: number, x: number) => acc + x, 0)
+  .toArray();
```
