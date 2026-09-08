---
"@outputty/pipeline": minor
---

**Breaking:** error handling moves onto the function that failed. `Transformer.onError(fn)` is now
the ROW handler, and `Pipeline.onError(fn)` is the RUN handler. `.catch()`, `ChunkErrorHandler` and
`ErrorHandler` are deleted.

- `Transformer.onError(fn)` - `fn` receives the failing item, the `Error` and the context.
  Returning a value puts that value in the row's place, returning the exported `DROP` sentinel
  removes the row, and throwing escalates to the pipeline's own run handler. Reaches every
  element-wise call (`.map()`, `.filter()`, `.flatMap()`, `.tap(fn)`) and `Transformer.reduce()`'s
  fold step, wherever in the chain it is written - position-independent.
- `Pipeline.onError(fn)` - `fn` receives the `Error` and the context. Returning drops the failing
  chunk and the run continues; throwing stops the run. Position-DEPENDENT: only a stage applied
  after this call is covered.
- `.catch()`, `ChunkErrorHandler` and `ErrorHandler` (`src/errors/`) are deleted outright, no
  deprecation period.

Migration:

```diff
-await new Pipeline(["a", "b", "3", "d", "5"])
-  .transform((t) => t.catch((sub) => sub.map(parseStrict), () => [999]))
-  .toArray();
-// [999] - "3" and "5" parsed fine and are lost with the chunk
+await new Pipeline(["a", "b", "3", "d", "5"])
+  .transform((t) => t.onError(() => DROP).map(parseStrict))
+  .toArray();
+// [3, 5] - only the two bad rows are dropped
```
