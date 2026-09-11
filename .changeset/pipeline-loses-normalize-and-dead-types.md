---
"@outputty/pipeline": major
---

**Breaking:** `normalize` is removed from `@outputty/pipeline`'s public export surface. It had zero
production callers (chunking moved onto `Pipeline.buffer()`, which never calls it) and was kept
exported only by inertia.

- A caller importing `normalize` from `@outputty/pipeline` gets a build error instead of the
  function. There is no drop-in replacement: `normalize` took a stream mixing loose items and
  pre-chunked arrays and flushed the loose ones into a chunk whenever a real array arrived or the
  stream ended - `Pipeline`'s own cutting (`.buffer(size)`) is what does this job now, on a stream
  that is never mixed to begin with.
- Every other public export is unaffected: `buildChunkGenerator` and `isContextAware` are untouched.

Migration: delete the import. A caller who genuinely needs to normalize a loose/pre-chunked mix of
its own writes the same generator directly - `normalize`'s own body took no dependency on anything
else in this package.

```diff
-import { normalize } from "@outputty/pipeline";
-const chunks = normalize(mixedSource);
+async function* normalize(stream) {
+  let buffer = [];
+  for await (const item of stream) {
+    if (!Array.isArray(item)) {
+      buffer.push(item);
+      continue;
+    }
+    if (buffer.length > 0) {
+      yield buffer;
+      buffer = [];
+    }
+    yield item;
+  }
+  if (buffer.length > 0) yield buffer;
+}
+const chunks = normalize(mixedSource);
```

Two more internal changes ship in this same release, neither requiring any action from a consumer:

- `RowErrorHandler`'s return type is respelled to bare `unknown` from an equivalent union - no
  caller-visible change.
- The unused, unexported `src/context/types.ts` module is deleted; `IContextManager` keeps its one
  real export from `src/types.ts`, unchanged.
