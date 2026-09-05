---
"@outputty/pipeline": patch
---

Fixes `Transformer.catch()`'s `onError` handler silently discarding its returned replacement array
and always dropping the failing chunk instead — the exported `ChunkErrorHandler<In, U>` type
promised a replacement array was honored, but the shipped `ErrorHandler.handle()` returned nothing.
It now returns the first registered handler's non-`undefined` result (handlers run LIFO, so the
last-registered one runs first and is the natural winner), and `.catch()` substitutes that array for
the chunk, falling back to `[]` only when no handler replaced it.

```diff
 const out = await new Pipeline(["a", "b", "3", "d", "5"])
   .transform((t) =>
     t.catch(
       (sub) => sub.map((s) => parseInt(s)),
       () => [999],
     ),
   )
   .toArray();
-// []  (the handler's [999] was silently discarded)
+// [999]
```

Also tightens `Transformer.onError()`'s function-arm type to a bare `void` return, restoring an
ordinary `(chunk, err) => arr.push(err)` notification hook (which briefly stopped typechecking under
this fix's own `ChunkErrorHandler<In>` default) as a compile-time pass, not just a runtime one.
