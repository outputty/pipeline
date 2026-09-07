---
"@outputty/pipeline": minor
---

**Breaking:** `.local(build)` runs a whole region of the chain in the orchestrating process,
replacing the per-stage `{ local: true }` flag. `StageOptions` and its `options?` argument - the
second on `.apply()`/`.transform()`, the third on `.reduce()` - are removed entirely, on every
dispatching class.

- `.local(build)` builds a bare `Pipeline` over the caller's own chunk stream, runs `build` against
  it - nothing inside can dispatch - and resumes the caller's own class afterward. One
  implementation on the base `Pipeline`; every dispatching subclass narrows only its return type.
- Several consecutive stages that must stay in the orchestrating process are written once, inside
  one `.local(build)` call, instead of repeating the old flag on every one of them.
- The old flag lived only on the dispatching subclasses, so a chain using it never typechecked on a
  base `Pipeline`. `.local(build)` is declared on `Pipeline` itself, so the same call compiles on
  every class.

Migration:

```diff
-new ConcurrentPipeline(rows, { maxConcurrency: 8 })
-  .transform((t) => t.map(expensiveScore))
-  .transform((t) => t.filter((r) => r.ok), { local: true })
+new ConcurrentPipeline(rows, { maxConcurrency: 8 })
+  .transform((t) => t.map(expensiveScore))
+  .local((p) => p.transform((t) => t.filter((r) => r.ok)))
   .toArray();
```
