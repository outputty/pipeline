---
"@outputty/pipeline": minor
---

**Breaking:** `.withHooks()` and `TransformerLifecycleHooks` are deleted. `.tap()` is the one
observation surface, at two levels: `Transformer.tap(fn | transformer)` travels with its stage; the
new `Pipeline.tap(fn | transformer)` (#72) always runs in the orchestrating process, even on
`HttpPipeline`/`ClusterPipeline`, where a dispatched stage either side of it still dispatches.

- `Transformer.tap(fn)` calls `fn` per item with its context; `Transformer.tap(transformer)` hands
  the whole chunk to a nested `Transformer` instead. Either travels with the stage it sits in.
- `Pipeline.tap(fn | transformer)` wraps the same call in `.local(build)`, pinning the callback and
  its context writes to the process that called it, whatever class it is called on.
- `onStart`/`onComplete`/`onItemStart`/`onItemComplete` go unreplaced by decision - `.tap()` covers
  observation; nothing replaces a lifecycle notification with no data to carry.

Migration:

```diff
-const t = new Transformer<number, number>().withHooks({
-  onStart: (chunk) => console.log(`start ${chunk.length}`),
-});
+const t = new Transformer<number, number>();
+const p = new Pipeline(source).tap((x) => console.log(`saw ${x}`)).apply(t);
```
