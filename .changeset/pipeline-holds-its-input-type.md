---
"@outputty/pipeline": major
---

**Breaking:** a `Pipeline` holds its input TYPE, not its data. It is composed once with no data and
RUN by calling it, so one definition serves every input (#90). A chain whose every callback is
synchronous now returns a plain array with no `Promise` created anywhere - measured at zero with
`node:async_hooks` - and one async callback, or an async input, widens the whole chain.

- `new Pipeline(data, options)` and `.from(data)` are deleted. Compose with
  `new Pipeline<In>(options?)` and call the result with the input.
- Every terminal op leaves `Pipeline` for `PipelineResult`, what calling a pipeline returns:
  `toArray`, `first`, `consume`, `forEach`, `chunks()` and both iteration protocols. A chain cannot
  be drained without an input, and a result cannot be extended.
- `Pipeline.merge(pipelines)` and `.merge(...others)` are deleted and go unreplaced. Both
  concatenated SOURCES, which a source-less pipeline has none of; concatenate inputs before calling.
- `.branch(definitions)` becomes `.branch((b) => …)`, configured by a fluent builder, and is a STAGE
  rather than a terminal: it returns a runner, and each arm receives a PIPELINE of the parent's own
  class, so an arm dispatches wherever the parent does and `.local()` inside it pins the arm.
  `BranchDefinition` and `BranchOptions.firstMatch` are deleted; `.broadcast()` replaces the latter.
- A dispatched stage's wire path reads as the chain was built: `/transform/<n>`, `/reduce/<n>`, and
  `/branch/<i>/<name>/transform/<n>` for an arm's own. A worker and its caller must be deployed
  together.
- `ConcurrentPipeline`, `HttpPipeline` and `ClusterPipeline` take `(pipeline, options)` and wrap a
  chain built elsewhere, so an HTTP worker and an HTTP trigger share one definition with no
  placeholder source.
- Type parameters: `Pipeline<T, M, P, In>` is now `Pipeline<T, M, In>`, and the three wrapping
  classes take `<T, In>`. `SourcePolicy` and `AssignMode` are deleted as types; `SourcePolicy`
  survives as the runtime value `sourcePolicy()` returns. Both deleted parameters were defaulted, so
  a one- or two-argument spelling is unaffected.
- A failure on a synchronous chain THROWS out of the terminal op instead of rejecting. There is no
  promise for a rejection to travel on.

Migration:

```diff
-const out = await new Pipeline(orders)
-  .transform((t) => t.map((o) => ({ ...o, total: o.total * 1.2 })))
-  .toArray();
+const withVat = new Pipeline<Order>()
+  .transform((t) => t.map((o) => ({ ...o, total: o.total * 1.2 })));
+const out = withVat(orders).toArray();   // number[], no await
+const other = withVat(moreOrders).toArray();   // same chain, no rebuild
```

```diff
-const split = await pipeline.branch({
-  big: { predicate: (o) => o.total > 200, transformer: label("BIG") },
-}, { firstMatch: false });
+const split = pipeline.branch((b) =>
+  b.when("big", (o) => o.total > 200, (q) => q.transform((t) => t.map((o) => `BIG:${o.id}`)))
+    .otherwise("rest")
+    .broadcast(),
+);
+const results = split(orders);
```
