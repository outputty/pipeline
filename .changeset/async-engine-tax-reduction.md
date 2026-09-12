---
"@outputty/pipeline": patch
---

`ConcurrentPipeline`/`HttpPipeline`/`ClusterPipeline` pay less to run a fully synchronous chain over
a plain array, even though `sourcePolicy()` still forces Mode `"async"` for dispatch purposes.
`toAsyncIterable` hand-rolls its iterator instead of an `async function*` (fewer Promise-wraps per
pull, `.return()` still forwarded for an early `.first(n)`); `.buffer()`'s async fold loop
(`buildBufferGenerator`) no longer `await`s a result that was never a thenable; `fromSource()` keeps
the original sync view alive under a forced-async Mode so `.buffer()` can fold through it
synchronously and cross the async boundary once per chunk instead of once per raw item. Output
unchanged - measured on this package's own committed `bench/overhead.ts` harness, each dispatching
class's own `.local()` row roughly halved; see `bench/baseline.json` for the exact figures.
