---
"@outputty/pipeline": patch
---

Every runner is faster, with the same public API and outputs.

- A composed chain compiles once and runs each call from the cached plan, instead of replaying its stages per call.
- `.buffer(fn)` runs its own item loop instead of the reduce fold.
- Re-cutting after an async stage is linear in the chunk size.
- `ConcurrentPipeline` with `ordered: false` takes finishers from a completion queue, so its cost per chunk no longer grows with `maxConcurrency`. Work that settles in the same tick is yielded in the order it settled.
- `reduce`, `tap`, `flatMap`, the ordered fan-out and the HTTP and WebSocket dispatch paths allocate less per row and per chunk.
