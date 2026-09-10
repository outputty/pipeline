---
"@outputty/pipeline": patch
---

Eight defects a whole-project review found, all of them older than #90 (#113). Two returned wrong
values with no error.

- A partitioned reduce now gives each partition its OWN accumulator seed. It handed every partition
  the single `initial` the caller passed, so a mutable seed was one accumulator shared by all of
  them: `.buffer(1).reduce((acc, x) => (acc.push(x), acc), [])` over `[1, 2, 3, 4]` at
  `maxConcurrency: 2` returned `[[1,2,3,4],[1,2,3,4]]` - the same array twice - where two partitions
  owe `[[1,3],[2,4]]`, and a downstream merge then double-counted every item. A seed
  `structuredClone` cannot copy now raises, naming `.local((p) => p.reduce(fn, initial))` as the
  unpartitioned alternative, rather than silently reverting to the shared object.
- Two sibling `ClusterPipeline` chains built off one base now claim their own worker registry slots.
  Both inherited the base's `pipelineIndex` and the second overwrote the first, so calling the first
  returned the second's output.
- `fanOutUnordered` closes its source on an early exit or a failure, as `ordered: true` already did
  through its own `for await` - one boolean apart, two resource outcomes before this.
- A `ClusterPipeline` worker that dies before reporting its port now REJECTS the bootstrap with its
  exit code and signal. Every dispatch in the process hung forever instead, with no diagnosis.
- The reduce request body enqueues one frame per `pull` rather than draining the whole upstream in
  `start`, so the stream's own `desiredSize` backpressures the shared iterator. Each of
  `maxConcurrency` partitions used to race the entire source into its own in-memory queue.
- `writeStreamedBody` removes both its `drain` and `close` listeners when either fires. Repeated
  backpressure on one response accumulated them until Node warned about a leak.
- `SimpleContextManager.getOrDefault` tests key presence rather than `value !== undefined`, so a
  deliberately stored `undefined` reads back as itself and agrees with `.get()`.
- `Transformer.toAsyncIterable` is deleted - private, with no call sites.
