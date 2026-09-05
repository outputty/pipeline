# @outputty/pipeline - Roadmap

Why each open ticket is worth building, and now. Status lives on GitHub Issues, not here: every ticket
is a GitHub issue, labelled `ready` (buildable) or `needs-planning` (grill it with `/plan` first), and
`gh` derives what is unblocked. This file is the durable index and the killed-idea dedup surface.

**Read the whole file** before evaluating an idea or closing work. A new idea is often a row that
already exists (Building / Later), or one already tried (Killed) - point the new one at that row.

## Building - open tickets, detail in each issue

- **Distributed and concurrent execution as `Pipeline` subclasses** (#17) - `ConcurrentPipeline`,
  `HttpPipeline` and `ClusterPipeline`, each overriding one thing, replace `ExecutionStrategy` and
  `.withExecutor()` entirely. A stage is its position in the chain, so a chunk crosses a boundary with
  an index instead of a function. Now, because the package cannot run CPU-bound work at all today: a
  chain finishes no faster on ten cores than on one, whatever `maxConcurrency` says.
- **`.catch()` must honour `onError`'s replacement array** (#15) - the exported `ChunkErrorHandler`
  promises a replacement and `.catch()` returns `[]`, so a caller's fallback silently drops rows.
- **`concurrent()` leaks unhandled rejections** (#16) - one chunk failure leaves every other in-flight
  rejection unhandled, which crashes the process under Node's defaults. #17 deletes `concurrent()`; if
  it lands first, #16 closes as moot.

### Later - not yet filed

- **A `ContextManager` class passed to a pipeline.** Today a remote stage's `ctx.set()` never reaches
  the caller - measured: the orchestrator's context stayed `{"multiplier":10}` after three remote
  `ctx.set()` calls. The shape agreed while planning #17: a dedicated `ContextManager` class the caller
  passes in, with a plain object meaning a local, one-way context that propagates to every worker.
- **A retry policy for a failed remote chunk.** Measured while planning #17: retrying one chunk on
  another instance ran that chunk twice (`runs per chunk {"[1,2]":1,"[5]":2,"[3,4]":1}`) - at-least-once,
  with no de-duplication surface.
- **Streaming `ordered: false`.** `unorderedExecution` drains the whole source before dispatching
  anything. #17 fixes this inside `ConcurrentPipeline`; the shipped strategy is deleted with the seam.

The two older candidates, still not filed:

- An executable docs harness mirroring `outputty/laygo`'s `docs-examples.test.ts`: every `<!-- compiles
  -->`/`<!-- illustrative -->` fence in `product.md`/`architecture.md`/`README.md` is hand-verified
  today, not machine-checked. Until it exists, a docs pass is checked against the real test suite by
  hand, per `.claude/rules/docs.md`'s standing rule.
- A `Pipeline`-against-real-`Layer` integration proof, once both packages publish to npm
  (`outputty/laygo` #746) and a consumer can actually install both.

## Built

- **Split from `outputty/laygo`** (`outputty/laygo` #743, #744, #745) - `@outputty/pipeline` moves from
  `packages/pipeline` inside the laygo monorepo to its own repository. #743 dropped the terminal ops'
  context-tuple return in favor of reading `.contextManager` directly off the `Pipeline` instance after
  a terminal op resolves; #744 swept every caller and test onto the new shape; #745 deleted the package
  from `outputty/laygo` and flattened laygo itself to a single-package repo.
- **Core chunked-transform engine** - `Pipeline`, `Transformer`, chunking, the sequential and
  concurrent execution strategies, `SimpleContextManager`, `.catch()` chunk-level error handling,
  `.branch()` / `Pipeline.merge()`, lifecycle hooks. Migrated from
  [laygo-python](https://github.com/ringoldsdev/laygo-python), async-first, before this repo's own
  tracker existed - no ticket number.
- **The execution-strategy seam as a function type** (#5) - `ExecutionStrategy<In, Out>` moved from a
  class-implementing interface (with its own closed, name-keyed executor registry) to a plain function
  type: `sequential`/`concurrent(options?)` replace the classes, a caller's own strategy is the same
  shape with no cast or registration, and `__tests__/` is typechecked for the first time. Also closed
  three related consumer-facing defects the untypechecked suite had hidden: `TransformerLifecycleHooks`
  callbacks, `loop`'s `condition` arity, and `Transformer`'s constructor accepting a mismatched
  `In`/`Out` with no `transform`. PRs #7, #8, #10, #12.

## Killed

Every row below was spiked and run while planning #17, not argued.

- **A worker-thread pool** (#17) - Piscina 5.3.2 works: `workerEntry(stages)` ran the canonical program
  in real worker threads, and a `stages.ts` importing the library constructed a real `Transformer`
  inside an isolate. Killed by measurement: isolated per-chunk dispatch cost flips at ~7-8 KB per chunk
  (81 B: piscina 15.0 us vs loopback 41.8; 10.4 KB: 104.8 vs 99.8; 1.12 MB: 8926 vs 6527), and
  `DEFAULT_CHUNK_SIZE` is 1000 items, so almost every real pipeline sits where loopback HTTP is faster.
  Removing it also removes an 836 KB dependency from a package that has one.
- **`workerPool()` as an execution-strategy factory** (#17) - the level the research document proposed.
  A strategy receives the whole composed chain and must discard it, so a caller's `.map()` silently
  never runs: `.map(x => x + 100).withExecutor(workerPool(...))` over `[1,2,3,4,5]` returned
  `[2,4,6,8,10]` where `[202,204,206,208,210]` was written.
- **Delegation as a plain async `transform`** (#17) - no new strategy, `concurrent()` unchanged, zero
  new surface, and it kept the caller's `.map()` (`[102,104,106,108,110]`). Killed with the seam itself.
- **A caller-owned stage map, named by string** (#17) - both `pipelineRoutes(stages)` and
  `.apply(stages.double)` by reference. Killed because a stage's position already identifies it, which
  removes the name, the map and the typo together.
- **The pull topology** (#17) - stages feeding each other, 3 HTTP requests against push's 6. Killed on
  four measurements: two pullers on one stream both received the identical full stream; adding a claim
  endpoint restored disjointness but pushed it back to 6 requests; two pullers on two instances each
  re-ran the whole source, which is the default behaviour behind a load balancer; a mid-stream failure
  arrives after a 200 so it cannot be reported as an error; and the chain deadlocked under
  `maxConnections = 1`, pinning one socket per stage.
- **"Pull is naturally backpressured"** (#17) - refuted. A slow consumer at 100ms/chunk read all ten
  chunks before processing one at 2 KB payloads; backpressure engaged only past ~4-6 MB in flight.
- **`get-port`** (#17) - built both paths, identical results. `listen(0)` already yields a shared port
  inside cluster and learns it from an already-bound socket, so it has none of the check-then-bind race
  `get-port`'s own readme documents.
- **A `.local(transformer)` method** (#17) - replaced by `{ local: true }` on `.transform()`/`.apply()`,
  which needs no new verb and confines the flag to the subclasses.
- **Wire-level drift protection** (#17) - a chain fingerprint, a stage count and a caller version string
  were all priced against a real reproduction (v1 `x*2`, v2 `x+1000`, mixed fleet -> `[2,4,1003,1004,1005]`
  at HTTP 200). Atomic deployment is documented instead.
- **`node:http` with a keep-alive agent as the client** (#17) - 94 us/chunk against `fetch`'s 467, in
  7/7 paired rounds. Killed for runtime neutrality: one code path on Node, Bun, Deno and Cloudflare.
- **Patterns 2-4 of the multicore research** (#17) - a data-URL worker cannot import workspace modules;
  `SharedArrayBuffer` and transferable objects copy objects and strings anyway, and this package's
  chunks are objects.
- **`workerpool`'s dynamic function offloading** (#17) - sends the function as a string and evals it.

- **An open `ExecutorType`, by any mechanism** (#5) - three ways to let a registered executor name
  typecheck were priced: a declaration-merged `ExecutorRegistry` interface, the `(string & {})`
  widening, and leaving the cast in place. All three died with the named registry itself. Passing the
  strategy function directly removes the name, so there is nothing left to open. The `(string & {})`
  form was independently disqualified: a spike proved a typo such as `"btched"` compiles under it and
  fails only at runtime.
- **`registerExecutor` and a named executor registry** (#5) - a second way to inject a strategy that
  passing the function already covers, backed by process-wide mutable state with no per-test reset.
- **`appliesInSourcePosition` on `ExecutionStrategy`** (#5) - a flag with exactly one true
  implementation, which made every other strategy declare a line whose only correct value was `false`.
  `inertKnobsOf` compares `transformer.strategy` against the built-in `sequential` by reference instead.
- **`createConcurrentTransformer`** (#5) - `createTransformer(chunkSize).withExecutor(concurrent(...))`
  says the same thing, and the helper was the last duplicate of the `maxConcurrency: 4 / ordered: true`
  defaults that `concurrent()` owns.
