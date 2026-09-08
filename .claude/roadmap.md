# @outputty/pipeline - Roadmap

Why each open ticket is worth building, and now. Status lives on GitHub Issues, not here: every ticket
is a GitHub issue, labelled `ready` (buildable) or `needs-planning` (grill it with `/plan` first), and
`gh` derives what is unblocked. This file is the durable index and the killed-idea dedup surface.

**Read the whole file** before evaluating an idea or closing work. A new idea is often a row that
already exists (Building / Later), or one already tried (Killed) - point the new one at that row.

## Building - open tickets, detail in each issue

- **A conformance suite every `Pipeline` and Context class runs** (#37) and **cross-runtime
  benchmarks** (#11) are the other open tickets; each issue carries its own detail.
- **`.tap()` becomes the one observation surface** (#72). `.withHooks()` is deleted with
  `TransformerLifecycleHooks`, and `Pipeline` gains its own `.tap()` that always runs in the
  orchestrating process. Now, because `.withHooks()` is silently order-sensitive - `pipe()` drops it,
  so `t.withHooks({onStart}).map(f)` fires nothing while `t.map(f).withHooks({onStart})` fires - and
  because its invariant `hooks` field is what breaks `t.tap(someTransformer)` at the type level.
  Blocked by #40: a fixed `.onError()` must ship before `hooks.onError`, today's only
  failure-observation surface, is removed.

  #37's own conformance case for `.reduce()` needs its `ConcurrentPipeline` branch rewritten now
  (#62): a bare `.reduce()` prints `[15]` on `Pipeline` but N values summing to 15 on
  `ConcurrentPipeline` (partition count is a ceiling, timing-dependent) - the conformance case
  either sums the array or adds `.local((p) => p.reduce(mergeFn, initial))` to compare one value.

### Later - not yet filed

- **A `ContextManager`'s write-back to the orchestrator.** #31 closed "which manager" - a caller's
  own `IContextManager` now survives `.context()`, `Pipeline.merge()`, and a `ClusterPipeline`
  worker's process boundary as the SAME instance. The write-back half stays unbuilt by decision, not
  omission: a remote stage's `ctx.set()` still never reaches the orchestrator - measured, still true
  post-#31: the orchestrator's context stayed `{"multiplier":10}` after three remote `ctx.set()`
  calls (`.claude/architecture.md`'s own constraint). What stays open is an in-memory manager with no
  backing store of its own to publish through.
- **A retry policy for a failed remote chunk.** Measured while planning #17: retrying one chunk on
  another instance ran that chunk twice (`runs per chunk {"[1,2]":1,"[5]":2,"[3,4]":1}`) - at-least-once,
  with no de-duplication surface.

The two older candidates, still not filed:

- An executable docs harness mirroring `outputty/laygo`'s `docs-examples.test.ts`: every `<!-- compiles
  -->`/`<!-- illustrative -->` fence in `product.md`/`architecture.md`/`README.md` is hand-verified
  today, not machine-checked. Until it exists, a docs pass is checked against the real test suite by
  hand, per `.claude/rules/docs.md`'s standing rule.
- A `Pipeline`-against-real-`Layer` integration proof, once both packages publish to npm
  (`outputty/laygo` #746) and a consumer can actually install both.

## Built

- **A dispatched reduce really partitions across `maxConcurrency` accumulators** (#62, `feat!`) -
  #45 shipped a reduce stage as a serialization point, one accumulator whatever `maxConcurrency`
  said, so a `ConcurrentPipeline` fanning a `.map` out four ways collapsed to a single fold the
  moment `.reduce()` appeared. `ConcurrentPipeline.reduce()` now folds `maxConcurrency` independent
  accumulators: `reduceWork()` is still called once, but the closure it returns is called
  `maxConcurrency` times, each its own `share()` view (`src/utils/chunk.ts`, free-slot dealing over
  one shared iterator) of the ONE chunk stream, merged in completion order by `mergeUnordered()`;
  `HttpPipeline`/`ClusterPipeline` inherit partitioning with no new code, since `reduceWork()`'s
  existing per-request `Reducer` already means N concurrent dispatches fold N independent
  accumulators. Each partition's own result - an `emit()` mid-fold, or its trailing accumulator once
  its share of the stream ends - flows downstream as an ordinary value, the same way a
  non-partitioned reduce's own `emit()` output already does: no forced merge, no thrown error. A
  caller who wants ONE final value writes an ordinary second reduce as the next stage -
  `.local((p) => p.reduce(mergeFn, initial))` (#61) - the same pattern used to fold down any other
  multi-value reduce output; reusing the fold itself as that merge is silently wrong in general (the
  count case's own fold, `(acc, _x) => acc + 1`, typechecks perfectly as its own merge and returns
  the number of partitions instead of the count), which is exactly why nothing is derived
  automatically. BREAKING, no deprecation period: every existing `ConcurrentPipeline.reduce()` call
  now returns as many values as there are partitions instead of one.
  PRs #68 (L1, pinned cases), #69 (L2, partitioning), #71 (enable), #74 (docs).
- **`.onError()` reports the chunk that actually failed** (#40, `feat!`) - `Transformer.process()`'s
  own catch sat outside the chunk loop, so a handler saw `[]`, and `ConcurrentPipeline.apply()`
  refused a dispatched stage carrying one outright. `Transformer.chunkErrorReporter` now reports the
  chunk that actually failed, on every class - `HttpPipeline`/`ClusterPipeline` included, since both
  only narrow `apply()`'s return type and delegate to `super.apply()` unchanged.
  `ConcurrentPipeline.apply()`'s wrapped `work` calls it immediately for a dispatched stage;
  `runSequentially`'s own per-chunk try/catch only CAPTURES the chunk for a local one, and
  `process()`'s own outer catch reports it after `hooks.onError` runs, keeping the two notification
  mechanisms' relative order unchanged. `dispatchKnobViolations` keeps only its `withHooks` branch.
  BREAKING, no deprecation period: a handler that read `chunk.length` as "no detail available" must
  be updated. PRs #75 (L1, the fix and its tests), #76 (docs).
- **`.local(build)` runs a whole region in the orchestrating process** (#61, `feat!`) - the per-stage
  flag it replaces had to be repeated on every stage of a region that must stay put, and lived only
  on the dispatching subclasses, so a chain using it never typechecked on a base `Pipeline`.
  `.local(build)` builds a bare `Pipeline` over the caller's own chunk stream, runs the caller's
  builder against it (nothing inside can dispatch), and carries the result back through
  `createPipeline()` so the caller's own class resumes afterward - one implementation on the base
  class, each dispatching subclass re-declaring it only to narrow its return type. BREAKING, no
  deprecation period: `StageOptions` and its `options?` argument - the second on `.apply()`/
  `.transform()`, the third on `.reduce()` - are deleted.
  PRs #64 (L1, `.local(build)` + narrowing overrides), #65 (enable, `StageOptions` deleted), #67
  (docs).
- **`Pipeline.prototype.merge()` continues a pipeline already held, keeping its class** (#41) - the
  static `Pipeline.merge()` always builds a plain `Pipeline` and always restarts `_chunkTransforms`
  at 0, so a merged dispatching pipeline gaining one more stage collides with its own first stage on
  `/stage/0`. Two spiked static designs both produced this exact collision (`out
  [100000,200000,300000,400000]` instead of `[1100,2100,3100,4100]`; a mixed-class refusal wrong too:
  `[3,4,4,5]` instead of `[3,5,4,5]`). `pipeline.merge(...others)` has no such problem - there is no
  stranger, it continues an instance that already has its own class, knobs and stage table via
  `createPipeline()`. `mergeContextsInto()`/`concatChunks()` are the one shared implementation the
  static and the instance method both call, rather than two copies of the same loop. PR #60.
- **A reducer on the `Pipeline`, folding every chunk it receives** (#45, `feat!`) - `reduce` only
  folded one chunk before, and the whole-dataset form returned a standalone callable that was never
  a stage, so a running total across a stream meant draining the pipeline and folding outside it,
  giving up both streaming and dispatch. `Pipeline.reduce(fn, initial)` folds every chunk, in-process
  and sequential - `ConcurrentPipeline.reduce()` overrides it and dispatches to `reduceWork()`,
  `stageWork()`'s sibling (at the time this shipped, keeping a fold in-process was a per-stage flag;
  #61 later replaced it with the region combinator `.local(build)`); `HttpPipeline.reduceWork()`
  opens one duplex POST to `/reduce/<n>` (NDJSON both ways), `maxConcurrency` inert on it at the
  time (#62 later partitioned it into `maxConcurrency` independent accumulators, each flowing
  downstream on its own); `ClusterPipeline`'s own bootstrap/`inFlight` bracket wraps the WHOLE
  connection instead of one chunk. `emit()`, the reducer callback's fourth argument, banks a value downstream mid-fold and
  resets the accumulator; the trailing accumulator is only emitted if items were folded since the
  last `emit()`. `toNodeHandler` streams both directions now instead of buffering them whole,
  unblocking the duplex response every Node consumer gets, one-shot routes included. BREAKING:
  `Transformer.reduce`'s old per-chunk-toggle overload and `ReduceOptions` are deleted;
  `PipelineReduceFunction` is `ReduceFunction`.
  PRs #52 (L1, pinned cases), #53 (L2, the fold + `emit`), #55 (L3, `ConcurrentPipeline`), #56 (L4,
  `toNodeHandler` streaming), #57 (L5, `HttpPipeline`/`ClusterPipeline` duplex dispatch), #58
  (enable), #59 (docs).
- **Chunking becomes an explicit `Pipeline.buffer()` boundary, off `Transformer` entirely** (#39,
  `feat!`) - `ConcurrentPipeline.apply()` used to refuse a custom chunker outright and re-derive its
  own cut from `transformer.chunkSize`, disagreeing with `.transform()`'s own seeding (#42, closed as
  superseded). `.buffer(size)` replaces both: the ONE place a cut happens, persisted across every
  later stage until called again, uniform across every `Pipeline` class - `Transformer` loses all
  chunking knowledge (`chunkSize`/`.setChunker()`/`execute()`), replaced by `.process(chunks, ctx?)`.
  The "source position" mechanism (a separate async-iteration replay path, and the throw it needed
  for a knob it couldn't honor) is deleted with it: every consumption path now reads the same
  persisted chunk stream. PRs #48 (L1, pinned cases), #50 (L2, the seam, also enable - no flag was
  possible for an API removal), #51 (docs).
- **A caller's own `IContextManager` survives `.context()`, `merge()` and a process boundary** (#31)
  - `.context()` mutates the caller's OWN manager in place instead of copying into a fresh
    `SimpleContextManager`, so a custom class keeps receiving writes and a rejected write propagates
    instead of being bypassed. `Pipeline.merge(pipelines, options?)` takes the pipelines as an array
    (BREAKING) and an optional `options.context`, the same instance later pipelines still win on a
    shared key against. `PipelineOptions.contextFactory` builds a `ClusterPipeline` worker's own
    class once per process; `.fetch()` reuses that instance to serve, instead of rebuilding one from
    the wire per request. PRs #33 (L1), #34 (L2), #36 (L3), #38 (docs).
- **Distributed and concurrent execution as `Pipeline` subclasses** (#17) - `ConcurrentPipeline`,
  `HttpPipeline` and `ClusterPipeline`, each overriding one thing, replace `ExecutionStrategy` and
  `.withExecutor()` entirely. A stage is its position in the chain, so a chunk crosses a boundary with
  an index instead of a function. Also closes #16 (`concurrent()`'s unhandled-rejection leak) as moot -
  `concurrent()` is deleted with the seam it belonged to. PRs #20 (L1, stubs and pinned cases), #21 (L2,
  polymorphic copy-on-write), #23 (L3, `ConcurrentPipeline`'s streaming fan-out), #24 (L4,
  `HttpPipeline`), #25 (L5, `ClusterPipeline`'s worker bootstrap), #26 (enable, the seam deleted).
- **`.catch()` honours `onError`'s replacement array** (#15, PR #19) - the two disagreeing
  `ChunkErrorHandler` declarations (`src/types.ts`, exported, promising a replacement;
  `src/errors/handler.ts`, what `.catch()` actually ran, always dropping the chunk) are one
  signature now. `ErrorHandler.handle()` runs its handlers LIFO and returns the first one's
  replacement array, `undefined` if none replaced - `.catch()` substitutes on an array, drops on
  `undefined`.
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

- **The combine debt** (#62, built and shipped on L1/L2, then deleted before merge) - `owesCombine`, a
  tracked flag every copy-on-write `Pipeline` method carried forward, and `assertCombined()`, throwing
  at every terminal op until a `.combine()` stage ran. Reused an ordinary `ReduceFunction` under a
  dedicated name and made every partitioned reduce a two-step ritual whether or not the caller wanted
  one final value. Killed by the user: "I don't see much value for it... run multiple reducers that
  emit their individual results... it's up to the user to decide." Shipped instead: each partition's
  result flows downstream as an ordinary value, same as any non-partitioned reduce's own `emit()`
  output; a caller who wants one value writes `.local((p) => p.reduce(mergeFn, initial))` by hand.

- **A phantom-type compile-time guard for the combine debt** (#62, spiked, never committed) - a second
  type parameter tracking whether a `Pipeline`'s pending reduce had been combined, so a forgotten
  `.combine()` failed `tsc` instead of throwing at runtime. Verified working end to end with real
  `tsc --strict` probes, including a property-name trick to shape the compiler's own error message.
  Killed alongside the mechanism it protected: once the combine debt itself was deleted, there was
  nothing left for a compile-time guard to guard.

- **`EventEmitterPipeline`** (#30, closed unbuilt) - a fourth `Pipeline` subclass publishing five
  chunk-level lifecycle events per dispatched stage, on a `PipelineEmitter` the caller passes in.
  Killed on its own opening premise, re-run while planning #72: `.withHooks()` was never the only
  observation surface. `Transformer.tap` already observes, and `dispatchKnobViolations` never refused
  it - `ConcurrentPipeline.buffer(2).transform((t) => t.map((x) => x * 2).tap(push))` over `[1..5]`
  returned `out [2,4,6,8,10]  seen [2,4,6,8,10]`. Two more of its premises went stale after it was
  filed: `{ local: true }` (#61 deleted it; `.local(build)` already gives an orchestrator-side tap)
  and "both fan-outs yield ITEMS" (#39 made both yield `U[]`). So the class bought nothing `.tap()`
  did not already do, at the cost of a `fanOut()` seam, an emitter interface, five event names and a
  consumer-error containment path. Its Enable layer - deleting `.withHooks()` - is what survives, as
  #72.

- **A forward-descending `Transformer` composition** (#45) - each link calling the NEXT one rather
  than wrapping the previous one, so the stack descends in the order the caller wrote the chain.
  Measured: today's composition enters last-link-first and produces data on the unwind (`enter
  filterOp`, `enter mapOp`, `enter reduceOp`, then `exit reduceOp -> [10]`, `exit mapOp -> [100]`).
  The forward form buys a stack trace in pipeline order and lets a link call its successor several
  times or not at all - which would let a reducer push each emitted value downstream immediately
  instead of returning them together at the end of the chunk. Killed by the user: it makes every
  link a middleware that decides whether the rest of the chain runs, which is a larger contract than
  `map`/`filter`/`reduce` need, and it would rewrite `pipe()` and every link including `.catch()`.
  A reducer stays one ordinary `pipe()` link (`Transformer.reduce`, `src/transformer.ts:626-634`).

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
- **A `.local(transformer)` method, single-stage** (#17) - replaced by a per-stage flag on
  `.transform()`/`.apply()`, which needed no new verb and confined the flag to the subclasses. That
  flag itself was killed by #61, whose Built entry above has the reasoning; #61 ships a DIFFERENT
  `.local(build)` - a region builder taking a whole sub-chain, parameterized over a base `Pipeline`
  so nothing inside it can dispatch at all, correct on every class unchanged. Not a revival of this
  row: the killed form took one `Transformer` for one stage; the shipped form takes a
  builder function over several stages.
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
