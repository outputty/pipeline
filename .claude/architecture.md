<!-- architecture.md - the module layout and how a chunk actually flows, end to end. Terse by design: a
     paragraph states the rule, a diagram or a snippet shows it. What each capability gives a user
     lives in product.md, never here. -->

# @outputty/pipeline - Architecture

A `Pipeline` wraps a source and a `Transformer` chain; a `Transformer` is the chain itself, independent
of `Pipeline`. This document is how a chunk actually moves through that chain, end to end, and what
restricts it.

## The stack

```text
┌─────────────────────────────────────────────────────────┐
│ App code — TypeScript, tsc-checked                       │
├─────────────────────────────────────────────────────────┤
│ @outputty/pipeline — Pipeline family · Transformer        │
├─────────────────────────────────────────────────────────┤
│ node:* — ClusterPipeline/HttpPipeline/EventEmitterPipeline │
│ only; a plain Pipeline/ConcurrentPipeline needs neither    │
├─────────────────────────────────────────────────────────┤
│ the caller's own AsyncIterable source                    │
└─────────────────────────────────────────────────────────┘
```

Nothing below the caller's source is this package's concern - no DB driver, no file I/O, no network
client of its own beyond what dispatching a stage requires. A `Pipeline` accepts an array, an
`AsyncIterable`, or any object shaped as one; a laygo `Model` reaches a `Pipeline` the same way,
structurally (`outputty/laygo`'s `Source` accepts any `AsyncIterable`), with no import edge in either
direction (#743, #745).

Both boundaries are oxlint-enforced, not merely descriptive (#117): `.oxlintrc.json`'s
`import/no-nodejs-modules` override fails any `node:` import added to a `src/**/*.ts` file other than
`pipelines/http.ts`, `pipelines/cluster.ts`, `pipelines/eventemitter.ts` (the third dispatching
file, added when #124 shipped `EventEmitterPipeline` after this diagram's own node: exception list was
first written) and `pipelines/client.ts` (the fourth, #179 - `HttpPipeline`'s own dispatch client,
whose `node:http` default is what its seam exists to choose between). `pipelines/websocket.ts` and
`pipelines/websocket-cluster.ts` (#239, both on the `websocket` entry) are the fifth and sixth: the
first takes `IncomingMessage`/`Duplex` as type-only imports so no published `.d.ts` names a `ws`
type, the second is `ClusterPipeline`'s own `node:cluster` bootstrap. A
`no-restricted-imports`
override fails any `@outputty/laygo` or `@outputty/laygo/**`
import from anywhere in `src/`. A reader no longer has to compare a new import against this diagram by
hand - `bunx oxlint src/` does it on every run.

## Module layout

```text
src/
  types.ts              PipelineFunction, IContextManager, InternalTransformer, every options
                          interface, plus DROP/RowErrorHandler/PipelineErrorHandler/RunScope (#78);
                          StageRegistries (a stage's chunkTransforms+reduceStages pair),
                          Drainable<T> (the 3-field drain view PipelineResult/BranchOwner share),
                          ReduceWork<T,U>, RouteVerb/StageRoute, Tagged<R> (#133)
  pipeline.ts            Pipeline: the chain, context, stages, Pipeline.drainable, createPipeline<U,
                          R>() + defer<U,R>() (each takes its own return type, letting a
                          DISPATCHING SUBCLASS's own two-argument call - `ConcurrentPipeline.apply()`
                          - skip the `as X` cast its base-class caller still needs, #133) + onError()
                          (#78); isSync()/freshPreBuffer() are `protected` methods a dispatching
                          subclass may override, `asyncIterableFrom()`/`isAsyncSource()` are unexported
                          module-level functions - all 4 unify 2-4 raw-spelled copies each within this
                          file (#133); emptyChunks<U>() is EXPORTED (`cluster.ts` calls it too, to empty
                          a worker's own chunk stream)
  transformer.ts          Transformer: the chainable map/filter/reduce/tap chain, plus onError()
                          (the row handler, #78) and runnable() (the seam that carries it in); every
                          element-wise link (map/filter/flatMap/tap(fn)) shares one pipe() body (#133)
  branch.ts               BranchBuilder (.when/.otherwise), BranchOwner/BranchRunner/BranchArm,
                          runBranch/joinArms(grouped, dispatch: ArmDispatch<T>, context) - pushArm()
                          and findCatchAll() are BranchBuilder's own private helpers, replacing a
                          repeated cast-and-push and a repeated find(isCatchAll) (#133);
                          classifyItems/classifyAsyncChunks/claimItem fuse the demux into ONE walk
                          over the chunk stream instead of collectItems() then a second loop (#180)
  result.ts               PipelineResult - every terminal op; drainable() returns Drainable<T>
                          directly; forEach()/[Symbol.iterator]() each test `syncChunks !== null` for the
                          sync/async branch (#232 deleted the dispatchSync() wrapper)
  websocket.ts           The `@outputty/pipeline/websocket` entry (#239): WebSocketPipeline,
                          ClusterPipeline, toNodeWebSocketHandler and their types, the only entry
                          that loads ws. index.ts exports none of them
  pipelines/
    concurrent.ts          ConcurrentPipeline - the fan-out (fanOutOrdered/fanOutUnordered),
                             stageWork()/reduceWork(); carriedKnobs() (not createPipeline()) is the
                             one override a subclass writes to carry its own knobs forward (#133)
    http.ts                 HttpPipeline - stageWork()/reduceWork() overrides, routePath(verb,
                             index), .fetch() (/transform/<n> and /reduce/<n>), toNodeHandler;
                             errorResponse()/unknownBranchRoute() replace 6+ inline
                             Response.json({error}) calls, buildReduceRequestBody()/
                             parseReduceFrames() split reduceWork()'s own dispatch generator,
                             nodeRequestToFetchRequest() is handleOverBridge's own request half (#133)
    client.ts               PipelineClient - how a dispatched chunk travels, and the knob
                             options.client replaces (#179). fetchClient is the global fetch;
                             defaultClient() resolves node:http with a shared keep-alive Agent once
                             per process, both node: imports dynamic and inside one try so a runtime
                             without them falls back rather than failing to load
    cluster.ts               ClusterHttpPipeline (#17, renamed #201) - the WorkerSet class
                             (register/claimIndex/lookup/bootstrap/enter/kill/startWorkerServer)
                             replaces 5 module-level mutable bindings and 4 free functions with one
                             per-process singleton (#133); bootstrapAndSetUrl() calls
                             workerSet.enter() once, no longer bootstraps twice. Loads no ws, and
                             websocket-cluster.ts never imports it: its module scope starts the HTTP
                             worker server in every worker (#239)
    websocket.ts             WebSocketPipeline (#201) - encodeFrame/decodeFrame (the 4-byte
                             length-prefixed binary framing), getConnection() (the per-connect-target
                             memoized client), stageWork()/reduceWork()/serve()/receiveFrame(),
                             toNodeWebSocketHandler (the Node upgrade bridge; NodeWebSocketHandler.
                             upgrade is typed with node:http's IncomingMessage and node:stream's
                             Duplex, so no .d.ts names a ws type, #239), peekFrame()/
                             sendUnknownRouteError() (ClusterPipeline's own shared-worker-server seam).
                             The one file that imports ws
    websocket-cluster.ts     ClusterPipeline (#201, moved here #239) - the SAME shape as cluster.ts
                             over WsWorkerSet, N distinct ws+unix: socket paths instead of one shared
                             port, enter() round-robining across them
    eventemitter.ts           EventEmitterPipeline (#124, events renamed to routes #221) -
                             stageWork() calls the composed function directly and dispatches
                             through pipeline.emitter for any extra Workers; apply()/drainable()
                             overridden a second and third time for a route's own :end/a trail's
                             own :end; carriedKnobs() (not createPipeline()) carries its emitter
  context/
    simple.ts              SimpleContextManager - the one shipped IContextManager; context/types.ts
                             (a dead re-export) is deleted (#133)
  utils/
    chunk.ts                a thin re-export barrel over cut.ts/drain.ts/recut.ts, so an existing
                             `from "@src/utils/chunk"` import keeps resolving (#133); `normalize`, its
                             one test file and the `utils/index.ts` barrel are deleted (#232) -
                             `src/index.ts` re-exports `buildChunkGenerator` from here and
                             `isContextAware` from helpers.ts directly
    cut.ts                  buildChunkGenerator/buildSyncChunkGenerator (cut) / flattenChunks
                             (undo) / share / collectItems (`collectAsyncChunks()` is
                             `collectItems()`'s own unexported async half); assertPositiveChunkSize()
                             is the one `chunkSize < 1` guard 3 sites shared inline before (#133);
                             assertWholeNumberAtLeastOne(label, value) is `.buffer(size)`/`.queue()`'s
                             own shared, labelled validator (#123); prefetch(upstream, capacity) is
                             `.queue()`'s own engine, beside `share()` (#123)
    drain.ts                 MaybeAsyncChunks<T>, drainSync/drainSyncSettled/close - the sync terminal-op
                             drivers; closingOnFailure() builds on helpers.ts's tryRecover()
    recut.ts                 RecutState<T> ({iterator, size}) / recutFrom / recutPending /
                             cutChunk / recutSyncChunks - the iterator+size pair `recutFrom` and
                             `recutPending` used to thread separately is now one state object (#133)
    helpers.ts               isContextAware - fn.length arity check (isContextAwareReduce, its
                             reduce-side twin, is gone: every reduce path always passes all four
                             ReduceFunction arguments, #45); dropOrRethrow - the run handler's own
                             "call it, or propagate" decision, shared by runSequentially and
                             ConcurrentPipeline.apply()'s wrapped work (#78); tryRecover() is the
                             try/catch-if-thenable/recover skeleton runStageChunk here and
                             drain.ts's closingOnFailure and transformer.ts's settleRowStep all share - NOT used by
                             utils/reduce.ts's Reducer.fold, whose own hot per-item path keeps its
                             measured-faster inlined form (#133)
    reduce.ts                Reducer/foldChunk/foldChunkStream - the shared fold, used by
                             Transformer.reduce, Pipeline.reduce and http.ts's own frame folding;
                             Reducer takes an optional row handler (#78); Reducer.final(seedIfEmpty)
                             answers a fold that never ran with the seed, passed only by an owner of
                             a whole stream (#241); Reducer.current() reads
                             the raw accumulator with no itemsSinceEmit gating (#88);
                             buildBufferGenerator/buildSyncBufferGenerator/recutSyncChunksWith are
                             .buffer(fn)'s own engine, bufferReduceFunction the one adapter onto
                             Reducer<T[], T> (#88; .buffer(size) left this engine in #179 and
                             sizeReduceFunction went with it)
    ndjson.ts                readNdjsonLines/ndjsonFrame - the reduce wire's framing, shared by
                             the client (reduceWork) and the server (.fetch's /reduce/<n>)
  factories.ts             createTransformer - Transformer construction sugar, no chunk-size
                             parameter (#39: chunking lives on Pipeline, not Transformer)
  index.ts                 barrel - the only export surface
```

## How a chunk flows

The cut lives on `Pipeline`, never `Transformer` (#39). `Pipeline` owns a persisted chunk stream
(`_chunks`), cut once - either by the constructor's own default the moment one is first needed, or
by `.buffer(size)` - and carried unchanged through every later stage; `Transformer.process()` never
cuts, only processes whatever chunk it is handed:

```text
Pipeline constructor / .buffer(size)              the ONLY place a cut happens
	buildChunkGenerator(size)(preBufferItems)       cuts the flattened item stream into In[] chunks
Pipeline.apply(transformer) (every later stage)
	Transformer.process(this._chunks, context)      NO cut here - runs the chunks it is handed
		runSequentially(internalTransformer, chunks, context)   one chunk at a time, in order
			internalTransformer(chunk, ctx)          one map/filter/flatMap/reduce/tap link, chained
				isContextAware(fn) ? fn(item, ctx) : fn(item)      arity-checked once per link, not per item
Pipeline.toArray() (or any terminal op, or async iteration)
	flattenChunks(_chunks)                          the ONE place chunks become items again
```

`_preBufferItems` is the pre-cut ITEM view a `.buffer()` call recuts from - carried forward
unchanged by every copy-on-write method EXCEPT `.apply()`, which nulls it (a real stage just
consumed `_chunks`, so nothing is left to recut from except that stage's own output). This is what
collapses `.buffer(2).buffer(3).buffer(4)` (nothing between them) to only the LAST cut ever actually
applied: each intermediate `.buffer()` call builds a chunk generator that is simply never driven,
since the next `.buffer()` reads `_preBufferItems`, not `_chunks`.

A `ConcurrentPipeline`/`HttpPipeline`/`ClusterPipeline` stage bypasses `Transformer.process()`
entirely - see "The pipeline family", below - but shares the SAME `_chunks` state: its own fan-out
reads `this._chunks` directly and cuts none of its own, so a custom `.buffer()` boundary reaches a
dispatched stage exactly like a local one. Wrap the chain in one of those classes for concurrency
instead of configuring the `Transformer`.

## buffer(fn) - a callback-driven chunk boundary - #88

`.buffer(fn: BufferFunction<T>)` folds through one `Reducer<T[], T>` (`src/utils/reduce.ts`,
unchanged from what `Pipeline.reduce()` already uses), configured by `bufferReduceFunction(fn)`,
which adapts a caller's zero-arg `emit`/`flush` onto the reducer's own value-taking `emit`.

⚠ `.buffer(size)` shared that engine until #179 and no longer does, though the chunk boundaries it
produces are identical. It was configured with an identity `fn` and a framework-side auto-flush at
`pending.length >= size`, through an adapter called `sizeReduceFunction` - deleted with the split. A
fold engine buys a per-ITEM decision, which a count never makes, and charged a closure call plus an
array-mutating accumulator per row for it: over an async generator with output asserted identical,
`.buffer(1000)` created clearly more promises per row through the fold than cutting by count, which
sits at `buildChunkGenerator`'s own floor, exactly where the same chain with NO `.buffer()` call at
all sits. `.buffer(size)` now cuts by count on all three arms with the same cutters
`fromSource()` uses, through the one private `cutBy()` that `.buffer(fn)` shares (#232), and `.buffer(size)` validates its size on the BOUND path too, where
`sizeReduceFunction` used to be what refused a bad one (BREAKING: `.local((p) => p.buffer(2.5))`
threw nothing before and now throws under `.buffer()`'s own name).

```text
Pipeline.buffer(sizeOrFn)
	isDeferred() ? record + replay : …
	typeof sizeOrFn === "number" ?
		cutBy(buildSyncChunkGenerator, recutSyncChunks, buildChunkGenerator)   by COUNT, no fold
			isSync() && _syncPreBufferItems  → buildSyncChunkGenerator(size)(items)
			isSync()                         → recutSyncChunks(_syncChunks, size)
			_syncPreBufferItems              → asAsyncChunks(buildSyncChunkGenerator(size)(items))
			else                             → buildChunkGenerator(size)(items)
	: bufferReduceFunction(fn)                              ONE ReduceFunction<T[], T>
		isSync() ?
			_syncPreBufferItems !== null → buildSyncBufferGenerator(reduceFn, ctx)(items)
			else (a real stage ran)      → recutSyncChunksWith(_syncChunks, reduceFn, ctx)
		: buildBufferGenerator(reduceFn, ctx)(items)        fully async arm
```

Each `emit()` - a caller's explicit `flush()` - IS a chunk
boundary, so `buildBufferGenerator`/`buildSyncBufferGenerator`/`recutSyncChunksWith` must never group
more than one emit into a single downstream chunk, unlike `foldChunkStream`'s own reduce-shaped fold
(there, everything one INPUT chunk emits collapses into one downstream value by design). The shared
`driveFold` generator (`buildSyncBufferGenerator`/`recutSyncChunksWith`'s common tail-chaining
engine) is what keeps that true once genuinely async: a `MaybeAsyncChunks` slot carries exactly one
`T[]` per yield, so a unit (one item, or one existing chunk's worth via `foldChunk`) that emits more
than once queues its later emits in `remaining`, drained - still in order - the moment the generator
resumes, which only happens after the caller has awaited the first one. Review-caught, fixed before
merge: an earlier cut `.flat()`-ed every emit from one unit into a single oversized chunk - real for
a normal multi-item re-cut after an async stage, not an edge case, measured: `.buffer(10)
.transform((t) => t.map(async (x) => x * 2)).buffer(sizeTwo)` (`sizeTwo` flushing every 2 items) over
`[0..9]` yielded one 9-item chunk instead of six.

`Reducer.final()`'s own `itemsSinceEmit` gate - built for `.reduce()`'s contract, where a value
returned right after an `emit()` may be an unrelated fresh seed - reads `0` whenever a fold both
flushes and appends the SAME item, which `bufferReduceFunction`'s flush-then-append shape does on
purpose. `.buffer(fn)`'s own trailing check is `Reducer.current()` (the raw accumulator, no gating)
via `trailingOf()` instead: found verifying the `driveFold` fix above with a real run rather than a
hand-derived expected value, a stream's own LAST item silently vanished whenever it both caused a
flush and repopulated the pending array - `Reducer.final()` read `0` where `Reducer.current()` reads
the real, non-empty pending array.

`.buffer(fn)` widens Mode to `"async"` when `fn` returns a `Promise`, via two overloads ordered
Promise-first - `(item, ctx, emit) => Promise<T | typeof DROP>` → `Pipeline<T, "async", In>`,
`(item, ctx, emit) => T | typeof DROP` → `this` - mirroring `Pipeline.reduce()`'s own split rather
than `.tap()`'s `M extends "async" ? this : …` conditional-collapse form: neither `.reduce()` nor
`.buffer()` has a subclass override needing `this`-preservation, so there is no "already async, stay
`this`" case worth the extra complexity. The implementation body's own `_mode` field is NOT updated
explicitly for an async `fn` on a `"sync"`-Mode chain - `.reduce()`'s own sync branch has the
identical gap (`mode: this.sourcePolicy() === "async" ? "async" : "sync"`, blind to `fn`'s own
async-ness) - and this is safe for the same reason `.reduce()`'s is: `buildSyncBufferGenerator`'s own
tail-chaining discovers a genuine `Promise` from the DATA, never from `_mode`, so a terminal op still
returns the right value; only `isSync()`'s own bookkeeping reads stale until the next real cut.

## Prefetching - #123

`.queue(capacity)` (`Pipeline.queue`, `src/pipeline.ts`) is `.buffer()`'s sibling, not its
replacement: it never cuts a chunk itself, it reads `this.chunkStream()` (never `_chunks` directly -
that skips a genuinely synchronous chain's own `_syncChunks`) and wraps whatever chunking is already
in effect (`.buffer()`'s own cut, or the `1000`-item default) with `prefetch()`
(`src/utils/cut.ts`, beside `share()`). `.buffer()` staying pull-driven is what makes it free when
unused; `.queue()` is the opt-in cost for a caller who wants the source running ahead of the
consumer.

`prefetch()` is a plain `async function*`, written to mirror `ConcurrentPipeline`'s own
`fanOutOrdered` shape (a sliding window of promises, `.catch(() => {})` attached at push time so a
promise queued deep but never reached first is still a HANDLED rejection - see `code.md`) rather
than a hand-rolled `AsyncIterable` object. That choice buys three of the ticket's own Constraints for
free, from the language's own generator semantics rather than code this package has to write and
verify itself:

- **Laziness.** An async generator's body does not run at all until its own first `.next()` call, so
  "the pump starts on the first consumer pull, not at construction" needs no `started` flag of its
  own - the generator function itself IS that flag.
- **Concurrent-caller safety, with no waiter list.** Two callers invoking `.next()` on the SAME
  generator instance "concurrently" (no `await` between the calls) does not run two overlapping
  activations of the body: the engine queues the calls and resumes the body once per call, strictly
  in order. `share()`-based fan-out (`ConcurrentPipeline.reduce()`'s own partitioning) wraps
  `prefetch()`'s own returned iterator exactly the way it wraps `_chunks`' - no extra locking, no
  planning-time FIFO waiter list, because the language already serializes the resumptions `share()`'s
  own docstring describes ("whichever consumer calls `.next()` next gets the next item").
- **Early-exit cleanup for free.** `prefetch()`'s own `try { ... } finally { await
  iterator.return?.(); }` is standard async-generator `.return()` injection - a consumer's `for
  await`/`break` (or `.first(n)`'s own early stop) propagates a `.return()` call down to `prefetch()`,
  which runs its `finally` and closes `upstream` in turn, with no manual `.return()` override needed.

Two failure shapes were found and fixed during planning, before the shipped design settled on a
plain generator - both still real constraints the generator-based design satisfies, just without the
mechanism planning assumed it would need:

- **`Promise.race` over concurrent `upstream.next()` calls buys nothing.** Proven twice, with real
  instrumented runs: issuing several `.next()` calls on ONE async generator without awaiting between
  them does not start their bodies concurrently - the generator queues and resolves them strictly in
  call order internally, so racing them returns whichever was CALLED first, not whichever's own work
  would finish first. Measured: a source with per-item delays `[300ms, 10ms, 10ms]`, three
  concurrent `.next()` calls issued at once - the 10ms item's own timer does not start until the
  300ms item's body returns (`item 1 STARTS its own 10ms delay at 301 ms`), despite being called at
  the same instant. `fanOutUnordered` (below) races real independent WORK on already-pulled chunks,
  never repeated pulls on one shared generator - that distinction is why the same shape pays off
  there and not here, and why `prefetch()` contains no `Promise.race` anywhere.
- **"The array is empty" is not "the stream is exhausted."** `prefetch()`'s own `pending` array is
  refilled synchronously, in the same tick as the shift that emptied one slot (`pull()` runs
  immediately after `pending.shift()`, before the next `yield`) - so two consumers sharing one
  `prefetch()` iterator via `share()` never observe a momentarily-empty array as a false "done."
  Verified: capacity 1 and capacity 3, two partitions, 8 items via `ConcurrentPipeline({
  maxConcurrency: 2 }).buffer(2).queue(3).reduce(...)`, both correct sums totaling 36, no deadlock, no
  starvation.

A `ReadableStream`+`CountQueuingStrategy` candidate was priced and killed during planning: Node's
`pull()` fires immediately at construction to fill `highWaterMark` rather than on first consumer pull
(5 pulls measured at 100ms idle with zero reads), and its own capacity accounting let `capacity + 1`
items through rather than an exact bound - both regressions against the settled requirement that
nothing touches the source until a terminal drains, which `.queue()` preserves like every other
`Pipeline` mechanism (`prefetch()`'s own laziness above).

`.queue()` unconditionally widens Mode to `"async"`, the same way `sourcePolicy()` forces every
dispatching class's own chain async regardless of the source's shape - a queue's own next value may
not be ready yet, so there is no conditional "stays sync" arm the way `.tap()`/`.onError()` keep one.
No new `JoinMode`/`SeedMode` plumbing was needed: `.queue()` is not stage-shaped (it never joins with
a stage's own Mode), it unconditionally overwrites the chain's Mode, so a flat `Pipeline<T, "async",
In>` return type sufficed against the real generic machinery. Re-declared on all four dispatching
classes (`ConcurrentPipeline`/`HttpPipeline`/`ClusterPipeline`/`EventEmitterPipeline`) to narrow that
return type to each one's own class, mirroring `.local()`'s own established pattern - `.buffer(fn)`'s
identical Promise-widening overload shipped this same gap unfixed in #88 (disclosed, not narrowed);
`.queue()`'s own review round closed it instead, since a SINGLE signature (not three overloads) made
the four one-liner overrides cheap.

End to end, a slow source through a faster transform runs clearly faster queued than fully serial,
from overlap alone, with the same outputs in the same order.

## Synchronous execution

`PipelineMode` (`"unset" | "sync" | "async"`) is decided by the input a chain is called with and by
its callbacks. Calling with an `Iterable` keeps the chain's Mode, an `AsyncIterable` widens it, and
one callback returning a `Promise` widens it through `.transform()`'s overloads. A synchronous chain
runs a parallel set of plain `function*` utilities (`buildSyncChunkGenerator`, `recutSyncChunks`,
`_syncChunks`), so nothing async-shaped is constructed until a stage's own function returns a
thenable; from that point every later link defers through `.then`.

`ConcurrentPipeline` and every dispatching subclass force `"async"` through `sourcePolicy()` - each
dispatches a chunk across a real boundary, whatever the caller's callbacks are.

## Error handling

Error handling sits on the function that failed, at two levels, and `.catch()` is deleted with the
`ErrorHandler`/`ChunkErrorHandler` chain that only ever served it (#78).

`Transformer.onError(fn)` is the row handler, `(item, error, ctx) => value | DROP | throw`. It is a
property of the transformer, not of a link, which is what makes it position-independent: `pipe()`
carries it forward exactly as it already carries the composed `transform` function itself.
`Transformer.runnable()` is the seam that hands it to the chain - it reads `this.rowHandler` off the
FINAL transformer and builds the `RunScope` the links read, and it is called wherever a `Transformer`
becomes runnable:

```text
Transformer.runnable()                     reads this.rowHandler off the FINAL transformer
	Pipeline.apply()                         stored into _chunkTransforms, and passed to process()
	ConcurrentPipeline.apply()               stored into _chunkTransforms
	ConcurrentPipeline.stageWork()           what HttpPipeline.fetch()'s registry lookup invokes
InternalTransformer(chunk, ctx, run?)      pipe() forwards `run` down the composed chain
	map/filter/flatMap/tap(fn)               per-row try/catch, only when run.rowHandler is set
	Transformer.reduce -> Reducer.fold       the one place a single item is folded
```

A link with no handler registered runs its existing `Promise.all` path unchanged - it matches the
pre-#78 floor within run-to-run JIT noise, so the seam costs nothing unused. A registered handler is
a little slower - noisy but consistently positive, never free. `DROP`
is a `unique symbol` and every site tests it with `!== DROP`.

`Pipeline.onError(fn)` is the run handler, `(error, ctx) => void`: returning drops the failing chunk
and the run continues, throwing stops it. It cannot be a catch on the drain side, because an async
generator that throws is finished - measured, a `Pipeline` over `["1","x","3","4"]` at `.buffer(1)`
yields `[[1]]` and then `done`, losing rows `3` and `4`, where the same failure guarded inside the
per-chunk loop yields `[1,3,4]`. So it plugs into the two per-chunk guards #40 already built:
`runSequentially`'s own try/catch for a local stage, and `ConcurrentPipeline.apply()`'s wrapped
`work` for a dispatched one, where returning `[]` IS the "drop this chunk" answer the fan-out needs.
The unit dropped is therefore the chunk; nothing smaller is in scope there.

`Pipeline.reduce()` is out of reach: it calls `foldChunkStream(fn, initial, this._chunks,
this._context)` with no `Transformer` anywhere, so only `Transformer.reduce()`'s fold gets row
recovery.

Async iteration (`for await` over a `PipelineResult`) reads the exact same drained stream every
terminal op reads (#39, #90) - there is no separate replay path any more. `.apply()` already ran `Transformer.process()` when it built `_chunks`, lazily, so `.tap()`
and `.onError()` fire identically whichever consumption path drains it. The killed "source position"
mechanism (`_rootSource`/`_sourcePositionViolations`/`inertKnobsOf`, `normalize(rootSource)`) existed
only to protect against a knob a SEPARATE replay path couldn't honor; once every consumption path
reads the one real chunk stream, there is nothing left for it to protect against.

## Observation

`.tap()` is the one observation surface, at two levels that differ only in WHERE the callback runs.
`Transformer.tap` is an ordinary `pipe()` link, so it travels with its stage: on a dispatching class
the callback executes in the worker, and the `ctx.set()` it makes never crosses back (the wire
carries `{ chunk, context }` out and `{ chunk }` back). `Pipeline.tap` is declared once on the base
as `tap(arg): this` and wraps `Transformer.tap` in `.local(build)`, which pins the callback to the
orchestrating process on every class. Its stage occupies an index like any other, and the dispatched
stages either side of it still dispatch - measured over a real loopback `HttpPipeline` with a tap
between two dispatched stages: `orchestratorSeen [2,4,6,8,10]`, two HTTP requests served (one per
dispatched stage, none for the tap), the worker's OWN identical `.tap()` call never invoked.

The index consequence follows from `.local()` carrying the built region's own `_chunkTransforms`
back (`Pipeline.local`, below): a `Pipeline.tap()` call occupies a real slot in that shared array even
though it never dispatches, so a stage placed after it gets the NEXT index along, not the one its
position in the chain alone would suggest. A second instance's own registry (`HttpPipeline.fetch`'s
`_chunkTransforms` lookup) needs the identical `.tap()` call built into it too, for its indices to
line up with the orchestrator's - a real second instance already has it, since it re-executes the
same entry module.

## The pipeline family

Where a chain's chunks run is chosen by CONSTRUCTING A CLASS, not by configuring a `Transformer`
(#17 - replaced the `ExecutionStrategy`/`.withExecutor()` seam entirely):

```text
Pipeline                one chunk at a time, in process              src/pipeline.ts
  ConcurrentPipeline      N chunks in flight; owns the fan-out         src/pipelines/concurrent.ts
    HttpPipeline            a chunk POSTed to another instance         src/pipelines/http.ts
      ClusterHttpPipeline     a chunk sent to another local process    src/pipelines/cluster.ts
                               (#17) - the ORIGINAL cluster class, kept
                               under this name for the unchanged HTTP
                               transport (#201)
    WebSocketPipeline        a chunk sent over one persistent,         src/pipelines/websocket.ts
                               multiplexed ws connection (#201) -       (entry: /websocket, #239)
                               a SIBLING of HttpPipeline: it overrides
                               stageWork()/reduceWork() the same way,
                               but never POSTs
      ClusterPipeline          a chunk sent to another local process    src/pipelines/websocket-cluster.ts
                               over that SAME ws connection (#201) -    (entry: /websocket, #239)
                               each worker binds its own unique
                               ws+unix: socket, dispatch round-robins
                               across the set (never a shared port - a
                               ws connection is persistent)
    EventEmitterPipeline    a chunk handed to Workers on an emitter    src/pipelines/eventemitter.ts
                             (#124) - a SIBLING of HttpPipeline, not a subclass: it
                             overrides stageWork() the same way, but never POSTs
```

Each level overrides ONE thing. `ConcurrentPipeline` owns the fan-out window (`fanOutOrdered`/
`fanOutUnordered`) and the default in-process `stageWork()`; `HttpPipeline` overrides `stageWork()`
alone to POST instead, adds `.fetch()`/`routePath()`/`toNodeHandler`; `ClusterHttpPipeline` adds the
worker bootstrap, wraps `stageWork()` to lazily bootstrap on first dispatch, and overrides
`routePath()` to route several pipeline definitions through one shared worker server
(`/pipeline/<i>/transform/<n>`, `<i>` a construction-order index reproduced identically by every
worker). `WebSocketPipeline` (#201) overrides `stageWork()`/`reduceWork()`/adds `serve()` the same
seam-shape, dispatching one binary frame per request over a connection memoized per `connect` target
instead of opening one per chunk; `ClusterPipeline` (#201) adds the SAME worker bootstrap pattern
`ClusterHttpPipeline` uses, over N distinct `ws+unix:` socket paths (one per worker, round-robined)
instead of one shared port - see "WebSocketPipeline - #201" below for the wire itself.
`routePath()`/the registries-resolving helper both moved to `ConcurrentPipeline` (#201 review): one
canonical implementation `HttpPipeline`/`WebSocketPipeline` inherit and `ClusterHttpPipeline`/
`ClusterPipeline` each override the same way, rather than two independently maintained copies.
`.local(build)` (#61) is the one way to keep a whole region in-process: it builds a bare `Pipeline`
over `this._chunks`/`this._context` (never `this.constructor` - the region must never be able to
dispatch, whatever class called it), runs `build` against that bare pipeline, and carries the built
region's `_chunks`/`_context`/`_chunkTransforms`/`_reduceStages` back through `this.createPipeline()`
- the SAME seam every other copy-on-write method uses to resume the caller's own class. Each
dispatching subclass re-declares `local()` to narrow its return type only
(`~/.claude/rules/typescript.md`); the body is an unchanged `super.local(build)` call at every
level, needing no per-level code - the base implementation is already correct everywhere because a
bare `Pipeline`'s own `.transform()`/`.reduce()` never fan out or POST. `.local()` runs the async
engine, skips only the round trip: the bare `Pipeline` it builds still pays chunk/generator cost,
never 0, so the speedup a pinned region buys is the removed dispatch (a `stageWork()` call, an HTTP
round trip, a cross-process hop), not the framework itself.

`ordered: true`'s own reorder buffer (`fanOutOrdered`'s sliding window, above) holds at most
`maxConcurrency - 1` resolved-but-unyielded chunks - a small, bounded amount at the default
`maxConcurrency: 4` (#180's own Done-when 11). On `bench/memory.ts`'s `heldAtEndMB` axis (the mid-run
peak, not `retainedMb`'s post-two-forced-GC leak reading - a reorder buffer releases everything it
ever held well before a run ends, so `retainedMb` reads near-zero regardless), `ordered:false` holds
a little MORE than `ordered:true` on a chain built to trigger holding, the opposite of the naive
prediction. A discriminating check (re-run at `maxConcurrency: 64`, raising the buffer's own bound)
found no consistent gap-vs-window-size correlation - the buffer's own real footprint is below
`heldAtEndMB`'s resolution on this chain; the gap is something else, unverified further (`fanOutUnordered`'s own `Promise.race()`-based bookkeeping,
a `Map<number, Promise>` re-raced on every settle, is the untested candidate).

That async-engine cost is essentially GONE, and the reason it survived so long is a diagnosis this
document had wrong. It read: reducible, though not eliminable while `sourcePolicy()` still pins Mode
to `"async"`. Measured, the forced Mode costs nothing on its own - `ConcurrentPipeline` with zero
stages, no `.buffer()` and no region reads the same as with `.local((p) => p)` added, within
run-to-run noise. The cost was never the Mode; it was four ordinary things the
async engine did per ROW for data that arrives per CHUNK (#179):

1. Every async terminal flattened the chunk stream back to items, paying one `await` per row to
   re-derive what the chunk view already held. All five now walk `chunks()` with a synchronous inner
   loop, and the item view is deleted for having no reader left.
2. `fromSource()` turned an ARRAY into an async iterator item by item, paying `toAsyncIterable`'s own
   `Promise.resolve` per pull and then `buildChunkGenerator`'s `for await` on top. An array is now
   cut with `slice` and handed over whole chunks.
3. `.buffer(size)` folded every item through `Reducer<T[], T>` - a per-item closure call and an
   array-mutating accumulator - for a cut that only counts. It now cuts by count on all three arms;
   `.buffer(fn)` keeps the fold engine, which is what a per-item decision needs.
4. `stageWork()` called the global `fetch` once per chunk, where `node:http` with a keep-alive agent
   is several times cheaper. `options.client` is the seam, and `node:http` the default on Node.

On `bench/overhead.ts`, every dispatching class's pinned row fell by more than an order of
magnitude, to within a little of a bare `Pipeline` running the same region - which is what pinning
a region always claimed to mean. `bench/memory.ts` measures the same chains for allocation:
`Concurrent .local() region` now allocates over an order of magnitude less, and collects far less
often.

`HttpPipeline`'s own DISPATCHED cost (`stageWork()`'s full round trip through `options.client`)
splits three ways by REMOVING each part on the real dispatched path (#180, four real A/B runs on a
loopback server, never isolated micro-timing - `.claude/rules/code.md`'s own rule): the round trip
itself is most of the total and is the DOMINANT cost, real socket I/O over `node:http`'s own
keep-alive `Agent`, already this fix's own target. Encode and decode (JSON `stringify`/`parse` on
both sides) together fit inside the clearly smaller remainder - removing either
ALONE changes the dispatch's own concurrency/queuing shape enough to swamp the measurement, so the
two do not decompose into separate numbers on this path (a real A/B delta does not always compose by
subtraction - `code.md`'s own 2026-09-12 entry). No fix lands: the round trip IS the boundary, already
optimized by this section's own point 4, and JSON's own wire format is #172's finding, not
re-litigated here.

`fromSource()`'s own async-generator branch (point 2 above is the ARRAY-forced-async branch; a
GENUINE async generator source keeps the general path, `toAsyncIterable` + `buildChunkGenerator`'s
`for await`) pays a few promises per row - `collectItems()`'s own docstring already named this "the
source's own floor" (#179). #180 confirms it is truly unreachable, not merely unoptimized: a
hand-rolled `.next()`-based consumer of the SAME generator, bypassing `for await`'s own sugar
entirely, creates the same number of promises as a plain `for await` drain - no daylight between
them - while today's real `fromSource()` path sits a negligible fraction above that floor already. The cost is the async generator PROTOCOL's own resumption machinery, paid once per
`.next()` call regardless of who calls it; no consumer shape, hand-rolled or otherwise, reaches below
it. No fix lands, recorded as why rather than attempted: `fromSource()` is already within noise of
the language's own floor.

Two mechanics make it work. `Pipeline`'s copy-on-write methods construct via a `protected
createPipeline<U, R = AnyPipeline<U>>(chunks, options)` that calls `this.constructor` rather than a
hard-coded `new Pipeline<U>`, so a subclass survives a `.transform()`/`.context()`/`.buffer()`
chain. Its own `R` type parameter is what lets a DISPATCHING SUBCLASS's own two-argument call get
back its OWN narrower type with no trailing `as X` cast -
`this.createPipeline<U, ConcurrentPipeline<U, In>>(...)` in `ConcurrentPipeline.apply()`/`.reduce()`
(#133); `defer<U, R = AnyPipeline<U>>()` carries the identical pattern for the source-less path. The
base `Pipeline`'s OWN copy-on-write methods still call the one-argument form and still cast
`as this` (`.context()`/`.onError()`/`.buffer()`'s three branches), since `R`'s default
(`AnyPipeline<U>`) cannot narrow to `this` without a second argument only a subclass site actually
supplies.

`createPipeline()` itself is declared ONCE, on the base, and is never overridden again (#133,
replacing a `createPipeline()` override at every level). It merges its `options` argument with
`this.carriedKnobs()`, and each subclass overrides ONLY `carriedKnobs()` to add its own extra
fields:

```ts
// pipeline.ts (base) - no super to spread
protected carriedKnobs(): object {
  return {};
}
// concurrent.ts - FLAT, since super is the base's empty {}
protected override carriedKnobs(): ConcurrentPipelineOptions {
  return { maxConcurrency: this.maxConcurrency, ordered: this.ordered };
}
// http.ts / cluster.ts / eventemitter.ts / websocket.ts - each spreads its super, adds its own fields
protected override carriedKnobs(): HttpPipelineOptions {
  return { ...super.carriedKnobs(), url: this._url };
}
```

`chunkSize` never appears here at all
(#39), since `.buffer()` is `Pipeline`'s own knob now, not a constructor option. The `options`
argument `createPipeline()` merges `carriedKnobs()` on top of is `this.carriedOptions()` (pre-#133,
unchanged) - the FULL `PipelineState`, declared apart from the exported `PipelineOptions` (#90): a
caller writes `context`/`contextFactory`, and every carried knob is named once in `carriedOptions()`
rather than field by field at each call site, which is what stops one being dropped, as `mode` and
then `bound` each silently were. `carriedKnobs()` is the narrower, #133-introduced sibling: only the
handful of fields a DISPATCHING subclass alone adds (never `mode`/`bound`/`context`, which
`carriedOptions()` already owns). And a
stage's identity is its INDEX in `_chunkTransforms` - the table `apply()` already maintains - so a
dispatching class sends a chunk plus an index, never a function. Every instance runs the same code,
so index N means the same transform on both sides; a mixed-version fleet breaks that assumption
silently, which is why atomic deploys are a documented requirement rather than a check.

`ConcurrentPipeline.apply()` does NOT call `transformer.process()` for a non-local stage - that
bypass IS the mechanism, since `process()` runs a chain sequentially, one chunk at a time. It fans
`this._chunks` - the pipeline's OWN already-cut chunk stream, set by `.buffer()` (#39) - out through
`stageWork()`, and `fanOutOrdered`/`fanOutUnordered` (`concurrent.ts`) yield each dispatched chunk's
own RESULT ARRAY rather than flattening it: the fanned-out output IS itself a real `_chunks`
boundary, so a later `.buffer()` recuts from it exactly like any other stage's output. `apply()`
wraps `stageWork()`'s own work in a try/catch of its own (#40) - `Transformer.process()` never runs
on this path, so this wrapper is the one place a dispatched stage's failing chunk is still in scope,
and #78 makes it the site `Pipeline.onError()` plugs into, returning `[]` to drop the chunk. No
`Transformer` knob is inert on a dispatched stage any more - #40 already moved `.onError()`'s own
notification off `process()` entirely, and #72 deletes the last one that only ever took effect there
(the old lifecycle-hooks knob) along with `dispatchKnobViolations`, the refusal that used to guard
it - so a caller who needs a stage kept in-process reaches for `.local(build)` (#61) by choice, not
because leaving it dispatched would silently do nothing. `.buffer()` reaches a dispatched stage
exactly like a local one, since `Pipeline` owns the cut, not `Transformer` - the refusal this used to
need for a custom chunker (`setChunker`, deleted with `Transformer`'s own chunking fields) has
nothing left to refuse.

`ConcurrentPipeline` bounds CHUNKS, not items: `maxConcurrency` chunks are in flight and every item
inside a chunk runs together, so a chain's items in flight is the buffer size times
`maxConcurrency`. Measured peak simultaneous callbacks on the current class, N=5000 (`10x10` 100,
`50x4` 200, `100x3` 300, `1000x1` 1000, `7x11` 77) and N=20000 (`1000x3` 3000) - the product exactly,
coprime factors included.

How that product is SPLIT is a throughput choice, not a parallelism one. Two pairs reaching the same
16 in flight over microtask-only work: `.buffer(16)` with `maxConcurrency: 1` runs roughly twice as
fast as `.buffer(1)` with `maxConcurrency: 16`, because a chunk pays the per-chunk cost once where
`.buffer(1)` pays it per item. The gap closes when the callback dominates - over a workload that
waits on every item, the same pair runs level. Prefer the widest chunk that fits the
in-flight budget.

## WebSocketPipeline - #201

Each chunk of a stage dispatched over one persistent, multiplexed WebSocket connection instead of
one HTTP request per chunk (`HttpPipeline`) - `#180`'s own finding put most of `HttpPipeline`'s
dispatched cost in HTTP's own request-line/header parsing on an already-open socket, and a
multiplexed connection pays that once per PROCESS LIFETIME rather than once per chunk. A planning
spike (`#201`'s own first ticket comment) measured `ws+unix:` (WebSocket over a Unix domain socket)
clearly cheaper than a minimal raw-HTTP/TCP floor, and one multiplexed connection a little faster
than a pool of four dedicated ones - both findings the shipped design follows.

The wire, one BINARY frame per dispatch (`src/pipelines/websocket.ts`'s own `encodeFrame`/
`decodeFrame`): a 4-byte big-endian header-length prefix, the JSON header, then the
`codec`-encoded payload:

```text
-> { id: 0, route: "/transform/0", context: { multiplier: 10 } } + codec.encode([1, 2])
<- { id: 0 }                                                     + codec.encode([2, 4])
```

`route` carries what a URL path carried before - `/transform/<n>`, `/reduce/<n>`,
`/branch/<i>/<name>/transform/<n>` - unchanged trail, parsed by the one `parseRoute` in
`src/pipelines/concurrent.ts`, which `HttpPipeline` uses for a URL pathname and this class for a
bare JSON field. A failure is a separate TEXT frame, always fixed JSON
regardless of `codec` - the WS opcode itself the discriminator: `{"id":0,"error":"…"}`.

`getConnection(connect)` memoizes ONE `ClientConnection` per `connect` string, module-level, shared
by every `WebSocketPipeline` dispatching to the same target - the spike's own "fewer sockets beats a
pool" finding. Every `stageWork()`/`reduceWork()` dispatch correlates its own request/response by an
`id` over that one shared socket; a closed or errored connection rejects every request still pending
on it and evicts itself from the cache, so the next dispatch to that target dials fresh.

⚠ `WebSocketPipeline.stageWork()`/`reduceWork()` capture their own dispatch target ONCE, into a local
`connectTarget`, rather than re-reading `this._connect` later in the same closure - a mutable shared
field read back after an `await` is exactly the shape `ClusterPipeline`'s own round-robin race
(below, `resolveConnect()`) went on to hit one level up, on `_connect` itself rather than on
`connectTarget`'s read of it. Found live: a `workers: 2, maxConcurrency: 2` reduce fixture failed
with "connection closed before dispatch" on a connection that had never closed, because a LATER
dispatch's own reassignment of `this._connect` landed between an EARLIER one's `await`s. Superseded
below: `resolveConnect()` removed the shared field this capture was ever protecting against, so
`connectTarget` today is derived from `resolveConnect()`'s own return, never a re-read of
`this._connect` at all.

A reduce stage shares the connection like any other stage, correlated by the SAME `id` across every
frame of its own stream: each upstream chunk is its own outgoing frame (`route`/`context` repeated
on every frame - simpler than tracking "have I sent this id's context yet" server-side, and cheap
next to a chunk's own payload), an `inputDone: true` frame (empty payload) signals no more chunks are
coming, and the server's own `done: true` frame (empty payload) closes the id after its trailing
`Reducer.final()` value, if any, has already been sent - the same `Reducer`/`foldChunk` engine
`HttpPipeline`'s own `runReduceStage` folds through (`src/utils/reduce.ts`). Unpriced, named so it is
not mistaken for load-bearing: this wire is NOT pull-driven the way the HTTP `ReadableStream` wire
is - a partition's own chunk frames go out as fast as `chunks` yields them, so the fastest of
`ConcurrentPipeline.reduce()`'s `share()`d partitions could in principle race ahead of a slow socket.

`serve(socket)` is the SERVER side, the role `.fetch()` plays for HTTP - `receiveFrame(socket, data)`
is its own body, exposed separately so `ClusterPipeline`'s own shared worker server can peek a
frame's `/pipeline/<i>/` prefix (`peekFrame()`, reading only `id`/`route` without decoding the
payload) and hand the SAME raw bytes to the RIGHT registered pipeline, mirroring `.fetch()`'s role
for `ClusterHttpPipeline`. ⚠ Every incoming frame is queued per `id` (`frameQueues`, a
`Map<number, Promise<void>>`) rather than dispatched concurrently - a reduce stream's own chunk frame
and its `inputDone` frame arrive back to back over the wire, and `inputDone`'s path to
`Reducer.final()` is shorter than a chunk's path to `foldChunk()`, so unordered dispatch let the
trailing flush run BEFORE the chunk it was meant to flush had folded (found live: `[1,2,3,4,5]`
summed to `[]` instead of `[15]`). `frameQueues` is cleared on every path an id's session can end -
`inputDone` reached, or the frame itself failing (an unknown route/stage, a decode error) - not only
the happy one, or a failed reduce chunk leaked its entry for the worker's whole life.

`toNodeWebSocketHandler(pipeline)` bridges `ws`'s own `WebSocketServer({ noServer: true })`/
`handleUpgrade` to `PipelineSocket` for Node, the same gap `toNodeHandler` bridges for `.fetch()`.
`NodeWebSocketHandler.upgrade(request: IncomingMessage, socket: Duplex, head: Buffer)` is typed with
`node:http` and `node:stream` type-only imports, never with `Parameters<...handleUpgrade>` off
`ws` (#239): a `ws`-derived type leaves an import of `ws` in the published `.d.ts`, so a consumer
would need `@types/ws`. The file therefore joins `.oxlintrc.json`'s `node:` import exceptions. `ws`'s own `ws+unix:` URL scheme splits its whole path on
the FIRST `:` (verified against `ws` 8.21.3's own `initAsClient`, `lib/websocket.js`) -
`ws+unix:/tmp/w.sock:/`, no leading `//`; the URL-with-authority shape every OTHER scheme here uses
(`ws+unix:///tmp/w.sock:/`) dials the wrong path, an empty authority segment `ws` does not strip.

`ClusterPipeline` (#201, in `websocket-cluster.ts`) reparents onto `WebSocketPipeline`;
`ClusterHttpPipeline` is its HTTP/TCP counterpart. `WsWorkerSet` mirrors `WorkerSet`'s shape one seam apart: each worker
binds its own UNIQUE `ws+unix:` socket path (never a shared port, the way HTTP's `listen(0)` shares
one across every worker) - a WebSocket connection is persistent, so sharing one target across workers
would mean only one worker is ever dialed, and `#201`'s own Done-when 3 needs one distinct connection
per worker to count. Each worker computes its own path from its own `process.pid` (unique, no
coordination needed) and reports it back over `cluster.fork()`'s IPC channel; `enter()` round-robins
across the bootstrapped set instead of handing back the single shared value `WorkerSet.enter()` does.
⚠ `WorkerSet.kill()` and `WsWorkerSet.kill()` both iterate `cluster.workers`, a registry `node:cluster`
shares PROCESS-WIDE - before #201 review only one `WorkerSet` ever existed per process, so this never
mattered; with two sibling classes now forking into the same shared registry, one class's idle timer
could kill the OTHER's still-in-flight workers. Both now track `ownWorkerIds` and kill only their own.

⚠ `ClusterPipeline.resolveConnect()` is the ONE override on the class - `bootstrapAndSetConnect()`/
`stageWork()`/`reduceWork()` overrides that used to wrap the round-robin around a SHARED
`this._connect` field are deleted entirely. That field-based design raced under
`maxConcurrency > 1`: `ConcurrentPipeline.reduce()` launches every partition in ONE synchronous burst
(`Array.from({length}, () => work(...))`), so every partition's own round-robin write landed on
`this._connect` before any partition read it back - all of them ended up dispatching to whichever
worker the LAST write picked. Found live: a `maxConcurrency: 2` reduce read `totalConnections: 1`,
not 2 (`websocket-cluster-reduce.ts`'s own regression case, now asserted in
`websocket-pipeline.e2e.test.ts`'s Done-when 4 test). `resolveConnect()` is called fresh by each
dispatch (`wsWorkerSet.enter(this.workers)`) with nothing shared to race on - the base
`WebSocketPipeline.resolveConnect()` still reads `this._connect` unchanged, single-target, for every
class that never overrides it. `WsWorkerSet.enter()`'s own round-robin index increments
synchronously right after its `await bootstrap()`, with no further `await` before the increment -
concurrent callers queue on that one `await` in registration order, so each gets a DISTINCT index
even when several `enter()` calls land in the same synchronous burst.

`WsWorkerSet.startWorkerServer()`'s connection counter (queried by `#201`'s own Done-when 3 IPC
channel) is a `Set<PipelineSocket>` sized on query, not an incrementing total - a plain counter with
no decrement read a transient reconnect on one worker as two open connections; `onClose` deletes the
socket from the set, so `.size` always reads what is connected NOW.

`pnpm bench:overhead`'s `ClusterPipeline` row runs roughly three times faster over WebSocket than
it did HTTP-based (pre-#201), beating the spike's own composed estimate. `bench/baseline.json`
itself stays the pre-#201 number (`bench/*.ts` is outside this ticket's own file
scope, Done-when 8) - `checkGate`'s own regression-only design never flags a speedup, so the gate
stays green with a now-stale ceiling; a future ticket updating the baseline for real would tighten
it, not loosen anything.

## Codec and encoded chunks - #209

A dispatched WebSocket reply stays encoded in the orchestrating process until a site reads its
items. `Codec` (the interface) and `JsonCodec` (the default class) live in `src/codec.ts`, outside
the file that loads `ws`, so a core chunk type can name `Codec` without a utils-to-pipelines import.
`Codec` is not generic: one instance serves every stage while the item type changes, so it sees
`unknown`, and `Pipeline<T>` carries the type hints. The `jsonCodec` object is deleted (BREAKING).

## Package entries

Five entries, so the root loads no package AND resolves no Node builtin: `@outputty/pipeline`
(`src/index.ts`), `@outputty/pipeline/websocket` (`src/websocket.ts`, needs `ws`), and
`@outputty/pipeline/http` / `/cluster` / `/eventemitter` (needs Node builtins), each with `import`,
`require` and `types` conditions in `package.json`'s `exports`.

```text
@outputty/pipeline              Pipeline, ConcurrentPipeline, Transformer, Codec, JsonCodec ...
  src/index.ts                                                                     no package, no node:*
@outputty/pipeline/websocket    WebSocketPipeline, ClusterPipeline, toNodeWebSocketHandler,
  src/websocket.ts                PipelineSocket, NodeWebSocketHandler + options types      needs ws
    src/pipelines/websocket.ts        the ONE file that imports ws
    src/pipelines/websocket-cluster.ts ClusterPipeline; must not import cluster.ts
@outputty/pipeline/http         HttpPipeline, toNodeHandler, PipelineClient, fetchClient,
  src/http.ts                     defaultClient + options type                    needs node:http/stream
@outputty/pipeline/cluster      ClusterHttpPipeline + options type                needs node:cluster/http/os
  src/cluster.ts                   extends HttpPipeline - pulls http.ts's module graph in too
@outputty/pipeline/eventemitter EventEmitterPipeline, PipelineEmitter, WorkEvent + options type
  src/eventemitter.ts              needs node:events - independent leaf, no cross-import either way
```

`#249`'s own split (`.claude/CLAUDE.md`'s Language, `http`/`cluster`/`eventemitter` entries): a
bundler resolves every static import before it can tree-shake unused exports, so `cluster.ts`'s
`node:cluster`/`node:http`/`node:os`, `http.ts`'s `node:stream` and `eventemitter.ts`'s `node:events`
each abort a browser build even for a consumer who never imports the class that needs them. Three
entries, not one, because the three files' only shared trait is "needs a Node builtin" - `client.ts`
feeds `http.ts` feeds `cluster.ts` (no cycle), `eventemitter.ts` is unrelated to either. No source
file moves; each new entry is a barrel re-exporting from its unmoved `pipelines/*.ts` file, the same
shape `websocket.ts` already uses.

Measured: `dist/index.js` shrank by nearly two orders of magnitude, and `grep -c 'from "cluster"\|from "http"\|
from "os"\|from "stream"\|from "events"' dist/index.js` (and its shared ESM/CJS chunks) reads `0`.
`packaging.e2e.test.ts` bundles `import { Pipeline, Transformer } from "@outputty/pipeline"` with
esbuild at `platform: "browser"` against the real built `dist/` and asserts it succeeds - the same
class of check that fails today against the unfixed root (a real Turbopack build: `Module not found:
Can't resolve 'cluster'`).

- **Root is package-free.** `ws` is the only package `src/` imports (`rg 'from "ws"' src` hits
  `pipelines/websocket.ts` alone), and `packaging.e2e.test.ts` runs the built `dist` in a directory
  with no `ws`, greps every root bundle, chunk and `.d.ts` for it, and typechecks a strict consumer
  with neither `ws` nor `@types/ws`.
- **`cluster.ts` and `websocket-cluster.ts` share nothing but `IDLE_KILL_MS`** (`src/types.ts`).
  `cluster.ts` starts the HTTP worker server at module scope, so importing it from the WebSocket side
  would start that server in every `/websocket` worker.
- **Both entries share one class copy.** ESM splits chunks by default; CJS needs `splitting: true`
  (Constraints in dependencies).

```text
BEFORE  stage 0 reply -> codec.decode (primary) -> rows -> codec.encode (primary) -> stage 1
AFTER   stage 0 reply -> encoded chunk { payload, rows, codec } ------------------> stage 1
                                      \-> materialize() only where items are read
```

- `WebSocketPipeline.stageWork()` resolves an encoded chunk; the reply header carries `rows`. A
  reduce emit frame carries `rows` too, and `reduceWork()` yields an encoded chunk.
- A later dispatched transform or reduce sends the payload verbatim when the codec is the same one.
- Three sites decode: `drainable()` (every item-returning terminal and `.branch()`), the `.local()`
  seed (which covers `Pipeline.tap`) and `flattenChunks` (which covers a `.buffer()` recut).
  `.consume()` decodes nothing.
- `ConcurrentPipeline`'s fan-out skips a chunk with 0 rows before dispatch, and `reduceWork()`'s own
  pump loop skips one too before sending it to a dispatched `.reduce()` - both scoped to
  `isEncodedChunk`, so a real, merely-empty chunk on `HttpPipeline`/`EventEmitterPipeline` still
  dispatches unchanged. The source cut (`cut.ts`) and the terminal (`result.ts`) already drop empties.
- The encoded chunk is internal and travels typed `T[]`. A site that reads items without decoding
  returns `[]` without an error, so each decoding site keeps its own e2e case.
- `Pipeline.mayCarryEncodedChunks()` is the structural gate `.local()`'s seed and `drainable()` read
  before wrapping a chunk stream in the materializing generator - `false` on the base, `true` only on
  `WebSocketPipeline`. Wrapping unconditionally regressed `bench:memory` on every non-WebSocket chain
  (one Promise per chunk for a mechanism it could never carry); this class-level override closes that
  with no runtime flag.
- ⚠ A `codec.decode()` failure no longer rejects at the dispatching stage: the primary keeps the
  reply encoded and only decodes at whichever site reads items first (`drainable()`, the `.local()`
  seed, `flattenChunks`), all outside `runStageChunk`'s own try/catch. `Pipeline.onError()`'s
  documented per-chunk drop-and-continue contract is not consulted for a decode failure - it throws
  out of the terminal (or `.local()` region) instead. `.consume()` never decodes at all, so a bad
  chunk there completes silently with nothing to report.

Measured (`__tests__/codec.e2e.test.ts`'s own Done-when cases, `WebSocketPipeline`, `[1..8]`,
`.buffer(1)`, two dispatched stages): the primary's decode/encode count fell from 16/16 to 8/8 with
the JSON codec, output unchanged; an emptied chunk cut the server's own decode count from 16 to 8; a
dispatched reduce decoded only at the terminal `.toArray()`. `.tap()`, `.local()`, a recut and
`.branch()` between stages kept their base counts, since each reads rows - matching planning's own
spike 2 findings exactly.

The package ships no storage codec. A caller's by-reference codec owns its store, its keys and its
cleanup; a dispatch that fails before decode leaves that codec's stored object unread.

## EventEmitterPipeline - #124

`stageWork()` is the only DISPATCH override, the same seam `HttpPipeline` overrides to POST -
`apply()`'s own fan-out (`fanOutOrdered`/`fanOutUnordered`, `maxConcurrency`, `ordered`) is
inherited UNCHANGED, and stays that way: no pool, no round-robin, no readiness tracking of its
own. `apply()`/`local()`/`transform()`/`reduce()` are each re-declared only to narrow the static
return type back to `EventEmitterPipeline<U, In>`, the same shape `HttpPipeline` uses - `.reduce()`
dispatch stays exactly `ConcurrentPipeline`'s own (folds in-process, no emitter involvement), left
that way by decision (`#124`'s own Settle first). A free-slot-dealing pool design (`share()`,
`src/utils/chunk.ts:454`, the mechanism `ConcurrentPipeline.reduce()` already uses to partition)
was built and measured working during planning, then killed by the user's own simplification
request - "not even think about concurrency at this stage" - not by a defect
(`.claude/roadmap.md`, Killed).

```text
stageWork(transformer, stageIndex)
  route = this.routePath("transform", stageIndex)      /transform/<n>, or an arm's own trail
  returns (chunk, ctx) => new Promise((resolve, reject) => {
    emitSafely(`${route}:dispatched`)                 <- every lifecycle emit is emitSafely, no exceptions
    try: Promise.resolve(transformer.runnable()(chunk, ctx)).then(respond, doReject)
    catch (sync throw): doReject(error)                <- the composed function, called DIRECTLY, never registered
    for each fn in emitter.listeners(route):          <- only a caller's OWN Workers live here
      try: Promise.resolve(fn({chunk, ctx, respond, reject})).catch(doReject)
      catch (sync throw): doReject(error)             <- one Worker's throw never skips the rest
  settle(outcome):  resolve/reject first, THEN emitSafely(`${route}:done` | ":error")
    respond(value) = settle({ok:true, value}); doReject(error) = settle({ok:false, error})
```

Dispatch calls the composed function and every `emitter.listeners(route)` entry directly, inside
its own `try` - never `emitter.emit()`, which cannot catch a Worker's throw after its own `await`
(an unhandled rejection Node/Bun may treat as fatal; measured, isolated with the composed function
removed: a plain `emit()`-based dispatch left the request permanently pending while the process
still crashed on the side). Both run on every chunk - broadcast, deliberately uncontrolled - and
the first one to SETTLE, the composed function's own resolve/reject or a Worker's
`respond()`/`reject()`, decides the chunk: measured, a Worker rejecting at 5ms beat the composed
function resolving at 30ms even though the composed function's own dispatch started first, so
"first to settle" is the real contract, not "first to start." The per-contender `try` (review-caught,
first cut lacked it) is what stops one SYNCHRONOUS throw aborting the whole loop before every
contender registered after it gets its turn - verified live: a throwing Worker registered ahead of
a correct one still leaves the correct one's own body run, even though the throw settles the
dispatch first. `settle()` (both `respond()`/`doReject()` narrow to it - one guard, not two,
review-caught: the first cut hand-rolled the same `if (settled) return; settled = true;` guard
twice) settles the REAL `Promise` (`resolve`/`reject`) BEFORE emitting its own lifecycle event,
through `emitSafely()` - EVERY lifecycle emit in this class goes through it, `:dispatched`
included, not only `:done`/`:error` (review-caught: the first cut left `:dispatched` as a raw
`emitter.emit()` call, so a throwing `:dispatched` listener synchronously rejected the whole
dispatch Promise as if it were a Worker's own failure, silently absorbed by `.onError()` -
measured: `out` came back `[]` with no error surfaced anywhere). A `:done`/`:error` listener that
itself throws would otherwise fire inside a `.then()` callback with no downstream `.catch()`, and
since that throw would happen BEFORE the real settle, the dispatch could hang forever rather than
merely leak an unhandled rejection (review-caught, verified live: a throwing `/transform/0:done`
listener produced zero unhandled rejections and no hang, surfacing instead as its own separate
`uncaughtException` on the next microtask via `emitSafely`'s `queueMicrotask`). Since the composed
function is never itself a listener, `#221`'s own no-worker-registered case never arises: the
composed function always answers, so there is no "no worker registered" rejection left to raise.

`emitSafely()` itself (shared by every lifecycle emit in this class) is a plain
`try { emitter.emit(event, payload) } catch { queueMicrotask(() => { throw error }) }` - the
simplest event-emitter shape available: one real `.emit()` call, so a caller-supplied emitter's own
`.emit()` override and Node's own `.once()` unwrap machinery both still run exactly as documented,
and every listener still SEES the event through the ordinary `EventEmitter` contract. ⚠ Two
consequences of staying this simple, both deliberate, neither fixed: a listener that throws
SYNCHRONOUSLY stops `.emit()`'s own internal loop, so a sibling listener registered AFTER it on the
SAME lifecycle event silently never runs for that dispatch - ordinary `EventEmitter` behavior a
caller registering two listeners on one event is expected to already know, not a guarantee this
class makes about listener isolation. A listener that throws ASYNCHRONOUSLY, after its own `await`,
leaks as a real `unhandledRejection` instead of surfacing through `emitSafely`'s own `queueMicrotask`
rethrow at all - `.emit()` never awaits a listener's return value, so nothing here can attach a
`.catch()` to it without abandoning plain `.emit()` for manual listener invocation, priced and
rejected as not worth the cost (a custom emitter's own `.emit()` bypassed entirely, `.once()` broken
for every event this class touches, not only the Worker channel). ⚠ A LOSING Worker's own failure on
a multi-Worker route is discarded with no trace once another contender has already settled -
`settle()`'s `if (settled) return` guard means no `:error` emit, no log, nothing observable anywhere
for it. This matches "first to settle wins" for the WINNER; nothing catches a bug in a Worker that
merely lost the race.

`apply()` is overridden a second time, wrapping the stage's own output chunk stream so
`<route>:end` fires once, after every chunk that stage's fan-out produced has been yielded from
THIS WRAPPED STREAM - the stage index it wraps under is read OFF THE RESULT
(`dispatched._chunkTransforms.length - 1`, the slot `super.apply()` just appended), never
independently re-derived, so it can never drift from `ConcurrentPipeline.apply()`'s own internal
computation, and `dispatched.routePath(...)` (not `this.routePath(...)`) is what makes the route
carry an arm's own trail rather than the parent's when `dispatched` is an arm's pipeline. Both the
wrapped generator's `onEnd` here and `drainable()`'s own `fireOnce` (below) call `emitSafely()`,
never a raw `emitter.emit()` - the callback runs inside the stream's own `finally` block, and JS's
finally-overrides-exception semantics mean an unguarded throw there would REPLACE whatever real
stream error was already propagating with the observer's own unrelated one (review-caught,
measured: a chunk that genuinely fails combined with a throwing `/transform/0:end` listener
rejected with the OBSERVER's error, not the real one, before this fix). ⚠ Under
`maxConcurrency > 1` with an early terminal (`.first(n)`), `<route>:end` can fire BEFORE some of
that same stage's own `:done`/`:error` events: an early return stops PULLING from the wrapped
stream, but a chunk already dispatched into `ConcurrentPipeline`'s own fan-out keeps running in the
background and settles independently of when the consumer stopped reading - measured, `maxConcurrency:
4` with a 50ms map over four items and `.first(1)`: event order `[done, end, done, done, done]`,
three more `:done`s after `:end`. Treat `:end` as "no more chunks will be YIELDED here", never as
"every in-flight Worker for this stage has finished."

`drainable()` (`src/pipeline.ts:1292`, the one seam every terminal calls) is overridden a third
time, wrapping whichever of `items()`/`chunks()` a terminal actually calls so the TRAIL's own
`:end` fires once the wrapped stream is exhausted - `this._routeTrail` is `""` for a chain (so the
event reads bare `:end`) and `/branch/<i>/<name>` for an arm - once per TERMINAL CALL, matching
`PipelineResult`'s own "every terminal re-drains" contract: calling `.first()` then `.toArray()` on
the same result fires it twice.

`Pipeline.onError()` (the run handler) reaches a rejecting Worker for free, through
`ConcurrentPipeline.apply()`'s existing wrapped `work` - no explicit `dropOrRethrow()` call is
needed in this class's own code, unlike the killed pool design, which bypassed that machinery
entirely and had to call it explicitly.

The constructor mirrors `HttpPipeline`'s own two-overload shape (`Pipeline.wrapping()`, `(pipeline,
options)` wraps a chain built elsewhere, `(options)` builds standalone), and validates a
caller-supplied `options.emitter` against `PipelineEmitter`'s five methods on EVERY construction
(`#221`) - a trust-boundary value, so a missing method fails loud there rather than as a generic
`TypeError` deep inside `stageWork()`'s dispatch closure later. A `.transform()`/`.buffer()`/
`.context()` call re-validates the identical, unchanged `emitter` object every time, which costs
nothing measurable for a real one and is what lets a stage composed after a bad emitter also fail
loud, rather than only the chain's very first construction.

One emitter, any number of chains built on it, none of them sharing anything to race on (`#221`):

- Two INDEPENDENTLY-CONSTRUCTED `EventEmitterPipeline`s sharing one caller-supplied `emitter`
  option each call their OWN composed function directly on dispatch - neither registers anything
  on the shared emitter, so there is nothing for the two to race on any more. `#124`'s own defect
  here (both composed functions registering as the shared emitter's first stage's own listener,
  then racing on every dispatch) has no mechanism left to reproduce it.
- Two chains FORKED from the SAME unbound instance - two `.transform()` calls off one shared base,
  or two `.branch()` arms (`Pipeline.branch()`'s own `emptyOfOwnClass()` resets the arm's
  `_chunkTransforms` to `[]`, so its first stage is index 0 again) - each closes over its OWN
  composed function inside its OWN `stageWork()` call; `#124`'s own defect here (the second fork's
  own first stage already marked registered on a shared `Set`, so its dispatch silently reused the
  first fork's Worker) has no `Set` left to share.
- A `.branch()` arm's own trail (`/branch/<i>/<name>`, carried into `routePath()` via
  `emptyOfOwnClass()`'s `routeTrail` option) distinguishes its routes from the parent's and from a
  sibling arm's even where a Worker IS registered externally: `/branch/0/evens/transform/0` and
  `/branch/0/odds/transform/0` name different channels, where `#124`'s own flat, position-only
  naming named the same one for both.

`.once(eventName, fn)` is not supported as "handle exactly one chunk": dispatch reads
`emitter.listeners(eventName)` and invokes each function directly (the reason above - `emit()`
cannot catch a throw after `await`), so Node's own once-unwrap machinery, which lives INSIDE
`EventEmitter.emit()`, never runs - measured, a Worker registered via `.once()` still fired on a
SECOND, later chunk, `listenerCount` unchanged after both calls. The composed function is never a
listener, so it never appears in this count at all - `emitter.eventNames()` reads `[]` for a chain
with no caller-registered Worker of its own.

## The chain and the run - `Pipeline` and `PipelineResult` (#90)

A `Pipeline` declares the type it ACCEPTS, holds no data, and IS the function you call. Calling one
returns a `PipelineResult`, which is where every drain lives. The split is what makes draining
without an input a compile error rather than a call resolving to `[]`, and what lets one chain serve
any number of inputs.

```text
new Pipeline<In>(options?)      the chain. Stages are RECORDED, not run.
  .transform / .apply           each records its own call in _pendingStages
  .buffer / .reduce / .local    same - which is what keeps each one's POSITION
  .branch(build)                -> a runner, the arms bound once
  (input)                       -> PipelineResult
                                     .toArray / .first / .consume / .forEach
                                     [Symbol.iterator] (sync results only)
                                     [Symbol.asyncIterator] (items) / .chunks()
```

Three mechanics make it work.

An instance is callable because the constructor RETURNS a function and reparents it onto
`new.target.prototype` - which restores the methods, `instanceof`, and the `this.constructor` that
`createPipeline()`'s copy-on-write depends on. `Pipeline.prototype` is itself reparented onto
`Function.prototype` once, below the class, so every instance is a real function. Never `class
Pipeline extends Function`: its `super()` runs `CreateDynamicFunction`, which throws `EvalError:
Code generation from strings disallowed for this context` wherever code generation is banned - a CSP
page, a Cloudflare Worker, `node --disallow-code-generation-from-strings`.

A stage composed before an input is recorded as its own CALL, not its result, and replayed against
the bound pipeline when one arrives. Recording the call is what keeps a deferred chain and a bound
one on identical code, and what preserves a stage's position - recording only a `.buffer()`'s SIZE
instead applied it to the source cut, so a `.buffer()` written after a stage took effect before it.

`Pipeline.drainable(input)` is the ONE seam between the two classes: it binds, then returns a
`Drainable<T>` - `{ syncChunks, chunks, context }` (`types.ts`, #133; three independent
re-spellings of this exact shape collapsed to the one type - `BranchOwner.drainable()` and
`PipelineResult`'s own field each used to declare it inline). Each terminal calls it exactly once
and threads what it got into its own async arm; calling it again there ran a user's `.local(build)`
callback twice per call. `PipelineResult.forEach()`/`[Symbol.iterator]()` and `utils/cut.ts`'s
`collectItems()` each branch on `syncChunks !== null` for the "is there a sync chunk stream, or
not" decision (#232 deleted the `dispatchSync()` wrapper that once held it).

A fourth field, a flattened per-ITEM view, sat beside `chunks` until #179 (BREAKING: `Drainable<T>`
is public, since `Pipeline.drainable(input)` is). Every async terminal read it, and flattening cost
one `await` - one microtask - per ROW to re-derive items the chunk view already held. All five now
walk `chunks()` and loop each chunk in process, `.branch()` collects through the same view, and
nothing reads the item view, so it is deleted rather than kept. `forEach` settles its callback only
when the return is genuinely thenable: a bare `await` on a plain value allocates a `Promise` too,
once per row, so a synchronous callback on an async chain paid for asynchrony it never used.
On `ConcurrentPipeline` over an async generator with `.buffer(1000)`, output asserted identical,
`.toArray()` and `.forEach()` each now create clearly fewer promises per row - both down to the
same count, which is the source's own floor.

Two knobs that look alike are deliberately apart. `PipelineMode` (`"unset" | "sync" | "async"`) is a
TYPE fact about what a chain produces; `_bound` is the RUNTIME fact of whether an input is attached.
`"unset"` answered both until a callable chain - `"unset"` for its whole life, bound only for the
duration of one call - made that impossible.

## Branching - a stage whose arms run where the chain runs (#90)

`.branch(build)` is a stage, not a terminal. Each arm receives a PIPELINE of the parent's own class,
which is what decides where its work runs - a `Transformer` has no class, so the shape this replaces
ran every arm in the orchestrating process however the chain was built.

```text
routed(orders)
	parent chain drains          /transform/0                 worker
	classifyItems                                             orchestrator
		claimItem per row, one fused walk   never dispatched
		one chunk in, one chunk PER ARM out
	router
		big  -> its own pipeline   /branch/0/big/transform/0    worker
		eu   -> its own pipeline   .local() pins it             orchestrator
	join                                                      orchestrator
	=> one record, keyed by arm name
```

Two placements are decisions rather than accidents. **Matching** stays on the orchestrator: a
predicate decides WHICH arm an item enters, so dispatching it would cost every item two trips - one
to be classified, one to be worked on - and would stop a predicate closing over anything the caller
holds. **The join** stays there too, because arms can be remote and it is the only process that sees
all of them.

The record is arrays, never results the caller drains at will. Two consumers over one shared source
can only buffer without bound, deadlock, or starve; measured on the shipped `share()`, draining one
view to completion gives it everything and the other `[]`. Owning the concurrency inside the join is
what makes that unrepresentable.

Nothing here is new machinery: `classifyItems`/`classifyAsyncChunks` share `claimItem` for the
per-item classify step and `drainSync` (`utils/drain.ts`, #133) for the sync/async
split every other synchronous drain in the package already uses, and the join is `settleMaybe` +
`chain`. That reuse is what makes the Mode rule reachable rather than aspirational: every arm
synchronous creates ZERO promises, and one asynchronous arm widens the whole record to a single
`Promise` while its synchronous siblings are never wrapped.

⚠ `runBranch` used to `collectItems()` the whole parent chain into one array, THEN walk it a second
time with a separate `demux()` - two full passes over every row, one to materialize, one to
classify (#180's own finding, Done-when 4). Measured: the two-pass shape cost roughly twice a
single-pass floor over the identical input, spiked and confirmed before the fix landed. Fixed, not Killed:
`classifyItems`/`classifyAsyncChunks` classify WHILE draining, reusing the same
`drainSync` primitive `collectItems()` itself is built from, so no intermediate array is
materialized at all - `runBranch`'s own body shrank from a collect call plus a `demux()` call to one
`classifyItems()` call.

An arm's stages address themselves under `/branch/<i>/<name>/`, the branch positional so two
`.branch()` calls may each declare an arm called `rest`, the arm by name. Without the trail an arm's
stage 0 collided with the parent's on the worker: measured, the parent's map ran twice
(`300 -> 360 -> 432`) and the arm's own transform never ran. The name must survive a URL path, so the
builder refuses one that would not.

## Benchmarks - pending #193

`benchmarks/` is a separate project, outside the pnpm workspace, that installs its comparators once
in a `deps` image and runs them on six pinned runtimes: `node:20/22/24/26-alpine`,
`oven/bun:1.3.14-alpine`, `denoland/deno:alpine`. It consumes the package the way a consumer does -
`npm pack` to a tarball, installed by `file:` reference - so `exports` and the `files` allowlist are
exercised rather than bypassed.

Two tables. The first times `map` then `filter` then `toArray` at 10k, 100k, 1M and 10M rows across
every chaining surface: `Array.prototype`, `Iterator.prototype`, `node:stream` `Readable`, Web
Streams `pipeThrough`, a hand-written `async function*`, this package, and `ix` /
`streaming-iterables` / `effect` / `rxjs`. The second controls ITEMS IN FLIGHT rather than any
declared concurrency option, because no two libraries name that knob the same way and this package
reaches it through `.buffer(size)` times `maxConcurrency`; the harness asserts each leg's measured
peak equals the target before recording a time.

A worker process (`ClusterPipeline`'s own bootstrap; `HttpPipeline`'s own `.fetch()`-side instance
in general) never orchestrates: its chunk stream is empty, set at construction, so every terminal op
resolves immediately with an EMPTY result - the worker exists only to hold the transforms
(`_chunkTransforms`, registered by running the same entry module the primary runs) and serve
`.fetch()` requests against them.

## Internal overhead benchmarks

`bench/` is a committed, in-repo, single-runtime harness - independent of `benchmarks/` above, which
stays the Docker/six-runtime/`npm pack` comparison against OTHER libraries. This one compares the
package against ITSELF: one leg per pipeline runner class (`Pipeline`, `ConcurrentPipeline`,
`HttpPipeline`, `ClusterPipeline`), each against a hand-rolled, output-matched, non-`Pipeline`
equivalent - the quickest in-process code producing the identical result, even where that skips a
real network/IPC boundary a dispatching class would cross. A committed baseline gates future runs on
an ABSOLUTE ns/row figure, 20% tolerance, one warm-up round discarded - `pipelineNsPerRow` for
`Pipeline`, `local.nsPerRow` instead for a dispatching class, since its own DISPATCHED leg crosses a
real network/IPC boundary whose jitter is not this package's own overhead (`bench/gate.ts`'s own
header has the full split). `.ratio` is read from every report and printed in the table below but
never gated - dividing two independently noisy measurements compounds their noise past what a 10%
tolerance survives (post-planning finding: across consecutive real runs `.ratio` spread several
times wider than `pipelineNsPerRow`, and a ratio gate failed some of them with no code change
between them). Each dispatching
class's own `.local()` row is measured and its correctness asserted (a pinned region never reaches
`stageWork()`/serves a request/runs on a worker pid). `pnpm bench:overhead` runs it; `bench/canonical.ts`
declares the one chain (`.map((x) => x * 2).filter((x) => x > 4)`) every leg and its floor measure.
The floor (`handRolledFloor`) is class-independent - the same loop regardless of which class its
ratio is compared against - so `bench/overhead.ts` measures it ONCE at `FLOOR_ROWS` (1,000,000 rows)
and shares that single number across every leg's own report, rather than each leg re-timing it at
its own smaller row count: at a small row count the same function varied several-fold between
consecutive runs, below stable measurement resolution (code-review finding). Every leg's ratio
therefore divides by the same single floor measurement, not four.

The committed baseline (`bench/baseline.json`) is one machine's reading; `pnpm bench:overhead`
checks a run against it rather than the docs recording its numbers. `Array.prototype` is kept as a
reference row - it runs no `Pipeline` machinery at all, so it carries no ratio of its own. Two legs
joined after #120's first table - `Branch` (#180, never dispatches - `.branch()`'s own matching and
join always run where the chain runs) and `EventEmitterPipeline` (#180, the fourth dispatching
class, #124). The relative picture: `Pipeline` and `ConcurrentPipeline` sit a little above
`Array.prototype` and the floor, `Branch` and `EventEmitterPipeline` a little above those, and the
dispatched `HttpPipeline`/`ClusterPipeline` legs an order of magnitude above, since they cross a real
boundary. Every `.local()` row sits close to `Pipeline`, and each pinned row asserts it never
dispatched:

| Class                  | `.local()` correctness                     |
| ---------------------- | ------------------------------------------ |
| `Pipeline`             | (never dispatches)                         |
| `ConcurrentPipeline`   | 0 `stageWork()` calls                      |
| `HttpPipeline`         | 0 HTTP requests served                     |
| `ClusterPipeline`      | every item on the primary pid              |
| `Branch`               | (never dispatches)                         |
| `EventEmitterPipeline` | 0 Workers registered or fired while pinned |

`ConcurrentPipeline`/`HttpPipeline`/`ClusterPipeline`'s current rows already carry the async-engine
tax reduction (#120 follow-up, above) and #179's own per-row-cost fixes: the ORIGINAL #120 baseline
(before either) read over an order of magnitude slower on every dispatched and every pinned row.

**`checkLocalParity` (`bench/gate.ts`, #180)** - `checkGate` above compares each leg to its OWN
committed baseline, so the five `.local()` rows in the table could drift apart from EACH OTHER
indefinitely with no violation. `checkLocalParity(report)` instead compares every dispatching
class's own `local.nsPerRow` against `Pipeline`'s `pipelineNsPerRow` FROM THE SAME REPORT, no
baseline file needed - regression-only, a ratio below its own ceiling (even below 1.0) is never a
violation. `pnpm bench:overhead` runs both gates; either failing sets `process.exitCode = 1`.

⚠ Finding (Done-when 9): `EventEmitterPipeline`'s own parity ratio reads consistently above its
three siblings (clearly above parity, where the others sit near it) - a swap probe (measuring
`EventEmitterPipeline` FIRST instead of last in the run order) isolated TWO effects, not one.
`Pipeline.pipelineNsPerRow` itself is a measurement-order artefact - it read clearly slower when
measured second than when measured first, with no code change. `EventEmitterPipeline`'s own
absolute `local.nsPerRow` held stable regardless of position - still clearly above `Pipeline`'s own
stable-when-first reading, a residual that survived the swap and stayed unexplained. Half explained,
not fully: `LOCAL_PARITY_CEILING`'s own per-class values (`ConcurrentPipeline`/`HttpPipeline` 1.15,
`ClusterPipeline` 1.35, `EventEmitterPipeline` 1.65) bake in both effects, read off the real, shipped
run order - NOT a claim that `.local()` costs the identical amount on every class. Untested candidate
for the residual: `EventEmitterPipeline.drainable()` wraps every drain in `withEndSignal`
unconditionally (above), a cost the other three classes do not pay the same way.

`Pipeline` has no `.local()` row: the base class never dispatches, so pinning it changes nothing to
measure. O1 (#120) collapsed `Transformer.filter()`'s sync no-handler branch from three passes to
one, making `Pipeline` clearly faster; O2 (fusing adjacent sync `map`/`filter` links) was spiked
against this same baseline and killed - roughly a third faster end to end, priced against a `#45`-shaped
rewrite or a leaky single-pattern peephole, in `.claude/roadmap.md`'s own Killed section.

## Constraints in dependencies

- `ws` 8.21.3 (#201; an optional peer dependency since #239, the only package `src/` imports -
  `bufferutil`/`utf-8-validate` are its own optional peer dependencies for native-accelerated
  masking/UTF-8 validation, not required, and stay absent from `package.json`) delivers every WS
  frame's payload - text or binary - as a `Buffer` on `"message"`, with `isBinary` the only
  discriminator; it never auto-decodes a text frame into a JS string. Its own `ws+unix:` scheme
  splits the WHOLE path on the FIRST `:` (`initAsClient`, `lib/websocket.js`) -
  `ws+unix:/path:/urlpath`, no leading `//`. `ws` ships no types, so `@types/ws` is a devDependency
  only. `tsup.config.ts`'s own `external` array lists it explicitly, alongside what tsup already
  excludes by default for a `peerDependencies` entry.
- tsup splits ESM chunks by default and CJS not at all: `splitting: true` is its experimental CJS
  flag. Without it, `dist/websocket.cjs` carries its own `Pipeline` copy, and `Pipeline.wrapping`'s
  `instanceof Pipeline` reads a chain built from `dist/index.cjs` as an options object.
- TypeScript removed `baseUrl` at 7.0; a tsconfig that sets it fails with `TS5102`.
- A conditional type distributes only over a naked type parameter. `Ps[number] extends Pipeline<infer
  U> ? U : never` is an indexed access, so it compiles and evaluates to `never`; the deleted merge
  extracts it as `ElementOf<P>` to make it distribute (#5).
- `Pipeline<T>` is invariant, because `apply<U>(transformer: Transformer<T, U>)` puts `T` in a
  parameter position. Only `Pipeline<any>` works as a constraint over pipelines of mixed item types.
- Node `>=26` (`package.json` `engines`) - the package targets `node18` at build (`tsup.config.ts`) for
  the widest consumer range, but development and CI run on 26 (`.nvmrc`).
- `ts-pattern` 5.9.0 cannot check exhaustiveness at a site generic in its payload type: with every
  arm present, `tsc --strict` still refuses with `TS2349: This expression is not callable. Type
  'NonExhaustiveError<unknown>' has no call signatures.` A concrete union works; a tagged
  `{ kind: "keep"; value: U } | { kind: "drop" }` wrapper restores it, but the match plus building
  the wrappers costs over an order of magnitude more than a bare `!== DROP` identity check.
- Node 26 exposes `Request`/`Response`/`fetch` but serves no fetch handler natively. A real probe of
  `createServer(async () => new Response("hi"))` hangs and times out - the returned `Response` is
  ignored and nothing is written to `res`. Hence `toNodeHandler` (#17). Bun, Deno and Cloudflare need
  nothing.
- There is no standard for the server interface. WinterTC's "Minimum common web API" (Draft,
  31 July 2026, Ecma TC55) standardises `Request`/`Response`/`Headers`/`fetch` as runtime capabilities
  and defines no server, handler or routing. `(request: Request) => Response` is a de facto convention.
- Hono's `mount()` rewrites the path by default (`hono-base.js:241-248`), so a mounted handler receives
  `/transform/0`, not `/pipeline/transform/0`. A mountable handler must be prefix-agnostic.
- `listen(0)` inside `node:cluster` yields the SAME port to every worker - probe:
  `PORTS [63262,63262,63262] UNIQUE_COUNT 1 SHARED`. The primary allocates once and shares the socket,
  so no port-picking dependency is needed.
- `cluster.fork()` re-runs the entry module, and a worker must run it to hold the transforms, so a file
  constructing a `ClusterPipeline` executes once per worker. Measured: `TOP_LEVEL_RAN_COUNT 11` on a
  ten-core box. Forked workers also hold the event loop open through cluster's shared `TCPServerWrap`,
  which no public API exposes, so they must be killed rather than unref'd for a script to exit.
- `undici`'s `fetch` is clearly slower per request than `node:http` with a keep-alive agent, measured
  at one payload size on Node 26 with socket reuse confirmed on both (0 new TCP connections per 100
  requests). Runtime neutrality was chosen over that cost (#17); Bun and Deno ship their own `fetch`,
  so the gap is Node-specific.
- A `ClusterPipeline` context mutation (`ctx.set()` inside a dispatched stage's own transform) still
  never reaches the orchestrator - `.fetch()` returns only `{ chunk }`, never the mutated context
  (#31 leaves the wire unchanged). What changed under #31: `.fetch()` no longer builds a fresh
  `SimpleContextManager` from the wire per request - it reuses `this._context`, the SAME instance
  `PipelineOptions.context`/`contextFactory` built at construction, applying the wire's own forward
  values onto it via `.set()`. A worker's `ctx.set()` mutates that instance for the rest of its
  process's life, but the mutation still never crosses back over the wire to the orchestrator.
  Measured (pre-#31): the orchestrator's context stayed `{"multiplier":10}` after three remote
  `ctx.set()` calls - the write-back direction is unaffected by #31 and stays a roadmap item
  (`.claude/roadmap.md`, "A `ContextManager`'s write-back to the orchestrator"), not fixed here.
  `.context()`'s own forward propagation (orchestrator → every stage) is unaffected.
- WHICH manager a process uses is the caller's choice, and the caller's class alone decides whether
  state crosses a process (#31). `PipelineOptions.context` is an instance for THIS process, kept by
  every operation - `.context()` merges into it. `PipelineOptions.contextFactory` is how to build one
  where an instance cannot travel; the constructor calls it at most once per process, only when
  `context` is absent, and `.fetch()` (above) reuses the built instance rather than calling it again.
  No registry and no serialization are needed: `cluster.fork()` re-execs the entry module, so a
  worker already holds the factory's own construction code. Measured with `workers: 3`
  (`__tests__/fixtures/cluster-context-factory.ts`): three worker processes, three distinct manager
  instances, each built in the pid that served the chunk, and `.context({multiplier:10})` still
  crossing.
- Two concurrent requests on the SAME worker share that one reused `this._context` (#31) - a
  transform that `.set()`s a key, awaits, then reads it back can see a sibling request's write
  instead of its own. Not new: the identical pattern reproduces on a plain `ConcurrentPipeline` with
  no HTTP or cluster involved at all, since `concurrent.ts`'s own `fanOut*` functions have always
  passed ONE `this._context` to every concurrently in-flight chunk. #31 carries that pre-existing
  sharing across a process boundary; it neither introduces nor worsens it, and no fix landed in that
  ticket by explicit decision.
- `WorkerSet`/`WsWorkerSet`'s own `registry` field (`cluster.ts`, one per-process singleton per
  class since #133 - was 5 separate module-level bindings) never evicts an entry - every distinct
  `ClusterHttpPipeline`/`ClusterPipeline` constructed in a process stays reachable for that process's
  life. Sound for the documented construction pattern (one `ClusterHttpPipeline`/`ClusterPipeline`
  per logical chain, built once at module scope, the same "no top-level side effects beyond
  registering transforms" rule above already assumes); a caller constructing a fresh one per request
  grows the registry unbounded.
- The idle-kill window between a `ClusterHttpPipeline`/`ClusterPipeline`'s last dispatch and its
  workers being killed (`cluster.ts`'s `IDLE_KILL_MS`, shared by both classes' own `kill()`) is
  `500`ms - a chosen value, not a tuned or caller-facing one. Long enough that back-to-back
  dispatches in a real workload never trigger a re-fork; short
  enough that a script holding only the canonical example exits on its own well inside a normal test
  timeout. `WorkerSet.kill()`/`WsWorkerSet.kill()` each track their own forked worker ids
  (`ownWorkerIds`, #201 review) and kill only those - both iterate `cluster.workers`, a registry
  `node:cluster` shares PROCESS-WIDE, so before either tracked its own ids, a process using BOTH
  classes had one's idle timer kill the other's still-in-flight workers.

- Node's `fetch` IS full duplex against a `node:http` server, refuting the half-duplex reading of
  `duplex: "half"`. Measured on Node 26.5.0 (undici): response headers at +207ms with the request
  body still open, each echo returned within 2ms of its item, `echoes received BEFORE the request
  body closed = 3 of 3`. So a streaming reducer needs no SSE, no long polling, no WebSocket and no
  session id. Verified on Node against `node:http` ONLY - Bun, Deno, Cloudflare and any buffering
  intermediary are unverified (#45).
- `toNodeHandler` used to buffer BOTH directions, defeating that duplex capability for every Node
  consumer: `handleOverBridge` collected `req` into a `Buffer` before building the `Request` and
  ended with `res.end(Buffer.from(await response.arrayBuffer()))`. Measured (pre-#45): a handler
  echoing per item saw nothing until the client closed its body at +457ms, then the client received
  all three replies in ONE frame at +477ms - no data lost, purely a streaming defect, invisible to
  the one-shot `/transform/<n>` route. `Readable.toWeb(req)` as the request body plus `writeStreamedBody`
  (a `for await` pipe of the response into `res`, stopping once `res.destroyed`, awaiting `'drain'`
  on backpressure) fixes it (#45): first reply back at +163ms, `frames delivered BEFORE the request
  body closed = 2 of 3`. A response-body failure after bytes are already flushed destroys the
  connection instead of hanging the client, since headers already sent rules out a fresh error
  response. Bun and Deno mount `.fetch` directly and already stream.
- One long-lived HTTP request stays inside ONE `node:cluster` worker for its whole life, so a
  streaming reducer's accumulator lives in the connection rather than in a session store. Measured
  with 4 workers on the shared port and 5 chunks fed 100ms apart: `DISTINCT PIDS THAT SERVED THIS ONE
  STREAM = 1`, all 9 items folded there. No separate reducer worker and no affinity mechanism is
  needed (#45).

## The reduce stage

A reducer is a fold with cross-chunk state, so it does not fit `InternalTransformer` (`chunk` in,
`Out[]` out, one output chunk per input chunk). Its shape is a stream operator: `Pipeline.reduce()`
folds `this._chunks` directly (in-process, sequential, no `reduceWork()` indirection - the base class
never dispatches); `ConcurrentPipeline.reduce()` overrides it to always delegate to `reduceWork()` -
`stageWork()`'s sibling, the one method a subclass overrides to change WHERE a reducer runs.
Wrapping the call in `.local(build)` (#61) runs `build`'s own `.reduce()` against a bare `Pipeline`
instead, the base class's own fold:

```text
ConcurrentPipeline.reduce(fn, initial)
	reduceWork(fn, initial, stageIndex)            the per-class override, its closure called maxConcurrency times
		ConcurrentPipeline    fold in-process, sequentially     maxConcurrency partitions, each its own
		                                                       accumulator, merged unordered (#62)
		HttpPipeline          one duplex POST /reduce/<n>       maxConcurrency concurrent POSTs, each
		                      per partition                     partition's own accumulator server-side
		ClusterPipeline       bootstrap + inFlight around the   one worker per partition, each serving
		                      WHOLE connection (stageWork()'s   its own share() view of the stream
		                      own bracket wraps one CHUNK)
	emit(value)                                     buffered per input chunk, yielded as its own chunk
	final accumulator                               only if items were folded since the last emit
	seed                                            once, when the stage folded nothing
```

A reduce that received no data emits its seed once, and the rule sits where the whole
stream is known:

```text
Pipeline.reduce(fn, initial)                          async arm and sync arm
	foldChunkStream(..., seedIfEmpty = true)          the async arm's one Reducer for the whole stream
		Reducer.final(true)                           [acc] if items folded since the last emit, OR fn never ran
	foldSyncChunkStream                               the sync arm, no Promise created
		Reducer.final(true)                           decided after `tail` settles, inside driveFold's final()
ConcurrentPipeline.reduce(fn, initial)                Http / WebSocket / Cluster / EventEmitter inherit it
	reduceWork()                                      maxConcurrency partitions, each foldChunkStream(..., false)
		Reducer.final()                               a partition never seeds; its share can be empty
	seedIfNoChunk(mergeUnordered(partitions), () => seedFor(initial))
		yields [seed] once, when no partition yielded any chunk
Transformer.reduce(fn, initial)                       per chunk
	Reducer.final(true)                               a chunk that arrives empty emits `initial`
```

Two constraints, both measured. A rule inside `Reducer.final()` unconditionally is wrong: a dispatched
partition builds its own `Reducer` even when it receives no chunk, so `maxConcurrency: 4` over `[]`
returned `[0,0,0,0]` and over five items in three chunks returned `[0,3,7,5]`. A "no chunk was
yielded" check is wrong on the sync arm: `driveFold` yields a pending slot for an async chunk that
then resolves empty, so the seed is decided in `final()` after `tail` settles. Output-empty and
fold-never-ran are the same fact only at the merged output of a partitioned stage, where any
partition that folded anything yields a trailing accumulator or an emit. `.buffer(fn)` keeps its own
trailing rule, `Reducer.current()`, and an empty pending array stays an empty chunk. `HttpPipeline`
still opens `maxConcurrency` requests for a stream of zero chunks.

`ConcurrentPipeline.reduce()` (#62) PARTITIONS rather than delegating once: `reduceWork()` itself is
still called ONCE, but the closure it RETURNS is called `maxConcurrency` times, each its own
independent accumulator over its own `share()` view (`src/utils/chunk.ts`) of the ONE shared chunk
stream - free-slot dealing, no dealer, no per-partition queues, a slow partition simply calls
`.next()` less often, so the others pick up its slack. `mergeUnordered()`
(`src/pipelines/concurrent.ts`) merges the partitions' own output in completion order, since there is
no order between them. Each partition's own result - an `emit()` mid-fold, or its trailing
accumulator once its share of the stream ends - flows downstream as an ordinary value, the same way
a non-partitioned reduce's own `emit()` output already does: no forced merge, no thrown error, no
`combine` parameter. A caller who wants ONE final value writes an ordinary second reduce as the next
stage - `.local((p) => p.reduce(mergeFn, initial))` - the same pattern used to fold down any other
multi-value reduce output. `HttpPipeline`/`ClusterPipeline` inherit partitioning with no new code of
their own: `reduceWork()`'s existing per-request `Reducer` construction (`runReduceStage`,
`src/pipelines/http.ts:86`) already means N concurrent duplex POSTs to the SAME `/reduce/<n>` fold N
independent accumulators.

`ReduceFunction<U, T> = (acc, item, ctx, emit) => U | Promise<U>` puts `emit` FOURTH so `ctx` keeps
arity 3. Every reduce path (`Transformer.reduce`, the shared `Reducer`/`foldChunk`/`foldChunkStream`
helpers in `src/utils/reduce.ts`, `http.ts`'s own frame folding) calls `fn` with all four arguments
unconditionally - JS ignores the extras a shorter callback never declared, so no `fn.length` arity
check is needed anywhere in the reduce path (unlike `map`/`filter`'s own `isContextAware`, which
still branches on arity to decide whether to pass `ctx` at all).

A reduce stage takes the next index in the SHARED stage-index space `_chunkTransforms` already uses,
so `/transform/<n>` and `/reduce/<n>` never collide: `pushReduceStage()` (`src/pipeline.ts`, shared by
base `Pipeline.reduce()` and `ConcurrentPipeline.reduce()`) registers the stage in `_reduceStages`
and writes a placeholder into the SAME `_chunkTransforms` index that throws if ever invoked as a
plain per-chunk transform - the fail-loud guard, and the only one needed: #39 already deleted the
whole source-position/replay mechanism a reduce-specific `sourcePositionViolations` list would have
needed to hook into, since async iteration reads the exact same persisted `_chunks` every terminal
op reads.

The wire is NDJSON both ways over one POST, `HttpPipeline.routePath("reduce", index)` (`routePath`
takes a verb, `stage` or `reduce`, replacing the old `stagePath(index)`): `{"context":{…}}` once,
then `{"chunk":[…]}` per upstream chunk, with `{"emit":[…]}` frames coming back as they happen and
`{"error":"…"}` for a mid-stream failure. That failure arrives AFTER the 200, so values already
emitted have already entered downstream stages - the price paid for results that arrive as they
happen, and the same property that killed the pull topology (`.claude/roadmap.md`) accepted
deliberately here.
