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
│ node:cluster / node:http — ClusterPipeline/HttpPipeline   │
│ only; a plain Pipeline/ConcurrentPipeline needs neither   │
├─────────────────────────────────────────────────────────┤
│ the caller's own AsyncIterable source                    │
└─────────────────────────────────────────────────────────┘
```

Nothing below the caller's source is this package's concern - no DB driver, no file I/O, no network
client of its own beyond what dispatching a stage requires. A `Pipeline` accepts an array, an
`AsyncIterable`, or any object shaped as one; a laygo `Model` reaches a `Pipeline` the same way,
structurally (`outputty/laygo`'s `Source` accepts any `AsyncIterable`), with no import edge in either
direction (#743, #745).

The `node:cluster`/`node:http` boundary above is oxlint-enforced - pending #117.

## Module layout

```text
src/
  types.ts              PipelineFunction, IContextManager, InternalTransformer, every options
                          interface, plus DROP/RowErrorHandler/PipelineErrorHandler/RunScope (#78);
                          StageRegistries (a stage's chunkTransforms+reduceStages pair),
                          Drainable<T> (the 4-field drain view PipelineResult/BranchOwner share),
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
                          runBranch/joinArms(grouped, dispatch: ArmDispatch<T>, context)/demux - pushArm()
                          and findCatchAll() are BranchBuilder's own private helpers, replacing a
                          repeated cast-and-push and a repeated find(isCatchAll) (#133)
  result.ts               PipelineResult - every terminal op; drainable() returns Drainable<T>
                          directly; forEach()/[Symbol.iterator]() share utils/drain.ts's own
                          dispatchSync() instead of each testing syncChunks separately (#133)
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
    cluster.ts               ClusterPipeline - the WorkerSet class (register/claimIndex/lookup/
                             bootstrap/enter/kill/startWorkerServer) replaces 5 module-level mutable
                             bindings and 4 free functions with one per-process singleton (#133);
                             bootstrapAndSetUrl() calls workerSet.enter() once, no longer bootstraps
                             twice
    eventemitter.ts           EventEmitterPipeline (#124) - stageWork() dispatches through
                             pipeline.emitter instead of HTTP/cluster; apply()/drainable() overridden
                             a second and third time for stage:<n>:end/pipeline:end; carriedKnobs()
                             (not createPipeline()) carries its emitter/registeredStages (#133)
  context/
    simple.ts              SimpleContextManager - the one shipped IContextManager; context/types.ts
                             (a dead re-export) is deleted (#133)
  utils/
    chunk.ts                a thin re-export barrel over cut.ts/drain.ts/recut.ts, so an existing
                             `from "@src/utils/chunk"` import keeps resolving (#133), normalize
                             included - this internal path is unchanged. What dropped is the
                             PACKAGE's own public surface: `utils/index.ts` and `src/index.ts`
                             never re-export normalize any more (BREAKING) - the one caller left
                             (`__tests__/normalize-and-chunks.e2e.test.ts`) reaches it through this
                             internal `@src/utils/chunk` path, same as before
    cut.ts                  buildChunkGenerator/buildSyncChunkGenerator (cut) / flattenChunks
                             (undo) / normalize / share / collectItems (`collectAsyncItems()` is
                             `collectItems()`'s own unexported async half); assertPositiveChunkSize()
                             is the one `chunkSize < 1` guard 3 sites shared inline before (#133);
                             assertWholeNumberAtLeastOne(label, value) is `.buffer(size)`/`.queue()`'s
                             own shared, labelled validator (#123); prefetch(upstream, capacity) is
                             `.queue()`'s own engine, beside `share()` (#123)
    drain.ts                 MaybeAsyncChunks<T>, drainSync/drainSyncSettled/close/dispatchSync -
                             dispatchSync(syncChunks, onSync, onAsync) is the sync/async branch
                             result.ts's forEach/[Symbol.iterator] and cut.ts's collectItems all
                             shared inline before (#133)
    recut.ts                 RecutState<T> ({iterator, size}) / recutFrom / recutPending /
                             cutChunk / recutSyncChunks - the iterator+size pair `recutFrom` and
                             `recutPending` used to thread separately is now one state object (#133)
    helpers.ts               isContextAware - fn.length arity check (isContextAwareReduce, its
                             reduce-side twin, is gone: every reduce path always passes all four
                             ReduceFunction arguments, #45); dropOrRethrow - the run handler's own
                             "call it, or propagate" decision, shared by runSequentially and
                             ConcurrentPipeline.apply()'s wrapped work (#78); tryRecover() is the
                             try/catch-if-thenable/recover skeleton runStageChunk here and
                             transformer.ts's attemptRow both now share - NOT used by
                             utils/reduce.ts's Reducer.fold, whose own hot per-item path keeps its
                             measured-faster inlined form (#133)
    reduce.ts                Reducer/foldChunk/foldChunkStream - the shared fold, used by
                             Transformer.reduce, Pipeline.reduce and http.ts's own frame folding;
                             Reducer takes an optional row handler (#78); Reducer.current() reads
                             the raw accumulator with no itemsSinceEmit gating (#88);
                             buildBufferGenerator/buildSyncBufferGenerator/recutSyncChunksWith are
                             .buffer(fn)'s own engine, sizeReduceFunction/bufferReduceFunction the
                             two adapters onto Reducer<T[], T> (#88)
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

`.buffer(size)` and `.buffer(fn: BufferFunction<T>)` fold through the SAME engine - one
`Reducer<T[], T>` (`src/utils/reduce.ts`, unchanged from what `Pipeline.reduce()` already uses),
configured by one of two adapters: `sizeReduceFunction(size)` (identity, framework-side auto-flush
at `pending.length >= size`) or `bufferReduceFunction(fn)` (adapts a caller's zero-arg `emit`/`flush`
onto the reducer's own value-taking `emit`). `.buffer(size)`'s own three branches
(deferred/sync/async) are unchanged in shape; only their innermost cutting call switched from
`buildChunkGenerator`/`buildSyncChunkGenerator` to the shared engine - `recutSyncChunks`'s own
index-based re-slice (the "a real stage already ran" sync sub-path) stays untouched, since it
operates on already-cut arrays with no per-item decision to make.

```text
Pipeline.buffer(sizeOrFn)
	sizeReduceFunction(size) | bufferReduceFunction(fn)     ONE ReduceFunction<T[], T>
	isDeferred() ? record + replay : …
	isSync() ?
		_syncPreBufferItems !== null → buildSyncBufferGenerator(reduceFn, ctx)(items)
		else (a real stage ran)      → typeof sizeOrFn === "number"
		                                  ? recutSyncChunks(_syncChunks, size)      untouched
		                                  : recutSyncChunksWith(_syncChunks, reduceFn, ctx)
	: buildBufferGenerator(reduceFn, ctx)(items)            fully async arm
```

Each `emit()` - `sizeReduceFunction`'s own auto-flush, or a caller's explicit `flush()` - IS a chunk
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

Measured end to end: a 100ms/item source through a 30ms/item transform, `.queue(3)`, 5 items - 674ms
fully serial, 542ms queued, same 5 outputs in order.

## Synchronous execution - pending #90

Every piece above (`buildChunkGenerator`, `flattenChunks`, `Transformer.process()`/
`runSequentially`, `.pipe()`'s own `await currentTransform(...)`) is unconditionally async, so a
`Pipeline` over a plain in-memory array with only synchronous functions still pays a full
async-generator round trip per item to convert its source into `_chunks`, before any stage or
`Promise.all` ever runs - measured at ~430 ns/row end to end against a plain `Array.prototype`
chain's ~20 ns/row, ~290 ns/row of it paid with zero transform stages at all (#90's own ticket).

`.from(source)` becomes the one place `PipelineMode` (`"unset" | "sync" | "async"`) is decided -
`Symbol.asyncIterator in Object(source)` the same way `toAsyncIterable()` already checks today. A
`"sync"` `Pipeline` runs a SECOND, parallel set of chunk/transform utilities - plain `function*`
counterparts to `buildChunkGenerator`/`flattenChunks`, and a plain (non-`async`) composed transform
function mirroring `pipe()` - so nothing async-shaped is ever constructed until a stage's own
function returns a `Promise`, or `.from()` is given an `AsyncIterable`, or `.merge()` combines in an
already-async pipeline. `Transformer<In, Out, M extends "sync" | "async">`'s 3-overload
`map()`/`filter()`/`flatMap()`/`reduce()`/`tap()` (async arm, a sync arm constrained
`U extends Promise<unknown> ? never : U`, a generic fallback returning `"async"` for a call site
generic in its own type parameters) is the seam that keeps a chain typed `"sync"` for exactly as
long as every stage's callback is provably synchronous, and silently (never a compile error) falls
back to `"async"` at a site TypeScript cannot prove sync-ness for. A stage whose callback is typed
sync but returns a thenable at runtime is caught by a per-chunk fail-loud check on the sync engine's
own output, at no measurable cost (~35 ns/row guarded, within noise of unguarded).

`ConcurrentPipeline`/`HttpPipeline`/`ClusterPipeline` each override `.from()` to return `"async"`
unconditionally - none has a synchronous case, since each dispatches a chunk across a real boundary
regardless of how synchronous the caller's own callbacks are.

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

A link with no handler registered runs its existing `Promise.all` path unchanged - measured on the
shipped code at 354-380 ns/row at 1M rows across four separate runs, matching the pre-#78 floor
within run-to-run JIT noise, so the seam costs nothing unused. A registered handler measured 6-14%
slower across the same runs (377-415 ns/row) - noisy but consistently positive, never free. `DROP`
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
      ClusterPipeline         a chunk sent to another local process    src/pipelines/cluster.ts
    EventEmitterPipeline    a chunk handed to Workers on an emitter    src/pipelines/eventemitter.ts
                             (#124) - a SIBLING of HttpPipeline, not a subclass: it
                             overrides stageWork() the same way, but never POSTs
```

Each level overrides ONE thing. `ConcurrentPipeline` owns the fan-out window (`fanOutOrdered`/
`fanOutUnordered`) and the default in-process `stageWork()`; `HttpPipeline` overrides `stageWork()`
alone to POST instead, adds `.fetch()`/`stagePath()`/`toNodeHandler`; `ClusterPipeline` adds the
worker bootstrap, wraps `stageWork()` to lazily bootstrap on first dispatch, and overrides
`stagePath()` to route several pipeline definitions through one shared worker server
(`/pipeline/<i>/transform/<n>`, `<i>` a construction-order index reproduced identically by every worker).
`.local(build)` (#61) is the one way to keep a whole region in-process: it builds a bare `Pipeline`
over `this._chunks`/`this._context` (never `this.constructor` - the region must never be able to
dispatch, whatever class called it), runs `build` against that bare pipeline, and carries the built
region's `_chunks`/`_context`/`_chunkTransforms`/`_reduceStages` back through `this.createPipeline()`
- the SAME seam every other copy-on-write method uses to resume the caller's own class. Each
dispatching subclass re-declares `local()` to narrow its return type only
(`~/.claude/rules/typescript.md`); the body is an unchanged `super.local(build)` call at every
level, needing no per-level code - the base implementation is already correct everywhere because a
bare `Pipeline`'s own `.transform()`/`.reduce()` never fan out or POST.

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
// http.ts / cluster.ts / eventemitter.ts - each spreads its super, adds its own fields
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
16 in flight over 50000 items of microtask-only work: `.buffer(16)` with `maxConcurrency: 1` ran
27 ms and `.buffer(1)` with `maxConcurrency: 16` ran 63 ms, because a chunk pays the per-chunk cost
once where `.buffer(1)` pays it per item. The gap closes when the callback dominates - the same pair
over a 2 ms-per-item workload, N=160, ran 23 ms each. Prefer the widest chunk that fits the
in-flight budget.

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
  registers transformer.runnable() on pipeline.emitter, "stage:<n>", ONCE PER STAGE INDEX
    (a Set keyed by event name, carried through createPipeline() - never once per RUN, since
    stageWork() replays on every bound call and a naive registration there leaks a listener)
  returns (chunk, ctx) => new Promise((resolve, reject) => {
    emitSafely("stage:<n>:dispatched")                <- every lifecycle emit is emitSafely, no exceptions
    for each fn in emitter.listeners("stage:<n>"):   <- composed fn is ALWAYS listeners()[0]
      try: Promise.resolve(fn({chunk, ctx, respond, reject})).catch(doReject)
      catch (sync throw): doReject(error)             <- one Worker's throw never skips the rest
    (no listeners at all -> doReject immediately, naming the stage)
  settle(outcome):  resolve/reject first, THEN emitSafely("stage:<n>:done" | ":error")
    respond(value) = settle({ok:true, value}); doReject(error) = settle({ok:false, error})
```

Dispatch calls `emitter.listeners(workEvent)` itself and invokes each directly, inside its own
`try` - never `emitter.emit()`, which cannot catch a Worker's throw after its own `await` (an
unhandled rejection Node/Bun may treat as fatal; measured, isolated with the composed Worker
removed: a plain `emit()`-based dispatch left the request permanently pending while the process
still crashed on the side). Every registered Worker runs on every chunk - broadcast, deliberately
uncontrolled - and the first one to SETTLE, `respond()` or `reject()`, decides the chunk: measured,
a Worker rejecting at 5ms beat one resolving at 30ms even though the resolving one was registered
first, so "first to settle" is the real contract, not "first to respond." The per-Worker `try`
(review-caught, first cut lacked it) is what stops a Worker's SYNCHRONOUS throw aborting the whole
loop before every Worker registered after it gets its turn - verified live: a throwing Worker
registered ahead of a correct one still leaves the correct one's own body run, even though the
throw settles the dispatch first. `settle()` (both `respond()`/`doReject()` narrow to it - one
guard, not two, review-caught: the first cut hand-rolled the same `if (settled) return; settled =
true;` guard twice) settles the REAL `Promise` (`resolve`/`reject`) BEFORE emitting its own
lifecycle event, through `emitSafely()` - EVERY lifecycle emit in this class goes through it,
`:dispatched` included, not only `:done`/`:error` (review-caught: the first cut left `:dispatched`
as a raw `emitter.emit()` call, so a throwing `:dispatched` listener synchronously rejected the
whole dispatch Promise as if it were a Worker's own failure, silently absorbed by `.onError()` -
measured: `out` came back `[]` with no error surfaced anywhere). A `:done`/`:error` listener that
itself throws would otherwise fire inside a `.then()` callback with no downstream `.catch()`, and
since that throw would happen BEFORE the real settle, the dispatch could hang forever rather than
merely leak an unhandled rejection (review-caught, verified live: a throwing `stage:0:done`
listener produced zero unhandled rejections and no hang, surfacing instead as its own separate
`uncaughtException` on the next microtask via `emitSafely`'s `queueMicrotask`).

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
a multi-Worker `stage:<n>` is discarded with no trace once another Worker has already settled -
`settle()`'s `if (settled) return` guard means no `:error` emit, no log, nothing observable anywhere
for it. This matches "first to settle wins" for the WINNER; nothing catches a bug in a Worker that
merely lost the race.

`apply()` is overridden a second time, wrapping the stage's own output chunk stream so
`stage:<n>:end` fires once, after every chunk that stage's fan-out produced has been yielded from
THIS WRAPPED STREAM - the stage index it wraps under is read OFF THE RESULT
(`dispatched._chunkTransforms.length - 1`, the slot `super.apply()` just appended), never
independently re-derived, so it can never drift from `ConcurrentPipeline.apply()`'s own internal
computation. Both the wrapped generator's `onEnd` here and `drainable()`'s own `fireOnce` (below)
call `emitSafely()`, never a raw `emitter.emit()` - the callback runs inside the stream's own
`finally` block, and JS's finally-overrides-exception semantics mean an unguarded throw there would
REPLACE whatever real stream error was already propagating with the observer's own unrelated one
(review-caught, measured: a chunk that genuinely fails combined with a throwing `stage:0:end`
listener rejected with the OBSERVER's error, not the real one, before this fix). ⚠ Under
`maxConcurrency > 1` with an early terminal (`.first(n)`), `stage:<n>:end` can fire BEFORE some of
that same stage's own `:done`/`:error` events: an early return stops PULLING from the wrapped
stream, but a chunk already dispatched into `ConcurrentPipeline`'s own fan-out keeps running in the
background and settles independently of when the consumer stopped reading - measured, `maxConcurrency:
4` with a 50ms map over four items and `.first(1)`: event order `[done, end, done, done, done]`,
three more `:done`s after `:end`. Treat `:end` as "no more chunks will be YIELDED here", never as
"every in-flight Worker for this stage has finished."

`drainable()` (`src/pipeline.ts:1292`, the one seam every terminal calls) is overridden a third
time, wrapping whichever of `items()`/`chunks()` a terminal actually calls so `pipeline:end` fires
once the wrapped stream is exhausted - once per TERMINAL CALL, matching `PipelineResult`'s own
"every terminal re-drains" contract: calling `.first()` then `.toArray()` on the same result fires
it twice.

`Pipeline.onError()` (the run handler) reaches a rejecting Worker for free, through
`ConcurrentPipeline.apply()`'s existing wrapped `work` - no explicit `dropOrRethrow()` call is
needed in this class's own code, unlike the killed pool design, which bypassed that machinery
entirely and had to call it explicitly.

The constructor mirrors `HttpPipeline`'s own two-overload shape (`Pipeline.wrapping()`, `(pipeline,
options)` wraps a chain built elsewhere, `(options)` builds standalone), and validates a
caller-supplied `options.emitter` against `PipelineEmitter`'s five methods at construction - a
trust-boundary value, so a missing method fails loud there rather than as a generic `TypeError`
deep inside `stageWork()`'s dispatch closure later. Validated ONCE, at the ORIGINAL caller-facing
construction only - gated on the ABSENCE of the internal `registeredStages` option, which only
`createPipeline()` (below) ever sets, so a long chain's own copy-on-write calls
(`.transform()`/`.buffer()`/`.context()`) never re-validate the identical, unchanged `emitter`
object a second time.

One emitter per chain, in two DIFFERENT failure shapes depending on where the `Set` comes from -
`#113`'s `pipelineIndex` fix for `ClusterPipeline` is the family's precedent for solving either
properly; unbuilt here, both named in `#124`'s own Settle first.

- Two INDEPENDENTLY-CONSTRUCTED `EventEmitterPipeline`s sharing one caller-supplied `emitter`
  option each get their OWN fresh `_registeredStages` (no `registeredStages` option was carried
  in), so BOTH composed functions register on the shared emitter's `"stage:0"` - measured, not
  "the second never registers" as an earlier draft of this section claimed: `listenerCount` reaches
  `2`, and the two RACE on every dispatch. Since neither's own work is genuinely async, the
  EARLIER-registered chain's function won: its `.then()` microtask is scheduled first in the same
  dispatch loop, so for two purely synchronous transforms the outcome is not a coin flip - the
  second chain's own transform never ran at all, its output silently the first chain's.
- Two chains FORKED from the SAME unbound instance - two `.transform()` calls off one shared base,
  or two `.branch()` arms (`Pipeline.branch()`'s own `emptyOfOwnClass()` resets the arm's
  `_chunkTransforms` to `[]`, so its first stage is index 0 again, while `createPipeline()` still
  carries the SAME `emitter`/registered-stages `Set` into it) - collide the OPPOSITE way: the
  `Set` is the SAME object by reference, so the second fork's own `stage:0` is already marked
  registered and its composed function never registers at all; its dispatch silently reuses the
  FIRST fork's Worker instead. `#124`'s own Settle first already names ARM naming as "undesigned."

`.once(eventName, fn)` is not supported as "handle exactly one chunk": dispatch reads
`emitter.listeners(eventName)` and invokes each function directly (the reason above - `emit()`
cannot catch a throw after `await`), so Node's own once-unwrap machinery, which lives INSIDE
`EventEmitter.emit()`, never runs - measured, a Worker registered via `.once()` alongside the
composed function (`listenerCount` `2`) still fired on a SECOND, later chunk, `listenerCount`
unchanged at `2` after both calls.

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
`Drainable<T>` - `{ syncChunks, items, chunks, context }` (`types.ts`, #133; three independent
re-spellings of this exact 4-field shape collapsed to the one type - `BranchOwner.drainable()` and
`PipelineResult`'s own field each used to declare it inline). Each terminal calls it exactly once
and threads what it got into its own async arm; calling it again there ran a user's `.local(build)`
callback twice per call. `PipelineResult.forEach()`/`[Symbol.iterator]()` and `utils/cut.ts`'s
`collectItems()` share one `dispatchSync(syncChunks, onSync, onAsync)` (`utils/drain.ts`, #133) for
the "is there a sync chunk stream, or not" branch every one of them used to test inline.

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
	demux                                                     orchestrator
		predicates, a plain loop   never dispatched
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

Nothing here is new machinery: the demux is `Transformer.reduce()`'s own shape - fold one chunk, keep
no state between chunks - and the join is `settleMaybe` + `chain`. That reuse is what makes the Mode
rule reachable rather than aspirational: every arm synchronous creates ZERO promises, and one
asynchronous arm widens the whole record to a single `Promise` while its synchronous siblings are
never wrapped.

An arm's stages address themselves under `/branch/<i>/<name>/`, the branch positional so two
`.branch()` calls may each declare an arm called `rest`, the arm by name. Without the trail an arm's
stage 0 collided with the parent's on the worker: measured, the parent's map ran twice
(`300 -> 360 -> 432`) and the arm's own transform never ran. The name must survive a URL path, so the
builder refuses one that would not.

## Benchmarks - pending #11

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

## Internal overhead benchmarks - pending #120

`bench/` is a committed, in-repo, single-runtime harness - independent of `benchmarks/` above, which
stays the Docker/six-runtime/`npm pack` comparison against OTHER libraries. This one compares the
package against ITSELF: one leg per pipeline runner class (`Pipeline`, `ConcurrentPipeline`,
`HttpPipeline`, `ClusterPipeline`), each against a hand-rolled, output-matched, non-`Pipeline`
equivalent - the quickest in-process code producing the identical result, even where that skips a
real network/IPC boundary a dispatching class would cross. A committed baseline gates future runs
(20% tolerance on absolute ns/row, 10% on the ratio, one warm-up round discarded); each dispatching
class's own `.local()` row is measured and its correctness asserted (a pinned region never reaches
`stageWork()`/serves a request/runs on a worker pid).

## Constraints in dependencies

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
  `{ kind: "keep"; value: U } | { kind: "drop" }` wrapper restores it, at 254.4 ns/row for the match
  plus 13.7 to build the wrappers, against 9.6 ns/row for a bare `!== DROP` identity check.
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
- `undici`'s `fetch` costs +373 us per request against `node:http` with a keep-alive agent, measured at
  one payload size on Node 26 with socket reuse confirmed on both (0 new TCP connections per 100
  requests). Runtime neutrality was chosen over that cost (#17); Bun and Deno ship their own `fetch`,
  so the number is Node-specific.
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
- `WorkerSet`'s own `registry` field (`cluster.ts`, one per-process singleton since #133 - was 5
  separate module-level bindings) never evicts an entry - every distinct `ClusterPipeline`
  constructed in a process stays reachable for that process's life. Sound for the documented
  construction pattern (one `ClusterPipeline` per logical chain, built once at module scope, the
  same "no top-level side effects beyond registering transforms" rule above already assumes); a
  caller constructing a fresh `ClusterPipeline` per request grows the registry unbounded.
- The idle-kill window between a `ClusterPipeline`'s last dispatch and its workers being killed
  (`cluster.ts`'s `IDLE_KILL_MS`) is `500`ms - a chosen value, not a tuned or caller-facing one. Long
  enough that back-to-back dispatches in a real workload never trigger a re-fork (~50-60ms per the
  measurement above); short enough that a script holding only the canonical `ClusterPipeline`
  example exits on its own well inside a normal test timeout.

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
```

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
