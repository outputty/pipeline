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

## Module layout

```text
src/
  types.ts              PipelineFunction, IContextManager, InternalTransformer, every options
                          interface, plus DROP/RowErrorHandler/PipelineErrorHandler/RunScope (#78)
  pipeline.ts            Pipeline: source + context + terminal ops + Pipeline.merge +
                          createPipeline() + onError() (#78)
  transformer.ts          Transformer: the chainable map/filter/reduce/tap chain, plus onError()
                          (the row handler, #78) and runnable() (the seam that carries it in)
  pipelines/
    concurrent.ts          ConcurrentPipeline - the fan-out (fanOutOrdered/fanOutUnordered),
                             stageWork()/reduceWork()
    http.ts                 HttpPipeline - stageWork()/reduceWork() overrides, routePath(verb,
                             index), .fetch() (/stage/<n> and /reduce/<n>), toNodeHandler
    cluster.ts               ClusterPipeline - worker bootstrap, the shared pipeline registry,
                             bootstrapAndSetUrl() shared by stageWork()/reduceWork()
  context/
    types.ts              re-exported IContextManager shape
    simple.ts              SimpleContextManager - the one shipped IContextManager
  utils/
    chunk.ts                buildChunkGenerator (cuts) / flattenChunks (undoes) / normalize (dead
                             in production code post-#39, kept as public API)
    helpers.ts               isContextAware - fn.length arity check (isContextAwareReduce, its
                             reduce-side twin, is gone: every reduce path always passes all four
                             ReduceFunction arguments, #45); dropOrRethrow - the run handler's own
                             "call it, or propagate" decision, shared by runSequentially and
                             ConcurrentPipeline.apply()'s wrapped work (#78)
    reduce.ts                Reducer/foldChunk/foldChunkStream - the shared fold, used by
                             Transformer.reduce, Pipeline.reduce and http.ts's own frame folding;
                             Reducer takes an optional row handler (#78)
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

**Pending #88**: `.buffer()` gains a second overload, `.buffer(fn: BufferFunction<T>)`, for a chunk
boundary decided by something other than count. `buildChunkGenerator(size)` is a `T[]` fold with an
implicit accumulator (push, flush at `size`, reset) - the same shape `Reducer<T[], T>` (`src/utils/
reduce.ts`, "The reduce stage" section below) already runs for `.reduce()`. `.buffer(fn)` reuses that
class directly: the pending `T[]` is framework-owned, never handed to `fn`; `fn`'s own three
parameters are `(item, ctx, emit)`, `emit()` taking no value since there is nothing for the caller to
choose beyond "flush now." `.buffer(size)` becomes the same mechanism with an identity `fn` and an
internal auto-flush at `pending.length >= size` - one engine behind both overloads, spiked for real
during planning (a hand-rolled adapter over the unmodified `Reducer` class reproduced both a
five-minute, item-triggered window and a size-3 buffer from the identical fold). `ChunkerFunction<T>`
(`src/types.ts`) - the raw `(data: AsyncIterable<T>) => AsyncGenerator<T[]>` shape
`Transformer.setChunker()` left behind, exported with zero consumers since #39 - is deleted from the
public surface rather than reused: it carries no `ctx` parameter, so it cannot express a windowing
rule that reads shared context. `buildChunkGenerator` itself stays, now only as branch dispatch's own
internal single-item-chunk helper (`pushBranchOutput`, below). Out of reach by construction: a window
that must close after real time elapses with no new item arriving, since `fn` only ever runs from
inside the item loop, never while parked on the source's own `next()` - unaddressed by #88, a
different mechanism if ever wanted.

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

Async iteration (`for await` over a `Pipeline`, the `outputty/laygo` `m.from(pipeline)` seam) reads
the exact same persisted `_chunks` every terminal op reads (#39) - there is no separate replay path
any more. `.apply()` already ran `Transformer.process()` when it built `_chunks`, lazily, so `.tap()`
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
```

Each level overrides ONE thing. `ConcurrentPipeline` owns the fan-out window (`fanOutOrdered`/
`fanOutUnordered`) and the default in-process `stageWork()`; `HttpPipeline` overrides `stageWork()`
alone to POST instead, adds `.fetch()`/`stagePath()`/`toNodeHandler`; `ClusterPipeline` adds the
worker bootstrap, wraps `stageWork()` to lazily bootstrap on first dispatch, and overrides
`stagePath()` to route several pipeline definitions through one shared worker server
(`/pipeline/<i>/stage/<n>`, `<i>` a construction-order index reproduced identically by every worker).
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
createPipeline()` calling `this.constructor` rather than a hard-coded `new Pipeline<U>`, so a
subclass survives a `.transform()`/`.context()`/`.buffer()`/`.merge()` chain; each level overrides
`createPipeline()` again to carry its OWN extra knobs forward (`ConcurrentPipeline`'s own
`concurrentOptions()` helper is the one place `maxConcurrency`/`ordered` are listed - `chunkSize`
dropped out of it (#39), since `.buffer()` is `Pipeline`'s own knob now, not
`ConcurrentPipelineOptions`' - so `HttpPipeline`/`ClusterPipeline` only add their own field). And a
stage's identity is its INDEX in `_chunkTransforms` - the table `apply()` already maintains - so a
dispatching class sends a chunk plus an index, never a function. Every instance runs the same code,
so index N means the same transform on both sides; a mixed-version fleet breaks that assumption
silently, which is why atomic deploys are a documented requirement rather than a check.

`pipeline.merge(...others)` (#41) is the instance-method sibling of the static `Pipeline.merge()`,
and goes through the SAME `createPipeline()` seam - the reason it never restarts `_chunkTransforms`
at 0 the way the static's own hard-coded `new Pipeline(...)` does. The static builds a fresh, class-
less pipeline because it has no instance of its own to continue; the instance method has one, so it
carries THIS pipeline's own class, knobs and stage table forward instead of starting over. Both
share one context-merge loop and one chunk-concatenation generator (`mergeContextsInto()`/
`concatChunks()`, `src/pipeline.ts`) rather than two independent copies of the same logic.

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

## Constraints in dependencies

- TypeScript removed `baseUrl` at 7.0; a tsconfig that sets it fails with `TS5102`.
- A conditional type distributes only over a naked type parameter. `Ps[number] extends Pipeline<infer
  U> ? U : never` is an indexed access, so it compiles and evaluates to `never`; `Pipeline.merge`
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
  `/stage/0`, not `/pipeline/stage/0`. A mountable handler must be prefix-agnostic.
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
- `ClusterPipeline`'s module-level pipeline registry (`cluster.ts`) never evicts an entry - every
  distinct `ClusterPipeline` constructed in a process stays reachable for that process's life. Sound
  for the documented construction pattern (one `ClusterPipeline` per logical chain, built once at
  module scope, the same "no top-level side effects beyond registering transforms" rule above already
  assumes); a caller constructing a fresh `ClusterPipeline` per request grows the registry unbounded.
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
  the one-shot `/stage/<n>` route. `Readable.toWeb(req)` as the request body plus `writeStreamedBody`
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
so `/stage/<n>` and `/reduce/<n>` never collide: `pushReduceStage()` (`src/pipeline.ts`, shared by
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
