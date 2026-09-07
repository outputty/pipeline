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
  types.ts              PipelineFunction, IContextManager, InternalTransformer, every options interface
  pipeline.ts            Pipeline: source + context + terminal ops + Pipeline.merge + createPipeline()
  transformer.ts          Transformer: the chainable map/filter/reduce/tap/catch chain
  pipelines/
    concurrent.ts          ConcurrentPipeline - the fan-out (fanOutOrdered/fanOutUnordered), stageWork()
    http.ts                 HttpPipeline - stageWork() override (POST), .fetch(), toNodeHandler
    cluster.ts               ClusterPipeline - worker bootstrap, the shared pipeline registry
  context/
    types.ts              re-exported IContextManager shape
    simple.ts              SimpleContextManager - the one shipped IContextManager
  errors/
    handler.ts              ErrorHandler - runs a ChunkErrorHandler, used by Transformer.catch()
  utils/
    chunk.ts                buildChunkGenerator - breaks an AsyncIterable into fixed-size chunks
    helpers.ts               isContextAware / isContextAwareReduce - fn.length arity checks
  factories.ts             createTransformer - Transformer construction sugar (pending #39: no
                             chunk-size parameter - chunking lives on Pipeline, not Transformer)
  index.ts                 barrel - the only export surface
```

## How a chunk flows

Pending #39: the cut moves off `Transformer` onto `Pipeline`. `Pipeline` owns a persisted chunk
stream (`_chunks`), cut once by `.buffer(size)` (defaulting to `DEFAULT_CHUNK_SIZE` the moment one is
first needed) and carried unchanged through every later stage - `Transformer.process()` never cuts,
only processes whatever chunk it is handed:

```text
Pipeline.buffer(size)                             the ONLY place a cut happens - explicit, opt-in
	buildChunkGenerator(items, size)                splits the flattened item stream into In[] chunks
Pipeline.apply(transformer) (every later stage)
	Transformer.process(this._chunks, context)      NO cut here - runs the chunks it is handed
		runSequentially(internalTransformer, chunks, context)   one chunk at a time, in order
			internalTransformer(chunk, ctx)          one map/filter/flatMap/reduce/tap link, chained
				isContextAware(fn) ? fn(item, ctx) : fn(item)      arity-checked once per link, not per item
Pipeline.toArray() (or any terminal op, or async iteration)
	flatten(_chunks)                                the ONE place chunks become items again
```

A `ConcurrentPipeline`/`HttpPipeline`/`ClusterPipeline` stage bypasses `Transformer.process()`
entirely - see "The pipeline family", below - but shares the SAME `_chunks` state: its own fan-out
reads `this._chunks` directly and cuts none of its own, so a custom `.buffer()` boundary reaches a
dispatched stage exactly like a local one. Wrap the chain in one of those classes for concurrency
instead of configuring the `Transformer`.

`.catch(build, onError)` wraps one internal transformer function in a try/catch at the CHUNK boundary:
a throw inside `build`'s chain hands the whole failing chunk to `onError`, whose return value (an
array, or nothing) replaces or drops it. The unit of failure is the chunk, never the row - there is no
per-item try/catch anywhere in the chain. `ErrorHandler.handle()` (`errors/handler.ts`) runs every
registered handler LIFO (last-registered first) and returns the FIRST one that returns an array; a
handler returning `undefined` passes to the next-oldest one, and `handle()` itself returns `undefined`
once every handler has passed, which `.catch()` reads as "drop the chunk" (#15). `.onError()`'s own
call into the same `handle()`, from `Transformer.process()`'s catch (pending #39: was `execute()`'s),
ignores this return value - it is a notification hook there, never a recovery path.

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
`{ local: true }` on `.transform()`/`.apply()` is `super.apply(transformer)` at every level - a
plain `Pipeline`'s own sequential, in-process `apply()`, needing no per-level code.

Two mechanics make it work. `Pipeline`'s copy-on-write methods construct via a `protected
createPipeline()` calling `this.constructor` rather than a hard-coded `new Pipeline<U>`, so a
subclass survives a `.transform()`/`.context()`/`.buffer()` chain; each level overrides
`createPipeline()` again to carry its OWN extra knobs forward (`ConcurrentPipeline`'s own
`concurrentOptions()` helper is the one place `maxConcurrency`/`ordered` are listed - pending #39:
`chunkSize` drops out of it, since `.buffer()` is `Pipeline`'s own knob now, not
`ConcurrentPipelineOptions`' - so `HttpPipeline`/`ClusterPipeline` only add their own field). And a
stage's identity is its INDEX in `_chunkTransforms` - the table `apply()` already maintains - so a
dispatching class sends a chunk plus an index, never a function. Every instance runs the same code,
so index N means the same transform on both sides; a mixed-version fleet breaks that assumption
silently, which is why atomic deploys are a documented requirement rather than a check.

`ConcurrentPipeline.apply()` does NOT call `transformer.process()` for a non-local stage - that
bypass IS the mechanism, since `process()` runs a chain sequentially, one chunk at a time. Pending
#39: it fans `this._chunks` - the pipeline's OWN already-cut chunk stream, set by `.buffer()` - out
through `stageWork()`'s return value directly, cutting none of its own; a knob that only ever takes
effect via `process()` (`.withHooks()`, `.onError()`) THROWS immediately on a non-local stage instead
of silently never firing; `{ local: true }` is the escape hatch. `.buffer()` reaches a dispatched
stage exactly like a local one now, since `Pipeline` owns the cut, not `Transformer` - the refusal
this used to need for a custom chunker (`inertKnobsOf` naming `setChunker`) has nothing left to
refuse.

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
- `ts-pattern` is a declared runtime dependency that `src/` never imports. Verified by a repo-wide
  search whose control target (`p-limit`) returned 4 hits. Dropped by #5.
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
- `toNodeHandler` buffers BOTH directions today, defeating that duplex capability for every Node
  consumer. `handleOverBridge` collects `req` into a `Buffer` before building the `Request` and ends
  with `res.end(Buffer.from(await response.arrayBuffer()))`. Measured: a handler echoing per item saw
  nothing until the client closed its body at +457ms, then the client received all three replies in
  ONE frame at +477ms. No data is lost - it is purely a streaming defect, invisible to the one-shot
  `/stage/<n>` route. `Readable.toWeb(req)` as the request body plus a `for await` pipe of the
  response into `res` fixes it: first reply back at +163ms, `frames delivered BEFORE the request body
  closed = 2 of 3` (#45). Bun and Deno mount `.fetch` directly and already stream.
- One long-lived HTTP request stays inside ONE `node:cluster` worker for its whole life, so a
  streaming reducer's accumulator lives in the connection rather than in a session store. Measured
  with 4 workers on the shared port and 5 chunks fed 100ms apart: `DISTINCT PIDS THAT SERVED THIS ONE
  STREAM = 1`, all 9 items folded there. No separate reducer worker and no affinity mechanism is
  needed (#45).

## The reduce stage (pending #45)

A reducer is a fold with cross-chunk state, so it does not fit `InternalTransformer` (`chunk` in,
`Out[]` out, one output chunk per input chunk). Its shape is a stream operator, and `reduceWork()` is
the one method a subclass overrides, mirroring `stageWork()`:

```text
Pipeline.reduce(fn, initial, options?)
	reduceWork(fn, initial, stageIndex)            the per-class override
		ConcurrentPipeline    fold in-process, sequentially     maxConcurrency inert: one accumulator
		HttpPipeline          one duplex POST /reduce/<n>       accumulator lives in the connection
		ClusterPipeline       inherits it, via routePath()      one worker serves the whole stream
	emit(value)                                     buffered, yielded as its chunk
	final accumulator                               only if items were folded since the last emit
```

`ReduceFunction<U, T> = (acc, item, ctx, emit) => U | Promise<U>` puts `emit` FOURTH so `ctx` keeps
arity 3 and `isContextAwareReduce`'s `fn.length` check is untouched. A reduce stage takes the next
index in the SHARED stage-index space, so `/stage/<n>` and `/reduce/<n>` never collide; the reducers
live in `_reduceStages` keyed by that index, the `_chunkTransforms` slot holds a placeholder that
throws if replayed, and the index enters `sourcePositionViolations` so async iteration fails loud
rather than folding silently in-process.

The wire is NDJSON both ways over one POST: `{"context":{…}}` once, then `{"chunk":[…]}` per upstream
chunk, with `{"emit":[…]}` frames coming back as they happen and `{"error":"…"}` for a mid-stream
failure. That failure arrives AFTER the 200, so values already emitted have already entered
downstream stages - the price paid for results that arrive as they happen, and the same property
that killed the pull topology (`.claude/roadmap.md`) accepted deliberately here.
