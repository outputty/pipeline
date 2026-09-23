# @outputty/pipeline - Roadmap

## Next

- **Cross-runtime benchmarks** (#193) - compare the package against `ix`, `streaming-iterables`, `effect`, `rxjs` and runtime stream helpers on six pinned runtimes; layout in `.claude/architecture.md`'s Benchmarks section.
- **Every exported symbol's TSDoc follows one convention, gated by `typedoc`** (#118) - `typedoc --validation.notDocumented` catches a missing docstring or a stale `@param` name.

## Later

- **A distributed event emitter layer for `EventEmitterPipeline`** - exactly-once delivery across a Worker pool via `share()` free-slot dealing, plus a per-stage `.concurrency(n)`.
- **A `ContextManager`'s write-back to the orchestrator** - a remote stage's `ctx.set()` never reaches the orchestrator; open for an in-memory manager with no backing store.
- **A retry policy for a failed remote chunk** - a retry on another instance runs the chunk twice (at-least-once) with no de-duplication surface.
- **An executable docs harness** - machine-check every `<!-- compiles -->`/`<!-- illustrative -->` fence in `product.md`/`architecture.md`/`README.md`.
- **A `Pipeline`-against-real-`Layer` integration proof** - once both packages publish to npm and a consumer can install both.

## Killed

- **The empty-stream seed inside `Reducer.final()`** - a dispatched partition builds its own `Reducer` with no chunk, so `[]` returned one seed per partition; the seed belongs to the stage.
- **A seed per partition** - one seed per `.reduce()` call; the partition count is a ceiling, so a non-identity seed would repeat.
- **"No chunk was yielded" as the seed rule on every arm** - wrong on the sync arm, where a slot can resolve empty; kept only at a partitioned stage's merged output.
- **`Transformer.reduce` left unchanged for an empty chunk** - it must give the same answer as `Pipeline.reduce`; a filter-emptied chunk is indistinguishable from `[]`.
- **A lazy `import("ws")` inside `WebSocketPipeline`** - the root `.d.ts` graph still reached `ws`, so `ws` stayed a mandatory install.
- **CJS output left unsplit under the second entry** - `dist/websocket.cjs` carried its own `Pipeline` copy and broke `instanceof`; `splitting: true` instead.
- **`NodeWebSocketHandler.upgrade` typed off `ws`'s `handleUpgrade`** - left a `ws` import in the published `.d.ts` (`TS7016` without `@types/ws`).
- **Colon-prefixed arm event names** - the route string HTTP already dispatches to names a stage on every class.
- **Renaming arm events with the composed function still a listener** - two forks of one base still shared one registration; no naming scheme separates forks.
- **A by-reference codec with the core unchanged** - a `.buffer()` recut packed handles together, `.tap()` saw handles, and emptied chunks went undetected.
- **Fusing consecutive dispatched stages into one hop** - changes stage identity on the wire; the core-aware encoded chunk reaches the same hop.
- **An exported `referenceCodec(store, { inner })` helper** - two codecs to juggle and storage is the caller's business; a by-reference codec is a caller's own class.
- **`HttpPipeline.reduceWork()` as multiple POSTs with a client-carried accumulator** - every POST resends the whole accumulator, so wire bytes grow roughly with N².
- **Normalizing an async generator source through a hand-rolled iterator** - the cost is the generator protocol itself, unreachable from any consuming shape.
- **A synchronous `.local()` region on a dispatching class** - the region was already free; the real costs were per-row habits, fixed without a Mode change.
- **Gating `nsPerRow` in the memory suite** - too noisy to gate; `bench/overhead.ts` already gates speed.
- **Fusing adjacent sync `map`/`filter` links into one loop** - the general form is a larger composition contract; the narrow peephole catches one pair only.
- **A numeric complexity/line-count lint gate** - unit size is an architectural question, judged in review, not a numeric cap.
- **A `Runner` class that takes a built pipeline** - forced the pipeline to be named twice; the wrapping classes take `(pipeline, options)` and are callable.
- **A `Chain` type between the builder and the pipeline** - the builder already carries its own stage list.
- **Detecting whether an input can be re-drained** - no reliable test (a `ReadableStream` locks on probing); every terminal re-drains.
- **`class Pipeline extends Function`** - `super()` throws `EvalError` wherever code generation is banned; `Pipeline.prototype` is reparented instead.
- **A conformance suite every `Pipeline` and Context class runs** - each reason a case could not run everywhere was a defect, fixed directly.
- **A `.catch()`-shaped per-row region** - per-row execution changes what a chunk-aware link inside the region means.
- **`Promise.allSettled` as the per-row mechanism** - a synchronous throw escapes during the array build, before `allSettled` runs.
- **A sequential fold or an optimistic re-run as the per-row mechanism** - both collapse on an async callback, and the re-run repeats side effects.
- **`Pipeline.onError()` as a catch on the drain side** - a thrown async generator is finished, so later rows are lost.
- **`ts-pattern` for the `DROP` sentinel** - a first runtime dependency, and `.exhaustive()` is not callable at the generic sites.
- **The combine debt (`owesCombine`/`assertCombined()`)** - forced a two-step ritual on every partitioned reduce; each partition's result flows downstream instead.
- **A phantom-type compile-time guard for the combine debt** - deleted with the mechanism it guarded.
- **`EventEmitterPipeline` as an observability class** - `.tap()` already observes, attaches mid-drain and isolates a throwing listener.
- **`EventEmitterPipeline` as a round-robin dispatch mode** - dropped with concurrency control on the user's scope pullback; broadcast dispatch shipped.
- **A forward-descending `Transformer` composition** - makes every link a middleware, a larger contract than `map`/`filter`/`reduce` need.
- **A worker-thread pool** - loopback HTTP is faster at all but the smallest chunks, and almost every real chunk sits in that range.
- **`workerPool()` as an execution-strategy factory** - a strategy discards the composed chain, so a caller's `.map()` silently never runs.
- **Delegation as a plain async `transform`** - killed with the strategy seam itself.
- **A caller-owned stage map, named by string** - a stage's position already identifies it.
- **The pull topology** - duplicate streams, doubled requests, errors after a 200, and a deadlock under one connection.
- **"Pull is naturally backpressured"** - backpressure engaged only past several MB in flight.
- **`get-port`** - `listen(0)` already shares a port inside cluster, with no check-then-bind race.
- **A single-stage `.local(transformer)` method** - superseded by the `.local(build)` region builder.
- **Wire-level drift protection** - fingerprints and version strings priced out; atomic deployment is documented instead.
- **Patterns 2-4 of the multicore research** - a data-URL worker cannot import workspace modules, and transfers still copy object chunks.
- **`workerpool`'s dynamic function offloading** - sends the function as a string and evals it.
- **An open `ExecutorType`, by any mechanism** - died with the named registry; a strategy is passed as a function.
- **`registerExecutor` and a named executor registry** - duplicate injection path backed by process-wide mutable state.
- **`appliesInSourcePosition` on `ExecutionStrategy`** - a flag with one true implementation.
- **`createConcurrentTransformer`** - a duplicate of what `concurrent()` already says.
