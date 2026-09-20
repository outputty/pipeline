# @outputty/pipeline - Roadmap

Why each open ticket is worth building, and now. Status lives on GitHub Issues, not here: every ticket
is a GitHub issue, labelled `ready` (buildable) or `needs-planning` (grill it with `/plan` first), and
`gh` derives what is unblocked. This file is the durable index and the killed-idea dedup surface.

**Read the whole file** before evaluating an idea or closing work. A new idea is often a row that
already exists (Building / Later), or one already tried (Killed) - point the new one at that row.

## Building - open tickets, detail in each issue

- **Cross-runtime benchmarks** (#11) - the package ships no numbers, so nothing compares it against
  `ix`, `streaming-iterables`, `effect`, `rxjs` or the runtime's own stream helpers, and a hot-path
  change has no baseline to regress against. Six pinned runtimes in Docker, two tables, results
  committed as JSON; the second table controls measured ITEMS IN FLIGHT rather than any declared
  concurrency option, because no two libraries name that knob the same way. Layout and rationale in
  `.claude/architecture.md`'s Benchmarks section.
- **Every exported symbol's TSDoc follows one convention, gated by `typedoc`** (#118) - `src/`'s
  docstrings mix what a symbol does with ticket numbers, benchmarks and mechanism narration that
  duplicates `.claude/architecture.md`, and some describe machinery #90 already deleted
  (`Pipeline.buffer()` still narrated `_preBufferItems`, gone since #90's own rewrite). Several
  current exports carry no docstring at all (`Pipeline`'s own call signature, `BranchOwner.drainable`),
  and nothing checks a `@param` name or a missing description - #90 alone renamed or deleted dozens
  of signatures in one stack with no tool catching drift. Now, because #90 and #113 landing the same
  day this was planned is exactly the kind of large, fast-moving change this gap lets through
  silently; `typedoc --validation.notDocumented` is proven this session to catch it for real
  (`Pipeline.local has an @param with name "wrongName", which was not used`).

### Later - not yet filed

- **A distributed event emitter solution layer for `EventEmitterPipeline`** (#124's own planning) -
  exactly-once delivery across a pool of Workers (one chunk per Worker, no redundant work) and
  per-stage concurrency control, both spiked and measured working during `#124`'s planning, then
  deliberately dropped from `#124` itself on the user's own simplification request ("not even
  think about concurrency at this stage... a distributed event emitter solution layer" is future
  work). Measured: plain broadcast dispatch floods a slow Worker with overlapping chunks once
  `maxConcurrency` exceeds the registered Worker count (`max simultaneous invocations of the SAME
  registered worker function: 2` at `maxConcurrency: 4` with one slow Worker). The fix, verified
  working: `share()` (`src/utils/chunk.ts:454`, the same free-slot-dealing mechanism
  `ConcurrentPipeline.reduce()` already uses to partition) instead of push-based dispatch - a slow
  Worker then never receives a second chunk while its first is pending (`max overlap … 1`), and
  registering the same Worker function twice gives real, additive capacity (two concurrent
  invocations, ~32ms for 6 chunks at 30ms each against a serialized ~180ms). A `.concurrency(n)`
  method mirroring `.buffer(size)`'s own shape (persists per-stage until called again) was also
  built and verified for per-stage control. Start from these numbers rather than re-deriving them.
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

- **`ws` is optional: the WebSocket runners move to `@outputty/pipeline/websocket`** (#239, `feat!`,
  PRs #240 (L1) and this docs PR) - `require("@outputty/pipeline")` failed with `Cannot find module
  'ws'` even for a plain `Pipeline`, because the root entry imported the package at load time.
  `WebSocketPipeline`, `ClusterPipeline`, `toNodeWebSocketHandler`, `PipelineSocket`,
  `NodeWebSocketHandler` and the two options types left the root for a second tsup entry (BREAKING,
  `minor`), `ws` became an optional peer dependency, and `NodeWebSocketHandler.upgrade` is typed with
  Node's own `IncomingMessage`/`Duplex` so no `.d.ts` names a `ws` type. Planning found what a unit
  run cannot see: tsup does not split CJS by default, and two `Pipeline` copies made `instanceof`
  false across the entries. `packaging.e2e.test.ts` builds and runs `dist` for it. Runtime cost
  against the base commit: promises per row, collections and allocation unchanged on every
  `bench/memory.ts` case.
- **The source, simplified with the public API unchanged** (#232, `refactor`, PRs #233 (L1)/#234
  (L2)/#236 (L3) and this docs PR) - a read-only survey of every module scored code and docstring
  complexity separately, and the stack landed the candidates that deleted a whole pattern or shrank
  the code: `normalize` and its 3-case test file, `dispatchSync`, the utils barrel, five one-caller
  helpers, 19 "Python equivalent" porting blocks, one `parseRoute` for `HttpPipeline` and
  `WebSocketPipeline`, and one private `cutBy` behind both `.buffer()` forms. Runtime cost against
  the base commit: garbage collections and peak heap unchanged on every `bench/memory.ts` case,
  promises per row down 7-8% on the `ConcurrentPipeline` cases, sync `.buffer(fn)` faster. Left as
  decisions, each changing a public surface: `createTransformer`, `flatten`, `shortCircuit`, the
  `EventEmitterPipeline` lifecycle events, and `toNodeHandler`'s pre-first-byte failure.

- **`EventEmitterPipeline` events read as routes, the composed function never a listener** (#221,
  `feat!`, PR #226 (L1)/#227 (L2) and this docs PR) - events renamed from a flat `stage:<n>`
  counter to the route the chain was built along (`/transform/<n>`, a `.branch()` arm's own
  `/branch/<i>/<name>/transform/<n>`, a trail-level `:end`), and `stageWork()` calls the chain's
  own composed function directly instead of registering it on `pipeline.emitter`. Fixes four
  documented collisions - a `.branch()` arm dispatching on the parent's own stage, two sibling
  arms, two forks of one chain, and two independently-constructed pipelines sharing one
  `emitter` - each of which used to silently answer with (or lose to) another chain's output;
  every one now answers with its own, proven by a real run per case rather than by reasoning
  about the deleted `Set`. Trades away three guarantees: `listenerCount` no longer counts the
  composed function, `off()` no longer takes a stage over, and a stage with no
  caller-registered Worker no longer rejects - the composed function always answers. Breaking,
  no deprecation period.
- **A `Codec` interface with a `JsonCodec` class, and dispatched replies stay encoded in the
  primary** (#209, `feat`, PR #216 (L1)/#217 (L2)/#219 (L3)/#220 (enable) and this docs PR) - `Codec`
  and `JsonCodec` move to `src/codec.ts`, outside the file that loads `ws`, so a core chunk type can
  name `Codec` with no utils-to-pipelines import edge; the `jsonCodec` object is deleted (BREAKING).
  A dispatched WebSocket reply stays bytes-on-the-wire in the orchestrating process until a site
  that reads items decodes it - `drainable()`, the `.local()` seed, `flattenChunks` - a later
  dispatched stage on the same codec forwards the payload verbatim, and a chunk a prior stage
  emptied is never re-dispatched, on both a plain dispatched transform and a dispatched `.reduce()`.
  `.consume()` decodes nothing at all. `Pipeline.mayCarryEncodedChunks()` (`true` only on
  `WebSocketPipeline`) replaces the build's own internal flag once shipped, so a plain
  `Pipeline`/`ConcurrentPipeline`/`HttpPipeline`/`EventEmitterPipeline` chain pays nothing for a
  mechanism it can never carry - closing a real `bench:memory` regression the naive unconditional
  form introduced. Measured (`__tests__/codec.e2e.test.ts`): the primary's own decode/encode count
  fell from 16/16 to 8/8 across two dispatched stages, an emptied chunk cut the server's own decode
  count from 16 to 8, and a dispatched reduce decoded only at the terminal - matching planning's own
  spike 2 findings exactly. ⚠ A `codec.decode()` failure now rejects the whole drain rather than
  being dropped by `Pipeline.onError()`'s per-chunk contract - confirmed with the user as final,
  documented behavior, not a defect owed a fix.
- **`ClusterPipeline` accepts `codec`, `ClusterHttpPipeline` accepts `client`** (#208, `feat`, PR
  #214) - both classes already forwarded the field to `super` at runtime; only the exported options
  types refused it, so every caller had to cast. `ClusterPipelineOptions` and
  `ClusterHttpPipelineOptions` (`src/pipelines/cluster.ts`) now widen to include them, proven by two
  subprocess fixtures: a file-backed codec that sends only a 36-byte key over the wire (each of 2
  workers decodes only that key, never the real value) and a counting `client` that shows every
  chunk dispatch from the primary goes through it.
- **`WebSocketPipeline`, a fourth dispatch mode, and `ClusterPipeline` reparented onto it** (#201,
  `feat`, PR #203 (L1)/#204 (L2)/#206 (L3) and this docs PR) - each chunk of a stage dispatched over
  one persistent, multiplexed `ws+unix:`/`ws://` connection instead of one HTTP request per chunk
  (`HttpPipeline`), one binary frame per dispatch (a 4-byte length-prefixed JSON header, then the
  `codec`-encoded payload; a JSON TEXT frame on failure). `ClusterPipeline` - the class name every
  existing caller already imports (BREAKING, no deprecation period, no code change required) - now
  dispatches over that wire; `ClusterHttpPipeline` is the class's own pre-#201 identity, kept under
  that name unchanged for a caller who wants the old HTTP/TCP transport. Each worker binds its own
  unique `ws+unix:` socket path (a WebSocket connection is persistent, so a shared port would leave
  every worker but one un-dialed); dispatch round-robins across the bootstrapped set.

  Measured live: `ClusterPipeline`'s own `pnpm bench:overhead` row moved from 217.08 ns/row
  (HTTP-based, pre-#201) to roughly 69-71 ns/row - a 3.0-3.2x reduction, beating the planning spike's
  own composed ~33-35% estimate. Seven findings, each caught by review and fixed before merge: (1) a
  reduce stream's own chunk and `inputDone` frames, dispatched concurrently rather than queued per
  `id`, could settle out of order - a `[1,2,3,4,5]` sum returned `[]` instead of `[15]` when the
  trailing flush ran before the chunk it was meant to flush had folded. (2) `WorkerSet`/`WsWorkerSet`
  both iterated `cluster.workers`, a registry `node:cluster` shares PROCESS-WIDE - a process
  constructing both a `ClusterHttpPipeline` and a `ClusterPipeline` had one class's idle timer kill
  the other's still-in-flight workers. (3) `ClusterPipeline`'s own round-robin mutated a shared
  dispatch-target field between concurrent dispatches (`maxConcurrency > 1`); a later dispatch's own
  reassignment could land inside an earlier one's `await`s and make it reject a perfectly healthy
  connection as closed - fixed by capturing the target once per dispatch. (4) That capture stopped
  the wrong REJECTION, but the same shared field still raced on the WRITE: every partition of a
  `maxConcurrency: 2` reduce launches in one synchronous burst, so every partition's own round-robin
  write landed on the field before any of them read it back, collapsing all partitions onto whichever
  worker the LAST write picked - `resolveConnect()`, a hook called fresh per dispatch with nothing
  shared to race on, replaced the field entirely; a `maxConcurrency: 2` reduce reading
  `totalConnections: 1` now reads 2, asserted directly in `websocket-pipeline.e2e.test.ts`'s
  Done-when 4 test. (5) The connection counter itself was a plain incrementing total with no
  decrement, so a transient reconnect on one worker read as two connections - now a `Set` sized on
  query, with the socket deleted on close. (6) `reduceWork()`'s own connection setup could throw
  before entering the `try`/`finally` that releases it, leaking an in-flight count the idle-kill
  timer waits on forever. (7) `stageWork()`'s own dispatch registered a pending request before a
  synchronous `send()` could throw, leaking that entry on a socket adapter whose `send()` throws
  rather than silently drops (`ws`'s own does not; the seam is public, and another `PipelineSocket`
  implementation can).

  `ws` 8.21.3 was this package's first runtime dependency (an optional peer since #239) - `bufferutil`/`utf-8-validate` (its own
  optional native-acceleration peers) stay absent from `package.json`, and `pnpm build && grep -c
  "ws/lib" dist/index.js` prints `0`. `bench/baseline.json`'s own committed `ClusterPipeline` number
  stays the pre-#201 figure (`bench/*.ts` sat outside this ticket's own file scope) -
  `checkGate`'s regression-only design stays green regardless, but a future ticket updating the
  baseline for real would tighten it, never loosen anything.

- **A fifth benchmark leg, a fourth dispatching class's own legs, and five real findings** (#180,
  `perf`, PR #194/#195/#197/#198/#199/#<DOCS_PR>) - `bench/legs/branch.ts` measures `.branch()`
  against a hand-rolled floor producing the identical record, its own `LEG_TOLERANCE` (0.15) set from
  a measured five-run spread; `bench/memory.ts` gains a router case and a broadcast case, both
  committed to the baseline at `maxConcurrency: 4` on 2+ arms. `EventEmitterPipeline` (#124) gains a
  leg in `bench/overhead.ts` and two cases (dispatched, `.local()`) in `bench/memory.ts`, its own
  zero-Workers-while-pinned correctness assertion, and joins a new `checkLocalParity` gate
  (`bench/gate.ts`) comparing every dispatching class's own `.local()` cost against a bare
  `Pipeline`'s, from one report, no baseline file needed.

  Five findings. (1) `runBranch`'s collect-then-walk cost 2.27x a single-pass floor - FIXED:
  `classifyItems`/`classifyAsyncChunks` fuse collection and classification into one walk, reusing the
  `dispatchSync`/`drainSync` primitives every other synchronous drain already shares. (2)
  `EventEmitterPipeline`'s own `.local()` parity ratio reads 30% above its siblings - HALF explained:
  a swap probe isolated a measurement-order artefact in `Pipeline`'s own reading, but
  `EventEmitterPipeline`'s absolute cost stayed elevated regardless of position, unexplained further
  (`architecture.md`'s own Internal overhead benchmarks section). (3) `HttpPipeline`'s dispatched cost
  splits three ways by removing each part on the real path: round trip is 60-75% and already
  optimized (#179); encode and decode do not decompose into separate numbers by removal on this path
  - no fix lands, the round trip is the boundary itself. (4) `fromSource()`'s ~4 promises/row cost
  over a genuine async generator source is the language's OWN protocol floor, confirmed by a
  hand-rolled `.next()`-based consumer measuring identically to a plain `for await` drain (4.000
  against 4.001) - unfixable, `fromSource()` already sits within 0.3% of it. (5) `ordered:true`'s
  reorder buffer, measured on `heldAtEndMB` against `ordered:false` on the identical chain: the
  buffer's own real bound (under `(maxConcurrency - 1)` chunks) is below this instrument's resolution
  on the canonical chain - negligible, not the cause of the gap the case measures.

  `.buffer(size)`/`maxConcurrency` tuning guidance, re-measured on the current tree (README.md,
  `.claude/product.md`): buffer size dominates concurrency, and pushing `maxConcurrency` past 4 at
  the default buffer stopped helping on the canonical chain. `DEFAULT_CHUNK_SIZE`/`maxConcurrency`'s
  own defaults (1000/4) are unchanged.

- **The per-row costs the async engine was paying for chunk-shaped data** (#179, `perf`) - six fixes,
  each located by measurement rather than by reading, and the largest of them nowhere this document
  had been looking. `buildSyncChunkGenerator` cuts an array with `slice`; `settleRows` keeps one
  output array on its armed sync arm; every async terminal on `PipelineResult` walks `chunks()` in a
  synchronous inner loop and the flattened item view is deleted; `fromSource()` slices an array on
  the forced-async branch too; `.buffer(size)` cuts by count instead of folding every item through
  `Reducer<T[], T>`; and `HttpPipeline` dispatches through `options.client`, defaulting to
  `node:http` with a keep-alive agent on Node.

  Measured on `bench/overhead.ts`, median of five runs: `ConcurrentPipeline`'s gated `.local()` row
  278.43 ns/row to 16.47, `HttpPipeline`'s 252.76 to 18.06, `ClusterPipeline`'s 257.11 to 18.90, a
  bare `Pipeline` 27.46 to 16.79 - so a pinned region now costs within about 2 ns/row of a plain
  `Pipeline`. On `bench/memory.ts`, allocation per 500,000 rows fell 41% to 96% on every case and
  collections fell from 23-47 to 1 on the in-process ones, with nothing retained.

  Three things it also changed, each priced on its own: the constructor refuses a fractional
  `chunkSize` (BREAKING - it silently made an array and a `Set` cut differently), `.buffer(size)`
  validates on the bound path as well (BREAKING), and `Drainable<T>` drops its item view (BREAKING -
  `Pipeline.drainable()` is public). The `.local()` sync-region design an earlier draft of this
  ticket proposed is under **Killed**: measured, the region was already free.

- **A memory and scheduling benchmark suite, gated like the speed one** (#179, `perf`) -
  `bench/memory.ts` measures allocation, garbage collections, promises per row and retained heap for
  all four runner classes; `bench/memory-gate.ts` gates them against a committed baseline,
  regression-only; `bench/compare.ts` measures any git ref against the working tree in one command,
  which is what a build runs before its first edit and again before its docs layer. The instruments
  were chosen by comparing candidates on a real chain: a `heapUsed` delta called a 2x allocation CUT
  a 2.7x regression, and `PerformanceObserver` on `gc` reported zero collections for a run with 13,
  so allocation reads `v8.getHeapStatistics().total_allocated_bytes` and GC reads `v8.GCProfiler`.
  Both gates now carry per-leg, measured tolerances - one 20% number sat inside `ClusterPipeline`'s
  own 18.9% run-to-run spread while being three times looser than `ConcurrentPipeline` needed.

- **A benchmark harness for the package's own internal overhead, and closing the gap it finds**
  (#120, `perf`, PR #166/#167/#169/#170/#175/#176) - `bench/overhead.ts` measures one leg per
  pipeline runner class (`Pipeline`, `ConcurrentPipeline`, `HttpPipeline`, `ClusterPipeline`)
  against a hand-rolled, output-matched floor, gated on a committed baseline (absolute ns/row -
  `pipelineNsPerRow` for `Pipeline`, `local.nsPerRow` for a dispatching class, since its own
  DISPATCHED leg crosses a real network/IPC boundary whose jitter is not this package's own overhead;
  `.ratio` prints but is never gated, since dividing two noisy measurements compounds their noise past
  what a tight tolerance survives), plus each dispatching
  class's own `.local()` row proving a pinned region never dispatches (0 `stageWork()` calls, 0
  HTTP requests, every item on the primary's own pid). Found and fixed O1: `Transformer.filter()`'s
  sync no-handler branch paid for three passes over the chunk where `.map()` pays for two -
  collapsed to one (`filterSettle`/`filterStep`), dropping `Pipeline`'s own ns/row from ~50 to ~30,
  output unchanged. O2 (fusing adjacent sync `map`/`filter` links) was spiked against the same
  baseline and killed - 1.36x end to end, see Killed below. A user follow-up ("do a spike on how
  the async pipeline's performance could improve") found and fixed a real, measured cost inside the
  async-engine tax itself: `toAsyncIterable` hand-rolls its iterator instead of an `async
  function*`, `buildBufferGenerator` no longer awaits a fold result that was never a thenable, and
  `fromSource()` keeps a sync pre-buffer view alive under a forced-async Mode so `.buffer()` can
  fold through it synchronously - together roughly halving every dispatching class's own `.local()`
  row; see `bench/baseline.json` for the exact committed figures. Layout and the full table in
  `.claude/architecture.md`'s Internal overhead benchmarks section.
- **The node:/laygo import boundaries, mechanized as oxlint rules** (#117, `feat`) -
  `architecture.md`'s own stack diagram drew `node:cluster`/`node:http` as scoped to
  `ClusterPipeline`/`HttpPipeline` only, and this package's own split from laygo (#743-745) drew "no
  import edge in either direction" - nothing ever checked either claim, the same shape
  `outputty/laygo` already mechanizes as six `no-restricted-imports` overrides. Two new
  `.oxlintrc.json` overrides close both gaps: `import/no-nodejs-modules` scoped via `excludeFiles`
  (a real spike showed a hand-rolled `node:*` glob misses a subpath specifier, `node:fs/promises`) -
  and, caught live rather than assumed from the ticket's own text, `EventEmitterPipeline`'s
  `node:events` import (#124, shipped after this ticket's file list was written) needed adding as a
  THIRD exception, not two; `no-restricted-imports` banning `@outputty/laygo` and its subpaths, both
  `paths` and `patterns` verified live for the identical subpath gap. First candidate of a wider
  sweep for prose that is really a lint-mechanizable structural rule; the sweep found one more
  (`never class Pipeline extends Function`) with no oxlint 1.81 mechanism to enforce it, recorded as
  considered rather than converted.
  Widened mid-build, on the user's own pick, into clearing the 36-warning `anti-slop` backlog
  `.oxlintrc.json`'s own header comment had deferred: 13 real fixes (a discarded-value `unknown`
  becomes `void`, or a real generic parameter - `PipelineEmitter`, `ndjsonFrame`, `emitSafely`,
  `splitLines`/`drainable` widening; two `defer`/replay casts now read `item: any`, matching
  `AnyPipeline<any>`'s own established convention rather than a placeholder `unknown`);
  `.tap()`'s two sync overloads join those 13 as a `void` fix, and its two async overloads become
  a generic `Promise<R>` (a `Promise<void>` attempt was caught by `/code-review`, below) - plus a
  real runtime validator (`isReadyMessage`) replacing a blind IPC-message cast in
  `cluster.ts`. 23 sites verified genuinely load-bearing FROM SOURCE, not their docstrings alone -
  `Transformer.pipe()`'s own comment confirms `RowErrorHandler` carries forward across `.map()`/
  `.filter()` calls with a DIFFERENT item type each time, so no type narrower than `unknown` is
  sound; `IContextManager`'s own canonical example needs a manual `as number` cast to read a value
  back, confirming its bag is genuinely heterogeneous - carry a disclosed `oxlint-disable-next-line`
  reason at the site instead of a cosmetic `<T = unknown>` default that would satisfy the lint
  rule's own AST check (it only refuses a non-generic alias) with zero real type-safety gain, a
  candidate spiked and killed during the same build. The four promoted rules move from `warn` to
  `error` in an override scoped to `src/**/*.ts` alone, not the top-level `rules` block - promoting
  globally would have turned every pre-existing `__tests__/` warning into a gate-breaking error too,
  caught by running the repo's own `pnpm lint` (not just the ticket's own `bunx oxlint src/`) before
  trusting the scope. `bunx oxlint src/` prints nothing and exits 0. PR #154 (both layers, one PR
  under the 200-line threshold - measured via `git diff --stat` before committing to a stack, not
  guessed).
- **`.buffer()` accepts a callback for custom buffering windows** (#88, `feat`) - `.buffer(size)`
  could only cut a chunk boundary by count, and the closest existing shape (`ChunkerFunction`) was
  dead code with no way to hand one to `.buffer()` at all. `.buffer(fn: BufferFunction<T>)` folds
  items through the SAME `Reducer<T[], T>` class `Pipeline.reduce()` already uses -
  `sizeReduceFunction`/`bufferReduceFunction` (`src/utils/reduce.ts`) adapt a size or a caller's own
  function onto it, one engine, not two; `.buffer(size)`'s own three branches keep their shape,
  their innermost cutting call swapped for the shared one. ⚠ The "one engine" half is undone by #179,
  which put `.buffer(size)` back on the ordinary chunk cutters and deleted `sizeReduceFunction`: a
  fold buys a per-ITEM decision that a count never makes, and charged a closure call plus an
  array-mutating accumulator per row for it. Chunk boundaries are identical either way, and
  `.buffer(fn)` keeps this engine. A `Promise`-returning `fn` widens the
  chain's Mode to `"async"`, two overloads ordered Promise-first, mirroring `.reduce()`'s own split.
  `ChunkerFunction` drops from the public export surface (dead since #39, zero consumers);
  `BufferFunction` takes its place. Code review found and fixed two real defects before merge: a
  recut-from-chunks sub-path collapsed a real, multi-item chunk's several emitted windows into one
  oversized chunk (`driveFold`, a shared tail-chaining engine, now yields each one separately); and
  `Reducer`'s own `itemsSinceEmit` gate - correct for `.reduce()`'s contract - silently dropped a
  stream's trailing chunk whenever its last item both flushed and repopulated the pending array in
  one call (`Reducer.current()`/`trailingOf` reads the real pending state instead), found while
  verifying the first fix with a real run rather than a hand-derived expected value.
  `.claude/architecture.md`'s own "buffer(fn) - a callback-driven chunk boundary" section has the
  full engine. PRs #144 (L1, pinned cases), #145 (L2, the engine + wiring), #147 (docs).
- **`.queue(n)` prefetches chunks ahead of the consumer** (#123, `feat`) - `.buffer()` pulls a chunk
  exactly when the consumer asks for it, so a slow producer or a slow consumer always pays the
  other's latency in full; `ConcurrentPipeline`'s own `fanOutUnordered` already overlaps pulling with
  WORK via a `Promise.race` pool, but only on that class, and only as a side effect of running
  `maxConcurrency` chunks concurrently. `share()` (checked first, per the reuse rule) turned out not
  to serve this - it is competitive pull for many consumers with zero storage, not a producer allowed
  to race ahead of one consumer. `prefetch()` (`src/utils/cut.ts`, beside `share()`) is a plain
  `async function*` mirroring `ConcurrentPipeline`'s own `fanOutOrdered`: an array of exactly
  `capacity` pending `upstream.next()` promises, refilled the instant the consumer takes the front
  one. Written as a generator rather than a hand-rolled `AsyncIterable` object, laziness and
  concurrent-caller safety (`share()`-based partitioning) come from the language's own generator
  semantics, not code this package had to write and verify itself - real, measured: 674ms serial vs
  542ms queued over a 100ms/item source and a 30ms/item stage, from overlap alone; two `.reduce()`
  partitions over 8 items summing to 36, no deadlock, no starvation. `Promise.race` was priced and
  killed twice during planning (proven to buy nothing over a single async generator source, which
  always serializes its own internal work); a `ReadableStream`/`CountQueuingStrategy` candidate was
  priced and killed (eager pull at construction, an off-by-one capacity bound). Code review found and
  fixed two real defects before merge: `.queue()` wasn't narrowed on the four dispatching subclasses
  (`ConcurrentPipeline`/`HttpPipeline`/`ClusterPipeline`/`EventEmitterPipeline`) - the identical gap
  `.buffer(fn)`'s async overload shipped unfixed in #88, closed here instead since a single signature
  made the four one-liner overrides cheap; and a test asserting an exact pull count depended on how
  many microtask ticks had run rather than on `capacity`, replaced with a poll-until-stable helper
  (`untilStable`, `__tests__/helpers/sequences.ts`). `.claude/architecture.md`'s own "Prefetching"
  section has the full engine. PRs #148 (L1, pinned cases), #149 (L2, the engine + wiring), #151
  (docs).
- **A repo-wide reuse and simplification pass** (#133, `refactor`) - 15 named duplications, each
  unified in its own layer, no observable output change anywhere: the canonical
  `new Pipeline<number>().transform((t) => t.map((x) => x * 2).filter((x) => x > 4))` example
  returns `[6, 8, 10]` before this stack and after every layer of it. `types.ts` gains
  `StageRegistries`/`Drainable<T>`/`ReduceWork<T,U>`/`RouteVerb`+`StageRoute`/`Tagged<R>`, each
  replacing a shape spelled inline 3-6 times with no shared name; `pipeline.ts`'s sync/async engine
  fork, `EMPTY_CHUNKS` cast, pre-buffer reset and `defer` cast each collapse to one call
  (`.claude/architecture.md`'s own Module layout names them), `createPipeline<U,R>()` gaining its
  own return type so a caller gets back its real class with no trailing cast; `transformer.ts`'s
  element-wise `pipe()` body, written twice per method, is one call per method, and `tryRecover()`
  unifies the try/catch-if-thenable/recover skeleton two other sites shared (`Reducer.fold`'s own
  hot per-item path keeps its measured-faster inlined form by decision).
  The four dispatching classes' `createPipeline()` overrides collapse behind one `carriedKnobs()`
  hook; 5 overrides that narrowed a return type nothing read are deleted; `ClusterPipeline`'s 5
  mutable module bindings and 4 free functions become one `WorkerSet` class; `http.ts`'s
  `fetch()`/`reduceWork()`/`handleOverBridge` each split their own mixed concerns into named
  helpers. `utils/chunk.ts` splits into `cut.ts`/`drain.ts`/`recut.ts` along its own real seams,
  re-exported from `chunk.ts` so no existing import changes; `normalize` drops out of the PUBLIC
  barrels only (`chunk.ts`'s own internal re-export is untouched - BREAKING, no deprecation
  period, never a documented capability). `branch.ts`/`result.ts` adopt `Drainable<T>`, collapsing
  3 independent re-spellings of the same 4-field drain view; `PipelineResult`'s three different
  sync/async terminal-dispatch styles share one `dispatchSync()`. `context/types.ts` (a dead
  re-export) is deleted whole.
  The test suite gets the same treatment: `__tests__/helpers/` gains the small utilities copied
  3-5x each (`countPromises`, the `Order`/`orders`/`withVat` trio, `parseStrict`, `chunksOf`, a
  closing-generator source pair, `runFixtureJson`, `withTrackedServer`); the 4 hand-rolled
  `IContextManager` test doubles now extend `SimpleContextManager`, fixing a pre-#113
  `value !== undefined` bug all 4 of them still carried. Every layer's diff shows zero
  `/code-review medium` findings. PRs #135 (types), #136 (pipeline.ts), #138
  (transformer/reduce/helpers), #139 (dispatching classes), #140 (chunk.ts split), #141
  (branch.ts + result.ts), #142 (test helpers), #143 (docs).
- **`EventEmitterPipeline`, a fourth dispatch mode** (#124, `feat`) - no mode lets another module
  attach a worker to a named stage after the chain already exists, or observe a stage's chunks
  without composing an observer into the chain. `.transform()`'s own composed function
  auto-registers as `stage:<n>`'s first Worker on `pipeline.emitter`, once per stage index; any
  number of extra Workers may register afterward, from anywhere in the process, running alongside
  it, and the first to SETTLE - `respond()` or `reject()` - decides the chunk, measured: a Worker
  rejecting at 5ms beat one resolving at 30ms despite registering second. `ConcurrentPipeline`'s own
  fan-out (`maxConcurrency`, `ordered`) is inherited unchanged - deliberately the simplest version:
  no pool, no round-robin selection. A code-review pass on the dispatch found and fixed two real
  gaps before merge: a Worker's own SYNCHRONOUS throw used to abort the dispatch loop before every
  later Worker ran, and a throwing `stage:<n>:done`/`:error` lifecycle listener could leave the real
  dispatch hanging rather than merely leak an unhandled rejection - both verified fixed against a
  real, thrown-away repro. Revives the NAME from `#30` (closed unbuilt) for an unrelated capability
  - `#30` was an observability class superseded by `.tap()` (#72); this is a dispatch mode, verified
  during planning to have no overlap with `.tap()`'s own coverage. `.branch()` arm naming and two
  chains sharing one caller-supplied `emitter` both stay unsupported by decision - `.claude/architecture.md`'s
  own EventEmitterPipeline section, `#124`'s own Settle first/Constraints. PR #129 (test layer), PR
  #130 (dispatch + lifecycle), PR #132 (docs).
- **A pipeline holds its input type, not its data** (#90, `feat!`) - a chain is composed once,
  without data, and RUN by calling it: `new Pipeline<Order>().transform(f)(orders).toArray()`.
  Calling one returns a `PipelineResult`, which is where `toArray`/`first`/`consume`/`forEach`,
  both iteration protocols and `chunks()` live - so a chain cannot be drained without an input, and
  a result cannot be extended. A chain whose every callback is synchronous returns a plain array
  with no `Promise` created anywhere, measured at zero with `node:async_hooks`; one async callback,
  or an async input, widens the whole chain. `ConcurrentPipeline`, `HttpPipeline` and
  `ClusterPipeline` take `(pipeline, options)` and wrap a chain built elsewhere, which is what lets
  the HTTP worker and the HTTP trigger share one definition with no placeholder source.
  `.branch()` is a STAGE configured by a fluent builder - `b.when(name, predicate, build?)` and
  `b.otherwise(name, build?)` - whose arms are pipelines of the parent's own class, so an arm
  dispatches wherever the parent does and `.local()` inside it pins the arm in the orchestrating
  process. It returns a runner built once and called with any data, matching and joining always on
  the orchestrator, and a routing-only arm needs no `Transformer` (#87, folded in). A route reads as
  the chain was built: `/transform/<n>`, `/reduce/<n>`, `/branch/<i>/<name>/transform/<n>`.
  BREAKING: `.from()`, both `merge` forms, and every terminal op leave `Pipeline`; the two-argument
  constructor is gone. A `/simplify` pass over the whole stack closed two regressions it had
  introduced - the sync fold allocated two closures per ITEM (372.5 -> 25.6 ns/item, where the async
  fold it replaced ran at 94.5) and the re-cut was quadratic in its chunk (1418 ms -> 7.1 ms over
  80 000 items) - and deleted two type parameters: `JoinMode` testing `M` first collapses on a
  dispatching class's own pinned `"async"`, which is what `SourcePolicy` and `AssignMode` had been
  carried through thirty signatures to do. PRs #92, #93, #95, #97, #102, #103, #104, #105, #106,
  #107, #108, #109, #110, #111, #112.
- **Eight pre-existing defects a whole-project review found** (#113, `fix`) - two returned wrong
  values with no error. A partitioned reduce handed every partition the ONE seed the caller passed,
  so a mutable accumulator was shared by all of them (`[[1,2,3,4],[1,2,3,4]]` where two partitions
  owe `[[1,3],[2,4]]`); each partition copies its own now, and a seed `structuredClone` cannot copy
  raises rather than reverting to the shared object. Two sibling `ClusterPipeline` chains off one
  base both inherited the base's `pipelineIndex` and the second overwrote the first in the worker
  registry, so calling the first returned the second's output; a composed, trail-less, unbound
  instance claims its own slot now, and a bound replay does not - which is what keeps the
  orchestrator and the lazily-replaying worker on the same index. The rest: `fanOutUnordered` closes
  its source on an early exit as `ordered: true` already did, a worker that dies before reporting
  its port rejects the bootstrap instead of hanging every dispatch forever, the reduce request body
  enqueues per `pull` so `desiredSize` backpressures the shared iterator, `writeStreamedBody` drops
  both listeners when either fires, `getOrDefault` tests key presence rather than
  `value !== undefined`, and the dead private `Transformer.toAsyncIterable` is deleted. PRs #114
  (code, tests), #115 (docs).
- **Error handling moves onto the function that failed** (#78, `feat!`) - `Transformer.onError(fn)`
  is now the ROW handler: returning a value replaces the row, the exported `DROP` sentinel removes
  it, throwing escalates. It reaches every element-wise call - `.map()`, `.filter()`, `.flatMap()`,
  `.tap(fn)` - and `Transformer.reduce()`'s fold step, wherever in the chain it is written,
  position-independent since `pipe()` carries the handler forward the same way it already carries
  the composed `transform` function. `Pipeline.onError(fn)` is the RUN handler, position-DEPENDENT:
  only a stage applied after it is covered; returning drops the failing chunk and the run
  continues, throwing stops it. `Transformer.runnable()` is the new seam that carries the row
  handler into a runnable chunk-transform function, read once wherever a `Transformer` becomes
  runnable (`Pipeline.apply()`, `ConcurrentPipeline.apply()`, `ConcurrentPipeline.stageWork()`'s own
  default) - a WORKER's own `_chunkTransforms` registry entry gets row recovery for free, since it
  was built the same `runnable()` call when its own copy of the entry module constructed the same
  chain. Measured on the shipped code across four runs: 354-380 ns/row for a `.map()` with no
  handler registered, matching the pre-#78 floor within run-to-run JIT noise (the seam costs
  nothing unused), and 6-14% slower with one registered (377-415 ns/row, same setup) - noisy but
  consistently positive. `.catch()`, `ChunkErrorHandler` and `ErrorHandler` (`src/errors/`) are
  deleted, no deprecation period; a chunk failure with no run handler registered still propagates
  and ends the run, same as before. PR #81 (code, tests), PR #82 (docs).
- **`.tap()` becomes the one observation surface, and `Pipeline` gains its own** (#72, `feat!`) -
  the old lifecycle-hooks knob depended on where in the chain it was written (`pipe()` dropped it on
  `Out` change, so attaching it before a later `.map()` fired nothing while attaching it after fired),
  and its invariant `Out` broke `t.tap(someTransformer)` at the type level.
  `Pipeline.tap(arg)`, declared once on the base, delegates to `Transformer.tap` wrapped in
  `.local(build)`, pinning the callback and its context writes to the orchestrating process on every
  class - measured over a real loopback `HttpPipeline`, a tap between two dispatched stages ran only
  in the caller, the worker's own identical `.tap()` call never invoked, and the dispatched stages
  either side of it still dispatched. `dispatchKnobViolations` and its refusal are deleted whole -
  the last knob it guarded is gone. BREAKING, no deprecation period: the old lifecycle-hooks knob and
  its `TransformerLifecycleHooks` type are removed; `onStart`/`onComplete`/`onItemStart`/
  `onItemComplete` have no replacement, by decision. PR #80 (code, tests and docs, one layer).
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
  `process()`'s own outer catch reports it after the (later deleted, #72) lifecycle-hooks knob's own
  `onError` runs, keeping the two notification mechanisms' relative order unchanged.
  `dispatchKnobViolations`'s own refusal keeps only its lifecycle-hooks branch - #72 deletes the
  function whole, the last knob it guarded. BREAKING, no deprecation period: a handler that read
  `chunk.length` as "no detail available" must be updated. PRs #75 (L1, the fix and its tests), #76
  (docs).
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

- **A lazy `import("ws")` inside `WebSocketPipeline`, keeping `ws` a dependency** (#239 planning) -
  no break and no second entry. Killed because the root `.d.ts` graph still reaches `ws`
  (`dist/pipelines/websocket.d.ts` imports it) and `ws` stays a mandatory install for every consumer.
- **CJS output left unsplit under the second entry** (#239 planning) - the build and 434 unit tests
  stayed green while `dist/websocket.cjs` carried its own `Pipeline` copy: `new
  WebSocketPipeline(rootChain, …) instanceof root.Pipeline` printed `false`, and `Pipeline.wrapping`
  dropped the chain's stages. Killed for `splitting: true`, which prints `true`.
- **`NodeWebSocketHandler.upgrade` typed off `ws`'s `handleUpgrade`** (#239 planning) - kept the file
  free of `node:` imports, but left an import of `ws` in the published `.d.ts`: a consumer with
  `ws` and no `@types/ws` got `TS7016`. Killed for `IncomingMessage`/`Duplex` type-only imports.

- **Colon-prefixed arm event names, `branch:<i>:<name>:stage:<n>`** (#221 planning) - spiked
  working and non-breaking at top level. Killed by the user's pick of the route string HTTP already
  dispatches to, so one address names a stage on every class.
- **Renaming arm events alone, with the composed function still a registered listener** (#221
  planning) - fixed every arm case, but two forks of one base still shared `/transform/1` and one
  registration, returning `[20,30]` for both. Killed for the fork fix: no naming scheme separates
  two forks, and running each chain's own function directly does.
- **A by-reference codec alone, with the core unchanged** (#209 planning) - the codec returned a
  one-element `[{ ref }]` chunk so keys would flow between stages untouched. Killed on a spike: a
  `.buffer()` recut packed two handles into one chunk and dispatch threw, `.tap()` saw handles
  instead of rows, and an emptied chunk could not be detected. A plain handle object instead of the
  array returned `[]` without an error at the terminal. The core now carries an encoded chunk and
  decodes only where items are read.
- **Fusing consecutive dispatched stages into one hop** (#209 planning) - it removes the primary's
  decode between stages only where stages sit back to back, while changing stage identity on the
  wire. Killed in favour of the core-aware encoded chunk, which reaches the same hop and a dispatched
  reduce without renumbering routes.
- **An exported `referenceCodec(store, { inner })` helper** (#209 planning, filed then withdrawn) -
  a shipped by-reference codec over a `{ put, get }` store, with an `inner` codec for the format.
  Killed by the user: a caller juggled two codecs, a function could not be extended, and storage is
  a caller's own business. #209 ships the `Codec` interface and a `JsonCodec` class instead; a
  by-reference codec is a caller's own class. A `delete` hook on it was dropped earlier for the same
  reason: cleanup belongs to the caller's store.

- **`HttpPipeline.reduceWork()` dispatched as multiple POSTs instead of one duplex connection**
  (#201, add-on scope) - the premise was that a client-carried accumulator (`{acc, chunk, context}`
  out, `{acc, emitted, pending}` back, `pending` derived from `Reducer.itemsSinceEmit` and simply
  overwritten per response - provably equivalent to the single-Reducer design's own trailing-flush
  decision) would let any worker serve any chunk, dropping the session-id/affinity machinery a
  multi-request reduce would otherwise need. Killed on measurement, not built: N=10,000,
  `.buffer(100)` (100 chunks), `maxConcurrency: 1` - a scalar accumulator (`acc + x`) went from
  ~24-25ms to ~185-197ms (~8x), an array accumulator (`acc.push(x)`) from ~7-10ms to ~228-244ms
  (~25-30x). The array case is the one that generalizes: today's accumulator never crosses the wire
  at all (it lives in worker memory for the connection's life); under this design every POST resends
  the WHOLE accumulated-so-far value, so total wire bytes grow roughly with N²/bufferSize for any
  non-scalar accumulator, on top of the per-request overhead `stageWork()` already pays per chunk
  (`http.ts:518-522`'s own measured reason `reduceWork()` uses one connection today). Shown the
  numbers, the user chose to keep the duplex design. Untested middle ground, if revisited: batch K
  chunks per POST instead of 1, amortizing the per-request cost while keeping the array-payload cost
  - traded a new knob (a reduce batch size) for a smaller regression, never priced.

- **Normalizing an async generator source through a hand-rolled iterator inside `fromSource()`**
  (#180) - the premise was that `fromSource()`'s own consumption of a genuine async generator adds
  avoidable cost on top of the source's own price. Killed on measurement: a hand-rolled consumer
  pulling the SAME generator via `.next()` directly, bypassing `for await`'s own sugar entirely,
  measured 4.000 promises/row against a plain `for await` drain's 4.001 at N=10,000 - no daylight
  between them, and `fromSource()`'s own real path already reads 4.013, within 0.3% of that floor.
  The cost is the async generator PROTOCOL's own resumption machinery (V8's), paid once per `.next()`
  call regardless of who calls it - not reachable from any consuming shape. A hand-rolled,
  non-generator `AsyncIterable` DOES halve the cost (2.001 promises/row, same `for await`
  consumption) - the fix available to a caller who controls how their own source is built, not to
  this package's own consumption of a generator it did not build.

- **A synchronous `.local()` region on a dispatching class** (#179's own first draft) - the design
  was to let a pinned region run the sync engine, on the premise that `sourcePolicy()` forcing Mode
  to `"async"` is what a region pays for. Killed on measurement, not deferred: the region was already
  free. `ConcurrentPipeline` with zero stages, no `.buffer()` and no region reads 240.50 ns/row, and
  the same chain with `.local((p) => p)` reads 236.88 - a difference inside run-to-run noise. The
  real costs were four per-ROW habits the async engine had over chunk-shaped data, all listed under
  **Built**, and none of them needed Mode to change. `architecture.md` had carried the wrong
  diagnosis ("reducible, though not eliminable while `sourcePolicy()` still pins Mode") since #120.

- **Gating `nsPerRow` in the memory suite** (#179) - built, then removed on its own second run.
  Measured, four runs of identical code read 37.3, 56.0, 68.5 and 77.9 ns/row on the fastest case, a
  2.1x spread, while the same runs' allocation read 45.7, 45.7 and 45.9 MB. `bench/overhead.ts`
  already gates speed on a harness built for it; a second gate on the same number only adds a second
  flake source. `bench/memory.ts` reports the figure and gates the memory axes.

- **Fusing adjacent sync `map`/`filter` links into one loop** (#120's O2, spiked in
  `tmp/spike-o2-fusion.ts`, deleted) - measured through the real shape (a full `Pipeline` run with
  chunking, N=1,000,000, matching `measurePipeline()`), not the isolated `.runnable()` call an
  earlier draft used (which read 35.7 ns/row, above the real `Pipeline` leg's own 29.7 - a shape
  the real chain never runs in). The real-shape number: 28.25 ns/row today, 20.73 ns/row fused,
  1.36x - well under the isolated draft's own 2.78x. Priced both forms: the general mechanism
  (converting `pipe()`'s eager composition to a deferred, fusable stage list, preserving
  `.onError()`'s per-original-link row-handler semantics under fusion, threading Mode-widening
  fallback) is the shape `#45`'s "forward-descending Transformer composition" was already killed
  for - "a larger contract than map/filter/reduce need." A narrow peephole (`.filter()` checking for
  a `lastLink: { kind: "map", fn }` marker with no row handler, fusing only that one pair) catches
  ONLY `map -> filter` immediately adjacent - a third chained link falls back to the general problem
  - and the marker is a copy-on-write field needing the same explicit carry-forward every subclass
  knob already needs through `.transform()`/`.local()`/`.buffer()`. `#90`'s own chunk-level costs
  (this ticket's own scope boundary) already dominate the per-row number more than fusion would
  close. Findings and both prices posted in full on `#120`'s own tracker thread.
- **A numeric complexity/line-count lint gate** (predates any ticket, `.oxlintrc.json`) -
  `max-lines-per-function`/`complexity`/`max-params` are deliberately not enabled, per the config's
  own comment: "unit size is an architectural question, not a numeric cap." Planning #133 (the
  codebase-wide simplification sweep) considered reviving one as a mechanized stopping criterion and
  rejected it for the same reason - a review-judgment gate (`/code-review`'s own
  reuse/simplification/efficiency dimension, run to zero findings) is the stopping criterion instead.
- **A `Runner` class that takes a built pipeline** (#90, PR #99, closed) - `ConcurrentRunner`,
  `HttpRunner` and `ClusterRunner`, each running a pipeline through `run(pipeline)`. It forced
  `new HttpRunner({ url }, pipeline).run(pipeline)` for the one class that must mount a server
  before anything runs, because `.fetch` has to be ready at construction. The wrapping classes keep
  their `Pipeline` names, take `(pipeline, options)` and are callable instead - the same separation,
  with the pipeline named once.
- **A `Chain` type between the builder and the pipeline** (#90, never built) - a separate function
  type holding a stage list. The builder already carries its own; removing it and re-running the
  cluster spike gave identical output.
- **Detecting whether an input can be re-drained** (#90, never built) - so a spent source could
  raise instead of reading empty. The `src[Symbol.iterator]() === src` test agrees with reality on
  arrays, `Set`s, strings, custom iterables, generators and `Map.values()`, then reports a
  `ReadableStream` as replayable when a second drain yields `[]` - and merely running the test locks
  the stream, so the FIRST drain throws `Invalid state: ReadableStream is locked`. No detection is
  attempted: every terminal re-drains, and a spent source reads empty.
- **`class Pipeline extends Function`** (#90, never shipped) - one line for `instanceof Function`,
  `.bind` and `.call`, and a spike confirmed it survives three levels of inheritance. Killed by
  `super()`, which runs `CreateDynamicFunction`: `EvalError: Code generation from strings disallowed
  for this context` on the first `new Pipeline()` under `node
  --disallow-code-generation-from-strings`, and the same on a CSP page or a Cloudflare Worker.
  `Pipeline.prototype` is reparented onto `Function.prototype` once instead.
- **A conformance suite every `Pipeline` and Context class runs** (#37, closed COMPLETED, never
  built) - one set of behaviour cases, defined once in `__tests__/conformance/cases.ts`, run by thin
  wrapper files, one per class; no such file exists in the repo. Planning found four separate
  reasons a case could not run everywhere, and each turned out to be a defect rather than a
  boundary the suite needed to encode: `.withHooks()` drops silently on a dispatched stage - `#30`,
  the `EventEmitterPipeline` entry below, closed unbuilt; the capability itself shipped later as
  `.tap()` (#72) - a custom chunker is refused there (#39), an error handler is refused there and
  never sees the failing chunk (#40), and `merge` demotes to a plain `Pipeline` (#41). `#39`/`#40`/
  `#41` fixed their defects directly; `#37` itself was never built to prove the rest, closing the
  gap the suite would have only proven. Its
  own Done-when 1 also named `EventEmitterPipeline` as a fifth wrapper class, which #72 later killed
  outright - stale before the suite could ever be built as originally scoped.

- **A `.catch()`-shaped per-row region** (#78, spiked) - a region combinator whose sub-chain runs row
  by row, the per-row sibling of `.catch()`. Killed by measurement: per-row execution changes what a
  chunk-aware link inside the region MEANS. Real, `.reduce((acc, x) => acc + x, 0)` over
  `[1,2,3,4,5]`: `.buffer(5)` gives `[15]`, `.buffer(1)` gives `[1,2,3,4,5]`. Sound only for
  element-wise links, which is a per-function handler wearing a bigger API.

- **`Promise.allSettled` as the per-row mechanism** (#78, spiked) - the obvious way to attribute a
  rejection to its row without a wrapper. Killed on correctness, not cost: `chunk.map((x) => fn(x))`
  runs `fn` during the array build, so a SYNCHRONOUS throw (`JSON.parse`, `parseInt`, a schema
  parse) escapes before `allSettled` is ever called. Real: `R1 bare allSettled -> THREW: Invalid: x`
  against `R2 async-wrapped -> ["fulfilled","rejected","fulfilled"]`. The `async` wrapper it needs
  IS the per-row try/catch candidate, plus allSettled's own result objects - 430 against 381 ns/row
  at 1M.

- **A sequential fold, and an optimistic re-run, as the per-row mechanism** (#78, spiked) - both sit
  at the floor for synchronous callbacks and collapse on an async one, because both recover a chunk
  by walking it one row at a time. Real at 10k rows with a 1 ms callback: `.map()` today 18.51 ms,
  per-row try/catch 18.15 ms, the sequential loop 12497.87 ms, the optimistic re-run 13804.81 ms
  once one row per chunk fails. The optimistic form also re-runs every good row's side effects.

- **`Pipeline.onError()` as a catch on the drain side** (#78, spiked) - catching at `toArray()` and
  continuing. Killed because an async generator that throws is finished: a `Pipeline` over
  `["1","x","3","4"]` at `.buffer(1)` yields `[[1]]` then `done`, losing rows `3` and `4`, where the
  same failure guarded inside the per-chunk loop yields `[1,3,4]`. The guard lives at the two
  per-chunk sites #40 built instead.

- **`ts-pattern` for the `DROP` sentinel** (#78, spiked at 5.9.0, then uninstalled) - asked for, then
  killed by the user on its measured price. It would be this package's FIRST runtime dependency
  (`package.json` carries no `dependencies` key) and is the one #5 already removed, and
  `tsup.config.ts`'s `external` names only `p-limit`, so it would be bundled into `dist`. It does
  match a `unique symbol`, but `.exhaustive()` - the only part worth paying for - is not callable at
  the three real sites, which are all generic in `U`: with every arm present, `tsc --strict` refuses
  with `TS2349 … NonExhaustiveError<unknown>`. The tagged
  `{ kind: "keep"; value: U } | { kind: "drop" }` wrapper that restores it costs 254.4 ns/row for the
  match plus 13.7 to build the wrappers, against 9.6 ns/row for `!== DROP`; `match/otherwise` on the
  bare sentinel compiles at 30.9 ns/row and carries no guarantee at all.

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
  Killed on its own opening premise, re-run while planning #72: the old lifecycle-hooks knob was
  never the only observation surface. `Transformer.tap` already observes, and `dispatchKnobViolations`
  never refused it - `ConcurrentPipeline.buffer(2).transform((t) => t.map((x) => x * 2).tap(push))`
  over `[1..5]` returned `out [2,4,6,8,10]  seen [2,4,6,8,10]`. Two more of its premises went stale
  after it was filed: `{ local: true }` (#61 deleted it; `.local(build)` already gives an
  orchestrator-side tap) and "both fan-outs yield ITEMS" (#39 made both yield `U[]`). So the class
  bought nothing `.tap()` did not already do, at the cost of a `fanOut()` seam, an emitter interface,
  five event names and a consumer-error containment path. Its Enable layer - deleting the old
  lifecycle-hooks knob - is what survives, as #72.

  Amendment (#124's planning): the runtime-attach/detach axis - a consumer unknown in advance,
  attaching and detaching mid-drain, isolated from a sibling's own throw - was re-tested against
  this same closing verdict and confirmed still closed. A `.tap()` call whose body loops a plain
  mutable listener array with a per-listener `try`/`catch` gives real runtime attach mid-drain (a
  listener added after the pipeline was already called still saw only the chunks dispatched after
  it attached), real isolation (a throwing listener never blocked its siblings or the run), and
  real detach - no gap survives on that axis either. The NAME `EventEmitterPipeline` is reused by
  `#124` for an UNRELATED capability - a fourth dispatch mode, not observability - so a reader
  should not read `#124` as reviving this row's design.

- **`EventEmitterPipeline` as a round-robin dispatch mode** (#124's planning, three designs spiked
  and killed in sequence) - `.transform()`'s own composed function IS the worker, so the only
  question was how MULTIPLE workers on one stage should share chunks.

  Round-robin-by-counter (`(raw % pool.length)`-indexed dispatch, no readiness tracking) was
  killed by measurement: `maxConcurrency: 4` over a pool of 2 (the composed function plus one slow
  worker) produced `max simultaneous invocations of the SAME registered worker function: 2` - the
  counter dispatches on a schedule set by `maxConcurrency`, completely decoupled from whether a
  worker is still busy, flooding a slow one.

  `share()`-based free-slot dealing (the fix, `src/utils/chunk.ts:454`) plus a `.concurrency(n)`
  method mirroring `.buffer(size)`'s own persists-until-changed shape (per-stage worker-pool
  sizing) was then built and verified working - `max overlap … 1`, real additive capacity from
  registering one function twice, per-stage concurrency control confirmed with two stages at
  different settings. Killed anyway: the user asked to drop concurrency control entirely for a
  first ticket ("not even think about concurrency at this stage... a distributed event emitter
  solution layer" is later work, `.claude/roadmap.md`'s own **Later** section). Not a defect - a
  scope pullback. `apply()` fully overridden as the dispatch seam (bypassing
  `ConcurrentPipeline.apply()`'s own fan-out) went with it.

  Composed-function-as-fallback-only (only answers a stage when the pool is otherwise empty),
  `.dispatch(name)` (a stage method taking no function, naming a channel instead), and `.serve()`
  (explicit opt-in registration of the composed function) were three earlier candidate shapes for
  "what does `.transform()`'s own function mean here", each offered and not picked before the user
  confirmed the composed function is always an active pool participant, auto-registered.
  `#124` ships the survivor: the composed function auto-registers as `stage:<n>`'s first worker,
  once per stage index; broadcast dispatch (every registered worker runs, first to SETTLE wins) is
  what remains once selection logic is dropped.

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
  ⚠ REVIVED by #179, because the trade it was killed on was a false choice: `options.client` is a
  seam, so `node:http` is the DEFAULT on Node without being the only path anywhere. Bun, Deno and
  Cloudflare keep the global `fetch`, both `node:` imports are dynamic inside one `try` so a runtime
  without them falls back rather than failing to load, and an `https:` url falls back too - `node:http`
  speaks cleartext only. Re-measured on the shipped path, 200 chunks of 1000 rows, output asserted
  identical: `/transform/<n>` 1749-1828 ns/row on `fetch` against 306-349 on `node:http`.
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
