# @outputty/pipeline

## 1.0.0

### Major Changes

- add7236: **Breaking:** `normalize` is removed from `@outputty/pipeline`'s public export surface. It had zero
  production callers (chunking moved onto `Pipeline.buffer()`, which never calls it) and was kept
  exported only by inertia.

  - A caller importing `normalize` from `@outputty/pipeline` gets a build error instead of the
    function. There is no drop-in replacement: `normalize` took a stream mixing loose items and
    pre-chunked arrays and flushed the loose ones into a chunk whenever a real array arrived or the
    stream ended - `Pipeline`'s own cutting (`.buffer(size)`) is what does this job now, on a stream
    that is never mixed to begin with.
  - Every other public export is unaffected: `buildChunkGenerator` and `isContextAware` are untouched.

  Migration: delete the import. A caller who genuinely needs to normalize a loose/pre-chunked mix of
  its own writes the same generator directly - `normalize`'s own body took no dependency on anything
  else in this package.

  ```diff
  -import { normalize } from "@outputty/pipeline";
  -const chunks = normalize(mixedSource);
  +async function* normalize(stream) {
  +  let buffer = [];
  +  for await (const item of stream) {
  +    if (!Array.isArray(item)) {
  +      buffer.push(item);
  +      continue;
  +    }
  +    if (buffer.length > 0) {
  +      yield buffer;
  +      buffer = [];
  +    }
  +    yield item;
  +  }
  +  if (buffer.length > 0) yield buffer;
  +}
  +const chunks = normalize(mixedSource);
  ```

  Two more internal changes ship in this same release, neither requiring any action from a consumer:

  - `RowErrorHandler`'s return type is respelled to bare `unknown` from an equivalent union - no
    caller-visible change.
  - The unused, unexported `src/context/types.ts` module is deleted; `IContextManager` keeps its one
    real export from `src/types.ts`, unchanged.

### Minor Changes

- dc97417: `Pipeline.buffer()` accepts a callback in place of a size, deciding the chunk boundary per item
  instead of by count.

  ```ts
  let windowStart = 0;
  const fiveMinuteWindow = (item, ctx, emit) => {
    if (item.ts - windowStart >= 300_000) {
      emit();
      windowStart = item.ts;
    }
    return item;
  };

  const data = await new Pipeline(events).buffer(fiveMinuteWindow).toArray();
  ```

  `emit()` takes no value: it flushes whatever is currently pending and resets it to `[]`. Returning a
  value appends it to the (possibly just-reset) pending array; returning the exported `DROP` sentinel
  skips the item entirely. A `Promise`-returning callback widens the pipeline's Mode to `"async"`,
  matching `.reduce()`'s own two-overload split. `.buffer(size)`'s own behavior is unchanged.

  **Breaking:** `ChunkerFunction` is no longer exported - dead since #39 (`Transformer.setChunker()`
  was removed then), zero consumers. `BufferFunction` is the new exported type, for typing a
  `.buffer(fn)` callback.

  Migration: a caller importing `ChunkerFunction` has no replacement to import - the type described a
  mechanism `.buffer()` has not used since #39, and nothing in this package's own source referenced it
  either.

- 0765e55: **Breaking:** chunking is now the `Pipeline`'s own explicit decision, never the `Transformer`'s.
  `Transformer.chunkSize`, `.setChunker()`, `.execute()` and `createTransformer(chunkSize)`'s argument
  are removed; `ConcurrentPipelineOptions.chunkSize` is removed with them.

  - `Pipeline.buffer(size)` replaces every one of those - the ONE place a cut happens, carried
    unchanged through every later stage until called again.
  - `Transformer.process(chunks, context?)` replaces `.execute(items, context?)` - it now takes an
    already-cut `AsyncIterable<In[]>` and yields `Out[]`, chunk in, chunk out, deciding nothing about
    chunk size itself.
  - `createTransformer()` takes no argument - a `Transformer` never carries a chunk size to begin with.

  Migration:

  ```diff
  -import { Transformer, createTransformer } from "@outputty/pipeline";
  +import { Pipeline, createTransformer } from "@outputty/pipeline";

  -const t = createTransformer<number>(2);
  -for await (const chunk of t.execute(items)) { ... }
  +const t = createTransformer<number>();
  +const out = await new Pipeline(items).buffer(2).apply(t).toArray();
  ```

- 5f75ba9: **Breaking:** `.local(build)` runs a whole region of the chain in the orchestrating process,
  replacing the per-stage `{ local: true }` flag. `StageOptions` and its `options?` argument - the
  second on `.apply()`/`.transform()`, the third on `.reduce()` - are removed entirely, on every
  dispatching class.

  - `.local(build)` builds a bare `Pipeline` over the caller's own chunk stream, runs `build` against
    it - nothing inside can dispatch - and resumes the caller's own class afterward. One
    implementation on the base `Pipeline`; every dispatching subclass narrows only its return type.
  - Several consecutive stages that must stay in the orchestrating process are written once, inside
    one `.local(build)` call, instead of repeating the old flag on every one of them.
  - The old flag lived only on the dispatching subclasses, so a chain using it never typechecked on a
    base `Pipeline`. `.local(build)` is declared on `Pipeline` itself, so the same call compiles on
    every class.

  Migration:

  ```diff
  -new ConcurrentPipeline(rows, { maxConcurrency: 8 })
  -  .transform((t) => t.map(expensiveScore))
  -  .transform((t) => t.filter((r) => r.ok), { local: true })
  +new ConcurrentPipeline(rows, { maxConcurrency: 8 })
  +  .transform((t) => t.map(expensiveScore))
  +  .local((p) => p.transform((t) => t.filter((r) => r.ok)))
     .toArray();
  ```

- f4b2109: **Breaking:** error handling moves onto the function that failed. `Transformer.onError(fn)` is now
  the ROW handler, and `Pipeline.onError(fn)` is the RUN handler. `.catch()`, `ChunkErrorHandler` and
  `ErrorHandler` are deleted.

  - `Transformer.onError(fn)` - `fn` receives the failing item, the `Error` and the context.
    Returning a value puts that value in the row's place, returning the exported `DROP` sentinel
    removes the row, and throwing escalates to the pipeline's own run handler. Reaches every
    element-wise call (`.map()`, `.filter()`, `.flatMap()`, `.tap(fn)`) and `Transformer.reduce()`'s
    fold step, wherever in the chain it is written - position-independent.
  - `Pipeline.onError(fn)` - `fn` receives the `Error` and the context. Returning drops the failing
    chunk and the run continues; throwing stops the run. Position-DEPENDENT: only a stage applied
    after this call is covered.
  - `.catch()`, `ChunkErrorHandler` and `ErrorHandler` (`src/errors/`) are deleted outright, no
    deprecation period.

  Migration:

  ```diff
  -await new Pipeline(["a", "b", "3", "d", "5"])
  -  .transform((t) => t.catch((sub) => sub.map(parseStrict), () => [999]))
  -  .toArray();
  -// [999] - "3" and "5" parsed fine and are lost with the chunk
  +await new Pipeline(["a", "b", "3", "d", "5"])
  +  .transform((t) => t.onError(() => DROP).map(parseStrict))
  +  .toArray();
  +// [3, 5] - only the two bad rows are dropped
  ```

- 3543eef: **Breaking:** where a chain's chunks run is now a class you construct, not an
  `ExecutionStrategy` you configure on a `Transformer`. `ExecutionStrategy`, `.withExecutor()`,
  `sequential`, `concurrent(options?)` and `ConcurrentStrategyOptions` are removed entirely.

  - `ConcurrentPipeline` replaces `.withExecutor(concurrent(options))` - runs up to `maxConcurrency`
    chunks of a stage at once, in this process.
  - `HttpPipeline` dispatches a stage's chunk to another instance over HTTP, given its url.
  - `ClusterPipeline` dispatches to worker processes on the same machine, brought up automatically -
    no server, listen, fork or url in caller code.
  - `.local(build)` keeps a whole region of the chain in the orchestrating process on any of the
    three.
  - A plain `Pipeline` (the default, one chunk at a time) is unchanged - `Transformer.execute()`
    itself also runs sequentially now, since the pluggable strategy it dispatched through is gone.

  Migration:

  ```diff
  -import { Transformer, concurrent } from "@outputty/pipeline";
  +import { ConcurrentPipeline } from "@outputty/pipeline";

  -const data = await new Pipeline(rows)
  -  .transform((t) => t.withExecutor(concurrent({ maxConcurrency: 8 })).map((x) => x * 2))
  +const data = await new ConcurrentPipeline(rows, { maxConcurrency: 8 })
  +  .transform((t) => t.map((x) => x * 2))
     .toArray();
  ```

  Also fixes two consumer-facing defects the deleted strategy seam had hidden: `.map()`/`.filter()`
  now await an async callback's result instead of casting a `Promise` straight into the output array
  (`new Pipeline([1,2,3]).transform((t) => t.map(async (x) => x*2).filter((x) => x>2)).toArray()`
  used to print `[]`, now `[4,6]`), and `Pipeline.buffer()` no longer silently drops the pipeline's
  own chunk-transform history and source-position violations on copy-on-write.

- 290746e: **Breaking:** a `Pipeline` holds its input TYPE, not its data. It is composed once with no data and
  RUN by calling it, so one definition serves every input (#90). A chain whose every callback is
  synchronous now returns a plain array with no `Promise` created anywhere - measured at zero with
  `node:async_hooks` - and one async callback, or an async input, widens the whole chain.

  - `new Pipeline(data, options)` and `.from(data)` are deleted. Compose with
    `new Pipeline<In>(options?)` and call the result with the input.
  - Every terminal op leaves `Pipeline` for `PipelineResult`, what calling a pipeline returns:
    `toArray`, `first`, `consume`, `forEach`, `chunks()` and both iteration protocols. A chain cannot
    be drained without an input, and a result cannot be extended.
  - `Pipeline.merge(pipelines)` and `.merge(...others)` are deleted and go unreplaced. Both
    concatenated SOURCES, which a source-less pipeline has none of; concatenate inputs before calling.
  - `.branch(definitions)` becomes `.branch((b) => …)`, configured by a fluent builder, and is a STAGE
    rather than a terminal: it returns a runner, and each arm receives a PIPELINE of the parent's own
    class, so an arm dispatches wherever the parent does and `.local()` inside it pins the arm.
    `BranchDefinition` and `BranchOptions.firstMatch` are deleted; `.broadcast()` replaces the latter.
  - A dispatched stage's wire path reads as the chain was built: `/transform/<n>`, `/reduce/<n>`, and
    `/branch/<i>/<name>/transform/<n>` for an arm's own. A worker and its caller must be deployed
    together.
  - `ConcurrentPipeline`, `HttpPipeline` and `ClusterPipeline` take `(pipeline, options)` and wrap a
    chain built elsewhere, so an HTTP worker and an HTTP trigger share one definition with no
    placeholder source.
  - Type parameters: `Pipeline<T, M, P, In>` is now `Pipeline<T, M, In>`, and the three wrapping
    classes take `<T, In>`. `SourcePolicy` and `AssignMode` are deleted as types; `SourcePolicy`
    survives as the runtime value `sourcePolicy()` returns. Both deleted parameters were defaulted, so
    a one- or two-argument spelling is unaffected.
  - A failure on a synchronous chain THROWS out of the terminal op instead of rejecting. There is no
    promise for a rejection to travel on.

  Migration:

  ```diff
  -const out = await new Pipeline(orders)
  -  .transform((t) => t.map((o) => ({ ...o, total: o.total * 1.2 })))
  -  .toArray();
  +const withVat = new Pipeline<Order>()
  +  .transform((t) => t.map((o) => ({ ...o, total: o.total * 1.2 })));
  +const out = withVat(orders).toArray();   // number[], no await
  +const other = withVat(moreOrders).toArray();   // same chain, no rebuild
  ```

  ```diff
  -const split = await pipeline.branch({
  -  big: { predicate: (o) => o.total > 200, transformer: label("BIG") },
  -}, { firstMatch: false });
  +const split = pipeline.branch((b) =>
  +  b.when("big", (o) => o.total > 200, (q) => q.transform((t) => t.map((o) => `BIG:${o.id}`)))
  +    .otherwise("rest")
  +    .broadcast(),
  +);
  +const results = split(orders);
  ```

- 5eb6442: `Pipeline.queue(capacity)` prefetches up to `capacity` chunks ahead of the consumer, decoupling when
  a chunk is pulled from when a downstream terminal asks for it.

  ```ts
  const data = await new Pipeline<number>()
    .buffer(2)
    .queue(3)
    .transform((t) => t.map((x) => x * 2).filter((x) => x > 4))([1, 2, 3, 4, 5])
    .toArray();

  console.log(data); // [6, 8, 10]
  ```

  An array of exactly `capacity` pending `upstream.next()` promises: the consumer takes the front one,
  and the instant it does, a fresh promise is pushed onto the back - order preserved, never a race.
  `.buffer()` still owns the cut; `.queue()` only changes when each already-cut chunk is fetched.
  Always widens the pipeline's Mode to `"async"`, even over an entirely synchronous chain, since a
  queued chunk may not be ready yet.

  A 100ms/item source through a 30ms/item transform, 5 items, ran 674ms fully serial and 542ms queued
  at `.queue(3)` - overlap between production and consumption, never concurrent production: a single
  async generator source still serializes its own internal work regardless of how many pulls are in
  flight.

- 83f232d: **Breaking:** `Pipeline.reduce(fn, initial)` folds every chunk the pipeline produces, not one.
  `ReduceOptions`, `PipelineReduceFunction` and `Transformer.reduce`'s old per-chunk-toggle overload
  are removed; `ReduceFunction<U, Out> = (acc, item, ctx, emit) => U | Promise<U>` is the one
  signature, `emit` fourth so `ctx` keeps arity 3.

  - `Transformer.reduce(fn, initial)` still folds the ONE chunk it receives and keeps no state
    between chunks.
  - `Pipeline.reduce(fn, initial)` folds EVERY chunk the pipeline produces, in-process and
    sequential - the only place cross-chunk state lives. `ConcurrentPipeline.reduce(fn, initial)` is
    the one override that always dispatches it; wrap it in `.local(build)` to keep it in-process.
  - `emit(value)` pushes one value downstream mid-fold and resets the accumulator; the trailing
    accumulator is only emitted if items were folded since the last `emit()`.
  - A reduce stage dispatches like any other stage, over one duplex POST to `/reduce/<n>` whose
    accumulator lives for the connection's life, so `maxConcurrency` is inert on it.

  Migration:

  ```diff
  -import { Transformer } from "@outputty/pipeline";
  +import { Pipeline } from "@outputty/pipeline";

  -const total = await new Transformer<number, number>()
  -  .reduce((acc, x, { perChunk: false }) => acc + x, 0)
  -  .process(chunks);
  +const total = await new Pipeline([1, 2, 3, 4, 5])
  +  .reduce((acc: number, x: number) => acc + x, 0)
  +  .toArray();
  ```

- 085c903: **Breaking:** `.withHooks()` and `TransformerLifecycleHooks` are deleted. `.tap()` is the one
  observation surface, at two levels: `Transformer.tap(fn | transformer)` travels with its stage; the
  new `Pipeline.tap(fn | transformer)` (#72) always runs in the orchestrating process, even on
  `HttpPipeline`/`ClusterPipeline`, where a dispatched stage either side of it still dispatches.

  - `Transformer.tap(fn)` calls `fn` per item with its context; `Transformer.tap(transformer)` hands
    the whole chunk to a nested `Transformer` instead. Either travels with the stage it sits in.
  - `Pipeline.tap(fn | transformer)` wraps the same call in `.local(build)`, pinning the callback and
    its context writes to the process that called it, whatever class it is called on.
  - `onStart`/`onComplete`/`onItemStart`/`onItemComplete` go unreplaced by decision - `.tap()` covers
    observation; nothing replaces a lifecycle notification with no data to carry.

  Migration:

  ```diff
  -const t = new Transformer<number, number>().withHooks({
  -  onStart: (chunk) => console.log(`start ${chunk.length}`),
  -});
  +const t = new Transformer<number, number>();
  +const p = new Pipeline(source).tap((x) => console.log(`saw ${x}`)).apply(t);
  ```

### Patch Changes

- b22db98: Fixes `Transformer.catch()`'s `onError` handler silently discarding its returned replacement array
  and always dropping the failing chunk instead — the exported `ChunkErrorHandler<In, U>` type
  promised a replacement array was honored, but the shipped `ErrorHandler.handle()` returned nothing.
  It now returns the first registered handler's non-`undefined` result (handlers run LIFO, so the
  last-registered one runs first and is the natural winner), and `.catch()` substitutes that array for
  the chunk, falling back to `[]` only when no handler replaced it.

  ```diff
   const out = await new Pipeline(["a", "b", "3", "d", "5"])
     .transform((t) =>
       t.catch(
         (sub) => sub.map((s) => parseInt(s)),
         () => [999],
       ),
     )
     .toArray();
  -// []  (the handler's [999] was silently discarded)
  +// [999]
  ```

  Also tightens `Transformer.onError()`'s function-arm type to a bare `void` return, restoring an
  ordinary `(chunk, err) => arr.push(err)` notification hook (which briefly stopped typechecking under
  this fix's own `ChunkErrorHandler<In>` default) as a compile-time pass, not just a runtime one.

- 6ad2c27: Eight defects a whole-project review found, all of them older than #90 (#113). Two returned wrong
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

## 0.2.0

### Minor Changes

- 8b1fc60: **Breaking:** `ExecutionStrategy<In, Out>` is now a plain function type instead of a class-implementing
  interface: `(transformerLogic, chunks, context) => AsyncGenerator<Out[]>`. `sequential` and
  `concurrent(options?)` replace the deleted `SequentialStrategy`/`ConcurrentStrategy` classes, and the
  executor registry (`registerExecutor`, `createStrategy`, `ExecutorType`, `ExecutorOptions`,
  `ExecutorSpec`, `CustomExecutor`, `ExecutorFactory`) is removed entirely — a custom strategy is now
  just a function passed straight to `.withExecutor()`, no registration or class needed.

  Migration:

  ```diff
  -import { Transformer, SequentialStrategy, ConcurrentStrategy } from "@outputty/pipeline";
  +import { Transformer, sequential, concurrent } from "@outputty/pipeline";

   const t = new Transformer<number, number>()
  -  .withExecutor(new ConcurrentStrategy({ maxConcurrency: 4 }))
  +  .withExecutor(concurrent({ maxConcurrency: 4 }))
     .map((x) => x * 2);
  ```

  Also fixes three consumer-facing type defects the untypechecked test suite had hidden:
  `TransformerLifecycleHooks` callbacks now type as a bare `void` (an ordinary `() => arr.push(x)` hook
  is assignable again); `Transformer.loop()`'s `condition` takes one `(chunk, ctx) => boolean` signature
  instead of a union of two arities; and `new Transformer<In, Out>()` with a mismatched `In`/`Out` and no
  `transform` is now a compile error instead of a silent runtime cast.

## 0.1.1

### Patch Changes

- 9b7b773: Set up changesets and a Trusted Publishing release workflow
