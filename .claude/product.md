<!-- product.md - the product's truth, written as finished documentation. Every claim verified by a run.
     Implementation lives in architecture.md; an example is pulled from examples.md, never duplicated. -->

# @outputty/pipeline - Product

## North Star

`@outputty/pipeline` is a TypeScript-native, async-first streaming transform library: chain
`map`/`filter`/`reduce`/`flatMap` over a data source the way `Array` methods chain over a list, but
process it in chunks, streaming, and choose where that work runs by choosing a `Pipeline` class. The
chain never changes between them: the same `.transform()` calls run one chunk at a time, several at
once, across processes, or across machines. It is `outputty/laygo`'s caller-side transform layer - laygo's own `Source` accepts any
`AsyncIterable`, so this package reaches a `Model`'s `from` structurally, unimported, never as a
laygo dependency. It must never grow a query-planning or storage layer of its own: chunking,
transforming and controlling concurrency over data already in flight is the whole job.

## Functionality

### Pipeline and Transformer

A `Pipeline` wraps a data source and composes a `Transformer` chain over it; a `Transformer` is the
chain itself, chunk-agnostic - it processes whatever chunk it is handed and never decides how its
input was cut. Splitting the two means a transform chain tested once (`Transformer.process()`) reruns
unchanged inside a `Pipeline`, over an HTTP paginator, or inside a laygo `Source`; a caller running a
`Transformer` standalone supplies its own already-cut chunks.

> **Pipeline** - the chain, and nothing else: `new Pipeline<In>(options?)` declares the type it
> ACCEPTS, holds no data, and IS the function you call. `.context()` to seed shared
> state, `.buffer(size)` to set the chunk boundary, `.apply()`/`.transform()` to run a `Transformer`,
> `.tap()` to observe without changing the data, and one of five terminal ops (`.toArray()`/`.first()`/`.consume()`/`.forEach()`/`.branch()`) to
> drain it.
> **Transformer** - the chainable, reusable chunk-transform: `new Transformer<In, Out>(options?)`,
> `.map()`/`.flatMap()`/`.filter()`/`.reduce()`/`.tap()`/`.onError()`. `.process(chunks, context?)` runs
> it directly over an `AsyncIterable` of already-cut chunks, independent of `Pipeline` - it takes no
> chunk size or chunker of its own.

```ts
import { Pipeline } from "@outputty/pipeline";

const data = await new Pipeline<number>()
  .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 4))
  ([1, 2, 3, 4, 5]).toArray();
```

```json
[6, 8, 10]
```

### Chunking

Rows move through a `Pipeline` in chunks, not one at a time: every `.map`/`.filter`/`.reduce` call
processes a chunk at a time, which is what makes a concurrent execution strategy (below) a batch of
parallel work rather than one promise per row. The boundary is the `Pipeline`'s own decision, not a
`Transformer`'s: `.buffer(size)` sets it explicitly, and every later stage sees those same chunks
unchanged until another `.buffer()` call declares a new one - a `Transformer` never knows how its own
input was cut, and just processes whatever chunk arrives. `.buffer(fn)` decides the boundary per item
instead of by count - useful when a chunk boundary means something (a time window, a batch of related
records) that a fixed count cannot express.

> **Chunk** - the streaming unit a chain operates on: `In[]`/`Out[]`. **`.buffer(size)`** - the
> explicit chunk boundary; defaults to `1000` when never called. Two `.buffer()` calls back to back,
> with no stage between them, collapse to the last one - only it is ever actually applied.
> **`.buffer(fn)`** - decides the boundary per item: a `T[]` pending array the framework owns, folded
> through `fn` item by item. `fn`'s own `emit()` takes no value - it flushes whatever is pending and
> resets it to `[]`; returning a value appends it to the (possibly just-reset) pending array,
> returning `DROP` skips the item entirely. A `Promise`-returning `fn` widens the pipeline to run
> asynchronously.

```ts
import { Pipeline } from "@outputty/pipeline";

const data = await new Pipeline<number>()
  .buffer(2)
  .transform((t) => t.map((x: number) => x * 2))
  ([1, 2, 3, 4, 5]).toArray();
```

```json
[2, 4, 6, 8, 10]
```

A caller who needs a boundary decided by something other than count - grouping events into
five-minute windows by each event's own timestamp - writes it directly:

```ts
import { Pipeline } from "@outputty/pipeline";

let windowStart = 0;
const fiveMinuteWindow = (item: { ts: number }, _ctx: unknown, emit: () => void) => {
  if (item.ts - windowStart >= 300_000) {
    emit();
    windowStart = item.ts;
  }
  return item;
};

const events = [
  { id: 1, ts: 0 },
  { id: 2, ts: 60_000 },
  { id: 3, ts: 240_000 },
  { id: 4, ts: 300_000 },
  { id: 5, ts: 301_000 },
];

const chunks: unknown[][] = [];
for await (const chunk of new Pipeline(events).buffer(fiveMinuteWindow)(events).chunks()) {
  chunks.push(chunk);
}
```

```json
[
  [{ "id": 1, "ts": 0 }, { "id": 2, "ts": 60000 }, { "id": 3, "ts": 240000 }],
  [{ "id": 4, "ts": 300000 }, { "id": 5, "ts": 301000 }]
]
```

### Prefetching

`.queue(capacity)` decouples a chain's pull from its own source: instead of drawing the next chunk
only when a downstream consumer asks for it, up to `capacity` chunks sit ready ahead of time,
produced as fast as the source allows. A slow producer's latency then overlaps with a slower
consumer's own processing instead of adding to it - measured, a 100ms/item source feeding a
30ms/item stage ran 674ms with no queue and 542ms with one, from overlap alone, same output. Order
is preserved: `.queue()` never reorders items, only changes WHEN they are pulled.

> **`.queue(capacity)`** - prefetches up to `capacity` chunks `.buffer()` already cut, ready ahead of
> the consumer. Delivered strictly in order; never runs a callback or transforms an item. Widens the
> chain to run asynchronously, since a queue's own next value may not be ready yet.

```ts
import { Pipeline } from "@outputty/pipeline";

const data = await new Pipeline<number>()
  .buffer(2)
  .queue(3)
  .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 4))
  ([1, 2, 3, 4, 5]).toArray();
```

```json
[6, 8, 10]
```

### Where the work runs

The class you construct decides where a chain's chunks are processed. The chain itself - the
`map`/`filter`/`reduce` calls - is identical in all five, and so is the output. Only the class name
changes.

> **`Pipeline`** - one chunk at a time, in this process. The default, and the base every other one
> extends.
> **`ConcurrentPipeline`** - several chunks in flight in this process, bounded by `maxConcurrency` and
> re-ordered by `ordered` (default `true`). Use it when the per-chunk work is I/O-bound.
> **`HttpPipeline`** - each chunk dispatched over HTTP to another instance running the same code. It
> mounts its own routes, one per stage; the caller gives it the url where it is mounted.
> **`ClusterPipeline`** - each chunk dispatched to another process on the same machine. It brings up
> its own workers on first run and every later pipeline in the process reuses them.
> **`EventEmitterPipeline`** - each chunk handed to whichever Worker functions are registered on
> `pipeline.emitter`, a `node:events` `EventEmitter`. The chain's own `.transform()` function
> auto-registers as a stage's first Worker; any number of extra Workers may register afterward from
> anywhere in the process, and every one of them runs on every chunk.
> **Stage** - one `.transform()` or `.apply()` call. A stage is identified by its position in the
> chain, so a dispatching class sends a chunk and a stage index, never a function.
> **`.local(build)`** - runs a whole region of the chain in the orchestrating process, on every
> class the same way: builds a base `Pipeline` over the caller's own chunk stream, runs `build`
> against it (nothing inside can dispatch), and resumes the caller's own class afterward.
> **Items in flight** - the number of callbacks a chain runs at once: the buffer size times
> `maxConcurrency`. `maxConcurrency` bounds CHUNKS; the items inside one chunk run together.

The chunk is the unit of concurrency, so a `ConcurrentPipeline`'s parallelism is its buffer size
times `maxConcurrency`, never `maxConcurrency` alone. A chain left at the default buffer of 1000 with
`maxConcurrency: 3` holds 3000 callbacks in flight, not 3. Call `.buffer(size)` to lower the ceiling,
and prefer the widest chunk that fits it: two chains holding the same 16 in flight over 50000 items
ran 27 ms and 63 ms, because a narrow chunk pays the per-chunk cost more often.

| `.buffer(size)` | `maxConcurrency` | items in flight |
| --- | --- | --- |
| 1000 | 1 | 1000 |
| 100 | 3 | 300 |
| 1 | 16 | 16 |


```ts
import { ClusterPipeline } from "@outputty/pipeline";

const data = await new ClusterPipeline<number>()
  .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 4))
  ([1, 2, 3, 4, 5]).toArray();
```

```json
[6, 8, 10]
```

A dispatching class runs every stage elsewhere by default. `.local(build)` keeps a whole region in
the orchestrating process instead - useful when a stage is cheap, or touches something only the
orchestrator has. It takes the region as one builder, so several consecutive stages that must stay
put are written once, not repeated on every one of them:

```ts
import { HttpPipeline } from "@outputty/pipeline";

const pipeline = new HttpPipeline<Row>({ url: process.env.SELF_URL! })
  .transform((t) => t.map(expensiveScore))
  .local((p) => p.transform((t) => t.filter((r) => r.ok)));

app.mount("/pipeline", pipeline.fetch);
```

Two rules follow from a stage being a position rather than a name, and both are the caller's to keep:

- Every instance must run the same build. A rolling deploy that mixes versions can run a chunk through
  an older stage and return it with HTTP 200, so drain the old instances before the new ones serve.
- A file that constructs a `ClusterPipeline` is re-executed once per worker, because a worker has to
  run it to hold the transforms. Keep top-level work out of that file - a query or a migration there
  runs once per worker, not once.

`EventEmitterPipeline` needs none of what `HttpPipeline`/`ClusterPipeline` need to dispatch a
chunk elsewhere - no server, no separate process, no url. A chain's own composed function already
answers its own
stage the moment the chain is built - registering an extra Worker is optional, for when other code
in the same process wants to add capacity or take over the work entirely:

```ts
import { EventEmitterPipeline } from "@outputty/pipeline";

const pipeline = new EventEmitterPipeline<number>()
  .transform((t) => t.map((x: number) => x * 2));

// Optional - from anywhere else in the process, runs alongside the chain's own function.
pipeline.emitter.on("stage:0", ({ chunk, respond }) => respond(chunk.map((x: number) => x * 2)));

const data = await pipeline([1, 2, 3, 4, 5]).toArray();
```

```json
[2, 4, 6, 8, 10]
```

Every Worker registered on a stage runs on every chunk that reaches it; whichever settles first -
answers with `respond(value)` or fails with `reject(error)` - decides that chunk. Lifecycle events
(`stage:<n>:dispatched`/`:done`/`:error`/`:end`, `pipeline:end`) let other code watch a run without
becoming a Worker itself, as long as it listens on one of those names rather than the bare
`stage:<n>` channel.

### Context

A shared key-value store threads through every stage of a chain, so a downstream `map` can read a
value an upstream stage - or the caller - set, without it becoming an explicit chain parameter. A
caller's own `IContextManager` class - a Postgres-backed pool, a manager that rejects an unknown
key - survives as the SAME instance through `.context()`, keeps receiving every write, and a
rejected write propagates instead of being silently bypassed (#31).

> **Context / `IContextManager`** - `.get()`/`.set()`/`.getOrDefault()`/`.toDict()`. Every callback
> receives it as an optional second parameter, so an un-annotated `(x) => …` still infers `x`'s type
> from the source - `types.ts`'s own docstring records why a two-arity union signature was rejected.
> **`PipelineOptions.context`** - an already-built manager, for THIS process.
> **`PipelineOptions.contextFactory`** - how to build one, for any OTHER process (a
> `ClusterPipeline` worker re-executing the entry module has no way to receive an already-built
> instance across the process boundary); invoked at most once per process (#31).

```ts
import { Pipeline } from "@outputty/pipeline";

const data = await new Pipeline<number>()
  .context({ multiplier: 10 })
  .transform((t) => t.map((x: number, ctx) => x * (ctx.get("multiplier") as number)))
  ([1, 2, 3, 4, 5]).toArray();
```

```json
[10, 20, 30, 40, 50]
```

The caller's own manager class decides whether state crosses a process. The pipeline ships one
mechanism and makes no other distinction: it seeds every worker forward, and never carries a
worker's writes back.

```ts
import { ClusterPipeline } from "@outputty/pipeline";

const data = await new ClusterPipeline<number>({
  workers: 3,
  contextFactory: () => new PgContext(pool),
})
  .context({ multiplier: 10 })
  .transform((t) => t.map((x: number, ctx) => x * (ctx.get("multiplier") as number)))
  ([1, 2, 3, 4, 5]).toArray();
```

```json
[10, 20, 30, 40, 50]
```

A `SimpleContextManager` keeps a worker's `ctx.set()` inside that worker. A manager backed by an
external store publishes it to every process through that store. A merge, static and
instance both, are the two places values flow the other way - the static into `options.context` (or
a fresh manager), the instance into the receiving pipeline's own - which is why each takes or
already holds the manager explicitly.

### Branching and merging

A single source splits into several named sub-chains by predicate, and several sources concatenate back
into one - the two directions of composing whole pipelines rather than chaining one.

> **Branch** - `Pipeline.branch(build)`: a builder pairs each `predicate` with an optional PIPELINE
> of the parent's own class, so an arm's stages run where the chain's do and `.local()` inside pins
> one. A STAGE, not a terminal - it returns a runner, and calling that produces one record keyed by
> arm name. `.when(name, predicate, build?)` routes, `.otherwise(name, build?)` is the catch-all and
> is always routed last, and `.broadcast()` sends an item to every matching arm rather than only the
> first. Matching runs where the caller is, so a predicate may read local state; the join does too,
> since arms can be remote.

```ts
import { Pipeline } from "@outputty/pipeline";

const split = new Pipeline<number>().branch((b) =>
  b.when("evens", (x) => x % 2 === 0).otherwise("odds"),
);

const data = split([1, 2, 3, 4, 5]);

console.log(data.evens); // [2, 4]
console.log(data.odds); // [1, 3, 5]
```

```json
{ "evens": [2, 4], "odds": [1, 3, 5] }
```

### Observing

`.tap()` watches data move without changing it. It receives each item, or a whole chunk, together
with the shared context, and passes the data through untouched. It exists at both levels, and the
level decides where the callback runs: on a `Transformer`, inside a `.transform()`, it travels with
the stage and runs wherever that stage runs; on a `Pipeline` it always runs in the orchestrating
process, so its output and its context writes land where the caller can see them, whichever class
the chain was built on.

Context is writable from a tap. A whole chunk is tapped before the next stage sees any of it, and an
async callback finishes in whatever order its work completes, so a tap that writes context writes it
per chunk and not per item.

> **`Transformer.tap(fn | transformer)`** - an observation point inside a chain. `fn` receives each
> item and the context; the `transformer` form receives the whole chunk.
> **`Pipeline.tap(fn | transformer)`** - the same observation point, run in the orchestrating
> process on every class.

```ts
import { ConcurrentPipeline } from "@outputty/pipeline";

const pipeline = new ConcurrentPipeline<number>({ maxConcurrency: 2 })
  .buffer(2)
  .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 4))
  .tap((x: number, ctx) => {
    ctx.set("seen", (ctx.getOrDefault("seen", 0) as number) + 1);
  });

console.log(await pipeline.toArray(), pipeline.contextManager.toDict());
```

```text
[ 6, 8, 10 ] { seen: 3 }
```

### Error handling

Error handling belongs to the function that failed. A callback that throws on one row hands that row
to the row handler, which replaces it, drops it, or escalates - the other rows in the chunk are
unaffected. A failure the row handler escalates, or one a chunk-wide operation raises, reaches the
pipeline's own handler, which decides whether the run continues without that chunk or stops.

> **`Transformer.onError(fn)`** - the row handler. `fn` receives the failing `item`, the `Error` and
> the context; returning a value puts that value in the row's place, returning `DROP` removes the
> row, and throwing escalates to the pipeline. It applies to every element-wise call in the chain -
> `.map()`, `.filter()`, `.flatMap()`, `.tap(fn)` and `.reduce()`'s fold step - wherever in the chain
> it is written, and it may be async.
> **`DROP`** - the exported sentinel a row handler returns to remove a row, so `undefined` stays an
> ordinary value a handler may return.
> **`Pipeline.onError(fn)`** - the run handler. `fn` receives the `Error` and the context; returning
> drops the failing chunk and the run continues, throwing stops the run with that error.

```ts
import { Pipeline, DROP } from "@outputty/pipeline";

const data = await new Pipeline<string>()
  .transform((t) =>
    t
      .onError(() => DROP)
      .map((s: string) => {
        const n = parseInt(s);
        if (isNaN(n)) throw new Error(`Invalid: ${s}`);
        return n;
      }),
  )
  (["a", "b", "3", "d", "5"]).toArray();
```

```json
[3, 5]
```

A row the handler replaces keeps its place in the output, so a chunk is repaired rather than lost:

```ts
const repaired = await new Pipeline<string>()
  .transform((t) => t.onError(() => -1).map(parseStrict))
  (["a", "b", "3", "d", "5"]).toArray();
```

```json
[-1, -1, 3, -1, 5]
```

The run handler is what keeps a stream alive past a chunk nothing could repair:

```ts
const survived = await new Pipeline<string>()
  .buffer(1)
  .onError((err) => console.warn(err.message))
  .transform((t) => t.map(parseStrict))
  (["1", "x", "3", "4"]).toArray();
```

```json
[1, 3, 4]
```

### Reducing

A reducer folds items into an accumulator, at two levels with one meaning. On a `Transformer` it
folds the one chunk it receives and keeps nothing between chunks. On a `Pipeline` it folds
everything it receives, which is the only place cross-chunk state lives. Both may produce more than
one value, and the chain continues after either - downstream stages run over every value a reducer
produced, never assuming there was one.

> **`Transformer.reduce(fn, initial)`** - folds ONE chunk. Run once and forget: no state survives to
> the next chunk.
> **`Pipeline.reduce(fn, initial)`** - folds EVERY chunk the pipeline produces. On a dispatching
> class (`ConcurrentPipeline.reduce(fn, initial)`, an override the base `Pipeline` never gains) it
> partitions the stream into `maxConcurrency` independent accumulators; each partition's own result
> flows downstream as an ordinary value, the same as any other multi-value reducer output.
> **`emit`** - the reducer callback's fourth parameter, `(acc, item, ctx, emit)`. Calling it pushes
> a value downstream mid-fold and lets the caller decide what a finished result is. The final
> accumulator is emitted only if items were folded since the last `emit()`.

```ts
import { Pipeline } from "@outputty/pipeline";

const data = await new Pipeline<number>()
  .reduce((acc: number, x: number) => acc + x, 0)
  .transform((t) => t.map((n: number) => n * 10))
  ([1, 2, 3, 4, 5]).toArray();
```

```json
[150]
```

A reducer that emits mid-fold turns one stream into a stream of finished results - a running total
banked whenever it crosses a threshold, and no trailing value when the last item already banked one:

```ts
import { Pipeline } from "@outputty/pipeline";

const data = await new Pipeline<number>()
  .reduce((acc: number, x: number, _ctx, emit) => {
    acc += x;
    if (acc >= 6) {
      emit(acc);
      return 0;
    }
    return acc;
  }, 0)
  .transform((t) => t.map((n: number) => n * 10))
  ([1, 2, 3, 4, 5]).toArray();
```

```json
[60, 90]
```

On a dispatching class, `.reduce()` partitions across `maxConcurrency` independent accumulators.
Nothing merges them automatically - each partition's own result flows downstream as its own value:

```ts
import { ConcurrentPipeline } from "@outputty/pipeline";

const data = await new ConcurrentPipeline<number>({ maxConcurrency: 2 })
  .buffer(2)
  .reduce((acc: number, x: number) => acc + x, 0)
  ([1, 2, 3, 4, 5]).toArray();
```

```json
[7, 8]
```

(two numbers summing to 15 - the split is timing-dependent). A caller who wants ONE value writes an
ordinary second reduce as the next stage, the same pattern used to fold down any other multi-value
reducer output:

```ts
import { ConcurrentPipeline } from "@outputty/pipeline";

const data = await new ConcurrentPipeline<number>({ maxConcurrency: 2 })
  .buffer(2)
  .reduce((acc: number, x: number) => acc + x, 0)
  .local((p) => p.reduce((acc: number, v: number) => acc + v, 0))
  ([1, 2, 3, 4, 5]).toArray();
```

```json
[15]
```

Reusing the fold itself as that second reduce is silently wrong in general: a count folds
`(acc, _x) => acc + 1`, and folding ITS OWN partials with the same function counts the partials, not
the items. There is no order between partitions, so a caller's own merge must be order-insensitive
regardless of `ordered` upstream - `ordered: false` upstream already required an order-insensitive
fold before partitioning existed, for the same reason. And because a remote reducer's emits arrive
while the input is still streaming, a failure mid-stream reaches the caller after values have
already flowed downstream - the price of results that arrive as they happen.
