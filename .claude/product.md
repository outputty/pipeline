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

> **Pipeline** - the high-level API: `new Pipeline(source, options?)`, `.context()` to seed shared
> state, `.buffer(size)` to set the chunk boundary, `.apply()`/`.transform()` to run a `Transformer`,
> `.tap()` to observe without changing the data, and one of five terminal ops (`.toArray()`/`.first()`/`.consume()`/`.forEach()`/`.branch()`) to
> drain it.
> **Transformer** - the chainable, reusable chunk-transform: `new Transformer<In, Out>(options?)`,
> `.map()`/`.flatMap()`/`.filter()`/`.reduce()`/`.tap()`/`.catch()`. `.process(chunks, context?)` runs
> it directly over an `AsyncIterable` of already-cut chunks, independent of `Pipeline` - it takes no
> chunk size or chunker of its own.

```ts
import { Pipeline } from "@outputty/pipeline";

const data = await new Pipeline([1, 2, 3, 4, 5])
  .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 4))
  .toArray();
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
input was cut, and just processes whatever chunk arrives.

> **Chunk** - the streaming unit a chain operates on: `In[]`/`Out[]`. **`.buffer(size)`** - the
> explicit chunk boundary; defaults to `1000` when never called. Two `.buffer()` calls back to back,
> with no stage between them, collapse to the last one - only it is ever actually applied.

```ts
import { Pipeline } from "@outputty/pipeline";

const data = await new Pipeline([1, 2, 3, 4, 5])
  .buffer(2)
  .transform((t) => t.map((x: number) => x * 2))
  .toArray();
```

```json
[2, 4, 6, 8, 10]
```

### Where the work runs

The class you construct decides where a chain's chunks are processed. The chain itself - the
`map`/`filter`/`reduce` calls - is identical in all four, and so is the output. Only the class name
changes.

> **`Pipeline`** - one chunk at a time, in this process. The default, and the base every other one
> extends.
> **`ConcurrentPipeline`** - several chunks in flight in this process, bounded by `maxConcurrency` and
> re-ordered by `ordered` (default `true`). Use it when the per-chunk work is I/O-bound.
> **`HttpPipeline`** - each chunk dispatched over HTTP to another instance running the same code. It
> mounts its own routes, one per stage; the caller gives it the url where it is mounted.
> **`ClusterPipeline`** - each chunk dispatched to another process on the same machine. It brings up
> its own workers on first run and every later pipeline in the process reuses them.
> **Stage** - one `.transform()` or `.apply()` call. A stage is identified by its position in the
> chain, so a dispatching class sends a chunk and a stage index, never a function.
> **`.local(build)`** - runs a whole region of the chain in the orchestrating process, on every
> class the same way: builds a base `Pipeline` over the caller's own chunk stream, runs `build`
> against it (nothing inside can dispatch), and resumes the caller's own class afterward.

```ts
import { ClusterPipeline } from "@outputty/pipeline";

const data = await new ClusterPipeline([1, 2, 3, 4, 5])
  .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 4))
  .toArray();
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

const pipeline = new HttpPipeline(rows, { url: process.env.SELF_URL! })
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

const data = await new Pipeline([1, 2, 3, 4, 5])
  .context({ multiplier: 10 })
  .transform((t) => t.map((x: number, ctx) => x * (ctx.get("multiplier") as number)))
  .toArray();
```

```json
[10, 20, 30, 40, 50]
```

The caller's own manager class decides whether state crosses a process. The pipeline ships one
mechanism and makes no other distinction: it seeds every worker forward, and never carries a
worker's writes back.

```ts
import { ClusterPipeline } from "@outputty/pipeline";

const data = await new ClusterPipeline([1, 2, 3, 4, 5], {
  workers: 3,
  contextFactory: () => new PgContext(pool),
})
  .context({ multiplier: 10 })
  .transform((t) => t.map((x: number, ctx) => x * (ctx.get("multiplier") as number)))
  .toArray();
```

```json
[10, 20, 30, 40, 50]
```

A `SimpleContextManager` keeps a worker's `ctx.set()` inside that worker. A manager backed by an
external store publishes it to every process through that store. `Pipeline.merge()`, static and
instance both, are the two places values flow the other way - the static into `options.context` (or
a fresh manager), the instance into the receiving pipeline's own - which is why each takes or
already holds the manager explicitly.

### Branching and merging

A single source splits into several named sub-chains by predicate, and several sources concatenate back
into one - the two directions of composing whole pipelines rather than chaining one.

> **Branch** - `Pipeline.branch(definitions)`: each `BranchDefinition` pairs a `predicate` with a
> `Transformer`; `BranchOptions.firstMatch` (default `true`) sends an item to the first matching branch
> only, `false` broadcasts it to every match.
> **Merge (static)** - `Pipeline.merge(pipelines, options?)`: concatenates every source pipeline's
> data and merges their contexts into one FRESH, plain `Pipeline` - for a caller who holds no
> pipeline of its own to continue. Each pipeline's item type is inferred on its own, so merging a
> `Pipeline<"a"|"b">` with a `Pipeline<"c"|"d">` gives a `Pipeline<"a"|"b"|"c"|"d">`. `options.context`,
> when given, is the SAME instance returned as the merged pipeline's `.contextManager` (#31) - later
> pipelines still win on a shared key; with no `options`, a fresh manager is built the same way.
> `Pipeline.merge([])` returns an empty pipeline.
> **Merge (instance)** - `pipeline.merge(...others)` (#41): concatenates OTHER pipelines' items and
> contexts onto ONE the caller already holds, keeping THIS pipeline's own class, knobs and stage
> numbering - a stage applied after the merge runs where this pipeline runs, at the NEXT index
> rather than restarting at 0. Reach for this over the static whenever work after the merge must
> stay concurrent, remote or clustered: the static always builds a plain `Pipeline`, so a merged
> `HttpPipeline` gaining one more stage would otherwise collide with its own first stage on
> `/stage/0`. `pipeline.merge()` with no arguments returns an equivalent pipeline of the same class.

```ts
import { Pipeline } from "@outputty/pipeline";

const data = await new Pipeline([1, 2, 3, 4, 5]).branch({
  evens: { predicate: (x: number) => x % 2 === 0, transformer: createTransformer<number>() },
  odds: { predicate: (x: number) => x % 2 !== 0, transformer: createTransformer<number>() },
});
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

const pipeline = new ConcurrentPipeline([1, 2, 3, 4, 5], { maxConcurrency: 2 })
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

A chunk that throws mid-chain is handled at the chunk, never the row: `.catch()` runs a sub-chain and
hands a failing chunk to a handler that can replace it or drop it, so one bad row's blast radius is
bounded and explicit.

> **`.catch(build, onError?)`** - `build` is the sub-chain to guard; `onError` receives the failing
> `chunk` and `Error`, and its return value (an array, or nothing) replaces or drops the chunk.

```ts
import { Pipeline } from "@outputty/pipeline";

const data = await new Pipeline(["a", "b", "3", "d", "5"])
  .transform((t) =>
    t.catch(
      (sub) =>
        sub.map((s: string) => {
          const n = parseInt(s);
          if (isNaN(n)) throw new Error(`Invalid: ${s}`);
          return n;
        }),
      () => [999],
    ),
  )
  .toArray();
```

```json
[999]
```

`.onError()` is the other half: a notification, never a recovery path, that fires on every chunk
failure with the chunk that actually failed - `.catch()` remains the only way to keep the run going.

> **`Transformer.onError(fn)`** - `fn` receives the failing `chunk` and `Error`; its return value is
> ignored. Several handlers registered this way run LIFO (last-registered first), and every one runs
> regardless of what an earlier one returned. The run still rejects with the original error - this
> is a hook, not `.catch()`'s replacement mechanism.

```ts
import { Pipeline, Transformer } from "@outputty/pipeline";

const seen: number[][] = [];
const transformer = new Transformer<number, number>()
  .map((x: number) => {
    if (x === 3) throw new Error("boom on 3");
    return x;
  })
  .onError((chunk) => seen.push(chunk));

await new Pipeline([1, 2, 3, 4]).buffer(2).apply(transformer).toArray().catch(() => {});
console.log(seen); // [[3, 4]] - the chunk that failed, chunkSize 2 over [1,2,3,4]
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

const data = await new Pipeline([1, 2, 3, 4, 5])
  .reduce((acc: number, x: number) => acc + x, 0)
  .transform((t) => t.map((n: number) => n * 10))
  .toArray();
```

```json
[150]
```

A reducer that emits mid-fold turns one stream into a stream of finished results - a running total
banked whenever it crosses a threshold, and no trailing value when the last item already banked one:

```ts
import { Pipeline } from "@outputty/pipeline";

const data = await new Pipeline([1, 2, 3, 4, 5])
  .reduce((acc: number, x: number, _ctx, emit) => {
    acc += x;
    if (acc >= 6) {
      emit(acc);
      return 0;
    }
    return acc;
  }, 0)
  .transform((t) => t.map((n: number) => n * 10))
  .toArray();
```

```json
[60, 90]
```

On a dispatching class, `.reduce()` partitions across `maxConcurrency` independent accumulators.
Nothing merges them automatically - each partition's own result flows downstream as its own value:

```ts
import { ConcurrentPipeline } from "@outputty/pipeline";

const data = await new ConcurrentPipeline([1, 2, 3, 4, 5], { maxConcurrency: 2 })
  .buffer(2)
  .reduce((acc: number, x: number) => acc + x, 0)
  .toArray();
```

```json
[7, 8]
```

(two numbers summing to 15 - the split is timing-dependent). A caller who wants ONE value writes an
ordinary second reduce as the next stage, the same pattern used to fold down any other multi-value
reducer output:

```ts
import { ConcurrentPipeline } from "@outputty/pipeline";

const data = await new ConcurrentPipeline([1, 2, 3, 4, 5], { maxConcurrency: 2 })
  .buffer(2)
  .reduce((acc: number, x: number) => acc + x, 0)
  .local((p) => p.reduce((acc: number, v: number) => acc + v, 0))
  .toArray();
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
