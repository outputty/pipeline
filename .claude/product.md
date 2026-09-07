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
> and one of five terminal ops (`.toArray()`/`.first()`/`.consume()`/`.forEach()`/`.branch()`) to
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

```ts
import { ClusterPipeline } from "@outputty/pipeline";

const data = await new ClusterPipeline([1, 2, 3, 4, 5])
  .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 4))
  .toArray();
```

```json
[6, 8, 10]
```

A dispatching class runs every stage elsewhere by default. `{ local: true }` keeps one stage in the
orchestrating process - useful when a stage is cheap, or touches something only the orchestrator has:

```ts
import { HttpPipeline } from "@outputty/pipeline";

const pipeline = new HttpPipeline(rows, { url: process.env.SELF_URL! })
  .transform((t) => t.map(expensiveScore))
  .transform((t) => t.filter((r) => r.ok), { local: true });

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

### Reducing

A reducer folds items into an accumulator, at two levels with one meaning. On a `Transformer` it
folds the one chunk it receives and keeps nothing between chunks. On a `Pipeline` it folds
everything it receives, which is the only place cross-chunk state lives. Both may produce more than
one value, and the chain continues after either - downstream stages run over every value a reducer
produced, never assuming there was one.

> **`Transformer.reduce(fn, initial)`** - folds ONE chunk. Run once and forget: no state survives to
> the next chunk.
> **`Pipeline.reduce(fn, initial)`** - folds EVERY chunk the pipeline produces. On a dispatching
> class (`ConcurrentPipeline.reduce(fn, initial, options?)`, `{ local: true }` its own addition, the
> base `Pipeline` never gains it) it runs remotely like any other stage, over one duplex connection
> whose accumulator lives for the life of that connection; `{ local: true }` keeps it in the
> orchestrating process instead.
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

A reduce stage is a serialization point: one accumulator, and on a dispatching class one connection,
so `maxConcurrency` does not apply to it. With `ordered: false` upstream the reducer folds in
completion order, so the fold must be order-insensitive. And because a remote reducer's emits arrive
while the input is still streaming, a failure mid-stream reaches the caller after values have
already flowed downstream - the price of results that arrive as they happen.
