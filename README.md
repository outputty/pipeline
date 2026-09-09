# @outputty/pipeline

Async streaming data processing pipelines with chunking and concurrency control.

## Installation

```bash
pnpm add @outputty/pipeline
```

## Quick Start

<!-- compiles -->

```typescript
import { Pipeline } from "@outputty/pipeline";

// Basic transformation
const data = await new Pipeline([1, 2, 3, 4, 5])
  .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 4))
  .toArray();

console.log(data); // [6, 8, 10]
```

## Core Concepts

```
┌───────────────────────────────────────────────────────────────────────────┐
│  PIPELINE ARCHITECTURE                                                    │
├───────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐             │
│  │   Pipeline   │      │ Transformer  │      │   Reducer    │             │
│  │              │      │              │      │              │             │
│  │ Data source  │      │ map / filter │      │ Folds chunks │             │
│  │  + context   │      │flatMap / tap │      │  into state  │             │
│  └──────────────┘      └──────────────┘      └──────────────┘             │
│                                                                           │
│  Data flow (one chunk at a time):                                         │
│  input[] ──▶ chunk[] ──▶ map/filter/flatMap ──▶ chunk[] ──▶ output[]      │
│                                        │                                  │
│                                        └──▶ .reduce() ──▶ folded[]        │
│                                             (optional, across every chunk)│
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

`Pipeline` wraps a source and decides where its chunks run - the class you construct, not a config
knob (see [Where the work runs](#where-the-work-runs)). `Transformer` is the chain itself:
per-chunk operations, chunk-agnostic. `Reducer` is the one exception - it folds STATE across every
chunk instead of transforming one, which is why it gets its own box; see
[How a Transformer runs a chunk](#how-a-transformer-runs-a-chunk) for the mechanism and
[Reducing](#reducing) for the API.

### Pipeline

High-level API for composing data sources with transformers:

<!-- compiles -->

```typescript
import { Pipeline } from "@outputty/pipeline";

const data = await new Pipeline([1, 2, 3, 4, 5])
  .context({ multiplier: 10 })
  .transform((t) => t.map((x: number, ctx) => x * (ctx.get("multiplier") as number)))
  .toArray();

console.log(data); // [10, 20, 30, 40, 50]
```

### Transformer

Chainable chunk transformation operations - chunk-agnostic: it never decides how its own input was
cut, only processes whatever chunk it is handed.

<!-- compiles -->

```typescript
import { Transformer } from "@outputty/pipeline";

const transformer = new Transformer<number, number>()
  .map((x: number) => x * 2)
  .filter((x: number) => x > 5)
  .map((x: number) => `Value: ${x}`);

// Run directly over already-cut chunks, independent of Pipeline.
async function* chunks() {
  yield [1, 2, 3];
  yield [4, 5];
}
const results: string[][] = [];
for await (const chunk of transformer.process(chunks())) {
  results.push(chunk);
}
console.log(results); // [["Value: 6"], ["Value: 8", "Value: 10"]]
```

### Where the work runs

The class you construct decides where a chain's chunks are processed - the chain itself never
changes, only the class name does:

<!-- compiles -->

```typescript
import { ConcurrentPipeline } from "@outputty/pipeline";

// Up to 10 chunks in flight at once, in this process - Pipeline (one at a time) is the default
const data = await new ConcurrentPipeline(["a", "b", "c"], { maxConcurrency: 10 })
  .transform((t) => t.map((s: string) => s.toUpperCase()))
  .toArray();

console.log(JSON.stringify(data)); // ["A","B","C"]
```

`HttpPipeline` dispatches each chunk to another instance over HTTP; `ClusterPipeline` dispatches to
worker processes on the same machine, brought up automatically. See [HttpPipeline](#httppipeline)
and [ClusterPipeline](#clusterpipeline) in the API Reference for their constructors and knobs.

The chunk is the unit of concurrency, so a `ConcurrentPipeline`'s parallelism is its buffer size
times `maxConcurrency` - items in flight - never `maxConcurrency` alone. A chain left at the
default buffer of 1000 with `maxConcurrency: 3` holds 3000 callbacks in flight, not 3. Call
`.buffer(size)` to lower the ceiling, and prefer the widest chunk that fits it: measured in
[`.claude/architecture.md`](.claude/architecture.md), two chains holding the same 16 in flight over
50000 items ran 27 ms and 63 ms, because a narrow chunk pays the per-chunk cost more often.

| `.buffer(size)` | `maxConcurrency` | items in flight |
| --------------- | ---------------- | --------------- |
| 1000            | 1                | 1000            |
| 100             | 3                | 300             |
| 1               | 16               | 16              |

`.tap()` is the one exception, and it is deliberate. `Pipeline.tap(fn)` always runs in the
orchestrating process, whichever class it is called on, so a `console.log` or a `ctx.set()` written
at pipeline level lands where you can see it. The stages either side of it still dispatch:

<!-- compiles -->

```typescript
import { ConcurrentPipeline } from "@outputty/pipeline";

const pipeline = new ConcurrentPipeline([1, 2, 3, 4, 5], { maxConcurrency: 2 })
  .buffer(2)
  .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 4))
  .tap((x: number, ctx) => {
    ctx.set("seen", (ctx.getOrDefault("seen", 0) as number) + 1);
  });

console.log(await pipeline.toArray(), pipeline.contextManager.toDict());
// [ 6, 8, 10 ] { seen: 3 }
```

Use `.tap()` inside a `.transform()` instead when you want the callback to run beside the work, in
the worker. A tap's context write is per chunk, not per item: the whole chunk is tapped before the
next stage sees any of it.

## API Reference

### Pipeline

#### Constructor

<!-- illustrative -->

```typescript
new Pipeline<T>(data: PipelineSource<T>, options?: PipelineOptions)
```

- **`options.context`** - an already-built `IContextManager`, for THIS process. Optional; survives
  every `.context()`/`.transform()`/`.buffer()` call as the SAME instance.
- **`options.contextFactory`** - builds an `IContextManager`, for any OTHER process (a
  `ClusterPipeline` worker re-executing the entry module has no way to receive an already-built
  instance across the process boundary). Optional; invoked at most once per process, only when
  `context` is absent.

#### Static Methods

- **`Pipeline.merge(pipelines, options?)`** - concatenate several pipelines' data and contexts into
  a fresh, plain `Pipeline`. See [Merging](#merging).

#### Instance Methods

- **`.context(obj)`** - merge values into the pipeline's OWN context manager, mutating it in place;
  a manager that rejects an unknown key propagates that error instead of being bypassed.
- **`.apply(transformer)`** - apply a pre-built transformer.
- **`.transform(fn)`** - build and apply a transformer inline.
- **`.local(build)`** - run a whole region of the chain in the orchestrating process; on
  `ConcurrentPipeline`/`HttpPipeline`/`ClusterPipeline`, nothing `build` does can dispatch.
- **`.buffer(size)`** - collect items and re-chunk.
- **`.tap(fn | transformer)`** - observe items without changing them. Always runs in the
  orchestrating process, on every class; the stages either side of it still dispatch. Use
  `Transformer.tap` inside a `.transform()` to observe beside the work instead.
- **`.toArray()`** - collect all results into an array. Read `.contextManager` afterward for context.
- **`.first(n)`** - take first n items.
- **`.consume()`** - process all items without collecting.
- **`.forEach(fn)`** - execute side-effect for each item.
- **`.branch(definitions)`** - split into multiple branches.
- **`.merge(...others)`** - concatenate other pipelines' items and contexts onto THIS one, keeping
  THIS pipeline's own class, knobs and stage numbering. See [Merging](#merging).
- **`.onError(fn)`** - the run handler. `fn` receives the error and the context; returning drops
  the failing chunk and the run continues, throwing stops the run. Position-dependent: only a
  stage applied AFTER this call is covered. See [Error Handling](#error-handling).

### Transformer

#### Chainable Operations

- **`.map(fn)`** - transform each element.
- **`.flatMap(fn)`** - transform and flatten results.
- **`.filter(fn)`** - keep elements matching predicate.
- **`.reduce(fn, initial)`** - fold this ONE chunk; `fn` is `(acc, item, ctx, emit) => acc`, called
  with all four arguments regardless of its own declared arity. See [Reducing](#reducing).
- **`.tap(fn | transformer)`** - execute a side-effect without changing data. `fn` receives each item
  and the context; the `transformer` form receives the whole chunk. This one travels with its stage,
  so on `HttpPipeline`/`ClusterPipeline` it runs in the worker. `Pipeline.tap(...)` is the same
  observation point run in the orchestrating process instead - see [Where the work runs](#where-the-work-runs).
- **`.onError(fn)`** - the row handler. `fn` receives the failing item, error and context; a
  returned value replaces the row, `DROP` removes it, throwing escalates to the pipeline. See
  [Error Handling](#error-handling).

### ConcurrentPipeline

Extends `Pipeline`. Runs several chunks of a stage at once, in this process - see
[Where the work runs](#where-the-work-runs) for a real construction example and items in flight.
Every `Pipeline` method above applies unchanged; `ConcurrentPipeline` adds no new ones, only its
own constructor knobs.

Internally, `.apply()` never calls `Transformer.process()` here the way `Pipeline` does - it fans
`this._chunks` (the pipeline's own already-cut chunk stream) out through up to `maxConcurrency`
concurrent calls of the SAME stage. `ordered: true` keeps them in a sliding window so a slower
chunk is never overtaken by a faster one; `false` yields whichever chunk finishes first.

<!-- illustrative -->

```typescript
new ConcurrentPipeline<T>(data: PipelineSource<T>, options?: ConcurrentPipelineOptions)
```

- **`options.maxConcurrency`** - chunks kept in flight at once. Default `4`.
- **`options.ordered`** - restore input order in the output. Default `true`.

### HttpPipeline

Extends `ConcurrentPipeline`. Dispatches each chunk of a stage over HTTP to another instance
running the same code, instead of running it here. A stage is its POSITION in the chain, never a
function - the client POSTs `{ chunk, context }` to `/stage/<n>`, and the receiving instance's own
`_chunkTransforms[n]` (populated by running the exact same `.transform()` calls) is what actually
runs it. Both instances must run the same build.

Spinning one up is a plain Node script - `node:http`, `toNodeHandler`, `.listen(0)`, call itself,
close the server:

<!-- compiles -->

```typescript
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { HttpPipeline, toNodeHandler } from "@outputty/pipeline";

// The "another instance" side: an empty-source pipeline holding the SAME chain, so its
// .fetch can serve it.
const worker = new HttpPipeline<number>([], { url: "" }).transform((t) =>
  t.map((x: number) => x * 2),
);

const server = createServer(toNodeHandler(worker.fetch));
await new Promise<void>((resolve) => server.listen(0, resolve));
const { port } = server.address() as AddressInfo;

const data = await new HttpPipeline([1, 2, 3, 4, 5], { url: `http://localhost:${port}` })
  .transform((t) => t.map((x: number) => x * 2))
  .toArray();

console.log(JSON.stringify(data)); // [2,4,6,8,10]

await new Promise<void>((resolve) => server.close(() => resolve()));
```

- **`options.url`** - required. Where another `HttpPipeline`/`ClusterPipeline` instance's `.fetch`
  is mounted.
- **`.fetch`** - a `(request: Request) => Promise<Response>` handler serving this pipeline's
  stages. Prefix-agnostic: it reads only its own trailing `/stage/<n>`/`/reduce/<n>` segment, so
  mounting it under any path is safe.
- **`toNodeHandler(handler)`** - bridges a `.fetch` handler to `node:http`'s `(req, res)` callback
  shape; Node exposes `Request`/`Response`/`fetch` but serves no fetch handler natively.

### ClusterPipeline

Extends `HttpPipeline`. Dispatches each chunk of a stage to another process on the same machine.
Needs no server, port, url or fork in caller code - it brings its own workers up on the first
dispatch and every later `ClusterPipeline` in the process reuses them.

Internally it reuses `HttpPipeline`'s own dispatch: on the first real dispatch it forks `workers`
processes via `node:cluster`, each re-running this SAME entry module (so each registers the same
stages), routed through one shared server - `listen(0)` inside `cluster` hands every worker the
identical port. A worker with nothing left in flight is killed after 500ms idle, which is why the
canonical example below exits on its own with no explicit teardown:

<!-- compiles -->

```typescript
import { ClusterPipeline } from "@outputty/pipeline";

const data = await new ClusterPipeline([1, 2, 3, 4, 5])
  .transform((t) => t.map((x: number) => x * 2))
  .toArray();

// Last line only - every worker also re-executes this module, each printing its own empty result first.
console.log(JSON.stringify(data)); // [2,4,6,8,10]
```

- **`options.workers`** - worker processes to bring up on first drain. Default
  `os.availableParallelism()`.

### SimpleContextManager

The one shipped context manager - an in-memory store, not process-safe. Pass your own class
through `options.context`/`options.contextFactory` for anything more.

<!-- illustrative -->

```typescript
new SimpleContextManager(initial?: Record<string, unknown>)
```

- **`.get(key)`** - the value at `key`, or `undefined`.
- **`.set(key, value)`** - stores a value at `key`.
- **`.getOrDefault(key, defaultValue)`** - the value at `key`, or `defaultValue` when absent.
- **`.toDict()`** - a shallow copy of the whole store.

### Context-Aware Functions

Operations can access shared context:

<!-- illustrative -->

```typescript
// Context-aware map (receives context as second parameter)
.map((item: Item, ctx: IContextManager) => {
  const config = ctx.get('config')
  return processItem(item, config)
})

// Context-aware filter
.filter((item: Item, ctx: IContextManager) => {
  return item.type === ctx.get('allowedType')
})
```

The `ctx` parameter is optional: omit it and the item type is still inferred from the source, so a
callback never needs an explicit annotation.

### Supplying Your Own Context Manager

Pass `context` to use your own `IContextManager` instance in this process. Every operation keeps it,
writes included, and `.context()` merges into it rather than replacing it.

<!-- illustrative -->

```typescript
const pipeline = new Pipeline([1, 2, 3], { context: myContextManager });
```

Pass `contextFactory` when a manager cannot travel - a `ClusterPipeline` worker or a separate
`HttpPipeline` instance runs in another process. Each process calls the factory once and reuses the
result, so a manager owning a connection opens one pool per worker rather than one per chunk.

<!-- illustrative -->

```typescript
const pipeline = new ClusterPipeline([1, 2, 3], {
  workers: 3,
  contextFactory: () => new PgContext(pool),
});
```

Your manager's class decides whether state crosses a process. The pipeline seeds every worker forward
and never carries a worker's writes back, so a worker publishes through its own manager's store.

<!-- compiles -->

```typescript
import { Pipeline } from "@outputty/pipeline";

interface Order {
  id: number;
  cents: number;
}

const orders: Order[] = [
  { id: 1, cents: 3000 },
  { id: 2, cents: 4500 },
];

// `o` / `acc` are INFERRED from the typed source — no annotation, no implicit `any`.
const totals = await new Pipeline(orders)
  .transform((t) =>
    t
      .filter((o) => o.cents > 0)
      .map((o) => o.cents / 100)
      .reduce((acc, cents) => acc + cents, 0),
  )
  .toArray();
```

## Chunking

Rows move through a `Pipeline` in chunks (`In[]`/`Out[]`), not one at a time. A pipeline processing
one item per call pays for a function call, a promise and often a garbage-collected object per
row; batching rows into an array and running the WHOLE array through one call amortizes that cost
over every item in the batch instead of paying it per row. `map`/`filter`/`flatMap` never stream
item by item internally either - each is one recursive call over the chunk array it is handed (see
[How a Transformer runs a chunk](#how-a-transformer-runs-a-chunk)).

The boundary is the `Pipeline`'s own decision, not the `Transformer`'s: `.buffer(size)` sets it
explicitly, defaulting to `1000` when never called, and every later stage sees those same chunks
unchanged until another `.buffer()` call declares a new one. Two `.buffer()` calls back to back,
with nothing between them, collapse to the LAST one - only it is ever actually applied:

<!-- compiles -->

```typescript
import { Pipeline } from "@outputty/pipeline";

const data = await new Pipeline([1, 2, 3, 4, 5])
  .buffer(2) // never applied - superseded before any stage reads it
  .buffer(1) // this is the boundary every later stage actually sees
  .transform((t) => t.map((x: number) => x * 2))
  .toArray();

console.log(data); // [2, 4, 6, 8, 10]
```

## How a Transformer runs a chunk

`.map(f).filter(g)` builds ONE composed function, not two calls chained at runtime: each operator
wraps the chain built so far, so calling the last one built recurses down to the first, then runs
every operator's own work as that recursion unwinds - one call per LINK per chunk, not one call per
item per link. Real, instrumented run over `[1, 2, 3, 4, 5]`:

<!-- compiles -->

```typescript
import { Transformer } from "@outputty/pipeline";

const t = new Transformer<number, number>()
  .map((x: number) => {
    console.log(`map(${x})`);
    return x * 2;
  })
  .filter((x: number) => {
    console.log(`filter(${x})`);
    return x > 4;
  });

async function* chunks() {
  yield [1, 2, 3, 4, 5];
}

for await (const chunk of t.process(chunks())) {
  console.log("result:", chunk);
}
```

```text
map(1)
map(2)
map(3)
map(4)
map(5)
filter(2)
filter(4)
filter(6)
filter(8)
filter(10)
result: [ 6, 8, 10 ]
```

Every item finishes `map` before `filter` sees any of them - the whole chunk crosses from one link
to the next as a single array, never one row rejoining a shared queue between operators.

```
t.process(chunks())
	for [1,2,3,4,5] (one chunk)
		filter's composed function(chunk)        the LAST .filter()/.map() call built
			await map's composed function(chunk)     recurses into what it was built on
				await identity(chunk)                the chain's own starting point
			chunk.map(x => x*2), settled together    map's own work - runs on the unwind
		chunk.filter(x => x>4)                       filter's own work - runs after map's finishes
	yield [6, 8, 10]
```

`Pipeline.reduce()`/`Transformer.reduce()` are the one exception this chunk-in-chunk-out shape
does not cover: a reducer keeps STATE across chunks instead of producing one output chunk per
input chunk, which is why it needed its own box in [Core Concepts](#core-concepts) - see
[Reducing](#reducing).

## Reducing

A reducer folds items into an accumulator, at two levels with one meaning: `Transformer.reduce`
folds the ONE chunk it receives and keeps nothing between chunks; `Pipeline.reduce` folds EVERY
chunk the pipeline produces, the only place cross-chunk state lives. The chain continues after
either - downstream stages run over every value a reducer produced.

<!-- compiles -->

```typescript
import { Pipeline } from "@outputty/pipeline";

const data = await new Pipeline([1, 2, 3, 4, 5])
  .reduce((acc: number, x: number) => acc + x, 0)
  .transform((t) => t.map((n: number) => n * 10))
  .toArray();

console.log(data); // [150]
```

`emit`, the reducer callback's fourth parameter (`(acc, item, ctx, emit) => acc`), pushes a value
downstream mid-fold and resets the accumulator - a running total banked whenever it crosses a
threshold, with no trailing value when the last item already banked one:

<!-- compiles -->

```typescript
import { Pipeline } from "@outputty/pipeline";

const data = await new Pipeline([1, 2, 3, 4, 5])
  .reduce((acc: number, x: number, _ctx, emit: (v: number) => void) => {
    acc += x;
    if (acc >= 6) {
      emit(acc);
      return 0;
    }
    return acc;
  }, 0)
  .transform((t) => t.map((n: number) => n * 10))
  .toArray();

console.log(data); // [60, 90]
```

On `ConcurrentPipeline`/`HttpPipeline`/`ClusterPipeline`, `.reduce()` partitions the stream into
`maxConcurrency` independent accumulators. Each partition's own result - an `emit()` mid-fold, or
its trailing accumulator once its share of the stream ends - flows downstream as an ordinary value,
the same way `emit()` output already does above: no forced merge, no thrown error.

<!-- compiles -->

```typescript
import { ConcurrentPipeline } from "@outputty/pipeline";

const data = await new ConcurrentPipeline([1, 2, 3, 4, 5], { maxConcurrency: 2 })
  .buffer(2)
  .reduce((acc: number, x: number) => acc + x, 0)
  .toArray();

console.log(data); // two numbers summing to 15, e.g. [7, 8] - split is timing-dependent
```

A caller who wants ONE final value writes an ordinary second reduce, the same pattern used to fold
down any other multi-value reduce output - nothing named "combine":

<!-- compiles -->

```typescript
import { ConcurrentPipeline } from "@outputty/pipeline";

const data = await new ConcurrentPipeline([1, 2, 3, 4, 5], { maxConcurrency: 2 })
  .buffer(2)
  .reduce((acc: number, x: number) => acc + x, 0)
  .local((p) => p.reduce((acc: number, v: number) => acc + v, 0))
  .toArray();

console.log(data); // [15]
```

## Error Handling

Error handling belongs to the function that failed, at two levels. `Transformer.onError(fn)` is the
ROW handler: `fn` receives the failing item, the error and the context - returning a value replaces
the row, the exported `DROP` sentinel removes it, throwing escalates to the pipeline. It reaches
`.map()`, `.filter()`, `.flatMap()`, `.tap(fn)` and `.reduce()`'s fold step, wherever in the chain
it's written. `Pipeline.onError(fn)` is the RUN handler: `fn` receives the error and the context -
returning drops the failing chunk and the run continues, throwing stops the run.

<!-- compiles -->

```typescript
import { Pipeline, DROP } from "@outputty/pipeline";

const parseStrict = (s: string): number => {
  const n = parseInt(s);
  if (isNaN(n)) throw new Error(`Invalid: ${s}`);
  return n;
};

// A dropped row is repaired out of the chunk, not lost with it.
const recovered = await new Pipeline(["a", "b", "3", "d", "5"])
  .transform((t) => t.onError(() => DROP).map(parseStrict))
  .toArray();

console.log(recovered); // [ 3, 5 ]

// The run handler is what keeps a stream alive past a chunk nothing could repair.
const survived = await new Pipeline(["1", "x", "3", "4"])
  .buffer(1)
  .onError((err) => console.warn("dropping chunk:", err.message))
  .transform((t) => t.map(parseStrict))
  .toArray();

console.log(survived); // [ 1, 3, 4 ]
```

## Branching

Split processing into multiple paths:

<!-- compiles -->

```typescript
import { Pipeline, createTransformer } from "@outputty/pipeline";

const data = await new Pipeline([1, 2, 3, 4, 5]).branch({
  evens: { predicate: (x: number) => x % 2 === 0, transformer: createTransformer<number>() },
  odds: { predicate: (x: number) => x % 2 !== 0, transformer: createTransformer<number>() },
});

console.log(data.evens); // [2, 4]
console.log(data.odds); // [1, 3, 5]
```

## Merging

Several sources concatenate back into one - the other direction from branching, and there are two
ways to do it, pinned in [`.claude/examples.md`](.claude/examples.md) Case 5 and Case 8.

`Pipeline.merge(pipelines, options?)` concatenates every source pipeline's data and context into a
FRESH, plain `Pipeline`, for a caller who holds no pipeline of its own to continue. `pipelines` is
an array; `options.context`, when given, is the SAME instance returned as the merged pipeline's
`.contextManager`, later pipelines still winning on a shared key. With no `options`, a fresh
manager is built the same way.

<!-- compiles -->

```typescript
import { Pipeline } from "@outputty/pipeline";

const pipeline1 = new Pipeline([1, 2, 3]);
const pipeline2 = new Pipeline([4, 5, 6]);

const merged = Pipeline.merge([pipeline1, pipeline2]);
const data = await merged.toArray();

console.log(data); // [ 1, 2, 3, 4, 5, 6 ]
```

`pipeline.merge(...others)` continues a pipeline you already hold instead: it keeps THIS
pipeline's own class, knobs and stage numbering, so a stage applied after the merge runs at this
pipeline's own next index rather than restarting at 0. Reach for this over the static form
whenever work after the merge must stay concurrent, remote or clustered - the static form always
returns a plain `Pipeline`, so a merged `HttpPipeline` gaining one more stage would otherwise
collide with its own first stage on `/stage/0`.

<!-- illustrative -->

```typescript
import { HttpPipeline, ConcurrentPipeline } from "@outputty/pipeline";

const remote = new HttpPipeline([1, 2, 3, 4], { url: process.env.WORKER_URL! })
  .buffer(1)
  .transform((t) => t.map((x: number) => x + 1)); // stage 0, dispatched over HTTP

const local = new ConcurrentPipeline([10, 20])
  .buffer(1)
  .transform((t) => t.map((x: number) => x + 5)); // its own in-process fan-out, never the wire

const merged = remote.merge(local).transform((t) => t.map((x: number) => x * 100)); // stage 1 - THIS pipeline's own next index

const data = await merged.toArray();

console.log(data); // [ 200, 300, 400, 500, 1500, 2500 ]
```

## Patterns

The chain itself is class-agnostic - a pattern written once runs unchanged on `Pipeline`,
`ConcurrentPipeline`, `HttpPipeline` or `ClusterPipeline`, so it is shown once here, on plain
`Pipeline`. Both are pinned first in [`.claude/examples.md`](.claude/examples.md) (Case 11, Case
12), each with all four classes run and verified for real, for the one time the class actually
matters: proving the pattern survives the trip over HTTP and a real forked worker unchanged.

### Repairing bad rows without losing the batch

`Transformer.onError(fn)` drops or replaces a row that throws; the rows that parsed keep going.

<!-- compiles -->

```typescript
import { Pipeline, DROP } from "@outputty/pipeline";

const parseStrict = (s: string): number => {
  const n = parseInt(s);
  if (isNaN(n)) throw new Error(`Invalid: ${s}`);
  return n;
};

const data = await new Pipeline(["a", "1", "b", "3", "5"])
  .transform((t) => t.onError(() => DROP).map(parseStrict))
  .toArray();

console.log(JSON.stringify(data)); // [1,3,5]
```

### Bounded-concurrency fan-out over a real per-item task

The same async task, run with a bounded number of chunks in flight instead of one at a time.

<!-- compiles -->

```typescript
import { Pipeline } from "@outputty/pipeline";

async function fetchScore(id: number): Promise<number> {
  await new Promise((resolve) => setTimeout(resolve, 5));
  return id * 10;
}

const data = await new Pipeline([1, 2, 3, 4, 5]).transform((t) => t.map(fetchScore)).toArray();

console.log(JSON.stringify(data)); // [10,20,30,40,50]
```

## License

MIT
