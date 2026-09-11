# @outputty/pipeline

Async streaming data processing pipelines with chunking and concurrency control.

## Installation

```bash
pnpm add @outputty/pipeline
```

## Quick Start

A pipeline declares the type it accepts, holds no data, and IS the function you call.

<!-- compiles -->

```typescript
import { Pipeline } from "@outputty/pipeline";

// Compose once, with no data.
const doubled = new Pipeline<number>().transform((t) => t.map((x) => x * 2).filter((x) => x > 4));

// Call it with any input. Every callback is synchronous, so no `await` and no `Promise`.
console.log(doubled([1, 2, 3, 4, 5]).toArray()); // [6, 8, 10]
console.log(doubled([10, 20]).toArray()); // [20, 40]

// One async callback, or an async input, widens the whole chain.
const slow = new Pipeline<number>().transform((t) => t.map(async (x) => x * 2));
console.log(await slow([1, 2, 3]).toArray()); // [2, 4, 6]
```

Calling a pipeline returns a `PipelineResult`. That is where the terminal ops live -
`toArray()`, `first(n)`, `consume()`, `forEach(fn)`, `chunks()`, and both iteration protocols - so
a chain cannot be drained without an input, and a result cannot be extended.

## Core Concepts

A `Pipeline` wraps a source, cuts it into chunks once, and runs every stage over that same chunk
stream - a stage is either a `Transformer` chain (chunk in, chunk out) or a reducer (many chunks
in, fewer chunks out). Nothing here decides WHERE a stage runs; that is the class you construct,
covered in [Where the work runs](#where-the-work-runs) below.

```text
new Pipeline<In>()
	.buffer(size)          cuts items into chunks once - In[] chunks, 1000 by default
	.transform(...)        a Transformer stage: chunk in, chunk out, one call per chunk
	.reduce(...)           a reducer stage: folds every chunk, emits fewer chunks onward
	.toArray()             terminal op - the one place chunks become items again
```

`Transformer` is the chain itself - `map`/`filter`/`flatMap`/`tap`, chunk-agnostic, one call per
chunk (see [How a Transformer runs a chunk](#how-a-transformer-runs-a-chunk)). A reducer is not a
separate class; `Pipeline.reduce()`/`Transformer.reduce()` are stages in that SAME flow, they just
produce fewer chunks than they receive instead of one chunk per chunk - see
[Reducing](#reducing).

### Pipeline

High-level API for composing data sources with transformers:

<!-- compiles -->

```typescript
import { Pipeline } from "@outputty/pipeline";

const data = await new Pipeline<number>()
  .context({ multiplier: 10 })
  .transform((t) => t.map((x: number, ctx) => x * (ctx.get("multiplier") as number)))([
    1, 2, 3, 4, 5,
  ])
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
const data = await new ConcurrentPipeline<string>({ maxConcurrency: 10 })
  .transform((t) => t.map((s: string) => s.toUpperCase()))(["a", "b", "c"])
  .toArray();

console.log(JSON.stringify(data)); // ["A","B","C"]
```

`HttpPipeline` dispatches each chunk to another instance over HTTP; `ClusterPipeline` dispatches to
worker processes on the same machine, brought up automatically; `EventEmitterPipeline` hands each
chunk to Worker functions registered on `pipeline.emitter`, in this same process. See
[HttpPipeline](#httppipeline), [ClusterPipeline](#clusterpipeline) and
[EventEmitterPipeline](#eventemitterpipeline) in the API Reference for their constructors and knobs.

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
import { ConcurrentPipeline, SimpleContextManager } from "@outputty/pipeline";

const shared = new SimpleContextManager();
const pipeline = new ConcurrentPipeline<number>({ maxConcurrency: 2, context: shared })
  .buffer(2)
  .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 4))
  .tap((x: number, ctx) => {
    ctx.set("seen", (ctx.getOrDefault("seen", 0) as number) + 1);
  });

console.log(await pipeline([1, 2, 3, 4, 5]).toArray(), shared.toDict());
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
new Pipeline<T>(options?: PipelineOptions)
```

A pipeline holds its input TYPE, not its data: `T` is what it will be called with. Calling one
returns a `PipelineResult`, which is where the terminal operations live. `PipelineOptions` is these
two knobs and nothing else - everything a chain carries between calls is internal.

- **`options.context`** - an already-built `IContextManager`, for THIS process. Optional; survives
  every `.context()`/`.transform()`/`.buffer()` call as the SAME instance.
- **`options.contextFactory`** - builds an `IContextManager`, for any OTHER process (a
  `ClusterPipeline` worker re-executing the entry module has no way to receive an already-built
  instance across the process boundary). Optional; invoked at most once per process, only when
  `context` is absent.

#### Chainable Operations

- **`.context(obj)`** - merge values into the pipeline's OWN context manager, mutating it in place;
  a manager that rejects an unknown key propagates that error instead of being bypassed.
- **`.apply(transformer)`** - apply a pre-built transformer.
- **`.transform(fn)`** - build and apply a transformer inline.
- **`.local(build)`** - run a whole region of the chain in the orchestrating process; on
  `ConcurrentPipeline`/`HttpPipeline`/`ClusterPipeline`/`EventEmitterPipeline`, nothing `build` does
  can dispatch.
- **`.buffer(size)`** - collect items and re-chunk.
- **`.buffer(fn)`** - decide the chunk boundary per item instead of by count. `fn`'s own `emit()`
  flushes whatever is pending and resets it to `[]`; returning a value appends it to the (possibly
  just-reset) pending array, returning `DROP` skips the item. A `Promise`-returning `fn` widens the
  pipeline's Mode to `"async"`.
- **`.tap(fn | transformer)`** - observe items without changing them. Always runs in the
  orchestrating process, on every class; the stages either side of it still dispatch. Use
  `Transformer.tap` inside a `.transform()` to observe beside the work instead.
- **`.branch(build)`** - route items into named arms, each with its own pipeline. A stage, not a
  terminal: it returns a runner, and the runner produces one record keyed by arm name.
- **`.onError(fn)`** - the run handler. `fn` receives the error and the context; returning drops
  the failing chunk and the run continues, throwing stops the run. Position-dependent: only a
  stage applied AFTER this call is covered. See [Error Handling](#error-handling).

#### Calling a pipeline

`pipeline(input)` runs it. `input` is an `Iterable<T>` or an `AsyncIterable<T>`, and the result is a
`PipelineResult<T>` - never another pipeline, so a result cannot be extended.

### PipelineResult

One call's output. Every operation below re-drains the input, so a spent generator or stream yields
`[]` on a second read, by decision.

- **`.toArray()`** - collect all results into an array. To read what the run wrote to context, build
  the pipeline with `{ context: shared }` and read `shared` afterward; a call seeds a fresh manager
  from the chain's own values, so nothing else sees those writes.
- **`.first(n)`** - take first n items.
- **`.consume()`** - process all items without collecting.
- **`.forEach(fn)`** - execute side-effect for each item.
- **`.chunks()`** - iterate the chunks rather than the items; empty chunks are dropped.
- **`[Symbol.iterator]`** - a synchronous result spreads: `[...pipeline(rows)]`.
- **`[Symbol.asyncIterator]`** - any result iterates with `for await`.

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

Extends `Pipeline`. Runs several chunks of a stage at once, in this process. Every `Pipeline`
method above applies unchanged; `ConcurrentPipeline` adds no new ones, only its own constructor
knobs - see [Where the work runs](#where-the-work-runs) for items in flight.

Internally, `.apply()` never calls `Transformer.process()` here the way `Pipeline` does - it fans
`this._chunks` (the pipeline's own already-cut chunk stream) out through up to `maxConcurrency`
concurrent calls of the SAME stage. `ordered: true` keeps them in a sliding window so a slower
chunk is never overtaken by a faster one; `false` yields whichever chunk finishes first.

<!-- compiles -->

```typescript
import { ConcurrentPipeline } from "@outputty/pipeline";

const data = await new ConcurrentPipeline<number>({ maxConcurrency: 2 })
  .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 4))([1, 2, 3, 4, 5])
  .toArray();

console.log(JSON.stringify(data)); // [6,8,10]
```

- **`options.maxConcurrency`** - chunks kept in flight at once. Default `4`.
- **`options.ordered`** - restore input order in the output. Default `true`.

### HttpPipeline

Extends `ConcurrentPipeline`. Dispatches each chunk of a stage over HTTP to another instance
running the same code, instead of running it here. A stage is its POSITION in the chain, never a
function - the client POSTs `{ chunk, context }` to `/transform/<n>`, and the receiving instance's own
`_chunkTransforms[n]` (populated by running the exact same `.transform()` calls) is what actually
runs it. Both instances must run the same build.

Spinning one up is a plain Node script - `node:http`, `toNodeHandler`, `.listen(0)`, call itself,
close the server:

<!-- compiles -->

```typescript
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { HttpPipeline, toNodeHandler } from "@outputty/pipeline";

// The "another instance" side: a source-less pipeline holding the SAME chain, so its
// .fetch can serve it.
const worker = new HttpPipeline<number>({ url: "" }).transform((t) => t.map((x: number) => x * 2));

const server = createServer(toNodeHandler(worker.fetch));
await new Promise<void>((resolve) => server.listen(0, resolve));
const { port } = server.address() as AddressInfo;

const data = await new HttpPipeline<number>({ url: `http://localhost:${port}` })
  .transform((t) => t.map((x: number) => x * 2))([1, 2, 3, 4, 5])
  .toArray();

console.log(JSON.stringify(data)); // [2,4,6,8,10]

await new Promise<void>((resolve) => server.close(() => resolve()));
```

- **`options.url`** - required. Where another `HttpPipeline`/`ClusterPipeline` instance's `.fetch`
  is mounted.
- **`.fetch`** - a `(request: Request) => Promise<Response>` handler serving this pipeline's
  stages. Prefix-agnostic: it reads only its own trailing `/transform/<n>`/`/reduce/<n>` segment, so
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

const data = await new ClusterPipeline<number>()
  .transform((t) => t.map((x: number) => x * 2))([1, 2, 3, 4, 5])
  .toArray();

// Last line only - every worker also re-executes this module, each printing its own empty result first.
console.log(JSON.stringify(data)); // [2,4,6,8,10]
```

- **`options.workers`** - worker processes to bring up on first drain. Default
  `os.availableParallelism()`.

### EventEmitterPipeline

Extends `ConcurrentPipeline`. Hands each chunk of a stage to whichever Worker functions are
registered on `pipeline.emitter`, a `node:events`-shaped `EventEmitter` - no server, no separate
process, no url. The chain's own composed function auto-registers as a stage's first Worker the
moment the chain is built; registering an extra one is optional, for when other code in the same
process wants to add capacity or take over the work entirely.

<!-- compiles -->

```typescript
import { EventEmitterPipeline } from "@outputty/pipeline";

const pipeline = new EventEmitterPipeline<number>().transform((t) => t.map((x: number) => x * 2));

// Optional - registered from anywhere else, runs alongside the chain's own function.
pipeline.emitter.on("stage:0", ({ chunk, respond }) => respond(chunk.map((x: number) => x * 2)));

const data = await pipeline([1, 2, 3, 4, 5]).toArray();
console.log(JSON.stringify(data)); // [2,4,6,8,10]
```

Every Worker registered on a stage runs on every chunk that reaches it; whichever settles first -
`respond(value)` or `reject(error)` - decides that chunk. Lifecycle events
(`stage:<n>:dispatched`/`:done`/`:error`/`:end`, `pipeline:end`) let other code watch a run without
becoming a Worker itself, as long as it listens on one of those names rather than the bare
`stage:<n>` channel - registering on the bare channel makes that listener a Worker too.

- **`options.emitter`** - a caller-supplied `node:events`-compatible emitter. Optional; a fresh
  `EventEmitter` is built when omitted. Validated at construction: a caller's own compatible
  emitter (a namespaced one, a test double) must still carry `on`/`off`/`listeners`/
  `listenerCount`/`emit`.
- `.reduce()` is inherited unchanged from `ConcurrentPipeline` - it folds in-process, with no
  emitter involvement.

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
const pipeline = new Pipeline<number>({ context: myContextManager });
```

Pass `contextFactory` when a manager cannot travel - a `ClusterPipeline` worker or a separate
`HttpPipeline` instance runs in another process. Each process calls the factory once and reuses the
result, so a manager owning a connection opens one pool per worker rather than one per chunk.

<!-- illustrative -->

```typescript
const pipeline = new ClusterPipeline<number>({
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
const totals = await new Pipeline<Order>()
  .transform((t) =>
    t
      .filter((o) => o.cents > 0)
      .map((o) => o.cents / 100)
      .reduce((acc, cents) => acc + cents, 0),
  )(orders)
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
with nothing between them, collapse to the LAST one - only it is ever actually applied. `.reduce()`
emits one value per chunk, so it is what actually shows which boundary won: `.buffer(2)` would give
chunks `[1,2] [3,4] [5]` and sums `[3,7,5]`; `.buffer(1)` gives one item per chunk and sums
`[1,2,3,4,5]` - unchanged from the input, since each chunk is a single number folded with itself:

<!-- compiles -->

```typescript
import { Pipeline } from "@outputty/pipeline";

const data = await new Pipeline<number>()
  .buffer(2) // never applied - superseded before any stage reads it
  .buffer(1) // this is the boundary every later stage actually sees
  .transform((t) => t.reduce((acc: number, x: number) => acc + x, 0))([1, 2, 3, 4, 5])
  .toArray();

console.log(JSON.stringify(data)); // [1,2,3,4,5] - buffer(2) would have printed [3,7,5]
```

`.buffer(fn)` decides the boundary per item instead of by count - a `T[]` pending array the
framework owns, folded through it item by item. `fn`'s own `emit()` takes no value: it flushes
whatever is currently pending and resets it to `[]`; returning a value appends it to the (possibly
just-reset) pending array, and returning `DROP` skips the item entirely. `.buffer(size)` is this
same mechanism configured with an identity `fn` and a framework-side auto-flush at
`pending.length >= size`:

<!-- compiles -->

```typescript
import { Pipeline } from "@outputty/pipeline";

type Event = { id: number; ts: number };

const events: Event[] = [
  { id: 1, ts: 0 },
  { id: 2, ts: 60_000 },
  { id: 3, ts: 240_000 },
  { id: 4, ts: 300_000 },
  { id: 5, ts: 301_000 },
];

let windowStart = 0;
const fiveMinuteWindow = (item: Event, _ctx: unknown, emit: () => void): Event => {
  if (item.ts - windowStart >= 300_000) {
    emit();
    windowStart = item.ts;
  }
  return item;
};

const chunks: Event[][] = [];
for await (const chunk of new Pipeline<Event>().buffer(fiveMinuteWindow)(events).chunks()) {
  chunks.push(chunk);
}

console.log(JSON.stringify(chunks));
// [[{"id":1,"ts":0},{"id":2,"ts":60000},{"id":3,"ts":240000}],[{"id":4,"ts":300000},{"id":5,"ts":301000}]]
```

A `Promise`-returning `fn` widens the pipeline's Mode to `"async"`, the same rule `.reduce()`'s own
two-overload split already follows.

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

```text
t.process(chunks())
	for [1,2,3,4,5] (one chunk)
		filter's composed function(chunk)        the LAST .filter()/.map() call built
			await map's composed function(chunk)     recurses into what it was built on
				await identity(chunk)                the chain's own starting point
			chunk.map(x => x*2), settled together    map's own work - runs on the unwind
		chunk.filter(x => x>4)                       filter's own work - runs after map's finishes
	yield [6, 8, 10]
```

`Transformer.reduce()` fits this same chunk-in-chunk-out shape exactly - one chunk in, its fold
out. `Pipeline.reduce()` is the one stage that genuinely breaks it: it keeps STATE across every
chunk instead of resetting per chunk, which is why [Core Concepts](#core-concepts) draws it as
producing FEWER chunks than it receives rather than one-for-one - see [Reducing](#reducing).

## Reducing

A reducer folds items into an accumulator, at two levels with one meaning: `Transformer.reduce`
folds the ONE chunk it receives and keeps nothing between chunks; `Pipeline.reduce` folds EVERY
chunk the pipeline produces, the only place cross-chunk state lives. The chain continues after
either - downstream stages run over every value a reducer produced.

<!-- compiles -->

```typescript
import { Pipeline } from "@outputty/pipeline";

const data = await new Pipeline<number>()
  .reduce((acc: number, x: number) => acc + x, 0)
  .transform((t) => t.map((n: number) => n * 10))([1, 2, 3, 4, 5])
  .toArray();

console.log(data); // [150]
```

`emit`, the reducer callback's fourth parameter (`(acc, item, ctx, emit) => acc`), pushes a value
downstream mid-fold and resets the accumulator - a running total banked whenever it crosses a
threshold, with no trailing value when the last item already banked one:

<!-- compiles -->

```typescript
import { Pipeline } from "@outputty/pipeline";

const data = await new Pipeline<number>()
  .reduce((acc: number, x: number, _ctx, emit: (v: number) => void) => {
    acc += x;
    if (acc >= 6) {
      emit(acc);
      return 0;
    }
    return acc;
  }, 0)
  .transform((t) => t.map((n: number) => n * 10))([1, 2, 3, 4, 5])
  .toArray();

console.log(data); // [60, 90]
```

On `ConcurrentPipeline`/`HttpPipeline`/`ClusterPipeline`/`EventEmitterPipeline`, `.reduce()`
partitions the stream into `maxConcurrency` independent accumulators. Each partition's own result - an `emit()` mid-fold, or
its trailing accumulator once its share of the stream ends - flows downstream as an ordinary value,
the same way `emit()` output already does above: no forced merge, no thrown error.

<!-- compiles -->

```typescript
import { ConcurrentPipeline } from "@outputty/pipeline";

const data = await new ConcurrentPipeline<number>({ maxConcurrency: 2 })
  .buffer(2)
  .reduce(
    (acc: number, x: number) => acc + x,
    0,
  )([1, 2, 3, 4, 5])
  .toArray();

console.log(data); // two numbers summing to 15, e.g. [7, 8] - split is timing-dependent
```

A caller who wants ONE final value writes an ordinary second reduce, the same pattern used to fold
down any other multi-value reduce output - nothing named "combine":

<!-- compiles -->

```typescript
import { ConcurrentPipeline } from "@outputty/pipeline";

const data = await new ConcurrentPipeline<number>({ maxConcurrency: 2 })
  .buffer(2)
  .reduce((acc: number, x: number) => acc + x, 0)
  .local((p) => p.reduce((acc: number, v: number) => acc + v, 0))([1, 2, 3, 4, 5])
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
const recovered = await new Pipeline<string>()
  .transform((t) => t.onError(() => DROP).map(parseStrict))(["a", "b", "3", "d", "5"])
  .toArray();

console.log(recovered); // [ 3, 5 ]

// The run handler is what keeps a stream alive past a chunk nothing could repair.
const survived = await new Pipeline<string>()
  .buffer(1)
  .onError((err) => console.warn("dropping chunk:", err.message))
  .transform((t) => t.map(parseStrict))(["1", "x", "3", "4"])
  .toArray();

console.log(survived); // [ 1, 3, 4 ]
```

## Branching

Split processing into multiple paths:

<!-- compiles -->

```typescript
import { Pipeline } from "@outputty/pipeline";

const split = new Pipeline<number>().branch((b) =>
  b.when("evens", (x) => x % 2 === 0).otherwise("odds"),
);

const data = split([1, 2, 3, 4, 5]);

console.log(data.evens); // [2, 4]
console.log(data.odds); // [1, 3, 5]
```

## Patterns

Both are pinned first in [`.claude/examples.md`](.claude/examples.md) (Case 11, Case 12), each
proven for real on `Pipeline`, `ConcurrentPipeline`, `HttpPipeline` and `ClusterPipeline` - the
chain itself is class-agnostic, so that proof lives once in `.claude/examples.md` rather than
repeated per class here.

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

const data = await new Pipeline<string>()
  .transform((t) => t.onError(() => DROP).map(parseStrict))(["a", "1", "b", "3", "5"])
  .toArray();

console.log(JSON.stringify(data)); // [1,3,5]
```

### Same chain, more in flight

An I/O-bound per-item task - a network call, a query, anything that mostly waits - wastes that
wait time run one at a time: `Pipeline` never starts item 2's wait until item 1's is over.
`ConcurrentPipeline` runs several chunks' waits at once instead, with no change to the chain
itself - only the class, and `.buffer(1)` so each item is its own chunk, change:

<!-- compiles -->

```typescript
import { Pipeline, ConcurrentPipeline } from "@outputty/pipeline";

async function fetchScore(id: number): Promise<number> {
  await new Promise((resolve) => setTimeout(resolve, 5)); // stands in for a real network wait
  return id * 10;
}

// Pipeline: one item's wait finishes before the next one starts.
const sequential = await new Pipeline<number>()
  .buffer(1)
  .transform((t) => t.map(fetchScore))([1, 2, 3, 4, 5])
  .toArray();

// ConcurrentPipeline: up to 4 items waiting at once - the SAME chain, unchanged.
const concurrent = await new ConcurrentPipeline<number>({ maxConcurrency: 4 })
  .buffer(1)
  .transform((t) => t.map(fetchScore))([1, 2, 3, 4, 5])
  .toArray();

console.log(JSON.stringify({ sequential, concurrent })); // identical - only the wait overlaps
// {"sequential":[10,20,30,40,50],"concurrent":[10,20,30,40,50]}
```

## License

MIT
