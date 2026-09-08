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
┌─────────────────────────────────────────────────────────────────────────┐
│  PIPELINE ARCHITECTURE                                                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐          │
│  │   Pipeline   │──────│  Transformer │──────│   Strategy   │          │
│  │              │      │              │      │              │          │
│  │ Data source  │      │ Chain of     │      │ How chunks   │          │
│  │ + context    │      │ operations   │      │ are executed │          │
│  └──────────────┘      └──────────────┘      └──────────────┘          │
│                                                                         │
│  Data Flow:                                                             │
│  input[] ──▶ chunk[] ──▶ transform ──▶ chunk[] ──▶ output[]            │
│                              │                                          │
│                        (map, filter,                                    │
│                         reduce, etc.)                                   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

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
  .toArray(); // ["A", "B", "C"]
```

`HttpPipeline` dispatches each chunk to another instance over HTTP; `ClusterPipeline` dispatches to
worker processes on the same machine, brought up automatically. See [Core Concepts](#core-concepts)
above.

`.tap()` is the one exception, and it is deliberate. `Pipeline.tap(fn)` always runs in the
orchestrating process, whichever class it is called on, so a `console.log` or a `ctx.set()` written
at pipeline level lands where you can see it. The stages either side of it still dispatch:

<!-- illustrative -->

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
  a fresh plain `Pipeline`, for a caller who holds no pipeline of its own to continue. `pipelines` is
  an array. `options.context`, when given, is the SAME instance returned as the merged pipeline's
  `.contextManager`, with later pipelines still winning on a shared key; with no `options`, a fresh
  manager is built the same way. `Pipeline.merge([])` returns an empty pipeline. See `.merge()`
  below (#41) to merge onto a pipeline already held, keeping its class.

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
- **`.merge(...others)`** - concatenate other pipelines' items and contexts onto THIS one,
  keeping THIS pipeline's own class, knobs and stage numbering (#41) - a stage applied after
  reaches `others`' items too, at THIS pipeline's next index rather than restarting at 0. Prefer
  this over the static `Pipeline.merge()` whenever work after the merge must stay concurrent,
  remote or clustered; the static always returns a plain `Pipeline`.

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
- **`.catch(build, onError?)`** - run a sub-chain, handling its errors.

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

Rows move through a `Pipeline` in chunks, not one at a time. The boundary is the `Pipeline`'s own
decision, not the `Transformer`'s: `.buffer(size)` sets it explicitly, defaulting to `1000` when
never called, and every later stage sees those same chunks unchanged until another `.buffer()`
call declares a new one.

<!-- compiles -->

```typescript
import { Pipeline } from "@outputty/pipeline";

const data = await new Pipeline([1, 2, 3, 4, 5])
  .buffer(2)
  .transform((t) => t.map((x: number) => x * 2))
  .toArray();

console.log(data); // [2, 4, 6, 8, 10]
```

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

<!-- compiles -->

```typescript
import { Pipeline } from "@outputty/pipeline";

// A handler's returned array REPLACES the failing chunk.
const replaced = await new Pipeline(["a", "b", "3", "d", "5"])
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

console.log(replaced); // [999]

// A handler returning nothing DROPS the failing chunk instead.
const dropped = await new Pipeline(["a", "b", "3", "d", "5"])
  .transform((t) =>
    t.catch(
      (sub) =>
        sub.map((s: string) => {
          const n = parseInt(s);
          if (isNaN(n)) throw new Error(`Invalid: ${s}`);
          return n;
        }),
      () => undefined,
    ),
  )
  .toArray();

console.log(dropped); // []
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

## Real-World Examples

### HTTP Batch Processing

<!-- compiles -->

```typescript
import { ConcurrentPipeline } from "@outputty/pipeline";

interface User {
  id: number;
  name: string;
}

const enrichedUsers = await new ConcurrentPipeline([1, 2, 3, 4, 5], { maxConcurrency: 3 })
  .transform((t) =>
    t.map(async (id: number): Promise<User> => {
      const res = await fetch(`/api/users/${id}`);
      return (await res.json()) as User;
    }),
  )
  .toArray();
```

### File Processing Pipeline

<!-- compiles -->

```typescript
import { Pipeline } from "@outputty/pipeline";
import * as fs from "fs/promises";

interface FileInfo {
  path: string;
  content: string;
  words: number;
}

const stats = await new Pipeline(await fs.readdir("./docs"))
  .transform((t) =>
    t
      .filter((f: string) => f.endsWith(".md"))
      .map(async (f: string): Promise<FileInfo> => {
        const content = await fs.readFile(`./docs/${f}`, "utf-8");
        return { path: f, content, words: content.split(/\s+/).length };
      }),
  )
  .toArray();

console.log(stats);
// [{ path: 'readme.md', content: '...', words: 1234 }, ...]
```

### LLM Batch Processing

<!-- illustrative -->

```typescript
import { ConcurrentPipeline } from "@outputty/pipeline";
import type { IContextManager } from "@outputty/pipeline";

interface LLM {
  complete(prompt: string): Promise<string>;
}

const summaries = await new ConcurrentPipeline(documents, { maxConcurrency: 5, ordered: true })
  .context({ llm: myLlmInstance })
  .transform((t) =>
    t.map(async (doc: Document, ctx: IContextManager) => {
      const llm = ctx.get("llm") as LLM;
      const summary = await llm.complete(`Summarize: ${doc.content}`);
      return { ...doc, summary };
    }),
  )
  .toArray();
```

### Multi-Step Transform

<!-- illustrative -->

```typescript
const processed = await new Pipeline(rawFiles)
  .transform((t) =>
    t
      // Step 1: Parse
      .map((raw: string) => JSON.parse(raw) as Record<string, unknown>)
      // Step 2: Validate
      .filter((obj: Record<string, unknown>) => obj.status === "active")
      // Step 3: Transform
      .map((obj: Record<string, unknown>) => ({
        id: obj.id,
        name: (obj.name as string).toUpperCase(),
      }))
      // Step 4: Enrich
      .flatMap(async (item: { id: unknown; name: string }) => {
        const details = await fetchDetails(item.id);
        return [{ ...item, ...details }];
      }),
  )
  .toArray();
```

## Comparison with JSON Graph

This package provides a more ergonomic API than JSON-based graph definitions:

<!-- illustrative -->

```typescript
// JSON Graph approach
const graph = {
  nodes: {
    fetch: { fn: "fetchData", inputs: ["id"] },
    parse: { fn: "parseData", inputs: ["fetch.output"] },
    filter: { fn: "filterActive", inputs: ["parse.output"] },
  },
};
execute(graph, { id: 123 });

// @outputty/pipeline approach
new Pipeline([123])
  .transform((t) =>
    t
      .map((id: number) => fetchData(id))
      .map((data: RawData) => parseData(data))
      .filter((item: Item) => item.active),
  )
  .toArray();
```

Benefits:

- **Type safety** - Full TypeScript support with generics
- **Composability** - Build reusable transformers
- **Streaming** - Process data as it arrives, don't wait for all
- **Debuggability** - Stack traces point to actual code
- **Testability** - Standard unit testing, no graph mocking

## License

MIT
