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

Chainable chunk transformation operations:

<!-- compiles -->

```typescript
import { Transformer } from "@outputty/pipeline";

const transformer = new Transformer<number, number>()
  .map((x: number) => x * 2)
  .filter((x: number) => x > 5)
  .map((x: number) => `Value: ${x}`);

// Execute directly over any async iterable
async function* source() {
  yield* [1, 2, 3, 4, 5];
}
const results: string[] = [];
for await (const item of transformer.execute(source())) {
  results.push(item);
}
```

### Execution Strategies

Control how chunks are processed:

<!-- compiles -->

```typescript
import { Transformer, sequential, concurrent } from "@outputty/pipeline";

// Sequential (default) - one chunk at a time
const sequentialUppercase = new Transformer<string, string>()
  .withExecutor(sequential)
  .map((s: string) => s.toUpperCase());

// Concurrent - parallel with limits
const concurrentUppercase = new Transformer<string, string>()
  .withExecutor(concurrent({ maxConcurrency: 10 }))
  .map((s: string) => s.toUpperCase());
```

## API Reference

### Pipeline

#### Constructor

<!-- illustrative -->

```typescript
new Pipeline<T>(data: PipelineSource<T>, options?: PipelineOptions)
```

#### Instance Methods

- **`.context(obj)`** - set context values for downstream operations.
- **`.apply(transformer)`** - apply a pre-built transformer.
- **`.transform(fn)`** - build and apply a transformer inline.
- **`.buffer(size)`** - collect items and re-chunk.
- **`.toArray()`** - collect all results into an array. Read `.contextManager` afterward for context.
- **`.first(n)`** - take first n items.
- **`.consume()`** - process all items without collecting.
- **`.forEach(fn)`** - execute side-effect for each item.
- **`.branch(definitions)`** - split into multiple branches.

### Transformer

#### Chainable Operations

- **`.map(fn)`** - transform each element.
- **`.flatMap(fn)`** - transform and flatten results.
- **`.filter(fn)`** - keep elements matching predicate.
- **`.reduce(fn, initial)`** - reduce to single value.
- **`.tap(fn)`** - execute side-effect without changing data.
- **`.catch(build, onError?)`** - run a sub-chain, handling its errors.
- **`.withExecutor(strategy)`** - set execution strategy (a function - `sequential`, `concurrent(options?)`, or your own).

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

This library processes data in chunks for efficiency:

<!-- compiles -->

```typescript
import { Transformer } from "@outputty/pipeline";

// Default chunk size is 1000
const t1 = new Transformer<number, number>();

// Custom chunk size
const t2 = new Transformer<number, number>({ chunkSize: 100 });

// Chunk size combines with any chain
const t3 = new Transformer<number, number>({ chunkSize: 100 }).map((x: number) => x * 2);
```

## Error Handling

<!-- compiles -->

```typescript
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
      (chunk, err) => {
        console.warn(`Skipping chunk [${chunk.join(", ")}]: ${err.message}`);
        // return a fallback array here to replace the failed chunk instead of dropping it
      },
    ),
  )
  .toArray();

console.log(data); // chunks that threw were dropped (or replaced by the handler's return)
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
import { Pipeline, concurrent } from "@outputty/pipeline";

interface User {
  id: number;
  name: string;
}

const enrichedUsers = await new Pipeline([1, 2, 3, 4, 5])
  .transform((t) =>
    t.withExecutor(concurrent({ maxConcurrency: 3 })).map(async (id: number): Promise<User> => {
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
import { Pipeline, concurrent } from "@outputty/pipeline";
import type { IContextManager } from "@outputty/pipeline";

interface LLM {
  complete(prompt: string): Promise<string>;
}

const summaries = await new Pipeline(documents)
  .context({ llm: myLlmInstance })
  .transform((t) =>
    t
      .withExecutor(concurrent({ maxConcurrency: 5, ordered: true }))
      .map(async (doc: Document, ctx: IContextManager) => {
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

## Execution Strategies

- **`sequential`** - default. Safe, predictable order. Use for I/O-bound or order-sensitive work.
- **`concurrent`** - parallel processing. Use for independent operations with rate limits.

See [Strategy Documentation](./docs/strategies/) for details:

- [sequential](./docs/strategies/sequential.md)
- [concurrent](./docs/strategies/concurrent.md)

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
