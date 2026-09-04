# Concurrent Strategy

`concurrent(options?)` processes multiple chunks in parallel using p-limit for concurrency control. This enables higher throughput for I/O-bound operations like HTTP requests or LLM calls.

## When to Use

- **I/O-bound operations** - HTTP requests, LLM API calls, database queries
- **Independent transformations** - When chunks don't depend on each other
- **High throughput needed** - When processing speed is critical
- **Rate limiting** - Control concurrent requests to APIs

## Usage

### Using withExecutor (Recommended)

```typescript
import { Pipeline, Transformer, concurrent } from "@outputty/pipeline";

// Process 4 items concurrently (default)
const result = await new Pipeline(urls)
  .transform((t) => t.withExecutor(concurrent()).map((url: string) => fetch(url)))
  .toArray();
```

### With Custom Options

```typescript
// Process 8 items concurrently, maintain order
const result = await new Pipeline(urls)
  .transform((t) =>
    t.withExecutor(concurrent({ maxConcurrency: 8, ordered: true })).map(async (url: string) => {
      const res = await fetch(url);
      return res.json();
    }),
  )
  .toArray();
```

### Explicit Strategy

```typescript
import { Transformer, concurrent } from "@outputty/pipeline";

const transformer = new Transformer<string, Response>({
  strategy: concurrent({ maxConcurrency: 4, ordered: true }),
  chunkSize: 10,
  transform: (chunk) => Promise.all(chunk.map((url: string) => fetch(url))),
});
```

## Options

- **`maxConcurrency`** (`number`, default `4`) - maximum parallel operations. Validated eagerly:
  `concurrent({ maxConcurrency: 0 })` throws `"maxConcurrency must be at least 1"` at call time,
  before `.withExecutor()` ever runs it.
- **`ordered`** (`boolean`, default `true`) - whether to maintain input order in output.

## Behavior

### Ordered Mode (`ordered: true`, default)

```
┌─────────────────────────────────────────────────────────────────────────┐
│  CONCURRENT ORDERED EXECUTION (maxConcurrency: 3)                       │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Input:  [c1] [c2] [c3] [c4] [c5]                                      │
│                                                                         │
│  Time 0: ┌────┐ ┌────┐ ┌────┐                                          │
│          │ c1 │ │ c2 │ │ c3 │  ← 3 concurrent                          │
│          └────┘ └────┘ └────┘                                          │
│                                                                         │
│  Time 1: yield result1 (wait for order)                                │
│          ┌────┐ ┌────┐ ┌────┐                                          │
│          │ c2 │ │ c3 │ │ c4 │  ← c1 done, c4 starts                    │
│          └────┘ └────┘ └────┘                                          │
│                                                                         │
│  Output order: Guaranteed [r1, r2, r3, r4, r5]                          │
│  Throughput:   ~3x single-threaded (with maxConcurrency: 3)             │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Unordered Mode (`ordered: false`)

```
┌─────────────────────────────────────────────────────────────────────────┐
│  CONCURRENT UNORDERED EXECUTION (maxConcurrency: 3)                     │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Input:  [c1] [c2] [c3] [c4] [c5]                                      │
│                                                                         │
│  Time 0: ┌────┐ ┌────┐ ┌────┐                                          │
│          │ c1 │ │ c2 │ │ c3 │  ← 3 concurrent                          │
│          └────┘ └────┘ └────┘                                          │
│                    ↓                                                    │
│  Time 1: yield result2 (c2 finished first!)                            │
│          ┌────┐       ┌────┐ ┌────┐                                    │
│          │ c1 │       │ c3 │ │ c4 │  ← c4 starts immediately           │
│          └────┘       └────┘ └────┘                                    │
│                                                                         │
│  Output order: NOT guaranteed - results yield as they complete          │
│  Throughput:   Maximum possible (no waiting for order)                  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## Examples

### HTTP Request Rate Limiting

```typescript
import { Pipeline, Transformer, concurrent } from "@outputty/pipeline";

const urls = [
  "https://api.example.com/item/1",
  "https://api.example.com/item/2",
  // ... 100 more URLs
];

// Fetch all URLs with max 10 concurrent requests
const results = await new Pipeline(urls)
  .transform((t) =>
    t.withExecutor(concurrent({ maxConcurrency: 10 })).map(async (url: string) => {
      const res = await fetch(url);
      if (!res.ok) throw new Error(`Failed: ${url}`);
      return res.json();
    }),
  )
  .toArray();
```

### LLM Batch Processing

```typescript
// Process documents with parallel LLM calls
const summaries = await new Pipeline(documents)
  .transform((t) =>
    t.withExecutor(concurrent({ maxConcurrency: 5 })).map(async (doc: Document) => {
      const summary = await llm.complete(`Summarize:\n${doc.content}`);
      return { id: doc.id, summary };
    }),
  )
  .toArray();
```

### Unordered Processing for Speed

```typescript
// When order doesn't matter, maximize throughput
const results = await new Pipeline(tasks)
  .transform((t) =>
    t.withExecutor(concurrent({ maxConcurrency: 20, ordered: false })).map(processTask),
  )
  .toArray();
```

## Writing a Custom Strategy

`concurrent` is not privileged - it is an `ExecutionStrategy<In, Out>` value like any other, the same
shape [`sequential`](./sequential.md) is. A caller writes their own directly, no registration step:

```typescript
import type { ExecutionStrategy } from "@outputty/pipeline";

// Runs chunks in fixed-size batches, sequentially between batches, in parallel within one.
function batched<In, Out>(batchSize = 5): ExecutionStrategy<In, Out> {
  return async function* (transformerLogic, chunks, context) {
    let buffer: In[][] = [];

    for await (const chunk of chunks) {
      buffer.push(chunk);
      if (buffer.length >= batchSize) {
        yield* buffer.map((c) => transformerLogic(c, context));
        buffer = [];
      }
    }
    yield* buffer.map((c) => transformerLogic(c, context));
  };
}

const transformer = new Transformer<string, string>()
  .withExecutor(batched(3))
  .map((url) => fetch(url));
```

## Comparison with sequential

- **Order guarantee** - Sequential: ✅ Always. Concurrent: ✅ With `ordered: true`.
- **Throughput** - Sequential: lower. Concurrent: higher.
- **Resource usage** - Sequential: minimal. Concurrent: configurable.
- **Best for** - Sequential: stateful operations. Concurrent: I/O-bound operations.
- **API rate limiting** - Sequential: N/A. Concurrent: ✅ via `maxConcurrency`.

## Implementation Notes

`concurrent` uses [p-limit](https://github.com/sindresorhus/p-limit) for concurrency control. This is a proven, battle-tested approach for Node.js environments.

```typescript
import pLimit from "p-limit";

// Internal implementation - a fresh limiter per run, so concurrent() results aren't shared state
const limit = pLimit(maxConcurrency);
const promises = chunks.map((chunk) => limit(() => transformerLogic(chunk, context)));
```

## See Also

- [sequential](./sequential.md) - Single-threaded execution
- [Pipeline](../pipeline.md) - Data pipeline API
