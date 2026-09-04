# Sequential Strategy

`sequential` processes chunks one at a time in order. This is the **default strategy** used by
`@outputty/pipeline` when no execution strategy is specified.

## When to Use

- **Ordered processing required** - When output order must match input order
- **Stateful operations** - When transformations depend on previous results
- **Limited resources** - When you want predictable, low memory usage
- **Debugging** - When you need deterministic execution order for debugging

## Usage

### Default (Implicit)

```typescript
import { Pipeline, Transformer } from "@outputty/pipeline";

// Sequential is the default strategy
const result = await new Pipeline([1, 2, 3])
  .transform((t) => t.map((x: number) => x * 2))
  .toArray();
// Output: [2, 4, 6]
```

### Explicit Strategy

```typescript
import { Pipeline, Transformer, sequential } from "@outputty/pipeline";

const transformer = new Transformer<number, number>({
  strategy: sequential,
  chunkSize: 10,
}).map((x: number) => x * 2);
```

### Using withExecutor

```typescript
import { Transformer, sequential } from "@outputty/pipeline";

const transformer = new Transformer<number, number>()
  .withExecutor(sequential)
  .map((x: number) => x * 2);
```

## Behavior

```
┌─────────────────────────────────────────────────────────────────────────┐
│  SEQUENTIAL EXECUTION                                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Input:  [chunk1] [chunk2] [chunk3] [chunk4]                           │
│              ↓                                                          │
│          process(chunk1) ─────────► yield result1                       │
│              ↓                                                          │
│          process(chunk2) ─────────► yield result2                       │
│              ↓                                                          │
│          process(chunk3) ─────────► yield result3                       │
│              ↓                                                          │
│          process(chunk4) ─────────► yield result4                       │
│                                                                         │
│  Output order: Guaranteed to match input order                          │
│  Memory:       O(chunk_size) - only one chunk in memory                 │
│  Concurrency:  1 (single-threaded)                                      │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## Options

`sequential` takes no options. It simply processes chunks in order.

## Implementation

`sequential` is an `ExecutionStrategy<In, Out>` - a plain function, `(transformerLogic, chunks,
context) => AsyncGenerator<Out[]>` - not a class. A custom strategy is the exact same shape:

```typescript
import type { ExecutionStrategy } from "@outputty/pipeline";

// This is `sequential`'s own implementation, and the shape any custom strategy takes:
const mySequential: ExecutionStrategy<number, number> = async function* (logic, chunks, ctx) {
  for await (const chunk of chunks) yield logic(chunk, ctx);
};
```

## Comparison with concurrent

- **Order guarantee** - Sequential: ✅ Always preserved. Concurrent: ✅ With `ordered: true`.
- **Throughput** - Sequential: lower. Concurrent: higher.
- **Resource usage** - Sequential: minimal. Concurrent: configurable.
- **Use case** - Sequential: I/O-bound, ordered. Concurrent: CPU-bound, parallel.

## Example: Multi-Step LLM Processing

Sequential is ideal when each step depends on the previous:

```typescript
const [result] = await new Pipeline(["Document content"])
  .transform((t) =>
    t
      .map((doc: string) => ({
        doc,
        summary: llm.complete(`Summarize: ${doc}`),
      }))
      .map((ctx: { doc: string; summary: string }) => ({
        ...ctx,
        critique: llm.complete(`Critique: ${ctx.summary}`),
      }))
      .map((ctx: { doc: string; summary: string; critique: string }) =>
        llm.complete(`Improve based on: ${ctx.critique}`),
      ),
  )
  .toArray();
```

## See Also

- [concurrent](./concurrent.md) - Parallel execution
- [Pipeline](../pipeline.md) - Data pipeline API
