# Executor Registry

`registerExecutor`/`createStrategy` map a named executor spec (`"sequential"`/`"concurrent"`/a
custom registered name) to the `ExecutionStrategy` it builds. Module-scope state, not a class: there
is no `ExecutorRegistry` instance to get or reset.

## Usage

### Create Built-in Strategies

```typescript
import { createStrategy } from "@outputty/pipeline";

const sequential = createStrategy("sequential");

const concurrent = createStrategy("concurrent", {
  maxConcurrency: 8,
  ordered: true,
});
```

### Use with Transformer

The recommended way is via `Transformer.withExecutor()`:

```typescript
import { Transformer } from "@outputty/pipeline";

const transformer = new Transformer<string, string>()
  .withExecutor("concurrent", { maxConcurrency: 10 })
  .map((s: string) => s.toUpperCase());
```

### Register Custom Strategies

```typescript
import { registerExecutor, createStrategy } from "@outputty/pipeline";
import type { ExecutorFactory, ExecutionStrategy, ExecutorOptions } from "@outputty/pipeline";

// Create a custom factory
class MyStrategyFactory implements ExecutorFactory {
  create(options?: ExecutorOptions): ExecutionStrategy<unknown, unknown> {
    return new MyCustomStrategy(options);
  }
}

// Register it — the name is now usable everywhere, including transformer.withExecutor(name)
registerExecutor("my-strategy", new MyStrategyFactory());

// Use it
const transformer = new Transformer<number, number>()
  .withExecutor("my-strategy", { customOption: true })
  .map((x: number) => x * 2);

// createStrategy sees the same registration
createStrategy("my-strategy");
```

### Use Inline Custom Strategy

For one-off custom strategies, skip registration entirely:

```typescript
const customStrategy = new MyCustomStrategy({ options });

const transformer = new Transformer<number, number>()
  .withExecutor({ custom: customStrategy })
  .map((x: number) => x * 2);
```

## API Reference

### `registerExecutor(name, factory)`

Registers a custom executor factory under `name`, making it usable later as
`transformer.withExecutor(name)` or `createStrategy(name)`.

### `createStrategy(spec, options?)`

Builds an `ExecutionStrategy` from a spec — a registered name, or `{ custom }` for a one-off
strategy instance handed straight through. Throws `"Unknown executor type: <name>. Available:
..."` for an unregistered name.

## Built-in Executor Types

- **`'sequential'`** - class `SequentialStrategy`: process chunks one at a time.
- **`'concurrent'`** - class `ConcurrentStrategy`: process chunks in parallel.

## Creating Custom Strategies

To create a custom execution strategy:

```typescript
import type { ExecutionStrategy, InternalTransformer, IContextManager } from "@outputty/pipeline";

class BatchedStrategy<In, Out> implements ExecutionStrategy<In, Out> {
  constructor(private batchSize: number = 5) {}

  async *execute(
    transformerLogic: InternalTransformer<In, Out>,
    chunks: AsyncIterable<In[]>,
    context: IContextManager,
  ): AsyncGenerator<Out[]> {
    const buffer: In[][] = [];

    for await (const chunk of chunks) {
      buffer.push(chunk);

      if (buffer.length >= this.batchSize) {
        // Process entire batch
        for (const c of buffer) {
          yield transformerLogic(c, context);
        }
        buffer.length = 0;
      }
    }

    // Process remaining
    for (const c of buffer) {
      yield transformerLogic(c, context);
    }
  }
}

// Register with factory
class BatchedStrategyFactory implements ExecutorFactory {
  create(options?: { batchSize?: number }): ExecutionStrategy<unknown, unknown> {
    return new BatchedStrategy(options?.batchSize ?? 5);
  }
}

registerExecutor("batched", new BatchedStrategyFactory());
```

## Testing

Registration is process-wide, module-scope state — there is no per-test reset. Register a
uniquely-named strategy per test (or per test file) rather than relying on isolation between runs:

```typescript
import { registerExecutor } from "@outputty/pipeline";

it("uses a custom strategy", () => {
  registerExecutor("test-strategy-unique-name", new TestStrategyFactory());

  // Test code...
});
```

## See Also

- [SequentialStrategy](./sequential.md) - Default sequential execution
- [ConcurrentStrategy](./concurrent.md) - Parallel execution
- [Transformer](../pipeline.md) - Pipeline and transformer API
