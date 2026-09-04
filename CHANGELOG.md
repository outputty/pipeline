# @outputty/pipeline

## 0.2.0

### Minor Changes

- 8b1fc60: **Breaking:** `ExecutionStrategy<In, Out>` is now a plain function type instead of a class-implementing
  interface: `(transformerLogic, chunks, context) => AsyncGenerator<Out[]>`. `sequential` and
  `concurrent(options?)` replace the deleted `SequentialStrategy`/`ConcurrentStrategy` classes, and the
  executor registry (`registerExecutor`, `createStrategy`, `ExecutorType`, `ExecutorOptions`,
  `ExecutorSpec`, `CustomExecutor`, `ExecutorFactory`) is removed entirely — a custom strategy is now
  just a function passed straight to `.withExecutor()`, no registration or class needed.

  Migration:

  ```diff
  -import { Transformer, SequentialStrategy, ConcurrentStrategy } from "@outputty/pipeline";
  +import { Transformer, sequential, concurrent } from "@outputty/pipeline";

   const t = new Transformer<number, number>()
  -  .withExecutor(new ConcurrentStrategy({ maxConcurrency: 4 }))
  +  .withExecutor(concurrent({ maxConcurrency: 4 }))
     .map((x) => x * 2);
  ```

  Also fixes three consumer-facing type defects the untypechecked test suite had hidden:
  `TransformerLifecycleHooks` callbacks now type as a bare `void` (an ordinary `() => arr.push(x)` hook
  is assignable again); `Transformer.loop()`'s `condition` takes one `(chunk, ctx) => boolean` signature
  instead of a union of two arities; and `new Transformer<In, Out>()` with a mismatched `In`/`Out` and no
  `transform` is now a compile error instead of a silent runtime cast.

## 0.1.1

### Patch Changes

- 9b7b773: Set up changesets and a Trusted Publishing release workflow
