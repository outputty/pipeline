# Multi-Core Parallelism Research for Laygo

**Date:** 2024-12-21
**Branch:** `feature/T090-multicore-parallelism-research`
**Status:** Research Complete

---

## Executive Summary

This document evaluates approaches to implementing **true multi-core parallelism** as a custom execution strategy in laygo. Our current `concurrent()` uses `p-limit` for concurrency control, but this only provides **async concurrency** on a single thread (the Node.js event loop). True parallelism requires `worker_threads` or `child_process`.

---

**Note (2026-09-04):** the class-shaped `WorkerPoolStrategy implements ExecutionStrategy<In, Out>` and
its `.execute()` method below predate `ExecutionStrategy`'s move from a class-implementing interface to
a plain function type (#5). `ExecutionStrategy<In, Out>` is now `(transformerLogic, chunks, context) =>
AsyncGenerator<Out[]>` - a class can no longer `implements` it. Building this proposal for real means a
factory function returning that shape, the same pattern `concurrent(options?)` already uses, with the
pool held in a closure instead of an instance field.

## Understanding the Problem

### Current State: Async Concurrency vs True Parallelism

```
┌─────────────────────────────────────────────────────────────────────────┐
│  CURRENT: concurrent() with p-limit                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Main Thread (Event Loop)                                               │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │  chunk1 ──┬── await I/O ──┬── process ──┬─▶ result1                │ │
│  │           │               │             │                          │ │
│  │  chunk2 ──┴── await I/O ──┴── process ──┴─▶ result2                │ │
│  │                                                                    │ │
│  │  ⚠️ CPU work still runs sequentially on event loop!                │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│  This works for I/O-bound tasks, but NOT for CPU-bound tasks!           │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│  GOAL: WorkerThreadStrategy with true parallelism                       │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Main Thread (Coordinator)                                              │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │  dispatch chunk1 ──▶ Worker 1 ──▶ result1                          │ │
│  │  dispatch chunk2 ──▶ Worker 2 ──▶ result2     ← runs in parallel!  │ │
│  │  dispatch chunk3 ──▶ Worker 3 ──▶ result3                          │ │
│  │  dispatch chunk4 ──▶ Worker 4 ──▶ result4                          │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│  Each worker is a real OS thread with its own V8 isolate.               │
│  CPU-intensive work runs in parallel across multiple cores.             │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### When Is Multi-Core Parallelism Beneficial?

- **I/O-bound** (HTTP, file reads) - Current concurrent(): ✅ Excellent. WorkerThread
  Strategy: ❌ Overhead hurts.
- **CPU-bound** (parsing, crypto) - Current concurrent(): ❌ Sequential. WorkerThread
  Strategy: ✅ True parallel.
- **Mixed I/O + CPU** - Current concurrent(): 🟡 Partial benefit. WorkerThread Strategy: ✅
  Full benefit.
- **Small data volume** - Current concurrent(): ✅ Low overhead. WorkerThread Strategy: ❌
  Thread overhead.
- **Large data volume** - Current concurrent(): 🟡 Event loop blocks. WorkerThread Strategy:
  ✅ Scales to cores.

---

## Top 3 Worker Pool Libraries Analysis

### 1. Piscina (Recommended)

**Weekly Downloads:** 6M+ | **Last Updated:** 1 month ago | **TypeScript:** Built-in

```typescript
// Usage pattern
import Piscina from "piscina";

const pool = new Piscina({
  filename: path.resolve(__dirname, "worker.js"),
  minThreads: 2,
  maxThreads: os.cpus().length,
});

const result = await pool.run({ a: 1, b: 2 });
```

**Architecture:**

```
┌────────────────────────────────────────────────────────────────────────┐
│  Main Thread                                                           │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │  Piscina Pool                                                    │  │
│  │  ┌─────────────────────────────────────────────────────────────┐ │  │
│  │  │  Task Queue (FIFO or Custom)                                │ │  │
│  │  │  [task1] [task2] [task3] [task4] ...                        │ │  │
│  │  └─────────────────────────────────────────────────────────────┘ │  │
│  │                 │         │         │         │                  │  │
│  │                 ▼         ▼         ▼         ▼                  │  │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐                │  │
│  │  │ Worker1 │ │ Worker2 │ │ Worker3 │ │ Worker4 │                │  │
│  │  │ V8 Iso  │ │ V8 Iso  │ │ V8 Iso  │ │ V8 Iso  │                │  │
│  │  │ Thread  │ │ Thread  │ │ Thread  │ │ Thread  │                │  │
│  │  └─────────┘ └─────────┘ └─────────┘ └─────────┘                │  │
│  └──────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────┘
```

**Pros:**

- High-performance Atomics-based communication
- Backpressure support (`maxQueue`, `needsDrain` event)
- AbortController support for cancellation
- Custom task queues (priority, LIFO, etc.)
- Multiple named functions per worker file
- Transferable objects support
- Extensive metrics (runTime, waitTime, utilization)
- Battle-tested (used by major projects)
- Core maintainers are Node.js collaborators

**Cons:**

- Workers must be in separate files (not inline functions)
- Data must be serializable (no closures, functions, or complex objects)
- Overhead for small tasks (~0.5ms per task dispatch)
- Memory per worker (~10-50MB depending on heap size)

**Pitfalls:**

1. **Serialization boundary** - Worker receives cloned data, not references
2. **Cold start** - First task to a new worker has initialization overhead
3. **Resource contention** - Too many pools = thread starvation
4. **Error isolation** - Uncaught worker errors terminate the worker

---

### 2. Workerpool

**Weekly Downloads:** 10M+ | **Last Updated:** 1 month ago | **TypeScript:** @types available

```typescript
import workerpool from "workerpool";

const pool = workerpool.pool(__dirname + "/worker.js");
const result = await pool.exec("add", [3, 4]);
await pool.terminate();
```

**Architecture:**

- Similar to Piscina but with dynamic function offloading
- Can send functions as strings (eval'd in worker)
- Supports browser Web Workers + Node.js worker_threads

**Pros:**

- Cross-platform (browser + Node.js)
- Dynamic function offloading (functions sent as strings)
- Proxy API for natural method calls
- Task timeout and cancellation
- Statistics available

**Cons:**

- Dynamic function offloading is **security risk** and **slow**
- Less performant than Piscina for Node.js use cases
- Larger bundle size (supports browser fallbacks)
- No Atomics-based fast path

**Pitfalls:**

1. **String eval** - Dynamic functions are eval'd = slow + insecure
2. **No typed returns** - Proxy loses type info
3. **Browser focus** - Node.js is secondary concern

---

### 3. Tinypool (Vitest's Choice)

**Weekly Downloads:** 13M+ | **Last Updated:** 4 months ago | **TypeScript:** Built-in

```typescript
import Tinypool from "tinypool";

const pool = new Tinypool({
  filename: new URL("./worker.mjs", import.meta.url).href,
});
const result = await pool.run({ a: 4, b: 6 });
await pool.destroy();
```

**Architecture:**

- Fork of Piscina with reduced feature set
- Focus on minimal size (38KB vs Piscina's ~800KB)
- Same core worker_threads mechanics
- Supports both `worker_threads` and `child_process`

**Pros:**

- Minimal footprint (no dependencies)
- Physical CPU core detection
- ESM-first design
- `isolateWorkers` option for fresh worker per task
- Memory limit detection (`maxMemoryLimitBeforeRecycle`)
- Worker teardown hooks

**Cons:**

- No utilization metrics
- No thread priority setting
- Less mature than Piscina
- Fewer configuration options

**Pitfalls:**

1. **Feature gaps** - Missing Piscina features may be needed later
2. **ESM only** - No CommonJS support
3. **Vitest-focused** - May not cover all use cases

---

### Library Comparison Matrix

- **Weekly Downloads** - Piscina 6M, Workerpool 10M, Tinypool 13M.
- **Bundle Size** - Piscina ~800KB, Workerpool ~600KB, Tinypool 38KB.
- **TypeScript** - Piscina built-in, Workerpool @types, Tinypool built-in.
- **Atomics Fast Path** - Piscina ✅, Workerpool ❌, Tinypool ✅.
- **Custom Task Queue** - Piscina ✅, Workerpool ❌, Tinypool ❌.
- **Backpressure** - Piscina ✅, Workerpool ❌, Tinypool ✅.
- **AbortController** - Piscina ✅, Workerpool ✅, Tinypool ✅.
- **Browser Support** - Piscina ❌, Workerpool ✅, Tinypool ❌.
- **Metrics** - Piscina ✅ Extensive, Workerpool 🟡 Basic, Tinypool ❌ None.
- **Multiple Functions** - Piscina ✅, Workerpool ✅, Tinypool ✅.
- **Memory Limits** - Piscina ✅, Workerpool ❌, Tinypool ✅.
- **child_process** - Piscina ❌, Workerpool ✅, Tinypool ✅.

**Recommendation: Piscina** for production use cases requiring reliability, metrics, and backpressure. **Tinypool** if minimal footprint is critical.

---

## 5 Distinct Implementation Patterns

### Pattern 1: Static Worker File (Recommended)

```
┌─────────────────────────────────────────────────────────────────────────┐
│  PATTERN 1: Static Worker File                                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  worker-transforms.ts (separate file)                                   │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  export function processChunk(data: { chunk: In[], config: any }) │  │
│  │    → runs heavy computation                                       │  │
│  │    → returns Out[]                                                │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  Main thread:                                                           │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  const pool = new Piscina({ filename: 'worker-transforms.js' })   │  │
│  │  await pool.run({ chunk, config }, { name: 'processChunk' })      │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  ✅ Best performance                                                    │
│  ✅ Type-safe (shared types)                                            │
│  ❌ Requires separate worker file                                       │
│  ❌ No closures or captured state                                       │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

**Implementation:**

```typescript
// worker-transforms.ts
import { workerData } from "worker_threads";

interface ProcessChunkInput<In> {
  chunk: In[];
  // Serialized transform config (no functions!)
  mapFn?: string; // Serialized function for simple cases
}

export default function processChunk<In, Out>(input: ProcessChunkInput<In>): Out[] {
  const { chunk } = input;
  // Perform CPU-intensive work here
  return chunk.map(/* ... */) as Out[];
}

// strategy.ts
export class WorkerPoolStrategy<In, Out> implements ExecutionStrategy<In, Out> {
  private pool: Piscina;

  constructor(options: WorkerPoolStrategyOptions) {
    this.pool = new Piscina({
      filename: options.workerFile,
      maxThreads: options.maxWorkers ?? os.cpus().length,
    });
  }

  async *execute(transformerLogic, chunks, context): AsyncGenerator<Out[]> {
    // NOTE: transformerLogic CANNOT be sent to worker (it's a function)
    // Worker must have its own logic
    for await (const chunk of chunks) {
      yield await this.pool.run({ chunk });
    }
  }
}
```

**Pitfall: transformerLogic cannot be passed to workers.** The worker must contain its own processing logic. This is a fundamental limitation.

---

### Pattern 2: Inline Worker via Data URL

```
┌─────────────────────────────────────────────────────────────────────────┐
│  PATTERN 2: Inline Worker via Data URL                                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Main thread generates worker code at runtime:                          │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  const workerCode = `                                             │  │
│  │    const { parentPort } = require('worker_threads')               │  │
│  │    parentPort.on('message', (chunk) => {                          │  │
│  │      const result = chunk.map(x => x * 2)                         │  │
│  │      parentPort.postMessage(result)                               │  │
│  │    })                                                             │  │
│  │  `                                                                │  │
│  │  const pool = new Piscina({                                       │  │
│  │    filename: `data:text/javascript,${encodeURIComponent(code)}`   │  │
│  │  })                                                               │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  ✅ No separate file needed                                             │
│  ✅ Dynamic worker code                                                 │
│  ❌ Limited to simple logic (no imports from workspace)                 │
│  ❌ Security concerns (code generation)                                 │
│  ❌ No TypeScript (must be transpiled)                                  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

**Pitfall:** Cannot import workspace modules from data URL workers. Limited to self-contained logic.

---

### Pattern 3: SharedArrayBuffer for Zero-Copy

```
┌─────────────────────────────────────────────────────────────────────────┐
│  PATTERN 3: SharedArrayBuffer for Zero-Copy Data Sharing                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Main Thread                          Worker Threads                    │
│  ┌────────────────────┐              ┌──────────────────┐               │
│  │ SharedArrayBuffer  │◀────────────▶│ SharedArrayBuffer│               │
│  │ [data data data]   │   same       │ [data data data] │               │
│  │                    │   memory!    │                  │               │
│  └────────────────────┘              └──────────────────┘               │
│                                                                         │
│  No serialization! Workers read/write same memory.                      │
│                                                                         │
│  ✅ Zero-copy = maximum performance for large data                      │
│  ✅ Real shared memory between threads                                  │
│  ❌ Only works for TypedArrays (numbers)                                │
│  ❌ Requires manual memory management                                   │
│  ❌ Race conditions possible (need Atomics)                             │
│  ❌ Objects/strings must still be serialized                            │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

**Use case:** Numeric data processing (signal processing, ML inference, image manipulation).

**Not suitable for:** String processing, object manipulation (our typical use case).

---

### Pattern 4: Transferable Objects

```
┌─────────────────────────────────────────────────────────────────────────┐
│  PATTERN 4: Transferable Objects (Move, Not Copy)                       │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Main Thread                                                            │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  const buffer = new ArrayBuffer(1024 * 1024) // 1MB              │   │
│  │  pool.run(buffer, { transferList: [buffer] })                    │   │
│  │  // buffer is now EMPTY in main thread (transferred, not copied) │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  Worker                                                                 │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  // buffer is now owned by this thread                           │   │
│  │  // Process and transfer back                                    │   │
│  │  return Piscina.move(processedBuffer)                            │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  ✅ Faster than copying for large buffers                               │
│  ✅ Clear ownership model                                               │
│  ❌ Only ArrayBuffer, MessagePort, FileHandle                           │
│  ❌ Original becomes unusable after transfer                            │
│  ❌ Objects/strings still copied                                        │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

**Use case:** Large binary data that doesn't need to be kept in main thread.

---

### Pattern 5: Hybrid Strategy (Recommended for Laygo)

```
┌─────────────────────────────────────────────────────────────────────────┐
│  PATTERN 5: Hybrid Strategy with Pluggable Workers                      │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  User provides a worker FILE that exports a transform function:         │
│                                                                         │
│  // my-heavy-transform.worker.ts                                        │
│  export default function transform(chunk: string[]): string[] {         │
│    return chunk.map(s => expensiveOperation(s))                         │
│  }                                                                       │
│                                                                         │
│  // Usage in laygo pipeline:                                            │
│  const strategy = new WorkerPoolStrategy({                              │
│    workerFile: './my-heavy-transform.worker.js',                        │
│    maxWorkers: 4,                                                       │
│  })                                                                      │
│                                                                         │
│  pipeline                                                                │
│    .map(preprocessFn)           // runs on main thread                  │
│    .withExecutor(strategy)      // next transform runs in workers       │
│    .map(heavyTransform)         // ← USER'S WORKER, NOT THIS FN!        │
│    .withExecutor(sequential)    // back to main thread                  │
│    .filter(postFilter)          // runs on main thread                  │
│                                                                         │
│  ✅ Clean API - user provides worker file path                          │
│  ✅ Type-safe with shared type definitions                              │
│  ✅ Flexible - mix main thread and worker strategies                    │
│  ❌ Requires discipline: worker file must match expected signature      │
│  ❌ Runtime validation needed (worker might not export default fn)      │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## The Fundamental Constraint

**Functions cannot be sent to workers.** This is the core challenge.

```typescript
// ❌ THIS IS IMPOSSIBLE
const strategy = new WorkerPoolStrategy();
pipeline
  .map((x) => x * 2) // This lambda...
  .withExecutor(strategy) // ...CANNOT be sent to workers
  .map((x) => x.toString()); // Functions are not serializable!
```

The transformer function (`transformerLogic` in our `ExecutionStrategy.execute()`) is a JavaScript function. Functions cannot be serialized and sent across the worker boundary.

**Solutions:**

1. **Worker file contains the logic** - Strategy ignores the pipeline's transformerLogic, uses worker's logic instead
2. **Function-to-string serialization** - Only works for pure functions with no closures
3. **Predefined transform registry** - Workers have named transforms, main thread sends name

---

## Recommended Implementation: WorkerPoolStrategy

### Design

```typescript
export interface WorkerPoolStrategyOptions {
  /**
   * Path to worker file that exports a default transform function.
   * The function signature must be: (chunk: In[]) => Out[] | Promise<Out[]>
   */
  workerFile: string;

  /**
   * Maximum number of worker threads. Default: os.cpus().length
   */
  maxWorkers?: number;

  /**
   * Whether to maintain input order in output. Default: true
   */
  ordered?: boolean;

  /**
   * Maximum queue size before applying backpressure. Default: 'auto'
   */
  maxQueue?: number | "auto";
}

export class WorkerPoolStrategy<In, Out> implements ExecutionStrategy<In, Out> {
  private pool: Piscina;

  constructor(options: WorkerPoolStrategyOptions) {
    this.pool = new Piscina({
      filename: options.workerFile,
      maxThreads: options.maxWorkers ?? os.availableParallelism(),
      maxQueue: options.maxQueue ?? "auto",
    });
  }

  async *execute(
    transformerLogic: InternalTransformer<In, Out>, // IGNORED for workers!
    chunks: AsyncIterable<In[]>,
    context: IContextManager,
  ): AsyncGenerator<Out[]> {
    // Note: transformerLogic is ignored because workers have their own logic
    // Context cannot be passed either (not serializable)

    if (this.ordered) {
      yield* this.orderedExecution(chunks);
    } else {
      yield* this.unorderedExecution(chunks);
    }
  }

  private async *orderedExecution(chunks: AsyncIterable<In[]>): AsyncGenerator<Out[]> {
    const promises: Promise<Out[]>[] = [];

    for await (const chunk of chunks) {
      promises.push(this.pool.run(chunk));

      while (promises.length >= this.pool.options.maxThreads) {
        yield await promises.shift()!;
      }
    }

    for (const promise of promises) {
      yield await promise;
    }
  }

  async destroy(): Promise<void> {
    await this.pool.destroy();
  }
}
```

### Worker File Template

```typescript
// my-transform.worker.ts
import type { InternalTransformer } from "@outputty/pipeline";

// This is the transform logic that runs in worker threads
const transform: InternalTransformer<string, string> = (chunk) => {
  return chunk.map((item) => {
    // CPU-intensive work here
    return expensiveOperation(item);
  });
};

export default transform;
```

### Usage Example

```typescript
import { Transformer, WorkerPoolStrategy, sequential } from "@outputty/pipeline";

// Create strategy pointing to worker file
const workerStrategy = new WorkerPoolStrategy({
  workerFile: new URL("./heavy-transform.worker.js", import.meta.url).href,
  maxWorkers: 4,
});

const pipeline = Transformer.from([1, 2, 3, 4, 5])
  .map((x) => x * 2) // Main thread
  .withExecutor(workerStrategy) // Switch to workers
  .batched() // Worker processes batches
  .withExecutor(sequential) // Back to main thread
  .filter((x) => x > 5);

const results = await pipeline.collect();

// Clean up worker pool
await workerStrategy.destroy();
```

---

## Pitfalls and Mitigations

### 1. Serialization Overhead

**Problem:** Every chunk must be serialized (structured clone) to send to workers.

**Mitigation:**

- Use larger chunk sizes to amortize overhead
- Consider SharedArrayBuffer for numeric data
- Use transferable objects when possible

### 2. Worker Cold Start

**Problem:** First task to a new worker has initialization overhead.

**Mitigation:**

- Set `minThreads` to pre-warm workers
- Use `idleTimeout: Infinity` to keep workers alive
- Accept warm-up cost for long-running pipelines

### 3. Context Not Shareable

**Problem:** `IContextManager` cannot be sent to workers.

**Mitigation:**

- Extract serializable config at strategy creation time
- Workers receive config in `workerData`, not per-task
- Consider BroadcastChannel for runtime context updates

### 4. Error Handling

**Problem:** Worker errors terminate the worker.

**Mitigation:**

- Wrap worker logic in try-catch
- Use Piscina's error events
- Implement retry logic in strategy

### 5. Memory Pressure

**Problem:** Each worker has its own V8 heap (~10-50MB).

**Mitigation:**

- Limit `maxThreads` based on available memory
- Use `resourceLimits.maxOldGenerationSizeMb`
- Use Tinypool's `maxMemoryLimitBeforeRecycle`

---

## Conclusion

**Recommended Approach:**

1. **Use Piscina** as the worker pool implementation (mature, well-supported, backpressure)
2. **Pattern 5 (Hybrid Strategy)** for integration with laygo
3. **User provides worker file** - cleanest API, avoids serialization of functions
4. **Document the constraint** - workers cannot access pipeline's transform functions

**Implementation Priority:**

1. Create `WorkerPoolStrategy` class
2. Add worker file template/generator
3. Document usage patterns
4. Add integration tests
5. Consider factory function for common cases

---

## Next Steps

1. [ ] Implement `WorkerPoolStrategy` using Piscina
2. [ ] Create worker file template
3. [ ] Add TypeScript type checking for worker↔main contract
4. [ ] Write comprehensive tests (CPU-bound workloads)
5. [ ] Benchmark against concurrent()
6. [ ] Document usage and limitations
