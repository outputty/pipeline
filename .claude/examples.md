<!-- examples.md - canonical worked examples, one per concept. No executable docs harness exists yet
     (roadmap.md's first Building candidate) - a fence marked `<!-- illustrative -->` names something
     undefined and is hand-verified against the real `__tests__/*.e2e.test.ts` suite; a fence marked
     `<!-- compiles -->` is a real, self-contained program, pasted into a throwaway `tmp/` script and
     run for real on each edit that touches it. Reused verbatim; pin a new example here first. -->

# @outputty/pipeline - Examples

The canonical worked examples, one per concept. Each is real code a reader can paste, followed by its
real output. A `<!-- compiles -->` fence is a real, self-contained program, hand-run in a throwaway
`tmp/` script on every edit that touches it; a `<!-- illustrative -->` fence names something the
reader supplies (a database pool, a url with no server behind it) and is verified by hand against
the real `__tests__/*.e2e.test.ts` suite instead. Once the executable docs harness (`roadmap.md`)
exists, every fence gets a machine check instead of a hand-run one.

## The base pipeline

One source, one transform, one terminal op. Every later example is a change to this program.

<!-- illustrative -->

```ts
import { Pipeline } from "@outputty/pipeline";

const data = await new Pipeline<number>()
  .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 4))
  ([1, 2, 3, 4, 5]).toArray();
```

```json
[6, 8, 10]
```

## Case 1 - context

A value seeded on the `Pipeline` reaches every stage of the chain as an optional second callback
parameter.

<!-- illustrative -->

```ts
import { Pipeline } from "@outputty/pipeline";

const data = await new Pipeline<number>()
  .context({ multiplier: 10 })
  .transform((t) => t.map((x: number, ctx) => x * (ctx.get("multiplier") as number)))
  ([1, 2, 3, 4, 5]).toArray();
```

```json
[10, 20, 30, 40, 50]
```

The caller supplies their own manager as an instance for this process, or as a factory for every
other one. `contextFactory` runs once per process, so a manager owning a connection opens one pool
per worker rather than one per chunk.

<!-- illustrative -->

```ts
import { ClusterPipeline } from "@outputty/pipeline";

const data = await new ClusterPipeline<number>({
  workers: 3,
  contextFactory: () => new PgContext(pool),
})
  .context({ multiplier: 10 })
  .transform((t) => t.map((x: number, ctx) => x * (ctx.get("multiplier") as number)))
  ([1, 2, 3, 4, 5]).toArray();
```

```json
[10, 20, 30, 40, 50]
```

## Case 2 - concurrent execution

The same chain shape, run with a bounded concurrency instead of sequentially - the class changes,
`.transform()`'s own chain does not. `ConcurrentPipeline` replaced the `ExecutionStrategy` seam
(`.withExecutor()`, `sequential`, `concurrent(options?)`) entirely (#17).

<!-- illustrative -->

```ts
import { ConcurrentPipeline } from "@outputty/pipeline";

const data = await new ConcurrentPipeline<string>({ maxConcurrency: 10 })
  .transform((t) => t.map((s: string) => s.toUpperCase()))
  (["a", "b", "c"]).toArray();
```

```json
["A", "B", "C"]
```

## Case 3 - execution somewhere else

The base pipeline, with its work running in other processes. The chain is unchanged and the output is
unchanged; only the class differs. `ClusterPipeline` brings up its own workers on first run and every
later pipeline in the process reuses them - there is no server, port, url or fork in caller code.

<!-- illustrative -->

```ts
import { ClusterPipeline } from "@outputty/pipeline";

const data = await new ClusterPipeline<number>()
  .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 4))
  ([1, 2, 3, 4, 5]).toArray();
```

```json
[6, 8, 10]
```

Across machines instead of processes, the same chain takes a url and mounts its own routes:

<!-- illustrative -->

```ts
import { HttpPipeline } from "@outputty/pipeline";

const pipeline = new HttpPipeline([1, 2, 3, 4, 5], { url: process.env.SELF_URL! })
  .buffer(2)
  .transform((t) => t.map((x: number) => x * 2))
  .local((p) => p.transform((t) => t.filter((x: number) => x > 4)));

app.mount("/pipeline", pipeline.fetch);
```

```json
[6, 8, 10]
```

## Case 4 - branching

One source, several named sub-chains, routed by predicate.

<!-- compiles -->

```ts
// The arms are written ONCE, and `.branch()` is a stage - so the runner it returns takes any input.
console.log(
  JSON.stringify(
    new Pipeline<number>().branch((b) =>
      b
        .when(
          "evens",
          (x) => x % 2 === 0,
          (q) => q.transform((t) => t.map((x) => x * 10)),
        )
        .otherwise("odds"),
    )([1, 2, 3, 4, 5]),
  ),
);
```

```json
{ "evens": [20, 40], "odds": [1, 3, 5] }
```

## Case 5 - per-row error recovery, and the run-level decision

Error handling sits on the function that failed. `Transformer.onError(fn)` is the row handler: it
receives the failing item and the error, and returns a replacement value, returns `DROP` to remove
the row, or throws to escalate. It applies to every element-wise call in the chain wherever it is
written, so the rows that parsed survive the one that did not.

<!-- compiles -->

```ts
import { Pipeline, DROP } from "@outputty/pipeline";

const parseStrict = (s: string): number => {
  const n = parseInt(s);
  if (isNaN(n)) throw new Error(`Invalid: ${s}`);
  return n;
};

const dropped = await new Pipeline<string>()
  .transform((t) => t.onError(() => DROP).map(parseStrict))
  (["a", "b", "3", "d", "5"]).toArray();
```

```json
[3, 5]
```

Returning a value repairs the row in place instead of removing it:

<!-- compiles -->

```ts
const repaired = await new Pipeline<string>()
  .transform((t) => t.onError(() => -1).map(parseStrict))
  (["a", "b", "3", "d", "5"]).toArray();
```

```json
[-1, -1, 3, -1, 5]
```

`Pipeline.onError(fn)` is the run handler, for a failure no row handler repaired: returning drops the
failing chunk and the run continues, throwing stops it. At `.buffer(1)` each row is its own chunk,
so the chunk carrying `"x"` is the only one lost.

<!-- compiles -->

```ts
const survived = await new Pipeline<string>()
  .buffer(1)
  .onError((err) => console.warn(err.message))
  .transform((t) => t.map(parseStrict))
  (["1", "x", "3", "4"]).toArray();
```

```json
[1, 3, 4]
```

## Case 6 - a reducer that folds the whole stream, and emits mid-fold

`Pipeline.reduce` folds every chunk the pipeline produces, unlike `Transformer.reduce`, which folds
the one chunk it receives. `emit()` banks a value downstream mid-fold, so the caller decides what a
finished result is; the final accumulator is emitted only if items were folded since the last
`emit()`, which is why `[60,90]` has no trailing `0`.

<!-- compiles -->

```ts
import { Pipeline } from "@outputty/pipeline";

const total = await new Pipeline<number>()
  .reduce((acc: number, x: number) => acc + x, 0)
  .transform((t) => t.map((n: number) => n * 10))
  ([1, 2, 3, 4, 5]).toArray(); // [150]

const banked = await new Pipeline<number>()
  .reduce((acc: number, x: number, _ctx, emit) => {
    acc += x;
    if (acc >= 6) {
      emit(acc);
      return 0;
    }
    return acc;
  }, 0)
  .transform((t) => t.map((n: number) => n * 10))
  ([1, 2, 3, 4, 5]).toArray(); // [60,90]
```

```json
{ "total": [150], "banked": [60, 90] }
```

## Case 7 - observing without changing the data

`.tap()` watches items go past and passes them through untouched. Called on the `Pipeline` (#72) it
always runs in the orchestrating process, so its context writes reach the caller whichever class the
chain was built on; called inside a `.transform()` it travels with the stage and runs wherever that
stage runs. Everything else here is the base program with `.buffer(2)` and a fan-out added.

<!-- compiles -->

```ts
import { ConcurrentPipeline, SimpleContextManager } from "@outputty/pipeline";

const shared = new SimpleContextManager();
const pipeline = new ConcurrentPipeline<number>({ maxConcurrency: 2, context: shared })
  .buffer(2)
  .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 4))
  .tap((x: number, ctx) => {
    ctx.set("seen", (ctx.getOrDefault("seen", 0) as number) + 1);
  });

const data = await pipeline([1, 2, 3, 4, 5]).toArray();
const context = shared.toDict();
console.log(JSON.stringify({ data, context }));
```

```json
{ "data": [6, 8, 10], "context": { "seen": 3 } }
```

```json
{ "seen": 3 }
```

A tap's context write is per chunk, never per item: the whole chunk is tapped before the next stage
sees any of it. Over `[1, 2, 3]`, a tap writing `last` followed by a map reading it gives `["1:3",
"2:3", "3:3"]`, not `["1:1", "2:2", "3:3"]`.

## Case 8 - a partitioned reduce, merged by hand

On `ConcurrentPipeline` (and `HttpPipeline`/`ClusterPipeline`), `.reduce()` (#62) partitions across
`maxConcurrency` independent accumulators instead of one. Each partition's own result flows
downstream as an ordinary value - no forced merge, no thrown error. A caller who wants ONE final
value writes an ordinary second reduce as the next stage.

<!-- compiles -->

```ts
import { ConcurrentPipeline } from "@outputty/pipeline";

const data = await new ConcurrentPipeline<number>({ maxConcurrency: 2 })
  .buffer(2)
  .reduce((acc: number, x: number) => acc + x, 0)
  .local((p) => p.reduce((acc: number, v: number) => acc + v, 0))
  ([1, 2, 3, 4, 5]).toArray();
```

```json
[15]
```

## Case 9 - repairing bad rows without losing the batch

`Transformer.onError(fn)` drops or replaces a row that throws; the rows that parsed keep going.
The same chain, run unchanged on all four `Pipeline` classes - only the class you construct, and
for `HttpPipeline` the worker it dispatches to, ever differ. `README.md`'s own Patterns section
reuses this chain, adapted to each class the same way.

<!-- compiles -->

```ts
import { Pipeline, DROP } from "@outputty/pipeline";

const parseStrict = (s: string): number => {
  const n = parseInt(s);
  if (isNaN(n)) throw new Error(`Invalid: ${s}`);
  return n;
};

const data = await new Pipeline<string>()
  .transform((t) => t.onError(() => DROP).map(parseStrict))
  (["a", "1", "b", "3", "5"]).toArray();

console.log(JSON.stringify(data)); // [1,3,5]
```

<!-- compiles -->

```ts
import { ConcurrentPipeline, DROP } from "@outputty/pipeline";

const parseStrict = (s: string): number => {
  const n = parseInt(s);
  if (isNaN(n)) throw new Error(`Invalid: ${s}`);
  return n;
};

const data = await new ConcurrentPipeline<string>({ maxConcurrency: 2 })
  .transform((t) => t.onError(() => DROP).map(parseStrict))
  (["a", "1", "b", "3", "5"]).toArray();

console.log(JSON.stringify(data)); // [1,3,5]
```

<!-- compiles -->

```ts
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { HttpPipeline, toNodeHandler, DROP } from "@outputty/pipeline";

const parseStrict = (s: string): number => {
  const n = parseInt(s);
  if (isNaN(n)) throw new Error(`Invalid: ${s}`);
  return n;
};

// The "another instance" side: an empty-source pipeline holding the SAME chain, so its
// .fetch can serve it.
const worker = new HttpPipeline<string>([], { url: "" }).transform((t) =>
  t.onError(() => DROP).map(parseStrict),
);

const server = createServer(toNodeHandler(worker.fetch));
await new Promise<void>((resolve) => server.listen(0, resolve));
const { port } = server.address() as AddressInfo;

const data = await new HttpPipeline<string>({
  url: `http://localhost:${port}`,
})
  .transform((t) => t.onError(() => DROP).map(parseStrict))
  (["a", "1", "b", "3", "5"]).toArray();

console.log(JSON.stringify(data)); // [1,3,5]

await new Promise<void>((resolve) => server.close(() => resolve()));
```

<!-- compiles -->

```ts
import { ClusterPipeline, DROP } from "@outputty/pipeline";

const parseStrict = (s: string): number => {
  const n = parseInt(s);
  if (isNaN(n)) throw new Error(`Invalid: ${s}`);
  return n;
};

const data = await new ClusterPipeline<string>()
  .transform((t) => t.onError(() => DROP).map(parseStrict))
  (["a", "1", "b", "3", "5"]).toArray();

// Last line only - every worker also re-executes this module, each printing its own empty result first.
console.log(JSON.stringify(data)); // [1,3,5]
```

## Case 10 - same chain, more in flight

An I/O-bound per-item task wastes its wait run one at a time; `ConcurrentPipeline` runs several
waits at once instead, with no change to the chain - only the class, and `.buffer(1)` so each item
is its own chunk, change. `.buffer(1)` matters on BOTH sides here: at the default buffer of 1000,
five items are one chunk and `.map()`'s own `Promise.all` already runs them together, so a plain
`Pipeline` would look just as "concurrent" as `ConcurrentPipeline` and the comparison would prove
nothing. Neither this nor Case 11 uses `.reduce()`: `ConcurrentPipeline.reduce()` partitions into
`maxConcurrency` independent accumulators by design (#62), so a "same output on every class" case
built on it would contradict itself.

<!-- compiles -->

```ts
import { Pipeline, ConcurrentPipeline } from "@outputty/pipeline";

async function fetchScore(id: number): Promise<number> {
  await new Promise((resolve) => setTimeout(resolve, 5)); // stands in for a real network wait
  return id * 10;
}

// Pipeline: one item's wait finishes before the next one starts.
const sequential = await new Pipeline<number>()
  .buffer(1)
  .transform((t) => t.map(fetchScore))
  ([1, 2, 3, 4, 5]).toArray();

// ConcurrentPipeline: up to 4 items waiting at once - the SAME chain, unchanged.
const concurrent = await new ConcurrentPipeline<number>({ maxConcurrency: 4 })
  .buffer(1)
  .transform((t) => t.map(fetchScore))
  ([1, 2, 3, 4, 5]).toArray();

console.log(JSON.stringify({ sequential, concurrent })); // identical - only the wait overlaps
```

```json
{ "sequential": [10, 20, 30, 40, 50], "concurrent": [10, 20, 30, 40, 50] }
```

The same chain dispatched over HTTP and to real worker processes, proving the pattern travels:

<!-- compiles -->

```ts
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { HttpPipeline, toNodeHandler } from "@outputty/pipeline";

async function fetchScore(id: number): Promise<number> {
  await new Promise((resolve) => setTimeout(resolve, 5));
  return id * 10;
}

const worker = new HttpPipeline<number>([], { url: "" }).transform((t) => t.map(fetchScore));

const server = createServer(toNodeHandler(worker.fetch));
await new Promise<void>((resolve) => server.listen(0, resolve));
const { port } = server.address() as AddressInfo;

const data = await new HttpPipeline<number>({
  url: `http://localhost:${port}`,
  maxConcurrency: 4,
})
  .buffer(1)
  .transform((t) => t.map(fetchScore))
  ([1, 2, 3, 4, 5]).toArray();

console.log(JSON.stringify(data)); // [10,20,30,40,50]

await new Promise<void>((resolve) => server.close(() => resolve()));
```

<!-- compiles -->

```ts
import { ClusterPipeline } from "@outputty/pipeline";

async function fetchScore(id: number): Promise<number> {
  await new Promise((resolve) => setTimeout(resolve, 5));
  return id * 10;
}

const data = await new ClusterPipeline<number>({ maxConcurrency: 4 })
  .buffer(1)
  .transform((t) => t.map(fetchScore))
  ([1, 2, 3, 4, 5]).toArray();

// Last line only - every worker also re-executes this module, each printing its own empty result first.
console.log(JSON.stringify(data)); // [10,20,30,40,50]
```

## Case 13 - a synchronous chain, widening once async is introduced

`.from(source)` decides whether the chain runs synchronously from the source's own shape - a plain
array stays synchronous through every stage, and `.toArray()` returns `number[]` directly, no
`await`. `.transform()` cannot be called before `.from()` at all - a compile error, since there is
no source yet to decide sync or async against.

<!-- illustrative, pending #90 -->

```ts
import { Pipeline } from "@outputty/pipeline";

const data = new Pipeline()
  .from([1, 2, 3, 4, 5])
  .transform((t) => t.map((x) => x * 2).filter((x) => x > 4))
  .toArray(); // number[] - no await
```

```json
[6, 8, 10]
```

The same chain widens to asynchronous the moment any stage's own function returns a `Promise`:

<!-- illustrative, pending #90 -->

```ts
const widened = await new Pipeline()
  .from([1, 2, 3, 4, 5])
  .transform((t) => t.map(async (x) => x * 2).filter((x) => x > 4))
  .toArray(); // Promise<number[]>
```

## Case 14 - prefetching ahead of the consumer

`.queue(capacity)` prefetches chunks `.buffer()` already cut, so a slow producer's latency overlaps
with the consumer's own processing instead of adding to it. Order is unchanged from the base
pipeline - `.queue()` only changes WHEN a chunk is pulled, never what it contains.

<!-- compiles -->

```ts
import { Pipeline } from "@outputty/pipeline";

const data = await new Pipeline<number>()
  .buffer(2)
  .queue(3)
  .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 4))([1, 2, 3, 4, 5])
  .toArray();
```

```json
[6, 8, 10]
```

## Case 15 - a callback-driven chunk boundary

`.buffer(fn)` decides the chunk boundary per item instead of by count, reused verbatim in
`README.md` and `product.md`'s own Chunking section.

<!-- compiles -->

```ts
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
```

```json
[
  [{ "id": 1, "ts": 0 }, { "id": 2, "ts": 60000 }, { "id": 3, "ts": 240000 }],
  [{ "id": 4, "ts": 300000 }, { "id": 5, "ts": 301000 }]
]
```
