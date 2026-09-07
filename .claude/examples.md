<!-- examples.md - canonical worked examples, one per concept. No executable docs harness exists yet
     (roadmap.md's first Building candidate) - every fence here is marked `<!-- illustrative -->` and
     hand-verified against the real `__tests__/*.e2e.test.ts` suite, not machine-checked on each edit.
     Reused verbatim; pin a new example here first. -->

# @outputty/pipeline - Examples

The canonical worked examples, one per concept. Each is real code a reader can paste, followed by its
real output. Once the executable docs harness (`roadmap.md`) exists, every fence here gets a `<!--
compiles -->`/`<!-- run -->` marker and a machine check; until then, verify by hand against the e2e
suite before editing an output block.

## The base pipeline

One source, one transform, one terminal op. Every later example is a change to this program.

<!-- illustrative -->

```ts
import { Pipeline } from "@outputty/pipeline";

const data = await new Pipeline([1, 2, 3, 4, 5])
  .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 4))
  .toArray();
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

const data = await new Pipeline([1, 2, 3, 4, 5])
  .context({ multiplier: 10 })
  .transform((t) => t.map((x: number, ctx) => x * (ctx.get("multiplier") as number)))
  .toArray();
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

const data = await new ClusterPipeline([1, 2, 3, 4, 5], {
  workers: 3,
  contextFactory: () => new PgContext(pool),
})
  .context({ multiplier: 10 })
  .transform((t) => t.map((x: number, ctx) => x * (ctx.get("multiplier") as number)))
  .toArray();
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

const data = await new ConcurrentPipeline(["a", "b", "c"], { maxConcurrency: 10 })
  .transform((t) => t.map((s: string) => s.toUpperCase()))
  .toArray();
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

const data = await new ClusterPipeline([1, 2, 3, 4, 5])
  .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 4))
  .toArray();
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

<!-- illustrative -->

```ts
import { Pipeline, createTransformer } from "@outputty/pipeline";

const data = await new Pipeline([1, 2, 3, 4, 5]).branch({
  evens: { predicate: (x: number) => x % 2 === 0, transformer: createTransformer<number>() },
  odds: { predicate: (x: number) => x % 2 !== 0, transformer: createTransformer<number>() },
});
```

```json
{ "evens": [2, 4], "odds": [1, 3, 5] }
```

## Case 5 - merging

Several pipelines' data and contexts concatenate into one. `pipelines` is an array, not a rest
param (#31, BREAKING) - room for an optional second `options` argument, `{ context: mine }`, to
carry a caller's own manager through the merge as the SAME instance; with none, the merged pipeline
gets a fresh `SimpleContextManager`.

<!-- compiles -->

```ts
import { Pipeline } from "@outputty/pipeline";

const pipeline1 = new Pipeline([1, 2, 3]);
const pipeline2 = new Pipeline([4, 5, 6]);

const merged = Pipeline.merge([pipeline1, pipeline2]);
const data = await merged.toArray(); // [1,2,3,4,5,6]
```

```json
[1, 2, 3, 4, 5, 6]
```

## Case 6 - chunk-level error handling

A throw inside `.catch()`'s sub-chain hands the whole failing chunk to the handler; a small enough
input is one chunk, so one bad item drops or replaces the entire result. The handler's returned
array REPLACES the chunk; returning nothing DROPS it (#15).

<!-- compiles -->

```ts
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
      () => [999],
    ),
  )
  .toArray();
```

```json
[999]
```

## Case 7 - a reducer that folds the whole stream, and emits mid-fold

`Pipeline.reduce` folds every chunk the pipeline produces, unlike `Transformer.reduce`, which folds
the one chunk it receives. `emit()` banks a value downstream mid-fold, so the caller decides what a
finished result is; the final accumulator is emitted only if items were folded since the last
`emit()`, which is why `[60,90]` has no trailing `0`.

<!-- compiles -->

```ts
import { Pipeline } from "@outputty/pipeline";

const total = await new Pipeline([1, 2, 3, 4, 5])
  .reduce((acc: number, x: number) => acc + x, 0)
  .transform((t) => t.map((n: number) => n * 10))
  .toArray(); // [150]

const banked = await new Pipeline([1, 2, 3, 4, 5])
  .reduce((acc: number, x: number, _ctx, emit) => {
    acc += x;
    if (acc >= 6) {
      emit(acc);
      return 0;
    }
    return acc;
  }, 0)
  .transform((t) => t.map((n: number) => n * 10))
  .toArray(); // [60,90]
```

```json
{ "total": [150], "banked": [60, 90] }
```

## Case 8 - merging onto a pipeline you already hold

The static `Pipeline.merge()` (Case 5) always builds a fresh, plain `Pipeline`. The instance
`.merge()` (#41) continues one already held instead - class, knobs and `_chunkTransforms` all carry
forward, so a stage applied after the merge runs at THIS pipeline's own next index rather than
restarting at 0 and colliding with its own first stage.

<!-- illustrative -->

```ts
import { HttpPipeline, ConcurrentPipeline } from "@outputty/pipeline";

const remote = new HttpPipeline([1, 2, 3, 4], { url: process.env.WORKER_URL! })
  .buffer(1)
  .transform((t) => t.map((x: number) => x + 1)); // stage 0, dispatched over HTTP

const local = new ConcurrentPipeline([10, 20])
  .buffer(1)
  .transform((t) => t.map((x: number) => x + 5)); // its own in-process fan-out, never the wire

const merged = remote
  .merge(local)
  .transform((t) => t.map((x: number) => x * 100)); // stage 1 - THIS pipeline's own next index

const data = await merged.toArray();
```

```json
[200, 300, 400, 500, 1500, 2500]
```

## Case 9 - observing without changing the data

`.tap()` watches items go past and passes them through untouched. Called on the `Pipeline` (#72) it
always runs in the orchestrating process, so its context writes reach the caller whichever class the
chain was built on; called inside a `.transform()` it travels with the stage and runs wherever that
stage runs. Everything else here is the base program with `.buffer(2)` and a fan-out added.

<!-- illustrative -->

```ts
import { ConcurrentPipeline } from "@outputty/pipeline";

const pipeline = new ConcurrentPipeline([1, 2, 3, 4, 5], { maxConcurrency: 2 })
  .buffer(2)
  .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 4))
  .tap((x: number, ctx) => {
    ctx.set("seen", (ctx.getOrDefault("seen", 0) as number) + 1);
  });

const data = await pipeline.toArray();
const context = pipeline.contextManager.toDict();
```

```json
[6, 8, 10]
```

```json
{ "seen": 3 }
```

A tap's context write is per chunk, never per item: the whole chunk is tapped before the next stage
sees any of it. Over `[1, 2, 3]`, a tap writing `last` followed by a map reading it gives `["1:3",
"2:3", "3:3"]`, not `["1:1", "2:2", "3:3"]`.

## Case 10 - a partitioned reduce, combined

On `ConcurrentPipeline` (and `HttpPipeline`/`ClusterPipeline`), `.reduce()` (#62) partitions across
`maxConcurrency` independent accumulators instead of one, when the fold declares no 4th (`emit`)
parameter - the result then owes a combine, and every terminal op throws until `.combine()` folds the
partials into one. A fold that DOES declare `emit` already means to produce several values on
purpose; nothing is ever owed for that case.

<!-- compiles -->

```ts
import { ConcurrentPipeline } from "@outputty/pipeline";

const data = await new ConcurrentPipeline([1, 2, 3, 4, 5], { maxConcurrency: 2 })
  .buffer(2)
  .reduce((acc: number, x: number) => acc + x, 0)
  .combine((acc: number, v: number) => acc + v)
  .toArray();
```

```json
[15]
```
