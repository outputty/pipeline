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

## Case 2 - concurrent execution

> ⚠ `pending #17` - #17 removes `.withExecutor()` and the `ExecutionStrategy` seam. This case becomes
> `new ConcurrentPipeline(["a", "b", "c"], { maxConcurrency: 10 })`, and Case 2b has no replacement:
> a strategy could change which chunks ran at all, and a concurrency knob cannot. Both blocks below are
> true against the shipped code until #17 lands.

The same chain shape, run with a bounded concurrency instead of sequentially. `.withExecutor()` takes
the strategy directly - a function, never a name - so `sequential`/`concurrent(options?)` and a
caller's own strategy are all the same shape.

<!-- illustrative -->

```ts
import { Pipeline, concurrent } from "@outputty/pipeline";

const data = await new Pipeline(["a", "b", "c"])
  .transform((t) => t.withExecutor(concurrent({ maxConcurrency: 10 })).map((s) => s.toUpperCase()))
  .toArray();
```

```json
["A", "B", "C"]
```

## Case 2b - a strategy of your own

A strategy is a function. Writing one takes no class, no interface to implement and no registration -
this one drops every second CHUNK, not every second item, so `chunkSize` decides what "second" means.

<!-- illustrative -->

```ts
import { Pipeline, Transformer } from "@outputty/pipeline";

const data = await new Pipeline([1, 2, 3, 4])
  .apply(
    new Transformer<number, number>({ chunkSize: 1 })
      .withExecutor(async function* (logic, chunks, ctx) {
        let n = 0;
        for await (const chunk of chunks) {
          n += 1;
          if (n % 2 === 1) yield logic(chunk, ctx);
        }
      })
      .map((x) => x * 2),
  )
  .toArray();
```

```json
[2, 6]
```

## Case 2c - execution somewhere else

> ⚠ `pending #17` - not yet built. The output block is what #17's Done when case 1 asserts.

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

const pipeline = new HttpPipeline([1, 2, 3, 4, 5], { url: process.env.SELF_URL!, chunkSize: 2 })
  .transform((t) => t.map((x: number) => x * 2))
  .transform((t) => t.filter((x: number) => x > 4), { local: true });

app.mount("/pipeline", pipeline.fetch);
```

```json
[6, 8, 10]
```

## Case 3 - branching

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

## Case 4 - merging

Several pipelines' data and contexts concatenate into one.

<!-- illustrative -->

```ts
import { Pipeline } from "@outputty/pipeline";

const pipeline1 = new Pipeline([1, 2, 3]);
const pipeline2 = new Pipeline([4, 5, 6]);

const merged = Pipeline.merge(pipeline1, pipeline2);
const data = await merged.toArray();
```

```json
[1, 2, 3, 4, 5, 6]
```

## Case 5 - chunk-level error handling

A throw inside `.catch()`'s sub-chain hands the whole failing chunk to the handler; a small enough
input is one chunk, so one bad item drops or replaces the entire result.

<!-- illustrative -->

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
      (chunk, err) => undefined,
    ),
  )
  .toArray();
```

```json
[]
```
