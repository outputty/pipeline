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

The same chain shape, run with a bounded concurrency instead of sequentially.

<!-- illustrative -->

```ts
import { Transformer } from "@outputty/pipeline";

const t = new Transformer<string, string>()
  .withExecutor("concurrent", { maxConcurrency: 10 })
  .map((s: string) => s.toUpperCase());
```

## Case 2b - items in flight

`maxConcurrency` bounds chunks, and every item inside a chunk runs together, so the callbacks
actually running at once is `chunkSize` times `maxConcurrency`. Both chains below hold 16 in flight,
and they are not equally fast: over 50000 items of microtask-only work `wide` ran 36 ms and `deep`
ran 124 ms, because `chunkSize: 1` pays the per-chunk cost once per item.

<!-- illustrative -->

```ts
import { Pipeline, Transformer } from "@outputty/pipeline";

const wide = new Transformer<number, number>({ chunkSize: 16 })
  .withExecutor("concurrent", { maxConcurrency: 1 })
  .map(async (x: number) => x * 2);

const deep = new Transformer<number, number>({ chunkSize: 1 })
  .withExecutor("concurrent", { maxConcurrency: 16 })
  .map(async (x: number) => x * 2);
```

Measured peak simultaneous callbacks, per `(chunkSize, maxConcurrency)` pair, N=5000:

```json
[
  { "chunkSize": 10, "maxConcurrency": 10, "measuredPeak": 100 },
  { "chunkSize": 50, "maxConcurrency": 4, "measuredPeak": 200 },
  { "chunkSize": 100, "maxConcurrency": 3, "measuredPeak": 300 },
  { "chunkSize": 1000, "maxConcurrency": 1, "measuredPeak": 1000 },
  { "chunkSize": 7, "maxConcurrency": 11, "measuredPeak": 77 }
]
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
