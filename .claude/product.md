<!-- product.md - the product's truth, written as finished documentation. Every claim verified by a run.
     Implementation lives in architecture.md; an example is pulled from examples.md, never duplicated. -->

# @outputty/pipeline - Product

## North Star

`@outputty/pipeline` is a TypeScript-native, async-first streaming transform library: chain
`map`/`filter`/`reduce`/`flatMap` over a data source the way `Array` methods chain over a list, but
process it in chunks, streaming, with an explicit choice between sequential and concurrent execution
per chain. It is `outputty/laygo`'s caller-side transform layer - laygo's own `Source` accepts any
`AsyncIterable`, so this package reaches a `Model`'s `from` structurally, unimported, never as a
laygo dependency. It must never grow a query-planning or storage layer of its own: chunking,
transforming and controlling concurrency over data already in flight is the whole job.

## Functionality

### Pipeline and Transformer

A `Pipeline` wraps a data source and composes a `Transformer` chain over it; a `Transformer` is the
chain itself, usable standalone against any `AsyncIterable`. Splitting the two means a transform chain
tested once (`Transformer.execute()`) reruns unchanged inside a `Pipeline`, over an HTTP paginator, or
inside a laygo `Source`.

> **Pipeline** - the high-level API: `new Pipeline(source, options?)`, `.context()` to seed shared
> state, `.apply()`/`.transform()` to run a `Transformer`, and one of five terminal ops
> (`.toArray()`/`.first()`/`.consume()`/`.forEach()`/`.branch()`) to drain it.
> **Transformer** - the chainable, reusable chunk-transform: `new Transformer<In, Out>(options?)`,
> `.map()`/`.flatMap()`/`.filter()`/`.reduce()`/`.tap()`/`.catch()`. `.execute(source)` runs it directly
> over any `AsyncIterable`, independent of `Pipeline`.

```ts
import { Pipeline } from "@outputty/pipeline";

const data = await new Pipeline([1, 2, 3, 4, 5])
  .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 4))
  .toArray();
```

```json
[6, 8, 10]
```

### Chunking

Rows move through a `Transformer` in chunks, not one at a time: every `.map`/`.filter`/`.reduce` call
processes a chunk-sized batch before the next chunk starts, which is what makes a concurrent execution
strategy (below) a batch of parallel work rather than one promise per row.

> **Chunk** - the streaming unit a `Transformer` operates on: `In[]`/`Out[]`, sized by
> `TransformerOptions.chunkSize` (default `DEFAULT_CHUNK_SIZE = 1000`).

```ts
import { Transformer } from "@outputty/pipeline";

const t = new Transformer<number, number>({ chunkSize: 100 }).map((x: number) => x * 2);
```

### Execution strategies

A chain declares once whether its chunks run one at a time or in parallel with a concurrency limit; the
chain itself - the `map`/`filter`/`reduce` calls - never changes between the two.

> **Execution strategy** - `SequentialStrategy` (default, order-preserving, one chunk at a time) or
> `ConcurrentStrategy` (`{maxConcurrency, ordered}`), selected via `.withExecutor("sequential" |
> "concurrent" | {custom})`.

```ts
import { Transformer } from "@outputty/pipeline";

const enrich = new Transformer<number, unknown>()
  .withExecutor("concurrent", { maxConcurrency: 3 })
  .map(async (id: number) => (await fetch(`/api/users/${id}`)).json());
```

The chunk is the unit of concurrency, so a chain's parallelism is `chunkSize` times
`maxConcurrency`, never `maxConcurrency` alone. The example above holds 3000 requests in flight at
the default `chunkSize` of 1000, not 3. Lower `chunkSize` to lower the ceiling.

> **Items in flight** - the number of callbacks a chain runs at once: `chunkSize` times
> `maxConcurrency`. `maxConcurrency` bounds chunks; the items inside one chunk run together.

| `chunkSize` | `maxConcurrency` | items in flight |
| --- | --- | --- |
| 1000 | 1 | 1000 |
| 100 | 3 | 300 |
| 1 | 16 | 16 |

### Context

A shared key-value store threads through every stage of a chain, so a downstream `map` can read a
value an upstream stage - or the caller - set, without it becoming an explicit chain parameter.

> **Context / `IContextManager`** - `.get()`/`.set()`/`.getOrDefault()`/`.toDict()`. Every callback
> receives it as an optional second parameter, so an un-annotated `(x) => …` still infers `x`'s type
> from the source - `types.ts`'s own docstring records why a two-arity union signature was rejected.

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

### Branching and merging

A single source splits into several named sub-chains by predicate, and several sources concatenate back
into one - the two directions of composing whole pipelines rather than chaining one.

> **Branch** - `Pipeline.branch(definitions)`: each `BranchDefinition` pairs a `predicate` with a
> `Transformer`; `BranchOptions.firstMatch` (default `true`) sends an item to the first matching branch
> only, `false` broadcasts it to every match.
> **Merge** - the static `Pipeline.merge(...pipelines)`: concatenates every source pipeline's data and
> merges their contexts into one new `Pipeline`.

```ts
import { Pipeline } from "@outputty/pipeline";

const data = await new Pipeline([1, 2, 3, 4, 5]).branch({
  evens: { predicate: (x: number) => x % 2 === 0, transformer: createTransformer<number>() },
  odds: { predicate: (x: number) => x % 2 !== 0, transformer: createTransformer<number>() },
});
```

```json
{ "evens": [2, 4], "odds": [1, 3, 5] }
```

### Error handling

A chunk that throws mid-chain is handled at the chunk, never the row: `.catch()` runs a sub-chain and
hands a failing chunk to a handler that can replace it or drop it, so one bad row's blast radius is
bounded and explicit.

> **`.catch(build, onError?)`** - `build` is the sub-chain to guard; `onError` receives the failing
> `chunk` and `Error`, and its return value (an array, or nothing) replaces or drops the chunk.

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
