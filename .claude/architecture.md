<!-- architecture.md - the module layout and how a chunk actually flows, end to end. Terse by design: a
     paragraph states the rule, a diagram or a snippet shows it. What each capability gives a user
     lives in product.md, never here. -->

# @outputty/pipeline - Architecture

A `Pipeline` wraps a source and a `Transformer` chain; a `Transformer` is the chain itself, independent
of `Pipeline`. This document is how a chunk actually moves through that chain, end to end, and what
restricts it.

## The stack

```text
┌─────────────────────────────────────────────────────────┐
│ App code — TypeScript, tsc-checked                       │
├─────────────────────────────────────────────────────────┤
│ @outputty/pipeline — Pipeline · Transformer · strategies  │
├─────────────────────────────────────────────────────────┤
│ ts-pattern (strategy-spec matching) · p-limit (concurrency│
│ gate inside ConcurrentStrategy)                           │
├─────────────────────────────────────────────────────────┤
│ the caller's own AsyncIterable source                    │
└─────────────────────────────────────────────────────────┘
```

Nothing below the caller's source is this package's concern - no DB driver, no file I/O, no network
client. A `Pipeline` accepts an array, an `AsyncIterable`, or any object shaped as one; a laygo `Model`
reaches a `Pipeline` the same way, structurally (`outputty/laygo`'s `Source` accepts any
`AsyncIterable`), with no import edge in either direction (#743, #745).

## Module layout

```text
src/
  types.ts              PipelineFunction, IContextManager, ExecutionStrategy, every options interface
  pipeline.ts            Pipeline: source + context + terminal ops + Pipeline.merge
  transformer.ts          Transformer: the chainable map/filter/reduce/tap/catch/withExecutor chain
  context/
    types.ts              re-exported IContextManager shape
    simple.ts              SimpleContextManager - the one shipped IContextManager
  strategies/
    sequential.ts          SequentialStrategy
    concurrent.ts           ConcurrentStrategy ({maxConcurrency, ordered}, backed by p-limit)
    registry.ts             ExecutorFactory / createStrategy - resolves a .withExecutor() spec
  errors/
    handler.ts              ErrorHandler - runs a ChunkErrorHandler, used by Transformer.catch()
  utils/
    chunk.ts                buildChunkGenerator - breaks an AsyncIterable into fixed-size chunks
    helpers.ts               isContextAware / isContextAwareReduce - fn.length arity checks
  factories.ts             createTransformer / createConcurrentTransformer sugar
  index.ts                 barrel - the only export surface
```

## How a chunk flows

```text
Pipeline.toArray() (or any terminal op)
	buildChunkGenerator(source, chunkSize)      splits the AsyncIterable into In[] chunks
	Transformer.execute(chunks, context)
		for each chunk:
			strategy.execute(internalTransformer, chunks, context)
				SequentialStrategy: await one chunk at a time, in order
				ConcurrentStrategy: p-limit(maxConcurrency) chunks in flight, re-ordered if `ordered`
			internalTransformer(chunk, ctx)          one map/filter/flatMap/reduce/tap link, chained
				isContextAware(fn) ? fn(item, ctx) : fn(item)      arity-checked once per link, not per item
	collect Out[] chunks into the terminal op's own shape
```

`.catch(build, onError)` wraps one internal transformer function in a try/catch at the CHUNK boundary:
a throw inside `build`'s chain hands the whole failing chunk to `onError`, whose return value (an
array, or nothing) replaces or drops it. The unit of failure is the chunk, never the row - there is no
per-item try/catch anywhere in the chain.

## The strategy family

`ExecutionStrategy<In, Out>` is the one pluggable seam: `execute(transformerLogic, chunks, context)`
plus the `appliesInSourcePosition` capability flag (`types.ts`). `SequentialStrategy` sets it `true` -
it runs chunk-by-chunk with no concurrency or reordering machinery of its own, so a `Transformer` using
it stays correct when consumed directly as an async source rather than through `.execute()`.
`ConcurrentStrategy` and any caller-supplied custom strategy default `false`, since routing this way
reads the flag generically - never an `instanceof` or `.name` check.

`ConcurrentStrategy` bounds CHUNKS, not items: `maxConcurrency` chunks are in flight and every item
inside a chunk runs together, so a chain's items in flight is `chunkSize` times `maxConcurrency`.
Measured peak simultaneous callbacks, N=5000 (`10x10` 100, `50x4` 200, `100x3` 300, `1000x1` 1000,
`7x11` 77) and N=20000 (`1000x3` 3000) - the product exactly, including coprime factors.

How the product is SPLIT is a throughput choice, not a parallelism one. Two pairs reaching the same
16 in flight, N=50000, microtask-only work: `chunkSize:16 maxConcurrency:1` ran 36 ms and
`chunkSize:1 maxConcurrency:16` ran 124 ms, because a chunk pays the per-chunk cost once and
`chunkSize: 1` pays it per item. The gap closes when the callback dominates - the same pairs over a
2 ms-per-item workload ran 377-402 ms, within noise. Prefer the widest chunk that fits the in-flight
budget.

`src/strategies/registry.ts`'s `createStrategy(spec, options?)` resolves a `.withExecutor()` call's
spec (a built-in name or `{custom: strategy}`) to a concrete instance; `registerExecutor` lets a caller
add a named built-in of their own alongside `"sequential"`/`"concurrent"`.

## Benchmarks (pending #11)

`benchmarks/` is a separate project, outside the pnpm workspace, that installs its comparators once
in a `deps` image and runs them on six pinned runtimes: `node:20/22/24/26-alpine`,
`oven/bun:1.3.14-alpine`, `denoland/deno:alpine`. It consumes the package the way a consumer does -
`npm pack` to a tarball, installed by `file:` reference - so `exports` and the `files` allowlist are
exercised rather than bypassed.

Two tables. The first times `map` then `filter` then `toArray` at 10k, 100k, 1M and 10M rows across
every chaining surface: `Array.prototype`, `Iterator.prototype`, `node:stream` `Readable`, Web
Streams `pipeThrough`, a hand-written `async function*`, this package, and `ix` /
`streaming-iterables` / `effect` / `rxjs`. The second controls ITEMS IN FLIGHT rather than any
declared concurrency option, because no two libraries name that knob the same way and this package
reaches it through `chunkSize`; the harness asserts each leg's measured peak equals the target
before recording a time.

## Constraints in dependencies

- `p-limit` gates `ConcurrentStrategy`'s in-flight chunk count. A `maxConcurrency` above the number of
  chunks in flight is a no-op ceiling, never a floor - `p-limit` never spawns work ahead of demand.
- Node `>=26` (`package.json` `engines`) - the package targets `node18` at build (`tsup.config.ts`) for
  the widest consumer range, but development and CI run on 26 (`.nvmrc`).
