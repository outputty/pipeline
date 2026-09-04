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
│ p-limit — the concurrency gate inside concurrent()        │
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
    sequential.ts          sequential - the default, one bare async generator function
    concurrent.ts           concurrent(options) - a closure over {maxConcurrency, ordered}, on p-limit
  errors/
    handler.ts              ErrorHandler - runs a ChunkErrorHandler, used by Transformer.catch()
  utils/
    chunk.ts                buildChunkGenerator - breaks an AsyncIterable into fixed-size chunks
    helpers.ts               isContextAware / isContextAwareReduce - fn.length arity checks
  factories.ts             createTransformer - chunk-size sugar over the constructor
  index.ts                 barrel - the only export surface
```

## How a chunk flows

```text
Pipeline.toArray() (or any terminal op)
	buildChunkGenerator(source, chunkSize)      splits the AsyncIterable into In[] chunks
	Transformer.execute(chunks, context)
		for each chunk:
			strategy(internalTransformer, chunks, context)
				sequential: await one chunk at a time, in order
				concurrent(opts): p-limit(maxConcurrency) chunks in flight, re-ordered if `ordered`
			internalTransformer(chunk, ctx)          one map/filter/flatMap/reduce/tap link, chained
				isContextAware(fn) ? fn(item, ctx) : fn(item)      arity-checked once per link, not per item
	collect Out[] chunks into the terminal op's own shape
```

`.catch(build, onError)` wraps one internal transformer function in a try/catch at the CHUNK boundary:
a throw inside `build`'s chain hands the whole failing chunk to `onError`, whose return value (an
array, or nothing) replaces or drops it. The unit of failure is the chunk, never the row - there is no
per-item try/catch anywhere in the chain.

## The strategy family

`ExecutionStrategy<In, Out>` is the one pluggable seam, and it is a plain FUNCTION TYPE, not an
interface a class implements: `(transformerLogic, chunks, context) => AsyncGenerator<Out[]>`. It joins
the five other pluggable seams in `types.ts`, which are all function types too: `PipelineFunction`,
`PipelineReduceFunction`, `ChunkErrorHandler`, `InternalTransformer` and `ChunkerFunction`.
`ChunkerFunction` is its direct shape sibling - an iterable in, an `AsyncGenerator<T[]>` out.
`.withExecutor(strategy)` takes the function directly - a built-in or a caller's own, same shape, no
registry or name lookup between them, and an arrow function can never be a generator so a caller's own
inline strategy is always `async function*`. `sequential` (`strategies/sequential.ts`) is the default:
one chunk at a time, in order, no concurrency or reordering machinery of its own. `concurrent(options?)`
(`strategies/concurrent.ts`) is a factory - it validates `maxConcurrency` eagerly and returns a fresh
strategy function backed by `p-limit`.

`Pipeline` has two drain paths, and only one runs the strategy. A terminal op goes through
`Transformer.execute()`, which calls it. The async-iteration path (`[Symbol.asyncIterator]`,
`pipeline.ts`) - reached when a caller uses a `Pipeline` directly as an `AsyncIterable` rather than
through a terminal op - replays each transform's plain function from `_chunkTransforms` and never
calls `Transformer.execute()`, so the strategy never runs there. A plain function carries no capability
flag of its own the way the prior class-based strategy interface's own source-position flag did, so
`inertKnobsOf` (`pipeline.ts`) decides whether `.withExecutor()` is source-position-safe by comparing
`transformer.strategy` against the built-in `sequential` BY REFERENCE - `.withExecutor(sequential)`
reads as safe (same as never calling `.withExecutor()`), any other function (`concurrent(...)`, a
caller's own) is flagged, whether or not it happens to behave like `sequential`. This mirrors the old
design's own default: a strategy there was flagged unsafe unless it explicitly declared otherwise too.

## Constraints in dependencies

- `p-limit` gates `concurrent()`'s in-flight chunk count. A `maxConcurrency` above the number of
  chunks in flight is a no-op ceiling, never a floor - `p-limit` never spawns work ahead of demand.
- TypeScript removed `baseUrl` at 7.0; a tsconfig that sets it fails with `TS5102`.
- A conditional type distributes only over a naked type parameter. `Ps[number] extends Pipeline<infer
  U> ? U : never` is an indexed access, so it compiles and evaluates to `never`; `Pipeline.merge`
  extracts it as `ElementOf<P>` to make it distribute (#5).
- `Pipeline<T>` is invariant, because `apply<U>(transformer: Transformer<T, U>)` puts `T` in a
  parameter position. Only `Pipeline<any>` works as a constraint over pipelines of mixed item types.
- Node `>=26` (`package.json` `engines`) - the package targets `node18` at build (`tsup.config.ts`) for
  the widest consumer range, but development and CI run on 26 (`.nvmrc`).
- `ts-pattern` is a declared runtime dependency that `src/` never imports. Verified by a repo-wide
  search whose control target (`p-limit`) returned 4 hits. Dropped by #5.
