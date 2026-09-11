---
"@outputty/pipeline": major
---

**Breaking:** two changes to `@outputty/pipeline`'s public export surface, both required by #133's
own dedupe pass. `.changeset/`'s own convention (`@changesets/cli`) ties a `feat!`/breaking change
to a major bump; this file originally shipped as `minor` and covered only the first of the two -
fixed here, before this stack's docs layer closes.

1. `normalize` is removed from the public barrel. It had zero production callers post-#39
   (chunking moved onto `Pipeline.buffer()`, which never calls it) and was kept exported only by
   inertia. It stays available internally - `src/utils/chunk.ts` still re-exports it unchanged,
   which is what the package's own test suite uses - this change touches only the PACKAGE's own
   public barrel (`src/index.ts`), not that internal path.
   - A caller importing `normalize` from `@outputty/pipeline` gets a build error instead of the
     function. There is no drop-in replacement: `normalize` took a stream mixing loose items and
     pre-chunked arrays and flushed the loose ones into a chunk whenever a real array arrived or the
     stream ended - `Pipeline`'s own cutting (`.buffer(size)`, `src/utils/cut.ts`) is what does this
     job now, on a stream that is never mixed to begin with.
   - Migration: delete the import. `normalize`'s own body (`src/utils/cut.ts`) depends on nothing in
     this package's internals; a caller who genuinely needs to normalize a loose/pre-chunked mix of
     its own writes the same generator directly.
2. `RowErrorHandler` loses its two type parameters. It was `RowErrorHandler<In, U>`, with `item`
   typed `In` and the return type spelled out as `unknown | typeof DROP | Promise<unknown | typeof
DROP>` - a union that #133 found compiler-identical to bare `unknown`, since the `DROP`/`Promise`
   arms already collapse into it. It is now the non-generic `RowErrorHandler`, with `item` typed
   `unknown` and the same collapsed return type.
   - A caller who wrote `RowErrorHandler<MyItem, MyResult>` gets `error TS2315: Type
'RowErrorHandler' is not generic`. Migration: drop the type arguments - `RowErrorHandler` alone
     is the whole type now, and a handler function's own parameter still narrows from `unknown` the
     same way any `.onError(fn)` callback already did.
   - Every other public export is unaffected: `buildChunkGenerator` and `isContextAware` (the only
     other two `utils/` exports this package ever publicly shipped) are untouched.

`src/context/types.ts` is also deleted whole in this stack, but carries no version impact: it was
never reachable from the public barrel (`src/index.ts` re-exports `SimpleContextManager` from
`./context/simple`, never from `./context/types`) and had zero importers anywhere in `src/` or
`__tests__/` even internally. `IContextManager` keeps its one real home, `src/types.ts`, unchanged.
