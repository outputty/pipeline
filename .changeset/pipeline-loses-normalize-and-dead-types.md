---
"@outputty/pipeline": major
---

**Breaking:** `normalize` is removed from `@outputty/pipeline`'s public export surface. It had zero
production callers post-#39 (chunking moved onto `Pipeline.buffer()`, which never calls it) and was
kept exported only by inertia. It stays available internally - `src/utils/chunk.ts` still
re-exports it unchanged, which is what the package's own test suite uses - this change touches only
the PACKAGE's own public barrel (`src/index.ts`), not that internal path.

- A caller importing `normalize` from `@outputty/pipeline` gets a build error instead of the
  function. There is no drop-in replacement: `normalize` took a stream mixing loose items and
  pre-chunked arrays and flushed the loose ones into a chunk whenever a real array arrived or the
  stream ended - `Pipeline`'s own cutting (`.buffer(size)`, `src/utils/cut.ts`) is what does this
  job now, on a stream that is never mixed to begin with.
- Migration: delete the import. `normalize`'s own body (`src/utils/cut.ts`) depends on nothing in
  this package's internals; a caller who genuinely needs to normalize a loose/pre-chunked mix of
  its own writes the same generator directly.
- Every other public export is unaffected: `buildChunkGenerator` and `isContextAware` (the only
  other two `utils/` exports this package ever publicly shipped) are untouched.

This bumps `major`, not `minor` as this file originally shipped: `.changeset/`'s own convention
(`@changesets/cli`) ties a `feat!` change to a major bump, and #133's own Constraints section names
this removal as one of "3 public-surface removals" the ticket settled as `feat!` in planning (its
own Q5). The other two carry no functional break, but ship in this same major bump because the
ticket's own settled decision bundles all three, not because either changes what a caller's code
does:

- `RowErrorHandler`'s return type is respelled from `unknown | typeof DROP | Promise<unknown |
typeof DROP>` to bare `unknown` - compiler-identical before and after, since `unknown` already
  absorbs every arm of that union (confirmed: `git show 2b7c3f7:src/types.ts` shows `item` was
  already untyped as `unknown` and the type already carried no `<In, U>` parameters at the base -
  the ticket's own "before" example cited a generic, `In`-typed shape that predates this repo's
  actual pre-#133 code and never matched it). No caller-visible change; nothing to migrate.
- `src/context/types.ts` is deleted whole. It re-exported `IContextManager` and had exactly one
  internal importer, `src/context/simple.ts`'s own relative import - repointed here to `@src/types`
  directly - and zero importers anywhere outside `src/`, since the file was never reachable from the
  public barrel (`src/index.ts` re-exports `SimpleContextManager` from `./context/simple`, never
  from `./context/types`). `IContextManager` keeps its one real home, `src/types.ts`, unchanged.
