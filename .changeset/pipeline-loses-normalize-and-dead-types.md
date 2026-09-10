---
"@outputty/pipeline": minor
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
- Every other public export is unaffected: `buildChunkGenerator` and `isContextAware` (the only
  other two `utils/` exports this package ever publicly shipped) are untouched.

Migration: delete the import. `normalize`'s own body (`src/utils/cut.ts`) depends on nothing in
this package's internals; a caller who genuinely needs to normalize a loose/pre-chunked mix of its
own writes the same generator directly.
