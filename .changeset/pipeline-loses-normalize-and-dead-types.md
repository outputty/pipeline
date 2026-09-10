---
"@outputty/pipeline": minor
---

**Breaking:** `normalize` is removed from `@outputty/pipeline`'s public export surface. It had zero
production callers post-#39 (chunking moved onto `Pipeline.buffer()`, which never calls it) and was
kept exported only by inertia. It stays available internally - `src/utils/chunk.ts` still
re-exports it unchanged, which is what the package's own test suite uses - this change touches only
the PACKAGE's own public barrel (`src/index.ts`), not that internal path.

- A caller importing `normalize` from `@outputty/pipeline` gets a build error instead of the
  function. There is no drop-in replacement: `normalize`'s own job (coercing a source into an
  `AsyncIterable`) is now `Pipeline`'s own internal concern, not something a caller does by hand.
- Every other public export is unaffected: `buildChunkGenerator` and `isContextAware` (the only
  other two `utils/` exports this package ever publicly shipped) are untouched.

Migration: delete the import. A caller that genuinely needs to coerce an arbitrary source into an
`AsyncIterable` reaches for the platform's own `Readable.toWeb`/`ReadableStream.from`, or builds a
`Pipeline` around the source directly - `normalize` never did more than either already does.
