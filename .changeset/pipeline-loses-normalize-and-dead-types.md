---
"@outputty/pipeline": minor
---

**Breaking:** `normalize` is removed from the public export surface. It had zero production callers
post-#39 (chunking moved onto `Pipeline.buffer()`, which never calls it) and was kept exported only
by inertia; every internal caller now reaches it, if at all, through `src/utils/cut.ts` directly.

- A caller importing `normalize` from `@outputty/pipeline` gets a build error instead of the
  function. There is no drop-in replacement: `normalize`'s own job (coercing a source into an
  `AsyncIterable`) is now `Pipeline`'s own internal concern, not something a caller does by hand.
- Every other export is unaffected. `buildChunkGenerator`, `flattenChunks`, `share` and the rest of
  `utils/chunk.ts`'s own surface are untouched.

Migration: delete the import. A caller that genuinely needs to coerce an arbitrary source into an
`AsyncIterable` reaches for the platform's own `Readable.toWeb`/`ReadableStream.from`, or builds a
`Pipeline` around the source directly - `normalize` never did more than either already does.
