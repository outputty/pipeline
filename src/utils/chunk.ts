/**
 * Chunk utilities - the barrel (#133). The real implementations split into `cut.ts`
 * (cutting/flattening/normalizing/sharing), `drain.ts` (draining a `MaybeAsyncChunks` stream) and
 * `recut.ts` (re-cutting an already-staged one), each its own real seam; this file re-exports every
 * symbol `chunk.ts` itself used to export, so every existing `@src/utils/chunk`/`./utils/chunk`
 * import keeps resolving unchanged. `assertPositiveChunkSize` and `close` are NOT re-exported here
 * (code-review, #133): both are new, internal-only helpers this split introduced - `recut.ts` is
 * their one cross-file caller, importing each directly from its own home - and re-exporting them
 * from this barrel would widen the package's public surface for symbols with no real consumer at
 * this path.
 */

export {
  buildChunkGenerator,
  normalize,
  flattenChunks,
  buildSyncChunkGenerator,
  share,
  collectItems,
  prefetch,
} from "@src/utils/cut";
export { type MaybeAsyncChunks, drainSync, drainSyncSettled } from "@src/utils/drain";
export { recutSyncChunks } from "@src/utils/recut";
