/**
 * Chunk utilities - the barrel. The real implementations live in `cut.ts`
 * (cutting/flattening/normalizing/sharing), `drain.ts` (draining a `MaybeAsyncChunks` stream) and
 * `recut.ts` (re-cutting an already-staged one), each its own real seam; this file re-exports every
 * symbol so every existing `@src/utils/chunk`/`./utils/chunk` import keeps resolving unchanged.
 * `assertPositiveChunkSize` and `close` are NOT re-exported here: both are internal-only helpers -
 * `recut.ts` is their one cross-file caller, importing each directly from its own home - and
 * re-exporting them from this barrel would widen the package's public surface for symbols with no
 * real consumer at this path.
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
