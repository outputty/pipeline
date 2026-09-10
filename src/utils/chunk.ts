/**
 * Chunk utilities - the barrel (#133). The real implementations split into `cut.ts`
 * (cutting/flattening/normalizing/sharing), `drain.ts` (draining a `MaybeAsyncChunks` stream) and
 * `recut.ts` (re-cutting an already-staged one), each its own real seam; this file re-exports all
 * three so every existing `@src/utils/chunk`/`./utils/chunk` import keeps resolving unchanged.
 */

export {
  assertPositiveChunkSize,
  buildChunkGenerator,
  normalize,
  flattenChunks,
  buildSyncChunkGenerator,
  share,
  collectItems,
} from "@src/utils/cut";
export { type MaybeAsyncChunks, drainSync, drainSyncSettled, close } from "@src/utils/drain";
export { recutSyncChunks } from "@src/utils/recut";
