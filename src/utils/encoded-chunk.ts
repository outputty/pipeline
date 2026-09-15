/**
 * The encoded chunk (#209) - a dispatched WebSocket reply the orchestrating process keeps as bytes
 * plus a row count instead of decoding, until a site that actually reads items materializes it. It
 * travels through `_chunks` typed `T[]` (a lie every function in this file undoes): the generic
 * chunk-stream machinery (`Transformer.process`, `flattenChunks`, `Pipeline.drainable`) carries it
 * unchanged, and only the functions exported here ever look inside one. Behind
 * `PIPELINE_ENCODED_CHUNKS=1` (#209) - `WebSocketPipeline`/`ClusterPipeline`'s own
 * `stageWork()`/`reduceWork()` are the only sites that ever CREATE one; every function below is
 * otherwise a safe no-op on a real array, so `ConcurrentPipeline`'s fan-out and every decode site
 * cost one type check when the flag is off and nothing else ever produces one.
 */
import type { Codec } from "@src/codec";

const ENCODED_CHUNK: unique symbol = Symbol("encodedChunk");

export interface EncodedChunk {
  readonly [ENCODED_CHUNK]: true;
  readonly payload: Uint8Array;
  readonly rows: number;
  readonly codec: Codec;
}

/** Builds an encoded chunk - the one place `{ payload, rows, codec }` is assembled, called only
 * from `websocket.ts`'s own dispatch (`stageWork()`/`reduceWork()`) when
 * `encodedChunksEnabled()`. */
export function encodedChunk(payload: Uint8Array, rows: number, codec: Codec): EncodedChunk {
  return { [ENCODED_CHUNK]: true, payload, rows, codec };
}

/** `chunk` is `T[]` by its own declared type everywhere this is called from - the encoded case is
 * the type lie this file exists to undo, so the parameter stays `unknown` here at the one place
 * that actually tells the two apart. */
// oxlint-disable-next-line anti-slop/no-unknown-parameters -- see this file's own header
export function isEncodedChunk(chunk: unknown): chunk is EncodedChunk {
  return typeof chunk === "object" && chunk !== null && ENCODED_CHUNK in chunk;
}

/** The row count of a chunk that may or may not be encoded, without decoding to find out - an
 * encoded chunk's own `rows` field, or a real array's `.length`. `ConcurrentPipeline`'s fan-out
 * (`pipelines/concurrent.ts`) uses this to skip dispatching a chunk with nothing in it.
 *
 * `rowsOf([1, 2, 3])` → `3`. `rowsOf(encodedChunk(bytes, 6, codec))` → `6`, no decode. */
export function rowsOf<T>(chunk: T[]): number {
  return isEncodedChunk(chunk) ? chunk.rows : chunk.length;
}

/** Decodes a chunk that may or may not be encoded - a real array passes through UNCHANGED, so this
 * is safe to call at every site whether or not anything ever produced an encoded chunk. The one
 * place every item-reading site (`Pipeline.drainable()`, the `.local()` seed, `flattenChunks`)
 * undoes the encoded-chunk lie.
 *
 * `await materialize([1, 2, 3])` → `[1, 2, 3]`, no decode call. `await
 * materialize(encodedChunk(bytes, 3, codec))` → `codec.decode(bytes)`'s own result. */
export async function materialize<T>(chunk: T[]): Promise<T[]> {
  if (!isEncodedChunk(chunk)) return chunk;
  return (await chunk.codec.decode(chunk.payload)) as T[];
}

/** `materialize()` over a whole chunk STREAM, one chunk at a time - `Pipeline.local()`'s own seed
 * (`pipeline.ts`) uses this, since the bare `Pipeline` it builds runs `Transformer.process()`
 * directly over the chunks it is handed and needs real items, never an encoded one. */
export async function* materializeChunks<T>(chunks: AsyncIterable<T[]>): AsyncGenerator<T[]> {
  for await (const chunk of chunks) {
    yield await materialize(chunk);
  }
}

/** Whether a dispatched WebSocket reply stays encoded until a site actually reads its items
 * (#209) - an environment flag, since `PipelineOptions` carries only `context`/`contextFactory`
 * and this is an internal wire behavior, never a caller-facing knob. Read fresh on every call
 * rather than cached at module load, so a test can flip it per case. */
export function encodedChunksEnabled(): boolean {
  return process.env.PIPELINE_ENCODED_CHUNKS === "1";
}
