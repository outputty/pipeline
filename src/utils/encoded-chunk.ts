/**
 * The encoded chunk (#209) - a dispatched WebSocket reply the orchestrating process keeps as bytes
 * plus a row count instead of decoding, until a site that actually reads items materializes it. It
 * travels through `_chunks` typed `T[]` (a lie every function in this file undoes): the generic
 * chunk-stream machinery (`Transformer.process`, `flattenChunks`, `Pipeline.drainable`) carries it
 * unchanged, and only the functions exported here ever look inside one.
 * `WebSocketPipeline`/`ClusterPipeline`'s own `stageWork()`/`reduceWork()` are the only sites that
 * ever CREATE one; every function below is otherwise a safe no-op on a real array, so
 * `ConcurrentPipeline`'s fan-out and every decode site cost one type check on a chain that never
 * dispatches through a codec.
 */
import type { Codec } from "@src/codec";

const ENCODED_CHUNK: unique symbol = Symbol("encodedChunk");

/** A dispatched WebSocket reply, kept as bytes plus a row count instead of decoding - the value
 * `encodedChunk()` (below) builds and every other function in this file reads back. `[ENCODED_CHUNK]`
 * is the tag `isEncodedChunk()` tests for; a caller never constructs or inspects one directly. */
export interface EncodedChunk {
  readonly [ENCODED_CHUNK]: true;
  readonly payload: Uint8Array;
  readonly rows: number;
  readonly codec: Codec;
}

/** Builds an encoded chunk - the one place `{ payload, rows, codec }` is assembled, called only
 * from `websocket.ts`'s own dispatch (`stageWork()`/`reduceWork()`).
 *
 * `encodedChunk(bytes, 3, codec)` → `{ [ENCODED_CHUNK]: true, payload: bytes, rows: 3, codec }`. */
export function encodedChunk(payload: Uint8Array, rows: number, codec: Codec): EncodedChunk {
  return { [ENCODED_CHUNK]: true, payload, rows, codec };
}

/** `chunk` is `T[]` by its own declared type everywhere this is called from - the encoded case is
 * the type lie this file exists to undo, so the parameter stays `unknown` here at the one place
 * that actually tells the two apart.
 *
 * `isEncodedChunk([1, 2, 3])` → `false`. `isEncodedChunk(encodedChunk(bytes, 3, codec))` → `true`. */
// oxlint-disable-next-line anti-slop/no-unknown-parameters -- see this file's own header
export function isEncodedChunk(chunk: unknown): chunk is EncodedChunk {
  return typeof chunk === "object" && chunk !== null && ENCODED_CHUNK in chunk;
}

/** Whether `chunk` is an encoded chunk a dispatched stage emptied - `ConcurrentPipeline.apply()`'s
 * fan-out and `WebSocketPipeline.reduceWork()`'s own pump loop both skip dispatching one for the
 * identical reason (#209 review, dedup: both spelled `isEncodedChunk(chunk) && chunk.rows === 0`
 * inline before this).
 *
 * `isEmptyEncodedChunk([])` → `false` (a real, merely-empty array is not this). `isEmptyEncodedChunk(encodedChunk(bytes, 0, codec))` → `true`. */
export function isEmptyEncodedChunk<T>(chunk: T[]): boolean {
  return isEncodedChunk(chunk) && chunk.rows === 0;
}

/** Decodes a chunk that may or may not be encoded - a real array passes through UNCHANGED, so this
 * is safe to call at every site whether or not anything ever produced an encoded chunk. The one
 * place every item-reading site (`Pipeline.drainable()`, the `.local()` seed, `flattenChunks`)
 * undoes the encoded-chunk lie. A KNOWN-empty encoded chunk (`isEmptyEncodedChunk`) skips the
 * decode call too - `rows` already answers "what does this payload decode to" without reading it,
 * the same reason the fan-out never dispatches one further (#209 review: an empty chunk this
 * function once decoded anyway, once the fan-out started forwarding it tagged instead of resetting
 * it to a plain `[]`, cost a real decode call for a known answer).
 *
 * `await materialize([1, 2, 3])` → `[1, 2, 3]`, no decode call. `await
 * materialize(encodedChunk(bytes, 3, codec))` → `codec.decode(bytes)`'s own result. `await
 * materialize(encodedChunk(bytes, 0, codec))` → `[]`, no decode call either. */
export async function materialize<T>(chunk: T[]): Promise<T[]> {
  if (!isEncodedChunk(chunk)) return chunk;
  if (chunk.rows === 0) return [] as T[];
  return (await chunk.codec.decode(chunk.payload)) as T[];
}

/** Encodes `chunk` for the wire with `codec` - the payload VERBATIM when `chunk` is already an
 * encoded chunk on this SAME codec instance (a prior dispatched stage's own reply, never decoded
 * to begin with), otherwise materialized then encoded fresh. The one place `websocket.ts`'s
 * `stageWork()` dispatch and `reduceWork()`'s pump loop both build an outgoing payload (#209
 * review, dedup: both carried an identical ternary before this).
 *
 * `await encodeOrForward([1, 2], codec)` → `codec.encode([1, 2])`'s own result. `await
 * encodeOrForward(encodedChunk(bytes, 2, codec), codec)` → `bytes`, unchanged, no encode call. */
export async function encodeOrForward<T>(chunk: T[], codec: Codec): Promise<Uint8Array> {
  const encoded = isEncodedChunk(chunk);
  if (encoded && chunk.codec === codec) return chunk.payload;
  return codec.encode(encoded ? await materialize(chunk) : chunk);
}

/** `materialize()` over a whole chunk STREAM, one chunk at a time - `Pipeline.local()`'s own seed
 * (`pipeline.ts`) uses this, since the bare `Pipeline` it builds runs `Transformer.process()`
 * directly over the chunks it is handed and needs real items, never an encoded one.
 *
 * `[...await Array.fromAsync(materializeChunks(chunksOf([[1, 2], encodedChunk(bytes, 1, codec)])))]`
 * → `[[1, 2], codec.decode(bytes)'s own result]`. */
export async function* materializeChunks<T>(chunks: AsyncIterable<T[]>): AsyncGenerator<T[]> {
  for await (const chunk of chunks) {
    yield isEncodedChunk(chunk) ? await materialize(chunk) : chunk;
  }
}

/** `materializeChunks()`, skipped entirely when `mayCarryEncoded` is `false` (#209 enable) - only a
 * class holding a `Codec` (`WebSocketPipeline`/`ClusterPipeline`) ever produces an `EncodedChunk`,
 * so wrapping a chunk stream that structurally cannot carry one in its own async generator would
 * cost a Promise per chunk for nothing: measured on `bench/memory.ts`, `Pipeline.local()`/
 * `drainable()` routing every chunk stream through this wrapper unconditionally regressed
 * `promisesPerRow` on plain, non-WebSocket chains (`Concurrent .local()` 0.013 -> 0.025, `Branch
 * router` 0.005 -> 0.011). `Pipeline.mayCarryEncodedChunks()` (`pipeline.ts`) is the one override
 * that answers `true`, on `WebSocketPipeline` alone - every other class inherits the base's
 * `false` and passes `chunks` through by reference, costing one boolean read.
 *
 * `materializeChunksIfNeeded(chunksOf([[1, 2]]), false)` → the SAME `chunks` iterable, no wrapper. */
export function materializeChunksIfNeeded<T>(
  chunks: AsyncIterable<T[]>,
  mayCarryEncoded: boolean,
): AsyncIterable<T[]> {
  return mayCarryEncoded ? materializeChunks(chunks) : chunks;
}
