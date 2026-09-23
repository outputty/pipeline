/**
 * The encoded chunk: a WebSocket reply kept as bytes until something reads its items. It travels
 * in a chunk stream typed `T[]`, and only this file's functions look inside one.
 */
import type { Codec } from "@src/codec";

const ENCODED_CHUNK: unique symbol = Symbol("encodedChunk");

/** A dispatched WebSocket reply, kept as its encoded bytes plus its row count. */
export interface EncodedChunk {
  readonly [ENCODED_CHUNK]: true;
  readonly payload: Uint8Array;
  readonly rows: number;
  readonly codec: Codec;
}

/** Builds an encoded chunk from a reply's bytes.
 *
 * `encodedChunk(bytes, 3, codec)` → `{ [ENCODED_CHUNK]: true, payload: bytes, rows: 3, codec }`. */
export function encodedChunk(payload: Uint8Array, rows: number, codec: Codec): EncodedChunk {
  return { [ENCODED_CHUNK]: true, payload, rows, codec };
}

/** Whether `chunk` is an encoded chunk rather than an array.
 *
 * `isEncodedChunk([1, 2, 3])` → `false`. `isEncodedChunk(encodedChunk(bytes, 3, codec))` → `true`. */
// oxlint-disable-next-line anti-slop/no-unknown-parameters -- callers hold it typed `T[]`, which is the case to test
export function isEncodedChunk(chunk: unknown): chunk is EncodedChunk {
  return typeof chunk === "object" && chunk !== null && ENCODED_CHUNK in chunk;
}

/** Whether `chunk` is an encoded chunk with no rows, which a dispatching stage need not send.
 *
 * `isEmptyEncodedChunk([])` → `false`. `isEmptyEncodedChunk(encodedChunk(bytes, 0, codec))` → `true`. */
export function isEmptyEncodedChunk<T>(chunk: T[]): boolean {
  return isEncodedChunk(chunk) && chunk.rows === 0;
}

/** Returns a chunk's items, decoding it if it is encoded. An array passes through unchanged.
 *
 * `await materialize([1, 2, 3])` → `[1, 2, 3]`, no decode call. `await
 * materialize(encodedChunk(bytes, 3, codec))` → `codec.decode(bytes)`'s own result. `await
 * materialize(encodedChunk(bytes, 0, codec))` → `[]`, no decode call either. */
export async function materialize<T>(chunk: T[]): Promise<T[]> {
  if (!isEncodedChunk(chunk)) return chunk;
  if (chunk.rows === 0) return [] as T[];
  return (await chunk.codec.decode(chunk.payload)) as T[];
}

/** Encodes `chunk` for the wire. An encoded chunk from the same `codec` is sent as it is. It returns
 * a plain value when `codec.encode` does, and may throw synchronously.
 *
 * `encodeOrForward([1, 2], codec)` → `codec.encode([1, 2])`'s own result.
 * `encodeOrForward(encodedChunk(bytes, 2, codec), codec)` → `bytes`, unchanged, no encode call. */
export function encodeOrForward<T>(chunk: T[], codec: Codec): Uint8Array | Promise<Uint8Array> {
  if (!isEncodedChunk(chunk)) return codec.encode(chunk);
  if (chunk.codec === codec) return chunk.payload;
  return materialize(chunk).then((items) => codec.encode(items));
}

/** Decodes every encoded chunk in `chunks`; an array passes through unchanged.
 *
 * A stream of `[1, 2]` then `encodedChunk(bytes, 1, codec)` → yields `[1, 2]`, then
 * `codec.decode(bytes)`'s own result. */
export async function* materializeChunks<T>(chunks: AsyncIterable<T[]>): AsyncGenerator<T[]> {
  for await (const chunk of chunks) {
    yield isEncodedChunk(chunk) ? materialize(chunk) : chunk;
  }
}
