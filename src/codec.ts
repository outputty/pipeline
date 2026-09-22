/**
 * How a chunk becomes bytes on a `WebSocketPipeline`/`ClusterPipeline` connection. Kept out of the
 * file that loads `ws`, so the root entry can export it.
 *
 * Not generic: one codec serves every stage while the item type changes, so `Pipeline<T>` carries
 * the types.
 *
 * `codec.decode(await codec.encode(chunk))` → a value equal to `chunk`.
 */
export interface Codec {
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- a codec encodes ANY chunk value, one handler for every item type a chain has ever carried; narrowing would break that contract, the same reason RowErrorHandler's `item` stays unknown (types.ts)
  encode(value: unknown): Uint8Array | Promise<Uint8Array>;
  // oxlint-disable-next-line anti-slop/no-unknown-returns -- the decoded value is unknown until a caller's own stage parses it at its own boundary, the same contract Context's IContextManager.get() already discloses
  decode(bytes: Uint8Array): unknown | Promise<unknown>;
  contentType?: string;
}

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

/**
 * The default codec: a chunk as JSON text in UTF-8 bytes.
 *
 * `new JsonCodec().decode(new JsonCodec().encode([1, 2]))` → `[1, 2]`.
 */
export class JsonCodec implements Codec {
  readonly contentType = "application/json";

  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- see Codec.encode above
  encode(value: unknown): Uint8Array {
    return textEncoder.encode(JSON.stringify(value));
  }

  // oxlint-disable-next-line anti-slop/no-unknown-returns -- see Codec.decode above
  decode(bytes: Uint8Array): unknown {
    return JSON.parse(textDecoder.decode(bytes)) as unknown;
  }
}
