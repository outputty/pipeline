/**
 * `Codec` (#209) — how a chunk becomes bytes on a `WebSocketPipeline`/`ClusterPipeline` connection,
 * orthogonal to transport. Lives outside `pipelines/websocket.ts` (the file that loads `ws`) so a
 * core chunk type can name `Codec` with no utils-to-pipelines import edge.
 *
 * Deliberately not generic: one codec instance serves every stage of a chain while the item type
 * changes per stage, so it sees `unknown` on both sides - `Pipeline<T>` carries the type hints, the
 * same reason `RowErrorHandler` (`types.ts`) stays untyped on its own item.
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
 * The shipped default codec - the same JSON shape `HttpPipeline`'s own wire already sends, over
 * `Uint8Array` bytes instead of a JSON-typed HTTP body. Both directions reuse one module-level
 * `TextEncoder`/`TextDecoder` (both stateless) rather than allocating a fresh instance per call,
 * since every dispatched chunk pays this on the hot path (#180 measured this cost).
 *
 * Replaces the deleted `jsonCodec` object (#209, BREAKING, no deprecation period): `import {
 * jsonCodec }` now fails `tsc` with `TS2724` (`JsonCodec`'s own similar spelling upgrades what
 * would otherwise be a bare TS2305 into tsc's "did you mean" form).
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
