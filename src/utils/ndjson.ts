/**
 * NDJSON framing for a reduce stage's duplex connection. Outgoing: `{"context":{…}}` once, then
 * `{"chunk":[…]}` per chunk. Incoming: `{"emit":[…]}` per emit, `{"error":"…"}` on a failure.
 */

/** Reads a byte stream as complete NDJSON lines, as they arrive. Both ends of `/reduce/<n>` use it.
 *
 * A stream carrying `'{"a":1}\n{"b"'` then `':2}\n'` → yields `'{"a":1}'`, then `'{"b":2}'`. */
export async function* readNdjsonLines(stream: ReadableStream<Uint8Array>): AsyncGenerator<string> {
  const decoder = new TextDecoder();
  let buffer = "";
  for await (const value of stream) {
    buffer += decoder.decode(value, { stream: true });
    const { lines, rest } = splitLines(buffer);
    buffer = rest;
    yield* lines;
  }
  if (buffer.length > 0) yield buffer;
}

function splitLines(buffer: string) {
  const lines: string[] = [];
  let rest = buffer;
  let newlineIndex: number;
  while ((newlineIndex = rest.indexOf("\n")) !== -1) {
    const line = rest.slice(0, newlineIndex);
    rest = rest.slice(newlineIndex + 1);
    if (line.length > 0) lines.push(line);
  }
  return { lines, rest };
}

/** Encodes one value as an NDJSON frame.
 *
 * `ndjsonFrame({ emit: [6] })` → the UTF-8 bytes of `'{"emit":[6]}\n'`. */
export function ndjsonFrame<T>(value: T): Uint8Array {
  return new TextEncoder().encode(`${JSON.stringify(value)}\n`);
}
