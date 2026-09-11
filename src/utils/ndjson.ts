/** Decodes a byte stream into complete lines as they arrive, buffering an incomplete trailing line
 * across reads. The one place both the client (`HttpPipeline.reduceWork`) and the server
 * (`HttpPipeline.fetch`'s `/reduce/<n>` handling) parse NDJSON frames from. A `ReadableStream` is
 * itself async-iterable (WHATWG streams), so no manual `.getReader()`/`.releaseLock()` is needed. */
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

/** Splits `buffer` on newlines - every complete line found, and the incomplete remainder still
 * owed a terminator. Its own function so `readNdjsonLines`'s own loop body stays one statement,
 * within this repo's own `max-depth: 2` rule. */
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

/** Encodes one NDJSON frame - `JSON.stringify(value)` plus the trailing newline every frame needs. */
export function ndjsonFrame<T>(value: T): Uint8Array {
  return new TextEncoder().encode(`${JSON.stringify(value)}\n`);
}
