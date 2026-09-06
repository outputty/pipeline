import { describe, it, expect } from "vitest";
import { normalize } from "@src/utils/chunk";

/**
 * normalize() - a standalone utility, unaffected by #39's chunking move. It flushes buffered
 * single items into a chunk whenever an array arrives (or the stream ends); its own callers used
 * to include `Pipeline`'s deleted source-position replay, which read an array-shaped source
 * element as its own chunk boundary - that reading died with the mechanism it served (#39:
 * `PipelineSource<T>` no longer accepts one), so `normalize()`'s coverage here is standalone only.
 */
describe("normalize", () => {
  it("flushes buffered items when an array arrives, and at stream end", async () => {
    async function* mixed() {
      yield { id: 1 };
      yield [{ id: 2 }, { id: 3 }];
      yield { id: 4 };
    }

    const chunks: unknown[] = [];
    for await (const chunk of normalize(mixed())) {
      chunks.push(chunk);
    }

    expect(chunks).toEqual([[{ id: 1 }], [{ id: 2 }, { id: 3 }], [{ id: 4 }]]);
  });

  it("accumulates all-single-item streams into one final chunk", async () => {
    async function* singles() {
      yield { id: 1 };
      yield { id: 2 };
    }

    const chunks: unknown[] = [];
    for await (const chunk of normalize(singles())) {
      chunks.push(chunk);
    }

    expect(chunks).toEqual([[{ id: 1 }, { id: 2 }]]);
  });

  it("passes pre-chunked arrays through unchanged", async () => {
    async function* preChunked() {
      yield [{ id: 1 }];
      yield [{ id: 2 }];
    }

    const chunks: unknown[] = [];
    for await (const chunk of normalize(preChunked())) {
      chunks.push(chunk);
    }

    expect(chunks).toEqual([[{ id: 1 }], [{ id: 2 }]]);
  });
});
