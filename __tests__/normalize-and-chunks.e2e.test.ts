import { describe, it, expect } from "vitest";
import { Pipeline } from "@src/pipeline";
import { normalize } from "@src/utils/chunk";

/**
 * normalize() + Pipeline's chunk-preserving async iteration.
 *
 * These encode the task's CONTRACT verbatim: normalize() must flush buffered
 * single items into a chunk whenever an array arrives (or the stream ends),
 * and Pipeline must be async-iterable, yielding transformed chunks whose
 * boundaries match normalize(source) rather than a re-chunked/flattened view.
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

describe("Pipeline async iteration (chunk-preserving)", () => {
  it("yields transformed chunks whose boundaries match the source's shape", async () => {
    async function* preChunked() {
      yield [{ id: 1 }];
      yield [{ id: 2 }, { id: 3 }];
    }

    const pipeline = new Pipeline<{ id: number }>(preChunked()).transform((t) =>
      t.map((r: { id: number }) => ({ id: r.id * 2 })),
    );

    const chunks: unknown[] = [];
    for await (const chunk of pipeline) {
      chunks.push(chunk);
    }

    expect(chunks).toEqual([[{ id: 2 }], [{ id: 4 }, { id: 6 }]]);
  });

  it("does not break existing toArray-based flattened consumption", async () => {
    const pipeline = new Pipeline([1, 2, 3]).transform((t) => t.map((x: number) => x * 2));
    const results = await pipeline.toArray();

    expect(results).toEqual([2, 4, 6]);
  });
});
