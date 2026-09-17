/**
 * #209 Done-when 1 — the ticket's own Interface program, run over a real `ClusterPipeline` (real
 * fork, real worker): a file-backed `Codec` class over a temp directory shared through
 * `process.env`, mirroring `cluster-file-codec.ts` (#208) for the process split. Counts the
 * PRIMARY's own `encode`/`decode` calls directly on the codec instance - unlike #208's own fixture,
 * no shared log file is needed, since each process runs its own separate instance of this module
 * (`cluster.fork()` re-execs it) and therefore its own separate `FileCodec` object.
 */
import cluster from "node:cluster";
import { randomUUID } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ClusterPipeline, type Codec } from "../../src";

if (cluster.isPrimary) {
  process.env.CODEC_CLASS_STORE = mkdtempSync(join(tmpdir(), "cluster-codec-class-"));
}
const storeDir = process.env.CODEC_CLASS_STORE!;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

class FileCodec implements Codec {
  writes = 0;
  reads = 0;

  encode(value: unknown): Uint8Array {
    this.writes++;
    const key = randomUUID();
    writeFileSync(join(storeDir, `${key}.json`), JSON.stringify(value));
    return encoder.encode(key);
  }

  decode(bytes: Uint8Array): unknown {
    this.reads++;
    const raw = readFileSync(join(storeDir, `${decoder.decode(bytes)}.json`), "utf8");
    return JSON.parse(raw);
  }
}

const codec = new FileCodec();

const pipeline = new ClusterPipeline<number>({ workers: 2, maxConcurrency: 2, codec })
  .buffer(1)
  .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 4))
  .transform((t) => t.map((x: number) => x + 1));

const out = await pipeline([1, 2, 3, 4, 5, 6, 7, 8]).toArray();
const toArrayOps = { write: codec.writes, read: codec.reads };

codec.writes = 0;
codec.reads = 0;
await pipeline([1, 2, 3, 4, 5, 6, 7, 8]).consume();
const consumeOps = { write: codec.writes, read: codec.reads };

console.log(JSON.stringify({ out, toArrayOps, consumeOps }));

if (cluster.isPrimary) {
  rmSync(storeDir, { recursive: true, force: true });
}
