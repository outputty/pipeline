/**
 * #208 Done-when 2 — a codec whose `encode` never sends the value itself: it writes the chunk to a
 * file in a shared store and sends only the 36-byte `randomUUID()` key. Every worker builds this
 * SAME codec by re-running the entry module (the ticket's own Constraints), so the store has to be
 * reachable from every one of them - shared here through `process.env`, which `cluster.fork()`
 * inherits, exactly as the ticket's own planning spike proved.
 *
 * `decode` only logs when it runs INSIDE a worker (`cluster.isWorker`): the primary decodes each
 * chunk's own RESULT back off the wire too (`websocket.ts`'s own dispatch, symmetric with encode),
 * and that decode is not what this Done-when is asking about - only that a worker's own decode
 * never sees more than the 36-byte key.
 */
import cluster from "node:cluster";
import { randomUUID } from "node:crypto";
import { appendFileSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { Codec } from "../../src";
import { ClusterPipeline } from "../../src/websocket";

// Only the PRIMARY creates the store - a worker re-executing this same module must NOT mint its
// own directory, or its codec silently writes and reads a different one than the primary's
// (`process.env.CLUSTER_FILE_CODEC_STORE` below is what makes the primary's directory visible to
// it instead).
if (cluster.isPrimary) {
  process.env.CLUSTER_FILE_CODEC_STORE = mkdtempSync(join(tmpdir(), "cluster-file-codec-"));
  writeFileSync(join(process.env.CLUSTER_FILE_CODEC_STORE, "decode.log"), "");
}
const storeDir = process.env.CLUSTER_FILE_CODEC_STORE!;
const logFile = join(storeDir, "decode.log");

const encoder = new TextEncoder();
const decoder = new TextDecoder();

const fileCodec: Codec = {
  encode(value) {
    const key = randomUUID();
    writeFileSync(join(storeDir, `${key}.json`), JSON.stringify(value));
    return encoder.encode(key);
  },
  decode(bytes) {
    const key = decoder.decode(bytes);
    if (cluster.isWorker) {
      appendFileSync(logFile, `${process.pid} ${bytes.length}\n`);
    }
    const raw = readFileSync(join(storeDir, `${key}.json`), "utf8");
    return JSON.parse(raw);
  },
};

const transform = await new ClusterPipeline<number>({
  workers: 2,
  maxConcurrency: 2,
  codec: fileCodec,
})
  .buffer(1)
  .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 4))([
    1, 2, 3, 4, 5, 6, 7, 8,
  ])
  .toArray();

const decodeLines = readFileSync(logFile, "utf8").trim().split("\n").filter(Boolean);
const workerPids = new Set(decodeLines.map((line) => line.split(" ")[0]));
const decodedByteLengths = new Set(decodeLines.map((line) => Number(line.split(" ")[1])));

console.log(
  JSON.stringify({
    transform,
    workerDecodeBytes: [...decodedByteLengths],
    workersThatDecoded: workerPids.size,
  }),
);

// The primary's own store dir is never read again after this - remove it so a repeated test run
// doesn't accumulate one `cluster-file-codec-*` directory (plus a per-chunk `<uuid>.json` file
// inside it) in the OS tmpdir per invocation.
if (cluster.isPrimary) {
  rmSync(storeDir, { recursive: true, force: true });
}
