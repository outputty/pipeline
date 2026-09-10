/**
 * #17 Done-when 4 — three `ClusterPipeline`s in one program share ONE port and ONE worker set.
 */
import { ClusterPipeline } from "../../src";

const p1 = new ClusterPipeline<number>();
const p2 = new ClusterPipeline<number>();
const p3 = new ClusterPipeline<number>();

const [r1, r2, r3] = await Promise.all([
  p1
    .transform((t) => t.map((x: number) => x))([1])
    .toArray(),
  p2
    .transform((t) => t.map((x: number) => x))([2])
    .toArray(),
  p3
    .transform((t) => t.map((x: number) => x))([3])
    .toArray(),
]);

console.log(JSON.stringify({ ports: [p1.url, p2.url, p3.url], results: [r1, r2, r3] }));
