/**
 * #208 Done-when 3 — `ClusterHttpPipeline` accepts `client` (#179's own knob on `HttpPipeline`)
 * and dispatches every chunk through it. A worker never dispatches (its own terminal op resolves
 * immediately, empty - `cluster.ts`'s own header), so a call count is a call count IN THE PRIMARY
 * by construction; naming it that way in the output is documentation, not a separate check.
 */
import { ClusterHttpPipeline } from "../../src";

let calls = 0;
const countingClient = async (url: string, init: RequestInit): Promise<Response> => {
  calls++;
  return fetch(url, init);
};

const output = await new ClusterHttpPipeline<number>({ workers: 2, client: countingClient })
  .buffer(1)
  .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 4))([1, 2, 3, 4, 5])
  .toArray();

console.log(JSON.stringify({ output, clientCallsInPrimary: calls }));
