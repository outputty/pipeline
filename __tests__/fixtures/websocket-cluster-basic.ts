/**
 * #201 Done-when 1 — the ticket's own Interface `after` example, verbatim, over the new ws+unix
 * `ClusterPipeline`: no server, no listen, no fork, no socket path in caller code. Mirrors
 * `cluster-basic.ts` (#17) exactly, imports swapped - `ClusterPipeline` forks real workers, so this
 * runs as a subprocess fixture the same reason that one does (`pipelines.e2e.test.ts`'s own header).
 */
import { ClusterPipeline } from "../../src/websocket";

const data = await new ClusterPipeline<number>()

  .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 4))([1, 2, 3, 4, 5])
  .toArray();

console.log(JSON.stringify(data));
