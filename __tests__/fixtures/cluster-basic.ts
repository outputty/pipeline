/**
 * #17 Done-when 1 & 3 — the ticket's own canonical program, verbatim, and NOTHING ELSE: no
 * server, no listen, no fork, no url, no explicit teardown. Case 3 asserts THIS script exits on
 * its own with code 0 - the unref'd idle timer (#17 L5) is what makes that true.
 */
import { ClusterPipeline } from "../../src";

const data = await new ClusterPipeline([1, 2, 3, 4, 5])
  .transform((t) => t.map((x: number) => x * 2).filter((x: number) => x > 4))
  .toArray();

console.log(JSON.stringify(data));
