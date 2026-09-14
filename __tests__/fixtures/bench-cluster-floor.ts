/**
 * #120 Done-when 2 - `ClusterHttpPipeline`'s own floor-equality, run as a subprocess (real
 * `ClusterHttpPipeline` construction forks real workers - never inside a Vitest worker).
 */
import { clusterMatchesFloor } from "../../bench/legs/cluster";

const matches = await clusterMatchesFloor(50);
console.log(JSON.stringify({ matches }));
