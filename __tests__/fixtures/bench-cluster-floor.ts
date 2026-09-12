/**
 * #120 Done-when 2 - `ClusterPipeline`'s own floor-equality, run as a subprocess (real
 * `ClusterPipeline` construction forks real workers - never inside a Vitest worker).
 */
import { clusterMatchesFloor } from "../../bench/legs/cluster";

const matches = await clusterMatchesFloor(50);
console.log(JSON.stringify({ matches }));
