/**
 * #17 Done-when 14 (cluster half) — `.context({multiplier:10})` propagates through a
 * `ClusterPipeline` stage. Also pins the constructor identity right after `.context()`: without
 * `Pipeline`'s polymorphic copy-on-write (#17 L2), `.context()` demotes to a plain `Pipeline`,
 * whose OWN real (already-working) `.transform()` then produces the same numeric result by
 * accident - the array alone cannot tell "really dispatched to a worker" from "silently ran
 * in-process instead".
 */
import { ClusterPipeline } from "../../src";

const afterContext = new ClusterPipeline([1, 2, 3, 4, 5]).context({ multiplier: 10 });
const out = await afterContext
  .transform((t) => t.map((x: number, ctx) => x * (ctx.get("multiplier") as number)))
  .toArray();

console.log(JSON.stringify({ out, ctorNameAfterContext: afterContext.constructor.name }));
