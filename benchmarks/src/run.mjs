/**
 * The entry point every runtime container runs (#178). This first cut proves Done-when 6/7: the
 * canonical pipeline survives the packed-tarball install on THIS runtime, gated before anything else
 * runs. `L4` (a later layer of this same ticket) adds the full table 1/table 2 leg sweep and JSON
 * output this file will grow into - a bare gate check is what exists today, and it is real,
 * runnable proof rather than a stub with no output.
 *
 * `node benchmarks/src/run.mjs` (or `bun`/`deno run --allow-read --allow-env`) prints one JSON line:
 * `{"runtime":"node v26.8.1","canonical":[6,8,10],"from":"dist via tarball install"}`.
 */

import { Pipeline } from "@outputty/pipeline";
import { canonicalSource, expectedOutput } from "./canonical.mjs";

/** `runtimeVersion()` reads whichever global each of the six pinned runtimes exposes for its own
 * version string - `process.version` (Node, Bun) or `Deno.version.deno` (Deno) - so ONE file runs
 * unmodified everywhere rather than three runtime-specific entry points. */
function runtimeVersion() {
  if (typeof Deno !== "undefined") return `deno ${Deno.version.deno}`;
  if (typeof Bun !== "undefined") return `bun ${Bun.version}`;
  return `node ${process.version}`;
}

async function main() {
  const source = canonicalSource(5);
  const expected = expectedOutput(source);

  const actual = await new Pipeline()
    .transform((t) => t.map((x) => x * 2).filter((x) => x > 4))(source)
    .toArray();

  const matches =
    actual.length === expected.length && actual.every((value, i) => value === expected[i]);

  if (!matches) {
    console.error(
      JSON.stringify({
        runtime: runtimeVersion(),
        error: "canonical pipeline gate failed",
        got: actual,
        expected,
      }),
    );
    process.exit(1);
  }

  console.log(
    JSON.stringify({
      runtime: runtimeVersion(),
      canonical: actual,
      from: "dist via tarball install",
    }),
  );
}

main();
