import { defineConfig } from "tsup";
import { resolve } from "path";

export default defineConfig({
  entry: [
    "src/index.ts",
    "src/websocket.ts",
    "src/http.ts",
    "src/cluster.ts",
    "src/eventemitter.ts",
  ],
  format: ["esm", "cjs"],
  dts: false,
  clean: true,
  // tsup splits ESM chunks by default and CJS not at all (experimental flag): without it
  // `dist/websocket.cjs` carries its own copy of `Pipeline`, and `Pipeline.wrapping`'s
  // `instanceof Pipeline` (`src/pipeline.ts`) reads a root chain as an options object (#239).
  splitting: true,
  sourcemap: true,
  target: "node18",
  outDir: "dist",
  // Don't bundle dependencies - `ws` (#201) stays external explicitly, not only by tsup's own
  // default (a `peerDependencies` entry, #239, is already excluded from the bundle without this): a
  // future `noExternal` change elsewhere in this config would otherwise silently start bundling it.
  external: ["ws"],
  esbuildOptions(options) {
    options.alias = {
      "@src": resolve(__dirname, "src"),
    };
  },
});
