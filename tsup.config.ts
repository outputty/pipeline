import { defineConfig } from "tsup";
import { resolve } from "path";

export default defineConfig({
  entry: ["src/index.ts"],
  format: ["esm", "cjs"],
  dts: false,
  clean: true,
  sourcemap: true,
  target: "node18",
  outDir: "dist",
  // Don't bundle dependencies - `ws` (#201) stays external explicitly, not only by tsup's own
  // default (a real `dependencies` entry is already excluded from the bundle without this): a
  // future `noExternal` change elsewhere in this config would otherwise silently start bundling it.
  external: ["p-limit", "ws"],
  esbuildOptions(options) {
    options.alias = {
      "@src": resolve(__dirname, "src"),
    };
  },
});
