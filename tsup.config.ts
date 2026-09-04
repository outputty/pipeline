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
  // Don't bundle dependencies
  external: ["p-limit"],
  esbuildOptions(options) {
    options.alias = {
      "@src": resolve(__dirname, "src"),
    };
  },
});
