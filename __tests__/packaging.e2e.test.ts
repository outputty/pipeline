/**
 * #239 - `ws` is optional; #249 - the root loads no Node builtin either. Builds the package for real
 * (the three commands `pnpm build` chains, into a scratch `outDir`) and runs the OUTPUT, because
 * vitest runs `src` and cannot see what the bundler did: CJS with no splitting gives
 * `dist/websocket.cjs` its own copy of `Pipeline`, and only a run against `dist` shows `instanceof`
 * failing across entries; only a real browser-platform bundle shows a Node builtin import aborting
 * resolution.
 *
 * Two directories: `withWs` sits under the repo's `tmp/`, so `ws` resolves from the repo's own
 * `node_modules`; `withoutWs` sits under the OS temp directory, where no `ws` exists.
 */
import { execFileSync } from "node:child_process";
import {
  cpSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import * as esbuild from "esbuild";

const repo = resolve(import.meta.dirname, "..");
const bin = (name: string): string => join(repo, "node_modules", ".bin", name);

let withWs: string;
let withoutWs: string;

/** Every file under `dir` whose name matches `pattern`, as absolute paths. */
function filesUnder(dir: string, pattern: RegExp): string[] {
  return readdirSync(dir, { recursive: true, withFileTypes: true })
    .filter((entry) => entry.isFile() && pattern.test(entry.name))
    .map((entry) => join(entry.parentPath, entry.name));
}

/** Runs `script` under `node` in `cwd` and returns its last stdout line - a `ClusterPipeline` worker
 * re-executes the entry module and prints its own empty placeholder first (architecture.md). */
function runNode(cwd: string, script: string): string {
  const file = join(
    cwd,
    `probe-${Math.random().toString(36).slice(2)}${script.startsWith("import") ? ".mjs" : ".cjs"}`,
  );
  writeFileSync(file, script);
  const stdout = execFileSync(process.execPath, [file], {
    cwd,
    encoding: "utf8",
    timeout: 30000,
    stdio: ["ignore", "pipe", "pipe"],
  });
  return stdout.trim().split("\n").at(-1) ?? "";
}

beforeAll(() => {
  mkdirSync(join(repo, "tmp"), { recursive: true });
  withWs = mkdtempSync(join(repo, "tmp", "packaging-"));
  withoutWs = mkdtempSync(join(tmpdir(), "outputty-packaging-"));
  const dist = join(withWs, "dist");
  execFileSync(bin("tsup"), ["--out-dir", dist], { cwd: repo });
  execFileSync(bin("tsc"), ["-p", "tsconfig.build.json", "--outDir", dist], { cwd: repo });
  execFileSync(bin("tsc-alias"), ["-p", "tsconfig.build.json", "--outDir", dist], { cwd: repo });
  cpSync(join(repo, "package.json"), join(withWs, "package.json"));
  cpSync(dist, join(withoutWs, "dist"), { recursive: true });
  cpSync(join(repo, "package.json"), join(withoutWs, "package.json"));
}, 120000);

afterAll(() => {
  rmSync(withWs, { recursive: true, force: true });
  rmSync(withoutWs, { recursive: true, force: true });
});

/** The `ws` package as a bundle names it: ESM keeps double quotes, tsup's split CJS emits single. */
const WS_SPECIFIER = /["']ws["']/;

const chain = `new Pipeline().transform((t) => t.map((x) => x * 2))([1, 2, 3]).toArray()`;

describe("the root entry needs no package", () => {
  it("runs a plain Pipeline from CJS with no ws installed", () => {
    const out = runNode(
      withoutWs,
      `const { Pipeline } = require("./dist/index.cjs");\nconsole.log(JSON.stringify(${chain}));`,
    );
    expect(out).toBe("[2,4,6]");
  });

  it("runs a plain Pipeline from ESM with no ws installed", () => {
    const out = runNode(
      withoutWs,
      `import { Pipeline } from "./dist/index.js";\nconsole.log(JSON.stringify(${chain}));`,
    );
    expect(out).toBe("[2,4,6]");
  });

  it("exports the plain runners and none of the moved names", () => {
    const out = runNode(
      withoutWs,
      `const root = require("./dist/index.cjs");\n` +
        `console.log(JSON.stringify(["WebSocketPipeline", "ClusterPipeline", "toNodeWebSocketHandler", "HttpPipeline", "ClusterHttpPipeline", "EventEmitterPipeline", "JsonCodec"].map((name) => name in root)));`,
    );
    expect(out).toBe("[false,false,false,false,false,false,true]");
  });

  it("bundles for a browser target with no unresolved import (#249)", async () => {
    const result = await esbuild.build({
      stdin: {
        contents: `import { Pipeline, Transformer } from "@outputty/pipeline";\nexport { Pipeline, Transformer };`,
        resolveDir: withWs,
        loader: "js",
      },
      bundle: true,
      platform: "browser",
      write: false,
      alias: { "@outputty/pipeline": join(withWs, "dist", "index.js") },
      logLevel: "silent",
    });
    expect(result.errors).toEqual([]);
    const bundle = result.outputFiles[0]?.text ?? "";
    expect(bundle).not.toMatch(/from "cluster"|from "http"|from "os"|from "stream"|from "events"/);
  });

  it("fails loud on the /websocket entry when ws is missing", () => {
    expect(() => runNode(withoutWs, `require("./dist/websocket.cjs");`)).toThrow(
      /Cannot find module 'ws'/,
    );
  });

  it("names ws in no root bundle, chunk or declaration file", () => {
    const rootJs = filesUnder(join(withWs, "dist"), /^(index\.(js|cjs)|chunk-.*\.c?js)$/);
    expect(rootJs.length).toBeGreaterThan(2);
    for (const file of rootJs) expect(readFileSync(file, "utf8"), file).not.toMatch(WS_SPECIFIER);
    const declarations = filesUnder(join(withWs, "dist"), /\.d\.ts$/);
    expect(declarations.length).toBeGreaterThan(10);
    for (const file of declarations)
      expect(readFileSync(file, "utf8"), file).not.toMatch(/from ["']ws["']/);
    // Control: the search finds `ws` where it must be.
    expect(readFileSync(join(withWs, "dist", "websocket.js"), "utf8")).toMatch(WS_SPECIFIER);
    expect(readFileSync(join(withWs, "dist", "websocket.cjs"), "utf8")).toMatch(WS_SPECIFIER);
  });
});

describe("the /websocket entry runs with ws installed", () => {
  it("runs ClusterPipeline from the CJS build", () => {
    const out = runNode(
      withWs,
      `const { ClusterPipeline } = require("./dist/websocket.cjs");\n` +
        `new ClusterPipeline({ workers: 1 }).transform((t) => t.map((x) => x * 2))([1, 2, 3, 4, 5]).toArray().then((rows) => console.log(JSON.stringify(rows)));`,
    );
    expect(out).toBe("[2,4,6,8,10]");
  });

  it("shares one Pipeline class between the two CJS entries", () => {
    const out = runNode(
      withWs,
      `const root = require("./dist/index.cjs");\nconst ws = require("./dist/websocket.cjs");\n` +
        `const chain = new root.Pipeline().transform((t) => t.map((x) => x * 2));\n` +
        `const wrapped = new ws.WebSocketPipeline(chain, { connect: "ws://127.0.0.1:1" });\n` +
        `console.log(JSON.stringify([wrapped instanceof root.Pipeline, ws.WebSocketPipeline.prototype instanceof root.ConcurrentPipeline]));`,
    );
    expect(out).toBe("[true,true]");
  });
});

describe("a consumer needs no ws types", () => {
  it("typechecks both entries strictly with neither ws nor @types/ws installed", () => {
    const consumer = join(withoutWs, "consumer");
    const installed = join(consumer, "node_modules", "@outputty", "pipeline");
    mkdirSync(installed, { recursive: true });
    cpSync(join(withWs, "dist"), join(installed, "dist"), { recursive: true });
    cpSync(join(repo, "package.json"), join(installed, "package.json"));
    mkdirSync(join(consumer, "node_modules", "@types"), { recursive: true });
    symlinkSync(
      join(repo, "node_modules", "@types", "node"),
      join(consumer, "node_modules", "@types", "node"),
    );
    writeFileSync(
      join(consumer, "use.ts"),
      [
        `import { createServer } from "node:http";`,
        `import { Pipeline, JsonCodec } from "@outputty/pipeline";`,
        `import { WebSocketPipeline, ClusterPipeline, toNodeWebSocketHandler } from "@outputty/pipeline/websocket";`,
        `import { HttpPipeline } from "@outputty/pipeline/http";`,
        `import { ClusterHttpPipeline } from "@outputty/pipeline/cluster";`,
        `import { EventEmitterPipeline } from "@outputty/pipeline/eventemitter";`,
        `export const kept = [new Pipeline<number>(), new JsonCodec()];`,
        `export const moved = [WebSocketPipeline, ClusterPipeline, HttpPipeline, ClusterHttpPipeline, EventEmitterPipeline];`,
        `const handler = toNodeWebSocketHandler({ serve() {} });`,
        `export const server = createServer().on("upgrade", (req, socket, head) => handler.upgrade(req, socket, head));`,
      ].join("\n"),
    );
    writeFileSync(
      join(consumer, "tsconfig.json"),
      JSON.stringify({
        compilerOptions: {
          target: "ES2022",
          module: "ESNext",
          moduleResolution: "bundler",
          strict: true,
          skipLibCheck: false,
          types: ["node"],
          noEmit: true,
        },
        files: ["use.ts"],
      }),
    );
    expect(() => execFileSync(bin("tsc"), ["-p", consumer], { encoding: "utf8" })).not.toThrow();
  }, 60000);
});
