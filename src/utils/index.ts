/**
 * Utils barrel export. `normalize` stays out of it (#133) - dropped from the PUBLIC surface, since
 * nothing in `src/` calls it any more (#39); it is still defined and used internally in
 * `./chunk`, and importable directly from `@src/utils/chunk` for a test that wants it standalone.
 */
export { buildChunkGenerator } from "./chunk";
export { isContextAware } from "./helpers";
