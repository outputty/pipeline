# anti-slop (vendored)

Third-party oxlint plugin — [dmmulroy/anti-slop](https://github.com/dmmulroy/anti-slop), MIT
(see `LICENSE`). 15 generic rules that reject AI-generated / low-evidence TypeScript patterns.

Vendored, not an npm dependency, because upstream ships as source to copy in. The runtime dependency
`@oxlint/plugins` (pinned to the oxlint version in the root `package.json`) IS an npm dep.

## Wiring

`.oxlintrc.json` loads this via `jsPlugins: ["./tools/oxlint/anti-slop/index.ts"]` and sets each rule's
level there. oxlint runs the `.ts` entry natively through Node's type-stripping (Node ≥ 22.18). This
directory is excluded from oxlint's own lint (`ignorePatterns: tools/**`) and from prettier
(`.prettierignore`) so third-party code stays byte-identical to upstream.

## What is vendored, and what is not

- Vendored: `index.ts` (the generic 15-rule plugin, the default export), `rules/*.ts`, `shared/*.ts`.
- Dropped: `*.test.ts` (need upstream's own `RuleTester` harness) and `effect/` — its one rule,
  `no-service-constructor-imports`, is Effect-library-specific and N/A here.

## Re-vendoring a new release

Pinned to upstream commit `6d538555cb151d4121ed51a27db81890eacf8ae9` (see `.upstream-commit`). To bump:
copy the new `src/{index,rules,shared}` over this directory (keep `LICENSE`, this `README`, and update
`.upstream-commit`), bump `@oxlint/plugins` in the root `package.json` to match oxlint, then re-run the
per-rule sweep and reconcile `.oxlintrc.json`'s levels.
