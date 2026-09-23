---
paths: ["**/*.ts", "**/*.tsx"]
---

# TypeScript

- Order a multi-arm conditional type so the parameter a caller pins is tested first (`JoinMode` tests `M` before `S`).
- Call `.buffer(1)` before the stage under test in any probe that reads a chunk-granular observable (concurrency, ordering, a per-chunk context write or error); at the default chunk size a small source is one chunk.
