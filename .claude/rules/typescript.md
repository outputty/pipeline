---
paths: ["**/*.ts", "**/*.tsx"]
---

# TypeScript

- Order a multi-arm conditional type so the parameter a CALLER pins is tested first. (2026-09-10)
  - A subclass that pins `M` to a literal on its own `extends` clause can only benefit from that if
    the conditional reads `M` before the still-abstract one: `JoinMode` testing `S` first stayed
    deferred inside every generic scope and made the three dispatching classes carry a whole extra
    type parameter, plus its own operator, to collapse what one reordering collapses. The two orders
    are identical at every concrete instantiation, so no test catches the difference - only a
    subclass override failing `TS2416` does.
- A conditional type gating a constructor or function's own parameter never resolves inside any generic
  scope that still holds its type parameters abstract, even where both sides of `extends` are the
  literally same parameter. Put a plain, non-conditional overload for the fully-supplied argument shape
  ahead of the conditional one: an internal generic call site resolves against the plain overload,
  while a concrete external call site still meets the conditional gate. (2026-09-04)
- An array literal passed with no assignment-context annotation widens to its element's base type, not
  its literal union, and this widening happens independently inside a wrapping generic function call's
  own arguments - the outer call's return-type annotation does not propagate it back in. Annotate the
  literal at its own construction site (`new Pipeline<"a" | "b">([...])`) when a literal union must
  survive. (2026-09-04)
- Call `.buffer(1)` before the stage under test in any probe that reads a CHUNK-granular observable -
  concurrency, ordering, a per-chunk context write, a per-chunk error. (2026-09-06, generalised
  2026-09-10)
  - At `DEFAULT_CHUNK_SIZE` a small source is ONE chunk, so nothing the mechanism does within a
    chunk is visible and the probe passes for the wrong reason: a plain `Pipeline` also reads 4 in
    flight, and a four-item branch also comes back in order. The same four items behind
    `.buffer(1)` returned `["item-2","item-3","item-4","item-1"]`.
