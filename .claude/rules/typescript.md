---
paths: ["**/*.ts", "**/*.tsx"]
---

# TypeScript

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
- Call `.buffer(1)` on BOTH sides before using an in-flight counter to tell a `Pipeline` from a
  `ConcurrentPipeline`. (2026-09-06)
  - At `DEFAULT_CHUNK_SIZE` a small source is one chunk and `.map()` runs its items together, so a
    plain `Pipeline` also reads 4 in flight and the counter proves nothing.
