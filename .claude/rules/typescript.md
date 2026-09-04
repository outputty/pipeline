---
paths: ["**/*.ts", "**/*.tsx"]
---

# TypeScript

- A conditional type gating a constructor or function's own parameter never resolves inside any generic
  scope that still holds its type parameters abstract, even where both sides of `extends` are the
  literally same parameter. Give the concrete-value call site a plain, non-conditional overload ahead
  of the conditional one, so an internal generic call site resolves against the plain overload instead.
  (2026-09-04)
- An array literal passed with no assignment-context annotation widens to its element's base type, not
  its literal union, and this widening happens independently inside a wrapping generic function call's
  own arguments - the outer call's return-type annotation does not propagate it back in. Annotate the
  literal at its own construction site (`new Pipeline<"a" | "b">([...])`) when a literal union must
  survive. (2026-09-04)
