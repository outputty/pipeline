---
"@outputty/pipeline": minor
---

`Pipeline.queue(capacity)` prefetches up to `capacity` chunks ahead of the consumer, decoupling when
a chunk is pulled from when a downstream terminal asks for it.

```ts
const data = await new Pipeline<number>()
  .buffer(2)
  .queue(3)
  .transform((t) => t.map((x) => x * 2).filter((x) => x > 4))([1, 2, 3, 4, 5])
  .toArray();

console.log(data); // [6, 8, 10]
```

An array of exactly `capacity` pending `upstream.next()` promises: the consumer takes the front one,
and the instant it does, a fresh promise is pushed onto the back - order preserved, never a race.
`.buffer()` still owns the cut; `.queue()` only changes when each already-cut chunk is fetched.
Always widens the pipeline's Mode to `"async"`, even over an entirely synchronous chain, since a
queued chunk may not be ready yet.

A 100ms/item source through a 30ms/item transform, 5 items, ran 674ms fully serial and 542ms queued
at `.queue(3)` - overlap between production and consumption, never concurrent production: a single
async generator source still serializes its own internal work regardless of how many pulls are in
flight.
