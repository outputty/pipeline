---
"@outputty/pipeline": minor
---

`EventEmitterPipeline`'s events read as the route the chain was built along, and the chain's own
composed function is never registered on `pipeline.emitter` any more - it is called directly.

```text
stage:<n>              -> /transform/<n>                              (a .branch() arm: /branch/<i>/<name>/transform/<n>)
stage:<n>:dispatched   -> /transform/<n>:dispatched                   (":error"/":done" the same way)
stage:<n>:end          -> /transform/<n>:end                          (":end" for a .branch() arm's own trail)
pipeline:end           -> :end                                       (/branch/<i>/<name>:end for an arm)
```

The composed function is called directly rather than registered as a listener, which is what fixes
four collisions the old naming shared no defense against: a `.branch()` arm dispatching on the
parent's own stage, two sibling arms, two forks of one chain, and two independently-constructed
pipelines sharing one `emitter` - each used to silently answer with (or lose to) another chain's
output, and each now answers with its own.

Three guarantees are gone with it:

- `emitter.listenerCount(route)` no longer counts the composed function - it was never a listener.
- A caller can no longer take a stage over by calling `emitter.off(route, composedFn)` - there is
  no composed-function listener to remove.
- A stage with no caller-registered Worker no longer rejects with "no worker registered" - the
  composed function always answers, so every stage has one.

No deprecation period.
