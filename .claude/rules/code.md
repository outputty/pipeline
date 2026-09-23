# Code - @outputty/pipeline

- Queue incoming messages on a multiplexed connection per correlation id (`frameQueues` in `src/pipelines/websocket.ts`); different ids still dispatch concurrently.
- Wrap the whole bridged call of a Promise-handler-to-callback-server adapter in its own try/catch, and write an explicit failure response.
- Start feeding a streaming or duplex probe's input before awaiting the call that consumes it.
- After `pnpm bench:compare <ref>` on a branch that deleted a `src/` file, `git rm -f` every `A` line under `src/` in `git status --porcelain`.
- Measure runtime cost before the first edit (on the base commit) and again before the docs layer: GC count, promises per row, peak heap (`pnpm bench:memory` on the ticket's chain). Report both; a divergence is the user's decision.
- A `Pipeline` subclass that adds its own constructor knob overrides `carriedKnobs()` too, since `createPipeline()` rebuilds through `this.constructor`; otherwise the next copy-on-write call resets the knob to its default.
