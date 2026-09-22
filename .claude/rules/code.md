# Code - @outputty/pipeline

- Before writing an import-boundary or structural prose rule, check whether an `.oxlintrc.json` override covers it or could.
- Queue incoming messages on a multiplexed connection per correlation id (`frameQueues` in `src/pipelines/websocket.ts`); different ids still dispatch concurrently.
- Give a raced or later-awaited promise a throwaway `.catch(() => {})` the moment it is created, before any code that could throw first.
- Wrap the whole bridged call of a Promise-handler-to-callback-server adapter in its own try/catch, and write an explicit failure response.
- Start feeding a streaming or duplex probe's input before awaiting the call that consumes it.
- After `pnpm bench:compare <ref>` on a branch that deleted a `src/` file, `git rm -f` every `A` line under `src/` in `git status --porcelain`.
- Re-run a zero-hit search's control on the exact artifact after any change to how it is produced (a bundler flag, a format, a target).
- Measure runtime cost before the first edit (on the base commit) and again before the docs layer: GC count, promises per row, peak heap (`pnpm bench:memory` on the ticket's chain). Report both; a divergence is the user's decision.
- Under Bun, `node:async_hooks` hooks and `PerformanceObserver('gc')` never fire; measure with `bun:jsc`'s `heapStats()`/`memoryUsage()` (a live-object snapshot, not a total-created count).
