/**
 * `bench/`'s own real loopback HTTP server helper - the same shape as
 * `__tests__/helpers/fixtures.ts`'s `withServer`/`withTrackedServer`, not imported from there: that
 * file imports `expect` from `vitest` at module top, which `bench/` never depends on (`bench/` runs
 * as a plain script, `pnpm bench:overhead`, outside the test runner entirely).
 */
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { toNodeHandler } from "../../src";

/** Binds `handler` to a real loopback server, runs `use` against its `http://localhost:<port>` url,
 * and always closes the server after. */
export async function withLoopbackServer<T>(
  handler: (request: Request) => Promise<Response>,
  use: (url: string) => Promise<T>,
): Promise<T> {
  const server = createServer(toNodeHandler(handler));
  await new Promise<void>((resolve) => server.listen(0, resolve));
  const port = (server.address() as AddressInfo).port;
  try {
    return await use(`http://localhost:${port}`);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

/** `countingHandler()`'s own return shape, named so its callers don't widen it to an anonymous
 * object type. */
export interface CountingHandler {
  handler: (request: Request) => Promise<Response>;
  counter: { requests: number };
}

/** Wraps `handler`, counting every request it serves while `use` runs - `HttpPipeline`'s own
 * `.local()` row reads this count to report `requestsWhilePinned` (Done-when 3). */
export function countingHandler(handler: (request: Request) => Promise<Response>): CountingHandler {
  const counter = { requests: 0 };
  return {
    handler: async (request) => {
      counter.requests++;
      return handler(request);
    },
    counter,
  };
}
