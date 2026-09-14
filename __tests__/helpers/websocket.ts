/**
 * Shared e2e helper for `WebSocketPipeline` (#201) - a real `http.Server` bound to a real unix
 * domain socket, upgraded via `toNodeWebSocketHandler`, so every case dials a real `ws+unix:` target
 * instead of a mock. Mirrors `helpers/fixtures.ts`'s own `withServer` for the HTTP wire.
 */
import { createServer } from "node:http";
import type { Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { unlink } from "node:fs/promises";
import { toNodeWebSocketHandler } from "../../src";
import type { PipelineSocket } from "../../src";

let nextSocketId = 0;

/** Binds `pipeline.serve` behind a real unix-socket HTTP upgrade, runs `use` against the
 * `ws+unix:<path>:/` target that reaches it, and always tears the socket down after - the one seam
 * every `WebSocketPipeline` wire case goes through, so a leaked socket file on a failed assertion
 * can't happen. */
export async function withWebSocketServer<T>(
  pipeline: { serve(socket: PipelineSocket): void },
  use: (connect: string) => Promise<T>,
): Promise<T> {
  const socketPath = join(
    tmpdir(),
    `outputty-pipeline-ws-test-${process.pid}-${nextSocketId++}.sock`,
  );
  const server = createServer();
  const handler = toNodeWebSocketHandler(pipeline);
  server.on("upgrade", (request, socket, head) => handler.upgrade(request, socket, head));

  // `server.closeAllConnections()` (Node 18.2+) does not reach a socket once it has been
  // UPGRADED - Node's http implementation hands the raw socket to the upgrade handler and stops
  // tracking it as one of the server's own HTTP connections, so `server.close()` waits forever on
  // it. Tracked here instead, at the raw `net.Server` "connection" event (fires on every accepted
  // TCP/unix connection, before any upgrade), and destroyed directly - the client's own connection
  // is deliberately persistent (one multiplexed socket per `connect` target, kept open across
  // calls, `websocket.ts`'s own `getConnection()`), so it never ends on its own.
  const sockets = new Set<Socket>();
  server.on("connection", (socket: Socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
  });

  await new Promise<void>((resolve) => server.listen(socketPath, resolve));
  try {
    return await use(`ws+unix:${socketPath}:/`);
  } finally {
    for (const socket of sockets) socket.destroy();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await unlink(socketPath).catch(() => {});
  }
}
