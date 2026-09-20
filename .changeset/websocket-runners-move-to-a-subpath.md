---
"@outputty/pipeline": minor
---

`WebSocketPipeline` and `ClusterPipeline` move to `@outputty/pipeline/websocket`, and `ws` becomes an
optional peer dependency. `require("@outputty/pipeline")` no longer fails with `Cannot find module
'ws'` for a plain `Pipeline`: the root entry loads no package.

```ts
// before
import { WebSocketPipeline, ClusterPipeline, toNodeWebSocketHandler } from "@outputty/pipeline";

// after
import {
  WebSocketPipeline,
  ClusterPipeline,
  toNodeWebSocketHandler,
} from "@outputty/pipeline/websocket";
```

Seven names leave the root: `WebSocketPipeline`, `WebSocketPipelineOptions`, `PipelineSocket`,
`toNodeWebSocketHandler`, `NodeWebSocketHandler`, `ClusterPipeline` and `ClusterPipelineOptions`.
`ClusterHttpPipeline`, `Codec` and `JsonCodec` stay on the root.

A caller of `/websocket` installs `ws` themselves (`pnpm add ws`); `@types/ws` is not needed, because
`NodeWebSocketHandler.upgrade` is typed with `node:http`'s `IncomingMessage` and `node:stream`'s
`Duplex` instead of `ws`'s own types. `Pipeline`, `ConcurrentPipeline`, `HttpPipeline`,
`ClusterHttpPipeline` and `EventEmitterPipeline` need no package.

A `/websocket`-only worker process now starts only the WebSocket worker server; it used to start the
HTTP one as well, because both lived in one file.

No deprecation period.
