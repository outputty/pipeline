---
"@outputty/pipeline": minor
---

`HttpPipeline`, `ClusterHttpPipeline` and `EventEmitterPipeline` move off the root entry to
`@outputty/pipeline/http`, `/cluster` and `/eventemitter`. `require("@outputty/pipeline")` (and a
browser bundle of it) no longer resolves `cluster`/`http`/`os`/`stream`/`events` for a consumer who
imports only `Pipeline`/`Transformer`: the root entry now loads no package AND no Node builtin.

```ts
// before
import { Pipeline, HttpPipeline, ClusterHttpPipeline, EventEmitterPipeline } from "@outputty/pipeline";

// after
import { Pipeline } from "@outputty/pipeline";
import { HttpPipeline, toNodeHandler, fetchClient, defaultClient } from "@outputty/pipeline/http";
import { ClusterHttpPipeline } from "@outputty/pipeline/cluster";
import { EventEmitterPipeline } from "@outputty/pipeline/eventemitter";
```

Seven names leave the root: `HttpPipeline`, `HttpPipelineOptions`, `PipelineClient`, `fetchClient`,
`defaultClient`, `toNodeHandler`, `ClusterHttpPipeline`, `ClusterHttpPipelineOptions`,
`EventEmitterPipeline`, `EventEmitterPipelineOptions`, `PipelineEmitter` and `WorkEvent` (all twelve).
`Pipeline`, `ConcurrentPipeline`, `Transformer`, `Codec` and `JsonCodec` stay on the root.

No new package is needed for any of the three new entries - only a Node runtime (or a runtime with
`node:cluster`/`node:http`/`node:os`/`node:stream`/`node:events`), same as before the split.

No deprecation period.
