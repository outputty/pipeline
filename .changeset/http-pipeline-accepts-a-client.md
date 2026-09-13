---
"@outputty/pipeline": minor
---

`HttpPipeline` accepts `options.client`: how a dispatched chunk reaches the other instance. It takes
the global `fetch` signature, so the default is a drop-in and so is a caller's own, and it is carried
through copy-on-write like every other knob. `ClusterPipeline` inherits it. It is named `client`
rather than `fetch` because `pipeline.fetch` is already that class's own server handler.

```ts
new HttpPipeline(chain, { url, client: myClient });
```

⚠ The DEFAULT changes on Node, for an `http:` url only. A caller who names no client moves from the
global `fetch` to `node:http` with one shared keep-alive agent, resolved once per process. Bun, Deno
and Cloudflare Workers keep the global `fetch`, and so does any runtime where `node:http` does not
import - the probe is a dynamic import inside a `try`, so a runtime without it falls back rather than
failing to load.

An `https:` url stays on the global `fetch` too, and that is a guard rather than an omission:
`node:http` speaks cleartext only and reads a url's empty `port` as 80, so dispatching an `https:`
url through it sends the chunk JSON and any auth header in the clear to whatever answers on port 80.
`node:https` would need its own agent and TLS surface for a case where a per-request saving is
dwarfed by a real network anyway. Output is unchanged on every runtime and every url scheme.

Measured on a real loopback server, 200 chunks of 1000 rows, output asserted identical between the
two clients:

```text
/transform/<n>  one request PER CHUNK   fetch 1749-1828 ns/row   node:http 306-349 ns/row
/reduce/<n>     ONE request per stream  fetch  117-135 ns/row    node:http 112-134 ns/row
```

The whole saving is per-REQUEST, so it lands on the transform route and vanishes on the reduce route,
where one request carries the entire stream. `reduceWork()` moves to the seam anyway, so one client
serves the whole class rather than leaving a caller's own client applied to half of it.

A caller supplying their own client must stream both directions to serve `/reduce/<n>`: its wire is a
duplex NDJSON stream, so the `Response` has to resolve on the response headers with its body still
arriving. A client that collects the whole reply first loses no data and passes every
`/transform/<n>` case, then silently breaks the reduce route.
