# Requires outputty-benchmarks-deps built first (see docker/deps.Dockerfile's own header).
# Deno resolves a bare specifier like "@outputty/pipeline" from a plain npm-produced node_modules
# with no deno.json needed, as long as package.json sits next to it - verified locally (deno 2.9.6,
# real): `deno run --allow-read --allow-env src/run.mjs` against this exact node_modules layout
# printed {"runtime":"deno 2.9.6","canonical":[6,8,10],...}.
FROM outputty-benchmarks-deps AS deps

FROM denoland/deno:alpine-2.9.6
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY --from=deps /app/package.json ./package.json
COPY src ./src
CMD ["deno", "run", "--allow-read", "--allow-env", "src/run.mjs"]
