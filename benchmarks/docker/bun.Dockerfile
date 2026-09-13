# Requires outputty-benchmarks-deps built first (see docker/deps.Dockerfile's own header).
# Bun reads a standard npm-produced node_modules directly - no separate install step needed.
FROM outputty-benchmarks-deps AS deps

FROM oven/bun:1.3.14-alpine
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY src ./src
CMD ["bun", "run", "src/run.mjs"]
