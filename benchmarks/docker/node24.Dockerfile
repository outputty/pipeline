# Requires outputty-benchmarks-deps built first (see docker/deps.Dockerfile's own header).
FROM outputty-benchmarks-deps AS deps

FROM node:24-alpine
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY src ./src
CMD ["node", "src/run.mjs"]
