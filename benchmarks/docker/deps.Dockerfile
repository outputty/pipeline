# Installs every comparator ONCE (#178) - `npm ci` from the committed lockfile, plus the packed
# @outputty/pipeline tarball `npm pack --pack-destination benchmarks/` produced. Every one of the six
# runtime Dockerfiles `COPY --from=outputty-benchmarks-deps` this image's own /app/node_modules rather
# than running its own install, so six columns can never measure six different library versions
# (`.claude/architecture.md`'s own reason: node20 ships npm 10.8.2, node26 ships npm 12.x, and
# npm/bun/deno resolve differently again). Built once, standalone, BEFORE `docker compose build`:
#
#   docker build -f benchmarks/docker/deps.Dockerfile -t outputty-benchmarks-deps benchmarks/
#
# node:22-alpine is the installer here regardless of which runtime later consumes the result -
# `npm ci` only needs a working npm, not the runtime under test.
FROM node:22-alpine
WORKDIR /app
COPY package.json package-lock.json outputty-pipeline-0.2.0.tgz ./
RUN npm ci
