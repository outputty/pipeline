# Installs every comparator ONCE (#178) - `npm ci` from the committed lockfile, plus the packed
# @outputty/pipeline tarball, renamed to a FIXED name so this file, package.json's own dependency and
# package-lock.json never drift when the root package's version bumps - `npm pack` itself names its
# output after the current version, and three files hardcoding that version was a code-review finding
# (bump to 0.3.0 and this file silently looked for a tarball that no longer existed). Every one of the
# six runtime Dockerfiles `COPY --from=outputty-benchmarks-deps` this image's own /app/node_modules
# rather than running its own install, so six columns can never measure six different library versions
# (`.claude/architecture.md`'s own reason: node20 ships npm 10.8.2, node26 ships npm 12.x, and
# npm/bun/deno resolve differently again). Built once, standalone, BEFORE `docker compose build`:
#
#   pnpm build
#   npm pack --pack-destination benchmarks/
#   mv benchmarks/outputty-pipeline-*.tgz benchmarks/pipeline.tgz
#   docker build -f benchmarks/docker/deps.Dockerfile -t outputty-benchmarks-deps benchmarks/
#
# `pnpm bench:cross-runtime:prep` runs the first three steps in one command.
#
# node:22-alpine is the installer here regardless of which runtime later consumes the result -
# `npm ci` only needs a working npm, not the runtime under test.
FROM node:22-alpine
WORKDIR /app
COPY package.json package-lock.json pipeline.tgz ./
RUN npm ci
