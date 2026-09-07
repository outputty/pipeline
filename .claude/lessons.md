# Lessons

The mistakes made building this product, recorded so they are not repeated. `retro` appends entries at
the end of every planning session and inside every build's docs layer.

- A lesson remembers; the rule, skill or doc change it produced enforces. Every entry links that change.
- An entry is one paragraph; the incident's detail stays in the session.
- Newest first. Development context lives here and in the tracker, never in `product.md`.

## 2026-09-07 #61's Done-when 7 silently contradicted Done-when 5 and its own Constraints

Done-when 7 said "no file outside `src/`, `__tests__/` and `.claude/` changed"; Done-when 5 and the
Constraints section both required a `README.md` edit - a file outside all three. Caught only by
reading the whole ticket before the first edit, then resolved with `AskUserQuestion` rather than
guessing either reading. `~/.claude/rules/issues.md` gets a new rule: read a ticket's Implementation
criteria against its own Constraints/Where scope for a contradiction before the first edit, and ask
rather than pick a side silently.

## 2026-09-07 #61's plan comment and `gh stack init` landed after the code, not before it

The base `Pipeline.local()` method, the three subclass overrides, new tests and a fixture were all
written and green before the layer plan was posted or the stack was initialized - only `advisor`
caught that the single-PR-vs-stack decision, the plan comment and `gh stack init` were all still
outstanding on code that already existed. Recovery cost a `git diff` backup, a `git checkout -- .`
reset, and rebuilding the same edits as two `gh stack add`-first layers. `~/.claude/rules/issues.md`'s
existing diff-measurement rule (2026-09-03) is sharpened: writing a hard-to-pre-slice ticket's code
first can be unavoidable, but the plan comment, `gh stack init` and the shape decision itself must
still land before the first COMMIT, not get skipped because the code already exists.

## 2026-09-07 A second `ScheduleWakeup` call, waiting on a backgrounded `/code-review` mid-build

After the first `/code-review` call for L1 finished and while waiting on the enable layer's own
`/code-review`, `ScheduleWakeup` was called again despite the exact same tool being flagged and
self-caught earlier in this same session. `~/.claude/rules/code.md`'s existing rule (already
re-violated four times before this) gets a fifth violation logged, self-caught one call later by
`ListAgents` instead.

## 2026-09-07 #41's instance `merge()` copied the static `merge()`'s own loop instead of sharing it

Writing `Pipeline.prototype.merge()` right below the static `Pipeline.merge()`, the same
context-merge loop and chunk-concatenation generator were typed out a second time - the new
method's own docstring even called it "the static's own semantics, unchanged," naming the
duplication without acting on it. `/code-review medium` caught it as a reuse finding; the fix
extracted `mergeContextsInto()`/`concatChunks()` as the one shared implementation both methods call.
`~/.claude/rules/code.md`'s Reuse ladder gains a line: a new method's own docstring calling itself
"the same semantics" as an adjacent sibling is the cue to extract before writing, not after review.

## 2026-09-07 #45's L2 commit landed on L1's own branch, again

Straight after L1's commit and PR went out clean, L2's `Reducer` helper, `Pipeline.reduce()` and the
`_reduceStages` bookkeeping were written, reviewed and committed - all still on `worktree-ticket-45`,
PR #52's own branch - before `gh stack add feature/pipeline-reduce-45-l2` ever ran. The commit had not
reached the remote yet, so recovery was a plain `git branch feature/pipeline-reduce-45-l2 eb327ea` +
`git reset --hard cb69cc6` on the worktree branch, then `gh stack add` adopted the pre-built branch
cleanly. `~/.claude/rules/issues.md`'s `gh stack add` rule is sharpened again (fourth violation): the
trap is the previous layer's own clean landing feeling like license to keep moving in the same flow,
not a fresh context losing track of the step - the rule now names that continuity itself as the
trigger to run `gh stack add` first, on every layer, not only the first one after a break.

## 2026-09-07 A `ScheduleWakeup` call justified as "a long fallback" was still outside any `/loop`

Waiting on a backgrounded `/code-review` subagent mid-build, `ScheduleWakeup` was called with a 1200s
delay and `noop: true`, reasoned as "not polling, a long fallback" per the tool's own guidance for
surviving a hung subagent. But this session was a `/goal`-driven build with no `/loop` running at all -
the tool is gated to `/loop` dynamic mode specifically, and the delay chosen does not change that. Self-
caught one tool call later and stopped. `~/.claude/rules/code.md`'s existing rule (already re-violated
five times) is sharpened to name the rationalization itself: framing a call as a "long fallback" does
not exempt it from the `/loop`-only gate.

## 2026-09-07 A hand-built JSON string as a `Read` call's whole input hit `__unparsedToolInput`

A `Read` call was made with a hand-assembled JSON-looking string (a stray trailing comma after the
`offset` field) instead of separate `file_path`/`offset` parameters, and the harness reported it as
`__unparsedToolInput`, refusing to parse it as the declared schema. Caught immediately and re-issued
with real parameters. `~/.claude/rules/code.md`'s existing `__unparsedToolInput` rule is sharpened: the
trap is not limited to AskUserQuestion's multi-question array (its one prior example) - any tool call
built by hand-assembling a JSON-shaped string instead of using the declared fields directly can fall
into the same failure.

## 2026-09-07 `insert_before_symbol` orphaned `HttpPipeline`'s own class docstring

Inserting `runReduceStage` and its helpers before the `HttpPipeline` class symbol landed the new
content BETWEEN the class's own preceding docstring and the class itself, since the docstring was not
treated as part of the symbol's leading trivia the way `insert_before_symbol` reasons about it. The
docstring stayed intact but now floated above unrelated helper functions, describing a class two
screens below it. Found on a routine re-read after the edit, not by any tool error. `~/.claude/rules
/code.md`'s existing lesson (about `replace_symbol_body` duplicating a docstring) is broadened to cover
`insert_before_symbol`/`insert_after_symbol`'s own inverse failure - orphaning rather than duplicating.

## 2026-09-07 #39's real L1 layer landed its first two commits on the ticket's own worktree branch

Rebuilding #39 after the replan, L1's test file was written, tested, reviewed and committed twice -
all on `worktree-ticket-39` itself, PR #47's own branch - before `gh stack add feature/buffer-39-l1`
ever ran. `gh stack add` then refused outright (a stale local `gh stack` entry, `feature/chunker-seam-
39-l2`, left over from the earlier aborted L1 attempt, made the tool think `worktree-ticket-39` was
not "the top of the stack"). Recovered with `gh stack unstack --local` + `gh stack init` to rebuild
clean tracking, `gh stack add` for the real branch, then `git branch -f worktree-ticket-39 bc5a048`
to move the two stray commits off the base branch (safe only because nothing had been pushed yet -
`git log origin/worktree-ticket-39` was checked first). `~/.claude/rules/issues.md`'s `gh stack add`
rule (already re-violated once before, 2026-09-04) is sharpened again: run it as the layer's FIRST
action, before any file touch, and its own new bullet documents the stale-entry recovery so the next
session doesn't spend ten tool calls hunting for where `gh stack` stores the phantom branch.

## 2026-09-06 A handoff note's coined word framed a whole question round

Planning #45, the primary session's notes called the thing to build a "producer". The word appears
nowhere in `src/`, `__tests__/`, `.claude/`, `CLAUDE.md` or `README.md` - verified before any question
was asked, and recorded as a premise. That verification was read as "settle what it names here"
rather than "an intermediary coined this", so the first question round opened on the term and the
user's first reply was to reject the frame: they had said reducer throughout.
`~/.claude/rules/code.md` now says to use the user's own noun for any term that arrived through a
handoff, a peer session or a summary and appears nowhere in the code.

## 2026-09-06 A duplex probe's own ordering nearly confirmed a false claim

The session carried in "Node's `fetch` is half-duplex", labelled unverified. The first probe awaited
`fetch` before starting the loop that fed its request body, so the body was never pushed and the run
ended `RESPONSE HEADERS AT +90011ms status 408` - indistinguishable from the response being withheld
until the body completes, which is exactly the claim under test. Only the absurd 90-second timing
prompted a second look; reordered, the real answer was `echoes received BEFORE the request body
closed = 3 of 3`, and the whole remote-reducer design rests on it. `.claude/rules/code.md` (new, this
repo) now says to feed a streaming probe's input before awaiting the call that consumes it.

## 2026-09-06 Closing #39's L1 draft PR left its commit on the worktree branch

Building #39, a mid-build architecture reopen (chunking should not live on `Transformer` at all)
triggered the "premise is false" stop: comment posted, draft PR #44 closed, ticket labelled
`needs-planning`. The worktree's own branch still carried L1's commit after all of that - a `git log`
during the resumed planning session showed `main` plus one extra commit, and a `grep` for the OLD
design's symbols (`chunkGenerator`, `executeChunks`) still matched `src/`, as if the closed PR's code
were the real, shipped state. Caught before it misled any premise-check; fixed with `git reset --hard`
to `main`. `~/.claude/rules/code.md`'s While you work section now says to reset the worktree branch
too, not just close the PR.

## 2026-09-06 `_chunkTransforms` was nearly deleted for a role it doesn't own

Replanning #39's chunk-first redesign, the ticket draft first said `_chunkTransforms` (`pipeline.ts`)
gets deleted alongside `_sourcePositionViolations`, reasoning from its ONE consumer that motivated the
question - the async-iteration replay loop being unified away. A second grep, while writing
`architecture.md`, found `HttpPipeline.fetch()`'s own server-side stage lookup
(`this._chunkTransforms[requested]`, `src/pipelines/http.ts:170`) - a second, unrelated role the field
also serves, unaffected by the redesign. Caught before it reached the filed ticket.
`~/.claude/rules/code.md`'s Names and pointers section now says to grep every consumer BEFORE
proposing a shared mechanism's deletion, not just the one that raised the question.

## 2026-09-06 Several rounds designed machinery to skip a cost the design itself had just added

Mid-build, asked whether a redundant re-chunk between two dispatched stages could be skipped, several
rounds were spent designing detection machinery for it - a `preservesCount` flag on `Transformer`,
then a chunker-identity comparison, each priced against the other, neither questioning why the
re-chunk was redundant AND necessary at every stage in the first place. The user redirected instead:
chunking becomes an explicit, opt-in `Pipeline.buffer()` call, downstream stages never re-chunk by
default - the "skip a redundant re-chunk" question stopped existing, by construction, once nothing
re-chunks unless asked to. `~/.claude/rules/code.md`'s existing "price fixing the inconsistency
instead of accommodating it" line is sharpened to cover a cost a design just introduced, not only an
inconsistency between siblings.

## 2026-09-06 A review finding was framed as #31's own regression before checking `main`

Building #31, code review found `HttpPipeline.fetch()` reusing `this._context` to serve concurrent
requests, and it was escalated to the user as a new race #31 introduces - a stack-splitting decision
was proposed and an `AskUserQuestion` fired before the base commit was ever checked. Reproducing the
identical probe against `main` (909ea41, zero relation to #31's diff) showed the SAME race on a plain
`ConcurrentPipeline`, pre-existing since #17. `~/.claude/rules/code.md`'s Prove it section now says to
check the base commit before treating a finding as a stop condition.

## 2026-09-06 `replace_symbol_body` left a stale docstring above the new one, twice

Building #31, `mcp__serena__replace_symbol_body` on `Pipeline.context()` and `Pipeline.merge()` each
left the OLD docstring sitting directly above the newly-written one - the tool's own body parameter
doesn't include a preceding JSDoc block, so writing a new docstring inside `body` adds a second one
rather than replacing the first. Code review caught both. `~/.claude/rules/code.md`'s While you work
section now says to re-read after every such call.

## 2026-09-06 A probe counted instances and the ticket claimed invocations

Planning #31 measured `contextFactory` with a spike that counted distinct manager instances SERVING
a chunk - one per worker - and wrote "the factory runs once per process" into `product.md`, the
README, `CLAUDE.md`'s Language and a Done-when case. Counting invocations instead showed each worker
called it TWICE: once in the `Pipeline` constructor for its own `_context`, which a worker never
uses, and once in `.fetch()`. For the user's own `new Pool(...)` example that is two pools per
worker, one dead. Caught by `advisor` after the ticket was already filed; corrected with a real
re-run (`maxBuiltPerWorkerPid` `[2,2]` before the unification, `[1,1]` after) posted as #31's first
comment. `~/.claude/rules/code.md`'s measure-the-right-quantity rule gained the sharpening.

## 2026-09-06 A base-class fix was priced without ever reaching a subclass

Planning #31, `.context()`'s mutate-in-place fix was patched into `src/pipeline.ts`, the full suite
run, and "122 passed, zero broken" put to the user as the price. None of it had touched
`ConcurrentPipeline`, `HttpPipeline` or `ClusterPipeline` - the classes where a context actually
crosses a process. The user had to ask for that verification, and it changed the design: a real
three-worker `cluster` run produced `contextFactory` and the per-process-versus-per-request question
the base class could never have surfaced. `~/.claude/rules/code.md`'s Prove it section now says to
price a base-class change against the leaf furthest from the base.

## 2026-09-06 Two questions in one round presumed each other's answers

Planning #31 asked "does F3 land in this ticket?" and "does this ticket commit to `IContextManager`
as the seam?" in the same `AskUserQuestion` round. The answers - F3 included, and don't settle the
seam here - contradicted, because the second question's options had presumed the first was answered
"sever". A turn was then spent reconciling them. The `plan` skill already says a question resting on
an open decision waits for a later round; nothing said it about two questions inside one round.
`~/.claude/rules/docs.md` now carries both that line and the user's extension: prefer more rounds,
and grill an answer whose grounding does not hold rather than building on it.

## 2026-09-05 A copy-on-write base method dropped a subclass's own knob

Building #17, `Pipeline`'s `createPipeline()` used `this.constructor` so a subclass survived
`.transform()`/`.context()`/`.buffer()`, but the base implementation only carried the FIELDS `Pipeline`
itself knows about. A probe showed `new ConcurrentPipeline([1], {maxConcurrency: 8}).context({}).maxConcurrency`
returning `4`, the default, not `8` - every subclass had to override `createPipeline()` again to carry
its own knobs forward. `~/.claude/rules/code.md`'s Shape section now says so.

## 2026-09-05 `HttpPipeline`/`ClusterPipeline` needed their inherited methods re-declared after all

Building #17, `.transform()`/`.apply()` were deleted from both subclasses reading the ticket's Interface
literally - the runtime call is an unchanged `super.apply()`. Typecheck broke against the canonical
example (`new HttpPipeline(...).transform(...).transform(...).fetch`): TypeScript does not narrow an
inherited method's declared return type to the subclass on its own. Re-added as thin delegations.
`~/.claude/rules/typescript.md` now says a subclass re-declares a method for return-type narrowing
alone, whatever its body does.

## 2026-09-05 A ramp-up loop leaked a promise before any race began

Building #17, `fanOutUnordered`'s ramp-up `for` loop could throw (a failing `iterator.next()`) before
any promise it had already created reached `Promise.race()`, leaving those promises with no rejection
handler - reproduced live, 3 of 3 runs leaked. Fixed by attaching a throwaway `.catch(() => {})` the
moment each promise is created, matching `fanOutOrdered`'s own pattern. `~/.claude/rules/code.md`'s
Fail loud section now covers it.

## 2026-09-05 An HTTP bridge left a client with no response on a bad body

Building #17, `toNodeHandler`'s call to `request.json()` was uncaught: a bodyless or malformed POST to
a stage endpoint never got a response, verified live. Fixed with a validating `parseStageRequest()`
plus a last-resort `.catch()` around the whole bridge, writing a 500 if headers were not yet sent.
`~/.claude/rules/code.md`'s Fail loud section now covers async-to-callback bridges generally.

## 2026-09-05 A background code-review agent was polled with a /loop-mode scheduling tool

Waiting on the #15 `/code-review medium` subagent, `ScheduleWakeup` was called to "check back in 3
minutes" - a tool meant for `/loop` pacing, not for a task the harness already notifies on completion
for. `~/.claude/rules/code.md` already named this exact mistake from two earlier dates; this is its
third occurrence, now stamped onto that rule's own date list rather than left unremarked. The correct
move, used for the rest of the wait, was `ListAgents` once to confirm it was still running, then
stopping the turn and letting the real `<task-notification>` arrive on its own.

## 2026-09-05 A type's new default parameter silently tightened a sibling method it did not touch

Building #15, `ChunkErrorHandler<In, U = void>` moved into `Transformer.onError()`'s signature to
replace the duplicate `handler.ts` declaration it used to take. `errors/handler.ts`'s old type was a
bare-`void`-returning function; the imported one at its `U = void` default is `void[] | void` - a
union, which loses TypeScript's void-return exemption. `pnpm typecheck` on the untouched suite still
passed (every existing `.onError()` handler uses a block body, which returns `undefined` either way),
so the break reached an ordinary expression-body `(chunk, err) => arr.push(err)` caller with no test
covering it, and was caught only by `advisor` naming the exact rule this violated. `~/.claude/rules/
typescript.md` gained the grep-every-sibling-site rule; `Transformer.onError()`'s function arm is now
typed inline as a bare `void`, independent of `ChunkErrorHandler`'s default.

## 2026-09-05 An early-exit loop passed every test because no test registered two handlers

`ErrorHandler.handle()`'s first draft (#15) picked a "winning" return value by returning as soon as
one handler returned non-`undefined`, which also skipped calling every handler after it - a real
regression from the shipped code's "run every handler, ignore what it returns" loop. `pnpm test`
stayed green because no existing test chained two `.onError()` handlers together; `/code-review
medium` caught it (independently, from four of five review angles), and a fix verified by reverting it
and confirming the added regression test actually failed first (`.claude/rules/code.md`'s own
flip-the-value-not-just-delete-the-line rule). `.claude/rules/typescript.md` (repo-local) has no entry
for this shape yet; the general form - a loop rewritten to also compute a return value must keep every
existing SIDE EFFECT unconditional unless the ticket says otherwise - is worth a `~/.claude/rules/
code.md` line if it recurs.

## 2026-09-05 Four spikes argued for designs the user had not asked for

Planning #17, the user asked for "everything implemented as a Pipeline subclass? One for http, one for
cluster?". The reply built a standalone `HttpPipeline` holding a caller-owned stage map - not a subclass
at all - and let a fork's "a WorkerPoolPipeline class is not coherent" verdict, which was about the
worker's far side, kill the second class without ever addressing the subclass question. Corrected, the
next re-pitch proposed `.apply(stages.double)` recognised by reference, which still kept the stage map.
Both fork briefs had been written from a paraphrase rather than the user's own words, so both spikes
returned real output arguing for the wrong shape. `~/.claude/rules/code.md` now says to restate the
user's words as code before writing a spike brief.

## 2026-09-05 A ratio of two total runtimes was reported as overhead

Planning #17 compared `node:cluster` against a worker-thread pool on one CPU-bound job - sequential
77ms, piscina 24ms, cluster 33ms - and reported "roughly 30-40% more overhead" from 33 divided by 24.
Both numbers were mostly the CPU work; the dispatch cost was never isolated. Challenged with "how did
you measure 40%", a proper measurement (identity transform, work removed, paired interleaved rounds
across payload sizes) showed the ranking FLIPS at roughly 7-8 KB per chunk, so the original claim was
wrong in method and in direction. The same session then divided a per-request cost measured at one
payload size by 1000 rows to state a per-row figure, which the measurement does not support.
`~/.claude/rules/code.md`'s existing measure-before-normalising rule gained both sharpenings.

## 2026-09-05 Two counts were repeated from prose instead of being run

Planning #17 carried "94 tests" through several rounds; it came from PR #4's body and the real count is
100, confirmed by `npx vitest run`. The same session claimed `inertKnobsOf` "exists only to catch a
strategy the async-iteration path cannot honor" after reading one line of it - it checks four knobs
(`strategy`, `hooks`, `chunkSize`, `chunker`), so removing the execution seam shrinks it rather than
deleting it. `~/.claude/rules/code.md`'s "a docstring's claim is not evidence" line now covers counts
read from a PR body and scope claims made from one line of a function.

## 2026-09-05 Four rounds were spent on options the reader could not judge

Planning #17 put decisions to the user as priced options without first showing the mechanism they
rested on, and drew "I dont understand your conclusions", "why are we specifically talking about this?"
and two more. The round that landed instead wrote the causal chain first - the user asked for opacity,
so the library calls `cluster.fork()`, so Node re-runs the entry module, so a `db.migrate()` in that
file runs once per worker - and the answer came immediately. `~/.claude/rules/docs.md` now requires the
problem as currently seen, then the causal chain, then the options, each with its own end-to-end
example.

## 2026-09-04 A deferred conditional type never resolves inside its own abstract scope

`Transformer`'s constructor tried gating a mismatched `In`/`Out` with no `transform` via a single
conditional-tuple overload (`In extends Out ? [options?] : never`), verified in isolated probes against
concrete types. It failed at every internal call site still holding `In`/`Out` abstract - the class's own
methods, and even `.catch()`'s `tempTransformer` where both sides of `extends` were the literally same
parameter (`Out extends Out`). TypeScript never distributes a deferred conditional over an unresolved
type parameter. The fix, from a second `advisor` consult: a plain, non-conditional overload for "a real
transform already in hand," ahead of the conditional one, so internal generic sites resolve against the
first. `.claude/rules/typescript.md` now carries the rule.

## 2026-09-04 An array literal widens inside a wrapping generic call, not just at its own site

Ticket #5's `Pipeline.merge` Done-when example assumed `new Pipeline(["a","b"])` (no annotation) would
carry `"a"|"b"` through `Pipeline.merge(...)`'s inferred return type. A real probe
(`tmp/merge-literal-probe.ts`) showed the literal widens to `string` inside `merge`'s own arguments
regardless of the outer assignment's target type - only an explicit annotation at the `Pipeline`
construction site (`new Pipeline<"a" | "b">([...])`) preserves it. `merge`'s own `ElementOf<Ps[number]>`
inference is correct once given explicitly-typed pipelines; reported the discrepancy on the ticket rather
than silently rewriting its example. `.claude/rules/typescript.md` now carries the rule.

## 2026-09-04 A ticket's "spiked and verified" claim missed the one signature that mattered most

Ticket #5's Constraints claimed every Interface signature - the execution-strategy seam, `merge`'s
`ElementOf` inference, the hooks/loop fixes, and `Transformer`'s constructor - "was spiked against real
`src/` during planning, each spike asserting its failure cases with `@ts-expect-error`, and each
returned clean." Re-spiking the constructor's own conditional-tuple design during build (per
`~/.claude/rules/typescript.md`'s existing "extract a distributing conditional" rule) surfaced a real
gap: a conditional type gating a constructor's OWN parameter never resolves inside any generic scope
still holding its type parameters abstract, even where both sides of `extends` are the literally same
parameter (`Out extends Out` inside `.catch()`'s own `tempTransformer` construction still failed) - a
case the ticket's blanket "spiked" claim did not actually cover. The fix (a plain, non-conditional
overload for "a real transform already in hand," ahead of the conditional one) came from a second
`advisor` consult, not the ticket text. `~/.claude/rules/issues.md` now says a ticket's own
verification claim is re-run as a probe during build, never trusted at face value.

## 2026-09-04 A spike's rejected rows were the design space, not settled negatives

Planning #5, a spike enumerated five ways to author an execution strategy and asserted two of them
errors: an unconstructed class, and a bare `async function*`. Both were reported as costs of the
recommended shape, and a round closed on that shape. The next message reversed it and chose the bare
`async function*` - the row the spike had drawn and dismissed. A probe that enumerates forms has
enumerated the design space, and its rejected rows are where a preference most often sits. Produced
`~/.claude/rules/code.md`'s "Present every authoring form a probe rejected as a design option before
pricing it as a cost" (2026-09-04).

## 2026-09-04 The ticket was filed before the repo was swept

Planning #5 swept `src/`, `.claude/` and `__tests__/` for every name the seam change renames, filed
the ticket, then found five more stale `.withExecutor` call sites in `README.md` while editing the
docs afterwards. The ticket needed an edit it should never have needed. The same pass also found
`ts-pattern` declared as a runtime dependency, credited in `architecture.md` with "strategy-spec
matching", and imported nowhere in `src/`. Sharpened `~/.claude/rules/issues.md`'s existing
planning-time-file-list line to say the sweep runs BEFORE filing and covers README and consumer docs,
and produced `~/.claude/rules/code.md`'s "Grep for a dependency's import before repeating a doc's
claim about the role it plays" (2026-09-04).

## 2026-09-04 The package outgrew a callback-arity split before it outgrew a monorepo

`outputty/laygo` carried this package as `packages/pipeline` inside its own pnpm workspace from the
start, coupled by nothing but co-location - no import edge either direction (laygo's `Source` reaches
any `AsyncIterable` structurally). The coupling that actually needed fixing first was internal:
terminal ops (`toArray`/`first`/`consume`/`forEach`/`branch`) returned a `[data, context]` tuple every
caller had to destructure, which made a context read a positional-return contract rather than a normal
property read. `outputty/laygo` #743 dropped the tuple in favor of `.contextManager` read directly off
the resolved `Pipeline` instance; #744 sourced every caller and test onto the new shape. Only once that
internal shape was settled did #745 split the package out into this repository and flatten laygo itself
down to one package - splitting a coupling that was co-location-only, before the API shape underneath
it had settled, would have meant redoing the split's own boilerplate (package.json, CI, `.claude/`
docs) a second time. See `outputty/laygo`'s own `.claude/lessons.md` (2026-09-04) for the pnpm
workspace-boundary-marker mistake made during the flatten half of that same ticket.
