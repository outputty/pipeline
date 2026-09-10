# Lessons

The mistakes made building this product, recorded so they are not repeated. `retro` appends entries at
the end of every planning session and inside every build's docs layer.

- A lesson remembers; the rule, skill or doc change it produced enforces. Every entry links that change.
- An entry is one paragraph; the incident's detail stays in the session.
- Newest first. Development context lives here and in the tracker, never in `product.md`.

## 2026-09-10 Built `.branch()`'s demux against the opposite of #90's own invariant

#90 exists so a synchronous chain creates zero promises, and the same ticket had already shipped
`chain`/`settleMaybe`/`mapSettle` to keep that. The `.branch()` demux prototype was written `async`
throughout with `Promise.all` on every arm anyway. It ran and returned correct values, which is why
nothing in the build caught it; the user did - "if all of them are synchronous, we should not be
awaiting anything… even then, we should not await what should not be awaited." Rebuilt on those
helpers, the branch measures 0 promises all-sync and `["Promise","array"]` with one async arm.
Recorded by sharpening `~/.claude/rules/code.md`'s **Reuse, before writing** ladder: a spike or
prototype climbs the same ladder, and its nearest rung is what THIS ticket already shipped.

## 2026-09-10 An ordering probe that proved nothing, because four items are one chunk

Asked whether order survives `.branch()`'s rejoin, the probe ran four items through and printed them
in order. `DEFAULT_CHUNK_SIZE` is 1000, so all four were ONE chunk and nothing could reorder - the
probe would have passed against a mechanism with no ordering at all. Re-run with `.buffer(1)` before
the stage, the same four came back `["item-2","item-3","item-4","item-1"]`. The repo already held a
line about this default hiding CONCURRENCY; it now covers every chunk-granular observable, in
`.claude/rules/typescript.md`.

## 2026-09-09 A typecheck-only spike was narrated in the scratch file as an executed run

Planning #90, `tmp/level3b-seam-spike.ts` was passed through `tsc --strict` alone, then the scratch
file recorded it as having "run correctly" and showing `[6, 8, 10]` as its output - a predicted
value written as an observed one. Caught by `advisor`, not self-caught: no `node`/`tsx` invocation
of that file exists anywhere in this session's tool history. Fixed by striking the claim and
re-running the actual sync engine for real (`tmp/sync-engine-bench.mjs`), producing the genuine
numbers that replaced it. `~/.claude/rules/code.md`'s existing "run it before relying on it" line
gains a sub-bullet: a claim that code "ran" or "produced" an output needs a real execution, never a
type-check alone.

## 2026-09-09 Verified README/examples.md fences by hand, then Prettier's format gate rewrote them

Building #84 (README/examples.md docs pass), 8 new runnable Pattern fences plus the Merging example
were hand-verified with real `node --import tsx` runs before `pnpm check` ever ran. `pnpm check`
then failed at `prettier --check .` on `README.md`; `prettier --write` (embedded-language formatting
reformats recognized languages inside markdown fences) collapsed one chain from three lines to one
and touched several other blocks, so every block Prettier rewrote needed a second, identical
verification pass for no reason but sequencing. Recorded as `~/.claude/rules/code.md`: "Run the
repo's format and lint step over edited files before hand-verifying their behavior, never after - an
auto-fixer (a formatter's embedded-code rewrite, a lint `--fix`) can rewrite the exact bytes just
verified, forcing a second verification pass."

## 2026-09-08 A rejected `AskUserQuestion` got two "still waiting" replies before switching to stated assumptions

Building #78, an `AskUserQuestion` about two ticket-scoping ambiguities came back rejected -
"the user doesn't want to proceed with this tool use" - inside an autonomous `/goal` session with no
interactive user watching. The next two `Stop hook feedback` re-invocations each got a plain "I'm
waiting on your answer" / "Still waiting on your answer" reply, adding no new information either
time, before the third one finally switched to stating both assumptions plainly and continuing the
build. The rejection itself was already the signal; the harness's own note ("no human input has
been received... any statement that the user said, approved, or confirmed something... is NOT real
user input") applied from the first re-invocation on. Recorded as `~/.claude/rules/code.md`:
"When `AskUserQuestion` comes back rejected inside a `/goal`-driven or otherwise unattended session,
treat the rejection itself as the signal that no interactive answer is coming."

## 2026-09-08 A pre-existing-ness check was already settled by `git log`, then re-checked with a stash anyway

Building #72, `/code-review medium` flagged a mutable-`initial` bug in `ConcurrentPipeline.reduce()` -
`git log`/`git show` on the diff's own two commits already confirmed neither touched `reduce()` or
`src/utils/reduce.ts`, settling "pre-existing" outright. A `git stash push -u` to "get a clean base"
followed anyway, reverting six staged doc edits and costing a full stash-recovery dance (capture SHA
by tag, apply, drop) to get them back - self-caught one tool call later from the harness's own
file-change reminders. `~/.claude/rules/code.md`'s existing base-commit-reproduction rule gains a
sub-bullet: once `--stat` confirms a finding's file is untouched, reproduce directly against the
current tree, no isolation needed.

## 2026-09-04 A shared knob name is not a shared quantity

Planning #11's cross-runtime benchmark, four priced ways to make a "concurrency 10" column fair
across `Pipeline` and five comparators were offered before checking that the 10 meant the same thing
in each. It did not: this package bounds CHUNKS, so its in-flight count is the buffer size times
`maxConcurrency`, while every comparator bounds items. The user stopped it - "Pipeline by definition
operates on chunks" - and measuring settled it: peak in-flight is the product exactly, coprime
factors included (`7x11` peaked at 77, re-measured on `ConcurrentPipeline` 2026-09-08). The fix was
not a fairness rule but a different controlled variable: measured items in flight, which every leg
reaches through its own knob and which the harness asserts before recording a time. A buffer of 1,
the normalisation all four options rested on, turned out never to be needed - `.buffer(16)` with
`maxConcurrency: 1` hits the same 16 with chunking intact. Two rules came out of it, both in
`~/.claude/rules/code.md`: measure that a number means the same in both implementations before
pricing options that normalise it, and control a zero-hit search against a term the SAME package is
known to contain (`ix` lacking bounded concurrency had been "proved" from a grep controlled on a
sibling package; `ix` has `flatMap(selector, concurrent?)`). The rule now lives as product truth in
`.claude/product.md`'s "Where the work runs" section, under **Items in flight**.

## 2026-09-08 A sibling knob's known defect was cited as fact for a different knob

Planning #78 objected to a chain-wide row handler on the grounds that `pipe()` drops it "like
`hooks`" - the defect #72 is deleting the old lifecycle-hooks knob for. `pipe()` carries
`errorHandler` forward deliberately, so `.onError()` was already position-independent, measured
immediately afterwards (`S1a onError BEFORE map -> fired 1x`, `S1b onError AFTER map -> fired 1x`).
The wrong objection shaped a whole question round and the user had to push back to get it corrected.
`~/.claude/rules/code.md` gains a line: read the method for the sibling knob by name before citing
one knob's defect as another's.

## 2026-09-08 A probe measured a compile-time guarantee on a concrete type the design site never has

The ts-pattern exhaustiveness probe for #78's `DROP` sentinel used a concrete `number | typeof DROP`
and reported both a working `.exhaustive()` and a 62 ns/row price. The three real sites are generic
in `U`, where `.exhaustive()` is not callable at all - `TS2349 … NonExhaustiveError<unknown>` with
every arm present - and the shape that does compile costs 254.4 ns/row. The user picked on the
62 ns number before the re-probe caught it. `~/.claude/rules/code.md`'s 2026-09-07 probe-shape line
is sharpened: a probe carries the design site's own TYPE PARAMETERS, not only its member placement.

## 2026-09-08 An option's label promised `.exhaustive()` while its preview showed `.otherwise()`

The same round's recommended option was labelled "Every DROP site, .exhaustive()" and previewed
`match(r).with(DROP, …).otherwise(…)` - a different mechanism with no compile-time guarantee at all.
The user picked on the label. `.claude/rules/docs.md` is created with one line: an option's preview
holds the literal code its label names.

## 2026-09-08 A diff was measured once, under the single-PR threshold, then trusted after it grew

#40 measured its diff before the first commit, under the single-PR threshold at that moment; a
`/code-review medium` round then added fixes that pushed it past the threshold, caught only because
the diff was measured again on request. `~/.claude/output-styles/outputty.md`'s Language section
now forbids measuring or stating a line/character/token count as a fact anywhere - it is false the
moment anything else changes the work - so a size decision names the check it passed, never a
number.

## 2026-09-08 A ticket's "move it, don't duplicate" covered one path, and the move dropped the rest

#40's own Interface moved a chunk-failure report from `Transformer.process()`'s loop-scope catch
into `runSequentially`'s per-chunk one; the ticket's own worked example only ever exercised the
chunk-failure path, so the move silently dropped the one case that never reaches that loop at all (a
lifecycle-hook throwing before any chunk runs) - caught by `/code-review medium`, not by the
ticket's own example. `.claude/rules/code.md` gains a line: the same call-site grep is owed before a
move, not only a deletion.

## 2026-09-08 "Neither overrides apply()" was written after already reading two overrides that do

`HttpPipeline.apply()` and `ClusterPipeline.apply()` were both read this session, each a real
`override apply()` narrowing the return type and delegating to `super.apply()` unchanged - the docs
pass still wrote "neither overrides `apply()`" in `CLAUDE.md` and `.claude/roadmap.md`, from memory
rather than the literal declaration, caught by `/code-review medium` rather than a re-read.
`.claude/rules/code.md` gains a line: re-check a method's literal override status right before
writing a claim about it.

## 2026-09-07 A "runs in the worker" claim reached four files before anything measured it

Planning #72 measured that `Pipeline.tap` runs in the orchestrator, over a real loopback
`HttpPipeline`. The sibling claim - that `Transformer.tap` inside a dispatched `.transform()` runs in
the WORKER - was derived by reading how `HttpPipeline.fetch` serves `_chunkTransforms[requested]`,
and written as fact into `product.md`, `CLAUDE.md`, `README.md` and the ticket body. It held when
finally measured (`callerSaw []`, `workerSaw [2,4,6,8,10]`), but it was unverified through the whole
docs pass. `~/.claude/rules/code.md` gains a rule: measure where code runs, never derive it from the
dispatch path, and one caller's measured placement settles that caller alone.

## 2026-09-07 A spike declared a base method on subclasses and blamed the design for the errors

The `Pipeline.tap` spike put `tap` on `TappableConcurrent`/`TappableHttp` while the design puts it on
the base `Pipeline`. `tsc` returned six errors - three `TS2339: Property 'tap' does not exist on type
'ConcurrentPipeline<number>'` plus knock-on inference failures - all artifacts of the stand-in. Every
one vanished when the method moved to `Pipeline.prototype`. `~/.claude/rules/code.md`'s probe-shape
rule gains its inverse: declare the member at the level the design puts it.

## 2026-09-07 #62's enable layer's `/code-review --fix` edited three docs-layer files

`code-review medium --fix`, scoped to the enable layer's own diff (`concurrent.ts` + the test file),
also rewrote `README.md`/`.claude/architecture.md`/`.claude/product.md` to fix a doc claim the code
change made stale - correct content, wrong PR, since the docs layer already owned those files in the
posted plan. Caught by reading `git status --porcelain` after the review returned and reverting the
three files before committing; the docs layer reproduced the identical fix as part of its own pass.
`~/.claude/rules/issues.md` gets a new rule: diff review `--fix` output against the current layer's
own planned file list before staging.

## 2026-09-07 A sixth `ScheduleWakeup` call waiting on a backgrounded `/code-review`

The identical mistake `.claude/lessons.md`'s own 2026-09-07 entry (from #61's build) already logged
- calling `ScheduleWakeup` to wait on a backgrounded `/code-review` subagent - recurred in this
session, which had not read that entry before making the call. Self-caught one message later, same
as the fifth time. `~/.claude/rules/code.md`'s existing rule is sharpened rather than given a sixth
enumerated trigger: the general form (any "come back later" tool reached for while background work
is pending) is now named as the trigger, not the specific tool or argument shape.

## 2026-09-07 #62's own shipped combine-debt throw was reopened mid-review without checking for prior art

The user's plain rejection of the throw ("I don't want it to throw") led straight into redesigning
it from scratch, without re-checking `~/.claude/skills/partitioned-folds/` - a domain skill that
existed two hours before this build's own L1 commit (so it was present at build start, when Orient
step 2 says to load it) and already named "a combine expressed as a second reduce stage" as its own
recommended pattern, which would have settled the "keep or remove the throw" question immediately.
Unverified whether the skill was loaded and its Patterns list skimmed past, or not loaded at all -
either way, the pattern it already recorded wasn't applied at the point the throw was reopened.
`~/.claude/skills/build/SKILL.md`'s Orient step 2 is sharpened to name reading the skill's own
`## Patterns` list as part of loading it, not just skimming it once; `partitioned-folds/SKILL.md`'s
own Pattern and Rule entries are marked with which one #62 shipped, so the next session's load finds
the answer already recorded instead of re-deriving it.

## 2026-09-07 A test's own tolerant `.local()` call masked test-isolation leakage that a stricter check surfaced

Several `reduce.e2e.test.ts` cases relied on `PIPELINE_PARTITIONED_REDUCE=1` still being set from a
sibling test's own setup rather than setting it themselves - harmless under a hand-written
`.local()` fold, which never checked anything and just re-folded regardless of which code path ran.
A stricter combine-debt check (since deleted, see the next entry) turned two of them into real
failures the moment it was tried. The bug was already there; the looser call had been silently
absorbing it - every affected test now sets its own flag rather than relying on ambient state.

## 2026-09-07 The same combine-debt mechanism was reopened three times in one session before it was deleted outright

The shipped throw (Done-when 4/6) became a `.combine()` sugar method, then a phantom compile-time
type layer (mid-build, never committed), then was deleted entirely - three real reversals of the
same mechanism, each time after fresh research (an expert skill, an independent multi-agent verdict)
recommended KEEPING some form of it. The user's own direction won every round; the research was not
wrong, it was answering a question the user had already closed differently. Past the second reversal
of the identical mechanism, re-litigating it with new evidence costs a round-trip the user's own
repeated, consistent statement had already settled - confirm the new shape once and build it instead.
`~/.claude/rules/code.md` gains a rule: when a shipped mechanism's value is questioned a second time,
price deleting it before designing another layer to protect it.

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

## 2026-09-06 Four mechanisms were designed to accommodate defects, then deleted

Planning #37, four knobs appeared to work on some `Pipeline` classes and not others, so the
conformance suite grew a Tier A / Tier B split, a `runsOn(subject)` predicate, three separate case
lists and a 5x3 permutation grid to route around them. Every one of the four was a defect:
the lifecycle-hooks knob drops silently through `pipe()`, `setChunker` and `onError` are refused on
a dispatched stage only because `apply()` never consults them, and `merge` demotes to a base
`Pipeline`. The user's
one-line standard - all pipeline classes support all functionality - dissolved three of the four
mechanisms at once, leaving one case list and five wrapper files. Produced
`~/.claude/rules/code.md`'s "price fixing the inconsistency instead" line.

## 2026-09-06 A merge spike varied one parameter four ways and missed the shape

`Pipeline.merge()` returns a plain `Pipeline` whatever went in, so four candidates were spiked in
parallel. All four answered "what class should the STATIC return" - first pipeline's, refuse mixed,
return a source, name it explicitly - because the brief inherited "static" from the question as
framed. Two produced wrong output from a stage-index collision. The user's own proposal, `merge` as an
INSTANCE method, beat all four and was never in the set: the merged pipeline keeps `this`'s
`_chunkTransforms`, so stage numbering continues instead of restarting at 0 and colliding. Produced
`~/.claude/rules/code.md`'s "a candidate set varies the SHAPE of the seam" sub-bullet.

## 2026-09-06 A prerequisite shipped mid-session and the plan was built on a stale base

#31 merged in four PRs while #37 was being planned. Every spike ran on `909ea41`, five ticket bodies
were drafted against it, and the staleness only surfaced at filing time when #31 was absent from the
open-issue list - after blockers had already been set pointing at a closed ticket. The branch was
rebased and all three load-bearing premises re-verified against the new `main` before the bodies were
corrected. Produced `~/.claude/rules/issues.md`'s "re-fetch before FILING" line.

## 2026-09-06 Project vocabulary was used in questions without ever defining it

Three rounds asked where the lifecycle-hooks knob and `setChunker` should live, and what should test
them, without once showing what either knob does. The user twice asked what the point of the whole
mechanism was, asked separately what `setChunker` did, and said "I dont understand this" twice. A
single worked example - nine sensor readings in
groups of 3, 2 and 4, where a fixed chunk size gives `b` a total of 130 instead of 30 - answered the
question the three rounds had been circling. Produced `~/.claude/rules/docs.md`'s "a question about an
EXISTING knob opens with what that knob does" line.

## 2026-09-06 A message string was asserted, and its divergence called a design blocker

A conformance case for `onError` asserted the error message, which reads `"boom on 3"` on a base
`Pipeline` and `"stage 0 at http://localhost:53529 failed: boom on 3"` on an `HttpPipeline` - carrying
a port that changes every run. That divergence was presented as blocking `onError` from the shared
contract, and a change to the error wrapping was priced to fix it. The user pointed out the contract
is that an error occurred and what it carried, not how it reads: the case asserts the failing chunk
and the rejection, and holds on every class unchanged. Produced `~/.claude/rules/code.md`'s "check
whether the diverging DETAIL is part of the contract at all" line.

## 2026-09-06 Two subagents edited the parent worktree through Serena

Four spike agents ran under `isolation: "worktree"`. Two of them used Serena, whose project root
resolved to the PARENT worktree rather than their own, and edited `src/pipeline.ts` there - inside the
planning session's live tree. Both detected it and restored the file; the parent's
`git status --porcelain` and `git diff --stat HEAD` were verified empty twice. Produced
`~/.claude/rules/code.md`'s "a worktree-isolated subagent edits with the built-in `Edit` tool, never
Serena" line.

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

## 2026-09-09 A batch find-and-replace reported success and changed nothing

`sd 'this: M extends "unset" \? never : Pipeline<T, "async", "async">,\n' '' src/pipelines/http.ts
src/pipelines/cluster.ts` exited 0 and edited neither file - the pattern did not match across the
newline the way the call assumed. Both dead `this` guards stayed, `pnpm check` stayed green because
no test chained two type-changing `.transform()` calls on those classes, and the deletion was
reported as done in a build message. Only `/code-review` found them, one layer later, by which point
they had also started breaking a second type-changing `.transform()` with `TS2684`. Produced
`~/.claude/rules/code.md`'s "grep for the old pattern after a batch find-and-replace, before
trusting it ran" (2026-09-09).

## 2026-09-09 A bulk mechanical migration is a pipeline, not an edit

Deleting `.from()` meant moving about 170 call sites across 18 test files. Six automated passes were
written, and each found a real defect in the one before it: comments and string literals edited,
variable tracking leaking across `it()` blocks, the input inserted after the variable rather than
before the terminal, a depth-blind backward scan stopping inside `new Pipeline({ context })`,
`.branch()` treated as a terminal when it returns a runner, and `Array.from` matched as a pipeline's
own `.from` - that last turned `const items = Array.from({ length: 30 }, ...)` into `const items =
Array` in four files. The scope leak is the one that mattered: it compiled cleanly wherever two
tests happened to name their source the same, so `tsc` could not have caught it. What made the sixth
pass affordable was `tmp/run-migration.sh` - reset to a fixed base commit, then every pass in order -
so a fix cost one command rather than a redo of the four passes before it. Produced
`~/.claude/rules/code.md`'s "drive a bulk mechanical migration from a fixed base through one
re-runnable script" (2026-09-09).
