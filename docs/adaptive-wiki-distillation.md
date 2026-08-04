# Adaptive Wiki Distillation Rules

How to turn project documents into durable wiki knowledge. These rules govern
both authoring (`forager offdesk wiki record-candidate`) and review (an operator
or a separate reviewing agent deciding promote, compress, rescope, or reject).

The wiki is not a mirror of the docs. The source docs stay the source of truth.
The wiki carries the compact, non-obvious, projectable knowledge an agent should
hold in context so it does not repeat a known mistake. If an item is already a
stable line in an authored doc and adds nothing when injected as an instruction,
it does not belong in the wiki.

## Why compression matters (measured)

The AI projection is budgeted: roughly 8 entries, ~4000 estimated context
characters, and ~500 characters per instruction. Distillation is not cosmetic.
An A/B test on the TwinPaper wiki (12 verbose entries vs 10 distilled) cut
injected context by 18 percent in analysis mode and 40 percent in development
mode, with the same scope discipline and no loss of the non-obvious knowledge.
The verbose version was already at the 8-entry budget cap; the distilled version
left headroom. Compression is what keeps the most important knowledge in budget
as the wiki grows.

## Keep (high projection value)

- Non-obvious gotchas that prevent a real mistake ("cohort denominator is 822,
  840 is legacy only"; "figure outputs_svg must be []").
- Authority and boundary rules an agent could plausibly violate ("the frontend
  must not recompute ranking in the browser"; "approval before runtime mutation").
- Durable domain facts an agent needs in context (outcome variables, canonical
  data sources and IDs, analysis window).
- Methodology rules that shape correct work (pair fixed effects with within-pair
  variables only; start a direction review from the latest baseline).

## Prune (low value or harmful)

- Doc mirrors: near-verbatim restatements of a stable source-of-truth doc. The
  doc already carries it, and the wiki copy goes stale when the doc changes.
- Generic best practice that is already universal ("run tests before finishing",
  "state over scrollback") unless it is project-specific and non-obvious.
- Operational trivia unlikely to change the agent's task (specific legacy-path
  blocklists), unless the risk of getting it wrong is high.
- Anything better served by a link to the doc than by a projected instruction.

## Compress

- `claim`: one durable statement, roughly <= 120 characters. Drop throat-clearing
  ("TwinPaper work must ..."), the project name (scope already carries it),
  hedges, and narrative.
- `ai_instruction`: imperative and actionable, roughly <= 200 characters. Say
  what to do or not do; do not restate the claim.
- Prefer one precise sentence over a paragraph. If it needs a paragraph, it is
  probably two entries or belongs in the doc.

## Environment and compute gotchas (highest-value class)

Settings that silently break large computations are the wiki's highest-value
entries, because they live nowhere else: the workaround lands in code (a pinned
`PRAGMA threads=8`, an `OMP_NUM_THREADS=1`, a batch-size cap) while the reason
evaporates with the debugging session, and the mistake recurs months later.

Capture them at the moment of resolution, with all three parts:

1. the exact failure (error text, OOM/kill, wrong result, hang);
2. the working setting (verbatim: pragma, env var, flag, version pin);
3. why -- what about the machine or data triggers it (core count vs memory,
   dataset scale, thread oversubscription between libraries).

Conventions: kind `failure_pattern` (the breakage) or `procedure` (the working
recipe); facet `ops`; scope `user_global` when the constraint is the machine's
(shared server, GPU box), `project` when it is the dataset's; tags
`tool/<duckdb|torch|sklearn|...>` plus `env/<host>` so one server's constraints
are queryable across projects. A code fossil (a pinned setting with no comment)
is capturable evidence -- record it with `src:<file>:<line>` refs and a review
note asking the operator to confirm the original failure mode.

## Constraints (measured failure modes)

The first batch harvest (60 sessions -> 364 machine candidates -> tier-2/3
review) measured how local-model distillation actually fails. These are now
hard constraints, enforced mechanically where possible:

1. **One claim per evidence quote.** The dominant noise source was fan-out:
   one quote inflated into up to seven near-duplicate claims. The distillers
   reject a second candidate reusing the same normalized quote in a run.
2. **A reference is not a quote.** Pointer strings (`chat:...#incident3`,
   `#ex19`) appeared in the quote field and passed existence checks while
   carrying no evidence. Quotes that look like refs are rejected mechanically,
   and the pre-review marks candidates with no stored quote `unclear` instead
   of falling back to the evidence ref.
3. **Contentless speech grounds nothing.** Approvals ("좋아 진행하자"),
   rhetorical questions, and opinion-seeking prompts cannot ground a
   methodology claim; models attached invented rules to them. If the quote
   does not itself carry the lesson, do not emit.
4. **Preserve gap polarity.** A statement that something does NOT exist ("no
   independent human validation") must stay a gap; models recast gaps as
   established procedures, which teaches agents the opposite of the truth.
5. **Status narration is not knowledge.** "Running X", "completed Y",
   mid-investigation speculation, and one-off task steps are rejected.
6. **Rubrics carry no example claims.** Small models parrot a rubric's example
   sentence into unrelated evidence: the same example claim surfaced verbatim
   in three different projects' harvests before the example was removed.
   Describe the desired shape abstractly; never show a complete claim.

## Classify

- `kind`: preference, procedure, failure_pattern, policy_rule, or fact. Pick the
  one that matches how the knowledge is used, not its tone.
- `scope`: project (project-specific), user_global (cross-project), or
  artifact_kind. Scope drives runtime projection matching.
- `facet` (tag `facet/<x>`): research or product (the substance) vs ops (how to
  run and operate). A research project skews research; a software app skews
  product; the tool's own profile is all ops.
- tags: `domain/<project>` plus a topical tag when useful (`risk/...`,
  `method/...`, `harness/<area>`).

## Evidence

- Every entry cites a verifiable source: `doc:<path> (Section Name)`. Do not
  invent anchors that do not resolve.
- Prefer one or two precise refs over many vague ones.

## Review gate

Authoring never self-promotes. Candidates are authored with these rules, then a
separate reviewer (the operator, or a reviewing agent) decides per candidate:
promote (with activation mode and scope), compress, rescope, merge, or reject.
This keeps observation separate from the decision to trust knowledge, and it is
where promotion precision is actually validated.

Review is tiered by cost, cheapest first:

1. **Operator packet** (tier 1): `scripts/offdesk_wiki_prereview.py --packet`
   emits a sorted markdown packet with pre-filter flags and apply commands; ten
   quoted claims read in two or three minutes, making the operator the cheapest
   high-quality reviewer.
2. **Local pre-filter** (tier 2): the same script has a local model judge each
   candidate's claim against its stored evidence quote
   (supported / inverted / unsupported / unclear). In live testing it caught a
   planted inversion for ~2s and ~1k local tokens -- three orders of magnitude
   cheaper than an agent Council -- with occasional false positives on compound
   sentences, which is why flags stay advisory.
2b. **Model-family council** (tier 2, `offdesk_wiki_prereview.py --council`):
   judge each candidate with several distinct model FAMILIES (default
   qwen3-coder:30b, gemma4:26b, gpt-oss:120b on the local GPU server) and
   aggregate votes. Unanimous `supported` passes; any dissent surfaces as a
   council-split verdict carrying per-model votes, taking the most severe
   verdict. Rationale: family-specific failure modes (claim inversion,
   rubric parroting) rarely repeat across families, so diversity converts
   silent single-judge errors into visible splits at zero API cost.
   Validated on seeded true/inverted/overreaching claims: both inversions
   (including the historical localhost-preference inversion) were caught
   2-of-3 with the third abstaining.

3. **Agent Council** (tier 3): reserve multi-agent review for contested or
   high-stakes sets.

Review can be a single agent or a Council of independent reviewers with distinct
lenses whose verdicts are synthesized into a consensus. Three lenses have proven
useful and catch different defects:

- accuracy and evidence: verify each claim against the cited source (and, for a
  tool wiki, against the code); flag drift and unresolved refs.
- value and distillation: is it non-obvious and durable, or a doc-mirror or
  generic best-practice that should be pruned or compressed.
- classification and scope: is the kind, scope, facet, tags, and agent-mode
  projection correct.

The value lens reliably catches the doc-mirrors; the classification lens catches
scope and agent-mode mistakes a value-only review misses.

Apply the verdicts in place, without reject and re-record:

- `compress` -> `forager offdesk wiki edit <id> --claim <shorter> [--ai-instruction <text>]`
- evidence fix -> `forager offdesk wiki edit <id> --evidence-ref <ref>`
- retag / classify -> `forager offdesk wiki add-tag <id> --core-tag facet/<x>`
- `rescope` -> `forager offdesk wiki rescope <id> --scope <scope> --scope-ref <ref>`
- `reject` -> `forager offdesk wiki reject <id> --reason <text>`
- `promote` -> `forager offdesk wiki promote <id> --activation-mode <mode>`

Each mutation appends an audit record, so the review decision stays traceable.

## Continuous distillation with a local LLM

`scripts/offdesk_wiki_distiller.py` runs this contract continuously with a
local Ollama-compatible model (cost ~0, data stays on the machine):

```bash
OFFDESK_LLM_BASE_URL=http://<gpu-server>:11434 \
OFFDESK_LLM_MODEL=qwen3-coder:30b \
scripts/offdesk_wiki_distiller.py --doc <PROJECT>/AGENTS.md \
  --profile <project> --scope-ref <project> [--record]
```

The rubric above is the model's prompt contract. Two mechanical safety valves
make a small model trustworthy enough to author candidates:

- every candidate must carry a verbatim evidence quote that is verified
  (whitespace-normalized substring) against the source document, so
  hallucinated provenance cannot enter the store. Small models often extract a
  TRUE fact but paraphrase the quote, which would silently discard good
  knowledge (a live run lost 3 of 8 true candidates this way); a non-matching
  quote is therefore fuzzy-matched against actual document lines and replaced
  with the best verbatim original when similarity is high. Because the
  replacement is copied out of the document by the tool, the guarantee is
  preserved -- only genuinely unsupported quotes still reject, and a rejection
  now actually signals an unsupported claim;
- output is validated (kind/facet enums, claim length cap, per-run candidate
  cap, in-run claim de-dup) and truncated model responses are salvaged down to
  the last complete candidate.

The distiller NEVER promotes: `--record` writes candidates with
`origin=background_review` and `confidence=inferred` through the same
`record-candidate` path, and they wait in the normal review queue. Default is
a dry run. In live testing qwen3-coder:30b yielded noticeably more verified
candidates than gemma4:26b on the same document, with the quote check catching
its paraphrased evidence.

## Continuous operation (nightly recipe)

The pipeline composes into an unattended loop that still keeps promotion
operator-gated. A nightly cron (or systemd timer) per project:

```bash
# 1) capture: docs + latest session transcript -> unpromoted candidates
export OFFDESK_LLM_BASE_URL=http://<gpu-server>:11434 OFFDESK_LLM_MODEL=qwen3-coder:30b
scripts/offdesk_wiki_distiller.py --doc <PROJECT>/AGENTS.md \
  --profile <project> --scope-ref <project> --record
scripts/offdesk_wiki_session_distiller.py \
  --transcript "$(ls -t ~/.claude/projects/<proj-dir>/*.jsonl | head -1)" \
  --profile <project> --scope-ref <project> --scope project --record

# 2) pre-filter + operator packet (tier 1+2)
scripts/offdesk_wiki_prereview.py --profile <project> \
  --packet <out>/wiki-review-packet.md
```

Morning review stays explicit: the operator reads the packet and applies
promote/reject; occurrence merging absorbs re-observed candidates across
nights, so re-running is idempotent in effect. Nothing in the loop promotes.

## Review rubric (for the reviewing agent)

For each candidate, decide a verdict and give a one-line reason:

1. Durable and non-obvious? (If it is a doc mirror or generic, lean reject.)
2. Accurate to the cited source?
3. Right kind, scope, and facet?
4. Is the claim tight and the instruction actionable? (If long, verdict
   `compress` with a proposed shorter claim.)
5. Does the evidence ref resolve?

Verdicts: `promote:<activation>` | `compress` | `rescope:<scope>` | `merge:<id>`
| `reject`, each with a short reason.
