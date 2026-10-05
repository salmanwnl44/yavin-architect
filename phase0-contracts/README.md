# Yavin Architect — Phase 0 Contracts (FROZEN v1.2)

These five schemas are the frozen interfaces everything in Phase 1 builds against.
They are the machine-readable form of the architecture spec; nothing in them is new —
each file implements a section of the spec, and the spec wins on any conflict until sign-off.

| File | Governs | Spec section |
| --- | --- | --- |
| `ledger_events.schema.json` | Every event in the append-only project ledger (the truth plane) | §5-P2, §6, §7 |
| `claim.schema.json` | The atomic unit of knowledge: typed claims with evidence, conditions, taint, bitemporal stamps | §7 |
| `system_model.schema.json` | The typed, versioned System Model the IDE canvas edits (components, flows, SLOs, trust boundaries, FMEA) | §11 |
| `agent_protocol.schema.json` | Every message between the Harness and its stateless agent workers | §12 |
| `check_catalog.schema.json` | The verification check catalog (L0–L5) as versioned data | §14 |

## Status and versioning rules

- **Status: FROZEN v1.2** — signed off by the owner. `CHANGELOG.md` lists what changed from
  the v0.1 draft (v1.0), from v1.0 (v1.1) and from v1.1 (v1.2). v1.1 and v1.2 are minor
  versions: they only add optional fields, new event types and documented rules, so every
  document valid under v1.0 or v1.1 is valid under v1.2, and the schema `$id`s keep the `/v1/`
  path.
- A frozen contract never changes in place. A change is a **new version** plus a
  migration event in the ledger (the §5-P2 discipline applied to the contracts themselves).
- v1.0 keeps each file **self-contained** (shared enums like `EpistemicStatus`, `Severity`,
  `Taint` are duplicated per file) so each schema validates standalone. A later version
  consolidates them behind `$ref`s to a common schema once tooling is in place.
- Cross-schema references (e.g. a ledger event carrying a claim) are **by convention** in
  v1.0 (`"type": "object"` + description naming the governing schema); `fixture/replay.py`
  and the Arbiter deep-validate them. A later version makes them mechanical `$ref`s.
- **`format` is an assertion.** `date-time` and `date` values must be valid; `validate.py`,
  `fixture/replay.py` and the Arbiter all validate with a format checker. `date-time` needs
  the `rfc3339-validator` package, and both scripts fail when it is not installed.

## Rules the Arbiter enforces beyond the schemas

JSON Schema cannot express these, so the schema descriptions state them and the Arbiter
refuses events that break them.

- **`prev_hash`.** The lowercase hex SHA-256 of the previous committed event in the same
  project, computed over that event's canonical JSON: the whole event object as committed
  (its own `prev_hash` included), keys sorted, separators `,` and `:`, non-ASCII characters
  not escaped, encoded as UTF-8. The first event of a project (`seq` 0) omits the field.
- **`model.version_created`.** Without `parent` it is the genesis version and is allowed only
  while the project has no head (`DUPLICATE_GENESIS`). With `parent`, the parent must be a
  model version already committed in the same project (`UNKNOWN_MODEL_VERSION`), and the new
  version's model is a copy of the parent's (v1.1, P-7; `fixture/replay.py` folds it so).
  Either way the new version becomes the head of its branch.
- **Model branches (v1.2).** `model.version_created`, `model.patch_proposed` and
  `model.patch_committed` carry an optional `branch` (default `"main"`), and a project has
  one head per branch. A patch's `base_version` must be the head of its branch: `BASE_MOVED`
  when it is an earlier version of that branch (refetch and rebase), `BASE_NOT_BRANCH_HEAD`
  when it is not a version of that branch at all (another branch's version, or a branch
  that has no head). A branch begins with a `model.version_created` that names it and a
  committed parent on any branch; a version without a parent is the project's genesis and
  is refused once the project has any version (`DUPLICATE_GENESIS` on a branch that has a
  head, `BRANCH_NEEDS_PARENT` on a new one). A ledger without the field means exactly what
  it meant under v1.1. `fixture/replay.py` folds per branch and reports main's head.
- **Model versions fold deterministically (v1.1, P-8).** A `model.patch_committed` or
  `model.patch_proposed` must apply to the head's model: every `update_element` or
  `remove_element` target exists (`PATCH_TARGET_MISSING`), and the resulting model validates
  against `system_model.schema.json` (`INVALID_MODEL_RESULT`). A `version_id` is used once
  per project (`DUPLICATE_VERSION_ID`), whether by `model.version_created` or
  `model.patch_committed`. The Arbiter refuses what breaks these; the read models and
  `fixture/replay.py` fold with the same rules.
- **Capacity params bind to elements (v1.1, P-9).** A `CapacityParam` names the element it
  describes in `applies_to` and what it measures in `metric` (`max_qps`, `availability`).
  Without them, the v1.0 convention `name = "<element_id>.<metric>"` is still read, and a
  check result that relied on it records `evidence.deprecated`; the convention is removed in
  v2.0. `applies_to` must resolve to an element (check C-013).
- **Check results name their subject (v1.1, P-10).** A `check.result` carries
  `model_version` and `as_of_seq`; results without them (v1.0) are read as judging the head
  of their time.
- **Session status (v1.2, P-11).** `session.status_changed` records every change of a
  session's status and every decision at a human gate; with `refused: true` it records a
  decision that was asked for and not applied. An event whose `decision` is `approve`,
  `approve_with_risks` or `reject` must come from a human actor (`DECISION_NOT_HUMAN`).
  Together with `session.phase_changed` (which gained `round`), `session.checkpoint` (which
  gained `package_ref`), `budget.updated` and `waiver.signed`, the session read model is a
  projection of the ledger.
- **Findings (v1.2, P-12).** A `finding.raised` is a question, a risk or a hypothesis, never
  a fact: it is not a claim and nothing may cite it as evidence. Each of its `refs` must
  resolve (`UNKNOWN_REF`) to a committed claim, a source, a decision, a model version, an
  element of a committed model version, or an entity: the subject or object of a committed
  claim, named `ent:<entity_type>:<id>` or by its bare `id`. Its `evidence_claims` must be
  committed claims (`UNKNOWN_CLAIM`). A `finding_id` is used once (`DUPLICATE_FINDING_ID`),
  and no two OPEN findings share a `dedupe_key` (`DUPLICATE_FINDING`). `finding.resolved`
  closes an open finding (`FINDING_NOT_OPEN` otherwise). `fixture/replay.py` holds a ledger
  to the same rules.
- **Proposal ids.** `claim.proposed` and `model.patch_proposed` share one `proposal_id`
  namespace per project, and an id is used at most once. `from_proposal` on
  `claim.committed` must name a claim proposal, and on `model.patch_committed` a model patch
  proposal. No id pattern is imposed.

## Phase 0 exit test (spec §21)

1. All five schemas meta-validate against JSON Schema 2020-12. ✔ (`validate.py`)
2. Sample instances round-trip: a claim, a ledger event, an objection. ✔ (`validate.py`)
3. A fixture ledger (`fixture/fixture_ledger.jsonl`, one tiny design session as 40 events)
   that a hand-written replayer (`fixture/replay.py`) turns into a graph projection and a
   model version. ✔ That fixture is the contract's real test and Phase 1's first integration
   test.

## Harness benchmarking (decision record §22, Entry 2)

No public benchmark exists for design-verification harnesses (SWE-bench and friends test
coding agents). The harness therefore carries its own benchmark, reported per release:

1. **Golden tasks** — reference design problems with planted flaws; headline metric is
   planted-flaw catch-rate (§18).
2. **Postmortem replays** — reconstruct the pre-incident design of systems with publicly
   documented failures; the harness must flag the documented cause. This grounds the
   benchmark in reality, not in flaws we invented.
3. Component-level proxies where public datasets exist: claim extraction vs. information-
   extraction sets, retrieval vs. standard QA sets.

## Identifier conventions

ULID-style ids with typed prefixes: `evt_` event, `clm_` claim, `src_` source, `cmp_`
component, `if_` interface, `flw_` flow, `req_` requirement, `adr_` decision, `obj_`
objection, `chk_` check result, `ses_` session, `tsk_` task, `mv_` model version,
`msg_` protocol message, `exp_` experiment, `wvr_` waiver, `fnd_` finding (v1.2).
