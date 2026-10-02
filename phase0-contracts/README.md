# Yavin Architect — Phase 0 Contracts (FROZEN v1.0)

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

- **Status: FROZEN v1.0** — signed off by the owner. What changed from the v0.1 draft is in
  `CHANGELOG.md`.
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
  while the project has no head. With `parent`, the parent must be a model version already
  committed in the same project. Either way the new version becomes the project's single
  head. Multiple heads for alternative designs are deferred to v1.1.
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
`msg_` protocol message, `exp_` experiment, `wvr_` waiver.
