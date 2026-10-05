# Contract proposals

Friction found while building against `phase0-contracts/` (v0.1 draft). The owner decided all
five entries, and they were applied in the one sanctioned edit that froze the contracts as
v1.0; `phase0-contracts/CHANGELOG.md` is the record on the contract side. New friction goes
below as P-6 onwards and waits for the next contract version: the contracts are frozen again.
v1.1 (module C2) applied P-7 to P-10. v1.2 (module C3) applied P-11 and P-12 and the
multi-head branches deferred from P-4. P-6 stays open.

## P-1: `validate.py` fails on Windows

**Status: applied in v1.0** (the path keys; the encoding half was not part of the decision).

- **Problem.** `validate.py` keys its schema table by the paths `glob.glob` returns
  (`phase0-contracts\claim.schema.json` on Windows) and then looks schemas up by
  forward-slash paths, so the first smoke test dies with
  `KeyError: 'phase0-contracts/claim.schema.json'`. All five META checks pass before that. It
  also opens the schemas without `encoding="utf-8"`, which only works on Windows because the
  non-ASCII characters are in descriptions.
- **Applied.** The table is keyed by `Path(path).as_posix()`. `py phase0-contracts/validate.py`
  prints `RESULT: ALL GREEN` on Windows with no shim.
- **Still open.** `validate.py` and `replay.py` still open files with the platform's default
  encoding. On Windows that mis-decodes non-ASCII text without failing today. Carried forward
  as P-6.

## P-2: `format` keywords are annotations, but `ts` has to be a real timestamp

**Status: applied in v1.0.**

- **Problem.** `ts` is `"format": "date-time"`, and `validate.py` builds its validators without
  a format checker, so under the contract as executed any string is a valid `ts`. The Arbiter
  stores `ts` as `timestamptz` and cannot accept that.
- **Applied.** `validate.py` and `fixture/replay.py` validate with a `FormatChecker`, and fail
  if `rfc3339-validator` is missing rather than passing every timestamp. The Arbiter's
  validators use a format checker too, so formats inside embedded objects (`claim.recorded_at`,
  `valid_from`, `valid_to`) are now enforced, as `replay.py` enforces them. The Arbiter's own
  `ts` check stays. `rfc3339-validator` is a runtime dependency.

## P-3: `prev_hash` is specified only as "a string"

**Status: applied in v1.0.**

- **Problem.** The schema types `prev_hash` as a string with no null, and does not say what is
  hashed or how.
- **Applied.** The schema description and the README now state what the Arbiter does: the
  lowercase hex sha256 of the previous committed event's canonical JSON (the event's wire
  form, exactly what `architect dump` writes, `prev_hash` included; keys sorted, separators
  `,` and `:`, UTF-8, non-ASCII not escaped). The first event of a project omits the field;
  the owner chose omission over an explicit null, so the schema type is unchanged.

## P-4: `model.version_created` with a `parent` is underspecified

**Status: applied in v1.0.**

- **Problem.** Nothing says whether `parent` must be the current head, or whether a
  `model.version_created` accompanies each `model.patch_committed` or is only the genesis.
- **Applied.** Without `parent` it is the genesis and is refused if a head exists
  (`DUPLICATE_GENESIS`, 409). With `parent`, the parent must be a model version already
  committed in the same project, else `UNKNOWN_MODEL_VERSION` (422); it need not be the head.
  Either way the new version becomes the single head. Multiple heads for alternative designs
  are deferred to v1.1. The Arbiter enforces this; `replay.py` does not.

## P-5: proposal ids share no namespace rule

**Status: applied in v1.0.**

- **Problem.** `ClaimProposed.proposal_id` and `ModelPatchProposed.proposal_id` are both free
  strings, with no prefix convention like the other ids, so one id can name a claim proposal
  and a patch proposal.
- **Applied.** One namespace per project, shared by both kinds, and an id is used at most
  once: reusing one is `DUPLICATE_PROPOSAL` (409). `from_proposal` must still name a proposal
  of the matching kind, else `UNKNOWN_PROPOSAL`. No pattern was added, so the fixture's ids
  stay valid. The Arbiter enforces this; `replay.py` does not.

## P-6: the contract scripts read files with the platform's default encoding

**Status: open, for the next contract version.**

- **Problem.** `validate.py` and `fixture/replay.py` open the schemas and the ledger without
  `encoding="utf-8"`. On Windows they are decoded as cp1252: non-ASCII text is mis-decoded,
  and a byte cp1252 does not define (for example in a right double quote) would raise.
- **Workaround.** None needed today: the current files decode, and CI runs on Linux. The
  Arbiter reads every contract file and ledger as UTF-8.
- **Proposal.** Pass `encoding="utf-8"` wherever the two scripts open a file.

## P-7: `replay.py` empties the model on every `model.version_created`

**Status: applied in v1.1.** `replay.py` keeps a model per version; a version created from a
parent is a copy of the parent's. Its output on the fixture is unchanged.

- **Problem.** v1.0 lets `model.version_created` carry a `parent` (P-4), but `replay.py` folds
  every `model.version_created` to an empty model, parent or not. The contracts do not say
  what a version created from a parent contains. The fixture only has a genesis, so its
  replay is unaffected.
- **Workaround.** The Arbiter and the read models (both through `architect.model_fold`)
  materialize a version created from a parent as a copy of the parent's model, under the new
  `version_id`. That differs from `replay.py` on any ledger that uses `parent`.
- **Proposal.** State in the contracts that a version created from a parent starts as the
  parent's model, and have `replay.py` fold it that way.

## P-8: the model rules the Arbiter enforces are not in the contracts

**Status: applied in v1.1.** The README and the `ModelPatchProposed`, `ModelPatchCommitted`
and `ModelVersionCreated` descriptions state the rules with the shipped rejection codes.

- **Problem.** Since M1.1 the Arbiter folds every model version before committing it and
  refuses what `replay.py` would refuse: an `update_element` on a missing target, and a
  version that is not a valid system model. It also refuses what `replay.py` lets through:
  a `remove_element` on a missing target, a reused `version_id`, a proposal whose patch
  would not apply, and an intermediate version that is invalid (`replay.py` validates the
  final model only). The README's list of Arbiter-enforced rules (P-4, P-5) does not name
  these, so CLAUDE.md's reference-semantics rule has unlisted exceptions.
- **Workaround.** `CLAUDE.md` points here. `architect.model_fold` is the one fold both the
  Arbiter and the projector use; `tests/test_model_fold.py` pins its agreement with
  `replay.py` on the fixture.
- **Proposal.** State in the README that a committed patch must apply (every target exists)
  and leave a valid system model, that proposals are held to the same rule, and that version
  ids are unique per project; have `replay.py` enforce the same.

## P-9: CapacityParam has no `applies_to`

**Status: applied in v1.1.** `applies_to` and `metric` are optional fields; the naming
convention is still read as a fallback and recorded as `evidence.deprecated`, for removal in
v2.0.

- **Problem.** `system_model.schema.json` gives `CapacityParam` an `id`, a `name`, a `value`
  and a `unit`, but nothing that binds it to the element it describes. Checks C-005 and
  C-006 need the capacity and availability of a specific component.
- **Workaround.** By convention `name = "<element_id>.<metric>"`, with the metrics
  `max_qps` (a throughput) and `availability` (a ratio). The convention lives in one
  helper, `architect.checks.graph.capacity_param`; no check parses a name itself.
- **Proposal.** An `applies_to` field (an element id) and a `metric` field on
  `CapacityParam`, so the binding is data rather than a naming rule.

## P-10: CheckResult names no model version

**Status: applied in v1.1.** `model_version` and `as_of_seq` are optional fields on
`CheckResult`; the runner sets both and keeps the evidence copies.

- **Problem.** `CheckResult` in `ledger_events.schema.json` has `result_id`, `check_id`,
  `element_refs`, `status` and a free `evidence` object, but no field for the model version
  the result judges. A result is only meaningful against one version.
- **Workaround.** The runner writes `evidence.model_version` (with `as_of_seq`,
  `catalog_version`, `check_version`, `params` and `inputs_hash`), and `proj_checks` keys a
  result by `evidence.model_version` when it is present, else by the head of its time (the
  fixture's own three results).
- **Proposal.** A first-class `model_version` field on `CheckResult`, and `as_of_seq` with
  it, so a result's subject is part of the contract.

## P-11: the ledger cannot say how a session ended or what the human decided

**Status: applied in v1.2.** `session.status_changed` carries the status, outcome, decision,
reason and package ref; `session.checkpoint` gained `package_ref`. So that the session read
model is rebuildable EXACTLY, v1.2 also added, all optional: `refused`, `gate_verdict`,
`preset`, `limits` and `brief_source_id` on `session.status_changed`, and `round` on
`session.phase_changed`. `status` is a string, not the closed enum proposed below, so a
later module can add a status without a new contract version. `ses_sessions` is now a
projection. What the proposal said, for the record:

- **Problem.** The session event types in `ledger_events.schema.json` are
  `session.phase_changed` (a `Phase`), `session.checkpoint` (phase, best version, open risk
  ids, spend) and `budget.updated`. None of them can carry a session's status or outcome
  (`awaiting_approval`, `approved`, `rejected`, `completed_with_risks`, `stopped_budget`,
  `stopped_time`, `cancelled`, `failed`), the owner's approve/reject decision at a human gate,
  a pause or a cancel, or the key of the session package. `Phase` is also closed, so the
  convergence rule's reason (`allowed`, `no_improvement`, `max_rounds`, `budget`, `time`,
  `cancel`) has nowhere to go either.
- **Workaround.** M6 keeps status, outcome, stop reason, the human decision and the package
  key in the operational table `ses_sessions` (rebuildable from Temporal history plus the
  object store, not from the ledger), and records the owner's steer as a `user` source plus a
  claim so that at least the guidance is in the ledger. The ledger still holds every phase
  change, checkpoint, budget, claim, version, check result and decision of the session.
- **Proposal.** Add `session.status_changed` `{session_id, from?, to, reason?, by?}` with the
  status enum above, and allow `session.checkpoint` to carry an optional `package_ref`
  (content hash). Then `ses_sessions` becomes a projection like `proj_*`, and a session's
  whole story, including the human's decision, replays from the ledger alone.

## P-12: discovery has nowhere to put what it finds

**Status: applied in v1.2** (raised and decided with module M9, before its code).

- **Problem.** The Discovery Engine (spec §10) raises contradictions, gaps, hidden
  dependencies, open assumptions, hypotheses and stale evidence. None of them is a fact, so
  none may be a claim, and an objection is about a model element and needs a falsifiable
  test. The ledger had no event for a question or a risk about the knowledge itself.
- **Decision.** Two event types. `finding.raised` `{finding_id, kind, severity, summary, refs,
  evidence_claims?, suggested_action {kind, detail}, detector {id, version}, dedupe_key}` and
  `finding.resolved` `{finding_id, resolution, ref?}`. A finding is never a fact. The Arbiter
  refuses refs that do not resolve (`UNKNOWN_REF`), a reused id (`DUPLICATE_FINDING_ID`), a
  second open finding with the same `dedupe_key` (`DUPLICATE_FINDING`) and the resolution of
  a finding that is not open (`FINDING_NOT_OPEN`).

