# Contract proposals

Friction found while building against `phase0-contracts/` (v0.1 draft). The owner decided all
five entries, and they were applied in the one sanctioned edit that froze the contracts as
v1.0; `phase0-contracts/CHANGELOG.md` is the record on the contract side. New friction goes
below as P-6 onwards and waits for the next contract version: the contracts are frozen again.

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

**Status: open, for the next contract version.**

- **Problem.** v1.0 lets `model.version_created` carry a `parent` (P-4), but `replay.py` folds
  every `model.version_created` to an empty model, parent or not. The contracts do not say
  what a version created from a parent contains. The fixture only has a genesis, so its
  replay is unaffected.
- **Workaround.** The read models (`architect.projections`) materialize a version created
  from a parent as a copy of the parent's model, under the new `version_id`. That differs
  from `replay.py` on any ledger that uses `parent`.
- **Proposal.** State in the contracts that a version created from a parent starts as the
  parent's model, and have `replay.py` fold it that way.
