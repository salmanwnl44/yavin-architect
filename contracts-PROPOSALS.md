# Contract proposals

Friction found while building against `phase0-contracts/` (v0.1 draft). The contracts are
frozen, so nothing here has been applied: each entry records the problem, how the code works
around it today, and a proposed change for the next contract version.

## P-1: `validate.py` fails on Windows

- **Problem.** `validate.py` keys its schema table by the paths `glob.glob` returns
  (`phase0-contracts\claim.schema.json` on Windows) and then looks schemas up by
  forward-slash paths, so the first smoke test dies with
  `KeyError: 'phase0-contracts/claim.schema.json'`. All five META checks pass before that. It
  also opens the schemas without `encoding="utf-8"`, which only works on Windows because the
  non-ASCII characters are in descriptions.
- **Workaround.** None needed in CI (Linux). On Windows, run it with a POSIX-style glob; this
  prints `RESULT: ALL GREEN`:

  ```
  python -c "import glob, runpy; g = glob.glob; glob.glob = lambda p: [x.replace(chr(92), '/') for x in g(p)]; runpy.run_path('phase0-contracts/validate.py', run_name='__main__')"
  ```

- **Proposal.** Key the table by `pathlib.Path(path).as_posix()` and open with
  `encoding="utf-8"`.

## P-2: `format` keywords are annotations, but `ts` has to be a real timestamp

- **Problem.** `ts` is `"format": "date-time"`, and `validate.py` builds its validators without
  a format checker, so under the contract as executed any string is a valid `ts`. The Arbiter
  stores `ts` as `timestamptz` and cannot accept that.
- **Workaround.** The Arbiter rejects a `ts` that is not an RFC 3339 date-time with an offset
  (`SCHEMA_INVALID` at `$.ts`). That makes it stricter than the schema for this one field.
  Formats inside embedded objects (`claim.recorded_at`, `valid_from`, `valid_to`) are left as
  annotations, to match `validate.py`.
- **Proposal.** State in the contracts README that `format` is an assertion, and have
  `validate.py` and `replay.py` pass a `FormatChecker`.

## P-3: `prev_hash` is specified only as "a string"

- **Problem.** The schema types `prev_hash` as a string with no null, and does not say what is
  hashed or how.
- **Workaround.** The first event of a project omits `prev_hash` (the column is NULL). Every
  later event carries the lowercase hex sha256 of the previous event's canonical JSON: the
  event's wire form (exactly what `architect dump` writes, `prev_hash` included), keys sorted,
  separators `,` and `:`, UTF-8, non-ASCII not escaped.
- **Proposal.** Put that definition in the schema description, and decide whether the genesis
  event omits the field or carries a fixed sentinel.

## P-4: `model.version_created` with a `parent` is underspecified

- **Problem.** Nothing says whether `parent` must be the current head, or whether a
  `model.version_created` accompanies each `model.patch_committed` or is only the genesis.
- **Workaround.** The M1 matrix is implemented literally: without `parent` it is a genesis and
  is refused if a head exists (`DUPLICATE_GENESIS`, 409); with `parent` it sets the head to
  `version_id` and `parent` is not checked. This is the first thing to compare against
  `replay.py` when the fixture lands.
- **Proposal.** Define the relationship between the two events, and whether `parent` must
  equal the head.

## P-5: proposal ids share no namespace rule

- **Problem.** `ClaimProposed.proposal_id` and `ModelPatchProposed.proposal_id` are both free
  strings, with no prefix convention like the other ids, so one id can name a claim proposal
  and a patch proposal.
- **Workaround.** The Arbiter tracks proposals per kind: `claim.committed.from_proposal` must
  name a claim proposal and `model.patch_committed.from_proposal` a model patch proposal, else
  `UNKNOWN_PROPOSAL`.
- **Proposal.** Give proposals a typed prefix (`prp_`), or state that the namespaces are
  separate.
