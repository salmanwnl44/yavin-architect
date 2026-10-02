# Contract changelog

## v1.0 — frozen 2026-10-02

The v0.1 draft, with the five proposals from `contracts-PROPOSALS.md` decided and applied.
Every schema `$id` moved from `/contracts/v0/` to `/contracts/v1/`. No event, claim, model,
protocol message or catalog entry that was valid under v0.1 with well-formed dates is invalid
under v1.0, and `fixture/fixture_ledger.jsonl` is byte-identical.

- **P-1: `validate.py` passes on Windows.** The schema table is keyed by
  `Path(path).as_posix()`, so the forward-slash lookups find their schemas on Windows as on
  Linux. Behaviour is otherwise unchanged.
- **P-2: `format` is an assertion.** `validate.py` and `fixture/replay.py` validate with a
  `jsonschema.FormatChecker`, so `date-time` and `date` values are checked. `date-time` needs
  `rfc3339-validator`; both scripts fail if it is missing rather than letting every timestamp
  pass. `validate.py` gained one smoke test: an event whose `ts` is not RFC 3339 is rejected.
- **P-3: `prev_hash` is specified.** It is the lowercase hex SHA-256 of the previous
  committed event in the same project, over that event's canonical JSON (sorted keys,
  separators `,` and `:`, non-ASCII not escaped, UTF-8). The first event of a project
  (`seq` 0) omits the field. Stated in the `prev_hash` description in
  `ledger_events.schema.json` and in the README. The schema type is unchanged.
- **P-4: `model.version_created` is specified.** Without `parent` it is the genesis version,
  allowed only while the project has no head. With `parent`, the parent must be a model
  version already committed in the same project. Arbiter-enforced; stated in the
  `ModelVersionCreated` description and in the README. Multiple heads for alternative designs
  are deferred to v1.1.
- **P-5: proposal ids share one namespace.** `claim.proposed` and `model.patch_proposed`
  draw `proposal_id` from one namespace per project, and an id is used at most once.
  Arbiter-enforced; stated in the `proposal_id` and `from_proposal` descriptions and in the
  README. No pattern was added, so existing ids stay valid.

`fixture/replay.py` enforces P-2. It does not enforce P-4 or P-5, which are rules of the
Arbiter: a ledger that breaks them can still replay green.

## v0.1 — draft

The initial five schemas, `validate.py`, and the fixture ledger with its replayer.
