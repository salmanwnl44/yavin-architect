# Progress

## Status

| Milestone | State |
| --- | --- |
| M0 scaffold | done |
| M1 ledger + Arbiter | **done**: all seven exit tests green in CI against the Phase 0 fixture |
| C1 contracts v1.0 | **done**: proposals P-1 to P-5 applied, contracts FROZEN v1.0, CI green |
| M2 projections | parked: written on the local branch `m2-projections`, not run against a database, not merged |

M1 is complete, and module C1 froze the contracts at v1.0. The owner decided the five
proposals in `contracts-PROPOSALS.md`; they were applied in one sanctioned edit to
`phase0-contracts/` (see its `CHANGELOG.md`) and the Arbiter enforces the new rules. CI ran
the whole suite on `postgres:16` afterwards: 111 passed, no failures. See "Exit tests" for
the output and "Contracts v1.0" for C1's exit tests.

## M0: scaffold

- Python 3.11+ project, src layout, package `architect`, `architect` console entrypoint.
- `docker-compose.yml`: `postgres:16`, plus a Temporal dev server behind the `sessions`
  profile (unused until the sessions milestone). Not exercised locally: the development
  machine has no Docker, so M1 was verified against a native PostgreSQL 16.10.
- pytest + ruff. `phase0-contracts/` is excluded from ruff because it is frozen.
- GitHub Actions CI (`.github/workflows/ci.yml`) with a `postgres:16` service runs, in order:
  `python3 phase0-contracts/validate.py`, `python3 phase0-contracts/fixture/replay.py`,
  `ruff check .`, `pytest -v`. Its first run, before the fixture was added, stopped at the
  `replay.py` step; it has been green since the fixture landed.
- `CLAUDE.md` carries the architecture rules.

## M1: the ledger and the Arbiter

### What landed

- **Schema** (`src/architect/schema.sql`): `projects`, `events`, and the six `arb_*` state
  tables. A trigger on `events` raises on UPDATE, DELETE and TRUNCATE.
- **Arbiter** (`arbiter.py`): the only writer. One transaction per commit, serialized per
  project by `pg_advisory_xact_lock(hashtextextended(project_id, 0))`: idempotency lookup,
  stamp `seq` (dense from 0) and `prev_hash`, schema gate, per-type rules, insert, state fold.
- **Validation matrix** (`rules.py`): one `Rule(check, apply)` per event type, all 19 covered.
  `check` may reject and never writes; `apply` folds state and never rejects.
- **State as a projection** (`state.py`, `rebuild.py`): `arb_*` is written in the commit
  transaction and rebuilt from the ledger by replaying `apply`.
- **API** (`api.py`): `POST /v1/projects`, `POST|GET /v1/projects/{pid}/events`,
  `GET .../events/{event_id}`, `GET .../head`, `GET /healthz`.
- **CLI** (`cli.py`): `architect ingest | dump | rebuild-state | verify`, plus `init-db` and
  `serve`.

### Rejection codes

Body is `{code, detail, json_path?}`.

| HTTP | Codes |
| --- | --- |
| 422 | `SCHEMA_INVALID`, `CLAIM_ID_MISMATCH`, `UNKNOWN_SOURCE`, `UNKNOWN_PROPOSAL`, `UNKNOWN_CLAIM`, `STATUS_MISMATCH`, `UNKNOWN_CAUSE_EVENT`, `PROMOTION_FORBIDDEN`, `PATCH_BASE_MISMATCH`, `UNKNOWN_MODEL_VERSION`, `OBJECTION_NOT_OPEN`, `WAIVER_NOT_HUMAN`, `PROJECT_MISMATCH`, `MALFORMED_REQUEST` |
| 409 | `BASE_MOVED`, `DUPLICATE_SOURCE`, `DUPLICATE_CLAIM_ID`, `DUPLICATE_PROPOSAL`, `DUPLICATE_EVENT_ID`, `DUPLICATE_GENESIS`, `DUPLICATE_PROJECT` |
| 404 | `UNKNOWN_PROJECT`, `UNKNOWN_EVENT` |

Codes beyond the M1 brief: `DUPLICATE_EVENT_ID` (client `event_id` already committed),
`DUPLICATE_GENESIS` (a `model.version_created` without `parent` when a head exists),
`PROJECT_MISMATCH` (candidate `project_id` differs from the URL), `MALFORMED_REQUEST` (the
body is not what the endpoint takes), `DUPLICATE_PROJECT`, `UNKNOWN_PROJECT`, `UNKNOWN_EVENT`.

Codes added with contracts v1.0: `UNKNOWN_MODEL_VERSION` (the `parent` of a
`model.version_created` is not a committed model version in the project) and
`DUPLICATE_PROPOSAL` (a `proposal_id` is reused, by either kind of proposal).

### Decisions worth knowing

- **`events.ts_wire`** is one column more than the brief's data model. `timestamptz` keeps
  the instant but not the offset or precision the client sent; `ts_wire` keeps the string, so
  a dump returns `ts` byte for byte and the hash chain can be recomputed.
- **`prev_hash`** is the hex sha256 of the previous event's wire form in canonical JSON
  (sorted keys, compact separators, UTF-8). It is computed from the row as stored, so it does
  not depend on how the client formatted numbers. The first event has none.
- **A retry is answered before validation.** The same `(project_id, idempotency_key)` returns
  the original event with `replayed: true` whatever the rest of the candidate says.
- **A candidate carrying `seq` or `prev_hash` is refused** over the API. `architect ingest`
  strips both from each line before submitting.
- **`architect ingest --project P` rewrites each event's `project_id` to `P`.** The exit test
  ingests the fixture into `fix`, whatever project the fixture names.
- **`claim.status_changed` updates `arb_claims.status` only.** `arb_claims.claim` stays the
  claim as committed.
- **Proposal ids are one namespace per project**, shared by claim and model patch proposals
  and used at most once (contracts v1.0, P-5). `from_proposal` must still name a proposal of
  the matching kind.
- **Model versions are tracked in `arb_model_versions`**, filled by `model.version_created`
  and `model.patch_committed`. A `parent` must be in it; it need not be the head (P-4). The
  new version always becomes the single head.
- **`format` is asserted** in events and embedded objects, as `replay.py` asserts it (P-2).
  The Arbiter will not start validating without `rfc3339-validator`.
- **A database created before v1.0** keeps its old `arb_proposals` key and has an empty
  `arb_model_versions`; run `architect rebuild-state` on each project. `schema.sql` creates
  tables and does not migrate them.
- **`GET /events?since_seq=N`** returns events with `seq > N`; `next_since_seq` continues.
- **`architect rebuild-state`** exits 1 when the diff is not empty. The rebuilt state is
  kept either way.

### Commands

```
# once
pip install -e ".[dev]"
docker compose up -d postgres          # or any PostgreSQL 16
export ARCHITECT_DATABASE_URL=postgresql://architect:architect@localhost:5432/architect  # the default

# checks (what CI runs)
python3 phase0-contracts/validate.py                 # `py ...` on Windows
python3 phase0-contracts/fixture/replay.py
ruff check .
pytest -v

# run it
architect init-db
architect serve --port 8000
architect ingest phase0-contracts/fixture/fixture_ledger.jsonl --project fix
architect dump --project fix -o dump.jsonl
python3 phase0-contracts/fixture/replay.py dump.jsonl
architect verify --project fix
architect rebuild-state --project fix
```

Tests create one throwaway schema per test inside the database that
`ARCHITECT_DATABASE_URL` names, and drop it afterwards.

### Exit tests

| # | Exit test | Test | Result |
| --- | --- | --- | --- |
| 1 | Fixture round-trip replays green | `test_exit_fixture.py::test_fixture_round_trips_through_the_arbiter_and_replays_green` | green |
| 2 | Refusal suite | the nine tests under "Exit test 2" in `test_refusals.py` | green |
| 3 | Idempotency | `test_arbiter.py::test_idempotent_resubmission_returns_the_original_event` | green |
| 4 | Concurrency, 20 in parallel | `test_arbiter.py::test_parallel_submissions_get_dense_seq_and_a_valid_chain` | green |
| 5 | Append-only trigger | `test_arbiter.py::test_raw_sql_cannot_change_or_remove_a_committed_event` | green |
| 6 | Atomicity | `test_arbiter.py::test_a_failure_before_commit_rolls_back_event_and_state` | green |
| 7 | Rebuild after fixture ingest, zero diff | `test_exit_fixture.py::test_rebuild_state_after_the_fixture_ingest_reports_zero_diff` | green |

Exit test 1 ingests the fixture through the Arbiter into project `fix`, dumps it, and runs
`replay.py` on the dump; exit test 7 rebuilds `arb_*` after that ingest and expects an empty
diff. `test_cli.py` runs the same two paths on a hand-written 24-event ledger covering all 19
event types (`tests/builders.py::sample_ledger`).

CI output, 2026-10-02, GitHub Actions run 36967121987 on commit `806341c`
(https://github.com/salmanwnl44/yavin-architect/actions/runs/36967121987): ubuntu-latest,
Python 3.11.16, PostgreSQL 16.15 (`postgres:16`), contracts v1.0. Every step succeeded.

```
$ python3 phase0-contracts/validate.py
META OK   phase0-contracts/agent_protocol.schema.json
META OK   phase0-contracts/check_catalog.schema.json
META OK   phase0-contracts/claim.schema.json
META OK   phase0-contracts/ledger_events.schema.json
META OK   phase0-contracts/system_model.schema.json
SMOKE OK claim: documented w/ evidence (valid=True, expected=True)
SMOKE OK claim: documented w/o evidence rejected (valid=False, expected=False)
SMOKE OK claim: load-bearing assumption w/o plan rejected (valid=False, expected=False)
SMOKE OK claim: load-bearing assumption w/ plan accepted (valid=True, expected=True)
SMOKE OK event: waiver.signed (valid=True, expected=True)
SMOKE OK event: waiver w/o signer rejected (valid=False, expected=False)
SMOKE OK event: ts that is not RFC 3339 rejected (valid=False, expected=False)
SMOKE OK protocol: falsifiable objection (valid=True, expected=True)
SMOKE OK protocol: un-falsifiable objection rejected (valid=False, expected=False)
SMOKE OK catalog: C-005 entry (valid=True, expected=True)
SMOKE OK model: minimal version (valid=True, expected=True)

RESULT: ALL GREEN

$ python3 phase0-contracts/fixture/replay.py
ledger: fixture_ledger.jsonl — 40 events
sources ingested:   4
claims committed:   6  {'documented': 4, 'measured': 2}
graph edges:        16  {'EVIDENCES': 5, 'SATISFIES': 5, 'DEPENDS_ON': 3, 'MITIGATES': 0, 'DECISION_EVIDENCE': 3}
model versions:     4  head=mv_FIXV000003
  components=5 interfaces=1 flows=3 state_machines=1 trust_boundaries=1
objections:         raised=1 resolved=1 open=0
decisions (ADR):    1   waivers: 1   checkpoints: 2
recorded checks:    [('C-005', 'pass'), ('C-008', 'fail'), ('C-009', 'pass')]
L0 C-001 on final state: PASS
L0 C-002 on final state: PASS
L0 C-008 on final state: PASS
L0 C-009 on final state: PASS
gate IMPLEMENTATION_READY: ALLOWED  (open criticals=0, open load-bearing assumptions=0)

RESULT: REPLAY GREEN — Phase 0 exit test complete

$ ruff check .
All checks passed!

$ pytest -v
============================= test session starts ==============================
platform linux -- Python 3.11.16, pytest-9.1.1, pluggy-1.6.0 -- /opt/hostedtoolcache/Python/3.11.16/x64/bin/python
cachedir: .pytest_cache
rootdir: /home/runner/work/yavin-architect/yavin-architect
configfile: pyproject.toml
testpaths: tests
plugins: anyio-4.15.1
collecting ... collected 111 items

tests/test_api.py::test_healthz PASSED                                   [  0%]
tests/test_api.py::test_create_project PASSED                            [  1%]
tests/test_api.py::test_submit_returns_201_then_200_replayed PASSED      [  2%]
tests/test_api.py::test_the_whole_sample_session_commits PASSED          [  3%]
tests/test_api.py::test_events_page_in_seq_order PASSED                  [  4%]
tests/test_api.py::test_page_parameters_are_validated PASSED             [  5%]
tests/test_api.py::test_get_event_by_id PASSED                           [  6%]
tests/test_api.py::test_head_summarizes_the_arbiter_state PASSED         [  7%]
tests/test_api.py::test_head_counts_open_objections PASSED               [  8%]
tests/test_api.py::test_unknown_project_is_404_everywhere PASSED         [  9%]
tests/test_arbiter.py::test_every_event_type_has_a_rule PASSED           [  9%]
tests/test_arbiter.py::test_first_commit_is_seq_zero_without_prev_hash PASSED [ 10%]
tests/test_arbiter.py::test_arbiter_mints_event_id_and_ts_when_absent PASSED [ 11%]
tests/test_arbiter.py::test_client_event_id_and_ts_are_kept_verbatim PASSED [ 12%]
tests/test_arbiter.py::test_seq_is_dense_and_each_event_hashes_its_predecessor PASSED [ 13%]
tests/test_arbiter.py::test_projects_are_sequenced_independently PASSED  [ 14%]
tests/test_arbiter.py::test_committed_events_read_back_identically PASSED [ 15%]
tests/test_arbiter.py::test_idempotent_resubmission_returns_the_original_event PASSED [ 16%]
tests/test_arbiter.py::test_a_retry_wins_over_rules_that_its_first_commit_changed PASSED [ 17%]
tests/test_arbiter.py::test_parallel_submissions_get_dense_seq_and_a_valid_chain PASSED [ 18%]
tests/test_arbiter.py::test_parallel_retries_of_one_candidate_commit_once PASSED [ 18%]
tests/test_arbiter.py::test_raw_sql_cannot_change_or_remove_a_committed_event[UPDATE events SET payload = '{}'::jsonb] PASSED [ 19%]
tests/test_arbiter.py::test_raw_sql_cannot_change_or_remove_a_committed_event[UPDATE events SET seq = seq + 100] PASSED [ 20%]
tests/test_arbiter.py::test_raw_sql_cannot_change_or_remove_a_committed_event[DELETE FROM events] PASSED [ 21%]
tests/test_arbiter.py::test_raw_sql_cannot_change_or_remove_a_committed_event[TRUNCATE events] PASSED [ 22%]
tests/test_arbiter.py::test_a_failure_before_commit_rolls_back_event_and_state PASSED [ 23%]
tests/test_arbiter.py::test_the_hook_runs_after_both_writes PASSED       [ 24%]
tests/test_arbiter.py::test_candidates_must_not_carry_arbiter_stamped_fields[seq] PASSED [ 25%]
tests/test_arbiter.py::test_candidates_must_not_carry_arbiter_stamped_fields[prev_hash] PASSED [ 26%]
tests/test_arbiter.py::test_unknown_project_is_refused PASSED            [ 27%]
tests/test_arbiter.py::test_event_id_is_unique_across_projects PASSED    [ 27%]
tests/test_arbiter.py::test_unstorable_json_is_a_typed_rejection PASSED  [ 28%]
tests/test_architecture.py::test_only_the_arbiter_writes_events PASSED   [ 29%]
tests/test_architecture.py::test_no_code_path_updates_or_deletes_events PASSED [ 30%]
tests/test_cli.py::test_ingest_then_dump_round_trips_the_ledger PASSED   [ 31%]
tests/test_cli.py::test_ingest_is_idempotent PASSED                      [ 32%]
tests/test_cli.py::test_a_dump_can_be_ingested_into_a_fresh_database_project PASSED [ 33%]
tests/test_cli.py::test_ingest_stops_at_the_first_rejection_with_the_typed_error PASSED [ 34%]
tests/test_cli.py::test_dump_to_stdout PASSED                            [ 35%]
tests/test_cli.py::test_verify_reports_a_healthy_chain PASSED            [ 36%]
tests/test_cli.py::test_verify_reports_a_tampered_event PASSED           [ 36%]
tests/test_cli.py::test_rebuild_state_reports_zero_diff_on_a_healthy_ledger PASSED [ 37%]
tests/test_cli.py::test_rebuild_state_repairs_and_reports_drift PASSED   [ 38%]
tests/test_cli.py::test_dropping_the_state_tables_loses_nothing PASSED   [ 39%]
tests/test_cli.py::test_rebuild_only_touches_its_own_project PASSED      [ 40%]
tests/test_cli.py::test_commands_need_an_existing_project[dump] PASSED   [ 41%]
tests/test_cli.py::test_commands_need_an_existing_project[verify] PASSED [ 42%]
tests/test_cli.py::test_commands_need_an_existing_project[rebuild-state] PASSED [ 43%]
tests/test_cli.py::test_console_entrypoint PASSED                        [ 44%]
tests/test_contracts.py::test_all_five_schemas_meta_validate PASSED      [ 45%]
tests/test_contracts.py::test_every_schema_id_is_a_v1_id PASSED          [ 45%]
tests/test_contracts.py::test_the_contract_scripts_exit_zero[validate.py-RESULT: ALL GREEN] PASSED [ 46%]
tests/test_contracts.py::test_the_contract_scripts_exit_zero[fixture/replay.py-RESULT: REPLAY GREEN] PASSED [ 47%]
tests/test_contracts.py::test_event_types_match_the_payload_dispatch PASSED [ 48%]
tests/test_contracts.py::test_event_error_reports_the_branch_for_the_events_own_type PASSED [ 49%]
tests/test_contracts.py::test_formats_are_assertions PASSED              [ 50%]
tests/test_contracts.py::test_embedded_validators_resolve_their_defs PASSED [ 51%]
tests/test_contracts.py::test_json_path_formatting PASSED                [ 52%]
tests/test_exit_fixture.py::test_fixture_round_trips_through_the_arbiter_and_replays_green PASSED [ 53%]
tests/test_exit_fixture.py::test_rebuild_state_after_the_fixture_ingest_reports_zero_diff PASSED [ 54%]
tests/test_refusals.py::test_documented_claim_without_evidence PASSED    [ 54%]
tests/test_refusals.py::test_load_bearing_assumption_without_verification_plan PASSED [ 55%]
tests/test_refusals.py::test_promotion_to_measured_needs_an_experiment_as_cause PASSED [ 56%]
tests/test_refusals.py::test_status_change_with_the_wrong_from PASSED    [ 57%]
tests/test_refusals.py::test_waiver_signed_by_an_agent PASSED            [ 58%]
tests/test_refusals.py::test_evidence_citing_an_uningested_source PASSED [ 59%]
tests/test_refusals.py::test_patch_on_a_stale_base PASSED                [ 60%]
tests/test_refusals.py::test_resolving_an_objection_that_was_never_raised PASSED [ 61%]
tests/test_refusals.py::test_objection_without_a_falsifiable_test PASSED [ 62%]
tests/test_refusals.py::test_duplicate_source PASSED                     [ 63%]
tests/test_refusals.py::test_proposed_claim_must_be_a_valid_claim PASSED [ 63%]
tests/test_refusals.py::test_claim_id_mismatch PASSED                    [ 64%]
tests/test_refusals.py::test_duplicate_claim_id PASSED                   [ 65%]
tests/test_refusals.py::test_claim_from_an_unknown_proposal PASSED       [ 66%]
tests/test_refusals.py::test_a_patch_proposal_is_not_a_claim_proposal PASSED [ 67%]
tests/test_refusals.py::test_claim_and_patch_proposals_with_distinct_ids_are_accepted PASSED [ 68%]
tests/test_refusals.py::test_a_proposal_id_is_used_once_across_both_kinds PASSED [ 69%]
tests/test_refusals.py::test_formats_inside_embedded_objects_are_enforced PASSED [ 70%]
tests/test_refusals.py::test_status_change_of_an_unknown_claim PASSED    [ 71%]
tests/test_refusals.py::test_status_change_with_an_unknown_cause_event PASSED [ 72%]
tests/test_refusals.py::test_a_cause_event_from_another_project_is_unknown PASSED [ 72%]
tests/test_refusals.py::test_promotion_to_observed_is_guarded_too PASSED [ 73%]
tests/test_refusals.py::test_retracting_an_unknown_claim PASSED          [ 74%]
tests/test_refusals.py::test_second_genesis_version PASSED               [ 75%]
tests/test_refusals.py::test_version_created_with_a_committed_parent_is_accepted PASSED [ 76%]
tests/test_refusals.py::test_version_created_with_an_unknown_parent PASSED [ 77%]
tests/test_refusals.py::test_a_parent_version_from_another_project_is_unknown PASSED [ 78%]
tests/test_refusals.py::test_patch_must_be_a_valid_model_patch PASSED    [ 79%]
tests/test_refusals.py::test_patch_base_mismatch PASSED                  [ 80%]
tests/test_refusals.py::test_patch_proposed_on_a_stale_base PASSED       [ 81%]
tests/test_refusals.py::test_patch_before_any_model_version PASSED       [ 81%]
tests/test_refusals.py::test_patch_committed_from_an_unknown_proposal PASSED [ 82%]
tests/test_refusals.py::test_proposed_check_must_be_a_valid_check PASSED [ 83%]
tests/test_refusals.py::test_resolving_an_objection_twice PASSED         [ 84%]
tests/test_refusals.py::test_waiver_signed_by_the_system PASSED          [ 85%]
tests/test_refusals.py::test_experiment_with_an_uncommitted_result_claim PASSED [ 86%]
tests/test_refusals.py::test_decision_citing_an_uncommitted_claim PASSED [ 87%]
tests/test_refusals.py::test_merge_revert_must_cite_a_committed_merge PASSED [ 88%]
tests/test_refusals.py::test_schema_gate[change0-$.type] PASSED          [ 89%]
tests/test_refusals.py::test_schema_gate[change1-$.idempotency_key] PASSED [ 90%]
tests/test_refusals.py::test_schema_gate[change2-$] PASSED               [ 90%]
tests/test_refusals.py::test_schema_gate[change3-$.actor.kind] PASSED    [ 91%]
tests/test_refusals.py::test_schema_gate[change4-$.event_id] PASSED      [ 92%]
tests/test_refusals.py::test_schema_gate[change5-$.ts] PASSED            [ 93%]
tests/test_refusals.py::test_schema_gate[change6-$.ts] PASSED            [ 94%]
tests/test_refusals.py::test_schema_gate[change7-$.session_id] PASSED    [ 95%]
tests/test_refusals.py::test_schema_gate[change8-$] PASSED               [ 96%]
tests/test_refusals.py::test_schema_gate[change9-$.payload] PASSED       [ 97%]
tests/test_refusals.py::test_schema_gate_runs_before_the_rules PASSED    [ 98%]
tests/test_refusals.py::test_project_mismatch PASSED                     [ 99%]
tests/test_refusals.py::test_candidate_must_be_an_object PASSED          [100%]

============================= 111 passed in 9.89s ==============================
```

Removing the per-project lock makes exit test 4 fail with a `(project_id, seq)` unique
violation, so that test does depend on the lock.

### Fixture integrity

SHA-256, checked on disk and again on the committed blobs; the files are stored byte-for-byte
(`phase0-contracts/** -text` in `.gitattributes`). `fixture_ledger.jsonl` and
`build_fixture.py` are byte-identical to the files the owner supplied. `replay.py` changed in
the v1.0 edit (format checking, P-2), so its checksum is new; before v1.0 it was
`dbe60b992f59eafbef772557243b2fcbc521e470963eaf812bab2727b9ef080a`.

```
e62ec17232863134b46a50dde6e03df64f0d3f67923d07c480d4f591f8edaf4b  fixture_ledger.jsonl   (40 lines, LF)
e934263c45f898a109678614461e5aafd8b28f57541e84fa46a98fd391757f2b  replay.py
ac312708dfcdc5cea26016fc259ee16da87c71a17639dd644c3e866d7a3234fc  build_fixture.py
```

## Contracts v1.0 (module C1)

The one sanctioned edit to `phase0-contracts/`, authorised by the owner on 2026-10-02. The
contracts are frozen again; `phase0-contracts/CHANGELOG.md` is the record.

C1's exit tests, on the CI run pasted under "Exit tests" (run 36967121987) unless noted:

| # | Exit test | Result | Evidence |
| --- | --- | --- | --- |
| 1 | `validate.py` is ALL GREEN on Windows and in CI | green | `py phase0-contracts/validate.py` below; CI step "Phase 0 contracts validate" |
| 2 | `replay.py` is REPLAY GREEN in CI with format checking | green | CI step "Phase 0 fixture replays" |
| 3 | Fixture checksum unchanged | green | `e62ec17232863134b46a50dde6e03df64f0d3f67923d07c480d4f591f8edaf4b` |
| 4 | Every schema `$id` contains `/v1/` | green | `test_contracts.py::test_every_schema_id_is_a_v1_id` |
| 5 | P-4 and P-5 Arbiter tests green, earlier tests still green | green | the tests in the table below; 111 passed |
| 6 | CI on the final pushed commit is green with `pytest -v` | green | see the module report for the run id |

`test_contracts.py::test_the_contract_scripts_exit_zero` also runs `validate.py` and
`replay.py` inside the suite and requires exit 0 from both.

The C1 prompt named the rejection codes `UNKNOWN_PARENT`, `GENESIS_EXISTS` and
`DUPLICATE_PROPOSAL_ID`. The owner chose to keep the names already shipped:
`UNKNOWN_MODEL_VERSION`, `DUPLICATE_GENESIS` (which predates the freeze and has its own M1
test) and `DUPLICATE_PROPOSAL`.

| Proposal | In the contracts | In the Arbiter |
| --- | --- | --- |
| P-1 | `validate.py` keys schemas by POSIX path, so it passes on Windows | nothing |
| P-2 | `validate.py` and `replay.py` assert `format`; both fail without `rfc3339-validator` | validators assert `format` too; `rfc3339-validator` is a dependency |
| P-3 | `prev_hash` specified in the schema description and README; omitted at `seq` 0 | nothing: this documents what it already did |
| P-4 | `model.version_created` rules stated in the schema description and README | `parent` must be a committed model version: `UNKNOWN_MODEL_VERSION`, `arb_model_versions` |
| P-5 | one proposal id namespace stated in the schema descriptions and README; no pattern | reuse of a `proposal_id` is refused: `DUPLICATE_PROPOSAL` |

Also: every schema `$id` moved from `/v0/` to `/v1/`, the README status is "FROZEN v1.0",
`CLAUDE.md` says "FROZEN at v1.0", and CI runs `pytest -v`.

`replay.py` does not enforce P-4 or P-5, so the Arbiter now refuses some ledgers that
`replay.py` accepts. `CLAUDE.md` states the reference-semantics rule with that exception.

New tests, all green in the CI run above:

| Rule | Accepted | Refused |
| --- | --- | --- |
| P-4 | `test_version_created_with_a_committed_parent_is_accepted` | `test_version_created_with_an_unknown_parent`, `test_a_parent_version_from_another_project_is_unknown`, `test_second_genesis_version` (from M1) |
| P-5 | `test_claim_and_patch_proposals_with_distinct_ids_are_accepted` | `test_a_proposal_id_is_used_once_across_both_kinds` |
| P-2 | `test_contracts.py::test_formats_are_assertions` | `test_formats_inside_embedded_objects_are_enforced` |

On Windows 11 with `py` (Python 3.11.0), 2026-10-02, no shim:

```
$ py phase0-contracts/validate.py
META OK   phase0-contracts/agent_protocol.schema.json
META OK   phase0-contracts/check_catalog.schema.json
META OK   phase0-contracts/claim.schema.json
META OK   phase0-contracts/ledger_events.schema.json
META OK   phase0-contracts/system_model.schema.json
SMOKE OK claim: documented w/ evidence (valid=True, expected=True)
SMOKE OK claim: documented w/o evidence rejected (valid=False, expected=False)
SMOKE OK claim: load-bearing assumption w/o plan rejected (valid=False, expected=False)
SMOKE OK claim: load-bearing assumption w/ plan accepted (valid=True, expected=True)
SMOKE OK event: waiver.signed (valid=True, expected=True)
SMOKE OK event: waiver w/o signer rejected (valid=False, expected=False)
SMOKE OK event: ts that is not RFC 3339 rejected (valid=False, expected=False)
SMOKE OK protocol: falsifiable objection (valid=True, expected=True)
SMOKE OK protocol: un-falsifiable objection rejected (valid=False, expected=False)
SMOKE OK catalog: C-005 entry (valid=True, expected=True)
SMOKE OK model: minimal version (valid=True, expected=True)

RESULT: ALL GREEN

$ py phase0-contracts/fixture/replay.py
RESULT: REPLAY GREEN — Phase 0 exit test complete
```

## Not verified on Windows

The database-backed tests have only run on Linux in CI. The Windows development machine has
had no reachable PostgreSQL since the fixture landed, so they have not run there against the
fixture or against the v1.0 rules. What does run on Windows is green: `validate.py`,
`replay.py`, `ruff check .`, and the eleven tests that need no database.

## Open

1. **Run the suite once on Windows** against a local PostgreSQL 16, to confirm what CI shows.
2. **contracts-PROPOSALS.md P-6** (the contract scripts read files with the platform's
   default encoding) waits for the next contract version.
3. **M2 is parked** on the local branch `m2-projections` (one WIP commit, not pushed). Its
   database-backed tests have never run. Resume it only on the owner's word.
