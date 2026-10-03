# Progress

## Status

| Milestone | State |
| --- | --- |
| M0 scaffold | done |
| M1 ledger + Arbiter | **done**: all seven exit tests green in CI against the Phase 0 fixture |
| C1 contracts v1.0 | **done**: proposals P-1 to P-5 applied, contracts FROZEN v1.0, CI green |
| M1.1 Arbiter hardening | **done**: the Arbiter folds every model version and refuses what cannot be folded |
| M2 projections | **done**: all nine exit tests green in CI; `proj_*` read models, projector, read API |
| M3 checks engine | **done**: 13 L0/L1 checks, the runner, the gate, CLI and API; all ten exit tests green in CI |
| C2 contracts v1.1 | **done**: P-7 to P-10 applied, additive only; contracts FROZEN v1.1; CI green |

M1 is complete, C1 froze the contracts at v1.0, M1.1 closed the Arbiter's model gap, M2 built
the read side, M3 built the checks engine, and C2 moved the contracts to v1.1 (a minor
version: optional fields and documented rules, every v1.0 document still valid). CI runs the
whole suite on `postgres:16`: 206 passed, none skipped. See "Exit tests" for the output and
the C1, M1.1, M2, M3 and C2 sections below. M4 has not been started.

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

- **Schema** (`src/architect/schema.sql`): `projects`, `events`, the six `arb_*` state
  tables and, since M2, the eleven `proj_*` read-model tables. A trigger on `events` raises
  on UPDATE, DELETE and TRUNCATE.
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
| 422 | `SCHEMA_INVALID`, `CLAIM_ID_MISMATCH`, `UNKNOWN_SOURCE`, `UNKNOWN_PROPOSAL`, `UNKNOWN_CLAIM`, `STATUS_MISMATCH`, `UNKNOWN_CAUSE_EVENT`, `PROMOTION_FORBIDDEN`, `PATCH_BASE_MISMATCH`, `UNKNOWN_MODEL_VERSION`, `PATCH_TARGET_MISSING`, `INVALID_MODEL_RESULT`, `OBJECTION_NOT_OPEN`, `WAIVER_NOT_HUMAN`, `PROJECT_MISMATCH`, `MALFORMED_REQUEST` |
| 409 | `BASE_MOVED`, `DUPLICATE_SOURCE`, `DUPLICATE_CLAIM_ID`, `DUPLICATE_PROPOSAL`, `DUPLICATE_VERSION_ID`, `DUPLICATE_EVENT_ID`, `DUPLICATE_GENESIS`, `DUPLICATE_PROJECT` |
| 404 | `UNKNOWN_PROJECT`, `UNKNOWN_EVENT`, and on the read API `MODEL_VERSION_NOT_FOUND`, `CLAIM_NOT_FOUND`, `ELEMENT_NOT_FOUND` |

Codes beyond the M1 brief: `DUPLICATE_EVENT_ID` (client `event_id` already committed),
`DUPLICATE_GENESIS` (a `model.version_created` without `parent` when a head exists),
`PROJECT_MISMATCH` (candidate `project_id` differs from the URL), `MALFORMED_REQUEST` (the
body is not what the endpoint takes), `DUPLICATE_PROJECT`, `UNKNOWN_PROJECT`, `UNKNOWN_EVENT`.

Codes added with contracts v1.0: `UNKNOWN_MODEL_VERSION` (the `parent` of a
`model.version_created` is not a committed model version in the project) and
`DUPLICATE_PROPOSAL` (a `proposal_id` is reused, by either kind of proposal).

Codes added with M1.1: `PATCH_TARGET_MISSING` (an `update_element` or `remove_element` names
an element the head model does not have; `json_path` is the op), `INVALID_MODEL_RESULT` (the
patch, or the created version, would not leave a valid system model; here `json_path`
locates the violation in the resulting model, not in the event) and `DUPLICATE_VERSION_ID`
(a `model.version_created` or `model.patch_committed` reuses a `version_id`). A patch op
without the fields its kind needs is `SCHEMA_INVALID` at that op.

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
architect project                      # the projector worker; --once to catch up and exit
architect ingest phase0-contracts/fixture/fixture_ledger.jsonl --project fix
architect dump --project fix -o dump.jsonl
python3 phase0-contracts/fixture/replay.py dump.jsonl
architect verify --project fix
architect rebuild-state --project fix
architect rebuild-projections --project fix     # drop + rebuild proj_*, prints the content hash
architect check --project fix --version mv_FIXV000003 --dry-run      # compute, record nothing
architect check --project fix --version mv_FIXV000003 --as-of-seq 19 # knowledge as of seq 19
architect gate --project fix --version mv_FIXV000003                 # run, record, verdict (exit 2 = BLOCKED)
curl -X POST localhost:8000/v1/projects/fix/models/mv_FIXV000003/checks
curl localhost:8000/v1/projects/fix/models/mv_FIXV000003/checks
curl localhost:8000/v1/projects/fix/models/mv_FIXV000003/gate
curl localhost:8000/v1/projects/fix/models/head
curl localhost:8000/v1/projects/fix/claims?as_of_seq=31
curl localhost:8000/v1/projects/fix/claims/clm_FIXPAYLOAD1
curl localhost:8000/v1/projects/fix/elements/cmp_FIXSHARD01/why
curl localhost:8000/v1/projects/fix/projections/status
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

CI output, 2026-10-03, GitHub Actions run 37103718615 on commit `9ed76b2`
(https://github.com/salmanwnl44/yavin-architect/actions/runs/37103718615): ubuntu-latest,
Python 3.11.16, PostgreSQL 16.15 (`postgres:16`), contracts v1.1, M1.1, M2, M3. Every step
succeeded.

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
collecting ... collected 206 items

tests/test_api.py::test_healthz PASSED                                   [  0%]
tests/test_api.py::test_create_project PASSED                            [  0%]
tests/test_api.py::test_submit_returns_201_then_200_replayed PASSED      [  1%]
tests/test_api.py::test_the_whole_sample_session_commits PASSED          [  1%]
tests/test_api.py::test_events_page_in_seq_order PASSED                  [  2%]
tests/test_api.py::test_page_parameters_are_validated PASSED             [  2%]
tests/test_api.py::test_get_event_by_id PASSED                           [  3%]
tests/test_api.py::test_head_summarizes_the_arbiter_state PASSED         [  3%]
tests/test_api.py::test_head_counts_open_objections PASSED               [  4%]
tests/test_api.py::test_unknown_project_is_404_everywhere PASSED         [  4%]
tests/test_arbiter.py::test_every_event_type_has_a_rule PASSED           [  5%]
tests/test_arbiter.py::test_first_commit_is_seq_zero_without_prev_hash PASSED [  5%]
tests/test_arbiter.py::test_arbiter_mints_event_id_and_ts_when_absent PASSED [  6%]
tests/test_arbiter.py::test_client_event_id_and_ts_are_kept_verbatim PASSED [  6%]
tests/test_arbiter.py::test_seq_is_dense_and_each_event_hashes_its_predecessor PASSED [  7%]
tests/test_arbiter.py::test_projects_are_sequenced_independently PASSED  [  7%]
tests/test_arbiter.py::test_committed_events_read_back_identically PASSED [  8%]
tests/test_arbiter.py::test_idempotent_resubmission_returns_the_original_event PASSED [  8%]
tests/test_arbiter.py::test_a_retry_wins_over_rules_that_its_first_commit_changed PASSED [  9%]
tests/test_arbiter.py::test_parallel_submissions_get_dense_seq_and_a_valid_chain PASSED [  9%]
tests/test_arbiter.py::test_parallel_retries_of_one_candidate_commit_once PASSED [ 10%]
tests/test_arbiter.py::test_raw_sql_cannot_change_or_remove_a_committed_event[UPDATE events SET payload = '{}'::jsonb] PASSED [ 10%]
tests/test_arbiter.py::test_raw_sql_cannot_change_or_remove_a_committed_event[UPDATE events SET seq = seq + 100] PASSED [ 11%]
tests/test_arbiter.py::test_raw_sql_cannot_change_or_remove_a_committed_event[DELETE FROM events] PASSED [ 11%]
tests/test_arbiter.py::test_raw_sql_cannot_change_or_remove_a_committed_event[TRUNCATE events] PASSED [ 12%]
tests/test_arbiter.py::test_a_failure_before_commit_rolls_back_event_and_state PASSED [ 12%]
tests/test_arbiter.py::test_the_hook_runs_after_both_writes PASSED       [ 13%]
tests/test_arbiter.py::test_candidates_must_not_carry_arbiter_stamped_fields[seq] PASSED [ 13%]
tests/test_arbiter.py::test_candidates_must_not_carry_arbiter_stamped_fields[prev_hash] PASSED [ 14%]
tests/test_arbiter.py::test_unknown_project_is_refused PASSED            [ 14%]
tests/test_arbiter.py::test_event_id_is_unique_across_projects PASSED    [ 15%]
tests/test_arbiter.py::test_unstorable_json_is_a_typed_rejection PASSED  [ 15%]
tests/test_architecture.py::test_only_the_arbiter_writes_events PASSED   [ 16%]
tests/test_architecture.py::test_no_code_path_updates_or_deletes_events PASSED [ 16%]
tests/test_architecture.py::test_checks_import_nothing_impure PASSED     [ 16%]
tests/test_checks.py::test_the_bundled_catalog_validates_against_the_contract PASSED [ 17%]
tests/test_checks.py::test_registry_and_catalog_name_the_same_checks PASSED [ 17%]
tests/test_checks.py::test_a_proposed_check_loads_and_records_not_implemented PASSED [ 18%]
tests/test_checks.py::test_an_invalid_catalog_is_refused PASSED          [ 18%]
tests/test_checks.py::test_c001_requirement_coverage PASSED              [ 19%]
tests/test_checks.py::test_c002_no_orphans PASSED                        [ 19%]
tests/test_checks.py::test_c003_interface_binding PASSED                 [ 20%]
tests/test_checks.py::test_c004_requirement_refs_match_links PASSED      [ 20%]
tests/test_checks.py::test_c005_capacity_headroom PASSED                 [ 21%]
tests/test_checks.py::test_c006_availability_composition PASSED          [ 21%]
tests/test_checks.py::test_c007_stateful_durability PASSED               [ 22%]
tests/test_checks.py::test_c008_trust_boundaries_and_sensitive_data PASSED [ 22%]
tests/test_checks.py::test_c009_open_load_bearing_assumptions PASSED     [ 23%]
tests/test_checks.py::test_c010_single_points_of_failure PASSED          [ 23%]
tests/test_checks.py::test_c011_idempotent_async_interfaces PASSED       [ 24%]
tests/test_checks.py::test_c012_backpressure PASSED                      [ 24%]
tests/test_checks.py::test_c013_referential_integrity PASSED             [ 25%]
tests/test_checks.py::test_waiver_target_ref_forms PASSED                [ 25%]
tests/test_checks.py::test_units PASSED                                  [ 26%]
tests/test_checks.py::test_graph_helpers PASSED                          [ 26%]
tests/test_checks.py::test_outcomes_say_what_they_must PASSED            [ 27%]
tests/test_checks.py::test_the_catalog_on_the_reference_model_gives_the_expected_table PASSED [ 27%]
tests/test_checks.py::test_running_every_check_twice_on_the_same_inputs_is_identical PASSED [ 28%]
tests/test_checks.py::test_inputs_hash_follows_what_a_check_reads PASSED [ 28%]
tests/test_checks_runner.py::test_the_battery_on_v3_matches_the_answer_key PASSED [ 29%]
tests/test_checks_runner.py::test_v2_fails_c008_like_the_ledger_recorded PASSED [ 29%]
tests/test_checks_runner.py::test_time_travel_sees_the_assumption_still_open PASSED [ 30%]
tests/test_checks_runner.py::test_the_genesis_covers_no_requirement PASSED [ 30%]
tests/test_checks_runner.py::test_the_gate_counts_an_objection_open_as_of_the_seq PASSED [ 31%]
tests/test_checks_runner.py::test_a_gate_without_recorded_results_is_blocked_as_not_evaluated PASSED [ 31%]
tests/test_checks_runner.py::test_a_repair_patch_opens_the_gate PASSED   [ 32%]
tests/test_checks_runner.py::test_results_are_recorded_once_per_inputs PASSED [ 32%]
tests/test_checks_runner.py::test_a_dry_run_records_nothing PASSED       [ 33%]
tests/test_checks_runner.py::test_an_unknown_version_is_refused PASSED   [ 33%]
tests/test_checks_runner.py::test_a_waiver_signed_after_the_seq_does_not_count PASSED [ 33%]
tests/test_checks_runner.py::test_check_and_gate_commands PASSED         [ 34%]
tests/test_checks_runner.py::test_the_check_endpoints PASSED             [ 34%]
tests/test_cli.py::test_ingest_then_dump_round_trips_the_ledger PASSED   [ 35%]
tests/test_cli.py::test_ingest_is_idempotent PASSED                      [ 35%]
tests/test_cli.py::test_a_dump_can_be_ingested_into_a_fresh_database_project PASSED [ 36%]
tests/test_cli.py::test_ingest_stops_at_the_first_rejection_with_the_typed_error PASSED [ 36%]
tests/test_cli.py::test_dump_to_stdout PASSED                            [ 37%]
tests/test_cli.py::test_verify_reports_a_healthy_chain PASSED            [ 37%]
tests/test_cli.py::test_verify_reports_a_tampered_event PASSED           [ 38%]
tests/test_cli.py::test_rebuild_state_reports_zero_diff_on_a_healthy_ledger PASSED [ 38%]
tests/test_cli.py::test_rebuild_state_repairs_and_reports_drift PASSED   [ 39%]
tests/test_cli.py::test_dropping_the_state_tables_loses_nothing PASSED   [ 39%]
tests/test_cli.py::test_rebuild_only_touches_its_own_project PASSED      [ 40%]
tests/test_cli.py::test_commands_need_an_existing_project[dump] PASSED   [ 40%]
tests/test_cli.py::test_commands_need_an_existing_project[verify] PASSED [ 41%]
tests/test_cli.py::test_commands_need_an_existing_project[rebuild-state] PASSED [ 41%]
tests/test_cli.py::test_console_entrypoint PASSED                        [ 42%]
tests/test_contracts.py::test_all_five_schemas_meta_validate PASSED      [ 42%]
tests/test_contracts.py::test_every_schema_id_is_a_v1_id PASSED          [ 43%]
tests/test_contracts.py::test_the_contract_scripts_exit_zero[validate.py-RESULT: ALL GREEN] PASSED [ 43%]
tests/test_contracts.py::test_the_contract_scripts_exit_zero[fixture/replay.py-RESULT: REPLAY GREEN] PASSED [ 44%]
tests/test_contracts.py::test_event_types_match_the_payload_dispatch PASSED [ 44%]
tests/test_contracts.py::test_event_error_reports_the_branch_for_the_events_own_type PASSED [ 45%]
tests/test_contracts.py::test_formats_are_assertions PASSED              [ 45%]
tests/test_contracts.py::test_embedded_validators_resolve_their_defs PASSED [ 46%]
tests/test_contracts.py::test_json_path_formatting PASSED                [ 46%]
tests/test_contracts_v11.py::test_the_fixture_ledger_is_byte_identical PASSED [ 47%]
tests/test_contracts_v11.py::test_replay_prints_what_progress_recorded PASSED [ 47%]
tests/test_contracts_v11.py::test_the_schemas_still_say_v1 PASSED        [ 48%]
tests/test_contracts_v11.py::test_every_existing_instance_validates_under_v1_1 PASSED [ 48%]
tests/test_contracts_v11.py::test_fields_and_the_name_convention_give_the_same_verdict[architect.checks.c005] PASSED [ 49%]
tests/test_contracts_v11.py::test_fields_and_the_name_convention_give_the_same_verdict[architect.checks.c006] PASSED [ 49%]
tests/test_contracts_v11.py::test_c005_and_c006_verdicts_on_the_bound_model PASSED [ 50%]
tests/test_contracts_v11.py::test_c013_fails_a_capacity_param_whose_applies_to_dangles PASSED [ 50%]
tests/test_contracts_v11.py::test_a_capacity_param_with_the_new_fields_validates PASSED [ 50%]
tests/test_contracts_v11.py::test_replay_arbiter_and_projector_agree_on_a_branching_ledger PASSED [ 51%]
tests/test_contracts_v11.py::test_new_check_results_name_their_version_and_seq PASSED [ 51%]
tests/test_contracts_v11.py::test_a_v1_0_result_without_the_fields_is_keyed_by_the_head_of_its_time PASSED [ 52%]
tests/test_contracts_v11.py::test_the_v1_1_fields_are_optional_and_typed PASSED [ 52%]
tests/test_exit_fixture.py::test_fixture_round_trips_through_the_arbiter_and_replays_green PASSED [ 53%]
tests/test_exit_fixture.py::test_rebuild_state_after_the_fixture_ingest_reports_zero_diff PASSED [ 53%]
tests/test_exit_fixture.py::test_the_arbiters_head_model_is_what_replay_folds_before_and_after_a_rebuild PASSED [ 54%]
tests/test_model_fold.py::test_folding_the_fixture_gives_replays_final_model PASSED [ 54%]
tests/test_model_fold.py::test_each_op_kind PASSED                       [ 55%]
tests/test_model_fold.py::test_the_base_is_left_untouched_and_a_proposal_keeps_its_version PASSED [ 55%]
tests/test_model_fold.py::test_a_missing_target_is_an_error[update_element] PASSED [ 56%]
tests/test_model_fold.py::test_a_missing_target_is_an_error[remove_element] PASSED [ 56%]
tests/test_model_fold.py::test_an_op_without_the_fields_its_kind_needs_is_malformed[op0-element_type] PASSED [ 57%]
tests/test_model_fold.py::test_an_op_without_the_fields_its_kind_needs_is_malformed[op1-element] PASSED [ 57%]
tests/test_model_fold.py::test_an_op_without_the_fields_its_kind_needs_is_malformed[op2-element_id] PASSED [ 58%]
tests/test_model_fold.py::test_an_op_without_the_fields_its_kind_needs_is_malformed[op3-element_id] PASSED [ 58%]
tests/test_model_fold.py::test_an_op_without_the_fields_its_kind_needs_is_malformed[op4-link] PASSED [ 59%]
tests/test_model_fold.py::test_an_op_without_the_fields_its_kind_needs_is_malformed[op5-link_type] PASSED [ 59%]
tests/test_model_fold.py::test_a_child_is_a_copy_of_its_parent_under_a_new_id PASSED [ 60%]
tests/test_model_fold.py::test_a_genesis_is_empty PASSED                 [ 60%]
tests/test_projections.py::test_every_event_type_is_projected_or_explicitly_not PASSED [ 61%]
tests/test_projections.py::test_projections_never_write_events_or_reach_the_arbiter PASSED [ 61%]
tests/test_projections.py::test_fixture_projection_equals_what_replay_folds PASSED [ 62%]
tests/test_projections.py::test_every_fixture_version_is_materialized_and_valid PASSED [ 62%]
tests/test_projections.py::test_model_edges_are_kept_per_version PASSED  [ 63%]
tests/test_projections.py::test_projecting_event_by_event_equals_a_rebuild PASSED [ 63%]
tests/test_projections.py::test_batch_size_does_not_change_the_result PASSED [ 64%]
tests/test_projections.py::test_a_crash_mid_rebuild_resumes_to_the_same_hash PASSED [ 64%]
tests/test_projections.py::test_killing_the_projector_process_mid_rebuild_loses_nothing PASSED [ 65%]
tests/test_projections.py::test_dropping_every_proj_table_loses_nothing PASSED [ 65%]
tests/test_projections.py::test_a_claim_reads_as_it_stood_at_an_earlier_seq PASSED [ 66%]
tests/test_projections.py::test_claim_detail_has_history_and_provenance PASSED [ 66%]
tests/test_projections.py::test_why_traces_an_element_to_requirements_decisions_claims_and_sources PASSED [ 66%]
tests/test_projections.py::test_refuting_a_premise_compromises_everything_derived_from_it PASSED [ 67%]
tests/test_projections.py::test_projection_status_reports_the_lag PASSED [ 67%]
tests/test_projections.py::test_the_worker_wakes_on_the_arbiters_notification PASSED [ 68%]
tests/test_projections.py::test_the_worker_polls_when_no_notification_arrives PASSED [ 68%]
tests/test_projections.py::test_the_arbiter_notifies_with_the_project_id PASSED [ 69%]
tests/test_projections.py::test_objections_decisions_waivers_checks_and_timeline PASSED [ 69%]
tests/test_projections.py::test_a_version_created_from_a_parent_starts_as_the_parents_model PASSED [ 70%]
tests/test_projections.py::test_an_event_that_cannot_be_folded_stops_the_projector_in_front_of_it PASSED [ 70%]
tests/test_projections.py::test_projection_is_per_project PASSED         [ 71%]
tests/test_projections.py::test_reads_of_things_that_are_not_projected_are_404 PASSED [ 71%]
tests/test_refusals.py::test_documented_claim_without_evidence PASSED    [ 72%]
tests/test_refusals.py::test_load_bearing_assumption_without_verification_plan PASSED [ 72%]
tests/test_refusals.py::test_promotion_to_measured_needs_an_experiment_as_cause PASSED [ 73%]
tests/test_refusals.py::test_status_change_with_the_wrong_from PASSED    [ 73%]
tests/test_refusals.py::test_waiver_signed_by_an_agent PASSED            [ 74%]
tests/test_refusals.py::test_evidence_citing_an_uningested_source PASSED [ 74%]
tests/test_refusals.py::test_patch_on_a_stale_base PASSED                [ 75%]
tests/test_refusals.py::test_resolving_an_objection_that_was_never_raised PASSED [ 75%]
tests/test_refusals.py::test_objection_without_a_falsifiable_test PASSED [ 76%]
tests/test_refusals.py::test_duplicate_source PASSED                     [ 76%]
tests/test_refusals.py::test_proposed_claim_must_be_a_valid_claim PASSED [ 77%]
tests/test_refusals.py::test_claim_id_mismatch PASSED                    [ 77%]
tests/test_refusals.py::test_duplicate_claim_id PASSED                   [ 78%]
tests/test_refusals.py::test_claim_from_an_unknown_proposal PASSED       [ 78%]
tests/test_refusals.py::test_a_patch_proposal_is_not_a_claim_proposal PASSED [ 79%]
tests/test_refusals.py::test_claim_and_patch_proposals_with_distinct_ids_are_accepted PASSED [ 79%]
tests/test_refusals.py::test_a_proposal_id_is_used_once_across_both_kinds PASSED [ 80%]
tests/test_refusals.py::test_formats_inside_embedded_objects_are_enforced PASSED [ 80%]
tests/test_refusals.py::test_status_change_of_an_unknown_claim PASSED    [ 81%]
tests/test_refusals.py::test_status_change_with_an_unknown_cause_event PASSED [ 81%]
tests/test_refusals.py::test_a_cause_event_from_another_project_is_unknown PASSED [ 82%]
tests/test_refusals.py::test_promotion_to_observed_is_guarded_too PASSED [ 82%]
tests/test_refusals.py::test_retracting_an_unknown_claim PASSED          [ 83%]
tests/test_refusals.py::test_second_genesis_version PASSED               [ 83%]
tests/test_refusals.py::test_version_created_with_a_committed_parent_is_accepted PASSED [ 83%]
tests/test_refusals.py::test_version_created_with_an_unknown_parent PASSED [ 84%]
tests/test_refusals.py::test_a_parent_version_from_another_project_is_unknown PASSED [ 84%]
tests/test_refusals.py::test_patch_must_be_a_valid_model_patch PASSED    [ 85%]
tests/test_refusals.py::test_patch_base_mismatch PASSED                  [ 85%]
tests/test_refusals.py::test_patch_proposed_on_a_stale_base PASSED       [ 86%]
tests/test_refusals.py::test_patch_before_any_model_version PASSED       [ 86%]
tests/test_refusals.py::test_patch_committed_from_an_unknown_proposal PASSED [ 87%]
tests/test_refusals.py::test_proposed_check_must_be_a_valid_check PASSED [ 87%]
tests/test_refusals.py::test_resolving_an_objection_twice PASSED         [ 88%]
tests/test_refusals.py::test_waiver_signed_by_the_system PASSED          [ 88%]
tests/test_refusals.py::test_experiment_with_an_uncommitted_result_claim PASSED [ 89%]
tests/test_refusals.py::test_decision_citing_an_uncommitted_claim PASSED [ 89%]
tests/test_refusals.py::test_merge_revert_must_cite_a_committed_merge PASSED [ 90%]
tests/test_refusals.py::test_schema_gate[change0-$.type] PASSED          [ 90%]
tests/test_refusals.py::test_schema_gate[change1-$.idempotency_key] PASSED [ 91%]
tests/test_refusals.py::test_schema_gate[change2-$] PASSED               [ 91%]
tests/test_refusals.py::test_schema_gate[change3-$.actor.kind] PASSED    [ 92%]
tests/test_refusals.py::test_schema_gate[change4-$.event_id] PASSED      [ 92%]
tests/test_refusals.py::test_schema_gate[change5-$.ts] PASSED            [ 93%]
tests/test_refusals.py::test_schema_gate[change6-$.ts] PASSED            [ 93%]
tests/test_refusals.py::test_schema_gate[change7-$.session_id] PASSED    [ 94%]
tests/test_refusals.py::test_schema_gate[change8-$] PASSED               [ 94%]
tests/test_refusals.py::test_schema_gate[change9-$.payload] PASSED       [ 95%]
tests/test_refusals.py::test_schema_gate_runs_before_the_rules PASSED    [ 95%]
tests/test_refusals.py::test_project_mismatch PASSED                     [ 96%]
tests/test_refusals.py::test_candidate_must_be_an_object PASSED          [ 96%]
tests/test_refusals.py::test_a_valid_patch_is_accepted_and_its_result_becomes_the_head_model PASSED [ 97%]
tests/test_refusals.py::test_a_version_created_from_a_parent_copies_the_parents_model PASSED [ 97%]
tests/test_refusals.py::test_patch_whose_target_is_not_in_the_model PASSED [ 98%]
tests/test_refusals.py::test_patch_that_would_not_leave_a_valid_model PASSED [ 98%]
tests/test_refusals.py::test_reusing_a_model_version_id PASSED           [ 99%]
tests/test_refusals.py::test_a_patch_op_without_the_fields_its_kind_needs PASSED [ 99%]
tests/test_refusals.py::test_a_version_id_the_system_model_cannot_carry PASSED [100%]

============================= 206 passed in 30.22s =============================
```

Removing the per-project lock makes exit test 4 fail with a `(project_id, seq)` unique
violation, so that test does depend on the lock.

### Fixture integrity

SHA-256, checked on disk and again on the committed blobs; the files are stored byte-for-byte
(`phase0-contracts/** -text` in `.gitattributes`). `fixture_ledger.jsonl` and
`build_fixture.py` are byte-identical to the files the owner supplied. `replay.py` changed in
the v1.0 edit (format checking, P-2) and again in v1.1 (a model per version, P-7); before
v1.0 it was `dbe60b992f59eafbef772557243b2fcbc521e470963eaf812bab2727b9ef080a`, at v1.0
`e934263c45f898a109678614461e5aafd8b28f57541e84fa46a98fd391757f2b`.

```
e62ec17232863134b46a50dde6e03df64f0d3f67923d07c480d4f591f8edaf4b  fixture_ledger.jsonl   (40 lines, LF)
df59e9681b2dfac51593dfc4ea632cac36d9e69ae4671d6f3f8c18ac81cd5693  replay.py
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

## M1.1: the Arbiter folds the model

Before M1.1 the Arbiter accepted a model patch whose result was not a valid System Model, and
a reused model `version_id`: invalid state could enter the ledger, and `replay.py` refused
ledgers the Arbiter had committed. Now:

- **One fold** (`src/architect/model_fold.py`): pure functions that apply a patch's ops with
  `replay.py`'s semantics. The Arbiter and the projector both use it.
- **The Arbiter materializes every version** in `arb_model_versions.model` (one row per
  version, not only the head, because a `parent` may be any committed version). On
  `model.patch_proposed` and `model.patch_committed` it applies the patch to the head model
  and validates the result; on `model.version_created` it validates the genesis or the copy
  of the parent. `architect rebuild-state` rebuilds the models, and the fixture rebuilds with
  an empty diff.
- **A version created from a parent is a copy of the parent's model** (P-4 semantics;
  `replay.py` empties it instead, contracts-PROPOSALS.md P-7, deferred to v1.1).
- **Owner-authorized test-data change.** `tests/builders.py::patch()` used to add
  `{"name": "Fencer"}` under the element type `component`, which is not a valid System Model,
  so the M1 sample ledger encoded the bug. It now adds a complete component under
  `components` (`id` = `cmp_00FENCER<last two characters of the base version>`, kind
  `service`, stateless, no requirement refs). The owner authorized this one change in the M2
  prompt (part A, item 7); no test function changed.

| # | Exit test | Result | Evidence |
| --- | --- | --- | --- |
| A1 | `PATCH_TARGET_MISSING`, `INVALID_MODEL_RESULT`, `DUPLICATE_VERSION_ID` refused, nothing written | green | `test_refusals.py::test_patch_whose_target_is_not_in_the_model`, `::test_patch_that_would_not_leave_a_valid_model`, `::test_reusing_a_model_version_id` |
| A2 | A valid patch is accepted; a version created from a parent copies the parent | green | `test_refusals.py::test_a_valid_patch_is_accepted_and_its_result_becomes_the_head_model`, `::test_a_version_created_from_a_parent_copies_the_parents_model` |
| A3 | The fixture ingests with zero rejections; rebuild-state zero diff including the head model | green | `test_exit_fixture.py::test_fixture_round_trips_through_the_arbiter_and_replays_green`, `::test_rebuild_state_after_the_fixture_ingest_reports_zero_diff`, `::test_the_arbiters_head_model_is_what_replay_folds_before_and_after_a_rebuild` |

A database created before M1.1 has an `arb_model_versions` without the `model` column:
drop the `arb_*` tables and run `architect rebuild-state` per project (`schema.sql` creates
tables; it does not migrate them).

## M2: projections, the read side

`arb_*` is the Arbiter's minimal validation state; `proj_*` are read models for queries.
Both are disposable projections of the ledger. The projector never writes an event and never
calls the Arbiter.

### What landed

- **The fold** (`projections.py`): one handler per projected event type, writing the eleven
  `proj_*` tables: `proj_sources`, `proj_claims`, `proj_claim_status_history`,
  `proj_model_versions`, `proj_edges`, `proj_objections`, `proj_decisions`, `proj_waivers`,
  `proj_checks`, `proj_session_timeline`, plus the projector's `proj_cursors`. Model versions
  go through `model_fold`, and every stored version is validated against
  `system_model.schema.json`; a failure raises `ProjectionError`, the projector commits the
  events before the offending one and stops in front of it, exit 1.
- **The projector** (`projector.py`, `architect project`): one projection, `read_models`,
  folded per project in `seq` order in batches (default 200), each batch one transaction that
  locks the project's `proj_cursors` row, folds, and advances it. A crash rolls the batch
  back with its cursor. It wakes on the Arbiter's `NOTIFY architect_events` (the one M1 code
  change: `pg_notify` in the commit transaction, delivered on commit) and polls every 2 s
  regardless. `--once` catches up and exits.
- **Refutation propagation**: `proj_claims.premise_compromised` is recomputed (recursive CTE
  over `DERIVED_FROM` edges) whenever a claim with premises is committed or a status enters
  or leaves `refuted`/`retracted`. The as-of query recomputes it from the history.
- **Read API** (`readmodel.py`, `api.py`), all GET, reading `proj_*` only:
  `/models/{version_id}`, `/models/head`, `/claims?status=&load_bearing=&as_of_seq=`,
  `/claims/{claim_id}` (status history, evidence sources, premise chain),
  `/elements/{element_id}/why`, `/projections/status`.
- **CLI**: `architect project [--once] [--poll-seconds] [--batch-size]` and
  `architect rebuild-projections --project P` (deletes the project's `proj_*` rows and the
  cursor, folds the ledger again, prints the sha256 content hash of every row).

### Decisions worth knowing

- **One projection, one cursor.** All `proj_*` tables advance together, so a read that joins
  them (the why-trace) sees one consistent seq. `proj_cursors.projection` is there for the
  projections later milestones add.
- **`proj_claims.claim` is the claim as committed**; `status` is current. Rewriting the
  claim's own `status` would make some claims schema-invalid (a `measured` claim without
  evidence), so the two are kept apart.
- **Model edges are written once per version** with `version_id` set; claim and decision
  edges have `version_id` NULL. `ord` numbers the edges one event produces, so two evidence
  entries for one source stay two edges.
- **Where the read side differs from `replay.py`**: `claim.retracted` sets the status to
  `retracted` (`replay.py` ignores the event); a version created from a parent copies the
  parent (P-7); `remove_element` of a missing target is an error (the Arbiter refuses it
  anyway). The fixture exercises none of these, so B1 holds.
- **The content hash** covers every `proj_*` row of the project (not the cursor), each row as
  canonical JSON, sorted, so physical row order and batch size do not matter.
- **Not projected (listed, so a new event type is a decision)**: `claim.proposed`,
  `model.patch_proposed`, `entity.merged`, `entity.merge_reverted`, `experiment.recorded`,
  `budget.updated`.

### Exit tests

| # | Exit test | Result | Evidence (`tests/test_projections.py`) |
| --- | --- | --- | --- |
| B1 | Head model and claim statuses equal what `replay.py` folds | green | `test_fixture_projection_equals_what_replay_folds` |
| B2 | All four fixture versions materialized and valid; `flw_FIXINGR001` gains `input_validation` in V3 | green | `test_every_fixture_version_is_materialized_and_valid` |
| B3 | Event-by-event projection hashes like a rebuild | green | `test_projecting_event_by_event_equals_a_rebuild`, `test_batch_size_does_not_change_the_result` |
| B4 | Crash safety: hook raise and subprocess kill mid-rebuild | green | `test_a_crash_mid_rebuild_resumes_to_the_same_hash`, `test_killing_the_projector_process_mid_rebuild_loses_nothing` |
| B5 | Drop every `proj_*` table, rebuild, same hash | green | `test_dropping_every_proj_table_loses_nothing` |
| B6 | `clm_FIXPAYLOAD1` is `assumed` at seq 31 and `measured` at 32 | green | `test_a_claim_reads_as_it_stood_at_an_earlier_seq` |
| B7 | Why-trace of `cmp_FIXSHARD01` | green | `test_why_traces_an_element_to_requirements_decisions_claims_and_sources` |
| B8 | Refutation propagates transitively and only to dependents | green | `test_refuting_a_premise_compromises_everything_derived_from_it` |
| B9 | Zero lag after catch-up | green | `test_projection_status_reports_the_lag` |

Also covered: the NOTIFY wake-up and the polling fallback, the per-version edges, the other
read models, the 404s of the read API, and a legacy event that cannot be folded (inserted
with raw SQL, since the Arbiter refuses it now) stopping the projector in front of it.

## M3: the checks engine

Checks are the deterministic core of the harness (spec §14, principle P4): if code can decide
a property, code decides it, and that outranks any model opinion.

### What landed

- **Pure checks** (`src/architect/checks/c001.py` to `c013.py`):
  `check(model, ctx, params) -> CheckOutcome`, no database, network, clock or randomness;
  `tests/test_architecture.py::test_checks_import_nothing_impure` enforces the import rule.
  Shared helpers: `outcome.py` (pass/fail/error/skipped and `settle`), `units.py`
  (throughput, ratio, time; an unknown unit is an error naming the parameter), `graph.py`
  (inbound, ingress, reachable_from_ingress, sync_closure, replicas, the P-9 capacity-param
  convention), `waivers.py` (the four target_ref forms), `context.py` (`CheckContext`).
- **The catalog** (`checks/catalog.json`, version 1.0.0, validated against
  `check_catalog.schema.json` on load) and the registry (`catalog.py`): one entry per
  check, `implementation.ref = architect.checks.c0NN`. An entry nobody implements (the
  fixture's proposed C-031) loads and evaluates to skipped / not_implemented.
- **The runner** (`checks/runner.py`): catches the projector up, builds the context as of a
  seq from the read models (requirements and claim statuses from the status history,
  waivers signed by the seq), runs the catalog, and records one `check.result` per check
  through the Arbiter as actor `check-runner`. Evidence always carries `model_version`,
  `as_of_seq`, `catalog_version`, `check_version`, `params` and `inputs_hash`
  (sha256 over the model, the context fields the check declares in `USES`, its params and
  its version). The idempotency key is `check:{version}:{check_id}:{check_version}:{hash}`,
  so a re-run with unchanged inputs writes nothing and returns the recorded result.
- **The gate**: IMPLEMENTATION_READY from the results on record for (version, as_of_seq).
  Blocking: a critical check with no recorded result (`not_evaluated`), a critical check
  that failed or errored (an error is "cannot verify", not a pass), or a critical objection
  open as of the seq whose element_refs touch the version. Major and minor findings are
  warnings.
- **CLI**: `architect check --project P --version V [--as-of-seq N] [--dry-run]` and
  `architect gate ...` (runs, records, prints the verdict; exit 2 when BLOCKED).
- **API**: `POST /v1/projects/{pid}/models/{version_id}/checks[?as_of_seq=]` (run and
  record), `GET .../checks` (latest recorded result per check), `GET .../gate[?as_of_seq=]`.
- **`proj_checks` alignment**: a result is keyed by `evidence.model_version` when the runner
  names it; a result without it (the fixture's own three) is keyed by the head of its time,
  as before.

### Decisions worth knowing

- **The gate's default seq is the most recent battery's.** Recording a battery appends
  events, so the ledger's latest seq is always past the battery's `as_of_seq`; the gate
  without an explicit seq judges the latest battery recorded for the version. `architect
  gate` and the POST endpoint record first, so they always find one.
- **Status precision**: `error` is reserved for inputs the
  check cannot evaluate (a missing capacity param, an unknown unit); a rule that is
  violated is `fail`; nothing applicable is `skipped` with a reason.
- **Waivers apply in every check**: `C-00N` waives the whole check, `C-00N:<element>` one
  element; requirement ids waive C-001 and claim ids waive C-009. A waived element is in
  `evidence.waived`, never in `element_refs`. Only waivers signed by `as_of_seq` count.
- **`replicas` default to 1 and say so** (`evidence.assumed`), which is why the fixture's
  request path reads as single points of failure.
- **The runner reads projections, and catches the projector up first**, so a check never
  judges stale read models; after recording it catches up again so the GET endpoints see
  the results.

### The fixture's verdict (the answer key)

`mv_FIXV000003` at the latest seq (39):

| Check | Status | element_refs / key evidence |
| --- | --- | --- |
| C-001 | pass | both requirements satisfied |
| C-002 | pass | |
| C-003 | pass | |
| C-004 | pass | |
| C-005 | error | `cmp_FIXROUTER1`, `cmp_FIXSHARD01`, `cmp_FIXWAL0001`: no `<id>.max_qps` params |
| C-006 | skipped | no availability SLOs |
| C-007 | fail | `cmp_FIXLEASE01`: rebuildable without `recovery.rto_s` / `recovery.path` |
| C-008 | pass | `flw_FIXINGR001` crosses `tb-cluster` with validation, encryption, service_identity |
| C-009 | pass | `clm_FIXPAYLOAD1` is measured |
| C-010 | fail | `cmp_FIXROUTER1`, `cmp_FIXSHARD01`, `cmp_FIXWAL0001` (replicas assumed 1); `cmp_FIXLEASE01` waived by `wvr_FIXSPOF001` |
| C-011 | skipped | no async/stream interfaces |
| C-012 | skipped | no queues, fan-in or async flows |
| C-013 | pass | |

Gate: BLOCKED, reasons exactly {C-005 error, C-007 fail}; C-010 is a warning. `replay.py`
prints ALLOWED from four checks; the full battery deliberately reaches a different verdict,
and neither is to be "fixed" toward the other. The C-007 finding is real: the Lease
Manager's fencing epochs must survive a restart, and the design never says how its state is
rebuilt.

### Exit tests

| # | Exit test | Result | Evidence |
| --- | --- | --- | --- |
| E1 | Catalog validates; registry and catalog match; C-031 validates and records not_implemented | green | `test_checks.py::test_the_bundled_catalog_validates_against_the_contract`, `::test_registry_and_catalog_name_the_same_checks`, `::test_a_proposed_check_loads_and_records_not_implemented` |
| E2 | Per-check unit tests: pass, fail, empty case; C-005/C-006 error and unknown unit; waiver forms; as-of filtering | green | `test_checks.py::test_c001_*` to `::test_c013_*`, `::test_waiver_target_ref_forms`; `test_checks_runner.py::test_a_waiver_signed_after_the_seq_does_not_count` |
| E3 | Fixture golden table on `mv_FIXV000003`; gate BLOCKED with exactly C-005 error and C-007 fail | green | `test_checks_runner.py::test_the_battery_on_v3_matches_the_answer_key`; also on the reference model in `test_checks.py::test_the_catalog_on_the_reference_model_gives_the_expected_table` |
| E4 | `mv_FIXV000002`: C-008 fail `[flw_FIXINGR001]`, missing input_validation and encryption_in_transit | green | `::test_v2_fails_c008_like_the_ledger_recorded` |
| E5 | Time travel: C-009 fail at seq 19, pass at latest | green | `::test_time_travel_sees_the_assumption_still_open` |
| E6 | Genesis at seq 14: C-001 fail both requirements, C-013 pass | green | `::test_the_genesis_covers_no_requirement` |
| E7 | Repair patch: C-005 pass, C-007 pass, C-010 warning, gate ALLOWED | green | `::test_a_repair_patch_opens_the_gate` |
| E8 | Recording and cache: 13 events from `check-runner`; re-run writes nothing; headroom 2.0 at version 2 records one new C-005 fail; `proj_checks` keyed by version | green | `::test_results_are_recorded_once_per_inputs` |
| E9 | Purity: import rule; identical outcomes on identical inputs | green | `test_architecture.py::test_checks_import_nothing_impure`, `test_checks.py::test_running_every_check_twice_on_the_same_inputs_is_identical` |
| E10 | API: POST checks, GET checks, GET gate give the E3 verdicts | green | `::test_the_check_endpoints` |
| | Every pre-existing test still green | green | 193 passed in the run above |

## Contracts v1.1 (module C2)

The sanctioned v1.0 to v1.1 change set, authorised by the owner on 2026-10-03. A minor
version: it only adds optional fields and documents rules, so every document valid under
v1.0 is valid under v1.1 (`test_contracts_v11.py::test_every_existing_instance_validates_under_v1_1`
checks every instance the repo holds); the schema `$id`s keep `/v1/`; the fixture ledger is
byte-identical and `replay.py` prints the same output on it. `phase0-contracts/CHANGELOG.md`
is the record.

| Proposal | In the contracts | In the code |
| --- | --- | --- |
| P-7 | `replay.py` keeps a model per version; a version created from a parent is a copy of the parent's | nothing: the Arbiter and the projector already folded so |
| P-8 | README and the `ModelPatchProposed`, `ModelPatchCommitted`, `ModelVersionCreated` descriptions state the fold rules with the shipped codes | nothing |
| P-9 | `CapacityParam.applies_to` and `.metric` (optional) | `graph.capacity_binding` reads the fields first, the naming convention as a deprecated fallback; C-005/C-006 record `evidence.deprecated` when they relied on it; C-013 resolves `applies_to` |
| P-10 | `CheckResult.model_version` and `.as_of_seq` (optional) | the runner sets both on every result (evidence copies kept); `proj_checks` keys by the field first, then `evidence.model_version`, then the head of the time |

Multi-head branching is not in v1.1; the README says it is planned for v1.2 with M11.

| # | Exit test | Result | Evidence |
| --- | --- | --- | --- |
| X1 | Fixture checksum unchanged; `validate.py` ALL GREEN; `replay.py` REPLAY GREEN with the output PROGRESS.md records | green | `test_contracts_v11.py::test_the_fixture_ledger_is_byte_identical`, `::test_replay_prints_what_progress_recorded`; CI steps |
| X2 | Every existing instance validates under v1.1 (fixture and sample ledgers with their embedded objects and folded models, the builders, the catalog; collected programmatically) | green | `::test_every_existing_instance_validates_under_v1_1` |
| X3 | P-7 three-way agreement on a branching ledger: `replay.py`, the Arbiter and the projector agree on every version; the child is a copy of its parent | green | `::test_replay_arbiter_and_projector_agree_on_a_branching_ledger` |
| X4 | P-9: fields and the name convention give identical verdicts, only the convention run records `evidence.deprecated`; C-013 fails a dangling `applies_to` | green | `::test_fields_and_the_name_convention_give_the_same_verdict`, `::test_c013_fails_a_capacity_param_whose_applies_to_dangles` |
| X5 | P-10: new results carry `model_version` and `as_of_seq` and validate; `proj_checks` keys by them; the M3 verdict table and gate are unchanged | green | `::test_new_check_results_name_their_version_and_seq`, `::test_a_v1_0_result_without_the_fields_is_keyed_by_the_head_of_its_time` |
| X6 | Every pre-existing test green, none modified | green | 206 passed in the run above |

## Not verified on Windows

The database-backed tests have only run on Linux in CI. The Windows development machine has
had no reachable PostgreSQL since the fixture landed, so they have not run there against the
fixture, the v1.0 rules, M1.1, M2 or M3. What does run on Windows is green: `validate.py`,
`replay.py`, `ruff check .`, and the 51 tests that need no database.

## Open

1. **Run the suite once on Windows** against a local PostgreSQL 16, to confirm what CI shows.
2. **contracts-PROPOSALS.md P-6** (the contract scripts' file encoding) stays open; P-7 to
   P-10 were applied in v1.1. Multi-head branching is planned for contracts v1.2 with M11.
