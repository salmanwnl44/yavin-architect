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
| M4 model gateway | **done**: the one path to any LLM; all twelve exit tests green in CI on the mock provider |
| M5 ingestion + extraction | **done**: sources, segments, two-pass extraction, quarantine, grades, injection suite; all eleven exit tests green in CI |
| M6 design sessions | **done**: Temporal workflow over the nine phases, Architect agent v1, Context Compiler; all twelve exit tests green in CI, including the real-dev-server job |
| M7 console, golden task #1 | **built**: gate decisions and extend, seed models, the console, golden task gt-001 and its runner; exit tests P1 to P6 green in CI. M7-live Part 1 (per-element waivers, L3 partial baseline, external kill with heartbeats) merged. **Phase 1 exit test: PENDING LIVE RUN** (M7-live Part 2) |
| M8 knowledge graph + retrieval | **done**: write-ahead gateway calls, the graph projection, GraphStore (AGE and SQL) and VectorIndex (pgvector and exact) with parity tests, entity resolution, embeddings, hybrid retrieval, communities; exit tests K0 to K9 green in CI |

M1 is complete, C1 froze the contracts at v1.0, M1.1 closed the Arbiter's model gap, M2 built
the read side, M3 built the checks engine, C2 moved the contracts to v1.1 (a minor version:
optional fields and documented rules, every v1.0 document still valid), M4 built the gateway,
M5 the ingestion pipeline and M6 the session engine. CI runs the whole suite on `postgres:16`
with a Temporal dev server beside it: see the M7 section for the output. The live half of M7
(the baseline and the Phase 1 exit test) is pending: see "M7-live" below. Phase 2 began
with M8, the knowledge plane; CI now runs the suite twice, on Postgres 16 with Apache AGE
and pgvector and on a plain Postgres 16.

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

CI output, 2026-10-03, GitHub Actions run 37110051059 on commit `fce6f25`
(https://github.com/salmanwnl44/yavin-architect/actions/runs/37110051059): ubuntu-latest,
Python 3.11.16, PostgreSQL 16.15 (`postgres:16`), contracts v1.1, M1.1 to M5. Every step
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
collecting ... collected 280 items / 3 deselected / 277 selected

tests/test_api.py::test_healthz PASSED                                   [  0%]
tests/test_api.py::test_create_project PASSED                            [  0%]
tests/test_api.py::test_submit_returns_201_then_200_replayed PASSED      [  1%]
tests/test_api.py::test_the_whole_sample_session_commits PASSED          [  1%]
tests/test_api.py::test_events_page_in_seq_order PASSED                  [  1%]
tests/test_api.py::test_page_parameters_are_validated PASSED             [  2%]
tests/test_api.py::test_get_event_by_id PASSED                           [  2%]
tests/test_api.py::test_head_summarizes_the_arbiter_state PASSED         [  2%]
tests/test_api.py::test_head_counts_open_objections PASSED               [  3%]
tests/test_api.py::test_unknown_project_is_404_everywhere PASSED         [  3%]
tests/test_arbiter.py::test_every_event_type_has_a_rule PASSED           [  3%]
tests/test_arbiter.py::test_first_commit_is_seq_zero_without_prev_hash PASSED [  4%]
tests/test_arbiter.py::test_arbiter_mints_event_id_and_ts_when_absent PASSED [  4%]
tests/test_arbiter.py::test_client_event_id_and_ts_are_kept_verbatim PASSED [  5%]
tests/test_arbiter.py::test_seq_is_dense_and_each_event_hashes_its_predecessor PASSED [  5%]
tests/test_arbiter.py::test_projects_are_sequenced_independently PASSED  [  5%]
tests/test_arbiter.py::test_committed_events_read_back_identically PASSED [  6%]
tests/test_arbiter.py::test_idempotent_resubmission_returns_the_original_event PASSED [  6%]
tests/test_arbiter.py::test_a_retry_wins_over_rules_that_its_first_commit_changed PASSED [  6%]
tests/test_arbiter.py::test_parallel_submissions_get_dense_seq_and_a_valid_chain PASSED [  7%]
tests/test_arbiter.py::test_parallel_retries_of_one_candidate_commit_once PASSED [  7%]
tests/test_arbiter.py::test_raw_sql_cannot_change_or_remove_a_committed_event[UPDATE events SET payload = '{}'::jsonb] PASSED [  7%]
tests/test_arbiter.py::test_raw_sql_cannot_change_or_remove_a_committed_event[UPDATE events SET seq = seq + 100] PASSED [  8%]
tests/test_arbiter.py::test_raw_sql_cannot_change_or_remove_a_committed_event[DELETE FROM events] PASSED [  8%]
tests/test_arbiter.py::test_raw_sql_cannot_change_or_remove_a_committed_event[TRUNCATE events] PASSED [  9%]
tests/test_arbiter.py::test_a_failure_before_commit_rolls_back_event_and_state PASSED [  9%]
tests/test_arbiter.py::test_the_hook_runs_after_both_writes PASSED       [  9%]
tests/test_arbiter.py::test_candidates_must_not_carry_arbiter_stamped_fields[seq] PASSED [ 10%]
tests/test_arbiter.py::test_candidates_must_not_carry_arbiter_stamped_fields[prev_hash] PASSED [ 10%]
tests/test_arbiter.py::test_unknown_project_is_refused PASSED            [ 10%]
tests/test_arbiter.py::test_event_id_is_unique_across_projects PASSED    [ 11%]
tests/test_arbiter.py::test_unstorable_json_is_a_typed_rejection PASSED  [ 11%]
tests/test_architecture.py::test_only_the_arbiter_writes_events PASSED   [ 11%]
tests/test_architecture.py::test_no_code_path_updates_or_deletes_events PASSED [ 12%]
tests/test_architecture.py::test_checks_import_nothing_impure PASSED     [ 12%]
tests/test_checks.py::test_the_bundled_catalog_validates_against_the_contract PASSED [ 12%]
tests/test_checks.py::test_registry_and_catalog_name_the_same_checks PASSED [ 13%]
tests/test_checks.py::test_a_proposed_check_loads_and_records_not_implemented PASSED [ 13%]
tests/test_checks.py::test_an_invalid_catalog_is_refused PASSED          [ 14%]
tests/test_checks.py::test_c001_requirement_coverage PASSED              [ 14%]
tests/test_checks.py::test_c002_no_orphans PASSED                        [ 14%]
tests/test_checks.py::test_c003_interface_binding PASSED                 [ 15%]
tests/test_checks.py::test_c004_requirement_refs_match_links PASSED      [ 15%]
tests/test_checks.py::test_c005_capacity_headroom PASSED                 [ 15%]
tests/test_checks.py::test_c006_availability_composition PASSED          [ 16%]
tests/test_checks.py::test_c007_stateful_durability PASSED               [ 16%]
tests/test_checks.py::test_c008_trust_boundaries_and_sensitive_data PASSED [ 16%]
tests/test_checks.py::test_c009_open_load_bearing_assumptions PASSED     [ 17%]
tests/test_checks.py::test_c010_single_points_of_failure PASSED          [ 17%]
tests/test_checks.py::test_c011_idempotent_async_interfaces PASSED       [ 18%]
tests/test_checks.py::test_c012_backpressure PASSED                      [ 18%]
tests/test_checks.py::test_c013_referential_integrity PASSED             [ 18%]
tests/test_checks.py::test_waiver_target_ref_forms PASSED                [ 19%]
tests/test_checks.py::test_units PASSED                                  [ 19%]
tests/test_checks.py::test_graph_helpers PASSED                          [ 19%]
tests/test_checks.py::test_outcomes_say_what_they_must PASSED            [ 20%]
tests/test_checks.py::test_the_catalog_on_the_reference_model_gives_the_expected_table PASSED [ 20%]
tests/test_checks.py::test_running_every_check_twice_on_the_same_inputs_is_identical PASSED [ 20%]
tests/test_checks.py::test_inputs_hash_follows_what_a_check_reads PASSED [ 21%]
tests/test_checks_runner.py::test_the_battery_on_v3_matches_the_answer_key PASSED [ 21%]
tests/test_checks_runner.py::test_v2_fails_c008_like_the_ledger_recorded PASSED [ 22%]
tests/test_checks_runner.py::test_time_travel_sees_the_assumption_still_open PASSED [ 22%]
tests/test_checks_runner.py::test_the_genesis_covers_no_requirement PASSED [ 22%]
tests/test_checks_runner.py::test_the_gate_counts_an_objection_open_as_of_the_seq PASSED [ 23%]
tests/test_checks_runner.py::test_a_gate_without_recorded_results_is_blocked_as_not_evaluated PASSED [ 23%]
tests/test_checks_runner.py::test_a_repair_patch_opens_the_gate PASSED   [ 23%]
tests/test_checks_runner.py::test_results_are_recorded_once_per_inputs PASSED [ 24%]
tests/test_checks_runner.py::test_a_dry_run_records_nothing PASSED       [ 24%]
tests/test_checks_runner.py::test_an_unknown_version_is_refused PASSED   [ 24%]
tests/test_checks_runner.py::test_a_waiver_signed_after_the_seq_does_not_count PASSED [ 25%]
tests/test_checks_runner.py::test_check_and_gate_commands PASSED         [ 25%]
tests/test_checks_runner.py::test_the_check_endpoints PASSED             [ 25%]
tests/test_cli.py::test_ingest_then_dump_round_trips_the_ledger PASSED   [ 26%]
tests/test_cli.py::test_ingest_is_idempotent PASSED                      [ 26%]
tests/test_cli.py::test_a_dump_can_be_ingested_into_a_fresh_database_project PASSED [ 27%]
tests/test_cli.py::test_ingest_stops_at_the_first_rejection_with_the_typed_error PASSED [ 27%]
tests/test_cli.py::test_dump_to_stdout PASSED                            [ 27%]
tests/test_cli.py::test_verify_reports_a_healthy_chain PASSED            [ 28%]
tests/test_cli.py::test_verify_reports_a_tampered_event PASSED           [ 28%]
tests/test_cli.py::test_rebuild_state_reports_zero_diff_on_a_healthy_ledger PASSED [ 28%]
tests/test_cli.py::test_rebuild_state_repairs_and_reports_drift PASSED   [ 29%]
tests/test_cli.py::test_dropping_the_state_tables_loses_nothing PASSED   [ 29%]
tests/test_cli.py::test_rebuild_only_touches_its_own_project PASSED      [ 29%]
tests/test_cli.py::test_commands_need_an_existing_project[dump] PASSED   [ 30%]
tests/test_cli.py::test_commands_need_an_existing_project[verify] PASSED [ 30%]
tests/test_cli.py::test_commands_need_an_existing_project[rebuild-state] PASSED [ 31%]
tests/test_cli.py::test_console_entrypoint PASSED                        [ 31%]
tests/test_contracts.py::test_all_five_schemas_meta_validate PASSED      [ 31%]
tests/test_contracts.py::test_every_schema_id_is_a_v1_id PASSED          [ 32%]
tests/test_contracts.py::test_the_contract_scripts_exit_zero[validate.py-RESULT: ALL GREEN] PASSED [ 32%]
tests/test_contracts.py::test_the_contract_scripts_exit_zero[fixture/replay.py-RESULT: REPLAY GREEN] PASSED [ 32%]
tests/test_contracts.py::test_event_types_match_the_payload_dispatch PASSED [ 33%]
tests/test_contracts.py::test_event_error_reports_the_branch_for_the_events_own_type PASSED [ 33%]
tests/test_contracts.py::test_formats_are_assertions PASSED              [ 33%]
tests/test_contracts.py::test_embedded_validators_resolve_their_defs PASSED [ 34%]
tests/test_contracts.py::test_json_path_formatting PASSED                [ 34%]
tests/test_contracts_v11.py::test_the_fixture_ledger_is_byte_identical PASSED [ 35%]
tests/test_contracts_v11.py::test_replay_prints_what_progress_recorded PASSED [ 35%]
tests/test_contracts_v11.py::test_the_schemas_still_say_v1 PASSED        [ 35%]
tests/test_contracts_v11.py::test_every_existing_instance_validates_under_v1_1 PASSED [ 36%]
tests/test_contracts_v11.py::test_fields_and_the_name_convention_give_the_same_verdict[architect.checks.c005] PASSED [ 36%]
tests/test_contracts_v11.py::test_fields_and_the_name_convention_give_the_same_verdict[architect.checks.c006] PASSED [ 36%]
tests/test_contracts_v11.py::test_c005_and_c006_verdicts_on_the_bound_model PASSED [ 37%]
tests/test_contracts_v11.py::test_c013_fails_a_capacity_param_whose_applies_to_dangles PASSED [ 37%]
tests/test_contracts_v11.py::test_a_capacity_param_with_the_new_fields_validates PASSED [ 37%]
tests/test_contracts_v11.py::test_replay_arbiter_and_projector_agree_on_a_branching_ledger PASSED [ 38%]
tests/test_contracts_v11.py::test_new_check_results_name_their_version_and_seq PASSED [ 38%]
tests/test_contracts_v11.py::test_a_v1_0_result_without_the_fields_is_keyed_by_the_head_of_its_time PASSED [ 38%]
tests/test_contracts_v11.py::test_the_v1_1_fields_are_optional_and_typed PASSED [ 39%]
tests/test_exit_fixture.py::test_fixture_round_trips_through_the_arbiter_and_replays_green PASSED [ 39%]
tests/test_exit_fixture.py::test_rebuild_state_after_the_fixture_ingest_reports_zero_diff PASSED [ 40%]
tests/test_exit_fixture.py::test_the_arbiters_head_model_is_what_replay_folds_before_and_after_a_rebuild PASSED [ 40%]
tests/test_gateway.py::test_tiers_resolve_to_their_configured_candidates PASSED [ 40%]
tests/test_gateway.py::test_the_shipped_config_routes_every_tier_to_anthropic_first PASSED [ 41%]
tests/test_gateway.py::test_only_providers_import_llm_clients_and_only_the_config_names_models PASSED [ 41%]
tests/test_gateway.py::test_invalid_then_valid_structured_output_succeeds_on_the_second_attempt PASSED [ 41%]
tests/test_gateway.py::test_persistently_invalid_structured_output_is_refused_after_three_attempts PASSED [ 42%]
tests/test_gateway.py::test_parsed_is_only_ever_validated_data PASSED    [ 42%]
tests/test_gateway.py::test_identical_deterministic_requests_hit_the_cache PASSED [ 42%]
tests/test_gateway.py::test_auto_mode_does_not_cache_sampled_requests_but_force_does PASSED [ 43%]
tests/test_gateway.py::test_a_call_over_the_session_token_cap_is_refused_before_the_provider PASSED [ 43%]
tests/test_gateway.py::test_twenty_concurrent_calls_never_exceed_a_tight_cap PASSED [ 44%]
tests/test_gateway.py::test_null_limits_are_uncapped_including_the_fixtures_budget PASSED [ 44%]
tests/test_gateway.py::test_spend_is_tracked_under_every_sub_scope PASSED [ 44%]
tests/test_gateway.py::test_every_attempt_failure_and_hit_has_a_row PASSED [ 45%]
tests/test_gateway.py::test_the_call_log_is_append_only PASSED           [ 45%]
tests/test_gateway.py::test_prompt_hash_is_deterministic_and_routing_independent PASSED [ 45%]
tests/test_gateway.py::test_replay_serves_recorded_responses_and_never_calls_the_provider PASSED [ 46%]
tests/test_gateway.py::test_replay_mode_comes_from_the_environment PASSED [ 46%]
tests/test_gateway.py::test_two_rate_limits_then_success_takes_three_attempts_without_real_sleep PASSED [ 46%]
tests/test_gateway.py::test_a_persistently_failing_primary_falls_back PASSED [ 47%]
tests/test_gateway.py::test_a_non_retryable_error_falls_back_at_once PASSED [ 47%]
tests/test_gateway.py::test_all_candidates_failing_names_each PASSED     [ 48%]
tests/test_gateway.py::test_exclude_families_removes_candidates PASSED   [ 48%]
tests/test_gateway.py::test_a_key_in_the_environment_never_reaches_rows_logs_or_errors PASSED [ 48%]
tests/test_gateway.py::test_wrap_untrusted_is_delimited_and_carries_the_source PASSED [ 49%]
tests/test_gateway.py::test_the_untrusted_rule_is_prepended_verbatim_only_when_tainted PASSED [ 49%]
tests/test_gateway.py::test_gateway_cli PASSED                           [ 49%]
tests/test_gateway_providers.py::test_openai_compat_request_shape_and_usage PASSED [ 50%]
tests/test_gateway_providers.py::test_openai_compat_structured_output_uses_json_schema PASSED [ 50%]
tests/test_gateway_providers.py::test_openai_compat_falls_back_to_json_mode_when_the_server_lacks_json_schema PASSED [ 50%]
tests/test_gateway_providers.py::test_openai_compat_maps_statuses[429-True] PASSED [ 51%]
tests/test_gateway_providers.py::test_openai_compat_maps_statuses[500-True] PASSED [ 51%]
tests/test_gateway_providers.py::test_openai_compat_maps_statuses[503-True] PASSED [ 51%]
tests/test_gateway_providers.py::test_openai_compat_maps_statuses[400-False] PASSED [ 52%]
tests/test_gateway_providers.py::test_openai_compat_maps_statuses[404-False] PASSED [ 52%]
tests/test_gateway_providers.py::test_openai_compat_timeouts_are_retryable PASSED [ 53%]
tests/test_gateway_providers.py::test_openai_compat_needs_a_base_url PASSED [ 53%]
tests/test_gateway_providers.py::test_redaction PASSED                   [ 53%]
tests/test_gateway_providers.py::test_anthropic_passes_system_separately_and_parses_usage PASSED [ 54%]
tests/test_gateway_providers.py::test_anthropic_structured_output_uses_the_native_json_schema_format PASSED [ 54%]
tests/test_gateway_providers.py::test_anthropic_maps_statuses[429-True] PASSED [ 54%]
tests/test_gateway_providers.py::test_anthropic_maps_statuses[529-True] PASSED [ 55%]
tests/test_gateway_providers.py::test_anthropic_maps_statuses[500-True] PASSED [ 55%]
tests/test_gateway_providers.py::test_anthropic_maps_statuses[400-False] PASSED [ 55%]
tests/test_gateway_providers.py::test_anthropic_maps_statuses[401-False] PASSED [ 56%]
tests/test_gateway_providers.py::test_anthropic_maps_statuses[404-False] PASSED [ 56%]
tests/test_gateway_providers.py::test_anthropic_refusals_are_not_retried PASSED [ 57%]
tests/test_gateway_providers.py::test_anthropic_timeouts_are_retryable PASSED [ 57%]
tests/test_ingestion.py::test_the_same_file_twice_is_one_source PASSED   [ 57%]
tests/test_ingestion.py::test_a_git_repository_is_one_external_source_with_skips PASSED [ 58%]
tests/test_ingestion.py::test_trusted_domains_are_the_only_external_trusted PASSED [ 58%]
tests/test_ingestion.py::test_pdf_segments_have_exact_locators PASSED    [ 58%]
tests/test_ingestion.py::test_repo_segments_have_exact_locators PASSED   [ 59%]
tests/test_ingestion.py::test_agreeing_passes_commit_a_documented_claim PASSED [ 59%]
tests/test_ingestion.py::test_each_disagreement_quarantines[missing-pass_b_missing] PASSED [ 59%]
tests/test_ingestion.py::test_each_disagreement_quarantines[spo-spo_mismatch] PASSED [ 60%]
tests/test_ingestion.py::test_each_disagreement_quarantines[magnitude-magnitude_mismatch] PASSED [ 60%]
tests/test_ingestion.py::test_each_disagreement_quarantines[condition-condition_conflict] PASSED [ 61%]
tests/test_ingestion.py::test_a_non_verbatim_quote_never_becomes_an_event PASSED [ 61%]
tests/test_ingestion.py::test_rerunning_extract_writes_nothing_and_a_new_version_re_extracts PASSED [ 61%]
tests/test_ingestion.py::test_the_same_claim_from_two_sources_is_design_grade PASSED [ 62%]
tests/test_ingestion.py::test_h1_status_and_confidence_in_the_output_are_rejected_by_the_schema PASSED [ 62%]
tests/test_ingestion.py::test_h2_a_fabricated_benchmark_without_a_verbatim_quote_is_dropped PASSED [ 62%]
tests/test_ingestion.py::test_h3_there_is_no_tool_path_and_the_environment_never_leaks PASSED [ 63%]
tests/test_ingestion.py::test_h4_hidden_pdf_text_yields_at_most_a_documented_claim_about_it PASSED [ 63%]
tests/test_ingestion.py::test_h5_the_taint_cannot_be_talked_up PASSED    [ 63%]
tests/test_ingestion.py::test_the_extraction_package_has_no_tool_or_exec_path PASSED [ 64%]
tests/test_ingestion.py::test_confidence_is_declared_monotonic_and_never_emitted PASSED [ 64%]
tests/test_ingestion.py::test_a_crash_mid_pass_b_resumes_without_duplicates PASSED [ 64%]
tests/test_ingestion.py::test_an_extraction_run_replays_to_identical_claims PASSED [ 65%]
tests/test_ingestion.py::test_ingest_source_and_extract_commands PASSED  [ 65%]
tests/test_ingestion.py::test_source_endpoints PASSED                    [ 66%]
tests/test_model_fold.py::test_folding_the_fixture_gives_replays_final_model PASSED [ 66%]
tests/test_model_fold.py::test_each_op_kind PASSED                       [ 66%]
tests/test_model_fold.py::test_the_base_is_left_untouched_and_a_proposal_keeps_its_version PASSED [ 67%]
tests/test_model_fold.py::test_a_missing_target_is_an_error[update_element] PASSED [ 67%]
tests/test_model_fold.py::test_a_missing_target_is_an_error[remove_element] PASSED [ 67%]
tests/test_model_fold.py::test_an_op_without_the_fields_its_kind_needs_is_malformed[op0-element_type] PASSED [ 68%]
tests/test_model_fold.py::test_an_op_without_the_fields_its_kind_needs_is_malformed[op1-element] PASSED [ 68%]
tests/test_model_fold.py::test_an_op_without_the_fields_its_kind_needs_is_malformed[op2-element_id] PASSED [ 68%]
tests/test_model_fold.py::test_an_op_without_the_fields_its_kind_needs_is_malformed[op3-element_id] PASSED [ 69%]
tests/test_model_fold.py::test_an_op_without_the_fields_its_kind_needs_is_malformed[op4-link] PASSED [ 69%]
tests/test_model_fold.py::test_an_op_without_the_fields_its_kind_needs_is_malformed[op5-link_type] PASSED [ 70%]
tests/test_model_fold.py::test_a_child_is_a_copy_of_its_parent_under_a_new_id PASSED [ 70%]
tests/test_model_fold.py::test_a_genesis_is_empty PASSED                 [ 70%]
tests/test_projections.py::test_every_event_type_is_projected_or_explicitly_not PASSED [ 71%]
tests/test_projections.py::test_projections_never_write_events_or_reach_the_arbiter PASSED [ 71%]
tests/test_projections.py::test_fixture_projection_equals_what_replay_folds PASSED [ 71%]
tests/test_projections.py::test_every_fixture_version_is_materialized_and_valid PASSED [ 72%]
tests/test_projections.py::test_model_edges_are_kept_per_version PASSED  [ 72%]
tests/test_projections.py::test_projecting_event_by_event_equals_a_rebuild PASSED [ 72%]
tests/test_projections.py::test_batch_size_does_not_change_the_result PASSED [ 73%]
tests/test_projections.py::test_a_crash_mid_rebuild_resumes_to_the_same_hash PASSED [ 73%]
tests/test_projections.py::test_killing_the_projector_process_mid_rebuild_loses_nothing PASSED [ 74%]
tests/test_projections.py::test_dropping_every_proj_table_loses_nothing PASSED [ 74%]
tests/test_projections.py::test_a_claim_reads_as_it_stood_at_an_earlier_seq PASSED [ 74%]
tests/test_projections.py::test_claim_detail_has_history_and_provenance PASSED [ 75%]
tests/test_projections.py::test_why_traces_an_element_to_requirements_decisions_claims_and_sources PASSED [ 75%]
tests/test_projections.py::test_refuting_a_premise_compromises_everything_derived_from_it PASSED [ 75%]
tests/test_projections.py::test_projection_status_reports_the_lag PASSED [ 76%]
tests/test_projections.py::test_the_worker_wakes_on_the_arbiters_notification PASSED [ 76%]
tests/test_projections.py::test_the_worker_polls_when_no_notification_arrives PASSED [ 76%]
tests/test_projections.py::test_the_arbiter_notifies_with_the_project_id PASSED [ 77%]
tests/test_projections.py::test_objections_decisions_waivers_checks_and_timeline PASSED [ 77%]
tests/test_projections.py::test_a_version_created_from_a_parent_starts_as_the_parents_model PASSED [ 77%]
tests/test_projections.py::test_an_event_that_cannot_be_folded_stops_the_projector_in_front_of_it PASSED [ 78%]
tests/test_projections.py::test_projection_is_per_project PASSED         [ 78%]
tests/test_projections.py::test_reads_of_things_that_are_not_projected_are_404 PASSED [ 79%]
tests/test_refusals.py::test_documented_claim_without_evidence PASSED    [ 79%]
tests/test_refusals.py::test_load_bearing_assumption_without_verification_plan PASSED [ 79%]
tests/test_refusals.py::test_promotion_to_measured_needs_an_experiment_as_cause PASSED [ 80%]
tests/test_refusals.py::test_status_change_with_the_wrong_from PASSED    [ 80%]
tests/test_refusals.py::test_waiver_signed_by_an_agent PASSED            [ 80%]
tests/test_refusals.py::test_evidence_citing_an_uningested_source PASSED [ 81%]
tests/test_refusals.py::test_patch_on_a_stale_base PASSED                [ 81%]
tests/test_refusals.py::test_resolving_an_objection_that_was_never_raised PASSED [ 81%]
tests/test_refusals.py::test_objection_without_a_falsifiable_test PASSED [ 82%]
tests/test_refusals.py::test_duplicate_source PASSED                     [ 82%]
tests/test_refusals.py::test_proposed_claim_must_be_a_valid_claim PASSED [ 83%]
tests/test_refusals.py::test_claim_id_mismatch PASSED                    [ 83%]
tests/test_refusals.py::test_duplicate_claim_id PASSED                   [ 83%]
tests/test_refusals.py::test_claim_from_an_unknown_proposal PASSED       [ 84%]
tests/test_refusals.py::test_a_patch_proposal_is_not_a_claim_proposal PASSED [ 84%]
tests/test_refusals.py::test_claim_and_patch_proposals_with_distinct_ids_are_accepted PASSED [ 84%]
tests/test_refusals.py::test_a_proposal_id_is_used_once_across_both_kinds PASSED [ 85%]
tests/test_refusals.py::test_formats_inside_embedded_objects_are_enforced PASSED [ 85%]
tests/test_refusals.py::test_status_change_of_an_unknown_claim PASSED    [ 85%]
tests/test_refusals.py::test_status_change_with_an_unknown_cause_event PASSED [ 86%]
tests/test_refusals.py::test_a_cause_event_from_another_project_is_unknown PASSED [ 86%]
tests/test_refusals.py::test_promotion_to_observed_is_guarded_too PASSED [ 87%]
tests/test_refusals.py::test_retracting_an_unknown_claim PASSED          [ 87%]
tests/test_refusals.py::test_second_genesis_version PASSED               [ 87%]
tests/test_refusals.py::test_version_created_with_a_committed_parent_is_accepted PASSED [ 88%]
tests/test_refusals.py::test_version_created_with_an_unknown_parent PASSED [ 88%]
tests/test_refusals.py::test_a_parent_version_from_another_project_is_unknown PASSED [ 88%]
tests/test_refusals.py::test_patch_must_be_a_valid_model_patch PASSED    [ 89%]
tests/test_refusals.py::test_patch_base_mismatch PASSED                  [ 89%]
tests/test_refusals.py::test_patch_proposed_on_a_stale_base PASSED       [ 89%]
tests/test_refusals.py::test_patch_before_any_model_version PASSED       [ 90%]
tests/test_refusals.py::test_patch_committed_from_an_unknown_proposal PASSED [ 90%]
tests/test_refusals.py::test_proposed_check_must_be_a_valid_check PASSED [ 90%]
tests/test_refusals.py::test_resolving_an_objection_twice PASSED         [ 91%]
tests/test_refusals.py::test_waiver_signed_by_the_system PASSED          [ 91%]
tests/test_refusals.py::test_experiment_with_an_uncommitted_result_claim PASSED [ 92%]
tests/test_refusals.py::test_decision_citing_an_uncommitted_claim PASSED [ 92%]
tests/test_refusals.py::test_merge_revert_must_cite_a_committed_merge PASSED [ 92%]
tests/test_refusals.py::test_schema_gate[change0-$.type] PASSED          [ 93%]
tests/test_refusals.py::test_schema_gate[change1-$.idempotency_key] PASSED [ 93%]
tests/test_refusals.py::test_schema_gate[change2-$] PASSED               [ 93%]
tests/test_refusals.py::test_schema_gate[change3-$.actor.kind] PASSED    [ 94%]
tests/test_refusals.py::test_schema_gate[change4-$.event_id] PASSED      [ 94%]
tests/test_refusals.py::test_schema_gate[change5-$.ts] PASSED            [ 94%]
tests/test_refusals.py::test_schema_gate[change6-$.ts] PASSED            [ 95%]
tests/test_refusals.py::test_schema_gate[change7-$.session_id] PASSED    [ 95%]
tests/test_refusals.py::test_schema_gate[change8-$] PASSED               [ 96%]
tests/test_refusals.py::test_schema_gate[change9-$.payload] PASSED       [ 96%]
tests/test_refusals.py::test_schema_gate_runs_before_the_rules PASSED    [ 96%]
tests/test_refusals.py::test_project_mismatch PASSED                     [ 97%]
tests/test_refusals.py::test_candidate_must_be_an_object PASSED          [ 97%]
tests/test_refusals.py::test_a_valid_patch_is_accepted_and_its_result_becomes_the_head_model PASSED [ 97%]
tests/test_refusals.py::test_a_version_created_from_a_parent_copies_the_parents_model PASSED [ 98%]
tests/test_refusals.py::test_patch_whose_target_is_not_in_the_model PASSED [ 98%]
tests/test_refusals.py::test_patch_that_would_not_leave_a_valid_model PASSED [ 98%]
tests/test_refusals.py::test_reusing_a_model_version_id PASSED           [ 99%]
tests/test_refusals.py::test_a_patch_op_without_the_fields_its_kind_needs PASSED [ 99%]
tests/test_refusals.py::test_a_version_id_the_system_model_cannot_carry PASSED [100%]

====================== 277 passed, 3 deselected in 45.72s ======================
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

## M4: the model gateway

Spec principle P8: models are replaceable workers. `architect.gateway` is the only path from
the platform to any LLM; no later module imports an LLM SDK or names a model.

### What landed

- **Request and response** (`gateway/request.py`, Pydantic): a caller gives a role, a tier
  (`tier-cheap | tier-mid | tier-frontier`), a purpose, system and messages, an optional
  JSON Schema, max_tokens, temperature, a scope `{tenant?, session?, phase?}`, the taints
  present in the input, families to exclude and a cache mode; it gets text, the validated
  `parsed` object, the `call_id`, provider/model/family, tokens, usd, latency, `cache_hit`
  and `attempts`.
- **Routing** (`router.py`, `config/models.yaml`): each tier is an ordered candidate list.
  Defaults: tier-cheap and tier-mid and tier-frontier route to Anthropic (family
  `anthropic-claude`); an OpenAI-compatible candidate (family `local-vllm`) is appended to
  every tier only when `OPENAI_COMPAT_BASE_URL` is set. Prices sit next to the models, each
  marked "VERIFY against the provider's current pricing page". `exclude_families` filters
  candidates; nothing left is `NoEligibleModel`.
- **Providers** (`gateway/providers/`): `mock.py` (deterministic, scripted, counts calls;
  all of CI runs on it), `anthropic.py` (the official SDK, key from `ARCHITECT_ANTHROPIC_API_KEY`, falling back
  to `ANTHROPIC_API_KEY`;
  structured output through the API's native `output_config.format` JSON Schema; 429, 529,
  5xx, timeouts and connection failures retryable, other 4xx and refusals not),
  `openai_compat.py` (httpx2 against `{base}/v1/chat/completions`; `response_format`
  json_schema with a JSON-mode fallback when the server rejects it; optional
  `OPENAI_COMPAT_API_KEY`).
- **Structured output** (`structured.py`): the provider is asked natively and the gateway
  always validates locally with jsonschema; an invalid reply is sent back as the next turn
  with the validation error, up to two more times; still invalid is `StructuredOutputInvalid`.
  `parsed` is never unvalidated data.
- **Cache** (`cache.py`, `gw_cache`): key = sha256 over (provider, model, system, messages,
  output_schema, temperature, max_tokens). `auto` caches deterministic calls only, `force`
  always, `off` never. A hit costs nothing and never reaches a provider.
- **Budgets** (`budget.py`, `gw_spend`, `proj_budgets`): limits come from `budget.updated`
  events through the new `proj_budgets` read model. Spend is tracked under every non-empty
  subset of a call's scope, so a limit on any of them applies. Before each provider call the
  estimate (input chars / 4 + max_tokens, priced) is reserved under a row lock and refused
  with `BudgetExceeded` if any scope would go over; after the call it is settled to the
  actual numbers; a failed call releases it. Null limits are uncapped. `wall_clock_minutes`
  and `gpu_minutes` are recorded, not enforced.
- **Recording** (`recorder.py`, `gw_calls`): every attempt, failure, invalid output, cache
  hit, replay and budget refusal is a row, with the full request, the response or error,
  tokens, usd, latency, attempt number and input taints. The table has the ledger's
  append-only trigger. **Model calls are deliberately not ledger events**: the ledger holds
  decisions, the call log holds volume; agent messages reference `call_id`s.
- **Replay** (`replay.py`, `ARCHITECT_GATEWAY_MODE=replay`): responses are served from
  `gw_calls` by exact `prompt_hash` (sha256 over system, messages, schema, temperature,
  max_tokens; independent of routing) and the provider is never called; a miss is
  `ReplayMiss`.
- **Retries and fallback** (`gateway.py`): exponential backoff with jitter on retryable
  errors (`retries.max_attempts` per candidate, clock and RNG injectable), then the next
  candidate; all failing is `AllCandidatesFailed` naming each. A per-provider semaphore
  caps concurrency (`concurrency` in the config).
- **Untrusted content** (`untrusted.py`): `wrap_untrusted(content, source_id)` delimits a
  data block; with `external_untrusted` among the input taints the fixed rule is prepended
  to the system prompt verbatim.
- **Secrets**: keys are read from the environment only and never stored, logged or raised;
  `Authorization` and `x-api-key` headers are redacted in debug logging.
- **CLI**: `architect gateway call --tier T --purpose P --prompt "..." [--schema f.json]
  [--session S]`, `architect gateway spend --session S`, `architect gateway calls --limit N`.

### Decisions worth knowing

- **Structured output on Anthropic uses `output_config.format`, not a forced tool.** The
  prompt asked for a forced single tool; the current Sonnet and Opus generations reject
  forced `tool_choice` with a 400, and the API's native JSON Schema output constrains the
  reply directly.
- **No temperature is sent to Anthropic.** The 1.x SDK has no sampling parameters. The
  request's temperature still governs the gateway's caching policy and is sent to the
  OpenAI-compatible provider. `sampling: false` in the config records this per candidate.
- **The OpenAI-compatible adapter uses httpx2**, the HTTP library the Anthropic SDK already
  depends on, rather than adding `httpx` beside it.
- **Spend is per sub-scope, limits are matched by containment.** A `budget.updated` whose
  scope is `{session: S}` caps every call whose scope includes `session: S`, and the fixture's
  budget (`usd: null`, `tokens: null`) leaves its session uncapped.
- **The gateway's default temperature is 0**, so `auto` caching is on unless a caller samples.

### Live tests (not in CI)

`tests/test_gateway_live.py` is marked `live` and deselected by default (pyproject
`addopts = -m "not live"`; deselection, not skipping). To run them, put the key in the shell,
never in a file in the repo:

```
$env:ARCHITECT_ANTHROPIC_API_KEY = "..."   # bash: export ARCHITECT_ANTHROPIC_API_KEY=...
pytest -m live -v
# L2 (OpenAI-compatible) also needs:
$env:OPENAI_COMPAT_BASE_URL = "http://localhost:8000"   # and OPENAI_COMPAT_API_KEY if required
```

L1 makes a tiny completion and a structured round trip on tier-cheap and records tokens and
usd; L2 does the same through the OpenAI-compatible provider. They have not been run. L2
also carries the marker `live_openai_compat`: while `OPENAI_COMPAT_BASE_URL` is unset it is
deselected at collection (`tests/conftest.py`), not failed and not skipped, so
`pytest -m live` on a machine without such a server runs L1, L3 and L4 only.

**The key's variable (M7 amendment).** The app reads `ARCHITECT_ANTHROPIC_API_KEY` first and
falls back to `ANTHROPIC_API_KEY` (`gateway/config.py::anthropic_api_key`, used by the
provider and by `default_providers`). The app's own name keeps the key away from any other
tool in the same shell that uses `ANTHROPIC_API_KEY` for its own auth. `tests/conftest.py`
removes `ARCHITECT_ANTHROPIC_API_KEY` from every test not marked `live`, so a real key in a
developer's shell cannot reach the default gateway of a mock test. The guards at the top of
L1, L3 and L4 use the same lookup.

### Exit tests

| # | Exit test | Result | Evidence (`tests/test_gateway.py` unless noted) |
| --- | --- | --- | --- |
| G1 | Tiers resolve to their candidates; no LLM import outside providers; no model id outside the config | green | `test_tiers_resolve_to_their_configured_candidates`, `test_the_shipped_config_routes_every_tier_to_anthropic_first`, `test_only_providers_import_llm_clients_and_only_the_config_names_models` |
| G2 | Structured output: invalid then valid is attempts = 2; always invalid is `StructuredOutputInvalid` after 3; every attempt recorded; `parsed` never unvalidated | green | `test_invalid_then_valid_structured_output_succeeds_on_the_second_attempt`, `test_persistently_invalid_structured_output_is_refused_after_three_attempts`, `test_parsed_is_only_ever_validated_data` |
| G3 | Cache hits, misses on any change, no caching of sampled requests in auto | green | `test_identical_deterministic_requests_hit_the_cache`, `test_auto_mode_does_not_cache_sampled_requests_but_force_does` |
| G4 | Refused before the provider over the cap; 20 concurrent calls never exceed a tight cap; null is uncapped, including the fixture's budget | green | `test_a_call_over_the_session_token_cap_is_refused_before_the_provider`, `test_twenty_concurrent_calls_never_exceed_a_tight_cap`, `test_null_limits_are_uncapped_including_the_fixtures_budget` |
| G5 | Every call, failure, retry and hit has a row; raw UPDATE/DELETE/TRUNCATE raise; `prompt_hash` deterministic | green | `test_every_attempt_failure_and_hit_has_a_row`, `test_the_call_log_is_append_only`, `test_prompt_hash_is_deterministic_and_routing_independent` |
| G6 | Replay serves recorded responses byte-identically with the provider rigged to fail; a miss is `ReplayMiss` | green | `test_replay_serves_recorded_responses_and_never_calls_the_provider`, `test_replay_mode_comes_from_the_environment` |
| G7 | 429, 429, success is attempts = 3 with no real sleep; a failing primary falls back, both recorded; all failing names each | green | `test_two_rate_limits_then_success_takes_three_attempts_without_real_sleep`, `test_a_persistently_failing_primary_falls_back`, `test_a_non_retryable_error_falls_back_at_once`, `test_all_candidates_failing_names_each` |
| G8 | `exclude_families` removes candidates; nothing left is `NoEligibleModel` | green | `test_exclude_families_removes_candidates` |
| G9 | A fake key in the environment never appears in any `gw_*` row, log line or exception | green | `test_a_key_in_the_environment_never_reaches_rows_logs_or_errors` |
| G10 | OpenAI-compatible provider against fake HTTP: request shape, `response_format` json_schema, usage, JSON-mode fallback, status mapping | green | `test_gateway_providers.py::test_openai_compat_*` |
| G11 | Anthropic provider against mocked HTTP: system passed separately, native JSON Schema output, usage, 429/529/5xx retryable, 400 not, refusals not retried | green | `test_gateway_providers.py::test_anthropic_*` |
| G12 | `wrap_untrusted` is delimited with the source id; the rule is prepended verbatim only with `external_untrusted` | green | `test_wrap_untrusted_is_delimited_and_carries_the_source`, `test_the_untrusted_rule_is_prepended_verbatim_only_when_tainted` |
| | Every pre-existing test green | green | the run above |

## M5: ingestion, two-pass extraction, quarantine, injection defense

Messy sources in, schema-valid, evidence-backed claims out, through the Arbiter. Models only
propose content; the pipeline decides status, evidence, taint, provenance and ids.

### The pipeline, in words

1. **Ingest** (`sources.py`): a local file (txt, md, pdf), a PDF or a git repository
   (`git clone --depth 1`, the resolved commit sha in the uri) becomes one `source.ingested`
   event; raw bytes go to the content-addressed object store (`data/objects/<sha256>`,
   `ARCHITECT_OBJECT_STORE` to move it). A repository is one source: a manifest of the
   included files, each stored by hash; `vendor/`, `node_modules/`, `third_party/`, `dist/`,
   `build/`, binaries and files over 512 KiB are skipped and listed as skipped. Taint: local
   files are `user` (overridable to `internal` or `external_trusted`), repositories are
   `external_untrusted` unless the host is in `config/ingest.yaml`'s empty allowlist. The same
   content hash in a project is the same source (idempotency key `source:<hash>`).
2. **Parse** (`parse.py`, `ing_segments`): segments with ids `sha256(source_id|locator)[:32]`
   and human locators. PDF: `p.<page> ¶<n>` and, best effort via PyMuPDF `find_tables`,
   `p.<page> table <t> row <r>` with cells as structured values; Markdown:
   `<path>#<heading-slug> L<a>-L<b>`; Python: `<path>:<symbol> L<a>-L<b>` from the AST;
   other code: 60-line windows `<path> L<a>-L<b>`. Rebuildable; a rebuild gives identical ids.
3. **Pass A** (`extract.py`, tier-cheap, purpose `extract-claims-a`): segments packed into
   calls under `pack_chars`, each wrapped by `wrap_untrusted(text, "<source_id>:<locator>")`
   so the gateway prepends the fixed untrusted-content rule. Fixed, versioned prompt
   (`PROMPT_VERSION`, `PIPELINE_VERSION`); strict output schema with only segment_locator,
   subject, predicate, object, magnitude, conditions, quote. Normalization: predicate to
   UPPER_SNAKE, entity names to slugs, units through the M3 units module. R2: a candidate
   whose quote is not in its segment (whitespace-normalized) is dropped and counted.
4. **Pass B** (tier-mid, `extract-claims-b`): for each segment with surviving candidates,
   one call with only that segment, never pass A's output, excluding pass A's family when
   the tier has another (recorded on the job). Agreement: same segment, identical normalized
   (subject, predicate, object), equal magnitude after unit normalization or both absent, no
   conflicting shared condition values.
5. **Commit** (`commit.py`): deterministic id `clm_` + base32(sha256(source_id, locator,
   SPO, magnitude, pipeline version))[:26]; `claim.proposed` then `claim.committed` as actor
   `extractor`, status `documented`, evidence `[{source, span: locator, kind}]`, taint from
   the source, provenance `{model_tier, prompt_hash, pipeline_version}`, `recorded_at` = the
   source event's own timestamp (so a replay is byte-identical). Never a confidence.
6. **Quarantine** (`ing_quarantine`): a disagreeing candidate is proposed, never committed,
   with its reason (`pass_b_missing | spo_mismatch | magnitude_mismatch | condition_conflict`)
   and both passes' outputs.

The job runner (`pipeline.py`, `ing_jobs`, `ing_candidates`, `ing_pass_b`, `ing_metrics`)
keys a job by (project, content hash, pipeline version), marks each stage done, and resumes
from the first unfinished stage; the Arbiter's idempotency keys make the commit stage safe
to repeat. All calls run under the gateway scope `{"session": "ingest:<job_id>"}`.

### Grades and confidence (projection, never ledger)

- **quarantined**: proposed, never committed (`proj_claim_proposals`).
- **unverified**: committed, a single source.
- **design_grade**: committed and corroborated by a second distinct source (different content
  hash, same normalized SPO, compatible magnitude), or status `measured`/`observed`, or
  referenced by a passing `check.result` or a recorded experiment (`proj_experiments`).
- **confidence v1** (`config/confidence.yaml`): base by source tier (user 0.60, internal 0.70,
  external_trusted 0.60, external_untrusted 0.40) + 0.10 per corroboration (cap 0.30) + 0.10
  for two-pass agreement + 0.10 per verification event (cap 0.20), clamped to [0, 1], times
  recency_decay 1.0. The inputs are stored beside the value (`proj_claims.confidence_inputs`).
  Monotonic by construction. **These weights are a declared prior, not a measurement;
  calibration against labeled data is M14.**

### Metrics (`ing_metrics`, per job)

`pass_a_candidates`, `dropped_quote_a`, `pass_b_calls`, `pass_b_candidates`, `dropped_quote_b`,
`agreed`, `quarantined`, `committed`, plus the lists `committed_ids` and `quarantined`.

### Commands

```
architect ingest-source --project P (--file PATH | --pdf PATH | --github URL [--ref REF]) [--origin user|internal|external_trusted]
architect extract --project P --source S [--pipeline-version N]
architect sources --project P
architect claims --project P [--grade quarantined|unverified|design_grade]
curl -F file=@doc.md localhost:8000/v1/projects/P/sources
curl -F github_url=https://github.com/org/repo -F ref=main localhost:8000/v1/projects/P/sources
curl -X POST localhost:8000/v1/projects/P/sources/S/extract
curl localhost:8000/v1/projects/P/sources ; curl "localhost:8000/v1/projects/P/claims?grade=design_grade"
```

L3 (manual): put a short document in `tests/live_docs/` (gitignored), set the key in the
shell (M4, "Live tests"), run `pytest -m live -v -k l3`; it prints counts by grade, drop counts and usd.
It runs under a usd cap ($2.50, `ARCHITECT_L3_USD_CAP`); reaching the cap commits what both
passes finished and reports a PARTIAL baseline, and a later run resumes (M7-live Part 1).

### Exit tests

| # | Exit test | Result | Evidence (`tests/test_ingestion.py`) |
| --- | --- | --- | --- |
| I1 | Same file twice is one source; the git fixture is external_untrusted with a sha in the uri; a local file is user; vendor, binary and oversize files skipped | green | `test_the_same_file_twice_is_one_source`, `test_a_git_repository_is_one_external_source_with_skips`, `test_trusted_domains_are_the_only_external_trusted` |
| I2 | Exact locators for the generated PDF and the repository; a rebuild gives identical ids | green | `test_pdf_segments_have_exact_locators`, `test_repo_segments_have_exact_locators` |
| I3 | Agreeing passes commit a documented claim with exact evidence, taint, provenance, deterministic id; grade unverified; agreement in the confidence inputs | green | `test_agreeing_passes_commit_a_documented_claim` |
| I4 | Each of the four disagreement reasons: proposal only, reason recorded, grade quarantined | green | `test_each_disagreement_quarantines` (4 cases) |
| I5 | A non-verbatim quote never produces an event and is counted | green | `test_a_non_verbatim_quote_never_becomes_an_event` |
| I6 | Re-running extract writes nothing; a new pipeline version re-extracts into new ids | green | `test_rerunning_extract_writes_nothing_and_a_new_version_re_extracts` |
| I7 | The same claim from two different-content sources is design_grade with a higher confidence | green | `test_the_same_claim_from_two_sources_is_design_grade` |
| I8 | H1 to H5 as specified; every extraction request tainted and ruled; no tool or exec path in the package | green | `test_h1_*` to `test_h5_*`, `test_the_extraction_package_has_no_tool_or_exec_path` |
| I9 | No emitted claim carries a confidence; the declared function is monotonic | green | `test_confidence_is_declared_monotonic_and_never_emitted`, assertions in I3 and I8 |
| I10 | A crash mid pass B resumes without duplicate events; ing_jobs shows the stages | green | `test_a_crash_mid_pass_b_resumes_without_duplicates` |
| I11 | A recorded run replays in `ARCHITECT_GATEWAY_MODE=replay` to identical claims | green | `test_an_extraction_run_replays_to_identical_claims` |
| | CLI and API; every pre-existing test green | green | `test_ingest_source_and_extract_commands`, `test_source_endpoints`; the run above |

## M6: durable design sessions, the Architect agent v1, the Context Compiler

Spec §13 and principle P5: a design session is a budgeted, checkpointed workflow over nine
phases that can be paused, resumed, steered, cancelled or killed and always ends with a
usable result, the best design so far plus an honest list of open risks. Phase 1 scope:
one alternative per session (K = 1; the tournament is M11), the deterministic check battery
as the only adversary (AI adversaries are M10), research as retrieval over existing claims.

### What landed

- **The workflow** (`sessions/workflow.py`, Temporal, `temporalio`): deterministic by
  construction and by test (S12): no I/O, no clock but `workflow.now()`, no randomness, and
  every side effect an activity called by name. Signals `pause`, `resume`, `cancel`
  (graceful: converge and package the best so far), `approve`, `reject`, `steer(text)`;
  query `status` → `{phase, round, best_version, open_risk_count, spend, status, outcome,
  package_key}`.
- **The activities** (`sessions/activities.py`): every Arbiter write, gateway call,
  projection catch-up and object-store write. Idempotent: Arbiter keys are
  `session:<sid>:<phase>:<round>:<step>`; version ids, claim ids, proposal ids, task ids and
  message ids are hashes of (session, phase, round, step) or of content; a retried or
  replayed activity writes nothing twice (S6), and the gateway cache answers a repeated
  prompt without a second charge.
- **The Architect agent v1** (`sessions/agent.py`): purposes `frame`, `draft`, `repair`
  on `tier-frontier`, strict output schemas (requirements with metric/target/unit/quote;
  patch ops plus optional ADRs and questions; repairs plus waiver requests), one fixed
  prompt per purpose (`architect-v1`; `architect-v2` since M7) with a marker line `[architect purpose=.. round=..]`
  the scripted test provider keys on. Its output is turned into typed protocol messages
  (`Task`, `ClaimProposal`, `ModelPatchProposal`, `Question`) validated against
  `agent_protocol.schema.json` and appended to `ag_messages` (same `append_only()` trigger
  as `gw_calls`; messages reference `call_id`s and carry the compiled context's manifest and
  dropped list). Arbiter rejections come back as the next user turn
  (`{code, detail, json_path}`) for at most `architect.rejection_retries` (3) more attempts;
  past that the round records the open risk `architect-could-not-produce-valid-patch:<phase>:<round>`
  and the session goes on (S3). Waivers are never signed by the agent: a request becomes a
  `Question` to the owner and the open risk `waiver-requested:<check>:<element>` (A5, S2).
- **The requirement linter** (`sessions/linter.py`): a requirement is measurable when it has
  a metric, a numeric target and a unit the M3 units module knows (the Architect's fields,
  else parsed from the text and inferred from its vocabulary). Measurable → a `documented`
  claim `{requirement req_<slug>} CONSTRAINS {metric}` with the magnitude, evidence the brief
  source and the section locator of the quoted words, taint `user`. Unmeasurable → the same
  claim without a magnitude plus the open risk `requirement-unmeasurable:<req_id>`; unknowns
  → `unknown:<slug>`; constraints → `constraint` claims (S10, S1).
- **The Context Compiler** (`sessions/compiler.py`): `compile(task)` → goal, the brief (frame
  only), requirements and constraints, owner guidance (steer claims), the ranked relevant
  claims, the compact head model, the failing checks' evidence and the remaining budget,
  packed to `context.token_target` (6000 tokens, four characters each): the mandatory
  sections first, then ranked claims until the target, the rest dropped with reason
  `token_target`. The manifest (claim ids, version id, result ids) and the dropped ids with
  reasons go on the `Task` message. A quarantined claim (proposed, never committed) is never
  a fact whatever confidence it carries (rule 10): dropped with reason `quarantined`. An
  `external_untrusted` claim is wrapped by `wrap_untrusted` under its claim id and the
  request's `input_taints` say so, so the gateway prepends the fixed rule (S9). Research
  retrieval is `rank_claims`: vocabulary overlap between the brief and a claim's triple, best
  first, commit order on ties.
- **Presets** (`config/presets.yaml`): quick (3 rounds, 360 min, 2M tokens, $25, end gate),
  deep (5 rounds, 600 min, 6M tokens, $75, gate after the first attack and at the end),
  exhaustive (10 rounds, 1440 min, 25M tokens, usd null = uncapped, end gate only, §22-5).
  K = 1 everywhere. Overrides: `max_rounds`, `wall_clock_minutes`, `tokens`, `usd`. At start
  the session writes `budget.updated` on scope `{session}`, so the gateway enforces tokens
  and usd; the workflow enforces the wall clock.
- **The read model**: `ses_sessions` (status, outcome, phase, round, best version, open risk
  ids, spend, package key), written by the activities; the phase timeline comes from the
  ledger (`proj_session_timeline`). The package is JSON in the object store under its content
  hash: best version, gate verdict with reasons, every check result, the requirement trace
  (requirement → satisfying components), open risks with evidence, waiver requests, ADRs,
  rounds, spend and the timeline.
- **CLI**: `architect worker`; `architect session start --project P --brief brief.md
  [--preset quick|deep|exhaustive] [--source PATH|src_id ...] [--override key=value ...]`;
  `architect session status|show|pause|resume|cancel|approve|reject --session S`;
  `architect session steer --session S --text "..."`; `show` needs `--project` and prints
  the timeline, the rounds, the gate, the open risks and the package key.
- **API**: `POST /v1/projects/{pid}/sessions` `{brief, preset?, overrides?, sources?}`;
  `GET /v1/projects/{pid}/sessions[/{sid}]`;
  `POST /v1/projects/{pid}/sessions/{sid}/{pause|resume|cancel|approve|reject|steer}`
  (steer takes `{text}`). Unknown sessions are 404 `SESSION_NOT_FOUND`.

### The phase machine

```
start ── session_start: brief → user source; budget.updated {session}; ses_sessions row
frame ─── Architect(frame) → requirements (linter) → claims; unknowns → risks   checkpoint
research  rank_claims(brief) over committed, uncompromised claims                checkpoint
model ─── model.version_created (genesis, or a child of the project's head)      checkpoint
draft ─── Architect(draft) → ModelPatchProposal → patch_proposed + patch_committed
          (rejection loop ≤ 3 retries) → ADRs                                    checkpoint
round r:
  attack ─ battery on the head (check.result), gate      [deep: human gate after round 1]
  repair ─ Architect(repair, failing evidence) → one patch, or waiver requests, or nothing
  verify ─ battery on the new head, gate                                         checkpoint
  until the convergence rule fires
converge  best so far = fewest blocking reasons, ties to the later version       checkpoint
package ─ the package JSON → object store; ses_sessions.package_key              checkpoint
end gate  status awaiting_approval until approve → approved | reject → rejected
```

`session.phase_changed` is emitted at every transition and `session.checkpoint`
(phase, best version, open risk ids, spend from `gw_spend`) at the end of every phase and,
by a workflow timer, at least every `checkpoint_minutes` (5) of workflow time while a phase
runs; nothing ticks while the session is paused or waiting for a human (no compute held).

**Convergence rule**: (a) the gate is ALLOWED; (b) no improvement for 2 consecutive rounds,
improvement being fewer blocking reasons, then fewer failing or erroring checks; (c)
`max_rounds` reached; (d) the gateway refused a call over the session's cap, or the wall
clock ran out (checked before every step); (e) the cancel signal. Pause, steer and cancel
take effect at the next step boundary; the wall clock is `workflow.now()` against the start.

**Outcome and status** (as built in M6; M7 changed the gate: every outcome but cancel now
ends at it, and it takes four decisions; see M7, Part A). The outcome says how the
deliberation ended: `completed` (a),
`completed_with_risks` (b or c with the gate BLOCKED), `stopped_budget`, `stopped_time`,
`cancelled`, `rejected` (at the deep preset's mid gate), `failed` (an unexpected error after
the activities' retries; the only outcome without a package). The status says where the
session stands: `running`, `paused`, `awaiting_approval`, then `approved` or `rejected`, or
the stopped outcome itself. The end human gate applies to `completed` and
`completed_with_risks`; a session stopped by budget, time or cancel ends there with its
package.

### Decisions worth knowing

- **`started_at` is the session clock's origin**, chosen by the client at start and recorded
  on every claim the session commits (`recorded_at`) and every agent message (`ts`). The
  workflow's own clock is `workflow.now()`. This is what lets a replay of a session be
  content-identical (S8).
- **The prompt sees the budget as coarse buckets** ("more than 90%", "50-90%", ...), never
  the exact numbers: exact spend is in the checkpoints and `gw_spend`. Exact numbers in the
  prompt would make a live run and its replay ask different questions.
- **Replay charges nothing** (M4, G6), so a replayed session's checkpoints report the
  recorded run's total rather than the running sum; S8 compares every payload byte for byte
  except the checkpoints' `spend`. The replay runs into a fresh ledger (a second schema, the
  same project id) with the gateway reading the first run's call log.
- **The session's status is operational state, not a ledger event**: the frozen event types
  cannot carry it (contracts-PROPOSALS P-11). Everything else a session does is in the ledger.
- **A session in a project that already has a model starts from its head**
  (`model.version_created` with `parent`), not from an empty genesis.
- **Requirement claim ids derive from the brief source and the requirement**, not from the
  session, so two sessions on the same brief share the claims; a duplicate commit is read as
  "already committed", not as an error.
- **Sync activities on a thread pool**, psycopg's pool underneath; each phase activity
  catches the projector up before reading and after writing.
- **Temporal's time-skipping test server skips time only while a test awaits a result**, so
  the tests poll the status query and skip time explicitly (`env.sleep`) where a test is
  about the wall clock (S5). A found-and-fixed race: a decision signal that arrives while
  the workflow is still recording `awaiting_approval` must not be wiped.
- **Windows**: the whole suite, including the sessions, now runs on the development machine
  against a portable PostgreSQL 16 (no Docker needed): 1 platform bug surfaced and was fixed
  (repository manifest order is now by POSIX path on every platform).

### Running sessions locally

```
docker compose --profile sessions up -d          # Postgres 16 + Temporal dev server (UI :8233)
pip install -e ".[dev]"
architect init-db
architect worker                                 # keeps running; ARCHITECT_TEMPORAL_ADDRESS=localhost:7233
architect session start --project P --brief brief.md --preset quick
architect session status --session ses_...       # the live query
architect session steer --session ses_... --text "prefer at-least-once delivery"
architect session pause|resume|cancel|approve|reject --session ses_...
architect session show --project P --session ses_...
curl -X POST localhost:8000/v1/projects/P/sessions -H 'content-type: application/json' \
     -d '{"brief": "...", "preset": "deep", "overrides": {"usd": null}}'
curl localhost:8000/v1/projects/P/sessions/ses_... ; curl -X POST .../sessions/ses_.../approve
```

Without Docker, any Temporal dev server works (`temporal server start-dev`); the tests start
one themselves when `ARCHITECT_TEMPORAL_ADDRESS` is unset. Model calls go through the gateway
as everywhere else: with no key in the environment the worker has only the mock provider.

L4 (manual): `pytest -m live -v -k l4` with the key in the shell (M4, "Live tests") runs a real
quick session on a tiny brief with a small cap and prints the timeline, gate, risks and usd.

### Exit tests

| # | Exit test | Result | Evidence (`tests/test_sessions.py` unless noted) |
| --- | --- | --- | --- |
| S1 | Planted flaws end to end: C-012 and C-007 caught, repaired, gate ALLOWED, package, approve → approved; the ledger in order | green | `test_s1_planted_flaws_are_caught_repaired_and_the_session_is_approved` |
| S2 | Unrepairable C-007: stops by rule (b), `completed_with_risks`, gate BLOCKED, C-007 with evidence in the package's risks, waiver requested never signed | green | `test_s2_an_unrepairable_flaw_ends_with_risks_and_a_blocked_gate` |
| S3 | Invalid patch → `INVALID_MODEL_RESULT` fed back → corrected; past the bound → open risk, no crash | green | `test_s3_an_invalid_patch_is_fed_back_and_corrected`, `test_s3_exceeding_the_retry_bound_is_an_open_risk_not_a_crash` |
| S4 | Tight token cap → `stopped_budget`, best so far packaged, checkpoint spend = `gw_spend`; usd null uncapped | green | `test_s4_a_tight_token_cap_stops_the_session_with_the_best_so_far`, `test_s4_usd_null_is_uncapped` |
| S5 | Time skipped past the preset → `stopped_time` with a package | green | `test_s5_the_wall_clock_stops_the_session_with_a_package` |
| S6 | A worker subprocess killed after the attack phase; a new worker resumes; dense seq, unique keys, model content-identical to an uninterrupted run | green | `test_sessions_server.py::test_s6_a_killed_worker_is_resumed_by_a_new_one_without_duplicates` |
| S7 | pause stops progress, resume continues, reject → rejected; steer enters the next context, cancel → cancelled with a package | green | `test_s7_pause_resume_and_reject`, `test_s7_steer_enters_the_next_context_and_cancel_packages` |
| S8 | A recorded session replays in gateway replay mode to a content-identical ledger | green | `test_s8_a_recorded_session_replays_to_a_content_identical_ledger` |
| S9 | Token target respected, dropped ids with reasons, untrusted claims wrapped and ruled, a confident quarantined claim never a fact | green | `test_s9_the_compiler_packs_to_the_target_and_never_includes_quarantined_facts` |
| S10 | "must be fast" → unmeasurable; "p99 < 200 ms" → a claim with magnitude 200 ms | green | `test_s10_the_linter_separates_measurable_from_unmeasurable_requirements` |
| S11 | S1 on a real Temporal dev server, green in CI | green | `test_sessions_server.py::test_s11_the_planted_flaw_session_runs_green_on_a_real_dev_server` |
| S12 | Workflow modules import no DB, gateway, Arbiter, httpx, time or random modules | green | `test_s12_workflow_modules_import_nothing_impure` |
| | CLI and API; every pre-existing test green | green | `test_session_cli`, `test_session_endpoints`; the run below |

CI output (branch head `af2ae54`, run 37115718021 on `ubuntu-latest` with `postgres:16` and a
Temporal dev server started by the Temporal CLI, `ARCHITECT_TEMPORAL_ADDRESS=localhost:7233`):

```
================= 295 passed, 4 deselected in 80.58s (0:01:20) =================
```

Nothing skipped; the four deselected tests are the live ones (L1, L2, L3, L4). The same suite
on the Windows development machine against the portable PostgreSQL 16: `295 passed, 4
deselected in 402.38s`.

## M7: console, golden task #1, and the Phase 1 exit test

**Phase 1 exit test: PENDING LIVE RUN.** Everything that needs no key is built and
tested. The live baseline (Step 0: L1, L3, L4) and the live exit test (Part E: two golden
runs under a $3 cap each) are deferred to a follow-up session, "M7-live", because the app's
key was not visible to the session that built this.

### Before the module: three amendments

1. **The key's variable.** The Anthropic provider and `default_providers` read
   `ARCHITECT_ANTHROPIC_API_KEY`, then `ANTHROPIC_API_KEY` (see M4, "Live tests"). G9 sets a
   second, distinct fake key under the new name and asserts neither fake reaches a `gw_*`
   row, a log line or an exception. The guards of L1, L3 and L4 use the same lookup.
   `tests/conftest.py` removes the app's variable from every test not marked `live`.
2. **L2** carries `live_openai_compat` and is deselected at collection unless
   `OPENAI_COMPAT_BASE_URL` is set: not failed, not skipped.
3. **The L3 document** is `tests/live_docs/arxiv-2609.32972v1.pdf` (gitignored): "Time
   Semantics and Liveness Artifacts in Adversarial Consensus Simulation",
   <https://arxiv.org/abs/2609.32972v1>, primary category cs.DC, license CC BY 4.0 as read
   from arXiv's own record, 6 pages counted from the file, sha256
   `ceeacf2faebc55a473fee138e9ba083312e3da9066ab826351090d4d35eaeb6c`. It parses into 136
   segments, so L3 runs under a per-test usd cap: a `budget.updated` of $1.00 ($2.50 since
   M7-live Part 1) on the extraction job's own gateway scope (`ARCHITECT_L3_USD_CAP`
   overrides). A mock test pins
   that such a cap refuses before the provider and that the job resumes.

### The intermittent S7 test: found, fixed, proven

`test_s7_steer_enters_the_next_context_and_cancel_packages` failed once in a full local run.

| Step | Result |
| --- | --- |
| Reproduce: the unmodified test, 50 isolated runs, full output kept | **9 failed, 41 passed (18%)** |
| The failing assertion, the same in all nine | `'Prefer at-least-once' in '[]'`: no `architect-draft` gateway call existed |
| After the fix: the three S7 tests, 100 iterations on a frozen checkout of `93a14e9` | **100 green, 0 failed** |

**Root cause, in the test.** It waited for the status view to show phase `draft` and then
sent `pause` and `cancel`. The phase becomes visible before the draft step's boundary, so
both signals could land before the draft call was made, and the session was cancelled with
no draft. The earlier guess (the frame step compiled before the steer) was wrong; the
captured assertion showed what actually happened.

**A real workflow bug, found while reading the signal handling.** A steer sent after the
loop's last step boundary, or while the session waited for the owner, was never recorded:
the buffer was only drained at step boundaries. A new test pins it by holding the session
inside its last step; it failed on the old workflow.

**The fix.** In the workflow, signals only record (a flag or a buffer) and are consumed at
well-defined points: steers at every step boundary, at the loop's exit and at the human
gates; decisions only at a gate (rule 12 in CLAUDE.md). In the tests,
`session_fixtures.ActivityGate` holds the session inside a named activity through the
activities' hooks and the test awaits a `threading.Event`: the pause is sent while the
first activity is held, the cancel while the attack on the draft is held. No polling of a
transient state, no sleeps. The original assertions are kept and three are added (the
steer is recorded before the pause takes effect; the session is paused before `repair`;
no repair was asked for after the cancel). "Pause stops progress" is now checked by sending
another signal and querying the settled state, instead of sleeping 1.5 s.

### Part A: the gate

- **Every outcome but `cancelled` ends at the human gate** in `awaiting_approval` with its
  package: `completed`, `completed_with_risks`, and now `stopped_budget` and `stopped_time`.
  Cancel ends directly: the human already decided.
- **Decisions.** `approve` applies only when the package's gate verdict is ALLOWED.
  `approve_with_risks` applies to any package, needs a non-empty reason, and records one
  `waiver.signed` per open blocking reason: actor the human, `risk` the reason, `target_ref`
  the check (or objection). `reject` always applies. Final statuses: `approved`,
  `approved_with_risks`, `rejected`.
- **`extend(tokens?, usd?, wall_clock_minutes?, rounds?)`** applies to a session stopped by
  budget or wall clock. The amounts are added to the current limits, written as a new
  `budget.updated` signed by the human, and the attack/repair/verify loop resumes in a new
  round from the best version so far (a new head is created from it when it is not the
  head already), then converges and packages again. A time-stopped session needs
  `wall_clock_minutes`; a budget-stopped one needs `tokens` or `usd`.
- **Refusals.** A decision the open gate cannot take is refused with a recorded reason
  (`last_refusal` in the status query and in `ses_sessions`) and the gate stays open. A
  decision sent while no gate is open is refused, not kept. The CLI and the API apply the
  same rules from the read model first (`service.decision_problem`), so the owner hears
  "no" at once: exit code 1, or HTTP 422 `DECISION_REFUSED`.
- **The wall clock counts running time.** Time spent waiting at a human gate moves the
  deadline; paused time still counts.
- **CLI/API**: `architect session approve | approve-with-risks --reason R | reject |
  extend [--tokens N] [--usd X] [--wall-clock-minutes M] [--rounds R]`, each with
  `--signer` (default `$ARCHITECT_USER`, else the login name);
  `POST /v1/projects/{pid}/sessions/{sid}/{approve|approve-with-risks|reject|extend}` with
  `{reason?, signer?, tokens?, usd?, wall_clock_minutes?, rounds?}`.

### Part B: seed models (review mode)

`architect session start --seed model.json` (API: `seed`). The seed is a SystemModel
document. After genesis it is committed as one patch (`model.patch_proposed` then
`model.patch_committed`, one `add_element` per element and one `add_link` per link) through
the Arbiter, so it must be valid; the draft phase is skipped and the session goes to the
attack. Frame still runs on the brief. A seed the Arbiter refuses fails the session cleanly:
status and outcome `failed`, and the typed rejection as `failure`
(`the seed model was refused: {"code": "INVALID_MODEL_RESULT", ...}`).

A brief can fix its requirement ids: a list item that starts with `[slug]` is the
requirement `req_<slug>`, whatever slug the model chooses (the pipeline maps a returned
requirement to the labelled line that contains its quote). A seed's `requirement_refs` and
SATISFIES links can then name requirements before any session has run.

### Part C: the console

- **`architect session watch --session S`** (`rich`): status and outcome, the phase timeline
  with durations, the round, the gate's blocking reasons, each failing check with one line
  of evidence, the open risks, spend against budget, the last five agent messages, and at a
  gate the decisions that apply. `console.snapshot` reads the read models into plain data;
  `console.render` is a pure function of that snapshot (durations come from the timeline
  and the snapshot's own `as_of`). `--once` prints one frame.
- **`architect model diff --project P V1 V2`** (`modeldiff.py`, pure): elements added,
  removed and changed field by field, links added and removed. `session show` prints the
  diff each round's repair made ("diff vs previous version").
- **`architect why --project P --element E [--version V]`**: the M2 why-trace as text:
  element, requirements, the claims stating them, ADRs, their evidence claims, and each
  source with its locator.
- `architect new-project P` creates a project from the command line.

### Part D: golden task gt-001 and the golden runner

- **`goldens/gt-001/`**: `brief.md` (four measurable requirements and "the system should be
  robust"), `seed.json` (an API gateway, a queue, a worker pool and a datastore, an external
  client and a trust boundary around the cluster), `expected.yaml` (the answer key),
  `mock/` (the scripted architect's outputs).
- **Exactly four planted flaws**, verified by a test against the real checks: F1 the
  gateway→queue flow has no `backpressure_ref` (C-012); F2 the client→gateway flow crosses
  the boundary without `input_validation` (C-008); F3 the worker pool is stateful and
  durable with no recovery block (C-007); F4 the datastore declares `max_qps` 2000 against a
  required 2250 (C-005, a fail, not an error). Every other check passes or is skipped.
- **`architect golden run gt-001 [--mode review|design] [--live] [--kill-after attack]`**
  creates a fresh project, starts a real session with the task's usd cap ($3), and runs the
  worker in a process of its own. With `--kill-after attack` the first worker ends abruptly
  (`os._exit`) once the first attack phase has completed and a second one resumes the
  session from its history. At the end gate the runner approves an ALLOWED package and
  rejects any other; it never signs a waiver. When no Temporal server is configured it
  starts the SDK's dev server for the run.
- **The scorecard** (`goldens/scorecard.schema.json`, validated on write): mode, live or
  mock, each planted flaw with the round it was caught and the round it was repaired, the
  outcome and status, the gate verdict and reasons, requirement coverage (C-001), the
  linter's risks, whether a kill/resume was performed and was clean (dense seq, no duplicate
  idempotency key), rounds, duration, tokens, usd, and each pass criterion as evaluated.
  Mock scorecards go to `data/golden/`; live ones to `goldens/results/`.

### Part F

`docs/getting-started.md`: install, PostgreSQL and Temporal on Windows without Docker and
on Linux with Docker Compose, the key, the worker, a session from a brief, watching it,
deciding at the gate, reading the result, and running a golden task.

### Decisions worth knowing

- **Existing tests that asserted the old gate semantics were updated**: S2, the second S3
  test, S4 and S5 ended in `approved` or in the stopped status directly; they now decide at
  the gate with `reject` and assert the outcome separately. L4 closes the gate with
  approve-if-ALLOWED-else-reject. Their other assertions are unchanged.
- **The frame prompt gained one sentence** (use the brief's bracket labels as slugs) and is
  now `architect-v2`. This is about requirement ids, which a seed needs; it is not tuned to
  repair quality, which Part E reports and does not gate.
- **The worker of a golden run ends when its stdin closes.** On Windows a virtualenv's
  `python.exe` is a launcher, so killing the process the runner started would leave the real
  interpreter running.
- **`--kill-after attack` is the worker ending itself**, right after the attack activity has
  completed and while refusing further activities, rather than an outside kill at an
  arbitrary instant: an activity cut off mid-flight would wait out its 30-minute timeout
  before Temporal retried it. (M7-live Part 1 added heartbeats and a real outside kill,
  `--kill-mode external`; this is now `--kill-mode self`.)
- **A waiver's `target_ref` is the check id** (one waiver per blocking reason, as decided),
  which waives that check for the model, not for one element. (Changed in M7-live Part 1:
  one waiver per check and element.)
- **Agent messages have an insertion-order column** (`ag_messages.n`): their `ts` is the
  session clock's origin and cannot order them.

### Exit tests (mock and real dev server; no key)

| # | Exit test | Result | Evidence |
| --- | --- | --- | --- |
| P1 | Gate: budget stop → `awaiting_approval` with its package; `extend` resumes from the best and completes; `approve` on BLOCKED refused; `approve_with_risks` without a reason refused, with one records one human waiver per blocking reason; cancel → no gate | green | `test_m7_gate_seed.py::test_p1_a_budget_stop_waits_at_the_gate_refuses_what_cannot_apply_and_extend_resumes`, `::test_p1_approve_with_risks_signs_one_human_waiver_per_blocking_reason`, `::test_p1_cancel_ends_directly_and_a_decision_without_an_open_gate_is_refused`, `::test_p1_the_read_model_predicts_what_the_workflow_refuses` |
| P2 | Diff: added, removed, changed elements and links, field-level changes, identical versions → empty | green | `test_m7_console.py::test_p2_*` (4 tests) |
| P3 | Watch: the frame of a recorded session shows phases, blocking reasons, risks and spend | green | `test_m7_console.py::test_p3_a_frame_is_a_pure_function_of_its_snapshot`, `::test_p3_watch_shows_a_recorded_session_and_the_cli_takes_the_gate_decisions` |
| P4 | Seed: committed through the Arbiter, draft skipped; an invalid seed refused with the typed error, session `failed` with the reason | green | `test_m7_gate_seed.py::test_p4_a_seed_is_committed_through_the_arbiter_and_the_draft_is_skipped`, `::test_p4_an_invalid_seed_is_refused_by_the_arbiter_and_the_session_fails_cleanly` |
| P5 | Golden (mock): review catches F1 to F4 in round 1 with exactly C-012, C-008, C-007, C-005; the seed is clean otherwise; the scripted repairs → ALLOWED; the scorecard validates; the linter flags the unmeasurable requirement | green | `test_m7_golden.py::test_p5_*` (4 tests) |
| P6 | Golden kill/resume (mock, real dev server): `--kill-after attack` → clean resume in the scorecard | green | `test_m7_golden.py::test_p6_killing_the_worker_after_attack_resumes_cleanly` |
| | S7 deterministic, the lost steer pinned | green | `test_sessions.py::test_s7_*` (3 tests); 100 of 100 iterations |
| | CLI and API of Parts A to D; every pre-existing test green | green | `test_m7_console.py`, `test_m7_golden.py::test_golden_run_from_the_command_line`; the run below |

CI output (branch head `e4ffea5`, push run 37134603339 on `ubuntu-latest` with `postgres:16`
and a Temporal dev server started by the Temporal CLI; the pull_request run 37134607917
agreed):

```
================ 323 passed, 4 deselected in 105.73s (0:01:45) =================
```

Nothing failed and nothing was skipped; the four deselected tests are the live ones (L1 to
L4). Every P1 to P6 test, the three S7 tests, S6 and S11 are PASSED by name in that log.
Earlier CI on the S7 fix alone (`93a14e9`): run 37131649796, success.

Local runs on Windows (portable PostgreSQL 16, the SDK's own Temporal servers, mock providers
only):

| Commit | Run | Result |
| --- | --- | --- |
| `390ab5b` (amendments only) | full suite, first | 297 passed, **1 failed** (the S7 test above), 4 deselected |
| `390ab5b` | full suite, second | 298 passed, 4 deselected |
| `4b87d58` | the unmodified S7 test, 50 isolated runs | 41 passed, **9 failed** |
| `93a14e9` (the fix) | the three S7 tests, 100 iterations | 100 green |
| `8e2d2a9` (Parts A to D) | full suite | 322 passed, 4 deselected in 585 s |

### M7-live Part 1: three fixes (no key needed)

Built on branch `m7-live` in a session where the app's key was again not visible, so the
live half is still to run. **Phase 1 exit test: PENDING LIVE RUN.**

**1. Waivers name the element.** `approve_with_risks` signs one waiver per (check, element)
of the package's blocking reasons, with `target_ref` `"<check_id>:<element_id>"`, the form
the gate already matched. A blocking reason with no element refs gets a whole-check waiver
(`target_ref` the check id), and a blocking objection keeps its objection id. The targets
are computed by `activities.waiver_targets`, deduplicated and in order, and the Arbiter key
of each waiver derives from (session, gate, target), so a retried activity signs nothing
twice.
A new element failing the same check after the approval is not covered: the gate blocks on
it and reports the earlier element as waived.

**A gate defect found on the way.** `runner.recorded(as_of)` read the results whose
`as_of_seq` was exactly the ledger seq asked for. A re-run of the battery on an unchanged
head records nothing for the checks whose inputs did not change (the runner is idempotent on
the inputs hash), so those checks had no row at the new seq and the gate called them
`not_evaluated`. Two visible effects: after a waiver the gate lost the results it had not
re-recorded, and a session whose later rounds changed nothing scored its head on a partial
battery and could pick the draft over the repaired version as its best. `recorded` now takes
each check's latest result at or before the seq. Three tests fail on the old behaviour.

**2. L3 at its cap is a partial baseline.** The default cap is $2.50. When the gateway
refuses the call that would cross it, L3 commits what both passes finished
(`Pipeline.commit_processed`: the candidates of the segments pass B has answered, through
the same quarantine and the Arbiter, without moving the job's stage) and reports a PARTIAL
baseline: segments answered of segments with candidates of segments in the document, claims
by grade, drops, usd and calls. It fails only on errors. Pass B's metrics are now counted
per segment, so a stopped job reports what it did; a later run with a higher cap resumes
where it stopped and commits the rest, each claim once.

**3. A real kill.** `architect golden run ... --kill-after attack --kill-mode external`
(the default with `--live`) kills the worker process from outside: the runner polls the
ledger, and when it shows the session entering the phase after the attack it terminates the
worker's process tree (`taskkill /F /T` on Windows, where the interpreter runs behind the
virtualenv's launcher; `SIGKILL` to the worker's own process group elsewhere) and starts a
new worker. The worker takes no part in it and gets no chance to clean up. `--kill-mode
self` (the default in mock mode) is the earlier behaviour, the worker ending itself.

What made an outside kill workable is **activity heartbeats**. Before, an activity cut off
in mid-flight was retried only when its 30-minute start-to-close timeout ran out. Now every
activity is scheduled with a heartbeat timeout (`temporal.heartbeat_seconds` in
`config/presets.yaml`, 30 s), and the activity wrapper beats from a helper thread at a third
of it for as long as the activity runs, however long a model call takes. When the worker
dies the beats stop and Temporal retries the activity on another worker within the timeout.
The retry is safe for the reason every retry is: Arbiter keys and ids derive from
(session, phase, round, step).

In a mock run a whole session takes about a second, so the first worker of an external
mock run is started with `--hold-after attack`: once the session has left the attack phase
it parks inside the next activity (heartbeating) until it is killed. That only fixes where
the kill lands. A live worker gets no such flag: the kill lands wherever the repair phase
happens to be, usually in the middle of a model call.

The scorecard records `kill_mode` (`self`, `external` or null) and, for an external kill,
`events_at_kill`: how many events the ledger held once the worker was dead. Everything
after them was written by the worker that resumed.

| Fix | Test | Result |
| --- | --- | --- |
| 1 | `test_m7_gate_seed.py::test_p1_approve_with_risks_signs_one_human_waiver_per_blocking_check_and_element` (P1's waiver assertions, updated with the owner's authorization) | green |
| 1 | `test_m7_gate_seed.py::test_p1_a_waiver_covers_its_element_only_and_a_new_one_failing_the_check_is_not_covered` | green |
| 1 | `test_m7_gate_seed.py::test_p1_waiver_targets_name_elements_and_fall_back_to_the_check_only_without_any` | green |
| gate | `test_checks_runner.py::test_the_gate_keeps_the_results_a_rerun_did_not_have_to_record_again`, `test_sessions.py::test_the_best_version_is_the_repaired_one_when_later_rounds_change_nothing` | green |
| 2 | `test_ingestion.py::test_a_job_stopped_at_its_cap_commits_what_both_passes_finished_and_resumes` | green |
| 2 | L3 itself (`test_ingestion_live.py`), cap $2.50, partial baseline at the cap | not run: no key |
| 3 | `test_m7_golden.py::test_p6_a_worker_killed_from_outside_in_the_middle_of_an_activity_is_resumed_cleanly` (mock, real dev server) | green |
| 3 | `test_m7_golden.py::test_the_kill_mode_defaults_to_self_in_mock_and_is_recorded_in_the_scorecard`, `::test_a_phase_is_complete_once_the_ledger_shows_the_session_entering_the_next_one` | green |
| 3 | `test_sessions.py::test_an_activity_heartbeats_for_as_long_as_it_runs_and_stops_when_it_ends`, `::test_every_activity_of_a_session_is_scheduled_with_the_heartbeat_timeout` | green |
| | P6 with the worker ending itself, S6, S11 and every other pre-existing test | green |

CI output (branch head `5e58f1a`, push run 37183782227 on `ubuntu-latest` with `postgres:16`
and a Temporal dev server started by the Temporal CLI; the pull_request run 37183787888
agreed):

```
================ 333 passed, 4 deselected in 136.32s (0:02:16) =================
```

Nothing failed and nothing was skipped; the four deselected tests are the live ones (L1 to
L4). Every test in the table above is PASSED by name in that log, the outside kill included
(there it is `SIGKILL` to the worker's process group), with P6's self-ending kill, S6 and S11.

Local runs on Windows (portable PostgreSQL 16, the SDK's own Temporal servers, mock providers
only):

| Commit | Run | Result |
| --- | --- | --- |
| `baad5ca` | full suite | 333 passed, 4 deselected in 680 s |
| `5e58f1a` | the outside-kill test and the two heartbeat tests, 20 iterations (here the kill is `taskkill /F /T`) | 20 green, 0 failed |

### M7-live: what remains

Run in a session started from a shell where `ARCHITECT_ANTHROPIC_API_KEY` is set.

1. **Step 0**: `pytest -m live -v`. L1 (gateway), L3 (extraction of the arXiv paper, under
   its $2.50 cap; a partial baseline if the cap is reached), L4 (a quick session). L2 is
   deselected unless an OpenAI-compatible server is configured. Report each result and its
   usd, the quarantine rate in L3, the timeline in L4.
2. **Part E**, each under the $3 cap:
   `architect golden run gt-001 --mode review --live --kill-after attack` (the kill is
   external by default) and `architect golden run gt-001 --mode design --live`.
   Review passes when all four flaws are caught in round 1, the resume is clean, a final
   model exists and the session ends with a package. Design passes when a model is produced,
   C-001 passes or every unsatisfied requirement is an open risk, and a package exists.
   Repair quality is reported, not gated.
3. Commit both scorecards under `goldens/results/` and paste them here.

## M8: knowledge graph, entity resolution, hybrid retrieval, communities

Phase 2 starts here. The flat claim store becomes a graph, searchable by meaning and
summarized by theme. Retrieval is claim-first: every hit is a claim with its status, grade,
conditions, taint and evidence locators, never a bare chunk of text. Built on branch
`m8-graph-retrieval`. The live Phase 1 exit test is still pending (M7-live Part 2).

### Step 0: the gateway never loses a paid call

A provider attempt is now two `gw_calls` rows. A `started` row (the request, the prompt
hash, the tokens and usd reserved) commits together with the reservation BEFORE the provider
is called. The row that closes it (`ok`, `invalid_output` or `error`) names it in
`started_id` and settles the spend in the same transaction. `state` is derived from
`status`: started | completed | failed | abandoned. The table stays append-only.

A `started` row that nothing closed is a call the process did not live to record.
`Gateway.sweep_abandoned` closes it with an `abandoned` row and turns its RESERVED cost into
spend: the provider may have been paid, so the books over-count and never under-count.
`gw_spend.tokens`, `usd` and `calls` include abandoned calls and `abandoned_tokens`,
`abandoned_usd`, `abandoned_calls` report them on their own (`architect gateway spend`). An
abandoned reservation counts against a cap like any spend. The sweep runs:

- when a session activity is retried (the retry is the evidence, so no minimum age applies);
- when an ingestion job resumes, in the worker every `abandon_after_s`, and from
  `architect gateway sweep` (calls older than `write_ahead.abandon_after_s`, 30 s, the
  heartbeat timeout).

The sweep cannot tell a dead call from a slow one. If an abandoned call does finish, closing
it puts the books right: the actual numbers replace the reservation. Two partial unique
indexes make "closed once, abandoned once" a fact of the table. Replay ignores `started` and
`abandoned` rows. Heartbeats: every 10 s, timeout 30 s (`config/presets.yaml`).

Ten existing tests compared the exact list of call statuses; with the owner's authorization
each list gained the `started` row of every attempt (they are still exact):
`test_gateway.py` (seven tests), `test_ingestion.py::test_h1_*`,
`test_m7_gate_seed.py::test_p1_a_budget_stop_*`, `test_sessions.py::test_s4_a_tight_token_cap_*`.

### The graph model

A projection like every read model: folded by the projector in the cursor's transaction,
rebuildable from the ledger, deterministic.

| Node type | From |
| --- | --- |
| `source` | `source.ingested` |
| `claim` | `claim.committed` |
| `entity` | the subject and object of a committed claim that name an id (`ent:<type>:<id>`); a literal is a value, not a node |
| `requirement` | a claim subject of type `requirement`, or a requirement element of the head model |
| `element` | the elements of the HEAD model (replaced with every new head) |
| `adr` | `decision.recorded` |
| `community` | the subject of a community summary claim |

Edges are everything in `proj_edges` (EVIDENCES, DERIVED_FROM, SUPERSEDES,
DECISION_EVIDENCE, and the head model's SATISFIES, DEPENDS_ON, MITIGATES) plus, for every
committed claim, `(subject) -[PREDICATE]-> (object)` when both are entities and
`(claim) -[ABOUT]-> (entity)` for each entity it names (`proj_graph_edges`). Provenance (P2):
every edge row carries the seq of its event and a claim-derived edge its claim id, both NOT
NULL. Status, grade and conditions are read from the claim when an edge is returned.

Entity merges resolve at read time: `proj_entity_alias` maps a merged id to the kept one,
recomputed from the merges that stand. A reverted merge leaves the alias table, so the graph
is what it was.

### Two backends, proven identical

| Interface | Backend | Where | What it is |
| --- | --- | --- | --- |
| GraphStore | `age` | CI (the knowledge image), Docker | the resolved graph loaded into an Apache AGE graph, traversed with Cypher one hop at a time; reloaded when the projection's cursor moves |
| GraphStore | `sql` | everywhere | recursive CTEs over the projection tables |
| VectorIndex | `pgvector` | CI (the knowledge image), Docker | a mirror table with a pgvector column and an HNSW index (cosine) |
| VectorIndex | `exact` | everywhere | exact cosine in numpy over `emb_vectors` |

`auto` (the default in `config/knowledge.yaml`) picks the extension when it is installed.
The projection tables and `emb_vectors` are the record for both; the AGE graph and the
pgvector table are indexes that can be dropped. Queries: `neighbors(node, depth <= 3,
edge_types?)`, `paths(from, to, max_depth <= 6, edge_types?)` (every shortest path),
`subgraph(node_ids)`.

### Entity resolution

Blocking: same entity_type AND (a shared name token OR slugs within edit distance 2). Score:
the cosine of the entities' embeddings (the name, with up to 3 predicates it appears in).
At or above **0.92**: merge (method `embedding`). From **0.80** to 0.92: a model adjudicates
through the gateway (tier-cheap, purpose `entity-adjudicate`, structured `{same, reason}`,
the names passed as untrusted data); a yes merges (method `llm_adjudicated`). Below 0.80:
distinct. A merge is an `entity.merged` event through the Arbiter, keeping the entity more
claims are about. `entity.merge_reverted` (signed by a human) undoes it, and the reverted
merges in the ledger are the record of pairs never to propose again. Types never mix.

### Embeddings

`Gateway.embed(texts, purpose, scope)`: one path, recorded in `gw_calls` (written ahead),
cached by (model, content hash) in `gw_embed_cache`, budget-scoped. The call log records
counts and the hash of the batch, not the texts. Providers: the mock (deterministic vectors
from hashed words; every CI test but the smoke job), `openai_compat` (`POST /v1/embeddings`)
and `fastembed` (local ONNX on CPU; optional: `pip install -e ".[embeddings]"`). The model id
is in `config/models.yaml` under the `embedding` tier, and nowhere else. What is embedded:
each committed claim's canonical text (subject, predicate, object, magnitude, conditions,
quote) and each entity's name. `emb_vectors` remembers the hash of the text, so a claim or an
entity is embedded again only when its text or the model changes. The knowledge plane's
calls are charged to the scope `{session: "knowledge:<project>"}`.

### Retrieval fusion

`search(project, query, k=20, filters)` runs four signals and fuses their ranks:

| Signal | What ranks |
| --- | --- |
| `text` | Postgres full-text (`tsvector`, English) over each claim's canonical text and quote |
| `vector` | nearest neighbours of the query among the claim embeddings |
| `graph` | the entities the query matches (by name and by meaning, at most 5) and the claims one hop from them; a claim about better-matched entities ranks higher |
| `community` | the current community summaries, when `scope=global` or no entity matched |

Reciprocal Rank Fusion: `score = sum over signals of 1 / (60 + rank)`, ties by claim id. Each
signal contributes up to 50 candidates. Filters: grades (default design_grade and
unverified), statuses (default: not refuted, not retracted), taint origins, entity scope.
Quarantined proposals are returned only when `grade=quarantined` is asked for. Each hit
lists the signals that found it. Confidence is returned for display; it never includes or
excludes (rule 10). Without an embedding provider the vector signal is absent and the others
run. The Context Compiler's `rank_claims` now calls this search.

### Communities

Leiden (leidenalg over python-igraph), seed 7, over the entity graph (entities related by
live claims, weighted by how many). Level 0 at resolution 1.0; level 1 is Leiden again over
level 0's communities at resolution 0.5, so the levels nest. One summary per community of at
least 2 entities, written by tier-mid (purpose `community-summary`) from its top 12 claims
(design grade first), untrusted claims wrapped. The summary is a claim: subject
`{community, comm_<level>_<hash of members>}`, predicate SUMMARIZES, object `{text, ...}`,
status `inferred`, `derived_from` the claims used, taint the most restrictive among them.
A community's input hash (members and the claims used) decides regeneration: unchanged
communities never call the model again.

### Commands

```
architect search --project P "what prevents split brain" [-k 20] [--grade design_grade]
                 [--taint user] [--entity ent:protocol:fencing-token] [--scope global] [--json]
architect graph counts --project P
architect graph neighbors --project P ent:protocol:fencing-token --depth 2 [--edge-type PREVENTS]
architect graph path --project P FROM TO [--max-depth 4]
architect resolve-entities --project P [--revert EVENT_ID --signer NAME]
architect communities rebuild --project P ; architect communities list --project P [--level 0]
architect gateway sweep [--session S] [--older-than SECONDS]

GET /v1/projects/{pid}/search?q=&k=&grade=&status=&taint=&entity=&scope=
GET /v1/projects/{pid}/graph ; GET .../graph/nodes/{id}/neighbors?depth=&edge_type=
GET /v1/projects/{pid}/graph/path?from=&to=&max_depth= ; GET /v1/projects/{pid}/communities[?level=]
```

### Decisions worth knowing

- **Merges resolve at read time, not by rewriting edges.** Raw edges keep the ids the claims
  used; the alias table says what they answer to. A revert is one deleted alias.
- **The AGE graph is a loaded copy**, rebuilt in full when the cursor moves. Fine for the
  graphs of today; an incremental load is the obvious next step when a project grows.
- **AGE never runs on a pooled connection.** It keeps per-session caches, and a session that
  has dropped a graph failed later on an unrelated statement ("label (relation) cache
  corrupted", AGE 1.6). Each AGE query opens a connection and closes it.
- **The AGE backend traverses breadth-first over one-hop Cypher queries** rather than one
  variable-length pattern: AGE's support for filtering relationship properties inside a
  variable-length match is thin, and it makes the two backends independent implementations.
- **Retrieval's one-hop graph expansion reads the projection tables directly** on both
  backends: it is an indexed lookup, not a traversal, and it keeps search independent of the
  AGE load.
- **A community summary is returned only through the community signal.** In a local search
  it would compete with the claims it was written from.
- **Session tests run retrieval without vectors.** Their model tables have no embedding
  tier, so the compiler ranks with full text and the graph and a session's call log holds
  only its own calls. `test_k7_*` exercises the compiler with embeddings.
- **The search endpoint brings the search index up to the read models before it answers**
  (and embeds the query): it is a GET that may write derived index rows, never an event.
- **Quotes.** A claim carries only a locator, so the pipeline now keeps the verbatim quote
  it committed a claim with (`ing_claim_quotes`) for the claim's searchable text. Search
  never returns it: a hit is the claim and its locator.

### An intermittent deadlock at startup: found in CI, reproduced, fixed

After the exit tests were green, one CI run on the knowledge image failed two session tests
(`test_sessions.py::test_session_cli` and `test_m7_console.py::test_p3_watch_*`) with
`psycopg.errors.DeadlockDetected`. Three earlier runs of the same code had passed.

| Step | Result |
| --- | --- |
| CI, run 37191610184 on `d08d665`, job test (age-pgvector) | **2 failed**, 371 passed |
| The failing statement, the same in both | `ensure_schema` in the CLI's `main`: "waits for AccessExclusiveLock on relation (gw_spend); blocked by process B. Process B waits for RowExclusiveLock on relation (gw_calls)" |
| Reproduce: the two unmodified tests, 12 isolated runs on a frozen checkout of `d08d665`, output kept | **2 failed, 10 passed**, both with that deadlock |
| Reproduce deterministically | `test_schema_bootstrap.py::test_applying_the_ddl_under_a_writer_is_the_deadlock_and_the_fast_path_avoids_it` |
| After the fix: the two tests, 100 iterations on a frozen checkout of `9f661ad` | **100 green of 100, 0 failed** |

**Root cause, in the code.** Every process re-applied `schema.sql` when it started. The DDL
is idempotent but not free: `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, `CREATE INDEX IF NOT
EXISTS` and `CREATE OR REPLACE TRIGGER` take table locks that conflict with writers, one
table after another, and hold them until the transaction ends. Step 0 made a gateway
transaction write `gw_spend` and then `gw_calls` (the reservation and the `started` row
commit together), and added ALTERs on both tables. A command starting at that moment held
`gw_calls` and waited for `gw_spend`, while the worker held `gw_spend` and waited for
`gw_calls`. Postgres broke the cycle by failing one of them; it could as well have been the
worker's model call.

**The fix.** `ensure_schema` applies the DDL only when it has something to do: the sha256
of `schema.sql` is kept in `schema_meta`, and the DDL runs when it differs (an upgrade) or
when a table the schema creates is missing. A process starting on a current schema reads
two catalog queries and takes no table lock. `architect init-db` forces it. The same rule
was applied to the pgvector mirror table, which a new index instance used to re-declare on
first use. Tests: a start under an open writer transaction returns at once; the forced DDL
under the same writer is the deadlock; a dropped table or a changed hash is applied again;
four processes starting on an empty schema apply it once.

### Exit tests

| # | Exit test | Result | Evidence |
| --- | --- | --- | --- |
| K0 | Step 0: a started row exists before the provider is called; an unclosed call is abandoned with its reservation charged; conservative spend; the retried call completes; a worker killed mid-call | green | `test_gateway.py::test_k0_*` (7 tests), `test_sessions_server.py::test_k0_a_worker_killed_in_the_middle_of_a_model_call_loses_no_spend` (real dev server, OS-level kill), `test_ingestion.py::test_a_resumed_job_abandons_the_call_its_dead_run_started` |
| K1 | Graph: node and edge counts on the fixture and on the M5 ingestion corpus match the pinned tables; rebuild identical; every edge has provenance | green | `test_m8_graph.py::test_k1_*` (4 tests) |
| K2 | Backend parity: GraphStore sql vs age on 20 seeded queries; VectorIndex pgvector (exact mode) vs numpy on 20 queries; HNSW recall recorded | green | `test_m8_graph.py::test_k2_the_age_backend_gives_the_same_answers_as_sql_on_twenty_seeded_queries`, `test_m8_retrieval.py::test_k2_pgvector_in_exact_mode_gives_the_same_top_k_as_numpy_and_hnsw_recall_is_recorded` (both on the knowledge image); each fallback against a reference: `::test_k2_the_sql_graph_store_agrees_with_a_reference_traversal`, `::test_k2_the_exact_vector_index_agrees_with_brute_force` (both jobs) |
| K3 | Entity resolution with scripted embeddings: auto-merge, adjudication, types never mix, merges are events, revert restores the graph, a re-run proposes nothing, a reverted pair is not proposed again | green | `test_m8_graph.py::test_k3_*` (4 tests) |
| K4 | Embeddings recorded in gw_calls, cached (no second provider call), budget-scoped; fastembed embeds 3 sentences | green | `test_m8_retrieval.py::test_k4_*` (4 tests); smoke job: `::test_k4_fastembed_embeds_three_sentences_and_similar_ones_are_closer` |
| K5 | Retrieval golden (61 claims, 10 queries): every expected claim in the top 5; one query only vectors answer, one only full text, hybrid both; quarantined never by default; taint filter; signals listed | green | `test_m8_retrieval.py::test_k5_*` (4 tests) |
| K6 | Communities: same partition on rebuild; summaries are inferred claims with derived_from and propagated taint; one changed claim regenerates exactly the affected summaries; a global query returns a summary | green | `test_m8_retrieval.py::test_k6_*` (3 tests) |
| K7 | The S1 session and the P5 golden pass on the new compiler; S9's rules hold | green | `test_sessions.py::test_s1_*`, `::test_s9_*` and `test_m7_golden.py::test_p5_*`, all unchanged; `test_m8_retrieval.py::test_k7_the_compiler_ranks_with_hybrid_retrieval_and_keeps_its_rules` |
| K8 | 10,000 synthetic claims on pgvector: search p95 < 500 ms | green | `test_m8_retrieval.py::test_k8_search_over_ten_thousand_claims_stays_under_half_a_second_at_p95`: **p50 23.5 ms, p95 25.3 ms** |
| K9 | Search never returns raw segment text without its claim; the knowledge package imports no provider SDK | green | `test_m8_retrieval.py::test_k9_*` (2 tests) |
| | API and CLI of the knowledge plane; every pre-existing test | green | `test_m8_retrieval.py::test_search_graph_and_community_endpoints`, `::test_search_graph_resolve_and_communities_commands`; the runs below |

Numbers from CI (push run 37190781914):

```
K2 HNSW recall@10 over 2000 vectors of 64 d, 20 queries: 1.000
K8 search over 10,000 claims (4887 entities, pgvector HNSW, 64 d): p50 23.5 ms, p95 25.3 ms, max 29.6 ms over 100 queries; indexing 23.8 s
fastembed BAAI/bge-small-en-v1.5: similar 0.777, unrelated 0.471
```

K8 uses the mock embedder's 64-d vectors and claims written straight into the read models
(the ledger path would fold grades 10,000 times over); it times the whole search: the query
embedding, full text, pgvector HNSW, the entity match, the graph expansion and the fusion.

CI output (branch head `9f661ad`, push run 37192278371 on `ubuntu-latest`, with a Temporal
dev server started by the Temporal CLI; the pull_request run 37192280760 agreed):

```
test (age-pgvector)   ================ 378 passed, 5 deselected in 234.01s (0:03:54) =================
test (plain)          ================ 374 passed, 9 deselected in 203.12s (0:03:23) =================
fastembed-smoke       ====================== 1 passed, 382 deselected in 3.13s =======================
```

Nothing failed and nothing was skipped. On the knowledge image (Postgres 16, Apache AGE 1.6,
pgvector 0.8) the five deselected tests are the four live ones and the fastembed smoke test.
On the plain Postgres the AGE parity test, the two pgvector tests and K8 are deselected as
well: they run where the extensions exist. The K2 and K8 numbers above are from run
37190781914 (`4b82d37`); on `9f661ad` K8 measured p50 21.7 ms, p95 23.5 ms.

The first CI run of the branch (`9b7b911`, run 37189691150) passed every M8 test on the
knowledge image and then failed 100 later tests at setup: a test's teardown had dropped an
AGE graph on the suite's long-lived admin connection, and AGE then failed that session's
next statement with "label (relation) cache corrupted". The fix is in the product, not only
the tests: the AGE backend now runs every query on a connection of its own.

Local runs on Windows (portable PostgreSQL 16, no extensions: the fallbacks; mock providers):

| Commit | Run | Result |
| --- | --- | --- |
| `a88394e` (step 0) | full suite | 340 passed, 4 deselected in 505 s |
| `9b7b911` | full suite | 368 passed, 8 deselected in 744 s |
| `d08d665` (before the startup fix) | the two session CLI tests, 12 isolated runs | 10 passed, **2 failed** (deadlock) |
| `9f661ad` (the fix) | the two session CLI tests, 100 iterations | 100 green |

## Windows

The database-backed tests ran only on Linux in CI through M5. With M6 the development
machine runs the whole suite against a portable PostgreSQL 16 (the EDB binaries zip, no
Docker) and Temporal's downloaded test servers: green, same counts as CI (295 passed, 4
deselected).

## Open

1. **M7-live Part 2** (still open after M8): the live baseline (L1, L3, L4) and the two live golden runs that
   decide the Phase 1 exit test. They need `ARCHITECT_ANTHROPIC_API_KEY` in the launching shell.
2. **contracts-PROPOSALS.md P-6** (the contract scripts' file encoding) and **P-11** (a
   session status event) stay open; P-7 to P-10 were applied in v1.1. Multi-head branching is
   planned for contracts v1.2 with M11.
