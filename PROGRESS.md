# Progress

## Status

| Milestone | State |
| --- | --- |
| M0 scaffold | done |
| M1 ledger + Arbiter | built; **exit tests 1 and 7 are red until the Phase 0 fixture is added** |

M1 is not "done" by its own definition yet. `phase0-contracts/fixture/fixture_ledger.jsonl`
and `fixture/replay.py` are not in the repo (the owner is adding them), so the two exit tests
that run against them fail, and so does the CI step `python3 phase0-contracts/fixture/replay.py`.
Everything else is green. See "Open" at the bottom.

## M0: scaffold

- Python 3.11+ project, src layout, package `architect`, `architect` console entrypoint.
- `docker-compose.yml`: `postgres:16`, plus a Temporal dev server behind the `sessions`
  profile (unused until the sessions milestone). Not exercised locally: the development
  machine has no Docker, so M1 was verified against a native PostgreSQL 16.10.
- pytest + ruff. `phase0-contracts/` is excluded from ruff because it is frozen.
- GitHub Actions CI (`.github/workflows/ci.yml`) with a `postgres:16` service runs, in order:
  `python3 phase0-contracts/validate.py`, `python3 phase0-contracts/fixture/replay.py`,
  `ruff check .`, `pytest`. The workflow has not run yet (the repo has no remote).
- `CLAUDE.md` carries the architecture rules.

## M1: the ledger and the Arbiter

### What landed

- **Schema** (`src/architect/schema.sql`): `projects`, `events`, and the five `arb_*` state
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
| 422 | `SCHEMA_INVALID`, `CLAIM_ID_MISMATCH`, `UNKNOWN_SOURCE`, `UNKNOWN_PROPOSAL`, `UNKNOWN_CLAIM`, `STATUS_MISMATCH`, `UNKNOWN_CAUSE_EVENT`, `PROMOTION_FORBIDDEN`, `PATCH_BASE_MISMATCH`, `OBJECTION_NOT_OPEN`, `WAIVER_NOT_HUMAN`, `PROJECT_MISMATCH`, `MALFORMED_REQUEST` |
| 409 | `BASE_MOVED`, `DUPLICATE_SOURCE`, `DUPLICATE_CLAIM_ID`, `DUPLICATE_EVENT_ID`, `DUPLICATE_GENESIS`, `DUPLICATE_PROJECT` |
| 404 | `UNKNOWN_PROJECT`, `UNKNOWN_EVENT` |

Codes beyond the M1 brief: `DUPLICATE_EVENT_ID` (client `event_id` already committed),
`DUPLICATE_GENESIS` (a `model.version_created` without `parent` when a head exists),
`PROJECT_MISMATCH` (candidate `project_id` differs from the URL), `MALFORMED_REQUEST` (the
body is not what the endpoint takes), `DUPLICATE_PROJECT`, `UNKNOWN_PROJECT`, `UNKNOWN_EVENT`.

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
- **Proposals are tracked per kind** (claim vs model patch); see contracts-PROPOSALS.md P-5.
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
python3 phase0-contracts/validate.py                 # on Windows see contracts-PROPOSALS.md P-1
python3 phase0-contracts/fixture/replay.py
ruff check .
pytest

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
| 1 | Fixture round-trip replays green | `test_exit_fixture.py::test_fixture_round_trips_through_the_arbiter_and_replays_green` | **red: fixture missing** |
| 2 | Refusal suite | the nine tests under "Exit test 2" in `test_refusals.py` | green |
| 3 | Idempotency | `test_arbiter.py::test_idempotent_resubmission_returns_the_original_event` | green |
| 4 | Concurrency, 20 in parallel | `test_arbiter.py::test_parallel_submissions_get_dense_seq_and_a_valid_chain` | green |
| 5 | Append-only trigger | `test_arbiter.py::test_raw_sql_cannot_change_or_remove_a_committed_event` | green |
| 6 | Atomicity | `test_arbiter.py::test_a_failure_before_commit_rolls_back_event_and_state` | green |
| 7 | Rebuild after fixture ingest, zero diff | `test_exit_fixture.py::test_rebuild_state_after_the_fixture_ingest_reports_zero_diff` | **red: fixture missing** |

Tests 1 and 7 have stand-ins that run the same paths on a hand-written 24-event ledger
covering all 19 event types (`tests/builders.py::sample_ledger`):
`test_cli.py::test_ingest_then_dump_round_trips_the_ledger` and
`test_cli.py::test_rebuild_state_reports_zero_diff_on_a_healthy_ledger`. They show the
plumbing works; they do not show agreement with `replay.py`, which nobody has run against
this code.

Output, 2026-10-02, Windows 11, Python 3.11.15, PostgreSQL 16.10:

```
$ pytest
=========================== short test summary info ===========================
ERROR tests/test_exit_fixture.py::test_fixture_round_trips_through_the_arbiter_and_replays_green
ERROR tests/test_exit_fixture.py::test_rebuild_state_after_the_fixture_ingest_reports_zero_diff
=================== 99 passed, 2 errors in 64.61s (0:01:04) ===================

E       AssertionError: the frozen fixture is missing: C:\Projects\yavin-architect\phase0-contracts\fixture\fixture_ledger.jsonl

$ ruff check .
All checks passed!

$ pytest -v   (exit tests 2 to 6, and the stand-ins for 1 and 7)
tests/test_arbiter.py::test_idempotent_resubmission_returns_the_original_event PASSED
tests/test_arbiter.py::test_parallel_submissions_get_dense_seq_and_a_valid_chain PASSED
tests/test_arbiter.py::test_raw_sql_cannot_change_or_remove_a_committed_event[UPDATE events SET payload = '{}'::jsonb] PASSED
tests/test_arbiter.py::test_raw_sql_cannot_change_or_remove_a_committed_event[UPDATE events SET seq = seq + 100] PASSED
tests/test_arbiter.py::test_raw_sql_cannot_change_or_remove_a_committed_event[DELETE FROM events] PASSED
tests/test_arbiter.py::test_raw_sql_cannot_change_or_remove_a_committed_event[TRUNCATE events] PASSED
tests/test_arbiter.py::test_a_failure_before_commit_rolls_back_event_and_state PASSED
tests/test_cli.py::test_ingest_then_dump_round_trips_the_ledger PASSED
tests/test_cli.py::test_rebuild_state_reports_zero_diff_on_a_healthy_ledger PASSED
tests/test_refusals.py::test_documented_claim_without_evidence PASSED
tests/test_refusals.py::test_load_bearing_assumption_without_verification_plan PASSED
tests/test_refusals.py::test_promotion_to_measured_needs_an_experiment_as_cause PASSED
tests/test_refusals.py::test_status_change_with_the_wrong_from PASSED
tests/test_refusals.py::test_waiver_signed_by_an_agent PASSED
tests/test_refusals.py::test_evidence_citing_an_uningested_source PASSED
tests/test_refusals.py::test_patch_on_a_stale_base PASSED
tests/test_refusals.py::test_resolving_an_objection_that_was_never_raised PASSED
tests/test_refusals.py::test_objection_without_a_falsifiable_test PASSED

$ python3 phase0-contracts/validate.py   (via the POSIX glob shim, P-1)
RESULT: ALL GREEN
```

Removing the per-project lock makes exit test 4 fail with a `(project_id, seq)` unique
violation, so that test does depend on the lock.

## Open

1. **Add the fixture.** Drop `fixture_ledger.jsonl` and `replay.py` into
   `phase0-contracts/fixture/` and commit them. Then run `pytest tests/test_exit_fixture.py`.
   Where the Arbiter and `replay.py` disagree, `replay.py` wins and the rule in `rules.py`
   changes. The likeliest places: `model.version_created` with a `parent`
   (contracts-PROPOSALS.md P-4), proposal namespaces (P-5), the `project_id` rewrite on
   ingest, and whether `replay.py` accepts the `prev_hash` the Arbiter adds.
2. **CI has never run.** It needs a GitHub remote, and it stays red until item 1.
3. **`docs/`** holds the architecture spec (`.docx`). It is untracked; commit it or ignore it.
4. **contracts-PROPOSALS.md** has five entries for the next contract version.
