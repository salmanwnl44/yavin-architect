# Progress

## Status

| Milestone | State |
| --- | --- |
| M0 scaffold | done |
| M1 ledger + Arbiter | built; fixture added; **exit tests 1 and 7 have not run against it yet (no PostgreSQL available)** |

M1 is not "done" by its own definition yet. The Phase 0 fixture (`fixture_ledger.jsonl`,
`replay.py`, `build_fixture.py`) is now in `phase0-contracts/fixture/`, checksums verified, and
`replay.py` prints `REPLAY GREEN` on it. Exit tests 1 and 7 need a database, and none was
reachable when the fixture landed, so they have not run against it. Exit tests 2 to 6 were
green before the fixture landed and have not been re-run since. See "Fixture audit" and "Open".

## M0: scaffold

- Python 3.11+ project, src layout, package `architect`, `architect` console entrypoint.
- `docker-compose.yml`: `postgres:16`, plus a Temporal dev server behind the `sessions`
  profile (unused until the sessions milestone). Not exercised locally: the development
  machine has no Docker, so M1 was verified against a native PostgreSQL 16.10.
- pytest + ruff. `phase0-contracts/` is excluded from ruff because it is frozen.
- GitHub Actions CI (`.github/workflows/ci.yml`) with a `postgres:16` service runs, in order:
  `python3 phase0-contracts/validate.py`, `python3 phase0-contracts/fixture/replay.py`,
  `ruff check .`, `pytest`. It has run once, before the fixture was added, and stopped at
  the `replay.py` step; see "Open".
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
| 1 | Fixture round-trip replays green | `test_exit_fixture.py::test_fixture_round_trips_through_the_arbiter_and_replays_green` | **not run against the fixture: no database** |
| 2 | Refusal suite | the nine tests under "Exit test 2" in `test_refusals.py` | green |
| 3 | Idempotency | `test_arbiter.py::test_idempotent_resubmission_returns_the_original_event` | green |
| 4 | Concurrency, 20 in parallel | `test_arbiter.py::test_parallel_submissions_get_dense_seq_and_a_valid_chain` | green |
| 5 | Append-only trigger | `test_arbiter.py::test_raw_sql_cannot_change_or_remove_a_committed_event` | green |
| 6 | Atomicity | `test_arbiter.py::test_a_failure_before_commit_rolls_back_event_and_state` | green |
| 7 | Rebuild after fixture ingest, zero diff | `test_exit_fixture.py::test_rebuild_state_after_the_fixture_ingest_reports_zero_diff` | **not run against the fixture: no database** |

Tests 1 and 7 have stand-ins that run the same paths on a hand-written 24-event ledger
covering all 19 event types (`tests/builders.py::sample_ledger`):
`test_cli.py::test_ingest_then_dump_round_trips_the_ledger` and
`test_cli.py::test_rebuild_state_reports_zero_diff_on_a_healthy_ledger`. They show the
plumbing works; they do not show agreement with `replay.py`, which nobody has run against
this code.

Results 2 to 6 in the table are from the run below, which predates the fixture.

Output, 2026-10-02, before the fixture was added, Windows 11, Python 3.11.15, PostgreSQL 16.10:

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

## Fixture audit (2026-10-02, after the fixture was added)

Windows 11, Python 3.11.0. No PostgreSQL was reachable, so nothing below touches a database.

```
$ Get-FileHash phase0-contracts\fixture\* -Algorithm SHA256      (all three match the owner's checksums)
e62ec17232863134b46a50dde6e03df64f0d3f67923d07c480d4f591f8edaf4b  fixture_ledger.jsonl   (40 lines, LF)
dbe60b992f59eafbef772557243b2fcbc521e470963eaf812bab2727b9ef080a  replay.py
ac312708dfcdc5cea26016fc259ee16da87c71a17639dd644c3e866d7a3234fc  build_fixture.py

$ py phase0-contracts/validate.py
META OK   phase0-contracts\agent_protocol.schema.json
META OK   phase0-contracts\check_catalog.schema.json
META OK   phase0-contracts\claim.schema.json
META OK   phase0-contracts\ledger_events.schema.json
META OK   phase0-contracts\system_model.schema.json
KeyError: 'phase0-contracts/claim.schema.json'        (contracts-PROPOSALS.md P-1; exit 1)

$ py phase0-contracts/validate.py   (via the POSIX glob shim, P-1)
RESULT: ALL GREEN

$ py phase0-contracts/fixture/replay.py
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

$ git add --renormalize .          (nothing staged: every tracked file is already LF)

$ pytest
8 passed, 93 errors in 268.09s (0:04:28)      (every error is psycopg ConnectionTimeout: no database)

$ pytest tests/test_contracts.py tests/test_architecture.py
7 passed in 0.49s

$ ruff check .
All checks passed!
```

An offline run, not an exit test: the fixture was put through the Arbiter's schema gate, `ts`
check and `rules.py` with an in-memory object standing in for the `arb_*` tables, stamped with
`seq` and `prev_hash` as `arbiter.py` does, and written the way `architect dump` writes. All 40
events were accepted, a fold-only rebuild gave the same state, and `replay.py` printed
`REPLAY GREEN` on that dump. So the rules, the `project_id` rewrite and the added `prev_hash`
agree with `replay.py` on this ledger. The SQL, the transaction and the jsonb round trip were
not exercised.

## Open

1. **Run the suite against PostgreSQL with the fixture present.** `pytest` needs a PostgreSQL
   16 at `ARCHITECT_DATABASE_URL`. Exit tests 1 and 7 have never run against the fixture, and
   2 to 6 have not been re-run since it landed. Where the Arbiter and `replay.py` disagree,
   `replay.py` wins and the rule in `rules.py` changes.
2. **CI has run once and failed**, on the commit before the fixture: `validate.py` passed on
   Linux, `replay.py` failed (fixture missing), and `ruff` and `pytest` were skipped. Pushing
   the fixture commit runs the whole suite on CI's `postgres:16`.
3. **contracts-PROPOSALS.md** has five entries for the next contract version.
