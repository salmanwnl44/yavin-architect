# Progress

## Status

| Milestone | State |
| --- | --- |
| M0 scaffold | done |
| M1 ledger + Arbiter | **done**: all seven exit tests green in CI against the Phase 0 fixture |

M1 is complete. The Phase 0 fixture (`fixture_ledger.jsonl`, `replay.py`, `build_fixture.py`)
is in `phase0-contracts/fixture/` with the owner's checksums verified, and CI ran the whole
suite against it on `postgres:16`: 101 passed, no failures. The Arbiter needed no change to
agree with `replay.py`. See "Exit tests" for the output. M2 has not been started.

## M0: scaffold

- Python 3.11+ project, src layout, package `architect`, `architect` console entrypoint.
- `docker-compose.yml`: `postgres:16`, plus a Temporal dev server behind the `sessions`
  profile (unused until the sessions milestone). Not exercised locally: the development
  machine has no Docker, so M1 was verified against a native PostgreSQL 16.10.
- pytest + ruff. `phase0-contracts/` is excluded from ruff because it is frozen.
- GitHub Actions CI (`.github/workflows/ci.yml`) with a `postgres:16` service runs, in order:
  `python3 phase0-contracts/validate.py`, `python3 phase0-contracts/fixture/replay.py`,
  `ruff check .`, `pytest`. Its first run, before the fixture was added, stopped at the
  `replay.py` step; it has been green since the fixture landed.
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

CI output, 2026-10-02, GitHub Actions run 36959487746 on commit `a048f5a`
(https://github.com/salmanwnl44/yavin-architect/actions/runs/36959487746): ubuntu-latest,
Python 3.11.16, PostgreSQL 16.15 (`postgres:16`). Every step succeeded. CI runs `pytest`
without `-v`, so the log shows one line per file rather than per test; all 101 tests passed,
which includes every test named in the table.

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

$ pytest
============================= test session starts ==============================
platform linux -- Python 3.11.16, pytest-9.1.1, pluggy-1.6.0
rootdir: /home/runner/work/yavin-architect/yavin-architect
configfile: pyproject.toml
testpaths: tests
plugins: anyio-4.15.1
collected 101 items

tests/test_api.py ..........                                             [  9%]
tests/test_arbiter.py ......................                             [ 31%]
tests/test_architecture.py ..                                            [ 33%]
tests/test_cli.py ...............                                        [ 48%]
tests/test_contracts.py .....                                            [ 53%]
tests/test_exit_fixture.py ..                                            [ 55%]
tests/test_refusals.py .............................................     [100%]

============================= 101 passed in 7.94s ==============================
```

Removing the per-project lock makes exit test 4 fail with a `(project_id, seq)` unique
violation, so that test does depend on the lock.

### Fixture integrity

SHA-256, checked on disk and again on the committed blobs; the files are stored byte-for-byte
(`phase0-contracts/** -text` in `.gitattributes`):

```
e62ec17232863134b46a50dde6e03df64f0d3f67923d07c480d4f591f8edaf4b  fixture_ledger.jsonl   (40 lines, LF)
dbe60b992f59eafbef772557243b2fcbc521e470963eaf812bab2727b9ef080a  replay.py
ac312708dfcdc5cea26016fc259ee16da87c71a17639dd644c3e866d7a3234fc  build_fixture.py
```

### Not verified on Windows

The exit tests were green on Linux in CI only. On the Windows development machine no
PostgreSQL was reachable after the fixture landed, so the database-backed tests have not run
there against the fixture (`pytest`: 8 passed, 93 errors, every error a psycopg
`ConnectionTimeout`). `replay.py` is green on Windows; `validate.py` fails there until run
through the shim in contracts-PROPOSALS.md P-1.

## Open

1. **contracts-PROPOSALS.md** has five entries for the next contract version.
2. **Run the suite once on Windows** against a local PostgreSQL 16, to confirm what CI shows.
