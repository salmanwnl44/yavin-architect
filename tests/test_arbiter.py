"""The commit path: stamping, idempotency, concurrency, append-only, atomicity."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import psycopg
import pytest

from architect import ledger
from architect.arbiter import Arbiter
from architect.contracts import load_contracts
from architect.db import open_pool
from architect.errors import Rejection
from architect.rules import RULES
from builders import as_candidate, candidate, ident, sample_ledger, source
from conftest import PROJECT


def test_every_event_type_has_a_rule():
    assert set(RULES) == set(load_contracts().event_types)


def test_first_commit_is_seq_zero_without_prev_hash(arbiter):
    commit = arbiter.submit(PROJECT, source())
    assert commit.replayed is False
    assert commit.event["seq"] == 0
    assert "prev_hash" not in commit.event
    assert commit.event["project_id"] == PROJECT


def test_arbiter_mints_event_id_and_ts_when_absent(arbiter):
    event = arbiter.submit(PROJECT, source()).event
    assert event["event_id"].startswith("evt_") and len(event["event_id"]) == 30
    assert event["ts"].endswith("+00:00")
    assert load_contracts().event_error(event) is None


def test_client_event_id_and_ts_are_kept_verbatim(arbiter):
    sent = source() | {"event_id": ident("evt", "mine"), "ts": "2026-10-02T06:41:00.5+05:30"}
    event = arbiter.submit(PROJECT, sent).event
    assert event["event_id"] == sent["event_id"]
    assert event["ts"] == "2026-10-02T06:41:00.5+05:30"


def test_seq_is_dense_and_each_event_hashes_its_predecessor(arbiter, pool):
    events = [arbiter.submit(PROJECT, source(f"s{i}")).event for i in range(5)]
    assert [e["seq"] for e in events] == [0, 1, 2, 3, 4]
    for previous, event in zip(events, events[1:], strict=False):
        assert event["prev_hash"] == ledger.event_hash(previous)
    assert ledger.verify_chain(pool, PROJECT) == (5, [])


def test_projects_are_sequenced_independently(arbiter, pool):
    ledger.create_project(pool, "p2")
    arbiter.submit(PROJECT, source("a"))
    arbiter.submit(PROJECT, source("b"))
    other = arbiter.submit("p2", source("a")).event
    assert other["seq"] == 0 and "prev_hash" not in other


def test_committed_events_read_back_identically(arbiter, pool):
    for event in sample_ledger(PROJECT):
        arbiter.submit(PROJECT, as_candidate(event))
    stored = list(ledger.iter_events(pool, PROJECT))
    assert [{k: v for k, v in e.items() if k != "prev_hash"} for e in stored] == sample_ledger(
        PROJECT
    )
    assert ledger.verify_chain(pool, PROJECT) == (24, [])


# Exit test 3
def test_idempotent_resubmission_returns_the_original_event(arbiter, fingerprint):
    sent = source()
    first = arbiter.submit(PROJECT, sent)
    before = fingerprint()
    second = arbiter.submit(PROJECT, sent)

    assert first.replayed is False and second.replayed is True
    assert second.event == first.event
    assert (second.event["event_id"], second.event["seq"]) == (
        first.event["event_id"],
        first.event["seq"],
    )
    assert fingerprint() == before
    assert len(before["events"]) == 1


def test_a_retry_wins_over_rules_that_its_first_commit_changed(arbiter):
    """The retry of a source.ingested would be a DUPLICATE_SOURCE if it were re-validated."""
    sent = source()
    arbiter.submit(PROJECT, sent)
    arbiter.submit(PROJECT, source("other"))
    assert arbiter.submit(PROJECT, sent).replayed is True


# Exit test 4
def test_parallel_submissions_get_dense_seq_and_a_valid_chain(dsn, arbiter):
    n = 20
    candidates = [source(f"par{i}") for i in range(n)]
    wide = open_pool(dsn, min_size=n, max_size=n)  # one connection per submitter
    try:
        parallel = Arbiter(wide)
        with ThreadPoolExecutor(max_workers=n) as workers:
            commits = list(workers.map(lambda c: parallel.submit(PROJECT, c), candidates))
        assert sorted(c.event["seq"] for c in commits) == list(range(n))
        assert len({c.event["event_id"] for c in commits}) == n
        assert [e["seq"] for e in ledger.iter_events(wide, PROJECT)] == list(range(n))
        assert ledger.verify_chain(wide, PROJECT) == (n, [])
    finally:
        wide.close()


def test_parallel_retries_of_one_candidate_commit_once(dsn, arbiter, fingerprint):
    n = 8
    sent = source()
    wide = open_pool(dsn, min_size=n, max_size=n)
    try:
        parallel = Arbiter(wide)
        with ThreadPoolExecutor(max_workers=n) as workers:
            commits = list(workers.map(lambda _: parallel.submit(PROJECT, sent), range(n)))
    finally:
        wide.close()
    assert sum(not c.replayed for c in commits) == 1
    assert len({c.event["event_id"] for c in commits}) == 1
    assert len(fingerprint()["events"]) == 1


# Exit test 5
@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE events SET payload = '{}'::jsonb",
        "UPDATE events SET seq = seq + 100",
        "DELETE FROM events",
        "TRUNCATE events",
    ],
)
def test_raw_sql_cannot_change_or_remove_a_committed_event(arbiter, dsn, fingerprint, statement):
    arbiter.submit(PROJECT, source())
    before = fingerprint()
    with psycopg.connect(dsn) as conn:
        with pytest.raises(psycopg.errors.IntegrityConstraintViolation, match="append-only"):
            conn.execute(statement)
    assert fingerprint() == before


# Exit test 6
def test_a_failure_before_commit_rolls_back_event_and_state(arbiter, fingerprint):
    arbiter.submit(PROJECT, source("first"))
    before = fingerprint()

    def explode() -> None:
        raise RuntimeError("forced failure between the state write and the commit")

    arbiter.before_commit = explode
    with pytest.raises(RuntimeError, match="forced failure"):
        arbiter.submit(PROJECT, source("second"))
    assert fingerprint() == before

    arbiter.before_commit = None
    retried = arbiter.submit(PROJECT, source("second")).event
    assert retried["seq"] == 1
    assert len(fingerprint()["arb_sources"]) == 2


def test_the_hook_runs_after_both_writes(arbiter, dsn, pool):
    """Guards the atomicity test itself: at the hook, the event and its state both exist."""
    seen = {}

    def peek() -> None:
        # Uncommitted, so only visible from inside; a second connection must see nothing.
        with psycopg.connect(dsn) as other:
            seen["outside"] = other.execute("SELECT count(*) FROM events").fetchone()[0]

    arbiter.before_commit = peek
    arbiter.submit(PROJECT, source())
    assert seen["outside"] == 0
    assert ledger.head(pool, PROJECT)["last_seq"] == 0


@pytest.mark.parametrize("stamped", ["seq", "prev_hash"])
def test_candidates_must_not_carry_arbiter_stamped_fields(arbiter, fingerprint, stamped):
    before = fingerprint()
    with pytest.raises(Rejection) as refused:
        arbiter.submit(PROJECT, source() | {stamped: 0 if stamped == "seq" else "abc"})
    assert refused.value.code == "SCHEMA_INVALID"
    assert refused.value.json_path == f"$.{stamped}"
    assert fingerprint() == before


def test_unknown_project_is_refused(arbiter):
    with pytest.raises(Rejection) as refused:
        arbiter.submit("nope", source())
    assert refused.value.code == "UNKNOWN_PROJECT"
    assert refused.value.http_status == 404


def test_event_id_is_unique_across_projects(arbiter, pool, fingerprint):
    ledger.create_project(pool, "p2")
    arbiter.submit(PROJECT, source() | {"event_id": ident("evt", "shared")})
    before = fingerprint()
    with pytest.raises(Rejection) as refused:
        arbiter.submit(
            "p2",
            candidate(
                "entity.merged",
                {
                    "kept_id": "a",
                    "merged_ids": ["b"],
                    "method": "human",
                },
            )
            | {"event_id": ident("evt", "shared")},
        )
    assert refused.value.code == "DUPLICATE_EVENT_ID"
    assert refused.value.http_status == 409
    assert fingerprint() == before


def test_unstorable_json_is_a_typed_rejection(arbiter, fingerprint):
    before = fingerprint()
    bad = source()
    bad["payload"]["uri"] = "nul\x00byte"
    with pytest.raises(Rejection) as refused:
        arbiter.submit(PROJECT, bad)
    assert refused.value.code == "SCHEMA_INVALID"
    assert fingerprint() == before
