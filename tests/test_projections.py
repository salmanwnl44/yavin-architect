"""The read models (M2): the projector, the proj_* tables and the GET side of the API.

The nine M2 exit tests are marked as such. Most run on the frozen Phase 0 fixture, committed
through the Arbiter into the project the fixture names; refutation runs on a small synthetic
ledger, because the fixture refutes nothing.
"""

from __future__ import annotations

import contextlib
import io
import json
import re
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import psycopg
import pytest

import architect
from architect import ledger, projector, readmodel
from architect.arbiter import Arbiter
from architect.cli import main
from architect.contracts import contracts_dir, first_error, load_contracts
from architect.db import EVENTS_CHANNEL
from architect.projections import (
    HANDLERS,
    NOT_PROJECTED,
    PROJ_TABLES,
    PROJECTION,
    ProjectionError,
    apply_patch,
    empty_model,
)
from architect.projector import Projector
from builders import as_candidate, candidate, claim, claim_committed, ident, sample_ledger, source
from conftest import PROJECT

FIXTURE_DIR = contracts_dir() / "fixture"
REPLAY = FIXTURE_DIR / "replay.py"
FIX = "proj-architect-dogfood"  # the project the fixture ledger names
FIX_VERSIONS = ["mv_FIXGENESIS", "mv_FIXV000001", "mv_FIXV000002", "mv_FIXV000003"]
LAST_SEQ = 39


def fixture_events() -> list[dict[str, Any]]:
    text = (FIXTURE_DIR / "fixture_ledger.jsonl").read_text(encoding="utf-8")
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def reference() -> dict[str, Any]:
    """replay.py's folded state, obtained by running its source unmodified."""
    namespace: dict[str, Any] = {"__name__": "__main__", "__file__": str(REPLAY)}
    argv, sys.argv = sys.argv, [str(REPLAY)]
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            try:
                exec(compile(REPLAY.read_text(encoding="utf-8"), str(REPLAY), "exec"), namespace)
            except SystemExit as stop:
                assert not stop.code, "replay.py did not replay the fixture green"
    finally:
        sys.argv = argv
    return namespace


def normalized(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def commit_fixture(pool) -> None:
    ledger.create_project(pool, FIX)
    arbiter = Arbiter(pool)
    for event in fixture_events():
        arbiter.submit(FIX, as_candidate(event))


def cursor(pool, project_id: str = FIX) -> int | None:
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT last_seq FROM proj_cursors WHERE projection = %s AND project_id = %s",
            (PROJECTION, project_id),
        ).fetchone()
    return row["last_seq"] if row else None


def rows(pool, query: str, *params: Any) -> list[dict[str, Any]]:
    with pool.connection() as conn:
        return conn.execute(query, params).fetchall()


def row_counts(pool) -> dict[str, int]:
    return {
        table: rows(pool, f"SELECT count(*) AS n FROM {table} WHERE project_id = %s", FIX)[0]["n"]
        for table in PROJ_TABLES
    }


@pytest.fixture
def worker(pool) -> Projector:
    return Projector(pool)


@pytest.fixture
def projected(pool, worker) -> str:
    """The fixture, committed and fully projected. Returns the content hash."""
    commit_fixture(pool)
    assert worker.catch_up() == LAST_SEQ + 1
    return projector.content_hash(pool, FIX)


@pytest.fixture
def cli(dsn, capsys):
    def run(*argv: str) -> tuple[int, str, str]:
        code = main(["--database-url", dsn, *argv])
        captured = capsys.readouterr()
        return code, captured.out, captured.err

    return run


def get(client, path: str, project: str = FIX, **params: Any) -> Any:
    response = client.get(f"/v1/projects/{project}/{path}", params=params)
    assert response.status_code == 200, response.text
    return response.json()


# --- The fold, without a database ---------------------------------------------------------


def test_every_event_type_is_projected_or_explicitly_not():
    assert set(HANDLERS) | NOT_PROJECTED == set(load_contracts().event_types)
    assert not set(HANDLERS) & NOT_PROJECTED


def test_apply_patch_folds_the_fixture_into_replays_final_model():
    model = None
    for event in fixture_events():
        payload = event["payload"]
        if event["type"] == "model.version_created":
            model = empty_model(FIX, payload["version_id"])
        elif event["type"] == "model.patch_committed":
            model = apply_patch(model, payload["patch"], payload["version_id"])
    assert normalized(model) == normalized(reference()["final_model"])


def test_apply_patch_leaves_its_base_untouched_and_refuses_a_missing_target():
    base = empty_model("p", "mv_0000000001")
    patch = {"ops": [{"op": "add_link", "link_type": "satisfies", "link": {"a": 1}}]}
    assert apply_patch(base, patch, "mv_0000000002")["links"] == {"satisfies": [{"a": 1}]}
    assert base == empty_model("p", "mv_0000000001")

    update = {"op": "update_element", "element_type": "flows", "element_id": "x", "element": {}}
    with pytest.raises(ProjectionError, match="target x not found"):
        apply_patch(base, {"ops": [update]}, "mv_0000000002")
    with pytest.raises(ProjectionError, match="element_type"):
        apply_patch(base, {"ops": [{"op": "add_element", "element": {}}]}, "mv_0000000002")


def test_projections_never_write_events_or_reach_the_arbiter():
    src = Path(architect.__file__).parent
    for name in ("projections.py", "projector.py", "readmodel.py"):
        text = (src / name).read_text(encoding="utf-8")
        imports = r"^\s*(from|import)\s+architect\.(arbiter|rules|state)\b"
        assert not re.search(imports, text, re.MULTILINE), name
        assert not re.search(r"\b(INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+(events|arb_)", text), name
    queries = (src / "readmodel.py").read_text(encoding="utf-8")
    assert not re.search(r"\b(FROM|JOIN)\s+(events|arb_\w+)\b", queries)


# --- Exit test 1: reference equivalence ----------------------------------------------------


def test_fixture_projection_equals_what_replay_folds(pool, projected):
    ref = reference()

    head = readmodel.head_model(pool, FIX)
    assert head["version_id"] == "mv_FIXV000003"
    assert normalized(head["model"]) == normalized(ref["final_model"])

    statuses = {row["claim_id"]: row["status"] for row in readmodel.list_claims(pool, FIX)}
    assert statuses == ref["status"]

    counts = {
        row["edge_type"]: row["n"]
        for row in rows(
            pool,
            "SELECT edge_type, count(*) AS n FROM proj_edges WHERE project_id = %s "
            "AND (version_id IS NULL OR version_id = %s) GROUP BY edge_type",
            FIX,
            head["version_id"],
        )
    }
    assert {name: counts.get(name, 0) for name in ref["edges"]} == ref["edges"]


# --- Exit test 2: every version is materialized --------------------------------------------


def test_every_fixture_version_is_materialized_and_valid(client, projected):
    validator = load_contracts().model
    models = {}
    for version_id in FIX_VERSIONS:
        version = get(client, f"models/{version_id}")
        assert version["version_id"] == version["model"]["version_id"] == version_id
        assert first_error(validator, version["model"]) is None
        models[version_id] = version

    assert [models[v]["parent_version"] for v in FIX_VERSIONS] == [None, *FIX_VERSIONS[:-1]]
    assert [models[v]["committed_at_seq"] for v in FIX_VERSIONS] == [14, 16, 19, 26]
    assert models["mv_FIXGENESIS"]["model"]["elements"] == {}

    def ingress(version_id: str) -> dict[str, Any]:
        flows = models[version_id]["model"]["elements"]["flows"]
        return next(flow for flow in flows if flow["id"] == "flw_FIXINGR001")

    assert "input_validation" not in ingress("mv_FIXV000002")
    assert "encryption_in_transit" not in ingress("mv_FIXV000002")
    assert ingress("mv_FIXV000003")["input_validation"]
    assert ingress("mv_FIXV000003")["encryption_in_transit"] is True

    assert get(client, "models/head") == models["mv_FIXV000003"]


def test_model_edges_are_kept_per_version(pool, projected):
    per_version = {
        (row["version_id"], row["edge_type"]): row["n"]
        for row in rows(
            pool,
            "SELECT version_id, edge_type, count(*) AS n FROM proj_edges "
            "WHERE project_id = %s AND version_id IS NOT NULL GROUP BY version_id, edge_type",
            FIX,
        )
    }
    assert per_version == {
        ("mv_FIXV000001", "SATISFIES"): 4,
        ("mv_FIXV000001", "DEPENDS_ON"): 2,
        ("mv_FIXV000002", "SATISFIES"): 5,
        ("mv_FIXV000002", "DEPENDS_ON"): 3,
        ("mv_FIXV000003", "SATISFIES"): 5,
        ("mv_FIXV000003", "DEPENDS_ON"): 3,
    }
    assert rows(
        pool,
        "SELECT src, dst FROM proj_edges WHERE project_id = %s AND edge_type = 'SUPERSEDES'",
        FIX,
    ) == [{"src": "clm_FIXMEAS001", "dst": "clm_FIXPAYLOAD1"}]


# --- Exit test 3: incremental == rebuild ---------------------------------------------------


def test_projecting_event_by_event_equals_a_rebuild(pool, worker, cli):
    ledger.create_project(pool, FIX)
    arbiter = Arbiter(pool)
    empty = projector.content_hash(pool, FIX)
    for event in fixture_events():
        arbiter.submit(FIX, as_candidate(event))
        assert worker.catch_up() == 1
        assert cursor(pool) == event["seq"]
    live = projector.content_hash(pool, FIX)
    assert live != empty

    code, out, _ = cli("rebuild-projections", "--project", FIX)
    assert code == 0
    assert "from 40 events" in out
    assert f"content hash: {live}" in out
    assert projector.content_hash(pool, FIX) == live


def test_batch_size_does_not_change_the_result(pool, projected):
    for batch_size in (1, 7, 1000):
        assert Projector(pool, batch_size=batch_size).rebuild(FIX) == LAST_SEQ + 1
        assert projector.content_hash(pool, FIX) == projected


# --- Exit test 4: crash safety --------------------------------------------------------------


def test_a_crash_mid_rebuild_resumes_to_the_same_hash(pool, projected):
    """The hook raises on the 17th event: batches 0-6 and 7-13 are committed, the third is not."""
    expected_rows = row_counts(pool)
    crashing = Projector(pool, batch_size=7)
    folded = []

    def crash(event: dict[str, Any]) -> None:
        folded.append(event["seq"])
        if len(folded) == 17:
            raise RuntimeError("crash in the middle of a batch")

    crashing.after_event = crash
    with pytest.raises(RuntimeError, match="crash in the middle"):
        crashing.rebuild(FIX)

    # Cursor and contents agree: nothing from the rolled-back batch is visible.
    assert cursor(pool) == 13
    for table, column in (("proj_edges", "seq"), ("proj_session_timeline", "seq")):
        newest = rows(pool, f"SELECT max({column}) AS s FROM {table} WHERE project_id = %s", FIX)
        assert newest[0]["s"] <= 13
    assert projector.content_hash(pool, FIX) != projected

    seen = [cursor(pool)]
    resumed = Projector(pool, batch_size=7)
    resumed.after_event = lambda event: seen.append(cursor(pool))
    assert resumed.catch_up() == LAST_SEQ - 13
    seen.append(cursor(pool))

    assert seen == sorted(seen) and seen[-1] == LAST_SEQ, "the cursor moved backwards"
    assert projector.content_hash(pool, FIX) == projected
    assert row_counts(pool) == expected_rows


KILLED_WORKER = """
import sys, time
from architect.db import open_pool
from architect.projector import Projector

dsn, project = sys.argv[1:3]
worker = Projector(open_pool(dsn, max_size=2), batch_size=7)
folded = 0

def hang(event):
    global folded
    folded += 1
    if folded == 17:
        print("folded 17 events, holding the third batch open", flush=True)
        time.sleep(600)

worker.after_event = hang
worker.rebuild(project)
"""


def test_killing_the_projector_process_mid_rebuild_loses_nothing(dsn, pool, projected, cli):
    expected_rows = row_counts(pool)
    process = subprocess.Popen(
        [sys.executable, "-c", KILLED_WORKER, dsn, FIX],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        line = process.stdout.readline()  # the worker now holds an uncommitted batch
        assert "holding the third batch open" in line, process.stderr.read()
    finally:
        process.kill()
        process.wait()
        process.stdout.close()
        process.stderr.close()

    # Postgres rolls the dead session's batch back; the restart waits for that on the cursor.
    code, out, _ = cli("project", "--once", "--batch-size", "7")
    assert code == 0
    assert f"projected {LAST_SEQ - 13} events" in out
    assert cursor(pool) == LAST_SEQ
    assert projector.content_hash(pool, FIX) == projected
    assert row_counts(pool) == expected_rows


# --- Exit test 5: disposable ----------------------------------------------------------------


def test_dropping_every_proj_table_loses_nothing(dsn, pool, projected, cli):
    with psycopg.connect(dsn) as conn:
        for table in (*PROJ_TABLES, "proj_cursors"):
            conn.execute(f"DROP TABLE {table}")

    code, out, _ = cli("rebuild-projections", "--project", FIX)  # recreates the tables first
    assert code == 0
    assert f"content hash: {projected}" in out
    assert cursor(pool) == LAST_SEQ


# --- Exit test 6: time travel ---------------------------------------------------------------


def test_a_claim_reads_as_it_stood_at_an_earlier_seq(client, projected):
    def status_at(seq: int | None) -> dict[str, str]:
        params = {} if seq is None else {"as_of_seq": seq}
        return {c["claim_id"]: c["status"] for c in get(client, "claims", **params)["claims"]}

    assert status_at(31)["clm_FIXPAYLOAD1"] == "assumed"
    assert status_at(32)["clm_FIXPAYLOAD1"] == "measured"
    assert status_at(None)["clm_FIXPAYLOAD1"] == "measured"

    assert status_at(2) == {}
    assert list(status_at(4)) == ["clm_FIXREQ0001", "clm_FIXDUR0001"]
    assert "clm_FIXMEAS001" not in status_at(29) and "clm_FIXMEAS001" in status_at(30)

    def ids(**params: Any) -> list[str]:
        return [c["claim_id"] for c in get(client, "claims", **params)["claims"]]

    assert ids(status="assumed", as_of_seq=31) == ["clm_FIXPAYLOAD1"]
    assert ids(status="assumed") == []
    assert ids(status="measured") == ["clm_FIXPAYLOAD1", "clm_FIXMEAS001"]
    assert ids(load_bearing=True) == ["clm_FIXPAYLOAD1"]
    assert len(ids(load_bearing=False)) == 5


def test_claim_detail_has_history_and_provenance(client, projected):
    payload = get(client, "claims/clm_FIXPAYLOAD1")
    assert payload["status"] == "measured" and payload["claim"]["status"] == "assumed"
    assert (payload["first_seq"], payload["last_seq"]) == (12, 32)
    assert payload["status_history"] == [
        {"status": "assumed", "from_seq": 12, "to_seq": 32, "cause_event": "evt_FIXE000012"},
        {"status": "measured", "from_seq": 32, "to_seq": None, "cause_event": "evt_FIXE000031"},
    ]

    lease = get(client, "claims/clm_FIXLEASE01")
    (evidence,) = lease["provenance"]["evidence"]
    assert evidence["source"] == "src_FIXPAPER01" and evidence["span"] == "p.3 §2"
    assert evidence["source_record"]["uri"] and evidence["source_record"]["seq"] == 6
    assert lease["provenance"]["derived_from_chain"] == []
    assert lease["taint_origin"] == "external_untrusted"


# --- Exit test 7: the why-trace -------------------------------------------------------------


def test_why_traces_an_element_to_requirements_decisions_claims_and_sources(client, projected):
    trace = get(client, "elements/cmp_FIXSHARD01/why")
    assert trace["element_type"] == "components" and trace["element"]["name"] == "Shard Store"
    assert trace["model_version"] == "mv_FIXV000003"

    requirements = {r["requirement"]: r["claims"] for r in trace["requirements"]}
    assert list(requirements) == ["req_FIXQPS001", "req_FIXDUR001"]
    assert [c["claim_id"] for c in requirements["req_FIXQPS001"]] == ["clm_FIXREQ0001"]
    assert [c["claim_id"] for c in requirements["req_FIXDUR001"]] == ["clm_FIXDUR0001"]
    assert requirements["req_FIXQPS001"][0]["sources"][0]["source_id"] == "src_FIXBRIEF01"

    (decision,) = trace["decisions"]
    assert decision["adr_id"] == "adr_FIXLEASE01" and decision["seq"] == 35
    evidence = {c["claim_id"]: c for c in decision["evidence_claims"]}
    assert list(evidence) == ["clm_FIXLEASE01", "clm_FIXFSYNC01", "clm_FIXMEAS001"]
    assert {s["source_id"] for c in evidence.values() for s in c["sources"]} == {
        "src_FIXPAPER01",
        "src_FIXSPEC001",
        "src_FIXPROBE01",
    }
    assert all(s["uri"] for c in evidence.values() for s in c["sources"])
    assert evidence["clm_FIXMEAS001"]["status"] == "measured"
    assert [c["claim_id"] for c in decision["assumptions"]] == ["clm_FIXPAYLOAD1"]

    # An element no decision names still traces to its requirements.
    router = get(client, "elements/cmp_FIXROUTER1/why")
    assert [r["requirement"] for r in router["requirements"]] == ["req_FIXQPS001"]
    assert router["decisions"] == []


# --- Exit test 8: refutation propagation ------------------------------------------------------


def inferred(name: str, *premises: str) -> dict[str, Any]:
    provenance = claim()["provenance"] | {"derived_from": [ident("clm", p) for p in premises]}
    return claim(name, status="inferred", provenance=provenance)


def test_refuting_a_premise_compromises_everything_derived_from_it(client, pool, worker):
    arbiter = Arbiter(pool)

    def commit(event: dict[str, Any]) -> dict[str, Any]:
        return arbiter.submit(PROJECT, event).event

    def compromised(**params: Any) -> set[str]:
        worker.catch_up()
        listed = get(client, "claims", project=PROJECT, **params)["claims"]
        return {c["claim_id"] for c in listed if c["premise_compromised"]}

    a, b, c, d, e = (ident("clm", name) for name in "abcde")
    commit(source())
    commit(claim_committed(claim("a")))
    commit(claim_committed(inferred("b", "a")))
    commit(claim_committed(inferred("c", "b")))
    anchor = commit(claim_committed(claim("d")))
    commit(claim_committed(inferred("e", "d")))
    assert compromised() == set()

    def status_change(claim_id: str, frm: str, to: str) -> dict[str, Any]:
        payload = {"claim_id": claim_id, "from": frm, "to": to, "cause_event": anchor["event_id"]}
        return commit(candidate("claim.status_changed", payload))

    refuted = status_change(a, "documented", "refuted")
    assert compromised() == {b, c}, "transitive dependents, and nothing unrelated"
    assert compromised(as_of_seq=refuted["seq"] - 1) == set()
    assert compromised(as_of_seq=refuted["seq"]) == {b, c}

    chain = get(client, f"claims/{c}", project=PROJECT)["provenance"]["derived_from_chain"]
    assert [(link["claim_id"], link["depth"], link["status"]) for link in chain] == [
        (b, 1, "inferred"),
        (a, 2, "refuted"),
    ]

    commit(candidate("claim.retracted", {"claim_id": d, "cause": "source withdrawn"}))
    assert compromised() == {b, c, e}

    # A claim derived from an already compromised chain is compromised from the start.
    commit(claim_committed(inferred("f", "c")))
    assert compromised() == {b, c, e, ident("clm", "f")}

    status_change(a, "refuted", "documented")
    assert compromised() == {e}

    # None of this is in the ledger: only the events submitted above are.
    assert ledger.head(pool, PROJECT)["last_seq"] == 9


# --- Exit test 9: lag -------------------------------------------------------------------------


def test_projection_status_reports_the_lag(client, pool, worker):
    def status() -> dict[str, Any]:
        return get(client, "projections/status")

    ledger.create_project(pool, FIX)
    assert status() == {
        "ledger_last_seq": None,
        "projections": [{"projection": PROJECTION, "last_seq": None, "lag": 0}],
    }

    commit_fixture(pool)
    assert status() == {
        "ledger_last_seq": LAST_SEQ,
        "projections": [{"projection": PROJECTION, "last_seq": None, "lag": LAST_SEQ + 1}],
    }

    Projector(pool, batch_size=25).catch_up()
    assert status() == {
        "ledger_last_seq": LAST_SEQ,
        "projections": [{"projection": PROJECTION, "last_seq": LAST_SEQ, "lag": 0}],
    }
    assert worker.catch_up() == 0


# --- The worker: wake-up ----------------------------------------------------------------------


def wait_for(condition, seconds: float = 15.0) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.05)
    return False


@contextlib.contextmanager
def running(worker: Projector, pool, poll_seconds: float):
    stop = threading.Event()
    failures: list[BaseException] = []

    def run() -> None:
        try:
            worker.run(stop, poll_seconds=poll_seconds)
        except BaseException as error:
            failures.append(error)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        with pool.connection() as conn:  # wake it so it sees the stop flag
            conn.execute("SELECT pg_notify(%s, '')", (EVENTS_CHANNEL,))
        thread.join(timeout=poll_seconds + 15)
    assert not thread.is_alive() and not failures, failures


def test_the_worker_wakes_on_the_arbiters_notification(pool, worker):
    """With a 60 s poll, only the NOTIFY can explain a projection within seconds."""
    ledger.create_project(pool, FIX)
    with running(worker, pool, poll_seconds=60):
        time.sleep(0.5)  # let it finish its first pass and start listening
        commit_fixture(pool)
        assert wait_for(lambda: cursor(pool) == LAST_SEQ), "the notification did not wake it"


def test_the_worker_polls_when_no_notification_arrives(pool, worker, monkeypatch):
    """Listening on a channel nobody notifies stands in for a missed notification."""
    monkeypatch.setattr(projector, "EVENTS_CHANNEL", "architect_events_unheard")
    ledger.create_project(pool, FIX)
    with running(worker, pool, poll_seconds=0.2):
        time.sleep(0.5)
        commit_fixture(pool)
        assert wait_for(lambda: cursor(pool) == LAST_SEQ), "polling did not catch up"


def test_the_arbiter_notifies_with_the_project_id(dsn, arbiter):
    with psycopg.connect(dsn, autocommit=True) as listener:
        listener.execute(f"LISTEN {EVENTS_CHANNEL}")
        arbiter.submit(PROJECT, source())
        heard = [n.payload for n in listener.notifies(timeout=10, stop_after=1)]
    assert heard == [PROJECT]


# --- The other read models --------------------------------------------------------------------


def test_objections_decisions_waivers_checks_and_timeline(pool, projected):
    (objection,) = rows(pool, "SELECT * FROM proj_objections WHERE project_id = %s", FIX)
    assert objection["objection_id"] == "obj_FIXSPLIT01" and objection["severity"] == "critical"
    assert objection["open"] is False
    assert (objection["raised_seq"], objection["resolved_seq"]) == (23, 27)
    assert (objection["resolution"], objection["resolution_ref"]) == ("patched", "mv_FIXV000003")

    (decision,) = rows(pool, "SELECT * FROM proj_decisions WHERE project_id = %s", FIX)
    assert (decision["adr_id"], decision["seq"]) == ("adr_FIXLEASE01", 35)

    (waiver,) = rows(pool, "SELECT * FROM proj_waivers WHERE project_id = %s", FIX)
    assert (waiver["waiver_id"], waiver["signer"]) == ("wvr_FIXSPOF001", "saumya")

    checks = rows(
        pool,
        "SELECT check_id, status, version_id FROM proj_checks WHERE project_id = %s ORDER BY seq",
        FIX,
    )
    assert [tuple(check.values()) for check in checks] == [
        ("C-005", "pass", "mv_FIXV000002"),
        ("C-008", "fail", "mv_FIXV000002"),
        ("C-009", "pass", "mv_FIXV000003"),
    ]

    timeline = rows(
        pool,
        "SELECT kind, from_phase, phase, detail, ts FROM proj_session_timeline "
        "WHERE project_id = %s ORDER BY seq",
        FIX,
    )
    assert [step["kind"] for step in timeline].count("checkpoint") == 2 and len(timeline) == 11
    assert timeline[0] == {
        "kind": "phase_changed",
        "from_phase": None,
        "phase": "frame",
        "detail": None,
        "ts": "2026-10-02T07:00:00+05:30",
    }
    assert timeline[-1]["detail"]["best_version"] == "mv_FIXV000003"


def test_a_version_created_from_a_parent_starts_as_the_parents_model(pool, worker):
    mv1, mv2, mv3, mv4 = (ident("mv", f"v{n}") for n in (1, 2, 3, 4))
    component = {
        "id": ident("cmp", "fencer"),
        "name": "Fencer",
        "kind": "service",
        "stateful": False,
        "requirement_refs": [],
    }
    add = {"op": "add_element", "element_type": "components", "element": component}
    patch = {"base_version": mv1, "rationale": "add the fencer", "ops": [add]}
    patched = {"version_id": mv2, "base_version": mv1, "patch": patch}
    ledger.create_project(pool, PROJECT)
    arbiter = Arbiter(pool)
    for event in (
        candidate("model.version_created", {"version_id": mv1}),
        candidate("model.patch_committed", patched),
        candidate("model.version_created", {"version_id": mv3, "parent": mv2}),
        candidate("model.version_created", {"version_id": mv4, "parent": mv1}),
    ):
        arbiter.submit(PROJECT, event)
    worker.catch_up()

    def model(version_id: str) -> dict[str, Any]:
        return readmodel.model_version(pool, PROJECT, version_id)

    assert model(mv3)["model"]["elements"] == {"components": [component]}
    assert model(mv3)["parent_version"] == mv2
    assert model(mv4)["model"]["elements"] == {} and model(mv4)["model"]["version_id"] == mv4
    assert readmodel.head_model(pool, PROJECT)["version_id"] == mv4


def test_an_event_that_cannot_be_folded_stops_the_projector_in_front_of_it(pool, worker, cli):
    """The M1 sample ledger commits a patch that does not produce a valid system model."""
    ledger.create_project(pool, PROJECT)
    arbiter = Arbiter(pool)
    for event in sample_ledger(PROJECT):
        arbiter.submit(PROJECT, as_candidate(event))

    with pytest.raises(ProjectionError, match="seq 9 .*not a valid system model"):
        worker.catch_up()
    assert cursor(pool, PROJECT) == 8, "everything before the event is projected"
    assert len(readmodel.list_claims(pool, PROJECT)) == 2

    with pytest.raises(ProjectionError):
        worker.catch_up()
    assert cursor(pool, PROJECT) == 8

    code, _, err = cli("project", "--once")
    assert code == 1 and "projection stopped: seq 9" in err


def test_projection_is_per_project(pool, projected, worker):
    ledger.create_project(pool, PROJECT)
    Arbiter(pool).submit(PROJECT, source())
    assert worker.catch_up() == 1
    assert projector.content_hash(pool, FIX) == projected
    assert Projector(pool).rebuild(PROJECT) == 1
    assert projector.content_hash(pool, FIX) == projected and cursor(pool) == LAST_SEQ


# --- The read API's refusals --------------------------------------------------------------------


def test_reads_of_things_that_are_not_projected_are_404(client, pool, worker):
    def refused(path: str, project: str = PROJECT) -> tuple[int, str]:
        response = client.get(f"/v1/projects/{project}/{path}")
        return response.status_code, response.json()["code"]

    assert refused("models/head") == (404, "MODEL_VERSION_NOT_FOUND")
    assert refused(f"models/{ident('mv', 'ghost')}") == (404, "MODEL_VERSION_NOT_FOUND")
    assert refused(f"claims/{ident('clm', 'ghost')}") == (404, "CLAIM_NOT_FOUND")
    assert refused("elements/cmp_0000000001/why") == (404, "ELEMENT_NOT_FOUND")
    for path in ("models/head", "claims", "claims/x", "elements/x/why", "projections/status"):
        assert refused(path, project="nope") == (404, "UNKNOWN_PROJECT")

    commit_fixture(pool)
    worker.catch_up()
    assert refused("elements/cmp_0000000001/why", project=FIX) == (404, "ELEMENT_NOT_FOUND")

    assert get(client, "claims", project=PROJECT) == {"claims": [], "as_of_seq": None}
    bad_status = client.get(f"/v1/projects/{PROJECT}/claims", params={"status": "certain"})
    assert bad_status.status_code == 422
    assert bad_status.json()["code"] == "MALFORMED_REQUEST"
    assert bad_status.json()["json_path"] == "$.status"
    negative = client.get(f"/v1/projects/{PROJECT}/claims", params={"as_of_seq": -1})
    assert negative.status_code == 422 and negative.json()["code"] == "MALFORMED_REQUEST"
