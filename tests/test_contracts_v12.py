"""Contracts v1.2 (module C3): findings (P-12), session status (P-11), model branches.

Exit tests X1 to X5. v1.2 is a minor version: optional fields and new event types only, the
fixture ledger byte-identical, every v1.1 document still valid.
"""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from temporalio.testing import WorkflowEnvironment

from architect import ledger, projector, readmodel
from architect.arbiter import Arbiter
from architect.cli import main
from architect.contracts import contracts_dir, first_error, load_contracts
from architect.errors import Rejection
from architect.projector import Projector
from architect.rebuild import rebuild_state
from architect.sessions.service import session_row, start
from architect.sessions.worker import build_worker
from architect.state import STATE_TABLES
from builders import AGENT, HUMAN, SYSTEM, candidate, claim, claim_committed, ident, patch, source
from conftest import PROJECT
from replay_reference import FIXTURE_LEDGER, REPLAY, fixture_events
from session_fixtures import (
    S1_STORY,
    TASK_QUEUE,
    ActivityGate,
    ScriptedArchitect,
    create_project,
    events_of,
    make_activities,
    make_gateway,
    result_of,
    run_to_end,
    session_input,
)
from test_contracts_v11 import FIXTURE_SHA256, every_instance_in_the_repo
from test_m7_gate_seed import TIGHT, run

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")

NEW_EVENT_TYPES = {"session.status_changed", "finding.raised", "finding.resolved"}
OTHER = "p2"
MV1, MV2, MV3, MV4, MV5 = (ident("mv", f"v{n}") for n in (1, 2, 3, 4, 5))


# --- X1: integrity -------------------------------------------------------------------------------


def test_x1_the_fixture_is_byte_identical_and_the_contract_says_v1_2():
    assert hashlib.sha256(FIXTURE_LEDGER.read_bytes()).hexdigest() == FIXTURE_SHA256
    assert FIXTURE_SHA256 == "e62ec17232863134b46a50dde6e03df64f0d3f67923d07c480d4f591f8edaf4b"
    readme = (contracts_dir() / "README.md").read_text(encoding="utf-8")
    changelog = (contracts_dir() / "CHANGELOG.md").read_text(encoding="utf-8")
    assert readme.startswith("# Yavin Architect — Phase 0 Contracts (FROZEN v1.2)")
    assert "## v1.2" in changelog and changelog.index("## v1.2") < changelog.index("## v1.1")
    for name, schema in load_contracts().schemas.items():
        assert schema["$id"] == f"https://yavin.dev/contracts/v1/{name}", "the $id keeps /v1/"
    assert set(load_contracts().event_types) >= NEW_EVENT_TYPES
    assert not any(t in NEW_EVENT_TYPES for t in {e["type"] for e in fixture_events()})


def run_script(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-X", "utf8", *args],
        cwd=contracts_dir().parent,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )


def test_x1_validate_and_replay_are_green_and_the_fixture_output_has_no_v1_2_lines():
    validated = run_script("phase0-contracts/validate.py")
    assert validated.returncode == 0 and "RESULT: ALL GREEN" in validated.stdout
    for smoke in (
        "finding.raised (v1.2)",
        "finding.resolved (v1.2)",
        "session.status_changed (v1.2)",
        "model.version_created on a branch (v1.2)",
    ):
        assert f"SMOKE OK event: {smoke}" in validated.stdout
    replayed = run_script(str(REPLAY))
    assert replayed.returncode == 0 and "RESULT: REPLAY GREEN" in replayed.stdout
    # the lines v1.2 can add to the report appear only for a ledger that has what they report
    for line in ("model branches:", "session statuses:", "findings:"):
        assert line not in replayed.stdout
    # (that the fixture output is exactly what PROGRESS.md records is
    # test_contracts_v11.py::test_replay_prints_what_progress_recorded, unchanged)


# --- X2: backward compatibility ------------------------------------------------------------------


def test_x2_every_existing_instance_in_the_repo_validates_under_v1_2():
    contracts = load_contracts()
    instances = every_instance_in_the_repo()
    assert len(instances) > 100
    for where, kind, instance in instances:
        if kind == "event":
            error = contracts.event_error(instance)
        else:
            error = first_error(getattr(contracts, kind), instance)
        assert error is None, f"{where} ({kind}): {error}"


def event(type_: str, payload: dict[str, Any], **extra: Any) -> dict[str, Any]:
    return {
        "event_id": "evt_0000000001",
        "project_id": PROJECT,
        "seq": 0,
        "ts": "2026-10-04T09:00:00Z",
        "actor": SYSTEM,
        "type": type_,
        "payload": payload,
        "idempotency_key": "key-00000001",
        **extra,
    }


def finding(name: str = "one", **overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "finding_id": ident("fnd", name),
        "kind": "contradiction",
        "severity": "major",
        "summary": "Two claims disagree about the split-brain window.",
        "refs": [ident("clm", "latency")],
        "evidence_claims": [ident("clm", "latency")],
        "suggested_action": {"kind": "review", "detail": "read both sources"},
        "detector": {"id": "D1", "version": 1},
        "dedupe_key": f"d1:{name}",
    }
    for key, value in overrides.items():
        if value is None:
            body.pop(key, None)
        else:
            body[key] = value
    return body


def test_x2_the_v1_2_fields_are_optional_and_typed():
    error = load_contracts().event_error
    # v1.1 shapes of the three touched payloads are still valid, without any new field
    assert error(event("model.version_created", {"version_id": MV1})) is None
    assert error(event("session.phase_changed", {"session_id": "s", "to": "frame"})) is None
    checkpoint = {"session_id": "s", "phase": "frame", "open_risk_ids": [], "spend": {"tokens": 0}}
    assert error(event("session.checkpoint", checkpoint)) is None
    # and with them
    assert (
        error(event("model.version_created", {"version_id": MV2, "parent": MV1, "branch": "alt"}))
        is None
    )
    assert (
        error(event("session.phase_changed", {"session_id": "s", "to": "frame", "round": 0}))
        is None
    )
    assert error(event("session.checkpoint", checkpoint | {"package_ref": "sha256:ab"})) is None
    assert error(event("finding.raised", finding())) is None
    assert error(event("finding.raised", finding(evidence_claims=None))) is None, "optional"
    assert (
        error(event("finding.resolved", {"finding_id": "fnd_x", "resolution": "obsolete"})) is None
    )
    status = {
        "session_id": "s",
        "status": "approved",
        "decision": "approve",
        "outcome": "completed",
    }
    assert error(event("session.status_changed", status, actor=HUMAN)) is None
    assert error(event("session.status_changed", {"session_id": "s", "status": "running"})) is None

    for bad in (
        event("model.version_created", {"version_id": MV1, "branch": ""}),
        event("model.version_created", {"version_id": MV1, "branch": 7}),
        event("session.phase_changed", {"session_id": "s", "to": "frame", "round": -1}),
        event("session.checkpoint", checkpoint | {"package_ref": 7}),
        event("finding.raised", finding(refs=[])),
        event("finding.raised", finding(kind="fact")),
        event("finding.raised", finding(severity="blocker")),
        event("finding.raised", finding(finding_id="clm_0000000001")),
        event("finding.raised", finding(detector={"id": "D1"})),
        event("finding.raised", finding(suggested_action={"kind": "pray", "detail": ""})),
        event("finding.raised", finding(dedupe_key=None)),
        event("finding.raised", finding() | {"confidence": 0.9}),
        event("finding.resolved", {"finding_id": "fnd_x", "resolution": "ignored"}),
        event("session.status_changed", {"session_id": "s"}),
        event("session.status_changed", status | {"decision": "shrug"}),
        event("session.status_changed", status | {"gate_verdict": "MAYBE"}),
        event("session.status_changed", status | {"mood": "good"}),
    ):
        assert error(bad) is not None, bad["payload"]


# --- helpers: a small project, and replay.py on any ledger ---------------------------------------


def started(pool) -> Arbiter:
    """A project with a source, two claims, a decision and a two-version model."""
    ledger.create_project(pool, PROJECT)
    arbiter = Arbiter(pool)
    arbiter.submit(PROJECT, source())
    arbiter.submit(PROJECT, claim_committed(claim()))
    arbiter.submit(PROJECT, claim_committed(claim("other")))
    arbiter.submit(PROJECT, candidate("model.version_created", {"version_id": MV1}))
    arbiter.submit(
        PROJECT,
        candidate(
            "model.patch_committed", {"version_id": MV2, "base_version": MV1, "patch": patch(MV1)}
        ),
    )
    arbiter.submit(
        PROJECT,
        candidate(
            "decision.recorded",
            {
                "adr_id": ident("adr", "lease"),
                "decision": {
                    "title": "Leases",
                    "choice": "fencing epochs",
                    "evidence_claims": [ident("clm", "latency")],
                    "alternatives": [],
                    "assumptions": [],
                    "affected_elements": [],
                },
            },
        ),
    )
    return arbiter


def replay(ledger_path: Path) -> tuple[int, str, dict[str, Any]]:
    """Run replay.py's source, unmodified, on a ledger: (exit code, output, its namespace)."""
    namespace: dict[str, Any] = {"__name__": "__main__", "__file__": str(REPLAY)}
    argv, sys.argv = sys.argv, [str(REPLAY), str(ledger_path)]
    code = 0
    try:
        with contextlib.redirect_stdout(io.StringIO()) as out:
            try:
                exec(compile(REPLAY.read_text(encoding="utf-8"), str(REPLAY), "exec"), namespace)
            except SystemExit as stop:
                code = int(stop.code or 0)
    finally:
        sys.argv = argv
    return code, out.getvalue(), namespace


def dump(pool, dsn: str, tmp_path: Path, capsys, name: str = "ledger.jsonl") -> Path:
    path = tmp_path / name
    assert main(["--database-url", dsn, "dump", "--project", PROJECT, "-o", str(path)]) == 0
    capsys.readouterr()
    return path


def refused(arbiter: Arbiter, pool, event_: dict[str, Any], code: str, at: str | None = None):
    """The Arbiter refuses the candidate with the code, and nothing is written."""

    def everything() -> dict[str, list[str]]:
        out: dict[str, list[str]] = {}
        with pool.connection() as conn:
            for table in ("events", *STATE_TABLES):
                rows = conn.execute(f"SELECT to_jsonb(t) AS row FROM {table} t").fetchall()
                out[table] = sorted(json.dumps(r["row"], sort_keys=True) for r in rows)
        return out

    before = everything()
    with pytest.raises(Rejection) as rejection:
        arbiter.submit(PROJECT, event_)
    assert rejection.value.code == code, rejection.value
    if at is not None:
        assert rejection.value.json_path == at
    assert rejection.value.detail and set(rejection.value.body()) <= {"code", "detail", "json_path"}
    assert everything() == before, "a refused event left something behind"
    return rejection.value


def on_branch(type_: str, branch: str | None, **payload: Any) -> dict[str, Any]:
    return candidate(type_, payload | ({"branch": branch} if branch else {}))


def patched(version: str, base: str, branch: str | None = None) -> dict[str, Any]:
    return on_branch(
        "model.patch_committed", branch, version_id=version, base_version=base, patch=patch(base)
    )


# --- X3: branches --------------------------------------------------------------------------------


def test_x3_two_branches_from_one_parent_and_replay_arbiter_and_projector_agree(
    pool, dsn, tmp_path, capsys
):
    arbiter = started(pool)  # main: MV1 -> MV2
    alt3, alt4, main3 = MV3, MV4, MV5
    arbiter.submit(PROJECT, on_branch("model.version_created", "alt", version_id=alt3, parent=MV2))
    arbiter.submit(PROJECT, patched(alt4, alt3, "alt"))  # a patch on alt's head
    arbiter.submit(PROJECT, patched(main3, MV2))  # and one on main's head: MV2 still is
    Projector(pool).catch_up(PROJECT)

    # a patch based on a NON-HEAD version of its branch is refused (the head moved on) ...
    stale = refused(arbiter, pool, patched(ident("mv", "v6"), MV2), "BASE_MOVED")
    assert stale.http_status == 409 and main3 in stale.detail
    refused(arbiter, pool, patched(ident("mv", "v6"), alt3, "alt"), "BASE_MOVED")
    # ... and so is one based on another branch's version, head or not
    crossed = refused(
        arbiter,
        pool,
        patched(ident("mv", "v6"), alt4),
        "BASE_NOT_BRANCH_HEAD",
        "$.payload.base_version",
    )
    assert crossed.http_status == 422 and "'alt'" in crossed.detail
    refused(arbiter, pool, patched(ident("mv", "v6"), main3, "alt"), "BASE_NOT_BRANCH_HEAD")
    refused(arbiter, pool, patched(ident("mv", "v6"), MV2, "nowhere"), "BASE_NOT_BRANCH_HEAD")
    proposal = on_branch(
        "model.patch_proposed", "alt", proposal_id="prp-x", base_version=main3, patch=patch(main3)
    )
    refused(arbiter, pool, proposal, "BASE_NOT_BRANCH_HEAD")
    # a new branch's first version names a committed parent
    refused(
        arbiter,
        pool,
        on_branch("model.version_created", "fresh", version_id=ident("mv", "v7")),
        "BRANCH_NEEDS_PARENT",
        "$.payload.parent",
    )
    refused(
        arbiter,
        pool,
        candidate("model.version_created", {"version_id": ident("mv", "v7")}),
        "DUPLICATE_GENESIS",
    )
    refused(
        arbiter,
        pool,
        on_branch(
            "model.version_created", "fresh", version_id=ident("mv", "v7"), parent="mv_NOSUCH0001"
        ),
        "UNKNOWN_MODEL_VERSION",
    )

    # the three folds agree, per version and per branch
    code, out, namespace = replay(dump(pool, dsn, tmp_path, capsys))
    assert code == 0 and not namespace["failures"], out
    assert namespace["heads"] == {"main": main3, "alt": alt4}
    assert f"model branches:     {{'alt': '{alt4}', 'main': '{main3}'}}" in out
    assert namespace["head"] == main3, "the report is about main"
    with pool.connection() as conn:
        arbiter_heads = {
            r["branch"]: r["head_version"]
            for r in conn.execute("SELECT branch, head_version FROM arb_model_heads").fetchall()
        }
        arbiter_models = {
            r["version_id"]: (r["branch"], r["model"])
            for r in conn.execute(
                "SELECT version_id, branch, model FROM arb_model_versions"
            ).fetchall()
        }
        projected = {
            r["version_id"]: (r["branch"], r["model"])
            for r in conn.execute(
                "SELECT version_id, branch, model FROM proj_model_versions"
            ).fetchall()
        }
    assert arbiter_heads == namespace["heads"]
    assert (
        set(arbiter_models)
        == set(projected)
        == set(namespace["models"])
        == {MV1, MV2, alt3, alt4, main3}
    )
    for version, (branch, model) in arbiter_models.items():
        assert projected[version] == (branch, model), version
        content = {"elements": model["elements"], "links": model["links"]}
        assert namespace["models"][version] == content, version
    assert {v: b for v, (b, _) in projected.items()} == {
        MV1: "main", MV2: "main", main3: "main", alt3: "alt", alt4: "alt",
    }  # fmt: skip
    # the two heads really differ, and each grew from the shared parent
    ids = {
        v: [c["id"] for c in arbiter_models[v][1]["elements"]["components"]]
        for v in arbiter_models
        if v != MV1
    }
    assert ids[alt3] == ids[MV2] and ids[alt4][:1] == ids[MV2] and ids[main3][:1] == ids[MV2]
    assert ids[alt4] != ids[main3] and len(ids[alt4]) == len(ids[main3]) == 2

    # the project's head, everywhere a reader asks for it, is main's
    assert readmodel.head_model(pool, PROJECT)["version_id"] == main3
    assert ledger.head(pool, PROJECT)["model_head_version"] == main3
    # both projections rebuild to the same thing
    hashed = projector.content_hash(pool, PROJECT)
    Projector(pool).rebuild(PROJECT)
    assert projector.content_hash(pool, PROJECT) == hashed
    _, diffs = rebuild_state(pool, PROJECT)
    assert all(diff.empty for diff in diffs)


def test_x3_a_ledger_without_branches_means_what_it_meant(pool, dsn, tmp_path, capsys):
    arbiter = started(pool)
    arbiter.submit(PROJECT, candidate("model.version_created", {"version_id": MV3, "parent": MV1}))
    arbiter.submit(PROJECT, patched(MV4, MV3))
    with pool.connection() as conn:
        heads = conn.execute("SELECT branch, head_version FROM arb_model_heads").fetchall()
    assert heads == [{"branch": "main", "head_version": MV4}], "one head, as under v1.1"
    explicit = patched(MV5, MV4, "main")  # naming main is the same as naming nothing
    arbiter.submit(PROJECT, explicit)
    code, out, namespace = replay(dump(pool, dsn, tmp_path, capsys))
    assert code == 0 and namespace["heads"] == {"main": MV5} and "model branches:" not in out


# --- X4: sessions --------------------------------------------------------------------------------


def session_rows(pool) -> list[str]:
    with pool.connection() as conn:
        rows = conn.execute("SELECT to_jsonb(t) AS row FROM ses_sessions t").fetchall()
    return sorted(json.dumps(r["row"], sort_keys=True) for r in rows)


def test_x4_dropping_ses_sessions_and_rebuilding_from_the_ledger_reproduces_it_exactly(
    pool, tmp_path
):
    """Two sessions: one whose BLOCKED package the owner first tries to approve (refused) and
    then approves with risks, and one that is simply approved (in a project of its own, since
    a project has one model). Their rows are deleted and folded again from the ledger alone."""
    create_project(pool, PROJECT)
    create_project(pool, OTHER)
    activities = make_activities(
        pool, make_gateway(pool, ScriptedArchitect(S1_STORY)), tmp_path / "objects"
    )
    blocked = make_activities(
        pool, make_gateway(pool, ScriptedArchitect(S1_STORY)), tmp_path / "objects"
    )
    gate = ActivityGate(blocked)
    opened = gate.on_finish("record_status", status="awaiting_approval")
    first_id, second_id = "ses_X4APPROVED01", "ses_X4WITHRISKS1"

    async def body() -> tuple[dict[str, Any], dict[str, Any]]:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with build_worker(env.client, task_queue=TASK_QUEUE, activities=blocked):
                try:
                    handle = await start(
                        env.client,
                        session_input(PROJECT, session_id=second_id, overrides=TIGHT),
                        TASK_QUEUE,
                    )
                    await opened.wait()
                    await handle.signal("approve", {"signer": "saumya"})
                    await handle.signal(
                        "approve_with_risks", {"reason": "pilot only", "signer": "saumya"}
                    )
                    second = await result_of(handle)
                finally:
                    gate.release_all()
            first = await run_to_end(
                env.client, activities, session_input(OTHER, session_id=first_id)
            )
            return first, second

    first, second = run(body())
    assert (first["status"], second["status"]) == ("approved", "approved_with_risks")

    one, two = session_row(pool, OTHER, first_id), session_row(pool, PROJECT, second_id)
    assert (one["status"], one["outcome"], one["gate_verdict"]) == (
        "approved",
        "completed",
        "ALLOWED",
    )
    assert one["preset"] == "quick" and one["limits"]["max_rounds"] == 3 and one["round"] == 1
    assert one["phase"] == "package" and one["package_key"] == first["package_key"]
    assert (
        one["best_version"] == first["best_version"] and one["started_at"] == "2026-10-03T09:00:00Z"
    )
    assert one["spend"] == first["spend"] and one["last_refusal"] is None and one["waivers"] == []
    assert (two["status"], two["outcome"], two["gate_verdict"]) == (
        "approved_with_risks",
        "stopped_budget",
        "BLOCKED",
    )
    assert two["last_refusal"]["decision"] == "approve" and "BLOCKED" in two["last_refusal"]["why"]
    assert len(two["waivers"]) == 1 and two["waivers"][0]["target_ref"].startswith("C-007:")
    assert two["limits"]["tokens"] == TIGHT["tokens"] and two["brief_source_id"].startswith("src_")

    # the story of each session is in the ledger
    told = [
        (
            e["payload"]["status"],
            e["payload"].get("decision"),
            e["payload"].get("refused", False),
            e["actor"]["kind"],
        )
        for e in events_of(pool, PROJECT, "session.status_changed")
        if e["payload"]["session_id"] == second_id
    ]
    assert told == [
        ("running", None, False, "system"),
        ("awaiting_approval", None, False, "system"),
        ("awaiting_approval", "approve", True, "human"),
        ("approved_with_risks", "approve_with_risks", False, "human"),
    ]
    final = events_of(pool, PROJECT, "session.status_changed")[-1]
    assert (
        final["actor"] == {"kind": "human", "id": "saumya"}
        and final["payload"]["reason"] == "pilot only"
    )
    assert final["payload"]["package_ref"] == second["package_key"]
    checkpoints = [e["payload"] for e in events_of(pool, PROJECT, "session.checkpoint")]
    assert checkpoints[-1]["package_ref"] == second["package_key"]
    assert all("round" in e["payload"] for e in events_of(pool, PROJECT, "session.phase_changed"))

    # drop the table's rows and rebuild from the ledger: exactly the same rows
    before = session_rows(pool)
    assert len(before) == 2
    hashed = projector.content_hash(pool, PROJECT)
    with pool.connection() as conn:
        conn.execute("DELETE FROM ses_sessions")
    assert session_rows(pool) == []
    Projector(pool).rebuild(PROJECT)
    Projector(pool).rebuild(OTHER)
    assert session_rows(pool) == before
    assert projector.content_hash(pool, PROJECT) == hashed
    assert session_row(pool, PROJECT, second_id) == two
    assert session_row(pool, OTHER, first_id) == one

    # no session module writes the table any more: only the projector's fold does
    src = Path(projector.__file__).parent
    writers = [
        path.relative_to(src).as_posix()
        for path in src.rglob("*.py")
        if any(
            f"{verb} ses_sessions" in path.read_text(encoding="utf-8")
            for verb in ("INSERT INTO", "UPDATE", "DELETE FROM")
        )
    ]
    assert writers == ["projections.py"]


def test_x4_a_gate_decision_must_come_from_a_human(pool):
    arbiter = started(pool)
    for decision in ("approve", "approve_with_risks", "reject"):
        for actor in (SYSTEM, AGENT):
            refused(
                arbiter,
                pool,
                candidate(
                    "session.status_changed",
                    {"session_id": "ses_X", "status": "approved", "decision": decision},
                    actor=actor,
                ),
                "DECISION_NOT_HUMAN",
                "$.actor.kind",
            )
    # refused decisions are held to the same rule; extend and plain status changes are not
    refused(
        arbiter,
        pool,
        candidate(
            "session.status_changed",
            {
                "session_id": "ses_X",
                "status": "awaiting_approval",
                "decision": "approve",
                "refused": True,
            },
        ),
        "DECISION_NOT_HUMAN",
    )
    for payload, actor in (
        (
            {"session_id": "ses_X", "status": "running", "preset": "quick", "limits": {"k": 1}},
            SYSTEM,
        ),
        ({"session_id": "ses_X", "status": "awaiting_approval", "outcome": "completed"}, SYSTEM),
        ({"session_id": "ses_X", "status": "running", "decision": "extend"}, SYSTEM),
        ({"session_id": "ses_X", "status": "approved", "decision": "approve"}, HUMAN),
    ):
        arbiter.submit(PROJECT, candidate("session.status_changed", payload, actor=actor))
    Projector(pool).catch_up(PROJECT)
    row = session_row(pool, PROJECT, "ses_X")
    assert (row["status"], row["outcome"], row["preset"]) == ("approved", None, "quick")


# --- X5: findings, and every refusal -------------------------------------------------------------


def raised(name: str = "one", **overrides: Any) -> dict[str, Any]:
    return candidate(
        "finding.raised",
        finding(name, **overrides),
        actor={"kind": "system", "id": "discovery", "role": "discovery"},
    )


def resolved(name: str, resolution: str = "answered", **extra: Any) -> dict[str, Any]:
    return candidate(
        "finding.resolved",
        {"finding_id": ident("fnd", name), "resolution": resolution, **extra},
        actor=HUMAN,
    )


def test_x5_finding_refs_resolve_to_claims_entities_elements_sources_decisions_and_versions(pool):
    arbiter = started(pool)
    element = patch(MV1)["ops"][0]["element"]["id"]
    resolvable = [
        ident("clm", "latency"),  # a committed claim
        ident("src", "paper"),  # a source
        MV2,  # a model version
        ident("adr", "lease"),  # a decision
        element,  # an element of a committed model version
        "ent:technique:lease-fencing",  # an entity a claim names ...
        "lease-fencing",  # ... by its bare id too
        "ent:metric:split-brain-window",
    ]
    arbiter.submit(PROJECT, raised("refs", refs=resolvable))
    for n, dangling in enumerate(
        ("clm_NOSUCH0001", "ent:technique:nothing", "cmp_NOSUCH0001", "mv_NOSUCH0001", "x")
    ):
        refused(
            arbiter,
            pool,
            raised(f"bad{n}", refs=[ident("clm", "latency"), dangling]),
            "UNKNOWN_REF",
            "$.payload.refs[1]",
        )
    refused(
        arbiter,
        pool,
        raised("evidence", evidence_claims=[ident("clm", "latency"), "clm_NOSUCH0001"]),
        "UNKNOWN_CLAIM",
        "$.payload.evidence_claims[1]",
    )


def test_x5_finding_ids_are_unique_open_dedupe_keys_too_and_only_open_findings_resolve(pool):
    arbiter = started(pool)
    arbiter.submit(PROJECT, raised("one"))
    same_id = refused(arbiter, pool, raised("one", dedupe_key="another"), "DUPLICATE_FINDING_ID")
    assert same_id.http_status == 409 and same_id.json_path == "$.payload.finding_id"
    same_key = refused(arbiter, pool, raised("two", dedupe_key="d1:one"), "DUPLICATE_FINDING")
    assert same_key.http_status == 409 and ident("fnd", "one") in same_key.detail
    refused(arbiter, pool, resolved("never"), "FINDING_NOT_OPEN", "$.payload.finding_id")

    arbiter.submit(PROJECT, resolved("one", "answered", ref=ident("clm", "other")))
    refused(arbiter, pool, resolved("one"), "FINDING_NOT_OPEN")
    refused(arbiter, pool, raised("one"), "DUPLICATE_FINDING_ID")  # an id is used once, ever
    # the same thing found again after it was resolved is a new finding, under a new id
    arbiter.submit(PROJECT, raised("again", dedupe_key="d1:one"))
    refused(arbiter, pool, raised("third", dedupe_key="d1:one"), "DUPLICATE_FINDING")


def test_x5_findings_project_rebuild_and_replay(pool, dsn, tmp_path, capsys):
    arbiter = started(pool)
    arbiter.submit(PROJECT, raised("one"))
    arbiter.submit(PROJECT, raised("two", kind="gap", severity="minor", refs=["lease-fencing"]))
    arbiter.submit(PROJECT, resolved("one", "refuted", ref=ident("clm", "other")))
    Projector(pool).catch_up(PROJECT)
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT finding_id, kind, severity, open, resolution, resolution_ref, refs, "
            "evidence_claims, detector, dedupe_key, raised_seq, resolved_seq "
            "FROM proj_findings ORDER BY raised_seq"
        ).fetchall()
    assert [(r["finding_id"], r["kind"], r["open"], r["resolution"]) for r in rows] == [
        (ident("fnd", "one"), "contradiction", False, "refuted"),
        (ident("fnd", "two"), "gap", True, None),
    ]
    assert (
        rows[0]["resolution_ref"] == ident("clm", "other")
        and rows[0]["resolved_seq"] > rows[0]["raised_seq"]
    )
    assert rows[1]["refs"] == ["lease-fencing"] and rows[1]["detector"] == {
        "id": "D1",
        "version": 1,
    }
    # a finding is never a fact: it adds no claim, no edge, no node
    with pool.connection() as conn:
        claims = conn.execute("SELECT count(*) AS n FROM proj_claims").fetchone()["n"]
        about = conn.execute(
            "SELECT count(*) AS n FROM proj_graph_nodes WHERE node_id LIKE 'fnd_%'"
        ).fetchone()["n"]
    assert (claims, about) == (2, 0)

    hashed = projector.content_hash(pool, PROJECT)
    Projector(pool).rebuild(PROJECT)
    assert projector.content_hash(pool, PROJECT) == hashed
    folded, diffs = rebuild_state(pool, PROJECT)
    assert folded == 9 and all(diff.empty for diff in diffs)
    with pool.connection() as conn:
        open_ = conn.execute("SELECT finding_id FROM arb_findings WHERE open").fetchall()
    assert open_ == [{"finding_id": ident("fnd", "two")}]

    code, out, namespace = replay(dump(pool, dsn, tmp_path, capsys))
    assert code == 0 and not namespace["failures"], out
    assert "findings:           raised=2 resolved=1 open=1" in out
    assert namespace["open_findings"] == {ident("fnd", "two"): "d1:two"}


def test_x5_replay_refuses_what_the_arbiter_refuses(pool, dsn, tmp_path, capsys):
    """The reference holds a ledger to the v1.2 rules too: each ledger below is one the
    Arbiter would never have written, made by editing a good one."""
    arbiter = started(pool)
    arbiter.submit(PROJECT, raised("one"))
    arbiter.submit(
        PROJECT,
        candidate(
            "session.status_changed",
            {"session_id": "ses_X", "status": "approved", "decision": "approve"},
            actor=HUMAN,
        ),
    )
    arbiter.submit(PROJECT, on_branch("model.version_created", "alt", version_id=MV3, parent=MV2))
    arbiter.submit(PROJECT, patched(MV4, MV3, "alt"))
    good = dump(pool, dsn, tmp_path, capsys)
    events = [json.loads(line) for line in good.read_text(encoding="utf-8").splitlines()]
    code, out, namespace = replay(good)
    assert code == 0 and "session statuses:   {'ses_X': 'approved'}" in out

    def broken(name: str, change) -> str:
        edited = json.loads(json.dumps(events))
        change(edited)
        path = tmp_path / f"{name}.jsonl"
        path.write_text("".join(json.dumps(e) + "\n" for e in edited), encoding="utf-8")
        code, out, _ = replay(path)
        assert code == 1 and "REPLAY FAILED" in out, name
        return out

    def of(edited: list[dict[str, Any]], type_: str) -> dict[str, Any]:
        return next(e for e in edited if e["type"] == type_)

    def not_human(edited):
        of(edited, "session.status_changed")["actor"] = SYSTEM

    def dangling(edited):
        of(edited, "finding.raised")["payload"]["refs"] = ["clm_NOSUCH0001"]

    def wrong_branch(edited):
        patches = [e for e in edited if e["type"] == "model.patch_committed"]
        patches[-1]["payload"].pop("branch")  # based on alt's head, now claimed for main

    def resolve_unknown(edited):
        last = edited[-1]
        extra = json.loads(json.dumps(last)) | {
            "event_id": "evt_ZZZZZZZZZZ9",
            "seq": last["seq"] + 1,
            "type": "finding.resolved",
            "payload": {"finding_id": "fnd_NOSUCH0001", "resolution": "obsolete"},
            "idempotency_key": "resolve-unknown",
        }
        edited.append(extra)

    assert "by a non-human actor" in broken("not-human", not_human)
    assert "does not resolve" in broken("dangling", dangling)
    assert "(chain broken)" in broken("wrong-branch", wrong_branch)
    assert "resolving unknown/closed finding" in broken("resolve-unknown", resolve_unknown)


def test_x5_the_api_refuses_with_the_typed_error_and_its_status(client, pool):
    base = f"/v1/projects/{PROJECT}/events"
    assert client.post(base, json=source()).status_code == 201
    assert client.post(base, json=claim_committed(claim())).status_code == 201
    assert client.post(base, json=raised("one")).status_code == 201
    for event_, status, code in (
        (raised("two", refs=["nope"]), 422, "UNKNOWN_REF"),
        (raised("one", dedupe_key="k"), 409, "DUPLICATE_FINDING_ID"),
        (raised("two", dedupe_key="d1:one"), 409, "DUPLICATE_FINDING"),
        (resolved("nine"), 422, "FINDING_NOT_OPEN"),
        (
            candidate(
                "session.status_changed",
                {"session_id": "s", "status": "rejected", "decision": "reject"},
            ),
            422,
            "DECISION_NOT_HUMAN",
        ),
        (on_branch("model.version_created", "b", version_id=MV1), 201, None),
        (on_branch("model.version_created", "c", version_id=MV2), 422, "BRANCH_NEEDS_PARENT"),
        (patched(MV3, MV1, "c"), 422, "BASE_NOT_BRANCH_HEAD"),
    ):
        response = client.post(base, json=event_)
        assert response.status_code == status, response.text
        if code is not None:
            assert response.json()["code"] == code


def test_every_new_event_type_has_a_rule_and_a_projection():
    from architect.projections import HANDLERS, NOT_PROJECTED
    from architect.rules import RULES

    assert NEW_EVENT_TYPES <= set(RULES) and NEW_EVENT_TYPES <= set(HANDLERS)
    assert set(RULES) == set(load_contracts().event_types) == set(HANDLERS) | NOT_PROJECTED
