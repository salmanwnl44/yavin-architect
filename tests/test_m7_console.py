"""M7 Part C: the structural model diff (exit test P2), the session watch view rendered to a
string (P3), and the CLI and API surface of Parts A to C (decisions, --seed, watch, model
diff, why)."""

from __future__ import annotations

import copy
import json
import time
from typing import Any

import pytest
from fastapi.testclient import TestClient

from architect import console, modeldiff, readmodel
from architect.api import create_app
from architect.cli import main
from architect.ingestion.objectstore import LocalObjectStore
from architect.projector import Projector
from architect.sessions.service import find_session, session_row
from conftest import PROJECT
from session_fixtures import (
    BRIEF,
    DRAFT_V1,
    FLW_ENQUEUE,
    GATEWAY,
    P1_EXTEND_STORY,
    P4_SEED_STORY,
    S1_STORY,
    TASK_QUEUE,
    WORKER,
    ScriptedArchitect,
    background_worker,
    create_project,
    events_of,
    make_activities,
    make_gateway,
    model_from_ops,
)

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


# --- P2: the diff, a pure function ------------------------------------------------------------

BASE: dict[str, Any] = {
    "version_id": "mv_AAAAAAAAAA",
    "project_id": "p",
    "elements": {
        "components": [
            {"id": "cmp_A000000001", "name": "A", "kind": "service", "stateful": False},
            {"id": "cmp_B000000001", "name": "B", "kind": "queue", "stateful": True},
        ],
        "flows": [{"id": "flw_AB00000001", "from": "cmp_A000000001", "to": "cmp_B000000001"}],
    },
    "links": {
        "depends_on": [{"from": "cmp_A000000001", "to": "cmp_B000000001", "kind": "async"}],
        "satisfies": [{"component": "cmp_A000000001", "requirement": "req_x"}],
    },
}


def test_p2_identical_versions_have_an_empty_diff():
    diff = modeldiff.diff_models(BASE, copy.deepcopy(BASE) | {"version_id": "mv_BBBBBBBBBB"})
    assert diff == {
        "elements": {"added": [], "removed": [], "changed": []},
        "links": {"added": [], "removed": []},
    }
    assert modeldiff.is_empty(diff) and modeldiff.format_diff(diff) == []
    assert modeldiff.summary(diff) == "no structural change"
    assert modeldiff.is_empty(modeldiff.diff_models({}, {"elements": {}, "links": {}}))


def test_p2_elements_added_removed_and_changed_field_by_field():
    after = copy.deepcopy(BASE)
    after["elements"]["components"][0] |= {"kind": "gateway", "deployment_unit": "du-a"}
    del after["elements"]["components"][0]["stateful"]
    after["elements"]["components"].pop(1)  # B removed
    after["elements"]["components"].append({"id": "cmp_C000000001", "name": "C"})
    after["elements"]["interfaces"] = [{"id": "if_C0000000001", "style": "sync"}]
    diff = modeldiff.diff_models(BASE, after)

    assert [(e["element_type"], e["id"]) for e in diff["elements"]["added"]] == [
        ("components", "cmp_C000000001"),
        ("interfaces", "if_C0000000001"),
    ]
    assert diff["elements"]["added"][0]["element"] == {"id": "cmp_C000000001", "name": "C"}
    assert [(e["element_type"], e["id"]) for e in diff["elements"]["removed"]] == [
        ("components", "cmp_B000000001")
    ]
    (changed,) = diff["elements"]["changed"]
    assert (changed["element_type"], changed["id"]) == ("components", "cmp_A000000001")
    assert changed["fields"] == [
        {"field": "deployment_unit", "to": "du-a"},
        {"field": "kind", "from": "service", "to": "gateway"},
        {"field": "stateful", "from": False},
    ]
    assert not modeldiff.is_empty(diff)
    assert modeldiff.summary(diff) == "elements +2 -1 ~1; links +0 -0"
    assert modeldiff.format_diff(diff) == [
        "+ components cmp_C000000001",
        "+ interfaces if_C0000000001",
        "- components cmp_B000000001",
        "~ components cmp_A000000001",
        '    deployment_unit: (unset) -> "du-a"',
        '    kind: "service" -> "gateway"',
        "    stateful: false -> (unset)",
    ]
    # the diff is directional, and its inputs are left untouched
    back = modeldiff.diff_models(after, BASE)
    assert len(back["elements"]["added"]) == 1 and len(back["elements"]["removed"]) == 2
    assert BASE["elements"]["components"][0]["kind"] == "service"


def test_p2_links_added_and_removed_as_values():
    after = copy.deepcopy(BASE)
    after["links"]["depends_on"] = [
        {"from": "cmp_A000000001", "to": "cmp_B000000001", "kind": "sync"}
    ]
    after["links"]["satisfies"].append({"component": "cmp_B000000001", "requirement": "req_x"})
    after["links"]["satisfies"].append({"component": "cmp_B000000001", "requirement": "req_x"})
    after["links"]["mitigates"] = [{"control": "cmp_A000000001", "risk": "r1"}]
    diff = modeldiff.diff_models(BASE, after)
    assert diff["elements"] == {"added": [], "removed": [], "changed": []}
    assert diff["links"]["added"] == [
        {
            "link_type": "depends_on",
            "link": {"from": "cmp_A000000001", "to": "cmp_B000000001", "kind": "sync"},
        },
        {"link_type": "mitigates", "link": {"control": "cmp_A000000001", "risk": "r1"}},
        {"link_type": "satisfies", "link": {"component": "cmp_B000000001", "requirement": "req_x"}},
        {"link_type": "satisfies", "link": {"component": "cmp_B000000001", "requirement": "req_x"}},
    ]
    assert diff["links"]["removed"] == [
        {
            "link_type": "depends_on",
            "link": {"from": "cmp_A000000001", "kind": "async", "to": "cmp_B000000001"},
        }
    ]
    assert modeldiff.summary(diff) == "elements +0 -0 ~0; links +4 -1"
    lines = modeldiff.format_diff(diff)
    assert lines[0].startswith("+ link depends_on ") and lines[-1].startswith("- link depends_on ")


def test_p2_an_id_that_moves_to_another_element_type_is_a_removal_and_an_addition():
    after = copy.deepcopy(BASE)
    moved = after["elements"]["components"].pop(1)
    after["elements"]["deployment_units"] = [moved]
    diff = modeldiff.diff_models(BASE, after)
    assert [(e["element_type"], e["id"]) for e in diff["elements"]["removed"]] == [
        ("components", "cmp_B000000001")
    ]
    assert [(e["element_type"], e["id"]) for e in diff["elements"]["added"]] == [
        ("deployment_units", "cmp_B000000001")
    ]


# --- P3: the watch view, rendered to a string -------------------------------------------------

HAND_SNAPSHOT: dict[str, Any] = {
    "as_of": "2026-10-03T09:10:00+00:00",
    "session": {
        "session_id": "ses_HANDWRITTEN01",
        "status": "running",
        "outcome": None,
        "preset": "quick",
        "round": 2,
        "phase": "repair",
        "limits": {"tokens": 4000, "usd": None},
        "spend": {"tokens": 3000, "usd": 0.03},
        "failure": None,
        "last_refusal": None,
    },
    "timeline": [
        {"kind": "phase_changed", "phase": "frame", "ts": "2026-10-03T09:00:00+00:00"},
        {"kind": "checkpoint", "phase": "frame", "ts": "2026-10-03T09:01:00+00:00"},
        {"kind": "phase_changed", "phase": "attack", "ts": "2026-10-03T09:02:30+00:00"},
        {"kind": "phase_changed", "phase": "repair", "ts": "2026-10-03T09:02:35.500000+00:00"},
    ],
    "gate": {
        "verdict": "BLOCKED",
        "reasons": [{"check_id": "C-007", "status": "fail", "element_refs": ["cmp_W"]}],
        "warnings": [],
    },
    "failing_checks": [
        {
            "check_id": "C-007",
            "severity": "critical",
            "status": "fail",
            "element_refs": ["cmp_W"],
            "evidence": '{"components":{"cmp_W":{"missing":["durability_class"]}}}',
        }
    ],
    "open_risks": ["unknown:payload-size"],
    "messages": [{"type": "Task", "summary": "Repair the head model"}],
}


def test_p3_a_frame_is_a_pure_function_of_its_snapshot():
    before = copy.deepcopy(HAND_SNAPSHOT)
    text = console.render_text(HAND_SNAPSHOT)
    assert console.render_text(HAND_SNAPSHOT) == text, "the same snapshot renders the same frame"
    assert HAND_SNAPSHOT == before, "rendering does not touch the snapshot"
    assert "ses_HANDWRITTEN01" in text and "status running" in text and "round 2" in text
    # durations come from the timeline and the snapshot's own as_of, never from a clock
    assert console.phase_rows(HAND_SNAPSHOT) == [
        ("frame", "09:00:00", "2m30s"),
        ("attack", "09:02:30", "5.5s"),
        ("repair", "09:02:35", "7m24s"),
    ]
    assert "gate IMPLEMENTATION_READY: BLOCKED" in text
    assert "blocking C-007 fail on cmp_W" in text and "durability_class" in text
    assert "unknown:payload-size" in text
    assert "tokens 3,000 / 4,000 (75%)" in text and "usd    $0.0300 / uncapped" in text
    assert "Task: Repair the head model" in text
    assert "waiting for the owner" not in text
    waiting = copy.deepcopy(HAND_SNAPSHOT)
    waiting["session"] |= {
        "status": "awaiting_approval",
        "phase": "package",
        "outcome": "stopped_budget",
    }
    waiting["session"]["last_refusal"] = {"decision": "approve", "why": "the gate is BLOCKED"}
    shown = console.render_text(waiting)
    assert "waiting for the owner: approve-with-risks --reason | extend | reject" in shown
    assert "last refused decision: approve: the gate is BLOCKED" in shown
    assert console.phase_rows(waiting)[-1] == ("repair", "09:02:35", "-")


def cli(dsn: str, capsys, *argv: str) -> tuple[int, str, str]:
    code = main(["--database-url", dsn, *argv])
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def wait_cli(dsn: str, capsys, session_id: str, wanted: str) -> dict[str, Any]:
    """Until the live status query reports `wanted`: a state that holds until the test acts."""
    for _ in range(900):
        code, out, _ = cli(dsn, capsys, "session", "status", "--session", session_id)
        assert code == 0
        view = json.loads(out)
        if view["status"] == wanted:
            return view
        assert view["status"] != "failed", view
        time.sleep(0.2)
    raise AssertionError(f"never reached {wanted}: {view}")


@pytest.fixture
def cli_env(dsn, pool, tmp_path, monkeypatch):
    create_project(pool, PROJECT)
    monkeypatch.setenv("ARCHITECT_OBJECT_STORE", str(tmp_path / "objects"))
    monkeypatch.setenv("ARCHITECT_USER", "saumya")
    brief = tmp_path / "brief.md"
    brief.write_text(BRIEF, encoding="utf-8")
    return brief


def start_cli(dsn, capsys, brief, *extra: str) -> str:
    code, out, err = cli(
        dsn, capsys, "session", "--task-queue", TASK_QUEUE, "start",
        "--project", PROJECT, "--brief", str(brief), *extra,
    )  # fmt: skip
    assert code == 0, err
    return json.loads(out)["session_id"]


def test_p3_watch_shows_a_recorded_session_and_the_cli_takes_the_gate_decisions(
    dsn, pool, tmp_path, monkeypatch, capsys, cli_env
):
    activities = make_activities(
        pool, make_gateway(pool, ScriptedArchitect(S1_STORY)), tmp_path / "objects"
    )
    with background_worker(activities) as address:
        monkeypatch.setenv("ARCHITECT_TEMPORAL_ADDRESS", address)
        session_id = start_cli(dsn, capsys, cli_env, "--override", "tokens=4000")
        wait_cli(dsn, capsys, session_id, "awaiting_approval")

        # P3: the frame of the recorded session, from the read models
        Projector(pool).catch_up(PROJECT)
        store = LocalObjectStore(tmp_path / "objects")
        snap = console.snapshot(pool, store, PROJECT, session_id)
        json.dumps(snap, default=str)  # plain data
        text = console.render_text(snap, width=200)
        assert "status awaiting_approval" in text and "outcome stopped_budget" in text
        positions = [
            text.index(f" {phase} ")
            for phase in ("frame", "research", "model", "draft", "attack", "repair", "converge")
        ]
        assert positions == sorted(positions), "the phases in order"
        assert [p for p, _, _ in console.phase_rows(snap)] == [
            "frame", "research", "model", "draft", "attack", "repair", "converge", "package",
        ]  # fmt: skip
        assert "gate IMPLEMENTATION_READY: BLOCKED" in text
        assert f"blocking C-007 fail on {WORKER}" in text
        assert "C-012" in text and FLW_ENQUEUE in text and "durability_class" in text
        for risk in (
            "budget-exhausted",
            "requirement-unmeasurable:req_no-loss",
            "check-fail:C-007",
        ):
            assert risk in text
        assert "tokens 3,000 / 4,000 (75%)" in text and "/ $25.00" in text
        assert "ModelPatchProposal: 26 ops" in text and "ClaimProposal:" in text
        assert "waiting for the owner: approve-with-risks --reason | extend | reject" in text
        assert console.render_text(snap, width=200) == text

        # the same frame from the command line, the project looked up from the session
        code, out, _ = cli(dsn, capsys, "session", "watch", "--session", session_id, "--once")
        assert code == 0 and "awaiting_approval" in out and "C-007" in out
        assert find_session(pool, session_id)["project_id"] == PROJECT
        assert cli(dsn, capsys, "session", "watch", "--session", "ses_NOSUCH0001", "--once")[0] == 1

        # decisions the gate cannot take are refused at once, with the reason
        code, _, err = cli(dsn, capsys, "session", "approve", "--session", session_id)
        assert code == 1 and "cannot approve" in err and "BLOCKED" in err
        code, _, err = cli(
            dsn, capsys, "session", "extend", "--session", session_id, "--rounds", "1"
        )
        assert code == 1 and "tokens or usd" in err
        code, out, _ = cli(
            dsn, capsys, "session", "approve-with-risks", "--session", session_id,
            "--reason", "Accepted for the pilot.",
        )  # fmt: skip
        assert code == 0 and "approve-with-risks sent" in out
        wait_cli(dsn, capsys, session_id, "approved_with_risks")
    (waiver,) = events_of(pool, PROJECT, "waiver.signed")
    assert waiver["actor"] == {"kind": "human", "id": "saumya"}, "the signer is $ARCHITECT_USER"
    assert waiver["payload"]["risk"] == "Accepted for the pilot."
    assert session_row(pool, PROJECT, session_id)["status"] == "approved_with_risks"


def test_seed_show_diff_and_why_from_the_command_line(
    dsn, pool, tmp_path, monkeypatch, capsys, cli_env
):
    activities = make_activities(
        pool, make_gateway(pool, ScriptedArchitect(P4_SEED_STORY)), tmp_path / "objects"
    )
    seed_path = tmp_path / "seed.json"
    seed_path.write_text(json.dumps(model_from_ops(DRAFT_V1["ops"])), encoding="utf-8")
    with background_worker(activities) as address:
        monkeypatch.setenv("ARCHITECT_TEMPORAL_ADDRESS", address)
        session_id = start_cli(dsn, capsys, cli_env, "--seed", str(seed_path))
        wait_cli(dsn, capsys, session_id, "awaiting_approval")
        assert cli(dsn, capsys, "session", "approve", "--session", session_id)[0] == 0
        wait_cli(dsn, capsys, session_id, "approved")
    seeded, repaired = (
        e["payload"]["version_id"] for e in events_of(pool, PROJECT, "model.patch_committed")
    )

    code, shown, _ = cli(
        dsn, capsys, "session", "show", "--project", PROJECT, "--session", session_id
    )
    assert code == 0 and f"session {session_id}: approved" in shown
    assert "-> draft" not in shown, "review mode skipped the draft"
    assert f"diff vs previous version ({seeded} -> {repaired}):" in shown
    assert "elements +0 -0 ~2; links +0 -0" in shown
    assert f"~ components {WORKER}" in shown and f"~ flows {FLW_ENQUEUE}" in shown
    assert 'durability_class: (unset) -> "rebuildable"' in shown

    code, out, _ = cli(dsn, capsys, "model", "diff", "--project", PROJECT, seeded, repaired)
    assert code == 0 and out.splitlines()[0] == (
        f"{seeded} -> {repaired}: elements +0 -0 ~2; links +0 -0"
    )
    assert 'backpressure_ref: (unset) -> "gateway-returns-429-above-queue-depth"' in out
    code, out, _ = cli(dsn, capsys, "model", "diff", "--project", PROJECT, seeded, seeded)
    assert code == 0 and out.strip() == f"{seeded} -> {seeded}: no structural change"
    code, out, _ = cli(
        dsn, capsys, "model", "diff", "--project", PROJECT, "--json", seeded, repaired
    )
    assert code == 0 and len(json.loads(out)["elements"]["changed"]) == 2
    code, _, err = cli(dsn, capsys, "model", "diff", "--project", PROJECT, seeded, "mv_NOSUCH00001")
    assert code == 1 and "no model version mv_NOSUCH00001" in err

    code, out, _ = cli(dsn, capsys, "why", "--project", PROJECT, "--element", GATEWAY)
    assert code == 0
    lines = out.splitlines()
    assert lines[0] == f"{GATEWAY} (components) API gateway  in {repaired}"
    assert "  req_peak-throughput" in lines and "  req_ack-latency" in lines
    claim_line = next(line for line in lines if "req_peak-throughput CONSTRAINS throughput" in line)
    assert claim_line.strip().startswith("clm_") and "[documented]" in claim_line
    source_line = lines[lines.index(claim_line) + 1]
    assert "source src_" in source_line and "@ brief.md#requirements" in source_line
    assert "session://" in source_line and "[user]" in source_line
    assert "decisions that affect it:" in lines
    # the element as it was in the seed version, and an element that does not exist
    code, out, _ = cli(
        dsn, capsys, "why", "--project", PROJECT, "--element", WORKER, "--version", seeded
    )
    assert code == 0 and out.splitlines()[0].endswith(f"in {seeded}")
    trace = readmodel.why(pool, PROJECT, WORKER, seeded)
    assert "durability_class" not in trace["element"]
    assert "durability_class" in readmodel.why(pool, PROJECT, WORKER)["element"]
    code, _, err = cli(dsn, capsys, "why", "--project", PROJECT, "--element", "cmp_NOSUCH0001")
    assert code == 1 and "no element cmp_NOSUCH0001" in err


def test_the_api_takes_a_seed_refuses_what_the_gate_cannot_take_and_extends(
    dsn, pool, tmp_path, monkeypatch
):
    activities = make_activities(
        pool, make_gateway(pool, ScriptedArchitect(P1_EXTEND_STORY)), tmp_path / "objects"
    )
    monkeypatch.setenv("ARCHITECT_TEMPORAL_TASK_QUEUE", TASK_QUEUE)
    with (
        background_worker(activities) as address,
        TestClient(create_app(dsn, temporal_address=address)) as client,
    ):
        assert client.post("/v1/projects", json={"project_id": PROJECT}).status_code == 201
        client.app.state.object_store = LocalObjectStore(tmp_path / "objects")
        base = f"/v1/projects/{PROJECT}/sessions"
        created = client.post(
            base, json={"brief": BRIEF, "preset": "quick", "overrides": {"tokens": 4000}}
        )
        assert created.status_code == 201, created.text
        session_id = created.json()["session_id"]

        def wait(outcome: str) -> dict[str, Any]:
            """Until the session waits at its gate with `outcome`: stable until the test acts."""
            for _ in range(900):
                response = client.get(f"{base}/{session_id}")
                if response.status_code == 200:
                    row = response.json()["session"]
                    assert row["status"] != "failed", row
                    if row["status"] == "awaiting_approval" and row["outcome"] == outcome:
                        return response.json()
                time.sleep(0.2)
            raise AssertionError(f"never waited with outcome {outcome}")

        stopped = wait("stopped_budget")
        assert stopped["session"]["gate_verdict"] == "BLOCKED" and stopped["package_key"]
        refused = client.post(f"{base}/{session_id}/approve")
        assert refused.status_code == 422 and refused.json()["code"] == "DECISION_REFUSED"
        assert "BLOCKED" in refused.json()["detail"]
        blank = client.post(f"{base}/{session_id}/approve-with-risks", json={"reason": " "})
        assert blank.status_code == 422 and "non-empty reason" in blank.json()["detail"]
        short = client.post(f"{base}/{session_id}/extend", json={"rounds": 2})
        assert short.status_code == 422 and "tokens or usd" in short.json()["detail"]
        assert client.post(f"{base}/{session_id}/extend", json={"tokens": -5}).status_code == 422

        extended = client.post(
            f"{base}/{session_id}/extend", json={"tokens": 100000, "signer": "saumya"}
        )
        assert extended.status_code == 200 and extended.json()["signal"] == "extend"
        done = wait("completed")
        assert (
            done["gate"]["verdict"] == "ALLOWED" and done["session"]["limits"]["tokens"] == 104000
        )
        assert done["rounds"][-1]["diff"]["summary"] == "elements +0 -0 ~2; links +0 -0"
        assert (
            client.post(f"{base}/{session_id}/approve", json={"signer": "saumya"}).status_code
            == 200
        )
        for _ in range(900):
            if client.get(f"{base}/{session_id}").json()["session"]["status"] == "approved":
                break
            time.sleep(0.2)
        else:
            raise AssertionError("never approved")
        late = client.post(f"{base}/{session_id}/reject")
        assert late.status_code == 422 and "no human gate is open" in late.json()["detail"]


def test_new_project_from_the_command_line(dsn, pool, capsys):
    assert cli(dsn, capsys, "new-project", "demo") == (0, "project 'demo' created\n", "")
    assert cli(dsn, capsys, "new-project", "demo") == (0, "project 'demo' already exists\n", "")
    code, _, err = cli(
        dsn, capsys, "session", "show", "--project", "demo", "--session", "ses_NONE000001"
    )
    assert code == 1 and "is not in project 'demo'" in err
