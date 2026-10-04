"""M7 Parts A and B on the Temporal time-skipping environment, with the scripted Architect:
the human gate's decisions (exit test P1) and seed models (exit test P4).

Every wait is on an activity event (session_fixtures.ActivityGate), so nothing depends on
timing."""

from __future__ import annotations

import asyncio
import re
from typing import Any

import pytest
from temporalio.testing import WorkflowEnvironment

from architect import ledger
from architect.arbiter import Arbiter
from architect.checks import runner
from architect.sessions.activities import waiver_targets
from architect.sessions.service import decision_problem, load_package, session_row, start
from architect.sessions.worker import build_worker
from builders import candidate
from conftest import PROJECT
from session_fixtures import (
    DRAFT_INVALID,
    DRAFT_V1,
    P1_EXTEND_STORY,
    P4_SEED_STORY,
    S1_STORY,
    SESSION_ID,
    TASK_QUEUE,
    WORKER,
    ActivityGate,
    ScriptedArchitect,
    create_project,
    events_of,
    make_activities,
    make_gateway,
    model_from_ops,
    result_of,
    run_to_end,
    session_input,
)

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")

TIGHT = {"tokens": 4000}  # frame and draft fit; the round-1 repair's reservation does not


def run(coroutine) -> Any:
    return asyncio.run(coroutine)


def of_type(pool, *types: str) -> list[dict[str, Any]]:
    return events_of(pool, PROJECT, *types)


# --- P1: the gate -----------------------------------------------------------------------------


def test_p1_a_budget_stop_waits_at_the_gate_refuses_what_cannot_apply_and_extend_resumes(
    pool, tmp_path
):
    create_project(pool, PROJECT)
    architect_ = ScriptedArchitect(P1_EXTEND_STORY)
    activities = make_activities(pool, make_gateway(pool, architect_), tmp_path / "objects")
    gate = ActivityGate(activities)
    # the status is recorded as awaiting_approval when the gate opens and after each refusal
    opened, refused_1, refused_2, reopened = (
        gate.on_finish("record_status", status="awaiting_approval") for _ in range(4)
    )
    seen: dict[str, Any] = {}

    async def body() -> dict[str, Any]:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with build_worker(env.client, task_queue=TASK_QUEUE, activities=activities):
                try:
                    handle = await start(
                        env.client, session_input(PROJECT, overrides=TIGHT), TASK_QUEUE
                    )
                    await opened.wait()
                    seen["stopped"] = await handle.query("status")
                    seen["stopped_row"] = session_row(pool, PROJECT, SESSION_ID)

                    await handle.signal("approve")
                    await refused_1.wait()
                    seen["after_approve"] = await handle.query("status")
                    seen["refusal_row"] = session_row(pool, PROJECT, SESSION_ID)

                    await handle.signal("approve_with_risks", {"reason": "   "})
                    await refused_2.wait()
                    seen["after_blank_reason"] = await handle.query("status")

                    await handle.signal(
                        "extend", {"tokens": 100000, "rounds": 1, "signer": "saumya"}
                    )
                    await reopened.wait()
                    seen["extended"] = await handle.query("status")
                    await handle.signal("approve", {"signer": "saumya"})
                    return await result_of(handle)
                finally:
                    gate.release_all()

    final = run(body())

    # stopped by the budget: awaiting approval, with its package and the best so far
    stopped = seen["stopped"]
    draft_version = of_type(pool, "model.patch_committed")[0]["payload"]["version_id"]
    assert (stopped["status"], stopped["outcome"]) == ("awaiting_approval", "stopped_budget")
    assert stopped["gate_open"] == "end" and stopped["gate_verdict"] == "BLOCKED"
    assert stopped["package_key"] and stopped["best_version"] == draft_version
    assert "budget-exhausted" in stopped["open_risk_ids"]
    assert seen["stopped_row"]["status"] == "awaiting_approval"
    assert seen["stopped_row"]["gate_verdict"] == "BLOCKED"
    first_package = load_package(activities._store, stopped["package_key"])
    assert first_package["outcome"] == "stopped_budget" and first_package["extensions"] == 0

    # approve on a BLOCKED package is refused, and the gate stays open
    refused = seen["after_approve"]
    assert (refused["status"], refused["refusals"]) == ("awaiting_approval", 1)
    assert refused["last_refusal"]["decision"] == "approve"
    assert "BLOCKED" in refused["last_refusal"]["why"]
    assert seen["refusal_row"]["last_refusal"] == refused["last_refusal"]
    # approve_with_risks without a reason is refused
    blank = seen["after_blank_reason"]
    assert (blank["status"], blank["refusals"]) == ("awaiting_approval", 2)
    assert blank["last_refusal"] == {
        "decision": "approve_with_risks",
        "why": "approve_with_risks needs a non-empty reason",
    }
    assert of_type(pool, "waiver.signed") == []

    # extend: a new budget.updated signed by the human, and the loop resumed from the best
    extended = seen["extended"]
    assert (extended["status"], extended["outcome"]) == ("awaiting_approval", "completed")
    assert extended["gate_verdict"] == "ALLOWED" and extended["extensions"] == 1
    assert extended["round"] == 2 and extended["package_key"] != stopped["package_key"]
    budgets = of_type(pool, "budget.updated")
    assert [b["payload"]["limits"]["tokens"] for b in budgets] == [4000, 104000]
    assert budgets[1]["actor"] == {"kind": "human", "id": "saumya"}
    assert budgets[1]["payload"]["limits"] == {
        "tokens": 104000,
        "usd": 25.0,
        "wall_clock_minutes": 360,
    }
    assert architect_.served == {("frame", 0): 1, ("draft", 1): 1, ("repair", 2): 1}
    phases = [e["payload"]["to"] for e in of_type(pool, "session.phase_changed")]
    assert phases == [
        "frame", "research", "model", "draft",
        "attack", "repair", "converge", "package",
        "attack", "repair", "verify", "converge", "package",
    ]  # fmt: skip
    patches = of_type(pool, "model.patch_committed")
    assert len(patches) == 2 and patches[1]["payload"]["base_version"] == draft_version
    assert len(of_type(pool, "model.version_created")) == 1, "the best was already the head"
    with pool.connection() as conn:
        calls = [
            r["status"]
            for r in conn.execute("SELECT status FROM gw_calls ORDER BY ts, call_id").fetchall()
        ]
    assert calls == ["started", "ok", "started", "ok", "budget_refused", "started", "ok"]

    assert (final["status"], final["outcome"]) == ("approved", "completed")
    assert final["best_version"] == patches[1]["payload"]["version_id"]
    assert final["limits"]["tokens"] == 104000 and final["limits"]["max_rounds"] == 4
    assert "budget-exhausted" not in final["open_risk_ids"]
    row = session_row(pool, PROJECT, SESSION_ID)
    assert row["status"] == "approved" and row["limits"]["tokens"] == 104000
    package = load_package(activities._store, final["package_key"])
    assert package["outcome"] == "completed" and package["extensions"] == 1
    assert package["gate"]["verdict"] == "ALLOWED"
    assert ledger.verify_chain(pool, PROJECT)[1] == []


def approve_a_blocked_package_with_risks(pool, tmp_path, reason: str):
    """A budget-stopped session whose package is BLOCKED by C-007 on the worker, approved
    with risks by saumya. Returns (the view while waiting, the final view, the activities)."""
    create_project(pool, PROJECT)
    architect_ = ScriptedArchitect(S1_STORY)
    activities = make_activities(pool, make_gateway(pool, architect_), tmp_path / "objects")
    gate = ActivityGate(activities)
    opened = gate.on_finish("record_status", status="awaiting_approval")

    async def body() -> tuple[dict[str, Any], dict[str, Any]]:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with build_worker(env.client, task_queue=TASK_QUEUE, activities=activities):
                try:
                    handle = await start(
                        env.client, session_input(PROJECT, overrides=TIGHT), TASK_QUEUE
                    )
                    await opened.wait()
                    waiting = await handle.query("status")
                    await handle.signal(
                        "approve_with_risks", {"reason": reason, "signer": "saumya"}
                    )
                    return waiting, await result_of(handle)
                finally:
                    gate.release_all()

    waiting, final = run(body())
    return waiting, final, activities


def test_p1_approve_with_risks_signs_one_human_waiver_per_blocking_check_and_element(
    pool, tmp_path
):
    reason = "Accepted for the pilot; the worker's recovery lands in sprint 2."
    waiting, final, activities = approve_a_blocked_package_with_risks(pool, tmp_path, reason)
    package = load_package(activities._store, final["package_key"])
    blocking = package["gate"]["reasons"]
    pairs = [(r["check_id"], element) for r in blocking for element in r["element_refs"]]
    assert pairs == [("C-007", WORKER)], "one blocking (check, element) in this package"
    assert (final["status"], final["outcome"]) == ("approved_with_risks", "stopped_budget")
    waivers = of_type(pool, "waiver.signed")
    assert len(waivers) == len(pairs) == 1, "one waiver per blocking (check, element)"
    (waiver,) = waivers
    assert waiver["actor"] == {"kind": "human", "id": "saumya"}
    assert waiver["payload"]["target_ref"] == f"C-007:{WORKER}", "the element, not the check"
    assert waiver["payload"]["risk"] == reason
    assert waiver["payload"]["signer"] == "saumya"
    assert re.fullmatch(r"wvr_[0-9A-Za-z]{10,26}", waiver["payload"]["waiver_id"])
    assert final["waivers"] == [
        {"waiver_id": waiver["payload"]["waiver_id"], "target_ref": f"C-007:{WORKER}"}
    ]
    row = session_row(pool, PROJECT, SESSION_ID)
    assert row["status"] == "approved_with_risks" and row["waivers"] == final["waivers"]
    assert waiting["refusals"] == 0 and final["refusals"] == 0


def test_p1_a_waiver_covers_its_element_only_and_a_new_one_failing_the_check_is_not_covered(
    pool, tmp_path
):
    _, final, _ = approve_a_blocked_package_with_risks(pool, tmp_path, "Accepted for the pilot.")
    approved = final["best_version"]
    (signed,) = final["waivers"]
    # the approved version again, with the waiver now on record: the worker is waived
    again = {r.check_id: r for r in runner.run(pool, PROJECT, approved).results}
    assert again["C-007"].status == "pass" and again["C-007"].element_refs == []
    assert again["C-007"].evidence["waived"] == {WORKER: signed["waiver_id"]}
    assert runner.gate(pool, PROJECT, approved)["verdict"] == "ALLOWED"

    # after the approval, a NEW stateful component with no durability class joins the model
    newcomer = "cmp_DEDUPCACHE1"
    patch = {
        "base_version": approved,
        "rationale": "a deduplication cache, added after the approval",
        "ops": [
            {
                "op": "add_element",
                "element_type": "components",
                "element": {
                    "id": newcomer,
                    "name": "Dedup cache",
                    "kind": "cache",
                    "stateful": True,
                    "requirement_refs": [],
                },
            }
        ],
    }
    later = "mv_AFTERWAIVER1"
    Arbiter(pool).submit(
        PROJECT,
        candidate(
            "model.patch_committed",
            {"version_id": later, "base_version": approved, "patch": patch},
        ),
    )
    results = {r.check_id: r for r in runner.run(pool, PROJECT, later).results}
    c007 = results["C-007"]
    assert c007.status == "fail" and c007.element_refs == [newcomer], "the waiver does not cover it"
    assert c007.evidence["waived"] == {WORKER: signed["waiver_id"]}, "it still covers the worker"
    verdict = runner.gate(pool, PROJECT, later)
    assert verdict["verdict"] == "BLOCKED"
    assert [(r["check_id"], r["element_refs"]) for r in verdict["reasons"]] == [
        ("C-007", [newcomer])
    ]
    assert len(of_type(pool, "waiver.signed")) == 1


def test_p1_waiver_targets_name_elements_and_fall_back_to_the_check_only_without_any():
    blocking = [
        {"check_id": "C-007", "status": "fail", "element_refs": ["cmp_A", "cmp_B"]},
        {"check_id": "C-005", "status": "not_evaluated", "reason": "no recorded result"},
        {"check_id": "C-008", "status": "error", "element_refs": []},
        {"objection_id": "obj_SPLITBRAIN1", "status": "open", "element_refs": ["flw_X"]},
        {"check_id": "C-007", "status": "fail", "element_refs": ["cmp_B"]},
        {"status": "open"},
    ]
    assert waiver_targets(blocking) == [
        "C-007:cmp_A",
        "C-007:cmp_B",
        "C-005",
        "C-008",
        "obj_SPLITBRAIN1",
    ]
    assert waiver_targets([]) == []


def test_p1_cancel_ends_directly_and_a_decision_without_an_open_gate_is_refused(pool, tmp_path):
    create_project(pool, PROJECT)
    architect_ = ScriptedArchitect(S1_STORY)
    activities = make_activities(pool, make_gateway(pool, architect_), tmp_path / "objects")
    gate = ActivityGate(activities)
    attack = gate.hold("attack", phase="attack")

    async def body() -> tuple[dict[str, Any], dict[str, Any]]:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with build_worker(env.client, task_queue=TASK_QUEUE, activities=activities):
                try:
                    handle = await start(env.client, session_input(PROJECT), TASK_QUEUE)
                    await attack.wait()
                    # no gate is open while the attack runs: the approval is refused, not kept
                    await handle.signal("approve")
                    early = await handle.query("status")
                    await handle.signal("cancel")
                    attack.release()
                    return early, await result_of(handle)
                finally:
                    gate.release_all()

    early, final = run(body())
    assert early["gate_open"] is None and early["refusals"] == 1
    assert early["last_refusal"] == {"decision": "approve", "why": "no human gate is open"}
    assert (final["status"], final["outcome"]) == ("cancelled", "cancelled")
    assert final["package_key"] and final["gate_open"] is None
    recorded = [args["status"] for name, args in gate.started if name == "record_status"]
    assert "awaiting_approval" not in recorded, "cancel opens no gate"
    assert recorded[-1] == "cancelled"
    assert session_row(pool, PROJECT, SESSION_ID)["status"] == "cancelled"


def test_p1_the_read_model_predicts_what_the_workflow_refuses():
    waiting = {"status": "awaiting_approval", "phase": "package", "gate_verdict": "BLOCKED"}
    assert "BLOCKED" in decision_problem(waiting | {"outcome": "stopped_budget"}, "approve")
    assert decision_problem(waiting | {"gate_verdict": "ALLOWED"}, "approve") is None
    assert "non-empty reason" in decision_problem(waiting, "approve_with_risks", reason=" ")
    assert decision_problem(waiting, "approve_with_risks", reason="accepted") is None
    assert decision_problem(waiting, "reject") is None
    assert "stopped by" in decision_problem(
        waiting | {"outcome": "completed_with_risks"}, "extend", extension={"tokens": 1}
    )
    budget = waiting | {"outcome": "stopped_budget"}
    assert "tokens or usd" in decision_problem(budget, "extend", extension={"rounds": 1})
    assert decision_problem(budget, "extend", extension={"usd": 2.5}) is None
    clock = waiting | {"outcome": "stopped_time"}
    assert "wall_clock_minutes" in decision_problem(clock, "extend", extension={"tokens": 5})
    assert decision_problem(clock, "extend", extension={"wall_clock_minutes": 60}) is None
    assert "no human gate" in decision_problem({"status": "running"}, "approve")
    mid = {"status": "awaiting_approval", "phase": "attack"}
    assert decision_problem(mid, "approve") is None and decision_problem(mid, "reject") is None
    assert "end gate" in decision_problem(mid, "extend", extension={"tokens": 1})


# --- P4: seed models --------------------------------------------------------------------------


def test_p4_a_seed_is_committed_through_the_arbiter_and_the_draft_is_skipped(pool, tmp_path):
    create_project(pool, PROJECT)
    architect_ = ScriptedArchitect(P4_SEED_STORY)  # no draft in the story: a draft call fails
    activities = make_activities(pool, make_gateway(pool, architect_), tmp_path / "objects")
    seed = model_from_ops(DRAFT_V1["ops"])

    async def body() -> dict[str, Any]:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            return await run_to_end(env.client, activities, session_input(PROJECT, seed=seed))

    final = run(body())
    assert (final["status"], final["outcome"]) == ("approved", "completed")
    assert architect_.served == {("frame", 0): 1, ("repair", 1): 1}
    phases = [e["payload"]["to"] for e in of_type(pool, "session.phase_changed")]
    assert phases == [
        "frame", "research", "model", "attack", "repair", "verify", "converge", "package",
    ]  # fmt: skip
    events = events_of(pool, PROJECT)
    types = [e["type"] for e in events]
    genesis = types.index("model.version_created")
    assert types[genesis + 1 : genesis + 3] == ["model.patch_proposed", "model.patch_committed"]
    seeded = events[genesis + 2]
    assert seeded["payload"]["base_version"] == events[genesis]["payload"]["version_id"]
    assert seeded["payload"]["from_proposal"] == f"prp-{SESSION_ID}-seed"
    assert seeded["payload"]["patch"]["rationale"].startswith("seed model")
    assert seeded["actor"]["kind"] == "system", "the seed is the owner's input, not the architect's"
    assert len(seeded["payload"]["patch"]["ops"]) == len(DRAFT_V1["ops"])
    assert genesis + 2 < types.index("check.result")
    on_seed = {
        e["payload"]["check_id"]: e["payload"]["status"]
        for e in of_type(pool, "check.result")
        if e["payload"]["model_version"] == seeded["payload"]["version_id"]
    }
    assert on_seed["C-007"] == "fail" and on_seed["C-012"] == "fail"
    assert len(of_type(pool, "model.patch_committed")) == 2, "the seed and one repair"
    from architect import readmodel

    model = readmodel.model_version(pool, PROJECT, seeded["payload"]["version_id"])["model"]
    assert model["project_id"] == PROJECT, "the seed file's own ids are ignored"
    assert model["elements"] == seed["elements"] and model["links"] == seed["links"]


def test_p4_an_invalid_seed_is_refused_by_the_arbiter_and_the_session_fails_cleanly(pool, tmp_path):
    create_project(pool, PROJECT)
    architect_ = ScriptedArchitect(P4_SEED_STORY)
    activities = make_activities(pool, make_gateway(pool, architect_), tmp_path / "objects")
    bad_seed = model_from_ops(DRAFT_INVALID["ops"])  # a component without `kind`

    async def body() -> dict[str, Any]:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with build_worker(env.client, task_queue=TASK_QUEUE, activities=activities):
                handle = await start(env.client, session_input(PROJECT, seed=bad_seed), TASK_QUEUE)
                try:
                    await result_of(handle)
                except AssertionError as failure:
                    return {"error": str(failure), "view": await handle.query("status")}
                raise AssertionError("the session did not fail")

    outcome = run(body())
    assert "INVALID_MODEL_RESULT" in outcome["error"]
    view = outcome["view"]
    assert (view["status"], view["outcome"]) == ("failed", "failed")
    assert "the seed model was refused" in view["failure"]
    assert '"code": "INVALID_MODEL_RESULT"' in view["failure"] and "kind" in view["failure"]
    row = session_row(pool, PROJECT, SESSION_ID)
    assert row["status"] == "failed" and row["outcome"] == "failed"
    assert row["failure"] == view["failure"] and row["package_key"] is None
    assert of_type(pool, "model.patch_proposed", "model.patch_committed") == []
    assert len(of_type(pool, "model.version_created")) == 1
    assert architect_.served == {("frame", 0): 1}
    assert ledger.verify_chain(pool, PROJECT)[1] == []
