"""Durable design sessions (M6) on the Temporal time-skipping test environment, with the
scripted Architect. Exit tests S1 to S5, S7 to S10 and S12 live here; S6 and S11 (a real
Temporal dev server) in test_sessions_server.py."""

from __future__ import annotations

import ast
import asyncio
import json
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from temporalio.testing import WorkflowEnvironment

import architect
from architect import ledger
from architect.arbiter import Arbiter
from architect.gateway.untrusted import UNTRUSTED_RULE, with_rule
from architect.projector import Projector
from architect.sessions import agent, linter
from architect.sessions.agent import messages_of
from architect.sessions.compiler import CompileTask, compile, rank_claims
from architect.sessions.service import load_package, session_row, show, start
from architect.sessions.worker import build_worker
from builders import candidate, claim, ident
from conftest import PROJECT
from session_fixtures import (
    BRIEF,
    REQ_LATENCY,
    REQ_NO_LOSS,
    REQ_THROUGHPUT,
    S1_STORY,
    S2_STORY,
    S3_GIVE_UP_STORY,
    S3_RETRY_STORY,
    SESSION_ID,
    TASK_QUEUE,
    WORKER,
    ActivityGate,
    ScriptedArchitect,
    background_worker,
    create_project,
    events_of,
    make_activities,
    make_gateway,
    result_of,
    run_to_end,
    session_input,
    wait_for,
)

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


def run(coroutine) -> Any:
    return asyncio.run(coroutine)


async def time_skipping():
    return await WorkflowEnvironment.start_time_skipping()


def catch_up(pool, project_id: str = PROJECT) -> None:
    Projector(pool).catch_up(project_id)


def payloads(pool, project_id: str = PROJECT) -> list[tuple[str, dict[str, Any]]]:
    return [(e["type"], e["payload"]) for e in events_of(pool, project_id)]


# --- S1: planted flaws, end to end ------------------------------------------------------------


def test_s1_planted_flaws_are_caught_repaired_and_the_session_is_approved(pool, tmp_path):
    create_project(pool, PROJECT)
    architect_ = ScriptedArchitect(S1_STORY)
    activities = make_activities(pool, make_gateway(pool, architect_), tmp_path / "objects")

    async def body() -> dict[str, Any]:
        async with await time_skipping() as env:
            return await run_to_end(env.client, activities, session_input(PROJECT))

    final = run(body())
    assert final["status"] == "approved" and final["outcome"] == "completed"
    assert final["stop_reason"] == "allowed" and final["round"] == 1
    assert final["package_key"] and final["best_version"]
    assert architect_.served == {("frame", 0): 1, ("draft", 1): 1, ("repair", 1): 1}

    # the ledger tells the whole story, in order
    types = [t for t, _ in payloads(pool)]
    phases = [p["to"] for t, p in payloads(pool) if t == "session.phase_changed"]
    assert phases == [
        "frame",
        "research",
        "model",
        "draft",
        "attack",
        "repair",
        "verify",
        "converge",
        "package",
    ]
    assert types[0] == "source.ingested" and types[1] == "budget.updated"
    assert types.index("model.version_created") < types.index("model.patch_committed")
    first_check = types.index("check.result")
    assert types.index("model.patch_committed") < first_check
    assert types.count("model.patch_committed") == 2, "the draft and one repair"
    assert types.count("session.checkpoint") >= 6
    assert types[-1] == "session.checkpoint" and types[-2] == "session.phase_changed"

    # requirements traced to the brief, with the linter's verdicts
    requirements = {
        p["claim"]["subject"]["id"]: p["claim"]
        for t, p in payloads(pool)
        if t == "claim.committed" and p["claim"]["subject"]["entity_type"] == "requirement"
    }
    assert set(requirements) == {REQ_THROUGHPUT, REQ_LATENCY, REQ_NO_LOSS}
    assert requirements[REQ_LATENCY]["magnitude"] == {"value": 200.0, "unit": "ms"}
    assert requirements[REQ_THROUGHPUT]["magnitude"] == {"value": 5000.0, "unit": "msg/s"}
    assert "magnitude" not in requirements[REQ_NO_LOSS]
    brief = [p for t, p in payloads(pool) if t == "source.ingested"][0]
    for requirement in requirements.values():
        assert requirement["taint"] == {"origin": "user"} and requirement["status"] == "documented"
        assert requirement["evidence"][0]["source"] == brief["source_id"]
        assert requirement["evidence"][0]["span"].startswith("brief.md#requirements")

    # the attack caught both planted flaws on the draft; the verify battery is clean
    checks = [
        (p["check_id"], p["status"], p["model_version"])
        for t, p in payloads(pool)
        if t == "check.result"
    ]
    draft_version = [p["version_id"] for t, p in payloads(pool) if t == "model.patch_committed"][0]
    repair_version = [p["version_id"] for t, p in payloads(pool) if t == "model.patch_committed"][1]
    on_draft = {c: s for c, s, v in checks if v == draft_version}
    on_repair = {c: s for c, s, v in checks if v == repair_version}
    assert on_draft["C-007"] == "fail" and on_draft["C-012"] == "fail"
    assert {s for c, s in on_draft.items() if c not in ("C-007", "C-012")} <= {"pass", "skipped"}
    assert set(on_repair.values()) <= {"pass", "skipped"}
    assert final["best_version"] == repair_version

    # open risks: the unmeasurable requirement and the unknown, nothing else
    assert final["open_risk_ids"] == [
        f"requirement-unmeasurable:{REQ_NO_LOSS}",
        "unknown:payload-size",
    ]

    # the package
    package = load_package(activities._store, final["package_key"])
    assert package["outcome"] == "completed" and package["gate"]["verdict"] == "ALLOWED"
    assert package["best_version"] == repair_version
    assert {r["check_id"] for r in package["check_results"]} == {f"C-{n:03d}" for n in range(1, 14)}
    trace = {t["requirement"]: t["components"] for t in package["requirement_trace"]}
    assert trace[REQ_NO_LOSS] == ["cmp_QUEUE00001", "cmp_STORE00001"]
    assert {r["id"] for r in package["open_risks"]} == set(final["open_risk_ids"])
    assert [a["decision"]["title"] for a in package["adrs"]] == [
        "Decouple ingestion from storage with a durable queue"
    ]
    assert package["spend"]["tokens"] == 3 * 1500 and package["spend"]["usd"] > 0
    # the ADR citing a claim outside the context was never recorded
    assert len([t for t, _ in payloads(pool) if t == "decision.recorded"]) == 1

    # the read model agrees
    row = session_row(pool, PROJECT, SESSION_ID)
    assert row["status"] == "approved" and row["package_key"] == final["package_key"]
    assert row["spend"]["tokens"] == 4500
    detail = show(pool, activities._store, PROJECT, SESSION_ID)
    assert [
        step["phase"] for step in detail["timeline"] if step["kind"] == "phase_changed"
    ] == phases
    assert (
        detail["gate"]["verdict"] == "ALLOWED"
        and detail["rounds"][0]["verify"]["verdict"] == "ALLOWED"
    )

    # every agent message is typed and references its gateway call; the budget was written
    messages = messages_of(pool, PROJECT, SESSION_ID)
    assert {m["type"] for m in messages} == {"Task", "ClaimProposal", "ModelPatchProposal"}
    assert all(m["call_ids"] for m in messages)
    tasks = [m for m in messages if m["type"] == "Task"]
    assert all(m["context_manifest"] and m["context_dropped"] is not None for m in tasks)
    budget = [p for t, p in payloads(pool) if t == "budget.updated"][0]
    assert budget == {
        "scope": {"session": SESSION_ID},
        "limits": {"tokens": 2000000, "usd": 25.0, "wall_clock_minutes": 360},
    }
    assert ledger.verify_chain(pool, PROJECT)[1] == []


# --- S2: unrepairable -------------------------------------------------------------------------


def test_s2_an_unrepairable_flaw_ends_with_risks_and_a_blocked_gate(pool, tmp_path):
    create_project(pool, PROJECT)
    architect_ = ScriptedArchitect(S2_STORY)
    activities = make_activities(pool, make_gateway(pool, architect_), tmp_path / "objects")

    async def body() -> dict[str, Any]:
        async with await time_skipping() as env:
            # deep: five rounds allowed, so rule (b) is what stops it; the after-attack gate
            # is approved as soon as it is reached
            async with build_worker(env.client, task_queue=TASK_QUEUE, activities=activities):
                handle = await start(env.client, session_input(PROJECT, preset="deep"), TASK_QUEUE)
                view = await wait_for(handle, lambda v: v["status"] == "awaiting_approval")
                assert view["phase"] == "attack" and view["round"] == 1
                await handle.signal("approve")
                view = await wait_for(
                    handle, lambda v: v["status"] == "awaiting_approval" and v["phase"] == "package"
                )
                assert view["outcome"] == "completed_with_risks"
                await handle.signal("approve")
                return await result_of(handle)

    final = run(body())
    assert final["outcome"] == "completed_with_risks" and final["status"] == "approved"
    assert final["stop_reason"] == "no_improvement" and final["round"] == 3
    package = load_package(activities._store, final["package_key"])
    assert package["gate"]["verdict"] == "BLOCKED"
    assert [r["check_id"] for r in package["gate"]["reasons"]] == ["C-007"]
    risks = {r["id"]: r for r in package["open_risks"]}
    assert risks["check-fail:C-007"]["element_refs"] == [WORKER]
    assert risks["check-fail:C-007"]["evidence"]["components"][WORKER]["missing"] == [
        "durability_class"
    ]
    assert f"waiver-requested:C-007:{WORKER}" in risks, "the architect only requested a waiver"
    assert not [t for t, _ in payloads(pool) if t == "waiver.signed"], "never auto-signed"
    assert [r["repair"]["version"] is not None for r in package["rounds"]] == [True, False, False]
    assert (
        package["rounds"][0]["verify"]["failing"] == 1
        and package["rounds"][0]["attack"]["failing"] == 2
    )
    # the waiver request is a Question to the owner
    questions = messages_of(pool, PROJECT, SESSION_ID, "Question")
    assert len(questions) == 1 and questions[0]["body"]["blocking"] is False
    assert "Waiver requested for C-007" in questions[0]["body"]["question"]


# --- S3: the Arbiter rejection loop -----------------------------------------------------------


def test_s3_an_invalid_patch_is_fed_back_and_corrected(pool, tmp_path):
    create_project(pool, PROJECT)
    architect_ = ScriptedArchitect(S3_RETRY_STORY)
    activities = make_activities(pool, make_gateway(pool, architect_), tmp_path / "objects")

    async def body() -> dict[str, Any]:
        async with await time_skipping() as env:
            return await run_to_end(env.client, activities, session_input(PROJECT))

    final = run(body())
    assert final["status"] == "approved" and final["outcome"] == "completed"
    assert architect_.served[("draft", 1)] == 2
    with pool.connection() as conn:
        drafts = conn.execute(
            "SELECT request FROM gw_calls WHERE purpose = 'architect-draft' AND status = 'ok' "
            "ORDER BY ts, call_id"
        ).fetchall()
    assert len(drafts) == 2
    feedback = drafts[1]["request"]["messages"][-1]["content"]
    assert "INVALID_MODEL_RESULT" in feedback and "kind" in feedback
    assert [t for t, _ in payloads(pool) if t == "model.patch_proposed"].count(
        "model.patch_proposed"
    ) == 2
    assert "architect-could-not-produce-valid-patch:draft:1" not in final["open_risk_ids"]
    proposals = messages_of(pool, PROJECT, SESSION_ID, "ModelPatchProposal")
    assert len(proposals) == 3, "both draft attempts and the repair were recorded as messages"


def test_s3_exceeding_the_retry_bound_is_an_open_risk_not_a_crash(pool, tmp_path):
    create_project(pool, PROJECT)
    architect_ = ScriptedArchitect(S3_GIVE_UP_STORY)
    activities = make_activities(pool, make_gateway(pool, architect_), tmp_path / "objects")

    async def body() -> dict[str, Any]:
        async with await time_skipping() as env:
            return await run_to_end(env.client, activities, session_input(PROJECT))

    final = run(body())
    assert final["status"] == "approved" and final["outcome"] == "completed_with_risks"
    assert architect_.served[("draft", 1)] == 4, "one attempt plus three retries"
    assert "architect-could-not-produce-valid-patch:draft:1" in final["open_risk_ids"]
    assert [t for t, _ in payloads(pool) if t == "model.patch_committed"] == []
    genesis = [p["version_id"] for t, p in payloads(pool) if t == "model.version_created"][0]
    assert final["best_version"] == genesis
    package = load_package(activities._store, final["package_key"])
    assert package["gate"]["verdict"] == "BLOCKED"
    assert [r["check_id"] for r in package["gate"]["reasons"]] == ["C-001"]
    risk = next(r for r in package["open_risks"] if r["id"].startswith("architect-could-not"))
    assert risk["attempts"] == 4 and risk["detail"]["code"] == "INVALID_MODEL_RESULT"


# --- S4: budget -------------------------------------------------------------------------------


def test_s4_a_tight_token_cap_stops_the_session_with_the_best_so_far(pool, tmp_path):
    create_project(pool, PROJECT)
    architect_ = ScriptedArchitect(S1_STORY)
    gateway = make_gateway(pool, architect_)
    activities = make_activities(pool, gateway, tmp_path / "objects")

    async def body() -> dict[str, Any]:
        async with await time_skipping() as env:
            # frame and draft fit (1500 tokens each, 512 reserved per call), the repair's
            # reservation does not
            return await run_to_end(
                env.client, activities, session_input(PROJECT, overrides={"tokens": 4000})
            )

    final = run(body())
    assert final["status"] == "stopped_budget" and final["outcome"] == "stopped_budget"
    assert architect_.served == {("frame", 0): 1, ("draft", 1): 1}
    draft_version = [p["version_id"] for t, p in payloads(pool) if t == "model.patch_committed"][0]
    assert final["best_version"] == draft_version
    assert "budget-exhausted" in final["open_risk_ids"]
    with pool.connection() as conn:
        statuses = [
            r["status"]
            for r in conn.execute("SELECT status FROM gw_calls ORDER BY ts, call_id").fetchall()
        ]
    assert statuses == ["ok", "ok", "budget_refused"]
    checkpoints = [p for t, p in payloads(pool) if t == "session.checkpoint"]
    spent = gateway.spend({"session": SESSION_ID})
    assert checkpoints[-1]["spend"] == {"tokens": spent["tokens"], "usd": spent["usd"]}
    assert checkpoints[-1]["spend"]["tokens"] == 3000
    package = load_package(activities._store, final["package_key"])
    assert package["stop_reason"] == "budget" and package["best_version"] == draft_version
    assert package["gate"]["verdict"] == "BLOCKED"
    phases = [p["to"] for t, p in payloads(pool) if t == "session.phase_changed"]
    assert phases[-2:] == ["converge", "package"] and "repair" in phases


def test_s4_usd_null_is_uncapped(pool, tmp_path):
    create_project(pool, PROJECT)
    architect_ = ScriptedArchitect(S1_STORY)
    gateway = make_gateway(pool, architect_)
    activities = make_activities(pool, gateway, tmp_path / "objects")

    async def body() -> dict[str, Any]:
        async with await time_skipping() as env:
            return await run_to_end(
                env.client, activities, session_input(PROJECT, preset="exhaustive")
            )

    final = run(body())
    assert final["status"] == "approved"
    limits = gateway.limits({"session": SESSION_ID})
    assert list(limits.values()) == [{"tokens": 25000000, "usd": None, "wall_clock_minutes": 1440}]
    assert gateway.spend({"session": SESSION_ID})["usd"] > 0


# --- S5: wall clock ---------------------------------------------------------------------------


def test_s5_the_wall_clock_stops_the_session_with_a_package(pool, tmp_path):
    create_project(pool, PROJECT)
    architect_ = ScriptedArchitect(S1_STORY)
    activities = make_activities(pool, make_gateway(pool, architect_), tmp_path / "objects")

    async def body() -> dict[str, Any]:
        async with await time_skipping() as env:
            async with build_worker(env.client, task_queue=TASK_QUEUE, activities=activities):
                handle = await start(env.client, session_input(PROJECT), TASK_QUEUE)
                await handle.signal("pause")
                await wait_for(handle, lambda v: v["status"] == "paused")
                await env.sleep(timedelta(minutes=361))
                await handle.signal("resume")
                await wait_for(
                    handle, lambda v: v["status"] != "paused" and v["status"] != "running"
                )
                return await result_of(handle)

    final = run(body())
    assert final["status"] == "stopped_time" and final["outcome"] == "stopped_time"
    assert "wall-clock-exhausted" in final["open_risk_ids"] and final["package_key"]
    package = load_package(activities._store, final["package_key"])
    assert package["stop_reason"] == "time"
    phases = [p["to"] for t, p in payloads(pool) if t == "session.phase_changed"]
    assert phases[-2:] == ["converge", "package"]


# --- S7: signals ------------------------------------------------------------------------------


def test_s7_pause_resume_and_reject(pool, tmp_path):
    create_project(pool, PROJECT)
    architect_ = ScriptedArchitect(S1_STORY)
    activities = make_activities(pool, make_gateway(pool, architect_), tmp_path / "objects")
    gate = ActivityGate(activities)
    first = gate.hold("session_start")
    paused_recorded = gate.on_finish("record_status", status="paused")
    awaiting = gate.on_finish("record_status", status="awaiting_approval")

    async def body() -> dict[str, Any]:
        async with await time_skipping() as env:
            async with build_worker(env.client, task_queue=TASK_QUEUE, activities=activities):
                try:
                    handle = await start(env.client, session_input(PROJECT), TASK_QUEUE)
                    # Held inside its very first activity, the session has passed no step
                    # boundary yet: the pause is in its history before the first one.
                    await first.wait()
                    await handle.signal("pause")
                    first.release()
                    await paused_recorded.wait()
                    paused = await handle.query("status")
                    assert (paused["status"], paused["phase"]) == ("paused", "frame")
                    assert paused["active"] is False
                    assert session_row(pool, PROJECT, SESSION_ID)["status"] == "paused"
                    # Pause stops progress. Another signal makes the workflow run again; the
                    # query after it sees the state it settled in: still paused, same phase,
                    # no step started.
                    before = gate.names()
                    await handle.signal("pause")
                    still = await handle.query("status")
                    assert (still["status"], still["phase"]) == ("paused", paused["phase"])
                    assert still["active"] is False
                    assert gate.names() == before and "frame" not in before
                    await handle.signal("resume")
                    await awaiting.wait()
                    view = await handle.query("status")
                    assert view["status"] == "awaiting_approval" and view["phase"] == "package"
                    assert "frame" in gate.names(), "resume continued"
                    await handle.signal("reject")
                    return await result_of(handle)
                finally:
                    gate.release_all()

    final = run(body())
    assert final["status"] == "rejected" and final["outcome"] == "completed"
    assert session_row(pool, PROJECT, SESSION_ID)["status"] == "rejected"


def test_s7_steer_enters_the_next_context_and_cancel_packages(pool, tmp_path):
    create_project(pool, PROJECT)
    architect_ = ScriptedArchitect(S1_STORY)
    activities = make_activities(pool, make_gateway(pool, architect_), tmp_path / "objects")
    gate = ActivityGate(activities)
    first = gate.hold("session_start")
    paused_before_frame = gate.on_finish("record_status", status="paused")
    attack = gate.hold("attack", phase="attack")
    paused_before_repair = gate.on_finish("record_status", status="paused")

    async def body() -> dict[str, Any]:
        async with await time_skipping() as env:
            async with build_worker(env.client, task_queue=TASK_QUEUE, activities=activities):
                try:
                    handle = await start(env.client, session_input(PROJECT), TASK_QUEUE)
                    # Held in its first activity: pause and steer are both in the history
                    # before the first step boundary, where the steer is recorded and the
                    # session then pauses.
                    await first.wait()
                    await handle.signal("pause")
                    await handle.signal(
                        "steer", "Prefer at-least-once delivery with idempotent writes."
                    )
                    first.release()
                    await paused_before_frame.wait()
                    paused = await handle.query("status")
                    assert (paused["status"], paused["phase"]) == ("paused", "frame")
                    assert len(paused["steer_claims"]) == 1 and paused["pending_steers"] == 0
                    await handle.signal("resume")
                    # Held inside the attack on the draft: the draft exists, the repair has
                    # not been asked for. Pause, then cancel at the boundary before repair.
                    await attack.wait()
                    assert architect_.served == {("frame", 0): 1, ("draft", 1): 1}
                    await handle.signal("pause")
                    attack.release()
                    await paused_before_repair.wait()
                    again = await handle.query("status")
                    assert (again["status"], again["phase"]) == ("paused", "repair")
                    await handle.signal("cancel")
                    return await result_of(handle)
                finally:
                    gate.release_all()

    final = run(body())
    assert final["status"] == "cancelled" and final["outcome"] == "cancelled"
    assert final["package_key"] and final["stop_reason"] == "cancel"
    assert ("repair", 1) not in architect_.served, "cancel took effect at the next boundary"
    (steer_claim,) = final["steer_claims"]
    committed = {p["claim_id"]: p["claim"] for t, p in payloads(pool) if t == "claim.committed"}
    assert committed[steer_claim]["subject"] == {"entity_type": "owner_guidance", "id": "steer-1"}
    assert committed[steer_claim]["taint"] == {"origin": "user"}
    assert committed[steer_claim]["object"]["literal"].startswith("Prefer at-least-once")
    # every architect call compiled after the steer carried it in its context
    tasks = messages_of(pool, PROJECT, SESSION_ID, "Task")
    assert tasks and all(steer_claim in m["context_manifest"] for m in tasks)
    assert "Prefer at-least-once" in json.dumps(
        [c["request"] for c in gateway_calls(pool) if c["purpose"] == "architect-draft"]
    )
    package = load_package(activities._store, final["package_key"])
    assert package["outcome"] == "cancelled" and package["best_version"]
    assert [p["to"] for t, p in payloads(pool) if t == "session.phase_changed"][-2:] == [
        "converge",
        "package",
    ]


def gateway_calls(pool) -> list[dict[str, Any]]:
    with pool.connection() as conn:
        return conn.execute(
            "SELECT purpose, status, request FROM gw_calls ORDER BY ts, call_id"
        ).fetchall()


def test_s7_a_steer_after_the_last_step_boundary_is_still_recorded(pool, tmp_path):
    """Pins the race deterministically: the steer arrives while the LAST step of the loop is
    running, so no step boundary follows it; a second one arrives while the session waits
    for the owner. Neither may be lost."""
    create_project(pool, PROJECT)
    architect_ = ScriptedArchitect(S1_STORY)
    activities = make_activities(pool, make_gateway(pool, architect_), tmp_path / "objects")
    gate = ActivityGate(activities)
    verify = gate.hold("attack", phase="verify")
    awaiting = gate.on_finish("record_status", status="awaiting_approval")
    second = gate.on_finish("steer", n=2)

    async def body() -> tuple[dict[str, Any], dict[str, Any]]:
        async with await time_skipping() as env:
            async with build_worker(env.client, task_queue=TASK_QUEUE, activities=activities):
                try:
                    handle = await start(env.client, session_input(PROJECT), TASK_QUEUE)
                    await verify.wait()  # held inside the last step: no gate comes after it
                    await handle.signal("steer", "Keep the queue's retention at seven days.")
                    verify.release()
                    await awaiting.wait()
                    waiting = await handle.query("status")
                    await handle.signal("steer", "Name an owner for the datastore runbook.")
                    await second.wait()
                    await handle.signal("approve")
                    return waiting, await result_of(handle)
                finally:
                    gate.release_all()

    waiting, final = run(body())
    assert waiting["status"] == "awaiting_approval"
    assert len(waiting["steer_claims"]) == 1, "the steer sent during the last step was lost"
    assert waiting["pending_steers"] == 0
    assert final["status"] == "approved" and len(final["steer_claims"]) == 2
    events = events_of(pool, PROJECT)
    committed = {e["payload"]["claim_id"]: e for e in events if e["type"] == "claim.committed"}
    first, later = (committed[claim_id] for claim_id in final["steer_claims"])
    assert first["payload"]["claim"]["object"]["literal"].startswith("Keep the queue")
    assert later["payload"]["claim"]["object"]["literal"].startswith("Name an owner")
    converge = next(
        e["seq"]
        for e in events
        if e["type"] == "session.phase_changed" and e["payload"]["to"] == "converge"
    )
    assert first["seq"] < converge, "recorded at the loop's exit, before converge"
    assert later["seq"] > converge
    package = load_package(activities._store, final["package_key"])
    assert package["steer_claims"] == [final["steer_claims"][0]]
    assert len(events_of(pool, PROJECT, "source.ingested")) == 3, "the brief and two steers"


# --- S8: replay -------------------------------------------------------------------------------


def _content(pool, project_id: str) -> list[dict[str, Any]]:
    """Events without the stamps that differ by construction (event_id, ts, prev_hash). A
    checkpoint's spend is metering, not content: the replay gateway charges nothing (M4, G6),
    so a replayed checkpoint reports the recorded run's total rather than the running sum."""
    out = []
    for event in events_of(pool, project_id):
        content = {k: v for k, v in event.items() if k not in ("event_id", "ts", "prev_hash")}
        if event["type"] == "session.checkpoint":
            content["payload"] = {k: v for k, v in event["payload"].items() if k != "spend"}
        out.append(content)
    return out


def test_s8_a_recorded_session_replays_to_a_content_identical_ledger(pool, admin, tmp_path):
    """The session is re-run into a fresh ledger (a second schema, same project id) with the
    gateway in replay mode reading the first run's call log; the provider is rigged to fail."""
    import uuid

    from psycopg import sql
    from psycopg.conninfo import make_conninfo

    from architect.db import database_url, ensure_schema, open_pool

    create_project(pool, PROJECT)
    live = make_activities(pool, make_gateway(pool, ScriptedArchitect(S1_STORY)), tmp_path / "o1")
    rigged = ScriptedArchitect(S1_STORY)
    rigged.fail_if_called = True
    schema = sql.Identifier(f"t_{uuid.uuid4().hex}")
    admin.execute(sql.SQL("CREATE SCHEMA {}").format(schema))
    fresh = open_pool(
        make_conninfo(database_url(), options=f"-c search_path={schema.as_string()}"), max_size=4
    )
    try:
        ensure_schema(fresh)
        create_project(fresh, PROJECT)
        # the replay gateway answers from the first run's gw_calls; everything else is fresh
        replay = make_activities(fresh, make_gateway(pool, rigged, mode="replay"), tmp_path / "o2")

        async def body() -> tuple[dict[str, Any], dict[str, Any]]:
            async with await time_skipping() as env:
                first = await run_to_end(env.client, live, session_input(PROJECT))
                second = await run_to_end(env.client, replay, session_input(PROJECT))
                return first, second

        first, second = run(body())
        assert first["status"] == second["status"] == "approved"
        assert rigged.call_count == 0
        assert _content(fresh, PROJECT) == _content(pool, PROJECT)
        assert first["best_version"] == second["best_version"]
        assert first["spend"] == second["spend"], "replay reports the recorded spend"
        assert ledger.verify_chain(fresh, PROJECT)[1] == []
        with pool.connection() as conn:
            statuses = [r["status"] for r in conn.execute("SELECT status FROM gw_calls").fetchall()]
        assert statuses.count("replay") == statuses.count("ok") == 3
    finally:
        fresh.close()
        admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(schema))


# --- S9: the Context Compiler -----------------------------------------------------------------


def _commit(pool, project_id: str, event_type: str, payload: dict[str, Any], **extra: Any) -> None:
    Arbiter(pool).submit(project_id, candidate(event_type, payload, **extra))


def test_s9_the_compiler_packs_to_the_target_and_never_includes_quarantined_facts(pool):
    create_project(pool, PROJECT)
    _commit(
        pool,
        PROJECT,
        "source.ingested",
        {
            "source_id": ident("src", "paper"),
            "uri": "https://example.org/paper.pdf",
            "content_hash": "sha256:paper",
            "media_type": "application/pdf",
            "taint_origin": "external_untrusted",
        },
    )
    _commit(
        pool,
        PROJECT,
        "source.ingested",
        {
            "source_id": ident("src", "notes"),
            "uri": "file:///notes.md",
            "content_hash": "sha256:notes",
            "media_type": "text/markdown",
            "taint_origin": "user",
        },
    )
    requirement = claim(
        "req",
        subject={"entity_type": "requirement", "id": "req_p99"},
        predicate="CONSTRAINS",
        object={"entity_type": "metric", "id": "latency"},
        magnitude={"value": 200, "unit": "ms"},
        taint={"origin": "user"},
        evidence=[{"source": ident("src", "notes"), "kind": "statement"}],
    )
    _commit(pool, PROJECT, "claim.committed", {"claim_id": requirement["id"], "claim": requirement})
    untrusted = claim(
        "lease",
        subject={"entity_type": "technique", "id": "lease-fencing"},
        predicate="REDUCES",
        object={"entity_type": "metric", "id": "latency"},
        evidence=[{"source": ident("src", "paper"), "span": "p.4 ¶2", "kind": "benchmark"}],
    )
    _commit(pool, PROJECT, "claim.committed", {"claim_id": untrusted["id"], "claim": untrusted})
    fillers = []
    for n in range(12):
        filler = claim(
            f"fill{n}",
            subject={"entity_type": "technique", "id": f"lease-renewal-{n}"},
            predicate="IMPROVES",
            object={"entity_type": "metric", "id": "latency"},
            taint={"origin": "user"},
            evidence=[{"source": ident("src", "notes"), "kind": "statement"}],
        )
        fillers.append(filler["id"])
        _commit(pool, PROJECT, "claim.committed", {"claim_id": filler["id"], "claim": filler})
    confident = claim(
        "quarantined",
        subject={"entity_type": "technique", "id": "lease-fencing"},
        predicate="ELIMINATES",
        object={"entity_type": "metric", "id": "latency"},
        confidence={"value": 0.99, "inputs": {"source_tier": "external_untrusted"}},
    )
    _commit(pool, PROJECT, "claim.proposed", {"proposal_id": "prop-q", "claim": confident})
    catch_up(pool)

    ranked = rank_claims(pool, PROJECT, "lease fencing latency")
    assert ranked[0]["claim_id"] == untrusted["id"], "best vocabulary overlap first"
    assert confident["id"] not in {r["claim_id"] for r in ranked}
    assert requirement["id"] not in {r["claim_id"] for r in ranked}

    compiled = compile(
        pool,
        CompileTask(
            project_id=PROJECT,
            session_id=SESSION_ID,
            goal="draft",
            phase="draft",
            round=1,
            token_target=240,
            research_claim_ids=[r["claim_id"] for r in ranked],
            remaining_budget={"tokens": "more than 90%"},
        ),
    )
    assert compiled.tokens <= 240
    assert requirement["id"] in compiled.manifest and untrusted["id"] in compiled.manifest
    dropped = {d["id"]: d["reason"] for d in compiled.dropped}
    assert dropped[confident["id"]] == "quarantined"
    assert confident["id"] not in compiled.manifest and "ELIMINATES" not in compiled.text
    assert [reason for cid, reason in dropped.items() if cid in fillers] and all(
        reason == "token_target" for cid, reason in dropped.items() if cid in fillers
    )
    assert any(cid in compiled.manifest for cid in fillers), "some fit before the target"
    assert f"<<<UNTRUSTED-DATA source={untrusted['id']}>>>" in compiled.text
    assert compiled.input_taints == ["external_untrusted", "user"]
    request = agent.architect_request(
        purpose="draft",
        round_=1,
        session_id=SESSION_ID,
        phase="draft",
        tier="tier-frontier",
        max_tokens=512,
        messages=[{"role": "user", "content": compiled.text}],
        input_taints=compiled.input_taints,
    )
    assert with_rule(request.system, request.input_taints).startswith(UNTRUSTED_RULE)
    assert "confidence" not in compiled.text


# --- S10: the linter --------------------------------------------------------------------------


def test_s10_the_linter_separates_measurable_from_unmeasurable_requirements():
    vague = linter.lint({"slug": "fast", "text": "must be fast"})
    assert not vague.measurable and vague.requirement_id == "req_fast"
    assert vague.reason.startswith("unmeasurable")
    sharp = linter.lint({"slug": "p99-latency", "text": "p99 < 200 ms"})
    assert sharp.measurable and (sharp.metric, sharp.target, sharp.unit) == ("latency", 200.0, "ms")
    given = linter.lint(
        {
            "slug": "a",
            "text": "ninety-nine nines",
            "metric": "availability",
            "target": 99.9,
            "unit": "%",
        }
    )
    assert given.measurable and given.unit == "%"
    unknown_unit = linter.lint(
        {
            "slug": "u",
            "text": "weigh under 3 furlongs",
            "metric": "mass",
            "target": 3,
            "unit": "furlongs",
        }
    )
    assert not unknown_unit.measurable and "furlongs" in unknown_unit.reason
    no_unit = linter.lint(
        {"slug": "n", "text": "handle 5000 requests", "metric": "throughput", "target": 5000}
    )
    assert not no_unit.measurable and "no unit" in no_unit.reason


# --- S12: the determinism guard ---------------------------------------------------------------

WORKFLOW_MODULES = ("workflow.py", "types.py")
FORBIDDEN_IN_WORKFLOWS = (
    "architect.db",
    "architect.arbiter",
    "architect.gateway",
    "architect.ledger",
    "architect.projector",
    "architect.readmodel",
    "architect.checks",
    "architect.ingestion",
    "architect.sessions.activities",
    "architect.sessions.service",
    "architect.sessions.worker",
    "psycopg",
    "psycopg_pool",
    "httpx",
    "httpx2",
    "time",
    "random",
    "os",
    "subprocess",
    "socket",
    "urllib",
    "anthropic",
    "uuid",
    "ulid",
    "secrets",
)


def test_s12_workflow_modules_import_nothing_impure():
    package = Path(architect.__file__).parent / "sessions"
    for name in WORKFLOW_MODULES:
        tree = ast.parse((package / name).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or ""]
            else:
                continue
            for imported in names:
                for forbidden in FORBIDDEN_IN_WORKFLOWS:
                    assert not (imported == forbidden or imported.startswith(forbidden + ".")), (
                        f"sessions/{name} imports {imported}"
                    )
    text = (package / "workflow.py").read_text(encoding="utf-8")
    assert "datetime.now(" not in text and "import random" not in text


# --- the CLI and the API ----------------------------------------------------------------------


def _wait_cli(dsn: str, session_id: str, wanted: str, capsys) -> dict[str, Any]:
    import time

    from architect.cli import main

    for _ in range(600):
        assert main(["--database-url", dsn, "session", "status", "--session", session_id]) == 0
        view = json.loads(capsys.readouterr().out)
        if view["status"] == wanted:
            return view
        assert view["status"] != "failed", view
        time.sleep(0.2)
    raise AssertionError(f"never reached {wanted}: {view}")


def test_session_cli(dsn, pool, tmp_path, monkeypatch, capsys):
    from architect.cli import main

    create_project(pool, PROJECT)
    monkeypatch.setenv("ARCHITECT_OBJECT_STORE", str(tmp_path / "objects"))
    activities = make_activities(
        pool, make_gateway(pool, ScriptedArchitect(S1_STORY)), tmp_path / "objects"
    )
    brief_path = tmp_path / "brief.md"
    brief_path.write_text(BRIEF, encoding="utf-8")
    with background_worker(activities) as address:
        monkeypatch.setenv("ARCHITECT_TEMPORAL_ADDRESS", address)
        code = main(
            [
                "--database-url",
                dsn,
                "session",
                "--task-queue",
                TASK_QUEUE,
                "start",
                "--project",
                PROJECT,
                "--brief",
                str(brief_path),
                "--preset",
                "quick",
                "--override",
                "usd=null",
                "--override",
                "max_rounds=2",
            ]
        )
        started = json.loads(capsys.readouterr().out)
        assert code == 0 and started["session_id"].startswith("ses_")
        assert started["limits"]["usd"] is None and started["limits"]["max_rounds"] == 2
        session_id = started["session_id"]
        _wait_cli(dsn, session_id, "awaiting_approval", capsys)
        assert main(["--database-url", dsn, "session", "approve", "--session", session_id]) == 0
        assert "approve sent" in capsys.readouterr().out
        _wait_cli(dsn, session_id, "approved", capsys)
        assert (
            main(
                [
                    "--database-url",
                    dsn,
                    "session",
                    "show",
                    "--project",
                    PROJECT,
                    "--session",
                    session_id,
                ]
            )
            == 0
        )
        shown = capsys.readouterr().out
    assert f"session {session_id}: approved" in shown and "-> frame" in shown
    assert "gate: ALLOWED" in shown and "package: " in shown and "round 1:" in shown
    assert f"requirement-unmeasurable:{REQ_NO_LOSS}" in shown
    assert (
        main(
            [
                "--database-url",
                dsn,
                "session",
                "show",
                "--project",
                PROJECT,
                "--session",
                "ses_NOSUCH0001",
            ]
        )
        == 1
    )


def test_session_endpoints(dsn, pool, tmp_path, monkeypatch):
    import time

    from fastapi.testclient import TestClient

    from architect.api import create_app
    from architect.ingestion.objectstore import LocalObjectStore

    activities = make_activities(
        pool, make_gateway(pool, ScriptedArchitect(S1_STORY)), tmp_path / "objects"
    )
    monkeypatch.setenv("ARCHITECT_TEMPORAL_TASK_QUEUE", TASK_QUEUE)
    with (
        background_worker(activities) as address,
        TestClient(create_app(dsn, temporal_address=address)) as client,
    ):
        assert client.post("/v1/projects", json={"project_id": PROJECT}).status_code == 201
        client.app.state.object_store = LocalObjectStore(tmp_path / "objects")
        base = f"/v1/projects/{PROJECT}/sessions"
        created = client.post(base, json={"brief": BRIEF, "preset": "quick"})
        assert created.status_code == 201, created.text
        session_id = created.json()["session_id"]
        assert client.post(base, json={"brief": BRIEF, "preset": "leisurely"}).status_code == 422

        def wait(wanted: str) -> dict[str, Any]:
            for _ in range(600):
                response = client.get(f"{base}/{session_id}")
                if response.status_code == 404:  # the row appears once session_start ran
                    time.sleep(0.2)
                    continue
                detail = response.json()
                if detail["session"]["status"] == wanted:
                    return detail
                assert detail["session"]["status"] != "failed", detail
                time.sleep(0.2)
            raise AssertionError(f"never reached {wanted}")

        wait("awaiting_approval")
        assert client.post(f"{base}/{session_id}/steer").status_code == 422
        assert client.post(f"{base}/{session_id}/dance").status_code == 422
        missing = client.post(f"{base}/ses_NOSUCH0001/approve")
        assert missing.status_code == 404 and missing.json()["code"] == "SESSION_NOT_FOUND"
        approved = client.post(f"{base}/{session_id}/approve")
        assert approved.status_code == 200 and approved.json()["signal"] == "approve"
        detail = wait("approved")
        assert detail["gate"]["verdict"] == "ALLOWED" and detail["package_key"]
        assert [s["phase"] for s in detail["timeline"] if s["kind"] == "phase_changed"][
            -1
        ] == "package"
        listed = client.get(base).json()["sessions"]
        assert [s["session_id"] for s in listed] == [session_id]
        assert client.get(f"{base}/ses_NOSUCH0001").status_code == 404
