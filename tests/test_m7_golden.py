"""M7 Part D: golden task gt-001 and the golden runner, in mock mode (exit tests P5 and P6).

The runner drives a real session: a real Temporal dev server (the one CI starts, else one the
SDK starts) and the worker in a process of its own, with the scripted architect of the task.
No model is called."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from architect import ledger
from architect.checks.catalog import evaluate, load_catalog
from architect.checks.context import CheckContext, index_elements
from architect.cli import main
from architect.contracts import first_error, load_contracts
from architect.golden import scorecard as scoring
from architect.golden.runner import GoldenError, default_out, phase_completed, run_golden
from architect.golden.scripted import MARKER, ScriptedProvider
from architect.golden.tasks import load_task
from architect.golden.worker import EXIT_AFTER_KILL
from architect.sessions import linter
from architect.sessions.workflow import ACTIVITY_TIMEOUT
from session_fixtures import events_of

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")

FLAW_CHECKS = {"C-012", "C-008", "C-007", "C-005"}


def golden(dsn: str, tmp_path: Path, **options: Any) -> dict[str, Any]:
    return asyncio.run(
        run_golden(
            "gt-001",
            dsn=dsn,
            store_dir=tmp_path / "objects",
            out=tmp_path / "scorecard.json",
            **options,
        )
    )


# --- the task itself: no database, no session ------------------------------------------------


def test_p5_the_seed_has_exactly_the_four_planted_flaws_and_is_clean_otherwise():
    task = load_task("gt-001")
    expected = task.expected
    assert first_error(load_contracts().model, task.seed) is None, "the seed is schema-valid"
    requirements = [
        linter.requirement_id(label) for label in linter.labelled_requirements(task.brief)
    ]
    ctx = CheckContext(
        as_of_seq=0, requirements=tuple(requirements), elements=index_elements(task.seed)
    )
    outcomes = {e["id"]: evaluate(e, task.seed, ctx) for e in load_catalog().checks}
    failing = {
        check_id: outcome.element_refs
        for check_id, outcome in outcomes.items()
        if outcome.status != "pass" and outcome.status != "skipped"
    }
    planted = {f["check"]: [f["element"]] for f in expected["planted_flaws"]}
    assert failing == planted and set(planted) == FLAW_CHECKS
    assert all(outcomes[c].status == "fail" for c in FLAW_CHECKS), "fail, never error"
    assert [f["id"] for f in expected["planted_flaws"]] == ["F1", "F2", "F3", "F4"]
    store = outcomes["C-005"].evidence["components"]["cmp_STORE000001"]
    assert (store["capacity"], store["required"]) == (2000.0, 2250.0)
    assert sorted(set(outcomes) - FLAW_CHECKS) == expected["clean_checks"]
    assert {outcomes[c].status for c in expected["clean_checks"]} <= {"pass", "skipped"}


def test_p5_the_brief_has_four_measurable_requirements_and_one_that_is_not():
    task = load_task("gt-001")
    labels = linter.labelled_requirements(task.brief)
    results = {
        linter.requirement_id(label): linter.lint({"slug": label, "text": text})
        for label, text in labels.items()
    }
    measurable = sorted(rid for rid, r in results.items() if r.measurable)
    assert measurable == sorted(task.expected["linter"]["measurable"]) and len(measurable) == 4
    assert [rid for rid, r in results.items() if not r.measurable] == ["req_robust"]
    assert task.expected["linter"]["unmeasurable"] == ["req_robust"]
    assert (results["req_peak-ingest"].target, results["req_peak-ingest"].unit) == (1500.0, "req/s")
    assert (results["req_ingest-latency"].target, results["req_ingest-latency"].unit) == (
        300.0,
        "ms",
    )
    # the brief's own labels decide the ids, whatever slug the model chose
    paraphrased = {
        "slug": "throughput-target",
        "text": "1500 req/s",
        "quote": "peak ingest of 1500 req/s",
    }
    assert linter.label_for(paraphrased, labels) == "peak-ingest"
    assert linter.label_for({"slug": "robust", "text": "anything"}, labels) == "robust"
    assert linter.label_for({"slug": "new", "text": "an unrelated requirement"}, labels) is None
    assert linter.label_for(paraphrased, {}) is None
    # every satisfies link and requirement_ref of the seed names a labelled requirement
    known = set(results)
    assert {link["requirement"] for link in task.seed["links"]["satisfies"]} == known
    for component in task.seed["elements"]["components"]:
        assert set(component["requirement_refs"]) <= known


def test_the_scripted_architect_answers_by_purpose_and_round():
    task = load_task("gt-001")
    provider = ScriptedProvider(task.story("review"))
    assert sorted(task.story("review")) == [("frame", 0), ("repair", 1)]
    assert sorted(task.story("design")) == [("draft", 1), ("frame", 0)]

    class Call:
        model, system = "scripted-architect", "[architect purpose=repair round=1]\nfix it"

    assert MARKER.search(Call.system).groups() == ("repair", "1")
    answer = json.loads(provider.complete(Call).text)
    assert [op["element_id"] for op in answer["ops"]] == [
        "flw_ENQUEUE0001", "flw_INGEST00001", "cmp_WORKERS0001", "cap-store-max-qps",
    ]  # fmt: skip
    Call.system = "[architect purpose=draft round=1]"
    with pytest.raises(Exception, match="no output for"):
        provider.complete(Call)


# --- P5: the golden run, mock ----------------------------------------------------------------


def test_p5_review_mode_catches_the_four_flaws_in_round_one_and_the_repairs_pass(
    dsn, pool, tmp_path
):
    card = golden(dsn, tmp_path, mode="review")
    scoring.validate(card)
    assert json.loads((tmp_path / "scorecard.json").read_text(encoding="utf-8")) == card
    assert (card["task"], card["mode"], card["provider"]) == ("gt-001", "review", "mock")

    flaws = {f["id"]: f for f in card["planted_flaws"]}
    assert {f["check"] for f in flaws.values()} == FLAW_CHECKS and len(flaws) == 4
    assert all(f["caught"] and f["caught_in_round"] == 1 for f in flaws.values())
    assert all(f["repaired"] and f["repaired_in_round"] == 1 for f in flaws.values())
    assert (card["flaws_caught"], card["flaws_repaired"]) == (4, 4)
    assert card["unexpected_failures_on_seed"] == [], "every other check passes or is skipped"

    assert card["gate"] == {"verdict": "ALLOWED", "reasons": []}
    assert (card["outcome"], card["status_at_gate"]) == ("completed", "awaiting_approval")
    assert (card["closed_with"], card["final_status"]) == ("approve", "approved")
    assert card["rounds"] == 1 and card["final_model_version"] and card["package_key"]
    coverage = card["requirement_coverage"]
    assert coverage["status"] == "pass" and coverage["uncovered"] == []
    assert len(coverage["requirements"]) == 5
    assert card["linter"] == {
        "risks_found": ["requirement-unmeasurable:req_robust"],
        "expected_risks": ["requirement-unmeasurable:req_robust"],
        "expected_found": True,
    }
    assert card["kill_resume"]["performed"] is False and card["kill_resume"]["clean"] is None
    assert card["tokens"] == 3000 and 0 < card["usd"] <= card["usd_cap"] == 3.0
    assert card["criteria"] == {
        "all_planted_flaws_caught_by_round_1": True,
        "final_model_exists": True,
        "package_exists": True,
        "under_usd_cap": True,
    }
    assert card["passed"] is True

    # the session was a review: the seed went through the Arbiter and no draft was asked for
    project = card["project_id"]
    assert project.startswith("golden-gt-001-review-")
    phases = [e["payload"]["to"] for e in events_of(pool, project, "session.phase_changed")]
    assert "draft" not in phases and phases[:4] == ["frame", "research", "model", "attack"]
    on_seed = {
        e["payload"]["check_id"]: e["payload"]["status"]
        for e in events_of(pool, project, "check.result")
        if e["payload"]["model_version"]
        == events_of(pool, project, "model.patch_committed")[0]["payload"]["version_id"]
    }
    assert {c for c, s in on_seed.items() if s == "fail"} == FLAW_CHECKS
    assert {s for c, s in on_seed.items() if c not in FLAW_CHECKS} <= {"pass", "skipped"}
    budget = events_of(pool, project, "budget.updated")[0]["payload"]["limits"]
    assert budget["usd"] == 3.0, "the task's usd cap is the session's"
    assert ledger.verify_chain(pool, project)[1] == []


def test_p5_the_scorecard_schema_refuses_a_malformed_scorecard(dsn, tmp_path):
    card = golden(dsn, tmp_path, mode="design")
    scoring.validate(card)
    assert (card["mode"], card["planted_flaws"], card["flaws_caught"]) == ("design", [], 0)
    assert card["criteria"] == {
        "model_produced": True,
        "requirements_covered_or_listed_as_risks": True,
        "package_exists": True,
        "under_usd_cap": True,
    }
    assert card["passed"] and card["gate"]["verdict"] == "ALLOWED" and card["rounds"] == 1
    assert card["requirement_coverage"]["status"] == "pass"
    for broken in (
        {k: v for k, v in card.items() if k != "gate"},
        card | {"mode": "audit"},
        card | {"usd": -1},
        card | {"api_key": "must never be here"},
        card | {"planted_flaws": [{"id": "F1"}]},
    ):
        with pytest.raises(ValueError, match="scorecard invalid"):
            scoring.validate(broken)
    with pytest.raises(ValueError, match="scorecard invalid"):
        scoring.write(card | {"passed": "yes"}, tmp_path / "never-written.json")
    assert not (tmp_path / "never-written.json").exists()


# --- P6: kill and resume ---------------------------------------------------------------------


def test_p6_killing_the_worker_after_attack_resumes_cleanly(dsn, pool, tmp_path):
    card = golden(dsn, tmp_path, mode="review", kill_after="attack")
    scoring.validate(card)
    assert card["kill_resume"] == {
        "performed": True,
        "after": "attack",
        "clean": True,
        "worker_exit_code": 7,
        "duplicate_idempotency_keys": 0,
        "seq_dense": True,
    }
    assert card["criteria"]["kill_resume_clean"] is True and card["passed"] is True
    assert (card["flaws_caught"], card["flaws_repaired"]) == (4, 4)
    assert all(f["caught_in_round"] == 1 for f in card["planted_flaws"])
    assert card["gate"]["verdict"] == "ALLOWED" and card["final_status"] == "approved"
    events = events_of(pool, card["project_id"])
    assert [e["seq"] for e in events] == list(range(len(events)))
    assert len({e["idempotency_key"] for e in events}) == len(events)
    assert len([e for e in events if e["type"] == "model.patch_committed"]) == 2
    phases = [e["payload"]["to"] for e in events if e["type"] == "session.phase_changed"]
    assert phases == [
        "frame",
        "research",
        "model",
        "attack",
        "repair",
        "verify",
        "converge",
        "package",
    ]


def test_p6_a_worker_killed_from_outside_in_the_middle_of_an_activity_is_resumed_cleanly(
    dsn, pool, tmp_path
):
    """The real kill: the runner terminates the worker PROCESS from outside once the session
    has left the attack phase, while the worker is in the middle of the next activity. The
    worker takes no part in it. A new worker finishes the session from its history."""
    card = golden(
        dsn, tmp_path, mode="review", kill_after="attack", kill_mode="external", heartbeat_seconds=5
    )
    scoring.validate(card)
    assert card["kill_mode"] == "external"
    resume = card["kill_resume"]
    assert (resume["performed"], resume["after"], resume["clean"]) == (True, "attack", True)
    assert resume["duplicate_idempotency_keys"] == 0 and resume["seq_dense"] is True
    # it did not end itself (that is exit code 7) and it did not exit normally: it was killed
    assert resume["worker_exit_code"] not in (None, 0, EXIT_AFTER_KILL)
    assert card["criteria"]["kill_resume_clean"] is True and card["passed"] is True

    events = events_of(pool, card["project_id"])
    at_kill = card["events_at_kill"]
    assert 0 < at_kill < len(events), "the worker that resumed wrote the rest"
    last_of_the_dead_worker = events[at_kill - 1]
    assert last_of_the_dead_worker["type"] == "session.phase_changed"
    assert last_of_the_dead_worker["payload"]["to"] == "repair", "killed right after the attack"
    before, after = events[:at_kill], events[at_kill:]
    assert len([e for e in before if e["type"] == "model.patch_committed"]) == 1, "the seed"
    assert len([e for e in after if e["type"] == "model.patch_committed"]) == 1, "the repair"

    # nothing was lost and nothing was done twice
    assert [e["seq"] for e in events] == list(range(len(events)))
    assert len({e["idempotency_key"] for e in events}) == len(events)
    phases = [e["payload"]["to"] for e in events if e["type"] == "session.phase_changed"]
    assert phases == [
        "frame",
        "research",
        "model",
        "attack",
        "repair",
        "verify",
        "converge",
        "package",
    ]
    assert (card["flaws_caught"], card["flaws_repaired"]) == (4, 4)
    assert all(f["caught_in_round"] == 1 for f in card["planted_flaws"])
    assert card["gate"]["verdict"] == "ALLOWED" and card["final_status"] == "approved"
    assert ledger.verify_chain(pool, card["project_id"])[1] == []
    # the killed activity was retried when its heartbeats stopped, not when it timed out
    assert card["duration_s"] < ACTIVITY_TIMEOUT.total_seconds() / 10


def test_the_kill_mode_defaults_to_self_in_mock_and_is_recorded_in_the_scorecard(dsn, tmp_path):
    card = golden(dsn, tmp_path, mode="design", kill_after="attack")
    scoring.validate(card)
    assert (card["kill_mode"], card["events_at_kill"]) == ("self", None)
    assert card["kill_resume"]["worker_exit_code"] == EXIT_AFTER_KILL
    assert card["kill_resume"]["clean"] is True and card["passed"] is True
    assert "kill/resume after attack (self kill): clean True" in scoring.format_scorecard(card)
    with pytest.raises(GoldenError, match="--kill-mode must be one of"):
        golden(dsn, tmp_path, mode="review", kill_after="attack", kill_mode="gentle")
    for broken in (card | {"kill_mode": "gentle"}, card | {"events_at_kill": "many"}):
        with pytest.raises(ValueError, match="scorecard invalid"):
            scoring.validate(broken)


def test_a_phase_is_complete_once_the_ledger_shows_the_session_entering_the_next_one():
    def entered(*phases: str) -> list[dict[str, Any]]:
        events = [{"type": "session.phase_changed", "payload": {"to": p}} for p in phases]
        return [{"type": "check.result", "payload": {}}, *events]

    assert not phase_completed([], "attack")
    assert not phase_completed(entered("frame", "research", "model"), "attack")
    assert not phase_completed(entered("frame", "research", "model", "attack"), "attack")
    assert phase_completed(entered("frame", "research", "model", "attack", "repair"), "attack")
    assert phase_completed(entered("frame", "model", "attack", "converge", "package"), "attack")


# --- the command ------------------------------------------------------------------------------


def test_golden_run_from_the_command_line(dsn, tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("ARCHITECT_OBJECT_STORE", str(tmp_path / "objects"))
    out = tmp_path / "card.json"
    code = main(
        ["--database-url", dsn, "golden", "run", "gt-001", "--mode", "review", "--out", str(out)]
    )
    captured = capsys.readouterr()
    assert code == 0, captured.err
    assert "golden gt-001  mode review  provider mock  PASSED" in captured.out
    assert "F3 C-007 on cmp_WORKERS0001: caught in round 1, repaired in round 1" in captured.out
    assert "planted flaws caught 4/4, repaired 4/4" in captured.out
    assert "[x] all_planted_flaws_caught_by_round_1" in captured.out
    assert f"scorecard: {out}" in captured.out
    scoring.validate(json.loads(out.read_text(encoding="utf-8")))
    assert main(["--database-url", dsn, "golden", "run", "gt-999"]) == 1
    assert "no golden task 'gt-999'" in capsys.readouterr().err


def test_a_live_run_without_a_key_is_refused_before_anything_starts(dsn, tmp_path, monkeypatch):
    monkeypatch.delenv("ARCHITECT_ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    with pytest.raises(GoldenError, match="ARCHITECT_ANTHROPIC_API_KEY"):
        golden(dsn, tmp_path, mode="review", live=True)
    assert not (tmp_path / "scorecard.json").exists()
    with pytest.raises(GoldenError, match="mode must be"):
        golden(dsn, tmp_path, mode="audit")
    assert default_out("gt-001", "review", live=True).parent.name == "results"
    assert "mock" in default_out("gt-001", "design", live=False).name
