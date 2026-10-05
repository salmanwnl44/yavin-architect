"""Contracts v1.1 (module C2): integrity, backward compatibility, and the four proposals.

Exit tests X1 to X5. X1, X2 and X4 need no database; X3 and X5 do.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import psycopg
import pytest

from architect import ledger, readmodel
from architect.arbiter import Arbiter
from architect.checks import c005, c006, c013, runner
from architect.checks.catalog import load_catalog
from architect.checks.context import CheckContext
from architect.checks.graph import DEPRECATED_CONVENTION
from architect.cli import main
from architect.contracts import contracts_dir, first_error, load_contracts
from architect.model_fold import apply_patch, empty_model
from architect.projector import Projector
from builders import (
    as_candidate,
    candidate,
    claim,
    ident,
    objection,
    patch,
    proposed_check,
    sample_ledger,
)
from conftest import PROJECT
from replay_reference import FIXTURE_LEDGER, REPLAY, fixture_events, reference

FIXTURE_SHA256 = "e62ec17232863134b46a50dde6e03df64f0d3f67923d07c480d4f591f8edaf4b"
PROGRESS = contracts_dir().parent / "PROGRESS.md"


# --- X1: integrity


def test_the_fixture_ledger_is_byte_identical():
    assert hashlib.sha256(FIXTURE_LEDGER.read_bytes()).hexdigest() == FIXTURE_SHA256


def test_replay_prints_what_progress_recorded():
    """The v1.1 replay.py prints, on the fixture, exactly the output PROGRESS.md records."""
    result = subprocess.run(
        [sys.executable, "-X", "utf8", str(REPLAY)],
        cwd=contracts_dir().parent,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    printed = [line.rstrip() for line in result.stdout.splitlines()]

    progress = PROGRESS.read_text(encoding="utf-8")
    start = progress.index("$ python3 phase0-contracts/fixture/replay.py\n") + len(
        "$ python3 phase0-contracts/fixture/replay.py\n"
    )
    end = progress.index("\n$ ", start)
    recorded = [line.rstrip() for line in progress[start:end].splitlines()]
    assert printed == recorded


def test_the_schemas_still_say_v1():
    for name, schema in load_contracts().schemas.items():
        assert schema["$id"] == f"https://yavin.dev/contracts/v1/{name}"
    readme = (contracts_dir() / "README.md").read_text(encoding="utf-8")
    assert "FROZEN v1.2" in readme  # v1.1 until contracts v1.2 (module C3)


# --- X2: backward compatibility


def embedded(event: dict[str, Any]) -> list[tuple[str, Any]]:
    """(validator name, instance) for every object a ledger event embeds."""
    payload, out = event["payload"], []
    if event["type"] in ("claim.proposed", "claim.committed"):
        out.append(("claim", payload["claim"]))
    if event["type"] in ("model.patch_proposed", "model.patch_committed"):
        out.append(("model_patch", payload["patch"]))
    if event["type"] == "objection.raised":
        out.append(("objection", payload["objection"]))
        proposed = payload["objection"].get("detecting_check", {}).get("proposed_check")
        if proposed is not None:
            out.append(("check", proposed))
    return out


def every_instance_in_the_repo() -> list[tuple[str, str, Any]]:
    """(where, validator name, instance): the fixture, the sample ledger, the builders, the
    catalog, and the models they fold to. Collected, not hand-picked."""
    found: list[tuple[str, str, Any]] = []
    for source, events in (("fixture", fixture_events()), ("sample", sample_ledger())):
        head = None
        for event in events:
            found.append((f"{source}[{event['seq']}]", "event", event))
            found += [(f"{source}[{event['seq']}]", kind, obj) for kind, obj in embedded(event)]
            payload = event["payload"]
            if event["type"] == "model.version_created":
                head = empty_model(event["project_id"], payload["version_id"])
            elif event["type"] == "model.patch_committed":
                head = apply_patch(head, payload["patch"], payload["version_id"])
            if head is not None and event["type"].startswith("model."):
                found.append((f"{source}[{event['seq']}] model", "model", head))
    for status in ("documented", "measured", "observed", "inferred", "assumed", "simulated"):
        extra = {"load_bearing": True, "verification_plan": {"kind": "check", "ref": "C-009"}}
        found.append((f"builders.claim({status})", "claim", claim(status=status)))
        found.append(
            (f"builders.claim({status}, load-bearing)", "claim", claim(status=status, **extra))
        )
    found.append(("builders.patch", "model_patch", patch(ident("mv", "v1"))))
    found.append(("builders.objection", "objection", objection()))
    found.append(("builders.proposed_check", "check", proposed_check()))
    found.append(("reference final model", "model", reference()["final_model"]))
    catalog = load_catalog()
    found.append(
        ("catalog", "catalog", {"catalog_version": catalog.version, "checks": list(catalog.checks)})
    )
    return found


def test_every_existing_instance_validates_under_v1_1():
    contracts = load_contracts()
    instances = every_instance_in_the_repo()
    assert len(instances) > 100
    for where, kind, instance in instances:
        if kind == "event":
            error = contracts.event_error(instance)
        else:
            error = first_error(getattr(contracts, kind), instance)
        assert error is None, f"{where} ({kind}): {error}"


# --- X4: P-9


def component(cid: str, **extra: Any) -> dict[str, Any]:
    return {
        "id": cid,
        "name": cid,
        "kind": "service",
        "stateful": False,
        "requirement_refs": [],
    } | extra


def flow(fid: str, source: str, target: str) -> dict[str, Any]:
    return {
        "id": fid,
        "from": source,
        "to": target,
        "data_class": "internal",
        "rate": {"peak_qps": 1000, "payload_bytes": 64},
    }


X, A, B = (ident("cmp", name) for name in ("x", "a", "b"))
IN, AB = ident("flw", "in"), ident("flw", "ab")


def bound_model() -> dict[str, Any]:
    """Params bound through applies_to and metric (v1.1)."""
    return {
        "version_id": "mv_TEST000001",
        "project_id": "p",
        "elements": {
            "components": [component(X, kind="external"), component(A), component(B)],
            "flows": [flow(IN, X, A), flow(AB, A, B)],
            "slos": [
                {
                    "id": "slo",
                    "applies_to": A,
                    "metric": "availability",
                    "target": 0.99,
                    "unit": "ratio",
                }
            ],
            "capacity_params": [
                {
                    "id": "q-a",
                    "name": "a throughput",
                    "value": 1600,
                    "unit": "qps",
                    "applies_to": A,
                    "metric": "max_qps",
                },
                {
                    "id": "q-b",
                    "name": "b throughput",
                    "value": 900,
                    "unit": "qps",
                    "applies_to": B,
                    "metric": "max_qps",
                },
                {
                    "id": "av-a",
                    "name": "a availability",
                    "value": 0.999,
                    "unit": "ratio",
                    "applies_to": A,
                    "metric": "availability",
                },
            ],
        },
        "links": {"depends_on": [{"from": A, "to": B, "kind": "sync"}]},
    }


def conventional_model() -> dict[str, Any]:
    """The same quantities, bound by the v1.0 naming convention only."""
    m = bound_model()
    for param in m["elements"]["capacity_params"]:
        param["name"] = f"{param.pop('applies_to')}.{param.pop('metric')}"
    return m


@pytest.mark.parametrize("module", [c005, c006])
def test_fields_and_the_name_convention_give_the_same_verdict(module):
    ctx = CheckContext(as_of_seq=0)
    fielded = module.check(bound_model(), ctx, {})
    conventional = module.check(conventional_model(), ctx, {})
    assert (fielded.status, fielded.element_refs) == (
        conventional.status,
        conventional.element_refs,
    )
    assert "deprecated" not in fielded.evidence
    assert conventional.evidence["deprecated"] == DEPRECATED_CONVENTION
    stripped = {k: v for k, v in conventional.evidence.items() if k != "deprecated"}
    assert stripped == fielded.evidence


def test_c005_and_c006_verdicts_on_the_bound_model():
    ctx = CheckContext(as_of_seq=0)
    out = c005.check(bound_model(), ctx, {})
    assert (out.status, out.element_refs) == ("fail", [B]), "b: 900 < 1.5 x 1000"
    out = c006.check(bound_model(), ctx, {})
    assert out.status == "error"
    assert out.evidence["missing"] == [f"slo: no capacity param {B}.availability"]


def test_c013_fails_a_capacity_param_whose_applies_to_dangles():
    m = bound_model()
    m["elements"]["capacity_params"][0]["applies_to"] = "ghost"
    out = c013.check(m, CheckContext(as_of_seq=0), {})
    assert out.status == "fail" and out.element_refs == ["q-a"]
    assert out.evidence["dangling"] == [
        {"element": "q-a", "field": "applies_to", "missing": "ghost"}
    ]
    assert c013.check(bound_model(), CheckContext(as_of_seq=0), {}).status == "pass"
    assert c013.check(conventional_model(), CheckContext(as_of_seq=0), {}).status == "pass"


def test_a_capacity_param_with_the_new_fields_validates():
    error = first_error(load_contracts().model, bound_model())
    assert error is None, error


# --- X3: P-7 three-way agreement


def fold_with_replay(ledger_path: Path) -> dict[str, dict[str, Any]]:
    """replay.py's models, running its source unmodified on another ledger."""
    import contextlib
    import io

    namespace: dict[str, Any] = {"__name__": "__main__", "__file__": str(REPLAY)}
    argv, sys.argv = sys.argv, [str(REPLAY), str(ledger_path)]
    try:
        with contextlib.redirect_stdout(io.StringIO()) as out:
            try:
                exec(compile(REPLAY.read_text(encoding="utf-8"), str(REPLAY), "exec"), namespace)
            except SystemExit as stop:
                assert not stop.code, out.getvalue()
    finally:
        sys.argv = argv
    assert not namespace["failures"], namespace["failures"]
    return namespace["models"]


def test_replay_arbiter_and_projector_agree_on_a_branching_ledger(pool, dsn, tmp_path, capsys):
    mv1, mv2, mv3, mv4 = (ident("mv", f"v{n}") for n in (1, 2, 3, 4))
    fencer = {
        "id": ident("cmp", "fencer"),
        "name": "Fencer",
        "kind": "service",
        "stateful": False,
        "requirement_refs": [],
    }
    router = {
        "id": ident("cmp", "router"),
        "name": "Router",
        "kind": "gateway",
        "stateful": False,
        "requirement_refs": [],
    }
    ledger.create_project(pool, PROJECT)
    arbiter = Arbiter(pool)
    for event in (
        candidate("model.version_created", {"version_id": mv1}),
        candidate(
            "model.patch_committed",
            {
                "version_id": mv2,
                "base_version": mv1,
                "patch": {
                    "base_version": mv1,
                    "rationale": "fencer",
                    "ops": [{"op": "add_element", "element_type": "components", "element": fencer}],
                },
            },
        ),
        candidate("model.version_created", {"version_id": mv3, "parent": mv2}),
        candidate(
            "model.patch_committed",
            {
                "version_id": mv4,
                "base_version": mv3,
                "patch": {
                    "base_version": mv3,
                    "rationale": "router",
                    "ops": [{"op": "add_element", "element_type": "components", "element": router}],
                },
            },
        ),
    ):
        arbiter.submit(PROJECT, event)
    Projector(pool).catch_up(PROJECT)

    dump = tmp_path / "branching.jsonl"
    assert main(["--database-url", dsn, "dump", "--project", PROJECT, "-o", str(dump)]) == 0
    capsys.readouterr()
    replayed = fold_with_replay(dump)

    with psycopg.connect(dsn, row_factory=psycopg.rows.dict_row) as conn:
        arbiter_models = {
            row["version_id"]: row["model"]
            for row in conn.execute(
                "SELECT version_id, model FROM arb_model_versions WHERE project_id = %s", (PROJECT,)
            )
        }
    projected = {
        v: readmodel.model_version(pool, PROJECT, v)["model"] for v in (mv1, mv2, mv3, mv4)
    }

    assert set(replayed) == set(arbiter_models) == set(projected) == {mv1, mv2, mv3, mv4}
    for version in (mv1, mv2, mv3, mv4):
        content = {
            "elements": arbiter_models[version]["elements"],
            "links": arbiter_models[version]["links"],
        }
        assert replayed[version] == content, version
        assert projected[version] == arbiter_models[version], version
        assert projected[version]["version_id"] == version
    assert replayed[mv3] == replayed[mv2] == {"elements": {"components": [fencer]}, "links": {}}
    assert replayed[mv4]["elements"]["components"] == [fencer, router]
    assert replayed[mv1] == {"elements": {}, "links": {}}


# --- X5: P-10


FIX = "proj-architect-dogfood"


def test_new_check_results_name_their_version_and_seq(pool, dsn):
    ledger.create_project(pool, FIX)
    arbiter = Arbiter(pool)
    for event in fixture_events():
        arbiter.submit(FIX, as_candidate(event))
    report = runner.run(pool, FIX, "mv_FIXV000003")
    contracts = load_contracts()
    for result in report.results:
        event = ledger.get_event(pool, FIX, result.event_id)
        assert event["payload"]["model_version"] == "mv_FIXV000003"
        assert event["payload"]["as_of_seq"] == 39
        assert event["payload"]["evidence"]["model_version"] == "mv_FIXV000003"
        assert contracts.event_error(event) is None
    with psycopg.connect(dsn, row_factory=psycopg.rows.dict_row) as conn:
        keyed = conn.execute(
            "SELECT version_id, count(*) AS n FROM proj_checks WHERE project_id = %s "
            "GROUP BY version_id ORDER BY version_id",
            (FIX,),
        ).fetchall()
    # the fixture's own two v1.0 results at V2 (head then), its one at V3, and the thirteen new
    assert [(row["version_id"], row["n"]) for row in keyed] == [
        ("mv_FIXV000002", 2),
        ("mv_FIXV000003", 14),
    ]
    recorded = runner.recorded(pool, FIX, "mv_FIXV000003")
    assert {cid: r["status"] for cid, r in recorded.items()} == {
        "C-001": "pass",
        "C-002": "pass",
        "C-003": "pass",
        "C-004": "pass",
        "C-005": "error",
        "C-006": "skipped",
        "C-007": "fail",
        "C-008": "pass",
        "C-009": "pass",
        "C-010": "fail",
        "C-011": "skipped",
        "C-012": "skipped",
        "C-013": "pass",
    }
    verdict = runner.gate(pool, FIX, "mv_FIXV000003")
    assert verdict["verdict"] == "BLOCKED"
    assert {(r["check_id"], r["status"]) for r in verdict["reasons"]} == {
        ("C-005", "error"),
        ("C-007", "fail"),
    }


def test_a_v1_0_result_without_the_fields_is_keyed_by_the_head_of_its_time(pool):
    ledger.create_project(pool, PROJECT)
    arbiter = Arbiter(pool)
    mv1 = ident("mv", "v1")
    arbiter.submit(PROJECT, candidate("model.version_created", {"version_id": mv1}))
    legacy = {
        "result_id": ident("chk", "legacy"),
        "check_id": "C-002",
        "element_refs": [],
        "status": "pass",
        "evidence": {"note": "written by a v1.0 runner"},
    }
    arbiter.submit(PROJECT, candidate("check.result", legacy))
    Projector(pool).catch_up(PROJECT)
    assert runner.recorded(pool, PROJECT, mv1)["C-002"]["result_id"] == legacy["result_id"]


def test_the_v1_1_fields_are_optional_and_typed():
    contracts = load_contracts()
    base = fixture_events()[21]  # the fixture's C-005 result, a v1.0 instance
    assert contracts.event_error(base) is None
    named = json.loads(json.dumps(base))
    named["payload"] |= {"model_version": "mv_FIXV000002", "as_of_seq": 21}
    assert contracts.event_error(named) is None
    negative = json.loads(json.dumps(named))
    negative["payload"]["as_of_seq"] = -1
    error = contracts.event_error(negative)
    assert error is not None and re.search(r"as_of_seq", json.dumps(list(error.path)))
