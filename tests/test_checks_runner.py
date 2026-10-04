"""The checks engine against a database: the runner, the gate, recording, the CLI and the API.

M3 exit tests E3 to E8 and E10 live here, on the frozen fixture committed through the
Arbiter into the project it names.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from architect import ledger
from architect.arbiter import Arbiter
from architect.checks import runner
from architect.checks.catalog import REGISTRY, load_catalog
from architect.cli import main
from architect.errors import Rejection
from architect.projector import Projector
from builders import as_candidate, candidate
from conftest import PROJECT
from replay_reference import fixture_events

FIX = "proj-architect-dogfood"
V1, V2, V3 = "mv_FIXV000001", "mv_FIXV000002", "mv_FIXV000003"
GENESIS = "mv_FIXGENESIS"
LAST_SEQ = 39

EXPECTED_ON_V3 = {
    "C-001": ("pass", []),
    "C-002": ("pass", []),
    "C-003": ("pass", []),
    "C-004": ("pass", []),
    "C-005": ("error", ["cmp_FIXROUTER1", "cmp_FIXSHARD01", "cmp_FIXWAL0001"]),
    "C-006": ("skipped", []),
    "C-007": ("fail", ["cmp_FIXLEASE01"]),
    "C-008": ("pass", []),
    "C-009": ("pass", []),
    "C-010": ("fail", ["cmp_FIXROUTER1", "cmp_FIXSHARD01", "cmp_FIXWAL0001"]),
    "C-011": ("skipped", []),
    "C-012": ("skipped", []),
    "C-013": ("pass", []),
}


def commit_fixture(pool, project_id: str = FIX) -> Arbiter:
    """The fixture into a project; another project gets its own event ids, which are unique
    across the database."""
    ledger.create_project(pool, project_id)
    arbiter = Arbiter(pool)
    events = fixture_events()
    renamed = {
        e["event_id"]: f"evt_{project_id.upper():0>8}{e['seq']:04d}"
        for e in events
        if project_id != FIX
    }
    for event in events:
        candidate = as_candidate(event) | {"project_id": project_id}
        if renamed:
            candidate["event_id"] = renamed[event["event_id"]]
            for field in ("cause_event", "merge_event"):  # the fixture's references to itself
                if candidate["payload"].get(field) in renamed:
                    candidate["payload"][field] = renamed[candidate["payload"][field]]
        arbiter.submit(project_id, candidate)
    return arbiter


@pytest.fixture
def fixture_project(pool) -> Arbiter:
    return commit_fixture(pool)


def table(report: runner.RunReport) -> dict[str, tuple[str, list[str]]]:
    return {r.check_id: (r.status, r.element_refs) for r in report.results}


def by_id(report: runner.RunReport) -> dict[str, runner.CheckRun]:
    return {r.check_id: r for r in report.results}


def reasons(verdict: dict[str, Any]) -> set[tuple[str, str]]:
    return {(r.get("check_id") or r.get("objection_id"), r["status"]) for r in verdict["reasons"]}


# --- E3: the fixture's verdict table ----------------------------------------------------------


def test_the_battery_on_v3_matches_the_answer_key(pool, fixture_project):
    report = runner.run(pool, FIX, V3)
    assert (report.model_version, report.as_of_seq, report.catalog_version) == (
        V3,
        LAST_SEQ,
        "1.0.0",
    )
    assert table(report) == EXPECTED_ON_V3

    results = by_id(report)
    assert results["C-010"].evidence["waived"] == {"cmp_FIXLEASE01": "wvr_FIXSPOF001"}
    assert results["C-007"].evidence["components"]["cmp_FIXLEASE01"]["missing"] == [
        "recovery.rto_s",
        "recovery.path",
    ]
    assert results["C-005"].evidence["missing"] == [
        f"{c}: no capacity param {c}.max_qps"
        for c in ("cmp_FIXROUTER1", "cmp_FIXSHARD01", "cmp_FIXWAL0001")
    ]
    flow = results["C-008"].evidence["flows"]["flw_FIXINGR001"]
    assert flow == {"boundaries": ["tb-cluster"], "data_class": "internal", "missing": []}
    assert results["C-009"].evidence["load_bearing"] == {"clm_FIXPAYLOAD1": "measured"}
    for result in report.results:
        envelope = {
            k: result.evidence[k] for k in ("model_version", "as_of_seq", "catalog_version")
        }
        assert envelope == {"model_version": V3, "as_of_seq": LAST_SEQ, "catalog_version": "1.0.0"}
        assert result.evidence["check_version"] == 1 and result.evidence["inputs_hash"]
        assert result.replayed is False and result.event_id

    verdict = runner.gate(pool, FIX, V3)
    assert verdict["verdict"] == "BLOCKED"
    assert reasons(verdict) == {("C-005", "error"), ("C-007", "fail")}
    assert [(w["check_id"], w["status"]) for w in verdict["warnings"]] == [("C-010", "fail")]


# --- E4, E5, E6: other versions and other seqs ------------------------------------------------


def test_v2_fails_c008_like_the_ledger_recorded(pool, fixture_project):
    report = runner.run(pool, FIX, V2)
    c008 = by_id(report)["C-008"]
    assert (c008.status, c008.element_refs) == ("fail", ["flw_FIXINGR001"])
    assert c008.evidence["flows"]["flw_FIXINGR001"]["missing"] == [
        "input_validation",
        "encryption_in_transit",
    ]
    assert table(report)["C-007"] == ("fail", ["cmp_FIXLEASE01"])
    assert ("C-008", "fail") in reasons(runner.gate(pool, FIX, V2))


def test_time_travel_sees_the_assumption_still_open(pool, fixture_project):
    then = runner.run(pool, FIX, V2, as_of_seq=19)
    assert table(then)["C-009"] == ("fail", ["clm_FIXPAYLOAD1"])
    assert then.as_of_seq == 19
    now = runner.run(pool, FIX, V2)
    assert table(now)["C-009"] == ("pass", [])
    # C-010 at seq 19: the waiver (seq 36) does not exist yet, so the Lease Manager is a SPOF
    assert table(then)["C-010"][1] == [
        "cmp_FIXROUTER1",
        "cmp_FIXSHARD01",
        "cmp_FIXLEASE01",
        "cmp_FIXWAL0001",
    ]
    assert by_id(now)["C-010"].evidence["waived"] == {"cmp_FIXLEASE01": "wvr_FIXSPOF001"}


def test_the_genesis_covers_no_requirement(pool, fixture_project):
    report = runner.run(pool, FIX, GENESIS, as_of_seq=14)
    assert table(report)["C-001"] == ("fail", ["req_FIXQPS001", "req_FIXDUR001"])
    assert table(report)["C-013"] == ("pass", [])
    assert table(report)["C-002"] == ("skipped", [])


def test_the_gate_counts_an_objection_open_as_of_the_seq(pool, fixture_project):
    runner.run(pool, FIX, V2, as_of_seq=24)
    blocked = runner.gate(pool, FIX, V2, as_of_seq=24)
    assert ("obj_FIXSPLIT01", "open") in reasons(blocked)
    runner.run(pool, FIX, V2, as_of_seq=27)
    assert ("obj_FIXSPLIT01", "open") not in reasons(runner.gate(pool, FIX, V2, as_of_seq=27))


def test_a_gate_without_recorded_results_is_blocked_as_not_evaluated(pool, fixture_project):
    verdict = runner.gate(pool, FIX, V3)
    assert verdict["verdict"] == "BLOCKED"
    assert {r["status"] for r in verdict["reasons"]} == {"not_evaluated"}
    assert {r["check_id"] for r in verdict["reasons"]} == {
        "C-001",
        "C-003",
        "C-005",
        "C-006",
        "C-007",
        "C-008",
        "C-009",
        "C-013",
    }


# --- E7: the repair loop ----------------------------------------------------------------------


def capacity(cid: str) -> dict[str, Any]:
    return {
        "op": "add_element",
        "element_type": "capacity_params",
        "element": {
            "id": f"cap-{cid[-8:]}",
            "name": f"{cid}.max_qps",
            "value": 3600,
            "unit": "qps",
        },
    }


REPAIR = {
    "base_version": V3,
    "rationale": "declare capacity and the lease manager's rebuild path",
    "ops": [
        capacity("cmp_FIXROUTER1"),
        capacity("cmp_FIXSHARD01"),
        capacity("cmp_FIXWAL0001"),
        {
            "op": "update_element",
            "element_type": "components",
            "element_id": "cmp_FIXLEASE01",
            "element": {
                "recovery": {
                    "rto_s": 10,
                    "path": "rebuild leases from WAL; fencing epoch persisted in WAL",
                }
            },
        },
    ],
}


def test_a_repair_patch_opens_the_gate(pool):
    arbiter = commit_fixture(pool, "repair")
    repaired = "mv_FIXREPAIR01"
    arbiter.submit(
        "repair",
        candidate(
            "model.patch_committed", {"version_id": repaired, "base_version": V3, "patch": REPAIR}
        ),
    )
    report = runner.run(pool, "repair", repaired)
    assert table(report)["C-005"] == ("pass", [])
    assert table(report)["C-007"] == ("pass", [])
    assert table(report)["C-010"] == (
        "fail",
        ["cmp_FIXROUTER1", "cmp_FIXSHARD01", "cmp_FIXWAL0001"],
    )
    assert by_id(report)["C-005"].evidence["components"]["cmp_FIXROUTER1"] == {
        "inbound_qps": 2000.0,
        "required": 3000.0,
        "capacity": 3600.0,
        "unit": "qps",
    }
    verdict = runner.gate(pool, "repair", repaired)
    assert verdict["verdict"] == "ALLOWED" and verdict["reasons"] == []
    assert [w["check_id"] for w in verdict["warnings"]] == ["C-010"]


# --- E8: recording and the cache ------------------------------------------------------------


def test_results_are_recorded_once_per_inputs(pool, fixture_project, tmp_path):
    first = runner.run(pool, FIX, V3)
    after_first = ledger.head(pool, FIX)["last_seq"]
    assert after_first == LAST_SEQ + 13
    events = list(ledger.iter_events(pool, FIX))[-13:]
    assert all(e["type"] == "check.result" and e["actor"] == runner.ACTOR for e in events)
    assert [e["payload"]["check_id"] for e in events] == [r.check_id for r in first.results]

    second = runner.run(pool, FIX, V3)
    assert ledger.head(pool, FIX)["last_seq"] == after_first, "nothing new was written"
    assert all(r.replayed for r in second.results)
    assert [r.event_id for r in second.results] == [r.event_id for r in first.results]
    assert table(second) == table(first)

    recorded = runner.recorded(pool, FIX, V3)
    assert {cid: (r["status"], r["element_refs"]) for cid, r in recorded.items()} == EXPECTED_ON_V3
    assert all(r["evidence"]["model_version"] == V3 for r in recorded.values())
    # The ledger's own three check results predate the runner and name no version, so they
    # are keyed by the head of their time: C-005 and C-008 under V2, C-009 under V3, where
    # the runner's later C-009 result now outranks it.
    own = runner.recorded(pool, FIX, V2)
    assert {cid: r["result_id"] for cid, r in own.items()} == {
        "C-005": "chk_FIXC005A01",
        "C-008": "chk_FIXC008F01",
    }
    assert runner.recorded(pool, FIX, V2, as_of_seq=LAST_SEQ) == {}
    assert recorded["C-009"]["result_id"] != "chk_FIXC009P01"

    # A stricter catalog: headroom 2.0, check version bumped, so the inputs hash changes.
    catalog = {"catalog_version": "1.1.0", "checks": list(load_catalog().checks)}
    c005 = next(c for c in catalog["checks"] if c["id"] == "C-005")
    c005["params"] = {"headroom": 2.0}
    c005["version"] = 2
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps(catalog), encoding="utf-8")
    arbiter = commit_fixture(pool, "repair")
    repaired = "mv_FIXREPAIR01"
    arbiter.submit(
        "repair",
        candidate(
            "model.patch_committed", {"version_id": repaired, "base_version": V3, "patch": REPAIR}
        ),
    )
    lenient = runner.run(pool, "repair", repaired)
    assert table(lenient)["C-005"] == ("pass", [])
    before = ledger.head(pool, "repair")["last_seq"]
    strict = runner.run(pool, "repair", repaired, catalog=load_catalog(path))
    assert table(strict)["C-005"] == (
        "fail",
        ["cmp_FIXROUTER1", "cmp_FIXSHARD01", "cmp_FIXWAL0001"],
    )
    assert by_id(strict)["C-005"].replayed is False
    assert by_id(strict)["C-005"].inputs_hash != by_id(lenient)["C-005"].inputs_hash
    assert ledger.head(pool, "repair")["last_seq"] == before + 1, "only C-005 changed"
    assert all(r.replayed for r in strict.results if r.check_id != "C-005")
    assert runner.recorded(pool, "repair", repaired)["C-005"]["status"] == "fail"
    assert runner.gate(pool, "repair", repaired, catalog=load_catalog(path))["verdict"] == "BLOCKED"


def test_a_dry_run_records_nothing(pool, fixture_project):
    report = runner.run(pool, FIX, V3, record=False)
    assert table(report) == EXPECTED_ON_V3
    assert all(r.event_id is None and r.replayed is None for r in report.results)
    assert ledger.head(pool, FIX)["last_seq"] == LAST_SEQ


def test_an_unknown_version_is_refused(pool, fixture_project):
    with pytest.raises(Rejection) as refused:
        runner.run(pool, FIX, "mv_FIXGHOST001")
    assert refused.value.code == "MODEL_VERSION_NOT_FOUND" and refused.value.http_status == 404


def test_a_waiver_signed_after_the_seq_does_not_count(pool, fixture_project):
    arbiter = Arbiter(pool)
    late = candidate(
        "waiver.signed",
        {
            "waiver_id": "wvr_FIXLATE0001",
            "target_ref": "C-007:cmp_FIXLEASE01",
            "risk": "accepted for the test",
            "signer": "saumya",
        },
        actor={"kind": "human", "id": "saumya", "role": "owner"},
    )
    signed = arbiter.submit(FIX, late).event
    Projector(pool).catch_up(FIX)
    before = runner.build_context(pool, FIX, {}, signed["seq"] - 1)
    after = runner.build_context(pool, FIX, {}, signed["seq"])
    assert "C-007:cmp_FIXLEASE01" not in before.waivers
    assert after.waivers["C-007:cmp_FIXLEASE01"] == "wvr_FIXLATE0001"
    assert table(runner.run(pool, FIX, V3, as_of_seq=signed["seq"] - 1))["C-007"][0] == "fail"
    assert table(runner.run(pool, FIX, V3, as_of_seq=signed["seq"]))["C-007"][0] == "pass"


# --- the CLI ------------------------------------------------------------------------------------


@pytest.fixture
def cli(dsn, capsys):
    def run(*argv: str) -> tuple[int, str, str]:
        code = main(["--database-url", dsn, *argv])
        captured = capsys.readouterr()
        return code, captured.out, captured.err

    return run


def test_check_and_gate_commands(pool, fixture_project, cli):
    code, out, _ = cli("check", "--project", FIX, "--version", V3, "--dry-run")
    assert code == 0 and "dry run: nothing recorded" in out
    assert "C-007 critical fail    cmp_FIXLEASE01" in out
    assert ledger.head(pool, FIX)["last_seq"] == LAST_SEQ

    code, out, _ = cli("gate", "--project", FIX, "--version", V3)
    assert code == 2
    assert "gate IMPLEMENTATION_READY: BLOCKED" in out
    assert out.count("blocking:") == 2 and out.count("warning:") == 1
    assert "(recorded)" in out and ledger.head(pool, FIX)["last_seq"] == LAST_SEQ + 13

    code, out, _ = cli("check", "--project", FIX, "--version", V3)
    assert code == 0 and "(on record)" in out and "(recorded)" not in out

    code, _, err = cli("check", "--project", FIX, "--version", "mv_FIXGHOST001")
    assert code == 1 and "MODEL_VERSION_NOT_FOUND" in err


# --- E10: the API -------------------------------------------------------------------------------


def test_the_check_endpoints(client, pool):
    commit_fixture(pool)
    base = f"/v1/projects/{FIX}/models/{V3}"
    unprojected = client.get(f"{base}/checks")
    assert unprojected.status_code == 404, "the read models have not caught up yet"
    Projector(pool).catch_up(FIX)

    # Before any run, the only result for V3 is the one the fixture's ledger carries.
    own = client.get(f"{base}/checks")
    assert own.status_code == 200
    assert [(r["check_id"], r["result_id"]) for r in own.json()["results"]] == [
        ("C-009", "chk_FIXC009P01")
    ]

    posted = client.post(f"{base}/checks")
    assert posted.status_code == 200, posted.text
    report = posted.json()
    assert (report["model_version"], report["as_of_seq"]) == (V3, LAST_SEQ)
    assert {
        r["check_id"]: (r["status"], r["element_refs"]) for r in report["results"]
    } == EXPECTED_ON_V3

    listed = client.get(f"{base}/checks").json()["results"]
    assert {r["check_id"]: (r["status"], r["element_refs"]) for r in listed} == EXPECTED_ON_V3
    assert all(r["evidence"]["model_version"] == V3 for r in listed), "the run outranks seq 33"

    gate = client.get(f"{base}/gate").json()
    assert gate["verdict"] == "BLOCKED" and gate["as_of_seq"] == LAST_SEQ
    assert {(r["check_id"], r["status"]) for r in gate["reasons"]} == {
        ("C-005", "error"),
        ("C-007", "fail"),
    }

    earlier = client.post(f"{base}/checks", params={"as_of_seq": 19}).json()
    assert {r["check_id"]: r["status"] for r in earlier["results"]}["C-009"] == "fail"

    missing = client.get(f"/v1/projects/{FIX}/models/mv_FIXGHOST001/gate")
    assert missing.status_code == 404 and missing.json()["code"] == "MODEL_VERSION_NOT_FOUND"
    assert client.post(f"/v1/projects/nope/models/{V3}/checks").status_code == 404
    assert client.post(f"{base}/checks", params={"as_of_seq": -1}).status_code == 422
    assert client.get(f"/v1/projects/{PROJECT}/models/{V3}/checks").status_code == 404


# --- M7-live: a re-run battery and the gate ---------------------------------------------------


def test_the_gate_keeps_the_results_a_rerun_did_not_have_to_record_again(pool, fixture_project):
    """A battery re-run records a new result only where a check's inputs changed. The gate
    must judge every check by its latest result as of the seq, not only by those stamped
    with the re-run's own seq (it used to call the others "not evaluated")."""
    first = runner.run(pool, FIX, V3)
    blocked = runner.gate(pool, FIX, V3)
    assert reasons(blocked) == {("C-005", "error"), ("C-007", "fail")}

    # a human waives C-007 for the one element that fails it. A check's inputs include the
    # parts of the context it declares, so every check that reads the waivers records again;
    # the others (C-013 among them) do not
    fixture_project.submit(
        FIX,
        candidate(
            "waiver.signed",
            {
                "waiver_id": "wvr_LEASEDURAB1",
                "target_ref": "C-007:cmp_FIXLEASE01",
                "risk": "accepted for the pilot",
                "signer": "saumya",
            },
            actor={"kind": "human", "id": "saumya"},
        ),
    )
    second = runner.run(pool, FIX, V3)
    reads_waivers = {m.CHECK_ID for m in REGISTRY.values() if "waivers" in m.USES}
    assert {r.check_id for r in second.results if not r.replayed} == reads_waivers
    kept = {r.check_id for r in second.results if r.replayed}
    assert "C-007" in reads_waivers and "C-013" in kept and len(kept) >= 1
    assert second.as_of_seq > first.as_of_seq
    for verdict in (runner.gate(pool, FIX, V3), runner.gate(pool, FIX, V3, second.as_of_seq)):
        assert reasons(verdict) == {("C-005", "error")}, "C-007 is waived; nothing went missing"
        assert verdict["as_of_seq"] == second.as_of_seq
    assert set(runner.recorded(pool, FIX, V3, second.as_of_seq)) == {
        r.check_id for r in second.results
    }
    # the knowledge as of the first battery is still what it was
    assert reasons(runner.gate(pool, FIX, V3, first.as_of_seq)) == {
        ("C-005", "error"),
        ("C-007", "fail"),
    }

    # a third run in which nothing changed records nothing, and the gate at its seq agrees
    third = runner.run(pool, FIX, V3)
    assert all(r.replayed for r in third.results) and third.as_of_seq > second.as_of_seq
    assert reasons(runner.gate(pool, FIX, V3, third.as_of_seq)) == {("C-005", "error")}
