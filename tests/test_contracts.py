"""The frozen contracts load, meta-validate, and locate errors usefully."""

import subprocess
import sys

import pytest
from jsonschema import Draft202012Validator

from architect.contracts import (
    SCHEMA_FILES,
    contracts_dir,
    first_error,
    json_path,
    load_contracts,
)


def test_all_five_schemas_meta_validate():
    contracts = load_contracts()
    assert set(contracts.schemas) == set(SCHEMA_FILES)
    for schema in contracts.schemas.values():
        Draft202012Validator.check_schema(schema)


def test_every_schema_id_is_a_v1_id():
    for name, schema in load_contracts().schemas.items():
        assert schema["$id"] == f"https://yavin.dev/contracts/v1/{name}"


@pytest.mark.parametrize(
    ("script", "verdict"),
    [("validate.py", "RESULT: ALL GREEN"), ("fixture/replay.py", "RESULT: REPLAY GREEN")],
)
def test_the_contract_scripts_exit_zero(script, verdict):
    """Run as CI runs them: from the repository root, on the frozen files as they are."""
    result = subprocess.run(
        [sys.executable, str(contracts_dir() / script)],
        cwd=contracts_dir().parent,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert verdict in result.stdout


def test_event_types_match_the_payload_dispatch():
    contracts = load_contracts()
    assert len(contracts.event_types) == 22  # 19 in v1.1; v1.2 added three
    assert set(contracts._event_branch) == set(contracts.event_types)


def test_event_error_reports_the_branch_for_the_events_own_type():
    contracts = load_contracts()
    event = {
        "event_id": "evt_0000000001",
        "project_id": "p",
        "seq": 0,
        "ts": "2026-10-02T06:41:00+05:30",
        "actor": {"kind": "human", "id": "saumya"},
        "type": "waiver.signed",
        "payload": {"waiver_id": "wvr_0000000001", "target_ref": "C-010", "risk": "r"},
        "idempotency_key": "waiver-0001",
    }
    error = contracts.event_error(event)
    assert error is not None
    assert "'signer' is a required property" in error.message
    assert json_path(error.path) == "$.payload"

    event["payload"]["signer"] = "saumya"
    assert contracts.event_error(event) is None


def test_formats_are_assertions():
    contracts = load_contracts()
    event = {
        "event_id": "evt_0000000001",
        "project_id": "p",
        "seq": 0,
        "ts": "2026-10-02 06:41",
        "actor": {"kind": "human", "id": "saumya"},
        "type": "session.phase_changed",
        "payload": {"session_id": "ses_0000000001", "to": "frame"},
        "idempotency_key": "phase-0001",
    }
    error = contracts.event_error(event)
    assert error is not None and json_path(error.path) == "$.ts"

    event["ts"] = "2026-10-02T06:41:00+05:30"
    assert contracts.event_error(event) is None

    claim = {"valid_from": "next tuesday"}
    assert any(e.path[-1] == "valid_from" for e in contracts.claim.iter_errors(claim) if e.path)


def test_embedded_validators_resolve_their_defs():
    contracts = load_contracts()
    objection = {
        "element_refs": ["flw_0000000001"],
        "narrative": "n",
        "trigger_condition": "t",
        "severity": "critical",
    }
    error = first_error(contracts.objection, objection)
    assert error is not None and "falsifiable_test" in error.message
    objection["falsifiable_test"] = "a test"
    assert first_error(contracts.objection, objection) is None


def test_json_path_formatting():
    assert json_path(()) == "$"
    assert json_path(("payload", "claim", "evidence", 0, "source")) == (
        "$.payload.claim.evidence[0].source"
    )
    assert json_path(("payload", "odd key")) == '$.payload["odd key"]'
