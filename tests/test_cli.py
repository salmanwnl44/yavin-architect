"""The `architect` CLI on the sample ledger: ingest, dump, verify, rebuild-state."""

from __future__ import annotations

import json
import subprocess
import sys

import psycopg
import pytest

from architect.cli import main
from architect.contracts import load_contracts
from architect.state import STATE_TABLES
from builders import sample_ledger


@pytest.fixture
def cli(dsn, capsys):
    def run(*argv: str) -> tuple[int, str, str]:
        code = main(["--database-url", dsn, *argv])
        captured = capsys.readouterr()
        return code, captured.out, captured.err

    return run


@pytest.fixture
def ledger_file(tmp_path):
    path = tmp_path / "ledger.jsonl"
    path.write_text(
        "".join(json.dumps(event) + "\n" for event in sample_ledger()), encoding="utf-8"
    )
    return path


def read_jsonl(path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_ingest_then_dump_round_trips_the_ledger(cli, ledger_file, tmp_path):
    code, out, _ = cli("ingest", str(ledger_file), "--project", "sample")
    assert code == 0
    assert "24 committed, 0 replayed" in out

    dump = tmp_path / "dump.jsonl"
    assert cli("dump", "--project", "sample", "-o", str(dump))[0] == 0
    dumped = read_jsonl(dump)

    contracts = load_contracts()
    assert all(contracts.event_error(event) is None for event in dumped)
    assert [e["seq"] for e in dumped] == list(range(24))
    # Everything the client supplied survives; only project_id (the --project) and the
    # Arbiter-stamped prev_hash differ.
    expected = [e | {"project_id": "sample"} for e in sample_ledger()]
    assert [{k: v for k, v in e.items() if k != "prev_hash"} for e in dumped] == expected
    assert "prev_hash" not in dumped[0] and all("prev_hash" in e for e in dumped[1:])


def test_ingest_is_idempotent(cli, ledger_file):
    cli("ingest", str(ledger_file), "--project", "sample")
    code, out, _ = cli("ingest", str(ledger_file), "--project", "sample")
    assert code == 0
    assert "0 committed, 24 replayed" in out


def test_a_dump_can_be_ingested_into_a_fresh_database_project(cli, ledger_file, tmp_path, dsn):
    """A dump carries seq and prev_hash; ingest restamps them and reaches the same chain."""
    cli("ingest", str(ledger_file), "--project", "sample")
    dump = tmp_path / "dump.jsonl"
    cli("dump", "--project", "sample", "-o", str(dump))

    # event_id is unique per database, so replay the dump into the same project: all replayed.
    code, out, _ = cli("ingest", str(dump), "--project", "sample")
    assert code == 0 and "0 committed, 24 replayed" in out


def test_ingest_stops_at_the_first_rejection_with_the_typed_error(cli, tmp_path):
    events = sample_ledger()
    events[18]["actor"] = {"kind": "agent", "id": "adversary-1"}  # the waiver
    path = tmp_path / "bad.jsonl"
    path.write_text("".join(json.dumps(e) + "\n" for e in events), encoding="utf-8")

    code, out, err = cli("ingest", str(path), "--project", "sample")
    assert code == 1
    assert f"{path}:19: rejected" in err
    assert '"code": "WAIVER_NOT_HUMAN"' in err
    assert "18 committed" in out

    code, out, _ = cli("verify", "--project", "sample")
    assert code == 0 and "OK (18 events)" in out


def test_dump_to_stdout(cli, ledger_file):
    cli("ingest", str(ledger_file), "--project", "sample")
    code, out, err = cli("dump", "--project", "sample")
    assert code == 0
    assert len(out.splitlines()) == 24
    assert "dumped 24 events" in err


def test_verify_reports_a_healthy_chain(cli, ledger_file):
    cli("ingest", str(ledger_file), "--project", "sample")
    code, out, _ = cli("verify", "--project", "sample")
    assert code == 0
    assert "hash chain for sample: OK (24 events)" in out


def test_verify_reports_a_tampered_event(cli, ledger_file, dsn):
    cli("ingest", str(ledger_file), "--project", "sample")
    with psycopg.connect(dsn) as conn:  # only possible by disabling the append-only trigger
        conn.execute("ALTER TABLE events DISABLE TRIGGER events_no_update_delete")
        conn.execute('UPDATE events SET payload = payload || \'{"risk": "none"}\' WHERE seq = 18')
        conn.execute("ALTER TABLE events ENABLE TRIGGER events_no_update_delete")

    code, out, _ = cli("verify", "--project", "sample")
    assert code == 1
    assert "BREAK seq=19" in out
    assert "1 break(s) in 24 events" in out


def test_rebuild_state_reports_zero_diff_on_a_healthy_ledger(cli, ledger_file, fingerprint):
    cli("ingest", str(ledger_file), "--project", "sample")
    before = fingerprint()
    # arb_findings (contracts v1.2) is the one table the v1.1 sample ledger has nothing for;
    # test_contracts_v12.py rebuilds a ledger with findings and branches
    populated = [table for table in STATE_TABLES if table != "arb_findings"]
    assert all(before[table] for table in populated), "the sample should populate every table"
    assert before["arb_findings"] == []

    code, out, _ = cli("rebuild-state", "--project", "sample")
    assert code == 0
    assert "from 24 events" in out
    assert "diff vs previous state: empty" in out
    assert fingerprint() == before


def test_rebuild_state_repairs_and_reports_drift(cli, ledger_file, dsn, fingerprint):
    cli("ingest", str(ledger_file), "--project", "sample")
    healthy = fingerprint()
    with psycopg.connect(dsn) as conn:
        conn.execute("UPDATE arb_claims SET status = 'measured' WHERE status = 'retracted'")
        conn.execute("DELETE FROM arb_sources")
        conn.execute(
            "INSERT INTO arb_objections VALUES ('sample', 'obj_0000INVENTED', 'minor', true)"
        )

    code, out, _ = cli("rebuild-state", "--project", "sample")
    assert code == 1
    assert "arb_sources: 0 stale, 2 missing" in out
    assert "arb_claims: 1 stale, 1 missing" in out
    assert "arb_objections: 1 stale, 0 missing" in out
    assert "NOT EMPTY" in out
    assert fingerprint() == healthy

    assert cli("rebuild-state", "--project", "sample")[0] == 0


def test_dropping_the_state_tables_loses_nothing(cli, ledger_file, dsn, fingerprint):
    cli("ingest", str(ledger_file), "--project", "sample")
    healthy = fingerprint()
    with psycopg.connect(dsn) as conn:
        for table in STATE_TABLES:
            conn.execute(f"DROP TABLE {table}")

    code, out, _ = cli("rebuild-state", "--project", "sample")  # recreates the tables first
    assert code == 1 and "NOT EMPTY" in out
    assert fingerprint() == healthy


def test_rebuild_only_touches_its_own_project(cli, ledger_file, tmp_path, fingerprint):
    cli("ingest", str(ledger_file), "--project", "sample")
    other = tmp_path / "other.jsonl"
    other.write_text(
        json.dumps(sample_ledger()[2] | {"event_id": "evt_OTHERPROJECT"}) + "\n", encoding="utf-8"
    )
    cli("ingest", str(other), "--project", "other")
    before = fingerprint()
    assert cli("rebuild-state", "--project", "other")[0] == 0
    assert fingerprint() == before


@pytest.mark.parametrize("command", ["dump", "verify", "rebuild-state"])
def test_commands_need_an_existing_project(cli, command):
    code, _, err = cli(command, "--project", "nope")
    assert code == 1 and "does not exist" in err


def test_console_entrypoint(dsn):
    done = subprocess.run(
        [sys.executable, "-m", "architect.cli", "--database-url", dsn, "init-db"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert done.returncode == 0, done.stderr
    assert "schema is up to date" in done.stdout
