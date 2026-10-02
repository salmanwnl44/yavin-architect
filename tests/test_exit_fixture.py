"""Exit tests 1 and 7, against the frozen Phase 0 fixture.

These need phase0-contracts/fixture/fixture_ledger.jsonl and fixture/replay.py. They fail,
rather than skip, while those files are missing: the milestone is not done without them.
"""

from __future__ import annotations

import subprocess
import sys

import pytest

from architect.cli import main
from architect.contracts import contracts_dir

FIXTURE_DIR = contracts_dir() / "fixture"
FIXTURE_LEDGER = FIXTURE_DIR / "fixture_ledger.jsonl"
REPLAY = FIXTURE_DIR / "replay.py"


@pytest.fixture
def ingested(dsn, capsys):
    assert FIXTURE_LEDGER.is_file(), f"the frozen fixture is missing: {FIXTURE_LEDGER}"
    code = main(["--database-url", dsn, "ingest", str(FIXTURE_LEDGER), "--project", "fix"])
    captured = capsys.readouterr()
    assert code == 0, captured.out + captured.err
    return dsn


# Exit test 1
def test_fixture_round_trips_through_the_arbiter_and_replays_green(ingested, tmp_path, capsys):
    assert REPLAY.is_file(), f"the reference replayer is missing: {REPLAY}"
    dump = tmp_path / "dump.jsonl"
    assert main(["--database-url", ingested, "dump", "--project", "fix", "-o", str(dump)]) == 0

    replay = subprocess.run(
        [sys.executable, str(REPLAY), str(dump)],
        cwd=contracts_dir().parent,
        capture_output=True,
        text=True,
        check=False,
    )
    assert replay.returncode == 0, replay.stdout + replay.stderr


# Exit test 7
def test_rebuild_state_after_the_fixture_ingest_reports_zero_diff(ingested, capsys):
    capsys.readouterr()
    code = main(["--database-url", ingested, "rebuild-state", "--project", "fix"])
    out = capsys.readouterr().out
    assert code == 0, out
    assert "diff vs previous state: empty" in out
