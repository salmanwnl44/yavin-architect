"""What phase0-contracts/fixture/replay.py folds the fixture into, without modifying it."""

from __future__ import annotations

import contextlib
import io
import json
import sys
from typing import Any

from architect.contracts import contracts_dir

FIXTURE_DIR = contracts_dir() / "fixture"
FIXTURE_LEDGER = FIXTURE_DIR / "fixture_ledger.jsonl"
REPLAY = FIXTURE_DIR / "replay.py"


def fixture_events() -> list[dict[str, Any]]:
    text = FIXTURE_LEDGER.read_text(encoding="utf-8")
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def reference() -> dict[str, Any]:
    """replay.py's namespace after it has replayed the fixture: final_model, status, edges."""
    namespace: dict[str, Any] = {"__name__": "__main__", "__file__": str(REPLAY)}
    argv, sys.argv = sys.argv, [str(REPLAY)]
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            try:
                exec(compile(REPLAY.read_text(encoding="utf-8"), str(REPLAY), "exec"), namespace)
            except SystemExit as stop:
                assert not stop.code, "replay.py did not replay the fixture green"
    finally:
        sys.argv = argv
    return namespace


def normalized(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))
