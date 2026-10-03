"""The architecture rules that can be checked by reading the source."""

from __future__ import annotations

import re
from pathlib import Path

import architect

SRC = Path(architect.__file__).parent
WRITES_EVENTS = re.compile(r"\b(INSERT\s+INTO|UPDATE|DELETE\s+FROM|TRUNCATE)\s+events\b", re.I)


def sources() -> dict[str, str]:
    return {path.name: path.read_text(encoding="utf-8") for path in SRC.glob("*.py")}


def test_only_the_arbiter_writes_events():
    writers = {name for name, text in sources().items() if WRITES_EVENTS.search(text)}
    assert writers == {"arbiter.py"}


def test_no_code_path_updates_or_deletes_events():
    for name, text in sources().items():
        for match in WRITES_EVENTS.finditer(text):
            assert match.group(1).upper().startswith("INSERT"), f"{name}: {match.group(0)}"


# M3: checks are pure. Only the runner may reach a database, the Arbiter or the clock.
CHECKS = SRC / "checks"
IMPURE = re.compile(
    r"^\s*(?:from|import)\s+("
    r"architect\.(?:api|arbiter|cli|db|ledger|projections|projector|readmodel|rebuild|rules|state)"
    r"|psycopg|psycopg_pool|fastapi|httpx|socket|urllib|random|time|datetime|os|subprocess"
    r")\b",
    re.M,
)


def test_checks_import_nothing_impure():
    modules = sorted(path.name for path in CHECKS.glob("*.py") if path.name != "runner.py")
    assert "c001.py" in modules and "catalog.py" in modules
    for name in modules:
        text = (CHECKS / name).read_text(encoding="utf-8")
        found = IMPURE.search(text)
        assert found is None, f"checks/{name} imports {found.group(1)}"
