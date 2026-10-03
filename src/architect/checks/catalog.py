"""The check catalog (contracts: check_catalog.schema.json) and the registry of built-in
checks, plus the pure core of a run: evaluate one catalog entry, hash its inputs.

A catalog entry whose implementation is not registered loads fine and evaluates to
skipped / not_implemented: a check that graduated from an objection can be listed before it
is written.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from types import ModuleType
from typing import Any

from architect.checks import (
    c001,
    c002,
    c003,
    c004,
    c005,
    c006,
    c007,
    c008,
    c009,
    c010,
    c011,
    c012,
    c013,
)
from architect.checks.context import CheckContext
from architect.checks.outcome import CheckOutcome, skipped
from architect.contracts import first_error, json_path, load_contracts

Entry = dict[str, Any]

# implementation ref -> the module holding `check`, CHECK_ID and USES
REGISTRY: dict[str, ModuleType] = {
    f"architect.checks.{module.__name__.rsplit('.', 1)[-1]}": module
    for module in (c001, c002, c003, c004, c005, c006, c007, c008, c009, c010, c011, c012, c013)
}


class CatalogError(ValueError):
    """The catalog file does not validate against check_catalog.schema.json."""


@dataclass(frozen=True)
class Catalog:
    version: str
    checks: tuple[Entry, ...]

    def entry(self, check_id: str) -> Entry | None:
        return next((entry for entry in self.checks if entry["id"] == check_id), None)


def bundled_catalog_path() -> Path:
    return Path(str(resources.files("architect.checks").joinpath("catalog.json")))


def load_catalog(path: Path | None = None) -> Catalog:
    """Load and validate a catalog; the bundled one when no path is given."""
    source = path or bundled_catalog_path()
    data = json.loads(source.read_text(encoding="utf-8"))
    error = first_error(load_contracts().catalog, data)
    if error is not None:
        raise CatalogError(f"{source}: {json_path(error.path)}: {error.message}")
    ids = [entry["id"] for entry in data["checks"]]
    if len(ids) != len(set(ids)):
        raise CatalogError(f"{source}: a check id appears twice")
    return Catalog(data["catalog_version"], tuple(data["checks"]))


def implementation(entry: Entry) -> ModuleType | None:
    """The registered module for a catalog entry, or None when none implements it."""
    spec = entry["implementation"]
    if spec["kind"] != "builtin":
        return None
    return REGISTRY.get(spec["ref"])


def evaluate(entry: Entry, model: dict[str, Any], ctx: CheckContext) -> CheckOutcome:
    """Run one catalog entry. Pure: the same inputs give the same outcome."""
    module = implementation(entry)
    if module is None:
        return skipped("not_implemented", implementation=entry["implementation"])
    return module.check(model, ctx, entry.get("params", {}))


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def inputs_hash(entry: Entry, model: dict[str, Any], ctx: CheckContext) -> str:
    """sha256 over the model, the context the check reads, its params and its version."""
    module = implementation(entry)
    uses = module.USES if module is not None else ()
    payload = {
        "model": model,
        "context": ctx.subset(uses),
        "params": entry.get("params", {}),
        "check_version": entry["version"],
    }
    return hashlib.sha256(canonical(payload)).hexdigest()
