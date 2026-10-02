"""Read-only access to the frozen Phase 0 contracts and validators built from them."""

from __future__ import annotations

import json
import os
from collections.abc import Iterable
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError, best_match
from referencing import Registry, Resource

CONTRACTS_DIRNAME = "phase0-contracts"
SCHEMA_FILES = (
    "ledger_events.schema.json",
    "claim.schema.json",
    "system_model.schema.json",
    "agent_protocol.schema.json",
    "check_catalog.schema.json",
)


def contracts_dir() -> Path:
    """Locate phase0-contracts/: $ARCHITECT_CONTRACTS_DIR, else the nearest ancestor that has it."""
    env = os.environ.get("ARCHITECT_CONTRACTS_DIR")
    if env:
        return Path(env)
    for start in (Path(__file__).resolve().parent, Path.cwd().resolve()):
        for base in (start, *start.parents):
            candidate = base / CONTRACTS_DIRNAME
            if (candidate / SCHEMA_FILES[0]).is_file():
                return candidate
    raise FileNotFoundError(
        f"{CONTRACTS_DIRNAME}/ not found; set ARCHITECT_CONTRACTS_DIR to its location"
    )


@dataclass(frozen=True)
class SchemaError:
    """One schema violation, located in the instance it was found in."""

    message: str
    path: tuple[str | int, ...]


@dataclass(frozen=True)
class Contracts:
    schemas: dict[str, dict[str, Any]]
    event: Draft202012Validator
    claim: Draft202012Validator
    model_patch: Draft202012Validator
    objection: Draft202012Validator
    check: Draft202012Validator
    event_types: tuple[str, ...]
    _event_branch: dict[str, int]

    def event_error(self, event: Any) -> SchemaError | None:
        """The most relevant violation of ledger_events.schema.json, or None if valid."""
        errors = list(self.event.iter_errors(event))
        if not errors:
            return None
        direct = [e for e in errors if e.validator != "oneOf"]
        if direct:
            return _to_schema_error(best_match(direct))
        # Only the payload dispatch failed. The oneOf error itself says nothing useful, so
        # report what is wrong with the branch that governs this event's type.
        one_of = errors[0]
        branch = self._event_branch.get(event.get("type"))
        in_branch = [e for e in one_of.context or () if e.relative_schema_path[0] == branch]
        return _to_schema_error(best_match(in_branch) if in_branch else one_of)


def first_error(validator: Draft202012Validator, instance: Any) -> SchemaError | None:
    error = best_match(validator.iter_errors(instance))
    return _to_schema_error(error) if error is not None else None


def json_path(path: Iterable[str | int]) -> str:
    out = "$"
    for part in path:
        if isinstance(part, int):
            out += f"[{part}]"
        elif part.isidentifier():
            out += f".{part}"
        else:
            out += f"[{json.dumps(part)}]"
    return out


def _to_schema_error(error: ValidationError) -> SchemaError:
    return SchemaError(message=error.message, path=tuple(error.absolute_path))


@lru_cache(maxsize=1)
def load_contracts() -> Contracts:
    base = contracts_dir()
    schemas: dict[str, dict[str, Any]] = {}
    for name in SCHEMA_FILES:
        with open(base / name, encoding="utf-8") as f:
            schemas[name] = json.load(f)
    registry = Registry().with_resources(
        (schema["$id"], Resource.from_contents(schema)) for schema in schemas.values()
    )
    # Contracts v1.0: `format` is an assertion. date-time is only checked when
    # rfc3339-validator is importable, so refuse to run without it.
    formats = FormatChecker()
    if "date-time" not in formats.checkers:
        raise RuntimeError("date-time is not enforced: rfc3339-validator is not installed")

    def validator(name: str, pointer: str | None = None) -> Draft202012Validator:
        schema = schemas[name]
        if pointer is not None:
            schema = {"$ref": f"{schema['$id']}#{pointer}"}
        return Draft202012Validator(schema, registry=registry, format_checker=formats)

    ledger = schemas["ledger_events.schema.json"]
    return Contracts(
        schemas=schemas,
        event=validator("ledger_events.schema.json"),
        claim=validator("claim.schema.json"),
        model_patch=validator("agent_protocol.schema.json", "/$defs/ModelPatchProposal"),
        objection=validator("agent_protocol.schema.json", "/$defs/Objection"),
        check=validator("check_catalog.schema.json", "/$defs/Check"),
        event_types=tuple(ledger["$defs"]["EventType"]["enum"]),
        _event_branch={
            branch["properties"]["type"]["const"]: i for i, branch in enumerate(ledger["oneOf"])
        },
    )
