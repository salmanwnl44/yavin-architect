"""Structured output: the provider is asked for the schema natively, and the gateway always
validates what came back. Nothing unvalidated is ever returned as `parsed`."""

from __future__ import annotations

import json
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import best_match


def parse(text: str, schema: dict[str, Any]) -> tuple[Any | None, str | None]:
    """(the validated object, None) or (None, why it failed)."""
    try:
        value = json.loads(text)
    except json.JSONDecodeError as error:
        return None, f"not JSON: {error.msg} at position {error.pos}"
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    error = best_match(validator.iter_errors(value))
    if error is not None:
        where = "/".join(str(p) for p in error.absolute_path) or "(root)"
        return None, f"schema violation at {where}: {error.message}"
    return value, None


def correction(error: str) -> str:
    """The next user turn after an invalid attempt."""
    return (
        f"Your previous output failed validation: {error}. "
        "Reply again with only a JSON document that satisfies the schema."
    )
