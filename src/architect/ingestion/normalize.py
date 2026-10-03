"""Deterministic normalization of what a model proposed: predicates, entity ids, units,
quotes. Pure functions; the pipeline applies them identically in both passes."""

from __future__ import annotations

import base64
import hashlib
import json
import re
from typing import Any

from architect.checks import units

_NON_ALNUM = re.compile(r"[^a-z0-9]+")
_WS = re.compile(r"\s+")

# canonical unit per dimension the checks module knows
_CANONICAL = {"throughput": "qps", "ratio": "ratio", "time": "s"}


def predicate(text: str) -> str:
    """UPPER_SNAKE: 'reduces latency' -> 'REDUCES_LATENCY'."""
    words = re.findall(r"[A-Za-z0-9]+", text)
    return "_".join(w.upper() for w in words) or "RELATES_TO"


def slug(name: str) -> str:
    """A lowercase entity id: 'Lease Manager' -> 'lease-manager'."""
    return _NON_ALNUM.sub("-", name.lower()).strip("-") or "unnamed"


def whitespace(text: str) -> str:
    return _WS.sub(" ", text).strip()


def quote_found(quote: str, segment_text: str) -> bool:
    """R2: the quote appears verbatim in the segment, after whitespace normalization."""
    return bool(quote.strip()) and whitespace(quote) in whitespace(segment_text)


def magnitude(value: Any) -> dict[str, Any] | None:
    """{value, unit} with the unit converted to its dimension's canonical unit when known."""
    if not isinstance(value, dict) or "value" not in value or "unit" not in value:
        return None
    try:
        number = float(value["value"])
    except (TypeError, ValueError):
        return None
    unit = str(value["unit"]).strip()
    for dimension, convert in (
        ("throughput", units.throughput),
        ("ratio", units.ratio),
        ("time", units.seconds),
    ):
        try:
            return {"value": convert(number, unit), "unit": _CANONICAL[dimension]}
        except units.UnknownUnit:
            continue
    return {"value": number, "unit": unit}


def entity(ref: dict[str, Any]) -> dict[str, Any]:
    """A contract EntityRef: {entity_type, id} from a name, or {entity_type, literal}."""
    entity_type = slug(str(ref.get("entity_type", "entity")))
    if "literal" in ref and "name" not in ref:
        literal = ref["literal"]
        if not isinstance(literal, (str, int, float, bool)) or isinstance(literal, bool) and False:
            literal = str(literal)
        return {"entity_type": entity_type, "literal": literal}
    return {"entity_type": entity_type, "id": slug(str(ref.get("name", "")))}


def spo_key(subject: dict[str, Any], pred: str, obj: dict[str, Any]) -> str:
    """The normalized (subject, predicate, object) as one canonical string."""
    return json.dumps([subject, pred, obj], sort_keys=True, separators=(",", ":"))


def conditions(value: Any) -> dict[str, Any]:
    """Scalars only, keys slugged."""
    if not isinstance(value, dict):
        return {}
    out: dict[str, Any] = {}
    for key, item in value.items():
        if isinstance(item, (str, int, float, bool)):
            out[slug(str(key))] = item
    return dict(sorted(out.items()))


def typed_id(prefix: str, *parts: str) -> str:
    """'<prefix>_' + 26 base32 characters of sha256 over the parts: fits every contract pattern."""
    digest = hashlib.sha256("\x1f".join(parts).encode()).digest()
    return f"{prefix}_{base64.b32encode(digest).decode('ascii')[:26]}"
