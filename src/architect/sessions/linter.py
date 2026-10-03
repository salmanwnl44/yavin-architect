"""The requirement linter (spec §13, frame): deterministic, no model.

A requirement is measurable when it names a metric, a numeric target and a unit the checks
engine knows (architect.checks.units). The Architect may supply the three fields; when it
does not, the number and unit are parsed from the requirement's own text and the metric is
inferred from its vocabulary. Anything short of all three is "unmeasurable" and becomes an
open risk, never a silent pass.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from architect.checks import units
from architect.ingestion.normalize import slug

KNOWN_UNITS: frozenset[str] = (
    frozenset(units.THROUGHPUT) | frozenset(units.RATIO) | frozenset(units.TIME)
)

# metric name -> words that imply it
METRIC_VOCABULARY: dict[str, tuple[str, ...]] = {
    "latency": ("latency", "p99", "p95", "p50", "response time", "round trip"),
    "throughput": ("throughput", "per second", "/s", "qps", "rps", "msg/s", "events/s"),
    "availability": ("availability", "uptime", "available"),
    "error_rate": ("error rate", "errors"),
    "rpo": ("rpo", "recovery point"),
    "rto": ("rto", "recovery time"),
    "durability": ("durability", "durable"),
}

_UNIT_ALTERNATION = "|".join(re.escape(unit) for unit in sorted(KNOWN_UNITS, key=len, reverse=True))
NUMBER_WITH_UNIT = re.compile(
    rf"(?<![\w.])(\d+(?:\.\d+)?)\s*({_UNIT_ALTERNATION})(?![\w/])", re.IGNORECASE
)


@dataclass(frozen=True)
class LintResult:
    requirement_id: str
    text: str
    measurable: bool
    metric: str | None
    target: float | None
    unit: str | None
    reason: str  # why it is or is not measurable, for the open risk


def requirement_id(slug_or_text: str) -> str:
    """`req_<slug>`: the subject id of the requirement claim and the target of SATISFIES links."""
    return f"req_{slug(slug_or_text)}"


def infer_metric(text: str) -> str | None:
    lowered = text.lower()
    for metric, words in METRIC_VOCABULARY.items():
        if any(word in lowered for word in words):
            return metric
    return None


def parse_target(text: str) -> tuple[float, str] | None:
    found = NUMBER_WITH_UNIT.search(text)
    if found is None:
        return None
    unit = found.group(2)
    canonical = next((u for u in KNOWN_UNITS if u.lower() == unit.lower()), unit)
    return float(found.group(1)), canonical


def lint(requirement: dict[str, Any]) -> LintResult:
    """Apply the rule to one requirement as the Architect returned it:
    {slug, text, metric?, target?, unit?}."""
    text = str(requirement.get("text", "")).strip()
    rid = requirement_id(str(requirement.get("slug") or text))
    metric = (requirement.get("metric") or "").strip().lower() or infer_metric(text)
    target = requirement.get("target")
    unit = (requirement.get("unit") or "").strip() or None
    if not isinstance(target, (int, float)) or isinstance(target, bool):
        parsed = parse_target(text)
        if parsed is not None:
            target, parsed_unit = parsed
            unit = unit or parsed_unit
        else:
            target = None
    missing = []
    if not metric:
        missing.append("metric")
    if target is None:
        missing.append("numeric target")
    if unit is None:
        missing.append("unit")
    elif unit not in KNOWN_UNITS:
        return LintResult(
            rid,
            text,
            False,
            metric,
            float(target) if target is not None else None,
            unit,
            f"unit {unit!r} is not a known unit ({', '.join(sorted(KNOWN_UNITS))})",
        )
    if missing:
        return LintResult(
            rid,
            text,
            False,
            metric,
            float(target) if target is not None else None,
            unit,
            "unmeasurable: no " + ", no ".join(missing),
        )
    return LintResult(
        rid, text, True, metric, float(target), unit, f"{metric} target {target:g} {unit}"
    )
