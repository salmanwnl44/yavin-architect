"""C-001 · L0 · critical · Requirement coverage.

Every requirement in the context is satisfied by at least one links.satisfies entry, or
waived. No requirements: skipped.
"""

from __future__ import annotations

from typing import Any

from architect.checks.context import CheckContext
from architect.checks.graph import links
from architect.checks.outcome import CheckOutcome, failed, passed, skipped
from architect.checks.waivers import requirement_waiver

CHECK_ID = "C-001"
USES = ("requirements", "waivers")


def check(model: dict[str, Any], ctx: CheckContext, params: dict[str, Any]) -> CheckOutcome:
    if not ctx.requirements:
        return skipped("no requirements in context")
    satisfied = {link["requirement"] for link in links(model, "satisfies")}
    uncovered: list[str] = []
    waived: dict[str, str] = {}
    for requirement in ctx.requirements:
        if requirement in satisfied:
            continue
        waiver = requirement_waiver(ctx, requirement)
        if waiver is not None:
            waived[requirement] = waiver
        else:
            uncovered.append(requirement)
    evidence = {
        "requirements": list(ctx.requirements),
        "satisfied": [r for r in ctx.requirements if r in satisfied],
        "waived": waived,
    }
    return failed(uncovered, **evidence) if uncovered else passed(**evidence)
