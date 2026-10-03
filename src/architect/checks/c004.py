"""C-004 · L0 · major · Requirement refs consistent with SATISFIES links.

For every component, its requirement_refs equal the requirements it SATISFIES. No
components: skipped.
"""

from __future__ import annotations

from typing import Any

from architect.checks.context import CheckContext
from architect.checks.graph import elements, links
from architect.checks.outcome import CheckOutcome, failed, passed, skipped
from architect.checks.waivers import check_waiver

CHECK_ID = "C-004"
USES = ("waivers",)


def check(model: dict[str, Any], ctx: CheckContext, params: dict[str, Any]) -> CheckOutcome:
    components = elements(model, "components")
    if not components:
        return skipped("no components")
    satisfies: dict[str, set[str]] = {}
    for link in links(model, "satisfies"):
        satisfies.setdefault(link["component"], set()).add(link["requirement"])
    mismatched: list[str] = []
    detail: dict[str, dict[str, list[str]]] = {}
    waived: dict[str, str] = {}
    for component in components:
        declared = set(component.get("requirement_refs", []))
        linked = satisfies.get(component["id"], set())
        if declared == linked:
            continue
        detail[component["id"]] = {
            "requirement_refs": sorted(declared),
            "satisfies": sorted(linked),
        }
        waiver = check_waiver(ctx, CHECK_ID, component["id"])
        if waiver is not None:
            waived[component["id"]] = waiver
        else:
            mismatched.append(component["id"])
    evidence = {"mismatches": detail, "waived": waived}
    return failed(mismatched, **evidence) if mismatched else passed(**evidence)
