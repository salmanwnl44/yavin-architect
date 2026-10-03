"""C-002 · L0 · major · No orphan components.

Every component that is not external appears in links.satisfies as a component, or is
waived. No non-external components: skipped.
"""

from __future__ import annotations

from typing import Any

from architect.checks.context import CheckContext
from architect.checks.graph import elements, links
from architect.checks.outcome import CheckOutcome, failed, passed, skipped
from architect.checks.waivers import check_waiver

CHECK_ID = "C-002"
USES = ("waivers",)


def check(model: dict[str, Any], ctx: CheckContext, params: dict[str, Any]) -> CheckOutcome:
    internal = [c for c in elements(model, "components") if c["kind"] != "external"]
    if not internal:
        return skipped("no non-external components")
    satisfiers = {link["component"] for link in links(model, "satisfies")}
    orphans: list[str] = []
    waived: dict[str, str] = {}
    for component in internal:
        if component["id"] in satisfiers:
            continue
        waiver = check_waiver(ctx, CHECK_ID, component["id"])
        if waiver is not None:
            waived[component["id"]] = waiver
        else:
            orphans.append(component["id"])
    evidence = {"components": [c["id"] for c in internal], "waived": waived}
    return failed(orphans, **evidence) if orphans else passed(**evidence)
