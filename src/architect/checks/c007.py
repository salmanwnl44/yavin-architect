"""C-007 · L0 · critical · Stateful durability declared.

Every stateful component declares a durability_class. durable: recovery.rpo_s, rto_s and path.
rebuildable: recovery.rto_s and path (how it is rebuilt, how long that takes). ephemeral:
nothing more; state loss is accepted. No stateful components: skipped.
"""

from __future__ import annotations

from typing import Any

from architect.checks.context import CheckContext
from architect.checks.graph import elements
from architect.checks.outcome import CheckOutcome, failed, passed, skipped
from architect.checks.waivers import check_waiver

CHECK_ID = "C-007"
USES = ("waivers",)
NEEDS = {
    "durable": ("recovery.rpo_s", "recovery.rto_s", "recovery.path"),
    "rebuildable": ("recovery.rto_s", "recovery.path"),
    "ephemeral": (),
}


def check(model: dict[str, Any], ctx: CheckContext, params: dict[str, Any]) -> CheckOutcome:
    stateful = [c for c in elements(model, "components") if c.get("stateful")]
    if not stateful:
        return skipped("no stateful components")
    lacking: list[str] = []
    detail: dict[str, Any] = {}
    waived: dict[str, str] = {}
    for component in stateful:
        cid = component["id"]
        durability = component.get("durability_class")
        if durability is None:
            fields = ["durability_class"]
        else:
            recovery = component.get("recovery", {})
            fields = [
                field for field in NEEDS[durability] if recovery.get(field.split(".")[1]) is None
            ]
        if durability == "ephemeral":
            detail[cid] = {"durability_class": durability, "note": "state loss accepted"}
        else:
            detail[cid] = {"durability_class": durability, "missing": fields}
        if not fields:
            continue
        waiver = check_waiver(ctx, CHECK_ID, cid)
        if waiver is not None:
            waived[cid] = waiver
        else:
            lacking.append(cid)
    evidence = {"components": detail, "waived": waived}
    return failed(lacking, **evidence) if lacking else passed(**evidence)
