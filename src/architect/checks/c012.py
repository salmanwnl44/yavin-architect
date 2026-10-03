"""C-012 · L0 · major · Backpressure on queues and fan-in.

A flow into a queue, into a component with two or more inbound flows, or over an async or
stream interface declares a backpressure_ref. No such flows: skipped.
"""

from __future__ import annotations

from typing import Any

from architect.checks.context import CheckContext
from architect.checks.graph import by_id, elements, inbound
from architect.checks.outcome import CheckOutcome, failed, passed, skipped
from architect.checks.waivers import check_waiver

CHECK_ID = "C-012"
USES = ("waivers",)


def check(model: dict[str, Any], ctx: CheckContext, params: dict[str, Any]) -> CheckOutcome:
    components, interfaces = by_id(model, "components"), by_id(model, "interfaces")
    applicable: dict[str, list[str]] = {}
    for flow in elements(model, "flows"):
        reasons = []
        target = components.get(flow["to"])
        if target is not None and target["kind"] == "queue":
            reasons.append("into a queue")
        if len(inbound(model, flow["to"])) >= 2:
            reasons.append("fan-in")
        via = interfaces.get(flow.get("via_interface"))
        if via is not None and via["style"] in ("async", "stream"):
            reasons.append(f"{via['style']} interface")
        if reasons:
            applicable[flow["id"]] = reasons
    if not applicable:
        return skipped("no flows into queues, fan-in or async interfaces")
    flows = by_id(model, "flows")
    offending: list[str] = []
    waived: dict[str, str] = {}
    for flow_id in applicable:
        if flows[flow_id].get("backpressure_ref"):
            continue
        waiver = check_waiver(ctx, CHECK_ID, flow_id)
        if waiver is not None:
            waived[flow_id] = waiver
        else:
            offending.append(flow_id)
    evidence = {"flows": applicable, "waived": waived}
    return failed(offending, **evidence) if offending else passed(**evidence)
