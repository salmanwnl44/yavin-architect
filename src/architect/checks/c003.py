"""C-003 · L0 · critical · Interface binding integrity.

(a) A flow's via_interface exists and is listed by the flow's target component.
(b) A flow into a service or gateway declares via_interface.
(c) Every interface's contract_ref is a typed reference: openapi:, proto:, jsonschema: or
    asyncapi:, with an optional #fragment.
No flows and no interfaces: skipped. Diffing producer and consumer schemas needs the contract
files themselves, which nothing ingests yet; evidence.limitations says so.
"""

from __future__ import annotations

import re
from typing import Any

from architect.checks.context import CheckContext
from architect.checks.graph import by_id, elements
from architect.checks.outcome import CheckOutcome, failed, passed, skipped
from architect.checks.waivers import check_waiver

CHECK_ID = "C-003"
USES = ("waivers",)

CONTRACT_REF = re.compile(r"^(openapi|proto|jsonschema|asyncapi):[^#\s]+(#\S*)?$")
BOUND_KINDS = ("service", "gateway")
LIMITATIONS = ["producer/consumer schema diffing needs ingested contract files"]


def check(model: dict[str, Any], ctx: CheckContext, params: dict[str, Any]) -> CheckOutcome:
    flows, interfaces = elements(model, "flows"), elements(model, "interfaces")
    if not flows and not interfaces:
        return skipped("no flows and no interfaces", limitations=LIMITATIONS)
    components, by_interface = by_id(model, "components"), by_id(model, "interfaces")
    violations: list[dict[str, str]] = []
    waived: dict[str, str] = {}

    def violate(element_id: str, rule: str, detail: str) -> None:
        waiver = check_waiver(ctx, CHECK_ID, element_id)
        if waiver is not None:
            waived[element_id] = waiver
        else:
            violations.append({"element": element_id, "rule": rule, "detail": detail})

    for flow in flows:
        target = components.get(flow["to"])
        via = flow.get("via_interface")
        if via is not None:
            if via not in by_interface:
                violate(flow["id"], "a", f"via_interface {via} does not exist")
            elif target is not None and via not in target.get("interfaces", []):
                violate(flow["id"], "a", f"{flow['to']} does not list interface {via}")
        elif target is not None and target["kind"] in BOUND_KINDS:
            violate(
                flow["id"], "b", f"flow into {target['kind']} {flow['to']} has no via_interface"
            )
    for interface in interfaces:
        if not CONTRACT_REF.match(interface["contract_ref"]):
            violate(
                interface["id"], "c", f"contract_ref {interface['contract_ref']!r} is not typed"
            )

    offending = list(dict.fromkeys(v["element"] for v in violations))
    evidence = {"violations": violations, "waived": waived, "limitations": LIMITATIONS}
    return failed(offending, **evidence) if offending else passed(**evidence)
