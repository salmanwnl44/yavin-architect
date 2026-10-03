"""C-008 · L0 · critical · Trust-boundary crossings and sensitive data.

A flow crosses a trust boundary when exactly one of its ends is a member. Every crossing flow
needs input_validation, encryption_in_transit and an interface with authn (via_interface set,
authn not "none"). Any flow carrying secret or pii data needs encryption_in_transit. No trust
boundaries and no secret/pii flows: skipped.
"""

from __future__ import annotations

from typing import Any

from architect.checks.context import CheckContext
from architect.checks.graph import by_id, elements
from architect.checks.outcome import CheckOutcome, failed, passed, skipped
from architect.checks.waivers import check_waiver

CHECK_ID = "C-008"
USES = ("waivers",)
SENSITIVE = ("secret", "pii")


def check(model: dict[str, Any], ctx: CheckContext, params: dict[str, Any]) -> CheckOutcome:
    boundaries = elements(model, "trust_boundaries")
    flows = elements(model, "flows")
    sensitive = [flow for flow in flows if flow["data_class"] in SENSITIVE]
    if not boundaries and not sensitive:
        return skipped("no trust boundaries and no secret or pii flows")
    interfaces = by_id(model, "interfaces")
    detail: dict[str, dict[str, Any]] = {}
    offending: list[str] = []
    waived: dict[str, str] = {}
    for flow in flows:
        crossed = [
            tb["id"]
            for tb in boundaries
            if (flow["from"] in tb["member_elements"]) != (flow["to"] in tb["member_elements"])
        ]
        missing: list[str] = []
        if crossed:
            if not flow.get("input_validation"):
                missing.append("input_validation")
            if flow.get("encryption_in_transit") is not True:
                missing.append("encryption_in_transit")
            via = flow.get("via_interface")
            if via is None or via not in interfaces:
                missing.append("authn_undeclared")
            elif interfaces[via]["authn"] == "none":
                missing.append("authn_none")
        if flow["data_class"] in SENSITIVE and flow.get("encryption_in_transit") is not True:
            if "encryption_in_transit" not in missing:
                missing.append("encryption_in_transit")
        if not crossed and flow["data_class"] not in SENSITIVE:
            continue
        detail[flow["id"]] = {
            "boundaries": crossed,
            "data_class": flow["data_class"],
            "missing": missing,
        }
        if not missing:
            continue
        waiver = check_waiver(ctx, CHECK_ID, flow["id"])
        if waiver is not None:
            waived[flow["id"]] = waiver
        else:
            offending.append(flow["id"])
    evidence = {"flows": detail, "waived": waived}
    return failed(offending, **evidence) if offending else passed(**evidence)
