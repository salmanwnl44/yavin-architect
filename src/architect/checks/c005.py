"""C-005 · L1 · critical · Capacity headroom.

For every non-external component with inbound flows: the capacity param "<id>.max_qps" is at
least headroom (param, default 1.5) times the sum of the inbound peak rates. A component
without that param, or with one in an unknown unit, cannot be evaluated. No applicable
components: skipped.
"""

from __future__ import annotations

from typing import Any

from architect.checks import units
from architect.checks.context import CheckContext
from architect.checks.graph import DEPRECATED_CONVENTION, capacity_binding, elements, inbound
from architect.checks.outcome import CheckOutcome, settle, skipped
from architect.checks.waivers import check_waiver

CHECK_ID = "C-005"
USES = ("waivers",)
DEFAULT_HEADROOM = 1.5


def check(model: dict[str, Any], ctx: CheckContext, params: dict[str, Any]) -> CheckOutcome:
    headroom = float(params.get("headroom", DEFAULT_HEADROOM))
    applicable = [
        (c, inbound(model, c["id"]))
        for c in elements(model, "components")
        if c["kind"] != "external" and inbound(model, c["id"])
    ]
    if not applicable:
        return skipped("no non-external component has inbound flows")
    failing, errors, missing = [], [], []
    detail: dict[str, dict[str, Any]] = {}
    waived: dict[str, str] = {}
    by_convention = False
    for component, flows in applicable:
        cid = component["id"]
        waiver = check_waiver(ctx, CHECK_ID, cid)
        if waiver is not None:
            waived[cid] = waiver
            continue
        inbound_qps = sum(float(flow["rate"]["peak_qps"]) for flow in flows)
        required = headroom * inbound_qps
        entry: dict[str, Any] = {"inbound_qps": inbound_qps, "required": required}
        param, deprecated = capacity_binding(model, cid, "max_qps")
        by_convention = by_convention or deprecated
        if param is None:
            errors.append(cid)
            missing.append(f"{cid}: no capacity param {cid}.max_qps")
            entry["capacity"] = None
        else:
            try:
                capacity = units.throughput(param["value"], param["unit"])
            except units.UnknownUnit as unknown:
                errors.append(cid)
                missing.append(f"{cid}: capacity param {param['name']} has {unknown}")
                entry |= {"capacity": None, "unit": param["unit"]}
            else:
                entry |= {"capacity": capacity, "unit": param["unit"]}
                if capacity < required:
                    failing.append(cid)
        detail[cid] = entry
    extra = {"deprecated": DEPRECATED_CONVENTION} if by_convention else {}
    return settle(
        failing, errors, missing, headroom=headroom, components=detail, waived=waived, **extra
    )
