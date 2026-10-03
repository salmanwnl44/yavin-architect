"""C-006 · L1 · critical · Availability composition.

For every availability SLO: the components on its path (the sync closure of the component it
applies to, or of the target of the flow it applies to) each carry "<id>.availability"; with
replicas r each contributes 1 - (1 - a)^r, and the serial composition over the path must reach
the target. No availability SLOs: skipped.
"""

from __future__ import annotations

from typing import Any

from architect.checks import units
from architect.checks.context import CheckContext
from architect.checks.graph import (
    DEPRECATED_CONVENTION,
    by_id,
    capacity_binding,
    elements,
    replicas,
    sync_closure,
)
from architect.checks.outcome import CheckOutcome, settle, skipped
from architect.checks.waivers import check_waiver

CHECK_ID = "C-006"
USES = ("waivers",)
ASSUMPTIONS = ["independent failures", "serial composition over sync dependencies"]


def check(model: dict[str, Any], ctx: CheckContext, params: dict[str, Any]) -> CheckOutcome:
    slos = [slo for slo in elements(model, "slos") if slo["metric"] == "availability"]
    if not slos:
        return skipped("no availability SLOs")
    components, flows = by_id(model, "components"), by_id(model, "flows")
    failing, errors, missing = [], [], []
    detail: dict[str, dict[str, Any]] = {}
    waived: dict[str, str] = {}
    by_convention = False
    for slo in slos:
        sid = slo["id"]
        waiver = check_waiver(ctx, CHECK_ID, sid)
        if waiver is not None:
            waived[sid] = waiver
            continue
        target_element = slo["applies_to"]
        if target_element in flows:
            target_element = flows[target_element]["to"]
        if target_element not in components:
            errors.append(sid)
            missing.append(f"{sid}: applies_to {slo['applies_to']} is not a component or flow")
            continue
        try:
            target = units.ratio(slo["target"], slo["unit"])
        except units.UnknownUnit as unknown:
            errors.append(sid)
            missing.append(f"{sid}: target has {unknown}")
            continue
        path: dict[str, dict[str, Any]] = {}
        lacking: list[str] = []
        composite = 1.0
        for cid in sync_closure(model, target_element):
            param, deprecated = capacity_binding(model, cid, "availability")
            by_convention = by_convention or deprecated
            count, assumed = replicas(model, components[cid])
            entry: dict[str, Any] = {"replicas": count}
            if assumed:
                entry["assumed"] = "replicas undeclared, assumed 1"
            if param is None:
                lacking.append(f"{sid}: no capacity param {cid}.availability")
            else:
                try:
                    a = units.ratio(param["value"], param["unit"])
                except units.UnknownUnit as unknown:
                    lacking.append(f"{sid}: capacity param {param['name']} has {unknown}")
                else:
                    a_eff = 1 - (1 - a) ** count
                    entry |= {"a": a, "a_eff": a_eff}
                    composite *= a_eff
            path[cid] = entry
        entry = {"components": path, "target": target, "assumptions": ASSUMPTIONS}
        if lacking:
            errors.append(sid)
            missing.extend(lacking)
            entry["composite"] = None
        else:
            entry["composite"] = composite
            if composite < target:
                failing.append(sid)
        detail[sid] = entry
    extra = {"deprecated": DEPRECATED_CONVENTION} if by_convention else {}
    return settle(failing, errors, missing, slos=detail, waived=waived, **extra)
