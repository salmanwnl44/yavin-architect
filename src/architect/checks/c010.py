"""C-010 · L1 · major · Single points of failure on the request path.

Every non-external component reachable from an ingress flow runs with at least two replicas,
or is waived. No ingress flows: skipped.
"""

from __future__ import annotations

from typing import Any

from architect.checks.context import CheckContext
from architect.checks.graph import by_id, ingress_flows, reachable_from_ingress, replicas
from architect.checks.outcome import CheckOutcome, failed, passed, skipped
from architect.checks.waivers import check_waiver

CHECK_ID = "C-010"
USES = ("waivers",)


def check(model: dict[str, Any], ctx: CheckContext, params: dict[str, Any]) -> CheckOutcome:
    if not ingress_flows(model):
        return skipped("no ingress flows")
    components = by_id(model, "components")
    spofs: list[str] = []
    detail: dict[str, dict[str, Any]] = {}
    waived: dict[str, str] = {}
    for cid in reachable_from_ingress(model):
        count, assumed = replicas(model, components[cid])
        detail[cid] = {"replicas": count}
        if assumed:
            detail[cid]["assumed"] = "replicas undeclared, assumed 1"
        if count >= 2:
            continue
        waiver = check_waiver(ctx, CHECK_ID, cid)
        if waiver is not None:
            waived[cid] = waiver
        else:
            spofs.append(cid)
    evidence = {"request_path": detail, "waived": waived}
    return failed(spofs, **evidence) if spofs else passed(**evidence)
