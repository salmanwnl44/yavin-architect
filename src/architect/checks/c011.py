"""C-011 · L0 · major · Idempotency for at-least-once delivery.

Every async or stream interface is idempotent. No such interfaces: skipped.
"""

from __future__ import annotations

from typing import Any

from architect.checks.context import CheckContext
from architect.checks.graph import elements
from architect.checks.outcome import CheckOutcome, failed, passed, skipped
from architect.checks.waivers import check_waiver

CHECK_ID = "C-011"
USES = ("waivers",)
AT_LEAST_ONCE = ("async", "stream")


def check(model: dict[str, Any], ctx: CheckContext, params: dict[str, Any]) -> CheckOutcome:
    applicable = [i for i in elements(model, "interfaces") if i["style"] in AT_LEAST_ONCE]
    if not applicable:
        return skipped("no async or stream interfaces")
    offending: list[str] = []
    waived: dict[str, str] = {}
    for interface in applicable:
        if interface["idempotent"] is True:
            continue
        waiver = check_waiver(ctx, CHECK_ID, interface["id"])
        if waiver is not None:
            waived[interface["id"]] = waiver
        else:
            offending.append(interface["id"])
    evidence = {
        "interfaces": {
            i["id"]: {"style": i["style"], "idempotent": i["idempotent"]} for i in applicable
        },
        "waived": waived,
    }
    return failed(offending, **evidence) if offending else passed(**evidence)
