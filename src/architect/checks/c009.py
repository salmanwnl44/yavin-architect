"""C-009 · L0 · critical · No open load-bearing assumptions.

Every load-bearing claim whose status as of the seq is still "assumed" fails, unless waived.
This check asserts that nothing is open, so no such claims is a pass.
"""

from __future__ import annotations

from typing import Any

from architect.checks.context import CheckContext
from architect.checks.outcome import CheckOutcome, failed, passed
from architect.checks.waivers import claim_waiver

CHECK_ID = "C-009"
USES = ("claims", "waivers")


def check(model: dict[str, Any], ctx: CheckContext, params: dict[str, Any]) -> CheckOutcome:
    open_assumptions: list[str] = []
    waived: dict[str, str] = {}
    load_bearing = [cid for cid, view in ctx.claims.items() if view["load_bearing"]]
    for cid in load_bearing:
        if ctx.claims[cid]["status"] != "assumed":
            continue
        waiver = claim_waiver(ctx, cid)
        if waiver is not None:
            waived[cid] = waiver
        else:
            open_assumptions.append(cid)
    evidence = {
        "load_bearing": {cid: ctx.claims[cid]["status"] for cid in load_bearing},
        "waived": waived,
    }
    return failed(open_assumptions, **evidence) if open_assumptions else passed(**evidence)
