"""Waiver matching, the one place a target_ref is interpreted.

A waiver's target_ref waives:
- "<check_id>"              the whole check for the model (every element),
- "<check_id>:<element_id>" that check for that element,
- "<requirement_id>"        that requirement, for C-001 only,
- "<claim_id>"              that claim, for C-009 only.
Only the waivers in the context count (the runner keeps those signed by as_of_seq). A waived
element goes to evidence.waived, never to element_refs.
"""

from __future__ import annotations

from architect.checks.context import CheckContext


def check_waiver(ctx: CheckContext, check_id: str, element_id: str | None = None) -> str | None:
    """The waiver_id covering a check for an element (or the whole check), or None."""
    whole = ctx.waivers.get(check_id)
    if whole is not None:
        return whole
    if element_id is not None:
        return ctx.waivers.get(f"{check_id}:{element_id}")
    return None


def requirement_waiver(ctx: CheckContext, requirement_id: str) -> str | None:
    """C-001: a waiver naming the requirement itself, or the check for it."""
    return check_waiver(ctx, "C-001", requirement_id) or ctx.waivers.get(requirement_id)


def claim_waiver(ctx: CheckContext, claim_id: str) -> str | None:
    """C-009: a waiver naming the claim itself, or the check for it."""
    return check_waiver(ctx, "C-009", claim_id) or ctx.waivers.get(claim_id)
