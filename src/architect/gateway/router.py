"""Which models may serve a request: the tier's ordered candidates minus excluded families."""

from __future__ import annotations

from architect.gateway.config import Candidate, GatewayConfig
from architect.gateway.errors import NoEligibleModel


def candidates(config: GatewayConfig, tier: str, exclude_families: list[str]) -> list[Candidate]:
    """The tier's candidates in fallback order, without the excluded families."""
    if tier not in config.tiers:
        raise NoEligibleModel(f"unknown tier {tier!r}")
    eligible = [c for c in config.tiers[tier] if c.family not in exclude_families]
    if not eligible:
        raise NoEligibleModel(
            f"every candidate of {tier} is in an excluded family {sorted(exclude_families)}"
        )
    return eligible
