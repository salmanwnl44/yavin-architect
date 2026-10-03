"""Grades and confidence (Stage 6): computed in the projection, never written to the ledger.

grade: quarantined (proposed, never committed) | unverified (committed, one source) |
design_grade (committed and corroborated by a second distinct source, or measured/observed,
or referenced by a passing verification event).

confidence: the declared scoring function v1 in config/confidence.yaml. Inputs are stored
next to the value. Monotonic: more corroboration, agreement or verification never lowers it.
Calibration against labeled data is M14's job.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any

from architect.ingestion.config import ConfidenceWeights, load_confidence_weights


@lru_cache(maxsize=1)
def weights() -> ConfidenceWeights:
    return load_confidence_weights()


def grade(status: str, corroborations: int, verification_events: int) -> str:
    if corroborations >= 1 or status in ("measured", "observed") or verification_events >= 1:
        return "design_grade"
    return "unverified"


def confidence(
    *,
    source_tier: str,
    corroborations: int,
    two_pass_agreement: bool,
    verification_events: int,
    recency_decay: float = 1.0,
    w: ConfidenceWeights | None = None,
) -> tuple[float, dict[str, Any]]:
    """(value, the inputs it was computed from)."""
    w = w or weights()
    base = w.base_by_source_tier.get(source_tier, min(w.base_by_source_tier.values(), default=0.4))
    value = base
    value += min(corroborations * w.per_corroboration, w.max_corroboration)
    value += w.two_pass_agreement if two_pass_agreement else 0.0
    value += min(verification_events * w.per_verification_event, w.max_verification)
    value = max(0.0, min(1.0, value)) * recency_decay
    inputs = {
        "source_tier": source_tier,
        "corroborations": corroborations,
        "two_pass_agreement": two_pass_agreement,
        "verification_events": verification_events,
        "recency_decay": recency_decay,
        "function": f"confidence-v{w.version}",
    }
    return round(value, 4), inputs
