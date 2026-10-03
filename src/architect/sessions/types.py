"""Plain data shared by the workflow and the activities. Imported inside the Temporal sandbox,
so nothing here reaches a database, a clock or a model."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

PHASES = (
    "frame",
    "research",
    "model",
    "draft",
    "attack",
    "repair",
    "verify",
    "converge",
    "package",
)

# How a session ended. Every outcome but `failed` has a package.
OUTCOMES = (
    "completed",  # the gate is ALLOWED
    "completed_with_risks",  # stopped by the convergence rule with the gate BLOCKED
    "stopped_budget",
    "stopped_time",
    "cancelled",
    "failed",
)

# Where a session stands: running (see `phase`), paused, awaiting_approval, or final.
STATUSES = (
    "running",
    "paused",
    "awaiting_approval",
    "approved",
    "rejected",
    "stopped_budget",
    "stopped_time",
    "cancelled",
    "failed",
)

HUMAN_GATES = ("after_attack", "end")

WORKFLOW_NAME = "DesignSession"


@dataclass
class SessionInput:
    """What `start` hands the workflow. The preset is resolved by the client so the workflow
    never reads a file; `started_at` is the session clock's origin (RFC 3339), recorded on
    the claims the session commits."""

    project_id: str
    session_id: str
    brief: str
    preset: str
    started_at: str
    limits: dict[str, Any] = field(default_factory=dict)
    sources: list[str] = field(default_factory=list)


def spend_zero() -> dict[str, Any]:
    return {"tokens": 0, "usd": 0.0}
