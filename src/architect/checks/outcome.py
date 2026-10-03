"""What a check returns."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

STATUSES = ("pass", "fail", "error", "skipped")


@dataclass
class CheckOutcome:
    """status: pass (holds; waived elements count as holding and are listed in
    evidence.waived), fail (violated; element_refs are the violators), error (could not be
    evaluated; element_refs lack inputs and evidence.missing says what), skipped (nothing to
    evaluate; evidence.reason says why)."""

    status: str
    element_refs: list[str] = field(default_factory=list)
    evidence: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.status not in STATUSES:
            raise ValueError(f"not a check status: {self.status!r}")
        if self.status == "error" and "missing" not in self.evidence:
            raise ValueError("an error outcome must say what is missing")
        if self.status == "skipped" and "reason" not in self.evidence:
            raise ValueError("a skipped outcome must give a reason")


def passed(**evidence: Any) -> CheckOutcome:
    return CheckOutcome("pass", [], evidence)


def failed(element_refs: list[str], **evidence: Any) -> CheckOutcome:
    return CheckOutcome("fail", list(element_refs), evidence)


def errored(element_refs: list[str], missing: list[str], **evidence: Any) -> CheckOutcome:
    return CheckOutcome("error", list(element_refs), {"missing": list(missing), **evidence})


def skipped(reason: str, **evidence: Any) -> CheckOutcome:
    return CheckOutcome("skipped", [], {"reason": reason, **evidence})


def settle(
    failing: list[str], errors: list[str], missing: list[str], **evidence: Any
) -> CheckOutcome:
    """The verdict of a check that evaluated elements one by one: any failure is a fail
    (errors are listed in the evidence), otherwise any error is an error, otherwise a pass."""
    if failing:
        extra = {"errors": {"element_refs": errors, "missing": missing}} if errors else {}
        return failed(failing, **evidence, **extra)
    if errors:
        return errored(errors, missing, **evidence)
    return passed(**evidence)
