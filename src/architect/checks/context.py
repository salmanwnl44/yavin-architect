"""What a check may know besides the model: the project's knowledge as of one seq.

The runner builds it from the read models; checks only read it. Everything in it is plain
data, so a check's inputs can be hashed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# A claim as the context sees it: the claim as committed, and its status as of the seq.
# {"claim": {...}, "status": str, "load_bearing": bool, "first_seq": int}
ClaimView = dict[str, Any]

# element id -> (element type, the element)
ElementIndex = dict[str, tuple[str, dict[str, Any]]]

CONTEXT_FIELDS = ("requirements", "claims", "waivers", "elements")


@dataclass(frozen=True)
class CheckContext:
    as_of_seq: int
    # requirement ids: claims whose subject is a requirement and whose status as of the seq
    # is neither refuted nor retracted, in commit order
    requirements: tuple[str, ...] = ()
    # claim id -> ClaimView, for every claim committed by the seq
    claims: dict[str, ClaimView] = field(default_factory=dict)
    # waiver target_ref -> waiver_id, for waivers signed by the seq (the latest one wins)
    waivers: dict[str, str] = field(default_factory=dict)
    # the model version's elements by id
    elements: ElementIndex = field(default_factory=dict)

    def subset(self, names: tuple[str, ...]) -> dict[str, Any]:
        """The parts of the context a check declares it reads, as plain data for hashing."""
        out: dict[str, Any] = {}
        for name in names:
            if name not in CONTEXT_FIELDS:
                raise ValueError(f"not a context field: {name}")
            value = getattr(self, name)
            out[name] = list(value) if isinstance(value, tuple) else value
        return out


def index_elements(model: dict[str, Any]) -> ElementIndex:
    """Every element of a model by id, in model order. A duplicate id keeps its first element;
    C-013 reports duplicates."""
    index: ElementIndex = {}
    for element_type, elements in model.get("elements", {}).items():
        for element in elements:
            index.setdefault(element["id"], (element_type, element))
    return index
