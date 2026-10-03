"""The one fold of the System Model: how a model version follows from the one before it.

Pure functions over model dicts, with no database and no I/O. The Arbiter uses them to refuse
a patch that cannot be applied or does not leave a valid model; the projector uses them to
materialize every version. Both go through here so the two can never drift apart.

`apply_patch` follows `apply_patch` in phase0-contracts/fixture/replay.py op for op. It
differs where replay.py is lenient: updating or removing an element that is not in the model
is an error here.
"""

from __future__ import annotations

import copy
from typing import Any

Model = dict[str, Any]


class PatchError(Exception):
    """A patch op cannot be applied. `index` is the op's position in `patch["ops"]`."""

    def __init__(self, index: int, message: str) -> None:
        super().__init__(f"ops[{index}]: {message}")
        self.index = index
        self.message = message


class PatchTargetMissing(PatchError):
    """update_element or remove_element names an element the model does not have."""


class MalformedOp(PatchError):
    """An op lacks a field its kind needs (the patch schema only requires `op`)."""


def empty_model(project_id: str, version_id: str) -> Model:
    """A genesis version: no elements, no links."""
    return {"version_id": version_id, "project_id": project_id, "elements": {}, "links": {}}


def child_of(parent: Model, version_id: str) -> Model:
    """A version created from a parent without a patch: the parent's model under a new id."""
    model = copy.deepcopy(parent)
    model["version_id"] = version_id
    return model


def apply_patch(base: Model, patch: dict[str, Any], version_id: str | None = None) -> Model:
    """The model `patch` produces from `base`, which is left untouched.

    The result carries `version_id`, or keeps the base's when none is given (a proposal has
    no version of its own yet).
    """
    model = child_of(base, version_id or base["version_id"])
    elements, links = model["elements"], model["links"]
    for i, op in enumerate(patch["ops"]):
        try:
            kind = op["op"]
            if kind == "add_element":
                elements.setdefault(op["element_type"], []).append(op["element"])
            elif kind == "update_element":
                target = op["element_id"]
                for element in elements.get(op["element_type"], []):
                    if element.get("id") == target:
                        element.update(op["element"])
                        break
                else:
                    raise PatchTargetMissing(
                        i, f"update_element: no {op['element_type']} element {target}"
                    )
            elif kind == "remove_element":
                target = op["element_id"]
                rows = elements.get(op["element_type"], [])
                kept = [element for element in rows if element.get("id") != target]
                if len(kept) == len(rows):
                    raise PatchTargetMissing(
                        i, f"remove_element: no {op['element_type']} element {target}"
                    )
                elements[op["element_type"]] = kept
            elif kind == "add_link":
                links.setdefault(op["link_type"], []).append(op["link"])
            elif kind == "remove_link":
                rows = links.get(op["link_type"], [])
                links[op["link_type"]] = [link for link in rows if link != op["link"]]
        except KeyError as missing:
            raise MalformedOp(i, f"{op.get('op')} needs {missing.args[0]}") from missing
    return model
