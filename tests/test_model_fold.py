"""architect.model_fold: the one fold the Arbiter and the projector share. No database."""

from __future__ import annotations

import copy

import pytest

from architect.model_fold import (
    MalformedOp,
    PatchTargetMissing,
    apply_patch,
    child_of,
    empty_model,
)
from replay_reference import fixture_events, normalized, reference

BASE = {
    "version_id": "mv_0000000001",
    "project_id": "p",
    "elements": {"flows": [{"id": "flw_0000000001", "from": "a", "to": "b"}]},
    "links": {"satisfies": [{"component": "a", "requirement": "req_1"}]},
}


def ops(*ops: dict) -> dict:
    return {"ops": list(ops)}


def test_folding_the_fixture_gives_replays_final_model():
    model = None
    for event in fixture_events():
        payload = event["payload"]
        if event["type"] == "model.version_created":
            model = empty_model(event["project_id"], payload["version_id"])
        elif event["type"] == "model.patch_committed":
            model = apply_patch(model, payload["patch"], payload["version_id"])
    assert normalized(model) == normalized(reference()["final_model"])


def test_each_op_kind():
    flow = {"id": "flw_0000000002", "from": "b", "to": "c"}
    link = {"component": "b", "requirement": "req_2"}
    patch = ops(
        {"op": "add_element", "element_type": "flows", "element": flow},
        {
            "op": "update_element",
            "element_type": "flows",
            "element_id": "flw_0000000001",
            "element": {"to": "c", "input_validation": "schema"},
        },
        {"op": "remove_element", "element_type": "flows", "element_id": "flw_0000000002"},
        {"op": "add_link", "link_type": "satisfies", "link": link},
        {"op": "remove_link", "link_type": "satisfies", "link": BASE["links"]["satisfies"][0]},
        {"op": "remove_link", "link_type": "mitigates", "link": {"control": "x", "risk": "y"}},
    )
    assert apply_patch(BASE, patch, "mv_0000000002") == {
        "version_id": "mv_0000000002",
        "project_id": "p",
        "elements": {
            "flows": [
                {"id": "flw_0000000001", "from": "a", "to": "c", "input_validation": "schema"}
            ]
        },
        "links": {"satisfies": [link], "mitigates": []},
    }


def test_the_base_is_left_untouched_and_a_proposal_keeps_its_version():
    before = copy.deepcopy(BASE)
    update = {
        "op": "update_element",
        "element_type": "flows",
        "element_id": "flw_0000000001",
        "element": {"to": "z"},
    }
    proposed = apply_patch(BASE, ops(update))
    assert BASE == before
    assert proposed["version_id"] == BASE["version_id"]
    assert proposed["elements"]["flows"][0]["to"] == "z"


@pytest.mark.parametrize("kind", ["update_element", "remove_element"])
def test_a_missing_target_is_an_error(kind):
    op = {"op": kind, "element_type": "flows", "element_id": "flw_0000000009", "element": {}}
    keep = {"op": "add_link", "link_type": "mitigates", "link": {"control": "x", "risk": "y"}}
    with pytest.raises(PatchTargetMissing, match="flw_0000000009") as refused:
        apply_patch(BASE, ops(keep, op), "mv_0000000002")
    assert refused.value.index == 1

    # The element type has no elements at all.
    with pytest.raises(PatchTargetMissing):
        apply_patch(BASE, ops(op | {"element_type": "components"}), "mv_0000000002")


@pytest.mark.parametrize(
    ("op", "needs"),
    [
        ({"op": "add_element", "element": {}}, "element_type"),
        ({"op": "add_element", "element_type": "flows"}, "element"),
        ({"op": "update_element", "element_type": "flows", "element": {}}, "element_id"),
        ({"op": "remove_element", "element_type": "flows"}, "element_id"),
        ({"op": "add_link", "link_type": "satisfies"}, "link"),
        ({"op": "remove_link", "link": {}}, "link_type"),
    ],
)
def test_an_op_without_the_fields_its_kind_needs_is_malformed(op, needs):
    with pytest.raises(MalformedOp, match=needs) as refused:
        apply_patch(BASE, ops(op), "mv_0000000002")
    assert refused.value.index == 0


def test_a_child_is_a_copy_of_its_parent_under_a_new_id():
    child = child_of(BASE, "mv_0000000002")
    assert child == BASE | {"version_id": "mv_0000000002"}
    child["elements"]["flows"].append({"id": "flw_0000000003"})
    assert len(BASE["elements"]["flows"]) == 1


def test_a_genesis_is_empty():
    assert empty_model("p", "mv_0000000001") == {
        "version_id": "mv_0000000001",
        "project_id": "p",
        "elements": {},
        "links": {},
    }
