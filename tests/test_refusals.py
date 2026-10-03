"""The validation matrix, at the API. Every refusal proves the code AND that nothing was written.

Exit test 2 is the block marked as such; the rest covers every other rejection code.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from builders import (
    AGENT,
    HUMAN,
    SYSTEM,
    candidate,
    claim,
    claim_committed,
    ident,
    objection,
    patch,
    patch_committed,
    proposed_check,
    source,
)
from conftest import PROJECT

EVENTS = f"/v1/projects/{PROJECT}/events"
MV1, MV2, MV3 = (ident("mv", f"v{n}") for n in (1, 2, 3))


@pytest.fixture
def commit(client):
    def commit(event: dict[str, Any]) -> dict[str, Any]:
        response = client.post(EVENTS, json=event)
        assert response.status_code == 201, response.text
        return response.json()["event"]

    return commit


@pytest.fixture
def refuse(client, fingerprint):
    def refuse(event: dict[str, Any], status: int, code: str, at: str | None = None) -> dict:
        before = fingerprint()
        head_before = client.get(f"/v1/projects/{PROJECT}/head").json()
        response = client.post(EVENTS, json=event)
        body = response.json()
        assert (response.status_code, body.get("code")) == (status, code), response.text
        assert set(body) <= {"code", "detail", "json_path"} and body["detail"]
        if at is not None:
            assert body["json_path"] == at
        assert fingerprint() == before, "a refused event left something behind"
        assert client.get(f"/v1/projects/{PROJECT}/head").json() == head_before
        return body

    return refuse


def status_change(claim_id: str, frm: str, to: str, cause: str) -> dict[str, Any]:
    return candidate(
        "claim.status_changed",
        {"claim_id": claim_id, "from": frm, "to": to, "cause_event": cause},
    )


def raised(body: dict[str, Any], name: str = "splitbrain") -> dict[str, Any]:
    return candidate(
        "objection.raised", {"objection_id": ident("obj", name), "objection": body}, actor=AGENT
    )


def proposed_claim(proposal_id: str, body: dict[str, Any]) -> dict[str, Any]:
    return candidate("claim.proposed", {"proposal_id": proposal_id, "claim": body}, actor=AGENT)


def proposed_patch(proposal_id: str, base: str) -> dict[str, Any]:
    payload = {"proposal_id": proposal_id, "base_version": base, "patch": patch(base)}
    return candidate("model.patch_proposed", payload, actor=AGENT)


def waiver(actor: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "waiver_id": ident("wvr", "singlenode"),
        "target_ref": "C-010",
        "risk": "Single Postgres node in the dev environment",
        "signer": "saumya",
    }
    return candidate("waiver.signed", payload, actor=actor)


# --- Exit test 2 ------------------------------------------------------------------------


def test_documented_claim_without_evidence(commit, refuse):
    commit(source())
    body = refuse(claim_committed(claim(evidence=None)), 422, "SCHEMA_INVALID", "$.payload.claim")
    assert "evidence" in body["detail"]


def test_load_bearing_assumption_without_verification_plan(refuse):
    unplanned = claim(status="assumed", load_bearing=True)
    body = refuse(claim_committed(unplanned), 422, "SCHEMA_INVALID", "$.payload.claim")
    assert "verification_plan" in body["detail"]


def test_promotion_to_measured_needs_an_experiment_as_cause(commit, refuse):
    assumption = claim(status="assumed")
    commit(claim_committed(assumption))
    not_an_experiment = commit(waiver(HUMAN))
    refuse(
        status_change(assumption["id"], "assumed", "measured", not_an_experiment["event_id"]),
        422,
        "PROMOTION_FORBIDDEN",
        "$.payload.cause_event",
    )


def test_status_change_with_the_wrong_from(commit, refuse):
    assumption = claim(status="assumed")
    cause = commit(claim_committed(assumption))
    refuse(
        status_change(assumption["id"], "proposed", "refuted", cause["event_id"]),
        422,
        "STATUS_MISMATCH",
        "$.payload.from",
    )


def test_waiver_signed_by_an_agent(refuse):
    refuse(waiver(AGENT), 422, "WAIVER_NOT_HUMAN", "$.actor.kind")


def test_evidence_citing_an_uningested_source(refuse):
    refuse(claim_committed(claim()), 422, "UNKNOWN_SOURCE", "$.payload.claim.evidence[0].source")


def test_patch_on_a_stale_base(commit, refuse):
    commit(candidate("model.version_created", {"version_id": MV1}))
    commit(patch_committed(MV2, MV1))
    body = refuse(patch_committed(MV3, MV1), 409, "BASE_MOVED", "$.payload.base_version")
    assert MV2 in body["detail"]


def test_resolving_an_objection_that_was_never_raised(refuse):
    resolved = candidate(
        "objection.resolved",
        {"objection_id": ident("obj", "ghost"), "resolution": "withdrawn", "ref": "n/a"},
    )
    refuse(resolved, 422, "OBJECTION_NOT_OPEN", "$.payload.objection_id")


def test_objection_without_a_falsifiable_test(refuse):
    body = refuse(
        raised(objection(falsifiable_test=None)), 422, "SCHEMA_INVALID", "$.payload.objection"
    )
    assert "falsifiable_test" in body["detail"]


# --- The rest of the matrix -------------------------------------------------------------


def test_duplicate_source(commit, refuse):
    commit(source())
    refuse(source(), 409, "DUPLICATE_SOURCE", "$.payload.source_id")


def test_proposed_claim_must_be_a_valid_claim(refuse):
    proposed = candidate(
        "claim.proposed", {"proposal_id": "prop-1", "claim": claim(predicate=None)}, actor=AGENT
    )
    refuse(proposed, 422, "SCHEMA_INVALID", "$.payload.claim")


def test_claim_id_mismatch(commit, refuse):
    commit(source())
    mismatched = candidate("claim.committed", {"claim_id": ident("clm", "other"), "claim": claim()})
    refuse(mismatched, 422, "CLAIM_ID_MISMATCH", "$.payload.claim.id")


def test_duplicate_claim_id(commit, refuse):
    commit(source())
    commit(claim_committed(claim()))
    refuse(claim_committed(claim()), 409, "DUPLICATE_CLAIM_ID", "$.payload.claim_id")


def test_claim_from_an_unknown_proposal(commit, refuse):
    commit(source())
    refuse(
        claim_committed(claim(), from_proposal="prop-missing"),
        422,
        "UNKNOWN_PROPOSAL",
        "$.payload.from_proposal",
    )


def test_a_patch_proposal_is_not_a_claim_proposal(commit, refuse):
    commit(source())
    commit(candidate("model.version_created", {"version_id": MV1}))
    commit(
        candidate(
            "model.patch_proposed",
            {"proposal_id": "prop-1", "base_version": MV1, "patch": patch(MV1)},
        )
    )
    refuse(claim_committed(claim(), from_proposal="prop-1"), 422, "UNKNOWN_PROPOSAL")


def test_claim_and_patch_proposals_with_distinct_ids_are_accepted(commit):
    commit(source())
    commit(candidate("model.version_created", {"version_id": MV1}))
    commit(proposed_claim("prop-1", claim()))
    commit(proposed_patch("prop-2", MV1))
    commit(claim_committed(claim(), from_proposal="prop-1"))
    commit(patch_committed(MV2, MV1, from_proposal="prop-2"))


def test_a_proposal_id_is_used_once_across_both_kinds(commit, refuse):
    commit(candidate("model.version_created", {"version_id": MV1}))
    commit(proposed_claim("prop-1", claim()))
    for reuse in (proposed_patch("prop-1", MV1), proposed_claim("prop-1", claim("other"))):
        body = refuse(reuse, 409, "DUPLICATE_PROPOSAL", "$.payload.proposal_id")
        assert "claim proposal" in body["detail"]


def test_formats_inside_embedded_objects_are_enforced(commit, refuse):
    commit(source())
    not_a_timestamp = claim(recorded_at="yesterday")
    refuse(claim_committed(not_a_timestamp), 422, "SCHEMA_INVALID", "$.payload.claim.recorded_at")


def test_status_change_of_an_unknown_claim(commit, refuse):
    cause = commit(source())
    refuse(
        status_change(ident("clm", "ghost"), "assumed", "refuted", cause["event_id"]),
        422,
        "UNKNOWN_CLAIM",
        "$.payload.claim_id",
    )


def test_status_change_with_an_unknown_cause_event(commit, refuse):
    assumption = claim(status="assumed")
    commit(claim_committed(assumption))
    refuse(
        status_change(assumption["id"], "assumed", "refuted", ident("evt", "ghost")),
        422,
        "UNKNOWN_CAUSE_EVENT",
        "$.payload.cause_event",
    )


def test_a_cause_event_from_another_project_is_unknown(client, commit, refuse):
    assert client.post("/v1/projects", json={"project_id": "p2"}).status_code == 201
    elsewhere = client.post("/v1/projects/p2/events", json=source()).json()["event"]
    assumption = claim(status="assumed")
    commit(claim_committed(assumption))
    refuse(
        status_change(assumption["id"], "assumed", "refuted", elsewhere["event_id"]),
        422,
        "UNKNOWN_CAUSE_EVENT",
    )


def test_promotion_to_observed_is_guarded_too(commit, refuse):
    assumption = claim(status="assumed")
    cause = commit(claim_committed(assumption))
    refuse(
        status_change(assumption["id"], "assumed", "observed", cause["event_id"]),
        422,
        "PROMOTION_FORBIDDEN",
    )


def test_retracting_an_unknown_claim(refuse):
    retracted = candidate("claim.retracted", {"claim_id": ident("clm", "ghost"), "cause": "x"})
    refuse(retracted, 422, "UNKNOWN_CLAIM", "$.payload.claim_id")


def test_second_genesis_version(commit, refuse):
    commit(candidate("model.version_created", {"version_id": MV1}))
    refuse(candidate("model.version_created", {"version_id": MV2}), 409, "DUPLICATE_GENESIS")


def test_version_created_with_a_committed_parent_is_accepted(client, commit):
    commit(candidate("model.version_created", {"version_id": MV1}))
    commit(patch_committed(MV2, MV1))
    commit(candidate("model.version_created", {"version_id": MV3, "parent": MV2}))
    assert client.get(f"/v1/projects/{PROJECT}/head").json()["model_head_version"] == MV3


def test_version_created_with_an_unknown_parent(commit, refuse):
    commit(candidate("model.version_created", {"version_id": MV1}))
    orphan = candidate("model.version_created", {"version_id": MV2, "parent": MV3})
    refuse(orphan, 422, "UNKNOWN_MODEL_VERSION", "$.payload.parent")


def test_a_parent_version_from_another_project_is_unknown(client, refuse):
    assert client.post("/v1/projects", json={"project_id": "p2"}).status_code == 201
    elsewhere = candidate("model.version_created", {"version_id": MV1})
    assert client.post("/v1/projects/p2/events", json=elsewhere).status_code == 201
    child = candidate("model.version_created", {"version_id": MV2, "parent": MV1})
    refuse(child, 422, "UNKNOWN_MODEL_VERSION", "$.payload.parent")


def test_patch_must_be_a_valid_model_patch(commit, refuse):
    commit(candidate("model.version_created", {"version_id": MV1}))
    no_ops = patch(MV1) | {"ops": []}
    for event in (
        candidate(
            "model.patch_proposed", {"proposal_id": "prop-1", "base_version": MV1, "patch": no_ops}
        ),
        candidate(
            "model.patch_committed", {"version_id": MV2, "base_version": MV1, "patch": no_ops}
        ),
    ):
        refuse(event, 422, "SCHEMA_INVALID", "$.payload.patch.ops")


def test_patch_base_mismatch(commit, refuse):
    commit(candidate("model.version_created", {"version_id": MV1}))
    for event in (
        candidate(
            "model.patch_proposed",
            {"proposal_id": "prop-1", "base_version": MV1, "patch": patch(MV2)},
        ),
        candidate(
            "model.patch_committed", {"version_id": MV2, "base_version": MV1, "patch": patch(MV2)}
        ),
    ):
        refuse(event, 422, "PATCH_BASE_MISMATCH", "$.payload.patch.base_version")


def test_patch_proposed_on_a_stale_base(commit, refuse):
    commit(candidate("model.version_created", {"version_id": MV1}))
    commit(patch_committed(MV2, MV1))
    stale = candidate(
        "model.patch_proposed", {"proposal_id": "prop-1", "base_version": MV1, "patch": patch(MV1)}
    )
    refuse(stale, 409, "BASE_MOVED", "$.payload.base_version")


def test_patch_before_any_model_version(refuse):
    refuse(patch_committed(MV2, MV1), 409, "BASE_MOVED")


def test_patch_committed_from_an_unknown_proposal(commit, refuse):
    commit(candidate("model.version_created", {"version_id": MV1}))
    refuse(
        patch_committed(MV2, MV1, from_proposal="prop-missing"),
        422,
        "UNKNOWN_PROPOSAL",
        "$.payload.from_proposal",
    )


def test_proposed_check_must_be_a_valid_check(refuse):
    broken = proposed_check() | {"layer": "L9"}
    refuse(
        raised(objection(detecting_check={"proposed_check": broken})),
        422,
        "SCHEMA_INVALID",
        "$.payload.objection.detecting_check.proposed_check.layer",
    )


def test_resolving_an_objection_twice(commit, refuse):
    commit(raised(objection()))
    resolution = {
        "objection_id": ident("obj", "splitbrain"),
        "resolution": "patched",
        "ref": MV2,
    }
    commit(candidate("objection.resolved", resolution))
    refuse(candidate("objection.resolved", resolution), 422, "OBJECTION_NOT_OPEN")


def test_waiver_signed_by_the_system(refuse):
    refuse(waiver(SYSTEM), 422, "WAIVER_NOT_HUMAN")


def test_experiment_with_an_uncommitted_result_claim(commit, refuse):
    commit(source())
    known = claim()
    commit(claim_committed(known))
    experiment = candidate(
        "experiment.recorded",
        {
            "experiment_id": ident("exp", "probe"),
            "result_claims": [known["id"], ident("clm", "ghost")],
        },
    )
    refuse(experiment, 422, "UNKNOWN_CLAIM", "$.payload.result_claims[1]")


def test_decision_citing_an_uncommitted_claim(refuse):
    decision = candidate(
        "decision.recorded",
        {
            "adr_id": ident("adr", "fencing"),
            "decision": {
                "title": "t",
                "choice": "c",
                "evidence_claims": [ident("clm", "ghost")],
                "alternatives": [],
                "assumptions": [],
                "affected_elements": [],
            },
        },
        actor=HUMAN,
    )
    refuse(decision, 422, "UNKNOWN_CLAIM", "$.payload.decision.evidence_claims[0]")


def test_merge_revert_must_cite_a_committed_merge(commit, refuse):
    not_a_merge = commit(source())
    for merge_event in (ident("evt", "ghost"), not_a_merge["event_id"]):
        refuse(
            candidate("entity.merge_reverted", {"merge_event": merge_event}),
            422,
            "UNKNOWN_CAUSE_EVENT",
            "$.payload.merge_event",
        )


# --- The schema gate --------------------------------------------------------------------


@pytest.mark.parametrize(
    ("change", "at"),
    [
        ({"type": "claim.invented"}, "$.type"),
        ({"idempotency_key": "short"}, "$.idempotency_key"),
        ({"idempotency_key": None}, "$"),
        ({"actor": {"kind": "robot", "id": "x"}}, "$.actor.kind"),
        ({"event_id": "not-an-event-id"}, "$.event_id"),
        ({"ts": "yesterday"}, "$.ts"),
        ({"ts": "2026-10-02T06:41:00"}, "$.ts"),
        ({"session_id": "s1"}, "$.session_id"),
        ({"surprise": True}, "$"),
        ({"payload": {"source_id": "src_0000000001"}}, "$.payload"),
    ],
)
def test_schema_gate(refuse, change, at):
    event = source() | change
    if change.get("idempotency_key", "") is None:
        del event["idempotency_key"]
    refuse(event, 422, "SCHEMA_INVALID", at)


def test_schema_gate_runs_before_the_rules(commit, refuse):
    """A duplicate source with a malformed payload is a schema error, not DUPLICATE_SOURCE."""
    commit(source())
    malformed = source()
    malformed["payload"]["taint_origin"] = "moon"
    refuse(malformed, 422, "SCHEMA_INVALID", "$.payload.taint_origin")


def test_project_mismatch(refuse):
    refuse(source() | {"project_id": "another"}, 422, "PROJECT_MISMATCH", "$.project_id")


def test_candidate_must_be_an_object(client):
    response = client.post(EVENTS, json=[1, 2, 3])
    assert response.status_code == 422
    assert response.json()["code"] == "MALFORMED_REQUEST"


# --- Model versions are folded by the Arbiter (M1.1) ------------------------------------


def component(name: str, **fields: Any) -> dict[str, Any]:
    return {
        "id": ident("cmp", name),
        "name": name.title(),
        "kind": "service",
        "stateful": False,
        "requirement_refs": [],
    } | fields


def add(element: dict[str, Any], element_type: str = "components") -> dict[str, Any]:
    return {"op": "add_element", "element_type": element_type, "element": element}


def patch_events(version: str, base: str, *ops: dict[str, Any]) -> list[dict[str, Any]]:
    """The same patch as a proposal and as a commit: both are held to the same rules."""
    body = {"base_version": base, "rationale": "test", "ops": list(ops)}
    proposed = {"proposal_id": f"prop-{version}", "base_version": base, "patch": body}
    committed = {"version_id": version, "base_version": base, "patch": body}
    return [
        candidate("model.patch_proposed", proposed, actor=AGENT),
        candidate("model.patch_committed", committed),
    ]


@pytest.fixture
def models(fingerprint):
    """version_id -> the model the Arbiter holds for it."""

    def models() -> dict[str, dict[str, Any]]:
        rows = [json.loads(row) for row in fingerprint()["arb_model_versions"]]
        return {row["version_id"]: row["model"] for row in rows}

    return models


def test_a_valid_patch_is_accepted_and_its_result_becomes_the_head_model(commit, models):
    router, store = component("router"), component("store", kind="datastore", stateful=True)
    depends = {"from": router["id"], "to": store["id"], "kind": "sync"}
    link = {"link_type": "depends_on", "link": depends}
    commit(candidate("model.version_created", {"version_id": MV1}))
    proposed, committed = patch_events(MV2, MV1, add(router), add(store), {"op": "add_link"} | link)
    commit(proposed)
    commit(committed)
    rename = {
        "op": "update_element",
        "element_type": "components",
        "element_id": router["id"],
        "element": {"name": "Write Router"},
    }
    drop = {"op": "remove_element", "element_type": "components", "element_id": store["id"]}
    commit(patch_events(MV3, MV2, rename, drop, {"op": "remove_link"} | link)[1])

    held = models()
    assert held[MV1] == {"version_id": MV1, "project_id": PROJECT, "elements": {}, "links": {}}
    assert held[MV2] == {
        "version_id": MV2,
        "project_id": PROJECT,
        "elements": {"components": [router, store]},
        "links": {"depends_on": [depends]},
    }
    assert held[MV3]["elements"] == {"components": [router | {"name": "Write Router"}]}
    assert held[MV3]["links"] == {"depends_on": []}


def test_a_version_created_from_a_parent_copies_the_parents_model(client, commit, models):
    commit(candidate("model.version_created", {"version_id": MV1}))
    commit(patch_events(MV2, MV1, add(component("router")))[1])
    mv4 = ident("mv", "v4")
    commit(candidate("model.version_created", {"version_id": MV3, "parent": MV2}))
    commit(candidate("model.version_created", {"version_id": mv4, "parent": MV1}))

    held = models()
    assert held[MV3] == held[MV2] | {"version_id": MV3}
    assert held[MV3]["elements"]["components"] == [component("router")]
    assert held[mv4] == held[MV1] | {"version_id": mv4}, "a parent need not be the head"
    assert client.get(f"/v1/projects/{PROJECT}/head").json()["model_head_version"] == mv4


def test_patch_whose_target_is_not_in_the_model(commit, refuse):
    commit(candidate("model.version_created", {"version_id": MV1}))
    ghost = ident("cmp", "ghost")
    update = {
        "op": "update_element",
        "element_type": "components",
        "element_id": ghost,
        "element": {"name": "x"},
    }
    remove = {"op": "remove_element", "element_type": "components", "element_id": ghost}
    for missing in (update, remove):
        for event in patch_events(MV2, MV1, add(component("router")), missing):
            body = refuse(event, 422, "PATCH_TARGET_MISSING", "$.payload.patch.ops[1]")
            assert ghost in body["detail"]


def test_patch_that_would_not_leave_a_valid_model(commit, refuse):
    commit(candidate("model.version_created", {"version_id": MV1}))
    for op, at in (
        (add({"name": "Fencer"}), "$.elements.components[0]"),
        (add(component("router"), element_type="component"), "$.elements"),
        (add(component("router", kind="teapot")), "$.elements.components[0].kind"),
    ):
        for event in patch_events(MV2, MV1, op):
            refuse(event, 422, "INVALID_MODEL_RESULT", at)


def test_reusing_a_model_version_id(commit, refuse):
    commit(candidate("model.version_created", {"version_id": MV1}))
    reused_by_patch = patch_events(MV1, MV1, add(component("router")))[1]
    refuse(reused_by_patch, 409, "DUPLICATE_VERSION_ID", "$.payload.version_id")

    commit(patch_events(MV2, MV1, add(component("router")))[1])
    for version in (MV1, MV2):
        reused = candidate("model.version_created", {"version_id": version, "parent": MV2})
        refuse(reused, 409, "DUPLICATE_VERSION_ID", "$.payload.version_id")


def test_a_patch_op_without_the_fields_its_kind_needs(commit, refuse):
    commit(candidate("model.version_created", {"version_id": MV1}))
    for event in patch_events(MV2, MV1, add(component("router")), {"op": "add_element"}):
        refuse(event, 422, "SCHEMA_INVALID", "$.payload.patch.ops[1]")


def test_a_version_id_the_system_model_cannot_carry(refuse):
    refuse(
        candidate("model.version_created", {"version_id": "v1"}),
        422,
        "INVALID_MODEL_RESULT",
        "$.version_id",
    )
