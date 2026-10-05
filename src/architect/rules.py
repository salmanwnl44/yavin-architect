"""The Arbiter validation matrix: one Rule per event type.

`check` runs after the schema gate and may raise a Rejection; it never writes.
`apply` folds a committed event into the arb_* state; it never rejects, so the same fold
rebuilds the state from the ledger.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from jsonschema import Draft202012Validator

from architect.contracts import first_error, json_path, load_contracts
from architect.errors import Rejection
from architect.model_fold import (
    MalformedOp,
    Model,
    PatchTargetMissing,
    apply_patch,
    child_of,
    empty_model,
)
from architect.state import MAIN, ArbiterState

Event = dict[str, Any]

# §7 no-silent-promotion: these statuses are only reachable through a recorded experiment.
PROMOTION_STATUSES = frozenset({"measured", "observed"})
# contracts v1.2 (P-11): the decisions at a human gate that only a human takes
HUMAN_DECISIONS = frozenset({"approve", "approve_with_risks", "reject"})


def _branch(event: Event) -> str:
    return event["payload"].get("branch", MAIN)


@dataclass(frozen=True)
class Rule:
    check: Callable[[Event, ArbiterState], None]
    apply: Callable[[Event, ArbiterState], None]


def _nothing(event: Event, state: ArbiterState) -> None:
    return None


def _deep_validate(
    validator: Draft202012Validator, instance: Any, at: tuple[str | int, ...], governed_by: str
) -> None:
    error = first_error(validator, instance)
    if error is not None:
        raise Rejection(
            "SCHEMA_INVALID", f"{governed_by}: {error.message}", json_path((*at, *error.path))
        )


def _validate_claim(claim: Any) -> None:
    _deep_validate(load_contracts().claim, claim, ("payload", "claim"), "claim.schema.json")


def _require_claim(state: ArbiterState, claim_id: str, path: str) -> str:
    status = state.claim_status(claim_id)
    if status is None:
        raise Rejection("UNKNOWN_CLAIM", f"claim {claim_id} is not committed in this project", path)
    return status


# source.ingested
def _check_source_ingested(event: Event, state: ArbiterState) -> None:
    source_id = event["payload"]["source_id"]
    if state.source_exists(source_id):
        raise Rejection(
            "DUPLICATE_SOURCE",
            f"source {source_id} is already ingested in this project",
            "$.payload.source_id",
        )


def _apply_source_ingested(event: Event, state: ArbiterState) -> None:
    state.add_source(event["payload"]["source_id"])


def _require_new_proposal(event: Event, state: ArbiterState) -> None:
    """Claim and model patch proposals share one id namespace per project (contracts v1.0)."""
    proposal_id = event["payload"]["proposal_id"]
    kind = state.proposal_kind(proposal_id)
    if kind is not None:
        raise Rejection(
            "DUPLICATE_PROPOSAL",
            f"proposal {proposal_id} is already used by a {kind.replace('_', ' ')} proposal",
            "$.payload.proposal_id",
        )


# claim.proposed
def _check_claim_proposed(event: Event, state: ArbiterState) -> None:
    _validate_claim(event["payload"]["claim"])
    _require_new_proposal(event, state)


def _apply_claim_proposed(event: Event, state: ArbiterState) -> None:
    state.add_proposal(event["payload"]["proposal_id"], "claim")


# claim.committed
def _check_claim_committed(event: Event, state: ArbiterState) -> None:
    payload = event["payload"]
    claim, claim_id = payload["claim"], payload["claim_id"]
    _validate_claim(claim)
    if claim_id != claim["id"]:
        raise Rejection(
            "CLAIM_ID_MISMATCH",
            f"payload.claim_id is {claim_id} but payload.claim.id is {claim['id']}",
            "$.payload.claim.id",
        )
    if state.claim_status(claim_id) is not None:
        raise Rejection(
            "DUPLICATE_CLAIM_ID", f"claim {claim_id} is already committed", "$.payload.claim_id"
        )
    for i, evidence in enumerate(claim.get("evidence", ())):
        if not state.source_exists(evidence["source"]):
            raise Rejection(
                "UNKNOWN_SOURCE",
                f"evidence cites source {evidence['source']}, which has not been ingested",
                f"$.payload.claim.evidence[{i}].source",
            )
    proposal = payload.get("from_proposal")
    if proposal is not None and state.proposal_kind(proposal) != "claim":
        raise Rejection(
            "UNKNOWN_PROPOSAL", f"no claim proposal {proposal}", "$.payload.from_proposal"
        )


def _apply_claim_committed(event: Event, state: ArbiterState) -> None:
    state.add_claim(event["payload"]["claim_id"], event["payload"]["claim"])


# claim.status_changed
def _check_claim_status_changed(event: Event, state: ArbiterState) -> None:
    payload = event["payload"]
    current = _require_claim(state, payload["claim_id"], "$.payload.claim_id")
    if payload["from"] != current:
        raise Rejection(
            "STATUS_MISMATCH",
            f"claim {payload['claim_id']} is {current}, not {payload['from']}",
            "$.payload.from",
        )
    cause_type = state.committed_event_type(payload["cause_event"])
    if cause_type is None:
        raise Rejection(
            "UNKNOWN_CAUSE_EVENT",
            f"cause_event {payload['cause_event']} is not a committed event in this project",
            "$.payload.cause_event",
        )
    if payload["to"] in PROMOTION_STATUSES and cause_type != "experiment.recorded":
        raise Rejection(
            "PROMOTION_FORBIDDEN",
            f"promotion to {payload['to']} needs an experiment.recorded cause; "
            f"{payload['cause_event']} is {cause_type}",
            "$.payload.cause_event",
        )


def _apply_claim_status_changed(event: Event, state: ArbiterState) -> None:
    state.set_claim_status(event["payload"]["claim_id"], event["payload"]["to"])


# claim.retracted
def _check_claim_retracted(event: Event, state: ArbiterState) -> None:
    _require_claim(state, event["payload"]["claim_id"], "$.payload.claim_id")


def _apply_claim_retracted(event: Event, state: ArbiterState) -> None:
    state.set_claim_status(event["payload"]["claim_id"], "retracted")


# model versions: the Arbiter folds every version, so no event that fails to produce a
# valid System Model is ever committed (architect.model_fold is the one fold).
def _require_new_version(state: ArbiterState, version_id: str) -> None:
    if state.model(version_id) is not None:
        raise Rejection(
            "DUPLICATE_VERSION_ID",
            f"model version {version_id} is already committed in this project",
            "$.payload.version_id",
        )


def _require_valid_model(model: Model, produced_by: str) -> None:
    """json_path locates the violation in the resulting model, not in the event."""
    error = first_error(load_contracts().model, model)
    if error is not None:
        raise Rejection(
            "INVALID_MODEL_RESULT",
            f"{produced_by} would not leave a valid system model: {error.message}",
            json_path(error.path),
        )


# model.version_created
def _created_model(event: Event, state: ArbiterState) -> Model:
    payload = event["payload"]
    parent = payload.get("parent")
    if parent is None:
        return empty_model(event["project_id"], payload["version_id"])
    return child_of(state.model(parent), payload["version_id"])


def _check_model_version_created(event: Event, state: ArbiterState) -> None:
    parent = event["payload"].get("parent")
    if parent is None:
        head = state.model_head(_branch(event))
        if head is not None:
            raise Rejection(
                "DUPLICATE_GENESIS",
                f"a genesis version needs an empty model; the head is already {head}",
                "$.payload",
            )
        if state.has_model_versions():
            raise Rejection(
                "BRANCH_NEEDS_PARENT",
                f"branch {_branch(event)!r} is new and the project already has model versions: "
                "its first version must name a committed parent",
                "$.payload.parent",
            )
    elif state.model(parent) is None:
        raise Rejection(
            "UNKNOWN_MODEL_VERSION",
            f"parent {parent} is not a committed model version in this project",
            "$.payload.parent",
        )
    _require_new_version(state, event["payload"]["version_id"])
    _require_valid_model(_created_model(event, state), "this version")


def _apply_model_version_created(event: Event, state: ArbiterState) -> None:
    state.add_model_version(
        event["payload"]["version_id"], _created_model(event, state), _branch(event)
    )


# model.patch_proposed / model.patch_committed
def _check_patch(event: Event, state: ArbiterState) -> None:
    payload = event["payload"]
    patch, base = payload["patch"], payload["base_version"]
    _deep_validate(
        load_contracts().model_patch,
        patch,
        ("payload", "patch"),
        "agent_protocol.schema.json#/$defs/ModelPatchProposal",
    )
    if patch["base_version"] != base:
        raise Rejection(
            "PATCH_BASE_MISMATCH",
            f"payload.base_version is {base} but the patch targets {patch['base_version']}",
            "$.payload.patch.base_version",
        )
    branch = _branch(event)
    head = state.model_head(branch)
    if base != head:
        base_branch = state.version_branch(base)
        if base_branch is not None and base_branch != branch:
            raise Rejection(
                "BASE_NOT_BRANCH_HEAD",
                f"base_version {base} is a version of branch {base_branch!r}, not of "
                f"{branch!r} (whose head is {head}); a patch applies to the head of its own "
                "branch",
                "$.payload.base_version",
            )
        raise Rejection(
            "BASE_MOVED",
            f"base_version {base} is not the model head ({head}); refetch the head and rebase",
            "$.payload.base_version",
        )


def _patched_model(event: Event, state: ArbiterState) -> Model:
    """The model the event's patch produces from its base, which _check_patch found to be the
    head. A proposal has no version_id, so its result keeps the base's."""
    payload = event["payload"]
    return apply_patch(
        state.model(payload["base_version"]), payload["patch"], payload.get("version_id")
    )


def _require_patch_applies(event: Event, state: ArbiterState) -> None:
    try:
        model = _patched_model(event, state)
    except PatchTargetMissing as error:
        raise Rejection(
            "PATCH_TARGET_MISSING", error.message, f"$.payload.patch.ops[{error.index}]"
        ) from error
    except MalformedOp as error:
        raise Rejection(
            "SCHEMA_INVALID", f"patch op: {error.message}", f"$.payload.patch.ops[{error.index}]"
        ) from error
    _require_valid_model(model, "this patch")


def _check_model_patch_proposed(event: Event, state: ArbiterState) -> None:
    _check_patch(event, state)
    _require_new_proposal(event, state)
    _require_patch_applies(event, state)


def _check_model_patch_committed(event: Event, state: ArbiterState) -> None:
    _check_patch(event, state)
    _require_new_version(state, event["payload"]["version_id"])
    proposal = event["payload"].get("from_proposal")
    if proposal is not None and state.proposal_kind(proposal) != "model_patch":
        raise Rejection(
            "UNKNOWN_PROPOSAL", f"no model patch proposal {proposal}", "$.payload.from_proposal"
        )
    _require_patch_applies(event, state)


def _apply_model_patch_proposed(event: Event, state: ArbiterState) -> None:
    state.add_proposal(event["payload"]["proposal_id"], "model_patch")


def _apply_model_patch_committed(event: Event, state: ArbiterState) -> None:
    state.add_model_version(
        event["payload"]["version_id"], _patched_model(event, state), _branch(event)
    )


# objection.raised
def _check_objection_raised(event: Event, state: ArbiterState) -> None:
    contracts = load_contracts()
    objection = event["payload"]["objection"]
    at = ("payload", "objection")
    _deep_validate(
        contracts.objection, objection, at, "agent_protocol.schema.json#/$defs/Objection"
    )
    proposed = objection.get("detecting_check", {}).get("proposed_check")
    if proposed is not None:
        _deep_validate(
            contracts.check,
            proposed,
            (*at, "detecting_check", "proposed_check"),
            "check_catalog.schema.json#/$defs/Check",
        )


def _apply_objection_raised(event: Event, state: ArbiterState) -> None:
    payload = event["payload"]
    state.open_objection(payload["objection_id"], payload["objection"]["severity"])


# objection.resolved
def _check_objection_resolved(event: Event, state: ArbiterState) -> None:
    objection_id = event["payload"]["objection_id"]
    if not state.objection_is_open(objection_id):
        raise Rejection(
            "OBJECTION_NOT_OPEN",
            f"objection {objection_id} was never raised or is already resolved",
            "$.payload.objection_id",
        )


def _apply_objection_resolved(event: Event, state: ArbiterState) -> None:
    state.close_objection(event["payload"]["objection_id"])


# waiver.signed (§5-P11: waivers are signed human events)
def _check_waiver_signed(event: Event, state: ArbiterState) -> None:
    if event["actor"]["kind"] != "human":
        raise Rejection(
            "WAIVER_NOT_HUMAN",
            f"a waiver must be signed by a human actor, not {event['actor']['kind']}",
            "$.actor.kind",
        )


# experiment.recorded
def _check_experiment_recorded(event: Event, state: ArbiterState) -> None:
    for i, claim_id in enumerate(event["payload"]["result_claims"]):
        _require_claim(state, claim_id, f"$.payload.result_claims[{i}]")


# decision.recorded
def _check_decision_recorded(event: Event, state: ArbiterState) -> None:
    for i, claim_id in enumerate(event["payload"]["decision"]["evidence_claims"]):
        _require_claim(state, claim_id, f"$.payload.decision.evidence_claims[{i}]")


# entity.merge_reverted
def _check_merge_reverted(event: Event, state: ArbiterState) -> None:
    merge_event = event["payload"]["merge_event"]
    if state.committed_event_type(merge_event) != "entity.merged":
        raise Rejection(
            "UNKNOWN_CAUSE_EVENT",
            f"merge_event {merge_event} is not a committed entity.merged event in this project",
            "$.payload.merge_event",
        )


def _apply_decision_recorded(event: Event, state: ArbiterState) -> None:
    state.add_ref(event["payload"]["adr_id"], "decision")


# session.status_changed (contracts v1.2, P-11)
def _check_session_status_changed(event: Event, state: ArbiterState) -> None:
    decision = event["payload"].get("decision")
    if decision in HUMAN_DECISIONS and event["actor"]["kind"] != "human":
        raise Rejection(
            "DECISION_NOT_HUMAN",
            f"the decision {decision} at a human gate must come from a human actor, not "
            f"{event['actor']['kind']}",
            "$.actor.kind",
        )


# finding.raised / finding.resolved (contracts v1.2, P-12)
def _check_finding_raised(event: Event, state: ArbiterState) -> None:
    payload = event["payload"]
    for i, ref in enumerate(payload["refs"]):
        if not state.ref_resolves(ref):
            raise Rejection(
                "UNKNOWN_REF",
                f"ref {ref} names no committed claim, source, model version, decision, model "
                "element or entity of this project",
                f"$.payload.refs[{i}]",
            )
    for i, claim_id in enumerate(payload.get("evidence_claims", ())):
        _require_claim(state, claim_id, f"$.payload.evidence_claims[{i}]")
    if state.finding_exists(payload["finding_id"]):
        raise Rejection(
            "DUPLICATE_FINDING_ID",
            f"finding {payload['finding_id']} was already raised in this project",
            "$.payload.finding_id",
        )
    already = state.open_finding_with(payload["dedupe_key"])
    if already is not None:
        raise Rejection(
            "DUPLICATE_FINDING",
            f"open finding {already} has the same dedupe_key: the same thing was already found",
            "$.payload.dedupe_key",
        )


def _apply_finding_raised(event: Event, state: ArbiterState) -> None:
    state.open_finding(event["payload"]["finding_id"], event["payload"]["dedupe_key"])


def _check_finding_resolved(event: Event, state: ArbiterState) -> None:
    finding_id = event["payload"]["finding_id"]
    if not state.finding_is_open(finding_id):
        raise Rejection(
            "FINDING_NOT_OPEN",
            f"finding {finding_id} was never raised or is already resolved",
            "$.payload.finding_id",
        )


def _apply_finding_resolved(event: Event, state: ArbiterState) -> None:
    state.close_finding(event["payload"]["finding_id"])


_SCHEMA_ONLY = Rule(_nothing, _nothing)

RULES: dict[str, Rule] = {
    "source.ingested": Rule(_check_source_ingested, _apply_source_ingested),
    "claim.proposed": Rule(_check_claim_proposed, _apply_claim_proposed),
    "claim.committed": Rule(_check_claim_committed, _apply_claim_committed),
    "claim.status_changed": Rule(_check_claim_status_changed, _apply_claim_status_changed),
    "claim.retracted": Rule(_check_claim_retracted, _apply_claim_retracted),
    "entity.merged": _SCHEMA_ONLY,
    "entity.merge_reverted": Rule(_check_merge_reverted, _nothing),
    "model.patch_proposed": Rule(_check_model_patch_proposed, _apply_model_patch_proposed),
    "model.patch_committed": Rule(_check_model_patch_committed, _apply_model_patch_committed),
    "model.version_created": Rule(_check_model_version_created, _apply_model_version_created),
    "decision.recorded": Rule(_check_decision_recorded, _apply_decision_recorded),
    "objection.raised": Rule(_check_objection_raised, _apply_objection_raised),
    "objection.resolved": Rule(_check_objection_resolved, _apply_objection_resolved),
    "waiver.signed": Rule(_check_waiver_signed, _nothing),
    "check.result": _SCHEMA_ONLY,
    "experiment.recorded": Rule(_check_experiment_recorded, _nothing),
    "session.phase_changed": _SCHEMA_ONLY,
    "session.checkpoint": _SCHEMA_ONLY,
    "budget.updated": _SCHEMA_ONLY,
    "session.status_changed": Rule(_check_session_status_changed, _nothing),
    "finding.raised": Rule(_check_finding_raised, _apply_finding_raised),
    "finding.resolved": Rule(_check_finding_resolved, _apply_finding_resolved),
}
