"""The Architect agent v1: prompts, output schemas and the typed protocol messages it
exchanges with the harness (agent_protocol.schema.json). The agent proposes; the Arbiter
decides. Every message is recorded in the append-only ag_messages table and references the
gateway calls it came from.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import best_match
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from architect.contracts import load_contracts
from architect.errors import Rejection
from architect.gateway.request import GatewayRequest
from architect.ingestion.normalize import typed_id

PROMPT_VERSION = "architect-v1"
ROLE = "architect"
ACTOR = {"kind": "agent", "id": "architect", "role": "architect"}
ORCHESTRATOR = {"kind": "system", "id": "session-orchestrator", "role": "system"}

# The one line a scripted provider may key on: what the call is for.
MARKER = "[architect purpose={purpose} round={round}]"

SYSTEM_COMMON = (
    "You are the Architect agent of a design-verification harness. You propose; a separate "
    "Arbiter validates every proposal against frozen schemas and rules and commits or refuses "
    "it. Use only the facts in the context you are given; cite claims by their ids. Never "
    "assign a status or a confidence to anything. Return JSON only, matching the schema."
)

SYSTEM_BY_PURPOSE = {
    "frame": (
        "Read the brief and return its requirements, constraints and unknowns. A requirement "
        "is measurable when it has a metric, a numeric target and a unit; give metric, target "
        "and unit whenever the brief states them, and quote the brief's words that state the "
        "requirement verbatim. Slugs are short, lowercase, hyphenated."
    ),
    "draft": (
        "Propose the first System Model as patch ops on the current head: add_element ops with "
        "element_type among components, interfaces, flows, capacity_params, deployment_units, "
        "slos, trust_boundaries, failure_modes, state_machines, cost_models, "
        "observability_specs, and add_link ops with link_type satisfies, depends_on or "
        "mitigates. Ids: components cmp_<10-26 alnum>, interfaces if_<10-26 alnum>, flows "
        "flw_<10-26 alnum>; requirements are referenced by their req_ ids from the context. "
        "Every requirement must be satisfied by at least one component through a satisfies "
        "link, and each component's requirement_refs must equal the requirements it "
        "satisfies. Stateful components declare durability_class and recovery; flows into "
        "queues or over async interfaces declare backpressure_ref; every component with "
        "inbound flows has a capacity param with metric max_qps. Record each decision worth "
        "an ADR with evidence_claims drawn only from claim ids in the context."
    ),
    "repair": (
        "The checks listed with their evidence failed or could not be evaluated on the current "
        "head. Propose one patch (update_element, add_element, add_link, remove_element, "
        "remove_link ops) that repairs as many as you can without breaking others. Where a "
        "violation is acceptable for this design, do not patch it: request a waiver for that "
        "check and element and state the risk; a human decides. Return ops: [] only when "
        "nothing can be improved."
    ),
}

_OP_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["op"],
    "properties": {
        "op": {
            "enum": ["add_element", "update_element", "remove_element", "add_link", "remove_link"]
        },
        "element_type": {"type": "string"},
        "element": {"type": "object"},
        "element_id": {"type": "string"},
        "link_type": {"enum": ["satisfies", "depends_on", "mitigates"]},
        "link": {"type": "object"},
    },
}

_DECISION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "title",
        "choice",
        "evidence_claims",
        "alternatives",
        "assumptions",
        "affected_elements",
    ],
    "properties": {
        "title": {"type": "string"},
        "choice": {"type": "string"},
        "evidence_claims": {"type": "array", "items": {"type": "string"}},
        "alternatives": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["option", "rejected_because"],
                "properties": {
                    "option": {"type": "string"},
                    "rejected_because": {"type": "string"},
                },
            },
        },
        "assumptions": {"type": "array", "items": {"type": "string"}},
        "affected_elements": {"type": "array", "items": {"type": "string"}},
    },
}

_QUESTION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["question", "options_considered"],
    "properties": {
        "question": {"type": "string"},
        "options_considered": {"type": "array", "items": {"type": "string"}},
    },
}

FRAME_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["requirements", "constraints", "unknowns"],
    "properties": {
        "requirements": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["slug", "text"],
                "properties": {
                    "slug": {"type": "string"},
                    "text": {"type": "string"},
                    "metric": {"type": "string"},
                    "target": {"type": "number"},
                    "unit": {"type": "string"},
                    "quote": {"type": "string"},
                },
            },
        },
        "constraints": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["slug", "text"],
                "properties": {
                    "slug": {"type": "string"},
                    "text": {"type": "string"},
                    "quote": {"type": "string"},
                },
            },
        },
        "unknowns": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["slug", "text"],
                "properties": {"slug": {"type": "string"}, "text": {"type": "string"}},
            },
        },
    },
}

DRAFT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["rationale", "ops"],
    "properties": {
        "rationale": {"type": "string"},
        "ops": {"type": "array", "items": _OP_SCHEMA},
        "decisions": {"type": "array", "items": _DECISION_SCHEMA},
        "questions": {"type": "array", "items": _QUESTION_SCHEMA},
    },
}

REPAIR_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["rationale", "ops"],
    "properties": {
        "rationale": {"type": "string"},
        "ops": {"type": "array", "items": _OP_SCHEMA},
        "waiver_requests": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["check_id", "element_id", "risk"],
                "properties": {
                    "check_id": {"type": "string"},
                    "element_id": {"type": "string"},
                    "risk": {"type": "string"},
                },
            },
        },
        "decisions": {"type": "array", "items": _DECISION_SCHEMA},
        "questions": {"type": "array", "items": _QUESTION_SCHEMA},
    },
}

SCHEMAS = {"frame": FRAME_SCHEMA, "draft": DRAFT_SCHEMA, "repair": REPAIR_SCHEMA}


def system_prompt(purpose: str, round_: int) -> str:
    return (
        f"{MARKER.format(purpose=purpose, round=round_)}\n{SYSTEM_COMMON}\n\n"
        f"{SYSTEM_BY_PURPOSE[purpose]}\n\nPrompt version: {PROMPT_VERSION}."
    )


def architect_request(
    *,
    purpose: str,
    round_: int,
    session_id: str,
    phase: str,
    tier: str,
    max_tokens: int,
    messages: list[dict[str, str]],
    input_taints: list[str],
) -> GatewayRequest:
    return GatewayRequest(
        role=ROLE,
        tier=tier,
        purpose=f"architect-{purpose}",
        system=system_prompt(purpose, round_),
        messages=messages,
        output_schema=SCHEMAS[purpose],
        max_tokens=max_tokens,
        temperature=0.0,
        scope={"session": session_id, "phase": phase},
        input_taints=input_taints,
        cache="auto",
    )


def rejection_feedback(rejection: Rejection) -> str:
    """The Arbiter's refusal as the next user turn: structured, so the retry is bounded and
    the model knows exactly what was wrong."""
    body = rejection.body()
    return (
        "The Arbiter refused your proposal: " + json.dumps(body, sort_keys=True) + ". "
        "Reply again with only a JSON document that fixes exactly this and satisfies the schema."
    )


# ---------------------------------------------------------------- protocol messages
_protocol_validator: Draft202012Validator | None = None


def protocol_validator() -> Draft202012Validator:
    global _protocol_validator
    if _protocol_validator is None:
        schema = load_contracts().schemas["agent_protocol.schema.json"]
        _protocol_validator = Draft202012Validator(schema, format_checker=FormatChecker())
    return _protocol_validator


def task_id(session_id: str, phase: str, round_: int, step: str) -> str:
    return typed_id("tsk", session_id, phase, str(round_), step)


def msg_id(session_id: str, phase: str, round_: int, step: str, kind: str) -> str:
    return typed_id("msg", session_id, phase, str(round_), step, kind)


def message(
    *,
    session_id: str,
    task_id_: str,
    msg_id_: str,
    type_: str,
    body: dict[str, Any],
    agent: dict[str, Any],
    ts: str,
    cost: dict[str, Any] | None = None,
    depends_on: list[str] | None = None,
    parent_task: str | None = None,
) -> dict[str, Any]:
    """A protocol message, validated against agent_protocol.schema.json before anything
    records it. ValueError names the first violation."""
    out: dict[str, Any] = {
        "msg_id": msg_id_,
        "session_id": session_id,
        "task_id": task_id_,
        "agent": agent,
        "ts": ts,
        "cost": cost or {"tokens_in": 0, "tokens_out": 0},
        "depends_on": depends_on or [],
        "type": type_,
        "body": body,
    }
    if parent_task is not None:
        out["parent_task"] = parent_task
    error = best_match(protocol_validator().iter_errors(out))
    if error is not None:
        where = "/".join(str(p) for p in error.absolute_path) or "(root)"
        raise ValueError(f"agent message {type_} at {where}: {error.message}")
    return out


def patch_proposal_error(body: dict[str, Any]) -> str | None:
    """None when the body is a valid ModelPatchProposal, else the violation."""
    error = best_match(load_contracts().model_patch.iter_errors(body))
    if error is None:
        return None
    where = "/".join(str(p) for p in error.absolute_path) or "(root)"
    return f"ModelPatchProposal at {where}: {error.message}"


def record_message(
    pool: ConnectionPool,
    project_id: str,
    msg: dict[str, Any],
    *,
    call_ids: list[str],
    context_manifest: list[str] | None = None,
    context_dropped: list[dict[str, str]] | None = None,
) -> None:
    """Append one message (idempotent on its id: a retried activity records nothing twice)."""
    with pool.connection() as conn:
        conn.execute(
            "INSERT INTO ag_messages (project_id, msg_id, session_id, task_id, parent_task, type, "
            "agent, ts, body, depends_on, cost, call_ids, context_manifest, context_dropped) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (project_id, msg_id) DO NOTHING",
            (
                project_id,
                msg["msg_id"],
                msg["session_id"],
                msg["task_id"],
                msg.get("parent_task"),
                msg["type"],
                Jsonb(msg["agent"]),
                msg["ts"],
                Jsonb(msg["body"]),
                Jsonb(msg["depends_on"]),
                Jsonb(msg["cost"]),
                Jsonb(call_ids),
                Jsonb(context_manifest) if context_manifest is not None else None,
                Jsonb(context_dropped) if context_dropped is not None else None,
            ),
        )


def messages_of(
    pool: ConnectionPool, project_id: str, session_id: str, type_: str | None = None
) -> list[dict[str, Any]]:
    query = (
        "SELECT msg_id, task_id, parent_task, type, agent, ts, body, depends_on, cost, call_ids, "
        "context_manifest, context_dropped FROM ag_messages "
        "WHERE project_id = %s AND session_id = %s"
    )
    params: list[Any] = [project_id, session_id]
    if type_ is not None:
        query += " AND type = %s"
        params.append(type_)
    with pool.connection() as conn:
        return conn.execute(query + " ORDER BY n", params).fetchall()


def content_key(*parts: Any) -> str:
    """A short deterministic key over any JSON-able parts (idempotency keys, adr ids)."""
    return hashlib.sha256(
        json.dumps(parts, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()[:16]
