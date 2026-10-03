"""The Context Compiler (spec §12-6, minimal): one agent call's working context, packed to a
token target, with a manifest of what went in and a record of what was dropped and why.

Facts come from the read models only. Requirements, owner guidance, the current head model,
the failing check evidence and the remaining budget are always included; ranked relevant
claims fill the rest of the budget in rank order. A claim is a fact only when it is
committed: proposals never committed (quarantined, M5) are never included, whatever their
confidence (rule 10). Claims from external_untrusted sources arrive inside untrusted data
blocks and the request's input taints say so, so the gateway prepends the fixed rule.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass, field
from typing import Any

from psycopg_pool import ConnectionPool

from architect.gateway.untrusted import wrap_untrusted
from architect.projections import COMPROMISING_STATUSES

# subject entity types the compiler treats as the session's own statements
REQUIREMENT_TYPES = ("requirement", "constraint")
GUIDANCE_TYPE = "owner_guidance"

STOPWORDS = frozenset(
    "the and that with from this into for are must will shall should when then than over "
    "under each every also only have has been being within without about after before "
    "between through during which while where their there these those what some such".split()
)
_WORD = re.compile(r"[a-z0-9][a-z0-9_./-]{2,}")


@dataclass
class CompileTask:
    project_id: str
    session_id: str
    goal: str
    phase: str
    round: int
    token_target: int
    brief: str | None = None
    brief_source_id: str | None = None
    head_version: str | None = None
    research_claim_ids: list[str] = field(default_factory=list)
    failing_checks: list[dict[str, Any]] = field(default_factory=list)
    remaining_budget: dict[str, Any] = field(default_factory=dict)


@dataclass
class Compiled:
    text: str
    manifest: list[str]
    dropped: list[dict[str, str]]
    input_taints: list[str]
    claim_ids: list[str]
    scope_element_ids: list[str]
    depends_on: list[str]
    tokens: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "manifest": self.manifest,
            "dropped": self.dropped,
            "input_taints": self.input_taints,
            "claim_ids": self.claim_ids,
            "scope_element_ids": self.scope_element_ids,
            "depends_on": self.depends_on,
            "tokens": self.tokens,
        }


def estimate_tokens(text: str) -> int:
    """The gateway's estimate: four characters per token."""
    return max(1, math.ceil(len(text) / 4))


def words(text: str) -> set[str]:
    return {w for w in _WORD.findall(text.lower()) if w not in STOPWORDS}


def claim_words(claim: dict[str, Any]) -> set[str]:
    parts = [
        str(claim["subject"].get("id", claim["subject"].get("literal", ""))),
        claim["predicate"].replace("_", " "),
        str(claim["object"].get("id", claim["object"].get("literal", ""))),
        " ".join(str(k) for k in claim.get("conditions", {})),
    ]
    return words(" ".join(parts).replace("-", " ") + " " + " ".join(parts))


def _committed_claims(pool: ConnectionPool, project_id: str) -> list[dict[str, Any]]:
    with pool.connection() as conn:
        return conn.execute(
            "SELECT claim_id, claim, status, grade, confidence, taint_origin, first_seq "
            "FROM proj_claims WHERE project_id = %s ORDER BY first_seq",
            (project_id,),
        ).fetchall()


def rank_claims(pool: ConnectionPool, project_id: str, query_text: str) -> list[dict[str, Any]]:
    """Research retrieval (M6): committed, uncompromised claims that share vocabulary with the
    query, best overlap first, commit order on ties. Requirements, constraints and owner
    guidance are the session's own statements and are not ranked here."""
    query = words(query_text)
    ranked: list[tuple[int, int, dict[str, Any]]] = []
    for row in _committed_claims(pool, project_id):
        claim = row["claim"]
        if row["status"] in COMPROMISING_STATUSES:
            continue
        if claim["subject"].get("entity_type") in (*REQUIREMENT_TYPES, GUIDANCE_TYPE):
            continue
        score = len(query & claim_words(claim))
        if score > 0:
            ranked.append((-score, row["first_seq"], row | {"score": score}))
    ranked.sort(key=lambda item: (item[0], item[1]))
    return [row for _, _, row in ranked]


def _entity(ref: dict[str, Any]) -> str:
    if "id" in ref:
        return f"{ref['entity_type']}:{ref['id']}"
    return f"{ref['entity_type']}={json.dumps(ref.get('literal'), ensure_ascii=False)}"


def claim_line(row: dict[str, Any]) -> str:
    """One claim as the agent sees it: id, status, grade, taint, the triple, magnitude,
    conditions and evidence locators. Confidence is deliberately absent (rule 10)."""
    claim = row["claim"]
    parts = [
        f"{row['claim_id']} [status={row['status']} grade={row.get('grade', 'unverified')} "
        f"taint={row['taint_origin']}]",
        f"{_entity(claim['subject'])} {claim['predicate']} {_entity(claim['object'])}",
    ]
    if claim.get("magnitude"):
        parts.append(f"= {claim['magnitude']['value']:g} {claim['magnitude']['unit']}")
    if claim.get("conditions"):
        parts.append("conditions " + json.dumps(claim["conditions"], sort_keys=True))
    locators = [
        e["source"] + (f" {e['span']}" if e.get("span") else "") for e in claim.get("evidence", [])
    ]
    if locators:
        parts.append("evidence " + "; ".join(locators))
    line = " ".join(parts)
    if row["taint_origin"] == "external_untrusted":
        return wrap_untrusted(line, row["claim_id"])
    return "- " + line


def compact_model(model: dict[str, Any]) -> str:
    lines = [f"version {model['version_id']}"]
    for element_type, elements in model.get("elements", {}).items():
        lines.append(f"{element_type}:")
        for element in elements:
            lines.append("  " + json.dumps(element, sort_keys=True, separators=(",", ":")))
    for link_type, links in model.get("links", {}).items():
        lines.append(f"links.{link_type}:")
        for link in links:
            lines.append("  " + json.dumps(link, sort_keys=True, separators=(",", ":")))
    return "\n".join(lines)


def _event_ids(pool: ConnectionPool, project_id: str, seqs: list[int]) -> list[str]:
    if not seqs:
        return []
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT event_id FROM events WHERE project_id = %s AND seq = ANY(%s) ORDER BY seq",
            (project_id, sorted(set(seqs))),
        ).fetchall()
    return [row["event_id"] for row in rows]


def compile(pool: ConnectionPool, task: CompileTask) -> Compiled:  # noqa: A001 - the spec's name
    claims = {row["claim_id"]: row for row in _committed_claims(pool, task.project_id)}
    manifest: list[str] = []
    dropped: list[dict[str, str]] = []
    claim_ids: list[str] = []
    seqs: list[int] = []
    taints: set[str] = set()

    def include(row: dict[str, Any]) -> str:
        manifest.append(row["claim_id"])
        claim_ids.append(row["claim_id"])
        seqs.append(row["first_seq"])
        taints.add(row["taint_origin"])
        return claim_line(row)

    sections: list[str] = [f"# Goal\n{task.goal}"]
    if task.brief is not None:
        sections.append("# Brief (owner's own words)\n" + task.brief.strip())
        if task.brief_source_id is not None:
            manifest.append(task.brief_source_id)

    live = [r for r in claims.values() if r["status"] not in COMPROMISING_STATUSES]
    requirements = [
        r for r in live if r["claim"]["subject"].get("entity_type") in REQUIREMENT_TYPES
    ]
    guidance = [r for r in live if r["claim"]["subject"].get("entity_type") == GUIDANCE_TYPE]
    if requirements:
        sections.append("# Requirements and constraints\n" + "\n".join(map(include, requirements)))
    if guidance:
        sections.append("# Owner guidance (steer)\n" + "\n".join(map(include, guidance)))

    scope: list[str] = []
    if task.head_version is not None:
        with pool.connection() as conn:
            version = conn.execute(
                "SELECT model, committed_at_seq FROM proj_model_versions "
                "WHERE project_id = %s AND version_id = %s",
                (task.project_id, task.head_version),
            ).fetchone()
        if version is not None:
            manifest.append(task.head_version)
            seqs.append(version["committed_at_seq"])
            for elements in version["model"].get("elements", {}).values():
                scope.extend(e["id"] for e in elements if "id" in e)
            sections.append("# Current head model\n" + compact_model(version["model"]))

    if task.failing_checks:
        lines = []
        for check in task.failing_checks:
            manifest.append(check.get("result_id", check["check_id"]))
            lines.append(
                f"- {check['check_id']} ({check.get('severity', '?')}) {check['status']} on "
                f"{', '.join(check.get('element_refs', [])) or '-'}: "
                + json.dumps(check.get("evidence", {}), sort_keys=True)
            )
        sections.append("# Failing checks (evidence)\n" + "\n".join(lines))

    if task.remaining_budget:
        sections.append("# Remaining budget\n" + json.dumps(task.remaining_budget, sort_keys=True))

    # Quarantined claims are never facts (rule 10), whatever they claim about themselves.
    with pool.connection() as conn:
        quarantined = conn.execute(
            "SELECT claim_id FROM proj_claim_proposals WHERE project_id = %s AND NOT committed "
            "ORDER BY seq",
            (task.project_id,),
        ).fetchall()
    for row in quarantined:
        if row["claim_id"] not in claims:
            dropped.append({"id": row["claim_id"], "reason": "quarantined"})

    mandatory = "\n\n".join(sections)
    used = estimate_tokens(mandatory)
    relevant_lines: list[str] = []
    for claim_id in task.research_claim_ids:
        row = claims.get(claim_id)
        if row is None:
            dropped.append({"id": claim_id, "reason": "not_committed"})
            continue
        if row["status"] in COMPROMISING_STATUSES:
            dropped.append({"id": claim_id, "reason": f"status_{row['status']}"})
            continue
        if claim_id in claim_ids:
            continue
        line = claim_line(row)
        cost = estimate_tokens(line + "\n")
        if used + cost > task.token_target:
            dropped.append({"id": claim_id, "reason": "token_target"})
            continue
        used += cost
        relevant_lines.append(include(row))
    if relevant_lines:
        sections.insert(
            2 if task.brief is not None else 1,
            "# Relevant claims (ranked)\n" + "\n".join(relevant_lines),
        )
    text = "\n\n".join(sections)
    return Compiled(
        text=text,
        manifest=manifest,
        dropped=dropped,
        input_taints=sorted(taints),
        claim_ids=claim_ids,
        scope_element_ids=scope,
        depends_on=_event_ids(pool, task.project_id, seqs),
        tokens=estimate_tokens(text),
    )
