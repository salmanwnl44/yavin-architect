"""Entity resolution (spec §9-4): find the entities that are one thing under two names and
merge them, as events.

1. Blocking. Candidate pairs have the same entity_type AND (share a normalized name token
   OR their slugs are within edit distance 2). Entities of different types are never
   compared, so never merged.
2. Score. The cosine of the two entities' embeddings (the name, with a few predicates the
   entity appears in as context).
3. Decide. At or above `high` (0.92): merge (method `embedding`). Between `low` (0.80) and
   `high`: a model adjudicates through the gateway (tier-cheap, purpose `entity-adjudicate`,
   structured {same, reason}) and a yes merges (method `llm_adjudicated`). Below `low`:
   distinct.

A merge is an `entity.merged` event through the Arbiter; the graph projection then resolves
the merged id to the kept one. `entity.merge_reverted` undoes it, and a pair that was
reverted is never proposed again: the reverted merges in the ledger are the record.
"""

from __future__ import annotations

import re
from typing import Any

from psycopg_pool import ConnectionPool

from architect.arbiter import Arbiter
from architect.gateway.gateway import Gateway
from architect.gateway.request import GatewayRequest
from architect.gateway.untrusted import wrap_untrusted
from architect.knowledge import index
from architect.knowledge.config import KnowledgeConfig
from architect.knowledge.graph import ABOUT
from architect.knowledge.vectors import VectorIndex, cosine, stored_vectors
from architect.projector import Projector

ACTOR = {"kind": "agent", "id": "entity-resolver", "role": "resolver"}
ADJUDICATION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"same": {"type": "boolean"}, "reason": {"type": "string"}},
    "required": ["same", "reason"],
    "additionalProperties": False,
}
ADJUDICATION_SYSTEM = (
    "You decide whether two names refer to the same real-world entity. The two entities "
    "below were extracted from documents; each comes with its type and the relations it "
    "appears in. Answer same=true only when they are one and the same thing under two "
    "names (a spelling variant, an abbreviation, a synonym). Related but different things "
    "are not the same. Give a one-sentence reason."
)
_TOKEN = re.compile(r"[a-z0-9]+")


def name_tokens(name: str) -> set[str]:
    """Normalized tokens of an entity name; one-character tokens say nothing."""
    return {token for token in _TOKEN.findall(name.lower()) if len(token) > 1}


def edit_distance(a: str, b: str, limit: int) -> int:
    """Levenshtein distance, or limit + 1 as soon as it is known to exceed the limit."""
    if abs(len(a) - len(b)) > limit:
        return limit + 1
    previous = list(range(len(b) + 1))
    for i, x in enumerate(a, start=1):
        current = [i]
        for j, y in enumerate(b, start=1):
            current.append(min(previous[j] + 1, current[j - 1] + 1, previous[j - 1] + (x != y)))
        if min(current) > limit:
            return limit + 1
        previous = current
    return previous[-1]


def slug_of(entity_id: str) -> str:
    """'ent:technique:lease-fencing' -> 'lease-fencing'."""
    return entity_id.split(":", 2)[-1]


def blocked_pairs(entities: list[dict[str, Any]], max_edit_distance: int) -> list[tuple[str, str]]:
    """The candidate pairs, each as (smaller id, larger id), sorted."""
    by_type: dict[str, list[dict[str, Any]]] = {}
    for entity in entities:
        by_type.setdefault(entity["entity_type"], []).append(entity)
    pairs: set[tuple[str, str]] = set()
    for group in by_type.values():
        group = sorted(group, key=lambda e: e["entity_id"])
        tokens = [name_tokens(e["name"]) for e in group]
        slugs = [slug_of(e["entity_id"]) for e in group]
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                if tokens[i] & tokens[j] or (
                    edit_distance(slugs[i], slugs[j], max_edit_distance) <= max_edit_distance
                ):
                    pairs.add((group[i]["entity_id"], group[j]["entity_id"]))
    return sorted(pairs)


def reverted_pairs(pool: ConnectionPool, project_id: str) -> set[frozenset[str]]:
    """Every (kept, merged) pair of a merge that was reverted."""
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT kept_id, merged_ids FROM proj_entity_merges WHERE project_id = %s "
            "AND reverted_seq IS NOT NULL",
            (project_id,),
        ).fetchall()
    return {frozenset((row["kept_id"], merged)) for row in rows for merged in row["merged_ids"]}


def _describe(entity: dict[str, Any]) -> str:
    return f"type: {entity['entity_type']}\nname: {entity['name']}\ncontext: {entity['text']}"


def adjudicate(
    gateway: Gateway,
    project_id: str,
    a: dict[str, Any],
    b: dict[str, Any],
    config: KnowledgeConfig,
) -> dict[str, Any]:
    """Ask a model whether two entities are the same. The names came out of documents, so
    they travel as untrusted data."""
    content = (
        "Entity A:\n"
        + wrap_untrusted(_describe(a), a["entity_id"])
        + "\n\nEntity B:\n"
        + wrap_untrusted(_describe(b), b["entity_id"])
        + "\n\nAre A and B the same entity?"
    )
    response = gateway.call(
        GatewayRequest(
            role="resolver",
            tier=config.adjudicate_tier,
            purpose="entity-adjudicate",
            system=ADJUDICATION_SYSTEM,
            messages=[{"role": "user", "content": content}],
            output_schema=ADJUDICATION_SCHEMA,
            max_tokens=config.adjudicate_max_tokens,
            scope=index.knowledge_scope(project_id),
            input_taints=["external_untrusted"],
        )
    )
    return {"same": bool(response.parsed["same"]), "reason": str(response.parsed["reason"])}


def resolve_entities(
    pool: ConnectionPool,
    project_id: str,
    *,
    gateway: Gateway,
    vectors: VectorIndex,
    config: KnowledgeConfig,
) -> dict[str, Any]:
    """One resolution pass. Returns what it considered and what it merged. Running it again
    on an unchanged graph proposes nothing: merged entities no longer exist as candidates,
    reverted pairs are skipped, and a pair already adjudicated distinct is answered from the
    gateway's cache."""
    Projector(pool).catch_up(project_id)
    state = index.sync(pool, project_id, config=config, gateway=gateway, vectors=vectors)
    model = state["model"]
    if model is None:
        raise RuntimeError("entity resolution needs an embedding provider (the embedding tier)")
    with pool.connection() as conn:
        entities = conn.execute(
            "SELECT entity_id, entity_type, name, text FROM kg_entity_text "
            "WHERE project_id = %s ORDER BY entity_id",
            (project_id,),
        ).fetchall()
        claim_counts = {
            row["entity"]: row["n"]
            for row in conn.execute(
                "SELECT coalesce(a.canonical_id, e.dst) AS entity, count(DISTINCT e.claim_id) AS n "
                "FROM proj_graph_edges e LEFT JOIN proj_entity_alias a "
                "ON a.project_id = e.project_id AND a.entity_id = e.dst "
                "WHERE e.project_id = %s AND e.edge_type = %s GROUP BY 1",
                (project_id, ABOUT),
            ).fetchall()
        }
    by_id = {entity["entity_id"]: entity for entity in entities}
    embedded = stored_vectors(pool, project_id, "entity", model, sorted(by_id))
    reverted = reverted_pairs(pool, project_id)

    scored = []
    for a, b in blocked_pairs(entities, config.max_edit_distance):
        if a in embedded and b in embedded:
            scored.append((cosine(embedded[a], embedded[b]), a, b))
    scored.sort(key=lambda item: (-item[0], item[1], item[2]))

    parent: dict[str, str] = {}

    def find(entity: str) -> str:
        while parent.get(entity, entity) != entity:
            entity = parent[entity]
        return entity

    report: dict[str, Any] = {
        "entities": len(entities),
        "pairs": len(scored),
        "merged": [],
        "adjudicated": [],
        "distinct": 0,
        "skipped_reverted": 0,
    }
    arbiter = Arbiter(pool)
    for score, a, b in scored:
        if by_id[a]["entity_type"] != by_id[b]["entity_type"]:
            continue  # never across types (blocking already guarantees it)
        root_a, root_b = find(a), find(b)
        if root_a == root_b:
            continue
        if frozenset((a, b)) in reverted or frozenset((root_a, root_b)) in reverted:
            report["skipped_reverted"] += 1
            continue
        if score < config.low:
            report["distinct"] += 1
            continue
        method = "embedding"
        if score < config.high:
            verdict = adjudicate(gateway, project_id, by_id[a], by_id[b], config)
            report["adjudicated"].append({"a": a, "b": b, "score": round(score, 6), **verdict})
            if not verdict["same"]:
                report["distinct"] += 1
                continue
            method = "llm_adjudicated"
        # keep the entity more claims are about; the smaller id on a tie
        kept, merged = sorted((root_a, root_b), key=lambda e: (-claim_counts.get(e, 0), e))
        commit = arbiter.submit(
            project_id,
            {
                "actor": ACTOR,
                "type": "entity.merged",
                "payload": {"kept_id": kept, "merged_ids": [merged], "method": method},
                "idempotency_key": f"entity-merge:{kept}:{merged}",
            },
        )
        parent[merged] = kept
        claim_counts[kept] = claim_counts.get(kept, 0) + claim_counts.get(merged, 0)
        report["merged"].append(
            {
                "kept_id": kept,
                "merged_id": merged,
                "method": method,
                "score": round(score, 6),
                "event_id": commit.event["event_id"],
            }
        )
    if report["merged"]:
        Projector(pool).catch_up(project_id)
        index.sync(pool, project_id, config=config, gateway=gateway, vectors=vectors)
    return report


def revert_merge(
    pool: ConnectionPool, project_id: str, merge_event: str, *, signer: str
) -> dict[str, Any]:
    """Undo one merge (an `entity.merge_reverted` event, signed by a human). The pair is
    never proposed again."""
    commit = Arbiter(pool).submit(
        project_id,
        {
            "actor": {"kind": "human", "id": signer},
            "type": "entity.merge_reverted",
            "payload": {"merge_event": merge_event},
            "idempotency_key": f"entity-merge-reverted:{merge_event}",
        },
    )
    Projector(pool).catch_up(project_id)
    return commit.event
