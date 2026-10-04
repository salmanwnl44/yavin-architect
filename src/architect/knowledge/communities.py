"""Communities (GraphRAG-style): the themes of a project's entity graph, each with a summary.

Leiden community detection (leidenalg over python-igraph) runs over the entity graph: the
entities that committed, live claims relate, an edge weighing as many claims as relate the
pair. The seed is fixed and the vertices and edges are fed in sorted order, so the same graph
always gives the same partition. Two levels: level 0 partitions the entities; level 1
partitions level 0's communities (the same algorithm on the aggregated graph, at the second
configured resolution), so every level-1 community is a union of level-0 ones.

Each community of at least `min_size` entities gets ONE summary, written by a model through
the gateway (tier-mid, purpose `community-summary`) from the community's top claims; a claim
from an untrusted source is handed over as untrusted data. The summary is committed as a
claim: subject {community, comm_<level>_<hash of the members>}, predicate SUMMARIZES, object
{text, the summary}, status `inferred`, provenance.derived_from = the claims it was written
from, taint = the most restrictive taint among them.

Incremental: a community's input hash (its members and the claims its summary is written
from) decides whether a summary is written. An unchanged community never calls the model
again.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any

from psycopg import Connection
from psycopg_pool import ConnectionPool

from architect.arbiter import Arbiter
from architect.gateway.gateway import Gateway
from architect.gateway.request import GatewayRequest
from architect.gateway.untrusted import wrap_untrusted
from architect.ingestion.normalize import typed_id
from architect.knowledge import index
from architect.knowledge.config import KnowledgeConfig
from architect.knowledge.graph import ABOUT
from architect.projections import COMPROMISING_STATUSES
from architect.projector import Projector

ACTOR = {"kind": "agent", "id": "community-summarizer", "role": "summarizer"}
COMMUNITY_TYPE = "community"
SUMMARIZES = "SUMMARIZES"
PIPELINE_VERSION = 1
# least trusted first: a summary is as tainted as the least trusted claim it was written from
TAINT_ORDER = ("external_untrusted", "external_trusted", "user", "internal")
SUMMARY_SYSTEM = (
    "You summarize one theme of a knowledge base. Below are the claims a group of related "
    "entities appears in, one per line, each with its status and grade. Write a summary of "
    "at most 120 words: what the group is about, what the claims say, and where they are "
    "conditional or weak (a status of assumed or proposed, a grade of unverified). State "
    "only what the claims state. Plain prose, no lists, no preamble."
)


@dataclass
class Community:
    level: int
    community_id: str
    members: list[str]
    parent_id: str | None = None
    claim_ids: list[str] = field(default_factory=list)
    input_hash: str = ""
    summary_claim_id: str | None = None


def community_id(level: int, members: list[str]) -> str:
    digest = hashlib.sha256("\x1f".join(sorted(members)).encode()).hexdigest()
    return f"comm_{level}_{digest[:16]}"


def entity_graph(
    conn: Connection[dict[str, Any]], project_id: str
) -> tuple[list[str], dict[tuple[str, str], int]]:
    """The entities related by live claims, and for each related pair how many claims relate
    it. Merged entities count as the one that was kept."""
    rows = conn.execute(
        "SELECT coalesce(a.canonical_id, e.src) AS src, coalesce(b.canonical_id, e.dst) AS dst "
        "FROM proj_graph_edges e "
        "JOIN proj_claims c ON c.project_id = e.project_id AND c.claim_id = e.claim_id "
        "LEFT JOIN proj_entity_alias a ON a.project_id = e.project_id AND a.entity_id = e.src "
        "LEFT JOIN proj_entity_alias b ON b.project_id = e.project_id AND b.entity_id = e.dst "
        "WHERE e.project_id = %s AND e.edge_type <> %s AND c.status <> ALL(%s)",
        (project_id, ABOUT, list(COMPROMISING_STATUSES)),
    ).fetchall()
    entities = {
        row["node_id"]
        for row in conn.execute(
            "SELECT node_id FROM proj_graph_nodes WHERE project_id = %s AND node_type = 'entity'",
            (project_id,),
        ).fetchall()
    }
    weights: dict[tuple[str, str], int] = {}
    for row in rows:
        a, b = sorted((row["src"], row["dst"]))
        if a != b and a in entities and b in entities:
            weights[(a, b)] = weights.get((a, b), 0) + 1
    nodes = sorted({node for pair in weights for node in pair})
    return nodes, weights


def _leiden(
    nodes: list[str], weights: dict[tuple[str, str], int], resolution: float, seed: int
) -> list[list[str]]:
    """Leiden over a weighted graph given in sorted order. Groups sorted, members sorted."""
    import igraph
    import leidenalg

    if not nodes:
        return []
    position = {node: i for i, node in enumerate(nodes)}
    pairs = sorted(weights)
    graph = igraph.Graph(n=len(nodes), edges=[(position[a], position[b]) for a, b in pairs])
    graph.es["weight"] = [weights[pair] for pair in pairs]
    found = leidenalg.find_partition(
        graph,
        leidenalg.RBConfigurationVertexPartition,
        weights="weight",
        resolution_parameter=resolution,
        seed=seed,
    )
    groups: dict[int, list[str]] = {}
    for node, group in zip(nodes, found.membership, strict=True):
        groups.setdefault(group, []).append(node)
    return sorted(sorted(members) for members in groups.values())


def partition(
    nodes: list[str], weights: dict[tuple[str, str], int], config: KnowledgeConfig
) -> list[Community]:
    """Two levels of communities. A level-1 community that would hold a single level-0
    community is not a theme of its own and is left out."""
    fine = _leiden(nodes, weights, config.resolutions[0], config.seed)
    level0 = [Community(0, community_id(0, members), members) for members in fine]
    home = {member: c.community_id for c in level0 for member in c.members}
    between: dict[tuple[str, str], int] = {}
    for (a, b), weight in weights.items():
        x, y = sorted((home[a], home[b]))
        if x != y:
            between[(x, y)] = between.get((x, y), 0) + weight
    coarse = _leiden(
        sorted(c.community_id for c in level0), between, config.resolutions[1], config.seed
    )
    by_id = {c.community_id: c for c in level0}
    level1: list[Community] = []
    for group in coarse:
        if len(group) < 2:
            continue
        members = sorted(member for child in group for member in by_id[child].members)
        parent = Community(1, community_id(1, members), members)
        for child in group:
            by_id[child].parent_id = parent.community_id
        level1.append(parent)
    return level0 + level1


def _top_claims(
    conn: Connection[dict[str, Any]], project_id: str, members: list[str], limit: int
) -> list[dict[str, Any]]:
    """The live claims about the community's entities that a summary is written from: design
    grade first, then the more confident (confidence ranks, it never decides), then the id."""
    return conn.execute(
        "SELECT c.claim_id, c.claim, c.status, c.grade, c.taint_origin FROM proj_claims c "
        "WHERE c.project_id = %(pid)s AND c.status <> ALL(%(bad)s) "
        "AND c.claim -> 'subject' ->> 'entity_type' <> %(community)s "
        "AND EXISTS (SELECT 1 FROM proj_graph_edges e LEFT JOIN proj_entity_alias a "
        " ON a.project_id = e.project_id AND a.entity_id = e.dst "
        " WHERE e.project_id = c.project_id AND e.claim_id = c.claim_id "
        " AND e.edge_type = %(about)s AND coalesce(a.canonical_id, e.dst) = ANY(%(members)s)) "
        "ORDER BY (c.grade = 'design_grade') DESC, c.confidence DESC NULLS LAST, c.claim_id "
        "LIMIT %(limit)s",
        {
            "pid": project_id,
            "bad": list(COMPROMISING_STATUSES),
            "community": COMMUNITY_TYPE,
            "about": ABOUT,
            "members": members,
            "limit": limit,
        },
    ).fetchall()


def _input_hash(community: Community, claims: list[dict[str, Any]]) -> str:
    parts = [f"v{PIPELINE_VERSION}", community.community_id, *community.members]
    parts += [f"{row['claim_id']}={row['status']}/{row['grade']}" for row in claims]
    return hashlib.sha256("\x1f".join(parts).encode()).hexdigest()


def _claim_line(row: dict[str, Any]) -> str:
    line = f"{row['claim_id']} [status={row['status']} grade={row['grade']}] " + index.claim_text(
        row["claim"]
    )
    if row["taint_origin"] == "external_untrusted":
        return wrap_untrusted(line, row["claim_id"])
    return "- " + line


def _summarize(
    pool: ConnectionPool,
    gateway: Gateway,
    project_id: str,
    community: Community,
    claims: list[dict[str, Any]],
    config: KnowledgeConfig,
) -> dict[str, Any]:
    """One model call, and the summary claim built from its answer."""
    taints = sorted({row["taint_origin"] for row in claims})
    response = gateway.call(
        GatewayRequest(
            role="summarizer",
            tier=config.summary_tier,
            purpose="community-summary",
            system=SUMMARY_SYSTEM,
            messages=[
                {
                    "role": "user",
                    "content": f"Entities: {', '.join(community.members)}\n\nClaims:\n"
                    + "\n".join(_claim_line(row) for row in claims),
                }
            ],
            max_tokens=config.summary_max_tokens,
            scope=index.knowledge_scope(project_id),
            input_taints=taints,
        )
    )
    with pool.connection() as conn:
        prompt_hash = conn.execute(
            "SELECT prompt_hash FROM gw_calls WHERE call_id = %s", (response.call_id,)
        ).fetchone()["prompt_hash"]
    claim: dict[str, Any] = {
        "id": community.summary_claim_id,
        "subject": {"entity_type": COMMUNITY_TYPE, "id": community.community_id},
        "predicate": SUMMARIZES,
        "object": {"entity_type": "text", "literal": response.text.strip() or "(empty summary)"},
        "status": "inferred",
        "taint": {"origin": next(t for t in TAINT_ORDER if t in taints)},
        "recorded_at": max(row["claim"]["recorded_at"] for row in claims),
        "provenance": {
            "extractor": {
                "model_tier": config.summary_tier,
                "prompt_hash": prompt_hash,
                "pipeline_version": PIPELINE_VERSION,
            },
            "derived_from": sorted(row["claim_id"] for row in claims),
        },
    }
    return claim


def rebuild(
    pool: ConnectionPool, project_id: str, *, gateway: Gateway, config: KnowledgeConfig
) -> dict[str, Any]:
    """Detect the communities of the project's entity graph and make sure each has a current
    summary. Returns the communities, how many summaries were written (one model call each)
    and how many were kept as they were."""
    Projector(pool).catch_up(project_id)
    arbiter = Arbiter(pool)
    written: list[str] = []
    kept: list[str] = []
    with pool.connection() as conn:
        nodes, weights = entity_graph(conn, project_id)
        communities = partition(nodes, weights, config)
        previous = {
            row["community_id"]: row
            for row in conn.execute(
                "SELECT community_id, input_hash, summary_claim_id FROM kg_communities "
                "WHERE project_id = %s",
                (project_id,),
            ).fetchall()
        }
        inputs: dict[str, list[dict[str, Any]]] = {}
        for community in communities:
            claims = _top_claims(conn, project_id, community.members, config.top_claims)
            inputs[community.community_id] = claims
            community.claim_ids = [row["claim_id"] for row in claims]
            community.input_hash = _input_hash(community, claims)
    for community in communities:
        claims = inputs[community.community_id]
        if len(community.members) < config.min_size or not claims:
            continue
        community.summary_claim_id = typed_id(
            "clm", project_id, community.community_id, community.input_hash
        )
        before = previous.get(community.community_id)
        unchanged = (
            before is not None
            and before["input_hash"] == community.input_hash
            and before["summary_claim_id"] == community.summary_claim_id
        )
        if not unchanged:
            with pool.connection() as conn:
                unchanged = (
                    conn.execute(
                        "SELECT 1 AS yes FROM proj_claims WHERE project_id = %s AND claim_id = %s",
                        (project_id, community.summary_claim_id),
                    ).fetchone()
                    is not None
                )
        if unchanged:
            kept.append(community.community_id)
            continue
        claim = _summarize(pool, gateway, project_id, community, claims, config)
        arbiter.submit(
            project_id,
            {
                "actor": ACTOR,
                "type": "claim.committed",
                "payload": {"claim_id": claim["id"], "claim": claim},
                "idempotency_key": f"community-summary:{claim['id']}",
            },
        )
        written.append(community.community_id)
    with pool.connection() as conn, conn.transaction():
        conn.execute("DELETE FROM kg_communities WHERE project_id = %s", (project_id,))
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO kg_communities (project_id, community_id, level, parent_id, "
                "members, claim_ids, input_hash, summary_claim_id) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                [
                    (
                        project_id,
                        c.community_id,
                        c.level,
                        c.parent_id,
                        c.members,
                        c.claim_ids,
                        c.input_hash,
                        c.summary_claim_id,
                    )
                    for c in communities
                ],
            )
    if written:
        Projector(pool).catch_up(project_id)
    return {
        "communities": list_communities(pool, project_id),
        "written": written,
        "kept": kept,
    }


def list_communities(
    pool: ConnectionPool, project_id: str, level: int | None = None
) -> list[dict[str, Any]]:
    """The current communities with their summaries, level 0 first, then by id."""
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT k.community_id, k.level, k.parent_id, k.members, k.claim_ids, "
            "k.summary_claim_id, c.claim -> 'object' ->> 'literal' AS summary, "
            "c.taint_origin, c.status FROM kg_communities k "
            "LEFT JOIN proj_claims c ON c.project_id = k.project_id "
            "AND c.claim_id = k.summary_claim_id "
            "WHERE k.project_id = %s AND (%s::integer IS NULL OR k.level = %s::integer) "
            "ORDER BY k.level, k.community_id",
            (project_id, level, level),
        ).fetchall()
    return [dict(row) for row in rows]
