"""Hybrid retrieval (spec §9): claim-first search over a project's knowledge.

Every hit is a CLAIM with its status, grade, conditions, taint and evidence locators, never
a bare chunk of text. Four signals each rank claims, and the ranks are fused:

  text       full-text search (Postgres tsvector) over each claim's canonical text and quote
  vector     nearest neighbours of the query among the claim embeddings
  graph      the query's entities (matched by name and by meaning) and the claims one hop
             from them
  community  the summaries of the graph's communities, when the query is global
             (scope="global", or no entity matched)

Fusion is Reciprocal Rank Fusion: score = sum over signals of 1 / (rrf_k + rank), ties broken
by claim id, so the same index gives the same answer. Each hit says which signals found it.

By default only committed claims of grade design_grade or unverified are returned, and
never a refuted or retracted one. Quarantined proposals are not claims: they appear only
when `grades` names "quarantined" explicitly. Confidence is returned for display and never
used to include or exclude (rule 10). Without an embedding provider the vector signal is
simply absent and search runs on the others.
"""

from __future__ import annotations

import re
from typing import Any

from psycopg import Connection
from psycopg_pool import ConnectionPool

from architect.gateway.errors import GatewayError
from architect.gateway.gateway import Gateway
from architect.knowledge import index
from architect.knowledge.config import KnowledgeConfig
from architect.knowledge.graph import ABOUT
from architect.knowledge.vectors import VectorIndex, cosine, stored_vectors
from architect.projections import COMPROMISING_STATUSES

SIGNALS = ("text", "vector", "graph", "community")
SCOPES = ("auto", "local", "global")
QUARANTINED = "quarantined"
COMMUNITY_TYPE = "community"

_WORD = re.compile(r"[a-z0-9]+")
STOPWORDS = frozenset(
    "a an and are as at be by does for from how in is it of on or that the this to what when "
    "where which who why with".split()
)


def query_terms(query: str) -> list[str]:
    """The query's words, lowercased, without stop words, in order and without repeats."""
    return list(dict.fromkeys(w for w in _WORD.findall(query.lower()) if w not in STOPWORDS))


def fuse(lists: dict[str, list[str]], rrf_k: int) -> list[tuple[str, float, dict[str, int]]]:
    """Reciprocal Rank Fusion of ranked id lists: (id, score, {signal: rank}), best first,
    ties broken by id."""
    scores: dict[str, float] = {}
    ranks: dict[str, dict[str, int]] = {}
    for signal, ids in lists.items():
        for rank, item_id in enumerate(ids, start=1):
            scores[item_id] = scores.get(item_id, 0.0) + 1.0 / (rrf_k + rank)
            ranks.setdefault(item_id, {})[signal] = rank
    return [
        (item_id, score, ranks[item_id])
        for item_id, score in sorted(scores.items(), key=lambda kv: (-kv[1], kv[0]))
    ]


def _tsquery(terms: list[str]) -> str:
    return " | ".join(terms)


def _text_list(
    conn: Connection[dict[str, Any]], project_id: str, terms: list[str], limit: int
) -> list[str]:
    if not terms:
        return []
    rows = conn.execute(
        "SELECT t.claim_id FROM kg_claim_text t, to_tsquery('english', %s) q "
        "WHERE t.project_id = %s AND t.tsv @@ q AND t.subject_type <> %s "
        "ORDER BY ts_rank(t.tsv, q) DESC, t.claim_id LIMIT %s",
        (_tsquery(terms), project_id, COMMUNITY_TYPE, limit),
    ).fetchall()
    return [row["claim_id"] for row in rows]


def _entities(
    conn: Connection[dict[str, Any]],
    project_id: str,
    terms: list[str],
    by_meaning: list[tuple[str, float]],
    config: KnowledgeConfig,
) -> list[dict[str, Any]]:
    """The entities a query is about: by name (full-text over entity names) and by meaning
    (nearest entity embeddings at or above the configured similarity), best first."""
    found: dict[str, dict[str, Any]] = {}
    if terms:
        rows = conn.execute(
            "SELECT e.entity_id, e.name FROM kg_entity_text e, to_tsquery('english', %s) q "
            "WHERE e.project_id = %s AND e.tsv @@ q "
            "ORDER BY ts_rank(e.tsv, q, 2) DESC, e.entity_id LIMIT %s",
            (_tsquery(terms), project_id, config.entity_matches),
        ).fetchall()
        for rank, row in enumerate(rows, start=1):
            found[row["entity_id"]] = {"id": row["entity_id"], "by": ["name"], "rank": rank}
    close = [(entity, score) for entity, score in by_meaning if score >= config.entity_similarity]
    for rank, (entity_id, score) in enumerate(close[: config.entity_matches], start=1):
        entry = found.setdefault(entity_id, {"id": entity_id, "by": [], "rank": rank})
        entry["by"].append("meaning")
        entry["rank"] = min(entry["rank"], rank)
        entry["similarity"] = round(score, 6)
    ordered = sorted(found.values(), key=lambda e: (e["rank"], e["id"]))
    return ordered[: config.entity_matches]


def _with_aliases(
    conn: Connection[dict[str, Any]], project_id: str, entity_ids: list[str]
) -> dict[str, str]:
    """Every id that answers to one of these entities (itself and what was merged into it)
    -> the entity."""
    members = {entity_id: entity_id for entity_id in entity_ids}
    rows = conn.execute(
        "SELECT entity_id, canonical_id FROM proj_entity_alias WHERE project_id = %s "
        "AND (canonical_id = ANY(%s) OR entity_id = ANY(%s))",
        (project_id, entity_ids, entity_ids),
    ).fetchall()
    for row in rows:
        members[row["entity_id"]] = row["canonical_id"]
        members.setdefault(row["canonical_id"], row["canonical_id"])
    return members


def _graph_list(
    conn: Connection[dict[str, Any]], project_id: str, entities: list[str], limit: int
) -> list[str]:
    """Graph expansion: the claims one hop (ABOUT) from the matched entities. A claim scores
    the sum, over the matched entities it is about, of 1 / (the entity's match rank): about
    more of them, and about the better matched ones, ranks higher. Ties by claim id."""
    if not entities:
        return []
    members = _with_aliases(conn, project_id, entities)
    order = {entity: n for n, entity in enumerate(entities)}
    rows = conn.execute(
        "SELECT claim_id, dst FROM proj_graph_edges WHERE project_id = %s AND edge_type = %s "
        "AND dst = ANY(%s)",
        (project_id, ABOUT, sorted(members)),
    ).fetchall()
    touched: dict[str, set[str]] = {}
    for row in rows:
        kept = members[row["dst"]]
        if kept in order:
            touched.setdefault(row["claim_id"], set()).add(kept)
    weight = {
        claim_id: sum(1.0 / (order[entity] + 1) for entity in about)
        for claim_id, about in touched.items()
    }
    ranked = sorted(weight.items(), key=lambda kv: (-kv[1], kv[0]))
    return [claim_id for claim_id, _ in ranked[:limit]]


def _community_list(
    pool: ConnectionPool,
    conn: Connection[dict[str, Any]],
    project_id: str,
    terms: list[str],
    query_vector: list[float] | None,
    model: str | None,
    limit: int,
) -> list[str]:
    """The current community summaries, the closest to the query first (by meaning when
    there are embeddings, by words otherwise), then the coarser level, then the id."""
    rows = conn.execute(
        "SELECT summary_claim_id, level FROM kg_communities WHERE project_id = %s "
        "AND summary_claim_id IS NOT NULL",
        (project_id,),
    ).fetchall()
    if not rows:
        return []
    level = {row["summary_claim_id"]: row["level"] for row in rows}
    relevance = dict.fromkeys(level, 0.0)
    if query_vector is not None and model is not None:
        stored = stored_vectors(pool, project_id, "claim", model, sorted(level))
        for claim_id, vector in stored.items():
            relevance[claim_id] = cosine(query_vector, vector)
    elif terms:
        ranked = conn.execute(
            "SELECT t.claim_id, ts_rank(t.tsv, q) AS rank FROM kg_claim_text t, "
            "to_tsquery('english', %s) q WHERE t.project_id = %s AND t.claim_id = ANY(%s) "
            "AND t.tsv @@ q",
            (_tsquery(terms), project_id, sorted(level)),
        ).fetchall()
        for row in ranked:
            relevance[row["claim_id"]] = float(row["rank"])
    ordered = sorted(level, key=lambda c: (-relevance[c], -level[c], c))
    return ordered[:limit]


def _locators(claim: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {k: e[k] for k in ("source", "span", "kind") if k in e} for e in claim.get("evidence", [])
    ]


def _hit(row: dict[str, Any], score: float, ranks: dict[str, int]) -> dict[str, Any]:
    claim = row["claim"]
    return {
        "claim_id": row["claim_id"],
        "subject": claim["subject"],
        "predicate": claim["predicate"],
        "object": claim["object"],
        "magnitude": claim.get("magnitude"),
        "conditions": claim.get("conditions", {}),
        "status": row["status"],
        "grade": row["grade"],
        "confidence": row["confidence"],  # for display and ranking only (rule 10)
        "taint": claim["taint"],
        "evidence": _locators(claim),
        "premise_compromised": row["premise_compromised"],
        "signals": [signal for signal in SIGNALS if signal in ranks],
        "ranks": ranks,
        "score": round(score, 9),
        "first_seq": row["first_seq"],
        "claim": claim,
    }


def _quarantined(
    conn: Connection[dict[str, Any]], project_id: str, terms: list[str], limit: int
) -> list[dict[str, Any]]:
    """Proposals that were never committed, by word overlap with the query. Only when asked."""
    rows = conn.execute(
        "SELECT claim_id, claim, seq FROM proj_claim_proposals WHERE project_id = %s "
        "AND NOT committed ORDER BY seq",
        (project_id,),
    ).fetchall()
    wanted = set(terms)
    scored = []
    for row in rows:
        overlap = len(wanted & set(_WORD.findall(index.claim_text(row["claim"]).lower())))
        if overlap:
            scored.append((-overlap, row["claim_id"], row))
    return [
        {
            "claim_id": row["claim_id"],
            "claim": row["claim"],
            "status": row["claim"]["status"],
            "grade": QUARANTINED,
            "confidence": None,
            "premise_compromised": False,
            "first_seq": row["seq"],
        }
        for _, _, row in sorted(scored, key=lambda item: item[:2])[:limit]
    ]


def search(
    pool: ConnectionPool,
    project_id: str,
    query: str,
    *,
    config: KnowledgeConfig,
    gateway: Gateway | None = None,
    vectors: VectorIndex | None = None,
    k: int | None = None,
    grades: list[str] | None = None,
    statuses: list[str] | None = None,
    taints: list[str] | None = None,
    entities: list[str] | None = None,
    scope: str = "auto",
    signals: list[str] | None = None,
    exclude_subject_types: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Search a project's claims. Returns {"hits": [...], "entities": [...], "signals": [...],
    "scope": ...}: the hits best first, the entities the query matched, the signals that ran.

    Filters: `grades` (default design_grade and unverified; add "quarantined" to see
    proposals that were never committed), `statuses` (default: everything but refuted and
    retracted), `taints` (taint origins), `entities` (only claims about these entity ids).
    `signals` restricts which signals run (for ablation)."""
    if scope not in SCOPES:
        raise ValueError(f"scope must be one of {SCOPES}")
    wanted_signals = set(signals) if signals is not None else set(SIGNALS)
    if not wanted_signals <= set(SIGNALS):
        raise ValueError(f"signals must be among {SIGNALS}")
    k = config.k if k is None else k
    grades = list(grades) if grades is not None else list(config.default_grades)
    limit = max(config.candidates, k)
    terms = query_terms(query)

    # the index follows the read models; embeddings only when a provider can serve them
    model: str | None = None
    query_vector: list[float] | None = None
    embedder = gateway if gateway is not None and vectors is not None else None
    if embedder is not None and embedder.can_embed(config.embedding_tier):
        try:
            state = index.sync(pool, project_id, config=config, gateway=embedder, vectors=vectors)
            model = state["model"]
            if query.strip():
                query_vector = embedder.embed(
                    [query],
                    purpose="search-query",
                    scope=index.knowledge_scope(project_id),
                    tier=config.embedding_tier,
                ).vectors[0]
        except GatewayError:
            model, query_vector = None, None  # no embeddings this time: the other signals run
    if model is None:
        index.sync(pool, project_id, config=config)

    lists: dict[str, list[str]] = {}
    with pool.connection() as conn:
        if "text" in wanted_signals:
            lists["text"] = _text_list(conn, project_id, terms, limit * 4)
        by_meaning: list[tuple[str, float]] = []
        if query_vector is not None and any(query_vector):
            if "vector" in wanted_signals:
                near = vectors.search(project_id, "claim", model, query_vector, limit * 4)
                lists["vector"] = [claim_id for claim_id, _ in near]
            by_meaning = vectors.search(
                project_id, "entity", model, query_vector, config.entity_matches
            )
        matched = _entities(conn, project_id, terms, by_meaning, config)
        if "graph" in wanted_signals:
            lists["graph"] = _graph_list(conn, project_id, [e["id"] for e in matched], limit * 4)
        is_global = scope == "global" or (scope == "auto" and not matched)
        if "community" in wanted_signals and is_global:
            lists["community"] = _community_list(
                pool, conn, project_id, terms, query_vector, model, limit
            )

        # one read of every candidate, with the filters applied
        candidates = sorted({claim_id for ids in lists.values() for claim_id in ids})
        committed_grades = [g for g in grades if g != QUARANTINED]
        only = None
        if entities:
            members = _with_aliases(conn, project_id, list(entities))
            only = sorted(members)
        rows = conn.execute(
            "SELECT c.claim_id, c.claim, c.status, c.grade, c.confidence, c.taint_origin, "
            "c.premise_compromised, c.first_seq FROM proj_claims c "
            "WHERE c.project_id = %(pid)s AND c.claim_id = ANY(%(ids)s) "
            "AND c.grade = ANY(%(grades)s) "
            "AND (%(statuses)s::text[] IS NULL OR c.status = ANY(%(statuses)s::text[])) "
            "AND (%(statuses)s::text[] IS NOT NULL OR c.status <> ALL(%(bad)s)) "
            "AND (%(taints)s::text[] IS NULL OR c.taint_origin = ANY(%(taints)s::text[])) "
            "AND (%(only)s::text[] IS NULL OR EXISTS (SELECT 1 FROM proj_graph_edges e "
            " WHERE e.project_id = c.project_id AND e.claim_id = c.claim_id "
            " AND e.edge_type = %(about)s AND e.dst = ANY(%(only)s::text[])))",
            {
                "pid": project_id,
                "ids": candidates,
                "grades": committed_grades,
                "statuses": list(statuses) if statuses is not None else None,
                "bad": list(COMPROMISING_STATUSES),
                "taints": list(taints) if taints is not None else None,
                "only": only,
                "about": ABOUT,
            },
        ).fetchall()
        # a community summary is an answer to a global question only: it comes in through
        # the community signal or not at all
        summaries = set(lists.get("community", []))
        kept = {}
        for row in rows:
            subject_type = row["claim"]["subject"].get("entity_type")
            if subject_type in exclude_subject_types:
                continue
            if subject_type == COMMUNITY_TYPE and row["claim_id"] not in summaries:
                continue
            kept[row["claim_id"]] = row
        if QUARANTINED in grades:
            for row in _quarantined(conn, project_id, terms, limit):
                if taints is None or row["claim"]["taint"]["origin"] in taints:
                    kept[row["claim_id"]] = row
                    lists.setdefault("text", []).append(row["claim_id"])

    filtered = {
        signal: [claim_id for claim_id in ids if claim_id in kept][:limit]
        for signal, ids in lists.items()
    }
    fused = fuse(filtered, config.rrf_k)
    hits = [_hit(kept[claim_id], score, ranks) for claim_id, score, ranks in fused[:k]]
    return {
        "query": query,
        "scope": "global" if is_global else "local",
        "signals": [signal for signal in SIGNALS if signal in lists],
        "entities": matched,
        "embedding_model": model,
        "hits": hits,
    }
