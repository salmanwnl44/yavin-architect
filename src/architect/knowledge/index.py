"""The search index of a project: the canonical text of every committed claim, the name of
every entity, and their embeddings. Derived from the read models (and, for embeddings, the
gateway); `sync` brings it up to the projection's cursor and does nothing when it is there.

What gets embedded: each committed claim's canonical text (subject, predicate, object,
magnitude, conditions, quote) and each entity's name with a few predicates it appears in.
A claim or an entity is embedded again only when that text or the embedding model changes.
"""

from __future__ import annotations

import hashlib
from typing import Any

from psycopg import Connection
from psycopg_pool import ConnectionPool

from architect.gateway.gateway import Gateway
from architect.knowledge.config import KnowledgeConfig
from architect.knowledge.graph import ABOUT, ref_text
from architect.knowledge.vectors import VectorIndex, stored_hashes
from architect.projections import PROJECTION

KINDS = ("claim", "entity")


def knowledge_scope(project_id: str) -> dict[str, str]:
    """What the knowledge plane's own gateway calls are charged to: one scope per project,
    which a budget.updated event can cap like any other."""
    return {"session": f"knowledge:{project_id}"}


def text_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def claim_text(claim: dict[str, Any], quote: str | None = None) -> str:
    """The canonical text of a claim: what full-text search indexes and what is embedded."""
    parts = [
        ref_text(claim["subject"]),
        claim["predicate"].replace("_", " ").lower(),
        ref_text(claim["object"]),
    ]
    magnitude = claim.get("magnitude")
    if magnitude:
        parts.append(f"{magnitude['value']:g} {magnitude['unit']}")
    conditions = claim.get("conditions") or {}
    if conditions:
        parts.append("when " + ", ".join(f"{k} {v}" for k, v in sorted(conditions.items())))
    if quote:
        parts.append(f'"{" ".join(quote.split())}"')
    return " ".join(part for part in parts if part)


def cursor_seq(conn: Connection[dict[str, Any]], project_id: str) -> int:
    row = conn.execute(
        "SELECT last_seq FROM proj_cursors WHERE projection = %s AND project_id = %s",
        (PROJECTION, project_id),
    ).fetchone()
    return row["last_seq"] if row else -1


def _claim_texts(conn: Connection[dict[str, Any]], project_id: str) -> dict[str, tuple[str, str]]:
    """claim id -> (subject entity type, canonical text)."""
    rows = conn.execute(
        "SELECT c.claim_id, c.claim, q.quote FROM proj_claims c "
        "LEFT JOIN ing_claim_quotes q ON q.project_id = c.project_id AND q.claim_id = c.claim_id "
        "WHERE c.project_id = %s",
        (project_id,),
    ).fetchall()
    return {
        row["claim_id"]: (
            str(row["claim"]["subject"].get("entity_type", "")),
            claim_text(row["claim"], row["quote"]),
        )
        for row in rows
    }


def _entity_texts(
    conn: Connection[dict[str, Any]], project_id: str, context_predicates: int
) -> dict[str, tuple[str, str, str]]:
    """entity id -> (entity type, name, text to embed). Entities merged into another are not
    listed; the one that was kept answers for them and carries their names."""
    nodes = conn.execute(
        "SELECT n.node_id, n.entity_type, n.label, a.canonical_id FROM proj_graph_nodes n "
        "LEFT JOIN proj_entity_alias a ON a.project_id = n.project_id AND a.entity_id = n.node_id "
        "WHERE n.project_id = %s AND n.node_type = 'entity' ORDER BY n.node_id",
        (project_id,),
    ).fetchall()
    predicates = conn.execute(
        "SELECT coalesce(a.canonical_id, x.entity) AS entity, x.edge_type, count(*) AS n FROM ("
        " SELECT src AS entity, edge_type FROM proj_graph_edges "
        "  WHERE project_id = %(pid)s AND edge_type <> %(about)s "
        " UNION ALL SELECT dst, edge_type FROM proj_graph_edges "
        "  WHERE project_id = %(pid)s AND edge_type <> %(about)s) x "
        "LEFT JOIN proj_entity_alias a ON a.project_id = %(pid)s AND a.entity_id = x.entity "
        "GROUP BY 1, 2",
        {"pid": project_id, "about": ABOUT},
    ).fetchall()
    context: dict[str, list[tuple[int, str]]] = {}
    for row in predicates:
        context.setdefault(row["entity"], []).append((-row["n"], row["edge_type"]))
    names: dict[str, list[str]] = {}
    types: dict[str, str] = {}
    for node in nodes:
        kept = node["canonical_id"] or node["node_id"]
        names.setdefault(kept, [])
        if node["canonical_id"] is None:
            names[kept].insert(0, node["label"])
            types[kept] = node["entity_type"] or "entity"
        elif node["label"] not in names[kept]:
            names[kept].append(node["label"])
    out: dict[str, tuple[str, str, str]] = {}
    for entity, labels in names.items():
        if entity not in types:
            continue  # merged into something that is not an entity node
        name = " / ".join(dict.fromkeys(labels))
        top = [p for _, p in sorted(context.get(entity, []))[:context_predicates]]
        text = (
            name if not top else f"{name} | {', '.join(p.replace('_', ' ').lower() for p in top)}"
        )
        out[entity] = (types[entity], name, text)
    return out


def _sync_text(
    conn: Connection[dict[str, Any]], project_id: str, config: KnowledgeConfig
) -> tuple[dict[str, str], dict[str, str]]:
    """Bring kg_claim_text and kg_entity_text up to the read models. Returns the text to embed
    of every claim and every entity."""
    claims = _claim_texts(conn, project_id)
    have = {
        row["claim_id"]: row["text_hash"]
        for row in conn.execute(
            "SELECT claim_id, text_hash FROM kg_claim_text WHERE project_id = %s", (project_id,)
        ).fetchall()
    }
    changed = [
        (project_id, claim_id, subject_type, text, text_hash(text))
        for claim_id, (subject_type, text) in claims.items()
        if have.get(claim_id) != text_hash(text)
    ]
    with conn.cursor() as cur:
        if changed:
            cur.executemany(
                "INSERT INTO kg_claim_text (project_id, claim_id, subject_type, text, text_hash) "
                "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (project_id, claim_id) DO UPDATE "
                "SET subject_type = EXCLUDED.subject_type, text = EXCLUDED.text, "
                "text_hash = EXCLUDED.text_hash",
                changed,
            )
        gone = sorted(set(have) - set(claims))
        if gone:
            cur.execute(
                "DELETE FROM kg_claim_text WHERE project_id = %s AND claim_id = ANY(%s)",
                (project_id, gone),
            )

    entities = _entity_texts(conn, project_id, config.context_predicates)
    have = {
        row["entity_id"]: row["text_hash"]
        for row in conn.execute(
            "SELECT entity_id, text_hash FROM kg_entity_text WHERE project_id = %s", (project_id,)
        ).fetchall()
    }
    changed_entities = [
        (project_id, entity_id, entity_type, name, text, text_hash(name + "\x1f" + text))
        for entity_id, (entity_type, name, text) in entities.items()
        if have.get(entity_id) != text_hash(name + "\x1f" + text)
    ]
    with conn.cursor() as cur:
        if changed_entities:
            cur.executemany(
                "INSERT INTO kg_entity_text (project_id, entity_id, entity_type, name, text, "
                "text_hash) VALUES (%s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (project_id, entity_id) DO UPDATE SET entity_type = "
                "EXCLUDED.entity_type, name = EXCLUDED.name, text = EXCLUDED.text, "
                "text_hash = EXCLUDED.text_hash",
                changed_entities,
            )
        gone = sorted(set(have) - set(entities))
        if gone:
            cur.execute(
                "DELETE FROM kg_entity_text WHERE project_id = %s AND entity_id = ANY(%s)",
                (project_id, gone),
            )
    return (
        {claim_id: text for claim_id, (_type, text) in claims.items()},
        {entity_id: text for entity_id, (_type, _name, text) in entities.items()},
    )


def _sync_vectors(
    pool: ConnectionPool,
    gateway: Gateway,
    vectors: VectorIndex,
    config: KnowledgeConfig,
    project_id: str,
    model: str,
    kind: str,
    texts: dict[str, str],
) -> int:
    """Embed what has no vector for its current text under this model. Returns how many."""
    stored = stored_hashes(pool, project_id, kind, model)
    todo = sorted(
        item_id for item_id, text in texts.items() if stored.get(item_id) != text_hash(text)
    )
    for start in range(0, len(todo), config.embedding_batch_size):
        batch = todo[start : start + config.embedding_batch_size]
        embedded = gateway.embed(
            [texts[item_id] for item_id in batch],
            purpose=f"index-{kind}",
            scope=knowledge_scope(project_id),
            tier=config.embedding_tier,
        )
        vectors.upsert(
            project_id,
            kind,
            model,
            [
                (item_id, text_hash(texts[item_id]), vector)
                for item_id, vector in zip(batch, embedded.vectors, strict=True)
            ],
        )
    vectors.delete(project_id, kind, sorted(set(stored) - set(texts)))
    return len(todo)


def sync(
    pool: ConnectionPool,
    project_id: str,
    *,
    config: KnowledgeConfig,
    gateway: Gateway | None = None,
    vectors: VectorIndex | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """Bring the project's search index up to its read models. Cheap when nothing changed:
    one comparison of the indexed seq with the projection's cursor. Embeddings are synced
    when a gateway with an embedding provider and a vector index are given."""
    embedding = gateway.embedding_model(config.embedding_tier) if gateway is not None else None
    model = embedding[0] if embedding and vectors is not None else None
    with pool.connection() as conn:
        seq = cursor_seq(conn, project_id)
        state = conn.execute(
            "SELECT seq, model FROM kg_index_state WHERE project_id = %s", (project_id,)
        ).fetchone()
        if not force and state is not None and state["seq"] == seq and state["model"] == model:
            return {"fresh": True, "seq": seq, "model": model, "embedded": 0}
        with conn.transaction():
            conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 8))", (project_id,))
            claim_texts, entity_texts = _sync_text(conn, project_id, config)
    embedded = 0
    if model is not None:
        embedded += _sync_vectors(
            pool, gateway, vectors, config, project_id, model, "claim", claim_texts
        )
        embedded += _sync_vectors(
            pool, gateway, vectors, config, project_id, model, "entity", entity_texts
        )
    with pool.connection() as conn:
        conn.execute(
            "INSERT INTO kg_index_state (project_id, seq, model) VALUES (%s, %s, %s) "
            "ON CONFLICT (project_id) DO UPDATE SET seq = EXCLUDED.seq, model = EXCLUDED.model",
            (project_id, seq, model),
        )
    return {
        "fresh": False,
        "seq": seq,
        "model": model,
        "claims": len(claim_texts),
        "entities": len(entity_texts),
        "embedded": embedded,
    }
