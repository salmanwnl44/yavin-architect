"""The knowledge graph (spec §8): what its nodes and edges are, and the GraphStore interface.

The graph is a projection of the ledger. The projector folds it into four tables
(architect.projections): `proj_graph_nodes`, `proj_graph_edges` (the edges derived from
claims), `proj_entity_merges` and `proj_entity_alias`. Its edges are everything in
`proj_edges` (evidence, premises, supersession, decision evidence, and the links of the
HEAD model) plus, for every committed claim,

    (subject entity) -[PREDICATE]-> (object entity)      when both are entities
    (claim) -[ABOUT]-> each entity it names

An edge is un-storable without provenance (P2): every row carries the seq of the event
that produced it, and a claim-derived edge its claim id. Status, grade and conditions are
read from the claim when an edge is returned, so they are never stale.

Entity merges resolve at read time: an edge's endpoints are mapped through
`proj_entity_alias` to the kept id, and a reverted merge simply leaves the alias table.

A GraphStore answers three traversal queries over that resolved graph. There are two
backends, proven identical by the parity tests: `sql` here (recursive CTEs, works on any
Postgres) and `age` (architect.knowledge.graph_age, Apache AGE and Cypher).
"""

from __future__ import annotations

from typing import Any, Protocol

from psycopg import Connection
from psycopg_pool import ConnectionPool

NODE_TYPES = ("source", "claim", "entity", "element", "requirement", "adr", "community")
ABOUT = "ABOUT"
MAX_NEIGHBOR_DEPTH = 3
MAX_PATH_DEPTH = 6

# entity types whose id is already a node id of its own kind
_OWN_ID_TYPES = {"community": "community", "requirement": "requirement"}

Node = dict[str, Any]
Edge = dict[str, Any]


def label_of(identifier: str) -> str:
    """'lease-manager' -> 'lease manager': an id as words."""
    return " ".join(identifier.replace("_", " ").replace("-", " ").split()) or identifier


def entity_node(ref: dict[str, Any]) -> tuple[str, str, str, str] | None:
    """(node_id, node_type, entity_type, label) of a claim's subject or object, or None when
    it is a literal: a value is not an entity."""
    if "id" not in ref:
        return None
    entity_type, identifier = str(ref["entity_type"]), str(ref["id"])
    own = _OWN_ID_TYPES.get(entity_type)
    if own is not None:
        return identifier, own, entity_type, label_of(identifier)
    return f"ent:{entity_type}:{identifier}", "entity", entity_type, label_of(identifier)


def ref_text(ref: dict[str, Any]) -> str:
    if "id" in ref:
        return label_of(str(ref["id"]))
    return str(ref.get("literal", ""))


def claim_label(claim: dict[str, Any]) -> str:
    return f"{ref_text(claim['subject'])} {claim['predicate']} {ref_text(claim['object'])}"[:200]


# The resolved edge set of one project as a CTE named `edges`: proj_edges (unversioned ones
# and the head model's links) and the claim-derived edges, endpoints mapped to kept ids.
EDGES_CTE = """
head AS (
    SELECT version_id FROM proj_model_versions
    WHERE project_id = %(pid)s ORDER BY committed_at_seq DESC LIMIT 1
),
raw AS (
    SELECT seq, ord, edge_type, src, dst, NULL::text AS claim_id
    FROM proj_edges
    WHERE project_id = %(pid)s
      AND (version_id IS NULL OR version_id = (SELECT version_id FROM head))
  UNION ALL
    SELECT seq, ord, edge_type, src, dst, claim_id
    FROM proj_graph_edges WHERE project_id = %(pid)s
),
edges AS (
    SELECT r.seq, r.ord, r.edge_type, r.claim_id,
           coalesce(a.canonical_id, r.src) AS src, coalesce(b.canonical_id, r.dst) AS dst
    FROM raw r
    LEFT JOIN proj_entity_alias a ON a.project_id = %(pid)s AND a.entity_id = r.src
    LEFT JOIN proj_entity_alias b ON b.project_id = %(pid)s AND b.entity_id = r.dst
    WHERE %(types)s::text[] IS NULL OR r.edge_type = ANY(%(types)s::text[])
)
"""


def edge_key(edge: Edge) -> tuple[int, int]:
    return (edge["seq"], edge["ord"])


def resolve(conn: Connection[dict[str, Any]], project_id: str, node_id: str) -> str:
    """The id a node answers to now: the kept id when it was merged away."""
    row = conn.execute(
        "SELECT canonical_id FROM proj_entity_alias WHERE project_id = %s AND entity_id = %s",
        (project_id, node_id),
    ).fetchone()
    return row["canonical_id"] if row else node_id


def all_edges(
    conn: Connection[dict[str, Any]], project_id: str, edge_types: list[str] | None = None
) -> list[Edge]:
    """Every resolved edge of the project, in (seq, ord) order."""
    return conn.execute(
        f"WITH {EDGES_CTE} SELECT seq, ord, edge_type, src, dst, claim_id FROM edges "
        "ORDER BY seq, ord",
        {"pid": project_id, "types": edge_types},
    ).fetchall()


def all_nodes(conn: Connection[dict[str, Any]], project_id: str) -> list[Node]:
    """Every node that is not merged into another, by id."""
    return conn.execute(
        "SELECT n.node_id AS id, n.node_type AS type, n.entity_type, n.label, n.seq "
        "FROM proj_graph_nodes n WHERE n.project_id = %s AND NOT EXISTS ("
        " SELECT 1 FROM proj_entity_alias a WHERE a.project_id = n.project_id "
        " AND a.entity_id = n.node_id) ORDER BY n.node_id",
        (project_id,),
    ).fetchall()


def describe(
    conn: Connection[dict[str, Any]], project_id: str, node_ids: list[str]
) -> dict[str, Node]:
    """Type and label for node ids; an id the graph has an edge to but no node for (a claim
    named as a premise and never committed, say) is described as `unknown`."""
    rows = conn.execute(
        "SELECT node_id AS id, node_type AS type, entity_type, label FROM proj_graph_nodes "
        "WHERE project_id = %s AND node_id = ANY(%s)",
        (project_id, node_ids),
    ).fetchall()
    known = {row["id"]: row for row in rows}
    return {
        node_id: known.get(
            node_id, {"id": node_id, "type": "unknown", "entity_type": None, "label": node_id}
        )
        for node_id in node_ids
    }


def enrich(
    conn: Connection[dict[str, Any]], project_id: str, edges: list[Edge]
) -> list[dict[str, Any]]:
    """Edges as the API returns them: provenance (the event, and the claim with its CURRENT
    status, grade and conditions for a claim-derived edge)."""
    seqs = sorted({e["seq"] for e in edges})
    claim_ids = sorted({e["claim_id"] for e in edges if e["claim_id"]})
    events = {
        row["seq"]: row["event_id"]
        for row in conn.execute(
            "SELECT seq, event_id FROM events WHERE project_id = %s AND seq = ANY(%s)",
            (project_id, seqs),
        ).fetchall()
    }
    claims = {
        row["claim_id"]: row
        for row in conn.execute(
            "SELECT claim_id, status, grade, claim -> 'conditions' AS conditions "
            "FROM proj_claims WHERE project_id = %s AND claim_id = ANY(%s)",
            (project_id, claim_ids),
        ).fetchall()
    }
    out = []
    for edge in sorted(edges, key=edge_key):
        item: dict[str, Any] = {
            "type": edge["edge_type"],
            "src": edge["src"],
            "dst": edge["dst"],
            "provenance": {"seq": edge["seq"], "event_id": events.get(edge["seq"])},
        }
        claim = claims.get(edge["claim_id"]) if edge["claim_id"] else None
        if edge["claim_id"]:
            item["provenance"]["claim_id"] = edge["claim_id"]
        if claim is not None:
            item |= {
                "claim_id": claim["claim_id"],
                "status": claim["status"],
                "grade": claim["grade"],
                "conditions": claim["conditions"] or {},
            }
        out.append(item)
    return out


def check_depth(depth: int, limit: int, what: str) -> None:
    if not 1 <= depth <= limit:
        raise ValueError(f"{what} must be between 1 and {limit}, not {depth}")


def neighborhood(
    conn: Connection[dict[str, Any]],
    project_id: str,
    start: str,
    distances: dict[str, int],
    edges: list[Edge],
) -> dict[str, Any]:
    """The answer of `neighbors`, the same shape from every backend."""
    described = describe(conn, project_id, sorted(distances))
    nodes = [
        described[node_id] | {"distance": distance}
        for node_id, distance in sorted(distances.items(), key=lambda kv: (kv[1], kv[0]))
    ]
    return {"node": start, "nodes": nodes, "edges": enrich(conn, project_id, edges)}


class GraphStore(Protocol):
    """Traversal over one project's resolved graph. Every backend returns the same answers."""

    name: str

    def neighbors(
        self, project_id: str, node_id: str, depth: int = 1, edge_types: list[str] | None = None
    ) -> dict[str, Any]:
        """The nodes within `depth` (at most 3) of the node, each with its distance, and the
        edges among them. Edges are followed in both directions."""

    def paths(
        self,
        project_id: str,
        src: str,
        dst: str,
        max_depth: int = 4,
        edge_types: list[str] | None = None,
    ) -> list[list[str]]:
        """Every shortest path between two nodes of at most `max_depth` (at most 6) edges, as
        lists of node ids, sorted. Empty when there is none."""

    def subgraph(self, project_id: str, node_ids: list[str]) -> dict[str, Any]:
        """The given nodes and the edges among them."""


class SqlGraphStore:
    """Recursive CTEs over the projection tables. Works on any Postgres."""

    name = "sql"

    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    def neighbors(
        self, project_id: str, node_id: str, depth: int = 1, edge_types: list[str] | None = None
    ) -> dict[str, Any]:
        check_depth(depth, MAX_NEIGHBOR_DEPTH, "depth")
        with self._pool.connection() as conn:
            start = resolve(conn, project_id, node_id)
            params = {"pid": project_id, "types": edge_types, "start": start, "depth": depth}
            rows = conn.execute(
                f"WITH RECURSIVE {EDGES_CTE}, "
                "walk (node, depth) AS ("
                "    SELECT %(start)s::text, 0 "
                "  UNION "
                "    SELECT CASE WHEN e.src = w.node THEN e.dst ELSE e.src END, w.depth + 1 "
                "    FROM walk w JOIN edges e ON e.src = w.node OR e.dst = w.node "
                "    WHERE w.depth < %(depth)s"
                ") SELECT node, min(depth) AS distance FROM walk GROUP BY node",
                params,
            ).fetchall()
            distances = {row["node"]: row["distance"] for row in rows}
            among = conn.execute(
                f"WITH {EDGES_CTE} SELECT seq, ord, edge_type, src, dst, claim_id FROM edges "
                "WHERE src = ANY(%(nodes)s) AND dst = ANY(%(nodes)s)",
                params | {"nodes": sorted(distances)},
            ).fetchall()
            return neighborhood(conn, project_id, start, distances, among)

    def paths(
        self,
        project_id: str,
        src: str,
        dst: str,
        max_depth: int = 4,
        edge_types: list[str] | None = None,
    ) -> list[list[str]]:
        check_depth(max_depth, MAX_PATH_DEPTH, "max_depth")
        with self._pool.connection() as conn:
            start, goal = resolve(conn, project_id, src), resolve(conn, project_id, dst)
            if start == goal:
                return [[start]]
            rows = conn.execute(
                f"WITH RECURSIVE {EDGES_CTE}, "
                "walk (node, path, depth) AS ("
                "    SELECT %(start)s::text, ARRAY[%(start)s::text], 0 "
                "  UNION "
                "    SELECT step.next, w.path || step.next, w.depth + 1 "
                "    FROM walk w "
                "    JOIN LATERAL ("
                "        SELECT CASE WHEN e.src = w.node THEN e.dst ELSE e.src END AS next "
                "        FROM edges e WHERE e.src = w.node OR e.dst = w.node"
                "    ) step ON NOT step.next = ANY(w.path) "
                "    WHERE w.depth < %(depth)s AND w.node <> %(goal)s"
                ") SELECT DISTINCT path FROM walk WHERE node = %(goal)s AND depth = ("
                "    SELECT min(depth) FROM walk WHERE node = %(goal)s)",
                {
                    "pid": project_id,
                    "types": edge_types,
                    "start": start,
                    "goal": goal,
                    "depth": max_depth,
                },
            ).fetchall()
        return sorted(row["path"] for row in rows)

    def subgraph(self, project_id: str, node_ids: list[str]) -> dict[str, Any]:
        with self._pool.connection() as conn:
            wanted = sorted({resolve(conn, project_id, node_id) for node_id in node_ids})
            among = conn.execute(
                f"WITH {EDGES_CTE} SELECT seq, ord, edge_type, src, dst, claim_id FROM edges "
                "WHERE src = ANY(%(nodes)s) AND dst = ANY(%(nodes)s)",
                {"pid": project_id, "types": None, "nodes": wanted},
            ).fetchall()
            described = describe(conn, project_id, wanted)
            return {
                "nodes": [described[node_id] for node_id in wanted],
                "edges": enrich(conn, project_id, among),
            }


def counts(pool: ConnectionPool, project_id: str) -> dict[str, dict[str, int]]:
    """Node counts by type and edge counts by type of the resolved graph."""
    with pool.connection() as conn:
        nodes: dict[str, int] = {}
        for node in all_nodes(conn, project_id):
            nodes[node["type"]] = nodes.get(node["type"], 0) + 1
        edges: dict[str, int] = {}
        for edge in all_edges(conn, project_id):
            edges[edge["edge_type"]] = edges.get(edge["edge_type"], 0) + 1
    return {"nodes": dict(sorted(nodes.items())), "edges": dict(sorted(edges.items()))}
