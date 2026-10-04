"""The Apache AGE backend of GraphStore (owner decision §22-1): the same answers as the SQL
backend, from Cypher.

The projection tables stay the record. A project's resolved graph is LOADED into an AGE
graph of its own (named from the database schema and the project), and loaded again
whenever the projection's cursor has moved since: it is an index, like a vector index, and
dropping it loses nothing. Vertices are `(:N {id})`; edges are `[:E {t, s, o, c}]` (edge
type, the seq and ord that name the edge, the claim id or "").

Every query runs on a connection of its own, closed when the query is done, never on the
application's pool. AGE keeps per-session caches of graphs and labels, and a session that
has dropped a graph can fail later, on an unrelated statement, with "label (relation) cache
corrupted" (seen in CI on AGE 1.6). A session that ends with the query cannot.

Traversal is breadth-first over one-hop Cypher expansions, so `neighbors` and `paths` here
and the recursive CTEs of the SQL backend are two independent implementations, which is
what the parity tests compare.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import re
from collections.abc import Iterator
from typing import Any

import psycopg
from psycopg import Connection
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from architect.knowledge.graph import (
    MAX_NEIGHBOR_DEPTH,
    MAX_PATH_DEPTH,
    Edge,
    all_edges,
    all_nodes,
    check_depth,
    describe,
    enrich,
    neighborhood,
    resolve,
)
from architect.projections import PROJECTION

_SAFE = re.compile(r"^[^$\x00]*$")
NODE_CHUNK = 200


def age_installed(pool: ConnectionPool, *, create: bool = True) -> bool:
    """Whether the AGE extension is installed (or, with `create`, could be installed now)."""
    with pool.connection() as conn:
        if conn.execute("SELECT 1 AS yes FROM pg_extension WHERE extname = 'age'").fetchone():
            return True
        available = conn.execute(
            "SELECT 1 AS yes FROM pg_available_extensions WHERE name = 'age'"
        ).fetchone()
    if available is None or not create:
        return False
    try:
        with pool.connection() as conn:
            conn.execute("CREATE EXTENSION IF NOT EXISTS age")
    except Exception:  # noqa: BLE001 - not ours to install: fall back
        return False
    return age_installed(pool, create=False)


def _quote(value: str) -> str:
    """A Cypher string literal."""
    if not _SAFE.match(value):
        raise ValueError(f"an id the AGE backend cannot hold: {value!r}")
    return json.dumps(value)


def _list(values: list[str]) -> str:
    return "[" + ", ".join(_quote(v) for v in values) + "]"


class AgeGraphStore:
    name = "age"

    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    # ------------------------------------------------------------------ plumbing
    @contextlib.contextmanager
    def _session(self) -> Iterator[Connection[dict[str, Any]]]:
        """A connection for one query, with the pool's settings, closed afterwards (see the
        module docstring for why AGE never runs on a pooled connection)."""
        with psycopg.connect(self._pool.conninfo, row_factory=dict_row) as conn:
            yield conn

    @staticmethod
    def _enter(conn: Connection[dict[str, Any]]) -> None:
        """Make AGE usable for the rest of this transaction: the library loaded, its catalog
        on the search path after the application's own schema."""
        try:
            with conn.transaction():
                conn.execute("LOAD 'age'")
        except Exception:  # noqa: BLE001 - preloaded by the server, or not ours to load
            pass
        schema = conn.execute("SELECT current_schema() AS s").fetchone()["s"]
        conn.execute(f'SET LOCAL search_path = "{schema}", ag_catalog')

    @staticmethod
    def _cypher(
        conn: Connection[dict[str, Any]], graph: str, query: str, columns: list[str]
    ) -> list[dict[str, Any]]:
        returns = ", ".join(f"{column} ag_catalog.agtype" for column in columns) or (
            "result ag_catalog.agtype"
        )
        rows = conn.execute(
            f"SELECT * FROM ag_catalog.cypher('{graph}', $cypher$ {query} $cypher$) AS ({returns})"
        ).fetchall()
        return [
            {k: (json.loads(v) if v is not None else None) for k, v in row.items()} for row in rows
        ]

    def graph_name(self, conn: Connection[dict[str, Any]], project_id: str) -> str:
        schema = conn.execute("SELECT current_schema() AS s").fetchone()["s"]
        return "kg_" + hashlib.sha1(f"{schema}|{project_id}".encode()).hexdigest()[:24]  # noqa: S324

    def _loaded(self, conn: Connection[dict[str, Any]], project_id: str) -> str:
        """The project's AGE graph, loaded from the projection tables if the cursor moved."""
        graph = self.graph_name(conn, project_id)
        conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 9))", (graph,))
        cursor = conn.execute(
            "SELECT last_seq FROM proj_cursors WHERE projection = %s AND project_id = %s",
            (PROJECTION, project_id),
        ).fetchone()
        seq = cursor["last_seq"] if cursor else -1
        synced = conn.execute(
            "SELECT seq FROM kg_age_sync WHERE project_id = %s AND graph = %s", (project_id, graph)
        ).fetchone()
        exists = conn.execute(
            "SELECT 1 AS yes FROM ag_catalog.ag_graph WHERE name = %s", (graph,)
        ).fetchone()
        if exists is not None and synced is not None and synced["seq"] == seq:
            return graph
        if exists is not None:
            conn.execute("SELECT ag_catalog.drop_graph(%s, true)", (graph,))
        conn.execute("SELECT ag_catalog.create_graph(%s)", (graph,))
        edges = all_edges(conn, project_id)
        ids = sorted(
            {node["id"] for node in all_nodes(conn, project_id)}
            | {edge["src"] for edge in edges}
            | {edge["dst"] for edge in edges}
        )
        for start in range(0, len(ids), NODE_CHUNK):
            chunk = ids[start : start + NODE_CHUNK]
            pattern = ", ".join(f"(:N {{id: {_quote(node_id)}}})" for node_id in chunk)
            self._cypher(conn, graph, f"CREATE {pattern}", [])
        for edge in edges:
            self._cypher(
                conn,
                graph,
                f"MATCH (a:N {{id: {_quote(edge['src'])}}}), (b:N {{id: {_quote(edge['dst'])}}}) "
                f"CREATE (a)-[:E {{t: {_quote(edge['edge_type'])}, s: {int(edge['seq'])}, "
                f"o: {int(edge['ord'])}, c: {_quote(edge['claim_id'] or '')}}}]->(b)",
                [],
            )
        conn.execute(
            "INSERT INTO kg_age_sync (project_id, graph, seq) VALUES (%s, %s, %s) "
            "ON CONFLICT (project_id) DO UPDATE SET graph = EXCLUDED.graph, seq = EXCLUDED.seq",
            (project_id, graph, seq),
        )
        return graph

    def drop(self, project_id: str) -> None:
        """Remove the project's AGE graph (it is rebuilt on the next query)."""
        with self._session() as conn, conn.transaction():
            self._enter(conn)
            graph = self.graph_name(conn, project_id)
            if conn.execute(
                "SELECT 1 AS yes FROM ag_catalog.ag_graph WHERE name = %s", (graph,)
            ).fetchone():
                conn.execute("SELECT ag_catalog.drop_graph(%s, true)", (graph,))
            conn.execute("DELETE FROM kg_age_sync WHERE project_id = %s", (project_id,))

    # ------------------------------------------------------------------ one hop, in Cypher
    def _expand(
        self,
        conn: Connection[dict[str, Any]],
        graph: str,
        frontier: list[str],
        edge_types: list[str] | None,
    ) -> list[tuple[str, str]]:
        """(node in the frontier, a node one edge away), following edges in both directions."""
        types = f" AND r.t IN {_list(edge_types)}" if edge_types is not None else ""
        rows = self._cypher(
            conn,
            graph,
            f"MATCH (a:N)-[r:E]-(b:N) WHERE a.id IN {_list(frontier)}{types} RETURN a.id, b.id",
            ["a", "b"],
        )
        return [(row["a"], row["b"]) for row in rows]

    def _among(
        self,
        conn: Connection[dict[str, Any]],
        graph: str,
        nodes: list[str],
        edge_types: list[str] | None,
    ) -> list[Edge]:
        types = f" AND r.t IN {_list(edge_types)}" if edge_types is not None else ""
        rows = self._cypher(
            conn,
            graph,
            f"MATCH (a:N)-[r:E]->(b:N) WHERE a.id IN {_list(nodes)} AND b.id IN {_list(nodes)}"
            f"{types} RETURN a.id, b.id, r.t, r.s, r.o, r.c",
            ["a", "b", "t", "s", "o", "c"],
        )
        return [
            {
                "src": row["a"],
                "dst": row["b"],
                "edge_type": row["t"],
                "seq": int(row["s"]),
                "ord": int(row["o"]),
                "claim_id": row["c"] or None,
            }
            for row in rows
        ]

    # ------------------------------------------------------------------ the interface
    def neighbors(
        self, project_id: str, node_id: str, depth: int = 1, edge_types: list[str] | None = None
    ) -> dict[str, Any]:
        check_depth(depth, MAX_NEIGHBOR_DEPTH, "depth")
        with self._session() as conn, conn.transaction():
            start = resolve(conn, project_id, node_id)
            self._enter(conn)
            graph = self._loaded(conn, project_id)
            distances = {start: 0}
            frontier = [start]
            for distance in range(1, depth + 1):
                reached = sorted(
                    {b for _, b in self._expand(conn, graph, frontier, edge_types)} - set(distances)
                )
                if not reached:
                    break
                distances |= dict.fromkeys(reached, distance)
                frontier = reached
            among = self._among(conn, graph, sorted(distances), edge_types)
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
        with self._session() as conn, conn.transaction():
            start, goal = resolve(conn, project_id, src), resolve(conn, project_id, dst)
            if start == goal:
                return [[start]]
            self._enter(conn)
            graph = self._loaded(conn, project_id)
            # breadth-first, keeping every predecessor that reaches a node at its distance
            distance = {start: 0}
            parents: dict[str, set[str]] = {}
            frontier = [start]
            for depth in range(1, max_depth + 1):
                layer: dict[str, set[str]] = {}
                for a, b in self._expand(conn, graph, frontier, edge_types):
                    if b not in distance:
                        layer.setdefault(b, set()).add(a)
                if not layer:
                    return []
                for node, before in layer.items():
                    distance[node] = depth
                    parents[node] = before
                if goal in layer:
                    break
                frontier = sorted(layer)
            if goal not in parents:
                return []

        def back(node: str) -> list[list[str]]:
            if node == start:
                return [[start]]
            return [path + [node] for parent in sorted(parents[node]) for path in back(parent)]

        return sorted(back(goal))

    def subgraph(self, project_id: str, node_ids: list[str]) -> dict[str, Any]:
        with self._session() as conn, conn.transaction():
            wanted = sorted({resolve(conn, project_id, node_id) for node_id in node_ids})
            self._enter(conn)
            graph = self._loaded(conn, project_id)
            among = self._among(conn, graph, wanted, None) if wanted else []
            described = describe(conn, project_id, wanted)
            return {
                "nodes": [described[node_id] for node_id in wanted],
                "edges": enrich(conn, project_id, among),
            }
