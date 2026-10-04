"""The VectorIndex interface: nearest neighbours by cosine over a project's stored vectors.

`emb_vectors` is the store of record for both backends (kind, id, model, dim, text_hash,
vector). Two backends, proven identical by the parity tests:

- `exact`: exact cosine in numpy over emb_vectors. Works on any Postgres.
- `pgvector`: a mirror table with a pgvector column and an HNSW index. `exact=True` turns
  the index off for a search, which is what the parity test compares; the HNSW answer is
  approximate and its recall is measured separately.

Scores are cosine similarities; ties are broken by id.
"""

from __future__ import annotations

import threading
from typing import Any, Protocol

import numpy as np
from psycopg import Connection, sql
from psycopg_pool import ConnectionPool

from architect.knowledge.config import KnowledgeConfig

Item = tuple[str, str, list[float]]  # (id, text_hash, vector)


class VectorIndex(Protocol):
    name: str

    def upsert(self, project_id: str, kind: str, model: str, items: list[Item]) -> None:
        """Store or replace the vectors of these ids."""

    def delete(self, project_id: str, kind: str, ids: list[str]) -> None:
        """Forget the vectors of things that no longer exist."""

    def search(
        self,
        project_id: str,
        kind: str,
        model: str,
        vector: list[float],
        k: int,
        *,
        exact: bool = False,
    ) -> list[tuple[str, float]]:
        """The k nearest ids with their cosine similarity, best first."""


def stored_hashes(pool: ConnectionPool, project_id: str, kind: str, model: str) -> dict[str, str]:
    """id -> the hash of the text its stored vector was computed from."""
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT id, text_hash FROM emb_vectors WHERE project_id = %s AND kind = %s "
            "AND model = %s",
            (project_id, kind, model),
        ).fetchall()
    return {row["id"]: row["text_hash"] for row in rows}


def stored_vectors(
    pool: ConnectionPool, project_id: str, kind: str, model: str, ids: list[str]
) -> dict[str, list[float]]:
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT id, vector FROM emb_vectors WHERE project_id = %s AND kind = %s "
            "AND model = %s AND id = ANY(%s)",
            (project_id, kind, model, ids),
        ).fetchall()
    return {row["id"]: row["vector"] for row in rows}


def cosine(a: list[float], b: list[float]) -> float:
    x, y = np.asarray(a, dtype=np.float64), np.asarray(b, dtype=np.float64)
    norm = float(np.linalg.norm(x) * np.linalg.norm(y))
    return float(x @ y / norm) if norm else 0.0


class _Store:
    """The writes to emb_vectors both backends share."""

    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    def _upsert(
        self, conn: Connection[dict[str, Any]], project_id: str, kind: str, model: str, items
    ) -> None:
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO emb_vectors (project_id, kind, id, model, dim, text_hash, vector) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (project_id, kind, id, model) DO UPDATE SET dim = EXCLUDED.dim, "
                "text_hash = EXCLUDED.text_hash, vector = EXCLUDED.vector",
                [
                    (project_id, kind, item_id, model, len(vector), text_hash, vector)
                    for item_id, text_hash, vector in items
                ],
            )

    def _delete(
        self, conn: Connection[dict[str, Any]], project_id: str, kind: str, ids: list[str]
    ) -> None:
        conn.execute(
            "DELETE FROM emb_vectors WHERE project_id = %s AND kind = %s AND id = ANY(%s)",
            (project_id, kind, ids),
        )


class ExactVectorIndex(_Store):
    """Exact cosine in numpy. The matrix of a (project, kind, model) is kept in memory and
    reloaded when the stored vectors change."""

    name = "exact"

    def __init__(self, pool: ConnectionPool) -> None:
        super().__init__(pool)
        self._cache: dict[tuple[str, str, str], tuple[tuple[int, int], list[str], np.ndarray]] = {}
        self._lock = threading.Lock()

    def upsert(self, project_id: str, kind: str, model: str, items: list[Item]) -> None:
        if items:
            with self._pool.connection() as conn, conn.transaction():
                self._upsert(conn, project_id, kind, model, items)

    def delete(self, project_id: str, kind: str, ids: list[str]) -> None:
        if ids:
            with self._pool.connection() as conn:
                self._delete(conn, project_id, kind, ids)

    def _matrix(self, project_id: str, kind: str, model: str) -> tuple[list[str], np.ndarray]:
        key = (project_id, kind, model)
        with self._pool.connection() as conn:
            stamp_row = conn.execute(
                "SELECT count(*) AS n, coalesce(sum(hashtext(id || text_hash)::bigint), 0) AS h "
                "FROM emb_vectors WHERE project_id = %s AND kind = %s AND model = %s",
                key,
            ).fetchone()
            stamp = (int(stamp_row["n"]), int(stamp_row["h"]))
            with self._lock:
                cached = self._cache.get(key)
            if cached is not None and cached[0] == stamp:
                return cached[1], cached[2]
            rows = conn.execute(
                "SELECT id, vector FROM emb_vectors WHERE project_id = %s AND kind = %s "
                "AND model = %s ORDER BY id",
                key,
            ).fetchall()
        ids = [row["id"] for row in rows]
        matrix = np.asarray([row["vector"] for row in rows], dtype=np.float32)
        if len(ids):
            norms = np.linalg.norm(matrix, axis=1, keepdims=True)
            norms[norms == 0] = 1.0
            matrix = matrix / norms
        with self._lock:
            self._cache[key] = (stamp, ids, matrix)
        return ids, matrix

    def search(
        self,
        project_id: str,
        kind: str,
        model: str,
        vector: list[float],
        k: int,
        *,
        exact: bool = False,
    ) -> list[tuple[str, float]]:
        ids, matrix = self._matrix(project_id, kind, model)
        query = np.asarray(vector, dtype=np.float32)
        norm = float(np.linalg.norm(query))
        if not ids or norm == 0.0 or k <= 0:
            return []
        scores = matrix @ (query / norm)
        # ids are in ascending order, so a stable sort on the score breaks ties by id
        order = np.argsort(-scores, kind="stable")[:k]
        return [(ids[i], float(scores[i])) for i in order]


def pgvector_schema(pool: ConnectionPool, *, create: bool = True) -> str | None:
    """The schema the pgvector extension lives in, or None when it is not installed (and,
    with `create`, cannot be)."""
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT n.nspname FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace "
            "WHERE e.extname = 'vector'"
        ).fetchone()
        if row is not None:
            return row["nspname"]
        available = conn.execute(
            "SELECT 1 AS yes FROM pg_available_extensions WHERE name = 'vector'"
        ).fetchone()
    if available is None or not create:
        return None
    try:
        with pool.connection() as conn:
            conn.execute("CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public")
    except Exception:  # noqa: BLE001 - not ours to install: fall back
        return None
    return pgvector_schema(pool, create=False)


def _literal(vector: list[float]) -> str:
    return "[" + ",".join(repr(float(x)) for x in vector) + "]"


class PgVectorIndex(_Store):
    """A mirror of emb_vectors with a pgvector column, one table per vector size, and an
    HNSW index for cosine distance."""

    name = "pgvector"

    def __init__(self, pool: ConnectionPool, config: KnowledgeConfig, schema: str) -> None:
        super().__init__(pool)
        self._hnsw = config.hnsw
        self._ext = sql.Identifier(schema)
        self._ready: set[int] = set()
        self._lock = threading.Lock()
        with pool.connection() as conn:
            version = conn.execute(
                "SELECT extversion FROM pg_extension WHERE extname = 'vector'"
            ).fetchone()["extversion"]
        parts = [int(p) for p in version.split(".")[:2] if p.isdigit()]
        self._iterative = tuple(parts) >= (0, 8)

    def _table(self, dim: int) -> sql.Identifier:
        return sql.Identifier(f"emb_pgvector_{int(dim)}")

    def _ensure(self, conn: Connection[dict[str, Any]], dim: int) -> None:
        with self._lock:
            if dim in self._ready:
                return
            table = self._table(dim)
            conn.execute(
                sql.SQL(
                    "CREATE TABLE IF NOT EXISTS {table} ("
                    "project_id text NOT NULL, kind text NOT NULL, id text NOT NULL, "
                    "model text NOT NULL, embedding {ext}.vector({dim}) NOT NULL, "
                    "PRIMARY KEY (project_id, kind, id, model))"
                ).format(table=table, ext=self._ext, dim=sql.Literal(int(dim)))
            )
            conn.execute(
                sql.SQL(
                    "CREATE INDEX IF NOT EXISTS {index} ON {table} USING hnsw "
                    "(embedding {ext}.vector_cosine_ops) WITH (m = {m}, ef_construction = {efc})"
                ).format(
                    index=sql.Identifier(f"emb_pgvector_{int(dim)}_hnsw"),
                    table=table,
                    ext=self._ext,
                    m=sql.Literal(int(self._hnsw.get("m", 16))),
                    efc=sql.Literal(int(self._hnsw.get("ef_construction", 64))),
                )
            )
            self._ready.add(dim)

    def upsert(self, project_id: str, kind: str, model: str, items: list[Item]) -> None:
        if not items:
            return
        by_dim: dict[int, list[Item]] = {}
        for item in items:
            by_dim.setdefault(len(item[2]), []).append(item)
        with self._pool.connection() as conn, conn.transaction():
            self._upsert(conn, project_id, kind, model, items)
            for dim, group in by_dim.items():
                self._ensure(conn, dim)
                with conn.cursor() as cur:
                    cur.executemany(
                        sql.SQL(
                            "INSERT INTO {table} (project_id, kind, id, model, embedding) "
                            "VALUES (%s, %s, %s, %s, %s::{ext}.vector) "
                            "ON CONFLICT (project_id, kind, id, model) "
                            "DO UPDATE SET embedding = EXCLUDED.embedding"
                        ).format(table=self._table(dim), ext=self._ext),
                        [
                            (project_id, kind, item_id, model, _literal(vector))
                            for item_id, _hash, vector in group
                        ],
                    )

    def delete(self, project_id: str, kind: str, ids: list[str]) -> None:
        if not ids:
            return
        with self._pool.connection() as conn, conn.transaction():
            self._delete(conn, project_id, kind, ids)
            for dim in self._dims(conn):
                conn.execute(
                    sql.SQL(
                        "DELETE FROM {table} WHERE project_id = %s AND kind = %s AND id = ANY(%s)"
                    ).format(table=self._table(dim)),
                    (project_id, kind, ids),
                )

    def _dims(self, conn: Connection[dict[str, Any]]) -> list[int]:
        rows = conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = current_schema() "
            "AND tablename LIKE 'emb\\_pgvector\\_%'"
        ).fetchall()
        return [int(row["tablename"].rsplit("_", 1)[1]) for row in rows]

    def search(
        self,
        project_id: str,
        kind: str,
        model: str,
        vector: list[float],
        k: int,
        *,
        exact: bool = False,
    ) -> list[tuple[str, float]]:
        if k <= 0 or not any(vector):
            return []
        dim = len(vector)
        distance = sql.SQL("embedding OPERATOR({ext}.<=>) %(q)s::{ext}.vector").format(
            ext=self._ext
        )
        where = sql.SQL("project_id = %(pid)s AND kind = %(kind)s AND model = %(model)s")
        params = {"pid": project_id, "kind": kind, "model": model, "q": _literal(vector), "k": k}
        with self._pool.connection() as conn, conn.transaction():
            self._ensure(conn, dim)
            if exact:
                conn.execute("SET LOCAL enable_indexscan = off")
                conn.execute("SET LOCAL enable_bitmapscan = off")
                query = sql.SQL(
                    "SELECT id, 1 - ({distance}) AS score FROM {table} WHERE {where} "
                    "ORDER BY {distance}, id LIMIT %(k)s"
                )
            else:
                ef_search = max(int(self._hnsw.get("ef_search", 100)), k)
                conn.execute(
                    sql.SQL("SET LOCAL hnsw.ef_search = {}").format(sql.Literal(ef_search))
                )
                if self._iterative:
                    # keep scanning the index until k rows of THIS project pass the filter
                    conn.execute("SET LOCAL hnsw.iterative_scan = 'relaxed_order'")
                query = sql.SQL(
                    "SELECT id, score FROM (SELECT id, 1 - ({distance}) AS score FROM {table} "
                    "WHERE {where} ORDER BY {distance} LIMIT %(k)s) nearest "
                    "ORDER BY score DESC, id"
                )
            rows = conn.execute(
                query.format(distance=distance, table=self._table(dim), where=where), params
            ).fetchall()
        return [(row["id"], float(row["score"])) for row in rows]


def vector_index(pool: ConnectionPool, config: KnowledgeConfig) -> VectorIndex:
    """The configured backend; `auto` is pgvector when the extension exists, else exact."""
    if config.vector_backend == "exact":
        return ExactVectorIndex(pool)
    schema = pgvector_schema(pool)
    if schema is not None:
        return PgVectorIndex(pool, config, schema)
    if config.vector_backend == "pgvector":
        raise RuntimeError("vectors.backend is pgvector, but the extension is not installed")
    return ExactVectorIndex(pool)
