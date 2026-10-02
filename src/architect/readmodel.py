"""Queries over the proj_* read models, for the GET side of the API.

Everything here reads proj_* tables only: never `events`, never arb_*. What it returns is as
fresh as the projector's cursor (GET .../projections/status reports the lag).
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from psycopg import Connection
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from architect.projections import COMPROMISING_STATUSES

Row = dict[str, Any]

_CLAIM_COLUMNS = (
    "claim_id, status, load_bearing, taint_origin, supersedes, derived_from, "
    "premise_compromised, first_seq, last_seq, claim"
)

# Claims as they stood once event `seq` was committed: status from the history, and
# premise_compromised recomputed from the statuses and DERIVED_FROM edges of that moment.
_CLAIMS_AS_OF = """
WITH RECURSIVE status_then AS (
    SELECT claim_id, status, from_seq
    FROM proj_claim_status_history
    WHERE project_id = %(pid)s AND from_seq <= %(seq)s AND (to_seq IS NULL OR to_seq > %(seq)s)
), compromised (claim_id) AS (
    SELECT e.src
    FROM proj_edges e
    JOIN status_then premise ON premise.claim_id = e.dst
    WHERE e.project_id = %(pid)s AND e.edge_type = 'DERIVED_FROM' AND e.seq <= %(seq)s
      AND premise.status = ANY(%(bad)s)
  UNION
    SELECT e.src
    FROM proj_edges e
    JOIN compromised c ON c.claim_id = e.dst
    WHERE e.project_id = %(pid)s AND e.edge_type = 'DERIVED_FROM' AND e.seq <= %(seq)s
)
SELECT c.claim_id, s.status, c.load_bearing, c.taint_origin, c.supersedes, c.derived_from,
       (c.claim_id IN (SELECT claim_id FROM compromised)) AS premise_compromised,
       c.first_seq, s.from_seq AS last_seq, c.claim
FROM proj_claims c
JOIN status_then s ON s.claim_id = c.claim_id
WHERE c.project_id = %(pid)s
"""

# Every premise a claim rests on, with its distance from the claim. A premise chain may
# cite claims that were never committed, and may loop.
_PREMISE_CHAIN = """
WITH RECURSIVE chain (claim_id, depth) AS (
    SELECT e.dst, 1
    FROM proj_edges e
    WHERE e.project_id = %(pid)s AND e.edge_type = 'DERIVED_FROM' AND e.src = %(claim_id)s
  UNION ALL
    SELECT e.dst, c.depth + 1
    FROM proj_edges e
    JOIN chain c ON c.claim_id = e.src
    WHERE e.project_id = %(pid)s AND e.edge_type = 'DERIVED_FROM'
) CYCLE claim_id SET is_cycle USING path
SELECT chain.claim_id, min(chain.depth) AS depth, c.status, c.premise_compromised
FROM chain
LEFT JOIN proj_claims c ON c.project_id = %(pid)s AND c.claim_id = chain.claim_id
WHERE NOT chain.is_cycle
GROUP BY chain.claim_id, c.status, c.premise_compromised
ORDER BY depth, chain.claim_id
"""


@contextmanager
def _snapshot(pool: ConnectionPool) -> Iterator[Connection[Row]]:
    """One consistent view of the read models for a multi-statement read."""
    with pool.connection() as conn, conn.transaction():
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        yield conn


def _version(row: Row | None) -> Row | None:
    return row and {
        "version_id": row["version_id"],
        "parent_version": row["parent_version"],
        "committed_at_seq": row["committed_at_seq"],
        "model": row["model"],
    }


def model_version(pool: ConnectionPool, project_id: str, version_id: str) -> Row | None:
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT version_id, parent_version, committed_at_seq, model FROM proj_model_versions "
            "WHERE project_id = %s AND version_id = %s",
            (project_id, version_id),
        ).fetchone()
    return _version(row)


def _head(conn: Connection[Row], project_id: str) -> Row | None:
    return conn.execute(
        "SELECT version_id, parent_version, committed_at_seq, model FROM proj_model_versions "
        "WHERE project_id = %s ORDER BY committed_at_seq DESC LIMIT 1",
        (project_id,),
    ).fetchone()


def head_model(pool: ConnectionPool, project_id: str) -> Row | None:
    with pool.connection() as conn:
        return _version(_head(conn, project_id))


def list_claims(
    pool: ConnectionPool,
    project_id: str,
    *,
    status: str | None = None,
    load_bearing: bool | None = None,
    as_of_seq: int | None = None,
) -> list[Row]:
    """Claims in commit order. With as_of_seq, as they stood once that event was committed."""
    params: dict[str, Any] = {"pid": project_id}
    if as_of_seq is None:
        query = f"SELECT {_CLAIM_COLUMNS} FROM proj_claims c WHERE c.project_id = %(pid)s"
        status_column = "c.status"
    else:
        query = _CLAIMS_AS_OF
        status_column = "s.status"
        params |= {"seq": as_of_seq, "bad": list(COMPROMISING_STATUSES)}
    if status is not None:
        query += f" AND {status_column} = %(status)s"
        params["status"] = status
    if load_bearing is not None:
        query += " AND c.load_bearing = %(load_bearing)s"
        params["load_bearing"] = load_bearing
    with pool.connection() as conn:
        return conn.execute(query + " ORDER BY c.first_seq", params).fetchall()


def claim_detail(pool: ConnectionPool, project_id: str, claim_id: str) -> Row | None:
    """A claim with its status history and provenance: evidence sources and premise chain."""
    with _snapshot(pool) as conn:
        claim = conn.execute(
            f"SELECT {_CLAIM_COLUMNS} FROM proj_claims WHERE project_id = %s AND claim_id = %s",
            (project_id, claim_id),
        ).fetchone()
        if claim is None:
            return None
        history = conn.execute(
            "SELECT status, from_seq, to_seq, cause_event FROM proj_claim_status_history "
            "WHERE project_id = %s AND claim_id = %s ORDER BY from_seq",
            (project_id, claim_id),
        ).fetchall()
        cited = [evidence["source"] for evidence in claim["claim"].get("evidence", [])]
        sources = _sources(conn, project_id, cited)
        chain = conn.execute(_PREMISE_CHAIN, {"pid": project_id, "claim_id": claim_id}).fetchall()
    evidence = [
        {**item, "source_record": sources.get(item["source"])}
        for item in claim["claim"].get("evidence", [])
    ]
    return claim | {
        "status_history": history,
        "provenance": {
            "extractor": claim["claim"]["provenance"]["extractor"],
            "evidence": evidence,
            "derived_from_chain": chain,
        },
    }


def _sources(conn: Connection[Row], project_id: str, source_ids: list[str]) -> dict[str, Row]:
    rows = conn.execute(
        "SELECT source_id, uri, content_hash, media_type, license, taint_origin, seq "
        "FROM proj_sources WHERE project_id = %s AND source_id = ANY(%s)",
        (project_id, source_ids),
    ).fetchall()
    return {row["source_id"]: row for row in rows}


def _claims_with_sources(
    conn: Connection[Row], project_id: str, claim_ids: list[str]
) -> dict[str, Row]:
    """claim_id -> a summary of the claim and the sources that evidence it."""
    claims = conn.execute(
        "SELECT claim_id, status, load_bearing, premise_compromised, claim "
        "FROM proj_claims WHERE project_id = %s AND claim_id = ANY(%s)",
        (project_id, claim_ids),
    ).fetchall()
    cited = [e["source"] for row in claims for e in row["claim"].get("evidence", [])]
    sources = _sources(conn, project_id, cited)
    out: dict[str, Row] = {}
    for row in claims:
        claim = row.pop("claim")
        out[row["claim_id"]] = row | {
            "subject": claim["subject"],
            "predicate": claim["predicate"],
            "object": claim["object"],
            "sources": [
                sources.get(e["source"], {"source_id": e["source"]})
                for e in claim.get("evidence", [])
            ],
        }
    return out


def why(pool: ConnectionPool, project_id: str, element_id: str) -> Row | None:
    """The §11 trace for an element of the head model.

    element -> the requirements it SATISFIES (and the claims stating them) -> the decisions
    that affect it -> their evidence claims -> the sources behind those claims.
    """
    with _snapshot(pool) as conn:
        head = _head(conn, project_id)
        if head is None:
            return None
        found = [
            (element_type, element)
            for element_type, elements in head["model"]["elements"].items()
            for element in elements
            if element.get("id") == element_id
        ]
        if not found:
            return None
        element_type, element = found[0]

        requirement_ids = [
            row["dst"]
            for row in conn.execute(
                "SELECT dst FROM proj_edges WHERE project_id = %s AND edge_type = 'SATISFIES' "
                "AND version_id = %s AND src = %s ORDER BY seq, ord",
                (project_id, head["version_id"], element_id),
            ).fetchall()
        ]
        stating = conn.execute(
            "SELECT claim_id, claim -> 'subject' ->> 'id' AS requirement FROM proj_claims "
            "WHERE project_id = %s AND claim -> 'subject' ->> 'entity_type' = 'requirement' "
            "AND claim -> 'subject' ->> 'id' = ANY(%s) ORDER BY first_seq",
            (project_id, requirement_ids),
        ).fetchall()
        # The latest record of each ADR, kept if it names this element.
        decisions = conn.execute(
            "SELECT seq, adr_id, decision FROM ("
            "  SELECT DISTINCT ON (adr_id) seq, adr_id, decision FROM proj_decisions "
            "  WHERE project_id = %s ORDER BY adr_id, seq DESC"
            ") latest WHERE decision -> 'affected_elements' @> %s ORDER BY seq",
            (project_id, Jsonb([element_id])),
        ).fetchall()

        wanted = [row["claim_id"] for row in stating]
        for row in decisions:
            wanted += row["decision"]["evidence_claims"] + row["decision"]["assumptions"]
        claims = _claims_with_sources(conn, project_id, wanted)

    def known(claim_ids: list[str]) -> list[Row]:
        return [claims.get(claim_id, {"claim_id": claim_id}) for claim_id in claim_ids]

    def stated_by(requirement: str) -> list[str]:
        return [row["claim_id"] for row in stating if row["requirement"] == requirement]

    return {
        "element_id": element_id,
        "element_type": element_type,
        "element": element,
        "model_version": head["version_id"],
        "requirements": [
            {"requirement": requirement, "claims": known(stated_by(requirement))}
            for requirement in requirement_ids
        ],
        "decisions": [
            {
                "adr_id": row["adr_id"],
                "seq": row["seq"],
                "title": row["decision"]["title"],
                "choice": row["decision"]["choice"],
                "evidence_claims": known(row["decision"]["evidence_claims"]),
                "assumptions": known(row["decision"]["assumptions"]),
            }
            for row in decisions
        ],
    }
