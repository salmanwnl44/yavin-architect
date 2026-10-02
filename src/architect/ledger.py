"""Read side of the ledger: projects, event pages, the head summary, and the hash chain.

Nothing in this module writes to `events`; only architect.arbiter does.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from psycopg import Connection
from psycopg_pool import ConnectionPool

from architect.db import EVENT_COLUMNS, row_to_event

Event = dict[str, Any]


def canonical_json(event: Event) -> bytes:
    """Canonical form used for hashing: sorted keys, compact separators, UTF-8."""
    return json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def event_hash(event: Event) -> str:
    """sha256 over the canonical JSON of a committed event (its wire form, as dumped)."""
    return hashlib.sha256(canonical_json(event)).hexdigest()


def create_project(pool: ConnectionPool, project_id: str) -> bool:
    """Create the project if it does not exist. Returns True when it was created."""
    with pool.connection() as conn:
        row = conn.execute(
            "INSERT INTO projects (project_id) VALUES (%s) ON CONFLICT DO NOTHING "
            "RETURNING project_id",
            (project_id,),
        ).fetchone()
    return row is not None


def project_exists(conn: Connection[dict[str, Any]], project_id: str) -> bool:
    row = conn.execute("SELECT 1 FROM projects WHERE project_id = %s", (project_id,)).fetchone()
    return row is not None


def get_event(pool: ConnectionPool, project_id: str, event_id: str) -> Event | None:
    with pool.connection() as conn:
        row = conn.execute(
            f"SELECT {EVENT_COLUMNS} FROM events WHERE project_id = %s AND event_id = %s",
            (project_id, event_id),
        ).fetchone()
    return row_to_event(row) if row else None


def page(pool: ConnectionPool, project_id: str, since_seq: int | None, limit: int) -> list[Event]:
    """Up to `limit` events with seq > since_seq, in seq order (from the start when None)."""
    with pool.connection() as conn:
        rows = conn.execute(
            f"SELECT {EVENT_COLUMNS} FROM events WHERE project_id = %s AND seq > %s "
            "ORDER BY seq LIMIT %s",
            (project_id, -1 if since_seq is None else since_seq, limit),
        ).fetchall()
    return [row_to_event(row) for row in rows]


def iter_events(pool: ConnectionPool, project_id: str, batch: int = 1000) -> Iterator[Event]:
    """Every event of the project in seq order."""
    since: int | None = None
    while True:
        events = page(pool, project_id, since, batch)
        yield from events
        if len(events) < batch:
            return
        since = events[-1]["seq"]


def head(pool: ConnectionPool, project_id: str) -> dict[str, Any]:
    with pool.connection() as conn, conn.transaction():
        last = conn.execute(
            "SELECT max(seq) AS last_seq FROM events WHERE project_id = %s", (project_id,)
        ).fetchone()
        model = conn.execute(
            "SELECT head_version FROM arb_model_heads WHERE project_id = %s", (project_id,)
        ).fetchone()
        objections = conn.execute(
            "SELECT count(*) AS n FROM arb_objections WHERE project_id = %s AND open",
            (project_id,),
        ).fetchone()
        claims = conn.execute(
            "SELECT status, count(*) AS n FROM arb_claims WHERE project_id = %s GROUP BY status",
            (project_id,),
        ).fetchall()
    return {
        "last_seq": last["last_seq"],
        "model_head_version": model["head_version"] if model else None,
        "open_objections": objections["n"],
        "claim_counts_by_status": {row["status"]: row["n"] for row in claims},
    }


@dataclass(frozen=True)
class ChainBreak:
    seq: int
    event_id: str
    problem: str


def verify_chain(pool: ConnectionPool, project_id: str) -> tuple[int, list[ChainBreak]]:
    """Recompute the prev_hash chain. Returns (events checked, breaks found)."""
    breaks: list[ChainBreak] = []
    expected_seq = 0
    expected_prev: str | None = None
    for event in iter_events(pool, project_id):
        if event["seq"] != expected_seq:
            breaks.append(
                ChainBreak(event["seq"], event["event_id"], f"seq gap: expected {expected_seq}")
            )
        if event.get("prev_hash") != expected_prev:
            breaks.append(
                ChainBreak(
                    event["seq"],
                    event["event_id"],
                    f"prev_hash is {event.get('prev_hash')}, expected {expected_prev}",
                )
            )
        expected_seq = event["seq"] + 1
        expected_prev = event_hash(event)
    return expected_seq, breaks
