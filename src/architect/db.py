"""Connection pool, schema bootstrap, and the row <-> wire-event mapping."""

from __future__ import annotations

import os
from importlib import resources
from typing import Any

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

DEFAULT_DATABASE_URL = "postgresql://architect:architect@localhost:5432/architect"

# Every column needed to rebuild the wire form of an event, in one place.
EVENT_COLUMNS = (
    "project_id, seq, event_id, ts_wire, actor, session_id, task_id, type, payload, "
    "idempotency_key, prev_hash"
)

# NOTIFY channel the Arbiter signals on each commit, with the project_id as payload. A wake-up
# for the projector only: it carries no data a reader may rely on.
EVENTS_CHANNEL = "architect_events"

_SCHEMA_LOCK = 0x41524348  # "ARCH": serializes concurrent ensure_schema() calls


def database_url() -> str:
    return os.environ.get("ARCHITECT_DATABASE_URL", DEFAULT_DATABASE_URL)


def open_pool(dsn: str | None = None, *, min_size: int = 1, max_size: int = 10) -> ConnectionPool:
    pool = ConnectionPool(
        dsn or database_url(),
        min_size=min_size,
        max_size=max_size,
        kwargs={"row_factory": dict_row},
        open=True,
    )
    pool.wait()
    return pool


def ensure_schema(pool: ConnectionPool) -> None:
    ddl = resources.files("architect").joinpath("schema.sql").read_text(encoding="utf-8")
    with pool.connection() as conn, conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (_SCHEMA_LOCK,))
        conn.execute(ddl)


def row_to_event(row: dict[str, Any]) -> dict[str, Any]:
    """The wire form of a committed event: exactly what ledger_events.schema.json describes."""
    event: dict[str, Any] = {
        "event_id": row["event_id"],
        "project_id": row["project_id"],
        "seq": row["seq"],
        "ts": row["ts_wire"],
        "actor": row["actor"],
    }
    if row["session_id"] is not None:
        event["session_id"] = row["session_id"]
    if row["task_id"] is not None:
        event["task_id"] = row["task_id"]
    event["type"] = row["type"]
    event["payload"] = row["payload"]
    event["idempotency_key"] = row["idempotency_key"]
    if row["prev_hash"] is not None:
        event["prev_hash"] = row["prev_hash"]
    return event
