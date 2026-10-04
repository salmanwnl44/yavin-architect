"""Connection pool, schema bootstrap, and the row <-> wire-event mapping."""

from __future__ import annotations

import hashlib
import os
import re
from importlib import resources
from typing import Any

from psycopg import Connection
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


_CREATED_TABLE = re.compile(r"^CREATE TABLE IF NOT EXISTS (\w+)", re.M)
SCHEMA_KEY = "schema_sha256"


def _schema_is_current(conn: Connection[dict[str, Any]], digest: str, tables: list[str]) -> bool:
    """Whether this schema already holds the shipped DDL: every table it creates exists and
    the recorded hash of schema.sql is the shipped one. Reads catalogs only."""
    present = conn.execute(
        "SELECT count(*) AS n FROM pg_tables WHERE schemaname = current_schema() "
        "AND tablename = ANY(%s)",
        (tables,),
    ).fetchone()["n"]
    if present != len(tables):
        return False
    row = conn.execute("SELECT value FROM schema_meta WHERE key = %s", (SCHEMA_KEY,)).fetchone()
    return row is not None and row["value"] == digest


def ensure_schema(pool: ConnectionPool, *, force: bool = False) -> bool:
    """Apply schema.sql unless the schema is already current. Returns whether it was applied.

    Every process calls this when it starts. The DDL is idempotent, but it is not free: its
    ALTER TABLE, CREATE INDEX and CREATE TRIGGER statements take table locks that conflict
    with writers, one table after another, and hold them to the end of the transaction. A
    command starting while a worker was in the middle of a gateway transaction (which holds
    gw_spend and wants gw_calls) could therefore deadlock with it. So the DDL runs only when
    it has something to do: a table is missing, or schema.sql has changed since it was last
    applied (its hash is kept in schema_meta). `force` applies it regardless (`init-db`)."""
    ddl = resources.files("architect").joinpath("schema.sql").read_text(encoding="utf-8")
    digest = hashlib.sha256(ddl.encode("utf-8")).hexdigest()
    tables = sorted(set(_CREATED_TABLE.findall(ddl)))
    if not force:
        with pool.connection() as conn:
            if _schema_is_current(conn, digest, tables):
                return False
    with pool.connection() as conn, conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (_SCHEMA_LOCK,))
        if not force and _schema_is_current(conn, digest, tables):
            return False  # another process applied it while this one waited
        conn.execute(ddl)
        conn.execute(
            "INSERT INTO schema_meta (key, value) VALUES (%s, %s) "
            "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
            (SCHEMA_KEY, digest),
        )
    return True


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
