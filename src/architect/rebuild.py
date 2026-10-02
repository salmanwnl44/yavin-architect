"""Rebuild the Arbiter's arb_* state for a project from its ledger alone."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from psycopg import Connection
from psycopg_pool import ConnectionPool

from architect.arbiter import lock_project
from architect.db import EVENT_COLUMNS, row_to_event
from architect.rules import RULES
from architect.state import STATE_TABLES, ArbiterState

Snapshot = dict[str, set[str]]


@dataclass(frozen=True)
class TableDiff:
    table: str
    stale: list[dict[str, Any]]  # rows the old state had that the ledger does not imply
    missing: list[dict[str, Any]]  # rows the ledger implies that the old state lacked

    @property
    def empty(self) -> bool:
        return not self.stale and not self.missing


def snapshot(conn: Connection[dict[str, Any]], project_id: str) -> Snapshot:
    """Each arb_* table as a set of canonical JSON rows."""
    out: Snapshot = {}
    for table in STATE_TABLES:
        rows = conn.execute(
            f"SELECT * FROM {table} WHERE project_id = %s", (project_id,)
        ).fetchall()
        out[table] = {json.dumps(row, sort_keys=True) for row in rows}
    return out


def rebuild_state(pool: ConnectionPool, project_id: str) -> tuple[int, list[TableDiff]]:
    """Drop the project's arb_* rows and fold the ledger back into them.

    Returns (events folded, per-table diff against the state that was there before).
    Runs in one transaction under the project's commit lock.
    """
    with pool.connection() as conn, conn.transaction():
        lock_project(conn, project_id)
        before = snapshot(conn, project_id)
        for table in STATE_TABLES:
            conn.execute(f"DELETE FROM {table} WHERE project_id = %s", (project_id,))

        folded = 0
        with conn.cursor(name="rebuild_events") as events, conn.cursor() as cur:
            state = ArbiterState(cur, project_id)
            events.execute(
                f"SELECT {EVENT_COLUMNS} FROM events WHERE project_id = %s ORDER BY seq",
                (project_id,),
            )
            for row in events:
                event = row_to_event(row)
                RULES[event["type"]].apply(event, state)
                folded += 1

        after = snapshot(conn, project_id)

    diffs = [
        TableDiff(
            table=table,
            stale=[json.loads(row) for row in sorted(before[table] - after[table])],
            missing=[json.loads(row) for row in sorted(after[table] - before[table])],
        )
        for table in STATE_TABLES
    ]
    return folded, diffs
