"""The projector: the one worker that keeps the proj_* read models up to date.

It folds each project's events in seq order (architect.projections.fold). Exactly-once comes
from the cursor: every batch runs in one transaction that locks the project's proj_cursors
row, folds the events after it, and advances it. A crash at any point rolls the batch back
with its cursor, so a restart resumes at last_seq + 1 and nothing is folded twice.

It reads `events` and writes only proj_*; it never commits an event or calls the Arbiter.
"""

from __future__ import annotations

import hashlib
import json
import threading
from collections.abc import Callable
from typing import Any

from psycopg import Connection
from psycopg_pool import ConnectionPool

from architect.db import EVENT_COLUMNS, EVENTS_CHANNEL, row_to_event
from architect.projections import PROJ_TABLES, PROJECTION, ProjectionError, fold

Event = dict[str, Any]

DEFAULT_BATCH = 200
DEFAULT_POLL_SECONDS = 2.0

_LAGGING_PROJECTS = """
SELECT p.project_id
FROM projects p
LEFT JOIN proj_cursors c ON c.project_id = p.project_id AND c.projection = %s
WHERE COALESCE(c.last_seq, -1)
    < COALESCE((SELECT max(e.seq) FROM events e WHERE e.project_id = p.project_id), -1)
ORDER BY p.project_id
"""


class Projector:
    def __init__(self, pool: ConnectionPool, batch_size: int = DEFAULT_BATCH) -> None:
        self._pool = pool
        self._batch_size = batch_size
        # Test hook: called inside the batch transaction after each event is folded. Raising
        # from it must leave no trace of the batch.
        self.after_event: Callable[[Event], None] | None = None

    def catch_up(self, project_id: str | None = None) -> int:
        """Fold every pending event of one project, or of all. Returns the events folded."""
        folded = 0
        projects = [project_id] if project_id is not None else self._lagging_projects()
        for pid in projects:
            while batch := self._fold_batch(pid):
                folded += batch
        return folded

    def rebuild(self, project_id: str) -> int:
        """Drop the project's read models and fold its whole ledger again."""
        with self._pool.connection() as conn, conn.transaction():
            self._lock_cursor(conn, project_id)
            for table in PROJ_TABLES:
                conn.execute(f"DELETE FROM {table} WHERE project_id = %s", (project_id,))
            conn.execute(
                "UPDATE proj_cursors SET last_seq = -1 WHERE projection = %s AND project_id = %s",
                (PROJECTION, project_id),
            )
        return self.catch_up(project_id)

    def run(
        self, stop: threading.Event | None = None, poll_seconds: float = DEFAULT_POLL_SECONDS
    ) -> None:
        """Keep every project caught up until `stop` is set.

        Wakes on the Arbiter's NOTIFY, and at least every `poll_seconds` regardless, so a
        missed notification delays a projection but never stalls it.
        """
        with self._pool.connection() as listener:
            listener.autocommit = True
            listener.execute(f"LISTEN {EVENTS_CHANNEL}")
            try:
                while stop is None or not stop.is_set():
                    self.catch_up()
                    for _ in listener.notifies(timeout=poll_seconds, stop_after=1):
                        pass
            finally:
                listener.execute(f"UNLISTEN {EVENTS_CHANNEL}")
                listener.autocommit = False

    def _lagging_projects(self) -> list[str]:
        with self._pool.connection() as conn:
            rows = conn.execute(_LAGGING_PROJECTS, (PROJECTION,)).fetchall()
        return [row["project_id"] for row in rows]

    def _lock_cursor(self, conn: Connection[dict[str, Any]], project_id: str) -> int:
        """Lock the project's cursor until the transaction ends and return its last_seq."""
        conn.execute(
            "INSERT INTO proj_cursors (projection, project_id, last_seq) VALUES (%s, %s, -1) "
            "ON CONFLICT DO NOTHING",
            (PROJECTION, project_id),
        )
        row = conn.execute(
            "SELECT last_seq FROM proj_cursors WHERE projection = %s AND project_id = %s "
            "FOR UPDATE",
            (PROJECTION, project_id),
        ).fetchone()
        return row["last_seq"]

    def _fold_batch(self, project_id: str, limit: int | None = None) -> int:
        """Fold the next events of a project, at most a batch, in one transaction."""
        folded = 0
        try:
            with self._pool.connection() as conn, conn.transaction():
                last_seq = self._lock_cursor(conn, project_id)
                rows = conn.execute(
                    f"SELECT {EVENT_COLUMNS} FROM events WHERE project_id = %s AND seq > %s "
                    "ORDER BY seq LIMIT %s",
                    (project_id, last_seq, limit or self._batch_size),
                ).fetchall()
                if not rows:
                    return 0
                with conn.cursor() as cur:
                    for row in rows:
                        event = row_to_event(row)
                        fold(cur, event)
                        folded += 1
                        if self.after_event is not None:
                            self.after_event(event)
                conn.execute(
                    "UPDATE proj_cursors SET last_seq = %s "
                    "WHERE projection = %s AND project_id = %s",
                    (rows[-1]["seq"], PROJECTION, project_id),
                )
        except ProjectionError:
            # The batch rolled back. Keep the events before the one that cannot be folded,
            # so the cursor stops right in front of it, then fail loudly.
            if folded:
                self._fold_batch(project_id, limit=folded)
            raise
        return folded


def status(pool: ConnectionPool, project_id: str) -> dict[str, Any]:
    """Each projection's cursor against the ledger's last seq."""
    with pool.connection() as conn, conn.transaction():
        ledger = conn.execute(
            "SELECT max(seq) AS last_seq FROM events WHERE project_id = %s", (project_id,)
        ).fetchone()
        cursor = conn.execute(
            "SELECT last_seq FROM proj_cursors WHERE projection = %s AND project_id = %s",
            (PROJECTION, project_id),
        ).fetchone()
    ledger_last = ledger["last_seq"]
    projected = cursor["last_seq"] if cursor and cursor["last_seq"] >= 0 else None
    behind = (-1 if ledger_last is None else ledger_last) - (-1 if projected is None else projected)
    return {
        "ledger_last_seq": ledger_last,
        "projections": [{"projection": PROJECTION, "last_seq": projected, "lag": behind}],
    }


def content_hash(pool: ConnectionPool, project_id: str) -> str:
    """sha256 over every proj_* row of the project, independent of physical row order."""
    digest = hashlib.sha256()
    with pool.connection() as conn, conn.transaction():
        for table in PROJ_TABLES:
            rows = conn.execute(
                f"SELECT to_jsonb(t) AS row FROM {table} t WHERE project_id = %s", (project_id,)
            ).fetchall()
            digest.update(f"{table}:{len(rows)}\n".encode())
            canonical = (
                json.dumps(r["row"], sort_keys=True, separators=(",", ":"), ensure_ascii=False)
                for r in rows
            )
            for line in sorted(canonical):
                digest.update(line.encode())
                digest.update(b"\n")
    return digest.hexdigest()
