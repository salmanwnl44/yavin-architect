"""The Arbiter: the single writer of the ledger.

A candidate is a ledger event without `seq` and `prev_hash`. `submit` either commits it as
exactly one event (with the arb_* state it implies, in one transaction) or raises a typed
Rejection and writes nothing.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool
from ulid import ULID

from architect.contracts import json_path, load_contracts
from architect.db import EVENT_COLUMNS, EVENTS_CHANNEL, row_to_event
from architect.errors import Rejection
from architect.ledger import event_hash, project_exists
from architect.rules import RULES
from architect.state import ArbiterState

Event = dict[str, Any]

ARBITER_STAMPED = ("seq", "prev_hash")
_RFC3339 = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})")


@dataclass(frozen=True)
class Commit:
    event: Event
    replayed: bool  # True when the idempotency key had already been committed


def lock_project(conn: psycopg.Connection[Any], project_id: str) -> None:
    """Serialize with every other commit to this project until the transaction ends."""
    conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (project_id,))


def parse_ts(value: str) -> datetime:
    normalized = value.upper()
    if not _RFC3339.fullmatch(normalized):
        raise Rejection("SCHEMA_INVALID", f"{value!r} is not an RFC 3339 date-time", "$.ts")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise Rejection("SCHEMA_INVALID", f"{value!r} is not a valid date-time", "$.ts") from exc


class Arbiter:
    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool
        # Test hook: called after the event and state writes, just before the transaction
        # commits. Raising from it must leave no trace of the event.
        self.before_commit: Callable[[], None] | None = None

    def submit(self, project_id: str, candidate: Any) -> Commit:
        if not isinstance(candidate, dict):
            raise Rejection("SCHEMA_INVALID", "a candidate must be a JSON object", "$")
        for stamped in ARBITER_STAMPED:
            if stamped in candidate:
                raise Rejection(
                    "SCHEMA_INVALID",
                    f"{stamped} is stamped by the Arbiter; a candidate must not carry it",
                    f"$.{stamped}",
                )
        if candidate.get("project_id", project_id) != project_id:
            raise Rejection(
                "PROJECT_MISMATCH",
                f"candidate is for project {candidate['project_id']!r}, "
                f"submitted to {project_id!r}",
                "$.project_id",
            )
        try:
            with self._pool.connection() as conn, conn.transaction():
                return self._commit(conn, project_id, candidate)
        except psycopg.errors.UniqueViolation as exc:
            # Backstop for a client event_id racing across two projects: the per-project
            # lock does not cover the global uniqueness of event_id.
            if exc.diag.constraint_name != "events_event_id_key":
                raise
            raise Rejection(
                "DUPLICATE_EVENT_ID",
                f"event_id {candidate.get('event_id')} is already committed",
                "$.event_id",
            ) from exc
        except (psycopg.DataError, UnicodeError) as exc:
            raise Rejection("SCHEMA_INVALID", f"event cannot be stored: {exc}") from exc

    def _commit(
        self, conn: psycopg.Connection[dict[str, Any]], project_id: str, candidate: Event
    ) -> Commit:
        if not project_exists(conn, project_id):
            raise Rejection("UNKNOWN_PROJECT", f"project {project_id!r} does not exist")
        lock_project(conn, project_id)

        # A retry is answered before any validation: the state it was valid against has
        # since moved (by its own first commit, at least).
        key = candidate.get("idempotency_key")
        if isinstance(key, str):
            original = conn.execute(
                f"SELECT {EVENT_COLUMNS} FROM events "
                "WHERE project_id = %s AND idempotency_key = %s",
                (project_id, key),
            ).fetchone()
            if original is not None:
                return Commit(row_to_event(original), replayed=True)

        previous = conn.execute(
            f"SELECT {EVENT_COLUMNS} FROM events WHERE project_id = %s ORDER BY seq DESC LIMIT 1",
            (project_id,),
        ).fetchone()

        event = dict(candidate)
        event["project_id"] = project_id
        event.setdefault("event_id", f"evt_{ULID()}")
        event.setdefault("ts", datetime.now(UTC).isoformat(timespec="microseconds"))
        if previous is None:
            event["seq"] = 0
        else:
            event["seq"] = previous["seq"] + 1
            event["prev_hash"] = event_hash(row_to_event(previous))

        error = load_contracts().event_error(event)
        if error is not None:
            raise Rejection(
                "SCHEMA_INVALID",
                f"ledger_events.schema.json: {error.message}",
                json_path(error.path),
            )
        ts = parse_ts(event["ts"])

        duplicate = conn.execute(
            "SELECT 1 FROM events WHERE event_id = %s", (event["event_id"],)
        ).fetchone()
        if duplicate is not None:
            raise Rejection(
                "DUPLICATE_EVENT_ID",
                f"event_id {event['event_id']} is already committed",
                "$.event_id",
            )

        with conn.cursor() as cur:
            state = ArbiterState(cur, project_id)
            rule = RULES[event["type"]]
            rule.check(event, state)
            committed = cur.execute(
                "INSERT INTO events (project_id, seq, event_id, ts, ts_wire, actor, session_id, "
                "task_id, type, payload, idempotency_key, prev_hash) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                f"RETURNING {EVENT_COLUMNS}",
                (
                    project_id,
                    event["seq"],
                    event["event_id"],
                    ts,
                    event["ts"],
                    Jsonb(event["actor"]),
                    event.get("session_id"),
                    event.get("task_id"),
                    event["type"],
                    Jsonb(event["payload"]),
                    event["idempotency_key"],
                    event.get("prev_hash"),
                ),
            ).fetchone()
            rule.apply(event, state)
            # Delivered by Postgres only if and when this transaction commits.
            cur.execute("SELECT pg_notify(%s, %s)", (EVENTS_CHANNEL, project_id))

        if self.before_commit is not None:
            self.before_commit()
        return Commit(row_to_event(committed), replayed=False)
