"""The client side of a session: start, signal, query, and the session read model. What the
CLI and the API call; the first slice of the control surface Yavin drives (M12)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from psycopg_pool import ConnectionPool
from temporalio.client import Client, WorkflowHandle
from ulid import ULID

from architect.ingestion.objectstore import ObjectStore
from architect.sessions.config import SessionConfig, temporal_address
from architect.sessions.types import WORKFLOW_NAME, SessionInput

SIGNALS = ("pause", "resume", "cancel", "approve", "reject", "steer")


def new_session_id() -> str:
    return f"ses_{ULID()}"


def now_rfc3339() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def build_input(
    config: SessionConfig,
    *,
    project_id: str,
    brief: str,
    preset: str = "quick",
    overrides: dict[str, Any] | None = None,
    sources: list[str] | None = None,
    session_id: str | None = None,
    started_at: str | None = None,
) -> SessionInput:
    limits = config.preset(preset).limits(overrides)
    limits["checkpoint_minutes"] = config.checkpoint_minutes
    return SessionInput(
        project_id=project_id,
        session_id=session_id or new_session_id(),
        brief=brief,
        preset=preset,
        started_at=started_at or now_rfc3339(),
        limits=limits,
        sources=list(sources or []),
    )


async def connect(address: str | None = None) -> Client:
    return await Client.connect(address or temporal_address())


async def start(client: Client, input: SessionInput, task_queue: str) -> WorkflowHandle:
    return await client.start_workflow(
        WORKFLOW_NAME, input, id=input.session_id, task_queue=task_queue
    )


async def signal(client: Client, session_id: str, name: str, text: str | None = None) -> None:
    if name not in SIGNALS:
        raise ValueError(f"unknown signal {name!r}; choose from {SIGNALS}")
    handle = client.get_workflow_handle(session_id)
    if name == "steer":
        if not text:
            raise ValueError("steer needs a text")
        await handle.signal("steer", text)
    else:
        await handle.signal(name)


async def status(client: Client, session_id: str) -> dict[str, Any]:
    return await client.get_workflow_handle(session_id).query("status")


# ---------------------------------------------------------------- the read model
def session_row(pool: ConnectionPool, project_id: str, session_id: str) -> dict[str, Any] | None:
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT project_id, session_id, preset, limits, status, outcome, phase, round, "
            "best_version, open_risks, spend, package_key, brief_source_id, started_at, "
            "updated_at FROM ses_sessions WHERE project_id = %s AND session_id = %s",
            (project_id, session_id),
        ).fetchone()
    if row is None:
        return None
    return row | {"updated_at": row["updated_at"].isoformat()}


def list_sessions(pool: ConnectionPool, project_id: str) -> list[dict[str, Any]]:
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT session_id, preset, status, outcome, phase, round, best_version, "
            "package_key, started_at FROM ses_sessions WHERE project_id = %s ORDER BY started_at",
            (project_id,),
        ).fetchall()
    return rows


def timeline(pool: ConnectionPool, project_id: str, session_id: str) -> list[dict[str, Any]]:
    with pool.connection() as conn:
        return conn.execute(
            "SELECT seq, kind, from_phase, phase, detail, ts FROM proj_session_timeline "
            "WHERE project_id = %s AND session_id = %s ORDER BY seq",
            (project_id, session_id),
        ).fetchall()


def load_package(store: ObjectStore, key: str) -> dict[str, Any]:
    return json.loads(store.get(key).decode("utf-8"))


def show(
    pool: ConnectionPool, store: ObjectStore, project_id: str, session_id: str
) -> dict[str, Any] | None:
    """Everything a returning owner reads first: the row, the phase timeline, the rounds,
    the gate, the open risks and the package key."""
    row = session_row(pool, project_id, session_id)
    if row is None:
        return None
    package = load_package(store, row["package_key"]) if row.get("package_key") else None
    return {
        "session": row,
        "timeline": timeline(pool, project_id, session_id),
        "rounds": package["rounds"] if package else [],
        "gate": package["gate"] if package else None,
        "open_risks": package["open_risks"] if package else row["open_risks"],
        "package_key": row.get("package_key"),
    }
