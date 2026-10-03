"""The client side of a session: start, signal, query, and the session read model. What the
CLI and the API call; the first slice of the control surface Yavin drives (M12)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from psycopg_pool import ConnectionPool
from temporalio.client import Client, WorkflowHandle
from ulid import ULID

from architect import modeldiff, readmodel
from architect.ingestion.objectstore import ObjectStore
from architect.sessions.config import SessionConfig, temporal_address
from architect.sessions.types import WORKFLOW_NAME, SessionInput

DECISIONS = ("approve", "approve_with_risks", "reject", "extend")
SIGNALS = ("pause", "resume", "cancel", "steer", *DECISIONS)
EXTENSION_FIELDS = ("tokens", "usd", "wall_clock_minutes", "rounds")
EXTENDABLE_OUTCOMES = ("stopped_budget", "stopped_time")

_ROW_COLUMNS = (
    "project_id, session_id, preset, limits, status, outcome, phase, round, best_version, "
    "open_risks, spend, package_key, brief_source_id, started_at, updated_at, failure, "
    "last_refusal, gate_verdict, waivers"
)


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
    seed: dict[str, Any] | None = None,
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
        seed=seed,
    )


async def connect(address: str | None = None) -> Client:
    return await Client.connect(address or temporal_address())


async def start(client: Client, input: SessionInput, task_queue: str) -> WorkflowHandle:
    return await client.start_workflow(
        WORKFLOW_NAME, input, id=input.session_id, task_queue=task_queue
    )


def decision_problem(
    row: dict[str, Any],
    name: str,
    *,
    reason: str | None = None,
    extension: dict[str, Any] | None = None,
) -> str | None:
    """Why a decision would be refused, judged from the session read model, or None. The
    workflow applies the same rules and is the authority; this lets the CLI and the API say
    no at once instead of sending a signal that will be refused."""
    if row["status"] != "awaiting_approval":
        return f"no human gate is open: the session is {row['status']}"
    at_end = row.get("phase") == "package"
    if not at_end:
        if name in ("approve", "reject"):
            return None
        return f"{name} applies at the end gate; here approve continues and reject stops"
    if name == "approve" and row.get("gate_verdict") != "ALLOWED":
        return (
            f"the package's gate is {row.get('gate_verdict')}: approve_with_risks with a "
            "reason, extend, or reject"
        )
    if name == "approve_with_risks" and not (reason or "").strip():
        return "approve_with_risks needs a non-empty reason"
    if name == "extend":
        amounts = {k: v for k, v in (extension or {}).items() if v is not None}
        if row.get("outcome") not in EXTENDABLE_OUTCOMES:
            return "extend applies to sessions stopped by their budget or the wall clock"
        if any(
            isinstance(v, bool) or not isinstance(v, (int, float)) or v < 0
            for v in amounts.values()
        ):
            return "extension amounts must be non-negative numbers"
        if row["outcome"] == "stopped_time" and not amounts.get("wall_clock_minutes"):
            return "a session stopped by the wall clock needs wall_clock_minutes"
        if row["outcome"] == "stopped_budget" and not (amounts.get("tokens") or amounts.get("usd")):
            return "a session stopped by its budget needs tokens or usd"
    return None


async def signal(
    client: Client,
    session_id: str,
    name: str,
    text: str | None = None,
    *,
    reason: str | None = None,
    signer: str | None = None,
    extension: dict[str, Any] | None = None,
) -> None:
    name = name.replace("-", "_")
    if name not in SIGNALS:
        raise ValueError(f"unknown signal {name!r}; choose from {SIGNALS}")
    handle = client.get_workflow_handle(session_id)
    who = {"signer": signer} if signer else {}
    if name == "steer":
        if not text:
            raise ValueError("steer needs a text")
        await handle.signal("steer", text)
    elif name == "approve_with_risks":
        if not (reason or "").strip():
            raise ValueError("approve_with_risks needs a non-empty reason")
        await handle.signal(name, {"reason": reason, **who})
    elif name == "extend":
        unknown = set(extension or {}) - set(EXTENSION_FIELDS)
        if unknown:
            raise ValueError(f"extend takes {EXTENSION_FIELDS}, not {sorted(unknown)}")
        amounts = {k: v for k, v in (extension or {}).items() if v is not None}
        if not amounts:
            raise ValueError("extend needs at least one of " + ", ".join(EXTENSION_FIELDS))
        await handle.signal(name, {**amounts, **who})
    elif name in ("approve", "reject"):
        await handle.signal(name, who)
    else:
        await handle.signal(name)


async def status(client: Client, session_id: str) -> dict[str, Any]:
    return await client.get_workflow_handle(session_id).query("status")


# ---------------------------------------------------------------- the read model
def _row(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return row | {"updated_at": row["updated_at"].isoformat()}


def session_row(pool: ConnectionPool, project_id: str, session_id: str) -> dict[str, Any] | None:
    with pool.connection() as conn:
        row = conn.execute(
            f"SELECT {_ROW_COLUMNS} FROM ses_sessions WHERE project_id = %s AND session_id = %s",
            (project_id, session_id),
        ).fetchone()
    return _row(row)


def find_session(pool: ConnectionPool, session_id: str) -> dict[str, Any] | None:
    """A session by its id alone (the most recently updated when a replay reused the id)."""
    with pool.connection() as conn:
        row = conn.execute(
            f"SELECT {_ROW_COLUMNS} FROM ses_sessions WHERE session_id = %s "
            "ORDER BY updated_at DESC LIMIT 1",
            (session_id,),
        ).fetchone()
    return _row(row)


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


def round_diffs(
    pool: ConnectionPool, project_id: str, rounds: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Each round with the structural diff its repair made: the attacked version against the
    repaired one (nothing when the round committed no repair)."""
    out: list[dict[str, Any]] = []
    for round_ in rounds:
        entry = dict(round_)
        before = (round_.get("attack") or {}).get("version")
        after = (round_.get("repair") or {}).get("version")
        if before and after and before != after:
            old = readmodel.model_version(pool, project_id, before)
            new = readmodel.model_version(pool, project_id, after)
            if old is not None and new is not None:
                diff = modeldiff.diff_models(old["model"], new["model"])
                entry["diff"] = {
                    "from": before,
                    "to": after,
                    "summary": modeldiff.summary(diff),
                    "lines": modeldiff.format_diff(diff),
                }
        out.append(entry)
    return out


def show(
    pool: ConnectionPool, store: ObjectStore, project_id: str, session_id: str
) -> dict[str, Any] | None:
    """Everything a returning owner reads first: the row, the phase timeline, the rounds with
    the diff each repair made, the gate, the open risks and the package key."""
    row = session_row(pool, project_id, session_id)
    if row is None:
        return None
    package = load_package(store, row["package_key"]) if row.get("package_key") else None
    return {
        "session": row,
        "timeline": timeline(pool, project_id, session_id),
        "rounds": round_diffs(pool, project_id, package["rounds"]) if package else [],
        "gate": package["gate"] if package else None,
        "open_risks": package["open_risks"] if package else row["open_risks"],
        "package_key": row.get("package_key"),
    }
