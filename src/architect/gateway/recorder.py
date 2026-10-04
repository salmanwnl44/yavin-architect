"""The call log (gw_calls): every attempt, failure, cache hit and replay, append-only.

Model calls are not ledger events: the ledger holds decisions, the call log holds volume.
Agent messages reference call ids.

A provider attempt is written AHEAD: a `started` row (the request, the prompt hash and what
was reserved for it) goes in before the provider is called, and a second row closes it
afterwards: `ok` or `invalid_output` (state completed) or `error` (state failed), pointing
back at it through `started_id`. A `started` row that nothing closed is a call the process
did not live to record; the sweep closes it with an `abandoned` row and its reservation
stays charged. The `state` column is derived from `status`:

    started                             started
    ok, invalid_output, cache_hit, replay   completed
    error, budget_refused               failed
    abandoned                           abandoned
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from psycopg import Connection
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool
from ulid import ULID

log = logging.getLogger("architect.gateway")

STATUSES = (
    "started",
    "ok",
    "error",
    "invalid_output",
    "cache_hit",
    "replay",
    "budget_refused",
    "abandoned",
)
# the statuses that close a `started` row because the provider answered or failed
CLOSING = ("ok", "error", "invalid_output")

_INSERT = (
    "INSERT INTO gw_calls (call_id, scope, role, purpose, tier, provider, model, family, "
    "prompt_hash, request, response, error, tokens_in, tokens_out, usd, latency_ms, "
    "cache_hit, attempt, status, input_taints, started_id, reserved_tokens, reserved_usd) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
    "%s, %s, %s, %s, %s)"
)


def record(
    pool: ConnectionPool,
    *,
    scope: dict[str, str],
    role: str,
    purpose: str,
    tier: str,
    provider: str | None,
    model: str | None,
    family: str | None,
    prompt_hash: str,
    request: dict[str, Any],
    response: dict[str, Any] | None,
    error: str | None,
    tokens_in: int,
    tokens_out: int,
    usd: float,
    latency_ms: int,
    cache_hit: bool,
    attempt: int,
    status: str,
    input_taints: list[str],
    started_id: str | None = None,
    reserved_tokens: int = 0,
    reserved_usd: float = 0.0,
    conn: Connection[dict[str, Any]] | None = None,
) -> str:
    """Append one row and return its call_id. With `conn` the row joins that transaction."""
    if status not in STATUSES:
        raise ValueError(f"not a call status: {status!r}")
    call_id = f"call_{ULID()}"
    values = (
        call_id,
        Jsonb(scope),
        role,
        purpose,
        tier,
        provider,
        model,
        family,
        prompt_hash,
        Jsonb(request),
        Jsonb(response) if response is not None else None,
        error,
        tokens_in,
        tokens_out,
        Decimal(str(usd)),
        latency_ms,
        cache_hit,
        attempt,
        status,
        Jsonb(input_taints),
        started_id,
        reserved_tokens,
        Decimal(str(reserved_usd)),
    )
    if conn is not None:
        conn.execute(_INSERT, values)
    else:
        with pool.connection() as own:
            own.execute(_INSERT, values)
    log.debug(
        "gw call %s %s %s/%s attempt=%s status=%s",
        call_id,
        purpose,
        provider,
        model,
        attempt,
        status,
    )
    return call_id


def lock_started(conn: Connection[dict[str, Any]], started_id: str) -> bool:
    """Serialize everything that closes one started row (its completion, its abandonment),
    for the rest of the transaction. Returns whether an `abandoned` row already closed it."""
    conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (started_id,))
    row = conn.execute(
        "SELECT 1 AS yes FROM gw_calls WHERE started_id = %s AND status = 'abandoned'",
        (started_id,),
    ).fetchone()
    return row is not None


def orphans(
    conn: Connection[dict[str, Any]], *, scope: dict[str, str] | None, older_than_s: float
) -> list[dict[str, Any]]:
    """The started rows nothing has closed, at least `older_than_s` old, oldest first.
    `scope` narrows it to the calls whose scope contains it."""
    return conn.execute(
        "SELECT s.call_id, s.scope, s.role, s.purpose, s.tier, s.provider, s.model, s.family, "
        "s.prompt_hash, s.attempt, s.input_taints, s.reserved_tokens, s.reserved_usd "
        "FROM gw_calls s WHERE s.status = 'started' "
        "AND s.ts <= now() - make_interval(secs => %s) "
        "AND (%s::jsonb IS NULL OR s.scope @> %s::jsonb) "
        "AND NOT EXISTS (SELECT 1 FROM gw_calls c WHERE c.started_id = s.call_id) "
        "ORDER BY s.ts, s.call_id",
        (older_than_s, Jsonb(scope) if scope else None, Jsonb(scope) if scope else None),
    ).fetchall()


def recent(pool: ConnectionPool, limit: int) -> list[dict[str, Any]]:
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT call_id, ts, scope, role, purpose, tier, provider, model, family, prompt_hash, "
            "tokens_in, tokens_out, usd, latency_ms, cache_hit, attempt, status, state, "
            "started_id, reserved_tokens, reserved_usd, error "
            "FROM gw_calls ORDER BY ts DESC, call_id DESC LIMIT %s",
            (limit,),
        ).fetchall()
    return [
        row
        | {
            "usd": float(row["usd"]),
            "reserved_usd": float(row["reserved_usd"]),
            "ts": row["ts"].isoformat(),
        }
        for row in rows
    ]
