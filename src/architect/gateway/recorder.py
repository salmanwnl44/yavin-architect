"""The call log (gw_calls): every attempt, failure, cache hit and replay, append-only.

Model calls are not ledger events: the ledger holds decisions, the call log holds volume.
Agent messages reference call ids.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool
from ulid import ULID

log = logging.getLogger("architect.gateway")

STATUSES = ("ok", "error", "invalid_output", "cache_hit", "replay", "budget_refused")


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
) -> str:
    """Append one row and return its call_id."""
    if status not in STATUSES:
        raise ValueError(f"not a call status: {status!r}")
    call_id = f"call_{ULID()}"
    with pool.connection() as conn:
        conn.execute(
            "INSERT INTO gw_calls (call_id, scope, role, purpose, tier, provider, model, family, "
            "prompt_hash, request, response, error, tokens_in, tokens_out, usd, latency_ms, "
            "cache_hit, attempt, status, input_taints) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
            "%s, %s)",
            (
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
            ),
        )
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


def recent(pool: ConnectionPool, limit: int) -> list[dict[str, Any]]:
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT call_id, ts, scope, role, purpose, tier, provider, model, family, prompt_hash, "
            "tokens_in, tokens_out, usd, latency_ms, cache_hit, attempt, status, error "
            "FROM gw_calls ORDER BY ts DESC, call_id DESC LIMIT %s",
            (limit,),
        ).fetchall()
    return [row | {"usd": float(row["usd"]), "ts": row["ts"].isoformat()} for row in rows]
