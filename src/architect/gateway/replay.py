"""Replay mode: answer from the call log by exact prompt hash, never from a provider."""

from __future__ import annotations

from typing import Any

from psycopg_pool import ConnectionPool

from architect.gateway.errors import ReplayMiss

MODE_ENV = "ARCHITECT_GATEWAY_MODE"
MODES = ("live", "replay")


def recorded(pool: ConnectionPool, prompt_hash: str) -> dict[str, Any]:
    """The most recent successful response recorded for the prompt, or ReplayMiss."""
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT call_id, provider, model, family, response, tokens_in, tokens_out "
            "FROM gw_calls WHERE prompt_hash = %s AND status = 'ok' "
            "ORDER BY ts DESC, call_id DESC LIMIT 1",
            (prompt_hash,),
        ).fetchone()
    if row is None:
        raise ReplayMiss(prompt_hash)
    return row
