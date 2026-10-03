"""The response cache (gw_cache): a hit costs nothing and never reaches a provider."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def cache_key(
    provider: str,
    model: str,
    system: str,
    messages: list[dict[str, str]],
    output_schema: dict[str, Any] | None,
    temperature: float | None,
    max_tokens: int,
) -> str:
    payload = {
        "provider": provider,
        "model": model,
        "system": system,
        "messages": messages,
        "output_schema": output_schema,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    return hashlib.sha256(canonical(payload)).hexdigest()


def cacheable(mode: str, temperature: float) -> bool:
    """auto caches deterministic calls only; force always; off never."""
    if mode == "force":
        return True
    if mode == "off":
        return False
    return temperature == 0


def get(pool: ConnectionPool, key: str) -> dict[str, Any] | None:
    with pool.connection() as conn:
        row = conn.execute("SELECT response FROM gw_cache WHERE key = %s", (key,)).fetchone()
    return row["response"] if row else None


def put(pool: ConnectionPool, key: str, response: dict[str, Any]) -> None:
    with pool.connection() as conn:
        conn.execute(
            "INSERT INTO gw_cache (key, response) VALUES (%s, %s) ON CONFLICT (key) DO NOTHING",
            (key, Jsonb(response)),
        )
