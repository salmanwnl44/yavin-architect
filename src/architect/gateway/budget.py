"""Budgets: limits come from budget.updated events (through the proj_budgets read model);
spend lives in gw_spend, one row per scope.

A call's scope is up to {tenant, session, phase}. Spend is tracked under every non-empty
subset of it, so a budget set on any of those subsets applies, now or later. Before a
provider call the estimate is RESERVED under a row lock and refused if any scope would go
over; after the call the reservation is SETTLED to the actual numbers. Concurrent callers
therefore never overspend. Null limits are uncapped (the Industrial tier's rule).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from itertools import combinations
from typing import Any

from psycopg import Connection
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from architect.gateway.errors import BudgetExceeded

CAPPED = ("tokens", "usd")  # wall_clock_minutes and gpu_minutes are recorded, not enforced here


def scope_key(scope: dict[str, str]) -> str:
    return json.dumps(scope, sort_keys=True, separators=(",", ":"))


def sub_scopes(scope: dict[str, str]) -> list[dict[str, str]]:
    """Every non-empty subset of the scope, largest first."""
    keys = sorted(scope)
    out: list[dict[str, str]] = []
    for size in range(len(keys), 0, -1):
        for chosen in combinations(keys, size):
            out.append({k: scope[k] for k in chosen})
    return out


@dataclass(frozen=True)
class Reservation:
    scope: dict[str, str]
    tokens: int
    usd: float


class Budget:
    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    def limits(self, scope: dict[str, str]) -> dict[str, dict[str, Any]]:
        """scope key -> the latest limits set on that exact sub-scope."""
        with self._pool.connection() as conn:
            return self._limits(conn, scope)

    @staticmethod
    def _limits(
        conn: Connection[dict[str, Any]], scope: dict[str, str]
    ) -> dict[str, dict[str, Any]]:
        rows = conn.execute(
            "SELECT DISTINCT ON (scope) scope, limits FROM proj_budgets "
            "WHERE scope <@ %s ORDER BY scope, seq DESC",
            (Jsonb(scope),),
        ).fetchall()
        return {scope_key(row["scope"]): row["limits"] for row in rows if row["scope"]}

    def reserve(self, scope: dict[str, str], tokens: int, usd: float) -> Reservation:
        """Hold the estimate against every sub-scope, or refuse before any provider is called."""
        reservation = Reservation(scope, tokens, usd)
        subsets = sub_scopes(scope)
        if not subsets:
            return reservation
        with self._pool.connection() as conn, conn.transaction():
            for sub in subsets:
                conn.execute(
                    "INSERT INTO gw_spend (scope_key, scope) VALUES (%s, %s) "
                    "ON CONFLICT DO NOTHING",
                    (scope_key(sub), Jsonb(sub)),
                )
            rows = conn.execute(
                "SELECT scope_key, scope, tokens, usd, reserved_tokens, reserved_usd FROM gw_spend "
                "WHERE scope_key = ANY(%s) ORDER BY scope_key FOR UPDATE",
                ([scope_key(sub) for sub in subsets],),
            ).fetchall()
            limits = self._limits(conn, scope)
            for row in rows:
                caps = limits.get(row["scope_key"])
                if not caps:
                    continue
                would = {
                    "tokens": row["tokens"] + row["reserved_tokens"] + tokens,
                    "usd": float(row["usd"]) + float(row["reserved_usd"]) + usd,
                }
                for dimension in CAPPED:
                    cap = caps.get(dimension)
                    if cap is not None and would[dimension] > cap:
                        raise BudgetExceeded(row["scope"], dimension, float(cap), would[dimension])
            conn.execute(
                "UPDATE gw_spend SET reserved_tokens = reserved_tokens + %s, "
                "reserved_usd = reserved_usd + %s WHERE scope_key = ANY(%s)",
                (tokens, Decimal(str(usd)), [scope_key(sub) for sub in subsets]),
            )
        return reservation

    def settle(self, reservation: Reservation, tokens: int, usd: float) -> None:
        """Replace the reservation with what the call actually cost."""
        self._adjust(reservation, tokens, usd)

    def release(self, reservation: Reservation) -> None:
        """Drop the reservation of a call that did not happen or failed."""
        self._adjust(reservation, 0, 0.0)

    def _adjust(self, reservation: Reservation, tokens: int, usd: float) -> None:
        subsets = sub_scopes(reservation.scope)
        if not subsets:
            return
        with self._pool.connection() as conn, conn.transaction():
            conn.execute(
                "UPDATE gw_spend SET reserved_tokens = GREATEST(reserved_tokens - %s, 0), "
                "reserved_usd = GREATEST(reserved_usd - %s, 0), tokens = tokens + %s, "
                "usd = usd + %s, calls = calls + %s WHERE scope_key = ANY(%s)",
                (
                    reservation.tokens,
                    Decimal(str(reservation.usd)),
                    tokens,
                    Decimal(str(usd)),
                    1 if tokens or usd else 0,
                    [scope_key(sub) for sub in subsets],
                ),
            )

    def spend(self, scope: dict[str, str]) -> dict[str, Any] | None:
        """What one exact scope has spent so far."""
        with self._pool.connection() as conn:
            row = conn.execute(
                "SELECT scope, tokens, usd, reserved_tokens, reserved_usd, calls FROM gw_spend "
                "WHERE scope_key = %s",
                (scope_key(scope),),
            ).fetchone()
        if row is None:
            return None
        return row | {"usd": float(row["usd"]), "reserved_usd": float(row["reserved_usd"])}
