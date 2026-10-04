"""The one public entry: Gateway.call(request) -> GatewayResponse."""

from __future__ import annotations

import hashlib
import math
import os
import random
import threading
import time
from collections.abc import Callable
from typing import Any

from psycopg_pool import ConnectionPool

from architect.gateway import cache, replay, router, structured, untrusted
from architect.gateway.budget import Budget, Reservation
from architect.gateway.config import Candidate, GatewayConfig, anthropic_api_key, load_config
from architect.gateway.errors import (
    AllCandidatesFailed,
    BudgetExceeded,
    ProviderError,
    StructuredOutputInvalid,
)
from architect.gateway.providers.base import Provider, ProviderCall, ProviderResult
from architect.gateway.recorder import lock_started, orphans, record
from architect.gateway.request import GatewayRequest, GatewayResponse


def prompt_hash(
    system: str,
    messages: list[dict[str, str]],
    output_schema: dict[str, Any] | None,
    temperature: float,
    max_tokens: int,
) -> str:
    """Deterministic across runs and independent of routing: what was asked, not who answered."""
    payload = {
        "system": system,
        "messages": messages,
        "output_schema": output_schema,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    return hashlib.sha256(cache.canonical(payload)).hexdigest()


def estimate_tokens(system: str, messages: list[dict[str, str]]) -> int:
    """A rough input estimate for reservations: four characters per token."""
    chars = len(system) + sum(len(m["content"]) for m in messages)
    return max(1, math.ceil(chars / 4))


class Gateway:
    def __init__(
        self,
        pool: ConnectionPool,
        config: GatewayConfig | None = None,
        providers: dict[str, Provider] | None = None,
        *,
        mode: str | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        rng: random.Random | None = None,
    ) -> None:
        self._pool = pool
        self.config = config or load_config()
        self.providers = providers or {}
        self.mode = mode or os.environ.get(replay.MODE_ENV, "live")
        if self.mode not in replay.MODES:
            raise ValueError(f"{replay.MODE_ENV} must be one of {replay.MODES}, not {self.mode!r}")
        self._clock, self._sleep = clock, sleep
        self._rng = rng or random.Random()
        self._budget = Budget(pool)
        self._semaphores = {
            name: threading.Semaphore(self.config.concurrency.get(name, 4))
            for name in self.providers
        }

    # ------------------------------------------------------------------ the call
    def call(self, request: GatewayRequest) -> GatewayResponse:
        system = untrusted.with_rule(request.system, list(request.input_taints))
        messages = [m.model_dump() for m in request.messages]
        digest = prompt_hash(
            system, messages, request.output_schema, request.temperature, request.max_tokens
        )
        scope = request.scope.present()
        logged_request = request.model_dump() | {"system": system}

        def log(**fields: Any) -> str:
            base = {
                "scope": scope,
                "role": request.role,
                "purpose": request.purpose,
                "tier": request.tier,
                "prompt_hash": digest,
                "request": logged_request,
                "input_taints": list(request.input_taints),
            }
            return record(self._pool, **(base | fields))

        if self.mode == "replay":
            row = replay.recorded(self._pool, digest)
            response = row["response"]
            call_id = log(
                provider=row["provider"],
                model=row["model"],
                family=row["family"],
                response=response,
                error=None,
                tokens_in=0,
                tokens_out=0,
                usd=0.0,
                latency_ms=0,
                cache_hit=True,
                attempt=0,
                status="replay",
            )
            return GatewayResponse(
                text=response["text"],
                parsed=response.get("parsed"),
                call_id=call_id,
                provider=row["provider"],
                model=row["model"],
                family=row["family"],
                tokens_in=0,
                tokens_out=0,
                usd=0.0,
                latency_ms=0,
                cache_hit=True,
                attempts=0,
            )

        failures: list[tuple[str, str, str]] = []
        for candidate in router.candidates(
            self.config, request.tier, list(request.exclude_families)
        ):
            temperature = request.temperature if candidate.sampling else None
            key = cache.cache_key(
                candidate.provider,
                candidate.model,
                system,
                messages,
                request.output_schema,
                temperature,
                request.max_tokens,
            )
            if cache.cacheable(request.cache, request.temperature):
                hit = cache.get(self._pool, key)
                if hit is not None:
                    call_id = log(
                        provider=candidate.provider,
                        model=candidate.model,
                        family=candidate.family,
                        response=hit,
                        error=None,
                        tokens_in=0,
                        tokens_out=0,
                        usd=0.0,
                        latency_ms=0,
                        cache_hit=True,
                        attempt=0,
                        status="cache_hit",
                    )
                    return GatewayResponse(
                        text=hit["text"],
                        parsed=hit.get("parsed"),
                        call_id=call_id,
                        provider=candidate.provider,
                        model=candidate.model,
                        family=candidate.family,
                        tokens_in=0,
                        tokens_out=0,
                        usd=0.0,
                        latency_ms=0,
                        cache_hit=True,
                        attempts=0,
                    )
            provider = self.providers.get(candidate.provider)
            if provider is None:
                failures.append(
                    (candidate.provider, candidate.model, "no such provider is configured")
                )
                continue
            try:
                return self._serve(candidate, provider, request, system, messages, key, scope, log)
            except ProviderError as error:
                failures.append((candidate.provider, candidate.model, str(error)))
        raise AllCandidatesFailed(failures)

    # ------------------------------------------------------------------ one candidate
    def _serve(
        self,
        candidate: Candidate,
        provider: Provider,
        request: GatewayRequest,
        system: str,
        messages: list[dict[str, str]],
        key: str,
        scope: dict[str, str],
        log: Callable[..., str],
    ) -> GatewayResponse:
        temperature = request.temperature if candidate.sampling else None
        conversation = list(messages)
        attempt = 0
        retries_left = self.config.max_attempts - 1
        structured_left = self.config.structured_retries
        spent_in = spent_out = 0
        spent_usd = 0.0
        ids = {"provider": candidate.provider, "model": candidate.model, "family": candidate.family}
        while True:
            attempt += 1
            call = ProviderCall(
                model=candidate.model,
                system=system,
                messages=conversation,
                output_schema=request.output_schema,
                max_tokens=request.max_tokens,
                temperature=temperature,
            )
            estimate_in = estimate_tokens(system, conversation)
            estimate_usd = self.config.usd(candidate.model, estimate_in, request.max_tokens)
            try:
                # written AHEAD: the reservation and the `started` row commit together, before
                # the provider hears anything. If this process dies during the call, the row
                # is what remains of it (see sweep_abandoned).
                with self._pool.connection() as conn, conn.transaction():
                    reservation = self._budget.reserve(
                        scope, estimate_in + request.max_tokens, estimate_usd, conn
                    )
                    started_id = log(
                        **ids,
                        response=None,
                        error=None,
                        tokens_in=0,
                        tokens_out=0,
                        usd=0.0,
                        latency_ms=0,
                        cache_hit=False,
                        attempt=attempt,
                        status="started",
                        reserved_tokens=reservation.tokens,
                        reserved_usd=reservation.usd,
                        conn=conn,
                    )
            except BudgetExceeded as refused:
                log(
                    **ids,
                    response=None,
                    error=str(refused),
                    tokens_in=0,
                    tokens_out=0,
                    usd=0.0,
                    latency_ms=0,
                    cache_hit=False,
                    attempt=attempt,
                    status="budget_refused",
                )
                raise
            started = self._clock()
            try:
                result = self._complete(candidate.provider, provider, call)
            except ProviderError as error:
                latency = int((self._clock() - started) * 1000)
                self._close(
                    log,
                    started_id,
                    reservation,
                    **ids,
                    response=None,
                    error=str(error),
                    tokens_in=0,
                    tokens_out=0,
                    usd=0.0,
                    latency_ms=latency,
                    cache_hit=False,
                    attempt=attempt,
                    status="error",
                )
                if error.retryable and retries_left > 0:
                    retries_left -= 1
                    self._sleep(self._backoff(attempt))
                    continue
                raise
            latency = int((self._clock() - started) * 1000)
            usd = self.config.usd(candidate.model, result.tokens_in, result.tokens_out)
            spent_in += result.tokens_in
            spent_out += result.tokens_out
            spent_usd += usd
            parsed = None
            if request.output_schema is not None:
                parsed, problem = structured.parse(result.text, request.output_schema)
                if problem is not None:
                    self._close(
                        log,
                        started_id,
                        reservation,
                        **ids,
                        response={"text": result.text},
                        error=problem,
                        tokens_in=result.tokens_in,
                        tokens_out=result.tokens_out,
                        usd=usd,
                        latency_ms=latency,
                        cache_hit=False,
                        attempt=attempt,
                        status="invalid_output",
                    )
                    if structured_left > 0:
                        structured_left -= 1
                        conversation = conversation + [
                            {"role": "assistant", "content": result.text},
                            {"role": "user", "content": structured.correction(problem)},
                        ]
                        continue
                    raise StructuredOutputInvalid(attempt, problem)
            response = {"text": result.text, "parsed": parsed}
            call_id = self._close(
                log,
                started_id,
                reservation,
                **ids,
                response=response,
                error=None,
                tokens_in=result.tokens_in,
                tokens_out=result.tokens_out,
                usd=usd,
                latency_ms=latency,
                cache_hit=False,
                attempt=attempt,
                status="ok",
            )
            if cache.cacheable(request.cache, request.temperature):
                cache.put(self._pool, key, response)
            return GatewayResponse(
                text=result.text,
                parsed=parsed,
                call_id=call_id,
                **ids,
                tokens_in=spent_in,
                tokens_out=spent_out,
                usd=spent_usd,
                latency_ms=latency,
                cache_hit=False,
                attempts=attempt,
            )

    def _close(
        self, log: Callable[..., str], started_id: str, reservation: Reservation, **row: Any
    ) -> str:
        """Close a started call: its second row and its spend, in one transaction. A call
        the sweep had already abandoned (it was slow, not dead) is put right: the actual
        numbers replace the reservation that was charged for it."""
        with self._pool.connection() as conn, conn.transaction():
            was_abandoned = lock_started(conn, started_id)
            call_id = log(**row, started_id=started_id, conn=conn)
            self._budget.settle(
                reservation,
                row["tokens_in"] + row["tokens_out"],
                row["usd"],
                conn,
                was_abandoned=was_abandoned,
            )
        return call_id

    # ------------------------------------------------------------------ calls nobody closed
    def sweep_abandoned(
        self, *, scope: dict[str, str] | None = None, older_than_s: float | None = None
    ) -> list[str]:
        """Close every `started` row that has no second row and is older than
        `older_than_s` (default: the configured `abandon_after_s`) with an `abandoned` row,
        and turn its reservation into spend: the provider may have been paid, so the books
        err upwards. `scope` narrows the sweep to calls whose scope contains it. Safe to run
        from anywhere, any number of times: each started row is closed once. Returns the
        call ids of the started rows it abandoned."""
        age = self.config.abandon_after_s if older_than_s is None else older_than_s
        with self._pool.connection() as conn:
            candidates = orphans(conn, scope=scope, older_than_s=age)
        abandoned: list[str] = []
        for row in candidates:
            with self._pool.connection() as conn, conn.transaction():
                lock_started(conn, row["call_id"])
                closed = conn.execute(
                    "SELECT 1 AS yes FROM gw_calls WHERE started_id = %s", (row["call_id"],)
                ).fetchone()
                if closed is not None:
                    continue  # it finished, or another sweep got here first
                record(
                    self._pool,
                    scope=row["scope"],
                    role=row["role"],
                    purpose=row["purpose"],
                    tier=row["tier"],
                    provider=row["provider"],
                    model=row["model"],
                    family=row["family"],
                    prompt_hash=row["prompt_hash"],
                    request={"started_id": row["call_id"]},
                    response=None,
                    error="started and never closed; its reservation stays charged",
                    tokens_in=0,
                    tokens_out=0,
                    usd=0.0,
                    latency_ms=0,
                    cache_hit=False,
                    attempt=row["attempt"],
                    status="abandoned",
                    input_taints=row["input_taints"],
                    started_id=row["call_id"],
                    reserved_tokens=row["reserved_tokens"],
                    reserved_usd=float(row["reserved_usd"]),
                    conn=conn,
                )
                Budget.abandon(
                    conn,
                    Reservation(row["scope"], row["reserved_tokens"], float(row["reserved_usd"])),
                )
            abandoned.append(row["call_id"])
        return abandoned

    def _complete(self, name: str, provider: Provider, call: ProviderCall) -> ProviderResult:
        semaphore = self._semaphores.get(name)
        if semaphore is None:
            return provider.complete(call)
        with semaphore:
            return provider.complete(call)

    def _backoff(self, attempt: int) -> float:
        """Exponential with jitter: base * 2^(attempt-1) + U(0, base), capped."""
        base = self.config.base_delay_s
        delay = base * (2 ** (attempt - 1)) + self._rng.uniform(0, base)
        return min(delay, self.config.max_delay_s)

    # ------------------------------------------------------------------ reads
    def spend(self, scope: dict[str, str]) -> dict[str, Any] | None:
        return self._budget.spend(scope)

    def limits(self, scope: dict[str, str]) -> dict[str, dict[str, Any]]:
        return self._budget.limits(scope)


def default_providers() -> dict[str, Provider]:
    """The providers the environment allows: the mock always, the real ones when configured."""
    from architect.gateway.providers.mock import MockProvider

    providers: dict[str, Provider] = {"mock": MockProvider()}
    if anthropic_api_key():
        from architect.gateway.providers.anthropic import AnthropicProvider

        providers["anthropic"] = AnthropicProvider()
    if os.environ.get("OPENAI_COMPAT_BASE_URL"):
        from architect.gateway.providers.openai_compat import OpenAICompatProvider

        providers["openai_compat"] = OpenAICompatProvider()
    return providers


__all__ = ["Gateway", "Reservation", "default_providers", "estimate_tokens", "prompt_hash"]
