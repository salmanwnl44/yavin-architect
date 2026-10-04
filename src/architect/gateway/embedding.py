"""Embeddings through the gateway (P8): one path, recorded, cached, budget-scoped.

`Gateway.embed` delegates here. A text is embedded once per model: its vector is cached in
gw_embed_cache by (model, sha256 of the text), and a request whose texts are all cached
never reaches a provider. The texts that are not cached go to the first candidate of the
`embedding` tier in ONE provider call, written ahead like any other call (a `started` row
with the reservation, then the row that closes it). The call log records how many texts and
the hash of the batch, not the texts.
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from architect.gateway import cache, router
from architect.gateway.config import Candidate
from architect.gateway.errors import (
    AllCandidatesFailed,
    BudgetExceeded,
    NoEligibleModel,
    ProviderError,
    ReplayMiss,
)
from architect.gateway.recorder import record

if TYPE_CHECKING:
    from architect.gateway.gateway import Gateway

EMBEDDING_TIER = "embedding"


@dataclass(frozen=True)
class Embedded:
    """Vectors in the order of the texts asked for."""

    vectors: list[list[float]]
    provider: str
    model: str
    dim: int
    call_id: str
    tokens: int
    usd: float
    cached: int  # texts answered from the cache
    computed: int  # texts a provider embedded


def text_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def embedding_candidates(gateway: Gateway, tier: str = EMBEDDING_TIER) -> list[Candidate]:
    """The tier's candidates whose provider is registered and can embed, in order."""
    if tier not in gateway.config.tiers:
        return []
    return [
        candidate
        for candidate in router.candidates(gateway.config, tier, [])
        if hasattr(gateway.providers.get(candidate.provider), "embed")
    ]


def embed(
    gateway: Gateway,
    texts: list[str],
    *,
    purpose: str,
    scope: dict[str, str] | None,
    role: str,
    tier: str,
) -> Embedded:
    candidates = embedding_candidates(gateway, tier)
    if not candidates:
        raise NoEligibleModel(f"no embedding provider is configured for tier {tier!r}")
    failures: list[tuple[str, str, str]] = []
    for candidate in candidates:
        try:
            return _embed_with(gateway, candidate, texts, purpose, scope or {}, role, tier)
        except ProviderError as error:
            failures.append((candidate.provider, candidate.model, str(error)))
    raise AllCandidatesFailed(failures)


def _embed_with(
    gateway: Gateway,
    candidate: Candidate,
    texts: list[str],
    purpose: str,
    scope: dict[str, str],
    role: str,
    tier: str,
) -> Embedded:
    pool = gateway._pool
    hashes = [text_hash(text) for text in texts]
    by_hash = dict(zip(hashes, texts, strict=True))
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT text_hash, vector FROM gw_embed_cache WHERE model = %s AND text_hash = ANY(%s)",
            (candidate.model, sorted(by_hash)),
        ).fetchall()
    known: dict[str, list[float]] = {row["text_hash"]: row["vector"] for row in rows}
    missing = sorted(h for h in by_hash if h not in known)
    digest = hashlib.sha256(
        cache.canonical({"embed": candidate.model, "texts": sorted(by_hash)})
    ).hexdigest()
    ids = {"provider": candidate.provider, "model": candidate.model, "family": candidate.family}

    def log(**fields: Any) -> str:
        base = {
            "scope": scope,
            "role": role,
            "purpose": purpose,
            "tier": tier,
            "prompt_hash": digest,
            "request": {"kind": "embedding", "texts": len(texts), "to_embed": len(missing)},
            "input_taints": [],
            "error": None,
            "tokens_out": 0,
            "cache_hit": False,
        }
        return record(pool, **(base | ids | fields))

    def answer(call_id: str, tokens: int, usd: float) -> Embedded:
        vectors = [known[h] for h in hashes]
        return Embedded(
            vectors=vectors,
            provider=candidate.provider,
            model=candidate.model,
            dim=len(vectors[0]) if vectors else (candidate.dim or 0),
            call_id=call_id,
            tokens=tokens,
            usd=usd,
            cached=len(by_hash) - len(missing),
            computed=len(missing),
        )

    if not missing or gateway.mode == "replay":
        if missing:
            raise ReplayMiss(digest)
        call_id = log(
            response={"vectors": len(texts)},
            tokens_in=0,
            usd=0.0,
            latency_ms=0,
            cache_hit=True,
            attempt=0,
            status="replay" if gateway.mode == "replay" else "cache_hit",
        )
        return answer(call_id, 0, 0.0)

    provider = gateway.providers[candidate.provider]
    batch = [by_hash[h] for h in missing]
    estimate = sum(max(1, math.ceil(len(text) / 4)) for text in batch)
    estimate_usd = gateway.config.usd(candidate.model, estimate, 0)
    attempt = 0
    retries_left = gateway.config.max_attempts - 1
    while True:
        attempt += 1
        try:
            with pool.connection() as conn, conn.transaction():
                reservation = gateway._budget.reserve(scope, estimate, estimate_usd, conn)
                started_id = log(
                    response=None,
                    tokens_in=0,
                    usd=0.0,
                    latency_ms=0,
                    attempt=attempt,
                    status="started",
                    reserved_tokens=reservation.tokens,
                    reserved_usd=reservation.usd,
                    conn=conn,
                )
        except BudgetExceeded as refused:
            log(
                response=None,
                error=str(refused),
                tokens_in=0,
                usd=0.0,
                latency_ms=0,
                attempt=attempt,
                status="budget_refused",
            )
            raise
        started = gateway._clock()
        try:
            result = provider.embed(candidate.model, batch, candidate.dim)
            _check(candidate, batch, result.vectors)
        except ProviderError as error:
            gateway._close(
                log,
                started_id,
                reservation,
                response=None,
                error=str(error),
                tokens_in=0,
                tokens_out=0,
                usd=0.0,
                latency_ms=int((gateway._clock() - started) * 1000),
                attempt=attempt,
                status="error",
            )
            if error.retryable and retries_left > 0:
                retries_left -= 1
                gateway._sleep(gateway._backoff(attempt))
                continue
            raise
        usd = gateway.config.usd(candidate.model, result.tokens, 0)
        call_id = gateway._close(
            log,
            started_id,
            reservation,
            response={"vectors": len(batch), "dim": len(result.vectors[0])},
            tokens_in=result.tokens,
            tokens_out=0,
            usd=usd,
            latency_ms=int((gateway._clock() - started) * 1000),
            attempt=attempt,
            status="ok",
        )
        with pool.connection() as conn, conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO gw_embed_cache (model, text_hash, dim, vector) "
                "VALUES (%s, %s, %s, %s) ON CONFLICT (model, text_hash) DO NOTHING",
                [
                    (candidate.model, h, len(vector), vector)
                    for h, vector in zip(missing, result.vectors, strict=True)
                ],
            )
        known |= dict(zip(missing, result.vectors, strict=True))
        return answer(call_id, result.tokens, usd)


def _check(candidate: Candidate, batch: list[str], vectors: list[list[float]]) -> None:
    """A provider that returns the wrong number of vectors, or the wrong size, is broken."""
    sizes = {len(vector) for vector in vectors}
    wrong_size = candidate.dim is not None and sizes != {candidate.dim}
    if len(vectors) != len(batch) or len(sizes) != 1 or wrong_size:
        raise ProviderError(
            candidate.provider,
            candidate.model,
            f"malformed embeddings: {len(vectors)} vectors of sizes {sorted(sizes)} for "
            f"{len(batch)} texts (configured dim {candidate.dim})",
            retryable=False,
        )
