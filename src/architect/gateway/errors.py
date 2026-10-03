"""Typed gateway failures. None of them ever carries a secret."""

from __future__ import annotations

from typing import Any


class GatewayError(Exception):
    """Base of every error the gateway raises on purpose."""


class NoEligibleModel(GatewayError):
    """No candidate of the tier survives exclude_families."""


class BudgetExceeded(GatewayError):
    """The call would take a scope over its cap; the provider was not called."""

    def __init__(self, scope: dict[str, Any], dimension: str, cap: float, would_be: float):
        super().__init__(
            f"budget for {scope} would exceed its {dimension} cap of {cap:g} "
            f"(would be {would_be:g})"
        )
        self.scope, self.dimension, self.cap, self.would_be = scope, dimension, cap, would_be


class StructuredOutputInvalid(GatewayError):
    """Every attempt produced output that failed the schema."""

    def __init__(self, attempts: int, last_error: str):
        super().__init__(f"structured output invalid after {attempts} attempts: {last_error}")
        self.attempts, self.last_error = attempts, last_error


class ReplayMiss(GatewayError):
    """Replay mode found no recorded response for the prompt hash."""

    def __init__(self, prompt_hash: str):
        super().__init__(f"no recorded response for prompt {prompt_hash}")
        self.prompt_hash = prompt_hash


class ProviderError(GatewayError):
    """A provider call failed. `retryable` decides between backoff and fallback."""

    def __init__(
        self,
        provider: str,
        model: str,
        message: str,
        *,
        retryable: bool,
        status: int | None = None,
    ):
        super().__init__(f"{provider}/{model}: {message}")
        self.provider, self.model, self.retryable, self.status = provider, model, retryable, status


class AllCandidatesFailed(GatewayError):
    """Every candidate of the tier failed. `failures` names each with its last error."""

    def __init__(self, failures: list[tuple[str, str, str]]):
        names = "; ".join(f"{provider}/{model}: {why}" for provider, model, why in failures)
        super().__init__(f"all candidates failed: {names}")
        self.failures = failures
