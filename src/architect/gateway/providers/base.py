"""What every provider adapter implements."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

SECRET_HEADERS = ("authorization", "x-api-key")


@dataclass(frozen=True)
class ProviderCall:
    """One attempt's inputs, as the gateway hands them to a provider."""

    model: str
    system: str
    messages: list[dict[str, str]]
    output_schema: dict[str, Any] | None
    max_tokens: int
    temperature: float | None  # None when the model does not accept one


@dataclass(frozen=True)
class ProviderResult:
    text: str
    tokens_in: int
    tokens_out: int


@dataclass(frozen=True)
class EmbedResult:
    vectors: list[list[float]]  # one per text, in order
    tokens: int


class Provider(Protocol):
    name: str

    def complete(self, call: ProviderCall) -> ProviderResult:
        """One model call. Raises ProviderError(retryable=...) on failure."""


class EmbeddingProvider(Protocol):
    """A provider that can also embed. `dim` is the configured size of the model's vectors."""

    name: str

    def embed(self, model: str, texts: list[str], dim: int | None = None) -> EmbedResult:
        """One vector per text. Raises ProviderError(retryable=...) on failure."""


def redact(headers: dict[str, str]) -> dict[str, str]:
    """Headers safe to log: credentials replaced."""
    return {k: ("<redacted>" if k.lower() in SECRET_HEADERS else v) for k, v in headers.items()}
