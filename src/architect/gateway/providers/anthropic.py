"""The Anthropic provider, on the official SDK. The key comes from ARCHITECT_ANTHROPIC_API_KEY,
falling back to ANTHROPIC_API_KEY (then to the SDK's other credential sources), and is never
stored, logged or raised."""

from __future__ import annotations

import logging
from typing import Any

import anthropic

from architect.gateway.config import anthropic_api_key
from architect.gateway.errors import ProviderError
from architect.gateway.providers.base import ProviderCall, ProviderResult

log = logging.getLogger("architect.gateway.anthropic")


class AnthropicProvider:
    name = "anthropic"

    def __init__(self, client: anthropic.Anthropic | None = None) -> None:
        # The SDK's own retries are off: the gateway owns backoff, fallback and recording.
        # api_key=None leaves the SDK to its own credential resolution, as before.
        self._client = client or anthropic.Anthropic(api_key=anthropic_api_key(), max_retries=0)

    def complete(self, call: ProviderCall) -> ProviderResult:
        kwargs: dict[str, Any] = {
            "model": call.model,
            "max_tokens": call.max_tokens,
            "messages": call.messages,
        }
        if call.system:
            kwargs["system"] = call.system
        # The 1.x SDK has no sampling parameters: the current models take none. The request's
        # temperature still governs the gateway's caching policy.
        if call.output_schema is not None:
            # Native structured output: the API constrains the reply to the schema. (Forcing a
            # tool would be refused by the current Sonnet and Opus generations.)
            kwargs["output_config"] = {
                "format": {"type": "json_schema", "schema": call.output_schema}
            }
        try:
            response = self._client.messages.create(**kwargs)
        except anthropic.RateLimitError as error:
            raise ProviderError(
                self.name, call.model, "rate limited (429)", retryable=True, status=429
            ) from error
        except anthropic.APIStatusError as error:
            retryable = error.status_code == 529 or error.status_code >= 500
            raise ProviderError(
                self.name,
                call.model,
                f"HTTP {error.status_code}: {_safe_message(error)}",
                retryable=retryable,
                status=error.status_code,
            ) from error
        except anthropic.APITimeoutError as error:
            raise ProviderError(self.name, call.model, "timed out", retryable=True) from error
        except anthropic.APIConnectionError as error:
            raise ProviderError(
                self.name, call.model, "connection failed", retryable=True
            ) from error
        if response.stop_reason == "refusal":
            raise ProviderError(
                self.name, call.model, "the model refused the request", retryable=False
            )
        text = "".join(block.text for block in response.content if block.type == "text")
        log.debug(
            "anthropic call model=%s stop=%s request_id=%s",
            call.model,
            response.stop_reason,
            response._request_id,
        )
        return ProviderResult(
            text=text,
            tokens_in=response.usage.input_tokens,
            tokens_out=response.usage.output_tokens,
        )


def _safe_message(error: anthropic.APIStatusError) -> str:
    """The API's error message without anything from the request (keys live in headers, which
    the SDK never echoes; this keeps the text short and free of the body)."""
    return str(getattr(error, "message", "") or "")[:200]
