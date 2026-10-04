"""An OpenAI-compatible chat-completions provider (vLLM and friends), on httpx2. The base URL
comes from OPENAI_COMPAT_BASE_URL and the optional key from OPENAI_COMPAT_API_KEY."""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import httpx2 as httpx

from architect.gateway.errors import ProviderError
from architect.gateway.providers.base import (
    EmbedResult,
    ProviderCall,
    ProviderResult,
    redact,
)

log = logging.getLogger("architect.gateway.openai_compat")

BASE_URL_ENV = "OPENAI_COMPAT_BASE_URL"
API_KEY_ENV = "OPENAI_COMPAT_API_KEY"


class OpenAICompatProvider:
    name = "openai_compat"

    def __init__(
        self,
        base_url: str | None = None,
        api_key: str | None = None,
        *,
        transport: httpx.BaseTransport | None = None,
        timeout: float = 120.0,
    ) -> None:
        self._base_url = (base_url or os.environ.get(BASE_URL_ENV, "")).rstrip("/")
        if not self._base_url:
            raise ValueError(f"{BASE_URL_ENV} is not set")
        key = api_key if api_key is not None else os.environ.get(API_KEY_ENV)
        headers = {"Content-Type": "application/json"}
        if key:
            headers["Authorization"] = f"Bearer {key}"
        self._client = httpx.Client(headers=headers, timeout=timeout, transport=transport)

    def complete(self, call: ProviderCall) -> ProviderResult:
        body = self._body(call, json_schema=True)
        response = self._post(body, call)
        if (
            response.status_code == 400
            and call.output_schema is not None
            and _mentions_response_format(response)
        ):
            # The server lacks json_schema: fall back to JSON mode, with the schema in the prompt.
            log.debug("server lacks response_format json_schema; falling back to JSON mode")
            response = self._post(self._body(call, json_schema=False), call)
        if response.status_code != 200:
            retryable = response.status_code == 429 or response.status_code >= 500
            raise ProviderError(
                self.name,
                call.model,
                f"HTTP {response.status_code}",
                retryable=retryable,
                status=response.status_code,
            )
        data = response.json()
        try:
            text = data["choices"][0]["message"]["content"] or ""
            usage = data.get("usage", {})
        except (KeyError, IndexError, TypeError) as error:
            raise ProviderError(
                self.name, call.model, "malformed completion", retryable=False
            ) from error
        return ProviderResult(
            text=text,
            tokens_in=int(usage.get("prompt_tokens", 0)),
            tokens_out=int(usage.get("completion_tokens", 0)),
        )

    def embed(self, model: str, texts: list[str], dim: int | None = None) -> EmbedResult:
        """POST /v1/embeddings: how a local embedding server plugs in."""
        url = f"{self._base_url}/v1/embeddings"
        log.debug("POST %s headers=%s model=%s", url, redact(dict(self._client.headers)), model)
        try:
            response = self._client.post(url, json={"model": model, "input": texts})
        except httpx.TimeoutException as error:
            raise ProviderError(self.name, model, "timed out", retryable=True) from error
        except httpx.TransportError as error:
            raise ProviderError(self.name, model, "connection failed", retryable=True) from error
        if response.status_code != 200:
            retryable = response.status_code == 429 or response.status_code >= 500
            raise ProviderError(
                self.name,
                model,
                f"HTTP {response.status_code}",
                retryable=retryable,
                status=response.status_code,
            )
        data = response.json()
        try:
            items = sorted(data["data"], key=lambda item: item["index"])
            vectors = [[float(x) for x in item["embedding"]] for item in items]
            usage = data.get("usage") or {}
        except (KeyError, TypeError, ValueError) as error:
            raise ProviderError(
                self.name, model, "malformed embeddings", retryable=False
            ) from error
        tokens = int(usage.get("prompt_tokens", usage.get("total_tokens", 0)))
        return EmbedResult(vectors=vectors, tokens=tokens)

    def _post(self, body: dict[str, Any], call: ProviderCall) -> httpx.Response:
        url = f"{self._base_url}/v1/chat/completions"
        log.debug(
            "POST %s headers=%s model=%s", url, redact(dict(self._client.headers)), call.model
        )
        try:
            return self._client.post(url, json=body)
        except httpx.TimeoutException as error:
            raise ProviderError(self.name, call.model, "timed out", retryable=True) from error
        except httpx.TransportError as error:
            raise ProviderError(
                self.name, call.model, "connection failed", retryable=True
            ) from error

    @staticmethod
    def _body(call: ProviderCall, *, json_schema: bool) -> dict[str, Any]:
        messages: list[dict[str, str]] = []
        system = call.system
        if call.output_schema is not None and not json_schema:
            schema_text = json.dumps(call.output_schema, sort_keys=True)
            instruction = "Reply with only a JSON document matching this schema:"
            system = f"{system}\n\n{instruction}\n{schema_text}".strip()
        if system:
            messages.append({"role": "system", "content": system})
        messages += call.messages
        body: dict[str, Any] = {
            "model": call.model,
            "messages": messages,
            "max_tokens": call.max_tokens,
        }
        if call.temperature is not None:
            body["temperature"] = call.temperature
        if call.output_schema is not None:
            if json_schema:
                body["response_format"] = {
                    "type": "json_schema",
                    "json_schema": {"name": "output", "schema": call.output_schema, "strict": True},
                }
            else:
                body["response_format"] = {"type": "json_object"}
        return body


def _mentions_response_format(response: httpx.Response) -> bool:
    try:
        return "response_format" in response.text or "json_schema" in response.text
    except Exception:  # noqa: BLE001 - a body we cannot read is not a schema complaint
        return False
